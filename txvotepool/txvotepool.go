package txvotepool

import (
	"container/list"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/Fantom-foundation/go-txflow/types"
	cfg "github.com/tendermint/tendermint/config"
	auto "github.com/tendermint/tendermint/libs/autofile"
	"github.com/tendermint/tendermint/libs/clist"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/mempool"
)

// TxVotePool is an ordered in-memory pool for votes before they are proposed in a consensus
// round.
type TxVotePool struct {
	config *cfg.MempoolConfig

	proxyMtx sync.Mutex
	txs      *clist.CList // concurrent linked-list of good txs

	// notify listeners (ie. consensus) when txs are available
	notifiedTxsAvailable bool
	txsAvailable         chan struct{} // fires once for each height, when the mempool is not empty

	// Atomic integers
	height int64 // the last event Update()'d to

	// Map for quick access to txs to record sender in CheckTx.
	// txsMap: txKey -> CElement
	txsMap   sync.Map
	txsBytes int64 // total size of mempool, in bytes

	// Keep a cache of already-seen txs.
	// This reduces the pressure on the proxyApp.
	cache txCache

	// A log of mempool txs
	wal *auto.AutoFile

	logger log.Logger

	metrics *mempool.Metrics
}

// TxVotePoolOption sets an optional parameter on the Mempool.
type TxVotePoolOption func(*TxVotePool)

// NewMempool returns a new Mempool with the given configuration and connection to an application.
func NewTxVotePool(
	config *cfg.MempoolConfig,
	height int64,
	options ...TxVotePoolOption,
) *TxVotePool {
	txVotePool := &TxVotePool{
		config:  config,
		txs:     clist.New(),
		height:  height,
		logger:  log.NewNopLogger(),
		metrics: mempool.NopMetrics(),
	}
	if config.CacheSize > 0 {
		txVotePool.cache = newMapTxCache(config.CacheSize)
	} else {
		txVotePool.cache = nopTxCache{}
	}
	for _, option := range options {
		option(txVotePool)
	}
	return txVotePool
}

// EnableTxsAvailable initializes the TxsAvailable channel,
// ensuring it will trigger once every height when transactions are available.
// NOTE: not thread safe - should only be called once, on startup
func (txVotePool *TxVotePool) EnableTxsAvailable() {
	txVotePool.txsAvailable = make(chan struct{}, 1)
}

// SetLogger sets the Logger.
func (txVotePool *TxVotePool) SetLogger(l log.Logger) {
	txVotePool.logger = l
}

// WithMetrics sets the metrics.
func WithMetrics(metrics *mempool.Metrics) TxVotePoolOption {
	return func(txVotePool *TxVotePool) { txVotePool.metrics = metrics }
}

// InitWAL creates a directory for the WAL file and opens a file itself.
//
// *panics* if can't create directory or open file.
// *not thread safe*
func (txVotePool *TxVotePool) InitWAL() {
	walDir := txVotePool.config.WalDir()
	err := cmn.EnsureDir(walDir, 0700)
	if err != nil {
		panic(errors.Wrap(err, "Error ensuring Mempool WAL dir"))
	}
	af, err := auto.OpenAutoFile(walDir + "/txwal")
	if err != nil {
		panic(errors.Wrap(err, "Error opening Mempool WAL file"))
	}
	txVotePool.wal = af
}

// CloseWAL closes and discards the underlying WAL file.
// Any further writes will not be relayed to disk.
func (txVotePool *TxVotePool) CloseWAL() {
	txVotePool.proxyMtx.Lock()
	defer txVotePool.proxyMtx.Unlock()

	if err := txVotePool.wal.Close(); err != nil {
		txVotePool.logger.Error("Error closing WAL", "err", err)
	}
	txVotePool.wal = nil
}

// Lock locks the mempool. The consensus must be able to hold lock to safely update.
func (txVotePool *TxVotePool) Lock() {
	txVotePool.proxyMtx.Lock()
}

// Unlock unlocks the mempool.
func (txVotePool *TxVotePool) Unlock() {
	txVotePool.proxyMtx.Unlock()
}

// Size returns the number of transactions in the mempool.
func (txVotePool *TxVotePool) Size() int {
	return txVotePool.txs.Len()
}

// TxsBytes returns the total size of all txs in the mempool.
func (txVotePool *TxVotePool) TxsBytes() int64 {
	return atomic.LoadInt64(&txVotePool.txsBytes)
}

// Flush removes all transactions from the mempool and cache
func (txVotePool *TxVotePool) Flush() {
	txVotePool.proxyMtx.Lock()
	defer txVotePool.proxyMtx.Unlock()

	txVotePool.cache.Reset()

	for e := txVotePool.txs.Front(); e != nil; e = e.Next() {
		txVotePool.txs.Remove(e)
		e.DetachPrev()
	}

	txVotePool.txsMap = sync.Map{}
	_ = atomic.SwapInt64(&txVotePool.txsBytes, 0)
}

// TxsFront returns the first transaction in the ordered list for peer
// goroutines to call .NextWait() on.
func (txVotePool *TxVotePool) TxsFront() *clist.CElement {
	return txVotePool.txs.Front()
}

// TxsWaitChan returns a channel to wait on transactions. It will be closed
// once the mempool is not empty (ie. the internal `mem.txs` has at least one
// element)
func (txVotePool *TxVotePool) TxsWaitChan() <-chan struct{} {
	return txVotePool.txs.WaitChan()
}

// CheckTx executes a new transaction against the application to determine its validity
// and whether it should be added to the mempool.
// It blocks if we're waiting on Update() or Reap().
// cb: A callback from the CheckTx command.
//     It gets called from another goroutine.
// CONTRACT: Either cb will get called, or err returned.
func (txVotePool *TxVotePool) CheckTx(tx types.TxVote) (err error) {
	return txVotePool.CheckTxWithInfo(tx, mempool.TxInfo{SenderID: UnknownPeerID})
}

// CheckTxWithInfo performs the same operation as CheckTx, but with extra meta data about the tx.
// Currently this metadata is the peer who sent it,
// used to prevent the tx from being gossiped back to them.
func (txVotePool *TxVotePool) CheckTxWithInfo(tx types.TxVote, txInfo mempool.TxInfo) (err error) {
	txVotePool.proxyMtx.Lock()
	// use defer to unlock mutex because application (*local client*) might panic
	defer txVotePool.proxyMtx.Unlock()

	var (
		memSize  = txVotePool.Size()
		txsBytes = txVotePool.TxsBytes()
	)

	if memSize >= txVotePool.config.Size ||
		int64(tx.Size())+txsBytes > txVotePool.config.MaxTxsBytes {
		return ErrMempoolIsFull{
			memSize, txVotePool.config.Size,
			txsBytes, txVotePool.config.MaxTxsBytes}
	}

	// The size of the corresponding amino-encoded TxMessage
	// can't be larger than the maxMsgSize, otherwise we can't
	// relay it to peers.
	if tx.Size() > maxTxSize {
		return mempool.ErrTxTooLarge
	}

	// CACHE
	if !txVotePool.cache.Push(tx) {
		// Record a new sender for a tx we've already seen.
		// Note it's possible a tx is still in the cache but no longer in the mempool
		// (eg. after committing a block, txs are removed from mempool but not cache),
		// so we only record the sender for txs still in the mempool.
		if e, ok := txVotePool.txsMap.Load(txVoteKey(tx)); ok {
			memTxVote := e.(*clist.CElement).Value.(*MempoolTxVote)
			if _, loaded := memTxVote.Senders.LoadOrStore(txInfo.SenderID, true); loaded {
				// TODO: consider punishing peer for dups,
				// its non-trivial since invalid txs can become valid,
				// but they can spam the same tx with little cost to them atm.
			}
		}

		return mempool.ErrTxInCache
	}
	// END CACHE

	// WAL
	if txVotePool.wal != nil {
		// TODO: Notify administrators when WAL fails
		_, err := txVotePool.wal.Write([]byte(cdc.MustMarshalBinaryBare(tx)))
		if err != nil {
			txVotePool.logger.Error("Error writing to WAL", "err", err)
		}
		_, err = txVotePool.wal.Write([]byte("\n"))
		if err != nil {
			txVotePool.logger.Error("Error writing to WAL", "err", err)
		}
	}
	// END WAL

	memTxVote := &MempoolTxVote{
		height: txVotePool.height,
		Tx:     tx,
	}

	memTxVote.Senders.Store(txInfo.SenderID, true)
	txVotePool.addTx(memTxVote)
	txVotePool.logger.Info("Added good vote",
		"signature", TxVoteID(tx),
		"height", memTxVote.height,
		"total", txVotePool.Size(),
	)
	txVotePool.notifyTxsAvailable()
	txVotePool.metrics.Size.Set(float64(txVotePool.Size()))

	return nil
}

// Called from:
//  - resCbFirstTime (lock not held) if tx is valid
func (txVotePool *TxVotePool) addTx(memTx *MempoolTxVote) {
	e := txVotePool.txs.PushBack(memTx)
	txVotePool.txsMap.Store(txVoteKey(memTx.Tx), e)
	atomic.AddInt64(&txVotePool.txsBytes, int64(memTx.Tx.Size()))
	txVotePool.metrics.TxSizeBytes.Observe(float64(memTx.Tx.Size()))
}

// Called from:
//  - Update (lock held) if tx was committed
// 	- resCbRecheck (lock not held) if tx was invalidated
func (txVotePool *TxVotePool) removeTx(tx types.TxVote, elem *clist.CElement, removeFromCache bool) {
	txVotePool.txs.Remove(elem)
	elem.DetachPrev()
	txVotePool.txsMap.Delete(txVoteKey(tx))
	atomic.AddInt64(&txVotePool.txsBytes, int64(-tx.Size()))

	if removeFromCache {
		txVotePool.cache.Remove(tx)
	}
}

// TxsAvailable returns a channel which fires once for every height,
// and only when transactions are available in the mempool.
// NOTE: the returned channel may be nil if EnableTxsAvailable was not called.
func (txVotePool *TxVotePool) TxsAvailable() <-chan struct{} {
	return txVotePool.txsAvailable
}

func (txVotePool *TxVotePool) notifyTxsAvailable() {
	if txVotePool.Size() == 0 {
		panic("notified txs available but mempool is empty!")
	}
	if txVotePool.txsAvailable != nil && !txVotePool.notifiedTxsAvailable {
		// channel cap is 1, so this will send once
		txVotePool.notifiedTxsAvailable = true
		select {
		case txVotePool.txsAvailable <- struct{}{}:
		default:
		}
	}
}

// ReapMaxTxs reaps up to max transactions from the mempool.
// If max is negative, there is no cap on the size of all returned
// transactions (~ all available transactions).
func (txVotePool *TxVotePool) ReapMaxTxs(max int) []types.TxVote {
	txVotePool.proxyMtx.Lock()
	defer txVotePool.proxyMtx.Unlock()

	if max < 0 {
		max = txVotePool.txs.Len()
	}

	txs := make([]types.TxVote, 0, cmn.MinInt(txVotePool.txs.Len(), max))
	for e := txVotePool.txs.Front(); e != nil && len(txs) <= max; e = e.Next() {
		memTx := e.Value.(*MempoolTxVote)
		txs = append(txs, memTx.Tx)
	}
	return txs
}

// Update informs the mempool that the given txs were committed and can be discarded.
// NOTE: this should be called *after* block is committed by consensus.
// NOTE: unsafe; Lock/Unlock must be managed by caller
func (txVotePool *TxVotePool) Update(
	height int64,
	txs []types.TxVote,
) error {

	// Set Height
	txVotePool.height = height
	txVotePool.notifiedTxsAvailable = false

	// Add committed transactions to cache (if missing).
	for _, tx := range txs {
		_ = txVotePool.cache.Push(tx)
		if e, ok := txVotePool.txsMap.Load(txVoteKey(tx)); ok {
			txVotePool.removeTx(tx, e.(*clist.CElement), false)
		}
	}

	// Remove committed transactions.
	//txsLeft := txVotePool.removeTxs(txs)

	// Either recheck non-committed txs to see if they became invalid
	// or just notify there're some txs left.
	if txVotePool.Size() > 0 {
		txVotePool.notifyTxsAvailable()
	}

	// Update metrics
	txVotePool.metrics.Size.Set(float64(txVotePool.Size()))

	return nil
}

//--------------------------------------------------------------------------------

// mempoolTxVote is a transaction that successfully ran
type MempoolTxVote struct {
	height int64        // height that this tx had been validated in
	Tx     types.TxVote //

	// ids of peers who've sent us this tx (as a map for quick lookups).
	// senders: PeerID -> bool
	Senders sync.Map
}

// Height returns the height for this transaction
func (memTxVote *MempoolTxVote) Height() int64 {
	return atomic.LoadInt64(&memTxVote.height)
}

//--------------------------------------------------------------------------------

type txCache interface {
	Reset()
	Push(tx types.TxVote) bool
	Remove(tx types.TxVote)
}

// mapTxCache maintains a LRU cache of transactions. This only stores the hash
// of the tx, due to memory concerns.
type mapTxCache struct {
	mtx  sync.Mutex
	size int
	map_ map[[sha256.Size]byte]*list.Element
	list *list.List
}

var _ txCache = (*mapTxCache)(nil)

// newMapTxCache returns a new mapTxCache.
func newMapTxCache(cacheSize int) *mapTxCache {
	return &mapTxCache{
		size: cacheSize,
		map_: make(map[[sha256.Size]byte]*list.Element, cacheSize),
		list: list.New(),
	}
}

// Reset resets the cache to an empty state.
func (cache *mapTxCache) Reset() {
	cache.mtx.Lock()
	cache.map_ = make(map[[sha256.Size]byte]*list.Element, cache.size)
	cache.list.Init()
	cache.mtx.Unlock()
}

// Push adds the given tx to the cache and returns true. It returns
// false if tx is already in the cache.
func (cache *mapTxCache) Push(tx types.TxVote) bool {
	cache.mtx.Lock()
	defer cache.mtx.Unlock()

	// Use the tx hash in the cache
	txHash := txVoteKey(tx)
	if moved, exists := cache.map_[txHash]; exists {
		cache.list.MoveToBack(moved)
		return false
	}

	if cache.list.Len() >= cache.size {
		popped := cache.list.Front()
		poppedTxHash := popped.Value.([sha256.Size]byte)
		delete(cache.map_, poppedTxHash)
		if popped != nil {
			cache.list.Remove(popped)
		}
	}
	e := cache.list.PushBack(txHash)
	cache.map_[txHash] = e
	return true
}

// Remove removes the given tx from the cache.
func (cache *mapTxCache) Remove(tx types.TxVote) {
	cache.mtx.Lock()
	txHash := txVoteKey(tx)
	popped := cache.map_[txHash]
	delete(cache.map_, txHash)
	if popped != nil {
		cache.list.Remove(popped)
	}

	cache.mtx.Unlock()
}

type nopTxCache struct{}

var _ txCache = (*nopTxCache)(nil)

func (nopTxCache) Reset()                 {}
func (nopTxCache) Push(types.TxVote) bool { return true }
func (nopTxCache) Remove(types.TxVote)    {}

// TxVoteID is the hex encoded hash of the bytes as a types.Tx.
func TxVoteID(tx types.TxVote) string {
	return string(tx.Signature)
}

// txVoteKey is the fixed length array sha256 hash used as the key in maps.
func txVoteKey(tx types.TxVote) [sha256.Size]byte {
	return sha256.Sum256(tx.Signature)
}

// ErrMempoolIsFull means Tendermint & an application can't handle that much load
type ErrMempoolIsFull struct {
	numTxs int
	maxTxs int

	txsBytes    int64
	maxTxsBytes int64
}

func (e ErrMempoolIsFull) Error() string {
	return fmt.Sprintf(
		"mempool is full: number of txs %d (max: %d), total txs bytes %d (max: %d)",
		e.numTxs, e.maxTxs,
		e.txsBytes, e.maxTxsBytes)
}
