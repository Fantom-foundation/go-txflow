package eventpool

import (
	"container/list"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/andrecronje/babble-abci/config"
	"github.com/andrecronje/babble-abci/types"
	abci "github.com/tendermint/tendermint/abci/types"
	auto "github.com/tendermint/tendermint/libs/autofile"
	"github.com/tendermint/tendermint/libs/clist"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/log"
)

// PreCheckFunc is an optional filter executed before CheckTx and rejects
// transaction if false is returned. An example would be to ensure that a
// transaction doesn't exceeded the block size.
type PreCheckFunc func(types.EventBlock) error

// EventInfo are parameters that get passed when attempting to add a event to the
// mempool.
type EventInfo struct {
	// We don't use p2p.ID here because it's too big. The gain is to store max 2
	// bytes with each tx to identify the sender rather than 20 bytes.
	PeerID uint16
}

var (
	// ErrEventInCache is returned to the client if we saw event earlier
	ErrEventInCache = errors.New("Event already exists in cache")

	// ErrEventTooLarge means the event is too big to be sent in a message to other peers
	ErrEventTooLarge = fmt.Errorf("Event too large. Max size is %d", maxEventSize)
)

// ErrEventpoolIsFull means Tendermint & an application can't handle that much load
type ErrEventpoolIsFull struct {
	numEvents int
	maxEvents int

	eventsBytes    int64
	maxEventsBytes int64
}

func (e ErrEventpoolIsFull) Error() string {
	return fmt.Sprintf(
		"Eventpool is full: number of events %d (max: %d), total event bytes %d (max: %d)",
		e.numEvents, e.maxEvents,
		e.eventsBytes, e.maxEventsBytes)
}

// ErrPreCheck is returned when tx is too big
type ErrPreCheck struct {
	Reason error
}

func (e ErrPreCheck) Error() string {
	return e.Reason.Error()
}

// IsPreCheckError returns true if err is due to pre check failure.
func IsPreCheckError(err error) bool {
	_, ok := err.(ErrPreCheck)
	return ok
}

// EventID is the hex encoded hash of the event.
func EventID(event *types.EventBlock) string {
	return fmt.Sprintf("%X", event.Hash())
}

// eventKey is the fixed length array sha256 hash used as the key in maps.
func eventKey(event *types.EventBlock) [sha256.Size]byte {
	return sha256.Sum256(event.Hash())
}

// proposerKey is the fixed length array sha256 hash used as the key in maps.
func proposerKey(event *types.EventBlock) [sha256.Size]byte {
	return sha256.Sum256(event.ProposerAddress)
}

// Eventpool is an ordered in-memory pool for events before they are proposed in a consensus
// round. The Eventpool uses a concurrent list structure for storing transactions that
// can be efficiently accessed by multiple concurrent readers.
type Eventpool struct {
	config   *config.EventpoolConfig
	proxyMtx sync.Mutex

	events *clist.CList // concurrent linked-list of good events

	// notify listeners (ie. consensus) when events are available
	notifiedEventsAvailable bool
	eventsAvailable         chan struct{} // fires once for each height, when the eventpool is not empty

	// Map for quick access to events to record sender.
	// eventsMap: eventKey -> CElement
	eventsMap     sync.Map
	headEventsMap sync.Map

	// Atomic integers
	height      int64 // the last event Update()'d to
	rechecking  int32 // for re-checking filtered events on Update()
	eventsBytes int64 // total size of eventpool, in bytes

	// Keep a cache of already-seen events.
	cache eventsCache

	// A log of eventspool events
	wal *auto.AutoFile

	logger log.Logger

	metrics *Metrics
}

// EventOption sets an optional parameter on the Eventpool.
type EventpoolOption func(*Eventpool)

// NewEventpool returns a new Eventpool with the given configuration and connection to an application.
func NewEventpool(
	config *config.EventpoolConfig,
	height int64,
	options ...EventpoolOption,
) *Eventpool {
	eventpool := &Eventpool{
		config:     config,
		events:     clist.New(),
		height:     height,
		rechecking: 0,
		logger:     log.NewNopLogger(),
		metrics:    NopMetrics(),
	}
	if config.CacheSize > 0 {
		eventpool.cache = newMapEventsCache(config.CacheSize)
	} else {
		eventpool.cache = nopEventsCache{}
	}
	for _, option := range options {
		option(eventpool)
	}
	return eventpool
}

// EnableEventsAvailable initializes the EventsAvailable channel,
// ensuring it will trigger once every height when events are available.
// NOTE: not thread safe - should only be called once, on startup
func (ep *Eventpool) EnableEventsAvailable() {
	ep.eventsAvailable = make(chan struct{}, 1)
}

// SetLogger sets the Logger.
func (ep *Eventpool) SetLogger(l log.Logger) {
	ep.logger = l
}

// WithMetrics sets the metrics.
func WithMetrics(metrics *Metrics) EventpoolOption {
	return func(ep *Eventpool) { ep.metrics = metrics }
}

// InitWAL creates a directory for the WAL file and opens a file itself.
//
// *panics* if can't create directory or open file.
// *not thread safe*
func (ep *Eventpool) InitWAL() {
	//TODO: will conflict with mempool, create a new WAL dir in config
	walDir := ep.config.WalDir()
	err := cmn.EnsureDir(walDir, 0700)
	if err != nil {
		panic(errors.Wrap(err, "Error ensuring Mempool WAL dir"))
	}
	af, err := auto.OpenAutoFile(walDir + "/ewal")
	if err != nil {
		panic(errors.Wrap(err, "Error opening Mempool WAL file"))
	}
	ep.wal = af
}

// CloseWAL closes and discards the underlying WAL file.
// Any further writes will not be relayed to disk.
func (ep *Eventpool) CloseWAL() {
	ep.proxyMtx.Lock()
	defer ep.proxyMtx.Unlock()

	if err := ep.wal.Close(); err != nil {
		ep.logger.Error("Error closing WAL", "err", err)
	}
	ep.wal = nil
}

// Lock locks the eventpool. The consensus must be able to hold lock to safely update.
func (ep *Eventpool) Lock() {
	ep.proxyMtx.Lock()
}

// Unlock unlocks the mempool.
func (ep *Eventpool) Unlock() {
	ep.proxyMtx.Unlock()
}

// Size returns the number of events in the eventpool.
func (ep *Eventpool) Size() int {
	return ep.events.Len()
}

// EventsBytes returns the total size of all events in the mempool.
func (ep *Eventpool) EventsBytes() int64 {
	return atomic.LoadInt64(&ep.eventsBytes)
}

// Flush removes all transactions from the eventpool and cache
func (ep *Eventpool) Flush() {
	ep.proxyMtx.Lock()
	defer ep.proxyMtx.Unlock()

	ep.cache.Reset()

	for e := ep.events.Front(); e != nil; e = e.Next() {
		ep.events.Remove(e)
		e.DetachPrev()
	}

	ep.eventsMap = sync.Map{}
	_ = atomic.SwapInt64(&ep.eventsBytes, 0)
}

// EventsFront returns the first event in the ordered list for peer
// goroutines to call .NextWait() on.
func (ep *Eventpool) EventsFront() *clist.CElement {
	return ep.events.Front()
}

// EventsWaitChan returns a channel to wait on events. It will be closed
// once the eventpool is not empty (ie. the internal `ep.events` has at least one
// element)
func (ep *Eventpool) EventsWaitChan() <-chan struct{} {
	return ep.events.WaitChan()
}

// CheckEvent determine the events validity
// and whether it should be added to the eventpool.
// It blocks if we're waiting on Update() or Reap().
// cb: A callback from the CheckTx command.
//     It gets called from another goroutine.
// CONTRACT: Either cb will get called, or err returned.
func (ep *Eventpool) CheckEvent(event *types.EventBlock, cb func(*abci.Response)) (err error) {
	return ep.CheckEventWithInfo(event, cb, EventInfo{PeerID: UnknownPeerID})
}

// CheckEventWithInfo performs the same operation as CheckEvent, but with extra meta data about the event.
// Currently this metadata is the peer who sent it,
// used to prevent the event from being gossiped back to them.
func (ep *Eventpool) CheckEventWithInfo(event *types.EventBlock, cb func(*abci.Response), eventInfo EventInfo) (err error) {
	ep.proxyMtx.Lock()
	// use defer to unlock mutex because application (*local client*) might panic
	defer ep.proxyMtx.Unlock()

	var (
		memSize     = ep.Size()
		eventsBytes = ep.EventsBytes()
	)
	//TODO: Check Size
	if memSize >= ep.config.Size ||
		int64(event.Size())+eventsBytes > ep.config.MaxEventsBytes {
		return ErrEventpoolIsFull{
			memSize, ep.config.Size,
			eventsBytes, ep.config.MaxEventsBytes}
	}

	// The size of the corresponding amino-encoded EventMessage
	// can't be larger than the maxMsgSize, otherwise we can't
	// relay it to peers.
	if event.Size() > maxEventSize {
		return ErrEventTooLarge
	}

	// CACHE
	if !ep.cache.Push(event) {
		// Record a new sender for a event we've already seen.
		// Note it's possible an event is still in the cache but no longer in the eventpool
		// (eg. after committing a block, events are removed from eventpool but not cache),
		// so we only record the sender for events still in the eventpool.
		if e, ok := ep.eventsMap.Load(eventKey(event)); ok {
			epE := e.(*clist.CElement).Value.(*eventpoolEvent)
			if _, loaded := epE.senders.LoadOrStore(eventInfo.PeerID, true); loaded {
				// TODO: consider punishing peer for dups,
				// its non-trivial since invalid events can become valid,
				// but they can spam the same events with little cost to them atm.
			}
		}

		return ErrEventInCache
	}
	// END CACHE

	// WAL
	//TODO: Convert event to byte array
	if ep.wal != nil {
		// TODO: Notify administrators when WAL fails
		_, err := ep.wal.Write([]byte(cdc.MustMarshalBinaryBare(event)))
		if err != nil {
			ep.logger.Error("Error writing to WAL", "err", err)
		}
		_, err = ep.wal.Write([]byte("\n"))
		if err != nil {
			ep.logger.Error("Error writing to WAL", "err", err)
		}
	}
	// END WAL

	epE := &eventpoolEvent{
		height: ep.height,
		event:  event,
	}
	epE.senders.Store(eventInfo.PeerID, true)
	ep.addEvent(epE)
	ep.logger.Info("Added good transaction",
		"event", EventID(event),
		"height", epE.height,
		"total", ep.Size(),
	)
	ep.notifyEventsAvailable()
	ep.metrics.Size.Set(float64(ep.Size()))

	return nil
}

// Called from:
//  - resCbFirstTime (lock not held) if event is valid
func (ep *Eventpool) addEvent(epE *eventpoolEvent) {
	e := ep.events.PushBack(epE)
	ep.eventsMap.Store(eventKey(epE.event), e)
	// Compare with previous proposer event to see if new height event
	// We only want to bond to top events in the DAG
	if e, ok := ep.headEventsMap.Load(proposerKey(epE.event)); ok {
		epHe := e.(*clist.CElement).Value.(*eventpoolEvent)
		if epHe.event.Header.Height <= epE.event.Header.Height {
			ep.headEventsMap.Store(proposerKey(epE.event), e)
		}
	} else {
		ep.headEventsMap.Store(proposerKey(epE.event), e)
	}

	atomic.AddInt64(&ep.eventsBytes, int64(epE.event.Size()))
	ep.metrics.EventSizeBytes.Observe(float64(epE.event.Size()))
}

// Called from:
//  - Update (lock held) if event was committed
// 	- resCbRecheck (lock not held) if event was invalidated
func (ep *Eventpool) removeEvent(event *types.EventBlock, elem *clist.CElement, removeFromCache bool) {
	ep.events.Remove(elem)
	elem.DetachPrev()
	ep.eventsMap.Delete(eventKey(event))
	// Remove from proposer map as well
	ep.headEventsMap.Delete(proposerKey(event))
	atomic.AddInt64(&ep.eventsBytes, int64(-event.Size()))

	if removeFromCache {
		ep.cache.Remove(event)
	}
}

// EventsAvailable returns a channel which fires once for every height,
// and only when evenyts are available in the mempool.
// NOTE: the returned channel may be nil if EnableEventsAvailable was not called.
func (ep *Eventpool) EventsAvailable() <-chan struct{} {
	return ep.eventsAvailable
}

func (ep *Eventpool) notifyEventsAvailable() {
	if ep.Size() == 0 {
		panic("notified events available but eventpool is empty!")
	}
	if ep.eventsAvailable != nil && !ep.notifiedEventsAvailable {
		// channel cap is 1, so this will send once
		ep.notifiedEventsAvailable = true
		select {
		case ep.eventsAvailable <- struct{}{}:
		default:
		}
	}
}

// ReapMaxEvents reaps up to max events from the eventpool.
// If max is negative, there is no cap on the size of all returned
// events (~ all available events).
func (ep *Eventpool) ReapMaxEvents(max int) []*types.EventBlock {
	ep.proxyMtx.Lock()
	defer ep.proxyMtx.Unlock()

	if max < 0 {
		max = ep.events.Len()
	}

	events := make([]*types.EventBlock, 0, cmn.MinInt(ep.events.Len(), max))
	for e := ep.events.Front(); e != nil && len(events) <= max; e = e.Next() {
		epE := e.Value.(*eventpoolEvent)
		events = append(events, epE.event)
	}
	return events
}

// HeadEvents returns head events from the eventpool.
func (ep *Eventpool) HeadEvents() []*types.EventBlock {
	ep.proxyMtx.Lock()
	defer ep.proxyMtx.Unlock()

	events := []*types.EventBlock{}
	ep.headEventsMap.Range(func(k, v interface{}) bool {
		epE := v.(*clist.CElement).Value.(*eventpoolEvent)
		events = append(events, epE.event)
		return true
	})
	return events
}

// HeadEventBlockIDs returns head events from the eventpool.
func (ep *Eventpool) HeadEventBlockIDs() []types.EventBlockID {
	ep.proxyMtx.Lock()
	defer ep.proxyMtx.Unlock()

	eventBlockIDs := []types.EventBlockID{}
	ep.headEventsMap.Range(func(k, v interface{}) bool {
		epE := v.(*clist.CElement).Value.(*eventpoolEvent)
		eventBlockIDs = append(eventBlockIDs, types.EventBlockID{Hash: epE.event.Hash()})
		return true
	})
	return eventBlockIDs
}

// Update informs the mempool that the given events were committed and can be discarded.
// NOTE: this should be called *after* block is committed by consensus.
// NOTE: unsafe; Lock/Unlock must be managed by caller
func (ep *Eventpool) Update(
	height int64,
	events []*types.EventBlock,
) error {
	// Set height
	ep.height = height
	ep.notifiedEventsAvailable = false

	// Add committed events to cache (if missing).
	for _, event := range events {
		_ = ep.cache.Push(event)
	}

	// Remove committed events.
	eventsLeft := ep.removeEvents(events)

	// Notify there're some events left.
	if len(eventsLeft) > 0 {
		ep.notifyEventsAvailable()
	}

	// Update metrics
	ep.metrics.Size.Set(float64(ep.Size()))

	return nil
}

func (ep *Eventpool) removeEvents(events []*types.EventBlock) []*types.EventBlock {
	// Build a map for faster lookups.
	eventsMap := make(map[string]struct{}, len(events))
	for _, event := range events {
		eventsMap[EventID(event)] = struct{}{}
	}

	eventsLeft := make([]*types.EventBlock, 0, ep.events.Len())
	for e := ep.events.Front(); e != nil; e = e.Next() {
		epE := e.Value.(*eventpoolEvent)
		// Remove the event if it's already in a block.
		if _, ok := eventsMap[EventID(epE.event)]; ok {
			// NOTE: we don't remove committed events from the cache.
			ep.removeEvent(epE.event, e, false)

			continue
		}
		eventsLeft = append(eventsLeft, epE.event)
	}
	return eventsLeft
}

//--------------------------------------------------------------------------------

// eventpoolEvent is a transaction that successfully ran
type eventpoolEvent struct {
	height    int64             // height that this tx had been validated in
	gasWanted int64             // amount of gas this tx states it will require
	event     *types.EventBlock //

	// ids of peers who've sent us this tx (as a map for quick lookups).
	// senders: PeerID -> bool
	senders sync.Map
}

// Height returns the height for this transaction
func (epE *eventpoolEvent) Height() int64 {
	return atomic.LoadInt64(&epE.height)
}

//--------------------------------------------------------------------------------

type eventsCache interface {
	Reset()
	Push(event *types.EventBlock) bool
	Remove(event *types.EventBlock)
}

// mapEventsCache maintains a LRU cache of events. This only stores the hash
// of the event, due to memory concerns.
type mapEventsCache struct {
	mtx  sync.Mutex
	size int
	map_ map[[sha256.Size]byte]*list.Element
	list *list.List
}

var _ eventsCache = (*mapEventsCache)(nil)

// newMapEventCache returns a new mapEventCache.
func newMapEventsCache(cacheSize int) *mapEventsCache {
	return &mapEventsCache{
		size: cacheSize,
		map_: make(map[[sha256.Size]byte]*list.Element, cacheSize),
		list: list.New(),
	}
}

// Reset resets the cache to an empty state.
func (cache *mapEventsCache) Reset() {
	cache.mtx.Lock()
	cache.map_ = make(map[[sha256.Size]byte]*list.Element, cache.size)
	cache.list.Init()
	cache.mtx.Unlock()
}

// Push adds the given event to the cache and returns true. It returns
// false if event is already in the cache.
func (cache *mapEventsCache) Push(event *types.EventBlock) bool {
	cache.mtx.Lock()
	defer cache.mtx.Unlock()

	// Use the event hash in the cache
	eventHash := eventKey(event)
	if moved, exists := cache.map_[eventHash]; exists {
		cache.list.MoveToBack(moved)
		return false
	}

	if cache.list.Len() >= cache.size {
		popped := cache.list.Front()
		poppedEventHash := popped.Value.([sha256.Size]byte)
		delete(cache.map_, poppedEventHash)
		if popped != nil {
			cache.list.Remove(popped)
		}
	}
	e := cache.list.PushBack(eventHash)
	cache.map_[eventHash] = e
	return true
}

// Remove removes the given event from the cache.
func (cache *mapEventsCache) Remove(event *types.EventBlock) {
	cache.mtx.Lock()
	eventHash := eventKey(event)
	popped := cache.map_[eventHash]
	delete(cache.map_, eventHash)
	if popped != nil {
		cache.list.Remove(popped)
	}

	cache.mtx.Unlock()
}

// mapHeadEventsCache maintains a LRU cache of events. This only stores the hash
// of the event, due to memory concerns.
type mapHeadEventsCache struct {
	mtx  sync.Mutex
	size int
	map_ map[[sha256.Size]byte]*list.Element
	list *list.List
}

var _ eventsCache = (*mapHeadEventsCache)(nil)

// newMapHeadEventsCache returns a new mapEventCache.
func newMapHeadEventsCache(cacheSize int) *mapHeadEventsCache {
	return &mapHeadEventsCache{
		size: cacheSize,
		map_: make(map[[sha256.Size]byte]*list.Element, cacheSize),
		list: list.New(),
	}
}

// Reset resets the cache to an empty state.
func (cache *mapHeadEventsCache) Reset() {
	cache.mtx.Lock()
	cache.map_ = make(map[[sha256.Size]byte]*list.Element, cache.size)
	cache.list.Init()
	cache.mtx.Unlock()
}

// Push adds the given event to the cache and returns true. It returns
// false if event is already in the cache.
func (cache *mapHeadEventsCache) Push(event *types.EventBlock) bool {
	cache.mtx.Lock()
	defer cache.mtx.Unlock()

	// Use the proposer hash in the cache
	proposerHash := proposerKey(event)

	if moved, exists := cache.map_[proposerHash]; exists {
		cache.list.MoveToBack(moved)
		return false
	}

	if cache.list.Len() >= cache.size {
		popped := cache.list.Front()
		poppedProposerHash := popped.Value.([sha256.Size]byte)
		delete(cache.map_, poppedProposerHash)
		if popped != nil {
			cache.list.Remove(popped)
		}
	}
	e := cache.list.PushBack(proposerHash)
	cache.map_[proposerHash] = e
	return true
}

// Remove removes the given event from the cache.
func (cache *mapHeadEventsCache) Remove(event *types.EventBlock) {
	cache.mtx.Lock()
	proposerHash := proposerKey(event)
	popped := cache.map_[proposerHash]
	delete(cache.map_, proposerHash)
	if popped != nil {
		cache.list.Remove(popped)
	}

	cache.mtx.Unlock()
}

type nopEventsCache struct{}

var _ eventsCache = (*nopEventsCache)(nil)

func (nopEventsCache) Reset()                      {}
func (nopEventsCache) Push(*types.EventBlock) bool { return true }
func (nopEventsCache) Remove(*types.EventBlock)    {}
