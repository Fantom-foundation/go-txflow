package txvotepool

import (
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	amino "github.com/tendermint/go-amino"

	"github.com/Fantom-foundation/go-txflow/types"
	cfg "github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/libs/clist"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/mempool"
	"github.com/tendermint/tendermint/p2p"
	ttypes "github.com/tendermint/tendermint/types"
)

const (
	TxVotePoolChannel = byte(0x32)

	maxMsgSize = 1048576        // 1MB TODO make it configurable
	maxTxSize  = maxMsgSize - 8 // account for amino overhead of TxMessage

	peerCatchupSleepIntervalMS = 100 // If peer is behind, sleep this amount

	// UnknownPeerID is the peer ID to use when running CheckTx when there is
	// no peer (e.g. RPC)
	UnknownPeerID uint16 = 0

	maxActiveIDs = math.MaxUint16
)

// Reactor handles txpool tx broadcasting amongst peers.
// It maintains a map from peer ID to counter, to prevent gossiping txs to the
// peers you received it from.
type Reactor struct {
	p2p.BaseReactor
	config     *cfg.MempoolConfig
	txVotePool *TxVotePool
	ids        *txVotePoolIDs
}

type txVotePoolIDs struct {
	mtx       sync.RWMutex
	peerMap   map[p2p.ID]uint16
	nextID    uint16              // assumes that a node will never have over 65536 active peers
	activeIDs map[uint16]struct{} // used to check if a given peerID key is used, the value doesn't matter
}

// Reserve searches for the next unused ID and assignes it to the
// peer.
func (ids *txVotePoolIDs) ReserveForPeer(peer p2p.Peer) {
	ids.mtx.Lock()
	defer ids.mtx.Unlock()

	curID := ids.nextPeerID()
	ids.peerMap[peer.ID()] = curID
	ids.activeIDs[curID] = struct{}{}
}

// nextPeerID returns the next unused peer ID to use.
// This assumes that ids's mutex is already locked.
func (ids *txVotePoolIDs) nextPeerID() uint16 {
	if len(ids.activeIDs) == maxActiveIDs {
		panic(fmt.Sprintf("node has maximum %d active IDs and wanted to get one more", maxActiveIDs))
	}

	_, idExists := ids.activeIDs[ids.nextID]
	for idExists {
		ids.nextID++
		_, idExists = ids.activeIDs[ids.nextID]
	}
	curID := ids.nextID
	ids.nextID++
	return curID
}

// Reclaim returns the ID reserved for the peer back to unused pool.
func (ids *txVotePoolIDs) Reclaim(peer p2p.Peer) {
	ids.mtx.Lock()
	defer ids.mtx.Unlock()

	removedID, ok := ids.peerMap[peer.ID()]
	if ok {
		delete(ids.activeIDs, removedID)
		delete(ids.peerMap, peer.ID())
	}
}

// GetForPeer returns an ID reserved for the peer.
func (ids *txVotePoolIDs) GetForPeer(peer p2p.Peer) uint16 {
	ids.mtx.RLock()
	defer ids.mtx.RUnlock()

	return ids.peerMap[peer.ID()]
}

func newTxVotePoolIDs() *txVotePoolIDs {
	return &txVotePoolIDs{
		peerMap:   make(map[p2p.ID]uint16),
		activeIDs: map[uint16]struct{}{0: {}},
		nextID:    1, // reserve unknownPeerID(0) for mempoolReactor.BroadcastTx
	}
}

// NewReactor returns a new TxpoolReactor with the given config and txpool.
func NewReactor(config *cfg.MempoolConfig, txpool *TxVotePool) *Reactor {
	txR := &Reactor{
		config:     config,
		txVotePool: txpool,
		ids:        newTxVotePoolIDs(),
	}
	txR.BaseReactor = *p2p.NewBaseReactor("TxVotePoolReactor", txR)
	return txR
}

// SetLogger sets the Logger on the reactor and the underlying Mempool.
func (txR *Reactor) SetLogger(l log.Logger) {
	txR.Logger = l
	txR.txVotePool.SetLogger(l)
}

// OnStart implements p2p.BaseReactor.
func (txR *Reactor) OnStart() error {
	if !txR.config.Broadcast {
		txR.Logger.Info("Tx broadcasting is disabled")
	}
	return nil
}

// GetChannels implements Reactor.
// It returns the list of channels for this reactor.
func (txR *Reactor) GetChannels() []*p2p.ChannelDescriptor {
	return []*p2p.ChannelDescriptor{
		{
			ID:       TxVotePoolChannel,
			Priority: 5,
		},
	}
}

// AddPeer implements Reactor.
// It starts a broadcast routine ensuring all txs are forwarded to the given peer.
func (txR *Reactor) AddPeer(peer p2p.Peer) {
	txR.ids.ReserveForPeer(peer)
	go txR.broadcastTxRoutine(peer)
}

// RemovePeer implements Reactor.
func (txR *Reactor) RemovePeer(peer p2p.Peer, reason interface{}) {
	txR.ids.Reclaim(peer)
	// broadcast routine checks if peer is gone and returns
}

// Receive implements Reactor.
// It adds any received transactions to the txpool.
func (txR *Reactor) Receive(chID byte, src p2p.Peer, msgBytes []byte) {
	msg, err := decodeMsg(msgBytes)
	if err != nil {
		txR.Logger.Error("Error decoding message", "src", src, "chId", chID, "msg", msg, "err", err, "bytes", msgBytes)
		txR.Switch.StopPeerForError(src, err)
		return
	}
	txR.Logger.Debug("Receive", "src", src, "chId", chID, "msg", msg)

	switch msg := msg.(type) {
	case *TxVoteMessage:
		peerID := txR.ids.GetForPeer(src)
		err := txR.txVotePool.CheckTxWithInfo(msg.Tx, mempool.TxInfo{SenderID: peerID})
		if err != nil {
			txR.Logger.Info("Could not check tx", "tx", TxVoteID(msg.Tx), "err", err)
		}
		// broadcasting happens from go routines per peer
	default:
		txR.Logger.Error(fmt.Sprintf("Unknown message type %v", reflect.TypeOf(msg)))
	}
}

// PeerState describes the state of a peer.
type PeerState interface {
	GetHeight() int64
}

// Send new txpool txs to peer.
func (txR *Reactor) broadcastTxRoutine(peer p2p.Peer) {
	if !txR.config.Broadcast {
		return
	}

	peerID := txR.ids.GetForPeer(peer)
	var next *clist.CElement
	for {
		// In case of both next.NextWaitChan() and peer.Quit() are variable at the same time
		if !txR.IsRunning() || !peer.IsRunning() {
			return
		}
		// This happens because the CElement we were looking at got garbage
		// collected (removed). That is, .NextWait() returned nil. Go ahead and
		// start from the beginning.
		if next == nil {
			select {
			case <-txR.txVotePool.TxsWaitChan(): // Wait until a tx is available
				if next = txR.txVotePool.TxsFront(); next == nil {
					continue
				}
			case <-peer.Quit():
				return
			case <-txR.Quit():
				return
			}
		}

		txTx := next.Value.(*mempoolTxVote)

		// make sure the peer is up to date
		peerState, ok := peer.Get(ttypes.PeerStateKey).(PeerState)
		if !ok {
			// Peer does not have a state yet. We set it in the consensus reactor, but
			// when we add peer in Switch, the order we call reactors#AddPeer is
			// different every time due to us using a map. Sometimes other reactors
			// will be initialized before the consensus reactor. We should wait a few
			// milliseconds and retry.
			time.Sleep(peerCatchupSleepIntervalMS * time.Millisecond)
			continue
		}
		if peerState.GetHeight() < txTx.Height()-1 { // Allow for a lag of 1 block
			time.Sleep(peerCatchupSleepIntervalMS * time.Millisecond)
			continue
		}

		// ensure peer hasn't already sent us this tx
		if _, ok := txTx.senders.Load(peerID); !ok {
			// send txTx
			msg := &TxVoteMessage{Tx: txTx.tx}
			success := peer.Send(TxVotePoolChannel, cdc.MustMarshalBinaryBare(msg))
			if !success {
				time.Sleep(peerCatchupSleepIntervalMS * time.Millisecond)
				continue
			}
		}

		select {
		case <-next.NextWaitChan():
			// see the start of the for loop for nil check
			next = next.Next()
		case <-peer.Quit():
			return
		case <-txR.Quit():
			return
		}
	}
}

//-----------------------------------------------------------------------------
// Messages

// TxpoolMessage is a message sent or received by the TxpoolReactor.
type TxpoolMessage interface{}

func RegisterTxVotePoolMessages(cdc *amino.Codec) {
	cdc.RegisterInterface((*TxpoolMessage)(nil), nil)
	cdc.RegisterConcrete(&TxVoteMessage{}, "tendermint/txvotepool/TxVoteMessage", nil)
}

func decodeMsg(bz []byte) (msg TxpoolMessage, err error) {
	if len(bz) > maxMsgSize {
		return msg, fmt.Errorf("Msg exceeds max size (%d > %d)", len(bz), maxMsgSize)
	}
	err = cdc.UnmarshalBinaryBare(bz, &msg)
	return
}

//-------------------------------------

// TxMessage is a TxpoolMessage containing a transaction.
type TxVoteMessage struct {
	Tx types.TxVote
}

// String returns a string representation of the TxMessage.
func (m *TxVoteMessage) String() string {
	return fmt.Sprintf("[TxVoteMessage %v]", m.Tx)
}
