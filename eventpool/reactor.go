package eventpool

import (
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	amino "github.com/tendermint/go-amino"

	"github.com/andrecronje/babble-abci/types"
	cfg "github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/libs/clist"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/p2p"
	ttypes "github.com/tendermint/tendermint/types"
)

const (
	EventpoolChannel = byte(0x30)

	maxMsgSize   = 1048576        // 1MB TODO make it configurable
	maxEventSize = maxMsgSize - 8 // account for amino overhead of TxMessage

	peerCatchupSleepIntervalMS = 100 // If peer is behind, sleep this amount

	// UnknownPeerID is the peer ID to use when running CheckTx when there is
	// no peer (e.g. RPC)
	UnknownPeerID uint16 = 0

	maxActiveIDs = math.MaxUint16
)

// EventpoolReactor handles eventpool events broadcasting amongst peers.
// It maintains a map from peer ID to counter, to prevent gossiping txs to the
// peers you received it from.
type EventpoolReactor struct {
	p2p.BaseReactor
	config    *cfg.MempoolConfig
	Eventpool *Eventpool
	ids       *eventpoolIDs
}

type eventpoolIDs struct {
	mtx       sync.RWMutex
	peerMap   map[p2p.ID]uint16
	nextID    uint16              // assumes that a node will never have over 65536 active peers
	activeIDs map[uint16]struct{} // used to check if a given peerID key is used, the value doesn't matter
}

// Reserve searches for the next unused ID and assignes it to the
// peer.
func (ids *eventpoolIDs) ReserveForPeer(peer p2p.Peer) {
	ids.mtx.Lock()
	defer ids.mtx.Unlock()

	curID := ids.nextPeerID()
	ids.peerMap[peer.ID()] = curID
	ids.activeIDs[curID] = struct{}{}
}

// nextPeerID returns the next unused peer ID to use.
// This assumes that ids's mutex is already locked.
func (ids *eventpoolIDs) nextPeerID() uint16 {
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
func (ids *eventpoolIDs) Reclaim(peer p2p.Peer) {
	ids.mtx.Lock()
	defer ids.mtx.Unlock()

	removedID, ok := ids.peerMap[peer.ID()]
	if ok {
		delete(ids.activeIDs, removedID)
		delete(ids.peerMap, peer.ID())
	}
}

// GetForPeer returns an ID reserved for the peer.
func (ids *eventpoolIDs) GetForPeer(peer p2p.Peer) uint16 {
	ids.mtx.RLock()
	defer ids.mtx.RUnlock()

	return ids.peerMap[peer.ID()]
}

func newEventpoolIDs() *eventpoolIDs {
	return &eventpoolIDs{
		peerMap:   make(map[p2p.ID]uint16),
		activeIDs: map[uint16]struct{}{0: {}},
		nextID:    1, // reserve unknownPeerID(0) for mempoolReactor.BroadcastTx
	}
}

// NewMEventpoolReactor returns a new MempoolReactor with the given config and mempool.
func NewEventpoolReactor(config *cfg.MempoolConfig, eventpool *Eventpool) *EventpoolReactor {
	epR := &EventpoolReactor{
		config:    config,
		Eventpool: eventpool,
		ids:       newEventpoolIDs(),
	}
	epR.BaseReactor = *p2p.NewBaseReactor("EventpoolReactor", epR)
	return epR
}

// SetLogger sets the Logger on the reactor and the underlying Mempool.
func (epR *EventpoolReactor) SetLogger(l log.Logger) {
	epR.Logger = l
	epR.Eventpool.SetLogger(l)
}

// OnStart implements p2p.BaseReactor.
func (epR *EventpoolReactor) OnStart() error {
	if !epR.config.Broadcast {
		epR.Logger.Info("Event broadcasting is disabled")
	}
	return nil
}

// GetChannels implements Reactor.
// It returns the list of channels for this reactor.
func (epR *EventpoolReactor) GetChannels() []*p2p.ChannelDescriptor {
	return []*p2p.ChannelDescriptor{
		{
			ID:       EventpoolChannel,
			Priority: 5,
		},
	}
}

// AddPeer implements Reactor.
// It starts a broadcast routine ensuring all events are forwarded to the given peer.
func (epR *EventpoolReactor) AddPeer(peer p2p.Peer) {
	epR.ids.ReserveForPeer(peer)
	go epR.broadcastEventsRoutine(peer)
}

// RemovePeer implements Reactor.
func (epR *EventpoolReactor) RemovePeer(peer p2p.Peer, reason interface{}) {
	epR.ids.Reclaim(peer)
	// broadcast routine checks if peer is gone and returns
}

// Receive implements Reactor.
// It adds any received events to the eventpool.
func (epR *EventpoolReactor) Receive(chID byte, src p2p.Peer, msgBytes []byte) {
	msg, err := decodeMsg(msgBytes)
	if err != nil {
		epR.Logger.Error("Error decoding message", "src", src, "chId", chID, "msg", msg, "err", err, "bytes", msgBytes)
		epR.Switch.StopPeerForError(src, err)
		return
	}
	epR.Logger.Debug("Receive", "src", src, "chId", chID, "msg", msg)

	switch msg := msg.(type) {
	case *EventMessage:
		peerID := epR.ids.GetForPeer(src)
		err := epR.Eventpool.CheckEventWithInfo(msg.Event, nil, EventInfo{PeerID: peerID})
		if err != nil {
			epR.Logger.Info("Could not check event", "event", EventID(msg.Event), "err", err)
		}
		// broadcasting happens from go routines per peer
	default:
		epR.Logger.Error(fmt.Sprintf("Unknown message type %v", reflect.TypeOf(msg)))
	}
}

// PeerState describes the state of a peer.
type PeerState interface {
	GetHeight() int64
}

// Send new eventpool events to peer.
func (epR *EventpoolReactor) broadcastEventsRoutine(peer p2p.Peer) {
	if !epR.config.Broadcast {
		return
	}

	peerID := epR.ids.GetForPeer(peer)
	var next *clist.CElement
	for {
		// In case of both next.NextWaitChan() and peer.Quit() are variable at the same time
		if !epR.IsRunning() || !peer.IsRunning() {
			return
		}
		// This happens because the CElement we were looking at got garbage
		// collected (removed). That is, .NextWait() returned nil. Go ahead and
		// start from the beginning.
		if next == nil {
			select {
			case <-epR.Eventpool.EventsWaitChan(): // Wait until a tx is available
				if next = epR.Eventpool.EventsFront(); next == nil {
					continue
				}
			case <-peer.Quit():
				return
			case <-epR.Quit():
				return
			}
		}

		epE := next.Value.(*eventpoolEvent)

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
		if peerState.GetHeight() < epE.Height()-1 { // Allow for a lag of 1 block
			time.Sleep(peerCatchupSleepIntervalMS * time.Millisecond)
			continue
		}

		// ensure peer hasn't already sent us this event
		if _, ok := epE.senders.Load(peerID); !ok {
			// send epE
			msg := &EventMessage{Event: epE.event}
			success := peer.Send(EventpoolChannel, cdc.MustMarshalBinaryBare(msg))
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
		case <-epR.Quit():
			return
		}
	}
}

//-----------------------------------------------------------------------------
// Messages

// EventpoolMessage is a message sent or received by the EventpoolReactor.
type EventpoolMessage interface{}

func RegisterEventpoolMessages(cdc *amino.Codec) {
	cdc.RegisterInterface((*EventpoolMessage)(nil), nil)
	cdc.RegisterConcrete(&EventMessage{}, "tendermint/eventpool/EventMessage", nil)
}

func decodeMsg(bz []byte) (msg EventpoolMessage, err error) {
	if len(bz) > maxMsgSize {
		return msg, fmt.Errorf("Msg exceeds max size (%d > %d)", len(bz), maxMsgSize)
	}
	err = cdc.UnmarshalBinaryBare(bz, &msg)
	return
}

//-------------------------------------

// EventMessage is a EventpoolMessage containing a events.
type EventMessage struct {
	Event types.Event
}

// String returns a string representation of the EventMessage.
func (e *EventMessage) String() string {
	return fmt.Sprintf("[EventMessage %v]", e.Event)
}
