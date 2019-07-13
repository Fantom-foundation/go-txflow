package txflow

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/pkg/errors"

	"github.com/Fantom-foundation/go-txflow/types"
	amino "github.com/tendermint/go-amino"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/p2p"
	ttypes "github.com/tendermint/tendermint/types"
)

const (
	StateChannel = byte(0x20)

	maxMsgSize = 1048576 // 1MB; NOTE/TODO: keep in sync with types.PartSet sizes.

	votesToContributeToBecomeGoodPeer = 10000
)

//-----------------------------------------------------------------------------

// TxFlowReactor defines a reactor for the consensus service.
type TxFlowReactor struct {
	// Do we need to be a p2p reactor? I think we can juse use state flow
	p2p.BaseReactor // BaseService + p2p.Switch

	// State keeps track of the current pending transactions and their vote state
	txS *TxFlowState

	mtx sync.RWMutex

	// Broadcast new committed tx events to the application layer
	eventBus *ttypes.EventBus
	chainID  string

	metrics *Metrics
}

type ReactorOption func(*TxFlowReactor)

// NewTxFlowReactor returns a new TxFlowReactor with the given
// txflowState.
func NewTxFlowReactor(txflowState *TxFlowState, options ...ReactorOption) *TxFlowReactor {
	txR := &TxFlowReactor{
		txS:     txflowState,
		metrics: NopMetrics(),
	}
	txR.BaseReactor = *p2p.NewBaseReactor("TxFlowReactor", txR)

	for _, option := range options {
		option(txR)
	}

	return txR
}

// OnStart implements BaseService by subscribing to events, which later will be
// broadcasted to other peers and starting state if we're not in fast sync.
func (txR *TxFlowReactor) OnStart() error {
	txR.Logger.Info("TxFlowReactor OnStart()")

	//State is simply required for WAL catchup, so we don't have to replay the tx's
	err := txR.txS.Start()
	if err != nil {
		return err
	}

	return nil
}

// OnStop implements BaseService by unsubscribing from events and stopping
// state.
func (txR *TxFlowReactor) OnStop() {
	txR.txS.Stop()
	txR.txS.Wait()
}

// GetChannels implements Reactor
func (txR *TxFlowReactor) GetChannels() []*p2p.ChannelDescriptor {
	// TODO optimize
	return []*p2p.ChannelDescriptor{
		{
			ID:                  StateChannel,
			Priority:            5,
			SendQueueCapacity:   100,
			RecvBufferCapacity:  100 * 100,
			RecvMessageCapacity: maxMsgSize,
		},
	}
}

// AddPeer implements Reactor
// Peers we can use to expedite maj23 by sending maj23 events
func (txR *TxFlowReactor) AddPeer(peer p2p.Peer) {
	if !txR.IsRunning() {
		return
	}

	// Create peerState for peer
	peerState := NewPeerState(peer).SetLogger(txR.Logger)
	peer.Set(types.PeerStateKey, peerState)
}

// RemovePeer implements Reactor
func (txR *TxFlowReactor) RemovePeer(peer p2p.Peer, reason interface{}) {
	if !txR.IsRunning() {
		return
	}
	// TODO
	// ps, ok := peer.Get(PeerStateKey).(*PeerState)
	// if !ok {
	// 	panic(fmt.Sprintf("Peer %v has no state", peer))
	// }
	// ps.Disconnect()
}

// Receive implements Reactor
// NOTE: We process these messages even when we're fast_syncing.
// Messages affect either a peer state or the consensus state.
// Peer state updates can happen in parallel, but processing of
// proposals, block parts, and votes are ordered by the receiveRoutine
// NOTE: blocks on consensus state for proposals, block parts, and votes
func (txR *TxFlowReactor) Receive(chID byte, src p2p.Peer, msgBytes []byte) {
	if !txR.IsRunning() {
		txR.Logger.Debug("Receive", "src", src, "chId", chID, "bytes", msgBytes)
		return
	}

	msg, err := decodeMsg(msgBytes)
	if err != nil {
		txR.Logger.Error("Error decoding message", "src", src, "chId", chID, "msg", msg, "err", err, "bytes", msgBytes)
		txR.Switch.StopPeerForError(src, err)
		return
	}

	if err = msg.ValidateBasic(); err != nil {
		txR.Logger.Error("Peer sent us invalid msg", "peer", src, "msg", msg, "err", err)
		txR.Switch.StopPeerForError(src, err)
		return
	}

	txR.Logger.Debug("Receive", "src", src, "chId", chID, "msg", msg)

	// Get peer states
	ps, ok := src.Get(types.PeerStateKey).(*PeerState)
	if !ok {
		panic(fmt.Sprintf("Peer %v has no state", src))
	}

	switch chID {
	case StateChannel:
		switch msg := msg.(type) {
		//A peer claims to have received Maj23 on a tx, we need to collect all missing votes
		case *TxVoteSetMaj23Message:
			ts := txR.txS
			ts.mtx.Lock()
			votes := ts.TxVoteSets
			ts.mtx.Unlock()
			// Peer claims to have a maj23 for some Vote at H,
			err := votes.SetPeerMaj23(msg.TxHash, msg.TxVoteSet)
			if err != nil {
				txR.Switch.StopPeerForError(src, err)
				return
			}
			// Should register peer here so we don't send to him again
			// We don't want to spam the network with Maj23 events
			// TxVotePool keeps track of this ID, we should use it here
			// TODO
		default:
			txR.Logger.Error(fmt.Sprintf("Unknown message type %v", reflect.TypeOf(msg)))
		}
	}

	if err != nil {
		txR.Logger.Error("Error in Receive()", "err", err)
	}
}

// SetEventBus sets event bus.
func (txR *TxFlowReactor) SetEventBus(b *ttypes.EventBus) {
	txR.eventBus = b
	txR.txS.SetEventBus(b)
}

// String returns a string representation of the ConsensusReactor.
// NOTE: For now, it is just a hard-coded string to avoid accessing unprotected shared variables.
// TODO: improve!
func (txR *TxFlowReactor) String() string {
	// better not to access shared variables
	return "TxFlowReactor" // conR.StringIndented("")
}

// StringIndented returns an indented string representation of the ConsensusReactor
func (txR *TxFlowReactor) StringIndented(indent string) string {
	s := "TxFlowReactor{\n"
	s += indent + "  " + txR.txS.StringIndented(indent+"  ") + "\n"
	for _, peer := range txR.Switch.Peers().List() {
		ps, ok := peer.Get(types.PeerStateKey).(*PeerState)
		if !ok {
			panic(fmt.Sprintf("Peer %v has no state", peer))
		}
		s += indent + "  " + ps.StringIndented(indent+"  ") + "\n"
	}
	s += indent + "}"
	return s
}

// ReactorMetrics sets the metrics
func ReactorMetrics(metrics *Metrics) ReactorOption {
	return func(txR *TxFlowReactor) { txR.metrics = metrics }
}

//-----------------------------------------------------------------------------
// Messages

// TxFlowMessage is a message that can be sent and received on the TxFlowReactor
type TxFlowMessage interface {
	ValidateBasic() error
}

func RegisterTxFlowMessages(cdc *amino.Codec) {
	cdc.RegisterInterface((*TxFlowMessage)(nil), nil)
	cdc.RegisterConcrete(&TxVoteSetMaj23Message{}, "tendermint/TxVoteSetMaj23", nil)
}

func decodeMsg(bz []byte) (msg TxFlowMessage, err error) {
	if len(bz) > maxMsgSize {
		return msg, fmt.Errorf("Msg exceeds max size (%d > %d)", len(bz), maxMsgSize)
	}
	err = cdc.UnmarshalBinaryBare(bz, &msg)
	return
}

//-------------------------------------

// TxVoteSetMaj23Message is sent to indicate that a given TxHash has seen +2/3 votes.
type TxVoteSetMaj23Message struct {
	Height    int64
	TxHash    cmn.HexBytes
	TxVoteSet types.TxVoteSet
}

// ValidateBasic performs basic validation.
func (m *TxVoteSetMaj23Message) ValidateBasic() error {
	if m.Height < 0 {
		return errors.New("Negative Height")
	}
	if err := ttypes.ValidateHash(m.TxHash); err != nil {
		return fmt.Errorf("Wrong TxHash: %v", err)
	}
	return nil
}

// String returns a string representation.
func (m *TxVoteSetMaj23Message) String() string {
	return fmt.Sprintf("[TVSM23 %v %X]", m.Height, m.TxHash)
}
