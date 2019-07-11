package txflow

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/Fantom-foundation/go-txflow/types"
	amino "github.com/tendermint/go-amino"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/p2p"
	sm "github.com/tendermint/tendermint/state"
	ttypes "github.com/tendermint/tendermint/types"
)

const (
	StateChannel       = byte(0x20)
	DataChannel        = byte(0x21)
	VoteChannel        = byte(0x22)
	VoteSetBitsChannel = byte(0x23)

	maxMsgSize = 1048576 // 1MB; NOTE/TODO: keep in sync with types.PartSet sizes.

	votesToContributeToBecomeGoodPeer = 10000
)

//-----------------------------------------------------------------------------

// TxFlowReactor defines a reactor for the consensus service.
type TxFlowReactor struct {
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
func NewTxFlowReactor(txflowState *TxFlowState, fastSync bool, options ...ReactorOption) *TxFlowReactor {
	txR := &TxFlowReactor{
		txS:      txflowState,
		fastSync: fastSync,
		metrics:  NopMetrics(),
	}
	txR.updateFastSyncingMetric()
	txR.BaseReactor = *p2p.NewBaseReactor("TxFlowReactor", txR)

	for _, option := range options {
		option(txR)
	}

	return txR
}

// OnStart implements BaseService by subscribing to events, which later will be
// broadcasted to other peers and starting state if we're not in fast sync.
func (txR *TxFlowReactor) OnStart() error {
	txR.Logger.Info("TxFlowReactor ", "fastSync", txR.FastSync())

	// start routine that computes peer statistics for evaluating peer quality
	go txR.peerStatsRoutine()

	txR.subscribeToBroadcastEvents()

	if !txR.FastSync() {
		err := txR.conS.Start()
		if err != nil {
			return err
		}
	}

	return nil
}

// OnStop implements BaseService by unsubscribing from events and stopping
// state.
func (txR *TxFlowReactor) OnStop() {
	txR.unsubscribeFromBroadcastEvents()
	txR.txS.Stop()
	if !txR.FastSync() {
		txR.txS.Wait()
	}
}

// SwitchToConsensus switches from fast_sync mode to consensus mode.
// It resets the state, turns off fast_sync, and starts the consensus state-machine
func (txR *TxFlowReactor) SwitchToConsensus(state sm.State, txsSynced int) {
	txR.Logger.Info("SwitchToConsensus")
	txR.txS.reconstructLastCommit(state)
	txR.txS.updateToState(state)

	txR.mtx.Lock()
	txR.fastSync = false
	txR.mtx.Unlock()
	txR.metrics.FastSyncing.Set(0)

	if txsSynced > 0 {
		// dont bother with the WAL if we fast synced
		txR.txS.doWALCatchup = false
	}
	err := txR.conS.Start()
	if err != nil {
		txR.Logger.Error("Error starting conS", "err", err)
		return
	}
}

// GetChannels implements Reactor
func (txR *TxFlowReactor) GetChannels() []*p2p.ChannelDescriptor {
	// TODO optimize
	return []*p2p.ChannelDescriptor{
		{
			ID:                  StateChannel,
			Priority:            5,
			SendQueueCapacity:   100,
			RecvMessageCapacity: maxMsgSize,
		},
		{
			ID:                  DataChannel, // maybe split between gossiping current txs and catchup stuff
			Priority:            10,          // once we gossip the whole tx there's nothing left to send until next height or round
			SendQueueCapacity:   100,
			RecvBufferCapacity:  50 * 4096,
			RecvMessageCapacity: maxMsgSize,
		},
		{
			ID:                  VoteChannel,
			Priority:            5,
			SendQueueCapacity:   100,
			RecvBufferCapacity:  100 * 100,
			RecvMessageCapacity: maxMsgSize,
		},
		{
			ID:                  VoteSetBitsChannel,
			Priority:            1,
			SendQueueCapacity:   2,
			RecvBufferCapacity:  1024,
			RecvMessageCapacity: maxMsgSize,
		},
	}
}

// AddPeer implements Reactor
func (txR *TxFlowReactor) AddPeer(peer p2p.Peer) {
	if !txR.IsRunning() {
		return
	}

	// Create peerState for peer
	peerState := NewPeerState(peer).SetLogger(txR.Logger)
	peer.Set(types.PeerStateKey, peerState)

	// Begin routines for this peer.
	go txR.gossipVotesRoutine(peer, peerState)
	go txR.queryMaj23Routine(peer, peerState)

	// Send our state to peer.
	// If we're fast_syncing, broadcast a RoundStepMessage later upon SwitchToConsensus().
	if !txR.FastSync() {
		txR.sendNewRoundStepMessage(peer)
	}
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
		case *NewValidTxMessage:
			ps.ApplyNewValidTxMessage(msg)
		case *HasTxVoteMessage:
			ps.ApplyHasTxVoteMessage(msg)
		case *TxVoteSetMaj23Message:
			cs := txR.conS
			cs.mtx.Lock()
			height, votes := cs.Height, cs.Votes
			cs.mtx.Unlock()
			// Peer claims to have a maj23 for some Vote at H,
			err := votes.SetPeerMaj23(msg.Type, ps.peer.ID(), msg.TxHash)
			if err != nil {
				conR.Switch.StopPeerForError(src, err)
				return
			}
			// Respond with a VoteSetBitsMessage showing which votes we have.
			// (and consequently shows which we don't have)
			var ourVotes *cmn.BitArray
			switch msg.Type {
			case types.PrevoteType:
				ourVotes = votes.Prevotes(msg.Round).BitArrayByBlockID(msg.BlockID)
			case types.PrecommitType:
				ourVotes = votes.Precommits(msg.Round).BitArrayByBlockID(msg.BlockID)
			default:
				panic("Bad VoteSetBitsMessage field Type. Forgot to add a check in ValidateBasic?")
			}
			src.TrySend(VoteSetBitsChannel, cdc.MustMarshalBinaryBare(&VoteSetBitsMessage{
				Height:  msg.Height,
				Round:   msg.Round,
				Type:    msg.Type,
				BlockID: msg.BlockID,
				Votes:   ourVotes,
			}))
		default:
			conR.Logger.Error(fmt.Sprintf("Unknown message type %v", reflect.TypeOf(msg)))
		}

	case DataChannel:
		if conR.FastSync() {
			conR.Logger.Info("Ignoring message received during fastSync", "msg", msg)
			return
		}
		switch msg := msg.(type) {
		case *ProposalMessage:
			ps.SetHasProposal(msg.Proposal)
			conR.conS.peerMsgQueue <- msgInfo{msg, src.ID()}
		case *ProposalPOLMessage:
			ps.ApplyProposalPOLMessage(msg)
		case *BlockPartMessage:
			ps.SetHasProposalBlockPart(msg.Height, msg.Round, msg.Part.Index)
			conR.metrics.BlockParts.With("peer_id", string(src.ID())).Add(1)
			conR.conS.peerMsgQueue <- msgInfo{msg, src.ID()}
		default:
			conR.Logger.Error(fmt.Sprintf("Unknown message type %v", reflect.TypeOf(msg)))
		}

	case VoteChannel:
		if conR.FastSync() {
			conR.Logger.Info("Ignoring message received during fastSync", "msg", msg)
			return
		}
		switch msg := msg.(type) {
		case *VoteMessage:
			cs := conR.conS
			cs.mtx.RLock()
			height, valSize, lastCommitSize := cs.Height, cs.Validators.Size(), cs.LastCommit.Size()
			cs.mtx.RUnlock()
			ps.EnsureVoteBitArrays(height, valSize)
			ps.EnsureVoteBitArrays(height-1, lastCommitSize)
			ps.SetHasVote(msg.Vote)

			cs.peerMsgQueue <- msgInfo{msg, src.ID()}

		default:
			// don't punish (leave room for soft upgrades)
			conR.Logger.Error(fmt.Sprintf("Unknown message type %v", reflect.TypeOf(msg)))
		}

	case VoteSetBitsChannel:
		if conR.FastSync() {
			conR.Logger.Info("Ignoring message received during fastSync", "msg", msg)
			return
		}
		switch msg := msg.(type) {
		case *VoteSetBitsMessage:
			cs := conR.conS
			cs.mtx.Lock()
			height, votes := cs.Height, cs.Votes
			cs.mtx.Unlock()

			if height == msg.Height {
				var ourVotes *cmn.BitArray
				switch msg.Type {
				case types.PrevoteType:
					ourVotes = votes.Prevotes(msg.Round).BitArrayByBlockID(msg.BlockID)
				case types.PrecommitType:
					ourVotes = votes.Precommits(msg.Round).BitArrayByBlockID(msg.BlockID)
				default:
					panic("Bad VoteSetBitsMessage field Type. Forgot to add a check in ValidateBasic?")
				}
				ps.ApplyVoteSetBitsMessage(msg, ourVotes)
			} else {
				ps.ApplyVoteSetBitsMessage(msg, nil)
			}
		default:
			// don't punish (leave room for soft upgrades)
			conR.Logger.Error(fmt.Sprintf("Unknown message type %v", reflect.TypeOf(msg)))
		}

	default:
		conR.Logger.Error(fmt.Sprintf("Unknown chId %X", chID))
	}

	if err != nil {
		conR.Logger.Error("Error in Receive()", "err", err)
	}
}

// SetEventBus sets event bus.
func (txR *TxFlowReactor) SetEventBus(b *ttypes.EventBus) {
	txR.eventBus = b
	txR.conS.SetEventBus(b)
}

// FastSync returns whether the consensus reactor is in fast-sync mode.
func (txR *TxFlowReactor) FastSync() bool {
	txR.mtx.RLock()
	defer txR.mtx.RUnlock()
	return txR.fastSync
}

// NOTE: `queryMaj23Routine` has a simple crude design since it only comes
// into play for liveness when there's a signature DDoS attack happening.
func (txR *TxFlowReactor) queryMaj23Routine(peer p2p.Peer, ps *PeerState) {
	logger := txR.Logger.With("peer", peer)

OUTER_LOOP:
	for {
		// Manage disconnects from self or peer.
		if !peer.IsRunning() || !txR.IsRunning() {
			logger.Info("Stopping queryMaj23Routine for peer")
			return
		}

		// Maybe send Height/Round/Prevotes
		{
			rs := conR.conS.GetRoundState()
			prs := ps.GetRoundState()
			if rs.Height == prs.Height {
				if maj23, ok := rs.Votes.Prevotes(prs.Round).TwoThirdsMajority(); ok {
					peer.TrySend(StateChannel, cdc.MustMarshalBinaryBare(&VoteSetMaj23Message{
						Height:  prs.Height,
						Round:   prs.Round,
						Type:    types.PrevoteType,
						BlockID: maj23,
					}))
					time.Sleep(conR.conS.config.PeerQueryMaj23SleepDuration)
				}
			}
		}

		// Maybe send Height/Round/Precommits
		{
			rs := conR.conS.GetRoundState()
			prs := ps.GetRoundState()
			if rs.Height == prs.Height {
				if maj23, ok := rs.Votes.Precommits(prs.Round).TwoThirdsMajority(); ok {
					peer.TrySend(StateChannel, cdc.MustMarshalBinaryBare(&VoteSetMaj23Message{
						Height:  prs.Height,
						Round:   prs.Round,
						Type:    types.PrecommitType,
						BlockID: maj23,
					}))
					time.Sleep(conR.conS.config.PeerQueryMaj23SleepDuration)
				}
			}
		}

		// Maybe send Height/Round/ProposalPOL
		{
			rs := conR.conS.GetRoundState()
			prs := ps.GetRoundState()
			if rs.Height == prs.Height && prs.ProposalPOLRound >= 0 {
				if maj23, ok := rs.Votes.Prevotes(prs.ProposalPOLRound).TwoThirdsMajority(); ok {
					peer.TrySend(StateChannel, cdc.MustMarshalBinaryBare(&VoteSetMaj23Message{
						Height:  prs.Height,
						Round:   prs.ProposalPOLRound,
						Type:    types.PrevoteType,
						BlockID: maj23,
					}))
					time.Sleep(conR.conS.config.PeerQueryMaj23SleepDuration)
				}
			}
		}

		// Little point sending LastCommitRound/LastCommit,
		// These are fleeting and non-blocking.

		// Maybe send Height/CatchupCommitRound/CatchupCommit.
		{
			prs := ps.GetRoundState()
			if prs.CatchupCommitRound != -1 && 0 < prs.Height && prs.Height <= conR.conS.blockStore.Height() {
				commit := conR.conS.LoadCommit(prs.Height)
				peer.TrySend(StateChannel, cdc.MustMarshalBinaryBare(&VoteSetMaj23Message{
					Height:  prs.Height,
					Round:   commit.Round(),
					Type:    types.PrecommitType,
					BlockID: commit.BlockID,
				}))
				time.Sleep(conR.conS.config.PeerQueryMaj23SleepDuration)
			}
		}

		time.Sleep(conR.conS.config.PeerQueryMaj23SleepDuration)

		continue OUTER_LOOP
	}
}

func (txR *TxFlowReactor) peerStatsRoutine() {
	for {
		if !txR.IsRunning() {
			txR.Logger.Info("Stopping peerStatsRoutine")
			return
		}

		select {
		case msg := <-txR.txS.statsMsgQueue:
			// Get peer
			peer := txR.Switch.Peers().Get(msg.PeerID)
			if peer == nil {
				conR.Logger.Debug("Attempt to update stats for non-existent peer",
					"peer", msg.PeerID)
				continue
			}
			// Get peer state
			ps, ok := peer.Get(types.PeerStateKey).(*PeerState)
			if !ok {
				panic(fmt.Sprintf("Peer %v has no state", peer))
			}
			switch msg.Msg.(type) {
			case *VoteMessage:
				if numVotes := ps.RecordVote(); numVotes%votesToContributeToBecomeGoodPeer == 0 {
					conR.Switch.MarkPeerAsGood(peer)
				}
			case *BlockPartMessage:
				if numParts := ps.RecordBlockPart(); numParts%blocksToContributeToBecomeGoodPeer == 0 {
					conR.Switch.MarkPeerAsGood(peer)
				}
			}
		case <-conR.conS.Quit():
			return

		case <-conR.Quit():
			return
		}
	}
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

func (txR *TxFlowReactor) updateFastSyncingMetric() {
	var fastSyncing float64
	if txR.fastSync {
		fastSyncing = 1
	} else {
		fastSyncing = 0
	}
	txR.metrics.FastSyncing.Set(fastSyncing)
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
	cdc.RegisterConcrete(&TxVoteMessage{}, "tendermint/TxVote", nil)
	cdc.RegisterConcrete(&HasTxVoteMessage{}, "tendermint/HasTxVote", nil)
	cdc.RegisterConcrete(&TxVoteSetMaj23Message{}, "tendermint/TxVoteSetMaj23", nil)
	cdc.RegisterConcrete(&TxVoteSetBitsMessage{}, "tendermint/TxVoteSetBits", nil)
}

func decodeMsg(bz []byte) (msg TxFlowMessage, err error) {
	if len(bz) > maxMsgSize {
		return msg, fmt.Errorf("Msg exceeds max size (%d > %d)", len(bz), maxMsgSize)
	}
	err = cdc.UnmarshalBinaryBare(bz, &msg)
	return
}

// TxVoteMessage is sent when voting for a tx (or lack thereof).
type TxVoteMessage struct {
	TxVote *types.TxVote
}

// ValidateBasic performs basic validation.
func (m *TxVoteMessage) ValidateBasic() error {
	return m.TxVote.ValidateBasic()
}

// String returns a string representation.
func (m *TxVoteMessage) String() string {
	return fmt.Sprintf("[TxVote %v]", m.TxVote)
}

//-------------------------------------

// TxVoteSetMaj23Message is sent to indicate that a given TxHash has seen +2/3 votes.
type TxVoteSetMaj23Message struct {
	Height int64
	TxHash cmn.HexBytes
}

// ValidateBasic performs basic validation.
func (m *TxVoteSetMaj23Message) ValidateBasic() error {
	if m.Height < 0 {
		return errors.New("Negative Height")
	}
	if err := types.ValidateHash(m.TxHash); err != nil {
		return fmt.Errorf("Wrong TxHash: %v", err)
	}
	return nil
}

// String returns a string representation.
func (m *TxVoteSetMaj23Message) String() string {
	return fmt.Sprintf("[TVSM23 %v %X]", m.Height, m.TxHash)
}

//-------------------------------------

// TxVoteSetBitsMessage is sent to communicate the bit-array of votes seen for the TxHash.
type TxVoteSetBitsMessage struct {
	Height int64
	TxHash cmn.HexBytes
	Votes  *cmn.BitArray
}

// ValidateBasic performs basic validation.
func (m *TxVoteSetBitsMessage) ValidateBasic() error {
	if m.Height < 0 {
		return errors.New("Negative Height")
	}
	if err := types.ValidateHash(m.TxHash); err != nil {
		return fmt.Errorf("Wrong TxHash: %v", err)
	}
	// NOTE: Votes.Size() can be zero if the node does not have any
	return nil
}

// String returns a string representation.
func (m *TxVoteSetBitsMessage) String() string {
	return fmt.Sprintf("[TVSB %v %X %v]", m.Height, m.TxHash, m.Votes)
}

//-------------------------------------
