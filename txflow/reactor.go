package txflow

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/Fantom-foundation/go-txflow/types"
	amino "github.com/tendermint/go-amino"
	cstypes "github.com/tendermint/tendermint/consensus/types"
	cmn "github.com/tendermint/tendermint/libs/common"
	tmevents "github.com/tendermint/tendermint/libs/events"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/p2p"
	sm "github.com/tendermint/tendermint/state"
	ttypes "github.com/tendermint/tendermint/types"
	tmtime "github.com/tendermint/tendermint/types/time"
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
	txR.conS.Stop()
	if !txR.FastSync() {
		txR.conS.Wait()
	}
}

// SwitchToConsensus switches from fast_sync mode to consensus mode.
// It resets the state, turns off fast_sync, and starts the consensus state-machine
func (txR *TxFlowReactor) SwitchToConsensus(state sm.State, txsSynced int) {
	txR.Logger.Info("SwitchToConsensus")
	txR.conS.reconstructLastCommit(state)
	txR.conS.updateToState(state)

	txR.mtx.Lock()
	txR.fastSync = false
	txR.mtx.Unlock()
	txR.metrics.FastSyncing.Set(0)

	if txsSynced > 0 {
		// dont bother with the WAL if we fast synced
		txR.conS.doWALCatchup = false
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

//--------------------------------------

// subscribeToBroadcastEvents subscribes for new round steps and votes
// using internal pubsub defined on state to broadcast
// them to peers upon receiving.
func (txR *TxFlowReactor) subscribeToBroadcastEvents() {
	const subscriber = "txflow-reactor"

	txR.txS.evsw.AddListenerForEvent(subscriber, types.TxVote,
		func(data tmevents.EventData) {
			txR.broadcastHasTxVoteMessage(data.(*types.TxVote))
		})

}

func (txR *TxFlowReactor) unsubscribeFromBroadcastEvents() {
	const subscriber = "txflow-reactor"
	txR.txS.evsw.RemoveListener(subscriber)
}

// Broadcasts HasTxVoteMessage to peers that care.
func (txR *TxFlowReactor) broadcastHasTxVoteMessage(vote *types.TxVote) {
	msg := &HasTxVoteMessage{
		TxHash:           vote.TxHash,
		ValidatorAddress: vote.ValidatorAddress,
	}
	conR.Switch.Broadcast(StateChannel, cdc.MustMarshalBinaryBare(msg))
	/*
		// TODO: Make this broadcast more selective.
		for _, peer := range conR.Switch.Peers().List() {
			ps, ok := peer.Get(PeerStateKey).(*PeerState)
			if !ok {
				panic(fmt.Sprintf("Peer %v has no state", peer))
			}
			prs := ps.GetRoundState()
			if prs.Height == vote.Height {
				// TODO: Also filter on round?
				peer.TrySend(StateChannel, struct{ ConsensusMessage }{msg})
			} else {
				// Height doesn't match
				// TODO: check a field, maybe CatchupCommitRound?
				// TODO: But that requires changing the struct field comment.
			}
		}
	*/
}

func (txR *TxFlowReactor) gossipVotesRoutine(peer p2p.Peer, ps *PeerState) {
	logger := conR.Logger.With("peer", peer)

	// Simple hack to throttle logs upon sleep.
	var sleeping = 0

OUTER_LOOP:
	for {
		// Manage disconnects from self or peer.
		if !peer.IsRunning() || !txR.IsRunning() {
			logger.Info("Stopping gossipVotesRoutine for peer")
			return
		}
		rs := txR.txS.GetRoundState()
		prs := ps.GetRoundState()

		switch sleeping {
		case 1: // First sleep
			sleeping = 2
		case 2: // No more sleep
			sleeping = 0
		}

		//logger.Debug("gossipVotesRoutine", "rsHeight", rs.Height, "rsRound", rs.Round,
		//	"prsHeight", prs.Height, "prsRound", prs.Round, "prsStep", prs.Step)

		// If height matches, then send LastCommit, Prevotes, Precommits.
		if rs.Height == prs.Height {
			heightLogger := logger.With("height", prs.Height)
			if txR.gossipVotesForHeight(heightLogger, rs, prs, ps) {
				continue OUTER_LOOP
			}
		}

		// Special catchup logic.
		// If peer is lagging by height 1, send LastCommit.
		if prs.Height != 0 && rs.Height == prs.Height+1 {
			if ps.PickSendVote(rs.LastCommit) {
				logger.Debug("Picked rs.LastCommit to send", "height", prs.Height)
				continue OUTER_LOOP
			}
		}

		// Catchup logic
		// If peer is lagging by more than 1, send Commit.
		if prs.Height != 0 && rs.Height >= prs.Height+2 {
			// Load the block commit for prs.Height,
			// which contains precommit signatures for prs.Height.
			commit := conR.conS.blockStore.LoadBlockCommit(prs.Height)
			if ps.PickSendVote(commit) {
				logger.Debug("Picked Catchup commit to send", "height", prs.Height)
				continue OUTER_LOOP
			}
		}

		if sleeping == 0 {
			// We sent nothing. Sleep...
			sleeping = 1
			logger.Debug("No votes to send, sleeping", "rs.Height", rs.Height, "prs.Height", prs.Height,
				"localPV", rs.Votes.Prevotes(rs.Round).BitArray(), "peerPV", prs.Prevotes,
				"localPC", rs.Votes.Precommits(rs.Round).BitArray(), "peerPC", prs.Precommits)
		} else if sleeping == 2 {
			// Continued sleep...
			sleeping = 1
		}

		time.Sleep(conR.conS.config.PeerGossipSleepDuration)
		continue OUTER_LOOP
	}
}

func (conR *ConsensusReactor) gossipVotesForHeight(logger log.Logger, rs *cstypes.RoundState, prs *cstypes.PeerRoundState, ps *PeerState) bool {

	// If there are lastCommits to send...
	if prs.Step == cstypes.RoundStepNewHeight {
		if ps.PickSendVote(rs.LastCommit) {
			logger.Debug("Picked rs.LastCommit to send")
			return true
		}
	}
	// If there are POL prevotes to send...
	if prs.Step <= cstypes.RoundStepPropose && prs.Round != -1 && prs.Round <= rs.Round && prs.ProposalPOLRound != -1 {
		if polPrevotes := rs.Votes.Prevotes(prs.ProposalPOLRound); polPrevotes != nil {
			if ps.PickSendVote(polPrevotes) {
				logger.Debug("Picked rs.Prevotes(prs.ProposalPOLRound) to send",
					"round", prs.ProposalPOLRound)
				return true
			}
		}
	}
	// If there are prevotes to send...
	if prs.Step <= cstypes.RoundStepPrevoteWait && prs.Round != -1 && prs.Round <= rs.Round {
		if ps.PickSendVote(rs.Votes.Prevotes(prs.Round)) {
			logger.Debug("Picked rs.Prevotes(prs.Round) to send", "round", prs.Round)
			return true
		}
	}
	// If there are precommits to send...
	if prs.Step <= cstypes.RoundStepPrecommitWait && prs.Round != -1 && prs.Round <= rs.Round {
		if ps.PickSendVote(rs.Votes.Precommits(prs.Round)) {
			logger.Debug("Picked rs.Precommits(prs.Round) to send", "round", prs.Round)
			return true
		}
	}
	// If there are prevotes to send...Needed because of validBlock mechanism
	if prs.Round != -1 && prs.Round <= rs.Round {
		if ps.PickSendVote(rs.Votes.Prevotes(prs.Round)) {
			logger.Debug("Picked rs.Prevotes(prs.Round) to send", "round", prs.Round)
			return true
		}
	}
	// If there are POLPrevotes to send...
	if prs.ProposalPOLRound != -1 {
		if polPrevotes := rs.Votes.Prevotes(prs.ProposalPOLRound); polPrevotes != nil {
			if ps.PickSendVote(polPrevotes) {
				logger.Debug("Picked rs.Prevotes(prs.ProposalPOLRound) to send",
					"round", prs.ProposalPOLRound)
				return true
			}
		}
	}

	return false
}

// NOTE: `queryMaj23Routine` has a simple crude design since it only comes
// into play for liveness when there's a signature DDoS attack happening.
func (txR *TxFlowReactor) queryMaj23Routine(peer p2p.Peer, ps *PeerState) {
	logger := conR.Logger.With("peer", peer)

OUTER_LOOP:
	for {
		// Manage disconnects from self or peer.
		if !peer.IsRunning() || !conR.IsRunning() {
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

var (
	ErrPeerStateHeightRegression = errors.New("Error peer state height regression")
	ErrPeerStateInvalidStartTime = errors.New("Error peer state invalid startTime")
)

// PeerState contains the known state of a peer, including its connection and
// threadsafe access to its PeerTxState.
// NOTE: THIS GETS DUMPED WITH rpc/core/consensus.go.
// Be mindful of what you Expose.
type PeerState struct {
	peer   p2p.Peer
	logger log.Logger

	mtx   sync.Mutex      // NOTE: Modify below using setters, never directly.
	PTXS  PeerTxState     `json:"tx_state"` // Exposed.
	Stats *peerStateStats `json:"stats"`    // Exposed.
}

// peerStateStats holds internal statistics for a peer.
type peerStateStats struct {
	TxVotes int `json:"txvotes"`
}

func (pss peerStateStats) String() string {
	return fmt.Sprintf("peerStateStats{votes: %d}",
		pss.Votes)
}

// NewPeerState returns a new PeerState for the given Peer
func NewPeerState(peer p2p.Peer) *PeerState {
	return &PeerState{
		peer:   peer,
		logger: log.NewNopLogger(),
		PTXS:   PeerTxState{},
		Stats:  &peerStateStats{},
	}
}

// SetLogger allows to set a logger on the peer state. Returns the peer state
// itself.
func (ps *PeerState) SetLogger(logger log.Logger) *PeerState {
	ps.logger = logger
	return ps
}

// GetTxState returns a shallow copy of the PeerTxState.
// There's no point in mutating it since it won't change PeerState.
func (ps *PeerState) GetTxState() *PeerTxState {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	prs := ps.PTXS // copy
	return &prs
}

// ToJSON returns a json of PeerState, marshalled using go-amino.
func (ps *PeerState) ToJSON() ([]byte, error) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	return cdc.MarshalJSON(ps)
}

// GetHeight returns an atomic snapshot of the PeerRoundState's height
// used by the mempool to ensure peers are caught up before broadcasting new txs
func (ps *PeerState) GetHeight() int64 {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()
	return ps.PRS.Height
}

// PickSendTxVote picks a txvote and sends it to the peer.
// Returns true if txvote was sent.
func (ps *PeerState) PickSendTxVote(votes ttypes.VoteSetReader) bool {
	if vote, ok := ps.PickVoteToSend(votes); ok {
		msg := &VoteMessage{vote}
		ps.logger.Debug("Sending vote message", "ps", ps, "vote", vote)
		if ps.peer.Send(VoteChannel, cdc.MustMarshalBinaryBare(msg)) {
			ps.SetHasVote(vote)
			return true
		}
		return false
	}
	return false
}

// PickVoteToSend picks a vote to send to the peer.
// Returns true if a vote was picked.
// NOTE: `votes` must be the correct Size() for the Height().
func (ps *PeerState) PickVoteToSend(votes types.VoteSetReader) (vote *types.Vote, ok bool) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	if votes.Size() == 0 {
		return nil, false
	}

	height, round, type_, size := votes.Height(), votes.Round(), types.SignedMsgType(votes.Type()), votes.Size()

	// Lazily set data using 'votes'.
	if votes.IsCommit() {
		ps.ensureCatchupCommitRound(height, round, size)
	}
	ps.ensureVoteBitArrays(height, size)

	psVotes := ps.getVoteBitArray(height, round, type_)
	if psVotes == nil {
		return nil, false // Not something worth sending
	}
	if index, ok := votes.BitArray().Sub(psVotes).PickRandom(); ok {
		return votes.GetByIndex(index), true
	}
	return nil, false
}

func (ps *PeerState) getVoteBitArray(height int64, round int, type_ types.SignedMsgType) *cmn.BitArray {
	if !types.IsVoteTypeValid(type_) {
		return nil
	}

	if ps.PRS.Height == height {
		if ps.PRS.Round == round {
			switch type_ {
			case types.PrevoteType:
				return ps.PRS.Prevotes
			case types.PrecommitType:
				return ps.PRS.Precommits
			}
		}
		if ps.PRS.CatchupCommitRound == round {
			switch type_ {
			case types.PrevoteType:
				return nil
			case types.PrecommitType:
				return ps.PRS.CatchupCommit
			}
		}
		if ps.PRS.ProposalPOLRound == round {
			switch type_ {
			case types.PrevoteType:
				return ps.PRS.ProposalPOL
			case types.PrecommitType:
				return nil
			}
		}
		return nil
	}
	if ps.PRS.Height == height+1 {
		if ps.PRS.LastCommitRound == round {
			switch type_ {
			case types.PrevoteType:
				return nil
			case types.PrecommitType:
				return ps.PRS.LastCommit
			}
		}
		return nil
	}
	return nil
}

// 'round': A round for which we have a +2/3 commit.
func (ps *PeerState) ensureCatchupCommitRound(height int64, round int, numValidators int) {
	if ps.PRS.Height != height {
		return
	}
	/*
		NOTE: This is wrong, 'round' could change.
		e.g. if orig round is not the same as block LastCommit round.
		if ps.CatchupCommitRound != -1 && ps.CatchupCommitRound != round {
			cmn.PanicSanity(fmt.Sprintf("Conflicting CatchupCommitRound. Height: %v, Orig: %v, New: %v", height, ps.CatchupCommitRound, round))
		}
	*/
	if ps.PRS.CatchupCommitRound == round {
		return // Nothing to do!
	}
	ps.PRS.CatchupCommitRound = round
	if round == ps.PRS.Round {
		ps.PRS.CatchupCommit = ps.PRS.Precommits
	} else {
		ps.PRS.CatchupCommit = cmn.NewBitArray(numValidators)
	}
}

// EnsureVoteBitArrays ensures the bit-arrays have been allocated for tracking
// what votes this peer has received.
// NOTE: It's important to make sure that numValidators actually matches
// what the node sees as the number of validators for height.
func (ps *PeerState) EnsureVoteBitArrays(height int64, numValidators int) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()
	ps.ensureVoteBitArrays(height, numValidators)
}

func (ps *PeerState) ensureVoteBitArrays(height int64, numValidators int) {
	if ps.PRS.Height == height {
		if ps.PRS.Prevotes == nil {
			ps.PRS.Prevotes = cmn.NewBitArray(numValidators)
		}
		if ps.PRS.Precommits == nil {
			ps.PRS.Precommits = cmn.NewBitArray(numValidators)
		}
		if ps.PRS.CatchupCommit == nil {
			ps.PRS.CatchupCommit = cmn.NewBitArray(numValidators)
		}
		if ps.PRS.ProposalPOL == nil {
			ps.PRS.ProposalPOL = cmn.NewBitArray(numValidators)
		}
	} else if ps.PRS.Height == height+1 {
		if ps.PRS.LastCommit == nil {
			ps.PRS.LastCommit = cmn.NewBitArray(numValidators)
		}
	}
}

// RecordVote increments internal votes related statistics for this peer.
// It returns the total number of added votes.
func (ps *PeerState) RecordVote() int {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	ps.Stats.Votes++

	return ps.Stats.Votes
}

// VotesSent returns the number of blocks for which peer has been sending us
// votes.
func (ps *PeerState) VotesSent() int {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	return ps.Stats.Votes
}

// RecordBlockPart increments internal block part related statistics for this peer.
// It returns the total number of added block parts.
func (ps *PeerState) RecordBlockPart() int {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	ps.Stats.BlockParts++
	return ps.Stats.BlockParts
}

// BlockPartsSent returns the number of useful block parts the peer has sent us.
func (ps *PeerState) BlockPartsSent() int {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	return ps.Stats.BlockParts
}

// SetHasVote sets the given vote as known by the peer
func (ps *PeerState) SetHasVote(vote *types.Vote) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	ps.setHasVote(vote.Height, vote.Round, vote.Type, vote.ValidatorIndex)
}

func (ps *PeerState) setHasVote(height int64, round int, type_ types.SignedMsgType, index int) {
	logger := ps.logger.With("peerH/R", fmt.Sprintf("%d/%d", ps.PRS.Height, ps.PRS.Round), "H/R", fmt.Sprintf("%d/%d", height, round))
	logger.Debug("setHasVote", "type", type_, "index", index)

	// NOTE: some may be nil BitArrays -> no side effects.
	psVotes := ps.getVoteBitArray(height, round, type_)
	if psVotes != nil {
		psVotes.SetIndex(index, true)
	}
}

// ApplyNewRoundStepMessage updates the peer state for the new round.
func (ps *PeerState) ApplyNewRoundStepMessage(msg *NewRoundStepMessage) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	// Ignore duplicates or decreases
	if CompareHRS(msg.Height, msg.Round, msg.Step, ps.PRS.Height, ps.PRS.Round, ps.PRS.Step) <= 0 {
		return
	}

	// Just remember these values.
	psHeight := ps.PRS.Height
	psRound := ps.PRS.Round
	psCatchupCommitRound := ps.PRS.CatchupCommitRound
	psCatchupCommit := ps.PRS.CatchupCommit

	startTime := tmtime.Now().Add(-1 * time.Duration(msg.SecondsSinceStartTime) * time.Second)
	ps.PRS.Height = msg.Height
	ps.PRS.Round = msg.Round
	ps.PRS.Step = msg.Step
	ps.PRS.StartTime = startTime
	if psHeight != msg.Height || psRound != msg.Round {
		ps.PRS.Proposal = false
		ps.PRS.ProposalBlockPartsHeader = types.PartSetHeader{}
		ps.PRS.ProposalBlockParts = nil
		ps.PRS.ProposalPOLRound = -1
		ps.PRS.ProposalPOL = nil
		// We'll update the BitArray capacity later.
		ps.PRS.Prevotes = nil
		ps.PRS.Precommits = nil
	}
	if psHeight == msg.Height && psRound != msg.Round && msg.Round == psCatchupCommitRound {
		// Peer caught up to CatchupCommitRound.
		// Preserve psCatchupCommit!
		// NOTE: We prefer to use prs.Precommits if
		// pr.Round matches pr.CatchupCommitRound.
		ps.PRS.Precommits = psCatchupCommit
	}
	if psHeight != msg.Height {
		// Shift Precommits to LastCommit.
		if psHeight+1 == msg.Height && psRound == msg.LastCommitRound {
			ps.PRS.LastCommitRound = msg.LastCommitRound
			ps.PRS.LastCommit = ps.PRS.Precommits
		} else {
			ps.PRS.LastCommitRound = msg.LastCommitRound
			ps.PRS.LastCommit = nil
		}
		// We'll update the BitArray capacity later.
		ps.PRS.CatchupCommitRound = -1
		ps.PRS.CatchupCommit = nil
	}
}

// ApplyNewValidBlockMessage updates the peer state for the new valid block.
func (ps *PeerState) ApplyNewValidBlockMessage(msg *NewValidBlockMessage) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	if ps.PRS.Height != msg.Height {
		return
	}

	if ps.PRS.Round != msg.Round && !msg.IsCommit {
		return
	}

	ps.PRS.ProposalBlockPartsHeader = msg.BlockPartsHeader
	ps.PRS.ProposalBlockParts = msg.BlockParts
}

// ApplyProposalPOLMessage updates the peer state for the new proposal POL.
func (ps *PeerState) ApplyProposalPOLMessage(msg *ProposalPOLMessage) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	if ps.PRS.Height != msg.Height {
		return
	}
	if ps.PRS.ProposalPOLRound != msg.ProposalPOLRound {
		return
	}

	// TODO: Merge onto existing ps.PRS.ProposalPOL?
	// We might have sent some prevotes in the meantime.
	ps.PRS.ProposalPOL = msg.ProposalPOL
}

// ApplyHasVoteMessage updates the peer state for the new vote.
func (ps *PeerState) ApplyHasVoteMessage(msg *HasVoteMessage) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	if ps.PRS.Height != msg.Height {
		return
	}

	ps.setHasVote(msg.Height, msg.Round, msg.Type, msg.Index)
}

// ApplyVoteSetBitsMessage updates the peer state for the bit-array of votes
// it claims to have for the corresponding BlockID.
// `ourVotes` is a BitArray of votes we have for msg.BlockID
// NOTE: if ourVotes is nil (e.g. msg.Height < rs.Height),
// we conservatively overwrite ps's votes w/ msg.Votes.
func (ps *PeerState) ApplyVoteSetBitsMessage(msg *VoteSetBitsMessage, ourVotes *cmn.BitArray) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	votes := ps.getVoteBitArray(msg.Height, msg.Round, msg.Type)
	if votes != nil {
		if ourVotes == nil {
			votes.Update(msg.Votes)
		} else {
			otherVotes := votes.Sub(ourVotes)
			hasVotes := otherVotes.Or(msg.Votes)
			votes.Update(hasVotes)
		}
	}
}

// String returns a string representation of the PeerState
func (ps *PeerState) String() string {
	return ps.StringIndented("")
}

// StringIndented returns a string representation of the PeerState
func (ps *PeerState) StringIndented(indent string) string {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()
	return fmt.Sprintf(`PeerState{
%s  Key        %v
%s  RoundState %v
%s  Stats      %v
%s}`,
		indent, ps.peer.ID(),
		indent, ps.PRS.StringIndented(indent+"  "),
		indent, ps.Stats,
		indent)
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

// HasTxVoteMessage is sent to indicate that a particular vote has been received.
type HasTxVoteMessage struct {
	TxHash           cmn.HexBytes
	ValidatorAddress cmn.HexBytes
}

// ValidateBasic performs basic validation.
func (m *HasTxVoteMessage) ValidateBasic() error {
	if err := types.ValidateHash(m.TxHash); err != nil {
		return fmt.Errorf("Wrong TxHash: %v", err)
	}
	if err := types.ValidateHash(m.ValidatorAddress); err != nil {
		return fmt.Errorf("Wrong ValidatorAddress: %v", err)
	}
	return nil
}

// String returns a string representation.
func (m *HasTxVoteMessage) String() string {
	return fmt.Sprintf("[HasTxVote %X %X]", m.TxHash, m.ValidatorAddress)
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
