package txflow

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"

	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/log"
	ttypes "github.com/tendermint/tendermint/types"

	"github.com/Fantom-foundation/go-txflow/txflowstate"
	"github.com/Fantom-foundation/go-txflow/types"
	cstypes "github.com/tendermint/tendermint/consensus/types"
	tmevents "github.com/tendermint/tendermint/libs/events"
	"github.com/tendermint/tendermint/p2p"
	sm "github.com/tendermint/tendermint/state"
)

//-----------------------------------------------------------------------------
// Errors

var (
	ErrAddingVote = errors.New("Error adding vote")
)

//-----------------------------------------------------------------------------

var (
	msgQueueSize = 1000
)

// msgs from the reactor which may update the state
type msgInfo struct {
	Msg    TxFlowMessage `json:"msg"`
	PeerID p2p.ID        `json:"peer_key"`
}

// internally generated messages which may update the state
// Timeouts are used for refresher states, to re-transmit deadlocked transactions
type timeoutInfo struct {
	Duration time.Duration `json:"duration"`
	Height   int64         `json:"height"`
}

func (ti *timeoutInfo) String() string {
	return fmt.Sprintf("%v ; %d/%d %v", ti.Duration, ti.Height)
}

// interface to the votepool
type voteNotifier interface {
	VoteAvailable() <-chan struct{}
}

// TxFlowState handles execution of the txflow algorithm.
// It processes votes, and upon reaching agreement,
// commits txs and executes them against the application.
// The internal state machine receives input from peers, the internal validator, and from a timer.
type TxFlowState struct {
	cmn.BaseService

	Height                    int64
	StartTime                 time.Time
	CommitTime                time.Time
	Validators                ttypes.ValidatorSet
	TxVoteSets                map[string]*types.TxVoteSet
	LastValidators            *ttypes.ValidatorSet
	TriggeredTimeoutPrecommit bool

	// store txs and commits
	txStore txflowstate.TxStore

	// execute finalized txs
	txExec *txflowstate.TxExecutor

	// notify us if votes are available
	voteNotifier voteNotifier

	// internal state
	mtx sync.RWMutex

	// This is the blockchain state, which essentially becomes a BFT timer
	state sm.State // State until height-1.

	// state changes may be triggered by: msgs from peers,
	// msgs from ourself, or by timeouts
	peerMsgQueue     chan msgInfo
	internalMsgQueue chan msgInfo
	timeoutTicker    TimeoutTicker

	// information about added votes are written on this channel
	// so statistics can be computed by reactor
	statsMsgQueue chan msgInfo

	// we use eventBus to trigger msg broadcasts in the reactor,
	// and to notify external subscribers, eg. through a websocket
	eventBus *ttypes.EventBus

	// a Write-Ahead Log ensures we can recover from any kind of crash
	// and helps us avoid signing conflicting votes
	wal          WAL
	replayMode   bool // so we don't log signing errors during replay
	doWALCatchup bool // determines if we even try to do the catchup

	// some functions can be overwritten for testing
	decideTx  func(tx *ttypes.Tx)
	doPrevote func(tx *ttypes.Tx)
	setTx     func(tx *ttypes.Tx) error

	// closed when we finish shutting down
	done chan struct{}

	// synchronous pubsub between consensus state and reactor.
	// state only emits EventNewRoundStep and EventVote
	evsw tmevents.EventSwitch

	// for reporting metrics
	metrics *Metrics
}

// StateOption sets an optional parameter on the TxFlowState.
type StateOption func(*TxFlowState)

// NewTxFlowState returns a new TxFlowState.
func NewTxFlowState(
	state sm.State,
	txExec *txflowstate.TxExecutor,
	txStore txflowstate.TxStore,
	voteNotifier voteNotifier,
	options ...StateOption,
) *TxFlowState {
	ts := &TxFlowState{
		txExec:           txExec,
		txStore:          txStore,
		voteNotifier:     voteNotifier,
		peerMsgQueue:     make(chan msgInfo, msgQueueSize),
		internalMsgQueue: make(chan msgInfo, msgQueueSize),
		timeoutTicker:    NewTimeoutTicker(),
		statsMsgQueue:    make(chan msgInfo, msgQueueSize),
		done:             make(chan struct{}),
		doWALCatchup:     true,
		wal:              nilWAL{},
		evsw:             tmevents.NewEventSwitch(),
		metrics:          NopMetrics(),
	}
	// set function defaults (may be overwritten before calling Start)
	ts.decideProposal = ts.defaultDecideProposal
	ts.doPrevote = ts.defaultDoPrevote
	ts.setProposal = ts.defaultSetProposal

	ts.updateToState(state)

	// Don't call scheduleRound0 yet.
	// We do that upon Start().
	ts.reconstructLastCommit(state)
	ts.BaseService = *cmn.NewBaseService(nil, "TxFlowState", ts)
	for _, option := range options {
		option(ts)
	}
	return ts
}

//----------------------------------------
// Public interface

// SetLogger implements Service.
func (ts *TxFlowState) SetLogger(l log.Logger) {
	ts.BaseService.Logger = l
	ts.timeoutTicker.SetLogger(l)
}

// SetEventBus sets event bus.
func (ts *TxFlowState) SetEventBus(b *ttypes.EventBus) {
	ts.eventBus = b
	ts.txExec.SetEventBus(b)
}

// StateMetrics sets the metrics.
func StateMetrics(metrics *Metrics) StateOption {
	return func(ts *TxFlowState) { ts.metrics = metrics }
}

// String returns a string.
func (ts *TxFlowState) String() string {
	// better not to access shared variables
	return fmt.Sprintf("TxFlowState") //(H:%v R:%v S:%v", cs.Height, cs.Round, cs.Step)
}

// GetState returns a copy of the chain state.
func (ts *TxFlowState) GetState() sm.State {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.state.Copy()
}

// GetLastHeight returns the last height committed.
// If there were no blocks, returns 0.
func (ts *TxFlowState) GetLastHeight() int64 {
	cs.mtx.RLock()
	defer cs.mtx.RUnlock()
	return cs.Height - 1
}

// GetValidators returns a copy of the current validators.
func (ts *TxFlowState) GetValidators() (int64, []*ttypes.Validator) {
	ts.mtx.RLock()
	defer ts.mtx.RUnlock()
	return ts.state.LastBlockHeight, ts.state.Validators.Copy().Validators
}

// SetPrivValidator sets the private validator account for signing votes.
func (ts *TxFlowState) SetPrivValidator(priv types.PrivValidator) {
	ts.mtx.Lock()
	ts.privValidator = priv
	ts.mtx.Unlock()
}

// SetTimeoutTicker sets the local timer. It may be useful to overwrite for testing.
func (ts *TxFlowState) SetTimeoutTicker(timeoutTicker TimeoutTicker) {
	ts.mtx.Lock()
	ts.timeoutTicker = timeoutTicker
	ts.mtx.Unlock()
}

// LoadCommit loads the commit for a given hash.
func (ts *TxFlowState) LoadCommit(txHash cmn.HexBytes) *ttypes.Commit {
	ts.mtx.RLock()
	defer ts.mtx.RUnlock()
	return ts.txStore.LoadTxCommit(txHash)
}

// OnStart implements cmn.Service.
// It loads the latest state via the WAL, and starts the timeout and receive routines.
func (ts *TxFlowState) OnStart() error {
	if err := ts.evsw.Start(); err != nil {
		return err
	}

	// we may set the WAL in testing before calling Start,
	// so only OpenWAL if its still the nilWAL
	if _, ok := ts.wal.(nilWAL); ok {
		walFile := ts.config.WalFile()
		wal, err := ts.OpenWAL(walFile)
		if err != nil {
			cs.Logger.Error("Error loading ConsensusState wal", "err", err.Error())
			return err
		}
		ts.wal = wal
	}

	// we need the timeoutRoutine for replay so
	// we don't block on the tick chan.
	// NOTE: we will get a build up of garbage go routines
	// firing on the tockChan until the receiveRoutine is started
	// to deal with them (by that point, at most one will be valid)
	if err := ts.timeoutTicker.Start(); err != nil {
		return err
	}

	// we may have lost some votes if the process crashed
	// reload from consensus log to catchup
	if ts.doWALCatchup {
		if err := ts.catchupReplay(cs.Height); err != nil {
			// don't try to recover from data corruption error
			if IsDataCorruptionError(err) {
				ts.Logger.Error("Encountered corrupt WAL file", "err", err.Error())
				ts.Logger.Error("Please repair the WAL file before restarting")
				fmt.Println(`You can attempt to repair the WAL as follows:

----
WALFILE=~/.tendermint/data/cs.wal/wal
cp $WALFILE ${WALFILE}.bak # backup the file
go run scripts/wal2json/main.go $WALFILE > wal.json # this will panic, but can be ignored
rm $WALFILE # remove the corrupt file
go run scripts/json2wal/main.go wal.json $WALFILE # rebuild the file without corruption
----`)

				return err
			}

			ts.Logger.Error("Error on catchup replay. Proceeding to start ConsensusState anyway", "err", err.Error())
			// NOTE: if we ever do return an error here,
			// make sure to stop the timeoutTicker
		}
	}

	// now start the receiveRoutine
	go ts.receiveRoutine(0)

	return nil
}

// timeoutRoutine: receive requests for timeouts on tickChan and fire timeouts on tockChan
// receiveRoutine: serializes processing of proposoals, block parts, votes; coordinates state transitions
func (ts *TxFlowState) startRoutines(maxSteps int) {
	err := ts.timeoutTicker.Start()
	if err != nil {
		ts.Logger.Error("Error starting timeout ticker", "err", err)
		return
	}
	go ts.receiveRoutine(maxSteps)
}

// OnStop implements cmn.Service.
func (ts *TxFlowState) OnStop() {
	ts.evsw.Stop()
	ts.timeoutTicker.Stop()
	// WAL is stopped in receiveRoutine.
}

// Wait waits for the the main routine to return.
// NOTE: be sure to Stop() the event switch and drain
// any event channels or this may deadlock
func (ts *TxFlowState) Wait() {
	<-ts.done
}

// OpenWAL opens a file to log all consensus messages and timeouts for deterministic accountability
func (ts *TxFlowState) OpenWAL(walFile string) (WAL, error) {
	wal, err := NewWAL(walFile)
	if err != nil {
		ts.Logger.Error("Failed to open WAL for consensus state", "wal", walFile, "err", err)
		return nil, err
	}
	wal.SetLogger(ts.Logger.With("wal", walFile))
	if err := wal.Start(); err != nil {
		return nil, err
	}
	return wal, nil
}

//------------------------------------------------------------
// internal functions for managing the state

func (ts *TxFlowState) updateHeight(height int64) {
	ts.metrics.Height.Set(float64(height))
	ts.Height = height
}

// Attempt to schedule a timeout (by sending timeoutInfo on the tickChan)
func (ts *TxFlowState) scheduleTimeout(duration time.Duration, height int64) {
	ts.timeoutTicker.ScheduleTimeout(timeoutInfo{duration, height})
}

// send a msg into the receiveRoutine regarding our own proposal, block part, or vote
func (ts *TxFlowState) sendInternalMessage(mi msgInfo) {
	select {
	case ts.internalMsgQueue <- mi:
	default:
		// NOTE: using the go-routine means our votes can
		// be processed out of order.
		// TODO: use CList here for strict determinism and
		// attempt push to internalMsgQueue in receiveRoutine
		ts.Logger.Info("Internal msg queue is full. Using a go-routine")
		go func() { ts.internalMsgQueue <- mi }()
	}
}

// Attempt to add the vote. if its a duplicate signature, dupeout the validator
func (ts *TxFlowState) tryAddVote(vote *types.TxVote, peerID p2p.ID) (bool, error) {
	added, err := ts.addVote(vote, peerID)
	if err != nil {
		// If the vote height is off, we'll just ignore it,
		// But if it's a conflicting sig, add it to the cs.evpool.
		// If it's otherwise invalid, punish peer.
		if err == ErrVoteHeightMismatch {
			return added, err
		} else if voteErr, ok := err.(*types.ErrVoteConflictingVotes); ok {
			addr := cs.privValidator.GetPubKey().Address()
			if bytes.Equal(vote.ValidatorAddress, addr) {
				cs.Logger.Error("Found conflicting vote from ourselves. Did you unsafe_reset a validator?", "height", vote.Height, "round", vote.Round, "type", vote.Type)
				return added, err
			}
			cs.evpool.AddEvidence(voteErr.DuplicateVoteEvidence)
			return added, err
		} else {
			// Probably an invalid signature / Bad peer.
			// Seems this can also err sometimes with "Unexpected step" - perhaps not from a bad peer ?
			cs.Logger.Error("Error attempting to add vote", "err", err)
			return added, ErrAddingVote
		}
	}
	return added, nil
}

//-----------------------------------------------------------------------------

func (ts *TxFlowState) addVote(vote *types.TxVote) (added bool, err error) {
	ts.Logger.Debug("addVote", "voteHeight", vote.Height, "valAddress", vote.ValidatorAddress, "tsHeight", ts.Height)

	height := ts.Height
	added, err = ts.TxVoteSets[vote.ValidatorAddress].AddVote(vote)
	added, err = ts.Votes.AddVote(vote, peerID)
	if !added {
		// Either duplicate, or error upon cs.Votes.AddByIndex()
		return
	}

	cs.eventBus.PublishEventVote(types.EventDataVote{Vote: vote})
	cs.evsw.FireEvent(types.EventVote, vote)

	switch vote.Type {
	case types.PrevoteType:
		prevotes := cs.Votes.Prevotes(vote.Round)
		cs.Logger.Info("Added to prevote", "vote", vote, "prevotes", prevotes.StringShort())

		// If +2/3 prevotes for a block or nil for *any* round:
		if blockID, ok := prevotes.TwoThirdsMajority(); ok {

			// There was a polka!
			// If we're locked but this is a recent polka, unlock.
			// If it matches our ProposalBlock, update the ValidBlock

			// Unlock if `cs.LockedRound < vote.Round <= cs.Round`
			// NOTE: If vote.Round > cs.Round, we'll deal with it when we get to vote.Round
			if (cs.LockedBlock != nil) &&
				(cs.LockedRound < vote.Round) &&
				(vote.Round <= cs.Round) &&
				!cs.LockedBlock.HashesTo(blockID.Hash) {

				cs.Logger.Info("Unlocking because of POL.", "lockedRound", cs.LockedRound, "POLRound", vote.Round)
				cs.LockedRound = -1
				cs.LockedBlock = nil
				cs.LockedBlockParts = nil
				cs.eventBus.PublishEventUnlock(cs.RoundStateEvent())
			}

			// Update Valid* if we can.
			// NOTE: our proposal block may be nil or not what received a polka..
			if len(blockID.Hash) != 0 && (cs.ValidRound < vote.Round) && (vote.Round == cs.Round) {

				if cs.ProposalBlock.HashesTo(blockID.Hash) {
					cs.Logger.Info(
						"Updating ValidBlock because of POL.", "validRound", cs.ValidRound, "POLRound", vote.Round)
					cs.ValidRound = vote.Round
					cs.ValidBlock = cs.ProposalBlock
					cs.ValidBlockParts = cs.ProposalBlockParts
				} else {
					cs.Logger.Info(
						"Valid block we don't know about. Set ProposalBlock=nil",
						"proposal", cs.ProposalBlock.Hash(), "blockId", blockID.Hash)
					// We're getting the wrong block.
					cs.ProposalBlock = nil
				}
				if !cs.ProposalBlockParts.HasHeader(blockID.PartsHeader) {
					cs.ProposalBlockParts = types.NewPartSetFromHeader(blockID.PartsHeader)
				}
				cs.evsw.FireEvent(types.EventValidBlock, &cs.RoundState)
				cs.eventBus.PublishEventValidBlock(cs.RoundStateEvent())
			}
		}

		// If +2/3 prevotes for *anything* for future round:
		if cs.Round < vote.Round && prevotes.HasTwoThirdsAny() {
			// Round-skip if there is any 2/3+ of votes ahead of us
			cs.enterNewRound(height, vote.Round)
		} else if cs.Round == vote.Round && cstypes.RoundStepPrevote <= cs.Step { // current round
			blockID, ok := prevotes.TwoThirdsMajority()
			if ok && (cs.isProposalComplete() || len(blockID.Hash) == 0) {
				cs.enterPrecommit(height, vote.Round)
			} else if prevotes.HasTwoThirdsAny() {
				cs.enterPrevoteWait(height, vote.Round)
			}
		} else if cs.Proposal != nil && 0 <= cs.Proposal.POLRound && cs.Proposal.POLRound == vote.Round {
			// If the proposal is now complete, enter prevote of cs.Round.
			if cs.isProposalComplete() {
				cs.enterPrevote(height, cs.Round)
			}
		}

	case types.PrecommitType:
		precommits := cs.Votes.Precommits(vote.Round)
		cs.Logger.Info("Added to precommit", "vote", vote, "precommits", precommits.StringShort())

		blockID, ok := precommits.TwoThirdsMajority()
		if ok {
			// Executed as TwoThirdsMajority could be from a higher round
			cs.enterNewRound(height, vote.Round)
			cs.enterPrecommit(height, vote.Round)
			if len(blockID.Hash) != 0 {
				cs.enterCommit(height, vote.Round)
				if cs.config.SkipTimeoutCommit && precommits.HasAll() {
					cs.enterNewRound(cs.Height, 0)
				}
			} else {
				cs.enterPrecommitWait(height, vote.Round)
			}
		} else if cs.Round <= vote.Round && precommits.HasTwoThirdsAny() {
			cs.enterNewRound(height, vote.Round)
			cs.enterPrecommitWait(height, vote.Round)
		}

	default:
		panic(fmt.Sprintf("Unexpected vote type %X", vote.Type)) // go-wire should prevent this.
	}

	return
}
