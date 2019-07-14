package txflow

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/tendermint/tendermint/config"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/log"
	ttypes "github.com/tendermint/tendermint/types"

	"github.com/Fantom-foundation/go-txflow/txflowstate"
	"github.com/Fantom-foundation/go-txflow/types"
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

// TxFlowState handles execution of the txflow algorithm.
// It processes votes, and upon reaching agreement,
// commits txs and executes them against the application.
// The internal state machine receives input from peers, the internal validator, and from a timer.
type TxFlowState struct {
	cmn.BaseService
	config *config.ConsensusConfig

	Height     int64
	ChainID    string
	StartTime  time.Time
	Validators *ttypes.ValidatorSet
	TxVoteSets map[string]*types.TxVoteSet

	// store txs and commits
	txStore txflowstate.TxStore

	// execute finalized txs
	txExec *txflowstate.TxExecutor

	// internal state
	mtx sync.RWMutex

	// This is the blockchain state, which essentially becomes a BFT timer
	state sm.State // State until height-1.

	// we use eventBus to trigger msg broadcasts in the reactor,
	// and to notify external subscribers, eg. through a websocket
	eventBus *ttypes.EventBus

	// a Write-Ahead Log ensures we can recover from any kind of crash
	// and helps us avoid signing conflicting votes
	wal          WAL
	replayMode   bool // so we don't log signing errors during replay
	doWALCatchup bool // determines if we even try to do the catchup

	// closed when we finish shutting down
	done chan struct{}

	// for reporting metrics
	metrics *Metrics
}

// TxFlowOption sets an optional parameter on the TxFlowState.
type TxFlowOption func(*TxFlowState)

// NewTxFlowState returns a new TxFlowState.
func NewTxFlowState(
	state sm.State,
	txExec *txflowstate.TxExecutor,
	txStore txflowstate.TxStore,
	options ...TxFlowOption,
) *TxFlowState {
	ts := &TxFlowState{
		Height:       state.LastBlockHeight,
		ChainID:      state.ChainID,
		StartTime:    time.Now(),
		Validators:   state.Validators,
		TxVoteSets:   make(map[string]*types.TxVoteSet),
		txExec:       txExec,
		txStore:      txStore,
		done:         make(chan struct{}),
		doWALCatchup: true,
		wal:          nilWAL{},
		metrics:      NopMetrics(),
	}

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
}

// SetEventBus sets event bus.
func (ts *TxFlowState) SetEventBus(b *ttypes.EventBus) {
	ts.eventBus = b
	ts.txExec.SetEventBus(b)
}

// StateMetrics sets the metrics.
func StateMetrics(metrics *Metrics) TxFlowOption {
	return func(ts *TxFlowState) { ts.metrics = metrics }
}

// String returns a string.
func (ts *TxFlowState) String() string {
	// better not to access shared variables
	return fmt.Sprintf("TxFlowState") //(H:%v R:%v S:%v", cs.Height, cs.Round, cs.Step)
}

// GetState returns a copy of the chain state.
func (ts *TxFlowState) GetState() sm.State {
	ts.mtx.RLock()
	defer ts.mtx.RUnlock()
	return ts.state.Copy()
}

// GetLastHeight returns the last height committed.
// If there were no blocks, returns 0.
func (ts *TxFlowState) GetLastHeight() int64 {
	ts.mtx.RLock()
	defer ts.mtx.RUnlock()
	return ts.Height - 1
}

// GetValidators returns a copy of the current validators.
func (ts *TxFlowState) GetValidators() (int64, []*ttypes.Validator) {
	ts.mtx.RLock()
	defer ts.mtx.RUnlock()
	return ts.state.LastBlockHeight, ts.state.Validators.Copy().Validators
}

// LoadCommit loads the commit for a given hash.
func (ts *TxFlowState) LoadCommit(txHash cmn.HexBytes) *types.Commit {
	ts.mtx.RLock()
	defer ts.mtx.RUnlock()
	return ts.txStore.LoadTxCommit(txHash)
}

// OnStart implements cmn.Service.
// It loads the latest state via the WAL, and starts the timeout and receive routines.
func (ts *TxFlowState) OnStart() error {

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

// Attempt to add the vote. if its a duplicate signature, dupeout the validator
func (ts *TxFlowState) tryAddVote(vote *types.TxVote, peerID p2p.ID) (bool, error) {
	added, err := ts.addVote(vote, peerID)
	if err != nil {
		// If the vote height is off, we'll just ignore it,
		// But if it's a conflicting sig, add it to the cs.evpool.
		// If it's otherwise invalid, punish peer.
		if err == ErrVoteHeightMismatch {
			return added, err
		} else if voteErr, ok := err.(*types.ErrTxVoteConflictingVotes); ok {
			addr := ts.privValidator.GetPubKey().Address()
			if bytes.Equal(vote.ValidatorAddress, addr) {
				cs.Logger.Error("Found conflicting vote from ourselves. Did you unsafe_reset a validator?", "height", vote.Height)
				return added, err
			}
			ts.evpool.AddEvidence(voteErr.DuplicateVoteEvidence)
			return added, err
		} else {
			// Probably an invalid signature / Bad peer.
			// Seems this can also err sometimes with "Unexpected step" - perhaps not from a bad peer ?
			ts.Logger.Error("Error attempting to add vote", "err", err)
			return added, ErrAddingVote
		}
	}
	return added, nil
}

//-----------------------------------------------------------------------------

func (ts *TxFlowState) addVote(vote *types.TxVote) (added bool, err error) {
	ts.Logger.Debug("addVote", "voteHeight", vote.Height, "valAddress", vote.ValidatorAddress, "tsHeight", ts.Height)

	txHash := string(vote.TxHash)
	added, err = ts.TxVoteSets[txHash].AddVote(vote)
	if !added {
		// Either duplicate, or error upon cs.Votes.AddByIndex()
		return
	}
	if ts.TxVoteSets[txHash].HasTwoThirdsMajority() {
		//enter commit
	}
	return
}
