package state

import (
	"fmt"

	"github.com/Fantom-foundation/txflow/types"
	dbm "github.com/tendermint/tendermint/libs/db"
	"github.com/tendermint/tendermint/libs/fail"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/mempool"
	tstate "github.com/tendermint/tendermint/state"
	ttypes "github.com/tendermint/tendermint/types"
)

//-----------------------------------------------------------------------------
// EventBlockExecutor handles event block execution and state updates.
// then commits and updates the mempool atomically, then saves state.

// EventBlockExecutor provides the context and accessories for properly executing a block.
type EventBlockExecutor struct {
	// save state, validators, consensus params, abci responses here
	db dbm.DB

	// manage the eventpool lock during commit
	// and update both with block results after commit.
	eventpool Eventpool
	mempool   mempool.Mempool
	evpool    EvidencePool

	logger log.Logger

	metrics *tstate.Metrics
}

type EventBlockExecutorOption func(executor *EventBlockExecutor)

func EventBlockExecutorWithMetrics(metrics *tstate.Metrics) EventBlockExecutorOption {
	return func(eventBlockExec *EventBlockExecutor) {
		eventBlockExec.metrics = metrics
	}
}

// NewEventBlockExecutor returns a new BlockExecutor with a NopEventBus.
// Call SetEventBus to provide one.
func NewEventBlockExecutor(db dbm.DB, logger log.Logger, eventpool Eventpool, mempool mempool.Mempool, evpool EvidencePool, options ...EventBlockExecutorOption) *EventBlockExecutor {
	res := &EventBlockExecutor{
		db:        db,
		eventpool: eventpool,
		mempool:   mempool,
		evpool:    evpool,
		logger:    logger,
		metrics:   tstate.NopMetrics(),
	}

	for _, option := range options {
		option(res)
	}

	return res
}

// CreateEventBlock calls state.MakeEventBlock with evidence from the evpool
// and txs from the mempool. The max bytes must be big enough to fit the commit.
// Up to 1/10th of the block space is allcoated for maximum sized evidence.
// The rest is given to txs, up to the max gas.
func (eventBlockExec *EventBlockExecutor) CreateEventBlock(
	height int64,
	state State,
	proposerAddr []byte,
) (*types.EventBlock, *ttypes.PartSet) {

	maxBytes := state.ConsensusParams.Block.MaxBytes
	maxGas := state.ConsensusParams.Block.MaxGas

	// Fetch a limited amount of valid evidence
	maxNumEvidence, _ := ttypes.MaxEvidencePerBlock(maxBytes)
	evidence := eventBlockExec.evpool.PendingEvidence(maxNumEvidence)

	// Fetch a limited amount of valid txs
	maxDataBytes := types.MaxDataBytes(maxBytes, state.Validators.Size(), len(evidence))
	txs := eventBlockExec.mempool.ReapMaxBytesMaxGas(maxDataBytes, maxGas)

	// Fetch all head events for all validators
	eventBlockIDs := eventBlockExec.eventpool.HeadEventBlockIDs()

	return state.MakeEventBlock(height, txs, evidence, eventBlockIDs, proposerAddr)
}

// ValidateEventBlock validates the given event block against the given state.
// If the event block is invalid, it returns an error.
// Validation does not mutate state, but does require historical information from the stateDB,
// ie. to verify evidence from a validator at an old height.
func (eventBlockExec *EventBlockExecutor) ValidateEventBlock(state State, eventBlock *types.EventBlock) error {
	return validateEventBlock(eventBlockExec.evpool, eventBlockExec.db, state, eventBlock)
}

// ApplyEventBlock validates the block against the state
// It's the only function that needs to be called
// from outside this package to process and commit an entire block.
func (eventBlockExec *EventBlockExecutor) ApplyEventBlock(state State, eventBlockIDs []types.EventBlockID, eventBlock *types.EventBlock) (State, error) {

	if err := eventBlockExec.ValidateEventBlock(state, eventBlock); err != nil {
		return state, ErrInvalidBlock(err)
	}

	// Update the state with the event block and responses.
	state, err := updateState(state, eventBlockIDs, &eventBlock.Header)
	if err != nil {
		return state, fmt.Errorf("Commit failed for application: %v", err)
	}

	// Lock eventpool, commit app state, update eventpool.
	err = eventBlockExec.Commit(state, eventBlock)
	if err != nil {
		return state, fmt.Errorf("Commit failed for application: %v", err)
	}

	// Update evpool with the block and state.
	eventBlockExec.evpool.Update(eventBlock, state)

	fail.Fail() // XXX

	// Save the state.
	SaveState(eventBlockExec.db, state)

	fail.Fail() // XXX

	return state, nil
}

// Commit locks the mempool & eventpool, and updates both
// The pools must be locked during commit and update because state is
// typically reset on Commit and old txs must be replayed against committed
// state before new txs are run in the pool, lest they be invalid.
func (eventBlockExec *EventBlockExecutor) Commit(
	state State,
	eventBlock *types.EventBlock,
) (error) {
	eventBlockExec.mempool.Lock()
	defer eventBlockExec.mempool.Unlock()

	eventBlockExec.eventpool.Lock()
	defer eventBlockExec.eventpool.Unlock()

	// while mempool is Locked, flush to ensure all async requests have completed
	// in the ABCI app before Commit.
	err := eventBlockExec.mempool.FlushAppConn()
	if err != nil {
		eventBlockExec.logger.Error("Client error during mempool.FlushAppConn", "err", err)
		return err
	}

	eventBlockExec.logger.Info(
		"Committed state",
		"height", eventBlock.Height,
		"txs", eventBlock.NumTxs,
	)

	// Update mempool.
	//Can't use this state to update the Tx pool
	//TODO: remove the transactions out of the pool here
	err = eventBlockExec.mempool.Update(
		eventBlock.Height,
		eventBlock.Txs,
		nil,
		nil,
	)
	if err != nil {
		return err
	}

	// Update eventpool.
	err = eventBlockExec.eventpool.Update(
		eventBlock.Height,
		eventBlock.Header.LastEventBlockIDs,
	)

	return err
}

// updateState returns a new State updated according to the header and responses.
func updateState(
	state State,
	blockIDs []types.EventBlockID,
	header *types.Header,
) (State, error) {

	// TODO: allow app to upgrade version
	nextVersion := state.Version

	// NOTE: the AppHash has not been populated.
	// It will be filled on state.Save.
	return State{
		Version:              nextVersion,
		ChainID:              state.ChainID,
		LastEventBlockHeight: header.Height,
		LastEventBlockIDs:    blockIDs,
		LastEventBlockTime:   header.Time,
		Validators:           state.NextValidators.Copy(),
		LastValidators:       state.Validators.Copy(),
	}, nil
}
