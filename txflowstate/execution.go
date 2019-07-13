package txflowstate

import (
	"fmt"
	"time"

	"github.com/Fantom-foundation/go-txflow/txvotepool"
	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/libs/fail"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/proxy"
	"github.com/tendermint/tendermint/state"
	ttypes "github.com/tendermint/tendermint/types"
)

//-----------------------------------------------------------------------------
// TxExecutor handles tx execution and state updates.
// It exposes ApplyBlock(), which validates & executes the block, updates state w/ ABCI responses,
// then commits and updates the mempool atomically, then saves state.

// TxExecutor provides the context and accessories for properly executing a tx.
type TxExecutor struct {
	// execute the app against this
	proxyApp proxy.AppConnConsensus

	// events
	eventBus ttypes.BlockEventPublisher

	// manage the mempool lock during commit
	// and update both with block results after commit.
	mempool    Mempool
	txVotePool txvotepool.TxVotePool

	logger log.Logger

	metrics *Metrics
}

type TxExecutorOption func(executor *TxExecutor)

func TxExecutorWithMetrics(metrics *Metrics) TxExecutorOption {
	return func(txExec *TxExecutor) {
		txExec.metrics = metrics
	}
}

// NewTxExecutor returns a new TxExecutor with a NopEventBus.
// Call SetEventBus to provide one.
func NewTxExecutor(logger log.Logger, proxyApp proxy.AppConnConsensus, mempool Mempool, txVotePool txvotepool.TxVotePool, options ...TxExecutorOption) *TxExecutor {
	res := &TxExecutor{
		proxyApp:   proxyApp,
		eventBus:   ttypes.NopEventBus{},
		mempool:    mempool,
		txVotePool: txVotePool,
		logger:     logger,
		metrics:    NopMetrics(),
	}

	for _, option := range options {
		option(res)
	}

	return res
}

// SetEventBus - sets the event bus for publishing block related events.
// If not called, it defaults to types.NopEventBus.
func (txExec *TxExecutor) SetEventBus(eventBus ttypes.BlockEventPublisher) {
	txExec.eventBus = eventBus
}

// ApplyTx validates the tx against the state, executes it against the app,
// fires the relevant events, commits the app, and saves responses.
func (txExec *TxExecutor) ApplyTx(tx *ttypes.Tx) error {

	startTime := time.Now().UnixNano()
	abciResponses, err := execTxOnProxyApp(txExec.logger, txExec.proxyApp, tx)
	endTime := time.Now().UnixNano()
	txExec.metrics.BlockProcessingTime.Observe(float64(endTime-startTime) / 1000000)
	if err != nil {
		return state.ErrProxyAppConn(err)
	}

	fail.Fail() // XXX

	// Lock mempool, commit app state, update mempoool.
	_, err = txExec.Commit(tx)
	if err != nil {
		return fmt.Errorf("Commit failed for application: %v", err)
	}

	fail.Fail() // XXX

	// Events are fired after everything else.
	// NOTE: if we crash between Commit and Save, events wont be fired during replay
	fireEvents(txExec.logger, txExec.eventBus, tx, abciResponses)

	return nil
}

// Commit locks the mempool, runs the ABCI Commit message, and updates the
// mempool.
// It returns the result of calling abci.Commit (the AppHash), and an error.
// The Mempool must be locked during commit and update because state is
// typically reset on Commit and old txs must be replayed against committed
// state before new txs are run in the mempool, lest they be invalid.
func (txExec *TxExecutor) Commit(tx *ttypes.Tx) ([]byte, error) {
	txExec.mempool.Lock()
	defer txExec.mempool.Unlock()

	// while mempool is Locked, flush to ensure all async requests have completed
	// in the ABCI app before Commit.
	err := txExec.mempool.FlushAppConn()
	if err != nil {
		txExec.logger.Error("Client error during mempool.FlushAppConn", "err", err)
		return nil, err
	}

	// Commit block, get hash back
	res, err := txExec.proxyApp.CommitSync()
	if err != nil {
		txExec.logger.Error(
			"Client error during proxyAppConn.CommitSync",
			"err", err,
		)
		return nil, err
	}
	// ResponseCommit has no error code - just data

	txExec.logger.Info(
		"Committed state",
		"appHash", fmt.Sprintf("%X", res.Data),
	)

	// Update mempool.
	err = txExec.mempool.UpdateTx(tx)

	return res.Data, err
}

//---------------------------------------------------------
// Helper functions for executing tx and updating state

// Executes transaction on proxyAppConn.
func execTxOnProxyApp(
	logger log.Logger,
	proxyAppConn proxy.AppConnConsensus,
	tx *ttypes.Tx,
) (*state.ABCIResponses, error) {
	abciResponses := NewABCIResponses()

	// Execute transaction and get hash.
	proxyCb := func(req *abci.Request, res *abci.Response) {
		switch r := res.Value.(type) {
		case *abci.Response_DeliverTx:
			abciResponses.DeliverTx[0] = r.DeliverTx
		}
	}
	proxyAppConn.SetResponseCallback(proxyCb)

	proxyAppConn.DeliverTxAsync(abci.RequestDeliverTx{Tx: *tx})
	if err := proxyAppConn.Error(); err != nil {
		return nil, err
	}

	logger.Info("Executed tx %s", tx.Hash())

	return abciResponses, nil
}

// Fire NewBlock, NewBlockHeader.
// Fire TxEvent for every tx.
// NOTE: if Tendermint crashes before commit, some or all of these events may be published again.
func fireEvents(logger log.Logger, eventBus ttypes.BlockEventPublisher, tx *ttypes.Tx, abciResponses *state.ABCIResponses) {
	eventBus.PublishEventTx(ttypes.EventDataTx{TxResult: ttypes.TxResult{
		Tx:     *tx,
		Result: *(abciResponses.DeliverTx[0]),
	}})
}

//----------------------------------------------------------------------------------------------------
// Execute block without state. TODO: eliminate

// ExecCommitTx executes and commits a tx on the proxyApp.
// It returns the application root hash (result of abci.Commit).
func ExecCommitTx(
	appConnConsensus proxy.AppConnConsensus,
	tx *ttypes.Tx,
	logger log.Logger,
) ([]byte, error) {
	_, err := execTxOnProxyApp(logger, appConnConsensus, tx)
	if err != nil {
		logger.Error("Error executing tx on proxy app", "tx_hash", tx.Hash(), "err", err)
		return nil, err
	}
	// Commit block, get hash back
	res, err := appConnConsensus.CommitSync()
	if err != nil {
		logger.Error("Client error during proxyAppConn.CommitSync", "err", res)
		return nil, err
	}
	// ResponseCommit has no error or log, just data
	return res.Data, nil
}

// NewABCIResponses returns a new ABCIResponses
func NewABCIResponses() *state.ABCIResponses {
	resDeliverTxs := make([]*abci.ResponseDeliverTx, 1)
	return &state.ABCIResponses{
		DeliverTx: resDeliverTxs,
	}
}
