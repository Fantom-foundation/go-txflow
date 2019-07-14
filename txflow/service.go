package txflow

import (
	"sync"

	"github.com/Fantom-foundation/go-txflow/types"
	"github.com/tendermint/tendermint/libs/clist"
	cmn "github.com/tendermint/tendermint/libs/common"
	ttypes "github.com/tendermint/tendermint/types"
)

//-----------------------------------------------------------------------------

// TxFlowReactor defines a reactor for the consensus service.
type TxFlowReactor struct {
	cmn.BaseService

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
	txR.BaseService = *cmn.NewBaseService(nil, "TxFlowReactor", txR)
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
	go txR.checkMaj23Routine()

	return nil
}

// OnStop implements BaseService by unsubscribing from events and stopping
// state.
func (txR *TxFlowReactor) OnStop() {
	txR.txS.Stop()
	txR.txS.Wait()
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

// Sign new mempool txs.
func (txR *TxFlowReactor) checkMaj23Routine() {
	var next *clist.CElement
	for {
		// In case of both next.NextWaitChan() and peer.Quit() are variable at the same time
		if !txR.IsRunning() {
			return
		}
		// This happens because the CElement we were looking at got garbage
		// collected (removed). That is, .NextWait() returned nil. Go ahead and
		// start from the beginning.
		if next == nil {
			select {
			case <-txR.TxVotePool.TxsWaitChan(): // Wait until a tx is available
				if next = txR.TxVotePool.TxsFront(); next == nil {
					continue
				}
			case <-txR.Quit():
				return
			}
		}

		memTx := next.Value.(*txvotepool.MempoolTxVote)
		//Check if we have already seen this txHash
		txHash := string(memTx.tx.TxHash)
		if _, ok := txR.TxVoteSets[txHash]; !ok {
			voteSet := types.NewTxVoteSet(
				txR.ChainID,
				txR.Height,
				memTx.tx.TxHash,
				txR.Validators,
			)
			txR.TxVoteSets[txHash] = voteSet
		}

		txR.TxVoteSets[txHash].AddVote(&memTx.tx)

		_, val := txR.txS.Validators.GetByAddress(memTx.tx.ValidatorAddress)

		txR.Logger.Info("Validator", "Address", memTx.tx.String())

		txR.Logger.Info("HasTwoThirdsMajority",
			"Stake", txR.TxVoteSets[txHash].Stake(),
			"TotalStake", txR.TxVoteSets[txHash].TotalStake(),
			"ValidatorStake", val.VotingPower)

		select {
		case <-next.NextWaitChan():
			// see the start of the for loop for nil check
			next = next.Next()
		case <-txR.Quit():
			return
		}
	}
}

// ReactorMetrics sets the metrics
func ReactorMetrics(metrics *Metrics) ReactorOption {
	return func(txR *TxFlowReactor) { txR.metrics = metrics }
}
