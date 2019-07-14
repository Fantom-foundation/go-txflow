package txflow

import (
	"sync"
	"time"

	"github.com/Fantom-foundation/go-txflow/txflowstate"
	"github.com/Fantom-foundation/go-txflow/txvotepool"
	"github.com/Fantom-foundation/go-txflow/types"
	"github.com/tendermint/tendermint/consensus"
	"github.com/tendermint/tendermint/evidence"
	"github.com/tendermint/tendermint/libs/clist"
	cmn "github.com/tendermint/tendermint/libs/common"
	sm "github.com/tendermint/tendermint/state"
	ttypes "github.com/tendermint/tendermint/types"
)

//-----------------------------------------------------------------------------

// TxFlowReactor defines a reactor for the consensus service.
type TxFlowReactor struct {
	cmn.BaseService

	StartTime  time.Time
	TxVoteSets map[string]*types.TxVoteSet

	txV *txvotepool.TxVotePool

	// store txs and commits
	txStore txflowstate.TxStore

	// execute finalized txs
	txExec *txflowstate.TxExecutor

	evpool *evidence.EvidencePool

	// internal state
	mtx sync.RWMutex

	// This is the blockchain state, which essentially becomes a BFT timer
	state *sm.State // State until height-1.

	// Broadcast new committed tx events to the application layer
	eventBus *ttypes.EventBus

	metrics *Metrics
}

type ReactorOption func(*TxFlowReactor)

// NewTxFlowReactor returns a new TxFlowReactor with the given
// txflowState.
func NewTxFlowReactor(
	state *sm.State,
	txV *txvotepool.TxVotePool,
	txExec *txflowstate.TxExecutor,
	txStore txflowstate.TxStore,
	evpool *evidence.EvidencePool,
	options ...ReactorOption,
) *TxFlowReactor {
	txR := &TxFlowReactor{
		txV:     txV,
		txExec:  txExec,
		txStore: txStore,
		state:   state,
		evpool:  evpool,
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
	go txR.checkMaj23Routine()

	return nil
}

// OnStop implements BaseService by unsubscribing from events and stopping
// state.
func (txR *TxFlowReactor) OnStop() {

}

// SetEventBus sets event bus.
func (txR *TxFlowReactor) SetEventBus(b *ttypes.EventBus) {
	txR.eventBus = b
}

// String returns a string representation of the ConsensusReactor.
// NOTE: For now, it is just a hard-coded string to avoid accessing unprotected shared variables.
// TODO: improve!
func (txR *TxFlowReactor) String() string {
	// better not to access shared variables
	return "TxFlowReactor" // conR.StringIndented("")
}

// GetValidators returns a copy of the current validators.
func (txR *TxFlowReactor) GetValidators() (int64, []*ttypes.Validator) {
	txR.mtx.RLock()
	defer txR.mtx.RUnlock()
	return txR.state.LastBlockHeight, txR.state.Validators.Copy().Validators
}

// LoadCommit loads the commit for a given hash.
func (txR *TxFlowReactor) LoadCommit(txHash cmn.HexBytes) *types.Commit {
	txR.mtx.RLock()
	defer txR.mtx.RUnlock()
	return txR.txStore.LoadTxCommit(txHash)
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
			case <-txR.txV.TxsWaitChan(): // Wait until a tx is available
				if next = txR.txV.TxsFront(); next == nil {
					continue
				}
			case <-txR.Quit():
				return
			}
		}

		memTx := next.Value.(*txvotepool.MempoolTxVote)
		added, err := txR.TryAddVote(&memTx.Tx)
		if added {
			//ts.statsMsgQueue <- mi
		}

		if err == consensus.ErrAddingVote {
			// TODO: punish peer
			// We probably don't want to stop the peer here. The vote does not
			// necessarily comes from a malicious peer but can be just broadcasted by
			// a typical peer.
			// https://github.com/tendermint/tendermint/issues/1281
		}

		select {
		case <-next.NextWaitChan():
			// see the start of the for loop for nil check
			next = next.Next()
		case <-txR.Quit():
			return
		}
	}
}

// TryAddVote Attempt to add the vote. if its a duplicate signature, dupeout the validator
func (txR *TxFlowReactor) TryAddVote(vote *types.TxVote) (bool, error) {
	added, err := txR.addVote(vote)
	if err != nil {
		// If the vote height is off, we'll just ignore it,
		// But if it's a conflicting sig, add it to the cs.evpool.
		// If it's otherwise invalid, punish peer.
		if err == consensus.ErrVoteHeightMismatch {
			return added, err
		} else if voteErr, ok := err.(*ttypes.ErrVoteConflictingVotes); ok {
			txR.evpool.AddEvidence(voteErr.DuplicateVoteEvidence)
			return added, err
		} else {
			// Probably an invalid signature / Bad peer.
			// Seems this can also err sometimes with "Unexpected step" - perhaps not from a bad peer ?
			txR.Logger.Error("Error attempting to add vote", "err", err)
			return added, consensus.ErrAddingVote
		}
	}
	return added, nil
}

//-----------------------------------------------------------------------------

func (txR *TxFlowReactor) addVote(vote *types.TxVote) (added bool, err error) {
	txR.Logger.Debug("addVote", "voteHeight", vote.Height, "valAddress", vote.ValidatorAddress)

	txHash := string(vote.TxHash)
	if _, ok := txR.TxVoteSets[txHash]; !ok {
		voteSet := types.NewTxVoteSet(
			txR.state.ChainID,
			txR.state.LastBlockHeight,
			vote.TxHash,
			txR.state.Validators,
		)
		txR.TxVoteSets[txHash] = voteSet
	}

	added, err = txR.TxVoteSets[txHash].AddVote(vote)
	if !added {
		// Either duplicate, or error upon cs.Votes.AddByIndex()
		return
	}
	if txR.TxVoteSets[txHash].HasTwoThirdsMajority() {
		//enter commit
	}
	return
}

// ReactorMetrics sets the metrics
func ReactorMetrics(metrics *Metrics) ReactorOption {
	return func(txR *TxFlowReactor) { txR.metrics = metrics }
}
