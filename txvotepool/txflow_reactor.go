package txvotepool

import (
	"time"

	"github.com/Fantom-foundation/go-txflow/types"
	"github.com/tendermint/tendermint/libs/clist"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/p2p"
	ttypes "github.com/tendermint/tendermint/types"
)

// TxFlowVoteReactor handles consensus commits
type TxFlowVoteReactor struct {
	p2p.BaseReactor
	TxVotePool *TxVotePool

	ChainID    string
	Height     int64
	StartTime  time.Time
	Validators *ttypes.ValidatorSet
	TxVoteSets map[string]*types.TxVoteSet
}

// NewTxFlowVoteReactor returns a new MempoolReactor with the given config and mempool.
func NewTxFlowVoteReactor(
	txvotepool *TxVotePool,
	height int64,
	chainID string,
	validators *ttypes.ValidatorSet,
) *TxFlowVoteReactor {
	txFVR := &TxFlowVoteReactor{
		TxVotePool: txvotepool,
		Height:     height,
		ChainID:    chainID,
		Validators: validators,
		TxVoteSets: make(map[string]*types.TxVoteSet),
	}
	txFVR.BaseReactor = *p2p.NewBaseReactor("TxFlowVoteReactor", txFVR)
	return txFVR
}

// SetLogger sets the Logger on the reactor and the underlying Mempool.
func (txFVR *TxFlowVoteReactor) SetLogger(l log.Logger) {
	txFVR.Logger = l
	txFVR.TxVotePool.SetLogger(l)
}

// OnStart implements p2p.BaseReactor.
func (txFVR *TxFlowVoteReactor) OnStart() error {
	go txFVR.checkMaj23Routine()
	return nil
}

// GetChannels implements Reactor.
// It returns the list of channels for this reactor.
func (txFVR *TxFlowVoteReactor) GetChannels() []*p2p.ChannelDescriptor {
	return []*p2p.ChannelDescriptor{
		{
			ID:       TxVotePoolChannel,
			Priority: 5,
		},
	}
}

// Sign new mempool txs.
func (txFVR *TxFlowVoteReactor) checkMaj23Routine() {
	var next *clist.CElement
	for {
		// In case of both next.NextWaitChan() and peer.Quit() are variable at the same time
		if !txFVR.IsRunning() {
			return
		}
		// This happens because the CElement we were looking at got garbage
		// collected (removed). That is, .NextWait() returned nil. Go ahead and
		// start from the beginning.
		if next == nil {
			select {
			case <-txFVR.TxVotePool.TxsWaitChan(): // Wait until a tx is available
				if next = txFVR.TxVotePool.TxsFront(); next == nil {
					continue
				}
			case <-txFVR.Quit():
				return
			}
		}

		memTx := next.Value.(*mempoolTxVote)
		//Check if we have already seen this txHash
		txHash := string(memTx.tx.TxHash)
		if _, ok := txFVR.TxVoteSets[txHash]; !ok {
			voteSet := types.NewTxVoteSet(
				txFVR.ChainID,
				txFVR.Height,
				memTx.tx.TxHash,
				txFVR.Validators,
			)
			txFVR.TxVoteSets[txHash] = voteSet
		}

		txFVR.TxVoteSets[txHash].AddVote(&memTx.tx)

		_, val := txFVR.Validators.GetByAddress(memTx.tx.ValidatorAddress)

		txFVR.Logger.Info("Validator", "Address", memTx.tx.String())

		txFVR.Logger.Info("HasTwoThirdsMajority",
			"Stake", txFVR.TxVoteSets[txHash].Stake(),
			"TotalStake", txFVR.TxVoteSets[txHash].TotalStake(),
			"ValidatorStake", val.VotingPower)

		select {
		case <-next.NextWaitChan():
			// see the start of the for loop for nil check
			next = next.Next()
		case <-txFVR.Quit():
			return
		}
	}
}
