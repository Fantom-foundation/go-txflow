package txvotepool

import (
	"github.com/tendermint/tendermint/libs/clist"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/p2p"
)

// TxFlowVoteReactor handles consensus commits
type TxFlowVoteReactor struct {
	p2p.BaseReactor
	TxVotePool *TxVotePool
}

// NewTxFlowVoteReactor returns a new MempoolReactor with the given config and mempool.
func NewTxFlowVoteReactor(mempool *TxVotePool) *TxFlowVoteReactor {
	txFVR := &TxFlowVoteReactor{
		TxVotePool: mempool,
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
			ID:       TxpoolChannel,
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

		//memTx := next.Value.(*mempoolTxVote)
		//memTx.tx.TxHash
		//Get TxHash
		//Add Vote for TxHash
		//Check all Votes already received for TxHash
		//Compare with Validator Set
		//If stake > 2n/3 process via commitTx
		//Remove tx from mempool
		//Remove tx from votepool
		//Add tx to txstore

		//Check Vote for majority 2n/3

		select {
		case <-next.NextWaitChan():
			// see the start of the for loop for nil check
			next = next.Next()
		case <-txFVR.Quit():
			return
		}
	}
}
