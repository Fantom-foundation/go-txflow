package mempool

import (
	"github.com/Fantom-foundation/go-txflow/types"
	cfg "github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/libs/clist"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/p2p"
)

// VoteReactor handles mempool tx signing
type VoteReactor struct {
	p2p.BaseReactor
	config  *cfg.MempoolConfig
	Mempool *Mempool
	privval types.PrivValidator
}

// NewVoteReactor returns a new MempoolReactor with the given config and mempool.
func NewVoteReactor(config *cfg.MempoolConfig, mempool *Mempool, privval types.PrivValidator) *VoteReactor {
	vR := &VoteReactor{
		config:  config,
		Mempool: mempool,
		privval: privval,
	}
	vR.BaseReactor = *p2p.NewBaseReactor("VoteReactor", vR)
	return vR
}

// SetLogger sets the Logger on the reactor and the underlying Mempool.
func (vR *VoteReactor) SetLogger(l log.Logger) {
	vR.Logger = l
	vR.Mempool.SetLogger(l)
}

// OnStart implements p2p.BaseReactor.
func (vR *VoteReactor) OnStart() error {
	go vR.signTxRoutine()
	return nil
}

// GetChannels implements Reactor.
// It returns the list of channels for this reactor.
func (vR *VoteReactor) GetChannels() []*p2p.ChannelDescriptor {
	return []*p2p.ChannelDescriptor{
		{
			ID:       MempoolChannel,
			Priority: 5,
		},
	}
}

// Sign new mempool txs.
func (vR *VoteReactor) signTxRoutine() {
	var next *clist.CElement
	for {
		// In case of both next.NextWaitChan() and peer.Quit() are variable at the same time
		if !vR.IsRunning() {
			return
		}
		// This happens because the CElement we were looking at got garbage
		// collected (removed). That is, .NextWait() returned nil. Go ahead and
		// start from the beginning.
		if next == nil {
			select {
			case <-vR.Mempool.TxsWaitChan(): // Wait until a tx is available
				if next = vR.Mempool.TxsFront(); next == nil {
					continue
				}
			case <-vR.Quit():
				return
			}
		}

		memTx := next.Value.(*mempoolTx)

		//Sign this transaction with private validator and save TxVote in TxVotePool
		_ = vR.privval.SignTxVote("", &memTx.tx)

		select {
		case <-next.NextWaitChan():
			// see the start of the for loop for nil check
			next = next.Next()
		case <-vR.Quit():
			return
		}
	}
}
