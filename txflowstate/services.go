package txflowstate

import (
	"github.com/Fantom-foundation/go-txflow/types"
	abci "github.com/tendermint/tendermint/abci/types"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/mempool"
	ttypes "github.com/tendermint/tendermint/types"
)

//------------------------------------------------------
// blockchain services types
// NOTE: Interfaces used by RPC must be thread safe!
//------------------------------------------------------

//------------------------------------------------------
// mempool

// Mempool defines the mempool interface as used by the ConsensusState.
// Updates to the mempool need to be synchronized with committing a block
// so apps can reset their transient state on Commit
type Mempool interface {
	Lock()
	Unlock()

	Size() int
	CheckTx(ttypes.Tx, func(*abci.Response)) error
	CheckTxWithInfo(ttypes.Tx, func(*abci.Response), mempool.TxInfo) error
	ReapMaxBytesMaxGas(maxBytes, maxGas int64) ttypes.Txs
	Update(int64, ttypes.Txs, mempool.PreCheckFunc, mempool.PostCheckFunc) error
	UpdateTx(*ttypes.Tx) error
	Flush()
	FlushAppConn() error

	TxsAvailable() <-chan struct{}
	EnableTxsAvailable()
}

// MockMempool is an empty implementation of a Mempool, useful for testing.
type MockMempool struct{}

var _ Mempool = MockMempool{}

func (MockMempool) Lock()     {}
func (MockMempool) Unlock()   {}
func (MockMempool) Size() int { return 0 }
func (MockMempool) CheckTx(_ ttypes.Tx, _ func(*abci.Response)) error {
	return nil
}
func (MockMempool) CheckTxWithInfo(_ ttypes.Tx, _ func(*abci.Response),
	_ mempool.TxInfo) error {
	return nil
}
func (MockMempool) ReapMaxBytesMaxGas(_, _ int64) ttypes.Txs { return ttypes.Txs{} }
func (MockMempool) Update(
	_ int64,
	_ ttypes.Txs,
	_ mempool.PreCheckFunc,
	_ mempool.PostCheckFunc,
) error {
	return nil
}
func (MockMempool) UpdateTx(_ *ttypes.Tx) error {
	return nil
}
func (MockMempool) Flush()                        {}
func (MockMempool) FlushAppConn() error           { return nil }
func (MockMempool) TxsAvailable() <-chan struct{} { return make(chan struct{}) }
func (MockMempool) EnableTxsAvailable()           {}

//------------------------------------------------------
// txstore

// TxStoreRPC is the tx store interface used by the RPC.
type TxStoreRPC interface {
	Height() int64

	LoadTx(txHash cmn.HexBytes) *types.TxVoteSet
	LoadTxCommit(txHash cmn.HexBytes) *types.Commit
}

// TxStore defines the BlockStore interface used by the ConsensusState.
type TxStore interface {
	TxStoreRPC
	SaveTx(tx *types.TxVoteSet)
}
