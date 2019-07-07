package txflowstate

import (
	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/mempool"
	"github.com/tendermint/tendermint/types"
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
	CheckTx(types.Tx, func(*abci.Response)) error
	CheckTxWithInfo(types.Tx, func(*abci.Response), mempool.TxInfo) error
	ReapMaxBytesMaxGas(maxBytes, maxGas int64) types.Txs
	Update(int64, types.Txs, mempool.PreCheckFunc, mempool.PostCheckFunc) error
	UpdateTx(*types.Tx) error
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
func (MockMempool) CheckTx(_ types.Tx, _ func(*abci.Response)) error {
	return nil
}
func (MockMempool) CheckTxWithInfo(_ types.Tx, _ func(*abci.Response),
	_ mempool.TxInfo) error {
	return nil
}
func (MockMempool) ReapMaxBytesMaxGas(_, _ int64) types.Txs { return types.Txs{} }
func (MockMempool) Update(
	_ int64,
	_ types.Txs,
	_ mempool.PreCheckFunc,
	_ mempool.PostCheckFunc,
) error {
	return nil
}
func (MockMempool) UpdateTx(_ *types.Tx) error {
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

	/*LoadTxMeta(height int64) *types.BlockMeta
	LoadBlock(height int64) *types.Block
	LoadBlockPart(height int64, index int) *types.Part

	LoadBlockCommit(height int64) *types.Commit
	LoadSeenCommit(height int64) *types.Commit*/
}

// TxStore defines the BlockStore interface used by the ConsensusState.
type TxStore interface {
	TxStoreRPC
	SaveTx(tx *types.Tx, seenCommit *types.Commit)
}
