package state

import (
	"github.com/Fantom-foundation/txflow/eventpool"
	"github.com/Fantom-foundation/txflow/types"
	abci "github.com/tendermint/tendermint/abci/types"
	ttypes "github.com/tendermint/tendermint/types"
)

//------------------------------------------------------
// blockchain services types
// NOTE: Interfaces used by RPC must be thread safe!
//------------------------------------------------------

//------------------------------------------------------
// mempool

// Eventpool defines the evenpool interface as used by the eventstate.
// Updates to the eventpool need to be synchronized with committing a block
// so apps can reset their transient state on Commit
type Eventpool interface {
	Lock()
	Unlock()

	Size() int
	CheckEvent(*types.EventBlock, func(*abci.Response)) error
	CheckEventWithInfo(*types.EventBlock, func(*abci.Response), eventpool.EventInfo) error
	ReapMaxBytesMaxGas(maxBytes, maxGas int64) *types.EventBlock
	HeadEventBlockIDs() []types.EventBlockID
	Update(int64, *[]types.EventBlockIDs) error
	Flush()
	FlushAppConn() error

	EventsAvailable() <-chan struct{}
	EnableEventsAvailable()
}

// MockEventpool is an empty implementation of a Eventpool, useful for testing.
type MockEventpool struct{}

var _ Eventpool = MockEventpool{}

func (MockEventpool) Lock()     {}
func (MockEventpool) Unlock()   {}
func (MockEventpool) Size() int { return 0 }
func (MockEventpool) CheckEvent(_ *types.EventBlock, _ func(*abci.Response)) error {
	return nil
}
func (MockEventpool) CheckEventWithInfo(_ *types.EventBlock, _ func(*abci.Response),
	_ eventpool.EventInfo) error {
	return nil
}
func (MockEventpool) ReapMaxBytesMaxGas(_, _ int64) *types.EventBlock { return &types.EventBlock{} }
func (MockEventpool) Update(
	_ int64,
	_ *types.EventBlock,
) error {
	return nil
}
func (MockEventpool) Flush()                           {}
func (MockEventpool) FlushAppConn() error              { return nil }
func (MockEventpool) EventsAvailable() <-chan struct{} { return make(chan struct{}) }
func (MockEventpool) EnableEventsAvailable()           {}

//------------------------------------------------------
// blockstore

// BlockStoreRPC is the block store interface used by the RPC.
type BlockStoreRPC interface {
	Height() int64

	LoadBlockMeta(height int64) *ttypes.BlockMeta
	LoadEventBlock(height int64) *types.EventBlock
	LoadBlockPart(height int64, index int) *ttypes.Part

	LoadBlockCommit(height int64) *ttypes.Commit
	LoadSeenCommit(height int64) *ttypes.Commit
}

// BlockStore defines the BlockStore interface used by the ConsensusState.
type BlockStore interface {
	BlockStoreRPC
	SaveBlock(block *types.EventBlock, blockParts *ttypes.PartSet, seenCommit *ttypes.Commit)
}

//-----------------------------------------------------------------------------------------------------
// evidence pool

// EvidencePool defines the EvidencePool interface used by the ConsensusState.
// Get/Set/Commit
type EvidencePool interface {
	PendingEvidence(int64) []ttypes.Evidence
	AddEvidence(ttypes.Evidence) error
	Update(*types.EventBlock, State)
	// IsCommitted indicates if this evidence was already marked committed in another block.
	IsCommitted(ttypes.Evidence) bool
}

// MockMempool is an empty implementation of a Mempool, useful for testing.
type MockEvidencePool struct{}

func (m MockEvidencePool) PendingEvidence(int64) []ttypes.Evidence { return nil }
func (m MockEvidencePool) AddEvidence(ttypes.Evidence) error       { return nil }
func (m MockEvidencePool) Update(*ttypes.Block, State)             {}
func (m MockEvidencePool) IsCommitted(ttypes.Evidence) bool        { return false }
