package state

import (
	"github.com/Fantom-foundation/go-txflow/types"
	ttypes "github.com/tendermint/tendermint/types"
)

//------------------------------------------------------
// blockchain services types
// NOTE: Interfaces used by RPC must be thread safe!
//------------------------------------------------------

//------------------------------------------------------
// blockstore

// BlockStoreRPC is the block store interface used by the RPC.
type BlockStoreRPC interface {
	Height() int64

	LoadBlockMeta(height int64) *ttypes.BlockMeta
	LoadBlock(height int64) *types.Block
	LoadBlockPart(height int64, index int) *ttypes.Part

	LoadBlockCommit(height int64) *ttypes.Commit
	LoadSeenCommit(height int64) *ttypes.Commit
}

// BlockStore defines the BlockStore interface used by the ConsensusState.
type BlockStore interface {
	BlockStoreRPC
	SaveBlock(block *types.Block, blockParts *ttypes.PartSet, seenCommit *ttypes.Commit)
}

//-----------------------------------------------------------------------------------------------------
// evidence pool

// EvidencePool defines the EvidencePool interface used by the ConsensusState.
// Get/Set/Commit
type EvidencePool interface {
	PendingEvidence(int64) []ttypes.Evidence
	AddEvidence(ttypes.Evidence) error
	Update(*types.Block, State)
	// IsCommitted indicates if this evidence was already marked committed in another block.
	IsCommitted(ttypes.Evidence) bool
}

// MockEvidencePool is an empty implementation of EvidencePool, useful for testing.
type MockEvidencePool struct{}

func (m MockEvidencePool) PendingEvidence(int64) []ttypes.Evidence { return nil }
func (m MockEvidencePool) AddEvidence(ttypes.Evidence) error       { return nil }
func (m MockEvidencePool) Update(*ttypes.Block, State)             {}
func (m MockEvidencePool) IsCommitted(ttypes.Evidence) bool        { return false }
