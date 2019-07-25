package types

import (
	"testing"

	"github.com/Fantom-foundation/go-txflow/types"
	amino "github.com/tendermint/go-amino"
	"github.com/tendermint/tendermint/crypto/ed25519"
	cmn "github.com/tendermint/tendermint/libs/common"
	ttypes "github.com/tendermint/tendermint/types"
	tmtime "github.com/tendermint/tendermint/types/time"
)

func BenchmarkRoundStateDeepCopy(b *testing.B) {
	b.StopTimer()

	// Random validators
	nval, ntxs := 100, 100
	vset, _ := ttypes.RandValidatorSet(nval, 1)
	precommits := make([]*ttypes.CommitSig, nval)
	blockID := ttypes.BlockID{
		Hash: cmn.RandBytes(20),
		PartsHeader: ttypes.PartSetHeader{
			Hash: cmn.RandBytes(20),
		},
	}
	sig := make([]byte, ed25519.SignatureSize)
	for i := 0; i < nval; i++ {
		precommits[i] = (&ttypes.Vote{
			ValidatorAddress: ttypes.Address(cmn.RandBytes(20)),
			Timestamp:        tmtime.Now(),
			BlockID:          blockID,
			Signature:        sig,
		}).CommitSig()
	}
	txs := make([]ttypes.Tx, ntxs)
	for i := 0; i < ntxs; i++ {
		txs[i] = cmn.RandBytes(100)
	}
	// Random block
	block := &types.Block{
		Header: ttypes.Header{
			ChainID:         cmn.RandStr(12),
			Time:            tmtime.Now(),
			LastBlockID:     blockID,
			LastCommitHash:  cmn.RandBytes(20),
			DataHash:        cmn.RandBytes(20),
			ValidatorsHash:  cmn.RandBytes(20),
			ConsensusHash:   cmn.RandBytes(20),
			AppHash:         cmn.RandBytes(20),
			LastResultsHash: cmn.RandBytes(20),
			EvidenceHash:    cmn.RandBytes(20),
		},
		Data: types.Data{
			Txs: txs,
		},
		Evidence:   ttypes.EvidenceData{},
		LastCommit: ttypes.NewCommit(blockID, precommits),
	}
	parts := block.MakePartSet(4096)
	// Random Proposal
	proposal := &ttypes.Proposal{
		Timestamp: tmtime.Now(),
		BlockID:   blockID,
		Signature: sig,
	}
	// Random HeightVoteSet
	// TODO: hvs :=

	rs := &RoundState{
		StartTime:          tmtime.Now(),
		CommitTime:         tmtime.Now(),
		Validators:         vset,
		Proposal:           proposal,
		ProposalBlock:      block,
		ProposalBlockParts: parts,
		LockedBlock:        block,
		LockedBlockParts:   parts,
		ValidBlock:         block,
		ValidBlockParts:    parts,
		Votes:              nil, // TODO
		LastCommit:         nil, // TODO
		LastValidators:     vset,
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		amino.DeepCopy(rs)
	}
}
