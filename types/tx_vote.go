package types

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/tendermint/tendermint/crypto"
	cmn "github.com/tendermint/tendermint/libs/common"
	ttypes "github.com/tendermint/tendermint/types"
)

const (
	// MaxVoteBytes is a maximum vote size (including amino overhead).
	MaxVoteBytes int64 = 223
)

/*func NewConflictingVoteError(val *types.Validator, voteA, voteB *TxVote) *ErrVoteConflictingVotes {
	return &types.ErrVoteConflictingVotes{
		&DuplicateVoteEvidence{
			PubKey: val.PubKey,
			VoteA:  voteA,
			VoteB:  voteB,
		},
	}
}*/

var (
	ErrVoteInvalidSignature          = errors.New("Invalid signature")
	ErrVoteInvalidTxHash             = errors.New("Invalid tx hash")
	ErrVoteNonDeterministicSignature = errors.New("Non-deterministic signature")
	ErrVoteNil                       = errors.New("Nil vote")
)

// TxVote represents a commit vote from validators for consensus.
type TxVote struct {
	Height           int64          `json:"height"`
	TxHash           cmn.HexBytes   `json:"tx_hash"` // zero if vote is nil.
	Timestamp        time.Time      `json:"timestamp"`
	ValidatorAddress crypto.Address `json:"validator_address"`
	Signature        []byte         `json:"signature"`
}

func NewTxVote(height int64,
	txHash cmn.HexBytes,
) TxVote {
	txVote := TxVote{
		Height:           height,
		TxHash:           txHash,
		Timestamp:        time.Now(),
		ValidatorAddress: nil,
		Signature:        nil,
	}
	return txVote
}

// CommitSig converts the Vote to a CommitSig.
// If the Vote is nil, the CommitSig will be nil.
func (vote *TxVote) CommitSig() *CommitSig {
	if vote == nil {
		return nil
	}
	cs := CommitSig(*vote)
	return &cs
}

func (vote *TxVote) SignBytes(chainID string) []byte {
	bz, err := cdc.MarshalBinaryLengthPrefixed(CanonicalizeTxVote(chainID, vote))
	if err != nil {
		panic(err)
	}
	return bz
}

func (vote *TxVote) Copy() *TxVote {
	voteCopy := *vote
	return &voteCopy
}

func (vote *TxVote) String() string {
	if vote == nil {
		return "nil-Vote"
	}

	return fmt.Sprintf("TxVote{%X (%v) %X %X @ %s}",
		cmn.Fingerprint(vote.ValidatorAddress),
		vote.Height,
		cmn.Fingerprint(vote.TxHash),
		cmn.Fingerprint(vote.Signature),
		ttypes.CanonicalTime(vote.Timestamp),
	)
}

func (vote *TxVote) Verify(chainID string, pubKey crypto.PubKey) error {
	if !bytes.Equal(pubKey.Address(), vote.ValidatorAddress) {
		return ttypes.ErrVoteInvalidValidatorAddress
	}

	if !pubKey.VerifyBytes(vote.SignBytes(chainID), vote.Signature) {
		return ErrVoteInvalidSignature
	}
	return nil
}

// ValidateBasic performs basic validation.
func (vote *TxVote) ValidateBasic() error {
	if vote.Height < 0 {
		return errors.New("Negative Height")
	}

	// NOTE: Timestamp validation is subtle and handled elsewhere.
	if len(vote.ValidatorAddress) != crypto.AddressSize {
		return fmt.Errorf("Expected ValidatorAddress size to be %d bytes, got %d bytes",
			crypto.AddressSize,
			len(vote.ValidatorAddress),
		)
	}
	if len(vote.Signature) == 0 {
		return errors.New("Signature is missing")
	}
	if len(vote.Signature) > ttypes.MaxSignatureSize {
		return fmt.Errorf("Signature is too big (max: %d)", ttypes.MaxSignatureSize)
	}
	return nil
}

// Size returns size of the block in bytes.
func (vote *TxVote) Size() int {
	bz, err := cdc.MarshalBinaryBare(vote)
	if err != nil {
		return 0
	}
	return len(bz)
}

//-------------------------------------

// CommitSig is a vote included in a Commit.
// For now, it is identical to a vote,
// but in the future it will contain fewer fields
// to eliminate the redundancy in commits.
// See https://github.com/tendermint/tendermint/issues/1648.
type CommitSig TxVote

// String returns the underlying Vote.String()
func (cs *CommitSig) String() string {
	return cs.toVote().String()
}

// toVote converts the CommitSig to a vote.
// TODO: deprecate for #1648. Converting to Vote will require
// access to ValidatorSet.
func (cs *CommitSig) toVote() *TxVote {
	if cs == nil {
		return nil
	}
	v := TxVote(*cs)
	return &v
}

type CanonicalTxVote struct {
	Height    int64 `binary:"fixed64"`
	TxHash    cmn.HexBytes
	Timestamp time.Time
	ChainID   string
}

func CanonicalizeTxVote(chainID string, vote *TxVote) CanonicalTxVote {
	return CanonicalTxVote{
		Height:    vote.Height,
		TxHash:    vote.TxHash,
		Timestamp: vote.Timestamp,
		ChainID:   chainID,
	}
}
