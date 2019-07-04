package types

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/tendermint/tendermint/crypto"
	cmn "github.com/tendermint/tendermint/libs/common"
)

const (
	// MaxVoteBytes is a maximum vote size (including amino overhead).
	MaxVoteBytes int64 = 223
)

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
	bz, err := cdc.MarshalBinaryLengthPrefixed(CanonicalizeVote(chainID, vote))
	if err != nil {
		panic(err)
	}
	return bz
}

func (vote *TxVote) Copy() *Vote {
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
		CanonicalTime(vote.Timestamp),
	)
}

func (vote *TxVote) Verify(chainID string, pubKey crypto.PubKey) error {
	if !bytes.Equal(pubKey.Address(), vote.ValidatorAddress) {
		return ErrVoteInvalidValidatorAddress
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
	if len(vote.Signature) > MaxSignatureSize {
		return fmt.Errorf("Signature is too big (max: %d)", MaxSignatureSize)
	}
	return nil
}
