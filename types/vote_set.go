package types

import (
	"bytes"
	"crypto/sha256"
	"sync"

	"github.com/pkg/errors"

	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/types"
)

/*
	VoteSet helps collect signatures from validators for each tx

	NOTE: Assumes that the sum total of voting power does not exceed MaxUInt64.
*/
type TxVoteSet struct {
	chainID string
	height  int64
	valSet  *types.ValidatorSet

	TxHash string
	TxKey  [sha256.Size]byte

	mtx   sync.Mutex
	votes map[string]*TxVote // Primary votes to share
	sum   int64              // Sum of voting power for seen votes, discounting conflicts
	maj23 bool
}

// NewTxVoteSet Constructs a new VoteSet struct used to accumulate votes for given height/round.
func NewTxVoteSet(
	chainID string,
	height int64,
	txHash string,
	txKey [sha256.Size]byte,
	valSet *types.ValidatorSet,
) *TxVoteSet {
	return &TxVoteSet{
		chainID: chainID,
		height:  height,
		valSet:  valSet,
		TxHash:  txHash,
		TxKey:   txKey,
		votes:   make(map[string]*TxVote, valSet.Size()),
		sum:     0,
		maj23:   false,
	}
}

func (voteSet *TxVoteSet) ChainID() string {
	return voteSet.chainID
}

func (voteSet *TxVoteSet) Height() int64 {
	if voteSet == nil {
		return 0
	}
	return voteSet.height
}

func (voteSet *TxVoteSet) Size() int {
	if voteSet == nil {
		return 0
	}
	return voteSet.valSet.Size()
}

// AddVote Returns added=true if vote is valid and new.
func (voteSet *TxVoteSet) AddVote(vote *TxVote) (added bool, err error) {
	if voteSet == nil {
		panic("AddVote() on nil VoteSet")
	}
	voteSet.mtx.Lock()
	defer voteSet.mtx.Unlock()

	return voteSet.addVote(vote)
}

// addVote NOTE: Validates as much as possible before attempting to verify the signature.
func (voteSet *TxVoteSet) addVote(vote *TxVote) (added bool, err error) {
	if vote == nil {
		return false, ErrVoteNil
	}

	if len(vote.ValidatorAddress) == 0 {
		return false, errors.Wrap(types.ErrVoteInvalidValidatorAddress, "Empty address")
	}

	// Ensure that signer is a validator.
	_, val := voteSet.valSet.GetByAddress(vote.ValidatorAddress)
	if val == nil {
		return false, errors.Wrapf(types.ErrVoteInvalidValidatorIndex,
			"Cannot find validator %X in valSet of size %d", vote.ValidatorAddress, voteSet.valSet.Size())
	}

	// If we already know of this vote, return false.
	if existing, ok := voteSet.getVote(vote.ValidatorAddress); ok {
		if bytes.Equal(existing.Signature, vote.Signature) {
			return false, nil // duplicate
		}
		return false, errors.Wrapf(ErrVoteNonDeterministicSignature, "Existing vote: %v; New vote: %v", existing, vote)
	}

	// Check signature.
	if err := vote.Verify(voteSet.chainID, val.PubKey); err != nil {
		return false, errors.Wrapf(err, "Failed to verify vote with ChainID %s and PubKey %s", voteSet.chainID, val.PubKey)
	}

	// Add vote and get conflicting vote if any.
	added, conflicting := voteSet.addVerifiedVote(vote, val.VotingPower)
	if conflicting != nil {
		//This is evidence, we need to provide this TODO
		return added, nil //NewConflictingVoteError(val, conflicting, vote)
	}
	if !added {
		panic("Expected to add non-conflicting vote")
	}
	return added, nil
}

// Returns (vote, true) if vote exists for valAddress and txKey.
func (voteSet *TxVoteSet) getVote(valAddress cmn.HexBytes) (vote *TxVote, ok bool) {
	if existing := voteSet.votes[valAddress.String()]; existing != nil {
		return existing, true
	}
	return nil, false
}

// Assumes signature is valid.
// If conflicting vote exists, returns it.
func (voteSet *TxVoteSet) addVerifiedVote(vote *TxVote, votingPower int64) (added bool, conflicting *TxVote) {
	// Already exists in voteSet.votes?
	if existing := voteSet.votes[vote.ValidatorAddress.String()]; existing != nil {
		if existing.TxHash == vote.TxHash {
			panic("addVerifiedVote does not expect duplicate votes")
		} else {
			conflicting = existing
		}
		// Otherwise don't add it to voteSet.votes
	} else {
		// Add to voteSet.votes and incr .sum
		voteSet.votes[vote.ValidatorAddress.String()] = vote
		voteSet.sum += votingPower
	}

	quorum := voteSet.valSet.TotalVotingPower()*2/3 + 1

	// If we just crossed the quorum threshold and have 2/3 majority...
	if quorum <= voteSet.sum {
		voteSet.maj23 = true
	}

	return true, conflicting
}

// NOTE: if validator has conflicting votes, returns "canonical" vote
func (voteSet *TxVoteSet) GetByAddress(address cmn.HexBytes) *TxVote {
	if voteSet == nil {
		return nil
	}
	voteSet.mtx.Lock()
	defer voteSet.mtx.Unlock()
	return voteSet.votes[address.String()]
}

func (voteSet *TxVoteSet) HasTwoThirdsMajority() bool {
	if voteSet == nil {
		return false
	}
	voteSet.mtx.Lock()
	defer voteSet.mtx.Unlock()
	return voteSet.maj23
}

func (voteSet *TxVoteSet) IsCommit() bool {
	if voteSet == nil {
		return false
	}
	voteSet.mtx.Lock()
	defer voteSet.mtx.Unlock()
	return voteSet.maj23
}

func (voteSet *TxVoteSet) HasTwoThirdsAny() bool {
	if voteSet == nil {
		return false
	}
	voteSet.mtx.Lock()
	defer voteSet.mtx.Unlock()
	return voteSet.sum > voteSet.valSet.TotalVotingPower()*2/3
}

func (voteSet *TxVoteSet) Stake() int64 {
	if voteSet == nil {
		return -1
	}
	voteSet.mtx.Lock()
	defer voteSet.mtx.Unlock()
	return voteSet.sum
}

func (voteSet *TxVoteSet) TotalStake() int64 {
	if voteSet == nil {
		return -1
	}
	voteSet.mtx.Lock()
	defer voteSet.mtx.Unlock()
	return voteSet.valSet.TotalVotingPower() * 2 / 3
}

func (voteSet *TxVoteSet) HasAll() bool {
	voteSet.mtx.Lock()
	defer voteSet.mtx.Unlock()
	return voteSet.sum == voteSet.valSet.TotalVotingPower()
}

// return the power voted, the total, and the fraction
func (voteSet *TxVoteSet) sumTotalFrac() (int64, int64, float64) {
	voted, total := voteSet.sum, voteSet.valSet.TotalVotingPower()
	fracVoted := float64(voted) / float64(total)
	return voted, total, fracVoted
}

//--------------------------------------------------------------------------------
// Commit

// MakeCommit constructs a Commit from the VoteSet.
// Panics if the vote type is not PrecommitType or if
// there's no +2/3 votes for a single block.
func (voteSet *TxVoteSet) MakeCommit() *Commit {
	voteSet.mtx.Lock()
	defer voteSet.mtx.Unlock()

	// Make sure we have a 2/3 majority
	if !voteSet.maj23 {
		panic("Cannot MakeCommit() unless a blockhash has +2/3")
	}

	// For every validator, get the precommit
	commitSigs := make([]*CommitSig, len(voteSet.votes))
	i := 0
	for _, v := range voteSet.votes {
		commitSigs[i] = v.CommitSig()
		i = i + 1
	}
	return NewCommit(voteSet.TxHash, commitSigs)
}

//-------------------------------------

// Commit contains the evidence that a block was committed by a set of validators.
// NOTE: Commit is empty for height 1, but never nil.
type Commit struct {
	// NOTE: The Precommits are in order of address to preserve the bonded ValidatorSet order.
	// Any peer with a block can gossip precommits by index with a peer without recalculating the
	// active ValidatorSet.
	TxHash  string       `json:"tx_hash"`
	Commits []*CommitSig `json:"commits"`

	// memoized in first call to corresponding method
	// NOTE: can't memoize in constructor because constructor
	// isn't used for unmarshaling
	height int64
	hash   cmn.HexBytes
}

// NewCommit returns a new Commit with the given blockID and precommits.
// TODO: memoize ValidatorSet in constructor so votes can be easily reconstructed
// from CommitSig after #1648.
func NewCommit(txHash string, commits []*CommitSig) *Commit {
	return &Commit{
		TxHash:  txHash,
		Commits: commits,
	}
}

//--------------------------------------------------------------------------------

// VoteSetReader Common interface between *consensus.VoteSet and types.Commit
type VoteSetReader interface {
	Height() int64
	Size() int
	getByAddress(int) *TxVote
	IsCommit() bool
}
