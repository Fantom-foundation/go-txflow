package types

import (
	"sort"
	"testing"

	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/types"
	tmtime "github.com/tendermint/tendermint/types/time"
)

//----------------------------------------
// for testing

// RandValidatorSet returns a randomized validator set, useful for testing.
// NOTE: PrivValidator are in order.
// UNSTABLE
func RandValidatorSet(numValidators int, votingPower int64) (*types.ValidatorSet, []PrivValidator) {
	valz := make([]*types.Validator, numValidators)
	privValidators := make([]PrivValidator, numValidators)
	for i := 0; i < numValidators; i++ {
		val, privValidator := RandValidator(false, votingPower)
		valz[i] = val
		privValidators[i] = privValidator
	}
	vals := types.NewValidatorSet(valz)
	sort.Sort(PrivValidatorsByAddress(privValidators))
	return vals, privValidators
}

//----------------------------------------
// RandValidator

// RandValidator returns a randomized validator, useful for testing.
// UNSTABLE
func RandValidator(randPower bool, minPower int64) (*types.Validator, PrivValidator) {
	privVal := NewMockPV()
	votePower := minPower
	if randPower {
		votePower += int64(cmn.RandUint32())
	}
	pubKey := privVal.GetPubKey()
	val := types.NewValidator(pubKey, votePower)
	return val, privVal
}

// NOTE: privValidators are in order
func randTxVoteSet(height int64, numValidators int, votingPower int64) (*TxVoteSet, *types.ValidatorSet, []PrivValidator) {
	valSet, privValidators := RandValidatorSet(numValidators, votingPower)
	tx := types.Tx{}
	return NewTxVoteSet("test_chain_id", height, tx.Hash(), valSet), valSet, privValidators
}

// Convenience: Return new vote with different validator address/index
func withValidator(vote *TxVote, addr []byte, idx int) *TxVote {
	vote = vote.Copy()
	vote.ValidatorAddress = addr
	return vote
}

// Convenience: Return new vote with different height
func withHeight(vote *TxVote, height int64) *TxVote {
	vote = vote.Copy()
	vote.Height = height
	return vote
}

// Convenience: Return new vote with different blockHash
func withTxHash(vote *TxVote, txHash cmn.HexBytes) *TxVote {
	vote = vote.Copy()
	vote.TxHash = txHash
	return vote
}

func signAddVote(privVal PrivValidator, vote *TxVote, voteSet *TxVoteSet) (signed bool, err error) {
	err = privVal.SignTxVote(voteSet.ChainID(), vote)
	if err != nil {
		return false, err
	}
	return voteSet.AddVote(vote)
}

func TestAddVote(t *testing.T) {
	height := int64(1)
	voteSet, _, privValidators := randTxVoteSet(height, 10, 1)
	val0 := privValidators[0]

	// t.Logf(">> %v", voteSet)

	val0Addr := val0.GetPubKey().Address()
	if voteSet.GetByAddress(val0Addr) != nil {
		t.Errorf("Expected GetByAddress(val0.Address) to be nil")
	}
	if voteSet.HasTwoThirdsMajority() {
		t.Errorf("There should be no 2/3 majority")
	}

	tx := types.Tx("0x1")

	vote := &TxVote{
		ValidatorAddress: val0Addr,
		Height:           height,
		Timestamp:        tmtime.Now(),
		TxHash:           tx.Hash(),
	}
	_, err := signAddVote(val0, vote, voteSet)
	if err != nil {
		t.Error(err)
	}

	if voteSet.GetByAddress(val0Addr) == nil {
		t.Errorf("Expected GetByAddress(val0.Address) to be present")
	}
}

func Test2_3Majority(t *testing.T) {
	height := int64(1)
	voteSet, _, privValidators := randTxVoteSet(height, 10, 1)

	voteProto := &TxVote{
		ValidatorAddress: nil, // NOTE: must fill in
		Height:           height,
		Timestamp:        tmtime.Now(),
		TxHash:           types.Tx("0x1").Hash(),
	}
	// 6 out of 10 voted for nil.
	for i := 0; i < 6; i++ {
		addr := privValidators[i].GetPubKey().Address()
		vote := withValidator(voteProto, addr, i)
		_, err := signAddVote(privValidators[i], vote, voteSet)
		if err != nil {
			t.Error(err)
		}
	}
	if voteSet.HasTwoThirdsMajority() {
		t.Errorf("There should be no 2/3 majority")
	}

	// 7th validator voted for some txHash
	{
		addr := privValidators[6].GetPubKey().Address()
		vote := withValidator(voteProto, addr, 6)
		_, err := signAddVote(privValidators[6], withTxHash(vote, cmn.RandBytes(32)), voteSet)
		if err != nil {
			t.Error(err)
		}
		if voteSet.HasTwoThirdsMajority() {
			t.Errorf("There should be no 2/3 majority")
		}
	}

	// 8th validator voted for nil.
	{
		addr := privValidators[7].GetPubKey().Address()
		vote := withValidator(voteProto, addr, 7)
		_, err := signAddVote(privValidators[7], vote, voteSet)
		if err != nil {
			t.Error(err)
		}
		if !voteSet.HasTwoThirdsMajority() {
			t.Errorf("There should be 2/3 majority for nil")
		}
	}
}

func Test2_3MajorityRedux(t *testing.T) {
	height := int64(1)
	voteSet, _, privValidators := randTxVoteSet(height, 100, 1)

	voteProto := &TxVote{
		ValidatorAddress: nil, // NOTE: must fill in
		Height:           height,
		Timestamp:        tmtime.Now(),
		TxHash:           types.Tx("0x1").Hash(),
	}

	// 66 out of 100 voted for nil.
	for i := 0; i < 66; i++ {
		addr := privValidators[i].GetPubKey().Address()
		vote := withValidator(voteProto, addr, i)
		_, err := signAddVote(privValidators[i], vote, voteSet)
		if err != nil {
			t.Error(err)
		}
	}
	if voteSet.HasTwoThirdsMajority() {
		t.Errorf("There should be no 2/3 majority")
	}

	// 67th validator voted for nil
	{
		adrr := privValidators[66].GetPubKey().Address()
		vote := withValidator(voteProto, adrr, 66)
		_, err := signAddVote(privValidators[66], withTxHash(vote, nil), voteSet)
		if err != nil {
			t.Error(err)
		}
		if voteSet.HasTwoThirdsMajority() {
			t.Errorf("There should be no 2/3 majority: last vote added was nil")
		}
	}

	// 70th validator voted for different BlockHash
	{
		addr := privValidators[69].GetPubKey().Address()
		vote := withValidator(voteProto, addr, 69)
		_, err := signAddVote(privValidators[69], withTxHash(vote, cmn.RandBytes(32)), voteSet)
		if err != nil {
			t.Error(err)
		}
		if voteSet.HasTwoThirdsMajority() {
			t.Errorf("There should be no 2/3 majority: last vote added had different BlockHash")
		}
	}

	// 71st validator voted for the right BlockHash & BlockPartsHeader
	{
		addr := privValidators[70].GetPubKey().Address()
		vote := withValidator(voteProto, addr, 70)
		_, err := signAddVote(privValidators[70], vote, voteSet)
		if err != nil {
			t.Error(err)
		}
		if !voteSet.HasTwoThirdsMajority() {
			t.Errorf("There should be 2/3 majority")
		}
	}
}

func TestBadVotes(t *testing.T) {
	height := int64(1)
	voteSet, _, privValidators := randTxVoteSet(height, 10, 1)

	voteProto := &TxVote{
		ValidatorAddress: nil,
		Height:           height,
		Timestamp:        tmtime.Now(),
		TxHash:           types.Tx("0x1").Hash(),
	}

	// val0 votes for nil.
	{
		addr := privValidators[0].GetPubKey().Address()
		vote := withValidator(voteProto, addr, 0)
		added, err := signAddVote(privValidators[0], vote, voteSet)
		if !added || err != nil {
			t.Errorf("Expected VoteSet.Add to succeed")
		}
	}

	// val0 votes again for some block.
	{
		addr := privValidators[0].GetPubKey().Address()
		vote := withValidator(voteProto, addr, 0)
		added, err := signAddVote(privValidators[0], withTxHash(vote, cmn.RandBytes(32)), voteSet)
		if added || err == nil {
			t.Errorf("Expected VoteSet.Add to fail, conflicting vote.")
		}
	}

	// val1 votes on another height
	{
		addr := privValidators[1].GetPubKey().Address()
		vote := withValidator(voteProto, addr, 1)
		added, err := signAddVote(privValidators[1], withHeight(vote, height+1), voteSet)
		if added || err == nil {
			t.Errorf("Expected VoteSet.Add to fail, wrong height")
		}
	}
}

func TestConflicts(t *testing.T) {
	height := int64(1)
	voteSet, _, privValidators := randTxVoteSet(height, 4, 1)
	blockHash1 := cmn.RandBytes(32)
	blockHash2 := cmn.RandBytes(32)

	voteProto := &TxVote{
		ValidatorAddress: nil,
		Height:           height,
		Timestamp:        tmtime.Now(),
		TxHash:           types.Tx("0x1").Hash(),
	}

	val0Addr := privValidators[0].GetPubKey().Address()
	// val0 votes for nil.
	{
		vote := withValidator(voteProto, val0Addr, 0)
		added, err := signAddVote(privValidators[0], vote, voteSet)
		if !added || err != nil {
			t.Errorf("Expected VoteSet.Add to succeed")
		}
	}

	// val0 votes again for blockHash1.
	{
		vote := withValidator(voteProto, val0Addr, 0)
		added, err := signAddVote(privValidators[0], withTxHash(vote, blockHash1), voteSet)
		if added {
			t.Errorf("Expected VoteSet.Add to fail, conflicting vote.")
		}
		if err == nil {
			t.Errorf("Expected VoteSet.Add to return error, conflicting vote.")
		}
	}

	// val0 votes again for blockHash1.
	{
		vote := withValidator(voteProto, val0Addr, 0)
		added, err := signAddVote(privValidators[0], withTxHash(vote, blockHash1), voteSet)
		if !added {
			t.Errorf("Expected VoteSet.Add to succeed, called SetPeerMaj23().")
		}
		if err == nil {
			t.Errorf("Expected VoteSet.Add to return error, conflicting vote.")
		}
	}

	// val0 votes again for blockHash1.
	{
		vote := withValidator(voteProto, val0Addr, 0)
		added, err := signAddVote(privValidators[0], withTxHash(vote, blockHash2), voteSet)
		if added {
			t.Errorf("Expected VoteSet.Add to fail, duplicate SetPeerMaj23() from peerA")
		}
		if err == nil {
			t.Errorf("Expected VoteSet.Add to return error, conflicting vote.")
		}
	}

	// val1 votes for blockHash1.
	{
		addr := privValidators[1].GetPubKey().Address()
		vote := withValidator(voteProto, addr, 1)
		added, err := signAddVote(privValidators[1], withTxHash(vote, blockHash1), voteSet)
		if !added || err != nil {
			t.Errorf("Expected VoteSet.Add to succeed")
		}
	}

	// check
	if voteSet.HasTwoThirdsMajority() {
		t.Errorf("We shouldn't have 2/3 majority yet")
	}
	if voteSet.HasTwoThirdsAny() {
		t.Errorf("We shouldn't have 2/3 if any votes yet")
	}

	// val2 votes for blockHash2.
	{
		addr := privValidators[2].GetPubKey().Address()
		vote := withValidator(voteProto, addr, 2)
		added, err := signAddVote(privValidators[2], withTxHash(vote, blockHash2), voteSet)
		if !added || err != nil {
			t.Errorf("Expected VoteSet.Add to succeed")
		}
	}

	// check
	if voteSet.HasTwoThirdsMajority() {
		t.Errorf("We shouldn't have 2/3 majority yet")
	}
	if !voteSet.HasTwoThirdsAny() {
		t.Errorf("We should have 2/3 if any votes")
	}

	// val2 votes for blockHash1.
	{
		addr := privValidators[2].GetPubKey().Address()
		vote := withValidator(voteProto, addr, 2)
		added, err := signAddVote(privValidators[2], withTxHash(vote, blockHash1), voteSet)
		if !added {
			t.Errorf("Expected VoteSet.Add to succeed")
		}
		if err == nil {
			t.Errorf("Expected VoteSet.Add to return error, conflicting vote")
		}
	}

	// check
	if !voteSet.HasTwoThirdsMajority() {
		t.Errorf("We should have 2/3 majority for blockHash1")
	}
	if !voteSet.HasTwoThirdsAny() {
		t.Errorf("We should have 2/3 if any votes")
	}

}
