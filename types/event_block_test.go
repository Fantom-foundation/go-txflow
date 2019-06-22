package types

import (
	// it is ok to use math/rand here: we do not need a cryptographically secure random
	// number generator here and we can run the tests a bit faster
	"crypto/rand"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tendermint/tendermint/crypto"
	"github.com/tendermint/tendermint/crypto/tmhash"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/types"
	"github.com/tendermint/tendermint/version"
)

func TestMain(m *testing.M) {
	types.RegisterMockEvidences(cdc)

	code := m.Run()
	os.Exit(code)
}

func TestEventBlockAddEvidence(t *testing.T) {
	txs := []types.Tx{types.Tx("foo"), types.Tx("bar")}
	h := int64(3)

	_, valSet, _ := randVoteSet(h-1, 1, types.PrecommitType, 10, 1)

	ev := types.NewMockGoodEvidence(h, 0, valSet.Validators[0].Address)
	evList := []types.Evidence{ev}

	block := MakeEventBlock(h, txs, evList)
	require.NotNil(t, block)
	require.Equal(t, 1, len(block.Evidence.Evidence))
	require.NotNil(t, block.EvidenceHash)
}

func TestEventBlockValidateBasic(t *testing.T) {
	require.Error(t, (*EventBlock)(nil).ValidateBasic())

	txs := []types.Tx{types.Tx("foo"), types.Tx("bar")}
	h := int64(3)

	_, valSet, _ := randVoteSet(h-1, 1, types.PrecommitType, 10, 1)

	ev := types.NewMockGoodEvidence(h, 0, valSet.Validators[0].Address)
	evList := []types.Evidence{ev}

	testCases := []struct {
		testName      string
		malleateBlock func(*EventBlock)
		expErr        bool
	}{
		{"Make Block", func(blk *EventBlock) {}, false},
		{"Make Block w/ proposer Addr", func(blk *EventBlock) { blk.ProposerAddress = valSet.GetProposer().Address }, false},
		{"Negative Height", func(blk *EventBlock) { blk.Height = -1 }, true},
		{"Increase NumTxs", func(blk *EventBlock) { blk.NumTxs++ }, true},
		{"Tampered Data", func(blk *EventBlock) {
			blk.Data.Txs[0] = types.Tx("something else")
			blk.DataHash = cmn.RandBytes(len(blk.DataHash))
		}, true},
		{"Tampered DataHash", func(blk *EventBlock) {
			blk.DataHash = cmn.RandBytes(len(blk.DataHash))
		}, true},
		{"Tampered EvidenceHash", func(blk *EventBlock) {
			blk.EvidenceHash = []byte("something else")
		}, true},
	}
	for i, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			block := MakeEventBlock(h, txs, evList)
			block.ProposerAddress = valSet.GetProposer().Address
			tc.malleateBlock(block)
			err := block.ValidateBasic()
			assert.Equal(t, tc.expErr, err != nil, "#%d: %v", i, err)
		})
	}
}

func TestEventBlockHash(t *testing.T) {
	assert.Nil(t, (*EventBlock)(nil).Hash())
	assert.Nil(t, MakeEventBlock(int64(3), []types.Tx{types.Tx("Hello World")}, nil).Hash())
}

func TestEventBlockMakePartSet(t *testing.T) {
	assert.Nil(t, (*EventBlock)(nil).MakePartSet(2))

	partSet := MakeEventBlock(int64(3), []types.Tx{types.Tx("Hello World")}, nil).MakePartSet(1024)
	assert.NotNil(t, partSet)
	assert.Equal(t, 1, partSet.Total())
}

func TestEventBlockMakePartSetWithEvidence(t *testing.T) {
	assert.Nil(t, (*EventBlock)(nil).MakePartSet(2))

	h := int64(3)

	_, valSet, _ := randVoteSet(h-1, 1, types.PrecommitType, 10, 1)

	ev := types.NewMockGoodEvidence(h, 0, valSet.Validators[0].Address)
	evList := []types.Evidence{ev}

	partSet := MakeEventBlock(h, []types.Tx{types.Tx("Hello World")}, evList).MakePartSet(1024)
	assert.NotNil(t, partSet)
	assert.Equal(t, 1, partSet.Total())
}

func TestEventBlockHashesTo(t *testing.T) {
	assert.False(t, (*EventBlock)(nil).HashesTo(nil))

	h := int64(3)
	_, valSet, _ := randVoteSet(h-1, 1, types.PrecommitType, 10, 1)

	ev := types.NewMockGoodEvidence(h, 0, valSet.Validators[0].Address)
	evList := []types.Evidence{ev}

	block := MakeEventBlock(h, []types.Tx{types.Tx("Hello World")}, evList)
	block.ValidatorsHash = valSet.Hash()
	assert.False(t, block.HashesTo([]byte{}))
	assert.False(t, block.HashesTo([]byte("something else")))
	assert.True(t, block.HashesTo(block.Hash()))
}

func TestEventBlockSize(t *testing.T) {
	size := MakeEventBlock(int64(3), []types.Tx{types.Tx("Hello World")}, nil).Size()
	if size <= 0 {
		t.Fatal("Size of the block is zero or negative")
	}
}

func TestBlockString(t *testing.T) {
	assert.Equal(t, "nil-Block", (*EventBlock)(nil).String())
	assert.Equal(t, "nil-Block", (*EventBlock)(nil).StringIndented(""))
	assert.Equal(t, "nil-Block", (*EventBlock)(nil).StringShort())

	block := MakeEventBlock(int64(3), []types.Tx{types.Tx("Hello World")}, nil)
	assert.NotEqual(t, "nil-Block", block.String())
	assert.NotEqual(t, "nil-Block", block.StringIndented(""))
	assert.NotEqual(t, "nil-Block", block.StringShort())
}

func makeBlockIDRandom() EventBlockID {
	blockHash := make([]byte, tmhash.Size)
	rand.Read(blockHash) //nolint: gosec
	return EventBlockID{blockHash}
}

func makeBlockID(hash []byte) EventBlockID {
	return EventBlockID{
		Hash: hash,
	}

}

var nilBytes []byte

func TestNilHeaderHashDoesntCrash(t *testing.T) {
	assert.Equal(t, []byte((*Header)(nil).Hash()), nilBytes)
	assert.Equal(t, []byte((new(Header)).Hash()), nilBytes)
}

func TestNilDataHashDoesntCrash(t *testing.T) {
	assert.Equal(t, []byte((*types.Data)(nil).Hash()), nilBytes)
	assert.Equal(t, []byte(new(types.Data).Hash()), nilBytes)
}

func TestMaxHeaderBytes(t *testing.T) {
	// Construct a UTF-8 string of MaxChainIDLen length using the supplementary
	// characters.
	// Each supplementary character takes 4 bytes.
	// http://www.i18nguy.com/unicode/supplementary-test.html
	maxChainID := ""
	for i := 0; i < types.MaxChainIDLen; i++ {
		maxChainID += "𠜎"
	}

	// time is varint encoded so need to pick the max.
	// year int, month Month, day, hour, min, sec, nsec int, loc *Location
	timestamp := time.Date(math.MaxInt64, 0, 0, 0, 0, 0, math.MaxInt64, time.UTC)

	h := Header{
		Version:            version.Consensus{Block: math.MaxInt64, App: math.MaxInt64},
		ChainID:            maxChainID,
		Height:             math.MaxInt64,
		Time:               timestamp,
		NumTxs:             math.MaxInt64,
		TotalTxs:           math.MaxInt64,
		LastEventBlockIDs:  []EventBlockID{makeBlockID(make([]byte, tmhash.Size))},
		DataHash:           tmhash.Sum([]byte("data_hash")),
		ValidatorsHash:     tmhash.Sum([]byte("validators_hash")),
		NextValidatorsHash: tmhash.Sum([]byte("next_validators_hash")),
		ConsensusHash:      tmhash.Sum([]byte("consensus_hash")),
		EvidenceHash:       tmhash.Sum([]byte("evidence_hash")),
		ProposerAddress:    crypto.AddressHash([]byte("proposer_address")),
	}

	bz, err := cdc.MarshalBinaryLengthPrefixed(h)
	require.NoError(t, err)

	assert.EqualValues(t, MaxHeaderBytes, len(bz))
}

// NOTE: privValidators are in order
func randVoteSet(height int64, round int, type_ types.SignedMsgType, numValidators int, votingPower int64) (*types.VoteSet, *types.ValidatorSet, []types.PrivValidator) {
	valSet, privValidators := types.RandValidatorSet(numValidators, votingPower)
	return types.NewVoteSet("test_chain_id", height, round, type_, valSet), valSet, privValidators
}
