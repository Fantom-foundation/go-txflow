package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	amino "github.com/tendermint/go-amino"
	"github.com/tendermint/tendermint/crypto"
	"github.com/tendermint/tendermint/crypto/ed25519"
	ttypes "github.com/tendermint/tendermint/types"
)

func exampleVote() *TxVote {
	var stamp, err = time.Parse(ttypes.TimeFormat, "2017-12-25T03:00:01.234Z")
	if err != nil {
		panic(err)
	}

	return &TxVote{
		Height:           12345,
		Timestamp:        stamp,
		TxHash:           TxHash([]byte("tx_hash")),
		ValidatorAddress: crypto.AddressHash([]byte("validator_address")),
	}
}

// Ensure that Vote and CommitSig have the same encoding.
// This ensures using CommitSig isn't a breaking change.
// This test will fail and can be removed once CommitSig contains only sigs and
// timestamps.
func TestVoteEncoding(t *testing.T) {
	vote := exampleVote()
	commitSig := vote.CommitSig()
	cdc := amino.NewCodec()
	bz1 := cdc.MustMarshalBinaryBare(vote)
	bz2 := cdc.MustMarshalBinaryBare(commitSig)
	assert.Equal(t, bz1, bz2)
}

func TestVoteSignable(t *testing.T) {
	vote := exampleVote()
	signBytes := vote.SignBytes("test_chain_id")

	expected, err := cdc.MarshalBinaryLengthPrefixed(CanonicalizeTxVote("test_chain_id", vote))
	require.NoError(t, err)

	require.Equal(t, expected, signBytes, "Got unexpected sign bytes for Vote.")
}

func TestVoteSignBytesTestVectors(t *testing.T) {

	tests := []struct {
		chainID string
		vote    *TxVote
		want    []byte
	}{
		0: {
			"", &TxVote{},
			// NOTE: Height and Round are skipped here. This case needs to be considered while parsing.
			[]byte{0xd, 0x2a, 0xb, 0x8, 0x80, 0x92, 0xb8, 0xc3, 0x98, 0xfe, 0xff, 0xff, 0xff, 0x1},
		},
		// with proper (fixed size) height and round (PreCommit):
		1: {
			"", &TxVote{Height: 1},
			[]byte{
				0x21,                                   // length
				0x8,                                    // (field_number << 3) | wire_type
				0x2,                                    // PrecommitType
				0x11,                                   // (field_number << 3) | wire_type
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // height
				0x19,                                   // (field_number << 3) | wire_type
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // round
				0x2a, // (field_number << 3) | wire_type
				// remaining fields (timestamp):
				0xb, 0x8, 0x80, 0x92, 0xb8, 0xc3, 0x98, 0xfe, 0xff, 0xff, 0xff, 0x1},
		},
		// with proper (fixed size) height and round (PreVote):
		2: {
			"", &TxVote{Height: 1},
			[]byte{
				0x21,                                   // length
				0x8,                                    // (field_number << 3) | wire_type
				0x1,                                    // PrevoteType
				0x11,                                   // (field_number << 3) | wire_type
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // height
				0x19,                                   // (field_number << 3) | wire_type
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // round
				0x2a, // (field_number << 3) | wire_type
				// remaining fields (timestamp):
				0xb, 0x8, 0x80, 0x92, 0xb8, 0xc3, 0x98, 0xfe, 0xff, 0xff, 0xff, 0x1},
		},
		3: {
			"", &TxVote{Height: 1},
			[]byte{
				0x1f,                                   // length
				0x11,                                   // (field_number << 3) | wire_type
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // height
				0x19,                                   // (field_number << 3) | wire_type
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // round
				// remaining fields (timestamp):
				0x2a,
				0xb, 0x8, 0x80, 0x92, 0xb8, 0xc3, 0x98, 0xfe, 0xff, 0xff, 0xff, 0x1},
		},
		// containing non-empty chain_id:
		4: {
			"test_chain_id", &TxVote{Height: 1},
			[]byte{
				0x2e,                                   // length
				0x11,                                   // (field_number << 3) | wire_type
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // height
				0x19,                                   // (field_number << 3) | wire_type
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // round
				// remaining fields:
				0x2a,                                                                // (field_number << 3) | wire_type
				0xb, 0x8, 0x80, 0x92, 0xb8, 0xc3, 0x98, 0xfe, 0xff, 0xff, 0xff, 0x1, // timestamp
				0x32,                                                                               // (field_number << 3) | wire_type
				0xd, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x63, 0x68, 0x61, 0x69, 0x6e, 0x5f, 0x69, 0x64}, // chainID
		},
	}
	for i, tc := range tests {
		got := tc.vote.SignBytes(tc.chainID)
		require.Equal(t, tc.want, got, "test case #%v: got unexpected sign bytes for Vote.", i)
	}
}

func TestVoteVerifySignature(t *testing.T) {
	privVal := NewMockPV()
	pubkey := privVal.GetPubKey()

	vote := exampleVote()

	// sign it
	err := privVal.SignTxVote("test_chain_id", vote)
	require.NoError(t, err)

	// verify the same vote
	valid := pubkey.VerifyBytes(vote.SignBytes("test_chain_id"), vote.Signature)
	require.True(t, valid)
}

func TestVoteVerify(t *testing.T) {
	privVal := NewMockPV()
	pubkey := privVal.GetPubKey()

	vote := exampleVote()
	vote.ValidatorAddress = pubkey.Address()

	err := vote.Verify("test_chain_id", ed25519.GenPrivKey().PubKey())
	if assert.Error(t, err) {
		assert.Equal(t, ttypes.ErrVoteInvalidValidatorAddress, err)
	}

	err = vote.Verify("test_chain_id", pubkey)
	if assert.Error(t, err) {
		assert.Equal(t, ErrVoteInvalidSignature, err)
	}
}

func TestVoteValidateBasic(t *testing.T) {
	privVal := NewMockPV()

	testCases := []struct {
		testName     string
		malleateVote func(*TxVote)
		expectErr    bool
	}{
		{"Good Vote", func(v *TxVote) {}, false},
		{"Negative Height", func(v *TxVote) { v.Height = -1 }, true},
		{"Invalid Address", func(v *TxVote) { v.ValidatorAddress = make([]byte, 1) }, true},
		{"Invalid Signature", func(v *TxVote) { v.Signature = nil }, true},
		{"Too big Signature", func(v *TxVote) { v.Signature = make([]byte, ttypes.MaxSignatureSize+1) }, true},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			vote := exampleVote()
			err := privVal.SignTxVote("test_chain_id", vote)
			require.NoError(t, err)
			tc.malleateVote(vote)
			assert.Equal(t, tc.expectErr, vote.ValidateBasic() != nil, "Validate Basic had an unexpected result")
		})
	}
}
