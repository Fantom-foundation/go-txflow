package types

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/tendermint/tendermint/crypto"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/types"
)

// Block defines the atomic unit of a Tendermint blockchain.
type Block struct {
	mtx          sync.Mutex
	types.Header `json:"header"`
	Data         `json:"data"`
	Evidence     types.EvidenceData `json:"evidence"`
	LastCommit   *types.Commit      `json:"last_commit"`
}

// MakeBlock returns a new block with an empty header, except what can be
// computed from itself.
// It populates the same set of fields validated by ValidateBasic.
func MakeBlock(height int64, txs []types.Tx, vtxs []types.Tx, lastCommit *types.Commit, evidence []types.Evidence) *Block {
	block := &Block{
		Header: types.Header{
			Height: height,
			NumTxs: int64(len(txs)),
		},
		Data: Data{
			Txs:  txs,
			Vtxs: vtxs,
		},
		Evidence:   types.EvidenceData{Evidence: evidence},
		LastCommit: lastCommit,
	}
	block.fillHeader()
	return block
}

func (b *Block) GetTendermintBlock() *types.Block {
	return types.MakeBlock(b.Height, b.Txs, b.LastCommit, b.Evidence.Evidence)
}

// ValidateBasic performs basic validation that doesn't involve state data.
// It checks the internal consistency of the block.
// Further validation is done using state#ValidateBlock.
func (b *Block) ValidateBasic() error {
	if b == nil {
		return errors.New("nil block")
	}
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if len(b.ChainID) > types.MaxChainIDLen {
		return fmt.Errorf("ChainID is too long. Max is %d, got %d", types.MaxChainIDLen, len(b.ChainID))
	}

	if b.Height < 0 {
		return errors.New("Negative Header.Height")
	} else if b.Height == 0 {
		return errors.New("Zero Header.Height")
	}

	// NOTE: Timestamp validation is subtle and handled elsewhere.

	newTxs := int64(len(b.Data.Txs))
	if b.NumTxs != newTxs {
		return fmt.Errorf("Wrong Header.NumTxs. Expected %v, got %v",
			newTxs,
			b.NumTxs,
		)
	}

	// TODO: fix tests so we can do this
	/*if b.TotalTxs < b.NumTxs {
		return fmt.Errorf("Header.TotalTxs (%d) is less than Header.NumTxs (%d)", b.TotalTxs, b.NumTxs)
	}*/
	if b.TotalTxs < 0 {
		return errors.New("Negative Header.TotalTxs")
	}

	if err := b.LastBlockID.ValidateBasic(); err != nil {
		return fmt.Errorf("Wrong Header.LastBlockID: %v", err)
	}

	// Validate the last commit and its hash.
	if b.Header.Height > 1 {
		if b.LastCommit == nil {
			return errors.New("nil LastCommit")
		}
		if err := b.LastCommit.ValidateBasic(); err != nil {
			return fmt.Errorf("Wrong LastCommit")
		}
	}
	if err := types.ValidateHash(b.LastCommitHash); err != nil {
		return fmt.Errorf("Wrong Header.LastCommitHash: %v", err)
	}
	if !bytes.Equal(b.LastCommitHash, b.LastCommit.Hash()) {
		return fmt.Errorf("Wrong Header.LastCommitHash. Expected %v, got %v",
			b.LastCommit.Hash(),
			b.LastCommitHash,
		)
	}

	// Validate the hash of the transactions.
	// NOTE: b.Data.Txs may be nil, but b.Data.Hash()
	// still works fine
	if err := types.ValidateHash(b.DataHash); err != nil {
		return fmt.Errorf("Wrong Header.DataHash: %v", err)
	}
	if !bytes.Equal(b.DataHash, b.Data.Hash()) {
		return fmt.Errorf(
			"Wrong Header.DataHash. Expected %v, got %v",
			b.Data.Hash(),
			b.DataHash,
		)
	}

	// Basic validation of hashes related to application data.
	// Will validate fully against state in state#ValidateBlock.
	if err := types.ValidateHash(b.ValidatorsHash); err != nil {
		return fmt.Errorf("Wrong Header.ValidatorsHash: %v", err)
	}
	if err := types.ValidateHash(b.NextValidatorsHash); err != nil {
		return fmt.Errorf("Wrong Header.NextValidatorsHash: %v", err)
	}
	if err := types.ValidateHash(b.ConsensusHash); err != nil {
		return fmt.Errorf("Wrong Header.ConsensusHash: %v", err)
	}
	// NOTE: AppHash is arbitrary length
	if err := types.ValidateHash(b.LastResultsHash); err != nil {
		return fmt.Errorf("Wrong Header.LastResultsHash: %v", err)
	}

	// Validate evidence and its hash.
	if err := types.ValidateHash(b.EvidenceHash); err != nil {
		return fmt.Errorf("Wrong Header.EvidenceHash: %v", err)
	}
	// NOTE: b.Evidence.Evidence may be nil, but we're just looping.
	for i, ev := range b.Evidence.Evidence {
		if err := ev.ValidateBasic(); err != nil {
			return fmt.Errorf("Invalid evidence (#%d): %v", i, err)
		}
	}
	if !bytes.Equal(b.EvidenceHash, b.Evidence.Hash()) {
		return fmt.Errorf("Wrong Header.EvidenceHash. Expected %v, got %v",
			b.EvidenceHash,
			b.Evidence.Hash(),
		)
	}

	if len(b.ProposerAddress) != crypto.AddressSize {
		return fmt.Errorf("Expected len(Header.ProposerAddress) to be %d, got %d",
			crypto.AddressSize, len(b.ProposerAddress))
	}

	return nil
}

// fillHeader fills in any remaining header fields that are a function of the block data
func (b *Block) fillHeader() {
	if b.LastCommitHash == nil {
		b.LastCommitHash = b.LastCommit.Hash()
	}
	if b.DataHash == nil {
		b.DataHash = b.Data.Hash()
	}
	if b.EvidenceHash == nil {
		b.EvidenceHash = b.Evidence.Hash()
	}
}

// Hash computes and returns the block hash.
// If the block is incomplete, block hash is nil for safety.
func (b *Block) Hash() cmn.HexBytes {
	if b == nil {
		return nil
	}
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if b.LastCommit == nil {
		return nil
	}
	b.fillHeader()
	return b.Header.Hash()
}

// MakePartSet returns a PartSet containing parts of a serialized block.
// This is the form in which the block is gossipped to peers.
// CONTRACT: partSize is greater than zero.
func (b *Block) MakePartSet(partSize int) *types.PartSet {
	if b == nil {
		return nil
	}
	b.mtx.Lock()
	defer b.mtx.Unlock()

	// We prefix the byte length, so that unmarshaling
	// can easily happen via a reader.
	bz, err := cdc.MarshalBinaryLengthPrefixed(b)
	if err != nil {
		panic(err)
	}
	return types.NewPartSetFromData(bz, partSize)
}

// HashesTo is a convenience function that checks if a block hashes to the given argument.
// Returns false if the block is nil or the hash is empty.
func (b *Block) HashesTo(hash []byte) bool {
	if len(hash) == 0 {
		return false
	}
	if b == nil {
		return false
	}
	return bytes.Equal(b.Hash(), hash)
}

// Size returns size of the block in bytes.
func (b *Block) Size() int {
	bz, err := cdc.MarshalBinaryBare(b)
	if err != nil {
		return 0
	}
	return len(bz)
}

// String returns a string representation of the block
func (b *Block) String() string {
	return b.StringIndented("")
}

// StringIndented returns a string representation of the block
func (b *Block) StringIndented(indent string) string {
	if b == nil {
		return "nil-Block"
	}
	return fmt.Sprintf(`Block{
%s  %v
%s  %v
%s  %v
%s  %v
%s}#%v`,
		indent, b.Header.StringIndented(indent+"  "),
		indent, b.Data.StringIndented(indent+"  "),
		indent, b.Evidence.StringIndented(indent+"  "),
		indent, b.LastCommit.StringIndented(indent+"  "),
		indent, b.Hash())
}

// StringShort returns a shortened string representation of the block
func (b *Block) StringShort() string {
	if b == nil {
		return "nil-Block"
	}
	return fmt.Sprintf("Block#%v", b.Hash())
}

//-----------------------------------------------------------
// These methods are for Protobuf Compatibility

// Marshal returns the amino encoding.
func (b *Block) Marshal() ([]byte, error) {
	return cdc.MarshalBinaryBare(b)
}

// MarshalTo calls Marshal and copies to the given buffer.
func (b *Block) MarshalTo(data []byte) (int, error) {
	bs, err := b.Marshal()
	if err != nil {
		return -1, err
	}
	return copy(data, bs), nil
}

// Unmarshal deserializes from amino encoded form.
func (b *Block) Unmarshal(bs []byte) error {
	return cdc.UnmarshalBinaryBare(bs, b)
}

//-----------------------------------------------------------------------------

// Data contains the set of transactions included in the block
type Data struct {

	// Txs that will be applied by state @ block.Height+1.
	// NOTE: not all txs here are valid.  We're just agreeing on the order first.
	// This means that block.AppHash does not include these txs.
	Txs types.Txs `json:"txs"`

	// Validated Txs that should not be applied (already applied)
	Vtxs types.Txs `json:"vtxs"`

	// Volatile
	hash cmn.HexBytes
}

// Hash returns the hash of the data
func (data *Data) Hash() cmn.HexBytes {
	if data == nil {
		return (types.Txs{}).Hash()
	}
	if data.hash == nil {
		data.hash = data.Txs.Hash() // NOTE: leaves of merkle tree are TxIDs
	}
	return data.hash
}

// StringIndented returns a string representation of the transactions
func (data *Data) StringIndented(indent string) string {
	if data == nil {
		return "nil-Data"
	}
	txStrings := make([]string, cmn.MinInt(len(data.Txs), 21))
	for i, tx := range data.Txs {
		if i == 20 {
			txStrings[i] = fmt.Sprintf("... (%v total)", len(data.Txs))
			break
		}
		txStrings[i] = fmt.Sprintf("%X (%d bytes)", tx.Hash(), len(tx))
	}
	return fmt.Sprintf(`Data{
%s  %v
%s}#%v`,
		indent, strings.Join(txStrings, "\n"+indent+"  "),
		indent, data.hash)
}

//-----------------------------------------------------------------------------
