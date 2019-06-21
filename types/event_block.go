package types

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/tendermint/tendermint/crypto"
	"github.com/tendermint/tendermint/crypto/merkle"
	"github.com/tendermint/tendermint/crypto/tmhash"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/version"
	"github.com/tendermint/tendermint/types"
)

const (
	// MaxHeaderBytes is a maximum header size (including amino overhead).
	MaxHeaderBytes int64 = 653

	// MaxAminoOverheadForBlock - maximum amino overhead to encode a block (up to
	// MaxBlockSizeBytes in size) not including it's parts except Data.
	// This means it also excludes the overhead for individual transactions.
	// To compute individual transactions' overhead use types.ComputeAminoOverhead(tx types.Tx, fieldNum int).
	//
	// Uvarint length of MaxBlockSizeBytes: 4 bytes
	// 2 fields (2 embedded):               2 bytes
	// Uvarint length of Data.Txs:          4 bytes
	// Data.Txs field:                      1 byte
	MaxAminoOverheadForBlock int64 = 11
)

// EventBlock defines the atomic unit of a aBFT blockchain.
type EventBlock struct {
	mtx      sync.Mutex
	Header   `json:"header"`
	types.Data     `json:"data"`
	Evidence types.EvidenceData `json:"evidence"`
	SelfEventBlockID EventBlockID `json:"self_event_block_id"`
	EventBlockIDs    []EventBlockID      `json:"block_ids"`
}

// MakeEventBlock returns a new event block with an empty header, except what can be
// computed from itself.
// It populates the same set of fields validated by ValidateBasic.
func MakeEventBlock(height int64, txs []types.Tx, evidence []types.Evidence, eventBlockIDs []EventBlockID) *EventBlock {
	block := &EventBlock{
		Header: Header{
			Height: height,
			NumTxs: int64(len(txs)),
		},
		Data: types.Data{
			Txs: txs,
		},
		Evidence: types.EvidenceData{Evidence: evidence},
		EventBlockIDs: eventBlockIDs,
	}
	block.fillHeader()
	return block
}

// ValidateBasic performs basic validation that doesn't involve state data.
// It checks the internal consistency of the block.
// Further validation is done using state#ValidateBlock.
func (eb *EventBlock) ValidateBasic() error {
	if eb == nil {
		return errors.New("nil event block")
	}
	eb.mtx.Lock()
	defer eb.mtx.Unlock()

	if len(eb.ChainID) > types.MaxChainIDLen {
		return fmt.Errorf("ChainID is too long. Max is %d, got %d", types.MaxChainIDLen, len(eb.ChainID))
	}

	if eb.Height < 0 {
		return errors.New("Negative Header.Height")
	} else if eb.Height == 0 {
		return errors.New("Zero Header.Height")
	}

	// NOTE: Timestamp validation is subtle and handled elsewhere.

	newTxs := int64(len(eb.Data.Txs))
	if eb.NumTxs != newTxs {
		return fmt.Errorf("Wrong Header.NumTxs. Expected %v, got %v",
			newTxs,
			eb.NumTxs,
		)
	}

	// TODO: fix tests so we can do this
	/*if b.TotalTxs < b.NumTxs {
		return fmt.Errorf("Header.TotalTxs (%d) is less than Header.NumTxs (%d)", b.TotalTxs, b.NumTxs)
	}*/
	if eb.TotalTxs < 0 {
		return errors.New("Negative Header.TotalTxs")
	}

	if err := eb.LastBlockID.ValidateBasic(); err != nil {
		return fmt.Errorf("Wrong Header.LastBlockID: %v", err)
	}

	// Validate the last commit and its hash.
	// Validate the hashes of the parents, not the commit of the previous block
	if eb.Header.Height > 1 {
		if eb.LastCommit == nil {
			return errors.New("nil LastCommit")
		}
		if err := eb.LastCommit.ValidateBasic(); err != nil {
			return fmt.Errorf("Wrong LastCommit")
		}
	}
	if err := ValidateHash(eb.LastCommitHash); err != nil {
		return fmt.Errorf("Wrong Header.LastCommitHash: %v", err)
	}
	if !bytes.Equal(eb.LastCommitHash, eb.LastCommit.Hash()) {
		return fmt.Errorf("Wrong Header.LastCommitHash. Expected %v, got %v",
			eb.LastCommit.Hash(),
			eb.LastCommitHash,
		)
	}

	// Validate the hash of the transactions.
	// NOTE: b.Data.Txs may be nil, but b.Data.Hash()
	// still works fine
	if err := ValidateHash(b.DataHash); err != nil {
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
	if err := ValidateHash(b.ValidatorsHash); err != nil {
		return fmt.Errorf("Wrong Header.ValidatorsHash: %v", err)
	}
	if err := ValidateHash(b.NextValidatorsHash); err != nil {
		return fmt.Errorf("Wrong Header.NextValidatorsHash: %v", err)
	}
	if err := ValidateHash(b.ConsensusHash); err != nil {
		return fmt.Errorf("Wrong Header.ConsensusHash: %v", err)
	}
	// NOTE: AppHash is arbitrary length
	if err := ValidateHash(b.LastResultsHash); err != nil {
		return fmt.Errorf("Wrong Header.LastResultsHash: %v", err)
	}

	// Validate evidence and its hash.
	if err := ValidateHash(b.EvidenceHash); err != nil {
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
func (eb *EventBlock) fillHeader() {
	if eb.DataHash == nil {
		eb.DataHash = eb.Data.Hash()
	}
	if eb.EvidenceHash == nil {
		eb.EvidenceHash = eb.Evidence.Hash()
	}
}

// Hash computes and returns the block hash.
// If the block is incomplete, block hash is nil for safety.
func (eb *EventBlock) Hash() cmn.HexBytes {
	if eb == nil {
		return nil
	}
	eb.mtx.Lock()
	defer eb.mtx.Unlock()

	if eb == nil {
		return nil
	}
	eb.fillHeader()
	return eb.Header.Hash()
}

// MakePartSet returns a PartSet containing parts of a serialized block.
// This is the form in which the block is gossipped to peers.
// CONTRACT: partSize is greater than zero.
func (eb *EventBlock) MakePartSet(partSize int) *types.PartSet {
	if eb == nil {
		return nil
	}
	eb.mtx.Lock()
	defer eb.mtx.Unlock()

	// We prefix the byte length, so that unmarshaling
	// can easily happen via a reader.
	bz, err := cdc.MarshalBinaryLengthPrefixed(eb)
	if err != nil {
		panic(err)
	}
	return NewPartSetFromData(bz, partSize)
}

// HashesTo is a convenience function that checks if a block hashes to the given argument.
// Returns false if the block is nil or the hash is empty.
func (eb *EventBlock) HashesTo(hash []byte) bool {
	if len(hash) == 0 {
		return false
	}
	if eb == nil {
		return false
	}
	return bytes.Equal(eb.Hash(), hash)
}

// Size returns size of the block in bytes.
func (eb *EventBlock) Size() int {
	bz, err := cdc.MarshalBinaryBare(eb)
	if err != nil {
		return 0
	}
	return len(bz)
}

// String returns a string representation of the block
func (eb *EventBlock) String() string {
	return eb.StringIndented("")
}

// StringIndented returns a string representation of the block
func (eb *EventBlock) StringIndented(indent string) string {
	if eb == nil {
		return "nil-Block"
	}
	return fmt.Sprintf(`Block{
%s  %v
%s  %v
%s  %v
%s}#%v`,
		indent, eb.Header.StringIndented(indent+"  "),
		indent, eb.Data.StringIndented(indent+"  "),
		indent, eb.Evidence.StringIndented(indent+"  "),
		indent, eb.Hash())
}

// StringShort returns a shortened string representation of the block
func (eb *EventBlock) StringShort() string {
	if eb == nil {
		return "nil-Block"
	}
	return fmt.Sprintf("EventBlock#%v", eb.Hash())
}

//-----------------------------------------------------------
// These methods are for Protobuf Compatibility

// Marshal returns the amino encoding.
func (eb *EventBlock) Marshal() ([]byte, error) {
	return cdc.MarshalBinaryBare(eb)
}

// MarshalTo calls Marshal and copies to the given buffer.
func (eb *EventBlock) MarshalTo(data []byte) (int, error) {
	bs, err := eb.Marshal()
	if err != nil {
		return -1, err
	}
	return copy(data, bs), nil
}

// Unmarshal deserializes from amino encoded form.
func (eb *EventBlock) Unmarshal(bs []byte) error {
	return cdc.UnmarshalBinaryBare(bs, eb)
}

//-----------------------------------------------------------------------------

// MaxDataBytes returns the maximum size of block's data.
//
// XXX: Panics on negative result.
func MaxDataBytes(maxBytes int64, valsCount, evidenceCount int) int64 {
	maxDataBytes := maxBytes -
		MaxAminoOverheadForBlock -
		MaxHeaderBytes -
		int64(valsCount)*MaxVoteBytes -
		int64(evidenceCount)*MaxEvidenceBytes

	if maxDataBytes < 0 {
		panic(fmt.Sprintf(
			"Negative MaxDataBytes. Block.MaxBytes=%d is too small to accommodate header&lastCommit&evidence=%d",
			maxBytes,
			-(maxDataBytes - maxBytes),
		))
	}

	return maxDataBytes

}

// MaxDataBytesUnknownEvidence returns the maximum size of block's data when
// evidence count is unknown. MaxEvidencePerBlock will be used for the size
// of evidence.
//
// XXX: Panics on negative result.
func MaxDataBytesUnknownEvidence(maxBytes int64, valsCount int) int64 {
	_, maxEvidenceBytes := MaxEvidencePerBlock(maxBytes)
	maxDataBytes := maxBytes -
		MaxAminoOverheadForBlock -
		MaxHeaderBytes -
		int64(valsCount)*MaxVoteBytes -
		maxEvidenceBytes

	if maxDataBytes < 0 {
		panic(fmt.Sprintf(
			"Negative MaxDataBytesUnknownEvidence. Block.MaxBytes=%d is too small to accommodate header&lastCommit&evidence=%d",
			maxBytes,
			-(maxDataBytes - maxBytes),
		))
	}

	return maxDataBytes
}

//-----------------------------------------------------------------------------

// Header defines the structure of a Tendermint block header.
// NOTE: changes to the Header should be duplicated in:
// - header.Hash()
// - abci.Header
// - /docs/spec/blockchain/blockchain.md
type Header struct {
	// basic block info
	Version  version.Consensus `json:"version"`
	ChainID  string            `json:"chain_id"`
	Height   int64             `json:"height"`
	Time     time.Time         `json:"time"`
	NumTxs   int64             `json:"num_txs"`
	TotalTxs int64             `json:"total_txs"`

	// self prev block info
	SelfLastEventBlockID EventBlockID `json:"self_last_block_id"`

	// prev block info
	LastEventBlockIDs []EventBlockID `json:"last_block_ids"`

	// hashes of block data
	DataHash       cmn.HexBytes `json:"data_hash"`        // transactions

	// hashes from the app output from the prev block
	ValidatorsHash     cmn.HexBytes `json:"validators_hash"`      // validators for the current block
	NextValidatorsHash cmn.HexBytes `json:"next_validators_hash"` // validators for the next block
	ConsensusHash      cmn.HexBytes `json:"consensus_hash"`       // consensus params for current block
	LastResultsHash    cmn.HexBytes `json:"last_results_hash"`    // root hash of all results from the txs from the previous block

	// consensus info
	EvidenceHash    cmn.HexBytes `json:"evidence_hash"`    // evidence included in the block
	ProposerAddress types.Address      `json:"proposer_address"` // original proposer of the block
}

// Populate the Header with state-derived data.
// Call this after MakeBlock to complete the Header.
func (h *Header) Populate(
	version version.Consensus, chainID string,
	timestamp time.Time, lastBlockIDs []EventBlockID, totalTxs int64,
	valHash, nextValHash []byte,
	consensusHash, lastResultsHash []byte,
	proposerAddress Address,
) {
	h.Version = version
	h.ChainID = chainID
	h.Time = timestamp
	h.LastBlockIDs = lastBlockIDs
	h.TotalTxs = totalTxs
	h.ValidatorsHash = valHash
	h.NextValidatorsHash = nextValHash
	h.ConsensusHash = consensusHash
	h.LastResultsHash = lastResultsHash
	h.ProposerAddress = proposerAddress
}

// Hash returns the hash of the header.
// It computes a Merkle tree from the header fields
// ordered as they appear in the Header.
// Returns nil if ValidatorHash is missing,
// since a Header is not valid unless there is
// a ValidatorsHash (corresponding to the validator set).
func (h *Header) Hash() cmn.HexBytes {
	if h == nil || len(h.ValidatorsHash) == 0 {
		return nil
	}
	return merkle.SimpleHashFromByteSlices([][]byte{
		cdcEncode(h.Version),
		cdcEncode(h.ChainID),
		cdcEncode(h.Height),
		cdcEncode(h.Time),
		cdcEncode(h.NumTxs),
		cdcEncode(h.TotalTxs),
		cdcEncode(h.SelfLastBlockID),
		cdcEncode(h.LastBlockIDs),
		cdcEncode(h.LastCommitHash),
		cdcEncode(h.DataHash),
		cdcEncode(h.ValidatorsHash),
		cdcEncode(h.NextValidatorsHash),
		cdcEncode(h.ConsensusHash),
		cdcEncode(h.LastResultsHash),
		cdcEncode(h.EvidenceHash),
		cdcEncode(h.ProposerAddress),
	})
}

// StringIndented returns a string representation of the header
func (h *Header) StringIndented(indent string) string {
	if h == nil {
		return "nil-Header"
	}
	return fmt.Sprintf(`Header{
%s  Version:        %v
%s  ChainID:        %v
%s  Height:         %v
%s  Time:           %v
%s  NumTxs:         %v
%s  TotalTxs:       %v
%s  LastBlockID:    %v
%s  Data:           %v
%s  Validators:     %v
%s  NextValidators: %v
%s  Consensus:      %v
%s  Results:        %v
%s  Evidence:       %v
%s  Proposer:       %v
%s}#%v`,
		indent, h.Version,
		indent, h.ChainID,
		indent, h.Height,
		indent, h.Time,
		indent, h.NumTxs,
		indent, h.TotalTxs,
		indent, h.LastBlockID,
		indent, h.DataHash,
		indent, h.ValidatorsHash,
		indent, h.NextValidatorsHash,
		indent, h.ConsensusHash,
		indent, h.LastResultsHash,
		indent, h.EvidenceHash,
		indent, h.ProposerAddress,
		indent, h.Hash())
}
//--------------------------------------------------------------------------------

// EventBlockID defines the unique ID of a block as its Hash and its PartSetHeader
type EventBlockID struct {
	Hash        cmn.HexBytes  `json:"hash"`
	PartsHeader types.PartSetHeader `json:"parts"`
}

// Equals returns true if the EventBlockID matches the given EventBlockID
func (ebID EventBlockID) Equals(other BlockID) bool {
	return bytes.Equal(ebID.Hash, other.Hash) &&
		ebID.PartsHeader.Equals(other.PartsHeader)
}

// Key returns a machine-readable string representation of the BlockID
func (ebID EventBlockID) Key() string {
	bz, err := cdc.MarshalBinaryBare(ebID.PartsHeader)
	if err != nil {
		panic(err)
	}
	return string(ebID.Hash) + string(bz)
}

// ValidateBasic performs basic validation.
func (ebID EventBlockID) ValidateBasic() error {
	// Hash can be empty in case of POLBlockID in Proposal.
	if err := ValidateHash(ebID.Hash); err != nil {
		return fmt.Errorf("Wrong Hash")
	}
	if err := ebID.PartsHeader.ValidateBasic(); err != nil {
		return fmt.Errorf("Wrong PartsHeader: %v", err)
	}
	return nil
}

// IsZero returns true if this is the BlockID of a nil block.
func (ebID EventBlockID) IsZero() bool {
	return len(ebID.Hash) == 0 &&
		ebID.PartsHeader.IsZero()
}

// IsComplete returns true if this is a valid BlockID of a non-nil block.
func (ebID EventBlockID) IsComplete() bool {
	return len(ebID.Hash) == tmhash.Size &&
		ebID.PartsHeader.Total > 0 &&
		len(ebID.PartsHeader.Hash) == tmhash.Size
}

// String returns a human readable string representation of the EventBlockID
func (ebID EventBlockID) String() string {
	return fmt.Sprintf(`%v:%v`, ebID.Hash, ebID.PartsHeader)
}
