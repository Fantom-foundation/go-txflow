package types

import (
	"fmt"
	"time"
)

//-----------------------------------------------------------------------------

// PeerTxState contains the known state of a peer.
// NOTE: Read-only when returned by PeerState.GetTxState().
type PeerTxState struct {
	Height    int64     `json:"height"`     // Height peer is at
	StartTime time.Time `json:"start_time"` // Estimated start of round 0 at this height
}

// String returns a string representation of the PeerRoundState
func (pts PeerTxState) String() string {
	return pts.StringIndented("")
}

// StringIndented returns a string representation of the PeerRoundState
func (pts PeerTxState) StringIndented(indent string) string {
	return fmt.Sprintf(`PeerTxState{
%s  %v @%v
%s}`,
		indent, pts.Height, pts.StartTime,
		indent)
}

//-----------------------------------------------------------
// These methods are for Protobuf Compatibility

// Size returns the size of the amino encoding, in bytes.
func (pts PeerTxState) Size() int {
	bs, _ := pts.Marshal()
	return len(bs)
}

// Marshal returns the amino encoding.
func (pts PeerTxState) Marshal() ([]byte, error) {
	return cdc.MarshalBinaryBare(pts)
}

// MarshalTo calls Marshal and copies to the given buffer.
func (pts PeerTxState) MarshalTo(data []byte) (int, error) {
	bs, err := pts.Marshal()
	if err != nil {
		return -1, err
	}
	return copy(data, bs), nil
}

// Unmarshal deserializes from amino encoded form.
func (pts PeerTxState) Unmarshal(bs []byte) error {
	return cdc.UnmarshalBinaryBare(bs, pts)
}
