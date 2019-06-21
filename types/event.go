package types

import (
	"fmt"

	"github.com/andrecronje/babble/src/common"
	"github.com/tendermint/tendermint/crypto"
	"github.com/tendermint/tendermint/crypto/merkle"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/types"
)

/*******************************************************************************
EventCoordinates
*******************************************************************************/

type EventCoordinates struct {
	Hex    string
	Height int64
}

type CoordinatesMap map[string]EventCoordinates

func NewCoordinatesMap() CoordinatesMap {
	return make(map[string]EventCoordinates)
}

func (c CoordinatesMap) Copy() CoordinatesMap {
	res := make(map[string]EventCoordinates, len(c))
	for k, v := range c {
		res[k] = v
	}
	return res
}

/*******************************************************************************
Event
*******************************************************************************/

type Event struct {
	Transactions types.Txs      `json:"transactions"` //the payload
	Parents      []cmn.HexBytes `json:"parents"`      //hashes of the event's parents, self-parent first
	Height       int64          `json:"index"`        //index in the sequence of events created by Creator
	Signature    []byte         `json:"signature"`    //creator's digital signature of body

	TopologicalIndex int64 `json:"topological_index"`

	//used for sorting
	Round            int64 `json:"round"`
	LamportTimestamp int64 `json:"lamport_timestamp"`

	RoundReceived int64 `json:"round_received"`

	LastAncestors    CoordinatesMap `json:"last_ancestors"`    //[validator pubkey] => last ancestor
	FirstDescendants CoordinatesMap `json:"first_descendants"` //[validator pubkey] => first descendant

	Creator crypto.PubKey `json:"creator"`
	hash    []cmn.HexBytes
	hex     string
}

func NewEvent(txs types.Txs,
	parents []cmn.HexBytes,
	creator crypto.PubKey,
	height int64) *Event {
	//TODO: Check selfParent is made by creator
	return &Event{
		Transactions:     txs,
		Parents:          parents,
		Creator:          creator,
		Height:           height,
		RoundReceived:    -1,
		Round:            -1,
		LamportTimestamp: -1,
	}
}

func (e *Event) SelfParent() string {
	//Dangerous assumption
	return e.Parents[0].String()
}

func (e *Event) Bytes() []byte {
	return []byte(fmt.Sprintf("%v", e))
}

func (e *Event) OtherParent() string {
	//Always needs to be self parent, so we can check this from us as a creator?
	return e.Parents[1].String()
}

func (e *Event) Sign(privKey crypto.PrivKey) error {
	signBytes := e.Hash()
	sig, err := privKey.Sign(signBytes)
	if err != nil {
		return err
	}
	e.Signature = sig
	return err
}

func (e *Event) Verify() (bool, error) {
	pubKey := e.Creator
	signBytes := e.Hash()

	return pubKey.VerifyBytes(signBytes, e.Signature), nil
}

func (e *Event) Hash() cmn.HexBytes {
	if e == nil {
		return nil
	}
	return merkle.SimpleHashFromByteSlices([][]byte{
		cdcEncode(e.Transactions),
		cdcEncode(e.Parents),
		cdcEncode(e.Height),
		cdcEncode(e.Creator),
	})
}

func (e *Event) Hex() string {
	if e.hex == "" {
		hash := e.Hash()
		e.hex = common.EncodeToString(hash)
	}
	return e.hex
}

/*******************************************************************************
Sorting
*******************************************************************************/

// ByTopologicalOrder implements sort.Interface for []Event based on
// the topologicalIndex field.
// THIS IS A PARTIAL ORDER
type ByTopologicalOrder []*Event

func (a ByTopologicalOrder) Len() int      { return len(a) }
func (a ByTopologicalOrder) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByTopologicalOrder) Less(i, j int) bool {
	return a[i].TopologicalIndex < a[j].TopologicalIndex
}

// ByLamportTimestamp implements sort.Interface for []Event based on
// the lamportTimestamp field.
// THIS IS A TOTAL ORDER
type ByLamportTimestamp []*Event

func (a ByLamportTimestamp) Len() int64      { return int64(len(a)) }
func (a ByLamportTimestamp) Swap(i, j int64) { a[i], a[j] = a[j], a[i] }
func (a ByLamportTimestamp) Less(i, j int64) bool {
	it, jt := int64(-1), int64(-1)
	if a[i].LamportTimestamp != -1 {
		it = a[i].LamportTimestamp
	}
	if a[j].LamportTimestamp != -1 {
		jt = a[j].LamportTimestamp
	}
	return it < jt
}

/*******************************************************************************
FrameEvent
******************************************************************************/

//FrameEvent is a wrapper around a regular Event. It contains exported fields
//Round, Witness, and LamportTimestamp.
type FrameEvent struct {
	Core             *Event //EventBody + Signature
	Round            int64
	LamportTimestamp int64
	Witness          bool
}

//SortedFrameEvents implements sort.Interface for []FameEvent based on
//the lamportTimestamp field.
//THIS IS A TOTAL ORDER
type SortedFrameEvents []*FrameEvent

func (a SortedFrameEvents) Len() int      { return len(a) }
func (a SortedFrameEvents) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortedFrameEvents) Less(i, j int) bool {
	if a[i].LamportTimestamp != a[j].LamportTimestamp {
		return a[i].LamportTimestamp < a[j].LamportTimestamp
	}
	return i < j
}
