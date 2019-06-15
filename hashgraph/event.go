package hashgraph

import (
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
	hash  string
	index int64
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
	Transactions types.Txs `json:"transactions"` //the payload
	Parents      []string  `json:"parents"`      //hashes of the event's parents, self-parent first
	Index        int64     `json:"index"`        //index in the sequence of events created by Creator
	Signature    []byte    `json:"signature"`    //creator's digital signature of body

	topologicalIndex int64 `json:"topological_index"`

	//used for sorting
	round            *int64 `json:"round"`
	lamportTimestamp *int64 `json:"lamport_timestamp"`

	roundReceived *int64 `json:"round_received"`

	lastAncestors    CoordinatesMap `json:"last_ancestors"`    //[participant pubkey] => last ancestor
	firstDescendants CoordinatesMap `json:"first_descendants"` //[participant pubkey] => first descendant

	Creator crypto.PubKey `json:"creator"`
	hash    []byte        `json:"hash"`
	hex     string        `json:"hex"`
}

func NewEvent(txs types.Txs,
	parents []string,
	creator crypto.PubKey,
	index int64) *Event {
	return &Event{
		Transactions: txs,
		Parents:      parents,
		Creator:      creator,
		Index:        index,
	}
}

func (e *Event) SelfParent() string {
	return e.Parents[0]
}

func (e *Event) OtherParent() string {
	return e.Parents[1]
}

func (e *Event) Sign(privKey crypto.PrivKey) error {
	signBytes := e.Hash()
	got, err := privKey.Sign(signBytes)
	if err != nil {
		return err
	}
	e.Signature = got
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
		cdcEncode(e.Index),
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

func (e *Event) SetRound(r int64) {
	if e.round == nil {
		e.round = new(int64)
	}
	*e.round = r
}

func (e *Event) GetRound() *int64 {
	return e.round
}

func (e *Event) SetLamportTimestamp(t int64) {
	if e.lamportTimestamp == nil {
		e.lamportTimestamp = new(int64)
	}
	*e.lamportTimestamp = t
}

func (e *Event) SetRoundReceived(rr int64) {
	if e.roundReceived == nil {
		e.roundReceived = new(int64)
	}
	*e.roundReceived = rr
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
	return a[i].topologicalIndex < a[j].topologicalIndex
}

// ByLamportTimestamp implements sort.Interface for []Event based on
// the lamportTimestamp field.
// THIS IS A TOTAL ORDER
type ByLamportTimestamp []*Event

func (a ByLamportTimestamp) Len() int64      { return int64(len(a)) }
func (a ByLamportTimestamp) Swap(i, j int64) { a[i], a[j] = a[j], a[i] }
func (a ByLamportTimestamp) Less(i, j int64) bool {
	it, jt := int64(-1), int64(-1)
	if a[i].lamportTimestamp != nil {
		it = *a[i].lamportTimestamp
	}
	if a[j].lamportTimestamp != nil {
		jt = *a[j].lamportTimestamp
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
