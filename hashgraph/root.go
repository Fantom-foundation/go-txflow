package hashgraph

import (
	"bytes"
	"encoding/json"

	"github.com/andrecronje/babble/src/common"
	"github.com/andrecronje/babble/src/crypto"
)

//Root forms a base on top of which a participant's Events can be inserted. It
//contains FrameEvents sorted by Lamport timestamp.
type Root struct {
	Events []*FrameEvent
}

//NewRoot instantianted an new empty root
func NewRoot() *Root {
	return &Root{
		Events: []*FrameEvent{},
	}
}

//Insert appends a FrameEvent to the root's Event slice. It is assumend that
//items are inserted in topological order.
func (r *Root) Insert(frameEvent *FrameEvent) {
	r.Events = append(r.Events, frameEvent)
}

func (root *Root) Marshal() ([]byte, error) {
	var b bytes.Buffer

	enc := json.NewEncoder(&b)

	if err := enc.Encode(root); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (root *Root) Unmarshal(data []byte) error {
	b := bytes.NewBuffer(data)

	dec := json.NewDecoder(b) //will read from b

	return dec.Decode(root)
}

func (root *Root) Hash() (string, error) {
	hashBytes, err := root.Marshal()
	if err != nil {
		return "", err
	}
	hash := crypto.SHA256(hashBytes)
	return common.EncodeToString(hash), nil
}
