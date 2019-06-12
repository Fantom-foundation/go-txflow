package hashgraph

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"

	"github.com/andrecronje/babble/src/common"
	"github.com/andrecronje/babble/src/crypto"
	"github.com/andrecronje/babble/src/crypto/keys"
	"github.com/andrecronje/babble/src/peers"
)

/*******************************************************************************
BlockBody
*******************************************************************************/

type BlockBody struct {
	Index                       int
	RoundReceived               int
	StateHash                   []byte
	FrameHash                   []byte
	PeersHash                   []byte
	Transactions                [][]byte
	InternalTransactions        []InternalTransaction
	InternalTransactionReceipts []InternalTransactionReceipt
}

//json encoding of body only
func (bb *BlockBody) Marshal() ([]byte, error) {
	bf := bytes.NewBuffer([]byte{})
	enc := json.NewEncoder(bf)
	if err := enc.Encode(bb); err != nil {
		return nil, err
	}
	return bf.Bytes(), nil
}

func (bb *BlockBody) Unmarshal(data []byte) error {
	b := bytes.NewBuffer(data)
	dec := json.NewDecoder(b) //will read from b
	if err := dec.Decode(bb); err != nil {
		return err
	}
	return nil
}

func (bb *BlockBody) Hash() ([]byte, error) {
	hashBytes, err := bb.Marshal()
	if err != nil {
		return nil, err
	}
	return crypto.SHA256(hashBytes), nil
}

/*******************************************************************************
BlockSignature
*******************************************************************************/

type BlockSignature struct {
	Validator []byte
	Index     int //Block Index
	Signature string
}

func (bs *BlockSignature) ValidatorHex() string {
	return common.EncodeToString(bs.Validator)
}

func (bs *BlockSignature) Marshal() ([]byte, error) {
	bf := bytes.NewBuffer([]byte{})
	enc := json.NewEncoder(bf)
	if err := enc.Encode(bs); err != nil {
		return nil, err
	}
	return bf.Bytes(), nil
}

func (bs *BlockSignature) Unmarshal(data []byte) error {
	b := bytes.NewBuffer(data)
	dec := json.NewDecoder(b) //will read from b
	if err := dec.Decode(bs); err != nil {
		return err
	}
	return nil
}

func (bs *BlockSignature) ToWire() WireBlockSignature {
	return WireBlockSignature{
		Index:     bs.Index,
		Signature: bs.Signature,
	}
}

func (bs *BlockSignature) Key() string {
	return fmt.Sprintf("%d-%s", bs.Index, bs.ValidatorHex())
}

type WireBlockSignature struct {
	Index     int
	Signature string
}

/*******************************************************************************
Block
*******************************************************************************/

type Block struct {
	Body       BlockBody
	Signatures map[string]string // [validator hex] => signature

	hash    []byte
	hex     string
	peerSet *peers.PeerSet
}

func NewBlockFromFrame(blockIndex int, frame *Frame) (*Block, error) {
	frameHash, err := frame.Hash()
	if err != nil {
		return nil, err
	}

	transactions := [][]byte{}
	internalTransactions := []InternalTransaction{}
	for _, e := range frame.Events {
		transactions = append(transactions, e.Core.Transactions()...)
		internalTransactions = append(internalTransactions, e.Core.InternalTransactions()...)
	}

	return NewBlock(blockIndex, frame.Round, frameHash, frame.Peers, transactions, internalTransactions), nil
}

func NewBlock(blockIndex,
	roundReceived int,
	frameHash []byte,
	peerSlice []*peers.Peer,
	txs [][]byte,
	itxs []InternalTransaction) *Block {

	peerSet := peers.NewPeerSet(peerSlice)

	peersHash, err := peerSet.Hash()
	if err != nil {
		return nil
	}

	body := BlockBody{
		Index:                blockIndex,
		RoundReceived:        roundReceived,
		StateHash:            []byte{},
		FrameHash:            frameHash,
		PeersHash:            peersHash,
		Transactions:         txs,
		InternalTransactions: itxs,
	}

	return &Block{
		Body:       body,
		Signatures: make(map[string]string),
		peerSet:    peerSet,
	}
}

func (b *Block) Index() int {
	return b.Body.Index
}

func (b *Block) Transactions() [][]byte {
	return b.Body.Transactions
}

func (b *Block) InternalTransactions() []InternalTransaction {
	return b.Body.InternalTransactions
}

func (b *Block) InternalTransactionReceipts() []InternalTransactionReceipt {
	return b.Body.InternalTransactionReceipts
}

func (b *Block) RoundReceived() int {
	return b.Body.RoundReceived
}

func (b *Block) StateHash() []byte {
	return b.Body.StateHash
}

func (b *Block) FrameHash() []byte {
	return b.Body.FrameHash
}

func (b *Block) PeersHash() []byte {
	return b.Body.PeersHash
}

func (b *Block) GetSignatures() []BlockSignature {
	res := make([]BlockSignature, len(b.Signatures))
	i := 0
	for val, sig := range b.Signatures {
		validatorBytes, _ := common.DecodeFromString(val)
		res[i] = BlockSignature{
			Validator: validatorBytes,
			Index:     b.Index(),
			Signature: sig,
		}
		i++
	}
	return res
}

func (b *Block) GetSignature(validator string) (res BlockSignature, err error) {
	sig, ok := b.Signatures[validator]
	if !ok {
		return res, fmt.Errorf("signature not found")
	}

	validatorBytes, _ := common.DecodeFromString(validator)
	return BlockSignature{
		Validator: validatorBytes,
		Index:     b.Index(),
		Signature: sig,
	}, nil
}

func (b *Block) AppendTransactions(txs [][]byte) {
	b.Body.Transactions = append(b.Body.Transactions, txs...)
}

func (b *Block) Marshal() ([]byte, error) {
	bf := bytes.NewBuffer([]byte{})
	enc := json.NewEncoder(bf)
	if err := enc.Encode(b); err != nil {
		return nil, err
	}
	return bf.Bytes(), nil
}

func (b *Block) Unmarshal(data []byte) error {
	bf := bytes.NewBuffer(data)
	dec := json.NewDecoder(bf)
	if err := dec.Decode(b); err != nil {
		return err
	}
	return nil
}

func (b *Block) Hash() ([]byte, error) {
	if len(b.hash) == 0 {
		hashBytes, err := b.Marshal()
		if err != nil {
			return nil, err
		}
		b.hash = crypto.SHA256(hashBytes)
	}
	return b.hash, nil
}

func (b *Block) Hex() string {
	if b.hex == "" {
		hash, _ := b.Hash()
		b.hex = common.EncodeToString(hash)
	}
	return b.hex
}

func (b *Block) Sign(privKey *ecdsa.PrivateKey) (bs BlockSignature, err error) {
	signBytes, err := b.Body.Hash()
	if err != nil {
		return bs, err
	}
	R, S, err := keys.Sign(privKey, signBytes)
	if err != nil {
		return bs, err
	}
	signature := BlockSignature{
		Validator: keys.FromPublicKey(&privKey.PublicKey),
		Index:     b.Index(),
		Signature: keys.EncodeSignature(R, S),
	}

	return signature, nil
}

func (b *Block) SetSignature(bs BlockSignature) error {
	b.Signatures[bs.ValidatorHex()] = bs.Signature
	return nil
}

func (b *Block) Verify(sig BlockSignature) (bool, error) {
	signBytes, err := b.Body.Hash()
	if err != nil {
		return false, err
	}

	pubKey := keys.ToPublicKey(sig.Validator)

	r, s, err := keys.DecodeSignature(sig.Signature)
	if err != nil {
		return false, err
	}

	return keys.Verify(pubKey, signBytes, r, s), nil
}
