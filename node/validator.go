package node

import (
	"encoding/hex"

	"github.com/tendermint/tendermint/crypto"
)

//Validator struct holds information about the validator for a node
type Validator struct {
	Key     crypto.PrivKey
	Moniker string

	id       string
	pubBytes []byte
	pubHex   string
}

//NewValidator is a factory method for a Validator
func NewValidator(key crypto.PrivKey, moniker string) *Validator {
	return &Validator{
		Key:     key,
		Moniker: moniker,
	}
}

//ID returns an ID for the validator
func (v *Validator) ID() string {
	if v.id == "" {
		v.id = hex.EncodeToString(v.Key.PubKey().Address())
	}
	return v.id
}

//PublicKeyBytes returns the validator's public key as a byte array
func (v *Validator) PublicKeyBytes() []byte {
	if v.pubBytes == nil || len(v.pubBytes) == 0 {
		v.pubBytes = v.Key.PubKey().Bytes()
	}
	return v.pubBytes
}

//PublicKeyHex returns the validator's public key as a hex string
func (v *Validator) PublicKeyHex() string {
	if len(v.pubHex) == 0 {
		v.pubHex = v.Key.PubKey().Address().String()
	}
	return v.pubHex
}
