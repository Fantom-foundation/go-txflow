package eventpool

import (
	amino "github.com/tendermint/go-amino"
	cryptoAmino "github.com/tendermint/tendermint/crypto/encoding/amino"
)

var cdc = amino.NewCodec()

func init() {
	RegisterEventpoolMessages(cdc)
	RegisterEventAmino(cdc)
}

func RegisterEventAmino(cdc *amino.Codec) {
	cryptoAmino.RegisterAmino(cdc)
}
