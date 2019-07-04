package txflow

import (
	amino "github.com/tendermint/go-amino"
	"github.com/tendermint/tendermint/types"
)

var cdc = amino.NewCodec()

func init() {
	RegisterTxFlowMessages(cdc)
	RegisterWALMessages(cdc)
	types.RegisterBlockAmino(cdc)
}
