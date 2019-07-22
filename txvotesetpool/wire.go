package txvotepool

import (
	amino "github.com/tendermint/go-amino"
)

var cdc = amino.NewCodec()

func init() {
	RegisterTxVoteSetPoolMessages(cdc)
}
