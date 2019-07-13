package mempool

import (
	amino "github.com/tendermint/go-amino"
	"github.com/tendermint/tendermint/mempool"
)

var cdc = amino.NewCodec()

func init() {
	mempool.RegisterMempoolMessages(cdc)
}
