package txflowstate

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Fantom-foundation/go-txflow/txvotepool"
	"github.com/tendermint/tendermint/abci/example/kvstore"
	"github.com/tendermint/tendermint/libs/log"

	"github.com/tendermint/tendermint/mock"
	"github.com/tendermint/tendermint/proxy"
	"github.com/tendermint/tendermint/types"
)

var (
	chainID      = "execution_chain"
	testPartSize = 65536
	nTxsPerBlock = 10
)

func TestApplyTx(t *testing.T) {
	cc := proxy.NewLocalClientCreator(kvstore.NewKVStoreApplication())
	proxyApp := proxy.NewAppConns(cc)
	err := proxyApp.Start()
	require.Nil(t, err)
	defer proxyApp.Stop()

	txExec := NewTxExecutor(log.TestingLogger(), proxyApp.Consensus(), mock.Mempool{}, txvotepool.NewTxVotePool(nil, 0))

	tx := makeTx([]byte("1"))

	//nolint:ineffassign
	err = txExec.ApplyTx(nil, tx)
	require.Nil(t, err)

	// TODO check state and mempool
}

//----------------------------------------------------------------------------
func makeTx(tx []byte) types.Tx {
	return types.Tx(tx)
}

//----------------------------------------------------------------------------
