package txflow

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mempl "github.com/Fantom-foundation/go-txflow/mempool"
	"github.com/Fantom-foundation/go-txflow/tx"
	"github.com/Fantom-foundation/go-txflow/txvotepool"
	"github.com/Fantom-foundation/go-txflow/types"
	"github.com/tendermint/tendermint/abci/example/kvstore"
	cfg "github.com/tendermint/tendermint/config"
	cmn "github.com/tendermint/tendermint/libs/common"
	dbm "github.com/tendermint/tendermint/libs/db"
	"github.com/tendermint/tendermint/libs/log"
	tmempl "github.com/tendermint/tendermint/mempool"
	"github.com/tendermint/tendermint/proxy"
	sm "github.com/tendermint/tendermint/state"
	ttypes "github.com/tendermint/tendermint/types"
)

func TestTxVotes(t *testing.T) {
	config := cfg.ResetTestRoot("node_create_proposal")
	defer os.RemoveAll(config.RootDir)
	cc := proxy.NewLocalClientCreator(kvstore.NewKVStoreApplication())
	proxyApp := proxy.NewAppConns(cc)
	err := proxyApp.Start()
	require.Nil(t, err)
	defer proxyApp.Stop()

	logger := log.NewTMLogger(log.NewSyncWriter(os.Stdout))

	var height int64 = 1
	state, stateDB, privVal := stateWithPrivValidator(1, height)
	maxBytes := 16384
	state.ConsensusParams.Block.MaxBytes = int64(maxBytes)

	// Make Mempool
	memplMetrics := tmempl.PrometheusMetrics("node_test_1")
	mempool := mempl.NewCListMempool(
		config.Mempool,
		proxyApp.Mempool(),
		state.LastBlockHeight,
		mempl.WithMetrics(memplMetrics),
		mempl.WithPreCheck(sm.TxPreCheck(state)),
		mempl.WithPostCheck(sm.TxPostCheck(state)),
	)
	mempool.SetLogger(logger)

	// Make TxVotePool
	txvMetrics := tmempl.PrometheusMetrics("node_test_2")
	txVotePool := txvotepool.NewTxVotePool(
		config.Mempool,
		state.LastBlockHeight,
		txvotepool.WithMetrics(txvMetrics),
	)
	txVotePoolLogger := logger.With("module", "txvotepool")
	txVotePoolReactor := txvotepool.NewReactor(
		config.Mempool,
		mempool,
		txVotePool,
		&state,
		privVal,
	)
	txVotePoolReactor.SetLogger(txVotePoolLogger)

	err = txVotePoolReactor.Start()
	assert.NoError(t, err)
	// fill the mempool with more txs
	// than can fit in a block
	txLength := 1000
	for i := 0; i < maxBytes/txLength; i++ {
		tx := cmn.RandBytes(txLength)
		err := mempool.CheckTx(tx, nil)
		assert.NoError(t, err)
	}

	assert.Equal(t, true, txVotePoolReactor.IsRunning())
	assert.Equal(t, 16, mempool.Size())

	txStore := tx.NewTxStore(stateDB)

	txfLogger := logger.With("module", "txflow")
	txf := NewTxFlow(
		&state,
		txVotePool,
		mempool,
		nil,
		txStore,
		nil,
	)
	txf.SetLogger(txfLogger)
	err = txf.Start()
	assert.NoError(t, err)
}

//----------------------------------------------
// in-process testnets

func stateWithPrivValidator(nVals int, height int64) (sm.State, dbm.DB, types.PrivValidator) {
	vals := make([]ttypes.GenesisValidator, nVals)
	pk := types.NewMockPV()
	for i := 0; i < nVals; i++ {
		pk = types.NewMockPV()
		vals[i] = ttypes.GenesisValidator{
			Address: pk.GetPubKey().Address(),
			PubKey:  pk.GetPubKey(),
			Power:   1000,
			Name:    fmt.Sprintf("test%d", i),
		}
	}
	s, _ := sm.MakeGenesisState(&ttypes.GenesisDoc{
		ChainID:    "test-chain",
		Validators: vals,
		AppHash:    nil,
	})

	// save validators to db for 2 heights
	stateDB := dbm.NewMemDB()
	sm.SaveState(stateDB, s)

	for i := 1; i < int(height); i++ {
		s.LastBlockHeight++
		s.LastValidators = s.Validators.Copy()
		sm.SaveState(stateDB, s)
	}
	return s, stateDB, pk
}
