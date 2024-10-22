package node

import (
	"context"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mempl "github.com/Fantom-foundation/go-txflow/mempool"
	"github.com/Fantom-foundation/go-txflow/privval"
	"github.com/Fantom-foundation/go-txflow/tx"
	"github.com/Fantom-foundation/go-txflow/txflow"
	"github.com/Fantom-foundation/go-txflow/txflowstate"
	"github.com/Fantom-foundation/go-txflow/txvotepool"
	"github.com/Fantom-foundation/go-txflow/types"
	"github.com/tendermint/tendermint/abci/example/kvstore"
	cfg "github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/crypto/ed25519"
	"github.com/tendermint/tendermint/evidence"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/log"
	tmempl "github.com/tendermint/tendermint/mempool"
	"github.com/tendermint/tendermint/p2p"
	tprivval "github.com/tendermint/tendermint/privval"
	"github.com/tendermint/tendermint/proxy"
	sm "github.com/tendermint/tendermint/state"
	ttypes "github.com/tendermint/tendermint/types"
	tmtime "github.com/tendermint/tendermint/types/time"
	"github.com/tendermint/tendermint/version"
	dbm "github.com/tendermint/tm-cmn/db"
)

func TestNodeStartStop(t *testing.T) {
	config := cfg.ResetTestRoot("node_node_test")
	defer os.RemoveAll(config.RootDir)

	// create & start node
	n, err := DefaultNewNode(config, log.NewTMLogger(log.NewSyncWriter(os.Stdout)))
	require.NoError(t, err)
	err = n.Start()
	require.NoError(t, err)

	t.Logf("Started node %v", n.sw.NodeInfo())

	// wait for the node to produce a block
	blocksSub, err := n.EventBus().Subscribe(context.Background(), "node_test", ttypes.EventQueryNewBlock)
	require.NoError(t, err)
	select {
	case <-blocksSub.Out():
	case <-blocksSub.Cancelled():
		t.Fatal("blocksSub was cancelled")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the node to produce a block")
	}

	// stop the node
	go func() {
		n.Stop()
	}()

	select {
	case <-n.Quit():
	case <-time.After(5 * time.Second):
		pid := os.Getpid()
		p, err := os.FindProcess(pid)
		if err != nil {
			panic(err)
		}
		err = p.Signal(syscall.SIGABRT)
		fmt.Println(err)
		t.Fatal("timed out waiting for shutdown")
	}
}

func TestSplitAndTrimEmpty(t *testing.T) {
	testCases := []struct {
		s        string
		sep      string
		cutset   string
		expected []string
	}{
		{"a,b,c", ",", " ", []string{"a", "b", "c"}},
		{" a , b , c ", ",", " ", []string{"a", "b", "c"}},
		{" a, b, c ", ",", " ", []string{"a", "b", "c"}},
		{" a, ", ",", " ", []string{"a"}},
		{"   ", ",", " ", []string{}},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expected, splitAndTrimEmpty(tc.s, tc.sep, tc.cutset), "%s", tc.s)
	}
}

func TestNodeDelayedStart(t *testing.T) {
	config := cfg.ResetTestRoot("node_delayed_start_test")
	defer os.RemoveAll(config.RootDir)
	now := tmtime.Now()

	// create & start node
	n, err := DefaultNewNode(config, log.TestingLogger())
	n.GenesisDoc().GenesisTime = now.Add(2 * time.Second)
	require.NoError(t, err)

	err = n.Start()
	require.NoError(t, err)
	defer n.Stop()

	startTime := tmtime.Now()
	assert.Equal(t, true, startTime.After(n.GenesisDoc().GenesisTime))
}

func TestNodeSetAppVersion(t *testing.T) {
	config := cfg.ResetTestRoot("node_app_version_test")
	defer os.RemoveAll(config.RootDir)

	// create & start node
	n, err := DefaultNewNode(config, log.TestingLogger())
	require.NoError(t, err)

	// default config uses the kvstore app
	var appVersion version.Protocol = kvstore.ProtocolVersion

	// check version is set in state
	state := sm.LoadState(n.stateDB)
	assert.Equal(t, state.Version.Consensus.App, appVersion)

	// check version is set in node info
	assert.Equal(t, n.nodeInfo.(p2p.DefaultNodeInfo).ProtocolVersion.App, appVersion)
}

func TestNodeSetPrivValTCP(t *testing.T) {
	addr := "tcp://" + testFreeAddr(t)

	config := cfg.ResetTestRoot("node_priv_val_tcp_test")
	defer os.RemoveAll(config.RootDir)
	config.BaseConfig.PrivValidatorListenAddr = addr

	dialer := tprivval.DialTCPFn(addr, 100*time.Millisecond, ed25519.GenPrivKey())
	pvsc := tprivval.NewSignerServiceEndpoint(
		log.TestingLogger(),
		config.ChainID(),
		types.NewMockPV(),
		dialer,
	)
	tprivval.SignerServiceEndpointTimeoutReadWrite(100 * time.Millisecond)(pvsc)

	go func() {
		err := pvsc.Start()
		if err != nil {
			panic(err)
		}
	}()
	defer pvsc.Stop()

	n, err := DefaultNewNode(config, log.TestingLogger())
	require.NoError(t, err)
	assert.IsType(t, &privval.SignerValidatorEndpoint{}, n.PrivValidator())
}

// address without a protocol must result in error
func TestPrivValidatorListenAddrNoProtocol(t *testing.T) {
	addrNoPrefix := testFreeAddr(t)

	config := cfg.ResetTestRoot("node_priv_val_tcp_test")
	defer os.RemoveAll(config.RootDir)
	config.BaseConfig.PrivValidatorListenAddr = addrNoPrefix

	_, err := DefaultNewNode(config, log.TestingLogger())
	assert.Error(t, err)
}

/*func TestNodeSetPrivValIPC(t *testing.T) {
	tmpfile := "/tmp/kms." + cmn.RandStr(6) + ".sock"
	defer os.Remove(tmpfile) // clean up

	config := cfg.ResetTestRoot("node_priv_val_tcp_test")
	defer os.RemoveAll(config.RootDir)
	config.BaseConfig.PrivValidatorListenAddr = "unix://" + tmpfile

	dialer := tprivval.DialUnixFn(tmpfile)
	pvsc := tprivval.NewSignerServiceEndpoint(
		log.TestingLogger(),
		config.ChainID(),
		types.NewMockPV(),
		dialer,
	)
	tprivval.SignerServiceEndpointTimeoutReadWrite(100 * time.Millisecond)(pvsc)

	go func() {
		err := pvsc.Start()
		require.NoError(t, err)
	}()
	defer pvsc.Stop()

	n, err := DefaultNewNode(config, log.TestingLogger())
	require.NoError(t, err)
	assert.IsType(t, &privval.SignerValidatorEndpoint{}, n.PrivValidator())

}*/

// testFreeAddr claims a free port so we don't block on listener being ready.
func testFreeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	return fmt.Sprintf("127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
}

// create a proposal block using real and full
// mempool and evidence pool and validate it.
func TestCreateProposalBlock(t *testing.T) {
	config := cfg.ResetTestRoot("node_create_proposal")
	defer os.RemoveAll(config.RootDir)
	cc := proxy.NewLocalClientCreator(kvstore.NewKVStoreApplication())
	proxyApp := proxy.NewAppConns(cc)
	err := proxyApp.Start()
	require.Nil(t, err)
	defer proxyApp.Stop()

	logger := log.TestingLogger()

	var height int64 = 1
	state, stateDB := state(1, height)
	maxBytes := 16384
	state.ConsensusParams.Block.MaxBytes = int64(maxBytes)
	proposerAddr, _ := state.Validators.GetByIndex(0)

	// Make Mempool
	memplMetrics := tmempl.PrometheusMetrics("node_test")
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
	txVPool := txvotepool.NewTxVotePool(
		config.Mempool,
		state.LastBlockHeight,
		txvotepool.WithMetrics(memplMetrics),
	)
	txVotePoolLogger := logger.With("module", "txvotepool")
	txVotePoolReactor := txvotepool.NewReactor(
		config.Mempool,
		mempool,
		txVPool,
		&state,
		types.NewMockPV(),
	)
	txVotePoolReactor.SetLogger(txVotePoolLogger)

	// Make EvidencePool
	ttypes.RegisterMockEvidencesGlobal() // XXX!
	evidence.RegisterMockEvidences()
	evidenceDB := dbm.NewMemDB()
	evidencePool := evidence.NewEvidencePool(stateDB, evidenceDB)
	evidencePool.SetLogger(logger)

	// fill the evidence pool with more evidence
	// than can fit in a block
	minEvSize := 12
	numEv := (maxBytes / ttypes.MaxEvidenceBytesDenominator) / minEvSize
	for i := 0; i < numEv; i++ {
		ev := ttypes.NewMockRandomGoodEvidence(1, proposerAddr, cmn.RandBytes(minEvSize))
		err := evidencePool.AddEvidence(ev)
		assert.NoError(t, err)
	}

	// fill the mempool with more txs
	// than can fit in a block
	txLength := 1000
	for i := 0; i < maxBytes/txLength; i++ {
		tx := cmn.RandBytes(txLength)
		err := mempool.CheckTx(tx, nil)
		assert.NoError(t, err)
	}

	blockExec := sm.NewBlockExecutor(
		stateDB,
		logger,
		proxyApp.Consensus(),
		mempool,
		evidencePool,
	)

	commit := ttypes.NewCommit(ttypes.BlockID{}, nil)
	block, _ := blockExec.CreateProposalBlock(
		height,
		state, commit,
		proposerAddr,
	)

	err = blockExec.ValidateBlock(state, block)
	assert.NoError(t, err)
}

// create a proposal block using real and full
// mempool and evidence pool and validate it.
func TestTxVotes(t *testing.T) {
	config := cfg.ResetTestRoot("node_create_proposal")
	defer os.RemoveAll(config.RootDir)
	cc := proxy.NewLocalClientCreator(kvstore.NewKVStoreApplication())
	proxyApp := proxy.NewAppConns(cc)
	err := proxyApp.Start()
	require.Nil(t, err)
	defer proxyApp.Stop()

	logger := log.TestingLogger()

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

	txExec := txflowstate.NewTxExecutor(
		logger.With("module", "state"),
		proxyApp.Consensus(),
		mempool,
		txVotePool,
	)

	txfLogger := logger.With("module", "txflow")
	txf := txflow.NewTxFlow(
		&state,
		txVotePool,
		mempool,
		txExec,
		txStore,
		nil,
	)
	txf.SetLogger(txfLogger)
	err = txf.Start()
	assert.NoError(t, err)
}

func state(nVals int, height int64) (sm.State, dbm.DB) {
	vals := make([]ttypes.GenesisValidator, nVals)
	for i := 0; i < nVals; i++ {
		secret := []byte(fmt.Sprintf("test%d", i))
		pk := ed25519.GenPrivKeyFromSecret(secret)
		vals[i] = ttypes.GenesisValidator{
			Address: pk.PubKey().Address(),
			PubKey:  pk.PubKey(),
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
	return s, stateDB
}

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
