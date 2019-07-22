package txvotepool

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Fantom-foundation/go-txflow/mempool"
	"github.com/Fantom-foundation/go-txflow/types"
	"github.com/tendermint/tendermint/abci/example/counter"
	"github.com/tendermint/tendermint/abci/example/kvstore"
	abci "github.com/tendermint/tendermint/abci/types"
	cfg "github.com/tendermint/tendermint/config"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/log"
	tmempool "github.com/tendermint/tendermint/mempool"
	"github.com/tendermint/tendermint/proxy"
	ttypes "github.com/tendermint/tendermint/types"
)

// RandValidatorSet returns a randomized validator set, useful for testing.
// NOTE: PrivValidator are in order.
// UNSTABLE
func RandValidatorSet(numValidators int, votingPower int64) (*ttypes.ValidatorSet, []types.PrivValidator) {
	valz := make([]*ttypes.Validator, numValidators)
	privValidators := make([]types.PrivValidator, numValidators)
	for i := 0; i < numValidators; i++ {
		val, privValidator := RandValidator(false, votingPower)
		valz[i] = val
		privValidators[i] = privValidator
	}
	vals := ttypes.NewValidatorSet(valz)
	sort.Sort(types.PrivValidatorsByAddress(privValidators))
	return vals, privValidators
}

//----------------------------------------
// RandValidator

// RandValidator returns a randomized validator, useful for testing.
// UNSTABLE
func RandValidator(randPower bool, minPower int64) (*ttypes.Validator, types.PrivValidator) {
	privVal := types.NewMockPV()
	votePower := minPower
	if randPower {
		votePower += int64(cmn.RandUint32())
	}
	pubKey := privVal.GetPubKey()
	val := ttypes.NewValidator(pubKey, votePower)
	return val, privVal
}

// NOTE: privValidators are in order
func RandTxVoteSet(height int64, numValidators int, votingPower int64) (types.TxVoteSet, *ttypes.ValidatorSet, []types.PrivValidator) {
	valSet, privValidators := RandValidatorSet(numValidators, votingPower)
	tx := ttypes.Tx([]byte(strconv.FormatInt(height, 10)))
	return *types.NewTxVoteSet("test_chain_id", height, types.TxHash(tx), types.TxKey(tx), valSet), valSet, privValidators
}

// A cleanupFunc cleans up any config / test files created for a particular
// test.
type cleanupFunc func()

func newMempoolWithApp(cc proxy.ClientCreator) (*TxVoteSetPool, *mempool.CListMempool, cleanupFunc) {
	return newMempoolWithAppAndConfig(cc, cfg.ResetTestRoot("mempool_test"))
}

func newMempoolWithAppAndConfig(cc proxy.ClientCreator, config *cfg.Config) (*TxVoteSetPool, *mempool.CListMempool, cleanupFunc) {
	appConnMem, _ := cc.NewABCIClient()
	appConnMem.SetLogger(log.TestingLogger().With("module", "abci-client", "connection", "mempool"))
	err := appConnMem.Start()
	if err != nil {
		panic(err)
	}
	txvotepool := NewTxVoteSetPool(config.Mempool, 0)
	txvotepool.SetLogger(log.TestingLogger())
	mempool := mempool.NewCListMempool(config.Mempool, appConnMem, 0)
	return txvotepool, mempool, func() { os.RemoveAll(config.RootDir) }
}

func ensureNoFire(t *testing.T, ch <-chan struct{}, timeoutMS int) {
	timer := time.NewTimer(time.Duration(timeoutMS) * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("Expected not to fire")
	case <-timer.C:
	}
}

func ensureFire(t *testing.T, ch <-chan struct{}, timeoutMS int) {
	timer := time.NewTimer(time.Duration(timeoutMS) * time.Millisecond)
	select {
	case <-ch:
	case <-timer.C:
		t.Fatal("Expected to fire")
	}
}

func checkTxs(t *testing.T, txvotepool *TxVoteSetPool, count int, peerID uint16) []types.TxVoteSet {
	txs := make([]types.TxVoteSet, count)
	txInfo := tmempool.TxInfo{SenderID: peerID}
	for i := 0; i < count; i++ {
		txs[i], _, _ = RandTxVoteSet(int64(i), 1, 1)
		if err := txvotepool.CheckTxWithInfo(txs[i], txInfo); err != nil {
			// Skip invalid txs.
			// TestMempoolFilters will fail otherwise. It asserts a number of txs
			// returned.
			if mempool.IsPreCheckError(err) {
				continue
			}
			t.Fatalf("CheckTx failed: %v while checking #%d tx", err, i)
		}
	}
	return txs
}

func TestReapMaxBytesMaxGas(t *testing.T) {
	app := kvstore.NewKVStoreApplication()
	cc := proxy.NewLocalClientCreator(app)
	txvotepool, _, cleanup := newMempoolWithApp(cc)
	defer cleanup()

	// Ensure gas calculation behaves as expected
	checkTxs(t, txvotepool, 1, UnknownPeerID)
	tx0 := txvotepool.TxsFront().Value.(*MempoolTxVoteSet)
	// ensure each tx is 20 bytes long
	require.Equal(t, tx0.Tx.Size(), 114, "Tx is longer than 114 bytes")
	txvotepool.Flush()

	// each table driven test creates numTxsToCreate txs with checkTx, and at the end clears all remaining txs.
	// each tx has 20 bytes + amino overhead = 21 bytes, 1 gas

}

func TestMempoolUpdateAddsTxsToCache(t *testing.T) {
	app := kvstore.NewKVStoreApplication()
	cc := proxy.NewLocalClientCreator(app)
	txvotepool, _, cleanup := newMempoolWithApp(cc)
	defer cleanup()
	txvotepool.Update(0, []types.TxVoteSet{{}})
	err := txvotepool.CheckTx(types.TxVoteSet{})
	if assert.Error(t, err) {
		assert.Equal(t, mempool.ErrTxInCache, err)
	}
}

func TestTxsAvailable(t *testing.T) {
	app := kvstore.NewKVStoreApplication()
	cc := proxy.NewLocalClientCreator(app)
	txvotepool, _, cleanup := newMempoolWithApp(cc)
	defer cleanup()
	txvotepool.EnableTxsAvailable()

	timeoutMS := 500

	// with no txs, it shouldnt fire
	ensureNoFire(t, txvotepool.TxsAvailable(), timeoutMS)

	// send a bunch of txs, it should only fire once
	txs := checkTxs(t, txvotepool, 100, UnknownPeerID)
	ensureFire(t, txvotepool.TxsAvailable(), timeoutMS)
	ensureNoFire(t, txvotepool.TxsAvailable(), timeoutMS)

	// call update with half the txs.
	// it should fire once now for the new height
	// since there are still txs left
	committedTxs, txs := txs[:50], txs[50:]
	if err := txvotepool.Update(1, committedTxs); err != nil {
		t.Error(err)
	}
	ensureFire(t, txvotepool.TxsAvailable(), timeoutMS)
	ensureNoFire(t, txvotepool.TxsAvailable(), timeoutMS)

	// send a bunch more txs. we already fired for this height so it shouldnt fire again
	moreTxs := checkTxs(t, txvotepool, 50, UnknownPeerID)
	ensureNoFire(t, txvotepool.TxsAvailable(), timeoutMS)

	// now call update with all the txs. it should not fire as there are no txs left
	committedTxs = append(txs, moreTxs...)
	if err := txvotepool.Update(2, committedTxs); err != nil {
		t.Error(err)
	}
	ensureNoFire(t, txvotepool.TxsAvailable(), timeoutMS)

	// send a bunch more txs, it should only fire once
	checkTxs(t, txvotepool, 100, UnknownPeerID)
	ensureFire(t, txvotepool.TxsAvailable(), timeoutMS)
	ensureNoFire(t, txvotepool.TxsAvailable(), timeoutMS)
}

func TestSerialReap(t *testing.T) {
	app := counter.NewCounterApplication(true)
	app.SetOption(abci.RequestSetOption{Key: "serial", Value: "on"})
	cc := proxy.NewLocalClientCreator(app)

	txvotepool, _, cleanup := newMempoolWithApp(cc)
	defer cleanup()

	appConnCon, _ := cc.NewABCIClient()
	appConnCon.SetLogger(log.TestingLogger().With("module", "abci-client", "connection", "consensus"))
	err := appConnCon.Start()
	require.Nil(t, err)

	cacheMap := make(map[string]struct{})
	deliverTxsRange := func(start, end int) {
		// Deliver some txs.
		for i := start; i < end; i++ {
			// This will succeed
			txVote, _, _ := RandTxVoteSet(1, 1, 1)
			err := txvotepool.CheckTx(txVote)
			_, cached := cacheMap[TxVoteSetID(txVote)]
			if cached {
				require.NotNil(t, err, "expected error for cached tx")
			} else {
				require.Nil(t, err, "expected no err for uncached tx")
			}
			cacheMap[string(TxVoteSetID(txVote))] = struct{}{}

			// Duplicates are cached and should return error
			err = txvotepool.CheckTx(txVote)
			require.NotNil(t, err, "Expected error after CheckTx on duplicated tx")
		}
	}

	updateRange := func(start, end int) {
		txs := make([]types.TxVoteSet, 0)
		for i := start; i < end; i++ {
			txVote, _, _ := RandTxVoteSet(1, 1, 1)
			txs = append(txs, txVote)
		}
		if err := txvotepool.Update(3, txs); err != nil {
			t.Error(err)
		}
	}

	commitRange := func(start, end int) {
		// Deliver some txs.
		for i := start; i < end; i++ {
			txBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(txBytes, uint64(i))
			res, err := appConnCon.DeliverTxSync(abci.RequestDeliverTx{Tx: txBytes})
			if err != nil {
				t.Errorf("Client error committing tx: %v", err)
			}
			if res.IsErr() {
				t.Errorf("Error committing tx. Code:%v result:%X log:%v",
					res.Code, res.Data, res.Log)
			}
		}
		res, err := appConnCon.CommitSync()
		if err != nil {
			t.Errorf("Client error committing: %v", err)
		}
		if len(res.Data) != 8 {
			t.Errorf("Error committing. Hash:%X", res.Data)
		}
	}

	//----------------------------------------

	// Deliver some txs.
	deliverTxsRange(0, 100)

	// Deliver 0 to 999, we should reap 900 new txs
	// because 100 were already counted.
	deliverTxsRange(0, 1000)

	// Commit from the conensus AppConn
	commitRange(0, 500)
	updateRange(0, 500)

	// Deliver 100 invalid txs and 100 valid txs
	deliverTxsRange(900, 1100)
}

func TestMempoolCloseWAL(t *testing.T) {
	// 1. Create the temporary directory for mempool and WAL testing.
	rootDir, err := ioutil.TempDir("", "mempool-test")
	require.Nil(t, err, "expecting successful tmpdir creation")
	defer os.RemoveAll(rootDir)

	// 2. Ensure that it doesn't contain any elements -- Sanity check
	m1, err := filepath.Glob(filepath.Join(rootDir, "*"))
	require.Nil(t, err, "successful globbing expected")
	require.Equal(t, 0, len(m1), "no matches yet")

	// 3. Create the mempool
	wcfg := cfg.DefaultMempoolConfig()
	wcfg.RootDir = rootDir
	defer os.RemoveAll(wcfg.RootDir)
	txvotepool := NewTxVoteSetPool(wcfg, 10)
	txvotepool.InitWAL()

	// 4. Ensure that the directory contains the WAL file
	m2, err := filepath.Glob(filepath.Join(rootDir, "*"))
	require.Nil(t, err, "successful globbing expected")
	require.Equal(t, 1, len(m2), "expecting the wal match in")

	// 5. Write some contents to the WAL
	txVoteSet, _, _ := RandTxVoteSet(1, 1, 1)
	txvotepool.CheckTx(txVoteSet)
	walFilepath := txvotepool.wal.Path
	sum1 := checksumFile(walFilepath, t)

	// 6. Sanity check to ensure that the written TX matches the expectation.
	require.Equal(t, sum1, checksumIt([]byte("foo\n")), "foo with a newline should be written")

	// 7. Invoke CloseWAL() and ensure it discards the
	// WAL thus any other write won't go through.
	txvotepool.CloseWAL()
	txVoteSet, _, _ = RandTxVoteSet(1, 1, 1)
	txvotepool.CheckTx(txVoteSet)
	sum2 := checksumFile(walFilepath, t)
	require.Equal(t, sum1, sum2, "expected no change to the WAL after invoking CloseWAL() since it was discarded")

	// 8. Sanity check to ensure that the WAL file still exists
	m3, err := filepath.Glob(filepath.Join(rootDir, "*"))
	require.Nil(t, err, "successful globbing expected")
	require.Equal(t, 1, len(m3), "expecting the wal match in")
}

// Size of the amino encoded TxMessage is the length of the
// encoded byte array, plus 1 for the struct field, plus 4
// for the amino prefix.
func txMessageSize(tx types.TxVoteSet) int {
	return tx.Size() + 1 + 4 + 1
}

func TestMempoolMaxMsgSize(t *testing.T) {
	app := kvstore.NewKVStoreApplication()
	cc := proxy.NewLocalClientCreator(app)
	txvotepool, _, cleanup := newMempoolWithApp(cc)
	defer cleanup()

	testCases := []struct {
		len int
		err bool
	}{
		// check small txs. no error
		{10, false},
		{1000, false},
		{1000000, false},

		// check around maxTxSize
		// changes from no error to error
		{maxTxSize - 2, false},
		{maxTxSize - 1, false},
		{maxTxSize, false},
		{maxTxSize + 1, true},
		{maxTxSize + 2, true},

		// check around maxMsgSize. all error
		{maxMsgSize - 1, true},
		{maxMsgSize, true},
		{maxMsgSize + 1, true},
	}

	for i, testCase := range testCases {
		caseString := fmt.Sprintf("case %d, len %d", i, testCase.len)

		txVoteSet, _, _ := RandTxVoteSet(1, 1, 1)
		err := txvotepool.CheckTx(txVoteSet)
		msg := &TxVoteSetMessage{txVoteSet}
		encoded := cdc.MustMarshalBinaryBare(msg)
		require.Equal(t, len(encoded), txMessageSize(txVoteSet), caseString)
		if !testCase.err {
			require.True(t, len(encoded) <= maxMsgSize, caseString)
			require.NoError(t, err, caseString)
		} else {
			require.True(t, len(encoded) > maxMsgSize, caseString)
			require.Equal(t, err, mempool.ErrTxTooLarge, caseString)
		}
	}

}

func TestMempoolTxsBytes(t *testing.T) {
	app := kvstore.NewKVStoreApplication()
	cc := proxy.NewLocalClientCreator(app)
	config := cfg.ResetTestRoot("mempool_test")
	config.Mempool.MaxTxsBytes = 10
	txvotepool, _, cleanup := newMempoolWithAppAndConfig(cc, config)
	defer cleanup()

	// 1. zero by default
	assert.EqualValues(t, 0, txvotepool.TxsBytes())

	// 2. len(tx) after CheckTx
	txVoteSet, _, _ := RandTxVoteSet(1, 1, 1)
	err := txvotepool.CheckTx(txVoteSet)
	require.NoError(t, err)
	assert.EqualValues(t, 1, txvotepool.TxsBytes())

	// 3. zero again after tx is removed by Update
	txVoteSet, _, _ = RandTxVoteSet(1, 1, 1)
	txvotepool.Update(1, []types.TxVoteSet{txVoteSet})
	assert.EqualValues(t, 0, txvotepool.TxsBytes())

	// 4. zero after Flush
	txVoteSet, _, _ = RandTxVoteSet(2, 1, 1)
	err = txvotepool.CheckTx(txVoteSet)
	require.NoError(t, err)
	assert.EqualValues(t, 2, txvotepool.TxsBytes())

	txvotepool.Flush()
	assert.EqualValues(t, 0, txvotepool.TxsBytes())

	// 5. ErrMempoolIsFull is returned when/if MaxTxsBytes limit is reached.
	txVoteSet, _, _ = RandTxVoteSet(4, 1, 1)
	err = txvotepool.CheckTx(txVoteSet)
	require.NoError(t, err)
	txVoteSet, _, _ = RandTxVoteSet(5, 1, 1)
	err = txvotepool.CheckTx(txVoteSet)
	if assert.Error(t, err) {
		assert.IsType(t, mempool.ErrMempoolIsFull{}, err)
	}

	// 6. zero after tx is rechecked and removed due to not being valid anymore
	app2 := counter.NewCounterApplication(true)
	cc = proxy.NewLocalClientCreator(app2)
	txvotepool, _, cleanup = newMempoolWithApp(cc)
	defer cleanup()

	txVoteSet, _, _ = RandTxVoteSet(0, 1, 1)
	err = txvotepool.CheckTx(txVoteSet)
	require.NoError(t, err)
	assert.EqualValues(t, 8, txvotepool.TxsBytes())

	appConnCon, _ := cc.NewABCIClient()
	appConnCon.SetLogger(log.TestingLogger().With("module", "abci-client", "connection", "consensus"))
	err = appConnCon.Start()
	require.Nil(t, err)

	// Pretend like we committed nothing so txBytes gets rechecked and removed.
	txvotepool.Update(0, []types.TxVoteSet{})
	assert.EqualValues(t, 0, txvotepool.TxsBytes())
}

func checksumIt(data []byte) string {
	h := sha256.New()
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func checksumFile(p string, t *testing.T) string {
	data, err := ioutil.ReadFile(p)
	require.Nil(t, err, "expecting successful read of %q", p)
	return checksumIt(data)
}
