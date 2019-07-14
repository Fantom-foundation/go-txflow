package blockchain

import (
	"bytes"
	"fmt"
	"os"
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cfg "github.com/tendermint/tendermint/config"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/db"
	dbm "github.com/tendermint/tendermint/libs/db"
	"github.com/tendermint/tendermint/libs/log"
	sm "github.com/tendermint/tendermint/state"

	"github.com/Fantom-foundation/go-txflow/types"
)

// A cleanupFunc cleans up any config / test files created for a particular
// test.
type cleanupFunc func()

// make a Commit with a single vote containing just the height and a timestamp
func makeTestCommit(txHash cmn.HexBytes, timestamp time.Time) *types.Commit {
	commitSigs := []*types.CommitSig{{TxHash: txHash, Timestamp: timestamp}}
	return types.NewCommit(cmn.HexBytes{}, commitSigs)
}

func makeStateAndTxStore(logger log.Logger) (sm.State, *TxStore, cleanupFunc) {
	config := cfg.ResetTestRoot("blockchain_reactor_test")
	// blockDB := dbm.NewDebugDB("blockDB", dbm.NewMemDB())
	// stateDB := dbm.NewDebugDB("stateDB", dbm.NewMemDB())
	txDB := dbm.NewMemDB()
	stateDB := dbm.NewMemDB()
	state, err := sm.LoadStateFromDBOrGenesisFile(stateDB, config.GenesisFile())
	if err != nil {
		panic(cmn.ErrorWrap(err, "error constructing state from genesis file"))
	}
	return state, NewTxStore(txDB), func() { os.RemoveAll(config.RootDir) }
}

func TestLoadTxStoreStateJSON(t *testing.T) {
	db := db.NewMemDB()

	bsj := &TxStoreStateJSON{Height: 1000}
	bsj.Save(db)

	retrBSJ := LoadTxStoreStateJSON(db)

	assert.Equal(t, *bsj, retrBSJ, "expected the retrieved DBs to match")
}

func TestNewTxStore(t *testing.T) {
	db := db.NewMemDB()
	db.Set(txStoreKey, []byte(`{"height": "10000"}`))
	bs := NewTxStore(db)
	require.Equal(t, int64(10000), bs.Height(), "failed to properly parse txstore")

	panicCausers := []struct {
		data    []byte
		wantErr string
	}{
		{[]byte("artful-doger"), "not unmarshal bytes"},
		{[]byte(" "), "unmarshal bytes"},
	}

	for i, tt := range panicCausers {
		// Expecting a panic here on trying to parse an invalid blockStore
		_, _, panicErr := doFn(func() (interface{}, error) {
			db.Set(txStoreKey, tt.data)
			_ = NewTxStore(db)
			return nil, nil
		})
		require.NotNil(t, panicErr, "#%d panicCauser: %q expected a panic", i, tt.data)
		assert.Contains(t, fmt.Sprintf("%#v", panicErr), tt.wantErr, "#%d data: %q", i, tt.data)
	}

	db.Set(txStoreKey, nil)
	bs = NewTxStore(db)
	assert.Equal(t, bs.Height(), int64(0), "expecting nil bytes to be unmarshaled alright")
}

func freshBlockStore() (*TxStore, db.DB) {
	db := db.NewMemDB()
	return NewTxStore(db), db
}

var (
	state sm.State
	tx    *types.TxVoteSet
)

func TestMain(m *testing.M) {
	var cleanup cleanupFunc
	state, _, cleanup = makeStateAndTxStore(log.NewTMLogger(new(bytes.Buffer)))
	tx = makeTx([]byte("0x1"), state, new(types.Commit))
	code := m.Run()
	cleanup()
	os.Exit(code)
}

// TODO: This test should be simplified ...

func TestTxStoreSaveLoadBlock(t *testing.T) {
	state, ts, cleanup := makeStateAndTxStore(log.NewTMLogger(new(bytes.Buffer)))
	defer cleanup()
	require.Equal(t, ts.Height(), int64(0), "initially the height should be zero")

	// check there are no tx at various heights
	noTxHashes := [][]byte{[]byte("0"), []byte("-1"), []byte("100"), []byte("1000"), []byte("2")}
	for i, hash := range noTxHashes {
		if g := ts.LoadTx(hash); g != nil {
			t.Errorf("#%d: hash(%X) got a tx; want nil", i, hash)
		}
	}

	// save a block
	tx := makeTx([]byte("0"), state, new(types.Commit))
	ts.SaveTx(tx)
	require.Equal(t, ts.Height(), tx.Height, "expecting the new height to be changed")
}

func makeTx(txHash cmn.HexBytes, state sm.State, lastCommit *types.Commit) *types.TxVoteSet {
	tx := types.NewTxVoteSet(
		state.ChainID,
		state.LastBlockHeight,
		txHash,
		state.Validators,
	)
	return tx
}

func doFn(fn func() (interface{}, error)) (res interface{}, err error, panicErr error) {
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case error:
				panicErr = e
			case string:
				panicErr = fmt.Errorf("%s", e)
			default:
				if st, ok := r.(fmt.Stringer); ok {
					panicErr = fmt.Errorf("%s", st)
				} else {
					panicErr = fmt.Errorf("%s", debug.Stack())
				}
			}
		}
	}()

	res, err = fn()
	return res, err, panicErr
}
