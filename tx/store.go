package tx

import (
	"fmt"
	"sync"

	cmn "github.com/tendermint/tendermint/libs/common"
	dbm "github.com/tendermint/tm-cmn/db"

	"github.com/Fantom-foundation/go-txflow/types"
)

/*
TxStore is a simple low level store for approved transactions.

There are three types of information stored:
 - TxMeta:   Meta information about each tx
 - Tx:       Parts of each tx
 - Commit:   The commit part of each tx, for gossiping votes

Currently the commit signatures are duplicated in the Tx as
well as the Commit.  In the future this may change, perhaps by moving
the Commit data outside the Block. (TODO)

// NOTE: TxStore methods will panic if they encounter errors
// deserializing loaded data, indicating probable corruption on disk.
*/
type TxStore struct {
	db dbm.DB

	mtx    sync.RWMutex
	height int64
}

// NewTxStore returns a new TxStore with the given DB,
// initialized to the last height that was committed to the DB.
func NewTxStore(db dbm.DB) *TxStore {
	bsjson := LoadTxStoreStateJSON(db)
	return &TxStore{
		height: bsjson.Height,
		db:     db,
	}
}

// Height returns the last known contiguous block height.
func (ts *TxStore) Height() int64 {
	ts.mtx.RLock()
	defer ts.mtx.RUnlock()
	return ts.height
}

// LoadTx returns the tx for the given hash.
// If no tx is found for the given hash, it returns nil.
func (ts *TxStore) LoadTx(txHash string) *types.TxVoteSet {
	var tx = new(types.TxVoteSet)
	bz := ts.db.Get(calcTxKey(txHash))
	if len(bz) == 0 {
		return nil
	}
	err := cdc.UnmarshalBinaryBare(bz, tx)
	if err != nil {
		panic(cmn.ErrorWrap(err, "Error reading tx meta"))
	}
	return tx
}

// LoadTxCommit returns the Commit for the given txHash.
// This commit consists of the +2/3 and other votes for tx,
func (ts *TxStore) LoadTxCommit(txHash string) *types.Commit {
	var commit = new(types.Commit)
	bz := ts.db.Get(calcTxCommitKey(txHash))
	if len(bz) == 0 {
		return nil
	}
	err := cdc.UnmarshalBinaryBare(bz, commit)
	if err != nil {
		panic(cmn.ErrorWrap(err, "Error reading block commit"))
	}
	return commit
}

// SaveTx persists the given tx to the underlying db.
func (ts *TxStore) SaveTx(tx *types.TxVoteSet) {
	if tx == nil {
		panic("TxStore can only save a non-nil tx")
	}
	height := tx.Height()

	// Save tx
	txBytes := cdc.MustMarshalBinaryBare(tx)
	ts.db.Set(calcTxKey(tx.TxHash), txBytes)

	// Save tx commit (duplicate and separate from the Block)
	txCommitBytes := cdc.MustMarshalBinaryBare(tx.MakeCommit())
	ts.db.Set(calcTxCommitKey(tx.TxHash), txCommitBytes)

	// Save new TxStoreStateJSON descriptor
	TxStoreStateJSON{Height: height}.Save(ts.db)

	// Done!
	ts.mtx.Lock()
	ts.height = height
	ts.mtx.Unlock()

	// Flush
	ts.db.SetSync(nil, nil)
}

//-----------------------------------------------------------------------------

func calcTxKey(txHash string) []byte {
	return []byte(fmt.Sprintf("H:%X", txHash))
}

func calcTxCommitKey(txHash string) []byte {
	return []byte(fmt.Sprintf("C:%X", txHash))
}

//-----------------------------------------------------------------------------

var txStoreKey = []byte("txStore")

type TxStoreStateJSON struct {
	Height int64 `json:"height"`
}

// Save persists the txStore state to the database as JSON.
func (tsj TxStoreStateJSON) Save(db dbm.DB) {
	bytes, err := cdc.MarshalJSON(tsj)
	if err != nil {
		panic(fmt.Sprintf("Could not marshal state bytes: %v", err))
	}
	db.SetSync(txStoreKey, bytes)
}

// LoadTxStoreStateJSON returns the TxStoreStateJSON as loaded from disk.
// If no TxStoreStateJSON was previously persisted, it returns the zero value.
func LoadTxStoreStateJSON(db dbm.DB) TxStoreStateJSON {
	bytes := db.Get(txStoreKey)
	if len(bytes) == 0 {
		return TxStoreStateJSON{
			Height: 0,
		}
	}
	tsj := TxStoreStateJSON{}
	err := cdc.UnmarshalJSON(bytes, &tsj)
	if err != nil {
		panic(fmt.Sprintf("Could not unmarshal bytes: %X", bytes))
	}
	return tsj
}
