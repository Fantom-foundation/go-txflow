package txvotepool

import (
	"testing"
	"time"

	"github.com/Fantom-foundation/go-txflow/types"
	"github.com/tendermint/tendermint/abci/example/kvstore"
	"github.com/tendermint/tendermint/proxy"
)

func BenchmarkReap(b *testing.B) {
	app := kvstore.NewKVStoreApplication()
	cc := proxy.NewLocalClientCreator(app)
	mempool, cleanup := newMempoolWithApp(cc)
	defer cleanup()

	size := 10000
	for i := 0; i < size; i++ {
		tx := types.TxVote{int64(i), nil, time.Now(), nil, nil}
		mempool.CheckTx(tx)
	}
	b.ResetTimer()
}

func BenchmarkCheckTx(b *testing.B) {
	app := kvstore.NewKVStoreApplication()
	cc := proxy.NewLocalClientCreator(app)
	mempool, cleanup := newMempoolWithApp(cc)
	defer cleanup()

	for i := 0; i < b.N; i++ {
		tx := types.TxVote{int64(i), nil, time.Now(), nil, nil}
		mempool.CheckTx(tx)
	}
}

func BenchmarkCacheInsertTime(b *testing.B) {
	cache := newMapTxCache(b.N)
	txs := make([]types.TxVote, b.N)
	for i := 0; i < b.N; i++ {
		txs[i] = types.TxVote{int64(i), nil, time.Now(), nil, nil}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Push(txs[i])
	}
}

// This benchmark is probably skewed, since we actually will be removing
// txs in parallel, which may cause some overhead due to mutex locking.
func BenchmarkCacheRemoveTime(b *testing.B) {
	cache := newMapTxCache(b.N)
	txs := make([]types.TxVote, b.N)
	for i := 0; i < b.N; i++ {
		txs[i] = types.TxVote{int64(i), nil, time.Now(), nil, nil}
		cache.Push(txs[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Remove(txs[i])
	}
}
