package mempool

import (
	"encoding/binary"
	"testing"

	"github.com/tendermint/tendermint/abci/example/kvstore"
	"github.com/tendermint/tendermint/proxy"
)

func BenchmarkCheckEvent(b *testing.B) {
	app := kvstore.NewKVStoreApplication()
	cc := proxy.NewLocalClientCreator(app)
	eventpool, cleanup := newEventpoolWithApp(cc)
	defer cleanup()

	for i := 0; i < b.N; i++ {
		event := make([]byte, 8)
		binary.BigEndian.PutUint64(event, uint64(i))
		eventpool.CheckEvent(event, nil)
	}
}

func BenchmarkCacheInsertTime(b *testing.B) {
	cache := newMapEventsCache(b.N)
	events := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		events[i] = make([]byte, 8)
		binary.BigEndian.PutUint64(events[i], uint64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Push(events[i])
	}
}

// This benchmark is probably skewed, since we actually will be removing
// events in parallel, which may cause some overhead due to mutex locking.
func BenchmarkCacheRemoveTime(b *testing.B) {
	cache := newMapEventsCache(b.N)
	events := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		events[i] = make([]byte, 8)
		binary.BigEndian.PutUint64(events[i], uint64(i))
		cache.Push(events[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Remove(events[i])
	}
}
