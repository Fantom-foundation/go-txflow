package mempool

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/andrecronje/babble-abci/types"
	"github.com/tendermint/tendermint/abci/example/kvstore"
	"github.com/tendermint/tendermint/proxy"
)

func TestCacheRemove(t *testing.T) {
	cache := newMapEventsCache(100)
	numEvents := 10
	events := make([]types.Event, numEvents)
	for i := 0; i < numEvents; i++ {
		// probability of collision is 2**-256
		events[i] = types.Event{}
		events[i].Height = int64(i)
		cache.Push(events[i])
		// make sure its added to both the linked list and the map
		require.Equal(t, i+1, len(cache.map_))
		require.Equal(t, i+1, cache.list.Len())
	}
	for i := 0; i < numEvents; i++ {
		cache.Remove(events[i])
		// make sure its removed from both the map and the linked list
		require.Equal(t, numEvents-(i+1), len(cache.map_))
		require.Equal(t, numEvents-(i+1), cache.list.Len())
	}
}

func TestCacheAfterUpdate(t *testing.T) {
	app := kvstore.NewKVStoreApplication()
	cc := proxy.NewLocalClientCreator(app)
	eventpool, cleanup := newEventpoolWithApp(cc)
	defer cleanup()

	// reAddIndices & txsInCache can have elements > numTxsToCreate
	// also assumes max index is 255 for convenience
	// txs in cache also checks order of elements
	tests := []struct {
		numEventsToCreate int
		updateIndices     []int
		reAddIndices      []int
		eventsInCache     []int
	}{
		{1, []int{}, []int{1}, []int{1, 0}},    // adding new events works
		{2, []int{1}, []int{}, []int{1, 0}},    // update doesn't remove events from cache
		{2, []int{2}, []int{}, []int{2, 1, 0}}, // update adds new events to cache
		{2, []int{1}, []int{1}, []int{1, 0}},   // re-adding after update doesn't make duplicate
	}
	for tcIndex, tc := range tests {
		for i := 0; i < tc.numEventsToCreate; i++ {
			event := types.Event{}
			event.Height = int64(i)
			err := eventpool.CheckEvent(event, nil)
			require.NoError(t, err)
		}

		updateEvents := []types.Event{}
		for _, v := range tc.updateIndices {
			event := types.Event{}
			event.Height = int64(v)
			updateEvents = append(updateEvents, event)
		}
		eventpool.Update(int64(tcIndex), updateEvents)

		for _, v := range tc.reAddIndices {
			event := types.Event{}
			event.Height = int64(v)
			_ = eventpool.CheckEvent(event, nil)
		}

		cache := eventpool.cache.(*mapEventsCache)
		node := cache.list.Front()
		counter := 0
		for node != nil {
			require.NotEqual(t, len(tc.eventsInCache), counter,
				"cache larger than expected on testcase %d", tcIndex)

			//nodeVal := node.Value.([sha256.Size]byte)
			//expectedBz := sha256.Sum256([]byte{byte(tc.eventsInCache[len(tc.eventsInCache)-counter-1])})
			// Reference for reading the errors:
			// >>> sha256('\x00').hexdigest()
			// '6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d'
			// >>> sha256('\x01').hexdigest()
			// '4bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a'
			// >>> sha256('\x02').hexdigest()
			// 'dbc1b4c900ffe48d575b5da5c638040125f65db0fe3e24494b76ea986457d986'

			//require.Equal(t, expectedBz, nodeVal, "Equality failed on index %d, tc %d", counter, tcIndex)
			counter++
			node = node.Next()
		}
		require.Equal(t, len(tc.eventsInCache), counter,
			"cache smaller than expected on testcase %d", tcIndex)
		eventpool.Flush()
	}
}
