package eventpool

import (
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	amino "github.com/tendermint/go-amino"

	"github.com/Fantom-foundation/go-txflow/config"
	"github.com/Fantom-foundation/go-txflow/types"
	"github.com/tendermint/tendermint/abci/example/counter"
	"github.com/tendermint/tendermint/abci/example/kvstore"
	abciserver "github.com/tendermint/tendermint/abci/server"
	abci "github.com/tendermint/tendermint/abci/types"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/proxy"
	ttypes "github.com/tendermint/tendermint/types"
)

// A cleanupFunc cleans up any config / test files created for a particular
// test.
type cleanupFunc func()

func newEventpoolWithApp(cc proxy.ClientCreator) (*Eventpool, cleanupFunc) {
	return newEventpoolWithAppAndConfig(cc, config.TestEventpoolConfig())
}

func newEventpoolWithAppAndConfig(cc proxy.ClientCreator, config *config.EventpoolConfig) (*Eventpool, cleanupFunc) {
	appConnMem, _ := cc.NewABCIClient()
	appConnMem.SetLogger(log.TestingLogger().With("module", "abci-client", "connection", "eventpool"))
	err := appConnMem.Start()
	if err != nil {
		panic(err)
	}
	eventpool := NewEventpool(config, 0)
	eventpool.SetLogger(log.TestingLogger())
	return eventpool, func() { os.RemoveAll(config.RootDir) }
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

func checkEvents(t *testing.T, eventpool *Eventpool, count int, peerID uint16) []*types.EventBlock {
	events := make([]*types.EventBlock, count)
	eventInfo := EventInfo{PeerID: peerID}
	for i := 0; i < count; i++ {
		events[i] = &types.EventBlock{}
		events[i].Height = int64(i)
		if err := eventpool.CheckEventWithInfo(events[i], nil, eventInfo); err != nil {
			// Skip invalid events.
			// TestEventpoolFilters will fail otherwise. It asserts a number of events
			// returned.
			if IsPreCheckError(err) {
				continue
			}
			t.Fatalf("CheckEvent failed: %v while checking #%d tx", err, i)
		}
	}
	return events
}

func TestEventpoolUpdateAddsEventsToCache(t *testing.T) {
	app := kvstore.NewKVStoreApplication()
	cc := proxy.NewLocalClientCreator(app)
	eventpool, cleanup := newEventpoolWithApp(cc)
	defer cleanup()
	event := &types.EventBlock{}
	event.Height = 1
	eventpool.Update(1, []*types.EventBlock{event})
	err := eventpool.CheckEvent(event, nil)
	if assert.Error(t, err) {
		assert.Equal(t, ErrEventInCache, err)
	}
}

func TestEventsAvailable(t *testing.T) {
	app := kvstore.NewKVStoreApplication()
	cc := proxy.NewLocalClientCreator(app)
	eventpool, cleanup := newEventpoolWithApp(cc)
	defer cleanup()
	eventpool.EnableEventsAvailable()

	timeoutMS := 500

	// with no events, it shouldnt fire
	ensureNoFire(t, eventpool.EventsAvailable(), timeoutMS)

	// send a bunch of events, it should only fire once
	events := checkEvents(t, eventpool, 100, UnknownPeerID)
	ensureFire(t, eventpool.EventsAvailable(), timeoutMS)
	ensureNoFire(t, eventpool.EventsAvailable(), timeoutMS)

	// call update with half the events.
	// it should fire once now for the new height
	// since there are still events left
	committedEvents, events := events[:50], events[50:]
	if err := eventpool.Update(1, committedEvents); err != nil {
		t.Error(err)
	}
	ensureFire(t, eventpool.EventsAvailable(), timeoutMS)
	ensureNoFire(t, eventpool.EventsAvailable(), timeoutMS)

	// send a bunch more events. we already fired for this height so it shouldnt fire again
	moreEvents := checkEvents(t, eventpool, 50, UnknownPeerID)
	ensureNoFire(t, eventpool.EventsAvailable(), timeoutMS)

	// now call update with all the events. it should not fire as there are no events left
	committedEvents = append(events, moreEvents...)
	if err := eventpool.Update(2, committedEvents); err != nil {
		t.Error(err)
	}
	ensureNoFire(t, eventpool.EventsAvailable(), timeoutMS)

	// send a bunch more events, it should only fire once
	checkEvents(t, eventpool, 100, UnknownPeerID)
	ensureFire(t, eventpool.EventsAvailable(), timeoutMS)
	ensureNoFire(t, eventpool.EventsAvailable(), timeoutMS)
}

func TestSerialReap(t *testing.T) {
	app := counter.NewCounterApplication(true)
	app.SetOption(abci.RequestSetOption{Key: "serial", Value: "on"})
	cc := proxy.NewLocalClientCreator(app)

	eventpool, cleanup := newEventpoolWithApp(cc)
	defer cleanup()

	appConnCon, _ := cc.NewABCIClient()
	appConnCon.SetLogger(log.TestingLogger().With("module", "abci-client", "connection", "consensus"))
	err := appConnCon.Start()
	require.Nil(t, err)

	cacheMap := make(map[string]struct{})
	deliverEventsRange := func(start, end int) {
		// Deliver some events.
		for i := start; i < end; i++ {

			// This will succeed
			event := &types.EventBlock{}
			event.Height = int64(i)
			err := eventpool.CheckEvent(event, nil)
			_, cached := cacheMap[EventID(event)]
			if cached {
				require.NotNil(t, err, "expected error for cached event")
			} else {
				require.Nil(t, err, "expected no err for uncached event")
			}
			cacheMap[EventID(event)] = struct{}{}

			// Duplicates are cached and should return error
			err = eventpool.CheckEvent(event, nil)
			require.NotNil(t, err, "Expected error after CheckEvent on duplicated event")
		}
	}

	updateRange := func(start, end int) {
		events := make([]*types.EventBlock, 0)
		for i := start; i < end; i++ {
			event := &types.EventBlock{}
			event.Height = int64(i)
			events = append(events, event)
		}
		if err := eventpool.Update(0, events); err != nil {
			t.Error(err)
		}
	}

	commitRange := func(start, end int) {
		// Deliver some events.
		for i := start; i < end; i++ {
			event := &types.EventBlock{}
			event.Height = int64(i)
		}
	}

	//----------------------------------------

	// Deliver some events.
	deliverEventsRange(0, 100)

	// Deliver 0 to 999, we should reap 900 new events
	// because 100 were already counted.
	deliverEventsRange(0, 1000)

	// Commit from the conensus AppConn
	commitRange(0, 500)
	updateRange(0, 500)

	// Deliver 100 invalid events and 100 valid events
	deliverEventsRange(900, 1100)
}

func TestEventpoolCloseWAL(t *testing.T) {
	// 1. Create the temporary directory for eventpool and WAL testing.
	rootDir, err := ioutil.TempDir("", "eventpool-test")
	require.Nil(t, err, "expecting successful tmpdir creation")
	defer os.RemoveAll(rootDir)

	// 2. Ensure that it doesn't contain any elements -- Sanity check
	m1, err := filepath.Glob(filepath.Join(rootDir, "*"))
	require.Nil(t, err, "successful globbing expected")
	require.Equal(t, 0, len(m1), "no matches yet")

	// 3. Create the eventpool
	wcfg := config.DefaultEventpoolConfig()
	wcfg.RootDir = rootDir
	defer os.RemoveAll(wcfg.RootDir)
	eventpool := NewEventpool(wcfg, 10)
	eventpool.InitWAL()

	// 4. Ensure that the directory contains the WAL file
	m2, err := filepath.Glob(filepath.Join(rootDir, "*"))
	require.Nil(t, err, "successful globbing expected")
	require.Equal(t, 1, len(m2), "expecting the wal match in")

	// 5. Write some contents to the WAL
	event := &types.EventBlock{}
	event.Height = 1
	eventpool.CheckEvent(event, nil)
	walFilepath := eventpool.wal.Path
	sum1 := checksumFile(walFilepath, t)

	// 6. Sanity check to ensure that the written event matches the expectation.
	require.Equal(t, sum1, checksumIt([]byte("foo\n")), "foo with a newline should be written")

	// 7. Invoke CloseWAL() and ensure it discards the
	// WAL thus any other write won't go through.
	eventpool.CloseWAL()
	event.Height = 2
	eventpool.CheckEvent(event, nil)
	sum2 := checksumFile(walFilepath, t)
	require.Equal(t, sum1, sum2, "expected no change to the WAL after invoking CloseWAL() since it was discarded")

	// 8. Sanity check to ensure that the WAL file still exists
	m3, err := filepath.Glob(filepath.Join(rootDir, "*"))
	require.Nil(t, err, "successful globbing expected")
	require.Equal(t, 1, len(m3), "expecting the wal match in")
}

// Size of the amino encoded EventMessage is the length of the
// encoded byte array, plus 1 for the struct field, plus 4
// for the amino prefix.
func eventMessageSize(event *types.EventBlock) int {
	return amino.ByteSliceSize(cdc.MustMarshalBinaryBare(event)) + 1 + 4
}

func TestEventpoolMaxMsgSize(t *testing.T) {
	app := kvstore.NewKVStoreApplication()
	cc := proxy.NewLocalClientCreator(app)
	eventpool, cleanup := newEventpoolWithApp(cc)
	defer cleanup()

	testCases := []struct {
		len int
		err bool
	}{
		// check small events. no error
		{10, false},
		{1000, false},
		{1000000, false},

		// check around maxEventSize
		// changes from no error to error
		{maxEventSize - 2, false},
		{maxEventSize - 1, false},
		{maxEventSize, false},
		{maxEventSize + 1, true},
		{maxEventSize + 2, true},

		// check around maxMsgSize. all error
		{maxMsgSize - 1, true},
		{maxMsgSize, true},
		{maxMsgSize + 1, true},
	}

	for i, testCase := range testCases {
		caseString := fmt.Sprintf("case %d, len %d", i, testCase.len)

		event := &types.EventBlock{}
		event.Data.Txs = ttypes.Txs{cmn.RandBytes(testCase.len)}
		err := eventpool.CheckEvent(event, nil)
		msg := &EventMessage{event}
		encoded := cdc.MustMarshalBinaryBare(msg)
		require.Equal(t, len(encoded), eventMessageSize(event), caseString)
		if !testCase.err {
			require.True(t, len(encoded) <= maxMsgSize, caseString)
			require.NoError(t, err, caseString)
		} else {
			require.True(t, len(encoded) > maxMsgSize, caseString)
			require.Equal(t, err, ErrEventTooLarge, caseString)
		}
	}

}

func TestEventpoolEventsBytes(t *testing.T) {
	app := kvstore.NewKVStoreApplication()
	cc := proxy.NewLocalClientCreator(app)
	config := config.TestEventpoolConfig()
	config.MaxEventsBytes = 10
	eventpool, cleanup := newEventpoolWithAppAndConfig(cc, config)
	defer cleanup()

	// 1. zero by default
	assert.EqualValues(t, 0, eventpool.EventsBytes())
	// 2. len(event) after CheckEvent
	event := &types.EventBlock{}
	event.Height = 1
	err := eventpool.CheckEvent(event, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 1, eventpool.EventsBytes())

	// 3. zero again after event is removed by Update
	eventpool.Update(1, []*types.EventBlock{event})
	assert.EqualValues(t, 0, eventpool.EventsBytes())

	// 6. zero after event is rechecked and removed due to not being valid anymore
	app2 := counter.NewCounterApplication(true)
	cc = proxy.NewLocalClientCreator(app2)
	eventpool, cleanup = newEventpoolWithApp(cc)
	defer cleanup()

	err = eventpool.CheckEvent(event, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 8, eventpool.EventsBytes())

	// Pretend like we committed nothing so eventBytes gets rechecked and removed.
	eventpool.Update(1, []*types.EventBlock{})
	assert.EqualValues(t, 0, eventpool.EventsBytes())
}

// This will non-deterministically catch some concurrency failures like
// https://github.com/tendermint/tendermint/issues/3509
// TODO: all of the tests should probably also run using the remote proxy app
// since otherwise we're not actually testing the concurrency of the eventpool here!
func TestEventpoolRemoteAppConcurrency(t *testing.T) {
	sockPath := fmt.Sprintf("unix:///tmp/echo_%v.sock", cmn.RandStr(6))
	app := kvstore.NewKVStoreApplication()
	cc, server := newRemoteApp(t, sockPath, app)
	defer server.Stop()
	config := config.TestEventpoolConfig()
	eventpool, cleanup := newEventpoolWithAppAndConfig(cc, config)
	defer cleanup()

	// generate small number of txs
	nEvents := 10
	eventLen := 200
	events := make([]*types.EventBlock, nEvents)
	for i := 0; i < nEvents; i++ {
		events[i].Data.Txs = []ttypes.Tx{cmn.RandBytes(eventLen)}
	}

	// simulate a group of peers sending them over and over
	N := config.Size
	maxPeers := 5
	for i := 0; i < N; i++ {
		peerID := mrand.Intn(maxPeers)
		eventNum := mrand.Intn(nEvents)
		event := events[int(eventNum)]

		// this will err with ErrEventInCache many times ...
		eventpool.CheckEventWithInfo(event, nil, EventInfo{PeerID: uint16(peerID)})
	}
}

// caller must close server
func newRemoteApp(t *testing.T, addr string, app abci.Application) (clientCreator proxy.ClientCreator, server cmn.Service) {
	clientCreator = proxy.NewRemoteClientCreator(addr, "socket", true)

	// Start server
	server = abciserver.NewSocketServer(addr, app)
	server.SetLogger(log.TestingLogger().With("module", "abci-server"))
	if err := server.Start(); err != nil {
		t.Fatalf("Error starting socket server: %v", err.Error())
	}
	return clientCreator, server
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
