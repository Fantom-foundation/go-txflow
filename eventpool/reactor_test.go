package mempool

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/go-kit/kit/log/term"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/andrecronje/babble-abci/types"
	"github.com/tendermint/tendermint/abci/example/kvstore"
	cfg "github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/p2p/mock"
	"github.com/tendermint/tendermint/proxy"
)

type peerState struct {
	height int64
}

func (ps peerState) GetHeight() int64 {
	return ps.height
}

// eventpoolLogger is a TestingLogger which uses a different
// color for each validator ("validator" key must exist).
func eventpoolLogger() log.Logger {
	return log.TestingLoggerWithColorFn(func(keyvals ...interface{}) term.FgBgColor {
		for i := 0; i < len(keyvals)-1; i += 2 {
			if keyvals[i] == "validator" {
				return term.FgBgColor{Fg: term.Color(uint8(keyvals[i+1].(int) + 1))}
			}
		}
		return term.FgBgColor{}
	})
}

// connect N eventpool reactors through N switches
func makeAndConnectEventpoolReactors(config *cfg.Config, N int) []*EventpoolReactor {
	reactors := make([]*EventpoolReactor, N)
	logger := eventpoolLogger()
	for i := 0; i < N; i++ {
		app := kvstore.NewKVStoreApplication()
		cc := proxy.NewLocalClientCreator(app)
		eventpool, cleanup := newEventpoolWithApp(cc)
		defer cleanup()

		reactors[i] = NewEventpoolReactor(config.Mempool, eventpool) // so we dont start the consensus states
		reactors[i].SetLogger(logger.With("validator", i))
	}

	p2p.MakeConnectedSwitches(config.P2P, N, func(i int, s *p2p.Switch) *p2p.Switch {
		s.AddReactor("EVENTPOOL", reactors[i])
		return s

	}, p2p.Connect2Switches)
	return reactors
}

// wait for all events on all reactors
func waitForEvents(t *testing.T, events []types.Event, reactors []*EventpoolReactor) {
	// wait for the events in all eventpools
	wg := new(sync.WaitGroup)
	for i := 0; i < len(reactors); i++ {
		wg.Add(1)
		go _waitForEvents(t, wg, events, i, reactors)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.After(TIMEOUT)
	select {
	case <-timer:
		t.Fatal("Timed out waiting for events")
	case <-done:
	}
}

// wait for all events on a single eventpool
func _waitForEvents(t *testing.T, wg *sync.WaitGroup, events []types.Event, reactorIdx int, reactors []*EventpoolReactor) {

	eventpool := reactors[reactorIdx].Eventpool
	for eventpool.Size() != len(events) {
		time.Sleep(time.Millisecond * 100)
	}

	reapedEvents := eventpool.ReapMaxEvents(len(events))
	for i, event := range events {
		assert.Equal(t, event, reapedEvents[i], fmt.Sprintf("events at index %d on reactor %d don't match: %v vs %v", i, reactorIdx, event, reapedEvents[i]))
	}
	wg.Done()
}

// ensure no events on reactor after some timeout
func ensureNoEvents(t *testing.T, reactor *EventpoolReactor, timeout time.Duration) {
	time.Sleep(timeout) // wait for the txs in all eventpools
	assert.Zero(t, reactor.Eventpool.Size())
}

const (
	NUM_EVENTS = 1000
	TIMEOUT    = 120 * time.Second // ridiculously high because CircleCI is slow
)

func TestReactorBroadcastEventMessage(t *testing.T) {
	config := cfg.TestConfig()
	const N = 4
	reactors := makeAndConnectEventpoolReactors(config, N)
	defer func() {
		for _, r := range reactors {
			r.Stop()
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(types.PeerStateKey, peerState{1})
		}
	}

	// send a bunch of events to the first reactor's eventpool
	// and wait for them all to be received in the others
	events := checkEvents(t, reactors[0].Eventpool, NUM_EVENTS, UnknownPeerID)
	waitForEvents(t, events, reactors)
}

func TestReactorNoBroadcastToSender(t *testing.T) {
	config := cfg.TestConfig()
	const N = 2
	reactors := makeAndConnectEventpoolReactors(config, N)
	defer func() {
		for _, r := range reactors {
			r.Stop()
		}
	}()

	// send a bunch of events to the first reactor's eventpool, claiming it came from peer
	// ensure peer gets no events
	checkEvents(t, reactors[0].Eventpool, NUM_EVENTS, 1)
	ensureNoEvents(t, reactors[1], 100*time.Millisecond)
}

func TestBroadcastEventForPeerStopsWhenPeerStops(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	config := cfg.TestConfig()
	const N = 2
	reactors := makeAndConnectEventpoolReactors(config, N)
	defer func() {
		for _, r := range reactors {
			r.Stop()
		}
	}()

	// stop peer
	sw := reactors[1].Switch
	sw.StopPeerForError(sw.Peers().List()[0], errors.New("some reason"))

	// check that we are not leaking any go-routines
	// i.e. broadcastEvenyRoutine finishes when peer is stopped
	leaktest.CheckTimeout(t, 10*time.Second)()
}

func TestBroadcastEventForPeerStopsWhenReactorStops(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	config := cfg.TestConfig()
	const N = 2
	reactors := makeAndConnectEventpoolReactors(config, N)

	// stop reactors
	for _, r := range reactors {
		r.Stop()
	}

	// check that we are not leaking any go-routines
	// i.e. broadcastEventRoutine finishes when reactor is stopped
	leaktest.CheckTimeout(t, 10*time.Second)()
}

func TestMempoolIDsBasic(t *testing.T) {
	ids := newEventpoolIDs()

	peer := mock.NewPeer(net.IP{127, 0, 0, 1})

	ids.ReserveForPeer(peer)
	assert.EqualValues(t, 1, ids.GetForPeer(peer))
	ids.Reclaim(peer)

	ids.ReserveForPeer(peer)
	assert.EqualValues(t, 2, ids.GetForPeer(peer))
	ids.Reclaim(peer)
}

func TestMempoolIDsPanicsIfNodeRequestsOvermaxActiveIDs(t *testing.T) {
	if testing.Short() {
		return
	}

	// 0 is already reserved for UnknownPeerID
	ids := newEventpoolIDs()

	for i := 0; i < maxActiveIDs-1; i++ {
		peer := mock.NewPeer(net.IP{127, 0, 0, 1})
		ids.ReserveForPeer(peer)
	}

	assert.Panics(t, func() {
		peer := mock.NewPeer(net.IP{127, 0, 0, 1})
		ids.ReserveForPeer(peer)
	})
}
