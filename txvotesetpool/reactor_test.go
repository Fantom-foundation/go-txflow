package txvotepool

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

	"github.com/Fantom-foundation/go-txflow/types"
	"github.com/tendermint/tendermint/abci/example/kvstore"
	cfg "github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/p2p/mock"
	"github.com/tendermint/tendermint/proxy"
	ttypes "github.com/tendermint/tendermint/types"
)

type peerState struct {
	height int64
}

func (ps peerState) GetHeight() int64 {
	return ps.height
}

// mempoolLogger is a TestingLogger which uses a different
// color for each validator ("validator" key must exist).
func mempoolLogger() log.Logger {
	return log.TestingLoggerWithColorFn(func(keyvals ...interface{}) term.FgBgColor {
		for i := 0; i < len(keyvals)-1; i += 2 {
			if keyvals[i] == "validator" {
				return term.FgBgColor{Fg: term.Color(uint8(keyvals[i+1].(int) + 1))}
			}
		}
		return term.FgBgColor{}
	})
}

// connect N mempool reactors through N switches
func makeAndConnectMempoolReactors(config *cfg.Config, N int) []*Reactor {
	reactors := make([]*Reactor, N)
	logger := mempoolLogger()
	for i := 0; i < N; i++ {
		app := kvstore.NewKVStoreApplication()
		cc := proxy.NewLocalClientCreator(app)
		txvotepool, mempool, cleanup := newMempoolWithApp(cc)
		defer cleanup()

		reactors[i] = NewReactor(config.Mempool, mempool, txvotepool, nil, nil) // so we dont start the consensus states
		reactors[i].SetLogger(logger.With("validator", i))
	}

	p2p.MakeConnectedSwitches(config.P2P, N, func(i int, s *p2p.Switch) *p2p.Switch {
		s.AddReactor("MEMPOOL", reactors[i])
		return s

	}, p2p.Connect2Switches)
	return reactors
}

// wait for all txs on all reactors
func waitForTxs(t *testing.T, txs []types.TxVoteSet, reactors []*Reactor) {
	// wait for the txs in all mempools
	wg := new(sync.WaitGroup)
	for i := 0; i < len(reactors); i++ {
		wg.Add(1)
		go _waitForTxs(t, wg, txs, i, reactors)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.After(TIMEOUT)
	select {
	case <-timer:
		t.Fatal("Timed out waiting for txs")
	case <-done:
	}
}

// wait for all txs on a single mempool
func _waitForTxs(t *testing.T, wg *sync.WaitGroup, txs []types.TxVoteSet, reactorIdx int, reactors []*Reactor) {

	mempool := reactors[reactorIdx].txVoteSetPool
	for mempool.Size() != len(txs) {
		time.Sleep(time.Millisecond * 100)
	}

	reapedTxs := mempool.ReapMaxTxs(len(txs))
	for i, tx := range txs {
		assert.Equal(t, tx, reapedTxs[i], fmt.Sprintf("txs at index %d on reactor %d don't match: %v vs %v", i, reactorIdx, tx, reapedTxs[i]))
	}
	wg.Done()
}

// ensure no txs on reactor after some timeout
func ensureNoTxs(t *testing.T, reactor *Reactor, timeout time.Duration) {
	time.Sleep(timeout) // wait for the txs in all mempools
	assert.Zero(t, reactor.txVoteSetPool.Size())
}

const (
	NUM_TXS = 1000
	TIMEOUT = 120 * time.Second // ridiculously high because CircleCI is slow
)

func TestReactorBroadcastTxMessage(t *testing.T) {
	config := cfg.TestConfig()
	const N = 4
	reactors := makeAndConnectMempoolReactors(config, N)
	defer func() {
		for _, r := range reactors {
			r.Stop()
		}
	}()
	for _, r := range reactors {
		for _, peer := range r.Switch.Peers().List() {
			peer.Set(ttypes.PeerStateKey, peerState{1})
		}
	}

	// send a bunch of txs to the first reactor's mempool
	// and wait for them all to be received in the others
	txs := checkTxs(t, reactors[0].txVoteSetPool, NUM_TXS, UnknownPeerID)
	waitForTxs(t, txs, reactors)
}

func TestReactorNoBroadcastToSender(t *testing.T) {
	config := cfg.TestConfig()
	const N = 2
	reactors := makeAndConnectMempoolReactors(config, N)
	defer func() {
		for _, r := range reactors {
			r.Stop()
		}
	}()

	// send a bunch of txs to the first reactor's mempool, claiming it came from peer
	// ensure peer gets no txs
	checkTxs(t, reactors[0].txVoteSetPool, NUM_TXS, 1)
	ensureNoTxs(t, reactors[1], 100*time.Millisecond)
}

func TestBroadcastTxForPeerStopsWhenPeerStops(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	config := cfg.TestConfig()
	const N = 2
	reactors := makeAndConnectMempoolReactors(config, N)
	defer func() {
		for _, r := range reactors {
			r.Stop()
		}
	}()

	// stop peer
	sw := reactors[1].Switch
	sw.StopPeerForError(sw.Peers().List()[0], errors.New("some reason"))

	// check that we are not leaking any go-routines
	// i.e. broadcastTxRoutine finishes when peer is stopped
	leaktest.CheckTimeout(t, 10*time.Second)()
}

func TestBroadcastTxForPeerStopsWhenReactorStops(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	config := cfg.TestConfig()
	const N = 2
	reactors := makeAndConnectMempoolReactors(config, N)

	// stop reactors
	for _, r := range reactors {
		r.Stop()
	}

	// check that we are not leaking any go-routines
	// i.e. broadcastTxRoutine finishes when reactor is stopped
	leaktest.CheckTimeout(t, 10*time.Second)()
}

func TestMempoolIDsBasic(t *testing.T) {
	ids := newTxVoteSetPoolIDs()

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
	ids := newTxVoteSetPoolIDs()

	for i := 0; i < maxActiveIDs-1; i++ {
		peer := mock.NewPeer(net.IP{127, 0, 0, 1})
		ids.ReserveForPeer(peer)
	}

	assert.Panics(t, func() {
		peer := mock.NewPeer(net.IP{127, 0, 0, 1})
		ids.ReserveForPeer(peer)
	})
}
