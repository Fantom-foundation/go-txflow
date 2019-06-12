package node

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/andrecronje/babble-abci/hashgraph"
	"github.com/andrecronje/babble-abci/net"
	b "github.com/mosaicnetworks/babble/src/node"
	"github.com/mosaicnetworks/babble/src/peers"
	"github.com/mosaicnetworks/babble/src/service"

	cfg "github.com/tendermint/tendermint/config"
	dbm "github.com/tendermint/tendermint/libs/db"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/proxy"
	"github.com/tendermint/tendermint/types"
)

// Node defines a babble node
type Node struct {
	// Node operations are implemented as a state-machine. The embedded state
	// object is used to manage the node's state.
	state

	conf *cfg.Config

	Store        hashgraph.Store
	Peers        *peers.PeerSet
	GenesisPeers *peers.PeerSet
	Service      *service.Service

	logger log.Logger

	// core is the link between the node and the underlying hashgraph. It
	// controls some higher-level operations like inserting a list of events,
	// keeping track of the peers list, fast-forwarding, etc.
	core     *Core
	coreLock sync.Mutex

	// transport is the object used to transmit and receive commands to other
	// nodes.
	trans net.Transport
	netCh <-chan net.RPC

	// proxy is the link between the node and the application. It is used to
	// commit blocks from Babble to the application, and relay submitted
	// transactions from the applications to Babble.
	proxy proxy.AppConnConsensus

	// submitCh is where the node listens for incoming transactions to be
	// submitted to Babble
	submitCh chan []byte

	// sigintCh is where the node listens for signals to politely leave the
	// Babble network.
	sigintCh chan os.Signal

	// shutdownCh is where the node listens for commands to cleanly shutdown.
	shutdownCh chan struct{}

	// The node runs the controlTimer in the background to periodically receive
	// signals to initiate gossip routines. It is paused, reset, etc., based on
	// the node's current state
	controlTimer *ControlTimer

	start          time.Time
	syncRequests   int
	syncErrors     int
	SyncLimit      int
	EnableFastSync bool
}

// NewInAppNode is a factory method that returns a Node instance
func NewInAppNode(conf *cfg.Config,
	validator *b.Validator,
	peers *peers.PeerSet,
	genesisPeers *peers.PeerSet,
	store hashgraph.Store,
	trans net.Transport,
	proxy proxy.AppConnConsensus,
	logger log.Logger,
) *Node {
	//Prepare sigintCh to relay SIGINT system calls
	sigintCh := make(chan os.Signal)
	signal.Notify(sigintCh, os.Interrupt, syscall.SIGINT)

	node := Node{
		conf:         conf,
		logger:       logger,
		core:         NewCore(validator, peers, genesisPeers, store, proxy, logger),
		trans:        trans,
		netCh:        trans.Consumer(),
		proxy:        proxy,
		sigintCh:     sigintCh,
		shutdownCh:   make(chan struct{}),
		controlTimer: NewRandomControlTimer(),
	}

	return &node
}

// MetricsProvider returns a consensus, p2p and mempool Metrics.
type MetricsProvider func(chainID string)

// DefaultMetricsProvider returns Metrics build using Prometheus client library
// if Prometheus is enabled. Otherwise, it returns no-op Metrics.
func DefaultMetricsProvider(config *cfg.InstrumentationConfig) MetricsProvider {
	return func(chainID string) {

	}
}

// GenesisDocProvider returns a GenesisDoc.
// It allows the GenesisDoc to be pulled from sources other than the
// filesystem, for instance from a distributed key-value store cluster.
type GenesisDocProvider func() (*types.GenesisDoc, error)

// DefaultGenesisDocProviderFunc returns a GenesisDocProvider that loads
// the GenesisDoc from the config.GenesisFile() on the filesystem.
func DefaultGenesisDocProviderFunc(config *cfg.Config) GenesisDocProvider {
	return func() (*types.GenesisDoc, error) {
		return types.GenesisDocFromFile(config.GenesisFile())
	}
}

// DBContext specifies config information for loading a new DB.
type DBContext struct {
	ID     string
	Config *cfg.Config
}

// DBProvider takes a DBContext and returns an instantiated DB.
type DBProvider func(*DBContext) (dbm.DB, error)

// DefaultDBProvider returns a database using the DBBackend and DBDir
// specified in the ctx.Config.
func DefaultDBProvider(ctx *DBContext) (dbm.DB, error) {
	dbType := dbm.DBBackendType(ctx.Config.DBBackend)
	return dbm.NewDB(ctx.ID, dbType, ctx.Config.DBDir()), nil
}

// NewNode is a factory method that returns a Tendermint Node instance
func NewNode(config *cfg.Config,
	privValidator types.PrivValidator,
	nodeKey *p2p.NodeKey,
	clientCreator proxy.ClientCreator,
	genesisDocProvider GenesisDocProvider,
	dbProvider DBProvider,
	metricsProvider MetricsProvider,
	logger log.Logger) (*Node, error) {

	logger.Info("Starting Babble")
	return nil, nil
}

/*******************************************************************************
Public Methods
*******************************************************************************/

// Init initialises the node based on its configuration. It controls the
// boostrap process which loads the hashgraph from an existing database (if
// bootstrap option is set in config). It also decides what state the node will
// start in (Babbling, CatchingUp, or Joining) based on the current
// validator-set and the value of the fast-sync option.
func (n *Node) Init() error {
	_, ok := n.core.peers.ByID[n.core.validator.ID()]
	if ok {
		n.logger.Debug("Node belongs to PeerSet")
		n.setBabblingOrCatchingUpState()
	} else {
		n.logger.Debug("Node does not belong to PeerSet => Joining")
		n.setState(Joining)
	}

	return nil
}

// Run invokes the main loop of the node. The gossip parameter controls whether
// to actively participate in gossip or not.
func (n *Node) Run(gossip bool) {
	// The ControlTimer allows the background routines to control the heartbeat
	// timer when the node is in the Babbling state. The timer should only be
	// running when there are uncommitted transactions in the system.
	go n.controlTimer.Run(n.conf.P2P.FlushThrottleTimeout)

	// Execute some background work regardless of the state of the node.
	go n.doBackgroundWork()

	//Execute Node State Machine
	for {
		//Run different routines depending on node state
		state := n.getState()

		n.logger.Debug("Run loop", "state", state.String())

		switch state {
		case Babbling:
			n.babble(gossip)
		case CatchingUp:
			n.fastForward()
		case Joining:
			n.join()
		case Shutdown:
			return
		}
	}
}

// RunAsync runs the node in a separate goroutine
func (n *Node) RunAsync(gossip bool) {
	n.logger.Debug("runasync", "gossip", gossip)
	go n.Run(gossip)
}

// Leave causes the node to politely leave the network via a LeaveRequest and
// wait for the node to be removed from the validator-list via consensus.
func (n *Node) Leave() error {
	n.logger.Debug("LEAVING")

	defer n.Shutdown()

	err := n.core.Leave(n.conf.P2P.HandshakeTimeout)
	if err != nil {
		n.logger.Error("Leaving", "err", err)
		return err
	}

	return nil
}

// Shutdown attempts to cleanly shutdown the node by waiting for pending work to
// be finished, stopping the control-timer, and closing the transport.
func (n *Node) Shutdown() {
	if n.getState() != Shutdown {
		n.logger.Debug("Shutdown")

		//Exit any non-shutdown state immediately
		n.setState(Shutdown)

		//Stop and wait for concurrent operations
		close(n.shutdownCh)

		n.waitRoutines()

		//For some reason this needs to be called after closing the shutdownCh
		//Not entirely sure why...
		n.controlTimer.Shutdown()

		//transport and store should only be closed once all concurrent operations
		//are finished otherwise they will panic trying to use close objects
		n.trans.Close()

		n.core.hashgraph.Store.Close()
	}
}

// GetID returns the numeric ID of the node's validator
func (n *Node) GetID() uint32 {
	return n.core.validator.ID()
}

// GetStats returns information about the node.
func (n *Node) GetStats() map[string]string {
	toString := func(i *int) string {
		if i == nil {
			return "nil"
		}

		return strconv.Itoa(*i)
	}

	timeElapsed := time.Since(n.start)

	consensusEvents := n.core.GetConsensusEventsCount()

	consensusEventsPerSecond := float64(consensusEvents) / timeElapsed.Seconds()

	lastConsensusRound := n.core.GetLastConsensusRoundIndex()

	var consensusRoundsPerSecond float64

	if lastConsensusRound != nil {
		consensusRoundsPerSecond = float64(*lastConsensusRound) / timeElapsed.Seconds()
	}

	s := map[string]string{
		"last_consensus_round":   toString(lastConsensusRound),
		"last_block_index":       strconv.Itoa(n.core.GetLastBlockIndex()),
		"consensus_events":       strconv.Itoa(consensusEvents),
		"consensus_transactions": strconv.Itoa(n.core.GetConsensusTransactionsCount()),
		"undetermined_events":    strconv.Itoa(len(n.core.GetUndeterminedEvents())),
		"transaction_pool":       strconv.Itoa(len(n.core.transactionPool)),
		"num_peers":              strconv.Itoa(n.core.peerSelector.Peers().Len()),
		"sync_rate":              strconv.FormatFloat(n.syncRate(), 'f', 2, 64),
		"events_per_second":      strconv.FormatFloat(consensusEventsPerSecond, 'f', 2, 64),
		"rounds_per_second":      strconv.FormatFloat(consensusRoundsPerSecond, 'f', 2, 64),
		"round_events":           strconv.Itoa(n.core.GetLastCommitedRoundEventsCount()),
		"id":                     fmt.Sprint(n.core.validator.ID()),
		"state":                  n.getState().String(),
		"moniker":                n.core.validator.Moniker,
	}
	return s
}

// GetBlock returns a block
func (n *Node) GetBlock(blockIndex int) (*hashgraph.Block, error) {
	return n.core.hashgraph.Store.GetBlock(blockIndex)
}

// GetPeers returns the current peers
func (n *Node) GetPeers() []*peers.Peer {
	return n.core.peers.Peers
}

// GetGenesisPeers returns the genesis peers
func (n *Node) GetGenesisPeers() []*peers.Peer {
	return n.core.genesisPeers.Peers
}

/*******************************************************************************
Background
*******************************************************************************/

// doBackgroundWork coninuously listens to incoming RPC commands, incoming
// transactions, and the sigint signal, regardless of the node's state.
func (n *Node) doBackgroundWork() {
	for {
		select {
		case rpc := <-n.netCh:
			n.goFunc(func() {
				n.logger.Debug("Processing RPC")
				n.processRPC(rpc)
				n.resetTimer()
			})
		case t := <-n.submitCh:
			n.logger.Debug("Adding Transaction")
			n.addTransaction(t)
			n.resetTimer()
		case <-n.shutdownCh:
			return
		case <-n.sigintCh:
			n.logger.Debug("Reacting to SIGINT - LEAVE")
			n.Leave()
			os.Exit(0)
		}
	}
}

// resetTimer resets the control timer to the configured hearbeat timeout, or
// slows it down if the node is not busy.
func (n *Node) resetTimer() {
	n.coreLock.Lock()
	defer n.coreLock.Unlock()

	if !n.controlTimer.set {
		ts := n.conf.P2P.FlushThrottleTimeout

		//Slow gossip if nothing interesting to say
		if !n.core.Busy() {
			ts = time.Duration(time.Second)
		}

		n.controlTimer.resetCh <- ts
	}
}

/*******************************************************************************
Babbling
*******************************************************************************/

// babble periodically initiates gossip or monologue as triggered by the
// controlTimer.
func (n *Node) babble(gossip bool) {
	n.logger.Debug("BABBLING")

	for {
		select {
		case <-n.controlTimer.tickCh:
			if gossip {
				n.logger.Debug("Time to gossip!")
				peer := n.core.peerSelector.Next()
				if peer != nil {
					n.goFunc(func() { n.gossip(peer) })
				} else {
					n.monologue()
				}
			}
			n.resetTimer()
		case <-n.shutdownCh:
			return
		}
	}
}

// monologue is called when the node is alone in the network but wants to record
// some events anyway.
func (n *Node) monologue() error {
	n.coreLock.Lock()
	defer n.coreLock.Unlock()

	if n.core.Busy() {
		err := n.core.AddSelfEvent("")
		if err != nil {
			n.logger.Error("monologue, AddSelfEvent()", "err", err)
			return err
		}

		err = n.core.ProcessSigPool()
		if err != nil {
			n.logger.Error("monologue, ProcessSigPool()", "err", err)
			return err
		}
	}

	return nil
}

// gossip performs a pull-push gossip operation with the selected peer.
func (n *Node) gossip(peer *peers.Peer) error {
	//pull
	otherKnownEvents, err := n.pull(peer)
	if err != nil {
		n.logger.Error("gossip pull", "err", err)
		return err
	}

	//push
	err = n.push(peer, otherKnownEvents)
	if err != nil {
		n.logger.Error("gossip push", "err", err)
		return err
	}

	//update peer selector
	n.core.selectorLock.Lock()
	n.core.peerSelector.UpdateLast(peer.ID())
	n.core.selectorLock.Unlock()

	n.logStats()

	return nil
}

// pull performs a SyncRequest and processes the response.
func (n *Node) pull(peer *peers.Peer) (otherKnownEvents map[uint32]int, err error) {
	//Compute Known
	n.coreLock.Lock()
	knownEvents := n.core.KnownEvents()
	n.coreLock.Unlock()

	//Send SyncRequest
	start := time.Now()
	resp, err := n.requestSync(peer.NetAddr, knownEvents, n.SyncLimit)
	elapsed := time.Since(start)
	n.logger.Debug("requestSync()", "duration", elapsed.Nanoseconds())

	if err != nil {
		n.logger.Error("requestSync()", "err", err)
		return nil, err
	}

	n.logger.Debug("SyncResponse",
		"from_id", resp.FromID,
		"events", len(resp.Events),
		"known", resp.Known,
	)

	//Add Events to Hashgraph and create new Head if necessary
	n.coreLock.Lock()
	err = n.sync(peer.ID(), resp.Events)
	n.coreLock.Unlock()

	if err != nil {
		n.logger.Error("sync()", "err", err)
		return nil, err
	}

	return resp.Known, nil
}

// push preforms an EagerSyncRequest
func (n *Node) push(peer *peers.Peer, knownEvents map[uint32]int) error {
	// Compute Diff
	start := time.Now()
	n.coreLock.Lock()
	eventDiff, err := n.core.EventDiff(knownEvents)
	n.coreLock.Unlock()
	elapsed := time.Since(start)
	n.logger.Debug("Diff()", "duration", elapsed.Nanoseconds())
	if err != nil {
		n.logger.Error("Calculating Diff", "err", err)
		return err
	}

	if len(eventDiff) > 0 {
		// do not push more than sync_limit events
		if n.SyncLimit < len(eventDiff) {
			n.logger.Debug("Push sync_limit",
				"sync_limit", n.SyncLimit,
				"diff_length", len(eventDiff),
			)
			eventDiff = eventDiff[:n.SyncLimit]
		}

		// Convert to WireEvents
		wireEvents, err := n.core.ToWire(eventDiff)
		if err != nil {
			n.logger.Debug("Converting to WireEvent", "err", err)
			return err
		}

		// Create and Send EagerSyncRequest
		start = time.Now()
		resp2, err := n.requestEagerSync(peer.NetAddr, wireEvents)
		elapsed = time.Since(start)
		n.logger.Debug("requestEagerSync()", "duration", elapsed.Nanoseconds())
		if err != nil {
			n.logger.Error("requestEagerSync()", "err", err)
			return err
		}
		n.logger.Debug("EagerSyncResponse",
			"from_id", resp2.FromID,
			"success", resp2.Success,
		)
	}

	return nil
}

// sync attempts to insert a list of events into the hashgraph, record a new
// sync event, and process the signature pool.
func (n *Node) sync(fromID uint32, events []hashgraph.WireEvent) error {
	//Insert Events in Hashgraph and create new Head if necessary
	start := time.Now()
	err := n.core.Sync(fromID, events)
	elapsed := time.Since(start)
	n.logger.Debug("Sync()", "duration", elapsed.Nanoseconds())
	if err != nil {
		n.logger.Error("err", err)
		return err
	}

	//Process SignaturePool
	start = time.Now()
	err = n.core.ProcessSigPool()
	elapsed = time.Since(start)
	n.logger.Debug("ProcessSigPool()", "duration", elapsed.Nanoseconds())
	if err != nil {
		n.logger.Error("err", err)
		return err
	}

	return nil
}

/*******************************************************************************
CatchingUp
*******************************************************************************/

// fastForward enacts "CatchingUp"
func (n *Node) fastForward() error {
	n.logger.Debug("CATCHING-UP")

	//wait until sync routines finish
	n.waitRoutines()

	var err error

	// loop through all peers to check who is the most ahead, then fast-forward
	// from them. If no-one is ready to fast-forward, transition to the Babbling
	// state.
	resp := n.getBestFastForwardResponse()
	if resp == nil {
		n.logger.Error("getBestFastForwardResponse returned nil => Babbling")
		n.setState(Babbling)
		return fmt.Errorf("getBestFastForwardResponse returned nil")
	}

	//update app from snapshot
	/*err = n.proxy.Restore(resp.Snapshot)
	if err != nil {
		n.logger.WithError(err).Error("Restoring App from Snapshot")
		return err
	}*/

	//prepare core. ie: fresh hashgraph
	n.coreLock.Lock()
	err = n.core.FastForward(&resp.Block, &resp.Frame)
	n.coreLock.Unlock()
	if err != nil {
		n.logger.Error("Fast Forwarding Hashgraph", "err", err)
		return err
	}

	err = n.core.ProcessAcceptedInternalTransactions(resp.Block.RoundReceived(), resp.Block.InternalTransactionReceipts())
	if err != nil {
		n.logger.Error("Processing AnchorBlock InternalTransactionReceipts", "err", err)
	}

	n.logger.Debug("FastForward OK")

	n.setState(Babbling)

	return nil
}

// getBestFastForwardResponse performs a FastForwardRequest with all known peers
// and only selects the one corresponding to the hightest block number.
func (n *Node) getBestFastForwardResponse() *net.FastForwardResponse {
	var bestResponse *net.FastForwardResponse
	maxBlock := 0

	for _, p := range n.core.peerSelector.Peers().Peers {
		start := time.Now()
		resp, err := n.requestFastForward(p.NetAddr)
		elapsed := time.Since(start)
		n.logger.Debug("requestFastForward()", "duration", elapsed.Nanoseconds())
		if err != nil {
			n.logger.Error("requestFastForward()", "err", err)
			continue
		}

		n.logger.Debug("FastForwardResponse",
			"from_id", resp.FromID,
			"block_index", resp.Block.Index(),
			"block_round_received", resp.Block.RoundReceived(),
			"frame_events", len(resp.Frame.Events),
			"frame_roots", resp.Frame.Roots,
			"frame_peers", len(resp.Frame.Peers),
			"snapshot", resp.Snapshot,
		)

		if resp.Block.Index() > maxBlock {
			bestResponse = &resp
			maxBlock = resp.Block.Index()
		}
	}

	return bestResponse
}

/*******************************************************************************
Joining
*******************************************************************************/

// join attempts to add the node's validator public-key to the current
// validator-set via an InternalTransaction which has to go through consensus.
func (n *Node) join() error {
	n.logger.Debug("JOINING")

	peer := n.core.peerSelector.Next()

	start := time.Now()
	resp, err := n.requestJoin(peer.NetAddr)
	elapsed := time.Since(start)
	n.logger.Debug("requestJoin()", "duration", elapsed.Nanoseconds())

	if err != nil {
		n.logger.Error("Cannot join:", peer.NetAddr, err)
		return err
	}

	n.logger.Debug("JoinResponse",
		"from_id", resp.FromID,
		"accepted", resp.Accepted,
		"accepted_round", resp.AcceptedRound,
		"peers", len(resp.Peers),
	)

	if resp.Accepted {
		n.core.AcceptedRound = resp.AcceptedRound
		n.setBabblingOrCatchingUpState()
	} else {
		// Then JoinRequest was explicitely refused by the curren peer-set. This
		// is not an error.
		n.logger.Debug("JoinRequest refused. Shutting down.")
		n.Shutdown()
	}

	return nil
}

/*******************************************************************************
Utils
*******************************************************************************/

// setBabblingOrCatchingUpState sets the node's state to CatchingUp if fast-sync
// is enabled, or to Babbling if fast-sync is not enabled.
func (n *Node) setBabblingOrCatchingUpState() {
	if n.EnableFastSync {
		n.logger.Debug("FastSync enabled => CatchingUp")
		n.setState(CatchingUp)
	} else {
		n.logger.Debug("FastSync not enabled => Babbling")
		if err := n.core.SetHeadAndSeq(); err != nil {
			n.core.SetHeadAndSeq()
		}
		n.setState(Babbling)
	}
}

// addTransaction is a thread-safe function to add and incoming transaction to
// the core's transaction-pool.
func (n *Node) addTransaction(tx []byte) {
	n.coreLock.Lock()
	defer n.coreLock.Unlock()

	n.core.AddTransactions([][]byte{tx})
}

// logStats logs the output returned by GetStats()
func (n *Node) logStats() {
	stats := n.GetStats()

	n.logger.Debug("Stats",
		"last_consensus_round", stats["last_consensus_round"],
		"last_block_index", stats["last_block_index"],
		"consensus_events", stats["consensus_events"],
		"consensus_transactions", stats["consensus_transactions"],
		"undetermined_events", stats["undetermined_events"],
		"transaction_pool", stats["transaction_pool"],
		"num_peers", stats["num_peers"],
		"sync_rate", stats["sync_rate"],
		"events/s", stats["events_per_second"],
		"rounds/s", stats["rounds_per_second"],
		"round_events", stats["round_events"],
		"id", stats["id"],
		"state", stats["state"],
		"moniker", stats["moniker"],
	)
}

// syncRate computes the ratio of sync-errors over sync-requests
func (n *Node) syncRate() float64 {
	var syncErrorRate float64

	if n.syncRequests != 0 {
		syncErrorRate = float64(n.syncErrors) / float64(n.syncRequests)
	}

	return 1 - syncErrorRate
}

func (n *Node) requestSync(target string, known map[uint32]int, syncLimit int) (net.SyncResponse, error) {
	args := net.SyncRequest{
		FromID:    n.core.validator.ID(),
		SyncLimit: syncLimit,
		Known:     known,
	}

	var out net.SyncResponse

	err := n.trans.Sync(target, &args, &out)

	return out, err
}

func (n *Node) requestEagerSync(target string, events []hashgraph.WireEvent) (net.EagerSyncResponse, error) {
	args := net.EagerSyncRequest{
		FromID: n.core.validator.ID(),
		Events: events,
	}

	var out net.EagerSyncResponse

	err := n.trans.EagerSync(target, &args, &out)

	return out, err
}

func (n *Node) requestFastForward(target string) (net.FastForwardResponse, error) {
	n.logger.Debug("RequestFastForward()",
		"target", target,
	)

	args := net.FastForwardRequest{
		FromID: n.core.validator.ID(),
	}

	var out net.FastForwardResponse

	err := n.trans.FastForward(target, &args, &out)

	return out, err
}

func (n *Node) requestJoin(target string) (net.JoinResponse, error) {

	joinTx := hashgraph.NewInternalTransactionJoin(*peers.NewPeer(
		n.core.validator.PublicKeyHex(),
		n.trans.LocalAddr(),
		n.core.validator.Moniker))

	joinTx.Sign(n.core.validator.Key)

	args := net.JoinRequest{InternalTransaction: joinTx}

	var out net.JoinResponse

	err := n.trans.Join(target, &args, &out)

	return out, err
}

func (n *Node) processRPC(rpc net.RPC) {
	// Notify others that we are not in Babbling state to prevent
	// them from hitting timeouts.
	if n.state.state != Babbling {
		n.logger.Debug("Not in Babbling state", "state", n.state.state)
		rpc.Respond(nil, fmt.Errorf("Not in Babbling state"))
		return
	}

	switch cmd := rpc.Command.(type) {
	case *net.SyncRequest:
		n.processSyncRequest(rpc, cmd)
	case *net.EagerSyncRequest:
		n.processEagerSyncRequest(rpc, cmd)
	case *net.FastForwardRequest:
		n.processFastForwardRequest(rpc, cmd)
	case *net.JoinRequest:
		n.processJoinRequest(rpc, cmd)
	default:
		n.logger.Error("Unexpected RPC command", "cmd", rpc.Command)
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

func (n *Node) processSyncRequest(rpc net.RPC, cmd *net.SyncRequest) {
	n.logger.Debug("process SyncRequest",
		"from_id", cmd.FromID,
		"sync_limit", cmd.SyncLimit,
		"known", cmd.Known,
	)

	resp := &net.SyncResponse{
		FromID: n.core.validator.ID(),
	}

	var respErr error

	//Compute Diff
	start := time.Now()
	n.coreLock.Lock()
	eventDiff, err := n.core.EventDiff(cmd.Known)
	n.coreLock.Unlock()
	elapsed := time.Since(start)

	n.logger.Debug("Diff()", "duration", elapsed.Nanoseconds())

	if err != nil {
		n.logger.Error("Calculating Diff", "error", err)
		respErr = err
	}

	if len(eventDiff) > 0 {

		//select min(cmd.SyncLimit, this.SyncLimit) events
		limit := min(cmd.SyncLimit, n.SyncLimit)

		n.logger.Debug("Selecting max events",
			"req.sync_limit", cmd.SyncLimit,
			"own.sync_limit", n.SyncLimit,
			"diff_length", len(eventDiff),
		)

		if limit < len(eventDiff) {
			eventDiff = eventDiff[:limit]
		}

		//Convert to WireEvents
		wireEvents, err := n.core.ToWire(eventDiff)
		if err != nil {
			n.logger.Debug("Converting to WireEvent", "error", err)
			respErr = err
		} else {
			resp.Events = wireEvents
		}

	}

	//Get Self Known
	n.coreLock.Lock()
	knownEvents := n.core.KnownEvents()
	n.coreLock.Unlock()

	resp.Known = knownEvents

	n.logger.Debug("Responding to SyncRequest",
		"events", len(resp.Events),
		"known", resp.Known,
		"rpc_err", respErr,
	)

	rpc.Respond(resp, respErr)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (n *Node) processEagerSyncRequest(rpc net.RPC, cmd *net.EagerSyncRequest) {
	n.logger.Debug("EagerSyncRequest",
		"from_id", cmd.FromID,
		"events", len(cmd.Events),
	)

	success := true

	n.coreLock.Lock()
	err := n.sync(cmd.FromID, cmd.Events)
	n.coreLock.Unlock()

	if err != nil {
		n.logger.Error("sync()", "error", err)
		success = false
	}

	resp := &net.EagerSyncResponse{
		FromID:  n.core.validator.ID(),
		Success: success,
	}

	rpc.Respond(resp, err)
}

func (n *Node) processFastForwardRequest(rpc net.RPC, cmd *net.FastForwardRequest) {
	n.logger.Debug("process FastForwardRequest",
		"from", cmd.FromID,
	)

	resp := &net.FastForwardResponse{
		FromID: n.core.validator.ID(),
	}

	var respErr error

	//Get latest Frame
	n.coreLock.Lock()
	block, frame, err := n.core.GetAnchorBlockWithFrame()
	n.coreLock.Unlock()

	if err != nil {
		n.logger.Error("Getting Frame", "err", err)
		respErr = err
	} else {
		resp.Block = *block
		resp.Frame = *frame

		//Get snapshot
		/*snapshot, err := n.proxy.GetSnapshot(block.Index())

		if err != nil {
			n.logger.WithField("error", err).Error("Getting Snapshot")
			respErr = err
		} else {
			resp.Snapshot = snapshot
		}*/
	}

	n.logger.Debug("Responding to FastForwardRequest",
		"events", len(resp.Frame.Events),
		"block", resp.Block.Index(),
		"round_received", resp.Block.RoundReceived(),
		"rpc_err", respErr,
	)

	rpc.Respond(resp, respErr)
}

func (n *Node) processJoinRequest(rpc net.RPC, cmd *net.JoinRequest) {
	n.logger.Debug("process JoinRequest",
		"peer", cmd.InternalTransaction.Body.Peer,
	)

	var respErr error
	var accepted bool
	var acceptedRound int
	var peers []*peers.Peer

	if ok, _ := cmd.InternalTransaction.Verify(); !ok {

		respErr = fmt.Errorf("Unable to verify signature on join request")

		n.logger.Debug("Unable to verify signature on join request")

	} else if _, ok := n.core.peers.ByPubKey[cmd.InternalTransaction.Body.Peer.PubKeyString()]; ok {

		n.logger.Debug("JoinRequest peer is already present")

		accepted = true

		//Get current peerset and accepted round
		lastConsensusRound := n.core.GetLastConsensusRoundIndex()
		if lastConsensusRound != nil {
			acceptedRound = *lastConsensusRound
		}

		peers = n.core.peers.Peers

	} else {
		//XXX run this by the App first
		//Dispatch the InternalTransaction
		n.coreLock.Lock()
		promise := n.core.AddInternalTransaction(cmd.InternalTransaction)
		n.coreLock.Unlock()

		//Wait for the InternalTransaction to go through consensus
		timeout := time.After(n.conf.P2P.DialTimeout)
		select {
		case resp := <-promise.RespCh:
			accepted = resp.Accepted
			acceptedRound = resp.AcceptedRound
			peers = resp.Peers
		case <-timeout:
			respErr = fmt.Errorf("Timeout waiting for JoinRequest to go through consensus")
			n.logger.Error("err", respErr)
			break
		}
	}

	resp := &net.JoinResponse{
		FromID:        n.core.validator.ID(),
		Accepted:      accepted,
		AcceptedRound: acceptedRound,
		Peers:         peers,
	}

	n.logger.Debug("Responding to JoinRequest",
		"accepted", resp.Accepted,
		"accepted_round", resp.AcceptedRound,
		"peers", len(resp.Peers),
		"rpc_err", respErr,
	)

	rpc.Respond(resp, respErr)
}
