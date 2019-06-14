package node

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/andrecronje/babble-abci/hashgraph"
	bnet "github.com/andrecronje/babble-abci/net"
	"github.com/andrecronje/babble-abci/peers"
	"github.com/andrecronje/babble/src/service"

	_ "net/http/pprof"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"

	amino "github.com/tendermint/go-amino"
	bc "github.com/tendermint/tendermint/blockchain"
	cfg "github.com/tendermint/tendermint/config"
	cs "github.com/tendermint/tendermint/consensus"
	"github.com/tendermint/tendermint/crypto/ed25519"
	"github.com/tendermint/tendermint/evidence"
	cmn "github.com/tendermint/tendermint/libs/common"
	dbm "github.com/tendermint/tendermint/libs/db"
	"github.com/tendermint/tendermint/libs/log"
	tmpubsub "github.com/tendermint/tendermint/libs/pubsub"
	mempl "github.com/tendermint/tendermint/mempool"
	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/p2p/pex"
	"github.com/tendermint/tendermint/privval"
	"github.com/tendermint/tendermint/proxy"
	rpccore "github.com/tendermint/tendermint/rpc/core"
	ctypes "github.com/tendermint/tendermint/rpc/core/types"
	grpccore "github.com/tendermint/tendermint/rpc/grpc"
	rpcserver "github.com/tendermint/tendermint/rpc/lib/server"
	sm "github.com/tendermint/tendermint/state"
	"github.com/tendermint/tendermint/state/txindex"
	"github.com/tendermint/tendermint/state/txindex/kv"
	"github.com/tendermint/tendermint/state/txindex/null"
	"github.com/tendermint/tendermint/types"
	tmtime "github.com/tendermint/tendermint/types/time"
	"github.com/tendermint/tendermint/version"
)

const DefaultKeyfile = "priv_key"

// Node defines a babble node
type Node struct {
	cmn.BaseService
	// Node operations are implemented as a state-machine. The embedded state
	// object is used to manage the node's state.
	state

	config     *cfg.Config
	genesisDoc *types.GenesisDoc // initial validator set
	addrBook   pex.AddrBook      // known peers

	Store         hashgraph.Store
	Peers         *peers.PeerSet
	GenesisPeers  *peers.PeerSet
	Service       *service.Service
	privValidator types.PrivValidator // local node's validator key

	logger log.Logger

	// core is the link between the node and the underlying hashgraph. It
	// controls some higher-level operations like inserting a list of events,
	// keeping track of the peers list, fast-forwarding, etc.
	core     *Core
	coreLock sync.Mutex

	// transport is the object used to transmit and receive commands to other
	// nodes.
	trans bnet.Transport
	netCh <-chan bnet.RPC

	// proxy is the link between the node and the application. It is used to
	// commit blocks from Babble to the application, and relay submitted
	// transactions from the applications to Babble.
	proxy        proxy.AppConnConsensus
	rpcListeners []net.Listener // rpc servers

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
	stateDB        dbm.DB
	blockStore     *bc.BlockStore // store the blockchain to disk
	prometheusSrv  *http.Server
	eventBus       *types.EventBus // pub/sub for services
	proxyApp       proxy.AppConns  // connection to the application
	txIndexer      txindex.TxIndexer
	evidencePool   *evidence.EvidencePool // tracking evidence
	indexerService *txindex.IndexerService

	// network
	nodeInfo    p2p.NodeInfo
	nodeKey     *p2p.NodeKey // our node privkey
	isListening bool
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

// NodeProvider takes a config and a logger and returns a ready to go Node.
type NodeProvider func(*cfg.Config, log.Logger) (*Node, error)

// DefaultNewNode returns a Tendermint node with default settings for the
// PrivValidator, ClientCreator, GenesisDoc, and DBProvider.
// It implements NodeProvider.
func DefaultNewNode(config *cfg.Config, logger log.Logger) (*Node, error) {
	// Generate node PrivKey
	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	if err != nil {
		return nil, err
	}

	// Convert old PrivValidator if it exists.
	oldPrivVal := config.OldPrivValidatorFile()
	newPrivValKey := config.PrivValidatorKeyFile()
	newPrivValState := config.PrivValidatorStateFile()
	if _, err := os.Stat(oldPrivVal); !os.IsNotExist(err) {
		oldPV, err := privval.LoadOldFilePV(oldPrivVal)
		if err != nil {
			return nil, fmt.Errorf("Error reading OldPrivValidator from %v: %v\n", oldPrivVal, err)
		}
		logger.Info("Upgrading PrivValidator file",
			"old", oldPrivVal,
			"newKey", newPrivValKey,
			"newState", newPrivValState,
		)
		oldPV.Upgrade(newPrivValKey, newPrivValState)
	}

	return NewNode(config,
		privval.LoadOrGenFilePV(newPrivValKey, newPrivValState),
		nodeKey,
		proxy.DefaultClientCreator(config.ProxyApp, config.ABCI, config.DBDir()),
		DefaultGenesisDocProviderFunc(config),
		DefaultDBProvider,
		DefaultMetricsProvider(config.Instrumentation),
		logger,
	)
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

	// Get BlockStore
	blockStoreDB, err := dbProvider(&DBContext{"blockstore", config})
	if err != nil {
		return nil, err
	}
	blockStore := bc.NewBlockStore(blockStoreDB)

	// Get State
	stateDB, err := dbProvider(&DBContext{"state", config})
	if err != nil {
		return nil, err
	}

	// Get genesis doc
	// TODO: move to state package?
	genDoc, err := loadGenesisDoc(stateDB)
	if err != nil {
		genDoc, err = genesisDocProvider()
		if err != nil {
			return nil, err
		}
		// save genesis doc to prevent a certain class of user errors (e.g. when it
		// was changed, accidentally or not). Also good for audit trail.
		saveGenesisDoc(stateDB, genDoc)
	}

	state, err := sm.LoadStateFromDBOrGenesisDoc(stateDB, genDoc)
	if err != nil {
		return nil, err
	}

	// Create the proxyApp and establish connections to the ABCI app (consensus, mempool, query).
	proxyApp := proxy.NewAppConns(clientCreator)
	proxyApp.SetLogger(logger.With("module", "proxy"))
	if err := proxyApp.Start(); err != nil {
		return nil, fmt.Errorf("Error starting proxy app connections: %v", err)
	}

	// EventBus and IndexerService must be started before the handshake because
	// we might need to index the txs of the replayed block as this might not have happened
	// when the node stopped last time (i.e. the node stopped after it saved the block
	// but before it indexed the txs, or, endblocker panicked)
	eventBus := types.NewEventBus()
	eventBus.SetLogger(logger.With("module", "events"))

	err = eventBus.Start()
	if err != nil {
		return nil, err
	}

	// Transaction indexing
	var txIndexer txindex.TxIndexer
	switch config.TxIndex.Indexer {
	case "kv":
		store, err := dbProvider(&DBContext{"tx_index", config})
		if err != nil {
			return nil, err
		}
		if config.TxIndex.IndexTags != "" {
			txIndexer = kv.NewTxIndex(store, kv.IndexTags(splitAndTrimEmpty(config.TxIndex.IndexTags, ",", " ")))
		} else if config.TxIndex.IndexAllTags {
			txIndexer = kv.NewTxIndex(store, kv.IndexAllTags())
		} else {
			txIndexer = kv.NewTxIndex(store)
		}
	default:
		txIndexer = &null.TxIndex{}
	}

	indexerService := txindex.NewIndexerService(txIndexer, eventBus)
	indexerService.SetLogger(logger.With("module", "txindex"))

	err = indexerService.Start()
	if err != nil {
		return nil, err
	}

	// Create the handshaker, which calls RequestInfo, sets the AppVersion on the state,
	// and replays any blocks as necessary to sync tendermint with the app.
	consensusLogger := logger.With("module", "consensus")
	/*handshaker := cs.NewHandshaker(stateDB, state, blockStore, genDoc)
	handshaker.SetLogger(consensusLogger)
	handshaker.SetEventBus(eventBus)
	if err := handshaker.Handshake(proxyApp); err != nil {
		return nil, fmt.Errorf("Error during handshake: %v", err)
	}*/

	// Reload the state. It will have the Version.Consensus.App set by the
	// Handshake, and may have other modifications as well (ie. depending on
	// what happened during block replay).
	state = sm.LoadState(stateDB)

	// Log the version info.
	logger.Info("Version info",
		"software", version.TMCoreSemVer,
		"block", version.BlockProtocol,
		"p2p", version.P2PProtocol,
	)

	// If the state and software differ in block version, at least log it.
	if state.Version.Consensus.Block != version.BlockProtocol {
		logger.Info("Software and state have different block protocols",
			"software", version.BlockProtocol,
			"state", state.Version.Consensus.Block,
		)
	}

	/*if config.PrivValidatorListenAddr != "" {
		// If an address is provided, listen on the socket for a connection from an
		// external signing process.
		// FIXME: we should start services inside OnStart
		privValidator, err = createAndStartPrivValidatorSocketClient(config.PrivValidatorListenAddr, logger)
		if err != nil {
			return nil, errors.Wrap(err, "Error with private validator socket client")
		}
	}*/

	// Decide whether to fast-sync or not
	// We don't fast-sync when the only validator is us.
	/*fastSync := config.FastSync
	if state.Validators.Size() == 1 {
		addr, _ := state.Validators.GetByIndex(0)
		privValAddr := privValidator.GetPubKey().Address()
		if bytes.Equal(privValAddr, addr) {
			fastSync = false
		}
	}*/

	pubKey := privValidator.GetPubKey()
	addr := pubKey.Address()
	// Log whether this node is a validator or an observer
	if state.Validators.HasAddress(addr) {
		consensusLogger.Info("This node is a validator", "addr", addr, "pubKey", pubKey)
	} else {
		consensusLogger.Info("This node is not a validator", "addr", addr, "pubKey", pubKey)
	}

	// Make Evidence Reactor
	evidenceDB, err := dbProvider(&DBContext{"evidence", config})
	if err != nil {
		return nil, err
	}
	evidenceLogger := logger.With("module", "evidence")
	evidencePool := evidence.NewEvidencePool(stateDB, evidenceDB)
	evidencePool.SetLogger(evidenceLogger)
	evidenceReactor := evidence.NewEvidenceReactor(evidencePool)
	evidenceReactor.SetLogger(evidenceLogger)

	//p2pLogger := logger.With("module", "p2p")
	nodeInfo, err := makeNodeInfo(
		config,
		nodeKey.ID(),
		txIndexer,
		genDoc.ChainID,
		p2p.NewProtocolVersion(
			version.P2PProtocol, // global
			state.Version.Consensus.Block,
			state.Version.Consensus.App,
		),
	)
	if err != nil {
		return nil, err
	}

	// run the profile server
	profileHost := config.ProfListenAddress
	if profileHost != "" {
		go func() {
			logger.Error("Profile server", "err", http.ListenAndServe(profileHost, nil))
		}()
	}

	//Prepare sigintCh to relay SIGINT system calls
	sigintCh := make(chan os.Signal)
	signal.Notify(sigintCh, os.Interrupt, syscall.SIGINT)

	//addrBook := pex.NewAddrBook(config.P2P.AddrBookFile(), config.P2P.AddrBookStrict)

	//logger.Debug("config:", "addrBook", addrBook)

	participants := peers.NewPeerSetFromValidators(state.Validators.Validators)
	// Set Genesis Peer Set from peers.genesis.json

	genesisPeerStore := peers.NewJSONPeerSet(config.BaseConfig.RootDir, false)
	genesisParticipants, err := genesisPeerStore.PeerSet()
	if err != nil { // If there is any error, the current peer set is used as the genesis peer set
		logger.Debug("could not read peers.genesis.json:", "err", err)
		genesisParticipants = participants
	}

	validator := NewValidator(nodeKey.PrivKey, config.BaseConfig.Moniker)

	p, ok := participants.ByID[validator.ID()]
	if ok {
		if p.Moniker != validator.Moniker {
			logger.Debug("Using moniker from peers.json file",
				"json_moniker", p.Moniker,
				"cli_moniker", validator.Moniker,
			)
			validator.Moniker = p.Moniker
		}
	}

	logger.Debug("PARTICIPANTS",
		"genesis_peers", len(genesisParticipants.Peers),
		"peers", len(participants.Peers),
		"id", validator.ID(),
		"moniker", validator.Moniker,
	)

	transport, err := bnet.NewTCPTransport(
		config.P2P.ListenAddress,
		nil,
		config.P2P.MaxNumInboundPeers,
		config.P2P.DialTimeout,
		config.P2P.HandshakeTimeout,
		logger,
	)

	if err != nil {
		logger.Debug("bnet.NewTCPTransport", "err", err)
		return nil, err
	}

	logger.Debug("BadgerDB", "path", config.BaseConfig.DBDir())
	dbpath := config.BaseConfig.DBDir()
	i := 1

	for {
		if _, err := os.Stat(dbpath); err == nil {
			logger.Debug("already exists", dbpath)

			dbpath = fmt.Sprintf("%s(%d)", config.BaseConfig.DBDir(), i)
			logger.Debug("No Bootstrap - using new db", dbpath)
			i++
		} else {
			break
		}
	}

	logger.Debug("Creating BadgerStore", "path", dbpath)

	dbStore, err := hashgraph.NewBadgerStore(config.Mempool.CacheSize, dbpath)
	if err != nil {
		return nil, err
	}

	node := &Node{
		config:        config,
		genesisDoc:    genDoc,
		privValidator: privValidator,

		nodeInfo:     nodeInfo,
		nodeKey:      nodeKey,
		evidencePool: evidencePool,

		stateDB:        stateDB,
		blockStore:     blockStore,
		proxyApp:       proxyApp,
		txIndexer:      txIndexer,
		indexerService: indexerService,
		eventBus:       eventBus,

		logger:       logger,
		core:         NewCore(validator, participants, genesisParticipants, dbStore, proxyApp.Consensus(), logger),
		trans:        transport,
		netCh:        transport.Consumer(),
		proxy:        proxyApp.Consensus(),
		sigintCh:     sigintCh,
		shutdownCh:   make(chan struct{}),
		controlTimer: NewRandomControlTimer(),
	}
	node.BaseService = *cmn.NewBaseService(logger, "Node", node)
	return node, nil
}

func (n *Node) OnStart() error {
	now := tmtime.Now()
	genTime := n.genesisDoc.GenesisTime
	if genTime.After(now) {
		n.Logger.Info("Genesis time is in the future. Sleeping until then...", "genTime", genTime)
		time.Sleep(genTime.Sub(now))
	}

	// Add private IDs to addrbook to block those peers being added

	//n.addrBook.AddPrivateIDs(splitAndTrimEmpty(n.config.P2P.PrivatePeerIDs, ",", " "))

	// Start the RPC server before the P2P server
	// so we can eg. receive txs for the first block
	if n.config.RPC.ListenAddress != "" {
		listeners, err := n.startRPC()
		if err != nil {
			return err
		}
		n.rpcListeners = listeners
	}

	if n.config.Instrumentation.Prometheus &&
		n.config.Instrumentation.PrometheusListenAddr != "" {
		n.prometheusSrv = n.startPrometheusServer(n.config.Instrumentation.PrometheusListenAddr)
	}

	// Start the transport.
	// TODO: Replace with Babble P2P
	/*addr, err := p2p.NewNetAddressStringWithOptionalID(n.config.P2P.ListenAddress)
	if err != nil {
		return err
	}
	if err := n.transport.Listen(*addr); err != nil {
		return err
	}

	n.isListening = true

	// Start the switch (the P2P server).
	err = n.sw.Start()
	if err != nil {
		return err
	}

	// Always connect to persistent peers
	if n.config.P2P.PersistentPeers != "" {
		err = n.sw.DialPeersAsync(n.addrBook, splitAndTrimEmpty(n.config.P2P.PersistentPeers, ",", " "), true)
		if err != nil {
			return err
		}
	}*/

	return nil
}

// OnStop stops the Node. It implements cmn.Service.
func (n *Node) OnStop() {
	n.BaseService.OnStop()

	n.Logger.Info("Stopping Node")

	// first stop the non-reactor services
	n.eventBus.Stop()
	//n.indexerService.Stop()

	// now stop the reactors
	// TODO: gracefully disconnect from peers.
	//n.sw.Stop()

	// stop mempool WAL
	/*if n.config.Mempool.WalEnabled() {
		n.mempoolReactor.Mempool.CloseWAL()
	}

	if err := n.transport.Close(); err != nil {
		n.Logger.Error("Error closing transport", "err", err)
	}

	n.isListening = false
	*/
	// finally stop the listeners / external services
	for _, l := range n.rpcListeners {
		n.Logger.Info("Closing rpc listener", "listener", l)
		if err := l.Close(); err != nil {
			n.Logger.Error("Error closing listener", "listener", l, "err", err)
		}
	}

	if pvsc, ok := n.privValidator.(cmn.Service); ok {
		pvsc.Stop()
	}

	if n.prometheusSrv != nil {
		if err := n.prometheusSrv.Shutdown(context.Background()); err != nil {
			// Error from closing listeners, or context timeout:
			n.Logger.Error("Prometheus HTTP server Shutdown", "err", err)
		}
	}
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
	go n.controlTimer.Run(n.config.P2P.FlushThrottleTimeout)

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

	err := n.core.Leave(n.config.P2P.HandshakeTimeout)
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
		ts := n.config.P2P.FlushThrottleTimeout

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
func (n *Node) getBestFastForwardResponse() *bnet.FastForwardResponse {
	var bestResponse *bnet.FastForwardResponse
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

func (n *Node) requestSync(target string, known map[uint32]int, syncLimit int) (bnet.SyncResponse, error) {
	args := bnet.SyncRequest{
		FromID:    n.core.validator.ID(),
		SyncLimit: syncLimit,
		Known:     known,
	}

	var out bnet.SyncResponse

	err := n.trans.Sync(target, &args, &out)

	return out, err
}

func (n *Node) requestEagerSync(target string, events []hashgraph.WireEvent) (bnet.EagerSyncResponse, error) {
	args := bnet.EagerSyncRequest{
		FromID: n.core.validator.ID(),
		Events: events,
	}

	var out bnet.EagerSyncResponse

	err := n.trans.EagerSync(target, &args, &out)

	return out, err
}

func (n *Node) requestFastForward(target string) (bnet.FastForwardResponse, error) {
	n.logger.Debug("RequestFastForward()",
		"target", target,
	)

	args := bnet.FastForwardRequest{
		FromID: n.core.validator.ID(),
	}

	var out bnet.FastForwardResponse

	err := n.trans.FastForward(target, &args, &out)

	return out, err
}

func (n *Node) requestJoin(target string) (bnet.JoinResponse, error) {

	joinTx := hashgraph.NewInternalTransactionJoin(*peers.NewPeer(
		n.core.validator.PublicKeyHex(),
		n.trans.LocalAddr(),
		n.core.validator.Moniker))

	joinTx.Sign(n.core.validator.Key)

	args := bnet.JoinRequest{InternalTransaction: joinTx}

	var out bnet.JoinResponse

	err := n.trans.Join(target, &args, &out)

	return out, err
}

func (n *Node) processRPC(rpc bnet.RPC) {
	// Notify others that we are not in Babbling state to prevent
	// them from hitting timeouts.
	if n.state.state != Babbling {
		n.logger.Debug("Not in Babbling state", "state", n.state.state)
		rpc.Respond(nil, fmt.Errorf("Not in Babbling state"))
		return
	}

	switch cmd := rpc.Command.(type) {
	case *bnet.SyncRequest:
		n.processSyncRequest(rpc, cmd)
	case *bnet.EagerSyncRequest:
		n.processEagerSyncRequest(rpc, cmd)
	case *bnet.FastForwardRequest:
		n.processFastForwardRequest(rpc, cmd)
	case *bnet.JoinRequest:
		n.processJoinRequest(rpc, cmd)
	default:
		n.logger.Error("Unexpected RPC command", "cmd", rpc.Command)
		rpc.Respond(nil, fmt.Errorf("unexpected command"))
	}
}

func (n *Node) processSyncRequest(rpc bnet.RPC, cmd *bnet.SyncRequest) {
	n.logger.Debug("process SyncRequest",
		"from_id", cmd.FromID,
		"sync_limit", cmd.SyncLimit,
		"known", cmd.Known,
	)

	resp := &bnet.SyncResponse{
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

func (n *Node) processEagerSyncRequest(rpc bnet.RPC, cmd *bnet.EagerSyncRequest) {
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

	resp := &bnet.EagerSyncResponse{
		FromID:  n.core.validator.ID(),
		Success: success,
	}

	rpc.Respond(resp, err)
}

func (n *Node) processFastForwardRequest(rpc bnet.RPC, cmd *bnet.FastForwardRequest) {
	n.logger.Debug("process FastForwardRequest",
		"from", cmd.FromID,
	)

	resp := &bnet.FastForwardResponse{
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

func (n *Node) processJoinRequest(rpc bnet.RPC, cmd *bnet.JoinRequest) {
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
		timeout := time.After(n.config.P2P.DialTimeout)
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

	resp := &bnet.JoinResponse{
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

// splitAndTrimEmpty slices s into all subslices separated by sep and returns a
// slice of the string s with all leading and trailing Unicode code points
// contained in cutset removed. If sep is empty, SplitAndTrim splits after each
// UTF-8 sequence. First part is equivalent to strings.SplitN with a count of
// -1.  also filter out empty strings, only return non-empty strings.
func splitAndTrimEmpty(s, sep, cutset string) []string {
	if s == "" {
		return []string{}
	}

	spl := strings.Split(s, sep)
	nonEmptyStrings := make([]string, 0, len(spl))
	for i := 0; i < len(spl); i++ {
		element := strings.Trim(spl[i], cutset)
		if element != "" {
			nonEmptyStrings = append(nonEmptyStrings, element)
		}
	}
	return nonEmptyStrings
}

func (n *Node) startRPC() ([]net.Listener, error) {
	n.ConfigureRPC()
	listenAddrs := splitAndTrimEmpty(n.config.RPC.ListenAddress, ",", " ")
	coreCodec := amino.NewCodec()
	ctypes.RegisterAmino(coreCodec)

	if n.config.RPC.Unsafe {
		rpccore.AddUnsafeRoutes()
	}

	// we may expose the rpc over both a unix and tcp socket
	listeners := make([]net.Listener, len(listenAddrs))
	for i, listenAddr := range listenAddrs {
		mux := http.NewServeMux()
		rpcLogger := n.Logger.With("module", "rpc-server")
		wmLogger := rpcLogger.With("protocol", "websocket")
		wm := rpcserver.NewWebsocketManager(rpccore.Routes, coreCodec,
			rpcserver.OnDisconnect(func(remoteAddr string) {
				err := n.eventBus.UnsubscribeAll(context.Background(), remoteAddr)
				if err != nil && err != tmpubsub.ErrSubscriptionNotFound {
					wmLogger.Error("Failed to unsubscribe addr from events", "addr", remoteAddr, "err", err)
				}
			}))
		wm.SetLogger(wmLogger)
		mux.HandleFunc("/websocket", wm.WebsocketHandler)
		rpcserver.RegisterRPCFuncs(mux, rpccore.Routes, coreCodec, rpcLogger)

		config := rpcserver.DefaultConfig()
		config.MaxOpenConnections = n.config.RPC.MaxOpenConnections
		// If necessary adjust global WriteTimeout to ensure it's greater than
		// TimeoutBroadcastTxCommit.
		// See https://github.com/tendermint/tendermint/issues/3435
		if config.WriteTimeout <= n.config.RPC.TimeoutBroadcastTxCommit {
			config.WriteTimeout = n.config.RPC.TimeoutBroadcastTxCommit + 1*time.Second
		}

		listener, err := rpcserver.Listen(
			listenAddr,
			config,
		)
		if err != nil {
			return nil, err
		}

		var rootHandler http.Handler = mux
		if n.config.RPC.IsCorsEnabled() {
			corsMiddleware := cors.New(cors.Options{
				AllowedOrigins: n.config.RPC.CORSAllowedOrigins,
				AllowedMethods: n.config.RPC.CORSAllowedMethods,
				AllowedHeaders: n.config.RPC.CORSAllowedHeaders,
			})
			rootHandler = corsMiddleware.Handler(mux)
		}
		if n.config.RPC.IsTLSEnabled() {
			go rpcserver.StartHTTPAndTLSServer(
				listener,
				rootHandler,
				n.config.RPC.CertFile(),
				n.config.RPC.KeyFile(),
				rpcLogger,
				config,
			)
		} else {
			go rpcserver.StartHTTPServer(
				listener,
				rootHandler,
				rpcLogger,
				config,
			)
		}

		listeners[i] = listener
	}

	// we expose a simplified api over grpc for convenience to app devs
	grpcListenAddr := n.config.RPC.GRPCListenAddress
	if grpcListenAddr != "" {
		config := rpcserver.DefaultConfig()
		config.MaxOpenConnections = n.config.RPC.MaxOpenConnections
		listener, err := rpcserver.Listen(grpcListenAddr, config)
		if err != nil {
			return nil, err
		}
		go grpccore.StartGRPCServer(listener)
		listeners = append(listeners, listener)
	}

	return listeners, nil
}

// startPrometheusServer starts a Prometheus HTTP server, listening for metrics
// collectors on addr.
func (n *Node) startPrometheusServer(addr string) *http.Server {
	srv := &http.Server{
		Addr: addr,
		Handler: promhttp.InstrumentMetricHandler(
			prometheus.DefaultRegisterer, promhttp.HandlerFor(
				prometheus.DefaultGatherer,
				promhttp.HandlerOpts{MaxRequestsInFlight: n.config.Instrumentation.MaxOpenConnections},
			),
		),
	}
	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			// Error starting or closing listener:
			n.Logger.Error("Prometheus HTTP server ListenAndServe", "err", err)
		}
	}()
	return srv
}

// ConfigureRPC sets all variables in rpccore so they will serve
// rpc calls from this node
func (n *Node) ConfigureRPC() {
	//rpccore.SetP2PTransport(n)
	pubKey := n.privValidator.GetPubKey()
	rpccore.SetPubKey(pubKey)
	rpccore.SetGenesisDoc(n.genesisDoc)
	rpccore.SetAddrBook(n.addrBook)
	rpccore.SetProxyAppQuery(n.proxyApp.Query())
	rpccore.SetTxIndexer(n.txIndexer)
	rpccore.SetEventBus(n.eventBus)
	rpccore.SetLogger(n.Logger.With("module", "rpc"))
	rpccore.SetConfig(*n.config.RPC)
}

var (
	genesisDocKey = []byte("genesisDoc")
)

// panics if failed to unmarshal bytes
func loadGenesisDoc(db dbm.DB) (*types.GenesisDoc, error) {
	bytes := db.Get(genesisDocKey)
	if len(bytes) == 0 {
		return nil, errors.New("Genesis doc not found")
	}
	var genDoc *types.GenesisDoc
	err := cdc.UnmarshalJSON(bytes, &genDoc)
	if err != nil {
		cmn.PanicCrisis(fmt.Sprintf("Failed to load genesis doc due to unmarshaling error: %v (bytes: %X)", err, bytes))
	}
	return genDoc, nil
}

// panics if failed to marshal the given genesis document
func saveGenesisDoc(db dbm.DB, genDoc *types.GenesisDoc) {
	bytes, err := cdc.MarshalJSON(genDoc)
	if err != nil {
		cmn.PanicCrisis(fmt.Sprintf("Failed to save genesis doc due to marshaling error: %v %v", err, genDoc))
	}
	db.SetSync(genesisDocKey, bytes)
}

func createAndStartPrivValidatorSocketClient(
	listenAddr string,
	logger log.Logger,
) (types.PrivValidator, error) {
	var listener net.Listener

	protocol, address := cmn.ProtocolAndAddress(listenAddr)
	ln, err := net.Listen(protocol, address)
	if err != nil {
		return nil, err
	}
	switch protocol {
	case "unix":
		listener = privval.NewUnixListener(ln)
	case "tcp":
		// TODO: persist this key so external signer
		// can actually authenticate us
		listener = privval.NewTCPListener(ln, ed25519.GenPrivKey())
	default:
		return nil, fmt.Errorf(
			"Wrong listen address: expected either 'tcp' or 'unix' protocols, got %s",
			protocol,
		)
	}

	pvsc := privval.NewSignerValidatorEndpoint(logger.With("module", "privval"), listener)
	if err := pvsc.Start(); err != nil {
		return nil, errors.Wrap(err, "failed to start private validator")
	}

	return pvsc, nil
}

func makeNodeInfo(
	config *cfg.Config,
	nodeID p2p.ID,
	txIndexer txindex.TxIndexer,
	chainID string,
	protocolVersion p2p.ProtocolVersion,
) (p2p.NodeInfo, error) {
	txIndexerStatus := "on"
	if _, ok := txIndexer.(*null.TxIndex); ok {
		txIndexerStatus = "off"
	}
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: protocolVersion,
		ID_:             nodeID,
		Network:         chainID,
		Version:         version.TMCoreSemVer,
		Channels: []byte{
			bc.BlockchainChannel,
			cs.StateChannel, cs.DataChannel, cs.VoteChannel, cs.VoteSetBitsChannel,
			mempl.MempoolChannel,
			evidence.EvidenceChannel,
		},
		Moniker: config.Moniker,
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    txIndexerStatus,
			RPCAddress: config.RPC.ListenAddress,
		},
	}

	if config.P2P.PexReactor {
		nodeInfo.Channels = append(nodeInfo.Channels, pex.PexChannel)
	}

	lAddr := config.P2P.ExternalAddress

	if lAddr == "" {
		lAddr = config.P2P.ListenAddress
	}

	nodeInfo.ListenAddr = lAddr

	err := nodeInfo.Validate()
	return nodeInfo, err
}

// EventBus returns the Node's EventBus.
func (n *Node) EventBus() *types.EventBus {
	return n.eventBus
}

// PrivValidator returns the Node's PrivValidator.
// XXX: for convenience only!
func (n *Node) PrivValidator() types.PrivValidator {
	return n.privValidator
}

// GenesisDoc returns the Node's GenesisDoc.
func (n *Node) GenesisDoc() *types.GenesisDoc {
	return n.genesisDoc
}

// ProxyApp returns the Node's AppConns, representing its connections to the ABCI application.
func (n *Node) ProxyApp() proxy.AppConns {
	return n.proxyApp
}

// Config returns the Node's config.
func (n *Node) Config() *cfg.Config {
	return n.config
}
