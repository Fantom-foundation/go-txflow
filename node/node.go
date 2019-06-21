package node

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/andrecronje/babble-abci/eventpool"
	"github.com/andrecronje/babble-abci/hashgraph"
	"github.com/andrecronje/babble/src/service"

	_ "net/http/pprof"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"

	amino "github.com/tendermint/go-amino"
	abci "github.com/tendermint/tendermint/abci/types"
	bc "github.com/tendermint/tendermint/blockchain"
	cfg "github.com/tendermint/tendermint/config"
	cs "github.com/tendermint/tendermint/consensus"
	"github.com/tendermint/tendermint/crypto/ed25519"
	"github.com/tendermint/tendermint/evidence"
	cmn "github.com/tendermint/tendermint/libs/common"
	dbm "github.com/tendermint/tendermint/libs/db"
	"github.com/tendermint/tendermint/libs/log"
	tmpubsub "github.com/tendermint/tendermint/libs/pubsub"
	"github.com/tendermint/tendermint/mempool"
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

// Node defines a babble node
type Node struct {
	cmn.BaseService
	// Node operations are implemented as a state-machine. The embedded state
	// object is used to manage the node's state.
	state

	config        *cfg.Config
	genesisDoc    *types.GenesisDoc   // initial validator set
	privValidator types.PrivValidator // local node's validator key

	State   hashgraph.State
	Service *service.Service

	transport   *p2p.MultiplexTransport
	sw          *p2p.Switch  // p2p connections
	addrBook    pex.AddrBook // known peers
	nodeInfo    p2p.NodeInfo
	nodeKey     *p2p.NodeKey // our node privkey
	isListening bool

	logger log.Logger

	// core is the link between the node and the underlying hashgraph. It
	// controls some higher-level operations like inserting a list of events,
	// keeping track of the peers list, fast-forwarding, etc.
	core     *Core
	coreLock sync.Mutex

	// The node runs the controlTimer in the background to periodically receive
	// signals to initiate gossip routines. It is paused, reset, etc., based on
	// the node's current state
	controlTimer *ControlTimer

	start          time.Time
	syncRequests   int
	syncErrors     int
	SyncLimit      int
	EnableFastSync bool

	eventBus         *types.EventBus // pub/sub for services
	stateDB          dbm.DB
	blockStore       *bc.BlockStore              // store the blockchain to disk
	bcReactor        *bc.BlockchainReactor       // for fast-syncing
	mempoolReactor   *mempool.MempoolReactor     // for accepting transactions
	eventpoolReactor *eventpool.EventpoolReactor // for gossipping transactions
	consensusState   *cs.ConsensusState          // latest consensus state
	evidencePool     *evidence.EvidencePool      // tracking evidence
	proxyApp         proxy.AppConns              // connection to the application
	rpcListeners     []net.Listener              // rpc servers
	txIndexer        txindex.TxIndexer
	indexerService   *txindex.IndexerService
	prometheusSrv    *http.Server
}

// MetricsProvider returns a consensus, p2p and mempool Metrics.
type MetricsProvider func(chainID string) (*cs.Metrics, *p2p.Metrics, *mempool.Metrics, *sm.Metrics)

// DefaultMetricsProvider returns Metrics build using Prometheus client library
// if Prometheus is enabled. Otherwise, it returns no-op Metrics.
func DefaultMetricsProvider(config *cfg.InstrumentationConfig) MetricsProvider {
	return func(chainID string) (*cs.Metrics, *p2p.Metrics, *mempool.Metrics, *sm.Metrics) {
		if config.Prometheus {
			return cs.PrometheusMetrics(config.Namespace, "chain_id", chainID),
				p2p.PrometheusMetrics(config.Namespace, "chain_id", chainID),
				mempool.PrometheusMetrics(config.Namespace, "chain_id", chainID),
				sm.PrometheusMetrics(config.Namespace, "chain_id", chainID)
		}
		return cs.NopMetrics(), p2p.NopMetrics(), mempool.NopMetrics(), sm.NopMetrics()
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
	handshaker := cs.NewHandshaker(stateDB, state, blockStore, genDoc)
	handshaker.SetLogger(consensusLogger)
	handshaker.SetEventBus(eventBus)
	if err := handshaker.Handshake(proxyApp); err != nil {
		return nil, fmt.Errorf("Error during handshake: %v", err)
	}

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

	if config.PrivValidatorListenAddr != "" {
		// If an address is provided, listen on the socket for a connection from an
		// external signing process.
		// FIXME: we should start services inside OnStart
		privValidator, err = createAndStartPrivValidatorSocketClient(config.PrivValidatorListenAddr, logger)
		if err != nil {
			return nil, errors.Wrap(err, "Error with private validator socket client")
		}
	}

	// Decide whether to fast-sync or not
	// We don't fast-sync when the only validator is us.
	fastSync := config.FastSync
	if state.Validators.Size() == 1 {
		addr, _ := state.Validators.GetByIndex(0)
		privValAddr := privValidator.GetPubKey().Address()
		if bytes.Equal(privValAddr, addr) {
			fastSync = false
		}
	}

	pubKey := privValidator.GetPubKey()
	addr := pubKey.Address()
	// Log whether this node is a validator or an observer
	if state.Validators.HasAddress(addr) {
		consensusLogger.Info("This node is a validator", "addr", addr, "pubKey", pubKey)
	} else {
		consensusLogger.Info("This node is not a validator", "addr", addr, "pubKey", pubKey)
	}

	csMetrics, p2pMetrics, mempoolMetrics, smMetrics := metricsProvider(genDoc.ChainID)

	// Make MempoolReactor
	mp := mempool.NewMempool(
		config.Mempool,
		proxyApp.Mempool(),
		state.LastBlockHeight,
		mempool.WithMetrics(mempoolMetrics),
		mempool.WithPreCheck(sm.TxPreCheck(state)),
		mempool.WithPostCheck(sm.TxPostCheck(state)),
	)
	mempoolLogger := logger.With("module", "mempool")
	mp.SetLogger(mempoolLogger)
	if config.Mempool.WalEnabled() {
		mp.InitWAL() // no need to have the mempool wal during tests
	}
	mempoolReactor := mempool.NewMempoolReactor(config.Mempool, mp)
	mempoolReactor.SetLogger(mempoolLogger)

	if config.Consensus.WaitForTxs() {
		mp.EnableTxsAvailable()
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

	blockExecLogger := logger.With("module", "state")
	// make block executor for consensus and blockchain reactors to execute blocks
	blockExec := sm.NewBlockExecutor(
		stateDB,
		blockExecLogger,
		proxyApp.Consensus(),
		mp,
		evidencePool,
		sm.BlockExecutorWithMetrics(smMetrics),
	)

	// Make BlockchainReactor
	bcReactor := bc.NewBlockchainReactor(state.Copy(), blockExec, blockStore, fastSync)
	bcReactor.SetLogger(logger.With("module", "blockchain"))

	// Make ConsensusReactor
	consensusState := cs.NewConsensusState(
		config.Consensus,
		state.Copy(),
		blockExec,
		blockStore,
		mp,
		evidencePool,
		cs.StateMetrics(csMetrics),
	)
	consensusState.SetLogger(consensusLogger)
	if privValidator != nil {
		consensusState.SetPrivValidator(privValidator)
	}
	//consensusReactor := cs.NewConsensusReactor(consensusState, fastSync, cs.ReactorMetrics(csMetrics))
	//consensusReactor.SetLogger(consensusLogger)

	// services which will be publishing and/or subscribing for messages (events)
	// consensusReactor will set it on consensusState and blockExecutor
	//consensusReactor.SetEventBus(eventBus)

	p2pLogger := logger.With("module", "p2p")
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

	// Setup Transport.
	var (
		mConnConfig = p2p.MConnConfig(config.P2P)
		transport   = p2p.NewMultiplexTransport(nodeInfo, *nodeKey, mConnConfig)
		connFilters = []p2p.ConnFilterFunc{}
		peerFilters = []p2p.PeerFilterFunc{}
	)

	if !config.P2P.AllowDuplicateIP {
		connFilters = append(connFilters, p2p.ConnDuplicateIPFilter())
	}

	// Filter peers by addr or pubkey with an ABCI query.
	// If the query return code is OK, add peer.
	if config.FilterPeers {
		connFilters = append(
			connFilters,
			// ABCI query for address filtering.
			func(_ p2p.ConnSet, c net.Conn, _ []net.IP) error {
				res, err := proxyApp.Query().QuerySync(abci.RequestQuery{
					Path: fmt.Sprintf("/p2p/filter/addr/%s", c.RemoteAddr().String()),
				})
				if err != nil {
					return err
				}
				if res.IsErr() {
					return fmt.Errorf("Error querying abci app: %v", res)
				}

				return nil
			},
		)

		peerFilters = append(
			peerFilters,
			// ABCI query for ID filtering.
			func(_ p2p.IPeerSet, p p2p.Peer) error {
				res, err := proxyApp.Query().QuerySync(abci.RequestQuery{
					Path: fmt.Sprintf("/p2p/filter/id/%s", p.ID()),
				})
				if err != nil {
					return err
				}
				if res.IsErr() {
					return fmt.Errorf("Error querying abci app: %v", res)
				}

				return nil
			},
		)
	}

	p2p.MultiplexTransportConnFilters(connFilters...)(transport)

	// Setup Switch.
	sw := p2p.NewSwitch(
		config.P2P,
		transport,
		p2p.WithMetrics(p2pMetrics),
		p2p.SwitchPeerFilters(peerFilters...),
	)
	sw.SetLogger(p2pLogger)
	sw.AddReactor("MEMPOOL", mempoolReactor)
	sw.AddReactor("BLOCKCHAIN", bcReactor)
	//sw.AddReactor("CONSENSUS", consensusReactor)
	sw.AddReactor("EVIDENCE", evidenceReactor)
	sw.SetNodeInfo(nodeInfo)
	sw.SetNodeKey(nodeKey)

	p2pLogger.Info("P2P Node ID", "ID", nodeKey.ID(), "file", config.NodeKeyFile())

	// Optionally, start the pex reactor
	//
	// TODO:
	//
	// We need to set Seeds and PersistentPeers on the switch,
	// since it needs to be able to use these (and their DNS names)
	// even if the PEX is off. We can include the DNS name in the NetAddress,
	// but it would still be nice to have a clear list of the current "PersistentPeers"
	// somewhere that we can return with net_info.
	//
	// If PEX is on, it should handle dialing the seeds. Otherwise the switch does it.
	// Note we currently use the addrBook regardless at least for AddOurAddress
	addrBook := pex.NewAddrBook(config.P2P.AddrBookFile(), config.P2P.AddrBookStrict)

	// Add ourselves to addrbook to prevent dialing ourselves
	addrBook.AddOurAddress(sw.NetAddress())

	addrBook.SetLogger(p2pLogger.With("book", config.P2P.AddrBookFile()))
	if config.P2P.PexReactor {
		// TODO persistent peers ? so we can have their DNS addrs saved
		pexReactor := pex.NewPEXReactor(addrBook,
			&pex.PEXReactorConfig{
				Seeds:    splitAndTrimEmpty(config.P2P.Seeds, ",", " "),
				SeedMode: config.P2P.SeedMode,
				// See consensus/reactor.go: blocksToContributeToBecomeGoodPeer 10000
				// blocks assuming 10s blocks ~ 28 hours.
				// TODO (melekes): make it dynamic based on the actual block latencies
				// from the live network.
				// https://github.com/tendermint/tendermint/issues/3523
				SeedDisconnectWaitPeriod: 28 * time.Hour,
			})
		pexReactor.SetLogger(logger.With("module", "pex"))
		sw.AddReactor("PEX", pexReactor)
	}

	sw.SetAddrBook(addrBook)

	// run the profile server
	profileHost := config.ProfListenAddress
	if profileHost != "" {
		go func() {
			logger.Error("Profile server", "err", http.ListenAndServe(profileHost, nil))
		}()
	}

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
		core:         NewCore(privValidator, state.Validators, hashgraph.NewState(), proxyApp.Consensus(), config.ChainID(), logger),
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

		n.waitRoutines()

		//For some reason this needs to be called after closing the shutdownCh
		//Not entirely sure why...
		n.controlTimer.Shutdown()

		//transport and store should only be closed once all concurrent operations
		//are finished otherwise they will panic trying to use close objects

		//n.core.hashgraph.State.Close()
	}
}

// GetID returns the numeric ID of the node's validator
func (n *Node) GetID() string {
	return n.core.validator.GetPubKey().Address().String()
}

// GetStats returns information about the node.
/*func (n *Node) GetStats() map[string]string {
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
		"sync_rate":              strconv.FormatFloat(n.syncRate(), 'f', 2, 64),
		"events_per_second":      strconv.FormatFloat(consensusEventsPerSecond, 'f', 2, 64),
		"rounds_per_second":      strconv.FormatFloat(consensusRoundsPerSecond, 'f', 2, 64),
		"round_events":           strconv.Itoa(n.core.GetLastCommitedRoundEventsCount()),
		"id":                     fmt.Sprint(n.core.validator.ID()),
		"state":                  n.getState().String(),
		"moniker":                n.core.validator.Moniker,
	}
	return s
}*/

// GetBlock returns a block
func (n *Node) GetBlock(height int64) *types.Block {
	return n.core.hashgraph.State.Blocks[height]
}

/*******************************************************************************
Background
*******************************************************************************/

// doBackgroundWork coninuously listens to incoming RPC commands, incoming
// transactions, and the sigint signal, regardless of the node's state.
func (n *Node) doBackgroundWork() {
	for {
		select {
		/*case rpc := <-n.netCh:
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
			os.Exit(0)*/
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
				//Need to select a peer from address book here and gossip to them
				/*peer := n.core.peerSelector.Next()
				if peer != nil {
					n.goFunc(func() { n.gossip(peer) })
				} else {
					n.monologue()
				}*/
			}
			n.resetTimer()
		}
	}
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
			mempool.MempoolChannel,
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
