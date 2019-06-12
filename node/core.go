package node

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/andrecronje/babble-abci/hashgraph"
	"github.com/mosaicnetworks/babble/src/common"
	b "github.com/mosaicnetworks/babble/src/node"
	"github.com/mosaicnetworks/babble/src/peers"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/proxy"
)

//Core is the core Node object
type Core struct {

	// validator is a wrapper around the private-key controlling this node.
	validator *b.Validator

	// hg is the underlying hashgraph where all the consensus computation and
	// data reside.
	hashgraph *hashgraph.Hashgraph

	// genesisPeers is the validator-set that the hashgraph/blockchain was
	// initialised with
	genesisPeers *peers.PeerSet

	// validators reflects the latest validator-set used in the hashgraph
	// consensus methods.
	validators *peers.PeerSet

	// peers is the list of peers that the node will try to gossip with; not
	// necessarily the current validator-set.
	peers *peers.PeerSet

	// peerSelector is the object that decides which peer to talk to next.
	peerSelector b.PeerSelector
	selectorLock sync.Mutex

	// Hash and Index of this instance's head Event
	Head string
	Seq  int

	// AcceptedRound is the first round at which the node's last JoinRequest
	// takes effect. A node will not create SelfEvents before reaching
	// AcceptedRound.Default -1.
	AcceptedRound int

	// RemovedRound is the round at which the node's last LeaveRequest takes
	// effect (if there is one). Default -1.
	RemovedRound int

	// TargetRound is the minimum Consensus Round that the node needs to reach.
	// It is useful to set this value to a joining peer's accepted-round to
	// prevent them from having to wait.
	TargetRound int

	// Events that are not tied to this node's Head. This is managed by the Sync
	// method. If the gossip condition is false (there is nothing interesting to
	// record), items are added to heads; if the gossip condition is true, items
	// are removed from heads and used to record a new self-event. This
	// functionality allows to not grow the hashgraph continuously when there is
	// nothing to record.
	heads map[uint32]*hashgraph.Event

	// The transaction pool contains transactions submitted from the app that
	// still haven't made it into the hashgraph.
	transactionPool [][]byte

	// internalTransactionPool is the same as transactionPool but for
	// InternalTransactions
	internalTransactionPool []hashgraph.InternalTransaction

	// selfBlockSignatures is a pool of block-signatures, created by this node,
	// that still haven't made it into the hashgraph.
	selfBlockSignatures *hashgraph.SigPool

	// proxyCommitCallback is called by the hashgraph when a block is committed
	proxyCommitCallback proxy.AppConnConsensus

	// promises keeps track of pending JoinRequests while the corresponding
	// InternalTransactions go through consensus asynchronously.
	promises map[string]*JoinPromise

	logger log.Logger
}

// NewCore is a factory method that returns a new Core object
func NewCore(
	validator *b.Validator,
	peers *peers.PeerSet,
	genesisPeers *peers.PeerSet,
	store hashgraph.Store,
	proxyCommitCallback proxy.AppConnConsensus,
	logger log.Logger) *Core {

	peerSelector := b.NewRandomPeerSelector(peers, validator.ID())

	core := &Core{
		validator:               validator,
		proxyCommitCallback:     proxyCommitCallback,
		genesisPeers:            genesisPeers,
		validators:              genesisPeers,
		peers:                   peers,
		peerSelector:            peerSelector,
		transactionPool:         [][]byte{},
		internalTransactionPool: []hashgraph.InternalTransaction{},
		selfBlockSignatures:     hashgraph.NewSigPool(),
		promises:                make(map[string]*JoinPromise),
		heads:                   make(map[uint32]*hashgraph.Event),
		logger:                  logger,
		Head:                    "",
		Seq:                     -1,
		AcceptedRound:           -1,
		RemovedRound:            -1,
		TargetRound:             -1,
	}

	core.hashgraph = hashgraph.NewHashgraph(store, core.Commit, logger)

	core.hashgraph.Init(genesisPeers)

	return core
}

// SetHeadAndSeq sets the Head and Seq of a Core object
func (c *Core) SetHeadAndSeq() error {
	head := ""
	seq := -1

	_, ok := c.hashgraph.Store.RepertoireByID()[c.validator.ID()]

	if ok {
		last, err := c.hashgraph.Store.LastEventFrom(c.validator.PublicKeyHex())
		if err != nil && !common.Is(err, common.Empty) {
			return err
		}

		if last != "" {
			lastEvent, err := c.GetEvent(last)
			if err != nil {
				return err
			}

			head = last
			seq = lastEvent.Index()
		}
	} else {
		c.logger.Debug("Not in repertoire yet.")
	}

	c.Head = head
	c.Seq = seq

	c.logger.Debug("SetHeadAndSeq",
		"core.Head", c.Head,
		"core.Seq", c.Seq,
	)

	return nil
}

// Bootstrap calls the Hashgraph Bootstrap
func (c *Core) Bootstrap() error {
	c.logger.Debug("Bootstrap")
	return c.hashgraph.Bootstrap()
}

// SetPeers sets the peers property and a New RandomPeerSelector
func (c *Core) SetPeers(ps *peers.PeerSet) {
	c.peers = ps
	c.peerSelector = b.NewRandomPeerSelector(c.peers, c.validator.ID())
}

/*******************************************************************************
Busy
*******************************************************************************/

// Busy returns a boolean that denotes whether there is incomplete processing
func (c *Core) Busy() bool {
	return c.hashgraph.PendingLoadedEvents > 0 ||
		len(c.transactionPool) > 0 ||
		len(c.internalTransactionPool) > 0 ||
		c.selfBlockSignatures.Len() > 0 ||
		(c.hashgraph.LastConsensusRound != nil && *c.hashgraph.LastConsensusRound < c.TargetRound)
}

/*******************************************************************************
Sync
*******************************************************************************/

// Sync decodes and inserts new Events into the Hashgraph. UnknownEvents are
// expected to be in topoligical order.
func (c *Core) Sync(fromID uint32, unknownEvents []hashgraph.WireEvent) error {
	c.logger.Debug("Sync", "unknown_events", len(unknownEvents))

	var otherHead *hashgraph.Event
	for _, we := range unknownEvents {
		ev, err := c.hashgraph.ReadWireInfo(we)
		if err != nil {
			c.logger.Error("Reading WireEvent",
				"wire_event", we,
				"error", err,
			)
			return err
		}

		if err := c.InsertEventAndRunConsensus(ev, false); err != nil {
			c.logger.Error("Inserting Event", "err", err)
			return err
		}

		if we.Body.CreatorID == fromID {
			otherHead = ev
		}

		if h, ok := c.heads[we.Body.CreatorID]; ok &&
			h != nil &&
			we.Body.Index > h.Index() {

			delete(c.heads, we.Body.CreatorID)
		}
	}

	//Do not overwrite a non-empty head with an empty head
	if h, ok := c.heads[fromID]; !ok ||
		h == nil ||
		(otherHead != nil && otherHead.Index() > h.Index()) {

		c.heads[fromID] = otherHead
	}

	c.logger.Debug("Sync",
		"loaded_events", c.hashgraph.PendingLoadedEvents,
		"transaction_pool", len(c.transactionPool),
		"internal_transaction_pool", len(c.internalTransactionPool),
		"self_signature_pool", c.selfBlockSignatures.Len(),
		"target_round", c.TargetRound,
	)

	//Create new event with self head and other head only if there are pending
	//loaded events or the pools are not empty
	if c.Busy() {
		return c.RecordHeads()
	}

	return nil
}

// RecordHeads adds heads as SelfEvents
func (c *Core) RecordHeads() error {
	c.logger.Debug("RecordHeads()", "heads", len(c.heads))

	for id, ev := range c.heads {
		op := ""
		if ev != nil {
			op = ev.Hex()
		}
		if err := c.AddSelfEvent(op); err != nil {
			return err
		}
		delete(c.heads, id)
	}

	return nil
}

// AddSelfEvent adds a self event
func (c *Core) AddSelfEvent(otherHead string) error {
	if c.hashgraph.Store.LastRound() < c.AcceptedRound {
		c.logger.Debug("Too early to insert self-event (%d / %d)", c.hashgraph.Store.LastRound(), c.AcceptedRound)
		return nil
	}

	//Add own block signatures to next Event
	sigs := c.selfBlockSignatures.Slice()
	txs := len(c.transactionPool)
	itxs := len(c.internalTransactionPool)

	//create new event with self head and otherHead, and empty pools in its
	//payload
	newHead := hashgraph.NewEvent(c.transactionPool,
		c.internalTransactionPool,
		sigs,
		[]string{c.Head, otherHead},
		c.validator.PublicKeyBytes(),
		c.Seq+1)

	//Inserting the Event, and running consensus methods, can have a side-effect
	//of adding items to the transaction pools (via the commit callback).
	if err := c.SignAndInsertSelfEvent(newHead); err != nil {
		c.logger.Error("Error inserting new head", "err", err)
		return err
	}

	c.logger.Debug("Created Self-Event",
		"index", newHead.Index(),
		"transactions", len(newHead.Transactions()),
		"internal_transactions", len(newHead.InternalTransactions()),
		"block_signatures", len(newHead.BlockSignatures()),
	)

	//do not remove pool elements that were added by CommitCallback
	c.transactionPool = c.transactionPool[txs:]
	c.internalTransactionPool = c.internalTransactionPool[itxs:]
	c.selfBlockSignatures.RemoveSlice(sigs)

	return nil
}

// SignAndInsertSelfEvent signs a Hashgraph Event, inserts it and runs consensus
func (c *Core) SignAndInsertSelfEvent(event *hashgraph.Event) error {
	if err := event.Sign(c.validator.Key); err != nil {
		return err
	}
	return c.InsertEventAndRunConsensus(event, true)
}

// InsertEventAndRunConsensus Inserts a hashgraph event and runs consensus
func (c *Core) InsertEventAndRunConsensus(event *hashgraph.Event, setWireInfo bool) error {
	if err := c.hashgraph.InsertEventAndRunConsensus(event, setWireInfo); err != nil {
		return err
	}
	if event.Creator() == c.validator.PublicKeyHex() {
		c.Head = event.Hex()
		c.Seq = event.Index()
	}
	return nil
}

// KnownEvents returns known events from the Hashgraph store
func (c *Core) KnownEvents() map[uint32]int {
	return c.hashgraph.Store.KnownEvents()
}

/*******************************************************************************
FastForward
*******************************************************************************/

// FastForward is used whilst in catchingUp state to apply past blocks and frames
func (c *Core) FastForward(block *hashgraph.Block, frame *hashgraph.Frame) error {

	c.logger.Debug("Fast Forward", frame.Round)
	peerSet := peers.NewPeerSet(frame.Peers)

	//Check Block Signatures
	err := c.hashgraph.CheckBlock(block, peerSet)
	if err != nil {
		return err
	}

	//Check Frame Hash
	frameHash, err := frame.Hash()
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(block.FrameHash(), frameHash) {
		return fmt.Errorf("Invalid Frame Hash")
	}

	err = c.hashgraph.Reset(block, frame)
	if err != nil {
		return err
	}

	err = c.SetHeadAndSeq()
	if err != nil {
		return err
	}

	// Update peer-selector and validators
	c.SetPeers(peers.NewPeerSet(frame.Peers))
	c.validators = peers.NewPeerSet(frame.Peers)

	return nil
}

//GetAnchorBlockWithFrame returns GetAnchorBlockWithFrame from the hashgraph
func (c *Core) GetAnchorBlockWithFrame() (*hashgraph.Block, *hashgraph.Frame, error) {
	return c.hashgraph.GetAnchorBlockWithFrame()
}

/*******************************************************************************
Leave
*******************************************************************************/

// Leave causes the node to leave the network
func (c *Core) Leave(leaveTimeout time.Duration) error {
	p, ok := c.peers.ByID[c.validator.ID()]
	if !ok {
		return fmt.Errorf("Leaving: Peer not found")
	}

	itx := hashgraph.NewInternalTransaction(hashgraph.PEER_REMOVE, *p)
	itx.Sign(c.validator.Key)

	promise := c.AddInternalTransaction(itx)

	//Wait for the InternalTransaction to go through consensus
	timeout := time.After(leaveTimeout)
	select {
	case resp := <-promise.RespCh:
		c.logger.Debug("LeaveRequest processed",
			"leaving_round", resp.AcceptedRound,
			"peers", len(resp.Peers),
		)
		c.RemovedRound = resp.AcceptedRound
	case <-timeout:
		err := fmt.Errorf("Timeout waiting for LeaveRequest to go through consensus")
		c.logger.Error("err", err)
		return err
	}

	if c.peers.Len() >= 1 {
		//Wait for node to reach accepted round
		timeout = time.After(leaveTimeout)
		for {
			select {
			case <-timeout:
				err := fmt.Errorf("Timeout waiting for leaving node to reach TargetRound")
				c.logger.Error("err", err)
				return err
			default:
				if c.hashgraph.LastConsensusRound != nil && *c.hashgraph.LastConsensusRound < c.TargetRound {
					c.logger.Debug("Waiting to reach TargetRound: %d/%d", *c.hashgraph.LastConsensusRound, c.RemovedRound)
					time.Sleep(100 * time.Millisecond)
				} else {
					return nil
				}
			}
		}
	}

	return nil
}

/*******************************************************************************
Commit
*******************************************************************************/

// Commit the Block to the App using the proxyCommitCallback
func (c *Core) Commit(block *hashgraph.Block) error {
	//Commit the Block to the App
	_, err := c.proxyCommitCallback.CommitSync()

	//Handle the response to set Block StateHash and process InternalTransaction
	//receipts which might update the PeerSet.
	if err == nil {
		//Sign the block if we belong to its validator-set
		blockPeerSet, err := c.hashgraph.Store.GetPeerSet(block.RoundReceived())
		if err != nil {
			return err
		}

		if _, ok := blockPeerSet.ByID[c.validator.ID()]; ok {
			sig, err := c.SignBlock(block)
			if err != nil {
				return err
			}
			c.selfBlockSignatures.Add(sig)
		}

		err = c.hashgraph.SetAnchorBlock(block)
		if err != nil {
			return err
		}
		//TODO: Adjust validator set based on commitResponse validator change
		/*err = c.ProcessAcceptedInternalTransactions(block.RoundReceived(), commitResponse.InternalTransactionReceipts)
		if err != nil {
			return err
		}*/
	}

	return err
}

// SignBlock signs the block
func (c *Core) SignBlock(block *hashgraph.Block) (hashgraph.BlockSignature, error) {
	sig, err := block.Sign(c.validator.Key)
	if err != nil {
		return hashgraph.BlockSignature{}, err
	}

	err = block.SetSignature(sig)
	if err != nil {
		return hashgraph.BlockSignature{}, err
	}

	err = c.hashgraph.Store.SetBlock(block)
	if err != nil {
		return sig, err
	}

	return sig, nil
}

// ProcessAcceptedInternalTransactions processes the accepted internal transactions
func (c *Core) ProcessAcceptedInternalTransactions(roundReceived int, receipts []hashgraph.InternalTransactionReceipt) error {
	currentPeers := c.peers
	validators := c.validators

	changed := false
	for _, r := range receipts {
		txBody := r.InternalTransaction.Body

		if r.Accepted {
			c.logger.Debug("Processing accepted InternalTransaction",
				"peer", txBody.Peer,
				"round_received", roundReceived,
				"type", txBody.Type.String(),
			)

			switch txBody.Type {
			case hashgraph.PEER_ADD:
				validators = validators.WithNewPeer(&txBody.Peer)
				currentPeers = currentPeers.WithNewPeer(&txBody.Peer)
			case hashgraph.PEER_REMOVE:
				validators = validators.WithRemovedPeer(&txBody.Peer)
				currentPeers = currentPeers.WithRemovedPeer(&txBody.Peer)
			default:
			}

			changed = true
		} else {
			c.logger.Debug("InternalTransaction not accepted", "peer", txBody.Peer)
		}
	}

	//Why +6? According to lemmas 5.15 and 5.17 of the original whitepaper, all
	//consistent hashgraphs will have decided the fame of round r witnesses by
	//round r+5 or before; so it is safe to set the new peer-set at round r+6.
	effectiveRound := roundReceived + 6

	if changed {
		// Record the new validator-set in the underlying Hashgraph and in
		// the core's validators field

		err := c.hashgraph.Store.SetPeerSet(effectiveRound, validators)
		if err != nil {
			return fmt.Errorf("Updating Store PeerSet: %s", err)
		}

		c.validators = validators

		c.logger.Debug("Validators Changed",
			"effective_round", effectiveRound,
			"validators", len(validators.Peers),
		)

		// Update the current list of communicating peers. This is not
		// necessarily equal to the latest recorded validator_set.
		c.SetPeers(currentPeers)

		// A new validator-set has been recorded and will only be effective from
		// effectiveRound. A joining node will not be able to participate in the
		// consensus until the Hashgraph reaches that effectiveRound. Hence, we
		// force everyone to reach that round.
		if effectiveRound > c.TargetRound {
			c.logger.Debug("Update TargetRound from %d to %d", c.TargetRound, effectiveRound)
			c.TargetRound = effectiveRound
		}
	}

	for _, r := range receipts {
		//respond to the corresponding promise
		if p, ok := c.promises[r.InternalTransaction.HashString()]; ok {
			if r.Accepted {
				p.Respond(true, effectiveRound, c.validators.Peers)
			} else {
				p.Respond(false, 0, []*peers.Peer{})
			}
			delete(c.promises, r.InternalTransaction.HashString())
		}
	}

	return nil
}

/*******************************************************************************
Diff
*******************************************************************************/

// EventDiff returns events that c knowns about and are not in 'known'
func (c *Core) EventDiff(known map[uint32]int) (events []*hashgraph.Event, err error) {
	unknown := []*hashgraph.Event{}
	//known represents the index of the last event known for every participant
	//compare this to our view of events and fill unknown with events that we know of
	// and the other doesnt
	for id, ct := range known {
		peer, ok := c.hashgraph.Store.RepertoireByID()[id]
		if !ok {
			continue
		}

		//get participant Events with index > ct
		participantEvents, err := c.hashgraph.Store.ParticipantEvents(peer.PubKeyString(), ct)
		if err != nil {
			return []*hashgraph.Event{}, err
		}
		for _, e := range participantEvents {
			ev, err := c.hashgraph.Store.GetEvent(e)
			if err != nil {
				return []*hashgraph.Event{}, err
			}
			unknown = append(unknown, ev)
		}
	}
	sort.Sort(hashgraph.ByTopologicalOrder(unknown))

	return unknown, nil
}

// FromWire takes Wire Events and returns Hashgraph Events
func (c *Core) FromWire(wireEvents []hashgraph.WireEvent) ([]hashgraph.Event, error) {
	events := make([]hashgraph.Event, len(wireEvents), len(wireEvents))

	for i, w := range wireEvents {
		ev, err := c.hashgraph.ReadWireInfo(w)
		if err != nil {
			return nil, err
		}

		events[i] = *ev
	}

	return events, nil
}

// ToWire takes Hashgraph Events and returns Wire Events
func (c *Core) ToWire(events []*hashgraph.Event) ([]hashgraph.WireEvent, error) {
	wireEvents := make([]hashgraph.WireEvent, len(events), len(events))

	for i, e := range events {
		wireEvents[i] = e.ToWire()
	}

	return wireEvents, nil
}

/*******************************************************************************
Pools
*******************************************************************************/

// ProcessSigPool calls Hashgraph ProcessSigPool
func (c *Core) ProcessSigPool() error {
	return c.hashgraph.ProcessSigPool()
}

// AddTransactions appends transactions to the transaction pool
func (c *Core) AddTransactions(txs [][]byte) {
	c.transactionPool = append(c.transactionPool, txs...)
}

// AddInternalTransaction adds an internal transaction
func (c *Core) AddInternalTransaction(tx hashgraph.InternalTransaction) *JoinPromise {
	//create promise
	promise := NewJoinPromise(tx)

	//save it to promise store, for later use by the Commit callback
	c.promises[tx.HashString()] = promise

	//submit the internal tx to be processed asynchronously by the gossip
	//routines
	c.internalTransactionPool = append(c.internalTransactionPool, tx)

	//return the promise
	return promise
}

/*******************************************************************************
Getters
*******************************************************************************/

// GetHead returns the head from the hashgraph store
func (c *Core) GetHead() (*hashgraph.Event, error) {
	return c.hashgraph.Store.GetEvent(c.Head)
}

// GetEvent returns an event from the hashgrapg store
func (c *Core) GetEvent(hash string) (*hashgraph.Event, error) {
	return c.hashgraph.Store.GetEvent(hash)
}

// GetEventTransactions returns the transactions for an event
func (c *Core) GetEventTransactions(hash string) ([][]byte, error) {
	var txs [][]byte
	ex, err := c.GetEvent(hash)
	if err != nil {
		return txs, err
	}
	txs = ex.Transactions()
	return txs, nil
}

// GetConsensusEvents returns consensus events from the hashgragh store
func (c *Core) GetConsensusEvents() []string {
	return c.hashgraph.Store.ConsensusEvents()
}

// GetConsensusEventsCount returns the count of consensus events from the
// hashgragh store
func (c *Core) GetConsensusEventsCount() int {
	return c.hashgraph.Store.ConsensusEventsCount()
}

// GetUndeterminedEvents returns undetermined events from the hashgraph
func (c *Core) GetUndeterminedEvents() []string {
	return c.hashgraph.UndeterminedEvents
}

// GetPendingLoadedEvents returns pendign loading events from the hashgraph
func (c *Core) GetPendingLoadedEvents() int {
	return c.hashgraph.PendingLoadedEvents
}

// GetConsensusTransactions returns the transaction from the events returned by
// GetConsensusEvents()
func (c *Core) GetConsensusTransactions() ([][]byte, error) {
	txs := [][]byte{}
	for _, e := range c.GetConsensusEvents() {
		eTxs, err := c.GetEventTransactions(e)
		if err != nil {
			return txs, fmt.Errorf("Consensus event not found: %s", e)
		}
		txs = append(txs, eTxs...)
	}
	return txs, nil
}

// GetLastConsensusRoundIndex returns the Last Consensus Round from the hashgraph
func (c *Core) GetLastConsensusRoundIndex() *int {
	return c.hashgraph.LastConsensusRound
}

// GetConsensusTransactionsCount return ConsensusTransacions from the hashgraph
func (c *Core) GetConsensusTransactionsCount() int {
	return c.hashgraph.ConsensusTransactions
}

// GetLastCommitedRoundEventsCount returns LastCommitedRoundEvents from the
// hashgraph
func (c *Core) GetLastCommitedRoundEventsCount() int {
	return c.hashgraph.LastCommitedRoundEvents
}

// GetLastBlockIndex returns last block index from the hashgraph store
func (c *Core) GetLastBlockIndex() int {
	return c.hashgraph.Store.LastBlockIndex()
}
