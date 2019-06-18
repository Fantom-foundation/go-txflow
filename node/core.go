package node

import (
	"sort"

	"github.com/andrecronje/babble-abci/hashgraph"
	"github.com/tendermint/tendermint/crypto"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/proxy"
	"github.com/tendermint/tendermint/types"
)

//Core is the core Node object
type Core struct {

	// validator is a wrapper around the private-key controlling this node.
	validator types.PrivValidator

	// hg is the underlying hashgraph where all the consensus computation and
	// data reside.
	hashgraph *hashgraph.Hashgraph

	// validators reflects the latest validator-set used in the hashgraph
	// consensus methods.
	validators *types.ValidatorSet

	// Hash and Index of this instance's head Event
	Head    string
	Height  int64
	ChainID string

	// The transaction pool contains transactions submitted from the app that
	// still haven't made it into the hashgraph.
	transactionPool types.Txs

	// proxyCommitCallback is called by the hashgraph when a block is committed
	proxyCommitCallback proxy.AppConnConsensus

	logger log.Logger
}

// NewCore is a factory method that returns a new Core object
func NewCore(
	validator types.PrivValidator,
	validators *types.ValidatorSet,
	state *hashgraph.State,
	proxyCommitCallback proxy.AppConnConsensus,
	chainID string,
	logger log.Logger) *Core {

	core := &Core{
		validator:           validator,
		proxyCommitCallback: proxyCommitCallback,
		validators:          validators,
		transactionPool:     types.Txs{},
		logger:              logger,
		ChainID:             chainID,
		Head:                "",
		Height:              -1,
	}

	core.hashgraph = hashgraph.NewHashgraph(state, core.Commit, logger)

	core.hashgraph.Init(validators)

	return core
}

// SetValidators sets the Validators property
func (c *Core) SetValidators(vs *types.ValidatorSet) {
	c.validators = vs
}

/*******************************************************************************
Busy
*******************************************************************************/

// Busy returns a boolean that denotes whether there is incomplete processing
func (c *Core) Busy() bool {
	return c.hashgraph.PendingLoadedEvents > 0 ||
		len(c.transactionPool) > 0
}

/*******************************************************************************
Sync
*******************************************************************************/

// Sync decodes and inserts new Events into the Hashgraph. UnknownEvents are
// expected to be in topoligical order.
func (c *Core) Sync(from crypto.PubKey, unknownEvents []hashgraph.Event) error {
	c.logger.Debug("Sync", "unknown_events", len(unknownEvents))
	for _, we := range unknownEvents {
		if err := c.InsertEventAndRunConsensus(&we); err != nil {
			c.logger.Error("Inserting Event", "err", err)
			return err
		}
	}

	c.logger.Debug("Sync",
		"loaded_events", c.hashgraph.PendingLoadedEvents,
		"transaction_pool", len(c.transactionPool),
	)

	selfLastEvent := c.hashgraph.State.ValidatorEvents[c.validator.GetPubKey().Address().String()]
	otherLastEvent := c.hashgraph.State.ValidatorEvents[from.Address().String()]
	return c.AddSelfEvent(selfLastEvent[len(selfLastEvent)], otherLastEvent[len(otherLastEvent)])
}

// AddSelfEvent adds a self event
func (c *Core) AddSelfEvent(selfEvent *hashgraph.Event, otherEvent *hashgraph.Event) error {
	txs := len(c.transactionPool)

	//create new event with self head and otherHead, and empty pools in its
	//payload
	newHeadEvent := hashgraph.NewEvent(c.transactionPool,
		[]cmn.HexBytes{selfEvent.Hash(), otherEvent.Hash()},
		c.validator.GetPubKey(),
		int64(c.Height+1))

	//Inserting the Event, and running consensus methods, can have a side-effect
	//of adding items to the transaction pools (via the commit callback).
	if err := c.SignAndInsertSelfEvent(newHeadEvent); err != nil {
		c.logger.Error("Error inserting new head", "err", err)
		return err
	}

	c.logger.Debug("Created Self-Event",
		"index", newHeadEvent.Height,
		"transactions", len(newHeadEvent.Transactions),
	)

	//do not remove pool elements that were added by CommitCallback
	c.transactionPool = c.transactionPool[txs:]

	return nil
}

func NewVoteFromEvent(event *hashgraph.Event) {

}

// SignAndInsertSelfEvent signs a Hashgraph Event, inserts it and runs consensus
func (c *Core) SignAndInsertSelfEvent(event *hashgraph.Event) error {
	vote := event.GetVote()

	c.validator.SignVote(c.ChainID, vote)
	return c.InsertEventAndRunConsensus(event)
}

// InsertEventAndRunConsensus Inserts a hashgraph event and runs consensus
func (c *Core) InsertEventAndRunConsensus(event *hashgraph.Event) error {
	if err := c.hashgraph.InsertEventAndRunConsensus(event); err != nil {
		return err
	}
	if event.Creator == c.validator.GetPubKey() {
		c.Head = event.Hex()
		c.Height = event.Height
	}
	return nil
}

// KnownEvents returns known events from the Hashgraph store
func (c *Core) KnownEvents() map[string]int {
	return c.hashgraph.KnownEvents()
}

/*******************************************************************************
Commit
*******************************************************************************/

// Commit the Block to the App using the proxyCommitCallback
func (c *Core) Commit(block *types.Block) error {
	//Commit the Block to the App
	_, err := c.proxyCommitCallback.CommitSync()
	return err
}

/*******************************************************************************
Diff
*******************************************************************************/

// EventDiff returns events that c knowns about and are not in 'known'
func (c *Core) EventDiff(known map[string]int) (events []*hashgraph.Event, err error) {
	unknown := []*hashgraph.Event{}
	//known represents the index of the last event known for every participant
	//compare this to our view of events and fill unknown with events that we know of
	// and the other doesnt
	for address, height := range known {
		//get validator Events with index > ct
		validatorEvents, ok := c.hashgraph.State.ValidatorEvents[address]
		if !ok {
			return []*hashgraph.Event{}, err
		}
		for _, e := range validatorEvents[height:] {
			unknown = append(unknown, e)
		}
	}
	sort.Sort(hashgraph.ByTopologicalOrder(unknown))

	return unknown, nil
}

/*******************************************************************************
Pools
*******************************************************************************/

// AddTransactions appends transactions to the transaction pool
func (c *Core) AddTransactions(txs types.Txs) {
	c.transactionPool = append(c.transactionPool, txs...)
}

/*******************************************************************************
Getters
*******************************************************************************/

// GetHead returns the head from the hashgraph store
func (c *Core) GetHead() *hashgraph.Event {
	return c.hashgraph.State.Events[c.Head]
}

// GetEvent returns an event from the state
func (c *Core) GetEvent(hash string) *hashgraph.Event {
	return c.hashgraph.State.Events[hash]
}

// GetEventTransactions returns the transactions for an event
func (c *Core) GetEventTransactions(hash string) (types.Txs, error) {
	ex := c.GetEvent(hash)
	return ex.Transactions, nil
}

/*
// GetConsensusEvents returns consensus events from the hashgragh store
func (c *Core) GetConsensusEvents() []string {
	return c.hashgraph.State.ConsensusEvents
}

// GetConsensusEventsCount returns the count of consensus events from the
// hashgragh store
func (c *Core) GetConsensusEventsCount() int {
	return c.hashgraph.State.ConsensusEventsCount()
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
}*/
