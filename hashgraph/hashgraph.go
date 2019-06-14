package hashgraph

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"

	"github.com/andrecronje/babble/src/common"
	"github.com/andrecronje/babble-abci/peers"
	"github.com/tendermint/tendermint/libs/log"
)

const (
	/*
		ROOT_DEPTH determines how many FrameEvents are included in the Root. It
		is preferable not to make ROOT_DEPTH configurable because if peers use
		diffent values, they will produce different Roots, different Frames, and
		different Blocks. Perhaps this parameter should be tied to the number of
		Peers rather than hard-coded.
	*/
	ROOT_DEPTH = 10

	/*
		COIN_ROUND_FREQ defines the frequency of coin rounds. The value is
		arbitrary. Do something smarter.
	*/
	COIN_ROUND_FREQ = float64(4)
)

//Hashgraph is a DAG of Events. It also contains methods to extract a consensus
//order of Events and map them onto a blockchain.
type Hashgraph struct {
	Store                   Store                  //store of Events, Rounds, and Blocks
	UndeterminedEvents      []string               //[index] => hash . FIFO queue of Events whose consensus order is not yet determined
	PendingRounds           *PendingRoundsCache    //FIFO queue of Rounds which have not attained consensus yet
	PendingSignatures       *SigPool               //Pool of Block signatures that need to be processed (matched with Blocks)
	LastConsensusRound      *int                   //index of last consensus round
	FirstConsensusRound     *int                   //index of first consensus round (only used in tests)
	AnchorBlock             *int                   //index of last block with enough signatures
	roundLowerBound         *int                   //rounds and events below this lower bound have a special treatement (cf fastsync)
	LastCommitedRoundEvents int                    //number of events in round before LastConsensusRound
	ConsensusTransactions   int                    //number of consensus transactions
	PendingLoadedEvents     int                    //number of loaded events that are not yet committed
	commitCallback          InternalCommitCallback //commit block callback
	topologicalIndex        int                    //counter used to order events in topological order (only local)

	ancestorCache     *common.LRU
	selfAncestorCache *common.LRU
	stronglySeeCache  *common.LRU
	roundCache        *common.LRU
	timestampCache    *common.LRU
	witnessCache      *common.LRU

	logger log.Logger
}

//NewHashgraph instantiates a Hashgraph with an underlying data store and a
//commit callback
func NewHashgraph(store Store, commitCallback InternalCommitCallback, logger log.Logger) *Hashgraph {
	cacheSize := store.CacheSize()
	hashgraph := Hashgraph{
		Store:             store,
		PendingRounds:     NewPendingRoundsCache(),
		PendingSignatures: NewSigPool(),
		commitCallback:    commitCallback,
		ancestorCache:     common.NewLRU(cacheSize, nil),
		selfAncestorCache: common.NewLRU(cacheSize, nil),
		stronglySeeCache:  common.NewLRU(cacheSize, nil),
		roundCache:        common.NewLRU(cacheSize, nil),
		timestampCache:    common.NewLRU(cacheSize, nil),
		witnessCache:      common.NewLRU(cacheSize, nil),
		logger:            logger,
	}

	return &hashgraph
}

//Init sets the initial PeerSet, which also creates the corresponding Roots and
//updates the Repertoire.
func (h *Hashgraph) Init(peerSet *peers.PeerSet) error {
	if err := h.Store.SetPeerSet(0, peerSet); err != nil {
		return fmt.Errorf("Error setting PeerSet: %v", err)
	}

	//XXX Do something else? Genesis Block?

	return nil
}

/*******************************************************************************
Private Methods
*******************************************************************************/

//true if y is an ancestor of x
func (h *Hashgraph) ancestor(x, y string) (bool, error) {
	if c, ok := h.ancestorCache.Get(Key{x, y}); ok {
		return c.(bool), nil
	}
	a, err := h._ancestor(x, y)
	if err != nil {
		return false, err
	}
	h.ancestorCache.Add(Key{x, y}, a)
	return a, nil
}

func (h *Hashgraph) _ancestor(x, y string) (bool, error) {
	if x == y {
		return true, nil
	}

	ex, err := h.Store.GetEvent(x)
	if err != nil {
		return false, err
	}

	ey, err := h.Store.GetEvent(y)
	if err != nil {
		return false, err
	}

	entry, ok := ex.lastAncestors[ey.Creator()]

	res := ok && entry.index >= ey.Index()

	return res, nil
}

//true if y is a self-ancestor of x
func (h *Hashgraph) selfAncestor(x, y string) (bool, error) {
	if c, ok := h.selfAncestorCache.Get(Key{x, y}); ok {
		return c.(bool), nil
	}
	a, err := h._selfAncestor(x, y)
	if err != nil {
		return false, err
	}
	h.selfAncestorCache.Add(Key{x, y}, a)
	return a, nil
}

func (h *Hashgraph) _selfAncestor(x, y string) (bool, error) {
	if x == y {
		return true, nil
	}
	ex, err := h.Store.GetEvent(x)
	if err != nil {
		return false, err
	}

	ey, err := h.Store.GetEvent(y)
	if err != nil {
		return false, err
	}

	return ex.Creator() == ey.Creator() && ex.Index() >= ey.Index(), nil
}

/*
True if x sees y

It is not necessary to detect forks because we assume that the InsertEvent
function makes it impossible to insert two Events at the same height for
the same participant.
*/
func (h *Hashgraph) see(x, y string) (bool, error) {
	return h.ancestor(x, y)
}

//true if x strongly sees y based on peers set
func (h *Hashgraph) stronglySee(x, y string, peers *peers.PeerSet) (bool, error) {
	if c, ok := h.stronglySeeCache.Get(TreKey{x, y, peers.Hex()}); ok {
		return c.(bool), nil
	}
	ss, err := h._stronglySee(x, y, peers)
	if err != nil {
		return false, err
	}
	h.stronglySeeCache.Add(TreKey{x, y, peers.Hex()}, ss)
	return ss, nil
}

func (h *Hashgraph) _stronglySee(x, y string, peers *peers.PeerSet) (bool, error) {

	ex, err := h.Store.GetEvent(x)
	if err != nil {
		return false, err
	}

	ey, err := h.Store.GetEvent(y)
	if err != nil {
		return false, err
	}

	c := 0
	for p := range peers.ByPubKey {
		xla, xlaok := ex.lastAncestors[p]
		yfd, yfdok := ey.firstDescendants[p]
		if xlaok && yfdok && xla.index >= yfd.index {
			c++
		}
	}

	return c >= peers.SuperMajority(), nil
}

func (h *Hashgraph) round(x string) (int, error) {
	if c, ok := h.roundCache.Get(x); ok {
		return c.(int), nil
	}
	r, err := h._round(x)
	if err != nil {
		return -1, err
	}
	h.roundCache.Add(x, r)
	return r, nil
}

func (h *Hashgraph) _round(x string) (int, error) {
	ex, err := h.Store.GetEvent(x)
	if err != nil {
		return math.MinInt32, err
	}

	parentRound := -1

	if ex.SelfParent() != "" {
		parentRound, err = h.round(ex.SelfParent())
		if err != nil {
			return math.MinInt32, err
		}
	}

	if ex.OtherParent() != "" {
		opRound, err := h.round(ex.OtherParent())
		if err != nil {
			return math.MinInt32, err
		}

		if opRound > parentRound {
			parentRound = opRound
		}
	}

	if parentRound == -1 {
		return 0, nil
	}

	//Retrieve the ParentRound's PeerSet and count strongly-seen witnesses based
	//on this PeerSet.
	parentRoundObj, err := h.Store.GetRound(parentRound)
	if err != nil {
		return math.MinInt32, err
	}

	parentRoundPeerSet, err := h.Store.GetPeerSet(parentRound)
	if err != nil {
		return math.MinInt32, err
	}

	c := 0
	for _, w := range parentRoundObj.Witnesses() {
		ss, err := h.stronglySee(x, w, parentRoundPeerSet)
		if err != nil {
			return math.MinInt32, err
		}
		if ss {
			c++
		}
	}

	if c >= parentRoundPeerSet.SuperMajority() {
		parentRound++
	}

	return parentRound, nil
}

func (h *Hashgraph) witness(x string) (bool, error) {
	if c, ok := h.witnessCache.Get(x); ok {
		return c.(bool), nil
	}
	r, err := h._witness(x)
	if err != nil {
		return false, err
	}
	h.witnessCache.Add(x, r)
	return r, nil
}

//true if x is a witness (first event of a round for the owner)
func (h *Hashgraph) _witness(x string) (bool, error) {
	ex, err := h.Store.GetEvent(x)
	if err != nil {
		return false, err
	}

	xRound, err := h.round(x)
	if err != nil {
		return false, err
	}

	//does the creator belong to the PeerSet?
	peerSet, err := h.Store.GetPeerSet(xRound)
	if err != nil {
		return false, err
	}

	if _, ok := peerSet.ByPubKey[ex.Creator()]; !ok {
		return false, nil
	}

	spRound := -1
	if ex.SelfParent() != "" {
		spRound, err = h.round(ex.SelfParent())
		if err != nil {
			return false, err
		}
	}

	return xRound > spRound, nil
}

func (h *Hashgraph) roundReceived(x string) (int, error) {
	ex, err := h.Store.GetEvent(x)
	if err != nil {
		return -1, err
	}

	res := -1
	if ex.roundReceived != nil {
		res = *ex.roundReceived
	}

	return res, nil
}

func (h *Hashgraph) lamportTimestamp(x string) (int, error) {
	if c, ok := h.timestampCache.Get(x); ok {
		return c.(int), nil
	}
	r, err := h._lamportTimestamp(x)
	if err != nil {
		return -1, err
	}
	h.timestampCache.Add(x, r)
	return r, nil
}

func (h *Hashgraph) _lamportTimestamp(x string) (int, error) {
	plt := -1

	ex, err := h.Store.GetEvent(x)
	if err != nil {
		return math.MinInt32, err
	}

	if ex.SelfParent() != "" {
		plt, err = h.lamportTimestamp(ex.SelfParent())
		if err != nil {
			return math.MinInt32, err
		}
	}

	if ex.OtherParent() != "" {
		opLT := math.MinInt32
		if _, err := h.Store.GetEvent(ex.OtherParent()); err == nil {
			//if we know the other-parent, fetch its Round directly
			t, err := h.lamportTimestamp(ex.OtherParent())
			if err != nil {
				return math.MinInt32, err
			}
			opLT = t
		}

		if opLT > plt {
			plt = opLT
		}
	}

	return plt + 1, nil
}

//round(x) - round(y)
func (h *Hashgraph) roundDiff(x, y string) (int, error) {
	xRound, err := h.round(x)
	if err != nil {
		return math.MinInt32, fmt.Errorf("event %s has negative round", x)
	}

	yRound, err := h.round(y)
	if err != nil {
		return math.MinInt32, fmt.Errorf("event %s has negative round", y)
	}

	return xRound - yRound, nil
}

//Check the SelfParent is the Creator's last known Event
//returns error, warning
func (h *Hashgraph) checkSelfParent(event *Event) (err, warn error) {
	selfParent := event.SelfParent()
	creator := event.Creator()

	creatorLastKnown, err := h.Store.LastEventFrom(creator)
	if err != nil {
		//First Event
		if common.Is(err, common.Empty) && selfParent == "" {
			return nil, nil
		}
		return err, nil
	}

	selfParentLegit := selfParent == creatorLastKnown

	//If you find this line using grep, the appearance of this event in the logs
	//is to be expected in normal operation and may not be a cause for concern.
	if !selfParentLegit {
		return nil, fmt.Errorf("Self-parent not last known event by creator")
	}

	return nil, nil
}

//Check if we know the OtherParent
func (h *Hashgraph) checkOtherParent(event *Event) error {
	otherParent := event.OtherParent()
	if otherParent != "" {
		//Check if we have it
		_, err := h.Store.GetEvent(otherParent)
		if err != nil {
			return fmt.Errorf("Other-parent not known")
		}
	}
	return nil
}

//initialize arrays of last ancestors and first descendants
func (h *Hashgraph) initEventCoordinates(event *Event) error {
	event.lastAncestors = NewCoordinatesMap()
	event.firstDescendants = NewCoordinatesMap()

	selfParent, selfParentError := h.Store.GetEvent(event.SelfParent())
	otherParent, otherParentError := h.Store.GetEvent(event.OtherParent())

	if selfParentError != nil && otherParentError == nil {
		event.lastAncestors = otherParent.lastAncestors.Copy()
	} else if otherParentError != nil && selfParentError == nil {
		event.lastAncestors = selfParent.lastAncestors.Copy()
	} else if otherParentError == nil && selfParentError == nil {
		selfParentLastAncestors := selfParent.lastAncestors
		otherParentLastAncestors := otherParent.lastAncestors

		event.lastAncestors = selfParentLastAncestors.Copy()
		for p, ola := range otherParentLastAncestors {
			sla, ok := event.lastAncestors[p]
			if !ok || sla.index < ola.index {
				event.lastAncestors[p] = EventCoordinates{
					index: ola.index,
					hash:  ola.hash,
				}
			}
		}
	}

	event.firstDescendants[event.Creator()] = EventCoordinates{
		index: event.Index(),
		hash:  event.Hex(),
	}

	event.lastAncestors[event.Creator()] = EventCoordinates{
		index: event.Index(),
		hash:  event.Hex(),
	}

	return nil
}

//update first descendant of each last ancestor to point to event
func (h *Hashgraph) updateAncestorFirstDescendant(event *Event) error {
	for _, c := range event.lastAncestors {
		ah := c.hash
		for {
			a, err := h.Store.GetEvent(ah)
			if err != nil {
				break
			}

			_, ok := a.firstDescendants[event.Creator()]
			if !ok {
				a.firstDescendants[event.Creator()] = EventCoordinates{
					index: event.Index(),
					hash:  event.Hex(),
				}
				if err := h.Store.SetEvent(a); err != nil {
					return err
				}
				ah = a.SelfParent()
			} else {
				break
			}
		}
	}
	return nil
}

func (h *Hashgraph) createFrameEvent(x string) (*FrameEvent, error) {
	ev, err := h.Store.GetEvent(x)
	if err != nil {
		return nil, fmt.Errorf("FrameEvent %s not found", x)
	}

	round, err := h.round(x)
	if err != nil {
		return nil, err
	}

	roundInfo, err := h.Store.GetRound(round)
	if err != nil {
		return nil, err
	}

	te, ok := roundInfo.CreatedEvents[x]
	if !ok {
		return nil, err
	}

	witness := te.Witness

	lt, err := h.lamportTimestamp(x)
	if err != nil {
		return nil, err
	}

	frameEvent := &FrameEvent{
		Core:             ev,
		Round:            round,
		LamportTimestamp: lt,
		Witness:          witness,
	}

	return frameEvent, nil
}

func (h *Hashgraph) createRoot(participant string, head string) (*Root, error) {
	root := NewRoot()

	if head != "" {
		headEvent, err := h.createFrameEvent(head)
		if err != nil {
			return nil, err
		}

		reverseRootEvents := []*FrameEvent{headEvent}

		index := headEvent.Core.Index()
		for i := 0; i < ROOT_DEPTH; i++ {
			index = index - 1
			if index >= 0 {
				peh, err := h.Store.ParticipantEvent(participant, index)
				if err != nil {
					break
				}
				rev, err := h.createFrameEvent(peh)
				if err != nil {
					return nil, err
				}
				reverseRootEvents = append(reverseRootEvents, rev)

			} else {
				break
			}
		}

		for i := len(reverseRootEvents) - 1; i >= 0; i-- {
			root.Insert(reverseRootEvents[i])
		}
	}

	return root, nil
}

func (h *Hashgraph) setWireInfo(event *Event) error {
	selfParentIndex := -1
	otherParentCreatorID := uint32(0)
	otherParentIndex := -1

	creator, ok := h.Store.RepertoireByPubKey()[event.Creator()]
	if !ok {
		return fmt.Errorf("Creator %s not found", event.Creator())
	}

	if event.SelfParent() != "" {
		selfParent, err := h.Store.GetEvent(event.SelfParent())
		if err != nil {
			return err
		}
		selfParentIndex = selfParent.Index()
	}

	if event.OtherParent() != "" {
		otherParent, err := h.Store.GetEvent(event.OtherParent())
		if err != nil {
			return err
		}
		otherParentCreator, ok := h.Store.RepertoireByPubKey()[otherParent.Creator()]
		if !ok {
			return fmt.Errorf("Creator %s not found", otherParent.Creator())
		}
		otherParentCreatorID = otherParentCreator.ID()
		otherParentIndex = otherParent.Index()
	}

	event.SetWireInfo(selfParentIndex,
		otherParentCreatorID,
		otherParentIndex,
		creator.ID())

	return nil
}

//Remove processed Signatures from SigPool
func (h *Hashgraph) removeProcessedSignatures(processedSignatures map[string]bool) {
	for k := range processedSignatures {
		h.PendingSignatures.Remove(k)
	}
}

/*******************************************************************************
Public Methods
*******************************************************************************/

//InsertEventAndRunConsensus inserts an Event in the Hashgraph and call the
//consensus methods.
func (h *Hashgraph) InsertEventAndRunConsensus(event *Event, setWireInfo bool) error {
	if err := h.InsertEvent(event, setWireInfo); err != nil {
		h.logger.Error("InsertEvent", "err", err)
		return err
	}
	if err := h.DivideRounds(); err != nil {
		h.logger.Error("DivideRounds", "err", err)
		return err
	}
	if err := h.DecideFame(); err != nil {
		h.logger.Error("DecideFame", "err", err)
		return err
	}
	if err := h.DecideRoundReceived(); err != nil {
		h.logger.Error("DecideRoundReceived", "err", err)
		return err
	}
	if err := h.ProcessDecidedRounds(); err != nil {
		h.logger.Error("ProcessDecidedRounds", "err", err)
		return err
	}
	return nil
}

//InsertEvent attempts to insert an Event in the DAG. It verifies the signature,
//checks the ancestors are known, and prevents the introduction of forks.
func (h *Hashgraph) InsertEvent(event *Event, setWireInfo bool) error {
	//verify signature
	if ok, err := event.Verify(); !ok {
		if err != nil {
			return err
		}
		return fmt.Errorf("Invalid Event signature")
	}

	if err, warn := h.checkSelfParent(event); err != nil {
		h.logger.Error("CheckSelfParent",
			"event", event.Hex(),
			"creator", event.Creator(),
			"self_parent", event.SelfParent(),
			"err", err,
		)
		return err
	} else {
		if warn != nil {
			h.logger.Info("CheckSelfParent",
				"event", event.Hex(),
				"creator", event.Creator(),
				"self_parent", event.SelfParent(),
				"err", err,
			)
			return warn
		}
	}

	if err := h.checkOtherParent(event); err != nil {
		h.logger.Error("CheckOtherParent",
			"event", event.Hex(),
			"creator", event.Creator(),
			"other_parent", event.OtherParent(),
			"err", err,
		)
		return err
	}

	event.topologicalIndex = h.topologicalIndex
	h.topologicalIndex++

	if setWireInfo {
		if err := h.setWireInfo(event); err != nil {
			return fmt.Errorf("SetWireInfo: %s", err)
		}
	}

	if err := h.initEventCoordinates(event); err != nil {
		return fmt.Errorf("InitEventCoordinates: %s", err)
	}

	if err := h.Store.SetEvent(event); err != nil {
		return fmt.Errorf("SetEvent: %s", err)
	}

	if err := h.updateAncestorFirstDescendant(event); err != nil {
		return fmt.Errorf("UpdateAncestorFirstDescendant: %s", err)
	}

	h.UndeterminedEvents = append(h.UndeterminedEvents, event.Hex())

	if event.IsLoaded() {
		h.PendingLoadedEvents++
	}

	for _, bs := range event.BlockSignatures() {
		h.logger.Debug("Inserting pending signature", bs.Key())
		h.PendingSignatures.Add(bs)
	}

	return nil
}

//InsertFrameEvent inserts the FrameEvent's core Event, without checking its
//parents or signature. It doesnt add the Event to UndeterminedEvents either.
func (h *Hashgraph) InsertFrameEvent(frameEvent *FrameEvent) error {
	event := frameEvent.Core

	//Set caches so round, witness, and timestamp won't be recalculated
	h.roundCache.Add(event.Hex(), frameEvent.Round)
	h.witnessCache.Add(event.Hex(), frameEvent.Witness)
	h.timestampCache.Add(event.Hex(), frameEvent.LamportTimestamp)

	//Set the event's private fields for later use
	event.SetRound(frameEvent.Round)
	event.SetLamportTimestamp(frameEvent.LamportTimestamp)

	//Create/update RoundInfo object in store
	roundInfo, err := h.Store.GetRound(frameEvent.Round)
	if err != nil {
		if !common.Is(err, common.KeyNotFound) {
			return err
		}
		roundInfo = NewRoundInfo()
	}
	roundInfo.AddCreatedEvent(event.Hex(), frameEvent.Witness)

	err = h.Store.SetRound(frameEvent.Round, roundInfo)
	if err != nil {
		return err
	}

	//Init EventCoordinates.
	if err := h.initEventCoordinates(event); err != nil {
		return fmt.Errorf("InitEventCoordinates: %s", err)
	}

	if err := h.Store.SetEvent(event); err != nil {
		return fmt.Errorf("SetEvent: %s", err)
	}

	if err := h.updateAncestorFirstDescendant(event); err != nil {
		return fmt.Errorf("UpdateAncestorFirstDescendant: %s", err)
	}

	//All FrameEvents are consensus events, ie. they have a round-received and
	//were committed. We need to record FrameEvents as consensus events because
	//it comes into play in GetFrame/CreateRoot
	if err := h.Store.AddConsensusEvent(event); err != nil {
		return fmt.Errorf("AddConsensusEvent: %v", event)
	}

	return nil
}

//DivideRounds assigns a Round and LamportTimestamp to Events, and flags them as
//witnesses if necessary. Pushes Rounds in the PendingRounds queue if necessary.
func (h *Hashgraph) DivideRounds() error {
	for _, hash := range h.UndeterminedEvents {
		ev, err := h.Store.GetEvent(hash)
		if err != nil {
			return err
		}

		updateEvent := false

		//Compute Event's round, update the corresponding Round object, and add
		//it to the PendingRounds queue if necessary.
		if ev.round == nil {
			roundNumber, err := h.round(hash)
			if err != nil {
				return err
			}

			ev.SetRound(roundNumber)
			updateEvent = true

			roundInfo, err := h.Store.GetRound(roundNumber)
			if err != nil {
				if !common.Is(err, common.KeyNotFound) {
					return err
				}
				roundInfo = NewRoundInfo()
			}

			if !h.PendingRounds.Queued(roundNumber) &&
				!roundInfo.decided &&
				(h.roundLowerBound == nil || roundNumber > *h.roundLowerBound) {

				h.PendingRounds.Set(&PendingRound{roundNumber, false})
			}

			witness, err := h.witness(hash)
			if err != nil {
				return err
			}

			roundInfo.AddCreatedEvent(hash, witness)

			err = h.Store.SetRound(roundNumber, roundInfo)
			if err != nil {
				return err
			}
		}

		//Compute the Event's LamportTimestamp
		if ev.lamportTimestamp == nil {
			lamportTimestamp, err := h.lamportTimestamp(hash)
			if err != nil {
				return err
			}
			ev.SetLamportTimestamp(lamportTimestamp)
			updateEvent = true
		}

		if updateEvent {
			h.Store.SetEvent(ev)
		}
	}

	return nil
}

//DecideFame decides if witnesses are famous
func (h *Hashgraph) DecideFame() error {
	//Initialize the vote map
	votes := make(map[string](map[string]bool)) //[x][y]=>vote(x,y)
	setVote := func(votes map[string]map[string]bool, x, y string, vote bool) {
		if votes[x] == nil {
			votes[x] = make(map[string]bool)
		}
		votes[x][y] = vote
	}

	decidedRounds := []int{}

	for _, r := range h.PendingRounds.GetOrderedPendingRounds() {
		roundIndex := r.Index

		rRoundInfo, err := h.Store.GetRound(roundIndex)
		if err != nil {
			return err
		}

		rPeerSet, err := h.Store.GetPeerSet(roundIndex)
		if err != nil {
			return err
		}

		for _, x := range rRoundInfo.Witnesses() {
			if rRoundInfo.IsDecided(x) {
				continue
			}
		VOTE_LOOP:
			for j := roundIndex + 1; j <= h.Store.LastRound(); j++ {
				jRoundInfo, err := h.Store.GetRound(j)
				if err != nil {
					return err
				}

				jPeerSet, err := h.Store.GetPeerSet(j)
				if err != nil {
					return err
				}

				for _, y := range jRoundInfo.Witnesses() {
					diff := j - roundIndex
					if diff == 1 {
						ycx, err := h.see(y, x)
						if err != nil {
							return err
						}
						setVote(votes, y, x, ycx)
					} else {
						jPrevRoundInfo, err := h.Store.GetRound(j - 1)
						if err != nil {
							return err
						}

						jPrevPeerSet, err := h.Store.GetPeerSet(j - 1)
						if err != nil {
							return err
						}

						//collection of witnesses from round j-1 that are
						//strongly seen by y, based on round j-1 PeerSet.
						ssWitnesses := []string{}
						for _, w := range jPrevRoundInfo.Witnesses() {
							ss, err := h.stronglySee(y, w, jPrevPeerSet)
							if err != nil {
								return err
							}
							if ss {
								ssWitnesses = append(ssWitnesses, w)
							}
						}

						//Collect votes from these witnesses.
						yays := 0
						nays := 0
						for _, w := range ssWitnesses {
							if votes[w][x] {
								yays++
							} else {
								nays++
							}
						}
						v := false
						t := nays
						if yays >= nays {
							v = true
							t = yays
						}

						//normal round
						if math.Mod(float64(diff), COIN_ROUND_FREQ) > 0 {
							if t >= jPeerSet.SuperMajority() {
								rRoundInfo.SetFame(x, v)
								setVote(votes, y, x, v)
								break VOTE_LOOP //break out of j loop
							} else {
								setVote(votes, y, x, v)
							}
						} else { //coin round
							if t >= jPeerSet.SuperMajority() {
								setVote(votes, y, x, v)
							} else {
								setVote(votes, y, x, middleBit(y)) //middle bit of y's hash
							}
						}
					}
				}
			}
		}

		if rRoundInfo.WitnessesDecided(rPeerSet) {
			decidedRounds = append(decidedRounds, roundIndex)
		}

		err = h.Store.SetRound(roundIndex, rRoundInfo)
		if err != nil {
			return err
		}
	}

	h.PendingRounds.Update(decidedRounds)
	return nil
}

//DecideRoundReceived assigns a RoundReceived to undetermined events when they
//reach consensus
func (h *Hashgraph) DecideRoundReceived() error {
	newUndeterminedEvents := []string{}

	/* From whitepaper - 18/03/18
	   "[...] An event is said to be “received” in the first round where all the
	   unique famous witnesses have received it, if all earlier rounds have the
	   fame of all witnesses decided"
	*/
	for _, x := range h.UndeterminedEvents {
		received := false

		r, err := h.round(x)
		if err != nil {
			return err
		}

		for i := r + 1; i <= h.Store.LastRound(); i++ {
			tr, err := h.Store.GetRound(i)
			if err != nil {
				return err
			}

			tPeers, err := h.Store.GetPeerSet(i)
			if err != nil {
				return err
			}

			/*
				We are looping from earlier to later rounds; so if we encounter
				one round with undecided witnesses, we are sure that this event
				is not "received". Break out of i loop. This is not true for
				events below the roundLowerBound because these rounds are never
				processed by the DecideFame routine. It's ok because events
				below this round are either already committed or will be
				received later, so just continue through the i loop.
			*/
			if !(tr.WitnessesDecided(tPeers)) {
				if h.roundLowerBound == nil || *h.roundLowerBound < i {
					break
				} else {
					continue
				}
			}

			fws := tr.FamousWitnesses()
			//set of famous witnesses that see x
			s := []string{}
			for _, w := range fws {
				see, err := h.see(w, x)
				if err != nil {
					return err
				}
				if see {
					s = append(s, w)
				}
			}

			if len(s) == len(fws) && len(s) >= tPeers.SuperMajority() {
				received = true

				ex, err := h.Store.GetEvent(x)
				if err != nil {
					return err
				}

				ex.SetRoundReceived(i)

				err = h.Store.SetEvent(ex)
				if err != nil {
					return err
				}

				tr.AddReceivedEvent(x)
				err = h.Store.SetRound(i, tr)
				if err != nil {
					return err
				}

				//break out of i loop
				break
			}
		}

		if !received {
			newUndeterminedEvents = append(newUndeterminedEvents, x)
		}
	}

	h.UndeterminedEvents = newUndeterminedEvents

	return nil
}

/*
ProcessDecidedRounds takes Rounds whose witnesses are decided, computes the
corresponding Frames, maps them into Blocks, and commits the Blocks via the
commit channel
*/
func (h *Hashgraph) ProcessDecidedRounds() error {
	//Defer removing processed Rounds from the PendingRounds Queue
	processedRounds := []int{}
	defer func() {
		h.PendingRounds.Clean(processedRounds)
	}()

	for _, r := range h.PendingRounds.GetOrderedPendingRounds() {
		//Although it is possible for a Round to be 'decided' before a previous
		//round, we should NEVER process a decided round before all the earlier
		//rounds are processed.
		if !r.Decided {
			break
		}

		round, err := h.Store.GetRound(r.Index)
		if err != nil {
			return err
		}

		frame, err := h.GetFrame(r.Index)
		if err != nil {
			return fmt.Errorf("Getting Frame %d: %v", r.Index, err)
		}

		h.logger.Debug("Processing Decided Round",
			"round_received", r.Index,
			"witnesses", round.FamousWitnesses(),
			"created_events", len(round.CreatedEvents),
			"events", len(frame.Events),
			"peers", len(frame.Peers),
			"peer_sets", len(frame.PeerSets),
			"roots", len(frame.Roots),
		)

		if len(frame.Events) > 0 {
			for _, e := range frame.Events {
				err := h.Store.AddConsensusEvent(e.Core)
				if err != nil {
					return err
				}

				h.ConsensusTransactions += len(e.Core.Transactions())

				if e.Core.IsLoaded() {
					h.PendingLoadedEvents--
				}
			}

			lastBlockIndex := h.Store.LastBlockIndex()
			block, err := NewBlockFromFrame(lastBlockIndex+1, frame)
			if err != nil {
				return err
			}

			if len(block.Transactions()) > 0 ||
				len(block.InternalTransactions()) > 0 {

				if err := h.Store.SetBlock(block); err != nil {
					return err
				}

				err := h.commitCallback(block)
				if err != nil {
					h.logger.Info("Failed to commit block", block.Index())
				}
			}
		} else {
			h.logger.Debug("No Events to commit for ConsensusRound", r.Index)
		}

		processedRounds = append(processedRounds, r.Index)

		if h.LastConsensusRound == nil || r.Index > *h.LastConsensusRound {
			h.setLastConsensusRound(r.Index)
		}
	}

	return nil
}

//GetFrame computes the Frame corresponding to a RoundReceived.
func (h *Hashgraph) GetFrame(roundReceived int) (*Frame, error) {
	//Try to get it from the Store first
	frame, err := h.Store.GetFrame(roundReceived)
	if err == nil || !common.Is(err, common.KeyNotFound) {
		return frame, err
	}

	//Get the Round and corresponding consensus Events
	round, err := h.Store.GetRound(roundReceived)
	if err != nil {
		return nil, err
	}

	peerSet, err := h.Store.GetPeerSet(roundReceived)
	if err != nil {
		return nil, err
	}

	events := []*FrameEvent{}
	for _, eh := range round.ReceivedEvents {
		re, err := h.createFrameEvent(eh)
		if err != nil {
			return nil, err
		}
		events = append(events, re)
	}

	sort.Sort(SortedFrameEvents(events))

	/*
		Get/Create Roots. The events are in topological order; so each time we
		run into the first Event of a participant, we create a Root for it. Then
		we populate the root's Events slice.
	*/
	roots := make(map[string]*Root)

	for _, ev := range events {
		p := ev.Core.Creator()
		r, ok := roots[p]
		if !ok {
			r, err = h.createRoot(p, ev.Core.SelfParent())
			if err != nil {
				return nil, err
			}
			roots[p] = r
		}
	}

	/*
		Every participant, that was known before roundReceived, needs a Root in
		the Frame. For the participants that have no Events in this Frame, we
		create a Root from their last consensus Event, or their last known Root
	*/
	for p, peer := range h.Store.RepertoireByPubKey() {
		//Ignore if participant wasn't added before roundReceived
		firstRound, ok := h.Store.FirstRound(peer.ID())
		if !ok || firstRound > roundReceived {
			continue
		}

		if _, ok := roots[p]; !ok {
			var root *Root

			lastConsensusEventHash, err := h.Store.LastConsensusEventFrom(p)
			if err != nil {
				return nil, err
			}

			root, err = h.createRoot(p, lastConsensusEventHash)
			if err != nil {
				return nil, err
			}

			roots[p] = root
		}
	}

	//Get all PeerSets
	allPeerSets, err := h.Store.GetAllPeerSets()
	if err != nil {
		return nil, err
	}

	res := &Frame{
		Round:    roundReceived,
		Peers:    peerSet.Peers,
		Roots:    roots,
		Events:   events,
		PeerSets: allPeerSets,
	}

	if err := h.Store.SetFrame(res); err != nil {
		return nil, err
	}

	return res, nil
}

/*
ProcessSigPool runs through the SignaturePool and tries to map a Signature to
a known Block. If a Signature is valid, it is appended to the block and removed
from the SignaturePool. The function also updates the AnchorBlock if necessary.
*/
func (h *Hashgraph) ProcessSigPool() error {
	h.logger.Debug("ProcessSigPool()", "pending_signatures", h.PendingSignatures.Len())

	for _, bs := range h.PendingSignatures.Items() {
		block, err := h.Store.GetBlock(bs.Index)
		if err != nil {
			h.logger.Info("Verifying Block signature. Could not fetch Block",
				"index", bs.Index,
				"msg", err,
			)
			continue
		}

		peerSet, err := h.Store.GetPeerSet(block.RoundReceived())
		if err != nil {
			h.logger.Debug("Verifying Block signature. No PeerSet for Block's Round ",
				"index", bs.Index,
				"round", block.RoundReceived(),
				"err", err,
			)
			continue
		}

		//check if validator belongs to list of participants
		if _, ok := peerSet.ByPubKey[bs.ValidatorHex()]; !ok {
			h.logger.Debug("Verifying Block signature. Validator does not belong to Block's PeerSet",
				"index", bs.Index,
				"round", block.RoundReceived(),
				"validator", bs.ValidatorHex(),
				"peers", peerSet.Peers,
			)

			continue
		}

		valid, err := block.Verify(bs)
		if err != nil {
			h.logger.Error("Verifying Block signature",
				"index", bs.Index,
				"msg", err,
			)
			return err
		}
		if !valid {
			h.logger.Debug("Verifying Block signature. Invalid signature",
				"index", bs.Index,
				"validator", peerSet.ByPubKey[bs.ValidatorHex()],
				"block", block,
			)
			continue
		}

		block.SetSignature(bs)

		if err := h.Store.SetBlock(block); err != nil {
			h.logger.Debug("Saving Block",
				"index", bs.Index,
				"msg", err,
			)
		}

		if err := h.SetAnchorBlock(block); err != nil {
			return err
		}

		h.logger.Debug("processed sig", bs.Key())

		h.PendingSignatures.Remove(bs.Key())
	}

	return nil
}

/*
SetAnchorBlock sets the AnchorBlock index if the proposed block has collected
enough signatures (+1/3) and is above the current AnchorBlock. The AnchorBlock
is the latest Block that collected +1/3 signatures from validators. It is used
in FastForward responses when a node wants to sync to the top of the hashgraph.
*/
func (h *Hashgraph) SetAnchorBlock(block *Block) error {
	peerSet, err := h.Store.GetPeerSet(block.RoundReceived())
	if err != nil {
		h.logger.Error("No PeerSet for Block's Round ", "err", err)
		return err
	}

	if len(block.Signatures) > peerSet.TrustCount() &&
		(h.AnchorBlock == nil ||
			block.Index() > *h.AnchorBlock) {

		h.setAnchorBlock(block.Index())
		h.logger.Debug("Setting AnchorBlock",
			"block_index", block.Index(),
			"signatures", len(block.Signatures),
			"trustCount", peerSet.TrustCount(),
		)
	} else {
		var msg string
		if h.AnchorBlock != nil {
			msg = strconv.Itoa(*h.AnchorBlock)
		} else {
			msg = "Anchor Block not set"
		}
		h.logger.Debug("Block is not a suitable Anchor",
			"index", block.Index(),
			"sigs", len(block.Signatures),
			"trust_count", peerSet.TrustCount(),
			"anchor_block", msg,
		)
	}

	return nil
}

//GetAnchorBlockWithFrame returns the AnchorBlock and the corresponding Frame.
//This can be used as a base to Reset a Hashgraph
func (h *Hashgraph) GetAnchorBlockWithFrame() (*Block, *Frame, error) {
	if h.AnchorBlock == nil {
		return nil, nil, fmt.Errorf("No Anchor Block")
	}

	block, err := h.Store.GetBlock(*h.AnchorBlock)
	if err != nil {
		return nil, nil, err
	}

	frame, err := h.GetFrame(block.RoundReceived())
	if err != nil {
		return nil, nil, err
	}

	return block, frame, nil
}

//Reset clears the Hashgraph and resets it from a new base.
func (h *Hashgraph) Reset(block *Block, frame *Frame) error {
	//Clear all state
	h.LastConsensusRound = nil
	h.FirstConsensusRound = nil
	h.AnchorBlock = nil

	h.UndeterminedEvents = []string{}
	h.PendingRounds = NewPendingRoundsCache()
	h.PendingLoadedEvents = 0
	h.topologicalIndex = 0

	cacheSize := h.Store.CacheSize()
	h.ancestorCache = common.NewLRU(cacheSize, nil)
	h.selfAncestorCache = common.NewLRU(cacheSize, nil)
	h.stronglySeeCache = common.NewLRU(cacheSize, nil)
	h.roundCache = common.NewLRU(cacheSize, nil)
	h.witnessCache = common.NewLRU(cacheSize, nil)

	//Initialize new Roots
	if err := h.Store.Reset(frame); err != nil {
		return err
	}

	//Insert FrameEvents
	sortedFrameEvents := frame.SortedFrameEvents()
	for _, rev := range sortedFrameEvents {
		if err := h.InsertFrameEvent(rev); err != nil {
			return err
		}
	}

	//Insert Block
	if err := h.Store.SetBlock(block); err != nil {
		return err
	}
	h.setLastConsensusRound(block.RoundReceived())
	h.setRoundLowerBound(block.RoundReceived())

	return nil
}

/*
Bootstrap loads all Events from the Store's DB (if there is one) and feeds
them to the Hashgraph consensus methods in topological order. It is assumed that
no events are skipped/lost when loading from the database - WE CAN ONLY
BOOTSTRAP FROM 0. As Events are inserted and processed, Blocks will be created
and committed to the App layer (via the commit callback), so it is also assumed
that the application state was reset.
*/
func (h *Hashgraph) Bootstrap() error {
	if badgerStore, ok := h.Store.(*BadgerStore); ok {
		//Load Genesis PeerSet
		peerSet, err := badgerStore.dbGetPeerSet(0)
		if err != nil {
			return fmt.Errorf("No Genesis PeerSet: %v", err)
		}

		//Initialize the InmemStore with Genesis PeerSet. This has side-effects:
		//It will create the corresponding Roots and populate the Repertoires.
		badgerStore.inmemStore.SetPeerSet(0, peerSet)

		//Retreive the Events from the underlying DB. They come out in topological
		//order
		topologicalEvents, err := badgerStore.dbTopologicalEvents()
		if err != nil {
			return err
		}

		//Insert the Events in the Hashgraph
		for _, e := range topologicalEvents {
			if err := h.InsertEventAndRunConsensus(e, true); err != nil {
				return err
			}
		}

		//ProcessSigPool
		if err := h.ProcessSigPool(); err != nil {
			return err
		}
	}

	return nil
}

//ReadWireInfo converts a WireEvent to an Event by replacing int IDs with the
//corresponding public keys.
func (h *Hashgraph) ReadWireInfo(wevent WireEvent) (*Event, error) {
	selfParent := ""
	otherParent := ""
	var err error

	creator, ok := h.Store.RepertoireByID()[wevent.Body.CreatorID]
	if !ok {
		return nil, fmt.Errorf("Creator %d not found", wevent.Body.CreatorID)
	}

	creatorBytes, err := common.DecodeFromString(creator.PubKeyString())
	if err != nil {
		return nil, err
	}

	if wevent.Body.SelfParentIndex >= 0 {
		selfParent, err = h.Store.ParticipantEvent(creator.PubKeyString(), wevent.Body.SelfParentIndex)
		if err != nil {
			return nil, err
		}
	}

	if wevent.Body.OtherParentIndex >= 0 {
		otherParentCreator, ok := h.Store.RepertoireByID()[wevent.Body.OtherParentCreatorID]
		if !ok {
			return nil, fmt.Errorf("Participant %d not found", wevent.Body.OtherParentCreatorID)
		}

		otherParent, err = h.Store.ParticipantEvent(otherParentCreator.PubKeyString(), wevent.Body.OtherParentIndex)
		if err != nil {
			return nil, fmt.Errorf("OtherParent (creator: %d, index: %d) not found", wevent.Body.OtherParentCreatorID, wevent.Body.OtherParentIndex)
		}
	}

	body := EventBody{
		Transactions:         wevent.Body.Transactions,
		InternalTransactions: wevent.Body.InternalTransactions,
		BlockSignatures:      wevent.BlockSignatures(creatorBytes),
		Parents:              []string{selfParent, otherParent},
		Creator:              creatorBytes,
		Index:                wevent.Body.Index,

		selfParentIndex:      wevent.Body.SelfParentIndex,
		otherParentCreatorID: wevent.Body.OtherParentCreatorID,
		otherParentIndex:     wevent.Body.OtherParentIndex,
		creatorID:            wevent.Body.CreatorID,
	}

	event := &Event{
		Body:      body,
		Signature: wevent.Signature,
	}

	return event, nil
}

//CheckBlock returns an error if the Block does not contain valid signatures
//from MORE than 1/3 of participants
func (h *Hashgraph) CheckBlock(block *Block, peerSet *peers.PeerSet) error {
	psh, err := peerSet.Hash()
	if err != nil {
		return err
	}

	if !reflect.DeepEqual(psh, block.PeersHash()) {
		return fmt.Errorf("Wrong PeerSet")
	}

	validSignatures := 0
	for _, s := range block.GetSignatures() {
		validatorHex := s.ValidatorHex()
		if _, ok := peerSet.ByPubKey[validatorHex]; !ok {
			h.logger.Debug("Verifying Block signature. Unknown validator",
				"validator", validatorHex,
			)
			continue
		}
		ok, _ := block.Verify(s)
		if ok {
			validSignatures++
		}
	}

	if validSignatures <= peerSet.TrustCount() {
		return fmt.Errorf("Not enough valid signatures: got %d, need %d", validSignatures, peerSet.TrustCount())
	}

	h.logger.Debug("CheckBlock", "valid_signatures", validSignatures)
	return nil
}

/*******************************************************************************
Setters
*******************************************************************************/

func (h *Hashgraph) setLastConsensusRound(i int) {
	if h.LastConsensusRound == nil {
		h.LastConsensusRound = new(int)
	}
	*h.LastConsensusRound = i

	if h.FirstConsensusRound == nil {
		h.FirstConsensusRound = new(int)
		*h.FirstConsensusRound = i
	}
}

func (h *Hashgraph) setRoundLowerBound(i int) {
	if h.roundLowerBound == nil {
		h.roundLowerBound = new(int)
	}
	*h.roundLowerBound = i
}

func (h *Hashgraph) setAnchorBlock(i int) {
	if h.AnchorBlock == nil {
		h.AnchorBlock = new(int)
	}
	*h.AnchorBlock = i
}

/*******************************************************************************
   Helpers
*******************************************************************************/

func middleBit(ehex string) bool {
	hash, err := common.DecodeFromString(ehex)
	if err != nil {
		fmt.Printf("ERROR decoding hex string: %s\n", err)
	}
	if len(hash) > 0 && hash[len(hash)/2] == 0 {
		return false
	}
	return true
}

/*******************************************************************************
InternalCommitCallback
*******************************************************************************/

/*
InternalCommitCallback is called by the Hashgraph to commit a Block. The
InternalCommitCallback will likely itself call the ProxyCommitCallback. We add
a layer of indirection because processing the CommitResponse should be handled
by the Core object, not the hashgraph; the hashgraph only known if there was
an error or not.
*/
type InternalCommitCallback func(*Block) error

//DummyInternalCommitCallback is used for testing
func DummyInternalCommitCallback(b *Block) error {
	return nil
}
