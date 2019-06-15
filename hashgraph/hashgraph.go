package hashgraph

import (
	"fmt"
	"math"
	"sort"

	"github.com/andrecronje/babble/src/common"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/types"
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
	State                   State                  //store of Events, Rounds, and Blocks
	UndeterminedEvents      []string               //[index] => hash . FIFO queue of Events whose consensus order is not yet determined
	PendingRounds           *PendingRoundsCache    //FIFO queue of Rounds which have not attained consensus yet
	LastConsensusRound      *int64                 //index of last consensus round
	FirstConsensusRound     *int64                 //index of first consensus round (only used in tests)
	AnchorBlock             *int64                 //index of last block with enough signatures
	roundLowerBound         *int64                 //rounds and events below this lower bound have a special treatement (cf fastsync)
	LastCommitedRoundEvents int64                  //number of events in round before LastConsensusRound
	ConsensusTransactions   int64                  //number of consensus transactions
	PendingLoadedEvents     int64                  //number of loaded events that are not yet committed
	commitCallback          InternalCommitCallback //commit block callback
	topologicalIndex        int64                  //counter used to order events in topological order (only local)

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
func NewHashgraph(state State, commitCallback InternalCommitCallback, logger log.Logger) *Hashgraph {
	cacheSize := state.CacheSize()
	hashgraph := Hashgraph{
		State:             state,
		PendingRounds:     NewPendingRoundsCache(),
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

//Init sets the initial ValidatorSet, which also creates the corresponding Roots and
//updates the Validators.
func (h *Hashgraph) Init(validatorSet *types.ValidatorSet) error {
	//Validators per round for hash and certificate
	if err := h.State.SetValidatorSet(0, validatorSet); err != nil {
		return fmt.Errorf("Error setting ValidatorSet: %v", err)
	}
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

	ex, err := h.State.GetEvent(x)
	if err != nil {
		return false, err
	}

	ey, err := h.State.GetEvent(y)
	if err != nil {
		return false, err
	}

	entry, ok := ex.lastAncestors[ey.Creator.Address().String()]

	res := ok && entry.index >= ey.Index

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
	ex, err := h.State.GetEvent(x)
	if err != nil {
		return false, err
	}

	ey, err := h.State.GetEvent(y)
	if err != nil {
		return false, err
	}

	return ex.Creator == ey.Creator && ex.Index >= ey.Index, nil
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

//true if x strongly sees y based on validator set
func (h *Hashgraph) stronglySee(x, y string, validators *types.ValidatorSet) (bool, error) {
	if c, ok := h.stronglySeeCache.Get(TreKey{x, y, validators.Hash()}); ok {
		return c.(bool), nil
	}
	ss, err := h._stronglySee(x, y, validators)
	if err != nil {
		return false, err
	}
	h.stronglySeeCache.Add(TreKey{x, y, validators.Hash()}, ss)
	return ss, nil
}

func (h *Hashgraph) _stronglySee(x, y string, validators *types.ValidatorSet) (bool, error) {

	ex, err := h.State.GetEvent(x)
	if err != nil {
		return false, err
	}

	ey, err := h.State.GetEvent(y)
	if err != nil {
		return false, err
	}

	var c int64

	validators.Iterate(func(index int, val *types.Validator) bool {
		xla, xlaok := ex.lastAncestors[val.Address.String()]
		yfd, yfdok := ey.firstDescendants[val.Address.String()]
		if xlaok && yfdok && xla.index >= yfd.index {
			c++
		}
		return false
	})

	return c > validators.TotalVotingPower()*2/3, nil
}

func (h *Hashgraph) round(x string) (int64, error) {
	if c, ok := h.roundCache.Get(x); ok {
		return c.(int64), nil
	}
	r, err := h._round(x)
	if err != nil {
		return -1, err
	}
	h.roundCache.Add(x, r)
	return r, nil
}

func (h *Hashgraph) _round(x string) (int64, error) {
	ex, err := h.State.GetEvent(x)
	if err != nil {
		return math.MinInt32, err
	}

	parentRound := int64(-1)

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

	//Retrieve the ParentRound's ValidatorSet and count strongly-seen witnesses based
	//on this ValidatorSet.
	parentRoundObj, err := h.State.GetRound(parentRound)
	if err != nil {
		return math.MinInt32, err
	}

	parentRoundValidatorSet, err := h.State.GetValidatorSet(parentRound)
	if err != nil {
		return math.MinInt32, err
	}

	var c int64
	for _, w := range parentRoundObj.Witnesses() {
		ss, err := h.stronglySee(x, w, parentRoundValidatorSet)
		if err != nil {
			return math.MinInt32, err
		}
		if ss {
			c++
		}
	}

	if c > parentRoundValidatorSet.TotalVotingPower()*2/3 {
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
	ex, err := h.State.GetEvent(x)
	if err != nil {
		return false, err
	}

	xRound, err := h.round(x)
	if err != nil {
		return false, err
	}

	//does the creator belong to the ValidatorSet?
	validatorSet, err := h.State.GetValidatorSet(xRound)
	if err != nil {
		return false, err
	}

	if !validatorSet.HasAddress(ex.Creator.Address()) {
		return false, nil
	}

	spRound := int64(-1)
	if ex.SelfParent() != "" {
		spRound, err = h.round(ex.SelfParent())
		if err != nil {
			return false, err
		}
	}

	return xRound > spRound, nil
}

func (h *Hashgraph) roundReceived(x string) (int64, error) {
	ex, err := h.State.GetEvent(x)
	if err != nil {
		return -1, err
	}

	res := int64(-1)
	if ex.roundReceived != nil {
		res = *ex.roundReceived
	}

	return res, nil
}

func (h *Hashgraph) lamportTimestamp(x string) (int64, error) {
	if c, ok := h.timestampCache.Get(x); ok {
		return c.(int64), nil
	}
	r, err := h._lamportTimestamp(x)
	if err != nil {
		return -1, err
	}
	h.timestampCache.Add(x, r)
	return r, nil
}

func (h *Hashgraph) _lamportTimestamp(x string) (int64, error) {
	plt := int64(-1)

	ex, err := h.State.GetEvent(x)
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
		opLT := int64(math.MinInt64)
		if _, err := h.State.GetEvent(ex.OtherParent()); err == nil {
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
func (h *Hashgraph) roundDiff(x, y string) (int64, error) {
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
	creator := event.Creator

	creatorLastKnown, err := h.State.LastEventFrom(creator.Address().String())
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
		_, err := h.State.GetEvent(otherParent)
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

	selfParent, selfParentError := h.State.GetEvent(event.SelfParent())
	otherParent, otherParentError := h.State.GetEvent(event.OtherParent())

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

	event.firstDescendants[event.Creator.Address().String()] = EventCoordinates{
		index: event.Index,
		hash:  event.Hex(),
	}

	event.lastAncestors[event.Creator.Address().String()] = EventCoordinates{
		index: event.Index,
		hash:  event.Hex(),
	}

	return nil
}

//update first descendant of each last ancestor to point to event
func (h *Hashgraph) updateAncestorFirstDescendant(event *Event) error {
	for _, c := range event.lastAncestors {
		ah := c.hash
		for {
			a, err := h.State.GetEvent(ah)
			if err != nil {
				break
			}

			_, ok := a.firstDescendants[event.Creator.Address().String()]
			if !ok {
				a.firstDescendants[event.Creator.Address().String()] = EventCoordinates{
					index: event.Index,
					hash:  event.Hex(),
				}
				if err := h.State.SetEvent(a); err != nil {
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
	ev, err := h.State.GetEvent(x)
	if err != nil {
		return nil, fmt.Errorf("FrameEvent %s not found", x)
	}

	round, err := h.round(x)
	if err != nil {
		return nil, err
	}

	roundInfo, err := h.State.GetRound(round)
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

func (h *Hashgraph) createRoot(validator string, head string) (*Root, error) {
	root := NewRoot()

	if head != "" {
		headEvent, err := h.createFrameEvent(head)
		if err != nil {
			return nil, err
		}

		reverseRootEvents := []*FrameEvent{headEvent}

		index := headEvent.Core.Index
		for i := 0; i < ROOT_DEPTH; i++ {
			index = index - 1
			if index >= 0 {
				peh, err := h.State.ValidatorEvent(validator, index)
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
			"creator", event.Creator,
			"self_parent", event.SelfParent(),
			"err", err,
		)
		return err
	} else {
		if warn != nil {
			h.logger.Info("CheckSelfParent",
				"event", event.Hex(),
				"creator", event.Creator,
				"self_parent", event.SelfParent(),
				"err", err,
			)
			return warn
		}
	}

	if err := h.checkOtherParent(event); err != nil {
		h.logger.Error("CheckOtherParent",
			"event", event.Hex(),
			"creator", event.Creator,
			"other_parent", event.OtherParent(),
			"err", err,
		)
		return err
	}

	event.topologicalIndex = h.topologicalIndex
	h.topologicalIndex++

	if err := h.initEventCoordinates(event); err != nil {
		return fmt.Errorf("InitEventCoordinates: %s", err)
	}

	if err := h.State.SetEvent(event); err != nil {
		return fmt.Errorf("SetEvent: %s", err)
	}

	if err := h.updateAncestorFirstDescendant(event); err != nil {
		return fmt.Errorf("UpdateAncestorFirstDescendant: %s", err)
	}

	h.UndeterminedEvents = append(h.UndeterminedEvents, event.Hex())

	h.PendingLoadedEvents++

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
	roundInfo, err := h.State.GetRound(frameEvent.Round)
	if err != nil {
		if !common.Is(err, common.KeyNotFound) {
			return err
		}
		roundInfo = NewRoundInfo()
	}
	roundInfo.AddCreatedEvent(event.Hex(), frameEvent.Witness)

	err = h.State.SetRound(frameEvent.Round, roundInfo)
	if err != nil {
		return err
	}

	//Init EventCoordinates.
	if err := h.initEventCoordinates(event); err != nil {
		return fmt.Errorf("InitEventCoordinates: %s", err)
	}

	if err := h.State.SetEvent(event); err != nil {
		return fmt.Errorf("SetEvent: %s", err)
	}

	if err := h.updateAncestorFirstDescendant(event); err != nil {
		return fmt.Errorf("UpdateAncestorFirstDescendant: %s", err)
	}

	//All FrameEvents are consensus events, ie. they have a round-received and
	//were committed. We need to record FrameEvents as consensus events because
	//it comes into play in GetFrame/CreateRoot
	if err := h.State.AddConsensusEvent(event); err != nil {
		return fmt.Errorf("AddConsensusEvent: %v", event)
	}

	return nil
}

//DivideRounds assigns a Round and LamportTimestamp to Events, and flags them as
//witnesses if necessary. Pushes Rounds in the PendingRounds queue if necessary.
func (h *Hashgraph) DivideRounds() error {
	for _, hash := range h.UndeterminedEvents {
		ev, err := h.State.GetEvent(hash)
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

			roundInfo, err := h.State.GetRound(roundNumber)
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

			err = h.State.SetRound(roundNumber, roundInfo)
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
			h.State.SetEvent(ev)
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

	decidedRounds := []int64{}

	for _, r := range h.PendingRounds.GetOrderedPendingRounds() {
		roundIndex := r.Index

		rRoundInfo, err := h.State.GetRound(roundIndex)
		if err != nil {
			return err
		}

		rValidatorSet, err := h.State.GetValidatorSet(roundIndex)
		if err != nil {
			return err
		}

		for _, x := range rRoundInfo.Witnesses() {
			if rRoundInfo.IsDecided(x) {
				continue
			}
		VOTE_LOOP:
			for j := roundIndex + 1; j <= h.State.LastRound(); j++ {
				jRoundInfo, err := h.State.GetRound(j)
				if err != nil {
					return err
				}

				jValidatorSet, err := h.State.GetValidatorSet(j)
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
						jPrevRoundInfo, err := h.State.GetRound(j - 1)
						if err != nil {
							return err
						}

						jPrevValidatorSet, err := h.State.GetValidatorSet(j - 1)
						if err != nil {
							return err
						}

						//collection of witnesses from round j-1 that are
						//strongly seen by y, based on round j-1 PeerSet.
						ssWitnesses := []string{}
						for _, w := range jPrevRoundInfo.Witnesses() {
							ss, err := h.stronglySee(y, w, jPrevValidatorSet)
							if err != nil {
								return err
							}
							if ss {
								ssWitnesses = append(ssWitnesses, w)
							}
						}

						//Collect votes from these witnesses.
						var yays int64
						var nays int64
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
							if t > jValidatorSet.TotalVotingPower()*2/3 {
								rRoundInfo.SetFame(x, v)
								setVote(votes, y, x, v)
								break VOTE_LOOP //break out of j loop
							} else {
								setVote(votes, y, x, v)
							}
						} else { //coin round
							if t > jValidatorSet.TotalVotingPower()*2/3 {
								setVote(votes, y, x, v)
							} else {
								setVote(votes, y, x, middleBit(y)) //middle bit of y's hash
							}
						}
					}
				}
			}
		}

		if rRoundInfo.WitnessesDecided(rValidatorSet) {
			decidedRounds = append(decidedRounds, roundIndex)
		}

		err = h.State.SetRound(roundIndex, rRoundInfo)
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

		for i := r + 1; i <= h.State.LastRound(); i++ {
			tr, err := h.State.GetRound(i)
			if err != nil {
				return err
			}

			tValidators, err := h.State.GetValidatorSet(i)
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
			if !(tr.WitnessesDecided(tValidators)) {
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

			if len(s) == len(fws) && int64(len(s)) > tValidators.TotalVotingPower()*2/3 {
				received = true

				ex, err := h.State.GetEvent(x)
				if err != nil {
					return err
				}

				ex.SetRoundReceived(i)

				err = h.State.SetEvent(ex)
				if err != nil {
					return err
				}

				tr.AddReceivedEvent(x)
				err = h.State.SetRound(i, tr)
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
	processedRounds := []int64{}
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

		round, err := h.State.GetRound(r.Index)
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
			"validators", len(frame.Validators),
			"validator_sets", len(frame.ValidatorSets),
			"roots", len(frame.Roots),
		)

		if len(frame.Events) > 0 {
			for _, e := range frame.Events {
				err := h.State.AddConsensusEvent(e.Core)
				if err != nil {
					return err
				}

				h.ConsensusTransactions += int64(len(e.Core.Transactions))

				h.PendingLoadedEvents--
			}

			lastBlockIndex := h.State.LastBlockIndex()
			block, err := NewBlockFromFrame(lastBlockIndex+1, frame)
			if err != nil {
				return err
			}

			if len(block.Transactions()) > 0 ||
				len(block.InternalTransactions()) > 0 {

				if err := h.State.SetBlock(block); err != nil {
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
func (h *Hashgraph) GetFrame(roundReceived int64) (*Frame, error) {
	//Try to get it from the Store first
	frame, err := h.State.GetFrame(roundReceived)
	if err == nil || !common.Is(err, common.KeyNotFound) {
		return frame, err
	}

	//Get the Round and corresponding consensus Events
	round, err := h.State.GetRound(roundReceived)
	if err != nil {
		return nil, err
	}

	validatorSet, err := h.State.GetValidatorSet(roundReceived)
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
		p := ev.Core.Creator
		r, ok := roots[p.Address().String()]
		if !ok {
			r, err = h.createRoot(p.Address().String(), ev.Core.SelfParent())
			if err != nil {
				return nil, err
			}
			roots[p.Address().String()] = r
		}
	}

	/*
		Every participant, that was known before roundReceived, needs a Root in
		the Frame. For the participants that have no Events in this Frame, we
		create a Root from their last consensus Event, or their last known Root
	*/
	for p, validator := range h.State.RepertoireByPubKey() {
		//Ignore if participant wasn't added before roundReceived
		firstRound, ok := h.State.FirstRound(validator.Address.String())
		if !ok || firstRound > roundReceived {
			continue
		}

		if _, ok := roots[p]; !ok {
			var root *Root

			lastConsensusEventHash, err := h.State.LastConsensusEventFrom(p)
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

	//Get all ValidatorSets
	allValidatorSets, err := h.State.GetAllValidatorSets()
	if err != nil {
		return nil, err
	}

	res := &Frame{
		Round:         roundReceived,
		Validators:    validatorSet.Validators,
		Roots:         roots,
		Events:        events,
		ValidatorSets: allValidatorSets,
	}

	if err := h.State.SetFrame(res); err != nil {
		return nil, err
	}

	return res, nil
}

//Reset clears the Hashgraph and resets it from a new base.
func (h *Hashgraph) Reset(block *types.Block, frame *Frame) error {
	//Clear all state
	h.LastConsensusRound = nil
	h.FirstConsensusRound = nil
	h.AnchorBlock = nil

	h.UndeterminedEvents = []string{}
	h.PendingRounds = NewPendingRoundsCache()
	h.PendingLoadedEvents = 0
	h.topologicalIndex = 0

	cacheSize := h.State.CacheSize()
	h.ancestorCache = common.NewLRU(cacheSize, nil)
	h.selfAncestorCache = common.NewLRU(cacheSize, nil)
	h.stronglySeeCache = common.NewLRU(cacheSize, nil)
	h.roundCache = common.NewLRU(cacheSize, nil)
	h.witnessCache = common.NewLRU(cacheSize, nil)

	//Initialize new Roots
	if err := h.State.Reset(frame); err != nil {
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
	if err := h.State.SetBlock(block); err != nil {
		return err
	}
	h.setLastConsensusRound(block.Height)
	h.setRoundLowerBound(block.Height)

	return nil
}

/*******************************************************************************
Setters
*******************************************************************************/

func (h *Hashgraph) setLastConsensusRound(i int64) {
	if h.LastConsensusRound == nil {
		h.LastConsensusRound = new(int64)
	}
	*h.LastConsensusRound = i

	if h.FirstConsensusRound == nil {
		h.FirstConsensusRound = new(int64)
		*h.FirstConsensusRound = i
	}
}

func (h *Hashgraph) setRoundLowerBound(i int64) {
	if h.roundLowerBound == nil {
		h.roundLowerBound = new(int64)
	}
	*h.roundLowerBound = i
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
type InternalCommitCallback func(*types.Block) error

//DummyInternalCommitCallback is used for testing
func DummyInternalCommitCallback(b *types.Block) error {
	return nil
}
