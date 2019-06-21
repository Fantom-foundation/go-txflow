package hashgraph

import (
	"fmt"
	"math"
	"sort"

	"github.com/andrecronje/babble-abci/types"
	"github.com/andrecronje/babble/src/common"
	"github.com/tendermint/tendermint/libs/log"
	ttypes "github.com/tendermint/tendermint/types"
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
	State                   *State                 //store of Events, Rounds, and Blocks
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

	ancestors     map[Key]bool
	selfAncestors map[Key]bool
	stronglySees  map[TreKey]bool
	rounds        map[string]int64
	timestamps    map[string]int64
	witnesses     map[string]bool

	logger log.Logger
}

//NewHashgraph instantiates a Hashgraph with an underlying data store and a
//commit callback
func NewHashgraph(state *State, commitCallback InternalCommitCallback, logger log.Logger) *Hashgraph {
	hashgraph := Hashgraph{
		State:          state,
		PendingRounds:  NewPendingRoundsCache(),
		commitCallback: commitCallback,
		ancestors:      make(map[Key]bool),
		selfAncestors:  make(map[Key]bool),
		stronglySees:   make(map[TreKey]bool),
		rounds:         make(map[string]int64),
		timestamps:     make(map[string]int64),
		witnesses:      make(map[string]bool),
		logger:         logger,
	}

	return &hashgraph
}

func (h *Hashgraph) KnownEvents() map[string]int {
	return h.State.ValidatorHeadEvents
}

//Init sets the initial ValidatorSet, which also creates the corresponding Roots and
//updates the Validators.
func (h *Hashgraph) Init(validatorSet *ttypes.ValidatorSet) error {
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
	if c, ok := h.ancestors[Key{x, y}]; ok {
		return c, nil
	}
	a, err := h._ancestor(x, y)
	if err != nil {
		return false, err
	}
	h.ancestors[Key{x, y}] = a
	return a, nil
}

func (h *Hashgraph) _ancestor(x, y string) (bool, error) {
	if x == y {
		return true, nil
	}

	ex, ok := h.State.Events[x]
	if !ok {
		return false, nil
	}

	ey, ok := h.State.Events[y]
	if !ok {
		return false, nil
	}

	//Double check this logic, x's last Ancestors creator of Y
	entry, ok := ex.LastAncestors[ey.Creator.Address().String()]

	res := ok && entry.Height >= ey.Height

	return res, nil
}

//true if y is a self-ancestor of x
func (h *Hashgraph) selfAncestor(x, y string) (bool, error) {
	if c, ok := h.selfAncestors[Key{x, y}]; ok {
		return c, nil
	}
	a, err := h._selfAncestor(x, y)
	if err != nil {
		return false, err
	}
	h.selfAncestors[Key{x, y}] = a
	return a, nil
}

func (h *Hashgraph) _selfAncestor(x, y string) (bool, error) {
	if x == y {
		return true, nil
	}
	ex, ok := h.State.Events[x]
	if !ok {
		return false, nil
	}

	ey, ok := h.State.Events[y]
	if !ok {
		return false, nil
	}

	return ex.Creator == ey.Creator && ex.Height >= ey.Height, nil
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
func (h *Hashgraph) stronglySee(x, y string, validators *ttypes.ValidatorSet) (bool, error) {
	if c, ok := h.stronglySees[TreKey{x, y, string(validators.Hash())}]; ok {
		return c, nil
	}
	ss, err := h._stronglySee(x, y, validators)
	if err != nil {
		return false, err
	}
	h.stronglySees[TreKey{x, y, string(validators.Hash())}] = ss
	return ss, nil
}

func (h *Hashgraph) _stronglySee(x, y string, validators *ttypes.ValidatorSet) (bool, error) {

	ex, ok := h.State.Events[x]
	if !ok {
		return false, nil
	}

	ey, ok := h.State.Events[y]
	if !ok {
		return false, nil
	}

	c := int64(0)

	validators.Iterate(func(index int, val *ttypes.Validator) bool {
		xla, xlaok := ex.LastAncestors[val.Address.String()]
		yfd, yfdok := ey.FirstDescendants[val.Address.String()]
		if xlaok && yfdok && xla.Height >= yfd.Height {
			// Need to accumulate voting power here
			c++
		}
		return false
	})

	return c > validators.TotalVotingPower()*2/3, nil
}

func (h *Hashgraph) round(x string) (int64, error) {
	if c, ok := h.rounds[x]; ok {
		return c, nil
	}
	r, err := h._round(x)
	if err != nil {
		return -1, err
	}
	h.rounds[x] = r
	return r, nil
}

func (h *Hashgraph) _round(x string) (int64, error) {
	ex, ok := h.State.Events[x]
	if !ok {
		return math.MinInt32, nil
	}

	parentRound := int64(-1)
	var err error

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
	parentRoundObj, ok := h.State.Rounds[parentRound]
	if !ok {
		return math.MinInt64, nil
	}

	parentRoundValidatorSet, ok := h.State.ValidatorSets[parentRound]
	if !ok {
		return math.MinInt64, nil
	}

	c := int64(0)
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
	if c, ok := h.witnesses[x]; ok {
		return c, nil
	}
	r, err := h._witness(x)
	if err != nil {
		return false, err
	}
	h.witnesses[x] = r
	return r, nil
}

//true if x is a witness (first event of a round for the owner)
func (h *Hashgraph) _witness(x string) (bool, error) {
	ex, ok := h.State.Events[x]
	if !ok {
		return false, nil
	}

	xRound, err := h.round(x)
	if err != nil {
		return false, err
	}

	//does the creator belong to the ValidatorSet?
	validatorSet, ok := h.State.ValidatorSets[xRound]
	if !ok {
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
	ex, ok := h.State.Events[x]
	if !ok {
		return -1, nil
	}

	res := int64(-1)
	if ex.RoundReceived != -1 {
		res = ex.RoundReceived
	}

	return res, nil
}

func (h *Hashgraph) lamportTimestamp(x string) (int64, error) {
	if c, ok := h.timestamps[x]; ok {
		return c, nil
	}
	r, err := h._lamportTimestamp(x)
	if err != nil {
		return -1, err
	}
	h.timestamps[x] = r
	return r, nil
}

func (h *Hashgraph) _lamportTimestamp(x string) (int64, error) {
	plt := int64(-1)
	var err error

	ex, ok := h.State.Events[x]
	if !ok {
		return math.MinInt64, nil
	}

	if ex.SelfParent() != "" {
		plt, err = h.lamportTimestamp(ex.SelfParent())
		if err != nil {
			return math.MinInt32, err
		}
	}

	if ex.OtherParent() != "" {
		opLT := int64(math.MinInt64)
		if _, ok := h.State.Events[ex.OtherParent()]; !ok {
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
func (h *Hashgraph) checkSelfParent(event *types.Event) (err, warn error) {
	selfParent := event.SelfParent()
	creator := event.Creator

	creatorLastKnowns, ok := h.State.ValidatorEvents[creator.Address().String()]
	if !ok {
		//First Event
		if common.Is(err, common.Empty) && selfParent == "" {
			return nil, nil
		}
		return err, nil
	}
	creatorLastKnown := creatorLastKnowns[len(h.State.ValidatorEvents[creator.Address().String()])-1]

	selfParentLegit := selfParent == creatorLastKnown.Hash().String()

	//If you find this line using grep, the appearance of this event in the logs
	//is to be expected in normal operation and may not be a cause for concern.
	if !selfParentLegit {
		return nil, fmt.Errorf("Self-parent not last known event by creator")
	}

	return nil, nil
}

//Check if we know the OtherParent
func (h *Hashgraph) checkOtherParent(event *types.Event) error {
	otherParent := event.OtherParent()
	if otherParent != "" {
		//Check if we have it
		_, ok := h.State.Events[otherParent]
		if !ok {
			return fmt.Errorf("Other-parent not known")
		}
	}
	return nil
}

//initialize arrays of last ancestors and first descendants
func (h *Hashgraph) initEventCoordinates(event *types.Event) error {
	event.LastAncestors = types.NewCoordinatesMap()
	event.FirstDescendants = types.NewCoordinatesMap()

	selfParent, selfParentOk := h.State.Events[event.SelfParent()]
	otherParent, otherParentOk := h.State.Events[event.OtherParent()]

	if !selfParentOk && otherParentOk {
		event.LastAncestors = otherParent.LastAncestors.Copy()
	} else if !otherParentOk && selfParentOk {
		event.LastAncestors = selfParent.LastAncestors.Copy()
	} else if otherParentOk && selfParentOk {
		selfParentLastAncestors := selfParent.LastAncestors
		otherParentLastAncestors := otherParent.LastAncestors

		event.LastAncestors = selfParentLastAncestors.Copy()
		for p, ola := range otherParentLastAncestors {
			sla, ok := event.LastAncestors[p]
			if !ok || sla.Height < ola.Height {
				event.LastAncestors[p] = types.EventCoordinates{
					Height: ola.Height,
					Hex:    ola.Hex,
				}
			}
		}
	}

	event.FirstDescendants[event.Creator.Address().String()] = types.EventCoordinates{
		Height: event.Height,
		Hex:    event.Hex(),
	}

	event.LastAncestors[event.Creator.Address().String()] = types.EventCoordinates{
		Height: event.Height,
		Hex:    event.Hex(),
	}

	return nil
}

//update first descendant of each last ancestor to point to event
func (h *Hashgraph) updateAncestorFirstDescendant(event *types.Event) error {
	for _, c := range event.LastAncestors {
		ah := c.Hex
		for {
			a, ok := h.State.Events[ah]
			if !ok {
				break
			}

			_, ok = a.FirstDescendants[event.Creator.Address().String()]
			if !ok {
				a.FirstDescendants[event.Creator.Address().String()] = types.EventCoordinates{
					Height: event.Height,
					Hex:    event.Hex(),
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

func (h *Hashgraph) createFrameEvent(x string) (*types.FrameEvent, error) {
	ev, ok := h.State.Events[x]
	if !ok {
		return nil, fmt.Errorf("FrameEvent %s not found", x)
	}

	r, err := h.round(x)
	if err != nil {
		return nil, err
	}

	round, ok := h.State.Rounds[r]
	if !ok {
		return nil, nil
	}

	te, ok := round.CreatedEvents[x]
	if !ok {
		return nil, err
	}

	witness := te.Witness

	lt, err := h.lamportTimestamp(x)
	if err != nil {
		return nil, err
	}

	frameEvent := &types.FrameEvent{
		Core:             ev,
		Round:            r,
		LamportTimestamp: lt,
		Witness:          witness,
	}

	return frameEvent, nil
}

func (h *Hashgraph) createRoot(validator string, head string) (*types.Root, error) {
	root := types.NewRoot()

	if head != "" {
		headEvent, err := h.createFrameEvent(head)
		if err != nil {
			return nil, err
		}

		reverseRootEvents := []*types.FrameEvent{headEvent}

		height := headEvent.Core.Height
		for i := 0; i < ROOT_DEPTH; i++ {
			height = height - 1
			if height >= 0 {
				_, ok := h.State.ValidatorEvents[validator]
				if !ok {
					break
				}
				peh := h.State.ValidatorEvents[validator][height]
				rev, err := h.createFrameEvent(peh.Hash().String())
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
func (h *Hashgraph) InsertEventAndRunConsensus(event *types.Event) error {
	if err := h.InsertEvent(event); err != nil {
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
func (h *Hashgraph) InsertEvent(event *types.Event) error {
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

	event.TopologicalIndex = h.topologicalIndex
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
func (h *Hashgraph) InsertFrameEvent(frameEvent *types.FrameEvent) error {
	event := frameEvent.Core

	//Set caches so round, witness, and timestamp won't be recalculated
	h.rounds[event.Hex()] = frameEvent.Round
	h.witnesses[event.Hex()] = frameEvent.Witness
	h.timestamps[event.Hex()] = frameEvent.LamportTimestamp

	//Set the event's private fields for later use
	event.Round = frameEvent.Round
	event.LamportTimestamp = frameEvent.LamportTimestamp

	//Create/update RoundInfo object in store
	round, ok := h.State.Rounds[frameEvent.Round]
	if !ok {
		round = types.NewRound()
	}
	round.AddCreatedEvent(event.Hex(), frameEvent.Witness)

	h.State.Rounds[frameEvent.Round] = round

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
		ev, ok := h.State.Events[hash]
		if !ok {
			return nil
		}

		updateEvent := false

		//Compute Event's round, update the corresponding Round object, and add
		//it to the PendingRounds queue if necessary.
		if ev.Round == -1 {
			roundNumber, err := h.round(hash)
			if err != nil {
				return err
			}

			ev.Round = roundNumber
			updateEvent = true

			round, ok := h.State.Rounds[roundNumber]
			if !ok {
				round = types.NewRound()
			}

			if !h.PendingRounds.Queued(roundNumber) &&
				!round.Decided &&
				(h.roundLowerBound == nil || roundNumber > *h.roundLowerBound) {

				h.PendingRounds.Set(&PendingRound{roundNumber, false})
			}

			witness, err := h.witness(hash)
			if err != nil {
				return err
			}

			round.AddCreatedEvent(hash, witness)

			h.State.Rounds[roundNumber] = round
		}

		//Compute the Event's LamportTimestamp
		if ev.LamportTimestamp == -1 {
			lamportTimestamp, err := h.lamportTimestamp(hash)
			if err != nil {
				return err
			}
			ev.LamportTimestamp = lamportTimestamp
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

		rRoundInfo, ok := h.State.Rounds[roundIndex]
		if !ok {
			return nil
		}

		rValidatorSet, ok := h.State.ValidatorSets[roundIndex]
		if !ok {
			return nil
		}

		for _, x := range rRoundInfo.Witnesses() {
			if rRoundInfo.IsDecided(x) {
				continue
			}
		VOTE_LOOP:
			for j := roundIndex + 1; j <= h.State.LastRound(); j++ {
				jRoundInfo, ok := h.State.Rounds[j]
				if !ok {
					return nil
				}

				jValidatorSet, ok := h.State.ValidatorSets[j]
				if !ok {
					return nil
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
						jPrevRoundInfo, ok := h.State.Rounds[j-1]
						if !ok {
							return nil
						}

						jPrevValidatorSet, ok := h.State.ValidatorSets[j-1]
						if !ok {
							return nil
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

		h.State.Rounds[roundIndex] = rRoundInfo
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
			tr, ok := h.State.Rounds[i]
			if !ok {
				return nil
			}

			tValidators, ok := h.State.ValidatorSets[i]
			if !ok {
				return nil
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

				ex, ok := h.State.Events[x]
				if !ok {
					return nil
				}

				ex.RoundReceived = i

				err = h.State.SetEvent(ex)
				if err != nil {
					return err
				}

				tr.AddReceivedEvent(x)
				h.State.Rounds[i] = tr

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

		round, ok := h.State.Rounds[r.Index]
		if !ok {
			return nil
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
			//Create a new block here
			block := NewBlockFromFrame(lastBlockIndex+1, frame)
			if err != nil {
				return err
			}

			if len(block.Data.Txs) > 0 {

				if err := h.State.SetBlock(block); err != nil {
					return err
				}

				err := h.commitCallback(block)
				if err != nil {
					h.logger.Info("Failed to commit block", block.Header.Height)
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
func (h *Hashgraph) GetFrame(roundReceived int64) (*types.Frame, error) {
	//Try to get it from the Store first
	frame, ok := h.State.Frames[roundReceived]
	if ok {
		return frame, nil
	}

	//Get the Round and corresponding consensus Events
	round, ok := h.State.Rounds[roundReceived]
	if !ok {
		return nil, nil
	}

	validatorSet, ok := h.State.ValidatorSets[roundReceived]
	if !ok {
		return nil, nil
	}

	events := []*types.FrameEvent{}
	for _, eh := range round.ReceivedEvents {
		re, err := h.createFrameEvent(eh)
		if err != nil {
			return nil, err
		}
		events = append(events, re)
	}

	sort.Sort(types.SortedFrameEvents(events))

	/*
		Get/Create Roots. The events are in topological order; so each time we
		run into the first Event of a participant, we create a Root for it. Then
		we populate the root's Events slice.
	*/
	roots := make(map[string]*types.Root)

	for _, ev := range events {
		p := ev.Core.Creator
		var err error
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

	// Grab the round and add a root for all the validators
	for _, validator := range h.State.ValidatorSets[roundReceived].Validators {
		//Ignore if participant wasn't added before roundReceived

		if _, ok := roots[validator.Address.String()]; !ok {
			var root *types.Root

			lastConsensusEventHash, err := h.State.LastConsensusEventFrom(validator.Address.String())
			if err != nil {
				return nil, err
			}

			root, err = h.createRoot(validator.Address.String(), lastConsensusEventHash)
			if err != nil {
				return nil, err
			}

			roots[validator.Address.String()] = root
		}
	}

	res := &types.Frame{
		Round:         roundReceived,
		Validators:    validatorSet.Validators,
		Roots:         roots,
		Events:        events,
		ValidatorSets: h.State.ValidatorSets,
	}

	h.State.Frames[frame.Round] = frame

	return res, nil
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
type InternalCommitCallback func(*ttypes.Block) error

//DummyInternalCommitCallback is used for testing
func DummyInternalCommitCallback(b *ttypes.Block) error {
	return nil
}

func NewBlockFromFrame(blockIndex int64, frame *types.Frame) *ttypes.Block {
	transactions := ttypes.Txs{}
	for _, e := range frame.Events {
		transactions = append(transactions, e.Core.Transactions...)
	}

	return ttypes.MakeBlock(blockIndex, transactions, nil, nil)
}
