package hashgraph

import (
	"github.com/tendermint/tendermint/types"
)

type State struct {
	Events          map[string]*Event             //hash => Event
	Rounds          map[int64]*Round              //round number => Round
	Blocks          map[int64]*types.Block        //index => Block
	Frames          map[int64]*Frame              //round received => Frame
	ValidatorSets   map[int64]*types.ValidatorSet //start round => ValidatorSet
	ValidatorEvents map[string][]*Event           //pubkey => Events
	Roots           map[string]*Root              //[validator] => Root
	ConsensusEvents map[string]int64

	totConsensusEvents  int64
	lastRound           int64
	lastConsensusEvents map[string]string //[validator] => hex() of last consensus event
	lastBlock           int64
}

func NewState() *State {
	state := &State{
		Events:          make(map[string]*Event),
		Rounds:          make(map[int64]*Round),
		Blocks:          make(map[int64]*types.Block),
		Frames:          make(map[int64]*Frame),
		ValidatorSets:   make(map[int64]*types.ValidatorSet),
		ValidatorEvents: make(map[string][]*Event),
		Roots:           make(map[string]*Root),

		lastRound:           -1,
		lastBlock:           -1,
		lastConsensusEvents: map[string]string{},
	}
	return state
}

//SetValidatorSet updates the validatorSets and validatorEvents
func (s *State) SetValidatorSet(round int64, validatorSet *types.ValidatorSet) error {
	//Update ValidatorSets
	s.ValidatorSets[round] = validatorSet
	for _, v := range validatorSet.Validators {
		s.addValidator(v)
	}

	return nil
}

func (s *State) addValidator(v *types.Validator) error {
	if !s.ValidatorEvents[v.Address.String()] {
		s.ValidatorEvents[v.Address.String()] = []*Event{}
	}

	if _, ok := s.Roots[v.Address.String()]; !ok {
		s.Roots[v.Address.String()] = NewRoot()
	}

	return nil
}

func (s *State) SetEvent(event *Event) error {
	key := event.Hash().String()
	_, err := s.Events[key]
	if err != nil && !cm.Is(err, cm.KeyNotFound) {
		return err
	}
	if cm.Is(err, cm.KeyNotFound) {
		s.ValidatorEvents[event.Creator.Address().String()] = append(s.ValidatorEvents[event.Creator.Address().String()], event)
	}
	s.eventCache.Add(key, event)
	return nil
}

func (s *State) AddConsensusEvent(event *Event) error {
	s.ConsensusEvents[event.Hash().String()] = s.totConsensusEvents
	s.totConsensusEvents++
	s.lastConsensusEvents[event.Creator.Address().String()] = event.Hash().String()
	return nil
}
