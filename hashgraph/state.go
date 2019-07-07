package hashgraph

import (
	"github.com/Fantom-foundation/go-txflow/types"
	ttypes "github.com/tendermint/tendermint/types"
)

type State struct {
	Events              map[string]*types.Event        //hash => Event
	Rounds              map[int64]*types.Round         //round number => Round
	Blocks              map[int64]*ttypes.Block        //index => Block
	Frames              map[int64]*types.Frame         //round received => Frame
	ValidatorSets       map[int64]*ttypes.ValidatorSet //start round => ValidatorSet
	ValidatorEvents     map[string][]*types.Event      //pubkey => Events
	ValidatorHeadEvents map[string]int
	Roots               map[string]*types.Root //[validator] => Root
	ConsensusEvents     map[string]int64

	totConsensusEvents  int64
	lastRound           int64
	lastConsensusEvents map[string]string //[validator] => hex() of last consensus event
	lastBlock           int64
}

func NewState() *State {
	state := &State{
		Events:          make(map[string]*types.Event),
		Rounds:          make(map[int64]*types.Round),
		Blocks:          make(map[int64]*ttypes.Block),
		Frames:          make(map[int64]*types.Frame),
		ValidatorSets:   make(map[int64]*ttypes.ValidatorSet),
		ValidatorEvents: make(map[string][]*types.Event),
		Roots:           make(map[string]*types.Root),

		lastRound:           -1,
		lastBlock:           -1,
		lastConsensusEvents: map[string]string{},
	}
	return state
}

//SetValidatorSet updates the validatorSets and validatorEvents
func (s *State) SetValidatorSet(round int64, validatorSet *ttypes.ValidatorSet) error {
	//Update ValidatorSets
	s.ValidatorSets[round] = validatorSet
	for _, v := range validatorSet.Validators {
		s.addValidator(v)
	}

	return nil
}

func (s *State) addValidator(v *ttypes.Validator) error {
	if _, ok := s.ValidatorEvents[v.Address.String()]; !ok {
		s.ValidatorEvents[v.Address.String()] = []*types.Event{}
	}

	if _, ok := s.Roots[v.Address.String()]; !ok {
		s.Roots[v.Address.String()] = types.NewRoot()
	}

	return nil
}

//SetEvent
func (s *State) SetEvent(event *types.Event) error {
	s.ValidatorEvents[event.Creator.Address().String()] = append(s.ValidatorEvents[event.Creator.Address().String()], event)
	s.Events[event.Hex()] = event
	return nil
}

//AddConsensusEvent
func (s *State) AddConsensusEvent(event *types.Event) error {
	s.ConsensusEvents[event.Hash().String()] = s.totConsensusEvents
	s.totConsensusEvents++
	s.lastConsensusEvents[event.Creator.Address().String()] = event.Hash().String()
	return nil
}

func (s *State) LastRound() int64 {
	return s.lastRound
}

func (s *State) LastBlockIndex() int64 {
	return s.lastBlock
}

func (s *State) SetBlock(block *ttypes.Block) error {
	index := block.Header.Height
	s.Blocks[index] = block
	if index > s.lastBlock {
		s.lastBlock = index
	}
	return nil
}

func (s *State) LastConsensusEventFrom(validator string) (last string, err error) {
	last, _ = s.lastConsensusEvents[validator]
	return
}
