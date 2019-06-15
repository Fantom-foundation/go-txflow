package hashgraph

import (
	"strconv"

	cm "github.com/andrecronje/babble/src/common"
	"github.com/tendermint/tendermint/types"
)

type State struct {
	cacheSize            int64
	eventCache           *cm.LRU          //hash => Event
	roundCache           *cm.LRU          //round number => Round
	blockCache           *cm.LRU          //index => Block
	frameCache           *cm.LRU          //round received => Frame
	consensusCache       *cm.RollingIndex //consensus index => hash
	totConsensusEvents   int64
	validatorSetCache    *ValidatorSetCache    //start round => PeerSet
	validatorEventsCache *ValidatorEventsCache //pubkey => Events
	roots                map[string]*Root      //[participant] => Root
	lastRound            int64
	lastConsensusEvents  map[string]string //[participant] => hex() of last consensus event
	lastBlock            int64
}

func NewState(cacheSize int) *State {
	state := &State{
		cacheSize:            cacheSize,
		eventCache:           cm.NewLRU(cacheSize, nil),
		roundCache:           cm.NewLRU(cacheSize, nil),
		blockCache:           cm.NewLRU(cacheSize, nil),
		frameCache:           cm.NewLRU(cacheSize, nil),
		consensusCache:       cm.NewRollingIndex("ConsensusCache", cacheSize),
		validatorSetCache:    cm.NewLRU(cacheSize, nil),
		validatorEventsCache: cm.NewLRU(cacheSize, nil),
		roots:                make(map[string]*Root),
		lastRound:            -1,
		lastBlock:            -1,
		lastConsensusEvents:  map[string]string{},
	}
	return state
}

func (s *State) CacheSize() int {
	return s.cacheSize
}

func (s *State) GetValidatorSet(round int64) (*types.ValidatorSet, error) {
	return s.validatorSetCache.Get(round)
}

//SetPeerSet updates the peerSetCache and participantEventsCache
func (s *State) SetValidatorSet(round int, validatorSet *types.ValidatorSet) error {
	//Update ValidatorSetCache
	err := s.validatorSetCache.Set(round, validatorSet)
	if err != nil {
		return err
	}

	for _, v := range validatorSet.Validators {
		s.addValidator(v)
	}

	return nil
}

func (s *State) addValidator(v *types.Validator) error {
	if _, ok := s.validatorEventsCache.validators.ByID[v.ID()]; !ok {
		if err := s.validatorEventsCache.AddValidator(v); err != nil {
			return err
		}
	}

	if _, ok := s.roots[v.Address.String()]; !ok {
		s.roots[v.Address.String()] = NewRoot()
	}

	return nil
}

func (s *State) GetAllValidatorSets() (map[int][]*types.Validator, error) {
	return s.validatorSetCache.GetAll()
}

func (s *State) FirstRound(id string) (int64, bool) {
	return s.validatorSetCache.FirstRound(id)
}

func (s *State) RepertoireByPubKey() map[string]*types.Validator {
	return s.validatorSetCache.RepertoireByPubKey()
}

func (s *State) RepertoireByID() map[uint32]*types.Validator {
	return s.validatorSetCache.RepertoireByID()
}

func (s *State) GetEvent(key string) (*Event, error) {
	res, ok := s.eventCache.Get(key)
	if !ok {
		return nil, cm.NewStoreErr("EventCache", cm.KeyNotFound, key)
	}

	return res.(*Event), nil
}

func (s *State) SetEvent(event *Event) error {
	key := event.Hex()
	_, err := s.GetEvent(key)
	if err != nil && !cm.Is(err, cm.KeyNotFound) {
		return err
	}
	if cm.Is(err, cm.KeyNotFound) {
		if err := s.validatorEventsCache.Set(event.Creator(), key, event.Index()); err != nil {
			return err
		}
	}
	s.eventCache.Add(key, event)
	return nil
}

func (s *State) ValidatorEvents(validator string, skip int64) ([]string, error) {
	return s.validatorEventsCache.Get(validator, skip)
}

func (s *State) ValidatorEvent(validator string, index int64) (string, error) {
	return s.validatorEventsCache.GetItem(validator, index)
}

func (s *State) LastEventFrom(validator string) (last string, err error) {
	last, err = s.validatorEventsCache.GetLast(validator)
	return
}

func (s *State) LastConsensusEventFrom(validator string) (last string, err error) {
	last, _ = s.lastConsensusEvents[validator]
	return
}

func (s *State) KnownEvents() map[uint32]int {
	return s.validatorEventsCache.Known()
}

func (s *State) ConsensusEvents() []string {
	lastWindow, _ := s.consensusCache.GetLastWindow()
	res := make([]string, len(lastWindow))
	for i, item := range lastWindow {
		res[i] = item.(string)
	}
	return res
}

func (s *State) ConsensusEventsCount() int {
	return s.totConsensusEvents
}

func (s *State) AddConsensusEvent(event *Event) error {
	s.consensusCache.Set(event.Hex(), s.totConsensusEvents)
	s.totConsensusEvents++
	s.lastConsensusEvents[event.Creator()] = event.Hex()
	return nil
}

func (s *State) GetRound(r int64) (*RoundInfo, error) {
	res, ok := s.roundCache.Get(r)
	if !ok {
		return nil, cm.NewStoreErr("RoundCache", cm.KeyNotFound, strconv.Itoa(r))
	}
	return res.(*RoundInfo), nil
}

func (s *State) SetRound(r int64, round *RoundInfo) error {
	s.roundCache.Add(r, round)
	if r > s.lastRound {
		s.lastRound = r
	}
	return nil
}

func (s *State) LastRound() int64 {
	return s.lastRound
}

func (s *State) RoundWitnesses(r int64) []string {
	round, err := s.GetRound(r)
	if err != nil {
		return []string{}
	}
	return round.Witnesses()
}

func (s *State) RoundEvents(r int64) int64 {
	round, err := s.GetRound(r)
	if err != nil {
		return 0
	}
	return len(round.CreatedEvents)
}

func (s *State) GetRoot(validator string) (*Root, error) {
	res, ok := s.roots[validator]
	if !ok {
		return nil, cm.NewStoreErr("RootCache", cm.KeyNotFound, participant)
	}
	return res, nil
}

func (s *State) GetBlock(index int64) (*types.Block, error) {
	res, ok := s.blockCache.Get(index)
	if !ok {
		return nil, cm.NewStoreErr("BlockCache", cm.KeyNotFound, strconv.Itoa(index))
	}
	return res.(*Block), nil
}

func (s *State) SetBlock(block *types.Block) error {
	index := block.Index()
	_, err := s.GetBlock(index)
	if err != nil && !cm.Is(err, cm.KeyNotFound) {
		return err
	}
	s.blockCache.Add(index, block)
	if index > s.lastBlock {
		s.lastBlock = index
	}
	return nil
}

func (s *State) LastBlockIndex() int64 {
	return s.lastBlock
}

func (s *State) GetFrame(index int64) (*Frame, error) {
	res, ok := s.frameCache.Get(index)
	if !ok {
		return nil, cm.NewStoreErr("FrameCache", cm.KeyNotFound, strconv.Itoa(index))
	}
	return res.(*Frame), nil
}

func (s *State) SetFrame(frame *Frame) error {
	index := frame.Round
	_, err := s.GetFrame(index)
	if err != nil && !cm.Is(err, cm.KeyNotFound) {
		return err
	}
	s.frameCache.Add(index, frame)
	return nil
}

func (s *State) Reset(frame *Frame) error {
	//Clear all caches
	s.validatorSetCache = NewValidatorSetCache()
	s.eventCache = cm.NewLRU(s.cacheSize, nil)
	s.roundCache = cm.NewLRU(s.cacheSize, nil)
	s.blockCache = cm.NewLRU(s.cacheSize, nil)
	s.frameCache = cm.NewLRU(s.cacheSize, nil)
	s.validatorEventsCache = NewValidatorEventsCache(s.cacheSize)
	s.roots = make(map[string]*Root)
	s.lastRound = -1
	s.lastBlock = -1
	s.consensusCache = cm.NewRollingIndex("ConsensusCache", s.cacheSize)
	s.lastConsensusEvents = map[string]string{}

	//Set Roots from Frame
	s.roots = frame.Roots

	for round, vs := range frame.ValidatorSets {
		if err := s.SetValidatorSet(round, types.NewValidatorSet(vs)); err != nil {
			return err
		}
	}

	//Set Frame
	return s.SetFrame(frame)
}

func (s *State) Close() error {
	return nil
}

func (s *State) StorePath() string {
	return ""
}
