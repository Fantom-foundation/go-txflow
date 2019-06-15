package hashgraph

import (
	"fmt"
	"math"
	"sort"
	"strconv"

	cm "github.com/andrecronje/babble/src/common"
	"github.com/tendermint/tendermint/types"
)

type Key struct {
	x, y string
}

func (k Key) ToString() string {
	return fmt.Sprintf("{%s, %s}", k.x, k.y)
}

type TreKey struct {
	x, y string
	z    []byte
}

func (k TreKey) ToString() string {
	return fmt.Sprintf("{%s, %s, %s}", k.x, k.y, k.z)
}

//------------------------------------------------------------------------------

type ValidatorEventsCache struct {
	rim *cm.RollingIndexMap
}

func NewValidatorEventsCache(size int64) *ValidatorEventsCache {
	return &ValidatorEventsCache{
		rim: cm.NewRollingIndexMap("ValidatorEvents", int(size)),
	}
}

//Get returns validator events with index > skip
func (pec *ValidatorEventsCache) Get(validator string, skipIndex int) ([]string, error) {
	ve, err := pec.rim.Get(validator, skipIndex)
	if err != nil {
		return []string{}, err
	}

	res := make([]string, len(ve))
	for k := 0; k < len(ve); k++ {
		res[k] = ve[k].(string)
	}
	return res, nil
}

func (vec *ValidatorEventsCache) GetItem(validator string, index int) (string, error) {
	item, err := vec.rim.GetItem(validator, index)
	if err != nil {
		return "", err
	}
	return item.(string), nil
}

func (vec *ValidatorEventsCache) GetLast(validator string) (string, error) {
	last, err := pec.rim.GetLast(validator)
	if err != nil {
		return "", err
	}

	return last.(string), nil
}

func (vec *ValidatorEventsCache) Set(validator string, hash string, index int) error {
	return vec.rim.Set(validator, hash, index)
}

//returns [participant id] => lastKnownIndex
func (vec *ValidatorEventsCache) Known() map[string]int64 {
	return vec.rim.Known()
}

//------------------------------------------------------------------------------

type ValidatorSetCache struct {
	rounds              sort.IntSlice
	peerSets            map[int]*types.ValidatorSet
	validatorsByAddress map[string]*types.Validator
	firstRounds         map[string]int
}

func NewValidatorSetCache() *ValidatorSetCache {
	return &ValidatorSetCache{
		rounds:              sort.IntSlice{},
		validatorSets:       make(map[int]*types.ValidatorSet),
		validatorsByAddress: make(map[string]*types.Validator),
		firstRounds:         make(map[string]int),
	}
}

func (c *ValidatorSetCache) Set(round int64, validatorSet *types.ValidatorSet) error {
	if _, ok := c.validatorSet[round]; ok {
		return cm.NewStoreErr("ValidatorSetCache", cm.KeyAlreadyExists, strconv.Itoa(round))
	}

	c.validatorSets[round] = validator

	c.rounds = append(c.rounds, round)
	c.rounds.Sort()

	for _, v := range validatorSet.Validators {
		c.validatorByAddress[v.Address().String()] = v
		fr, ok := c.firstRounds[v.Address().String()]
		if !ok || fr > round {
			c.firstRounds[v.Address().String()] = round
		}
	}

	return nil

}

func (c *ValidatorSetCache) Get(round int64) (*types.ValidatorSet, error) {
	//check if directly in ValidatorSet
	vs, ok := c.validatorSet[round]
	if ok {
		return vs, nil
	}

	//situate round in sorted rounds
	if len(c.rounds) == 0 {
		return nil, cm.NewStoreErr("ValidatorSetCache", cm.KeyNotFound, strconv.Itoa(round))
	}

	if round < c.rounds[0] {
		return c.validatorSet[c.rounds[0]], nil
	}

	for i := 0; i < len(c.rounds)-1; i++ {
		if round >= c.rounds[i] && round < c.rounds[i+1] {
			return c.validatorSet[c.rounds[i]], nil
		}
	}

	//return last ValidatorSet
	return c.validatorSet[c.rounds[len(c.rounds)-1]], nil
}

func (c *ValidatorSetCache) GetAll() (map[int][]*types.Validator, error) {
	res := make(map[int][]*types.Validator)
	for _, r := range c.rounds {
		res[r] = c.validatorSet[r].Validators
	}
	return res, nil
}

func (c *ValidatorSetCache) ValidatorByAddress() map[string]*types.Validator {
	return c.validatorsByAddress
}

func (c *PeerSetCache) FirstRound(address string) (int64, bool) {
	fr, ok := c.firstRounds[address]
	if ok {
		return fr, true
	}
	return math.MaxInt64, false
}

//------------------------------------------------------------------------------

type PendingRound struct {
	Index   int64
	Decided bool
}

type OrderedPendingRounds []*PendingRound

func (a OrderedPendingRounds) Len() int      { return len(a) }
func (a OrderedPendingRounds) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a OrderedPendingRounds) Less(i, j int) bool {
	return a[i].Index < a[j].Index
}

type PendingRoundsCache struct {
	items       map[int64]*PendingRound
	sortedItems OrderedPendingRounds
}

func NewPendingRoundsCache() *PendingRoundsCache {
	return &PendingRoundsCache{
		items:       make(map[int64]*PendingRound),
		sortedItems: []*PendingRound{},
	}
}

func (c *PendingRoundsCache) Queued(round int64) bool {
	_, ok := c.items[round]
	return ok
}

func (c *PendingRoundsCache) Set(pendingRound *PendingRound) {
	c.items[pendingRound.Index] = pendingRound
	c.sortedItems = append(c.sortedItems, pendingRound)
	sort.Sort(c.sortedItems)
}

func (c *PendingRoundsCache) GetOrderedPendingRounds() OrderedPendingRounds {
	return c.sortedItems
}

func (c *PendingRoundsCache) Update(decidedRounds []int64) {
	for _, drn := range decidedRounds {
		if dr, ok := c.items[drn]; ok {
			dr.Decided = true
		}
	}
}

func (c *PendingRoundsCache) Clean(processedRounds []int64) {
	for _, pr := range processedRounds {
		delete(c.items, pr)
	}
	newSortedItems := OrderedPendingRounds{}
	for _, pr := range c.items {
		newSortedItems = append(newSortedItems, pr)
	}
	sort.Sort(newSortedItems)
	c.sortedItems = newSortedItems
}

//------------------------------------------------------------------------------
