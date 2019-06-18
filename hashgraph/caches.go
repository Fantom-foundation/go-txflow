package hashgraph

import (
	"fmt"
	"sort"
)

type Key struct {
	x, y string
}

func (k Key) ToString() string {
	return fmt.Sprintf("{%s, %s}", k.x, k.y)
}

type TreKey struct {
	x, y, z string
}

func (k TreKey) ToString() string {
	return fmt.Sprintf("{%s, %s, %s}", k.x, k.y, k.z)
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
