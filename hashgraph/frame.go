package hashgraph

import (
	"sort"

	"github.com/tendermint/tendermint/types"
)

type Frame struct {
	Round         int64 //RoundReceived
	Validators    []*types.Validator
	Roots         map[string]*Root
	Events        []*FrameEvent                 //Events with RoundReceived = Round
	ValidatorSets map[int64]*types.ValidatorSet //[round] => Peers
}

func (f *Frame) SortedFrameEvents() []*FrameEvent {
	sorted := SortedFrameEvents{}
	for _, r := range f.Roots {
		sorted = append(sorted, r.Events...)
	}
	sorted = append(sorted, f.Events...)
	sort.Sort(sorted)
	return sorted
}
