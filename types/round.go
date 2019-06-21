package types

import (
	"bytes"

	"github.com/andrecronje/babble/src/common"
	"github.com/tendermint/tendermint/types"
	"github.com/ugorji/go/codec"
)

type pendingRound struct {
	Index   int
	Decided bool
}

type RoundEvent struct {
	Witness bool
	Famous  common.Trilean
}

type Round struct {
	CreatedEvents  map[string]RoundEvent
	ReceivedEvents []string
	Queued         bool
	Decided        bool
}

func NewRound() *Round {
	return &Round{
		CreatedEvents:  make(map[string]RoundEvent),
		ReceivedEvents: []string{},
	}
}

func (r *Round) AddCreatedEvent(x string, witness bool) {
	_, ok := r.CreatedEvents[x]
	if !ok {
		r.CreatedEvents[x] = RoundEvent{
			Witness: witness,
		}
	}
}

func (r *Round) AddReceivedEvent(x string) {
	r.ReceivedEvents = append(r.ReceivedEvents, x)
}

func (r *Round) SetFame(x string, f bool) {
	e, ok := r.CreatedEvents[x]
	if !ok {
		e = RoundEvent{
			Witness: true,
		}
	}

	if f {
		e.Famous = common.True
	} else {
		e.Famous = common.False
	}

	r.CreatedEvents[x] = e
}

/*
WitnessesDecided returns true if a super-majority of witnesses are decided,
and there are no undecided witnesses. Our algorithm relies on the fact that a
witness that is not yet known when a super-majority of witnesses are already
decided, has no chance of ever being famous. Once a Round is decided it stays
decided, even if new witnesses are added after it was first decided.
*/
func (r *Round) WitnessesDecided(validatorSet *types.ValidatorSet) bool {
	//if the round was already decided, it stays decided no matter what.
	if r.Decided {
		return true
	}

	c := int64(0)
	for _, e := range r.CreatedEvents {
		if e.Witness && e.Famous != common.Undefined {
			c++
		} else if e.Witness && e.Famous == common.Undefined {
			return false
		}
	}

	r.Decided = c > validatorSet.TotalVotingPower()*2/3

	return r.Decided
}

//Witnesses return witnesses
func (r *Round) Witnesses() []string {
	res := []string{}
	for x, e := range r.CreatedEvents {
		if e.Witness {
			res = append(res, x)
		}
	}

	return res
}

//FamousWitnesses return famous witnesses
func (r *Round) FamousWitnesses() []string {
	res := []string{}
	for x, e := range r.CreatedEvents {
		if e.Witness && e.Famous == common.True {
			res = append(res, x)
		}
	}
	return res
}

func (r *Round) IsDecided(witness string) bool {
	w, ok := r.CreatedEvents[witness]
	return ok && w.Witness && w.Famous != common.Undefined
}

func (r *Round) Marshal() ([]byte, error) {
	b := new(bytes.Buffer)
	jh := new(codec.JsonHandle)
	jh.Canonical = true
	enc := codec.NewEncoder(b, jh)

	if err := enc.Encode(r); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (r *Round) Unmarshal(data []byte) error {
	b := bytes.NewBuffer(data)
	jh := new(codec.JsonHandle)
	jh.Canonical = true
	dec := codec.NewDecoder(b, jh)

	return dec.Decode(r)
}

func (r *Round) IsQueued() bool {
	return r.Queued
}
