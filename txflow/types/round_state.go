package types

import (
	"encoding/json"
	"fmt"
	"time"

	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/types"
)

// RoundState defines the internal consensus state.
// NOTE: Not thread safe. Should only be manipulated by functions downstream
// of the cs.receiveRoutine
type RoundState struct {
	Height                    int64               `json:"height"` // Height we are working on
	StartTime                 time.Time           `json:"start_time"`
	CommitTime                time.Time           `json:"commit_time"` // Subjective time when +2/3 precommits for Block at Round were found
	Validators                *types.ValidatorSet `json:"validators"`
	Proposal                  *types.Proposal     `json:"proposal"`
	ProposalBlock             *types.Block        `json:"proposal_block"`
	ProposalBlockParts        *types.PartSet      `json:"proposal_block_parts"`
	LockedRound               int                 `json:"locked_round"`
	LockedBlock               *types.Block        `json:"locked_block"`
	LockedBlockParts          *types.PartSet      `json:"locked_block_parts"`
	ValidRound                int                 `json:"valid_round"`       // Last known round with POL for non-nil valid block.
	ValidBlock                *types.Block        `json:"valid_block"`       // Last known block of POL mentioned above.
	ValidBlockParts           *types.PartSet      `json:"valid_block_parts"` // Last known block parts of POL metnioned above.
	Votes                     *HeightVoteSet      `json:"votes"`
	CommitRound               int                 `json:"commit_round"` //
	LastCommit                *types.VoteSet      `json:"last_commit"`  // Last precommits at Height-1
	LastValidators            *types.ValidatorSet `json:"last_validators"`
	TriggeredTimeoutPrecommit bool                `json:"triggered_timeout_precommit"`
}

// Compressed version of the RoundState for use in RPC
type RoundStateSimple struct {
	HeightRoundStep   string          `json:"height/round/step"`
	StartTime         time.Time       `json:"start_time"`
	ProposalBlockHash cmn.HexBytes    `json:"proposal_block_hash"`
	LockedBlockHash   cmn.HexBytes    `json:"locked_block_hash"`
	ValidBlockHash    cmn.HexBytes    `json:"valid_block_hash"`
	Votes             json.RawMessage `json:"height_vote_set"`
}

// Compress the RoundState to RoundStateSimple
func (rs *RoundState) RoundStateSimple() RoundStateSimple {
	votesJSON, err := rs.Votes.MarshalJSON()
	if err != nil {
		panic(err)
	}
	return RoundStateSimple{
		HeightRoundStep:   fmt.Sprintf("%d/%d/%d", rs.Height, rs.Round, rs.Step),
		StartTime:         rs.StartTime,
		ProposalBlockHash: rs.ProposalBlock.Hash(),
		LockedBlockHash:   rs.LockedBlock.Hash(),
		ValidBlockHash:    rs.ValidBlock.Hash(),
		Votes:             votesJSON,
	}
}

// NewRoundEvent returns the RoundState with proposer information as an event.
func (rs *RoundState) NewRoundEvent() types.EventDataNewRound {
	addr := rs.Validators.GetProposer().Address
	idx, _ := rs.Validators.GetByAddress(addr)

	return types.EventDataNewRound{
		Height: rs.Height,
		Round:  rs.Round,
		Step:   rs.Step.String(),
		Proposer: types.ValidatorInfo{
			Address: addr,
			Index:   idx,
		},
	}
}

// CompleteProposalEvent returns information about a proposed block as an event.
func (rs *RoundState) CompleteProposalEvent() types.EventDataCompleteProposal {
	// We must construct BlockID from ProposalBlock and ProposalBlockParts
	// cs.Proposal is not guaranteed to be set when this function is called
	blockId := types.BlockID{
		Hash:        rs.ProposalBlock.Hash(),
		PartsHeader: rs.ProposalBlockParts.Header(),
	}

	return types.EventDataCompleteProposal{
		Height:  rs.Height,
		Round:   rs.Round,
		Step:    rs.Step.String(),
		BlockID: blockId,
	}
}

// RoundStateEvent returns the H/R/S of the RoundState as an event.
func (rs *RoundState) RoundStateEvent() types.EventDataRoundState {
	return types.EventDataRoundState{
		Height: rs.Height,
		Round:  rs.Round,
		Step:   rs.Step.String(),
	}
}

// String returns a string
func (rs *RoundState) String() string {
	return rs.StringIndented("")
}

// StringIndented returns a string
func (rs *RoundState) StringIndented(indent string) string {
	return fmt.Sprintf(`RoundState{
%s  H:%v R:%v S:%v
%s  StartTime:     %v
%s  CommitTime:    %v
%s  Validators:    %v
%s  Proposal:      %v
%s  ProposalBlock: %v %v
%s  LockedRound:   %v
%s  LockedBlock:   %v %v
%s  ValidRound:   %v
%s  ValidBlock:   %v %v
%s  Votes:         %v
%s  LastCommit:    %v
%s  LastValidators:%v
%s}`,
		indent, rs.Height, rs.Round, rs.Step,
		indent, rs.StartTime,
		indent, rs.CommitTime,
		indent, rs.Validators.StringIndented(indent+"  "),
		indent, rs.Proposal,
		indent, rs.ProposalBlockParts.StringShort(), rs.ProposalBlock.StringShort(),
		indent, rs.LockedRound,
		indent, rs.LockedBlockParts.StringShort(), rs.LockedBlock.StringShort(),
		indent, rs.ValidRound,
		indent, rs.ValidBlockParts.StringShort(), rs.ValidBlock.StringShort(),
		indent, rs.Votes.StringIndented(indent+"  "),
		indent, rs.LastCommit.StringShort(),
		indent, rs.LastValidators.StringIndented(indent+"  "),
		indent)
}

// StringShort returns a string
func (rs *RoundState) StringShort() string {
	return fmt.Sprintf(`RoundState{H:%v R:%v S:%v ST:%v}`,
		rs.Height, rs.Round, rs.Step, rs.StartTime)
}

//-----------------------------------------------------------
// These methods are for Protobuf Compatibility

// Size returns the size of the amino encoding, in bytes.
func (rs *RoundStateSimple) Size() int {
	bs, _ := rs.Marshal()
	return len(bs)
}

// Marshal returns the amino encoding.
func (rs *RoundStateSimple) Marshal() ([]byte, error) {
	return cdc.MarshalBinaryBare(rs)
}

// MarshalTo calls Marshal and copies to the given buffer.
func (rs *RoundStateSimple) MarshalTo(data []byte) (int, error) {
	bs, err := rs.Marshal()
	if err != nil {
		return -1, err
	}
	return copy(data, bs), nil
}

// Unmarshal deserializes from amino encoded form.
func (rs *RoundStateSimple) Unmarshal(bs []byte) error {
	return cdc.UnmarshalBinaryBare(bs, rs)
}
