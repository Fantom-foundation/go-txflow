package state

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/Fantom-foundation/txflow/types"
	"github.com/tendermint/tendermint/crypto"
	dbm "github.com/tendermint/tendermint/libs/db"
	ttypes "github.com/tendermint/tendermint/types"
)

//-----------------------------------------------------
// Validate block

func validateEventBlock(evidencePool EvidencePool, stateDB dbm.DB, state State, eventBlock *types.EventBlock) error {
	// Validate internal consistency.
	if err := eventBlock.ValidateBasic(); err != nil {
		return err
	}

	// Validate basic info.
	if eventBlock.Version != state.Version.Consensus {
		return fmt.Errorf("Wrong EventBlock.Header.Version. Expected %v, got %v",
			state.Version.Consensus,
			eventBlock.Version,
		)
	}
	if eventBlock.ChainID != state.ChainID {
		return fmt.Errorf("Wrong EventBlock.Header.ChainID. Expected %v, got %v",
			state.ChainID,
			eventBlock.ChainID,
		)
	}
	if eventBlock.Height != state.LastBlockHeight+1 {
		return fmt.Errorf("Wrong EventBlock.Header.Height. Expected %v, got %v",
			state.LastBlockHeight+1,
			eventBlock.Height,
		)
	}

	// Validate prev block info.
	if !eventBlock.SelfEventBlockID.Equals(state.SelfEventBlockID) {
		return fmt.Errorf("Wrong EventBlock.Header.LastBlockID.  Expected %v, got %v",
			state.SelfEventBlockID,
			eventBlock.SelfEventBlockID,
		)
	}

	newTxs := int64(len(eventBlock.Data.Txs))
	if eventBlock.TotalTxs != state.LastBlockTotalTx+newTxs {
		return fmt.Errorf("Wrong EventBlock.Header.TotalTxs. Expected %v, got %v",
			state.LastBlockTotalTx+newTxs,
			eventBlock.TotalTxs,
		)
	}

	if !bytes.Equal(eventBlock.ConsensusHash, state.ConsensusParams.Hash()) {
		return fmt.Errorf("Wrong EventBlock.Header.ConsensusHash.  Expected %X, got %v",
			state.ConsensusParams.Hash(),
			eventBlock.ConsensusHash,
		)
	}
	if !bytes.Equal(eventBlock.ValidatorsHash, state.Validators.Hash()) {
		return fmt.Errorf("Wrong EventBlock.Header.ValidatorsHash.  Expected %X, got %v",
			state.Validators.Hash(),
			eventBlock.ValidatorsHash,
		)
	}
	if !bytes.Equal(eventBlock.NextValidatorsHash, state.NextValidators.Hash()) {
		return fmt.Errorf("Wrong EventBlock.Header.NextValidatorsHash.  Expected %X, got %v",
			state.NextValidators.Hash(),
			eventBlock.NextValidatorsHash,
		)
	}

	// Validate block Time
	if eventBlock.Height > 1 {
		if !eventBlock.Time.After(state.LastBlockTime) {
			return fmt.Errorf("EventBlock time %v not greater than last EventBlock time %v",
				eventBlock.Time,
				state.LastBlockTime,
			)
		}
	} else if eventBlock.Height == 1 {
		genesisTime := state.LastBlockTime
		if !eventBlock.Time.Equal(genesisTime) {
			return fmt.Errorf("EventBlock time %v is not equal to genesis time %v",
				eventBlock.Time,
				genesisTime,
			)
		}
	}

	// Limit the amount of evidence
	maxNumEvidence, _ := ttypes.MaxEvidencePerBlock(state.ConsensusParams.Block.MaxBytes)
	numEvidence := int64(len(eventBlock.Evidence.Evidence))
	if numEvidence > maxNumEvidence {
		return ttypes.NewErrEvidenceOverflow(maxNumEvidence, numEvidence)

	}

	// Validate all evidence.
	for _, ev := range eventBlock.Evidence.Evidence {
		if err := VerifyEvidence(stateDB, state, ev); err != nil {
			return ttypes.NewErrEvidenceInvalid(ev, err)
		}
		if evidencePool != nil && evidencePool.IsCommitted(ev) {
			return ttypes.NewErrEvidenceInvalid(ev, errors.New("evidence was already committed"))
		}
	}

	// NOTE: We can't actually verify it's the right proposer because we dont
	// know what round the block was first proposed. So just check that it's
	// a legit address and a known validator.
	if len(eventBlock.ProposerAddress) != crypto.AddressSize ||
		!state.Validators.HasAddress(eventBlock.ProposerAddress) {
		return fmt.Errorf("EventBlock.Header.ProposerAddress, %X, is not a validator",
			eventBlock.ProposerAddress,
		)
	}

	return nil
}

// VerifyEvidence verifies the evidence fully by checking:
// - it is sufficiently recent (MaxAge)
// - it is from a key who was a validator at the given height
// - it is internally consistent
// - it was properly signed by the alleged equivocator
func VerifyEvidence(stateDB dbm.DB, state State, evidence ttypes.Evidence) error {
	height := state.LastBlockHeight

	evidenceAge := height - evidence.Height()
	maxAge := state.ConsensusParams.Evidence.MaxAge
	if evidenceAge > maxAge {
		return fmt.Errorf("Evidence from height %d is too old. Min height is %d",
			evidence.Height(), height-maxAge)
	}

	valset, err := LoadValidators(stateDB, evidence.Height())
	if err != nil {
		// TODO: if err is just that we cant find it cuz we pruned, ignore.
		// TODO: if its actually bad evidence, punish peer
		return err
	}

	// The address must have been an active validator at the height.
	// NOTE: we will ignore evidence from H if the key was not a validator
	// at H, even if it is a validator at some nearby H'
	// XXX: this makes lite-client bisection as is unsafe
	// See https://github.com/tendermint/tendermint/issues/3244
	ev := evidence
	height, addr := ev.Height(), ev.Address()
	_, val := valset.GetByAddress(addr)
	if val == nil {
		return fmt.Errorf("Address %X was not a validator at height %d", addr, height)
	}

	if err := evidence.Verify(state.ChainID, val.PubKey); err != nil {
		return err
	}

	return nil
}
