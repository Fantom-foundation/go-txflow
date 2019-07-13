package privval

import (
	"fmt"

	"github.com/Fantom-foundation/go-txflow/types"
	"github.com/tendermint/tendermint/crypto"
	cmn "github.com/tendermint/tendermint/libs/common"
	ttypes "github.com/tendermint/tendermint/types"

	"github.com/tendermint/tendermint/privval"
)

//-------------------------------------------------------------------------------

// FilePV implements PrivValidator using data persisted to disk
// to prevent double signing.
// NOTE: the directories containing pv.Key.filePath and pv.LastSignState.filePath must already exist.
// It includes the LastSignature and LastSignBytes so we don't lose the signature
// if the process crashes after signing but before the resulting consensus message is processed.
type FilePV struct {
	tpv *privval.FilePV
}

// LoadOrGenFilePV loads a FilePV from the given filePaths
// or else generates a new one and saves it to the filePaths.
func LoadOrGenFilePV(keyFilePath, stateFilePath string) *FilePV {
	var pv *privval.FilePV
	if cmn.FileExists(keyFilePath) {
		pv = privval.LoadFilePV(keyFilePath, stateFilePath)
	} else {
		pv = privval.GenFilePV(keyFilePath, stateFilePath)
		pv.Save()
	}
	return &FilePV{tpv: pv}
}

// GetAddress returns the address of the validator.
// Implements PrivValidator.
func (pv *FilePV) GetAddress() ttypes.Address {
	return pv.tpv.GetAddress()
}

// GetPubKey returns the public key of the validator.
// Implements PrivValidator.
func (pv *FilePV) GetPubKey() crypto.PubKey {
	return pv.tpv.GetPubKey()
}

// SignVote signs a canonical representation of the vote, along with the
// chainID. Implements PrivValidator.
func (pv *FilePV) SignVote(chainID string, vote *ttypes.Vote) error {
	return pv.tpv.SignVote(chainID, vote)
}

// SignTxVote signs a canonical representation of the vote, along with the
// chainID. Implements PrivValidator.
func (pv *FilePV) SignTxVote(chainID string, vote *types.TxVote) error {
	if err := pv.signTxVote(chainID, vote); err != nil {
		return fmt.Errorf("error signing vote: %v", err)
	}
	return nil
}

// SignProposal signs a canonical representation of the proposal, along with
// the chainID. Implements PrivValidator.
func (pv *FilePV) SignProposal(chainID string, proposal *ttypes.Proposal) error {
	return pv.tpv.SignProposal(chainID, proposal)
}

// Save persists the FilePV to disk.
func (pv *FilePV) Save() {
	pv.tpv.Save()
}

// Reset resets all fields in the FilePV.
// NOTE: Unsafe!
func (pv *FilePV) Reset() {
	pv.tpv.Reset()
}

// String returns a string representation of the FilePV.
func (pv *FilePV) String() string {
	return pv.tpv.String()
}

//------------------------------------------------------------------------------------

// signVote checks if the vote is good to sign and sets the vote signature.
// It may need to set the timestamp as well if the vote is otherwise the same as
// a previously signed vote (ie. we crashed after signing but before the vote hit the WAL).
func (pv *FilePV) signTxVote(chainID string, vote *types.TxVote) error {

	signBytes := vote.SignBytes(chainID)
	// It passed the checks. Sign the vote
	sig, err := pv.tpv.Key.PrivKey.Sign(signBytes)
	if err != nil {
		return err
	}
	vote.Signature = sig
	return nil
}
