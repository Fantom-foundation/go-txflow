package privval

import (
	"github.com/Fantom-foundation/go-txflow/types"
	amino "github.com/tendermint/go-amino"
	"github.com/tendermint/tendermint/privval"
)

// RemoteSignerMsg is sent between SignerServiceEndpoint and the SignerServiceEndpoint client.
type RemoteSignerMsg interface{}

func RegisterRemoteSignerMsg(cdc *amino.Codec) {
	cdc.RegisterInterface((*RemoteSignerMsg)(nil), nil)
	cdc.RegisterConcrete(&SignTxVoteRequest{}, "tendermint/remotesigner/SignTxVoteRequest", nil)
	cdc.RegisterConcrete(&SignedTxVoteResponse{}, "tendermint/remotesigner/SignedTxVoteResponse", nil)
}

// SignTxVoteRequest is a PrivValidatorSocket message containing a vote.
type SignTxVoteRequest struct {
	Vote *types.TxVote
}

// SignedTxVoteResponse is a PrivValidatorSocket message containing a signed vote along with a potenial error message.
type SignedTxVoteResponse struct {
	Vote  *types.TxVote
	Error *privval.RemoteSignerError
}
