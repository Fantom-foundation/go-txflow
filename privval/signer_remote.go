package privval

import (
	"fmt"
	"io"
	"net"

	"github.com/Fantom-foundation/go-txflow/types"
	"github.com/tendermint/tendermint/crypto"
	cmn "github.com/tendermint/tendermint/libs/common"
	"github.com/tendermint/tendermint/privval"
	ttypes "github.com/tendermint/tendermint/types"
)

// timeoutError can be used to check if an error returned from the netp package
// was due to a timeout.
type timeoutError interface {
	Timeout() bool
}

// SignerRemote implements PrivValidator.
// It uses a net.Conn to request signatures from an external process.
type SignerRemote struct {
	sr   *privval.SignerRemote
	conn net.Conn
}

// Check that SignerRemote implements PrivValidator.
var _ types.PrivValidator = (*SignerRemote)(nil)

// NewSignerRemote returns an instance of SignerRemote.
func NewSignerRemote(conn net.Conn) (*SignerRemote, error) {

	// retrieve and memoize the consensus public key once.
	sr, err := privval.NewSignerRemote(conn)
	if err != nil {
		return nil, err
	}
	return &SignerRemote{
		sr:   sr,
		conn: conn,
	}, nil
}

// Close calls Close on the underlying net.Conn.
func (sc *SignerRemote) Close() error {
	return sc.sr.Close()
}

// GetPubKey implements PrivValidator.
func (sc *SignerRemote) GetPubKey() crypto.PubKey {
	return sc.sr.GetPubKey()
}

// SignVote implements PrivValidator.
func (sc *SignerRemote) SignVote(chainID string, vote *ttypes.Vote) error {
	return sc.sr.SignVote(chainID, vote)
}

// SignTxVote implements PrivValidator.
func (sc *SignerRemote) SignTxVote(chainID string, vote *types.TxVote) error {
	err := writeMsg(sc.conn, &SignTxVoteRequest{Vote: vote})
	if err != nil {
		return err
	}

	res, err := readMsg(sc.conn)
	if err != nil {
		return err
	}

	resp, ok := res.(*SignedTxVoteResponse)
	if !ok {
		return privval.ErrUnexpectedResponse
	}
	if resp.Error != nil {
		return resp.Error
	}
	*vote = *resp.Vote

	return nil
}

// SignProposal implements PrivValidator.
func (sc *SignerRemote) SignProposal(chainID string, proposal *ttypes.Proposal) error {
	return sc.sr.SignProposal(chainID, proposal)
}

// Ping is used to check connection health.
func (sc *SignerRemote) Ping() error {
	return sc.sr.Ping()
}

func readMsg(r io.Reader) (msg RemoteSignerMsg, err error) {
	const maxRemoteSignerMsgSize = 1024 * 10
	_, err = cdc.UnmarshalBinaryLengthPrefixedReader(r, &msg, maxRemoteSignerMsgSize)
	if _, ok := err.(timeoutError); ok {
		err = cmn.ErrorWrap(privval.ErrConnTimeout, err.Error())
	}
	return
}

func writeMsg(w io.Writer, msg interface{}) (err error) {
	_, err = cdc.MarshalBinaryLengthPrefixedWriter(w, msg)
	if _, ok := err.(timeoutError); ok {
		err = cmn.ErrorWrap(privval.ErrConnTimeout, err.Error())
	}
	return
}

func handleRequest(req RemoteSignerMsg, chainID string, privVal types.PrivValidator) (RemoteSignerMsg, error) {
	var res RemoteSignerMsg
	var err error

	switch r := req.(type) {
	case *privval.PubKeyRequest:
		var p crypto.PubKey
		p = privVal.GetPubKey()
		res = &privval.PubKeyResponse{p, nil}

	case *privval.SignVoteRequest:
		err = privVal.SignVote(chainID, r.Vote)
		if err != nil {
			res = &privval.SignedVoteResponse{nil, &privval.RemoteSignerError{0, err.Error()}}
		} else {
			res = &privval.SignedVoteResponse{r.Vote, nil}
		}

	case *SignTxVoteRequest:
		err = privVal.SignTxVote(chainID, r.Vote)
		if err != nil {
			res = &SignedTxVoteResponse{nil, &privval.RemoteSignerError{0, err.Error()}}
		} else {
			res = &SignedTxVoteResponse{r.Vote, nil}
		}

	case *privval.SignProposalRequest:
		err = privVal.SignProposal(chainID, r.Proposal)
		if err != nil {
			res = &privval.SignedProposalResponse{nil, &privval.RemoteSignerError{0, err.Error()}}
		} else {
			res = &privval.SignedProposalResponse{r.Proposal, nil}
		}

	case *privval.PingRequest:
		res = &privval.PingResponse{}

	default:
		err = fmt.Errorf("unknown msg: %v", r)
	}

	return res, err
}
