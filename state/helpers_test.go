package state_test

import (
	"bytes"
	"fmt"

	sm "github.com/Fantom-foundation/go-txflow/state"
	"github.com/Fantom-foundation/go-txflow/types"
	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/crypto"
	"github.com/tendermint/tendermint/crypto/ed25519"
	"github.com/tendermint/tendermint/proxy"
	tsm "github.com/tendermint/tendermint/state"
	ttypes "github.com/tendermint/tendermint/types"
	tmtime "github.com/tendermint/tendermint/types/time"
	dbm "github.com/tendermint/tm-cmn/db"
)

type paramsChangeTestCase struct {
	height int64
	params ttypes.ConsensusParams
}

// always returns true if asked if any evidence was already committed.
type mockEvPoolAlwaysCommitted struct{}

func (m mockEvPoolAlwaysCommitted) PendingEvidence(int64) []ttypes.Evidence { return nil }
func (m mockEvPoolAlwaysCommitted) AddEvidence(ttypes.Evidence) error       { return nil }
func (m mockEvPoolAlwaysCommitted) Update(*types.Block, sm.State)           {}
func (m mockEvPoolAlwaysCommitted) IsCommitted(ttypes.Evidence) bool        { return true }

func newTestApp() proxy.AppConns {
	app := &testApp{}
	cc := proxy.NewLocalClientCreator(app)
	return proxy.NewAppConns(cc)
}

func makeAndCommitGoodBlock(
	state sm.State,
	height int64,
	lastCommit *ttypes.Commit,
	proposerAddr []byte,
	blockExec *sm.BlockExecutor,
	privVals map[string]ttypes.PrivValidator,
	evidence []ttypes.Evidence) (sm.State, ttypes.BlockID, *ttypes.Commit, error) {
	// A good block passes
	state, blockID, err := makeAndApplyGoodBlock(state, height, lastCommit, proposerAddr, blockExec, evidence)
	if err != nil {
		return state, ttypes.BlockID{}, nil, err
	}

	// Simulate a lastCommit for this block from all validators for the next height
	commit, err := makeValidCommit(height, blockID, state.Validators, privVals)
	if err != nil {
		return state, ttypes.BlockID{}, nil, err
	}
	return state, blockID, commit, nil
}

func makeAndApplyGoodBlock(state sm.State, height int64, lastCommit *ttypes.Commit, proposerAddr []byte,
	blockExec *sm.BlockExecutor, evidence []ttypes.Evidence) (sm.State, ttypes.BlockID, error) {
	block, _ := state.MakeBlock(height, makeTxs(height), nil, lastCommit, evidence, proposerAddr)
	if err := blockExec.ValidateBlock(state, block); err != nil {
		return state, ttypes.BlockID{}, err
	}
	blockID := ttypes.BlockID{Hash: block.Hash(), PartsHeader: ttypes.PartSetHeader{}}
	state, err := blockExec.ApplyBlock(state, blockID, block)
	if err != nil {
		return state, ttypes.BlockID{}, err
	}
	return state, blockID, nil
}

func makeVote(height int64, blockID ttypes.BlockID, valSet *ttypes.ValidatorSet, privVal ttypes.PrivValidator) (*ttypes.Vote, error) {
	addr := privVal.GetPubKey().Address()
	idx, _ := valSet.GetByAddress(addr)
	vote := &ttypes.Vote{
		ValidatorAddress: addr,
		ValidatorIndex:   idx,
		Height:           height,
		Round:            0,
		Timestamp:        tmtime.Now(),
		Type:             ttypes.PrecommitType,
		BlockID:          blockID,
	}
	if err := privVal.SignVote(chainID, vote); err != nil {
		return nil, err
	}
	return vote, nil
}

func makeValidCommit(height int64, blockID ttypes.BlockID, vals *ttypes.ValidatorSet, privVals map[string]ttypes.PrivValidator) (*ttypes.Commit, error) {
	sigs := make([]*ttypes.CommitSig, 0)
	for i := 0; i < vals.Size(); i++ {
		_, val := vals.GetByIndex(i)
		vote, err := makeVote(height, blockID, vals, privVals[val.Address.String()])
		if err != nil {
			return nil, err
		}
		sigs = append(sigs, vote.CommitSig())
	}
	return ttypes.NewCommit(blockID, sigs), nil
}

// make some bogus txs
func makeTxs(height int64) (txs []ttypes.Tx) {
	for i := 0; i < nTxsPerBlock; i++ {
		txs = append(txs, ttypes.Tx([]byte{byte(height), byte(i)}))
	}
	return txs
}

func makeState(nVals, height int) (sm.State, dbm.DB, map[string]ttypes.PrivValidator) {
	vals := make([]ttypes.GenesisValidator, nVals)
	privVals := make(map[string]ttypes.PrivValidator, nVals)
	for i := 0; i < nVals; i++ {
		secret := []byte(fmt.Sprintf("test%d", i))
		pk := ed25519.GenPrivKeyFromSecret(secret)
		valAddr := pk.PubKey().Address()
		vals[i] = ttypes.GenesisValidator{
			Address: valAddr,
			PubKey:  pk.PubKey(),
			Power:   1000,
			Name:    fmt.Sprintf("test%d", i),
		}
		privVals[valAddr.String()] = ttypes.NewMockPVWithParams(pk, false, false)
	}
	s, _ := sm.MakeGenesisState(&ttypes.GenesisDoc{
		ChainID:    chainID,
		Validators: vals,
		AppHash:    nil,
	})

	stateDB := dbm.NewMemDB()
	sm.SaveState(stateDB, s)

	for i := 1; i < height; i++ {
		s.LastBlockHeight++
		s.LastValidators = s.Validators.Copy()
		sm.SaveState(stateDB, s)
	}
	return s, stateDB, privVals
}

func makeBlock(state sm.State, height int64) *types.Block {
	block, _ := state.MakeBlock(height, makeTxs(state.LastBlockHeight), nil, new(ttypes.Commit), nil, state.Validators.GetProposer().Address)
	return block
}

func genValSet(size int) *ttypes.ValidatorSet {
	vals := make([]*ttypes.Validator, size)
	for i := 0; i < size; i++ {
		vals[i] = ttypes.NewValidator(ed25519.GenPrivKey().PubKey(), 10)
	}
	return ttypes.NewValidatorSet(vals)
}

func makeConsensusParams(
	blockBytes, blockGas int64,
	blockTimeIotaMs int64,
	evidenceAge int64,
) ttypes.ConsensusParams {
	return ttypes.ConsensusParams{
		Block: ttypes.BlockParams{
			MaxBytes:   blockBytes,
			MaxGas:     blockGas,
			TimeIotaMs: blockTimeIotaMs,
		},
		Evidence: ttypes.EvidenceParams{
			MaxAge: evidenceAge,
		},
	}
}

func makeHeaderPartsResponsesValPubKeyChange(state sm.State, pubkey crypto.PubKey) (ttypes.Header, ttypes.BlockID, *tsm.ABCIResponses) {

	block := makeBlock(state, state.LastBlockHeight+1)
	abciResponses := &tsm.ABCIResponses{
		EndBlock: &abci.ResponseEndBlock{ValidatorUpdates: nil},
	}

	// If the pubkey is new, remove the old and add the new.
	_, val := state.NextValidators.GetByIndex(0)
	if !bytes.Equal(pubkey.Bytes(), val.PubKey.Bytes()) {
		abciResponses.EndBlock = &abci.ResponseEndBlock{
			ValidatorUpdates: []abci.ValidatorUpdate{
				ttypes.TM2PB.NewValidatorUpdate(val.PubKey, 0),
				ttypes.TM2PB.NewValidatorUpdate(pubkey, 10),
			},
		}
	}

	return block.Header, ttypes.BlockID{Hash: block.Hash(), PartsHeader: ttypes.PartSetHeader{}}, abciResponses
}

func makeHeaderPartsResponsesValPowerChange(state sm.State, power int64) (ttypes.Header, ttypes.BlockID, *tsm.ABCIResponses) {

	block := makeBlock(state, state.LastBlockHeight+1)
	abciResponses := &tsm.ABCIResponses{
		EndBlock: &abci.ResponseEndBlock{ValidatorUpdates: nil},
	}

	// If the pubkey is new, remove the old and add the new.
	_, val := state.NextValidators.GetByIndex(0)
	if val.VotingPower != power {
		abciResponses.EndBlock = &abci.ResponseEndBlock{
			ValidatorUpdates: []abci.ValidatorUpdate{
				ttypes.TM2PB.NewValidatorUpdate(val.PubKey, power),
			},
		}
	}

	return block.Header, ttypes.BlockID{Hash: block.Hash(), PartsHeader: ttypes.PartSetHeader{}}, abciResponses
}

func makeHeaderPartsResponsesParams(state sm.State, params ttypes.ConsensusParams) (ttypes.Header, ttypes.BlockID, *tsm.ABCIResponses) {

	block := makeBlock(state, state.LastBlockHeight+1)
	abciResponses := &tsm.ABCIResponses{
		EndBlock: &abci.ResponseEndBlock{ConsensusParamUpdates: ttypes.TM2PB.ConsensusParams(&params)},
	}
	return block.Header, ttypes.BlockID{Hash: block.Hash(), PartsHeader: ttypes.PartSetHeader{}}, abciResponses
}

func randomGenesisDoc() *ttypes.GenesisDoc {
	pubkey := ed25519.GenPrivKey().PubKey()
	return &ttypes.GenesisDoc{
		GenesisTime: tmtime.Now(),
		ChainID:     "abc",
		Validators: []ttypes.GenesisValidator{
			{
				Address: pubkey.Address(),
				PubKey:  pubkey,
				Power:   10,
				Name:    "myval",
			},
		},
		ConsensusParams: ttypes.DefaultConsensusParams(),
	}
}

//----------------------------------------------------------------------------

type testApp struct {
	abci.BaseApplication

	CommitVotes         []abci.VoteInfo
	ByzantineValidators []abci.Evidence
	ValidatorUpdates    []abci.ValidatorUpdate
}

var _ abci.Application = (*testApp)(nil)

func (app *testApp) Info(req abci.RequestInfo) (resInfo abci.ResponseInfo) {
	return abci.ResponseInfo{}
}

func (app *testApp) BeginBlock(req abci.RequestBeginBlock) abci.ResponseBeginBlock {
	app.CommitVotes = req.LastCommitInfo.Votes
	app.ByzantineValidators = req.ByzantineValidators
	return abci.ResponseBeginBlock{}
}

func (app *testApp) EndBlock(req abci.RequestEndBlock) abci.ResponseEndBlock {
	return abci.ResponseEndBlock{ValidatorUpdates: app.ValidatorUpdates}
}

func (app *testApp) DeliverTx(req abci.RequestDeliverTx) abci.ResponseDeliverTx {
	return abci.ResponseDeliverTx{Events: []abci.Event{}}
}

func (app *testApp) CheckTx(req abci.RequestCheckTx) abci.ResponseCheckTx {
	return abci.ResponseCheckTx{}
}

func (app *testApp) Commit() abci.ResponseCommit {
	return abci.ResponseCommit{}
}

func (app *testApp) Query(reqQuery abci.RequestQuery) (resQuery abci.ResponseQuery) {
	return
}
