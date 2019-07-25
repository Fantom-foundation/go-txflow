package consensus

import (
	"io"

	"github.com/tendermint/tendermint/consensus"
)

type nilWAL struct{}

var _ consensus.WAL = nilWAL{}

func (nilWAL) Write(m consensus.WALMessage)     {}
func (nilWAL) WriteSync(m consensus.WALMessage) {}
func (nilWAL) FlushAndSync() error              { return nil }
func (nilWAL) SearchForEndHeight(height int64, options *consensus.WALSearchOptions) (rd io.ReadCloser, found bool, err error) {
	return nil, false, nil
}
func (nilWAL) Start() error { return nil }
func (nilWAL) Stop() error  { return nil }
func (nilWAL) Wait()        {}
