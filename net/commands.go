package net

import (
	"github.com/andrecronje/babble-abci/hashgraph"
	"github.com/mosaicnetworks/babble/src/peers"
)

//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

type SyncRequest struct {
	FromID    uint32
	Known     map[uint32]int
	SyncLimit int
}

type SyncResponse struct {
	FromID uint32
	Events []hashgraph.WireEvent
	Known  map[uint32]int
}

//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

type EagerSyncRequest struct {
	FromID uint32
	Events []hashgraph.WireEvent
}

type EagerSyncResponse struct {
	FromID  uint32
	Success bool
}

//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

type FastForwardRequest struct {
	FromID uint32
}

type FastForwardResponse struct {
	FromID   uint32
	Block    hashgraph.Block
	Frame    hashgraph.Frame
	Snapshot []byte
}

//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

type JoinRequest struct {
	InternalTransaction hashgraph.InternalTransaction
}

type JoinResponse struct {
	FromID        uint32
	Accepted      bool
	AcceptedRound int
	Peers         []*peers.Peer
}
