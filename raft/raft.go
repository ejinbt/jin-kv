package raft

import (
	"sync"

	"github.com/ejinbt/jinkv/wal"
)

type State int

const (
	Follower State = iota
	Candiate
	Leader
)

type Raft struct {
	// persistent state
	currentTerm uint64
	votedFor    *uint64
	log         []wal.Entry

	// volatile state - all servers
	commitIndex uint64
	lastApplied uint64

	// volatile state - leaders only
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	// node metadata
	id    uint64
	state State
	peers []string
	mu    sync.Mutex
	wal   *wal.WAL
}

func NewRaft(id uint64, peers []string, w *wal.WAL) *Raft {
	return &Raft{
		id:          id,
		peers:       peers,
		wal:         w,
		currentTerm: 0,
		votedFor:    nil,
		log:         nil,
		commitIndex: 0,
		lastApplied: 0,
		state:       Follower,
		nextIndex:   make(map[uint64]uint64),
		matchIndex:  make(map[uint64]uint64),
	}
}
