package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ejinbt/jinkv/rpc"
	"github.com/ejinbt/jinkv/wal"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
	electionTimeoutMin = 150
	electionTimeoutMax = 300
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
	id            uint64
	state         State
	peers         []string
	mu            sync.Mutex
	wal           *wal.WAL
	electionTimer *time.Timer
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

func (r *Raft) resetElectionTimer() {
	// pick random duration in range , reset r.electionTimer
	randomMs := electionTimeoutMin + rand.Intn(electionTimeoutMax-electionTimeoutMin) // gives you 150 to 299
	timeout := time.Duration(randomMs) * time.Millisecond

	if r.electionTimer == nil {
		// first call ever - create it
		r.electionTimer = time.NewTimer(timeout)
	} else {
		// subsequent calls - resuse it
		r.electionTimer.Reset(timeout)
	}

}

func (r *Raft) requestVotes() {
	var lastLogIndex uint64
	var lastLogTerm uint64

	if len(r.log) > 0 {
		lastEntry := r.log[len(r.log)-1]
		lastLogIndex = lastEntry.Index
		lastLogTerm = lastEntry.Term
	}

	args := rpc.RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateID:  r.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	fmt.Printf("%d", args.Term)

	for _, peer := range r.peers {
		go func(peer string) {
			// TODO: send args to peer over the network, handle reply
		}(peer)
	}
}

func (r *Raft) becomeCandidate() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm++
	r.state = Candidate
	selfID := r.id
	r.votedFor = &selfID
	r.resetElectionTimer()
	r.requestVotes() // yet to write this
}

func (r *Raft) electionLoop() {
	for {
		<-r.electionTimer.C
		// timer fired with no reset - become candiate
		r.becomeCandidate()
	}
}

func (r *Raft) Start() {
	// this is where timer begins and the election watching goroutine launches
	r.resetElectionTimer()
	go r.electionLoop()
}
