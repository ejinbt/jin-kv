package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/ejinbt/jinkv/rpc"
	"github.com/ejinbt/jinkv/wal"
)

type Transport interface {
	SendRequestVote(peerAddr string, args rpc.RequestVoteArgs) (rpc.RequestVoteReply, error)
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

const (
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
	peers         map[uint64]string
	mu            sync.Mutex
	wal           *wal.WAL
	electionTimer *time.Timer
	transport     Transport
}

func NewRaft(id uint64, peers map[uint64]string, w *wal.WAL, t Transport) *Raft {
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
		transport:   t,
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

func (r *Raft) becomeLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.state = Leader
	lastIndex, _ := r.lastLogIndexAndTerm()

	for peerID := range r.peers {
		r.matchIndex[peerID] = lastIndex + 1
		r.matchIndex[peerID] = 0
	}
	log.Printf("[node %d] BECAME LEADER , term %d", r.id, r.currentTerm)
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

	votes := 1 // vote for self , already counted
	var voteMu sync.Mutex
	majority := len(r.peers)/2 + 1

	for peerID, peerAddr := range r.peers {
		if peerID == r.id {
			continue // don't send RPC to self , already voted for self in becomeCandidate
		}
		go func(peerID uint64, peerAddr string) {
			// TODO: send args to peer over the network, handle reply
			reply, err := r.transport.SendRequestVote(peerAddr, args)
			if err != nil {
				return // peer unreachable , just skip it
			}

			voteMu.Lock()
			defer voteMu.Unlock()

			// if peer's term is ahead , step down immediately
			if reply.Term > r.currentTerm {
				r.mu.Lock()
				r.currentTerm = reply.Term
				r.state = Follower
				r.votedFor = nil
				r.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				votes++
				if votes >= majority && r.state == Candidate {
					r.becomeLeader() // not written , next step
				}
			}
		}(peerID, peerAddr)
	}
}

func (r *Raft) lastLogIndexAndTerm() (uint64, uint64) {
	if len(r.log) == 0 {
		return 0, 0
	}

	last := r.log[len(r.log)-1]
	return last.Index, last.Term
}

func (r *Raft) isCandidateLogUpToDate(candiateLastLogTerm, candidateLastLogIndex uint64) bool {

	userIndex, userTerm := r.lastLogIndexAndTerm()

	if candiateLastLogTerm > userTerm {
		return true
	} else if candiateLastLogTerm == userTerm {
		if candidateLastLogIndex >= userIndex {
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

func (r *Raft) HandleRequestVote(args rpc.RequestVoteArgs) rpc.RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		return rpc.RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
	}

	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.votedFor = nil
		r.state = Follower
	}

	voteOK := r.votedFor == nil || *r.votedFor == args.CandidateID
	logOk := r.isCandidateLogUpToDate(args.LastLogTerm, args.LastLogIndex)

	if voteOK && logOk {
		candidateID := args.CandidateID
		r.votedFor = &candidateID
		r.resetElectionTimer()
		reply := rpc.RequestVoteReply{Term: r.currentTerm, VoteGranted: true}
		log.Printf("[node %d] vote request from %d , granted=%v", r.id, args.CandidateID, reply.VoteGranted)
		return reply
	} else {
		reply := rpc.RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
		log.Printf("[node %d] vote request from %d , granted=%v", r.id, args.CandidateID, reply.VoteGranted)
		return reply
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
	r.requestVotes()

	log.Printf("[node %d] become candidate, term %d", r.id, r.currentTerm)

}

func (r *Raft) electionLoop() {
	for {
		<-r.electionTimer.C
		r.mu.Lock()
		state := r.state
		r.mu.Unlock()
		// timer fired with no reset - become candiate
		if state != Leader {
			r.becomeCandidate()
		}
	}
}

func (r *Raft) Start() {
	// this is where timer begins and the election watching goroutine launches
	r.resetElectionTimer()
	go r.electionLoop()
}
