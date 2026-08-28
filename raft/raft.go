package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ejinbt/jinkv/rpc"
	"github.com/ejinbt/jinkv/wal"
)

type Transport interface {
	SendRequestVote(peerAddr string, args rpc.RequestVoteArgs) (rpc.RequestVoteReply, error)
	SendAppendEntries(peerAddr string, args rpc.AppendEntriesArgs) (rpc.AppendEntriesReply, error)
	SendInstallSnapshot(peerAddr string, args rpc.InstallSnapshotArgs) (rpc.InstallSnapshotReply, error)
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

type StateMachine struct {
	mu   sync.Mutex
	data map[string]string
}

func NewStateMachine() *StateMachine {
	return &StateMachine{data: make(map[string]string)}
}

// Apply parses a command string like "set x=1" and update the
// state machine accordingly . Caller does not need to hold the lock
func (s *StateMachine) Apply(entry wal.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	parts := strings.Fields(string(entry.Data)) // ["set","x=1"]
	if len(parts) != 2 || parts[0] != "set" {
		return // unrecognized command , ignore
	}

	kv := strings.SplitN(parts[1], "=", 2)
	if len(kv) != 2 {
		return // malformed key=value , ignore
	}
	key, value := kv[0], kv[1]
	s.data[key] = value
}

func (s *StateMachine) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.data[key]
	return val, ok
}

type Raft struct {
	// persistent state
	currentTerm uint64
	votedFor    *uint64
	log         []wal.Entry
	logOffset   uint64 // r.log[0] has index == logOffset + 1; 0 until first compact

	// volatile state - all servers
	commitIndex uint64
	lastApplied uint64

	// volatile state - leaders only
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	// node metadata
	id                uint64
	state             State
	peers             map[uint64]string
	mu                sync.Mutex
	wal               *wal.WAL
	electionTimer     *time.Timer
	transport         Transport
	stateMachine      *StateMachine
	lastSnapshotIndex uint64 // starts at 0
	leaderID          uint64 // the most recently known leader's ID; 0 if unknown
}

type peerData struct {
	entries   []wal.Entry
	prevIndex uint64
	prevTerm  uint64
}

func NewRaft(id uint64, peers map[uint64]string, w *wal.WAL, t Transport, s *StateMachine) *Raft {
	return &Raft{
		id:           id,
		peers:        peers,
		wal:          w,
		currentTerm:  0,
		votedFor:     nil,
		log:          nil,
		commitIndex:  0,
		lastApplied:  0,
		state:        Follower,
		nextIndex:    make(map[uint64]uint64),
		matchIndex:   make(map[uint64]uint64),
		transport:    t,
		stateMachine: s,
	}
}

// toSlicePos converts a Raft log index into a position in r.log,
// accounting for entries that have been compacted away via snapshots
// Caller must hold r.mu
func (r *Raft) toSlicePos(index uint64) int {
	return int(index - r.logOffset - 1)
}

// Get retrieves the current value for a key from this node's state
// machine. Note: this reads local state directly and does not guarantee
// linearizability — see §8 of the Raft paper for what a proper
// linearizable read requires (leader confirmation + no-op check).
// Good enough for testing; not yet safe for a real client-facing read.
func (r *Raft) Get(key string) (string, bool) {
	return r.stateMachine.Get(key)
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
	r.resetElectionTimer() // stop reacting to a timer that shouldn't matter anymore
	lastIndex, _ := r.lastLogIndexAndTerm()

	for peerID := range r.peers {
		r.nextIndex[peerID] = lastIndex + 1 // optimistic guess: peer needs everything from here on
		r.matchIndex[peerID] = 0            // don't yet know what's actually replicated
	}

	// append a no-op entry — establishes this leader's commit frontier
	// and indirectly commits any stuck previous-term entries (§5.3)
	newIndex := lastIndex + 1
	newEntry := wal.Entry{
		Term:  r.currentTerm,
		Index: newIndex,
		Data:  []byte("no-op"),
	}
	r.log = append(r.log, newEntry)
	if err := r.wal.Append(newEntry); err != nil {
		log.Printf("[node %d] failed to persist no-op entry :%v", r.id, err)
		return
	}

	go r.heartBeatLoop()
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

func (r *Raft) entryAt(index uint64) (wal.Entry, bool) {
	pos := r.toSlicePos(index)
	if pos < 0 || pos >= len(r.log) {
		return wal.Entry{}, false
	}

	return r.log[pos], true
}

func (r *Raft) HandleAppendEntries(args rpc.AppendEntriesArgs) rpc.AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		return rpc.AppendEntriesReply{Term: r.currentTerm, Success: false}
	}

	if args.Term >= r.currentTerm && r.state != Follower {
		r.state = Follower
	}

	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.votedFor = nil
	}
	r.leaderID = args.LeaderID
	// this is a legitimate, current-or-newer leader — reset the timer
	// NOW, regardless of whether the log consistency check below
	// passes. A rejoining node that's still catching up should not
	// keep re-electing itself just because its log isn't caught up yet.
	r.resetElectionTimer()

	// consistency check
	if args.PrevLogIndex > 0 {
		if entry, ok := r.entryAt(args.PrevLogIndex); ok {
			if entry.Term != args.PrevLogTerm {
				return rpc.AppendEntriesReply{Term: r.currentTerm, Success: false}
			}
		}
		// if !ok - entry is compacted away or genuinely missing
		// treated as consistent for now
	}

	// consistency check passed - safe to append new entries
	appendFrom := 0

	for i, newEntry := range args.Entries {
		if entry, ok := r.entryAt(newEntry.Index); ok {
			if entry.Term != newEntry.Term {
				r.log = r.log[:r.toSlicePos(newEntry.Index)]
				if err := r.wal.TruncateAfter(newEntry.Index - 1); err != nil {
					return rpc.AppendEntriesReply{Term: r.currentTerm, Success: false}
				}

				appendFrom = i
				break
			}
			appendFrom = i + 1
		} else {
			appendFrom = i
			break
		}
	}
	entriesToAppend := args.Entries[appendFrom:]

	r.log = append(r.log, entriesToAppend...)
	for _, entry := range entriesToAppend {
		if err := r.wal.Append(entry); err != nil {
			return rpc.AppendEntriesReply{Term: r.currentTerm, Success: false}
		}
	}

	// Update our commit index based on what the leader says is committed, then apply any newly committed address to our own state machine. This is the piece that was missing: followers previously stored entries but never actually applied them locally.
	if args.LeaderCommit > r.commitIndex {
		lastNewIndex, _ := r.lastLogIndexAndTerm()
		if args.LeaderCommit < lastNewIndex {
			r.commitIndex = args.LeaderCommit
		} else {
			r.commitIndex = lastNewIndex
		}
	}

	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		if entry, ok := r.entryAt(r.lastApplied); ok {
			r.stateMachine.Apply(entry)
		}
	}
	r.maybeSnapshot()

	reply := rpc.AppendEntriesReply{Term: r.currentTerm, Success: true}
	return reply

}

// LeaderID return this node's best current knowledge of who the
// cluster leader is. Returns 0 if unknown
func (r *Raft) LeaderID() uint64 {

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.leaderID
}

// isLeader reports whether this node currently believes it is the leader
func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.state == Leader
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

// tryAdvanceCommitIndex checks if a majority of peers have replicated
// a higher index than our current commitIndex, and advances it if so.
// Caller must hold r.mu
func (r *Raft) tryAdvanceCommitIndex() {
	// every matchIndex including the leaders
	var everyMatchIndex []uint64
	leaderMatchIndex, _ := r.lastLogIndexAndTerm()

	for _, matchIdx := range r.matchIndex {
		everyMatchIndex = append(everyMatchIndex, matchIdx)
	}

	everyMatchIndex = append(everyMatchIndex, leaderMatchIndex)

	sort.Slice(everyMatchIndex, func(i, j int) bool {
		return everyMatchIndex[i] > everyMatchIndex[j] // descending order
	})

	majorityIndex := everyMatchIndex[len(everyMatchIndex)/2]

	if majorityIndex == 0 {
		return // nothing to commit yet
	}

	entryAtMajority, ok := r.entryAt(majorityIndex)
	if !ok {
		return // guard against it
	}

	if entryAtMajority.Term != r.currentTerm {
		// this entry is from a PREVIOUS term - cannot commit it directly
		// per 5.4.2 , even though majority has it
		return
	}

	r.commitIndex = majorityIndex

	// apply newly-commited entries to the state machine
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		if entry, ok := r.entryAt(r.lastApplied); ok {
			r.stateMachine.Apply(entry)
		}
	}

	r.maybeSnapshot()
}

// sendSnapshotToPeer loads the current snapshot , encodes it , and sends
// it to the given peer via InstallSnapshot , On success , updates the
// peer's nextIndex / matchIndex to reflect it's now caught up
func (r *Raft) sendSnapshotToPeer(peerID uint64, peerAddr string, term uint64, leaderID uint64) {
	snapshotPath := fmt.Sprintf("node%d.snapshot", r.id)

	snapshot, err := LoadSnapshot(snapshotPath)
	if err != nil {
		log.Printf("[node %d] failed to load snapshot for peer %d:%v", r.id, peerID, err)
		return
	}
	if snapshot == nil {
		return // no snapshot exists yet , nothing to send
	}
	encoded, err := snapshot.Encode()
	if err != nil {
		log.Printf("[node %d] failed to encode the data ", r.id)
		return
	}
	args := rpc.InstallSnapshotArgs{
		Term:              term,
		LeaderID:          leaderID,
		LastIncludedIndex: snapshot.LastIncludedIndex,
		LastIncludedTerm:  snapshot.LastIncludedTerm,
		Data:              encoded}

	reply, err := r.transport.SendInstallSnapshot(peerAddr, args)
	if err != nil {
		log.Printf("SendInstallSnapshot failed !")
		return // missing ! falls through and updates nextIndex/matchIndex even though nothing was actually send successful
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if reply.Term > r.currentTerm {
		// we've been superseded - step down immediately
		r.currentTerm = reply.Term
		r.state = Follower
		r.votedFor = nil
		return
	}

	r.nextIndex[peerID] = snapshot.LastIncludedIndex + 1
	r.matchIndex[peerID] = snapshot.LastIncludedIndex
}
func (r *Raft) heartBeatLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		r.mu.Lock()
		if r.state != Leader {
			r.mu.Unlock()
			return // no longer leader , stop sending heartbeats
		}

		currentTerm := r.currentTerm
		leaderID := r.id

		// snapshot exactly what each peer needs WHIL STILL LOCKED

		peerSnapshots := make(map[uint64]peerData)
		var needsSnapshot []uint64 // peerIDs that need InstallSnapshot instead
		for peerID := range r.peers {
			prevIndexPos := r.toSlicePos(r.nextIndex[peerID])

			if prevIndexPos < 0 {
				// this peer needs entries we've already compacted away
				needsSnapshot = append(needsSnapshot, peerID)
				continue // skip building normal AppendEntries data for this peer
			}

			// existing AppendEntries peerData building continues here,
			// unchanged - prevIndex , prevTerm , entriesToSend , etc
			prevIndex := r.nextIndex[peerID] - 1
			var prevTerm uint64
			if prevIndex > 0 {
				if entry, ok := r.entryAt(prevIndex); ok {
					prevTerm = entry.Term
				}
			}

			// if !ok - already compacted away , prevTerm stays 0
			// which is acceptable for now (same simplification as the consistency check elsewhere)

			peerSnapshots[peerID] = peerData{
				entries:   r.log[r.toSlicePos(r.nextIndex[peerID]):],
				prevIndex: prevIndex,
				prevTerm:  prevTerm,
			}

		}

		r.mu.Unlock()

		for _, peerID := range needsSnapshot {
			peerAddr := r.peers[peerID] // safe to read after unlock ? think about this
			go r.sendSnapshotToPeer(peerID, peerAddr, currentTerm, leaderID)
		}

		for peerID, peerAddr := range r.peers {
			if peerID == r.id {
				continue
			}

			snap := peerSnapshots[peerID]
			go func(peerID uint64, peerAddr string, snap peerData) {

				args := rpc.AppendEntriesArgs{
					Term:         currentTerm,
					LeaderID:     leaderID,
					PrevLogIndex: snap.prevIndex,
					PrevLogTerm:  snap.prevTerm,
					Entries:      snap.entries,
					LeaderCommit: r.commitIndex,
				}
				reply, err := r.transport.SendAppendEntries(peerAddr, args)
				if err != nil {
					return
				}
				// if a peer's term is ahead of ours , we've been
				// superseded - step down immediately
				// (safety rule) same as in requestVotes
				if reply.Term > currentTerm {
					r.mu.Lock()
					r.currentTerm = reply.Term
					r.state = Follower
					r.votedFor = nil
					r.mu.Unlock()
				}
				r.mu.Lock()
				if reply.Success == true {
					if len(snap.entries) > 0 {
						lastSent := snap.entries[len(snap.entries)-1]
						r.matchIndex[peerID] = lastSent.Index
						r.nextIndex[peerID] = lastSent.Index + 1
						r.tryAdvanceCommitIndex() // check  if this update pushed us to majority
					}
					// if snap.entries was empty (pure heartbeat) , nothing changes . already upto date
				} else {
					if r.nextIndex[peerID] > 1 {
						r.nextIndex[peerID]--
					}
				}
				r.mu.Unlock()
			}(peerID, peerAddr, snap)
		}
	}
}

// compactLog discards log entries up through lastIncludedIndex
// since they're now caputred in a snapshot instead
// Caller must hold r.mu
func (r *Raft) compactLog(lastIncludedIndex uint64) {
	if lastIncludedIndex <= r.logOffset {
		return // already compacted at least this far
	}
	if lastIncludedIndex > r.lastApplied {
		// safety rule from etcd: never compact past what's actually
		// been applied to the state machine
		return
	}
	keepFrom := r.toSlicePos(lastIncludedIndex) + 1
	if keepFrom < 0 || keepFrom > len(r.log) {
		return // safety guard
	}
	r.log = r.log[keepFrom:]
	r.logOffset = lastIncludedIndex
}

// Propose accepts a new command from a client. Only valid if this node
// is currently the leader. Returns the index the entry was assigned to
func (r *Raft) Propose(data []byte) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return 0, fmt.Errorf("not the leader")
	}

	lastIndex, _ := r.lastLogIndexAndTerm()
	newIndex := lastIndex + 1
	newEntry := wal.Entry{
		Term:  r.currentTerm,
		Index: newIndex,
		Data:  data,
	}
	r.log = append(r.log, newEntry)
	if err := r.wal.Append(newEntry); err != nil {
		return 0, fmt.Errorf("failed to persist entry: %w", err)
	}

	return newIndex, nil
}

// Restore replaces the state machine's entire contents with the
// given data - used when installing a snapshot from the leader.
func (s *StateMachine) Restore(data map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = data
}

func (r *Raft) HandleInstallSnapshot(args rpc.InstallSnapshotArgs) rpc.InstallSnapshotReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		return rpc.InstallSnapshotReply{Term: r.currentTerm}
	}

	if args.Term >= r.currentTerm && r.state != Follower {
		r.state = Follower
	}

	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.votedFor = nil

	}

	r.resetElectionTimer()

	snapshot, err := DecodeSnapshot(args.Data)
	if err != nil {
		return rpc.InstallSnapshotReply{Term: r.currentTerm}
	}
	snapshotPath := fmt.Sprintf("node%d.snapshot", r.id)
	if err := SaveSnapshot(snapshotPath, snapshot); err != nil {
		return rpc.InstallSnapshotReply{Term: r.currentTerm}
	}

	r.stateMachine.Restore(snapshot.Data)
	r.logOffset = snapshot.LastIncludedIndex
	r.lastApplied = snapshot.LastIncludedIndex
	r.commitIndex = snapshot.LastIncludedIndex

	r.log = nil

	return rpc.InstallSnapshotReply{Term: r.currentTerm}

}

func (r *Raft) Start() {
	// this is where timer begins and the election watching goroutine launches
	r.resetElectionTimer()
	go r.electionLoop()
}
