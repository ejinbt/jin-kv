package httpapi

import (
	"fmt"
	"net/http"

	"github.com/ejinbt/jinkv/raft"
)

type Server struct {
	raft *raft.Raft
}

func NewServer(r *raft.Raft) *Server {
	return &Server{raft: r}
}

// HandleGet serves a read for the given key from this node's local
// state machine. NOTE: this is not linearizable — it does not confirm
// leadership or commit-frontier before answering, per §8 of the Raft
// paper. Fine for casual/testing use; a real production read path
// would need those safeguards. See raft.Raft.Get's own doc comment.
func (s *Server) HandleGet(w http.ResponseWriter, req *http.Request) {
	key := req.URL.Query().Get("key")
	value, ok := s.raft.Get(key)
	if ok {
		w.Write([]byte(value))
	} else {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	}
}

// HandleSet writes a key=value pair via Propose. if this node is not
// the current leader , it responds with the client leader's node ID instead
// of performing the write , so the client can retry against the
// correct node
func (s *Server) HandleSet(w http.ResponseWriter, req *http.Request) {

	if !s.raft.IsLeader() {
		leaderID := s.raft.LeaderID()
		w.WriteHeader(http.StatusMisdirectedRequest)
		fmt.Fprintf(w, "not the leader; try node %d", leaderID)
		return
	}

	key := req.URL.Query().Get("key")
	value := req.URL.Query().Get("value")

	command := fmt.Sprintf("set %s=%s", key, value)
	index, err := s.raft.Propose([]byte(command))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "propose failed: %v", err)
		return
	}

	fmt.Fprintf(w, "proposed at index %d", index)
}
