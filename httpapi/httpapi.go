package httpapi

import (
	"net/http"

	"github.com/ejinbt/jinkv/raft"
)

type Server struct {
	raft *raft.Raft
}

func NewServer(r *raft.Raft) *Server {
	return &Server{raft: r}
}

// handleGet serves a read for the given key from this node's local
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
