package main

import (
	"log"

	"flag"

	"github.com/ejinbt/jinkv/raft"
	"github.com/ejinbt/jinkv/transport"
	"github.com/ejinbt/jinkv/wal"
)

func main() {
	myID := flag.Uint64("id", 1, "this node's ID")
	myAddr := flag.String("addr", ":8080", "this node's address")
	flag.Parse()
	peers := map[uint64]string{
		2: "localhost:8081",
		3: "localhost:8082",
	}

	w, err := wal.Open("node1.wal")
	if err != nil {
		log.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	t := transport.NewClient()
	r := raft.NewRaft(*myID, peers, w, t)

	r.Start() // strats the election timer + loop

	// this blocks forever , serving incoming RPCs
	if err := transport.StartServer(*myAddr, r); err != nil {
		log.Fatalf("server failed : %v", err)
	}
}
