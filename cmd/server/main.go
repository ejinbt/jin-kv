package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"flag"

	"github.com/ejinbt/jinkv/httpapi"
	"github.com/ejinbt/jinkv/raft"
	"github.com/ejinbt/jinkv/transport"
	"github.com/ejinbt/jinkv/wal"
)

func main() {
	myID := flag.Uint64("id", 1, "this node's ID")
	myAddr := flag.String("addr", ":8080", "this node's address")
	httpAddr := flag.String("httpaddr", ":9080", "this node's client-facing HTTP address")
	flag.Parse()
	peers := map[uint64]string{
		1: "localhost:8080",
		2: "localhost:8081",
		3: "localhost:8082",
	}

	w, err := wal.Open(fmt.Sprintf("node%d.wal", *myID))
	if err != nil {
		log.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	t := transport.NewClient()
	sm := raft.NewStateMachine()
	r, err := raft.NewRaft(*myID, peers, w, t, sm)
	if err != nil {
		log.Fatalf("failed to create raft node:%v", err)
	}
	r.Start() // strats the election timer + loop

	go func() {
		scanner := bufio.NewScanner(os.Stdin)

		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			if strings.HasPrefix(line, "get ") {
				key := strings.TrimPrefix(line, "get ")
				val, ok := r.Get(key)
				if ok {
					fmt.Printf("%s = %s\n", key, val)
				} else {
					fmt.Printf("%s not found\n", key)
				}
				continue
			}
			index, err := r.Propose([]byte(line))
			if err != nil {
				fmt.Printf("propose failed : %v\n", err)
				continue
			}
			fmt.Printf("proposed at index %d\n", index)
		}
	}()
	httpServer := httpapi.NewServer(r)
	http.HandleFunc("/get", httpServer.HandleGet)
	http.HandleFunc("/set", httpServer.HandleSet)
	go http.ListenAndServe(*httpAddr, nil)
	// this blocks forever , serving incoming RPCs
	if err := transport.StartServer(*myAddr, r); err != nil {
		log.Fatalf("server failed : %v", err)
	}

}
