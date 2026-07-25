package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

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
	sm := raft.NewStateMachine()
	r := raft.NewRaft(*myID, peers, w, t, sm)

	r.Start() // strats the election timer + loop

	go func() {
		scanner := bufio.NewScanner(os.Stdin)

		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[debug] recieved line : %q\n", line)
			if line == "" {
				continue
			}
			if strings.HasPrefix(line, "get ") {
				key := strings.TrimPrefix(line, "get ")
				fmt.Printf("[debug] parsed key: %q\n", key) // Temporary
				val, ok := r.Get(key)
				fmt.Printf("[debug] Get returned : val=%q ok=%v\n", val, ok) // Temporary
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

	// this blocks forever , serving incoming RPCs
	if err := transport.StartServer(*myAddr, r); err != nil {
		log.Fatalf("server failed : %v", err)
	}
}
