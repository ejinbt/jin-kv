package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ejinbt/jinkv/wal"
)

func main() {
	// open the WAL
	w, err := wal.Open("test.wal")
	if err != nil {
		log.Fatalf("failed to open WAL : %v", err)
	}

	defer w.Close()

	// check if we are recovering or writing fresh
	entries, err := w.ReadAll()
	if err != nil {
		log.Fatalf("failed to read WAL : %v", err)
	}

	if len(entries) > 0 {
		// RECOVERY PATH
		fmt.Printf("recovered %d entries:\n", len(entries))
		for _, e := range entries {
			term := e.Term
			index := e.Index
			data := e.Data
			fmt.Printf("term=%d index=%d data=%s\n", term, index, data)
		}
		fmt.Println("recovery successful")
		os.Exit(0)
	}
	// FRESH WRITE PATH
	appendEntry := func(e wal.Entry) {
		if err := w.Append(e); err != nil {
			log.Fatalf("failed to append entry %d: %v", e.Index, err)
		}
	}

	appendEntry(wal.Entry{Term: 1, Index: 1, Data: []byte("set x=1")})
	appendEntry(wal.Entry{Term: 1, Index: 2, Data: []byte("set y=2")})
	appendEntry(wal.Entry{Term: 1, Index: 3, Data: []byte("set z=3")})
	appendEntry(wal.Entry{Term: 2, Index: 4, Data: []byte("del x")})
	appendEntry(wal.Entry{Term: 2, Index: 5, Data: []byte("set x=99")})

	fmt.Printf("wrote 5 entries , now run kill -9 on this process")
	fmt.Printf("my PID is %d\n", os.Getpid())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	fmt.Println("waiting - run kill -9 in another terminal")
	<-sig
}
