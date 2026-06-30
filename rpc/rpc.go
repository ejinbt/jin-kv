package rpc

import (
	"bytes"
	"encoding/binary"

	"github.com/ejinbt/jinkv/wal"
)

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

func EncodeRequestVoteArgs(args RequestVoteArgs) ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, args); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DecodeRequestVoteArgs(data []byte) (RequestVoteArgs, error) {
	var args RequestVoteArgs
	reader := bytes.NewReader(data)
	if err := binary.Read(reader, binary.BigEndian, &args); err != nil {
		return RequestVoteArgs{}, err
	}
	return args, nil
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

func EncodeRequestVoteReply(args RequestVoteReply) ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, args); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DecodeRequestVoteReply(data []byte) (RequestVoteReply, error) {
	var args RequestVoteReply
	reader := bytes.NewReader(data)
	if err := binary.Read(reader, binary.BigEndian, &args); err != nil {
		return RequestVoteReply{}, err
	}
	return args, nil
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []wal.Entry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}
