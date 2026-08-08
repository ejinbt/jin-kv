package rpc

import (
	"bytes"
	"encoding/binary"
	"io"

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

func EncodeAppendEntriesArgs(args AppendEntriesArgs) ([]byte, error) {
	var buf bytes.Buffer

	// fixed-size fields first
	if err := binary.Write(&buf, binary.BigEndian, args.Term); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.BigEndian, args.LeaderID); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, args.PrevLogIndex); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, args.PrevLogTerm); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, args.LeaderCommit); err != nil {
		return nil, err
	}

	// entry count , then each entry length-prefixed
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(args.Entries))); err != nil {
		return nil, err
	}

	for _, entry := range args.Entries {
		encoded := wal.EncodeEntry(entry)
		if err := binary.Write(&buf, binary.BigEndian, uint32(len(encoded))); err != nil {
			return nil, err
		}

		buf.Write(encoded)
	}

	return buf.Bytes(), nil
}

func DecodeAppendEntriesArgs(data []byte) (AppendEntriesArgs, error) {
	var args AppendEntriesArgs
	reader := bytes.NewReader(data)

	if err := binary.Read(reader, binary.BigEndian, &args.Term); err != nil {
		return AppendEntriesArgs{}, err
	}
	if err := binary.Read(reader, binary.BigEndian, &args.LeaderID); err != nil {
		return AppendEntriesArgs{}, err
	}
	if err := binary.Read(reader, binary.BigEndian, &args.PrevLogIndex); err != nil {
		return AppendEntriesArgs{}, err
	}
	if err := binary.Read(reader, binary.BigEndian, &args.PrevLogTerm); err != nil {
		return AppendEntriesArgs{}, err
	}
	if err := binary.Read(reader, binary.BigEndian, &args.LeaderCommit); err != nil {
		return AppendEntriesArgs{}, err
	}

	var entryCount uint32
	if err := binary.Read(reader, binary.BigEndian, &entryCount); err != nil {
		return AppendEntriesArgs{}, err
	}

	for i := uint32(0); i < entryCount; i++ {
		var entryLen uint32
		if err := binary.Read(reader, binary.BigEndian, &entryLen); err != nil {
			return AppendEntriesArgs{}, err
		}
		entryBuf := make([]byte, entryLen)
		if _, err := reader.Read(entryBuf); err != nil {
			return AppendEntriesArgs{}, err
		}
		entry, err := wal.DecodeEntry(entryBuf)
		if err != nil {
			return AppendEntriesArgs{}, err
		}
		args.Entries = append(args.Entries, entry)
	}

	return args, nil
}

func EncodeAppendEntriesReply(reply AppendEntriesReply) ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, reply); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeAppendEntriesReply(data []byte) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	reader := bytes.NewReader(data)
	if err := binary.Read(reader, binary.BigEndian, &reply); err != nil {
		return AppendEntriesReply{}, err
	}
	return reply, nil
}

type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          uint64
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte // the encoded snapshot itself
}

type InstallSnapshotReply struct {
	Term uint64
}

func EncodeInstallSnapshotArgs(args InstallSnapshotArgs) ([]byte, error) {
	var buf bytes.Buffer

	// fixed-size fields first
	if err := binary.Write(&buf, binary.BigEndian, args.Term); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.BigEndian, args.LeaderID); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.BigEndian, args.LastIncludedIndex); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.BigEndian, args.LastIncludedTerm); err != nil {
		return nil, err
	}

	// Data is already a complete blob of bytes - write its length once
	// then the whole blob in a single write , no loop needed
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(args.Data))); err != nil {
		return nil, err
	}

	buf.Write(args.Data)

	return buf.Bytes(), nil
}

func DecodeInstallSnapshotArgs(data []byte) (InstallSnapshotArgs, error) {
	var args InstallSnapshotArgs
	reader := bytes.NewReader(data)

	if err := binary.Read(reader, binary.BigEndian, &args.Term); err != nil {
		return InstallSnapshotArgs{}, err
	}
	if err := binary.Read(reader, binary.BigEndian, &args.LeaderID); err != nil {
		return InstallSnapshotArgs{}, err
	}
	if err := binary.Read(reader, binary.BigEndian, &args.LastIncludedIndex); err != nil {
		return InstallSnapshotArgs{}, err
	}
	if err := binary.Read(reader, binary.BigEndian, &args.LastIncludedTerm); err != nil {
		return InstallSnapshotArgs{}, err
	}

	var dataLen uint32
	if err := binary.Read(reader, binary.BigEndian, &dataLen); err != nil {
		return InstallSnapshotArgs{}, err
	}

	dataBytes := make([]byte, dataLen)

	if _, err := io.ReadFull(reader, dataBytes); err != nil {

		return InstallSnapshotArgs{}, err
	}

	args.Data = dataBytes

	return args, nil
}

func EncodeInstallSnapshotReply(reply InstallSnapshotReply) ([]byte, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, reply); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeInstallSnapshotReply(data []byte) (InstallSnapshotReply, error) {
	var reply InstallSnapshotReply
	reader := bytes.NewReader(data)

	if err := binary.Read(reader, binary.BigEndian, &reply); err != nil {
		return AppendEntriesReply{}, err
	}
	return reply, nil
}
