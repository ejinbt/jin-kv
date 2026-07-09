package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/ejinbt/jinkv/raft"
	"github.com/ejinbt/jinkv/rpc"
)

type MessageType byte

type Client struct{}

const (
	MsgRequestVoteArgs    MessageType = 1
	MsgRequestVoteReply   MessageType = 2
	MsgAppendEntriesArgs  MessageType = 3
	MsgAppendEntriesReply MessageType = 4
)

func NewClient() *Client {
	return &Client{}
}

// writeMessage writes a framed message: [type][length][payload]
func writeMessage(conn net.Conn, msgType MessageType, payload []byte) error {
	if _, err := conn.Write([]byte{byte(msgType)}); err != nil {
		return fmt.Errorf("failed to write message type: %w", err)
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if _, err := conn.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("failed to write length: %w", err)
	}
	if _, err := conn.Write(payload); err != nil {
		return fmt.Errorf("failed to write payload: %w", err)
	}
	return nil
}

// readMessage reads a framed message: [type][length][payload]
func readMessage(conn net.Conn) (MessageType, []byte, error) {
	var msgTypeBuf [1]byte
	if _, err := io.ReadFull(conn, msgTypeBuf[:]); err != nil {
		return 0, nil, fmt.Errorf("failed to read message type: %w", err)
	}

	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return 0, nil, fmt.Errorf("failed to read length: %w", err)
	}
	msgLen := binary.BigEndian.Uint32(lenBuf[:])

	payload := make([]byte, msgLen)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return 0, nil, fmt.Errorf("failed to read payload: %w", err)
	}

	return MessageType(msgTypeBuf[0]), payload, nil
}

// SendRequestVote dials a peer, sends a RequestVoteArgs, and returns the reply
func (c *Client) SendRequestVote(peerAddr string, args rpc.RequestVoteArgs) (rpc.RequestVoteReply, error) {
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return rpc.RequestVoteReply{}, err
	}
	defer conn.Close()

	encodedArgs, err := rpc.EncodeRequestVoteArgs(args)
	if err != nil {
		return rpc.RequestVoteReply{}, fmt.Errorf("failed to encode args: %w", err)
	}

	if err := writeMessage(conn, MsgRequestVoteArgs, encodedArgs); err != nil {
		return rpc.RequestVoteReply{}, err
	}

	msgType, payload, err := readMessage(conn)
	if err != nil {
		return rpc.RequestVoteReply{}, err
	}
	if msgType != MsgRequestVoteReply {
		return rpc.RequestVoteReply{}, fmt.Errorf("unexpected message type: %d", msgType)
	}

	return rpc.DecodeRequestVoteReply(payload)
}

func (c *Client) SendAppendEntries(peerAddr string, args rpc.AppendEntriesArgs) (rpc.AppendEntriesReply, error) {
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return rpc.AppendEntriesReply{}, err
	}

	defer conn.Close()
	encodedArgs, err := rpc.EncodeAppendEntriesArgs(args)
	if err != nil {
		return rpc.AppendEntriesReply{}, fmt.Errorf("failed to encode args :%w", err)
	}

	if err := writeMessage(conn, MsgAppendEntriesArgs, encodedArgs); err != nil {
		return rpc.AppendEntriesReply{}, err
	}
	msgType, payload, err := readMessage(conn)
	if err != nil {
		return rpc.AppendEntriesReply{}, err
	}
	if msgType != MsgAppendEntriesReply {
		return rpc.AppendEntriesReply{}, fmt.Errorf("unexpected message type: %d", msgType)
	}

	return rpc.DecodeAppendEntriesReply(payload)
}

// StartServer begins listening for incoming RPCs and dispatches them to r (raft)
func StartServer(address string, r *raft.Raft) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen : %w", err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue // log and keep serving other connections
		}
		go handleConnection(conn, r)
	}
}

func handleConnection(conn net.Conn, r *raft.Raft) {
	defer conn.Close()

	msgType, payload, err := readMessage(conn)

	if err != nil {
		return // connection closed or malformed message , just drop it
	}

	switch msgType {
	case MsgRequestVoteArgs:
		args, err := rpc.DecodeRequestVoteArgs(payload)
		if err != nil {
			return
		}

		reply := r.HandleRequestVote(args)
		encodedReply, err := rpc.EncodeRequestVoteReply(reply)
		if err != nil {
			return
		}

		writeMessage(conn, MsgRequestVoteReply, encodedReply)
	case MsgAppendEntriesArgs:
		args, err := rpc.DecodeAppendEntriesArgs(payload)
		if err != nil {
			return
		}

		reply := r.HandleAppendEntries(args)

		encodedReply, err := rpc.EncodeAppendEntriesReply(reply)
		if err != nil {
			return
		}

		writeMessage(conn, MsgAppendEntriesReply, encodedReply)
	default:
		// unknown message type , drop the connection
		return
	}
}
