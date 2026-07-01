package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/ejinbt/jinkv/rpc"
)

type MessageType byte

const (
	MsgRequestVoteArgs    MessageType = 1
	MsgRequestVoteReply   MessageType = 2
	MsgAppendEntriesArgs  MessageType = 3
	MsgAppendEntriesReply MessageType = 4
)

// SendRequestVote dials a peer, sends a RequestVoteArgs, and returns the reply
func SendRequestVote(peerAddr string, args rpc.RequestVoteArgs) (rpc.RequestVoteReply, error) {
	// TODO:
	// 1. dial the peer
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		return rpc.RequestVoteReply{}, err
	}

	defer conn.Close()
	// 2. encode args
	encodedArgs, err := rpc.EncodeRequestVoteArgs(args)
	if err != nil {

		return rpc.RequestVoteReply{}, fmt.Errorf("failed to encode args : %w ", err)
	}
	// 3. write messageType byte
	if _, err := conn.Write([]byte{byte(MsgRequestVoteArgs)}); err != nil {
		return rpc.RequestVoteReply{}, fmt.Errorf("failed to write message type : %w", err)
	}
	// 4. write length-prefixed payload
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(encodedArgs)))
	if _, err := conn.Write(lenBuf[:]); err != nil {
		return rpc.RequestVoteReply{}, fmt.Errorf("failed to write length: %w", err)
	}

	if _, err := conn.Write(encodedArgs); err != nil {
		return rpc.RequestVoteReply{}, fmt.Errorf("failed to write payload: %w", err)
	}

	// 5. read back the reply (messageType, length, payload)
	var msgTypeBuf [1]byte
	if msgTypeBuf[0] != byte(MsgRequestVoteReply) {
		return rpc.RequestVoteReply{}, fmt.Errorf("unexpected message type: %d", msgTypeBuf[0])
	}
	if _, err := io.ReadFull(conn, msgTypeBuf[:]); err != nil {
		return rpc.RequestVoteReply{}, fmt.Errorf("failed to read message type : %w", err)
	}
	var replyLenBuf [4]byte
	if _, err := io.ReadFull(conn, replyLenBuf[:]); err != nil {
		return rpc.RequestVoteReply{}, fmt.Errorf("failed to read reply length : %w", err)
	}

	replyLen := binary.BigEndian.Uint32(replyLenBuf[:])

	replyData := make([]byte, replyLen)
	if _, err := io.ReadFull(conn, replyData); err != nil {
		return rpc.RequestVoteReply{}, fmt.Errorf("failed to read reply payload : %w", err)
	}
	// 6. decode reply
	decodedReply, err := rpc.DecodeRequestVoteReply(replyData)

	// 7. return it
	return decodedReply, err
}
