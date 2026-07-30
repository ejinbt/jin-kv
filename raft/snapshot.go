package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Snapshot struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              map[string]string
}

func (s *Snapshot) Encode() ([]byte, error) {
	var buf bytes.Buffer

	// fixed-size fields first
	if err := binary.Write(&buf, binary.BigEndian, s.LastIncludedIndex); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.BigEndian, s.LastIncludedTerm); err != nil {
		return nil, err
	}

	// variable data (the map) - needs manual length-prefixing

	if err := binary.Write(&buf, binary.BigEndian, uint32(len(s.Data))); err != nil {
		return nil, err
	}

	for key, value := range s.Data {
		keyBytes := []byte(key)
		if err := binary.Write(&buf, binary.BigEndian, uint32(len(keyBytes))); err != nil {
			return nil, err
		}
		buf.Write(keyBytes)

		valBytes := []byte(value)
		if err := binary.Write(&buf, binary.BigEndian, uint32(len(valBytes))); err != nil {
			return nil, err
		}
		buf.Write(valBytes)
	}

	return buf.Bytes(), nil

}
func DecodeSnapshot(data []byte) (*Snapshot, error) {
	reader := bytes.NewReader(data)
	var s Snapshot

	if err := binary.Read(reader, binary.BigEndian, &s.LastIncludedIndex); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.BigEndian, &s.LastIncludedTerm); err != nil {
		return nil, err
	}

	var count uint32
	if err := binary.Read(reader, binary.BigEndian, &count); err != nil {
		return nil, err
	}

	s.Data = make(map[string]string)
	for i := uint32(0); i < count; i++ {
		var keyLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("failed to read key length: %w", err)
		}
		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read key: %w", err)
		}
		key := string(keyBytes)

		var valLen uint32
		if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
			return nil, fmt.Errorf("failed to read value length: %w", err)
		}
		valBytes := make([]byte, valLen)
		if _, err := io.ReadFull(reader, valBytes); err != nil {
			return nil, fmt.Errorf("failed to read value: %w", err)
		}
		val := string(valBytes)

		s.Data[key] = val

	}
	return &s, nil
}
