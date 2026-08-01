package raft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
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

// SaveSnapshot writes a snapshot to disk at the given path

func SaveSnapshot(path string, s *Snapshot) error {
	encoded, err := s.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode : %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)

	if err != nil {

		return fmt.Errorf("failed to open snapshot file :%w", err)
	}
	defer f.Close()

	if _, err := f.Write(encoded); err != nil {
		return fmt.Errorf("failed to write snapshot : %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to fsync snapshot : %w", err)
	}
	return nil
}

func LoadSnapshot(path string) (*Snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // no snapshot yet
		}
		return nil, fmt.Errorf("failed to read snapshot file :%w", err)
	}

	s, err := DecodeSnapshot(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode snapshot : %w", err)
	}

	return s, nil
}
