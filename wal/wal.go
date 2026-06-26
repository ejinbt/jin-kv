package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Entry represents a single WAL record
type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

// WAL is an append-only write-ahead log
type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

// Open opens or creates a WAL at the given path
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f, path: path}, nil
}

// encode serialize the entry into bits
func encode(entry Entry) []byte {
	// 8 bytes for term + 8 byte for index + 4 bytes for data length + data
	buf := make([]byte, 8+8+4+len(entry.Data))
	binary.BigEndian.PutUint64(buf[0:8], entry.Term)
	binary.BigEndian.PutUint64(buf[8:16], entry.Index)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(entry.Data)))

	copy(buf[20:], entry.Data)
	return buf
}

// decode deserialized bytes back into an Entry
func decode(buf []byte) (Entry, error) {
	if len(buf) < 20 {
		return Entry{}, fmt.Errorf("buffer too small : %d bytes", len(buf))
	}

	var entry Entry
	entry.Term = binary.BigEndian.Uint64(buf[0:8])
	entry.Index = binary.BigEndian.Uint64(buf[8:16])
	dataLen := binary.BigEndian.Uint32(buf[16:20])

	if len(buf) < 20+int(dataLen) {
		return Entry{}, fmt.Errorf("buffer truncated : expected %d data bytes", dataLen)
	}

	entry.Data = make([]byte, dataLen)
	copy(entry.Data, buf[20:20+dataLen])

	return entry, nil

}

// Append writes a single entry durably to the WAL
// It must fsync before returning
func (w *WAL) Append(entry Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// TODO:
	// 1. encode entry to bytes
	encoded := encode(entry)
	// 2. compute checksum
	crc := crc32.ChecksumIEEE(encoded)
	// 3. write checksum + encoded bytes
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], crc)
	if _, err := w.file.Write(buf[:]); err != nil {
		return fmt.Errorf("failed to write checksum : %w", err)
	}

	if _, err := w.file.Write(encoded); err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}
	// 4. fdatasync

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("fsync failed: %w", err)
	}
	// 5. return error if any step fails
	return nil
}

// ReadAll replays the entire WAL from the beginning
// Stops at first corrupt or partial entry
func (w *WAL) ReadAll() ([]Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// TODO:
	// seek to beginning of file
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek failed : %w", err)
	}
	// 2. read entries in a loop
	var entries []Entry
	reader := bufio.NewReader(w.file)

	for {
		// read checksum
		var crcBuf [4]byte
		_, err := io.ReadFull(reader, crcBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// clean end of file -- we 're done
			break
		}

		if err != nil {
			// partial read - corrupted record
			return nil, fmt.Errorf("failed to read checksum : %w", err)
		}
		storedCRC := binary.BigEndian.Uint32(crcBuf[:])

		var termBuf [8]byte
		_, err = io.ReadFull(reader, termBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// clean end of file -- we 're done
			break
		}

		if err != nil {
			// partial read - corrupted record
			return nil, fmt.Errorf("failed to read term : %w", err)
		}

		storedTerm := binary.BigEndian.Uint64(termBuf[:])

		var indexBuf [8]byte
		_, err = io.ReadFull(reader, indexBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("failed to read index : %w", err)
		}

		storedIndex := binary.BigEndian.Uint64(indexBuf[:])

		var dataLenBuf [4]byte
		_, err = io.ReadFull(reader, dataLenBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("failed to read data length : %w", err)
		}

		storedDataLen := binary.BigEndian.Uint32(dataLenBuf[:])

		data := make([]byte, storedDataLen)
		_, err = io.ReadFull(reader, data)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read data : %w", err)
		}

		// reconstruct what encode() would have produced
		encoded := encode(Entry{Term: storedTerm, Index: storedIndex, Data: data})

		// recompute checksum and compare
		if crc32.ChecksumIEEE(encoded) != storedCRC {
			// checksum mismatch - corrupted record , stop here
			break
		}

		// checksum good - this entry is valid
		entries = append(entries, Entry{
			Term:  storedTerm,
			Index: storedIndex,
			Data:  data,
		})
	}
	return entries, nil
}

// TruncateAfter removes all entries after the given index
// Used when a follower's log conflicts with the leader's
func (w *WAL) TruncateAfter(index uint64) error {
	// TODO:
	// 1. read all entries
	// 2. find the offset of the first entry with Index > index
	// 3. truncate file at that offset
	// 4. fdatasync
	return nil
}

// Close cleanly shuts down the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// checksum computes CRC32 of the given bytes
func checksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
