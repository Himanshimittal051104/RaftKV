package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrFileEngineClosed = errors.New("file engine is closed")
	ErrInvalidWALRecord = errors.New("invalid WAL record")
)

const (
	walOpPut    = "PUT"
	walOpDelete = "DELETE"
)

// walRecord is one operation persisted in the write-ahead log.
// []byte fields are encoded as Base64 by encoding/json.
type walRecord struct {
	Op    string `json:"op"`
	Key   []byte `json:"key"`
	Value []byte `json:"value,omitempty"`
}

// FileEngine is a disk-backed implementation of Engine.
// The WAL is replayed on startup to reconstruct the in-memory map.
type FileEngine struct {
	mu     sync.Mutex
	data   map[string][]byte
	file   *os.File
	writer *bufio.Writer
	closed bool
}

// NewFileEngine opens or creates a WAL at walPath and replays its records.
// A torn/incomplete final line is discarded; malformed complete records return an error.
func NewFileEngine(walPath string) (*FileEngine, error) {
	if walPath == "" {
		return nil, errors.New("WAL path cannot be empty")
	}

	dir := filepath.Dir(walPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create WAL directory: %w", err)
	}

	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	engine := &FileEngine{
		data: make(map[string][]byte),
		file: f,
	}

	if err := engine.recover(); err != nil {
		_ = f.Close()
		return nil, err
	}

	if _, err := f.Seek(0, io.SeekEnd); err != nil { //f.Seek(offset, whence)
		_ = f.Close()
		return nil, fmt.Errorf("seek WAL end: %w", err)
	}
	engine.writer = bufio.NewWriter(f)

	return engine, nil
}

// recover rebuilds the map by replaying newline-delimited WAL records.
// A final line without a newline is treated as an interrupted append and truncated.
func (e *FileEngine) recover() error {
	if _, err := e.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek WAL start: %w", err)
	}

	reader := bufio.NewReader(e.file)
	var validBytes int64

	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 && line[len(line)-1] == '\n' {
			recordBytes := line[:len(line)-1]
			var record walRecord
			if err := json.Unmarshal(recordBytes, &record); err != nil {
				return fmt.Errorf("%w at byte %d: %v", ErrInvalidWALRecord, validBytes, err)
			}
			if err := e.applyRecord(record); err != nil {
				return fmt.Errorf("%w at byte %d: %v", ErrInvalidWALRecord, validBytes, err)
			}
			validBytes += int64(len(line))
		} else if len(line) > 0 {
			// An incomplete last record can be caused by a crash mid-append.
			if err := e.file.Truncate(validBytes); err != nil {
				return fmt.Errorf("truncate incomplete WAL record: %w", err)
			}
			if err := e.file.Sync(); err != nil {
				return fmt.Errorf("sync WAL after truncation: %w", err)
			}
			break
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return fmt.Errorf("read WAL: %w", readErr)
		}
	}

	return nil
}

func (e *FileEngine) applyRecord(record walRecord) error {
	switch record.Op {
	case walOpPut:
		valueCopy := append([]byte(nil), record.Value...)
		e.data[string(record.Key)] = valueCopy
	case walOpDelete:
		delete(e.data, string(record.Key))
	default:
		return fmt.Errorf("unknown operation %q", record.Op)
	}
	return nil
}

// appendRecord writes and syncs a WAL record before the in-memory state is changed.
func (e *FileEngine) appendRecord(record walRecord) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode WAL record: %w", err)
	}
	encoded = append(encoded, '\n')

	if _, err := e.writer.Write(encoded); err != nil {
		// The buffered writer may now contain a partial record. Do not continue
		// using this engine after a write failure.
		e.closed = true
		_ = e.file.Close()
		return fmt.Errorf("write WAL record: %w", err)
	}
	if err := e.writer.Flush(); err != nil {
		e.closed = true
		_ = e.file.Close()
		return fmt.Errorf("flush WAL record: %w", err)
	}
	if err := e.file.Sync(); err != nil {
		e.closed = true
		_ = e.file.Close()
		return fmt.Errorf("sync WAL record: %w", err)
	}
	return nil
}

// Put persists and stores a key-value pair.
func (e *FileEngine) Put(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrFileEngineClosed
	}

	record := walRecord{
		Op:    walOpPut,
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
	}
	if err := e.appendRecord(record); err != nil {
		return err
	}

	e.data[string(key)] = append([]byte(nil), value...)
	return nil
}

// Get retrieves a copy of the value associated with key.
func (e *FileEngine) Get(key []byte) ([]byte, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil, false, ErrFileEngineClosed
	}

	value, ok := e.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

// Delete persists the deletion before removing the key from memory.
func (e *FileEngine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrFileEngineClosed
	}

	record := walRecord{
		Op:  walOpDelete,
		Key: append([]byte(nil), key...),
	}
	if err := e.appendRecord(record); err != nil {
		return err
	}

	delete(e.data, string(key))
	return nil
}

// Close flushes buffered data and closes the WAL. It is safe to call more than once.
func (e *FileEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}

	e.closed = true
	if err := e.writer.Flush(); err != nil {
		_ = e.file.Close()
		return fmt.Errorf("flush WAL on close: %w", err)
	}
	if err := e.file.Sync(); err != nil {
		_ = e.file.Close()
		return fmt.Errorf("sync WAL on close: %w", err)
	}
	if err := e.file.Close(); err != nil {
		return fmt.Errorf("close WAL: %w", err)
	}
	return nil
}
