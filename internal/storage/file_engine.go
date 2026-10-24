package storage

import (
	"RaftKV/internal/apperrors"
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	walOpPut    = "SET"
	walOpDelete = "DELETE"
	walOpNoop   = "NOOP"
)

// walRecord is one operation persisted in the write-ahead log.
// []byte fields are encoded as Base64 by encoding/json.
type walRecord struct {
	Index int    `json:"index"`
	Op    string `json:"op"`
	Key   []byte `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`
}

// FileEngine is a disk-backed implementation of Engine.
// The WAL is replayed on startup to reconstruct the in-memory map.
type FileEngine struct {
	mu          sync.Mutex
	path        string
	data        map[string][]byte
	file        *os.File
	writer      *bufio.Writer
	closed      bool
	failed      bool
	lastApplied int
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
		path: walPath,
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
				return fmt.Errorf("%w at byte %d: %v", apperrors.ErrInvalidRecord, validBytes, err)
			}
			if err := e.applyRecord(record); err != nil {
				return fmt.Errorf("%w at byte %d: %v", apperrors.ErrInvalidRecord, validBytes, err)
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
	if record.Index != e.lastApplied+1 {
		return fmt.Errorf(
			"unexpected WAL index %d; expected %d",
			record.Index, e.lastApplied+1,
		)
	}
	switch record.Op {
	case walOpNoop:
	case walOpPut:
		valueCopy := append([]byte(nil), record.Value...)
		e.data[string(record.Key)] = valueCopy
	case walOpDelete:
		delete(e.data, string(record.Key))
	default:
		return fmt.Errorf("unknown operation %q", record.Op)
	}
	e.lastApplied = record.Index
	return nil
}
func (e *FileEngine) markFailed() {
	e.failed = true

	if e.file != nil {
		_ = e.file.Close()
	}

	e.file = nil
	e.writer = nil
}
func (e *FileEngine) checkUsable() error {
	if e.closed {
		return apperrors.ErrEngineClosed
	}
	if e.failed {
		return apperrors.ErrEngineFailed
	}
	return nil
}

// appendRecord writes and syncs a WAL record before the in-memory state is changed.
func (e *FileEngine) appendRecord(record walRecord) error {
	if record.Index != e.lastApplied+1 {
		return fmt.Errorf(
			"unexpected record index %d; expected %d",
			record.Index, e.lastApplied+1,
		)
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode WAL record: %w", err)
	}
	encoded = append(encoded, '\n')

	if _, err := e.writer.Write(encoded); err != nil {
		// The buffered writer may now contain a partial record. Do not continue
		// using this engine after a write failure.
		e.markFailed()
		return fmt.Errorf("%w: write WAL record: %v", apperrors.ErrEngineFailed, err)
	}
	if err := e.writer.Flush(); err != nil {
		e.markFailed()
		return fmt.Errorf("%w: flush WAL record: %v", apperrors.ErrEngineFailed, err)
	}
	if err := e.file.Sync(); err != nil {
		e.markFailed()
		return fmt.Errorf("%w: sync WAL record: %v", apperrors.ErrEngineFailed, err)
	}
	e.lastApplied = record.Index
	return nil
}

func (e *FileEngine) Noop(index int) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.checkUsable(); err != nil {
		return err
	}

	record := walRecord{
		Index: index,
		Op:    walOpNoop,
	}
	if err := e.appendRecord(record); err != nil {
		return err
	}

	return nil
}

// Put persists and stores a key-value pair.
func (e *FileEngine) Put(index int, key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.checkUsable(); err != nil {
		return err
	}

	record := walRecord{
		Index: index,
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

	if err := e.checkUsable(); err != nil {
		return nil, false, err
	}

	value, ok := e.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

// Delete persists the deletion before removing the key from memory.
func (e *FileEngine) Delete(index int, key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.checkUsable(); err != nil {
		return err
	}

	record := walRecord{
		Index: index,
		Op:    walOpDelete,
		Key:   append([]byte(nil), key...),
	}
	if err := e.appendRecord(record); err != nil {
		return err
	}

	delete(e.data, string(key))
	return nil
}

func (e *FileEngine) Open() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return apperrors.ErrEngineClosed
	}
	if !e.failed {
		return nil
	}

	f, err := os.OpenFile(e.path, os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("%w: reopen WAL: %v", apperrors.ErrEngineFailed, err)
	}

	e.file = f
	e.data = make(map[string][]byte)
	e.lastApplied = 0

	if err := e.recover(); err != nil {
		_ = f.Close()
		e.file = nil
		e.writer = nil
		e.data = make(map[string][]byte)
		e.lastApplied = 0
		return fmt.Errorf("%w: recover WAL: %v", apperrors.ErrEngineFailed, err)
	}

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		e.file = nil
		e.writer = nil
		e.data = make(map[string][]byte)
		e.lastApplied = 0
		return fmt.Errorf("%w: seek WAL end: %v", apperrors.ErrEngineFailed, err)
	}

	e.writer = bufio.NewWriter(f)
	e.failed = false
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

	if e.failed {
		return nil
	}

	if e.writer != nil {
		if err := e.writer.Flush(); err != nil {
			if e.file != nil {
				_ = e.file.Close()
			}
			e.file = nil
			e.writer = nil
			return fmt.Errorf("flush WAL on close: %w", err)
		}
	}

	if e.file != nil {
		if err := e.file.Sync(); err != nil {
			_ = e.file.Close()
			e.file = nil
			e.writer = nil
			return fmt.Errorf("sync WAL on close: %w", err)
		}

		if err := e.file.Close(); err != nil {
			e.file = nil
			e.writer = nil
			return fmt.Errorf("close WAL: %w", err)
		}
	}

	e.file = nil
	e.writer = nil
	return nil
}

func (e *FileEngine) LastApplied() (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.checkUsable(); err != nil {
		return 0, err
	}

	return e.lastApplied, nil

}

func (e *FileEngine) IsClosed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closed
}

func (e *FileEngine) IsFailed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.failed
}
