package storage

import (
	"RaftKV/internal/apperrors"
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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
	mu                  sync.Mutex
	path                string
	data                map[string][]byte
	file                *os.File
	writer              *bufio.Writer
	closed              bool
	failed              bool
	lastApplied         int
	snapshotLastApplied int
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

	if err := recoverWALBackup(walPath); err != nil {
		return nil, fmt.Errorf("recover WAL backup: %w", err)
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

	snapshot, err := engine.ReadSnapshot()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	if snapshot != nil {
		engine.data = snapshot.Data
		engine.lastApplied = snapshot.LastApplied
		engine.snapshotLastApplied = snapshot.LastApplied
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
			validBytes += int64(len(line))
			if record.Index <= e.snapshotLastApplied {
				continue
			}
			if err := e.applyRecord(record); err != nil {
				return fmt.Errorf("%w at byte %d: %v", apperrors.ErrInvalidRecord, validBytes, err)
			}
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
	if err := e.applyRecord(record); err != nil {
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
	if err := e.applyRecord(record); err != nil {
		return err
	}
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
	if err := e.applyRecord(record); err != nil {
		return err
	}
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

	if err := recoverWALBackup(e.path); err != nil {
		return fmt.Errorf("recover WAL backup: %w", err)
	}

	f, err := os.OpenFile(e.path, os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("%w: reopen WAL: %v",
			apperrors.ErrEngineFailed, err)
	}

	e.file = f
	e.data = make(map[string][]byte)
	e.lastApplied = 0

	snapshot, err := e.readSnapshotLocked()
	if err != nil {
		_ = f.Close()
		e.file = nil
		e.writer = nil
		return fmt.Errorf("%w: read snapshot: %v", apperrors.ErrEngineFailed, err)
	}

	if snapshot != nil {
		e.data = snapshot.Data
		e.lastApplied = snapshot.LastApplied
		e.snapshotLastApplied = snapshot.LastApplied
	}

	if err := e.recover(); err != nil {
		_ = f.Close()
		e.file = nil
		e.writer = nil
		e.data = make(map[string][]byte)
		e.lastApplied = 0
		return fmt.Errorf("%w: recover WAL: %v",
			apperrors.ErrEngineFailed, err)
	}

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		e.file = nil
		e.writer = nil
		e.data = make(map[string][]byte)
		e.lastApplied = 0
		return fmt.Errorf("%w: seek WAL end: %v",
			apperrors.ErrEngineFailed, err)
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

func (e *FileEngine) snapshotPath() string {
	return filepath.Join(filepath.Dir(e.path), "snapshot.json")
}

func (e *FileEngine) SaveSnapshot() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.checkUsable(); err != nil {
		return err
	}

	snapshot := EngineSnapshot{
		LastApplied: e.lastApplied,
		Data:        make(map[string][]byte, len(e.data)),
	}

	for key, value := range e.data {
		snapshot.Data[key] = append([]byte(nil), value...)
	}

	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal snapshot: %w", err)
	}

	path := e.snapshotPath()
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".snapshot-*")
	if err != nil {
		return fmt.Errorf("create temporary snapshot: %w", err)
	}

	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if _, err := tmp.Write(encoded); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write snapshot: %w", err)
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync snapshot: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close snapshot: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace snapshot: %w", err)
	}
	e.snapshotLastApplied = e.lastApplied
	return nil
}

func (e *FileEngine) ReadSnapshot() (*EngineSnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.checkUsable(); err != nil {
		return nil, err
	}

	return e.readSnapshotLocked()
}
func (e *FileEngine) readSnapshotLocked() (*EngineSnapshot, error) {
	encoded, err := os.ReadFile(e.snapshotPath())
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}

	var snapshot EngineSnapshot
	if err := json.Unmarshal(encoded, &snapshot); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}

	if snapshot.LastApplied < 0 || snapshot.Data == nil {
		return nil, fmt.Errorf("invalid snapshot contents")
	}

	copied := &EngineSnapshot{
		LastApplied: snapshot.LastApplied,
		Data:        make(map[string][]byte, len(snapshot.Data)),
	}

	for key, value := range snapshot.Data {
		copied.Data[key] = append([]byte(nil), value...)
	}

	return copied, nil
}
func (e *FileEngine) ShouldSnapshot() bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.lastApplied-e.snapshotLastApplied >= 100
}
func (e *FileEngine) MaybeSaveSnapshot() {
	if !e.ShouldSnapshot() {
		return
	}

	if err := e.SaveSnapshot(); err != nil {
		// Snapshot failure should not undo an already-applied command.
		// Log the error so the failure is visible.
		log.Printf("failed to save snapshot: %v", err)
		return
	}

	if err := e.CompactWAL(); err != nil {
		log.Printf("failed to compact WAL: %v", err)
	}
}

func (e *FileEngine) CompactWAL() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.checkUsable(); err != nil {
		return err
	}

	if e.writer != nil {
		if err := e.writer.Flush(); err != nil {
			e.markFailed()
			return fmt.Errorf("flush WAL before compaction: %w", err)
		}
	}
	if e.file != nil {
		if err := e.file.Sync(); err != nil {
			e.markFailed()
			return fmt.Errorf("sync WAL before compaction: %w", err)
		}
	}

	dir := filepath.Dir(e.path)
	tmp, err := os.CreateTemp(dir, ".wal-compact-*")
	if err != nil {
		return fmt.Errorf("create temporary WAL: %w", err)
	}

	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	cleanup := func() {
		_ = tmp.Close()
	}

	src, err := os.Open(e.path)
	if err != nil {
		cleanup()
		return fmt.Errorf("open WAL for compaction: %w", err)
	}

	reader := bufio.NewReader(src)
	writer := bufio.NewWriter(tmp)

	for {
		line, readErr := reader.ReadBytes('\n')

		if len(line) > 0 {
			if line[len(line)-1] != '\n' {
				_ = src.Close()
				cleanup()
				return fmt.Errorf("incomplete WAL record during compaction")
			}

			var record walRecord
			if err := json.Unmarshal(line[:len(line)-1], &record); err != nil {
				_ = src.Close()
				cleanup()
				return fmt.Errorf("decode WAL record during compaction: %w", err)
			}

			if record.Index > e.snapshotLastApplied {
				if _, err := writer.Write(line); err != nil {
					_ = src.Close()
					cleanup()
					return fmt.Errorf("write compacted WAL: %w", err)
				}
			}
		}

		if readErr != nil {
			if readErr != io.EOF {
				_ = src.Close()
				cleanup()
				return fmt.Errorf("read WAL during compaction: %w", readErr)
			}
			break
		}
	}

	if err := src.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close source WAL: %w", err)
	}

	if err := writer.Flush(); err != nil {
		cleanup()
		return fmt.Errorf("flush compacted WAL: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("sync compacted WAL: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close compacted WAL: %w", err)
	}

	// Replacement and rollback logic is platform-sensitive.
	// Use replaceWALAtomically below rather than removing the
	// original WAL before the replacement is safely established.
	if err := e.replaceWALAtomically(tmpPath); err != nil {
		return err
	}

	return nil
}

func (e *FileEngine) replaceWALAtomically(tmpPath string) error {
	backupPath := e.path + ".bak"

	// Flush and close the currently active WAL.
	if e.writer != nil {
		if err := e.writer.Flush(); err != nil {
			return fmt.Errorf("flush current WAL: %w", err)
		}
	}
	if e.file != nil {
		if err := e.file.Sync(); err != nil {
			return fmt.Errorf("sync current WAL: %w", err)
		}
		if err := e.file.Close(); err != nil {
			return fmt.Errorf("close current WAL: %w", err)
		}
		e.file = nil
		e.writer = nil
	}

	// Remove an old backup before creating a fresh one.
	if err := os.Remove(backupPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove old WAL backup: %w", err)
	}

	// Preserve the original WAL so we can roll back.
	if err := os.Rename(e.path, backupPath); err != nil {
		return fmt.Errorf("back up current WAL: %w", err)
	}

	// Install the compacted WAL.

	// Install the compacted WAL.
	if err := os.Rename(tmpPath, e.path); err != nil {
		if rollbackErr := os.Rename(backupPath, e.path); rollbackErr != nil {
			e.markFailed()
			return fmt.Errorf(
				"install compacted WAL: %v; rollback failed: %w",
				err, rollbackErr,
			)
		}

		// Rollback succeeded. Reopen the restored original WAL.
		f, reopenErr := os.OpenFile(
			e.path,
			os.O_RDWR|os.O_APPEND,
			0o600,
		)
		if reopenErr != nil {
			e.markFailed()
			return fmt.Errorf(
				"install compacted WAL: %v; reopen restored WAL failed: %w",
				err, reopenErr,
			)
		}

		e.file = f
		e.writer = bufio.NewWriter(f)

		return fmt.Errorf("install compacted WAL: %w", err)
	}

	// Reopen the installed WAL.
	f, err := os.OpenFile(e.path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		e.markFailed()
		return fmt.Errorf("reopen compacted WAL: %w", err)
	}

	e.file = f
	e.writer = bufio.NewWriter(f)

	// The compacted WAL is installed and open; the backup is no
	// longer needed for normal operation.
	if err := os.Remove(backupPath); err != nil && !os.IsNotExist(err) {
		log.Printf("could not remove WAL backup %q: %v", backupPath, err)
	}

	return nil
}

func recoverWALBackup(walPath string) error {
	backupPath := walPath + ".bak"

	_, walErr := os.Stat(walPath)
	_, backupErr := os.Stat(backupPath)

	walExists := walErr == nil
	backupExists := backupErr == nil

	if walErr != nil && !os.IsNotExist(walErr) {
		return fmt.Errorf("check WAL: %w", walErr)
	}
	if backupErr != nil && !os.IsNotExist(backupErr) {
		return fmt.Errorf("check WAL backup: %w", backupErr)
	}

	if !walExists && backupExists {
		if err := os.Rename(backupPath, walPath); err != nil {
			return fmt.Errorf("restore WAL backup: %w", err)
		}
	}

	return nil
}
