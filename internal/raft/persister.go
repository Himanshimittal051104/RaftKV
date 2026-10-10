package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Persister stores the persistent state of a Raft node.
//
// The state stored here must survive a Raft node restart.
type Persister interface {
	SaveRaftState(state []byte) error
	ReadRaftState() ([]byte, error)
}

// MemPersister is an in-memory implementation of Persister.
// It is primarily useful for testing persistence semantics before
// introducing actual disk-backed storage.
type MemPersister struct {
	mu    sync.Mutex
	state []byte
}

func NewMemPersister() *MemPersister {
	return &MemPersister{}
}

func (p *MemPersister) SaveRaftState(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Copy the bytes so the caller cannot mutate our stored state.
	//If we did:p.state = state,then the caller could later modify state, and our supposedly persisted data would silently change.
	p.state = append([]byte(nil), state...)

	return nil
}

func (p *MemPersister) ReadRaftState() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Return a copy for the same reason.
	return append([]byte(nil), p.state...), nil
}

// FilePersister stores Raft state in a file.
type FilePersister struct {
	mu   sync.Mutex
	path string
}

// NewFilePersister creates a disk-backed persister.
// A missing state file represents a fresh node.
func NewFilePersister(path string) (*FilePersister, error) {
	if path == "" {
		return nil, fmt.Errorf("raft state file path cannot be empty")
	}

	return &FilePersister{path: path}, nil
}

// SaveRaftState writes state to a temporary file and then renames it
// over the destination file.
func (p *FilePersister) SaveRaftState(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	dir := filepath.Dir(p.path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".raft-state-*")
	if err != nil {
		return fmt.Errorf("create temporary state file: %w", err)
	}

	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if _, err := tmp.Write(state); err != nil {
		tmp.Close()
		return fmt.Errorf("write temporary state file: %w", err)
	}

	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync temporary state file: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary state file: %w", err)
	}

	if err := os.Rename(tmpPath, p.path); err != nil {
		return fmt.Errorf("replace Raft state file: %w", err)
	}

	return nil
}

// ReadRaftState reads the most recently saved state.
func (p *FilePersister) ReadRaftState() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	state, err := os.ReadFile(p.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read Raft state file: %w", err)
	}

	if len(state) == 0 {
		return nil, fmt.Errorf("Raft state file %q is empty", p.path)
	}

	return state, nil
}
