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
// It is primarily useful for testing persistence semantics before introducing actual disk-backed storage.
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

	valcopy := make([]byte, len(state))
	copy(valcopy, state)
	p.state = valcopy
	return nil
}

func (p *MemPersister) ReadRaftState() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Return a copy for the same reason.
	valcopy := make([]byte, len(p.state))
	copy(valcopy, p.state)
	return valcopy, nil
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
		//This validates that a nonempty path was provided and creates the object. It doesn't write any state yet.
	}

	return &FilePersister{path: path}, nil
}

// SaveRaftState writes state to a temporary file and then renames it over the destination file.
func (p *FilePersister) SaveRaftState(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	dir := filepath.Dir(p.path)

	if err := os.MkdirAll(dir, 0755); err != nil {//Ensures the destination directory exists.
		return fmt.Errorf("create state directory: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".raft-state-*") //creates a temporary file in the same directory as the destination file. This is important because renaming files across different filesystems can fail.	
	if err != nil {
		return fmt.Errorf("create temporary state file: %w", err)
	}

	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if _, err := tmp.Write(state); err != nil {
		tmp.Close()
		return fmt.Errorf("write temporary state file: %w", err)
	}

	//Sync() requests that file contents be flushed to stable storage. The code then closes the file and checks for errors.
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
	//If the program overwrote the destination directly and crashed halfway through, the file could contain incomplete data. Instead, the persister writes the new state to a temporary file first. Only after writing, syncing, and closing it does the code attempt to rename it over the destination.This helps prevent readers from seeing a partially written replacement file.
}

// ReadRaftState reads the most recently saved state.
func (p *FilePersister) ReadRaftState() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	state, err := os.ReadFile(p.path)
	if os.IsNotExist(err) { //checks whether that error means the file or path does not exist
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
