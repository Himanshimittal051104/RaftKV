package raft

import "sync"

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