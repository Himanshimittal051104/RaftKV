package storage

import (
	"sync"

	"RaftKV/internal/apperrors"
)

// MemEngine is a thread-safe, in-memory storage engine.
type MemEngine struct {
	mu          sync.Mutex
	data        map[string][]byte
	lastApplied int
	closed      bool
}

// NewMemEngine creates a new in-memory engine.
func NewMemEngine() *MemEngine {
	return &MemEngine{
		data: make(map[string][]byte),
	}
}

// Put stores a key-value pair.
func (m *MemEngine) Put(index int, key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return apperrors.ErrEngineClosed
	}

	valCopy := make([]byte, len(value))
	copy(valCopy, value)

	m.data[string(key)] = valCopy

	if index > m.lastApplied {
		m.lastApplied = index
	}

	return nil
}

// Get retrieves a value by key.
func (m *MemEngine) Get(key []byte) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil, false, apperrors.ErrEngineClosed
	}

	val, exists := m.data[string(key)]
	if !exists {
		return nil, false, nil
	}

	valCopy := make([]byte, len(val))
	copy(valCopy, val)

	return valCopy, true, nil
}

// Delete removes a key-value pair.
func (m *MemEngine) Delete(index int, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return apperrors.ErrEngineClosed
	}

	delete(m.data, string(key))

	if index > m.lastApplied {
		m.lastApplied = index
	}

	return nil
}

// Noop advances the applied index without modifying data.
func (m *MemEngine) Noop(index int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return apperrors.ErrEngineClosed
	}

	if index > m.lastApplied {
		m.lastApplied = index
	}

	return nil
}

// LastApplied returns the highest applied index.
func (m *MemEngine) LastApplied() (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, apperrors.ErrEngineClosed
	}

	return m.lastApplied, nil
}

// IsClosed reports whether the engine is closed.
func (m *MemEngine) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.closed
}

// Close marks the engine as closed and releases its in-memory data.
func (m *MemEngine) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	m.closed = true
	m.data = nil

	return nil
}

func (m *MemEngine) Open() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return apperrors.ErrEngineClosed
	}

	return nil
}

func (m *MemEngine) IsFailed() bool {
	return false
}
