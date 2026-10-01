package storage

import (
	"sync"
)

// MemEngine is a thread-safe, in-memory implementation of the storage.Engine interface.
type MemEngine struct {
	mu   sync.Mutex
	data map[string][]byte
}

// NewMemEngine creates a new instance of MemEngine.
func NewMemEngine() *MemEngine {
	return &MemEngine{
		data: make(map[string][]byte),
	}
}

// Put stores a key-value pair.
func (m *MemEngine) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Store a copy to prevent external mutation race conditions
	valCopy := make([]byte, len(value))
	copy(valCopy, value)
	m.data[string(key)] = valCopy
	return nil
}

// Get retrieves a value by key.
func (m *MemEngine) Get(key []byte) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	val, exists := m.data[string(key)]
	if !exists {
		return nil, false, nil
	}
	valCopy := make([]byte, len(val))
	copy(valCopy, val)
	return valCopy, true, nil
}

// Delete removes a key-value pair.
func (m *MemEngine) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, string(key))
	return nil
}

// Close cleans up engine resources.
func (m *MemEngine) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = nil
	return nil
}