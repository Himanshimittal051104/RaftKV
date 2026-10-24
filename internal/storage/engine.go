package storage

//Any storage implementation used by RaftKV must provide these four operations.

type Engine interface {
	Put(index int, key, value []byte) error
	Get(key []byte) ([]byte, bool, error)
	Delete(index int, key []byte) error
	Noop(index int) error
	LastApplied() (int, error)
	Open() error
	Close() error
	IsClosed() bool
	IsFailed() bool
}
