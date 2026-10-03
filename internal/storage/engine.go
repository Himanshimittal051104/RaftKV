package storage

//Any storage implementation used by RaftKV must provide these four operations.
type Engine interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, bool, error)
	Delete(key []byte) error
	Close() error
}