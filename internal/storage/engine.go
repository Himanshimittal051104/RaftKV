package storage

type EngineSnapshot struct {
	LastApplied int               `json:"lastApplied"`
	Data        map[string][]byte `json:"data"`
}

type Engine interface {
	Put(index int, key, value []byte) error
	Get(key []byte) ([]byte, bool, error)
	Delete(index int, key []byte) error
	Noop(index int) error
	LastApplied() (int, error)

	MaybeSaveSnapshot()

	Open() error
	Close() error
	IsClosed() bool
	IsFailed() bool
}
