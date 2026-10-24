package raft

//This tells Go that this file belongs to the raft package.

type Role int //Role -> integer representing the node's current state

const (
	Follower Role = iota //iota is a Go keyword used inside a const block. It starts at 0 and increments automatically.
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type Command struct {
	ClientID  int64
	SeqNumber int64
	Op        string
	Key       string
	Value     string
}

type LogEntry struct {
	Index   int
	Term    int
	Command Command
}

// An ApplyMsg represents a message from the Raft layer to the state machine.
// Raft -> "This entry is now committed.Please apply it."-> State Machine
type ApplyMsg struct {
	CommandValid bool
	Command      Command
	CommandIndex int
	CommandTerm  int
}

type ApplyResult struct {
	Done chan struct{}
	Err  error
}

type RequestID struct {
	ClientID  int64
	SeqNumber int64
}

type StateMachine interface {
	Apply(msg ApplyMsg) (err error)
	Get(key []byte) (value []byte, found bool, err error)
	LastApplied() (int, error)
	IsClosed() bool
	IsFailed() bool
	Open() error
}
