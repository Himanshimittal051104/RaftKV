package raft

type Role int

const (
	Follower Role = iota
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

type ApplyMsg struct {
	CommandValid bool
	Command      Command
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type StateMachine interface {
	Apply(cmd Command) (result string, err error)
	Get(key string) (value string, exists bool)
	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error
}