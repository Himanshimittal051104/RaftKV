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

//An ApplyMsg represents a message from the Raft layer to the state machine.
//Raft -> "This entry is now committed.Please apply it."-> State Machine
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

    //              CLIENT
    //                 │
    //                 │ SET A=10
    //                 ▼
    //              Command
    //                 │
    //                 ▼
    //            LogEntry
    //       ┌─────────┼─────────┐
    //       │         │         │
    //    Index      Term     Command
    //                           │
    //                           ▼
    //                    Raft replication
    //                           │
    //                      majority ACK
    //                           │
    //                           ▼
    //                        COMMIT
    //                           │
    //                           ▼
    //                       ApplyMsg
    //                           │
    //                           ▼
    //                    StateMachine
    //                           │
    //                           ▼
    //                      KV State