package raft

import (
	"errors"     // Used to create predefined errors:
	"math/rand" //Used to randomize the election timeout. Without randomization, multiple nodes could time out simultaneously and repeatedly start elections.
	"sync"     //multiple goroutines can access the Raft node simultaneously.
	"time"     //This handles election and heartbeat timing.

	"RaftKV/internal/storage"  //This connects the Raft layer to your KV storage engine.
)


//Instead of repeatedly creating new error instances, we define them as package-level variables. 
var (
	ErrWrongLeader = errors.New("raft: node is not the leader")  //"The client contacted a node that isn't currently the leader."
	ErrTimeout     = errors.New("raft: client request timed out") //represents a client request that did not complete within the expected time.
)

type RaftNode struct {
	mu sync.Mutex //This protects the node's shared state.

	peers []*RaftNode // Direct in-memory pointers to peers for Phase 1 testing
	me    int         // Index of this node in peers[]

	// Persistent state on all servers (will persist to disk in Phase 4)
	//According to Raft, these are the pieces of state that eventually need to survive a crash.
	currentTerm int
	votedFor    int        // -1 means no vote cast yet in currentTerm
	log         []LogEntry // Log entries; index 0 is a dummy entry

	// Volatile state on all servers
	commitIndex int 
	lastApplied int   //lastApplied <= commitIndex
	//After a crash, you can reconstruct the state machine from persistent storage/snapshot + log replay.
	role        Role

	// Volatile state on leaders
	nextIndex  []int  //The next log index the leader believes it should send to that follower.If a follower rejects AppendEntries, the leader moves its nextIndex backward and retries.This is the mechanism for repairing divergent logs.
	matchIndex []int //The highest log index that the leader knows has been successfully replicated on that follower.The leader can use these values to determine whether a majority has replicated an entry and therefore whether it can advance commitIndex.

	// Election and heartbeat tracking
	lastResetTime   time.Time    //Records when the election timer was last reset.
	electionTimeout time.Duration   //How long this node waits before starting an election.
	heartbeatPeriod time.Duration   //How frequently the leader sends heartbeats. (heartbeatPeriod < electionTimeout) Otherwise followers could start elections even though the leader is healthy.

	applyWaiters map[int]chan error //A map from a log index (int) to a Go channel that carries an error.

	//these three are not leader-specific.Every node has its own state machine/storage.Every node needs a way to send committed entries to its state machine/application layer
	Engine storage.Engine //This connects Raft to your KV engine.
	
	applyCh chan ApplyMsg   //This is a Go channel. It's used to send committed Raft commands toward the application/state-machine layer.
	stopCh  chan struct{} //This is typically used to tell background goroutines
}

// NewRaftNode creates and initializes a node in the Follower state.
func NewRaftNode(me int, peersCount int, applyCh chan ApplyMsg, engine storage.Engine) *RaftNode {
	rn := &RaftNode{ //You're allocating a RaftNode and getting a pointer to it.
		me:              me,
		currentTerm:     0,
		votedFor:        -1,
		role:            Follower,
		log:             make([]LogEntry, 1), //log[0] = dummy
		commitIndex:     0,
		lastApplied:     0,
		heartbeatPeriod: 50 * time.Millisecond,
		applyCh:         applyCh,
		stopCh:          make(chan struct{}),
		Engine:          engine, // Assign engine here
		applyWaiters: make(map[int]chan error),
	}
	
	rn.resetElectionTimeout() //This chooses a randomized election timeout and records the current time.
	return rn  //Returns the initialized node.
}

// SetPeers connects this node to its cluster peers.
func (rn *RaftNode) SetPeers(peers []*RaftNode) {
	rn.mu.Lock() //You lock because you're modifying shared state.
	defer rn.mu.Unlock()  //defer means: Run Unlock() when this function returns.
	rn.peers = peers
	rn.nextIndex = make([]int, len(peers))
	rn.matchIndex = make([]int, len(peers))
}

// resetElectionTimeout picks a randomized timeout between 150ms and 350ms.
func (rn *RaftNode) resetElectionTimeout() {
	jitter := time.Duration(rand.Intn(200)) * time.Millisecond
	rn.electionTimeout = 150*time.Millisecond + jitter
	rn.lastResetTime = time.Now()
}

// Start is called by the client to submit a new command.
func (rn *RaftNode) Start(command Command) (int, int, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.role != Leader {
		return -1, -1, false
	}

	index := len(rn.log)
	term := rn.currentTerm  //The new log entry belongs to the leader's current term.
	rn.log = append(rn.log, LogEntry{
		Term:    term,
		Command: command,
	})
	//Notice you don't explicitly set Index here.That's because you're using the slice position as the effective log index.

	//Update leader's own replication state. Since the leader has obviously appended the entry to its own log:
	rn.matchIndex[rn.me] = index
	rn.nextIndex[rn.me] = index + 1

	return index, term, true //"Yes, I'm the leader.The command is at this log index and term." At this point:The command is NOT committed yet.It has only been appended to the leader's local log.
}

// Put submits a SET command to the cluster via Raft consensus.
func (rn *RaftNode) Put(key, value string) (bool, error) {
    cmd := Command{
        Op:    "SET",
        Key:   key,
        Value: value,
    }

    index, _, isLeader := rn.Start(cmd)
    if !isLeader {
        return false, ErrWrongLeader
    }

    if err := rn.waitForApply(index); err != nil {
        return false, err
    }

    return true, nil
}

// Get reads directly from the engine if the node is a verified leader.
func (rn *RaftNode) Get(key string) (string, bool, error) {
	rn.mu.Lock()
	if rn.role != Leader {
		rn.mu.Unlock()
		return "", false, ErrWrongLeader
	}
	rn.mu.Unlock()

	// You're checking that the node is currently leader.Then directly Query local engine storage state
	val, found, err := rn.Engine.Get([]byte(key))
	if err != nil {
		return "", false, err
	}
	return string(val), found, nil
}

// Delete submits a DELETE command to the cluster via Raft consensus.
func (rn *RaftNode) Delete(key string) (bool, error) {
    cmd := Command{
        Op:  "DELETE",
        Key: key,
    }

    index, _, isLeader := rn.Start(cmd)
    if !isLeader {
        return false, ErrWrongLeader
    }

    if err := rn.waitForApply(index); err != nil {
        return false, err
    }

    return true, nil
}

func (rn *RaftNode) waitForApply(index int) error {
    rn.mu.Lock()

    if rn.lastApplied >= index {
        rn.mu.Unlock()
        return nil
    }

    ch := make(chan error, 1) //This creates a channel capable of carrying an error.The channel communicates the result of applying the command.
    rn.applyWaiters[index] = ch

    rn.mu.Unlock()

    select {
    case err := <-ch:
        return err

    case <-time.After(2 * time.Second):
        rn.mu.Lock()
        delete(rn.applyWaiters, index)
        rn.mu.Unlock()

        return ErrTimeout
    }
}


func (rn *RaftNode) notifyApplied(index int, err error) {
    rn.mu.Lock()
    defer rn.mu.Unlock()

    if waiter, ok := rn.applyWaiters[index]; ok { //If a channel exists for this log index, send the error (or nil) to that channel. This notifies the waiting goroutine that the command has been applied (or failed).
        waiter <- err
        delete(rn.applyWaiters, index) //Once we've notified the waiting client, we don't need that waiter anymore.
    }
}


// applyCommittedEntries collects committed log entries.
func (rn *RaftNode) collectCommittedEntries() []ApplyMsg {
    var msgs []ApplyMsg

    for rn.lastApplied < rn.commitIndex {
        rn.lastApplied++

        entry := rn.log[rn.lastApplied]

        msg := ApplyMsg{
            CommandValid: true,
            Command:      entry.Command,
            CommandIndex: rn.lastApplied,
            CommandTerm:  entry.Term,
        }

        msgs = append(msgs, msg)
    }

    return msgs
}