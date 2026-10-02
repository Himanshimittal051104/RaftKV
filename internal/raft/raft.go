package raft

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"RaftKV/internal/storage"
)

var (
	ErrWrongLeader = errors.New("raft: node is not the leader")
	ErrTimeout     = errors.New("raft: client request timed out")
)

type RaftNode struct {
	mu sync.Mutex

	peers []*RaftNode // Direct in-memory pointers to peers for Phase 1 testing
	me    int         // Index of this node in peers[]

	// Persistent state on all servers (will persist to disk in Phase 4)
	currentTerm int
	votedFor    int        // -1 means no vote cast yet in currentTerm
	log         []LogEntry // Log entries; index 0 is a dummy entry

	// Volatile state on all servers
	commitIndex int
	lastApplied int
	role        Role

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Election and heartbeat tracking
	lastResetTime   time.Time
	electionTimeout time.Duration
	heartbeatPeriod time.Duration

	Engine storage.Engine

	applyCh chan ApplyMsg
	stopCh  chan struct{}
}

// NewRaftNode creates and initializes a node in the Follower state.
func NewRaftNode(me int, peersCount int, applyCh chan ApplyMsg, engine storage.Engine) *RaftNode {
	rn := &RaftNode{
		me:              me,
		currentTerm:     0,
		votedFor:        -1,
		role:            Follower,
		log:             make([]LogEntry, 1),
		commitIndex:     0,
		lastApplied:     0,
		heartbeatPeriod: 50 * time.Millisecond,
		applyCh:         applyCh,
		stopCh:          make(chan struct{}),
		Engine:          engine, // Assign engine here
	}

	rn.resetElectionTimeout()
	return rn
}

// SetPeers connects this node to its cluster peers.
func (rn *RaftNode) SetPeers(peers []*RaftNode) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
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
	term := rn.currentTerm
	rn.log = append(rn.log, LogEntry{
		Term:    term,
		Command: command,
	})

	rn.matchIndex[rn.me] = index
	rn.nextIndex[rn.me] = index + 1

	return index, term, true
}

// Put submits a SET command to the cluster via Raft consensus.
func (rn *RaftNode) Put(key, value string) (bool, error) {
	cmd := Command{
		Op:    "SET",
		Key:   key,
		Value: value,
	}

	_, _, isLeader := rn.Start(cmd)
	if !isLeader {
		return false, ErrWrongLeader
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

	// Query local engine storage state
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

	_, _, isLeader := rn.Start(cmd)
	if !isLeader {
		return false, ErrWrongLeader
	}
	return true, nil
}

// applyCommittedEntries sends committed log entries to the application layer and executes them in storage.
func (rn *RaftNode) applyCommittedEntries() {
	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		entry := rn.log[rn.lastApplied]

		// Execute the command against the storage engine if an engine is provided
		if rn.Engine != nil {
			switch entry.Command.Op {
			case "SET":
				_ = rn.Engine.Put([]byte(entry.Command.Key), []byte(entry.Command.Value))
			case "DELETE":
				_ = rn.Engine.Delete([]byte(entry.Command.Key))
			case "GET":
				// Read operations do not mutate state machine logs
			}
		}

		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: rn.lastApplied,
		}

		select {
		case rn.applyCh <- msg:
		default:
		}
	}
}