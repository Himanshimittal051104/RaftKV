package raft

import (
	"math/rand"
	"sync"
	"time"
)

type RaftNode struct {
	mu sync.Mutex

	peers []*RaftNode // Direct in-memory pointers to peers for Phase 1 testing
	me    int         // Index of this node in peers[]

	// Persistent state on all servers (will persist to disk in Phase 3)
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

	applyCh chan ApplyMsg
	stopCh  chan struct{}
}

// NewRaftNode creates and initializes a node in the Follower state.
func NewRaftNode(me int, peersCount int, applyCh chan ApplyMsg) *RaftNode {
	rn := &RaftNode{
		me:              me,
		currentTerm:     0,
		votedFor:        -1,
		role:            Follower,
		log:             make([]LogEntry, 1), // Dummy entry at index 0 so indices are 1-based
		commitIndex:     0,
		lastApplied:     0,
		heartbeatPeriod: 50 * time.Millisecond,
		applyCh:         applyCh,
		stopCh:          make(chan struct{}),
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


// applyCommittedEntries sends committed log entries to the application layer.
func (rn *RaftNode) applyCommittedEntries() {
	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rn.log[rn.lastApplied].Command,
			CommandIndex: rn.lastApplied,
		}
		// Send non-blocking or push to channel
		select {
		case rn.applyCh <- msg:
		default:
		}
	}
}