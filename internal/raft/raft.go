package raft

import (
	"errors"    // Used to create predefined errors:
	"math/rand" //Used to randomize the election timeout. Without randomization, multiple nodes could time out simultaneously and repeatedly start elections.
	"sync"      //multiple goroutines can access the Raft node simultaneously.
	"time"      //This handles election and heartbeat timing.

	"RaftKV/internal/storage" //This connects the Raft layer to your KV storage engine.
	"fmt"
)

// Instead of repeatedly creating new error instances, we define them as package-level variables.
var (
	ErrWrongLeader     = errors.New("raft: node is not the leader")   //"The client contacted a node that isn't currently the leader."
	ErrTimeout         = errors.New("raft: client request timed out") //represents a client request that did not complete within the expected time.
	ErrReadIndexFailed = errors.New("read index quorum failed")
)

type RaftNode struct {
	mu sync.Mutex //This protects the node's shared state.

	peers []*RaftNode // Direct in-memory pointers to peers for Phase 1 testing
	me    int         // Index of this node in peers[]

	//According to Raft, these are the pieces of state that eventually need to survive a crash.
	currentTerm int
	votedFor    int        // -1 means no vote cast yet in currentTerm
	log         []LogEntry // Log entries; index 0 is a dummy entry

	// Volatile state on all servers
	commitIndex int
	lastApplied int //lastApplied <= commitIndex
	//After a crash, you can reconstruct the state machine from persistent storage/snapshot + log replay.
	role Role

	// Volatile state on leaders
	nextIndex  []int //The next log index the leader believes it should send to that follower.If a follower rejects AppendEntries, the leader moves its nextIndex backward and retries.This is the mechanism for repairing divergent logs.
	matchIndex []int //The highest log index that the leader knows has been successfully replicated on that follower.The leader can use these values to determine whether a majority has replicated an entry and therefore whether it can advance commitIndex.

	// Election and heartbeat tracking
	lastResetTime   time.Time     //Records when the election timer was last reset.
	electionTimeout time.Duration //How long this node waits before starting an election.
	heartbeatPeriod time.Duration //How frequently the leader sends heartbeats. (heartbeatPeriod < electionTimeout) Otherwise followers could start elections even though the leader is healthy.

	applyWaiters map[int]chan error //A map from a log index (int) to a Go channel that carries an error.

	//these three are not leader-specific.Every node has its own state machine/storage.Every node needs a way to send committed entries to its state machine/application layer
	Engine storage.Engine //This connects Raft to your KV engine.

	applyCh chan ApplyMsg //This is a Go channel. It's used to send committed Raft commands toward the application/state-machine layer.
	stopCh  chan struct{} //This is typically used to tell background goroutines

	persister Persister
}

// NewRaftNode creates and initializes a node in the Follower state.
func NewRaftNode(me int,peersCount int,applyCh chan ApplyMsg,engine storage.Engine) *RaftNode {

    return NewRaftNodeWithPersister(
        me,
        peersCount,
        applyCh,
        engine,
        NewMemPersister(),
    )
}
func NewRaftNodeWithPersister(me int, peersCount int, applyCh chan ApplyMsg, engine storage.Engine, persister Persister) *RaftNode {
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
		applyWaiters:    make(map[int]chan error),
		persister:       persister,
	}

	savedState, err := persister.ReadRaftState()
    if err != nil {
        panic(fmt.Sprintf("raft: failed to read persisted state: %v", err))
    }

	if len(savedState) > 0 {
        term, vote, restoredLog, err := decodeRaftState(savedState)
        if err != nil {
            panic(fmt.Sprintf("raft: failed to decode persisted state: %v", err))
        }

        rn.currentTerm = term
        rn.votedFor = vote
        rn.log = restoredLog
    }

	rn.resetElectionTimeout() //This chooses a randomized election timeout and records the current time.
	return rn                 //Returns the initialized node.
}

// SetPeers connects this node to its cluster peers.
func (rn *RaftNode) SetPeers(peers []*RaftNode) {
	rn.mu.Lock()         //You lock because you're modifying shared state.
	defer rn.mu.Unlock() //defer means: Run Unlock() when this function returns.
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
	term := rn.currentTerm //The new log entry belongs to the leader's current term.
	entry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}

	newLog := make([]LogEntry, len(rn.log)+1)
	copy(newLog, rn.log)
	newLog[len(rn.log)] = entry

	if err := rn.persistState(
		rn.currentTerm,
		rn.votedFor,
		newLog,
	); err != nil {
		return -1, -1, false
	}

	rn.log = newLog
	
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

	// Establish that this node is still the leader
	// by obtaining a ReadIndex from a majority.
	readIndex, err := rn.readIndex()
	if err != nil {
		return "", false, err
	}

	// wait until the state machine has applied
	// everything through readIndex.

	if err := rn.waitForApply(readIndex); err != nil {
		return "", false, err
	}

	// Verify that we are still the leader before reading.
	rn.mu.Lock()
	if rn.role != Leader {
		rn.mu.Unlock()
		return "", false, ErrWrongLeader
	}
	rn.mu.Unlock()

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

func (rn *RaftNode) readIndex() (int, error) {
	rn.mu.Lock()

	// Only the leader can establish a ReadIndex.
	if rn.role != Leader {
		rn.mu.Unlock()
		return 0, ErrWrongLeader
	}

	term := rn.currentTerm
	me := rn.me
	peers := rn.peers

	// The leader itself counts as one acknowledgement.
	acks := 1

	// Single-node cluster: leader already has a majority.
	if acks > len(peers)/2 {
		readIndex := rn.commitIndex
		rn.mu.Unlock()
		return readIndex, nil
	}

	rn.mu.Unlock()

	// Collect acknowledgements from followers.
	type probeResult struct {
		term int
	}

	results := make(chan probeResult, len(peers)-1)

	for i := range peers {
		if i == me {
			continue
		}

		go func(peerID int) {
			args := ReadProbeArgs{
				Term:     term,
				LeaderID: me,
			}

			var reply ReadProbeReply
			peers[peerID].ReadProbe(&args, &reply)

			results <- probeResult{
				term: reply.Term,
			}
		}(i)
	}

	// Count valid acknowledgements.
	for i := 0; i < len(peers)-1; i++ {
		result := <-results

		rn.mu.Lock()

		// Another node has a higher term.
		if result.term > rn.currentTerm {
			rn.currentTerm = result.term
			rn.role = Follower
			rn.votedFor = -1
			rn.resetElectionTimeout()

			rn.mu.Unlock()
			return 0, ErrWrongLeader
		}

		// We may have lost leadership while probes were in flight.
		if rn.role != Leader || rn.currentTerm != term {
			rn.mu.Unlock()
			return 0, ErrWrongLeader
		}

		// This reply belongs to our current term.
		if result.term == term {
			acks++
		}

		if acks > len(peers)/2 {
			// IMPORTANT:
			// Capture commitIndex only AFTER quorum confirmation.
			readIndex := rn.commitIndex

			rn.mu.Unlock()
			return readIndex, nil
		}

		rn.mu.Unlock()
	}

	return 0, ErrReadIndexFailed
}

func (rn *RaftNode) applyLoop() {
	for {
		select {
		case <-rn.stopCh:
			return

		case msg := <-rn.applyCh:
			if !msg.CommandValid {
				continue
			}

			var err error

			switch msg.Command.Op {
			case "SET":
				err = rn.Engine.Put(
					[]byte(msg.Command.Key),
					[]byte(msg.Command.Value),
				)

			case "DELETE":
				err = rn.Engine.Delete(
					[]byte(msg.Command.Key),
				)
			}

			rn.notifyApplied(msg.CommandIndex, err)
		}
	}
}

func (rn *RaftNode) persistState(currentTerm int,votedFor int,log []LogEntry,) error {
	state, err := encodeRaftState(
		currentTerm,
		votedFor,
		log,
	)
	if err != nil {
		return err
	}

	return rn.persister.SaveRaftState(state)
}