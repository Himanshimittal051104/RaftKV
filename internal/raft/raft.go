package raft

import (
	"errors"    // Used to create predefined errors
	"math/rand" //Used to randomize the election timeout. Without randomization, multiple nodes could time out simultaneously and repeatedly start elections.
	"sync"      //multiple goroutines can access the Raft node simultaneously.
	"time"      //This handles election and heartbeat timing.

	"RaftKV/internal/storage" //This connects the Raft layer to your KV storage engine.
	"fmt"                     //fmt is a package, and Println, Printf, and Sprintf are functions provided by that package.
)

// Instead of repeatedly creating new error instances, we define them as package-level variables.
var (
	ErrWrongLeader       = errors.New("raft: node is not the leader")   //"The client contacted a node that isn't currently the leader."
	ErrTimeout           = errors.New("raft: client request timed out") //represents a client request that did not complete within the expected time.
	ErrReadIndexFailed   = errors.New("read index quorum failed")
	ErrRequestIDConflict = errors.New("request ID reused with a different command")
)

type RaftNode struct {
	mu       sync.Mutex //This protects the node's shared state.
	stopOnce sync.Once
	peers    []*RaftNode // Direct in-memory pointers to peers for Phase 1 testing
	me       int         // Index of this node in peers[]

	//According to Raft, these are the pieces of state that eventually need to survive a crash.
	currentTerm int
	votedFor    int        // -1 means no vote cast yet in currentTerm
	log         []LogEntry // Log entries; index 0 is a dummy entry

	// Volatile state on all servers
	commitIndex int // for single-node clusters, commitIndex can be set to the last log index. In multi-node clusters, commitIndex is updated based on the leader's commitIndex , so for single-node clusters we are also persisting commitIndex.
	lastApplied int //lastApplied <= commitIndex
	lastQueued  int
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
	stopCh  chan struct{} //This is typically used to tell background goroutines to stop running. The struct{} type is an empty struct, which takes up no memory. It's often used as a signal in channels(no data, only signals).

	persister    Persister
	requestIndex map[RequestID]int
	stateMachine StateMachine
}

// NewRaftNode creates and initializes a node in the Follower state.
func NewRaftNode(me int, peersCount int, applyCh chan ApplyMsg, engine storage.Engine) *RaftNode {

	node, err := NewRaftNodeWithPersister(
		me,
		peersCount,
		applyCh,
		engine,
		NewMemPersister(),
	)
	if err != nil {
		// The in-memory persister should not fail during initialization.
		panic(fmt.Sprintf("raft: failed to initialize in-memory node: %v", err))
	}
	return node
}

func NewPersistentRaftNode(me int, peersCount int, applyCh chan ApplyMsg, engine storage.Engine, stateFile string) (*RaftNode, error) {
	persister, err := NewFilePersister(stateFile)
	if err != nil {
		return nil, err
	}

	node, err := NewRaftNodeWithPersister(
		me,
		peersCount,
		applyCh,
		engine,
		persister,
	)

	if err != nil {
		return nil, err
	}

	// Recover application state for our single-node configuration.
	if peersCount == 1 {
		if err := node.recoverSingleNodeState(); err != nil {
			return nil, err
		}
	}

	return node, nil
}

func NewRaftNodeWithPersister(me int, peersCount int, applyCh chan ApplyMsg, engine storage.Engine, persister Persister) (*RaftNode, error) {
	rn := &RaftNode{ //You're allocating a RaftNode and getting a pointer to it.
		me:              me,
		currentTerm:     0,
		votedFor:        -1,
		role:            Follower,
		log:             make([]LogEntry, 1), //log[0] = dummy
		nextIndex:       make([]int, peersCount),
		matchIndex:      make([]int, peersCount),
		commitIndex:     0,
		lastApplied:     0,
		heartbeatPeriod: 50 * time.Millisecond,
		applyCh:         applyCh,
		stopCh:          make(chan struct{}),
		Engine:          engine, // Assign engine here
		applyWaiters:    make(map[int]chan error),
		persister:       persister,
		requestIndex:    make(map[RequestID]int),
	}

	savedState, err := persister.ReadRaftState()
	if err != nil {
		return nil, fmt.Errorf("raft: failed to read persisted state: %w", err)
	}

	if len(savedState) > 0 {
		term, vote, restoredLog, commitIndex, err := decodeRaftState(savedState)
		if err != nil {
			return nil, fmt.Errorf("raft: failed to decode persisted state: %w", err)
		}

		rn.currentTerm = term
		rn.votedFor = vote
		rn.log = restoredLog
		rn.rebuildRequestIndex()
		if peersCount == 1 {
			rn.commitIndex = commitIndex
		} else {
			rn.commitIndex = 0
		}
	}

	rn.resetElectionTimeout() //This chooses a randomized election timeout and records the current time.
	return rn, nil            //Returns the initialized node.
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
func (rn *RaftNode) Start(command Command) (int, int, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.role != Leader {
		return -1, -1, ErrWrongLeader
	}

	if command.ClientID != 0 && command.SeqNumber > 0 {
		requestID := RequestID{
			ClientID:  command.ClientID,
			SeqNumber: command.SeqNumber,
		}

		if existingIndex, found := rn.requestIndex[requestID]; found {
			// Reuse the existing log position for this request.
			if existingIndex > 0 && existingIndex < len(rn.log) {
				existingCommand := rn.log[existingIndex].Command

				if existingCommand != command {
					// The same request ID cannot represent two different commands.
					return -1, -1, ErrRequestIDConflict
				}

				return existingIndex, rn.log[existingIndex].Term, nil
			}

			// Stale map entry: remove it and append normally.
			delete(rn.requestIndex, requestID)
		}
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
	newCommitIndex := rn.commitIndex

	// In a single-node cluster, this entry will be committed locally.
	if len(rn.peers) == 0 {
		newCommitIndex = index
	}

	if err := rn.persistState(
		rn.currentTerm,
		rn.votedFor,
		newLog,
		newCommitIndex,
	); err != nil {
		return -1, -1, err
	}

	rn.log = newLog

	if command.ClientID != 0 && command.SeqNumber > 0 {
		requestID := RequestID{
			ClientID:  command.ClientID,
			SeqNumber: command.SeqNumber,
		}
		rn.requestIndex[requestID] = index
	}

	//Update leader's own replication state. Since the leader has obviously appended the entry to its own log:
	rn.matchIndex[rn.me] = index
	rn.nextIndex[rn.me] = index + 1

	// In a single-node cluster, the leader itself is a majority.
	if len(rn.peers) == 0 {
		rn.commitIndex = newCommitIndex
		applyMsgs := rn.collectCommittedEntries()
		go func() {
			//The purpose is to send the application messages without making Start() wait synchronously for the receiver of applyCh. Without the go keyword, a send to an unbuffered channel—or a full buffered channel—could block Start() until a receiver became available.
			for _, msg := range applyMsgs {
				select {
				case rn.applyCh <- msg: //the message can be sent to the application channel.
				case <-rn.stopCh: //shutdown has been signalled, so the goroutine returns.
					return
				}
			}
		}()
	}

	return index, term, nil //"Yes, I'm the leader.The command is at this log index and term." At this point:The command is NOT committed yet.It has only been appended to the leader's local log.
}

// Put submits a SET command to the cluster via Raft consensus.
func (rn *RaftNode) Put(clientID, seqNumber int64, key, value string) (bool, error) {
	cmd := Command{
		ClientID:  clientID,
		SeqNumber: seqNumber,
		Op:        "SET",
		Key:       key,
		Value:     value,
	}

	index, _, err := rn.Start(cmd)

	if err != nil {
		return false, err
	}

	rn.mu.Lock()
	alreadyApplied := rn.lastApplied >= index
	rn.mu.Unlock()

	if !alreadyApplied {
		if err := rn.waitForApply(index); err != nil {
			return false, err
		}
	}

	return true, nil
}

// Get reads directly from the engine if the node is a verified leader.
func (rn *RaftNode) Get(clientID, seqNumber int64, key string) (string, bool, error) {
	rn.mu.Lock()
	if rn.role != Leader {
		rn.mu.Unlock()
		return "", false, ErrWrongLeader
	}
	term := rn.currentTerm
	rn.mu.Unlock()

	// Establish that this node is still the leader by obtaining a ReadIndex from a majority.
	readIndex, err := rn.readIndex()
	if err != nil {
		return "", false, err
	}

	// wait until the state machine has applied everything through readIndex.

	if err := rn.waitForApply(readIndex); err != nil {
		return "", false, err
	}

	// Verify that we are still the leader before reading.
	rn.mu.Lock()
	if rn.role != Leader || rn.currentTerm != term {
		rn.mu.Unlock()
		return "", false, ErrWrongLeader
	}
	rn.mu.Unlock()

	if rn.stateMachine != nil {
		val, found, err := rn.stateMachine.Get([]byte(key))

		if err != nil {
			return "", false, err
		}

		return string(val), found, nil
	}

	// Backward compatibility for existing tests/constructors
	val, found, err := rn.Engine.Get([]byte(key))

	if err != nil {
		return "", false, err
	}

	return string(val), found, nil
}

// Delete submits a DELETE command to the cluster via Raft consensus.
func (rn *RaftNode) Delete(clientID, seqNumber int64, key string) (bool, error) {
	cmd := Command{
		ClientID:  clientID,
		SeqNumber: seqNumber,
		Op:        "DELETE",
		Key:       key,
	}

	index, _, err := rn.Start(cmd)
	if err != nil {
		return false, err
	}

	rn.mu.Lock()
	alreadyApplied := rn.lastApplied >= index
	rn.mu.Unlock()

	if !alreadyApplied {
		if err := rn.waitForApply(index); err != nil {
			return false, err
		}
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

	case <-time.After(2 * time.Second): //it gives you a channel that will signal after two seconds have elapsed.
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

	for rn.lastQueued < rn.commitIndex {
		rn.lastQueued++

		entry := rn.log[rn.lastQueued]

		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
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

	noopIndex := -1

	for i := len(rn.log) - 1; i >= 1; i-- {
		if rn.log[i].Term == term && rn.log[i].Command.Op == "NOOP" {
			noopIndex = i
			break
		}
	}

	// The leader itself counts as one acknowledgement.
	acks := 1

	// Single-node cluster: leader already has a majority.
	if acks > len(peers)/2 {
		if noopIndex < 0 || rn.commitIndex < noopIndex {
			rn.mu.Unlock()
			return 0, ErrReadIndexFailed
		}

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
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	for i := 0; i < len(peers)-1; i++ {
		var result probeResult
		select { //In Go, select lets a goroutine wait on multiple channel operations.
		case result = <-results:
		case <-timer.C:
			return 0, ErrReadIndexFailed
		}

		rn.mu.Lock()

		// Another node has a higher term.
		if result.term > rn.currentTerm {
			if err := rn.persistState(
				result.term,
				-1,
				rn.log,
				rn.commitIndex,
			); err != nil {
				rn.mu.Unlock()
				return 0, err
			}
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
			// Capture commitIndex only AFTER quorum confirmation and noop commited
			if noopIndex < 0 || rn.commitIndex < noopIndex {
				rn.mu.Unlock()
				return 0, ErrReadIndexFailed
			}
			readIndex := rn.commitIndex

			rn.mu.Unlock()
			return readIndex, nil
		}

		rn.mu.Unlock()
	}

	return 0, ErrReadIndexFailed
}

func (rn *RaftNode) applyLoop() {
	pending := make(map[int]ApplyMsg)

	for {
		select {
		case <-rn.stopCh:
			return

		case msg := <-rn.applyCh:
			if !msg.CommandValid {
				continue
			}

			rn.mu.Lock()
			expectedIndex := rn.lastApplied + 1
			rn.mu.Unlock()

			if msg.CommandIndex < expectedIndex {
				// Ignore an entry that has already been applied.
				continue
			}

			if msg.CommandIndex > expectedIndex {
				pending[msg.CommandIndex] = msg
				continue
			}

			// Apply this message, then drain consecutive buffered messages.
			current := msg

			for {
				err := rn.applyMessage(current)

				if err != nil {
					rn.notifyApplied(current.CommandIndex, err)
					break
				}

				rn.mu.Lock()
				if current.CommandIndex == rn.lastApplied+1 {
					rn.lastApplied++
				}
				nextExpected := rn.lastApplied + 1
				rn.mu.Unlock()

				rn.notifyApplied(current.CommandIndex, nil)
				delete(pending, current.CommandIndex)

				next, ok := pending[nextExpected]
				if !ok {
					break
				}

				current = next
			}
		}
	}
}

func (rn *RaftNode) applyMessage(msg ApplyMsg) error {
	if rn.stateMachine != nil {
		return rn.stateMachine.Apply(msg)
	}

	// Backward-compatible path for existing callers and tests
	// that have not configured a state machine.
	switch msg.Command.Op {

	case "NOOP":
		return nil

	case "SET":
		return rn.Engine.Put(
			[]byte(msg.Command.Key),
			[]byte(msg.Command.Value),
		)

	case "DELETE":
		return rn.Engine.Delete(
			[]byte(msg.Command.Key),
		)

	default:
		return fmt.Errorf("unsupported operation %q", msg.Command.Op)
	}
}

func (rn *RaftNode) persistState(currentTerm int, votedFor int, log []LogEntry, commitIndex int) error {
	state, err := encodeRaftState(
		currentTerm,
		votedFor,
		log,
		commitIndex,
	)
	if err != nil {
		return err
	}

	return rn.persister.SaveRaftState(state)
}

// recoverSingleNodeState rebuilds the in-memory state machine
// from the persisted log during single-node startup.
// Call this before starting background goroutines or serving requests.
func (rn *RaftNode) recoverSingleNodeState() error {
	// Replay log entries in order, skipping the dummy entry at index 0.
	if rn.commitIndex < 0 || rn.commitIndex >= len(rn.log) {
		return fmt.Errorf(
			"invalid commit index %d for log length %d",
			rn.commitIndex,
			len(rn.log),
		)
	}

	for i := 1; i <= rn.commitIndex; i++ {
		entry := rn.log[i]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
			CommandTerm:  entry.Term,
		}
		if err := rn.stateMachine.Apply(msg); err != nil {
			return fmt.Errorf(
				"recover log index %d: %w",
				i,
				err,
			)
		}
		
	}

	// In this single-node configuration, the recovered durable log represents the committed state reconstructed above.
	rn.lastApplied = rn.commitIndex

	return nil
}

func (rn *RaftNode) rebuildRequestIndex() {
	rn.requestIndex = make(map[RequestID]int)

	for i := 1; i < len(rn.log); i++ {
		command := rn.log[i].Command

		if command.ClientID == 0 || command.SeqNumber <= 0 {
			continue
		}

		requestID := RequestID{
			ClientID:  command.ClientID,
			SeqNumber: command.SeqNumber,
		}

		rn.requestIndex[requestID] = i
	}
}
