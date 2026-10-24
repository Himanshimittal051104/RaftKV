package raft

import (
	"RaftKV/internal/apperrors" //This handles election and heartbeat timing.
	"errors"                    // Used to create predefined errors
	"fmt"                       //fmt is a package, and Println, Printf, and Sprintf are functions provided by that package.
	"math/rand"                 //Used to randomize the election timeout. Without randomization, multiple nodes could time out simultaneously and repeatedly start elections.
	"sync"                      //multiple goroutines can access the Raft node simultaneously.
	"time"
)

// Instead of repeatedly creating new error instances, we define them as package-level variables.
var (
	ErrWrongLeader       = errors.New("raft: node is not the leader")   //"The client contacted a node that isn't currently the leader."
	ErrTimeout           = errors.New("raft: client request timed out") //represents a client request that did not complete within the expected time.
	ErrReadIndexFailed   = errors.New("read index quorum failed")
	ErrRequestIDConflict = errors.New("request ID reused with a different command")
	ErrInvalidPeers      = errors.New("raft: invalid peer configuration")
	ErrAlreadyStarted    = errors.New("raft: node already started")
	ErrNodeStopped       = errors.New("raft: node is stopped")
)

type RaftNode struct {
	mu       sync.Mutex //This protects the node's shared state.
	stopOnce sync.Once

	me         int  // Index of this node in peers[]
	started    bool // protected by rn.mu
	stopped    bool
	peersCount int
	peers      []*RaftNode // Direct in-memory pointers to peers including itself

	role Role

	//According to Raft, these are the pieces of state that eventually need to survive a crash.
	currentTerm int
	votedFor    int        // -1 means no vote cast yet in currentTerm
	log         []LogEntry // Log entries; index 0 is a dummy entry

	// Volatile state on all servers
	commitIndex int
	lastQueued  int
	//After a crash, you can reconstruct the state machine from persistent storage/snapshot + log replay.

	// Volatile state on leaders
	nextIndex  []int //The next log index the leader believes it should send to that follower.If a follower rejects AppendEntries, the leader moves its nextIndex backward and retries.This is the mechanism for repairing divergent logs.
	matchIndex []int //The highest log index that the leader knows has been successfully replicated on that follower.The leader can use these values to determine whether a majority has replicated an entry and therefore whether it can advance commitIndex.

	// Election and heartbeat tracking
	lastResetTime   time.Time     //Records when the election timer was last reset.
	electionTimeout time.Duration //How long this node waits before starting an election.

	applyWaiters map[int]*ApplyResult //A map from a log index (int) to a Go channel that carries an error.

	applyCh chan ApplyMsg //This is a Go channel. It's used to send committed Raft commands toward the application/state-machine layer.
	stopCh  chan struct{} //This is typically used to tell background goroutines to stop running. The struct{} type is an empty struct, which takes up no memory. It's often used as a signal in channels(no data, only signals).

	persister    Persister
	requestIndex map[RequestID]int
	stateMachine StateMachine //In Go, the methods you can call depend on the variable's declared type, not just the actual object's type.

	recoveryMu   sync.Mutex
	recovering   bool
	recoveryErr  error
	recoveryDone chan struct{}

	wg sync.WaitGroup
}

// NewRaftNode creates and initializes a node in the Follower state.
func validateNodeConfig(me int, peersCount int, applyCh chan ApplyMsg, persister Persister, stateMachine StateMachine) error {
	if peersCount <= 0 {
		return ErrInvalidPeers
	}

	if me < 0 || me >= peersCount {
		return ErrInvalidPeers
	}

	if applyCh == nil {
		return errors.New("raft: apply channel cannot be nil")
	}

	if persister == nil {
		return errors.New("raft: persister cannot be nil")
	}

	if stateMachine == nil {
		return errors.New("raft: state machine cannot be nil")
	}

	return nil
}

func NewRaftNode(me int, peersCount int, applyCh chan ApplyMsg, persister Persister, statemachine StateMachine) (*RaftNode, error) {
	err := validateNodeConfig(me, peersCount, applyCh, persister, statemachine)
	if err != nil {
		return nil, err
	}

	rn := &RaftNode{ //You're allocating a RaftNode and getting a pointer to it.
		me:           me,
		peersCount:   peersCount,
		role:         Follower,
		currentTerm:  0,
		lastQueued:   0,
		votedFor:     -1,
		log:          make([]LogEntry, 1), //log[0] = dummy
		commitIndex:  0,
		nextIndex:    make([]int, peersCount),
		matchIndex:   make([]int, peersCount),
		applyWaiters: make(map[int]*ApplyResult),
		applyCh:      applyCh,
		stopCh:       make(chan struct{}),
		persister:    persister,
		requestIndex: make(map[RequestID]int),
		stateMachine: statemachine,
	}

	err = rn.fetchPersistedState()
	if err != nil {
		return nil, err
	}
	rn.resetElectionTimeout() //This chooses a randomized election timeout and records the current time.

	err = rn.fetchEngineState()
	if err != nil {
		if errors.Is(err, apperrors.ErrEngineClosed) {

		} else {
			return nil, err
		}
	}
	return rn, nil //Returns the initialized node.
}

func (rn *RaftNode) hasValidPeers(peers []*RaftNode) bool {
	if rn.peersCount == 1 {
		return true
	}
	if len(peers) != rn.peersCount {
		return false
	}

	if rn.me < 0 || rn.me >= len(peers) || peers[rn.me] != rn {
		return false
	}

	for _, peer := range peers {
		if peer == nil {
			return false
		}
	}
	return true
}

// SetPeers connects this node to its cluster peers.
func (rn *RaftNode) SetPeers(peers []*RaftNode) error {
	rn.mu.Lock()         //You lock because you're modifying shared state.
	defer rn.mu.Unlock() //defer means: Run Unlock() when this function returns.
	if rn.hasValidPeers(peers) == false {
		return ErrInvalidPeers
	}
	rn.peers = append([]*RaftNode(nil), peers...)
	return nil
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

	if rn.stopped {
		return -1, -1, ErrNodeStopped
	}

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
	if rn.peersCount == 1 {
		newCommitIndex = index
	}

	if err := rn.persistState(
		rn.currentTerm,
		rn.votedFor,
		newLog,
	); err != nil {
		return -1, -1, err
	}

	rn.log = newLog
	rn.commitIndex = newCommitIndex

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
	if rn.peersCount == 1 {
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

	return index, term, nil
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

	if err := rn.waitForApply(index); err != nil {
		return false, err
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

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	for {
		val, found, err := rn.stateMachine.Get([]byte(key))
		if err == nil {
			return string(val), found, nil
		}

		if errors.Is(err, apperrors.ErrEngineClosed) ||
			errors.Is(err, ErrNodeStopped) {
			return "", false, err
		}

		if !errors.Is(err, apperrors.ErrEngineFailed) {
			return "", false, err
		}

		recoveryResult := make(chan error, 1)
		go func() {
			recoveryResult <- rn.openFailedEngine()
		}()

		select {
		case recoveryErr := <-recoveryResult:
			if recoveryErr != nil {
				return "", false, recoveryErr
			}
			// Recovery succeeded; retry Get().
		case <-rn.stopCh:
			return "", false, ErrNodeStopped
		case <-timer.C:
			return "", false, ErrTimeout
		}

		// Recovery succeeded; retry Get().
	}
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

	if err := rn.waitForApply(index); err != nil {
		return false, err
	}

	return true, nil
}

func (rn *RaftNode) waitForApply(index int) error {
	if index < 0 {
		return fmt.Errorf("raft: invalid apply index %d", index)
	}

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	for {
		rn.mu.Lock()

		if rn.stopped {
			rn.mu.Unlock()
			return ErrNodeStopped
		}

		if rn.stateMachine.IsClosed() {
			rn.mu.Unlock()
			return apperrors.ErrEngineClosed
		}

		lastApplied, err := rn.stateMachine.LastApplied()
		if err != nil {
			rn.mu.Unlock()

			if errors.Is(err, apperrors.ErrEngineFailed) {
				recoveryDone := make(chan error, 1)

				go func() {
					recoveryDone <- rn.openFailedEngine()
				}()

				select {
				case recoveryErr := <-recoveryDone:
					if recoveryErr != nil {
						return recoveryErr
					}
					continue

				case <-rn.stopCh:
					return ErrNodeStopped

				case <-timer.C:
					return ErrTimeout
				}
			}

			return err
		}

		if lastApplied >= index {
			rn.mu.Unlock()
			return nil
		}

		result, exists := rn.applyWaiters[index]
		if !exists {
			result = &ApplyResult{
				Done: make(chan struct{}),
			}
			rn.applyWaiters[index] = result
		}

		rn.mu.Unlock()

		select {
		case <-result.Done:
			return result.Err

		case <-rn.stopCh:
			return ErrNodeStopped

		case <-timer.C:
			return ErrTimeout
		}
	}
}

func (rn *RaftNode) notifyApplied(index int, err error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	result, exists := rn.applyWaiters[index]
	if !exists {
		return
	}

	select {
	case <-result.Done:
		return
	default:
	}

	result.Err = err
	close(result.Done) //closing a channel wakes up every goroutine waiting to receive from it—not just one.
	delete(rn.applyWaiters, index)
}

func (rn *RaftNode) failApplyWaiters(err error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	for index, result := range rn.applyWaiters {
		select {
		case <-result.Done:
			// Already notified.
		default:
			result.Err = err
			close(result.Done)
		}
		delete(rn.applyWaiters, index)
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
	peers := append([]*RaftNode(nil), rn.peers...)

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
	if acks > rn.peersCount/2 {
		if noopIndex < 0 || rn.commitIndex < noopIndex {
			rn.mu.Unlock()
			return 0, ErrReadIndexFailed
		}

		readIndex := rn.commitIndex
		rn.mu.Unlock()
		return readIndex, nil
	}

	// Take a stable snapshot of the peer list while holding the lock.

	rn.mu.Unlock()
	// Collect acknowledgements from followers.
	type probeResult struct {
		term int
	}

	results := make(chan probeResult, rn.peersCount-1)

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
	for i := 0; i < rn.peersCount-1; i++ {
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

		if acks > rn.peersCount/2 {
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

		case msg, ok := <-rn.applyCh:
			if !ok {
				return
			}
			if !msg.CommandValid {
				continue
			}
			pending[msg.CommandIndex] = msg

			lastApplied, err := rn.stateMachine.LastApplied()
			if errors.Is(err, apperrors.ErrEngineClosed) {
				// this will only give us error if engine is closed , because in just reading lastapplied engine will not fail or does not throw any other type of errors
				rn.failApplyWaiters(err)
				return
			}
			expectedIndex := lastApplied + 1

			// Apply this message, then drain consecutive buffered messages.
			current, ok := pending[expectedIndex]
			if !ok {
				continue
			}

			for {
				lastApplied, err := rn.stateMachine.LastApplied()
				if err != nil {
					// this will only give us error if engine is closed , because in just raeding lastapplied engine will not fail or does not throw any other type of errors
					rn.failApplyWaiters(err)
					return
				}

				if lastApplied >= current.CommandIndex {
					// A previous attempt may have applied it already.
				} else if lastApplied != current.CommandIndex-1 {
					// Do not apply entries out of order.
					break
				} else {
					// this can throw enginefailed as well as engineclosed error
					for {
						err = rn.stateMachine.Apply(current)
						if err == nil {
							break
						}
						if errors.Is(err, apperrors.ErrEngineClosed) {
							// The engine is closed; retrying won't help.
							rn.failApplyWaiters(err)
							return
						}
						// Retry the same entry, unless shutting down.
						err = rn.openFailedEngine() // this can throw node stopped or engine closed err
						if err != nil {
							rn.failApplyWaiters(err)
							return
						}
						lastApplied, err = rn.stateMachine.LastApplied()
						if err != nil {
							rn.failApplyWaiters(err)
							return
						}

						if lastApplied >= current.CommandIndex {
							break
						}
						if lastApplied != current.CommandIndex-1 {
							rn.failApplyWaiters(
								fmt.Errorf("unexpected lastApplied index %d for command %d", lastApplied, current.CommandIndex),
							)
							return
						}
					}

					lastApplied, err = rn.stateMachine.LastApplied()
					if err != nil {
						rn.failApplyWaiters(err)
						return
					}

					if lastApplied != current.CommandIndex {
						err := fmt.Errorf("state machine applied index mismatch: got %d, want %d", lastApplied, current.CommandIndex)
						rn.failApplyWaiters(err)
						return
					}
				}

				rn.notifyApplied(current.CommandIndex, nil)
				delete(pending, current.CommandIndex)

				nextExpected := current.CommandIndex + 1
				next, ok := pending[nextExpected]
				if !ok {
					break
				}

				current = next
			}
		}
	}
}

func (rn *RaftNode) persistState(currentTerm int, votedFor int, log []LogEntry) error {
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
	// In this single-node configuration, the recovered durable log represents the committed state reconstructed above.

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

func (rn *RaftNode) fetchPersistedState() error {
	savedState, err := rn.persister.ReadRaftState()
	if err != nil {
		return fmt.Errorf("raft: failed to read persisted state: %w", err)
	}

	if len(savedState) == 0 {
		return nil
	}

	term, vote, restoredLog, err := decodeRaftState(savedState)
	if err != nil {
		return fmt.Errorf("raft: failed to decode persisted state: %w", err)
	}

	rn.currentTerm = term
	rn.votedFor = vote
	rn.log = restoredLog
	rn.rebuildRequestIndex()
	return nil
}

func (rn *RaftNode) fetchEngineState() error {
	lastApplied, err := rn.stateMachine.LastApplied()
	if err != nil {
		return fmt.Errorf("raft: failed to read engine state: %w", err)
	}

	lastLogIndex := len(rn.log) - 1

	if lastApplied < 0 || lastApplied > lastLogIndex {
		return fmt.Errorf(
			"raft: invalid state machine lastApplied index %d for last log index %d",
			lastApplied,
			lastLogIndex,
		)
	}
	rn.commitIndex = lastApplied
	rn.lastQueued = lastApplied
	if rn.peersCount == 1 {
		rn.commitIndex = len(rn.log) - 1
		if err := rn.recoverSingleNodeState(); err != nil {
			return err
		}
	}
	return nil
}

func (rn *RaftNode) openFailedEngine() error {
	for {
		// Stop if the node is shutting down.
		select {
		case <-rn.stopCh:
			return ErrNodeStopped
		default:
		}

		rn.recoveryMu.Lock()

		if rn.recovering {
			// Another goroutine owns recovery. Wait for its result.
			done := rn.recoveryDone
			rn.recoveryMu.Unlock()

			select {
			case <-done:
				// The owner has completed recovery.
				// Return its result instead of starting another attempt.
				rn.recoveryMu.Lock()
				err := rn.recoveryErr
				rn.recoveryMu.Unlock()
				return err

			case <-rn.stopCh:
				return ErrNodeStopped
			}
		}

		// This goroutine becomes the recovery owner.
		rn.recovering = true
		rn.recoveryDone = make(chan struct{})
		done := rn.recoveryDone
		rn.recoveryMu.Unlock()

		var recoveryErr error

		for {
			select {
			case <-rn.stopCh:
				recoveryErr = ErrNodeStopped
			default:
			}

			if recoveryErr != nil {
				break
			}

			recoveryErr = rn.stateMachine.Open()
			if recoveryErr == nil {
				break
			}

			// Explicitly closed engines must never be reopened automatically.
			if errors.Is(recoveryErr, apperrors.ErrEngineClosed) {
				break
			}

			// Retry transient errors after 100 ms.
			timer := time.NewTimer(100 * time.Millisecond)

			select {
			case <-timer.C:
				// Retry Open().
			case <-rn.stopCh:
				timer.Stop()
				recoveryErr = ErrNodeStopped
			}

			if errors.Is(recoveryErr, ErrNodeStopped) {
				break
			}
		}

		// Publish the result and wake all waiting goroutines.
		rn.recoveryMu.Lock()
		rn.recoveryErr = recoveryErr
		rn.recovering = false
		close(done)
		rn.recoveryMu.Unlock()

		return recoveryErr
	}
}
