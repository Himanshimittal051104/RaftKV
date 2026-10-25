package raft

import (
	"RaftKV/internal/storage"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBroadcastAppendEntriesCommitsMajority(t *testing.T) {
	nodes := make([]*RaftNode, 3)

	for i := 0; i < 3; i++ {
		persister := NewMemPersister()
		engine := storage.NewMemEngine()
		statemachine := NewTestStateMachine(engine)

		node, err := NewRaftNode(
			i,
			3,
			make(chan ApplyMsg, 10),
			persister,
			statemachine,
		)
		if err != nil {
			t.Fatalf("failed to create node %d: %v", i, err)
		}

		nodes[i] = node
		defer node.Stop()
	}

	// Connect all nodes.
	for i := 0; i < 3; i++ {
		if err := nodes[i].SetPeers(nodes); err != nil {
			t.Fatalf("failed to set peers for node %d: %v", i, err)
		}
	}

	leader := nodes[0]

	// Make node 0 the leader deterministically.
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1
	leader.nextIndex[0] = len(leader.log)
	leader.matchIndex[0] = len(leader.log) - 1
	leader.mu.Unlock()

	index, term, err := leader.Start(Command{
		Op:    "SET",
		Key:   "x",
		Value: "100",
	})
	if err != nil {
		t.Fatalf("leader rejected command: %v", err)
	}

	if index != 1 {
		t.Fatalf("expected index 1, got %d", index)
	}
	if term != 1 {
		t.Fatalf("expected term 1, got %d", term)
	}

	// nextIndex is the next index to send, not the last index replicated.
	leader.mu.Lock()
	leader.nextIndex[1] = index
	leader.nextIndex[2] = index
	leader.mu.Unlock()

	leader.broadcastAppendEntries()

	// Wait for majority commitment: leader plus at least one follower.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		leader.mu.Lock()
		commitIndex := leader.commitIndex
		match1 := leader.matchIndex[1]
		match2 := leader.matchIndex[2]
		leader.mu.Unlock()

		replicatedFollowers := 0
		if match1 >= index {
			replicatedFollowers++
		}
		if match2 >= index {
			replicatedFollowers++
		}

		if commitIndex >= index && replicatedFollowers >= 1 {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}

	leader.mu.Lock()
	commitIndex := leader.commitIndex
	match1 := leader.matchIndex[1]
	match2 := leader.matchIndex[2]
	leader.mu.Unlock()

	if commitIndex < index {
		t.Fatalf("expected commit index >= %d, got %d", index, commitIndex)
	}

	if match1 < index && match2 < index {
		t.Fatalf("entry was not replicated to either follower: matchIndex[1]=%d, matchIndex[2]=%d", match1, match2)
	}

	// Verify every follower that acknowledged replication has the entry.
	for i := 1; i < 3; i++ {
		if nodes[0].matchIndex[i] < index {
			continue
		}

		nodes[i].mu.Lock()
		if len(nodes[i].log) <= index {
			nodes[i].mu.Unlock()
			t.Fatalf("follower %d log too short", i)
		}
		entry := nodes[i].log[index]
		nodes[i].mu.Unlock()

		if entry.Term != term {
			t.Errorf("follower %d: term = %d, want %d", i, entry.Term, term)
		}
		if entry.Command.Key != "x" || entry.Command.Value != "100" {
			t.Errorf("follower %d: unexpected command %+v", i, entry.Command)
		}
	}
}

func TestApplyCommittedEntry(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	persister := NewMemPersister()
	engine := storage.NewMemEngine()
	statemachine := NewTestStateMachine(engine)

	node, err := NewRaftNode(
		0,
		1,
		applyCh,
		persister,
		statemachine,
	)
	if err != nil {
		t.Fatalf("failed to create Raft node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()

	node.log = append(node.log, LogEntry{
		Index: 1,
		Term:  1,
		Command: Command{
			Op:    "SET",
			Key:   "name",
			Value: "Himanshi",
		},
	})

	node.commitIndex = 1

	msgs := node.collectCommittedEntries()

	lastApplied, err := node.stateMachine.LastApplied()
	if err != nil {
		t.Fatalf("failed to fetch lastApplied: %v", err)
	}
	lastQueued := node.lastQueued

	node.mu.Unlock()

	if len(msgs) != 1 {
		t.Fatalf("expected 1 ApplyMsg, got %d", len(msgs))
	}

	msg := msgs[0]

	if !msg.CommandValid {
		t.Fatal("expected valid command")
	}
	if msg.CommandIndex != 1 {
		t.Fatalf("expected command index 1, got %d", msg.CommandIndex)
	}
	if msg.CommandTerm != 1 {
		t.Fatalf("expected command term 1, got %d", msg.CommandTerm)
	}
	if msg.Command.Op != "SET" {
		t.Fatalf("expected SET command, got %s", msg.Command.Op)
	}
	if msg.Command.Key != "name" {
		t.Fatalf("expected key name, got %s", msg.Command.Key)
	}
	if msg.Command.Value != "Himanshi" {
		t.Fatalf("expected value Himanshi, got %s", msg.Command.Value)
	}

	if lastApplied != 0 {
		t.Fatalf("expected lastApplied=0 before application, got %d", lastApplied)
	}
	if lastQueued != 1 {
		t.Fatalf("expected lastQueued=1, got %d", lastQueued)
	}
}

func TestAppendEntriesReplicatesEntry(t *testing.T) {
	leader, err := NewRaftNode(
		0,
		2,
		make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create leader: %v", err)
	}
	defer leader.Stop()

	follower, err := NewRaftNode(
		1,
		2,
		make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create follower: %v", err)
	}
	defer follower.Stop()

	// Set up a 2-node cluster.
	peers := []*RaftNode{leader, follower}
	leader.SetPeers(peers)
	follower.SetPeers(peers)

	// Make the leader a leader in term 1 and append a command.
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1

	leader.log = append(leader.log, LogEntry{
		Term: 1,
		Command: Command{
			Op:    "SET",
			Key:   "x",
			Value: "10",
		},
	})

	leader.nextIndex[1] = len(leader.log)
	leader.matchIndex[0] = len(leader.log) - 1

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  leader.log[0].Term,
		Entries:      []LogEntry{leader.log[1]},
		LeaderCommit: 0,
	}
	leader.mu.Unlock()

	var reply AppendEntriesReply

	// Send replication RPC directly to the follower.
	follower.AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept valid AppendEntries")
	}

	// Verify that the follower received the entry.
	follower.mu.Lock()
	defer follower.mu.Unlock()

	if len(follower.log) != 2 {
		t.Fatalf(
			"expected follower log length 2, got %d",
			len(follower.log),
		)
	}

	entry := follower.log[1]

	if entry.Term != 1 {
		t.Fatalf("expected entry term 1, got %d", entry.Term)
	}

	if entry.Command.Op != "SET" ||
		entry.Command.Key != "x" ||
		entry.Command.Value != "10" {
		t.Fatalf("replicated command incorrect: %+v", entry.Command)
	}

	// Replication alone must not commit the entry.
	if follower.commitIndex != 0 {
		t.Fatalf(
			"entry should not be committed yet; commitIndex=%d",
			follower.commitIndex,
		)
	}
}

func TestLeaderCommitsAfterMajorityReplication(t *testing.T) {
	applyCh0 := make(chan ApplyMsg, 10)
	applyCh1 := make(chan ApplyMsg, 10)
	applyCh2 := make(chan ApplyMsg, 10)

	leader, err := NewRaftNode(
		0, 3, applyCh0, NewMemPersister(), NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create leader: %v", err)
	}
	defer leader.Stop()

	follower1, err := NewRaftNode(
		1, 3, applyCh1, NewMemPersister(), NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create follower1: %v", err)
	}
	defer follower1.Stop()

	follower2, err := NewRaftNode(
		2, 3, applyCh2, NewMemPersister(), NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create follower2: %v", err)
	}
	defer follower2.Stop()

	peers := []*RaftNode{leader, follower1, follower2}
	leader.SetPeers(peers)
	follower1.SetPeers(peers)
	follower2.SetPeers(peers)

	// Make node 0 the leader.
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1
	leader.mu.Unlock()

	// Append a command through Start().
	index, term, err := leader.Start(Command{
		Op:    "SET",
		Key:   "x",
		Value: "10",
	})
	if err != nil {
		t.Fatalf("leader should accept command: %v", err)
	}

	if index != 1 {
		t.Fatalf("expected index 1, got %d", index)
	}
	if term != 1 {
		t.Fatalf("expected term 1, got %d", term)
	}

	leader.mu.Lock()

	if leader.commitIndex != 0 {
		got := leader.commitIndex
		leader.mu.Unlock()
		t.Fatalf("entry should initially be uncommitted, commitIndex=%d", got)
	}

	args := AppendEntriesArgs{
		Term:         leader.currentTerm,
		LeaderID:     leader.me,
		PrevLogIndex: 0,
		PrevLogTerm:  leader.log[0].Term,
		Entries:      []LogEntry{leader.log[index]},
		LeaderCommit: 0,
	}
	leader.mu.Unlock()

	// Replicate the entry to one follower.
	var reply AppendEntriesReply
	follower1.AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept AppendEntries")
	}

	// Record the successful replication on the leader.
	leader.mu.Lock()
	leader.matchIndex[1] = index
	leader.nextIndex[1] = index + 1

	// A 3-node cluster needs 2 replicas, including the leader.
	replicated := 1
	for i := range leader.peers {
		if i != leader.me && leader.matchIndex[i] >= index {
			replicated++
		}
	}

	if replicated > len(leader.peers)/2 {
		leader.commitIndex = index
	}

	gotCommitIndex := leader.commitIndex
	leader.mu.Unlock()

	if gotCommitIndex != index {
		t.Fatalf(
			"expected commitIndex=%d after majority replication, got %d",
			index,
			gotCommitIndex,
		)
	}
}

func TestClientAPIAndLeaderRedirection(t *testing.T) {
	n := 3

	nodes := make([]*RaftNode, n)
	engines := make([]storage.Engine, n)
	stateMachines := make([]StateMachine, n)
	applyChs := make([]chan ApplyMsg, n)
	persisters := make([]Persister, n)

	// Create three nodes with separate engines and persisters.
	for i := 0; i < n; i++ {
		engines[i] = storage.NewMemEngine()
		stateMachines[i] = NewTestStateMachine(engines[i])
		applyChs[i] = make(chan ApplyMsg, 100)
		persisters[i] = NewMemPersister()

		var err error
		nodes[i], err = NewRaftNode(
			i,
			n,
			applyChs[i],
			persisters[i],
			stateMachines[i],
		)
		if err != nil {
			t.Fatalf("failed to create node %d: %v", i, err)
		}
	}

	// Connect peers and start background loops.
	for i := 0; i < n; i++ {
		nodes[i].SetPeers(nodes)
		nodes[i].StartBackground()
	}

	defer func() {
		for i := 0; i < n; i++ {
			nodes[i].Stop()
		}
	}()

	// Wait for leader election.
	t.Log("Waiting for leader election...")

	var leader *RaftNode
	leaderID := -1
	start := time.Now()

	for time.Since(start) < 3*time.Second {
		for i := 0; i < n; i++ {
			nodes[i].mu.Lock()
			isLeader := nodes[i].role == Leader
			nodes[i].mu.Unlock()

			if isLeader {
				leader = nodes[i]
				leaderID = i
				break
			}
		}

		if leader != nil {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	if leader == nil {
		t.Fatal("failed to elect a leader within timeout")
	}

	t.Logf("Leader found: Node %d", leaderID)

	// Test 1: A follower must reject writes.
	followerID := (leaderID + 1) % n
	follower := nodes[followerID]

	_, err := follower.Put(1, 1, "test_key", "test_val")
	if !errors.Is(err, ErrWrongLeader) {
		t.Fatalf("expected ErrWrongLeader on follower Put, got: %v", err)
	}

	t.Log("Follower correctly rejected Put with ErrWrongLeader")

	// Test 2: Write a value through the leader.
	success, err := leader.Put(1, 2, "city", "Delhi")
	if !success || err != nil {
		t.Fatalf("failed to Put on leader: success=%v, err=%v", success, err)
	}

	t.Log("Put command submitted successfully to leader")

	// Test 3: Read the value from the leader.
	val, found, err := leader.Get(1, 3, "city")
	if err != nil || !found || val != "Delhi" {
		t.Fatalf(
			"failed to Get correct value: val=%s, found=%v, err=%v",
			val, found, err,
		)
	}

	t.Logf("Get verified on leader: city = %q", val)

	// Test 4: Delete the value through the leader.
	success, err = leader.Delete(1, 4, "city")
	if !success || err != nil {
		t.Fatalf(
			"failed to Delete: success=%v, err=%v",
			success, err,
		)
	}

	t.Log("Delete command submitted successfully")

	// Verify deletion.
	_, found, err = leader.Get(1, 5, "city")
	if err != nil {
		t.Fatalf("Get after Delete failed: %v", err)
	}
	if found {
		t.Fatal("expected key 'city' to be deleted")
	}

	t.Log("Client API and leader redirection tests passed")
}

func TestStartRejectedByFollower(t *testing.T) {
	engine := storage.NewMemEngine()
	stateMachine := NewTestStateMachine(engine)
	applyCh := make(chan ApplyMsg, 10)
	persister := NewMemPersister()

	node, err := NewRaftNode(0, 1, applyCh, persister, stateMachine)
	if err != nil {
		t.Fatalf("failed to create Raft node: %v", err)
	}
	defer node.Stop()

	index, term, err := node.Start(Command{
		Op:    "SET",
		Key:   "x",
		Value: "10",
	})

	if err == nil {
		t.Fatal("follower should reject Start()")
	}

	if index != -1 || term != -1 {
		t.Fatalf("expected (-1, -1), got (%d, %d, %v)",
			index, term, err)
	}
}

func TestStartLeaderAppendsEntry(t *testing.T) {
	engine := storage.NewMemEngine()
	stateMachine := NewTestStateMachine(engine)
	applyCh := make(chan ApplyMsg, 10)
	persister := NewMemPersister()

	node, err := NewRaftNode(0, 1, applyCh, persister, stateMachine)
	if err != nil {
		t.Fatalf("failed to create Raft node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 3
	node.peers = []*RaftNode{node}
	node.nextIndex = make([]int, 1)
	node.matchIndex = make([]int, 1)
	node.mu.Unlock()

	cmd := Command{
		Op:    "SET",
		Key:   "x",
		Value: "10",
	}

	index, term, err := node.Start(cmd)
	if err != nil {
		t.Fatalf("leader should accept Start(): %v", err)
	}

	if index != 1 {
		t.Fatalf("expected index 1, got %d", index)
	}

	if term != 3 {
		t.Fatalf("expected term 3, got %d", term)
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	if len(node.log) != 2 {
		t.Fatalf("expected log length 2, got %d", len(node.log))
	}

	entry := node.log[index]

	if entry.Term != 3 {
		t.Fatalf("expected entry term 3, got %d", entry.Term)
	}

	if entry.Command.Op != "SET" ||
		entry.Command.Key != "x" ||
		entry.Command.Value != "10" {
		t.Fatalf("command was not appended correctly: %+v", entry.Command)
	}
}

func TestStartDoesNotCommit(t *testing.T) {
	engine := storage.NewMemEngine()
	stateMachine := NewTestStateMachine(engine)
	applyCh := make(chan ApplyMsg, 10)
	persister := NewMemPersister()

	node, err := NewRaftNode(0, 2, applyCh, persister, stateMachine)
	if err != nil {
		t.Fatalf("failed to create Raft node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 1
	node.peers = []*RaftNode{node}
	node.nextIndex = make([]int, 1)
	node.matchIndex = make([]int, 1)
	node.mu.Unlock()

	_, _, err = node.Start(Command{
		Op:    "SET",
		Key:   "x",
		Value: "10",
	})
	if err != nil {
		t.Fatalf("leader should accept command: %v", err)
	}

	node.mu.Lock()
	commitIndex := node.commitIndex
	node.mu.Unlock()

	if commitIndex != 0 {
		t.Fatalf("Start() must not commit an entry; commitIndex=%d", commitIndex)
	}

	lastApplied, err := stateMachine.LastApplied()
	if err != nil {
		t.Fatalf("failed to get last applied index: %v", err)
	}

	if lastApplied != 0 {
		t.Fatalf("Start() must not apply an entry; lastApplied=%d", lastApplied)
	}
}

func TestAppendEntriesRepairsConflict(t *testing.T) {
	leaderEngine := storage.NewMemEngine()
	leaderSM := NewTestStateMachine(leaderEngine)
	leader, err := NewRaftNode(
		0, 2, make(chan ApplyMsg, 10), NewMemPersister(), leaderSM,
	)
	if err != nil {
		t.Fatalf("failed to create leader: %v", err)
	}
	defer leader.Stop()

	followerEngine := storage.NewMemEngine()
	followerSM := NewTestStateMachine(followerEngine)
	follower, err := NewRaftNode(
		1, 2, make(chan ApplyMsg, 10), NewMemPersister(), followerSM,
	)
	if err != nil {
		t.Fatalf("failed to create follower: %v", err)
	}
	defer follower.Stop()

	peers := []*RaftNode{leader, follower}
	leader.SetPeers(peers)
	follower.SetPeers(peers)

	// Leader log:
	// [dummy, A(term=1), B(term=2)]
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 2

	leader.log = append(leader.log,
		LogEntry{
			Term: 1,
			Command: Command{
				Op:    "SET",
				Key:   "x",
				Value: "A",
			},
		},
		LogEntry{
			Term: 2,
			Command: Command{
				Op:    "SET",
				Key:   "x",
				Value: "B",
			},
		},
	)
	leader.mu.Unlock()

	// Follower has a conflicting entry at index 2:
	// [dummy, A(term=1), X(term=3)]
	follower.mu.Lock()
	follower.currentTerm = 2

	follower.log = append(follower.log,
		LogEntry{
			Term: 1,
			Command: Command{
				Op:    "SET",
				Key:   "x",
				Value: "A",
			},
		},
		LogEntry{
			Term: 3,
			Command: Command{
				Op:    "SET",
				Key:   "x",
				Value: "X",
			},
		},
	)
	follower.mu.Unlock()

	// Leader sends its entry at index 2.
	leader.mu.Lock()
	entryToSend := leader.log[2]
	leader.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         2,
		LeaderID:     0,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{entryToSend},
		LeaderCommit: 0,
	}

	var reply AppendEntriesReply
	follower.AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept the leader's entry")
	}

	follower.mu.Lock()
	defer follower.mu.Unlock()

	if len(follower.log) != 3 {
		t.Fatalf("expected follower log length 3, got %d",
			len(follower.log))
	}

	entry := follower.log[2]

	if entry.Term != 2 {
		t.Fatalf("expected term 2, got %d", entry.Term)
	}

	if entry.Command.Value != "B" {
		t.Fatalf("expected conflicting entry to be replaced with B, got %s",
			entry.Command.Value)
	}
}

func TestCommittedEntryCannotBeOverwrittenByOutdatedCandidate(t *testing.T) {
	applyCh0 := make(chan ApplyMsg, 10)
	applyCh1 := make(chan ApplyMsg, 10)
	applyCh2 := make(chan ApplyMsg, 10)

	node0, err := NewRaftNode(
		0, 3, applyCh0, NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node0: %v", err)
	}
	defer node0.Stop()

	node1, err := NewRaftNode(
		1, 3, applyCh1, NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node1: %v", err)
	}
	defer node1.Stop()

	node2, err := NewRaftNode(
		2, 3, applyCh2, NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node2: %v", err)
	}
	defer node2.Stop()

	nodes := []*RaftNode{node0, node1, node2}
	for _, node := range nodes {
		node.SetPeers(nodes)
	}

	// Node 0 is the leader in term 1.
	node0.mu.Lock()
	node0.role = Leader
	node0.currentTerm = 1
	node0.log = append(node0.log, LogEntry{
		Term: 1,
		Command: Command{
			Op:    "SET",
			Key:   "x",
			Value: "100",
		},
	})
	node0.matchIndex[0] = 1
	node0.matchIndex[1] = 1
	node0.matchIndex[2] = 0
	node0.commitIndex = 1
	node0.mu.Unlock()

	// Node 1 has the committed entry.
	node1.mu.Lock()
	node1.currentTerm = 1
	node1.log = append(node1.log, LogEntry{
		Term: 1,
		Command: Command{
			Op:    "SET",
			Key:   "x",
			Value: "100",
		},
	})
	node1.commitIndex = 1
	node1.mu.Unlock()

	// Node 2 has only the dummy entry.
	node2.mu.Lock()
	node2.currentTerm = 1
	node2.mu.Unlock()

	// Node 2 requests a vote in term 2, but its log is outdated.
	request := RequestVoteArgs{
		Term:         2,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	var reply RequestVoteReply
	node1.RequestVote(&request, &reply)

	if reply.VoteGranted {
		t.Fatal("outdated candidate should not receive a vote")
	}

	if reply.Term != 2 {
		t.Fatalf("expected node 1 to update to term 2, got %d", reply.Term)
	}

	// Verify the committed entry remains intact.
	node1.mu.Lock()
	defer node1.mu.Unlock()

	if node1.commitIndex != 1 {
		t.Fatalf(
			"expected commitIndex=1, got %d",
			node1.commitIndex,
		)
	}

	if len(node1.log) <= 1 {
		t.Fatal("node 1 lost the committed log entry")
	}

	entry := node1.log[1]

	if entry.Term != 1 {
		t.Fatalf("expected entry term 1, got %d", entry.Term)
	}

	if entry.Command.Key != "x" || entry.Command.Value != "100" {
		t.Fatalf("committed entry was changed: %+v", entry.Command)
	}
}

func TestLeaderDoesNotCommitWithoutMajority(t *testing.T) {
	applyCh0 := make(chan ApplyMsg, 10)
	applyCh1 := make(chan ApplyMsg, 10)
	applyCh2 := make(chan ApplyMsg, 10)

	node0, err := NewRaftNode(
		0, 3, applyCh0, NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node0: %v", err)
	}
	defer node0.Stop()

	node1, err := NewRaftNode(
		1, 3, applyCh1, NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node1: %v", err)
	}
	defer node1.Stop()

	node2, err := NewRaftNode(
		2, 3, applyCh2, NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node2: %v", err)
	}
	defer node2.Stop()

	nodes := []*RaftNode{node0, node1, node2}
	for _, node := range nodes {
		node.SetPeers(nodes)
	}

	node0.mu.Lock()
	node0.role = Leader
	node0.currentTerm = 1
	node0.mu.Unlock()

	index, _, err := node0.Start(Command{
		Op:    "SET",
		Key:   "x",
		Value: "100",
	})
	if err != nil {
		t.Fatalf("leader should accept command: %v", err)
	}

	if index != 1 {
		t.Fatalf("expected entry at index 1, got %d", index)
	}

	node0.mu.Lock()
	commitIndex := node0.commitIndex
	match1 := node0.matchIndex[1]
	match2 := node0.matchIndex[2]
	node0.mu.Unlock()

	if match1 >= index || match2 >= index {
		t.Fatalf(
			"entry unexpectedly replicated: matchIndex[1]=%d, matchIndex[2]=%d",
			match1, match2,
		)
	}

	if commitIndex != 0 {
		t.Fatalf(
			"leader committed without majority: commitIndex=%d",
			commitIndex,
		)
	}
}

func TestLeaderDoesNotCommitOldTermEntry(t *testing.T) {
	applyCh0 := make(chan ApplyMsg, 10)
	applyCh1 := make(chan ApplyMsg, 10)
	applyCh2 := make(chan ApplyMsg, 10)

	node0, err := NewRaftNode(
		0, 3, applyCh0, NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node0: %v", err)
	}
	defer node0.Stop()

	node1, err := NewRaftNode(
		1, 3, applyCh1, NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node1: %v", err)
	}
	defer node1.Stop()

	node2, err := NewRaftNode(
		2, 3, applyCh2, NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node2: %v", err)
	}
	defer node2.Stop()

	nodes := []*RaftNode{node0, node1, node2}
	for _, node := range nodes {
		node.SetPeers(nodes)
	}

	node0.mu.Lock()
	node0.role = Leader
	node0.currentTerm = 2

	node0.log = append(node0.log, LogEntry{
		Term: 1,
		Command: Command{
			Op:    "SET",
			Key:   "x",
			Value: "100",
		},
	})

	// The old-term entry is replicated on a majority.
	node0.matchIndex[0] = 1
	node0.matchIndex[1] = 1
	node0.matchIndex[2] = 0
	node0.commitIndex = 0
	node0.mu.Unlock()

	// Simulate the leader's commit calculation.
	node0.mu.Lock()

	for n := len(node0.log) - 1; n > node0.commitIndex; n-- {
		if node0.log[n].Term != node0.currentTerm {
			continue
		}

		count := 0
		for j := range nodes {
			if node0.matchIndex[j] >= n {
				count++
			}
		}

		if count > len(nodes)/2 {
			node0.commitIndex = n
			break
		}
	}

	commitIndex := node0.commitIndex
	node0.mu.Unlock()

	if commitIndex != 0 {
		t.Fatalf(
			"old-term entry was incorrectly committed: commitIndex=%d",
			commitIndex,
		)
	}
}

func TestCommitIndexNeverDecreases(t *testing.T) {
	node, err := NewRaftNode(
		0,
		3,
		make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()

	node.role = Leader
	node.currentTerm = 2

	node.log = append(node.log,
		LogEntry{
			Term: 1,
			Command: Command{
				Op:    "SET",
				Key:   "x",
				Value: "100",
			},
		},
		LogEntry{
			Term: 2,
			Command: Command{
				Op:    "SET",
				Key:   "y",
				Value: "200",
			},
		},
	)

	node.commitIndex = 2
	before := node.commitIndex

	// Simulate a later commit calculation.
	for n := len(node.log) - 1; n > node.commitIndex; n-- {
		if node.log[n].Term == node.currentTerm {
			node.commitIndex = n
			break
		}
	}

	after := node.commitIndex
	node.mu.Unlock()

	if after < before {
		t.Fatalf(
			"commitIndex moved backwards: before=%d, after=%d",
			before, after,
		)
	}

	if after != 2 {
		t.Fatalf("expected commitIndex to remain 2, got %d", after)
	}
}

func TestLastAppliedNeverExceedsCommitIndex(t *testing.T) {
	stateMachine := NewTestStateMachine(storage.NewMemEngine())

	node, err := NewRaftNode(
		0, 1, make(chan ApplyMsg, 10),
		NewMemPersister(), stateMachine,
	)
	if err != nil {
		t.Fatalf("failed to create Raft node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.log = append(node.log,
		LogEntry{
			Term: 1,
			Command: Command{
				Op: "SET", Key: "x", Value: "100",
			},
		},
		LogEntry{
			Term: 1,
			Command: Command{
				Op: "SET", Key: "y", Value: "200",
			},
		},
	)

	node.commitIndex = 1
	msgs := node.collectCommittedEntries()

	if len(msgs) != 1 {
		node.mu.Unlock()
		t.Fatalf("expected 1 ApplyMsg, got %d", len(msgs))
	}

	commitIndex := node.commitIndex
	lastQueued := node.lastQueued
	node.mu.Unlock()

	lastApplied, err := stateMachine.LastApplied()
	if err != nil {
		t.Fatalf("failed to get last applied index: %v", err)
	}

	if lastApplied > commitIndex {
		t.Fatalf(
			"lastApplied exceeded commitIndex: lastApplied=%d, commitIndex=%d",
			lastApplied, commitIndex,
		)
	}

	if lastApplied != 0 {
		t.Fatalf("expected lastApplied=0 before application, got %d", lastApplied)
	}

	if lastQueued != 1 {
		t.Fatalf("expected lastQueued=1, got %d", lastQueued)
	}
}

func TestLastAppliedNeverDecreases(t *testing.T) {
	stateMachine := NewTestStateMachine(storage.NewMemEngine())

	node, err := NewRaftNode(
		0, 1, make(chan ApplyMsg, 10),
		NewMemPersister(), stateMachine,
	)
	if err != nil {
		t.Fatalf("failed to create Raft node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.log = append(node.log,
		LogEntry{
			Term: 1,
			Command: Command{
				Op: "SET", Key: "x", Value: "100",
			},
		},
		LogEntry{
			Term: 1,
			Command: Command{
				Op: "SET", Key: "y", Value: "200",
			},
		},
	)

	node.commitIndex = 2

	msgs := node.collectCommittedEntries()
	if len(msgs) != 2 {
		node.mu.Unlock()
		t.Fatalf("expected 2 ApplyMsgs, got %d", len(msgs))
	}

	lastQueued := node.lastQueued

	// Collecting again must not queue the same entries twice.
	msgs = node.collectCommittedEntries()
	node.mu.Unlock()

	if len(msgs) != 0 {
		t.Fatalf("expected no new ApplyMsgs, got %d", len(msgs))
	}

	lastApplied, err := stateMachine.LastApplied()
	if err != nil {
		t.Fatalf("failed to get last applied index: %v", err)
	}

	if lastApplied != 0 {
		t.Fatalf("expected lastApplied=0 before application, got %d", lastApplied)
	}

	if lastQueued != 2 {
		t.Fatalf("expected lastQueued=2, got %d", lastQueued)
	}
}

func TestWaitForApplyBlocksUntilApplied(t *testing.T) {
	stateMachine := NewTestStateMachine(storage.NewMemEngine())

	node, err := NewRaftNode(
		0, 1, make(chan ApplyMsg, 10),
		NewMemPersister(), stateMachine,
	)
	if err != nil {
		t.Fatalf("failed to create Raft node: %v", err)
	}
	defer node.Stop()

	done := make(chan error, 1)

	go func() {
		done <- node.waitForApply(1)
	}()

	// It must not return before index 1 is applied.
	select {
	case err := <-done:
		t.Fatalf("waitForApply returned before application: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Expected: still waiting.
	}

	// Simulate successful application.
	node.notifyApplied(1, nil)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected successful application, got: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waitForApply did not return after notifyApplied")
	}
}

func TestWaitForApplyTimeout(t *testing.T) {
	stateMachine := NewTestStateMachine(storage.NewMemEngine())

	node, err := NewRaftNode(
		0, 1, make(chan ApplyMsg, 10),
		NewMemPersister(), stateMachine,
	)
	if err != nil {
		t.Fatalf("failed to create Raft node: %v", err)
	}
	defer node.Stop()

	start := time.Now()

	err = node.waitForApply(1)

	elapsed := time.Since(start)

	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}

	if elapsed < 2*time.Second {
		t.Fatalf("waitForApply returned too early: elapsed=%v", elapsed)
	}

	// The timed-out result may remain registered because it can be
	// shared by multiple goroutines waiting for the same index.
	node.mu.Lock()
	result, exists := node.applyWaiters[1]
	node.mu.Unlock()

	if !exists {
		t.Fatal("expected shared ApplyResult to remain registered until notification")
	}

	if result == nil {
		t.Fatal("expected a non-nil ApplyResult")
	}

	select {
	case <-result.Done:
		t.Fatal("expected Done to remain open because index 1 was not applied")
	default:
		// Expected: no application notification has occurred.
	}
}

func TestNotifyAppliedReleasesWaiter(t *testing.T) {
	node, err := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create Raft node: %v", err)
	}
	defer node.Stop()

	done := make(chan error, 1)

	go func() {
		done <- node.waitForApply(1)
	}()

	// Give waitForApply a moment to register its waiter.
	time.Sleep(20 * time.Millisecond)

	node.mu.Lock()
	_, exists := node.applyWaiters[1]
	node.mu.Unlock()

	if !exists {
		t.Fatal("waiter was not registered")
	}

	// Simulate successful application.
	node.notifyApplied(1, nil)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waitForApply did not return after notifyApplied")
	}

	// The waiter should have been removed.
	node.mu.Lock()
	_, exists = node.applyWaiters[1]
	node.mu.Unlock()

	if exists {
		t.Fatal("waiter was not removed after notification")
	}
}

func TestCommittedCommandReleasesClient(t *testing.T) {
	applyChs := make([]chan ApplyMsg, 3)
	stateMachines := make([]StateMachine, 3)
	nodes := make([]*RaftNode, 3)

	// Create 3 nodes with their own state machines.
	for i := 0; i < 3; i++ {
		applyChs[i] = make(chan ApplyMsg, 10)
		engine := storage.NewMemEngine()
		stateMachines[i] = NewTestStateMachine(engine)

		node, err := NewRaftNode(
			i,
			3,
			applyChs[i],
			NewMemPersister(),
			stateMachines[i],
		)
		if err != nil {
			t.Fatalf("failed to create node %d: %v", i, err)
		}
		nodes[i] = node
	}

	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	// Connect peers.
	for _, node := range nodes {
		node.SetPeers(nodes)
	}

	// State-machine consumers.
	for i := 0; i < 3; i++ {
		go func(i int) {
			for msg := range applyChs[i] {
				if !msg.CommandValid {
					continue
				}

				err := stateMachines[i].Apply(msg)
				nodes[i].notifyApplied(msg.CommandIndex, err)
			}
		}(i)
	}

	leader := nodes[0]

	// Make node 0 the leader.
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1
	leader.nextIndex[0] = len(leader.log)
	leader.matchIndex[0] = 0
	leader.mu.Unlock()

	// Submit the client request asynchronously because Put()
	// waits for the command to be applied.
	done := make(chan error, 1)

	go func() {
		_, err := leader.Put(1, 1, "city", "Delhi")
		done <- err
	}()

	// Wait until the command appears in the leader's log.
	var index int
	deadline := time.Now().Add(time.Second)

	for time.Now().Before(deadline) {
		leader.mu.Lock()
		if len(leader.log) > 1 {
			index = len(leader.log) - 1
			leader.mu.Unlock()
			break
		}
		leader.mu.Unlock()

		time.Sleep(5 * time.Millisecond)
	}

	if index == 0 {
		t.Fatal("client command was not appended to leader log")
	}

	// Replicate the command to follower 1.
	leader.mu.Lock()
	args := AppendEntriesArgs{
		Term:         leader.currentTerm,
		LeaderID:     leader.me,
		PrevLogIndex: index - 1,
		PrevLogTerm:  leader.log[index-1].Term,
		Entries:      []LogEntry{leader.log[index]},
		LeaderCommit: 0,
	}
	leader.mu.Unlock()

	var reply AppendEntriesReply
	nodes[1].AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept AppendEntries")
	}

	// Record successful replication to follower 1.
	leader.mu.Lock()
	leader.matchIndex[1] = index
	leader.nextIndex[1] = index + 1

	// Leader + follower 1 form a majority.
	count := 0
	for i := range leader.peers {
		if leader.matchIndex[i] >= index {
			count++
		}
	}

	if count > len(leader.peers)/2 {
		leader.commitIndex = index
	}

	msgs := leader.collectCommittedEntries()
	leader.mu.Unlock()

	// Deliver the committed entry to the leader's state machine.
	for _, msg := range msgs {
		applyChs[0] <- msg
	}

	// Put() should return after successful application.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Put() returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Put() did not return after command was committed and applied")
	}

	value, found, err := stateMachines[0].Get([]byte("city"))
	if err != nil {
		t.Fatalf("failed to read state machine: %v", err)
	}
	if !found {
		t.Fatal("expected committed key to exist in state machine")
	}
	if string(value) != "Delhi" {
		t.Fatalf("expected value Delhi, got %s", string(value))
	}
}

func TestLeaderStepsDownOnHigherTermAppendEntriesReply(t *testing.T) {
	leader, err := NewRaftNode(
		0,
		2,
		make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create leader: %v", err)
	}
	defer leader.Stop()

	follower, err := NewRaftNode(
		1,
		2,
		make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create follower: %v", err)
	}
	defer follower.Stop()

	peers := []*RaftNode{leader, follower}
	leader.SetPeers(peers)
	follower.SetPeers(peers)

	// Leader starts in term 1 with one command.
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1

	leader.log = append(leader.log, LogEntry{
		Term: 1,
		Command: Command{
			Op:    "SET",
			Key:   "x",
			Value: "100",
		},
	})

	leader.nextIndex[1] = 1
	leader.matchIndex[0] = 1
	leader.mu.Unlock()

	// Follower is already in a newer term.
	follower.mu.Lock()
	follower.currentTerm = 2
	follower.mu.Unlock()

	// The follower should reject the old-term AppendEntries
	// and return its higher term.
	leader.broadcastAppendEntries()

	// Wait for the reply-processing goroutine.
	deadline := time.Now().Add(time.Second)

	for time.Now().Before(deadline) {
		leader.mu.Lock()
		role := leader.role
		term := leader.currentTerm
		commitIndex := leader.commitIndex
		leader.mu.Unlock()

		if role == Follower && term == 2 {
			if commitIndex != 0 {
				t.Fatalf(
					"old leader incorrectly advanced commitIndex=%d",
					commitIndex,
				)
			}
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatal("leader did not step down after receiving higher term")
}

func TestHeartbeatPreventsStaleElection(t *testing.T) {
	leader, err := NewRaftNode(
		0, 2, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create leader: %v", err)
	}
	defer leader.Stop()

	follower, err := NewRaftNode(
		1, 2, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create follower: %v", err)
	}
	defer follower.Stop()

	peers := []*RaftNode{leader, follower}
	leader.SetPeers(peers)
	follower.SetPeers(peers)

	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1
	leader.mu.Unlock()

	follower.mu.Lock()
	follower.currentTerm = 1
	follower.role = Follower
	follower.electionTimeout = 200 * time.Millisecond
	follower.lastResetTime = time.Now().Add(-190 * time.Millisecond)
	prevLogTerm := follower.log[0].Term
	follower.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  prevLogTerm,
		Entries:      nil,
		LeaderCommit: 0,
	}

	var reply AppendEntriesReply
	follower.AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept valid heartbeat")
	}

	follower.mu.Lock()
	elapsed := time.Since(follower.lastResetTime)
	timeout := follower.electionTimeout
	role := follower.role
	follower.mu.Unlock()

	if role != Follower {
		t.Fatalf("follower changed role after heartbeat: %v", role)
	}

	if elapsed >= timeout {
		t.Fatalf(
			"heartbeat did not reset election timer: elapsed=%v timeout=%v",
			elapsed, timeout,
		)
	}
}

func TestStaleRequestVoteDoesNotResetElectionTimer(t *testing.T) {
	node, err := NewRaftNode(
		0, 2, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.currentTerm = 3
	node.role = Follower
	node.electionTimeout = 200 * time.Millisecond
	node.lastResetTime = time.Now().Add(-190 * time.Millisecond)
	before := node.lastResetTime
	node.mu.Unlock()

	args := RequestVoteArgs{
		Term:         2,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	var reply RequestVoteReply
	node.RequestVote(&args, &reply)

	node.mu.Lock()
	after := node.lastResetTime
	role := node.role
	term := node.currentTerm
	node.mu.Unlock()

	if reply.VoteGranted {
		t.Fatal("stale RequestVote should not be granted")
	}

	if term != 3 {
		t.Fatalf("currentTerm changed after stale RequestVote: got %d", term)
	}

	if role != Follower {
		t.Fatalf("role changed after stale RequestVote: got %v", role)
	}

	if !after.Equal(before) {
		t.Fatalf(
			"stale RequestVote reset election timer: before=%v after=%v",
			before, after,
		)
	}
}

func TestHigherTermRequestVoteUpdatesTermAndStepsDown(t *testing.T) {
	node, err := NewRaftNode(
		0, 2, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.currentTerm = 3
	node.role = Candidate
	node.votedFor = 0
	node.mu.Unlock()

	args := RequestVoteArgs{
		Term:         4,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	var reply RequestVoteReply
	node.RequestVote(&args, &reply)

	node.mu.Lock()
	term := node.currentTerm
	role := node.role
	votedFor := node.votedFor
	node.mu.Unlock()

	if term != 4 {
		t.Fatalf("expected currentTerm=4, got %d", term)
	}

	if role != Follower {
		t.Fatalf("expected Follower, got %v", role)
	}

	if votedFor != 1 {
		t.Fatalf("expected vote for candidate 1, got %d", votedFor)
	}

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted in the higher term")
	}

	if reply.Term != 4 {
		t.Fatalf("expected reply term=4, got %d", reply.Term)
	}
}

func TestNodeVotesForOnlyOneCandidatePerTerm(t *testing.T) {
	node, err := NewRaftNode(
		0, 3, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.currentTerm = 5
	node.role = Follower
	node.votedFor = -1
	node.mu.Unlock()

	firstRequest := RequestVoteArgs{
		Term:         5,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	var firstReply RequestVoteReply
	node.RequestVote(&firstRequest, &firstReply)

	if !firstReply.VoteGranted {
		t.Fatal("expected first candidate to receive the vote")
	}

	secondRequest := RequestVoteArgs{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	var secondReply RequestVoteReply
	node.RequestVote(&secondRequest, &secondReply)

	if secondReply.VoteGranted {
		t.Fatal("node granted votes to two candidates in the same term")
	}

	node.mu.Lock()
	votedFor := node.votedFor
	currentTerm := node.currentTerm
	node.mu.Unlock()

	if votedFor != 1 {
		t.Fatalf("expected vote for candidate 1, got %d", votedFor)
	}

	if currentTerm != 5 {
		t.Fatalf("currentTerm changed unexpectedly: got %d", currentTerm)
	}
}

func TestCandidateBecomesLeaderAfterMajorityVotes(t *testing.T) {
	nodes := make([]*RaftNode, 3)

	for i := range nodes {
		node, err := NewRaftNode(
			i, 3, make(chan ApplyMsg, 10),
			NewMemPersister(),
			NewTestStateMachine(storage.NewMemEngine()),
		)
		if err != nil {
			t.Fatalf("failed to create node %d: %v", i, err)
		}
		nodes[i] = node
	}
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	for _, node := range nodes {
		node.SetPeers(nodes)
	}

	candidate := nodes[0]

	candidate.mu.Lock()
	candidate.role = Candidate
	candidate.currentTerm = 1
	candidate.votedFor = candidate.me
	candidate.resetElectionTimeout()

	term := candidate.currentTerm
	me := candidate.me
	lastLogIndex := len(candidate.log) - 1
	lastLogTerm := candidate.log[lastLogIndex].Term
	candidate.mu.Unlock()

	votesReceived := 1 // Candidate's own vote.

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	var reply RequestVoteReply
	nodes[1].RequestVote(&args, &reply)

	if !reply.VoteGranted {
		t.Fatal("expected node 1 to grant its vote")
	}

	votesReceived++

	if votesReceived <= len(nodes)/2 {
		t.Fatal("expected majority after receiving second vote")
	}

	// Simulate the transition after a majority is received.
	candidate.mu.Lock()

	if candidate.role == Candidate &&
		candidate.currentTerm == term &&
		votesReceived > len(nodes)/2 {

		candidate.role = Leader

		for j := range candidate.peers {
			candidate.nextIndex[j] = len(candidate.log)
			candidate.matchIndex[j] = 0
		}
	}

	role := candidate.role
	candidate.mu.Unlock()

	if role != Leader {
		t.Fatalf("candidate should become leader after majority, got %v", role)
	}
}

func TestLeaderDecrementsNextIndexAndRepairsFollower(t *testing.T) {
	leader, err := NewRaftNode(
		0, 2, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create leader: %v", err)
	}
	defer leader.Stop()

	follower, err := NewRaftNode(
		1, 2, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create follower: %v", err)
	}
	defer follower.Stop()

	peers := []*RaftNode{leader, follower}
	leader.SetPeers(peers)
	follower.SetPeers(peers)

	// Leader: [dummy, A(term=1), B(term=2)]
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 2
	leader.log = append(leader.log,
		LogEntry{
			Term:    1,
			Command: Command{Op: "SET", Key: "x", Value: "A"},
		},
		LogEntry{
			Term:    2,
			Command: Command{Op: "SET", Key: "x", Value: "B"},
		},
	)
	leader.nextIndex[1] = 3
	leader.matchIndex[0] = 2
	leader.mu.Unlock()

	// Follower: [dummy, A(term=1), X(term=1)]
	follower.mu.Lock()
	follower.currentTerm = 2
	follower.log = append(follower.log,
		LogEntry{
			Term:    1,
			Command: Command{Op: "SET", Key: "x", Value: "A"},
		},
		LogEntry{
			Term:    1,
			Command: Command{Op: "SET", Key: "x", Value: "X"},
		},
	)
	follower.mu.Unlock()

	leader.broadcastAppendEntries()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		leader.mu.Lock()
		nextIndex := leader.nextIndex[1]
		leader.mu.Unlock()

		if nextIndex == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	leader.mu.Lock()
	nextIndex := leader.nextIndex[1]
	leader.mu.Unlock()

	if nextIndex != 2 {
		t.Fatalf("expected nextIndex[1] to decrease to 2, got %d", nextIndex)
	}

	// Retry with the decremented nextIndex.
	leader.broadcastAppendEntries()

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		follower.mu.Lock()
		repaired := len(follower.log) == 3 &&
			follower.log[2].Term == 2 &&
			follower.log[2].Command.Value == "B"
		follower.mu.Unlock()

		if repaired {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	follower.mu.Lock()
	defer follower.mu.Unlock()

	if len(follower.log) != 3 {
		t.Fatalf("expected follower log length 3, got %d", len(follower.log))
	}
	if follower.log[2].Term != 2 {
		t.Fatalf("expected index 2 term 2, got %d", follower.log[2].Term)
	}
	if follower.log[2].Command.Value != "B" {
		t.Fatalf("expected index 2 value B, got %q", follower.log[2].Command.Value)
	}
}

func TestLeaderUpdatesMatchAndNextIndexAfterReplication(t *testing.T) {
	leader, err := NewRaftNode(
		0, 2, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create leader: %v", err)
	}
	defer leader.Stop()

	follower, err := NewRaftNode(
		1, 2, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create follower: %v", err)
	}
	defer follower.Stop()

	peers := []*RaftNode{leader, follower}
	leader.SetPeers(peers)
	follower.SetPeers(peers)

	// Leader: [dummy, A(term=1)]
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1
	leader.log = append(leader.log, LogEntry{
		Term:    1,
		Command: Command{Op: "SET", Key: "x", Value: "10"},
	})
	leader.nextIndex[1] = 1
	leader.matchIndex[0] = 1
	leader.mu.Unlock()

	leader.broadcastAppendEntries()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		leader.mu.Lock()
		matchIndex := leader.matchIndex[1]
		nextIndex := leader.nextIndex[1]
		leader.mu.Unlock()

		if matchIndex == 1 && nextIndex == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	leader.mu.Lock()
	defer leader.mu.Unlock()

	if leader.matchIndex[1] != 1 {
		t.Fatalf("expected matchIndex[1] = 1, got %d", leader.matchIndex[1])
	}
	if leader.nextIndex[1] != 2 {
		t.Fatalf("expected nextIndex[1] = 2, got %d", leader.nextIndex[1])
	}
}

func TestFollowerAdvancesCommitIndexFromLeaderCommit(t *testing.T) {
	leader, err := NewRaftNode(
		0, 2, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create leader: %v", err)
	}
	defer leader.Stop()

	followerSM := NewTestStateMachine(storage.NewMemEngine())
	followerApplyCh := make(chan ApplyMsg, 10)

	follower, err := NewRaftNode(
		1, 2, followerApplyCh,
		NewMemPersister(), followerSM,
	)
	if err != nil {
		t.Fatalf("failed to create follower: %v", err)
	}
	defer follower.Stop()

	peers := []*RaftNode{leader, follower}
	leader.SetPeers(peers)
	follower.SetPeers(peers)

	leader.mu.Lock()
	leader.currentTerm = 1
	leader.log = append(leader.log, LogEntry{
		Term:    1,
		Command: Command{Op: "SET", Key: "x", Value: "10"},
	})
	leader.mu.Unlock()

	follower.mu.Lock()
	follower.currentTerm = 1
	follower.log = append(follower.log, LogEntry{
		Term:    1,
		Command: Command{Op: "SET", Key: "x", Value: "10"},
	})
	follower.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 1,
	}

	var reply AppendEntriesReply
	follower.AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept valid AppendEntries")
	}

	// Apply the committed message through the StateMachine interface.
	select {
	case msg := <-followerApplyCh:
		if err := followerSM.Apply(msg); err != nil {
			t.Fatalf("apply committed entry: %v", err)
		}
		follower.notifyApplied(msg.CommandIndex, nil)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for committed ApplyMsg")
	}

	follower.mu.Lock()
	commitIndex := follower.commitIndex
	follower.mu.Unlock()

	if commitIndex != 1 {
		t.Fatalf("expected follower commitIndex = 1, got %d", commitIndex)
	}

	value, found, err := followerSM.Get([]byte("x"))
	if err != nil {
		t.Fatalf("get committed value: %v", err)
	}
	if !found || string(value) != "10" {
		t.Fatalf("follower state = (%q, %v), want (%q, true)",
			string(value), found, "10")
	}
}

func TestFollowerDoesNotCommitBeyondItsLog(t *testing.T) {
	followerSM := NewTestStateMachine(storage.NewMemEngine())

	follower, err := NewRaftNode(
		1, 2, make(chan ApplyMsg, 10),
		NewMemPersister(), followerSM,
	)
	if err != nil {
		t.Fatalf("failed to create follower: %v", err)
	}
	defer follower.Stop()

	follower.mu.Lock()
	follower.currentTerm = 1

	// Follower has one real entry at index 1.
	follower.log = append(follower.log, LogEntry{
		Term:    1,
		Command: Command{Op: "SET", Key: "x", Value: "10"},
	})
	follower.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 5,
	}

	var reply AppendEntriesReply
	follower.AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept valid heartbeat")
	}

	follower.mu.Lock()
	defer follower.mu.Unlock()

	expectedCommitIndex := len(follower.log) - 1
	if follower.commitIndex != expectedCommitIndex {
		t.Fatalf("expected commitIndex = %d, got %d",
			expectedCommitIndex, follower.commitIndex)
	}

	if follower.commitIndex >= len(follower.log) {
		t.Fatalf("commitIndex %d exceeds last log index %d",
			follower.commitIndex, len(follower.log)-1)
	}
}

func TestFollowerAcceptsHeartbeatWhenLeaderCommitExceedsLog(t *testing.T) {
	follower, err := NewRaftNode(
		1, 2, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create follower: %v", err)
	}
	defer follower.Stop()

	follower.mu.Lock()
	follower.currentTerm = 1
	follower.role = Follower
	follower.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 5,
	}

	var reply AppendEntriesReply
	follower.AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept a valid heartbeat")
	}

	follower.mu.Lock()
	commitIndex := follower.commitIndex
	lastLogIndex := len(follower.log) - 1
	follower.mu.Unlock()

	if commitIndex > lastLogIndex {
		t.Fatalf("commitIndex %d exceeds last log index %d",
			commitIndex, lastLogIndex)
	}
}

func TestReadProbeCurrentTerm(t *testing.T) {
	node, err := NewRaftNode(
		0, 1, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.currentTerm = 5
	node.role = Follower
	node.lastResetTime = time.Now().Add(-time.Second)
	oldReset := node.lastResetTime
	node.mu.Unlock()

	args := ReadProbeArgs{
		Term:     5,
		LeaderID: 0,
	}

	var reply ReadProbeReply
	node.ReadProbe(&args, &reply)

	node.mu.Lock()
	defer node.mu.Unlock()

	if reply.Term != 5 {
		t.Fatalf("expected reply term 5, got %d", reply.Term)
	}
	if node.role != Follower {
		t.Fatalf("expected follower, got %v", node.role)
	}
	if !node.lastResetTime.After(oldReset) {
		t.Fatal("expected election timer to reset")
	}
}

func TestReadProbeHigherTerm(t *testing.T) {
	node, err := NewRaftNode(
		0, 1, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.currentTerm = 3
	node.role = Candidate
	node.votedFor = 1
	node.mu.Unlock()

	args := ReadProbeArgs{
		Term:     5,
		LeaderID: 0,
	}

	var reply ReadProbeReply
	node.ReadProbe(&args, &reply)

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.currentTerm != 5 {
		t.Fatalf("expected term 5, got %d", node.currentTerm)
	}
	if node.role != Follower {
		t.Fatalf("expected follower, got %v", node.role)
	}
	if node.votedFor != -1 {
		t.Fatalf("expected votedFor to reset to -1, got %d", node.votedFor)
	}
	if reply.Term != 5 {
		t.Fatalf("expected reply term 5, got %d", reply.Term)
	}
}

func TestReadProbeStaleTerm(t *testing.T) {
	node, err := NewRaftNode(
		0, 1, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.currentTerm = 5
	node.role = Follower
	node.electionTimeout = 200 * time.Millisecond
	node.lastResetTime = time.Now().Add(-190 * time.Millisecond)
	before := node.lastResetTime
	node.mu.Unlock()

	args := ReadProbeArgs{
		Term:     4,
		LeaderID: 0,
	}

	var reply ReadProbeReply
	node.ReadProbe(&args, &reply)

	node.mu.Lock()
	defer node.mu.Unlock()

	if reply.Term != 5 {
		t.Fatalf("expected reply term 5, got %d", reply.Term)
	}
	if node.currentTerm != 5 {
		t.Fatalf("expected term to remain 5, got %d", node.currentTerm)
	}
	if !node.lastResetTime.Equal(before) {
		t.Fatal("stale probe should not reset election timer")
	}
}

func TestReadIndexMajority(t *testing.T) {
	nodes := make([]*RaftNode, 3)

	for i := range nodes {
		node, err := NewRaftNode(
			i, 3, make(chan ApplyMsg, 10),
			NewMemPersister(),
			NewTestStateMachine(storage.NewMemEngine()),
		)
		if err != nil {
			t.Fatalf("failed to create node %d: %v", i, err)
		}
		nodes[i] = node
	}
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	for i := range nodes {
		nodes[i].SetPeers(nodes)
	}

	leader := nodes[0]

	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 5

	// Build a log containing a committed NOOP from the current term.
	leader.log = make([]LogEntry, 8)
	leader.log[7] = LogEntry{
		Index: 7,
		Term:  5,
		Command: Command{
			Op: "NOOP",
		},
	}
	leader.commitIndex = 7
	leader.mu.Unlock()

	readIndex, err := leader.readIndex()
	if err != nil {
		t.Fatalf("readIndex failed: %v", err)
	}
	if readIndex != 7 {
		t.Fatalf("expected readIndex 7, got %d", readIndex)
	}
}

func TestReadIndexSingleNode(t *testing.T) {
	node, err := NewRaftNode(
		0, 1, make(chan ApplyMsg, 10),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 3

	node.log = node.log[:1]
	node.log = append(node.log,
		LogEntry{
			Index: 1,
			Term:  3,
			Command: Command{
				Op: "NOOP",
			},
		},
		LogEntry{Index: 2},
		LogEntry{Index: 3},
		LogEntry{Index: 4},
		LogEntry{Index: 5},
	)
	node.commitIndex = 5
	node.mu.Unlock()

	readIndex, err := node.readIndex()
	if err != nil {
		t.Fatalf("readIndex failed: %v", err)
	}
	if readIndex != 5 {
		t.Fatalf("expected readIndex 5, got %d", readIndex)
	}
}

func TestReadIndexHigherTermStepsDown(t *testing.T) {
	nodes := make([]*RaftNode, 2)

	for i := range nodes {
		node, err := NewRaftNode(
			i, 2, make(chan ApplyMsg, 10),
			NewMemPersister(),
			NewTestStateMachine(storage.NewMemEngine()),
		)
		if err != nil {
			t.Fatalf("failed to create node %d: %v", i, err)
		}
		nodes[i] = node
	}
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	for i := range nodes {
		nodes[i].SetPeers(nodes)
	}

	nodes[0].mu.Lock()
	nodes[0].role = Leader
	nodes[0].currentTerm = 5
	nodes[0].commitIndex = 3
	nodes[0].mu.Unlock()

	nodes[1].mu.Lock()
	nodes[1].currentTerm = 6
	nodes[1].mu.Unlock()

	_, err := nodes[0].readIndex()
	if err != ErrWrongLeader {
		t.Fatalf("expected ErrWrongLeader, got %v", err)
	}

	nodes[0].mu.Lock()
	defer nodes[0].mu.Unlock()

	if nodes[0].currentTerm != 6 {
		t.Fatalf("expected term 6, got %d", nodes[0].currentTerm)
	}
	if nodes[0].role != Follower {
		t.Fatalf("expected follower, got %v", nodes[0].role)
	}
}

func TestGetWaitsForReadIndexToApply(t *testing.T) {
	statemachine := NewTestStateMachine(storage.NewMemEngine())

	node, err := NewRaftNode(
		0, 1, make(chan ApplyMsg, 10),
		NewMemPersister(), statemachine,
	)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 1
	node.log = node.log[:1]
	node.log = append(node.log,
		LogEntry{
			Index: 1,
			Term:  1,
			Command: Command{
				Op: "NOOP",
			},
		},
		LogEntry{
			Index: 2,
			Term:  1,
			Command: Command{
				Op:    "SET",
				Key:   "Country",
				Value: "Index",
			},
		},
	)
	node.commitIndex = 2
	node.mu.Unlock()

	done := make(chan struct{})

	go func() {
		_, _, _ = node.Get(1, 1, "key")
		close(done)
	}()

	// Wait until Get has had an opportunity to block.
	time.Sleep(50 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("Get returned before the read index was applied")
	default:
	}

	// Simulate application reaching index 2.
	lastApplied, err := statemachine.LastApplied()
	if err != nil {
		t.Fatalf("failed to read last applied index: %v", err)
	}

	if lastApplied < 2 {
		// Apply the entries in order so the state machine can advance.
		if err := statemachine.Apply(ApplyMsg{
			CommandValid: true,
			CommandIndex: 1,
			Command: Command{
				Op: "NOOP",
			},
		}); err != nil {
			t.Fatalf("failed to apply entry 1: %v", err)
		}

		if err := statemachine.Apply(ApplyMsg{
			CommandValid: true,
			CommandIndex: 2,
			Command: Command{
				Op:    "SET",
				Key:   "Country",
				Value: "Index",
			},
		}); err != nil {
			t.Fatalf("failed to apply entry 2: %v", err)
		}
	}

	node.notifyApplied(2, nil)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Get did not return after the read index was applied")
	}
}

func TestGetWaitsForReadIndexApplication(t *testing.T) {
	nodes := make([]*RaftNode, 3)
	stateMachines := make([]StateMachine, 3)

	for i := range nodes {
		stateMachines[i] = NewTestStateMachine(storage.NewMemEngine())

		node, err := NewRaftNode(
			i, 3, make(chan ApplyMsg, 10),
			NewMemPersister(), stateMachines[i],
		)
		if err != nil {
			t.Fatalf("failed to create node %d: %v", i, err)
		}
		nodes[i] = node
	}
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	for i := range nodes {
		nodes[i].SetPeers(nodes)
	}

	leader := nodes[0]
	leaderSM := stateMachines[0]

	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1

	leader.log = leader.log[:1]
	leader.log = append(leader.log,
		LogEntry{
			Index: 1,
			Term:  1,
			Command: Command{
				Op: "NOOP",
			},
		},
		LogEntry{
			Index: 2,
			Term:  1,
			Command: Command{
				Op:    "SET",
				Key:   "x",
				Value: "value",
			},
		},
	)
	leader.commitIndex = 2
	leader.mu.Unlock()

	done := make(chan struct{})

	var (
		value  string
		found  bool
		getErr error
	)

	go func() {
		value, found, getErr = leader.Get(1, 1, "x")
		close(done)
	}()

	// Give Get time to perform ReadIndex and wait for application.
	time.Sleep(50 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("Get returned before the read index was applied")
	default:
	}

	// Apply entries in order through the StateMachine interface.
	if err := leaderSM.Apply(ApplyMsg{
		CommandValid: true,
		CommandIndex: 1,
		Command: Command{
			Op: "NOOP",
		},
	}); err != nil {
		t.Fatalf("failed to apply entry 1: %v", err)
	}

	if err := leaderSM.Apply(ApplyMsg{
		CommandValid: true,
		CommandIndex: 2,
		Command: Command{
			Op:    "SET",
			Key:   "x",
			Value: "value",
		},
	}); err != nil {
		t.Fatalf("failed to apply entry 2: %v", err)
	}

	leader.notifyApplied(2, nil)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Get did not return after the read index was applied")
	}

	if getErr != nil {
		t.Fatalf("Get returned error: %v", getErr)
	}
	if !found {
		t.Fatal("expected key to be found")
	}
	if value != "value" {
		t.Fatalf("expected value %q, got %q", "value", value)
	}
}

func TestGetFailsIfLeadershipLostBeforeRead(t *testing.T) {
	sm := NewTestStateMachine(storage.NewMemEngine())

	node, err := NewRaftNode(
		0, 1, make(chan ApplyMsg, 10),
		NewMemPersister(), sm,
	)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 1
	node.log = node.log[:1]
	node.log = append(node.log,
		LogEntry{
			Index:   1,
			Term:    1,
			Command: Command{Op: "NOOP"},
		},
		LogEntry{
			Index: 2,
			Term:  1,
			Command: Command{
				Op: "SET", Key: "x", Value: "value",
			},
		},
	)
	node.commitIndex = 2
	node.mu.Unlock()

	done := make(chan struct{})
	var getErr error

	go func() {
		_, _, getErr = node.Get(1, 1, "x")
		close(done)
	}()

	// Wait until Get has registered its apply waiter.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		node.mu.Lock()
		registered := len(node.applyWaiters) > 0
		node.mu.Unlock()

		if registered {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	node.mu.Lock()
	registered := len(node.applyWaiters) > 0
	node.role = Follower
	node.mu.Unlock()

	if !registered {
		t.Fatal("Get did not register an apply waiter")
	}

	// Simulate the read index being applied after leadership is lost.
	node.notifyApplied(2, nil)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Get did not return after application notification")
	}

	if getErr != ErrWrongLeader {
		t.Fatalf("expected ErrWrongLeader, got %v", getErr)
	}
}

func TestApplyLoopHandlesOutOfOrderMessages(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	sm := NewTestStateMachine(storage.NewMemEngine())

	node, err := NewRaftNode(
		0, 1, applyCh,
		NewMemPersister(), sm,
	)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.commitIndex = 3
	node.mu.Unlock()

	go node.applyLoop()

	applyCh <- ApplyMsg{
		CommandValid: true,
		CommandIndex: 1,
		CommandTerm:  1,
		Command: Command{
			Op: "SET", Key: "one", Value: "1",
		},
	}
	applyCh <- ApplyMsg{
		CommandValid: true,
		CommandIndex: 3,
		CommandTerm:  1,
		Command: Command{
			Op: "SET", Key: "three", Value: "3",
		},
	}
	applyCh <- ApplyMsg{
		CommandValid: true,
		CommandIndex: 2,
		CommandTerm:  1,
		Command: Command{
			Op: "SET", Key: "two", Value: "2",
		},
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		lastApplied, err := sm.LastApplied()
		if err != nil {
			t.Fatalf("read last applied: %v", err)
		}
		if lastApplied == 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	lastApplied, err := sm.LastApplied()
	if err != nil {
		t.Fatalf("read last applied: %v", err)
	}
	if lastApplied != 3 {
		t.Fatalf("expected LastApplied() = 3, got %d", lastApplied)
	}

	for _, tc := range []struct {
		key   string
		value string
	}{
		{key: "one", value: "1"},
		{key: "two", value: "2"},
		{key: "three", value: "3"},
	} {
		value, found, err := sm.Get([]byte(tc.key))
		if err != nil {
			t.Fatalf("Get(%q): %v", tc.key, err)
		}
		if !found || string(value) != tc.value {
			t.Fatalf(
				"Get(%q) = %q, found=%v; want %q",
				tc.key, string(value), found, tc.value,
			)
		}
	}
}

func TestNewLeaderAppendsCurrentTermNoop(t *testing.T) {
	nodes := make([]*RaftNode, 3)

	for i := range nodes {
		node, err := NewRaftNode(
			i, 3, make(chan ApplyMsg, 20),
			NewMemPersister(),
			NewTestStateMachine(storage.NewMemEngine()),
		)
		if err != nil {
			t.Fatalf("create node %d: %v", i, err)
		}
		nodes[i] = node
	}
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	for _, node := range nodes {
		node.SetPeers(nodes)
		node.StartBackground()
	}

	candidate := nodes[0]

	candidate.mu.Lock()
	candidate.currentTerm = 0
	candidate.role = Follower
	candidate.lastResetTime = time.Now().Add(-time.Second)
	candidate.electionTimeout = 100 * time.Millisecond
	candidate.mu.Unlock()

	candidate.startElection()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		candidate.mu.Lock()
		becameLeader := candidate.role == Leader
		candidate.mu.Unlock()

		if becameLeader {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	candidate.mu.Lock()
	if candidate.role != Leader {
		candidate.mu.Unlock()
		t.Fatal("candidate did not become leader")
	}

	lastIndex := len(candidate.log) - 1
	if lastIndex < 1 {
		candidate.mu.Unlock()
		t.Fatal("new leader did not append a no-op entry")
	}

	entry := candidate.log[lastIndex]
	term := candidate.currentTerm
	candidate.mu.Unlock()

	if entry.Term != term {
		t.Fatalf("no-op term = %d, current term = %d", entry.Term, term)
	}
	if entry.Command.Op != "NOOP" {
		t.Fatalf("expected last entry to be NOOP, got %q", entry.Command.Op)
	}

	// Wait for the no-op to be committed by the majority.
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		candidate.mu.Lock()
		committed := candidate.commitIndex >= lastIndex
		candidate.mu.Unlock()

		if committed {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	candidate.mu.Lock()
	commitIndex := candidate.commitIndex
	candidate.mu.Unlock()

	t.Fatalf(
		"expected no-op at index %d to be committed, commitIndex=%d",
		lastIndex, commitIndex,
	)
}

func TestGetFailsIfTermChangesBeforeRead(t *testing.T) {
	sm := NewTestStateMachine(storage.NewMemEngine())

	node, err := NewRaftNode(
		0, 1, make(chan ApplyMsg, 10),
		NewMemPersister(), sm,
	)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 1
	node.log = node.log[:1]
	node.log = append(node.log,
		LogEntry{
			Index:   1,
			Term:    1,
			Command: Command{Op: "NOOP"},
		},
		LogEntry{
			Index: 2,
			Term:  1,
			Command: Command{
				Op: "SET", Key: "x", Value: "value",
			},
		},
	)
	node.commitIndex = 2
	node.mu.Unlock()

	done := make(chan struct{})
	var getErr error

	go func() {
		_, _, getErr = node.Get(1, 1, "x")
		close(done)
	}()

	// Wait for Get to register its waiter.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		node.mu.Lock()
		registered := len(node.applyWaiters) > 0
		node.mu.Unlock()

		if registered {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	node.mu.Lock()
	registered := len(node.applyWaiters) > 0
	node.currentTerm = 2
	node.role = Leader
	node.mu.Unlock()

	if !registered {
		t.Fatal("Get did not register an apply waiter")
	}

	node.notifyApplied(2, nil)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Get did not return")
	}

	if getErr != ErrWrongLeader {
		t.Fatalf("expected ErrWrongLeader, got %v", getErr)
	}
}

func TestReadIndexReturnsWhenFollowerDoesNotRespond(t *testing.T) {
	nodes := make([]*RaftNode, 3)

	for i := range nodes {
		node, err := NewRaftNode(
			i, 3, make(chan ApplyMsg, 10),
			NewMemPersister(),
			NewTestStateMachine(storage.NewMemEngine()),
		)
		if err != nil {
			t.Fatalf("create node %d: %v", i, err)
		}
		nodes[i] = node
	}
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	for _, node := range nodes {
		node.SetPeers(nodes)
	}

	leader := nodes[0]
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 5
	leader.log = make([]LogEntry, 2)
	leader.log[1] = LogEntry{
		Index:   1,
		Term:    5,
		Command: Command{Op: "NOOP"},
	}
	leader.commitIndex = 1
	leader.mu.Unlock()

	// Block node 2's ReadProbe handler.
	nodes[2].mu.Lock()
	defer nodes[2].mu.Unlock()

	done := make(chan error, 1)
	go func() {
		_, err := leader.readIndex()
		done <- err
	}()

	select {
	case err := <-done:
		// The leader's own acknowledgement and node 1's
		// acknowledgement form a majority of three.
		if err != nil {
			t.Fatalf("readIndex failed despite a majority: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("readIndex did not return after reaching a majority")
	}
}

func TestReadIndexFailsWhenMajorityUnavailable(t *testing.T) {
	nodes := make([]*RaftNode, 3)

	for i := range nodes {
		node, err := NewRaftNode(
			i, 3, make(chan ApplyMsg, 10),
			NewMemPersister(),
			NewTestStateMachine(storage.NewMemEngine()),
		)
		if err != nil {
			t.Fatalf("create node %d: %v", i, err)
		}
		nodes[i] = node
	}
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	for _, node := range nodes {
		node.SetPeers(nodes)
	}

	leader := nodes[0]
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 5
	leader.log = make([]LogEntry, 2)
	leader.log[1] = LogEntry{
		Index:   1,
		Term:    5,
		Command: Command{Op: "NOOP"},
	}
	leader.commitIndex = 1
	leader.mu.Unlock()

	// Block both follower handlers.
	nodes[1].mu.Lock()
	nodes[2].mu.Lock()

	done := make(chan error, 1)
	go func() {
		_, err := leader.readIndex()
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected readIndex to fail without a majority")
		}
	case <-time.After(time.Second):
		t.Fatal("readIndex did not return when a majority was unavailable")
	}

	nodes[2].mu.Unlock()
	nodes[1].mu.Unlock()
}

func TestThreeNodeClusterRecoversCommittedValueAfterRestart(t *testing.T) {
	const n = 3

	// Keep the same persisters across reconstruction.
	persisters := make([]Persister, n)
	for i := range persisters {
		persisters[i] = NewMemPersister()
	}

	startCluster := func() ([]*RaftNode, []StateMachine) {
		nodes := make([]*RaftNode, n)
		stateMachines := make([]StateMachine, n)

		for i := 0; i < n; i++ {
			stateMachines[i] = NewTestStateMachine(storage.NewMemEngine())

			node, err := NewRaftNode(
				i,
				n,
				make(chan ApplyMsg, 100),
				persisters[i],
				stateMachines[i],
			)
			if err != nil {
				// Clean up nodes that were already created.
				for j := 0; j < i; j++ {
					nodes[j].Stop()
				}
				t.Fatalf("create node %d: %v", i, err)
			}

			nodes[i] = node
		}

		for _, node := range nodes {
			node.SetPeers(nodes)
		}

		for _, node := range nodes {
			go node.applyLoop()
			node.StartBackground()
		}

		return nodes, stateMachines
	}

	stopCluster := func(nodes []*RaftNode) {
		for _, node := range nodes {
			node.Stop()
		}
	}

	waitForLeader := func(nodes []*RaftNode) int {
		deadline := time.Now().Add(5 * time.Second)

		for time.Now().Before(deadline) {
			leader := -1
			leaderCount := 0

			for i, node := range nodes {
				node.mu.Lock()
				isLeader := node.role == Leader
				node.mu.Unlock()

				if isLeader {
					leader = i
					leaderCount++
				}
			}

			if leaderCount == 1 {
				return leader
			}

			time.Sleep(25 * time.Millisecond)
		}

		return -1
	}

	// Start the original cluster.
	nodes, _ := startCluster()
	leader := waitForLeader(nodes)

	if leader == -1 {
		stopCluster(nodes)
		t.Fatal("cluster failed to elect a leader")
	}

	// Commit a value before reconstruction.
	ok, err := nodes[leader].Put(1, 1, "language", "Go")
	if err != nil || !ok {
		stopCluster(nodes)
		t.Fatalf("write before reconstruction failed: ok=%v, err=%v", ok, err)
	}

	// Shut down the original nodes.
	stopCluster(nodes)
	time.Sleep(100 * time.Millisecond)

	// Reconstruct nodes with the same persisters but fresh state machines.
	nodes, _ = startCluster()
	t.Cleanup(func() {
		stopCluster(nodes)
	})

	leader = waitForLeader(nodes)
	if leader == -1 {
		t.Fatal("cluster failed to elect a leader after reconstruction")
	}

	// A current-term entry allows Raft to commit prior entries in the log.
	ok, err = nodes[leader].Put(1, 2, "restart-check", "ok")
	if err != nil || !ok {
		t.Fatalf("write after reconstruction failed: ok=%v, err=%v", ok, err)
	}

	// Verify that the previously committed value is present again.
	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		value, found, err := nodes[leader].Get(1, 3, "language")
		if err == nil && found && value == "Go" {
			return
		}

		time.Sleep(25 * time.Millisecond)
	}

	t.Fatal("previously committed value was not recovered after reconstruction")
}

func TestStartDeduplicatesRepeatedRequest(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	sm := NewTestStateMachine(storage.NewMemEngine())

	node, err := NewRaftNode(
		0, 1, applyCh,
		NewMemPersister(), sm,
	)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 1
	node.mu.Unlock()

	go node.applyLoop()

	command := Command{
		ClientID:  3,
		SeqNumber: 1,
		Op:        "SET",
		Key:       "framework",
		Value:     "RaftKV",
	}

	// First request.
	index1, term1, err := node.Start(command)
	if err != nil {
		t.Fatalf("first request was rejected: %v", err)
	}

	// Wait for application using the StateMachine interface.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		lastApplied, err := sm.LastApplied()
		if err != nil {
			t.Fatalf("read last applied: %v", err)
		}
		if lastApplied >= index1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	lastApplied, err := sm.LastApplied()
	if err != nil {
		t.Fatalf("read last applied: %v", err)
	}
	if lastApplied < index1 {
		t.Fatalf("timed out waiting for index %d to be applied", index1)
	}

	value, found, err := sm.Get([]byte("framework"))
	if err != nil {
		t.Fatalf("read stored value: %v", err)
	}
	if !found || string(value) != "RaftKV" {
		t.Fatalf("expected value %q, got %q", "RaftKV", string(value))
	}

	node.mu.Lock()
	logLenAfterFirst := len(node.log)
	node.mu.Unlock()

	// Retry the exact same request.
	index2, term2, err := node.Start(command)
	if err != nil {
		t.Fatalf("duplicate request was rejected: %v", err)
	}

	node.mu.Lock()
	logLenAfterSecond := len(node.log)
	node.mu.Unlock()

	if index1 != index2 {
		t.Fatalf("expected same index, got %d and %d", index1, index2)
	}
	if term1 != term2 {
		t.Fatalf("expected same term, got %d and %d", term1, term2)
	}
	if logLenAfterFirst != logLenAfterSecond {
		t.Fatalf(
			"duplicate request appended another entry: log length changed from %d to %d",
			logLenAfterFirst, logLenAfterSecond,
		)
	}

	t.Logf(
		"deduplication verified: index=%d, log length=%d",
		index1, logLenAfterSecond,
	)
}

func TestStartRejectsConflictingDuplicateRequest(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	stateMachine := NewTestStateMachine(storage.NewMemEngine())

	node, err := NewRaftNode(
		0, 1, applyCh, NewMemPersister(), stateMachine,
	)
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 1
	node.mu.Unlock()

	command1 := Command{
		ClientID:  3,
		SeqNumber: 1,
		Op:        "SET",
		Key:       "framework",
		Value:     "RaftKV",
	}

	index1, _, err := node.Start(command1)
	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}

	node.mu.Lock()
	logLenBefore := len(node.log)
	node.mu.Unlock()

	command2 := command1
	command2.Value = "DifferentValue"

	index2, _, err := node.Start(command2)

	if !errors.Is(err, ErrRequestIDConflict) {
		t.Fatalf("expected ErrRequestIDConflict, got %v", err)
	}

	if index2 != -1 {
		t.Fatalf("expected index -1, got %d", index2)
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	if len(node.log) != logLenBefore {
		t.Fatalf(
			"conflicting request changed log length: before=%d, after=%d",
			logLenBefore, len(node.log),
		)
	}

	if node.log[index1].Command != command1 {
		t.Fatalf("original command was changed: got %+v", node.log[index1].Command)
	}
}

func TestStartDeduplicatesRequestAfterRestart(t *testing.T) {
	persister := NewMemPersister()

	command := Command{
		ClientID:  3,
		SeqNumber: 1,
		Op:        "SET",
		Key:       "framework",
		Value:     "RaftKV",
	}

	createNode := func() *RaftNode {
		t.Helper()

		node, err := NewRaftNode(
			0,
			1,
			make(chan ApplyMsg, 10),
			persister,
			NewTestStateMachine(storage.NewMemEngine()),
		)
		if err != nil {
			t.Fatalf("create node: %v", err)
		}
		return node
	}

	original := createNode()

	original.mu.Lock()
	original.role = Leader
	original.currentTerm = 1
	original.mu.Unlock()

	index1, term1, err := original.Start(command)
	if err != nil {
		original.Stop()
		t.Fatalf("first request failed: %v", err)
	}

	original.Stop()

	restarted := createNode()
	defer restarted.Stop()

	restarted.mu.Lock()
	restarted.role = Leader
	restarted.mu.Unlock()

	index2, term2, err := restarted.Start(command)
	if err != nil {
		t.Fatalf("duplicate request after restart failed: %v", err)
	}

	restarted.mu.Lock()
	defer restarted.mu.Unlock()

	if index2 != index1 {
		t.Fatalf("expected original index %d, got %d", index1, index2)
	}

	if term2 != term1 {
		t.Fatalf("expected original term %d, got %d", term1, term2)
	}

	if len(restarted.log) != index1+1 {
		t.Fatalf(
			"duplicate appended an entry: log length=%d",
			len(restarted.log),
		)
	}
}

func waitForLeader(t *testing.T, nodes []*RaftNode, timeout time.Duration) *RaftNode {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		var leader *RaftNode
		leaderCount := 0

		for _, node := range nodes {
			node.mu.Lock()
			isLeader := node.role == Leader
			node.mu.Unlock()

			if isLeader {
				leader = node
				leaderCount++
			}
		}

		if leaderCount == 1 {
			return leader
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("timed out waiting for exactly one leader")
	return nil
}

func TestStartDeduplicatesRequestAfterLeadershipChange(t *testing.T) {
	nodes := make([]*RaftNode, 3)

	for i := range nodes {
		node, err := NewRaftNode(
			i,
			3,
			make(chan ApplyMsg, 100),
			NewMemPersister(),
			NewTestStateMachine(storage.NewMemEngine()),
		)
		if err != nil {
			t.Fatalf("failed to create node %d: %v", i, err)
		}
		nodes[i] = node
	}

	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	for _, node := range nodes {
		node.SetPeers(nodes)
		node.StartBackground()
	}

	oldLeader := waitForLeader(t, nodes, 3*time.Second)

	command := Command{
		ClientID:  3,
		SeqNumber: 1,
		Op:        "SET",
		Key:       "framework",
		Value:     "RaftKV",
	}

	originalIndex, originalTerm, err := oldLeader.Start(command)
	if err != nil {
		t.Fatalf("failed to submit original request: %v", err)
	}

	// Wait until a follower contains the request.
	var survivingFollower *RaftNode
	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node == oldLeader {
				continue
			}

			node.mu.Lock()
			hasEntry := len(node.log) > originalIndex &&
				node.log[originalIndex].Command.ClientID == command.ClientID &&
				node.log[originalIndex].Command.SeqNumber == command.SeqNumber
			node.mu.Unlock()

			if hasEntry {
				survivingFollower = node
				break
			}
		}

		if survivingFollower != nil {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	if survivingFollower == nil {
		t.Fatal("request was not replicated to any follower")
	}

	oldLeader.Stop()

	remainingNodes := make([]*RaftNode, 0, 2)
	for _, node := range nodes {
		if node != oldLeader {
			remainingNodes = append(remainingNodes, node)
		}
	}

	newLeader := waitForLeader(t, remainingNodes, 5*time.Second)

	index, term, err := newLeader.Start(command)
	if err != nil {
		t.Fatalf("new leader rejected identical retry: %v", err)
	}

	if index != originalIndex {
		t.Fatalf("expected original index %d, got %d", originalIndex, index)
	}

	if term != originalTerm {
		t.Fatalf("expected original term %d, got %d", originalTerm, term)
	}

	newLeader.mu.Lock()
	requestID := RequestID{
		ClientID:  command.ClientID,
		SeqNumber: command.SeqNumber,
	}
	gotIndex, exists := newLeader.requestIndex[requestID]
	newLeader.mu.Unlock()

	if !exists {
		t.Fatal("request ID missing from requestIndex")
	}

	if gotIndex != originalIndex {
		t.Fatalf("requestIndex = %d, want %d", gotIndex, originalIndex)
	}
}

func TestStartRejectsConflictingRequestAfterLeadershipChange(t *testing.T) {
	nodes := make([]*RaftNode, 3)

	for i := range nodes {
		node, err := NewRaftNode(
			i,
			3,
			make(chan ApplyMsg, 100),
			NewMemPersister(),
			NewTestStateMachine(storage.NewMemEngine()),
		)
		if err != nil {
			t.Fatalf("failed to create node %d: %v", i, err)
		}
		nodes[i] = node
	}

	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	for _, node := range nodes {
		node.SetPeers(nodes)
		node.StartBackground()
	}

	oldLeader := waitForLeader(t, nodes, 3*time.Second)

	command := Command{
		ClientID:  3,
		SeqNumber: 1,
		Op:        "SET",
		Key:       "framework",
		Value:     "RaftKV",
	}

	originalIndex, _, err := oldLeader.Start(command)
	if err != nil {
		t.Fatalf("failed to submit original request: %v", err)
	}

	// Wait until a follower contains the request.
	var survivingFollower *RaftNode
	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node == oldLeader {
				continue
			}

			node.mu.Lock()
			hasEntry := len(node.log) > originalIndex &&
				node.log[originalIndex].Command.ClientID == command.ClientID &&
				node.log[originalIndex].Command.SeqNumber == command.SeqNumber
			node.mu.Unlock()

			if hasEntry {
				survivingFollower = node
				break
			}
		}

		if survivingFollower != nil {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	if survivingFollower == nil {
		t.Fatal("request was not replicated to any follower")
	}

	oldLeader.Stop()

	remainingNodes := make([]*RaftNode, 0, 2)
	for _, node := range nodes {
		if node != oldLeader {
			remainingNodes = append(remainingNodes, node)
		}
	}

	newLeader := waitForLeader(t, remainingNodes, 5*time.Second)

	conflictingCommand := command
	conflictingCommand.Value = "DifferentValue"

	newLeader.mu.Lock()
	logLenBefore := len(newLeader.log)
	newLeader.mu.Unlock()

	_, _, err = newLeader.Start(conflictingCommand)

	if !errors.Is(err, ErrRequestIDConflict) {
		t.Fatalf("expected ErrRequestIDConflict, got %v", err)
	}

	newLeader.mu.Lock()
	logLenAfter := len(newLeader.log)

	requestID := RequestID{
		ClientID:  command.ClientID,
		SeqNumber: command.SeqNumber,
	}
	requestIndex, exists := newLeader.requestIndex[requestID]
	newLeader.mu.Unlock()

	if logLenAfter != logLenBefore {
		t.Fatalf(
			"conflicting retry changed log length: before=%d, after=%d",
			logLenBefore, logLenAfter,
		)
	}

	if !exists {
		t.Fatal("original request ID missing from requestIndex")
	}

	if requestIndex != originalIndex {
		t.Fatalf(
			"request index changed: got %d, want %d",
			requestIndex, originalIndex,
		)
	}
}

func TestConcurrentDuplicateRequestsAppendOnlyOneEntry(t *testing.T) {
	node, err := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 100),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create Raft node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 1
	node.mu.Unlock()

	command := Command{
		ClientID:  42,
		SeqNumber: 1,
		Op:        "SET",
		Key:       "language",
		Value:     "Go",
	}

	const requests = 20

	type result struct {
		index int
		term  int
		err   error
	}

	results := make(chan result, requests)
	start := make(chan struct{})

	node.mu.Lock()
	initialLogLen := len(node.log)
	node.mu.Unlock()

	var wg sync.WaitGroup

	for i := 0; i < requests; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			index, term, err := node.Start(command)
			results <- result{
				index: index,
				term:  term,
				err:   err,
			}
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	var expectedIndex, expectedTerm int
	first := true

	for res := range results {
		if res.err != nil {
			t.Fatalf("concurrent duplicate request failed: %v", res.err)
		}

		if first {
			expectedIndex = res.index
			expectedTerm = res.term
			first = false
			continue
		}

		if res.index != expectedIndex {
			t.Fatalf(
				"concurrent requests returned different indices: %d and %d",
				expectedIndex, res.index,
			)
		}

		if res.term != expectedTerm {
			t.Fatalf(
				"concurrent requests returned different terms: %d and %d",
				expectedTerm, res.term,
			)
		}
	}

	node.mu.Lock()
	finalLogLen := len(node.log)
	node.mu.Unlock()

	if finalLogLen != initialLogLen+1 {
		t.Fatalf(
			"expected exactly one appended entry: before=%d, after=%d",
			initialLogLen, finalLogLen,
		)
	}
}

func TestConcurrentDistinctRequestsAppendAllEntries(t *testing.T) {
	node, err := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 100),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create Raft node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 1
	initialLogLen := len(node.log)
	node.mu.Unlock()

	const requests = 20

	type result struct {
		seqNumber int
		index     int
		term      int
		err       error
	}

	results := make(chan result, requests)
	start := make(chan struct{})

	var wg sync.WaitGroup

	for i := 1; i <= requests; i++ {
		wg.Add(1)

		go func(seq int) {
			defer wg.Done()
			<-start

			command := Command{
				ClientID:  42,
				SeqNumber: int64(seq),
				Op:        "SET",
				Key:       fmt.Sprintf("key-%d", seq),
				Value:     fmt.Sprintf("value-%d", seq),
			}

			index, term, err := node.Start(command)

			results <- result{
				seqNumber: seq,
				index:     index,
				term:      term,
				err:       err,
			}
		}(i)
	}

	close(start)
	wg.Wait()
	close(results)

	seenIndices := make(map[int]bool)
	seenSequences := make(map[int]bool)

	for res := range results {
		if res.err != nil {
			t.Fatalf("request %d failed: %v", res.seqNumber, res.err)
		}

		if seenIndices[res.index] {
			t.Fatalf("multiple requests received log index %d", res.index)
		}
		seenIndices[res.index] = true

		if seenSequences[res.seqNumber] {
			t.Fatalf("duplicate result for sequence number %d", res.seqNumber)
		}
		seenSequences[res.seqNumber] = true
	}

	if len(seenIndices) != requests {
		t.Fatalf("expected %d unique indices, got %d",
			requests, len(seenIndices))
	}

	if len(seenSequences) != requests {
		t.Fatalf("expected %d unique sequences, got %d",
			requests, len(seenSequences))
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	if len(node.log) != initialLogLen+requests {
		t.Fatalf(
			"expected %d new log entries, got %d",
			requests, len(node.log)-initialLogLen,
		)
	}

	for seq := 1; seq <= requests; seq++ {
		requestID := RequestID{
			ClientID:  42,
			SeqNumber: int64(seq),
		}

		index, exists := node.requestIndex[requestID]
		if !exists {
			t.Fatalf("request index missing for sequence number %d", seq)
		}

		if index <= 0 || index >= len(node.log) {
			t.Fatalf("invalid log index %d for request %d", index, seq)
		}

		entry := node.log[index]
		if entry.Command.ClientID != 42 ||
			entry.Command.SeqNumber != int64(seq) {
			t.Fatalf(
				"request %d maps to incorrect log entry at index %d",
				seq, index,
			)
		}
	}
}

func TestConcurrentRequestsDuringLeadershipStepDown(t *testing.T) {
	node, err := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 100),
		NewMemPersister(),
		NewTestStateMachine(storage.NewMemEngine()),
	)
	if err != nil {
		t.Fatalf("failed to create Raft node: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 1
	node.mu.Unlock()

	const requests = 50

	start := make(chan struct{})
	var wg sync.WaitGroup

	type result struct {
		seq   int64
		index int
		err   error
	}

	results := make(chan result, requests)

	for i := int64(1); i <= requests; i++ {
		wg.Add(1)

		go func(seq int64) {
			defer wg.Done()
			<-start

			command := Command{
				ClientID:  42,
				SeqNumber: seq,
				Op:        "SET",
				Key:       fmt.Sprintf("key-%d", seq),
				Value:     fmt.Sprintf("value-%d", seq),
			}

			index, _, err := node.Start(command)
			results <- result{seq: seq, index: index, err: err}
		}(i)
	}

	close(start)
	wg.Wait()
	close(results)

	accepted := make(map[int64]int)

	for res := range results {
		if res.err != nil {
			t.Fatalf("request %d should succeed before step-down: %v",
				res.seq, res.err)
		}
		accepted[res.seq] = res.index
	}

	if len(accepted) != requests {
		t.Fatalf("expected %d accepted requests, got %d",
			requests, len(accepted))
	}

	// Step down to follower.
	node.mu.Lock()
	node.role = Follower
	node.mu.Unlock()

	// New writes must be rejected.
	for i := int64(requests + 1); i <= requests+10; i++ {
		command := Command{
			ClientID:  42,
			SeqNumber: i,
			Op:        "SET",
			Key:       fmt.Sprintf("key-%d", i),
			Value:     fmt.Sprintf("value-%d", i),
		}

		index, _, err := node.Start(command)

		if !errors.Is(err, ErrWrongLeader) {
			t.Fatalf("request %d: expected ErrWrongLeader, got %v", i, err)
		}

		if index != -1 {
			t.Fatalf("request %d: expected index -1, got %d", i, index)
		}
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	expectedLogLen := 1 + requests
	if len(node.log) != expectedLogLen {
		t.Fatalf("expected log length %d, got %d",
			expectedLogLen, len(node.log))
	}

	for seq, index := range accepted {
		if index <= 0 || index >= len(node.log) {
			t.Fatalf("request %d has invalid log index %d", seq, index)
		}

		if node.log[index].Command.SeqNumber != seq {
			t.Fatalf("request %d maps to wrong log entry", seq)
		}
	}
}

func TestConcurrentWritesDuringRealElection(t *testing.T) {
	nodes := make([]*RaftNode, 3)

	for i := range nodes {
		node, err := NewRaftNode(
			i,
			3,
			make(chan ApplyMsg, 100),
			NewMemPersister(),
			NewTestStateMachine(storage.NewMemEngine()),
		)
		if err != nil {
			t.Fatalf("failed to create node %d: %v", i, err)
		}
		nodes[i] = node
	}

	// Register cleanup before starting background goroutines.
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	for _, node := range nodes {
		node.SetPeers(nodes)
		node.StartBackground()
	}

	oldLeader := waitForLeader(t, nodes, 3*time.Second)

	// Allow the cluster to settle.
	time.Sleep(100 * time.Millisecond)

	oldLeader.Stop()

	remainingNodes := make([]*RaftNode, 0, 2)
	for _, node := range nodes {
		if node != oldLeader {
			remainingNodes = append(remainingNodes, node)
		}
	}

	const requests = 20
	start := make(chan struct{})

	type result struct {
		seq   int64
		index int
		err   error
	}

	results := make(chan result, requests)
	var wg sync.WaitGroup

	for i := int64(1); i <= requests; i++ {
		wg.Add(1)

		go func(seq int64) {
			defer wg.Done()
			<-start

			node := remainingNodes[int(seq%int64(len(remainingNodes)))]

			command := Command{
				ClientID:  77,
				SeqNumber: seq,
				Op:        "SET",
				Key:       fmt.Sprintf("election-key-%d", seq),
				Value:     fmt.Sprintf("value-%d", seq),
			}

			index, _, err := node.Start(command)
			results <- result{seq: seq, index: index, err: err}
		}(i)
	}

	close(start)
	wg.Wait()
	close(results)

	accepted := make(map[int64]int)

	for res := range results {
		if res.err == nil {
			accepted[res.seq] = res.index
		} else if !errors.Is(res.err, ErrWrongLeader) {
			t.Fatalf("request %d returned unexpected error: %v",
				res.seq, res.err)
		}
	}

	newLeader := waitForLeader(t, remainingNodes, 5*time.Second)
	if newLeader == oldLeader {
		t.Fatal("stopped node unexpectedly became leader")
	}

	// Verify each accepted request exists in a surviving node's log.
	for seq, index := range accepted {
		found := false

		for _, node := range remainingNodes {
			node.mu.Lock()

			if index > 0 && index < len(node.log) {
				entry := node.log[index]
				if entry.Command.ClientID == 77 &&
					entry.Command.SeqNumber == seq {
					found = true
				}
			}

			node.mu.Unlock()

			if found {
				break
			}
		}

		if !found {
			t.Fatalf(
				"accepted request %d at index %d was not found in surviving logs",
				seq, index,
			)
		}
	}
}

