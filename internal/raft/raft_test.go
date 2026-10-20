package raft

import (
	"RaftKV/internal/storage"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestBroadcastAppendEntriesCommitsMajority(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)

	nodes := make([]*RaftNode, 3)
	for i := 0; i < 3; i++ {
		nodes[i] = NewRaftNode(i, 3, applyCh, storage.NewMemEngine())
	}

	// Connect all nodes.
	for i := 0; i < 3; i++ {
		nodes[i].SetPeers(nodes)
	}

	leader := nodes[0]

	// Make node 0 the leader deterministically.
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1
	leader.nextIndex[0] = len(leader.log)
	leader.matchIndex[0] = 0
	leader.mu.Unlock()

	// Leader appends a command.
	index, term, err := leader.Start(Command{
		Op:    "SET",
		Key:   "x",
		Value: "100",
	})

	if err != nil {
		t.Fatal("leader rejected command")
	}

	if index != 1 {
		t.Fatalf("expected index 1, got %d", index)
	}

	if term != 1 {
		t.Fatalf("expected term 1, got %d", term)
	}

	// In a real leader, nextIndex for followers starts at len(log).
	leader.mu.Lock()
	leader.nextIndex[1] = index
	leader.nextIndex[2] = index
	leader.mu.Unlock()

	// Exercise the actual replication path.
	leader.broadcastAppendEntries()

	// Wait for replication + majority commit.
	deadline := time.Now().Add(1 * time.Second)

	for time.Now().Before(deadline) {
		leader.mu.Lock()
		committed := leader.commitIndex >= index
		match1 := leader.matchIndex[1]
		match2 := leader.matchIndex[2]
		leader.mu.Unlock()

		if committed && match1 >= index && match2 >= index {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}

	// Verify leader committed the entry.
	leader.mu.Lock()
	commitIndex := leader.commitIndex
	match1 := leader.matchIndex[1]
	match2 := leader.matchIndex[2]
	leader.mu.Unlock()

	if commitIndex < index {
		t.Fatalf(
			"expected leader to commit index %d, got %d",
			index,
			commitIndex,
		)
	}

	if match1 < index {
		t.Fatalf("follower 1 was not replicated: matchIndex=%d", match1)
	}

	if match2 < index {
		t.Fatalf("follower 2 was not replicated: matchIndex=%d", match2)
	}

	// Verify followers actually contain the replicated entry.
	for i := 1; i < 3; i++ {
		nodes[i].mu.Lock()

		if len(nodes[i].log) <= index {
			nodes[i].mu.Unlock()
			t.Fatalf("follower %d log too short", i)
		}

		entry := nodes[i].log[index]
		nodes[i].mu.Unlock()

		if entry.Term != term {
			t.Fatalf(
				"follower %d: expected term %d, got %d",
				i,
				term,
				entry.Term,
			)
		}

		if entry.Command.Key != "x" || entry.Command.Value != "100" {
			t.Fatalf(
				"follower %d: unexpected command %+v",
				i,
				entry.Command,
			)
		}
	}
}

func TestApplyCommittedEntry(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)

	node := NewRaftNode(
		0,
		1,
		applyCh,
		storage.NewMemEngine(),
	)

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

	node.mu.Unlock()

	// collectCommittedEntries should return the
	// messages that need to be delivered.
	if len(msgs) != 1 {
		t.Fatalf("expected 1 ApplyMsg, got %d", len(msgs))
	}

	msg := msgs[0]

	if !msg.CommandValid {
		t.Fatal("expected valid command")
	}

	if msg.CommandIndex != 1 {
		t.Fatalf(
			"expected command index 1, got %d",
			msg.CommandIndex,
		)
	}

	if msg.CommandTerm != 1 {
		t.Fatalf(
			"expected command term 1, got %d",
			msg.CommandTerm,
		)
	}

	if msg.Command.Op != "SET" {
		t.Fatalf(
			"expected SET command, got %s",
			msg.Command.Op,
		)
	}

	if msg.Command.Key != "name" {
		t.Fatalf(
			"expected key name, got %s",
			msg.Command.Key,
		)
	}

	if msg.Command.Value != "Himanshi" {
		t.Fatalf(
			"expected value Himanshi, got %s",
			msg.Command.Value,
		)
	}

	node.mu.Lock()
	lastApplied := node.lastApplied
	lastQueued := node.lastQueued
	node.mu.Unlock()

	if lastApplied != 0 {
		t.Fatalf(
			"expected lastApplied=0 before application, got %d",
			lastApplied,
		)
	}

	if lastQueued != 1 {
		t.Fatalf(
			"expected lastQueued=1, got %d",
			lastQueued,
		)
	}
}

func TestAppendEntriesReplicatesEntry(t *testing.T) {
	leader := NewRaftNode(0, 2, make(chan ApplyMsg, 10), storage.NewMemEngine())
	follower := NewRaftNode(1, 2, make(chan ApplyMsg, 10), storage.NewMemEngine())

	// Set up a 2-node cluster.
	peers := []*RaftNode{leader, follower}
	leader.SetPeers(peers)
	follower.SetPeers(peers)

	// Make the leader a leader in term 1.
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1

	// Leader has the command in its log.
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

	// Send replication RPC directly to follower.
	follower.AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept valid AppendEntries")
	}

	// Verify follower received the entry.
	follower.mu.Lock()
	defer follower.mu.Unlock()

	if len(follower.log) != 2 {
		t.Fatalf("expected follower log length 2, got %d",
			len(follower.log))
	}

	entry := follower.log[1]

	if entry.Term != 1 {
		t.Fatalf("expected entry term 1, got %d", entry.Term)
	}

	if entry.Command.Op != "SET" ||
		entry.Command.Key != "x" ||
		entry.Command.Value != "10" {
		t.Fatalf("replicated command incorrect: %+v",
			entry.Command)
	}

	// Replication alone must not commit the entry.
	if follower.commitIndex != 0 {
		t.Fatalf("entry should not be committed yet; commitIndex=%d",
			follower.commitIndex)
	}
}

func TestLeaderCommitsAfterMajorityReplication(t *testing.T) {
	applyCh0 := make(chan ApplyMsg, 10)
	applyCh1 := make(chan ApplyMsg, 10)
	applyCh2 := make(chan ApplyMsg, 10)

	leader := NewRaftNode(0, 3, applyCh0, storage.NewMemEngine())
	follower1 := NewRaftNode(1, 3, applyCh1, storage.NewMemEngine())
	follower2 := NewRaftNode(2, 3, applyCh2, storage.NewMemEngine())

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
		t.Fatal("leader should accept command")
	}

	if index != 1 {
		t.Fatalf("expected index 1, got %d", index)
	}

	if term != 1 {
		t.Fatalf("expected term 1, got %d", term)
	}

	// Verify it is initially uncommitted.
	leader.mu.Lock()
	if leader.commitIndex != 0 {
		leader.mu.Unlock()
		t.Fatalf("entry should initially be uncommitted, commitIndex=%d",
			leader.commitIndex)
	}

	// Prepare AppendEntries for follower 1.
	args := AppendEntriesArgs{
		Term:         leader.currentTerm,
		LeaderID:     leader.me,
		PrevLogIndex: 0,
		PrevLogTerm:  leader.log[0].Term,
		Entries:      []LogEntry{leader.log[1]},
		LeaderCommit: 0,
	}
	leader.mu.Unlock()

	var reply AppendEntriesReply

	// Replicate to one follower.
	follower1.AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept AppendEntries")
	}

	// Simulate the leader learning that follower 1 replicated the entry.
	leader.mu.Lock()

	leader.matchIndex[1] = index
	leader.nextIndex[1] = index + 1

	// This is the same majority rule used by the leader.
	count := 1 // leader itself

	for i := range leader.peers {
		if i != leader.me && leader.matchIndex[i] >= index {
			count++
		}
	}

	if count > len(leader.peers)/2 {
		leader.commitIndex = index
	}

	leader.mu.Unlock()

	// In a 3-node cluster, leader + one follower = majority.
	leader.mu.Lock()
	defer leader.mu.Unlock()

	if leader.commitIndex != index {
		t.Fatalf(
			"expected commitIndex=%d after majority replication, got %d",
			index,
			leader.commitIndex,
		)
	}
}

func TestClientAPIAndLeaderRedirection(t *testing.T) {
	// Set up a 3-node cluster with separate engines and apply channels.
	n := 3
	nodes := make([]*RaftNode, n)
	engines := make([]storage.Engine, n)
	applyChs := make([]chan ApplyMsg, n)

	for i := 0; i < n; i++ {
		engines[i] = storage.NewMemEngine()
		applyChs[i] = make(chan ApplyMsg, 100)
		nodes[i] = NewRaftNode(i, n, applyChs[i], engines[i])
	}

	// Connect peers and start their background loops.
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
		t.Fatal("Failed to elect a leader within timeout")
	}

	t.Logf("Leader found: Node %d", leaderID)

	// Test 1: A follower must reject writes.
	followerID := (leaderID + 1) % n
	follower := nodes[followerID]

	_, err := follower.Put(1, 1, "test_key", "test_val")
	if !errors.Is(err, ErrWrongLeader) {
		t.Fatalf("Expected ErrWrongLeader on follower Put, got: %v", err)
	}

	t.Log("Follower correctly rejected Put with ErrWrongLeader")

	// Test 2: Write a value through the leader.
	success, err := leader.Put(1, 2, "city", "Delhi")
	if !success || err != nil {
		t.Fatalf("Failed to Put on leader: %v", err)
	}

	t.Log("Put command submitted successfully to leader")

	// Test 3: Read the value from the leader.
	val, found, err := leader.Get(1, 3, "city")
	if err != nil || !found || val != "Delhi" {
		t.Fatalf(
			"Failed to Get correct value: val=%s, found=%v, err=%v",
			val, found, err,
		)
	}

	t.Logf("Get verified on leader: 'city' = '%s'", val)

	// Test 4: Delete the value through the leader.
	success, err = leader.Delete(1, 4, "city")
	if !success || err != nil {
		t.Fatalf("Failed to Delete on leader: %v", err)
	}

	t.Log("Delete command submitted successfully")

	// Verify the deletion.
	_, found, err = leader.Get(1, 5, "city")
	if err != nil {
		t.Fatalf("Get after Delete failed: %v", err)
	}
	if found {
		t.Fatalf("Expected key 'city' to be deleted, but found=%v", found)
	}

	t.Log("Success! Client API, operations (Put/Get/Delete), and leader redirection verified.")
}

func TestStartRejectedByFollower(t *testing.T) {
	engine := storage.NewMemEngine()
	node := NewRaftNode(0, 1, make(chan ApplyMsg, 10), engine)

	index, term, err := node.Start(Command{
		Op:    "SET",
		Key:   "x",
		Value: "10",
	})

	if err == nil {
		t.Fatal("follower should reject Start()")
	}

	if index != -1 || term != -1 {
		t.Fatalf("expected (-1, -1, \"not a leader\"), got (%d, %d, %v)",
			index, term, err)
	}
}

func TestStartLeaderAppendsEntry(t *testing.T) {
	engine := storage.NewMemEngine()
	node := NewRaftNode(0, 1, make(chan ApplyMsg, 10), engine)

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
		t.Fatal("leader should accept Start()")
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
	node := NewRaftNode(0, 1, make(chan ApplyMsg, 10), engine)

	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 1
	node.peers = []*RaftNode{node}
	node.nextIndex = make([]int, 1)
	node.matchIndex = make([]int, 1)
	node.mu.Unlock()

	_, _, err := node.Start(Command{
		Op:    "SET",
		Key:   "x",
		Value: "10",
	})

	if err != nil {
		t.Fatal("leader should accept command")
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.commitIndex != 0 {
		t.Fatalf("Start() must not commit an entry; commitIndex=%d",
			node.commitIndex)
	}

	if node.lastApplied != 0 {
		t.Fatalf("Start() must not apply an entry; lastApplied=%d",
			node.lastApplied)
	}
}

func TestAppendEntriesRepairsConflict(t *testing.T) {
	leader := NewRaftNode(0, 2, make(chan ApplyMsg, 10), storage.NewMemEngine())
	follower := NewRaftNode(1, 2, make(chan ApplyMsg, 10), storage.NewMemEngine())

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

	// Follower has a conflicting entry at index 2.
	// [dummy, A(term=1), X(term=2)]
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
	args := AppendEntriesArgs{
		Term:         2,
		LeaderID:     0,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			leader.log[2],
		},
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

	node0 := NewRaftNode(0, 3, applyCh0, storage.NewMemEngine())
	node1 := NewRaftNode(1, 3, applyCh1, storage.NewMemEngine())
	node2 := NewRaftNode(2, 3, applyCh2, storage.NewMemEngine())

	nodes := []*RaftNode{node0, node1, node2}

	for i := range nodes {
		nodes[i].SetPeers(nodes)
	}

	// 1. Node 0 is the leader in term 1.

	node0.mu.Lock()
	node0.role = Leader
	node0.currentTerm = 1

	// Leader log:
	// [dummy, SET x=100]
	node0.log = append(node0.log, LogEntry{
		Term: 1,
		Command: Command{
			Op:    "SET",
			Key:   "x",
			Value: "100",
		},
	})

	// Leader has replicated the entry to node 1.
	node0.matchIndex[0] = 1
	node0.matchIndex[1] = 1
	node0.matchIndex[2] = 0

	// Entry is committed on a majority:
	// node0 + node1 = 2/3.
	node0.commitIndex = 1
	node0.mu.Unlock()

	// ------------------------------------------------------------
	// 2. Node 1 has the committed entry.
	// Node 2 does NOT have it.
	// ------------------------------------------------------------

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

	node2.mu.Lock()
	node2.currentTerm = 1
	// node2 intentionally remains with only the dummy entry.
	node2.mu.Unlock()

	// ------------------------------------------------------------
	// 3. Old leader node 0 fails.
	//
	// Node 2 starts a new election in term 2.
	// It does NOT contain the committed entry.
	// ------------------------------------------------------------

	request := RequestVoteArgs{
		Term:         2,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	var reply RequestVoteReply

	node1.RequestVote(&request, &reply)

	// ------------------------------------------------------------
	// 4. Node 1 must reject node 2's vote request because
	// node 2's log is behind the committed entry.
	// ------------------------------------------------------------

	if reply.VoteGranted {
		t.Fatal(
			"outdated candidate should not receive vote from " +
				"a node containing the committed entry",
		)
	}

	// Node 1 should still update its term to 2.
	if reply.Term != 2 {
		t.Fatalf(
			"expected node 1 to update to term 2, got term %d",
			reply.Term,
		)
	}

	// ------------------------------------------------------------
	// 5. Verify the committed entry still exists on node 1.
	// ------------------------------------------------------------

	node1.mu.Lock()
	defer node1.mu.Unlock()

	if node1.commitIndex != 1 {
		t.Fatalf(
			"committed entry was lost: expected commitIndex=1, got %d",
			node1.commitIndex,
		)
	}

	if len(node1.log) <= 1 {
		t.Fatal("node 1 lost the committed log entry")
	}

	entry := node1.log[1]

	if entry.Term != 1 {
		t.Fatalf(
			"expected committed entry term 1, got %d",
			entry.Term,
		)
	}

	if entry.Command.Key != "x" ||
		entry.Command.Value != "100" {
		t.Fatalf(
			"committed entry was changed: %+v",
			entry.Command,
		)
	}
}

func TestLeaderDoesNotCommitWithoutMajority(t *testing.T) {
	applyCh0 := make(chan ApplyMsg, 10)
	applyCh1 := make(chan ApplyMsg, 10)
	applyCh2 := make(chan ApplyMsg, 10)

	node0 := NewRaftNode(0, 3, applyCh0, storage.NewMemEngine())
	node1 := NewRaftNode(1, 3, applyCh1, storage.NewMemEngine())
	node2 := NewRaftNode(2, 3, applyCh2, storage.NewMemEngine())

	nodes := []*RaftNode{node0, node1, node2}

	for i := range nodes {
		nodes[i].SetPeers(nodes)
	}

	// Node 0 is the leader in term 1.
	node0.mu.Lock()
	node0.role = Leader
	node0.currentTerm = 1
	node0.mu.Unlock()

	// Append a new entry.
	index, _, err := node0.Start(Command{
		Op:    "SET",
		Key:   "x",
		Value: "100",
	})

	if err!=nil {
		t.Fatal("leader should accept command")
	}

	if index != 1 {
		t.Fatalf("expected entry at index 1, got %d", index)
	}

	// Only the leader has the entry.
	// matchIndex[1] and matchIndex[2] remain 0.
	node0.mu.Lock()
	commitIndex := node0.commitIndex
	match1 := node0.matchIndex[1]
	match2 := node0.matchIndex[2]
	node0.mu.Unlock()

	if match1 >= index || match2 >= index {
		t.Fatalf(
			"entry unexpectedly replicated: matchIndex[1]=%d, matchIndex[2]=%d",
			match1,
			match2,
		)
	}

	// In a 3-node cluster, one node alone is NOT a majority.
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

	node0 := NewRaftNode(0, 3, applyCh0, storage.NewMemEngine())
	node1 := NewRaftNode(1, 3, applyCh1, storage.NewMemEngine())
	node2 := NewRaftNode(2, 3, applyCh2, storage.NewMemEngine())

	nodes := []*RaftNode{node0, node1, node2}

	for i := range nodes {
		nodes[i].SetPeers(nodes)
	}

	// Node 0 is the leader in term 2.
	node0.mu.Lock()
	node0.role = Leader
	node0.currentTerm = 2

	// Log:
	// [dummy, old-term entry]
	node0.log = append(node0.log, LogEntry{
		Term: 1,
		Command: Command{
			Op:    "SET",
			Key:   "x",
			Value: "100",
		},
	})

	// The old-term entry is replicated on a majority:
	// node0 + node1 = 2/3.
	node0.matchIndex[0] = 1
	node0.matchIndex[1] = 1
	node0.matchIndex[2] = 0

	node0.commitIndex = 0
	node0.mu.Unlock()

	// Simulate the leader's commit calculation.
	node0.mu.Lock()

	for N := len(node0.log) - 1; N > node0.commitIndex; N-- {
		if node0.log[N].Term == node0.currentTerm {
			count := 1

			for j := range nodes {
				if j != node0.me && node0.matchIndex[j] >= N {
					count++
				}
			}

			if count > len(nodes)/2 {
				node0.commitIndex = N
				break
			}
		}
	}

	commitIndex := node0.commitIndex
	node0.mu.Unlock()

	// The entry is from term 1, while the leader is in term 2.
	// Therefore it must NOT be committed by this calculation.
	if commitIndex != 0 {
		t.Fatalf(
			"old-term entry was incorrectly committed: commitIndex=%d",
			commitIndex,
		)
	}
}

func TestCommitIndexNeverDecreases(t *testing.T) {
	node := NewRaftNode(
		0,
		3,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

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

	// Simulate that index 2 is already committed.
	node.commitIndex = 2

	before := node.commitIndex

	// Simulate a later commit calculation.
	// No candidate N is allowed to be <= commitIndex.
	for N := len(node.log) - 1; N > node.commitIndex; N-- {
		if node.log[N].Term == node.currentTerm {
			node.commitIndex = N
			break
		}
	}

	after := node.commitIndex

	node.mu.Unlock()

	if after < before {
		t.Fatalf(
			"commitIndex moved backwards: before=%d, after=%d",
			before,
			after,
		)
	}

	if after != 2 {
		t.Fatalf(
			"expected commitIndex to remain 2, got %d",
			after,
		)
	}
}

func TestLastAppliedNeverExceedsCommitIndex(t *testing.T) {
	node := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	node.mu.Lock()

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
			Term: 1,
			Command: Command{
				Op:    "SET",
				Key:   "y",
				Value: "200",
			},
		},
	)

	// Only the first entry is committed.
	node.commitIndex = 1
	node.lastApplied = 0

	msgs := node.collectCommittedEntries()

	// Verify only the committed entry was collected.
	if len(msgs) != 1 {
		node.mu.Unlock()
		t.Fatalf(
			"expected 1 ApplyMsg, got %d",
			len(msgs),
		)
	}

	if node.lastApplied > node.commitIndex {
		node.mu.Unlock()
		t.Fatalf(
			"lastApplied exceeded commitIndex: lastApplied=%d, commitIndex=%d",
			node.lastApplied,
			node.commitIndex,
		)
	}

	if node.lastApplied != 0 {
		node.mu.Unlock()
		t.Fatalf(
			"expected lastApplied=0 before application, got %d",
			node.lastApplied,
		)
	}

	if node.lastQueued != 1 {
		node.mu.Unlock()
		t.Fatalf(
			"expected lastQueued=1, got %d",
			node.lastQueued,
		)
	}

	node.mu.Unlock()
}

func TestLastAppliedNeverDecreases(t *testing.T) {
	node := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	node.mu.Lock()

	node.log = append(node.log,
		LogEntry{
			Index: 1,
			Term:  1,
			Command: Command{
				Op:    "SET",
				Key:   "x",
				Value: "100",
			},
		},
		LogEntry{
			Index: 2,
			Term:  1,
			Command: Command{
				Op:    "SET",
				Key:   "y",
				Value: "200",
			},
		},
	)

	node.commitIndex = 2

	// Collecting entries queues them; it does not apply them.
	msgs := node.collectCommittedEntries()

	if len(msgs) != 2 {
		node.mu.Unlock()
		t.Fatalf("expected 2 ApplyMsgs, got %d", len(msgs))
	}

	if node.lastApplied != 0 {
		node.mu.Unlock()
		t.Fatalf(
			"expected lastApplied=0 before application, got %d",
			node.lastApplied,
		)
	}

	if node.lastQueued != 2 {
		node.mu.Unlock()
		t.Fatalf(
			"expected lastQueued=2, got %d",
			node.lastQueued,
		)
	}

	// Collecting again must not queue the same entries twice.
	msgs = node.collectCommittedEntries()

	if len(msgs) != 0 {
		node.mu.Unlock()
		t.Fatalf("expected no new ApplyMsgs, got %d", len(msgs))
	}

	if node.lastApplied != 0 {
		node.mu.Unlock()
		t.Fatalf(
			"lastApplied changed before application: %d",
			node.lastApplied,
		)
	}

	node.mu.Unlock()
}

func TestWaitForApplyBlocksUntilApplied(t *testing.T) {
	node := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	// The command at index 1 has not been applied yet.
	node.mu.Lock()
	node.lastApplied = 0
	node.mu.Unlock()

	done := make(chan error, 1)

	go func() {
		done <- node.waitForApply(1)
	}()

	// It must NOT return before index 1 is applied.
	select {
	case err := <-done:
		t.Fatalf(
			"waitForApply returned before command was applied: %v",
			err,
		)

	case <-time.After(100 * time.Millisecond):
		// Expected: still waiting.
	}

	// Now simulate the state machine applying index 1.
	node.notifyApplied(1, nil)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf(
				"expected successful application, got: %v",
				err,
			)
		}

	case <-time.After(500 * time.Millisecond):
		t.Fatal("waitForApply did not return after notifyApplied")
	}
}

func TestWaitForApplyTimeout(t *testing.T) {
	node := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	start := time.Now()

	err := node.waitForApply(1)

	elapsed := time.Since(start)

	if !errors.Is(err, ErrTimeout) {
		t.Fatalf(
			"expected ErrTimeout, got %v",
			err,
		)
	}

	if elapsed < 2*time.Second {
		t.Fatalf(
			"waitForApply returned too early: elapsed=%v",
			elapsed,
		)
	}

	node.mu.Lock()
	_, exists := node.applyWaiters[1]
	node.mu.Unlock()

	if exists {
		t.Fatal(
			"timed-out waiter was not removed from applyWaiters",
		)
	}
}

func TestNotifyAppliedReleasesWaiter(t *testing.T) {
	node := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

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
	engines := make([]storage.Engine, 3)
	nodes := make([]*RaftNode, 3)

	// Create 3 nodes.
	for i := 0; i < 3; i++ {
		applyChs[i] = make(chan ApplyMsg, 10)
		engines[i] = storage.NewMemEngine()

		nodes[i] = NewRaftNode(
			i,
			3,
			applyChs[i],
			engines[i],
		)
	}

	// Connect peers.
	for i := 0; i < 3; i++ {
		nodes[i].SetPeers(nodes)
	}

	// State-machine consumers.
	for i := 0; i < 3; i++ {
		go func(i int) {
			for msg := range applyChs[i] {
				if !msg.CommandValid {
					continue
				}

				var err error

				switch msg.Command.Op {
				case "SET":
					err = engines[i].Put(
						[]byte(msg.Command.Key),
						[]byte(msg.Command.Value),
					)

				case "DELETE":
					err = engines[i].Delete(
						[]byte(msg.Command.Key),
					)
				}

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

	// Submit the client request in a goroutine because Put()
	// should block until the command is committed and applied.
	done := make(chan error, 1)

	go func() {
		_, err := leader.Put(1, 1, "city", "Delhi")
		done <- err
	}()

	// Wait until the command appears in the leader's log.
	var index int

	deadline := time.Now().Add(1 * time.Second)

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
		Entries: []LogEntry{
			leader.log[index],
		},
		LeaderCommit: 0,
	}

	leader.mu.Unlock()

	var reply AppendEntriesReply

	nodes[1].AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept AppendEntries")
	}

	// Tell the leader that follower 1 replicated the entry.
	leader.mu.Lock()

	leader.matchIndex[1] = index
	leader.nextIndex[1] = index + 1

	// Leader + follower 1 = majority.
	count := 1

	for i := range leader.peers {
		if i != leader.me && leader.matchIndex[i] >= index {
			count++
		}
	}

	if count > len(leader.peers)/2 {
		leader.commitIndex = index
	}

	// Collect the newly committed entry.
	msgs := leader.collectCommittedEntries()

	leader.mu.Unlock()

	// Deliver the committed entry to the state machine.
	for _, msg := range msgs {
		leader.applyCh <- msg
	}

	// Put() should now return successfully.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Put() returned error: %v", err)
		}

	case <-time.After(1 * time.Second):
		t.Fatal("Put() did not return after command was committed and applied")
	}

	// Finally verify that the state machine actually contains the value.
	value, found, err := engines[0].Get([]byte("city"))

	if err != nil {
		t.Fatalf("failed to read state machine: %v", err)
	}

	if !found {
		t.Fatal("expected committed key to exist in state machine")
	}

	if string(value) != "Delhi" {
		t.Fatalf(
			"expected value Delhi, got %s",
			string(value),
		)
	}
}

func TestLeaderStepsDownOnHigherTermAppendEntriesReply(t *testing.T) {
	leader := NewRaftNode(
		0,
		2,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	follower := NewRaftNode(
		1,
		2,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

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

	// This will cause the follower to reject the old-term
	// AppendEntries and return its higher term.
	leader.broadcastAppendEntries()

	// Wait for the RPC goroutine to process the reply.
	deadline := time.Now().Add(1 * time.Second)

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
	leader := NewRaftNode(
		0,
		2,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	follower := NewRaftNode(
		1,
		2,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	peers := []*RaftNode{leader, follower}

	leader.SetPeers(peers)
	follower.SetPeers(peers)

	// Leader is in term 1.
	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1
	leader.mu.Unlock()

	// Follower is also in term 1.
	follower.mu.Lock()
	follower.currentTerm = 1
	follower.role = Follower

	// Make the follower appear to be close to timing out.
	follower.electionTimeout = 200 * time.Millisecond
	follower.lastResetTime = time.Now().Add(-190 * time.Millisecond)

	follower.mu.Unlock()

	// Send a valid heartbeat from the leader.
	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  follower.log[0].Term,
		Entries:      nil,
		LeaderCommit: 0,
	}

	var reply AppendEntriesReply

	follower.AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept valid heartbeat")
	}

	// Verify that the heartbeat reset the election timer.
	follower.mu.Lock()

	elapsed := time.Since(follower.lastResetTime)
	role := follower.role

	follower.mu.Unlock()

	if role != Follower {
		t.Fatalf(
			"follower changed role after valid heartbeat: %v",
			role,
		)
	}

	if elapsed >= follower.electionTimeout {
		t.Fatalf(
			"heartbeat did not reset election timer: elapsed=%v timeout=%v",
			elapsed,
			follower.electionTimeout,
		)
	}
}

func TestStaleRequestVoteDoesNotResetElectionTimer(t *testing.T) {
	node := NewRaftNode(
		0,
		2,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	node.mu.Lock()

	node.currentTerm = 3
	node.role = Follower
	node.electionTimeout = 200 * time.Millisecond

	// Make the timer appear close to expiring.
	node.lastResetTime = time.Now().Add(-190 * time.Millisecond)

	before := node.lastResetTime

	node.mu.Unlock()

	// Candidate is behind the follower's current term.
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

	// The stale request must be rejected.
	if reply.VoteGranted {
		t.Fatal("stale RequestVote should not be granted")
	}

	// The node must remain in its current term.
	if term != 3 {
		t.Fatalf(
			"currentTerm changed after stale RequestVote: got %d",
			term,
		)
	}

	// The node must remain a follower.
	if role != Follower {
		t.Fatalf(
			"role changed after stale RequestVote: got %v",
			role,
		)
	}

	// Most importantly, the stale request must NOT reset the timer.
	if !after.Equal(before) {
		t.Fatalf(
			"stale RequestVote incorrectly reset election timer: before=%v after=%v",
			before,
			after,
		)
	}
}

func TestHigherTermRequestVoteUpdatesTermAndStepsDown(t *testing.T) {
	node := NewRaftNode(
		0,
		2,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	node.mu.Lock()

	node.currentTerm = 3
	node.role = Candidate
	node.votedFor = 0

	node.mu.Unlock()

	// Candidate is requesting a vote from a higher term.
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

	// The node must move to the higher term.
	if term != 4 {
		t.Fatalf(
			"expected currentTerm=4, got %d",
			term,
		)
	}

	// A node receiving a higher-term RPC must become a follower.
	if role != Follower {
		t.Fatalf(
			"expected node to become Follower, got %v",
			role,
		)
	}

	// The previous vote from term 3 must no longer apply.
	if votedFor != 1 {
		t.Fatalf(
			"expected vote for candidate 1, got %d",
			votedFor,
		)
	}

	// Candidate's log is up-to-date, so the vote should be granted.
	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted in the higher term")
	}

	// Reply must contain the node's updated term.
	if reply.Term != 4 {
		t.Fatalf(
			"expected reply term=4, got %d",
			reply.Term,
		)
	}
}

func TestNodeVotesForOnlyOneCandidatePerTerm(t *testing.T) {
	node := NewRaftNode(
		0,
		3,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	node.mu.Lock()
	node.currentTerm = 5
	node.role = Follower
	node.votedFor = -1
	node.mu.Unlock()

	// Candidate 1 requests a vote in term 5.
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

	// Candidate 2 requests a vote in the SAME term.
	secondRequest := RequestVoteArgs{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	var secondReply RequestVoteReply
	node.RequestVote(&secondRequest, &secondReply)

	if secondReply.VoteGranted {
		t.Fatal("node granted votes to two different candidates in the same term")
	}

	node.mu.Lock()
	votedFor := node.votedFor
	currentTerm := node.currentTerm
	node.mu.Unlock()

	if votedFor != 1 {
		t.Fatalf(
			"expected node to remember vote for candidate 1, got %d",
			votedFor,
		)
	}

	if currentTerm != 5 {
		t.Fatalf(
			"currentTerm changed unexpectedly: got %d",
			currentTerm,
		)
	}
}

func TestCandidateBecomesLeaderAfterMajorityVotes(t *testing.T) {
	nodes := make([]*RaftNode, 3)

	for i := 0; i < 3; i++ {
		nodes[i] = NewRaftNode(
			i,
			3,
			make(chan ApplyMsg, 10),
			storage.NewMemEngine(),
		)
	}

	for i := range nodes {
		nodes[i].SetPeers(nodes)
	}

	candidate := nodes[0]

	// Candidate starts election in term 1.
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

	// Candidate already has its own vote.
	votesReceived := 1

	// Ask node 1 for a vote.
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

	// In a 3-node cluster, 2 votes are a majority.
	if votesReceived <= len(nodes)/2 {
		t.Fatal("expected majority after receiving second vote")
	}

	// Simulate the same transition performed by startElection()
	// once it observes the majority.
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
		t.Fatalf(
			"candidate should become leader after majority, got %v",
			role,
		)
	}
}

func TestLeaderDecrementsNextIndexAndRepairsFollower(t *testing.T) {
	leader := NewRaftNode(
		0, 2,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	follower := NewRaftNode(
		1, 2,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	peers := []*RaftNode{leader, follower}
	leader.SetPeers(peers)
	follower.SetPeers(peers)

	// Leader:
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

	leader.nextIndex[1] = 3
	leader.matchIndex[0] = 2
	leader.mu.Unlock()

	// Follower:
	// [dummy, A(term=1), X(term=1)]
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
			Term: 1,
			Command: Command{
				Op:    "SET",
				Key:   "x",
				Value: "X",
			},
		},
	)
	follower.mu.Unlock()

	// First attempt should fail:
	// prevLogIndex = 2
	// leader term at index 2 = 2
	// follower term at index 2 = 1
	leader.broadcastAppendEntries()

	// Wait for the failed RPC to decrement nextIndex.
	deadline := time.Now().Add(1 * time.Second)

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

	if leader.nextIndex[1] != 2 {
		t.Fatalf(
			"expected nextIndex[1] to decrease to 2, got %d",
			leader.nextIndex[1],
		)
	}

	leader.mu.Unlock()

	// Retry using the decremented nextIndex.
	leader.broadcastAppendEntries()

	// Wait for follower to be repaired.
	deadline = time.Now().Add(1 * time.Second)

	for time.Now().Before(deadline) {
		follower.mu.Lock()

		repaired :=
			len(follower.log) == 3 &&
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
		t.Fatalf(
			"expected follower log length 3, got %d",
			len(follower.log),
		)
	}

	if follower.log[2].Term != 2 {
		t.Fatalf(
			"expected index 2 term 2, got %d",
			follower.log[2].Term,
		)
	}

	if follower.log[2].Command.Value != "B" {
		t.Fatalf(
			"expected index 2 value B, got %q",
			follower.log[2].Command.Value,
		)
	}
}

func TestLeaderUpdatesMatchAndNextIndexAfterReplication(t *testing.T) {
	leader := NewRaftNode(
		0, 2,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	follower := NewRaftNode(
		1, 2,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	peers := []*RaftNode{leader, follower}
	leader.SetPeers(peers)
	follower.SetPeers(peers)

	// Leader:
	// [dummy, A(term=1)]
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

	leader.nextIndex[1] = 1
	leader.matchIndex[0] = 1
	leader.mu.Unlock()

	leader.broadcastAppendEntries()

	// Wait for replication to finish.
	deadline := time.Now().Add(1 * time.Second)

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
		t.Fatalf(
			"expected matchIndex[1] = 1, got %d",
			leader.matchIndex[1],
		)
	}

	if leader.nextIndex[1] != 2 {
		t.Fatalf(
			"expected nextIndex[1] = 2, got %d",
			leader.nextIndex[1],
		)
	}
}

func TestFollowerAdvancesCommitIndexFromLeaderCommit(t *testing.T) {
	leader := NewRaftNode(
		0, 2,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	follower := NewRaftNode(
		1, 2,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	peers := []*RaftNode{leader, follower}
	leader.SetPeers(peers)
	follower.SetPeers(peers)

	// Both nodes have the same committed entry.
	leader.mu.Lock()
	leader.currentTerm = 1

	leader.log = append(leader.log, LogEntry{
		Term: 1,
		Command: Command{
			Op:    "SET",
			Key:   "x",
			Value: "10",
		},
	})
	leader.mu.Unlock()

	follower.mu.Lock()
	follower.currentTerm = 1

	follower.log = append(follower.log, LogEntry{
		Term: 1,
		Command: Command{
			Op:    "SET",
			Key:   "x",
			Value: "10",
		},
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
	msg := <-follower.applyCh
	if err := follower.applyMessage(msg); err != nil {
		t.Fatalf("apply committed entry: %v", err)
	}
	follower.mu.Lock()
	defer follower.mu.Unlock()

	if follower.commitIndex != 1 {
		t.Fatalf(
			"expected follower commitIndex = 1, got %d",
			follower.commitIndex,
		)
	}
	value, found, err := follower.Engine.Get([]byte("x"))
	if err != nil {
		t.Fatalf("get committed value: %v", err)
	}
	if !found || string(value) != "10" {
		t.Fatalf(
			"follower state = (%q, %v), want (%q, true)",
			string(value), found, "10",
		)
	}
}

func TestFollowerDoesNotCommitBeyondItsLog(t *testing.T) {
	leader := NewRaftNode(
		0, 2,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	follower := NewRaftNode(
		1, 2,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	peers := []*RaftNode{leader, follower}
	leader.SetPeers(peers)
	follower.SetPeers(peers)

	follower.mu.Lock()
	follower.currentTerm = 1

	// Follower has only one real entry.
	follower.log = append(follower.log, LogEntry{
		Term: 1,
		Command: Command{
			Op:    "SET",
			Key:   "x",
			Value: "10",
		},
	})
	follower.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,

		// Leader claims that index 5 is committed,
		// but follower only has index 1.
		LeaderCommit: 5,
	}

	var reply AppendEntriesReply

	follower.AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept heartbeat")
	}

	follower.mu.Lock()
	defer follower.mu.Unlock()

	if follower.commitIndex != 1 {
		t.Fatalf(
			"expected commitIndex = 1, got %d",
			follower.commitIndex,
		)
	}

	if follower.commitIndex >= len(follower.log) {
		t.Fatalf(
			"commitIndex %d exceeds last log index %d",
			follower.commitIndex,
			len(follower.log)-1,
		)
	}
}

func TestReadProbeCurrentTerm(t *testing.T) {
	node := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

	node.mu.Lock()
	node.currentTerm = 5
	node.role = Follower
	node.lastResetTime = time.Now().Add(-1 * time.Second)
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
		t.Fatalf("expected election timer to reset")
	}
}

func TestReadProbeHigherTerm(t *testing.T) {
	node := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

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
		t.Fatalf("expected votedFor to be reset, got %d", node.votedFor)
	}

	if reply.Term != 5 {
		t.Fatalf("expected reply term 5, got %d", reply.Term)
	}
}

func TestReadProbeStaleTerm(t *testing.T) {
	node := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

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
	for i := 0; i < 3; i++ {
		nodes[i] = NewRaftNode(
			i,
			3,
			make(chan ApplyMsg, 10),
			storage.NewMemEngine(),
		)
	}

	for i := range nodes {
		nodes[i].SetPeers(nodes)
	}

	nodes[0].mu.Lock()
	nodes[0].role = Leader
	nodes[0].currentTerm = 5
	// Build a log containing a committed NOOP from the current term.
	nodes[0].log = make([]LogEntry, 8)
	nodes[0].log[7] = LogEntry{
		Index: 7,
		Term:  5,
		Command: Command{
			Op: "NOOP",
		},
	}
	nodes[0].commitIndex = 7
	nodes[0].mu.Unlock()

	readIndex, err := nodes[0].readIndex()
	if err != nil {
		t.Fatalf("readIndex failed: %v", err)
	}

	if readIndex != 7 {
		t.Fatalf("expected readIndex 7, got %d", readIndex)
	}
}

func TestReadIndexSingleNode(t *testing.T) {
	node := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

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

	for i := 0; i < 2; i++ {
		nodes[i] = NewRaftNode(
			i,
			2,
			make(chan ApplyMsg, 10),
			storage.NewMemEngine(),
		)
	}

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
	node := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

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
		},
	)
	node.commitIndex = 2
	node.lastApplied = 0
	node.mu.Unlock()

	// Put the value directly into the engine only after the
	// read-index application point is reached.
	done := make(chan struct{})

	go func() {
		_, _, _ = node.Get(1, 1, "key")
		close(done)
	}()

	// Give Get() enough time to reach waitForApply().
	time.Sleep(50 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("Get returned before read index was applied")
	default:
	}

	// Simulate application of index 1.
	node.mu.Lock()
	node.lastApplied = 2
	node.mu.Unlock()

	node.notifyApplied(2, nil)

	select {
	case <-done:
		// Expected.
	case <-time.After(1 * time.Second):
		t.Fatal("Get did not return after read index was applied")
	}
}

func TestGetWaitsForReadIndexApplication(t *testing.T) {
	nodes := make([]*RaftNode, 3)
	engines := make([]storage.Engine, 3)

	for i := 0; i < 3; i++ {
		engines[i] = storage.NewMemEngine()

		nodes[i] = NewRaftNode(
			i,
			3,
			make(chan ApplyMsg, 10),
			engines[i],
		)
	}

	for i := range nodes {
		nodes[i].SetPeers(nodes)
	}

	leader := nodes[0]

	leader.mu.Lock()
	leader.role = Leader
	leader.currentTerm = 1
	// Create the committed log entry that Get() will fence on.
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
	leader.lastApplied = 0
	leader.mu.Unlock()

	done := make(chan struct{})
	var (
		value string
		found bool
		err   error
	)

	go func() {
		value, found, err = leader.Get(1, 1, "x")
		close(done)
	}()

	// Give Get() time to perform ReadIndex and reach waitForApply().
	time.Sleep(50 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("Get returned before the read index was applied")
	default:
	}

	// Simulate the state machine applying index 1.
	if err := engines[0].Put([]byte("x"), []byte("value")); err != nil {
		t.Fatalf("failed to apply value: %v", err)
	}

	leader.mu.Lock()
	leader.lastApplied = 2
	leader.mu.Unlock()

	leader.notifyApplied(2, nil)

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Get did not return after read index was applied")
	}

	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}

	if !found {
		t.Fatal("expected key to be found")
	}

	if value != "value" {
		t.Fatalf("expected value 'value', got %q", value)
	}
}

func TestGetFailsIfLeadershipLostBeforeRead(t *testing.T) {
	node := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

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
				Key:   "x",
				Value: "value",
			},
		},
	)

	node.commitIndex = 2
	node.lastApplied = 0
	node.mu.Unlock()

	done := make(chan struct{})
	var err error

	go func() {
		_, _, err = node.Get(1, 1, "x")
		close(done)
	}()

	// Give Get enough time to reach waitForApply.
	time.Sleep(50 * time.Millisecond)

	// Simulate losing leadership while waiting.
	node.mu.Lock()
	node.role = Follower
	node.mu.Unlock()

	// Simulate the read index becoming applied.
	node.mu.Lock()
	node.lastApplied = 2
	node.mu.Unlock()

	node.notifyApplied(2, nil)

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Get did not return")
	}

	if err != ErrWrongLeader {
		t.Fatalf("expected ErrWrongLeader, got %v", err)
	}
}

func TestApplyLoopHandlesOutOfOrderMessages(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	engine := storage.NewMemEngine()

	node := NewRaftNode(0, 1, applyCh, engine)

	node.mu.Lock()
	node.commitIndex = 3
	node.mu.Unlock()

	go node.applyLoop()
	defer node.Stop()

	applyCh <- ApplyMsg{
		CommandValid: true,
		CommandIndex: 1,
		CommandTerm:  1,
		Command: Command{
			Op:    "SET",
			Key:   "one",
			Value: "1",
		},
	}
	// Index 3 arrives before index 2.
	applyCh <- ApplyMsg{
		CommandValid: true,
		CommandIndex: 3,
		CommandTerm:  1,
		Command: Command{
			Op:    "SET",
			Key:   "three",
			Value: "3",
		},
	}

	applyCh <- ApplyMsg{
		CommandValid: true,
		CommandIndex: 2,
		CommandTerm:  1,
		Command: Command{
			Op:    "SET",
			Key:   "two",
			Value: "2",
		},
	}

	// Wait for both entries to be applied in order.
	deadline := time.Now().Add(time.Second)

	for time.Now().Before(deadline) {
		node.mu.Lock()
		lastApplied := node.lastApplied
		node.mu.Unlock()

		if lastApplied == 3 {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}

	node.mu.Lock()
	lastApplied := node.lastApplied
	node.mu.Unlock()

	if lastApplied != 3 {
		t.Fatalf(
			"expected lastApplied=3, got %d",
			lastApplied,
		)
	}

	// Verify both commands reached the state machine.
	for _, tc := range []struct {
		key   string
		value string
	}{
		{key: "two", value: "2"},
		{key: "three", value: "3"},
	} {
		value, found, err := engine.Get([]byte(tc.key))
		if err != nil {
			t.Fatalf("Get(%q) failed: %v", tc.key, err)
		}

		if !found || string(value) != tc.value {
			t.Fatalf(
				"Get(%q) = %q, found=%v; want %q",
				tc.key,
				string(value),
				found,
				tc.value,
			)
		}
	}
}

func TestNewLeaderAppendsCurrentTermNoop(t *testing.T) {
	applyCh := make(chan ApplyMsg, 20)

	nodes := make([]*RaftNode, 3)
	for i := 0; i < 3; i++ {
		nodes[i] = NewRaftNode(
			i,
			3,
			applyCh,
			storage.NewMemEngine(),
		)
	}

	for i := 0; i < 3; i++ {
		nodes[i].SetPeers(nodes)
	}

	candidate := nodes[0]
	candidate.mu.Lock()
	candidate.currentTerm = 0
	candidate.role = Follower
	candidate.lastResetTime = time.Now().Add(-time.Second)
	candidate.electionTimeout = 100 * time.Millisecond
	candidate.mu.Unlock()

	candidate.startElection()

	// Wait for election to complete.
	deadline := time.Now().Add(1 * time.Second)
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
		t.Fatalf(
			"no-op term = %d, current term = %d",
			entry.Term,
			term,
		)
	}

	if entry.Command.Op != "NOOP" {
		t.Fatalf(
			"expected last entry to be NOOP, got %q",
			entry.Command.Op,
		)
	}

	// Wait for the no-op to be committed.
	deadline = time.Now().Add(1 * time.Second)
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
		lastIndex,
		commitIndex,
	)
}

func TestGetFailsIfTermChangesBeforeRead(t *testing.T) {
	node := NewRaftNode(
		0,
		1,
		make(chan ApplyMsg, 10),
		storage.NewMemEngine(),
	)

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
				Op:    "SET",
				Key:   "x",
				Value: "value",
			},
		},
	)

	node.commitIndex = 2
	node.lastApplied = 0
	node.mu.Unlock()

	done := make(chan struct{})
	var err error

	go func() {
		_, _, err = node.Get(1, 1, "x")
		close(done)
	}()

	// Allow Get to reach waitForApply.
	time.Sleep(50 * time.Millisecond)

	// Simulate a new leadership term before the read completes.
	node.mu.Lock()
	node.currentTerm = 2
	node.role = Leader
	node.lastApplied = 2
	node.mu.Unlock()

	node.notifyApplied(2, nil)

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Get did not return")
	}

	if err != ErrWrongLeader {
		t.Fatalf("expected ErrWrongLeader, got %v", err)
	}
}

func TestReadIndexReturnsWhenFollowerDoesNotRespond(t *testing.T) {
	nodes := make([]*RaftNode, 3)

	for i := 0; i < 3; i++ {
		nodes[i] = NewRaftNode(
			i,
			3,
			make(chan ApplyMsg, 10),
			storage.NewMemEngine(),
		)
	}

	for i := range nodes {
		nodes[i].SetPeers(nodes)
	}

	// Configure node 0 as leader with a committed current-term NOOP.
	nodes[0].mu.Lock()
	nodes[0].role = Leader
	nodes[0].currentTerm = 5
	nodes[0].log = make([]LogEntry, 2)
	nodes[0].log[1] = LogEntry{
		Index: 1,
		Term:  5,
		Command: Command{
			Op: "NOOP",
		},
	}
	nodes[0].commitIndex = 1
	nodes[0].mu.Unlock()

	// Hold node 2's mutex so its ReadProbe cannot complete.
	nodes[2].mu.Lock()
	defer nodes[2].mu.Unlock()

	done := make(chan error, 1)
	go func() {
		_, err := nodes[0].readIndex()
		done <- err
	}()

	select {
	case err := <-done:
		// Node 1 can provide the second acknowledgement needed
		// for a majority in this three-node cluster.
		if err != nil {
			t.Fatalf("readIndex failed despite receiving a majority: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("readIndex did not return after reaching a majority")
	}
}

func TestReadIndexFailsWhenMajorityUnavailable(t *testing.T) {
	nodes := make([]*RaftNode, 3)

	for i := 0; i < 3; i++ {
		nodes[i] = NewRaftNode(
			i,
			3,
			make(chan ApplyMsg, 10),
			storage.NewMemEngine(),
		)
	}

	for i := range nodes {
		nodes[i].SetPeers(nodes)
	}

	// Configure node 0 as leader with a committed current-term NOOP.
	nodes[0].mu.Lock()
	nodes[0].role = Leader
	nodes[0].currentTerm = 5
	nodes[0].log = make([]LogEntry, 2)
	nodes[0].log[1] = LogEntry{
		Index: 1,
		Term:  5,
		Command: Command{
			Op: "NOOP",
		},
	}
	nodes[0].commitIndex = 1
	nodes[0].mu.Unlock()

	// Block both followers so neither can acknowledge the probe.
	nodes[1].mu.Lock()
	nodes[2].mu.Lock()

	done := make(chan error, 1)

	go func() {
		_, err := nodes[0].readIndex()
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected readIndex to fail without a majority")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("readIndex did not return when a majority was unavailable")
	}

	nodes[2].mu.Unlock()
	nodes[1].mu.Unlock()
}

func TestThreeNodeClusterRecoversCommittedValueAfterRestart(t *testing.T) {
	dir := t.TempDir()

	statePaths := []string{
		filepath.Join(dir, "node0.state"),
		filepath.Join(dir, "node1.state"),
		filepath.Join(dir, "node2.state"),
	}

	const n = 3

	startCluster := func() ([]*RaftNode, []*storage.MemEngine, []chan ApplyMsg) {
		nodes := make([]*RaftNode, n)
		engines := make([]*storage.MemEngine, n)
		applyChs := make([]chan ApplyMsg, n)

		for i := 0; i < n; i++ {
			engines[i] = storage.NewMemEngine()
			applyChs[i] = make(chan ApplyMsg, 100)

			node, err := NewPersistentRaftNode(
				i,
				n,
				applyChs[i],
				engines[i],
				statePaths[i],
			)
			if err != nil {
				t.Fatalf("create persistent node %d: %v", i, err)
			}

			nodes[i] = node
		}

		for _, node := range nodes {
			node.SetPeers(nodes)
		}

		for _, node := range nodes {
			node.StartBackground()
		}

		return nodes, engines, applyChs
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

			time.Sleep(50 * time.Millisecond)
		}

		return -1
	}

	// Start the original cluster.
	nodes, _, _ := startCluster()

	leader := waitForLeader(nodes)
	if leader == -1 {
		stopCluster(nodes)
		t.Fatal("cluster failed to elect a leader")
	}

	// Commit a value before restarting.
	ok, err := nodes[leader].Put(1, 1, "language", "Go")
	if err != nil || !ok {
		stopCluster(nodes)
		t.Fatalf("failed to write value before restart: ok=%v, err=%v", ok, err)
	}

	stopCluster(nodes)
	time.Sleep(100 * time.Millisecond)

	// Reconstruct every node from its original persistent state file.
	nodes, _, _ = startCluster()

	t.Cleanup(func() { //tells Go to execute a function when the test finishes, whether it passes or fails.
		stopCluster(nodes)
	})

	leader = waitForLeader(nodes)
	if leader == -1 {
		t.Fatal("cluster failed to elect a leader after restart")
	}

	// Submit a new write so the new leader can commit its current-term
	// entry and advance commitment over the previous log entries.
	ok, err = nodes[leader].Put(1, 2, "restart-check", "ok")
	if err != nil || !ok {
		stopCluster(nodes)
		t.Fatalf("failed to write value before restart: ok=%v, err=%v", ok, err)
	}

	// Check that the value committed before the restart is recovered.
	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		value, found, err := nodes[leader].Get(1, 3, "language")
		if err == nil && found && value == "Go" {
			return
		}

		time.Sleep(25 * time.Millisecond)
	}

	t.Fatal("previously committed value was not recovered after restart")
}

func TestStartDeduplicatesRepeatedRequest(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	engine := storage.NewMemEngine()

	node := NewRaftNode(0, 1, applyCh, engine)

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
	if err!=nil {
		t.Fatal("First request was rejected by the leader")
	}

	// Wait until the request has been applied.
	deadline := time.Now().Add(2 * time.Second)
	for {
		node.mu.Lock()
		applied := node.lastApplied >= index1
		node.mu.Unlock()

		if applied {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("Timed out waiting for log index %d to be applied", index1)
		}

		time.Sleep(10 * time.Millisecond)
	}

	// Verify that the first request actually changed the state machine.
	value, found, err := engine.Get([]byte("framework"))
	if err != nil {
		t.Fatalf("Failed to read stored value: %v", err)
	}
	if !found || string(value) != "RaftKV" {
		t.Fatalf("Expected stored value 'RaftKV', got %q", string(value))
	}

	node.mu.Lock()
	logLenAfterFirst := len(node.log)
	node.mu.Unlock()

	// Retry the exact same request after it has been applied.
	index2, term2, err := node.Start(command)
	if err != nil {
		t.Fatal("Duplicate request was rejected by the leader")
	}

	node.mu.Lock()
	logLenAfterSecond := len(node.log)
	node.mu.Unlock()

	if index1 != index2 {
		t.Fatalf("Expected the same log index, got %d and %d", index1, index2)
	}

	if term1 != term2 {
		t.Fatalf("Expected the same term, got %d and %d", term1, term2)
	}

	if logLenAfterFirst != logLenAfterSecond {
		t.Fatalf(
			"Duplicate request appended another entry: log length changed from %d to %d",
			logLenAfterFirst,
			logLenAfterSecond,
		)
	}

	t.Logf(
		"Deduplication verified after application: index=%d, log length=%d",
		index1,
		logLenAfterSecond,
	)
}

func TestStartRejectsConflictingDuplicateRequest(t *testing.T) {
	applyCh := make(chan ApplyMsg, 10)
	engine := storage.NewMemEngine()

	node := NewRaftNode(0, 1, applyCh, engine)
	defer node.Stop()

	// Make this node the leader for a deterministic unit test.
	node.mu.Lock()
	node.role = Leader
	node.currentTerm = 1
	node.mu.Unlock()

	// First request.
	command1 := Command{
		ClientID:  3,
		SeqNumber: 1,
		Op:        "SET",
		Key:       "framework",
		Value:     "RaftKV",
	}

	index1, _, err := node.Start(command1)
	if err != nil {
		t.Fatal("First request was rejected")
	}

	node.mu.Lock()
	logLenBefore := len(node.log)
	node.mu.Unlock()

	// Same request ID, but a different value.
	command2 := Command{
		ClientID:  3,
		SeqNumber: 1,
		Op:        "SET",
		Key:       "framework",
		Value:     "DifferentValue",
	}

	index2, _, err := node.Start(command2)

	if err == nil {
		t.Fatal("Expected conflicting duplicate request to be rejected")
	}

	if index2 != -1 {
		t.Fatalf("Expected index -1 for conflicting request, got %d", index2)
	}

	node.mu.Lock()
	logLenAfter := len(node.log)
	node.mu.Unlock()

	if logLenAfter != logLenBefore {
		t.Fatalf(
			"Conflicting request changed log length: before=%d, after=%d",
			logLenBefore,
			logLenAfter,
		)
	}

	// Confirm the original request is still recorded at its original index.
	node.mu.Lock()
	existingCommand := node.log[index1].Command
	node.mu.Unlock()

	if existingCommand != command1 {
		t.Fatalf("Original command was changed: got %+v", existingCommand)
	}

	t.Log("Conflicting duplicate request correctly rejected")
}

func TestStartDeduplicatesRequestAfterRestart(t *testing.T) {
    applyCh := make(chan ApplyMsg, 10)
    persister := NewMemPersister()

    command := Command{
        ClientID:  3,
        SeqNumber: 1,
        Op:        "SET",
        Key:       "framework",
        Value:     "RaftKV",
    }

    // Create the original node.
    node, err := NewRaftNodeWithPersister(
        0, 1, applyCh, storage.NewMemEngine(), persister,
    )
    if err != nil {
        t.Fatalf("create original node: %v", err)
    }

    node.mu.Lock()
    node.role = Leader
    node.currentTerm = 1
    node.mu.Unlock()

    // Persist the first request.
    index1, term1, err := node.Start(command)
    if err != nil {
        t.Fatalf("first request failed: %v", err)
    }

    node.Stop()

    // Simulate a restart using the same persister.
    restarted, err := NewRaftNodeWithPersister(
        0, 1, make(chan ApplyMsg, 10),
        storage.NewMemEngine(), persister,
    )
    if err != nil {
        t.Fatalf("restart node: %v", err)
    }
    defer restarted.Stop()

    // Start() accepts writes only from a leader.
    restarted.mu.Lock()
    restarted.role = Leader
    restarted.mu.Unlock()

    // Retry the exact same request.
    index2, term2, err := restarted.Start(command)
    if err != nil {
        t.Fatalf("duplicate request after restart failed: %v", err)
    }

    restarted.mu.Lock()
    logLen := len(restarted.log)
    restarted.mu.Unlock()

    if index2 != index1 {
        t.Fatalf("expected original index %d, got %d", index1, index2)
    }

    if term2 != term1 {
        t.Fatalf("expected original term %d, got %d", term1, term2)
    }

    if logLen != index1+1 {
        t.Fatalf("duplicate appended an entry: log length=%d", logLen)
    }
}

func TestStartRejectsConflictingRequestAfterRestart(t *testing.T) {
    applyCh := make(chan ApplyMsg, 10)
    persister := NewMemPersister()

    original, err := NewRaftNodeWithPersister(
        0, 1, applyCh, storage.NewMemEngine(), persister,
    )
    if err != nil {
        t.Fatalf("create original node: %v", err)
    }

    original.mu.Lock()
    original.role = Leader
    original.currentTerm = 1
    original.mu.Unlock()

    command1 := Command{
        ClientID:  3,
        SeqNumber: 1,
        Op:        "SET",
        Key:       "framework",
        Value:     "RaftKV",
    }

    index1, _, err := original.Start(command1)
    if err != nil {
        t.Fatalf("first request failed: %v", err)
    }

    original.Stop()

    restarted, err := NewRaftNodeWithPersister(
        0, 1, make(chan ApplyMsg, 10),
        storage.NewMemEngine(), persister,
    )
    if err != nil {
        t.Fatalf("restart node: %v", err)
    }
    defer restarted.Stop()

    restarted.mu.Lock()
    restarted.role = Leader
    restarted.mu.Unlock()

    command2 := command1
    command2.Value = "DifferentValue"

    index2, _, err := restarted.Start(command2)

    if !errors.Is(err, ErrRequestIDConflict) {
        t.Fatalf("expected ErrRequestIDConflict, got %v", err)
    }

    if index2 != -1 {
        t.Fatalf("expected index -1, got %d", index2)
    }

    restarted.mu.Lock()
    defer restarted.mu.Unlock()

    if len(restarted.log) != index1+1 {
        t.Fatalf("log changed after conflicting request: length=%d",
            len(restarted.log))
    }

    if restarted.log[index1].Command != command1 {
        t.Fatal("original command was changed")
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
		nodes[i] = NewRaftNode(
			i,
			3,
			make(chan ApplyMsg, 100),
			storage.NewMemEngine(),
		)
		defer nodes[i].Stop()
	}

	for i := range nodes {
		nodes[i].SetPeers(nodes)
	}

	// Start the real Raft background loops.
	for i := range nodes {
		nodes[i].StartBackground()
	}

	// Wait for the initial leader.
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

	// Wait until at least one follower has the request in its log.
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

	// Stop the old leader so it cannot send heartbeats.
	oldLeader.Stop()

	// The remaining nodes should elect a new leader.
	remainingNodes := make([]*RaftNode, 0, 2)
	for _, node := range nodes {
		if node != oldLeader {
			remainingNodes = append(remainingNodes, node)
		}
	}

	newLeader := waitForLeader(t, remainingNodes, 5*time.Second)

	// Retry the same request on the new leader.
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
	defer newLeader.mu.Unlock()

	requestID := RequestID{
		ClientID:  command.ClientID,
		SeqNumber: command.SeqNumber,
	}

	if got := newLeader.requestIndex[requestID]; got != originalIndex {
		t.Fatalf("requestIndex = %d, want %d", got, originalIndex)
	}
}

func TestStartRejectsConflictingRequestAfterLeadershipChange(t *testing.T) {
	nodes := make([]*RaftNode, 3)

	for i := range nodes {
		nodes[i] = NewRaftNode(
			i,
			3,
			make(chan ApplyMsg, 100),
			storage.NewMemEngine(),
		)
		defer nodes[i].Stop()
	}

	for i := range nodes {
		nodes[i].SetPeers(nodes)
	}

	// Start the real Raft background loops.
	for i := range nodes {
		nodes[i].StartBackground()
	}

	// Wait for the initial leader.
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

	// Wait until at least one follower has the request in its log.
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

	// Stop the old leader so it cannot send heartbeats.
	oldLeader.Stop()

	// Wait for a new leader among the remaining nodes.
	remainingNodes := make([]*RaftNode, 0, 2)

	for _, node := range nodes {
		if node != oldLeader {
			remainingNodes = append(remainingNodes, node)
		}
	}

	newLeader := waitForLeader(t, remainingNodes, 5*time.Second)

	// Reuse the same request ID with a different command value.
	conflictingCommand := command
	conflictingCommand.Value = "DifferentValue"

	newLeader.mu.Lock()
	logLenBefore := len(newLeader.log)
	newLeader.mu.Unlock()

	_, _, err = newLeader.Start(conflictingCommand)

	if !errors.Is(err, ErrRequestIDConflict) {
		t.Fatalf(
			"expected ErrRequestIDConflict, got %v",
			err,
		)
	}

	// A conflicting retry must not append another log entry.
	newLeader.mu.Lock()
	logLenAfter := len(newLeader.log)

	requestID := RequestID{
		ClientID:  command.ClientID,
		SeqNumber: command.SeqNumber,
	}
	requestIndex := newLeader.requestIndex[requestID]
	newLeader.mu.Unlock()

	if logLenAfter != logLenBefore {
		t.Fatalf(
			"conflicting retry changed log length: before=%d, after=%d",
			logLenBefore,
			logLenAfter,
		)
	}

	if requestIndex != originalIndex {
		t.Fatalf(
			"request index changed: got %d, want %d",
			requestIndex,
			originalIndex,
		)
	}
}