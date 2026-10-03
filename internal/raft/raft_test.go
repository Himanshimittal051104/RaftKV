package raft

import (
	"errors"
	"testing"
	"time"

	"RaftKV/internal/storage"
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
	index, term, ok := leader.Start(Command{
		Op:    "SET",
		Key:   "x",
		Value: "100",
	})

	if !ok {
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
		Term: 1,
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
	node.mu.Unlock()

	if lastApplied != 1 {
		t.Fatalf(
			"expected lastApplied=1, got %d",
			lastApplied,
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
	index, term, ok := leader.Start(Command{
		Op:    "SET",
		Key:   "x",
		Value: "10",
	})

	if !ok {
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
	// Setup a 3-node cluster with MemEngines and apply channels
	n := 3
	nodes := make([]*RaftNode, n)
	engines := make([]storage.Engine, n)
	applyChs := make([]chan ApplyMsg, n)

	for i := 0; i < n; i++ {
    	engines[i] = storage.NewMemEngine()
    	applyChs[i] = make(chan ApplyMsg, 100)
    	nodes[i] = NewRaftNode(i, n, applyChs[i], engines[i])
	}

	// Test-side state-machine consumer.
	// Test-side state-machine consumer.
	for i := 0; i < n; i++ {
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


	// Connect peers and start their background loops
	for i := 0; i < n; i++ {
		nodes[i].SetPeers(nodes)
		nodes[i].StartBackground() // Start the background election and heartbeat loop
	}
	defer func() {
		for i := 0; i < n; i++ {
			nodes[i].Stop()
		}
	}()

	t.Log("Waiting for leader election...")
	var leader *RaftNode
	leaderID := -1

	// Wait up to 3 seconds to find a leader
	start := time.Now()
	for time.Since(start) < 3*time.Second {
		for i := 0; i < n; i++ {
			nodes[i].mu.Lock()
			if nodes[i].role == Leader {
				leader = nodes[i]
				leaderID = i
			}
			nodes[i].mu.Unlock()
		}
		if leader != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if leader == nil {
		t.Fatalf("Failed to elect a leader within timeout")
	}
	t.Logf("Leader found: Node %d", leaderID)

	// Test 1: Verify Follower returns ErrWrongLeader
	followerID := (leaderID + 1) % n
	follower := nodes[followerID]
	
	_, err := follower.Put("test_key", "test_val")
	if !errors.Is(err, ErrWrongLeader) {
		t.Fatalf("Expected ErrWrongLeader on follower Put, got: %v", err)
	}
	t.Log("Follower correctly rejected Put with ErrWrongLeader")

	// Test 2: Successful Put on Leader
	success, err := leader.Put("city", "Delhi")
	if !success || err != nil {
		t.Fatalf("Failed to Put on leader: %v", err)
	}
	t.Log("Put command submitted successfully to leader")

	

	// Test 3: Successful Get on Leader
	val, found, err := leader.Get("city")
	if err != nil || !found || val != "Delhi" {
		t.Fatalf("Failed to Get correct value: val=%s, found=%v, err=%v", val, found, err)
	}
	t.Logf("Get verified on leader: 'city' = '%s'", val)

	// Test 4: Successful Delete on Leader
	success, err = leader.Delete("city")
	if !success || err != nil {
		t.Fatalf("Failed to Delete on leader: %v", err)
	}
	t.Log("Delete command submitted successfully")

	// Verify deletion
	_, found, err = leader.Get("city")
	if found || err != nil {
		t.Fatalf("Expected key 'city' to be deleted, but found=%v", found)
	}
	t.Log("Success! Client API, operations (Put/Get/Delete), and leader redirection verified.")
}

func TestStartRejectedByFollower(t *testing.T) {
	engine := storage.NewMemEngine()
	node := NewRaftNode(0, 1, make(chan ApplyMsg, 10), engine)

	index, term, ok := node.Start(Command{
		Op:    "SET",
		Key:   "x",
		Value: "10",
	})

	if ok {
		t.Fatal("follower should reject Start()")
	}

	if index != -1 || term != -1 {
		t.Fatalf("expected (-1, -1, false), got (%d, %d, %v)",
			index, term, ok)
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

	index, term, ok := node.Start(cmd)

	if !ok {
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

	_, _, ok := node.Start(Command{
		Op:    "SET",
		Key:   "x",
		Value: "10",
	})

	if !ok {
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

	// ------------------------------------------------------------
	// 1. Node 0 is the leader in term 1.
	// ------------------------------------------------------------

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
	index, _, ok := node0.Start(Command{
		Op:    "SET",
		Key:   "x",
		Value: "100",
	})

	if !ok {
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

	if node.lastApplied != 1 {
		node.mu.Unlock()
		t.Fatalf(
			"expected lastApplied=1, got %d",
			node.lastApplied,
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

	// Both entries are committed.
	node.commitIndex = 2
	node.lastApplied = 0

	// First application.
	msgs := node.collectCommittedEntries()

	if len(msgs) != 2 {
		node.mu.Unlock()
		t.Fatalf("expected 2 ApplyMsgs, got %d", len(msgs))
	}

	if node.lastApplied != 2 {
		node.mu.Unlock()
		t.Fatalf(
			"expected lastApplied=2 after first application, got %d",
			node.lastApplied,
		)
	}

	// Calling it again must not re-apply anything.
	msgs = node.collectCommittedEntries()

	if len(msgs) != 0 {
		node.mu.Unlock()
		t.Fatalf(
			"expected no new ApplyMsgs, got %d",
			len(msgs),
		)
	}

	if node.lastApplied != 2 {
		node.mu.Unlock()
		t.Fatalf(
			"lastApplied changed unexpectedly: %d",
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
		_, err := leader.Put("city", "Delhi")
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

	follower.mu.Lock()
	defer follower.mu.Unlock()

	if follower.commitIndex != 1 {
		t.Fatalf(
			"expected follower commitIndex = 1, got %d",
			follower.commitIndex,
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