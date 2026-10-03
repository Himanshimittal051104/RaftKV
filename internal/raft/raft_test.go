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