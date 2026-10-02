package raft

import (
	"errors"
	"testing"
	"time"

	"RaftKV/internal/storage"
)

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

	// Wait briefly for log replication and state application
	time.Sleep(200 * time.Millisecond)

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

	time.Sleep(200 * time.Millisecond)

	// Verify deletion
	_, found, err = leader.Get("city")
	if found || err != nil {
		t.Fatalf("Expected key 'city' to be deleted, but found=%v", found)
	}
	t.Log("Success! Client API, operations (Put/Get/Delete), and leader redirection verified.")
}