package raft

import (
	"fmt"
	"testing"
	"time"

	"RaftKV/internal/storage" // Make sure this matches your go.mod module name
)

func TestInitialElection(t *testing.T) {
	n := 3
	nodes := make([]*RaftNode, n)
	applyChs := make([]chan ApplyMsg, n)

	for i := 0; i < n; i++ {
		applyChs[i] = make(chan ApplyMsg, 100)
		engine := storage.NewMemEngine()
		nodes[i] = NewRaftNode(i, n, applyChs[i], engine)
	}

	for i := 0; i < n; i++ {
		nodes[i].SetPeers(nodes)
		nodes[i].StartBackground()
	}

	fmt.Println("Waiting for leader election...")
	time.Sleep(600 * time.Millisecond)

	leaders := 0
	leaderID := -1
	for i := 0; i < n; i++ {
		nodes[i].mu.Lock()
		if nodes[i].role == Leader {
			leaders++
			leaderID = i
		}
		fmt.Printf("Node %d role: %s, term: %d\n", i, nodes[i].role, nodes[i].currentTerm)
		nodes[i].mu.Unlock()
	}

	for i := 0; i < n; i++ {
		nodes[i].Stop()
	}

	if leaders != 1 {
		t.Fatalf("Expected exactly 1 leader, but found %d", leaders)
	}

	fmt.Printf("Success! Node %d was elected leader.\n", leaderID)
}

func TestLogReplicationAndStorage(t *testing.T) {
	n := 3
	nodes := make([]*RaftNode, n)
	applyChs := make([]chan ApplyMsg, n)
	engines := make([]storage.Engine, n)

	// 1. Initialize nodes with their own storage engines and apply channels
	for i := 0; i < n; i++ {
		applyChs[i] = make(chan ApplyMsg, 100)
		engines[i] = storage.NewMemEngine()
		nodes[i] = NewRaftNode(i, n, applyChs[i], engines[i])
	}

	for i := 0; i < n; i++ {
		nodes[i].SetPeers(nodes)
		nodes[i].StartBackground()
	}

	// 2. Wait for leader election
	fmt.Println("Waiting for leader election...")
	time.Sleep(600 * time.Millisecond)

	// 3. Find the current leader
	var leader *RaftNode
	leaderID := -1
	for i := 0; i < n; i++ {
		nodes[i].mu.Lock()
		if nodes[i].role == Leader {
			leader = nodes[i]
			leaderID = i
		}
		nodes[i].mu.Unlock()
	}

	if leader == nil {
		t.Fatalf("Failed to elect a leader for the replication test")
	}

	fmt.Printf("Leader found: Node %d. Submitting write command...\n", leaderID)

	// 4. Submit a command to the leader
	cmd := Command{
		ClientID:  1,
		SeqNumber: 1,
		Op:        "SET",
		Key:       "name",
		Value:     "RaftKv",
	}
	index, term, ok := leader.Start(cmd)
	if !ok {
		t.Fatalf("Leader rejected command submission")
	}
	fmt.Printf("Command accepted at log index %d, term %d\n", index, term)

	// 5. Wait for replication / commitment cycle
	time.Sleep(300 * time.Millisecond)

	// 6. Check that all nodes applied the command and stored it in their storage engines
	appliedCount := 0
	for i := 0; i < n; i++ {
		// Drain apply channel
		select {
		case msg := <-applyChs[i]:
			if msg.CommandValid && msg.Command == cmd {
				appliedCount++
				fmt.Printf("Node %d applied command via applyCh at index %d\n", i, msg.CommandIndex)
			}
		default:
		}

		// Verify data is actually stored in the storage engine backend!
		valBytes, exists, err := engines[i].Get([]byte("name"))
		if err != nil || !exists {
			t.Fatalf("Node %d failed to persist key 'name' in storage engine", i)
		}
		valStr := string(valBytes)
		if valStr != "RaftKv" {
			t.Fatalf("Node %d storage engine has incorrect value: got %s, want RaftKv", i, valStr)
		}
		fmt.Printf("Node %d storage engine verified: 'name' = '%s'\n", i, valStr)
	}

	// 7. Clean up
	for i := 0; i < n; i++ {
		nodes[i].Stop()
		engines[i].Close()
	}

	// 8. Assertions
	if appliedCount == 0 {
		t.Fatalf("Expected nodes to apply the replicated command, but none did")
	}

	fmt.Println("Success! Consensus, log replication, state application, and storage persistence verified.")
}