package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	n := 3
	nodes := make([]*RaftNode, n)
	applyChs := make([]chan ApplyMsg, n)

	for i := 0; i < n; i++ {
		applyChs[i] = make(chan ApplyMsg, 100)
		nodes[i] = NewRaftNode(i, n, applyChs[i])
	}

	for i := 0; i < n; i++ {
		nodes[i].SetPeers(nodes)
		nodes[i].Start()
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