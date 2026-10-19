package server

import (
	"RaftKV/internal/raft"
	"RaftKV/internal/storage"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestKVServerNetworkAPI(t *testing.T) {
	n := 3
	nodes := make([]*raft.RaftNode, n)
	servers := make([]*KVServer, n)
	testServers := make([]*httptest.Server, n)
	applyChs := make([]chan raft.ApplyMsg, n)
	engines := make([]storage.Engine, n)

	for i := 0; i < n; i++ {
		engines[i] = storage.NewMemEngine()
		applyChs[i] = make(chan raft.ApplyMsg, 100)
		nodes[i] = raft.NewRaftNode(i, n, applyChs[i], engines[i])
	}

	for i := 0; i < n; i++ {
		nodes[i].SetPeers(nodes)
		nodes[i].StartBackground()
		servers[i] = NewKVServer(nodes[i])
		testServers[i] = httptest.NewServer(servers[i])
	}

	defer func() {
		for i := 0; i < n; i++ {
			testServers[i].Close()
			nodes[i].Stop()
		}
	}()

	// Wait for leader election and discover leader via network requests
	t.Log("Waiting for cluster leader election over test network...")
	var leaderIdx = -1
	start := time.Now()
	for time.Since(start) < 3*time.Second {
		for i := 0; i < n; i++ {
			body, _ := json.Marshal(WriteRequest{ClientID: 1, SeqNumber: 1, Key: "ping", Value: "pong"}) //converts that struct into JSON bytes.
			resp, err := http.Post(testServers[i].URL+"/kv", "application/json", bytes.NewBuffer(body))
			if err == nil {
				if resp.StatusCode == http.StatusOK {
					leaderIdx = i
					resp.Body.Close()
					break
				}
				resp.Body.Close()
			}
		}
		if leaderIdx != -1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if leaderIdx == -1 {
		t.Fatalf("Failed to identify leader server via network")
	}
	t.Logf("Leader HTTP server identified at node %d", leaderIdx)

	followerIdx := (leaderIdx + 1) % n

	// Test 1: Follower returns StatusServiceUnavailable (Wrong Leader)
	body, err := json.Marshal(WriteRequest{ClientID: 2, SeqNumber: 1, Key: "hello", Value: "world"})
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}
	resp, err := http.Post(testServers[followerIdx].URL+"/kv", "application/json", bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("Failed to send request to follower: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("Expected status 503 Service Unavailable on follower, got %d", resp.StatusCode)
	}
	t.Log("Follower network endpoint correctly rejected write with 503")

	// Test 2: Successful PUT on Leader Server
	putBody, err := json.Marshal(WriteRequest{ClientID: 3, SeqNumber: 1, Key: "framework", Value: "RaftKV"})
	if err != nil {
		t.Fatalf("Failed to marshal PUT request: %v", err)
	}
	resp, err = http.Post(testServers[leaderIdx].URL+"/kv", "application/json", bytes.NewBuffer(putBody))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed PUT on leader server: %v, status: %v", err, resp.StatusCode)
	}
	resp.Body.Close()
	t.Log("HTTP PUT successful on leader server")

	// Allow replication time
	time.Sleep(100 * time.Millisecond)

	// Test 3: Successful GET on Leader Server
	getBody, err := json.Marshal(WriteRequest{ClientID: 3, SeqNumber: 2, Key: "framework"})
	if err != nil {
		t.Fatalf("Failed to marshal GET request: %v", err)
	}

	getReq, err := http.NewRequest(http.MethodGet, testServers[leaderIdx].URL+"/kv", bytes.NewBuffer(getBody))
	if err != nil {
		t.Fatalf("Failed to create GET request: %v", err)
	}
	getReq.Header.Set("Content-Type", "application/json")

	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("Failed GET on leader server: %v", err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", getResp.StatusCode)
	}

	var getResult Response
	if err := json.NewDecoder(getResp.Body).Decode(&getResult); err != nil {
		t.Fatalf("Failed to decode GET response: %v", err)
	}

	if !getResult.Found || getResult.Value != "RaftKV" {
		t.Fatalf(
			"Expected value 'RaftKV', got found=%v, value='%s'",
			getResult.Found,
			getResult.Value,
		)
	}

	t.Logf("HTTP GET verified successfully: 'framework' = '%s'", getResult.Value)
}
