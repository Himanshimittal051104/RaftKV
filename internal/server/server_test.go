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
	// err is network error, resp is HTTP response. If the follower is not reachable, err will be non-nil. If the follower is reachable but returns an error status, err will be nil and resp.StatusCode will indicate the error.
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

func TestKVServerRejectsConflictingRequestID(t *testing.T) {
	applyCh := make(chan raft.ApplyMsg, 10)
	engine := storage.NewMemEngine()
	node := raft.NewRaftNode(0, 1, applyCh, engine)
	node.StartBackground()

	server := httptest.NewServer(NewKVServer(node))
	defer server.Close()
	defer node.Stop()

	// First request: retry until it succeeds or the deadline expires.
	firstBody, err := json.Marshal(WriteRequest{
		ClientID:  10,
		SeqNumber: 1,
		Key:       "framework",
		Value:     "RaftKV",
	})
	if err != nil {
		t.Fatalf("Failed to marshal first request: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	var firstResp *http.Response

	for time.Now().Before(deadline) {
		resp, err := http.Post(
			server.URL+"/kv",
			"application/json",
			bytes.NewBuffer(firstBody),
		)

		if err == nil {
			if resp.StatusCode == http.StatusOK {
				firstResp = resp
				break
			}
			resp.Body.Close()
		}

		time.Sleep(50 * time.Millisecond)
	}

	if firstResp == nil {
		t.Fatal("First request did not succeed before the deadline")
	}
	firstResp.Body.Close()

	// Second request: same ID, different value.
	secondBody, err := json.Marshal(WriteRequest{
		ClientID:  10,
		SeqNumber: 1,
		Key:       "framework",
		Value:     "DifferentValue",
	})
	if err != nil {
		t.Fatalf("Failed to marshal conflicting request: %v", err)
	}

	secondResp, err := http.Post(
		server.URL+"/kv",
		"application/json",
		bytes.NewBuffer(secondBody),
	)
	if err != nil {
		t.Fatalf("Failed to send conflicting request: %v", err)
	}
	defer secondResp.Body.Close()

	if secondResp.StatusCode != http.StatusConflict {
		t.Fatalf(
			"Expected conflicting request status 409, got %d",
			secondResp.StatusCode,
		)
	}

	// Ensure the original value remains unchanged.
	value, found, err := engine.Get([]byte("framework"))
	if err != nil {
		t.Fatalf("Failed to read stored value: %v", err)
	}
	if !found || string(value) != "RaftKV" {
		t.Fatalf(
			"Expected original value 'RaftKV', got found=%v, value=%q",
			found,
			string(value),
		)
	}

	t.Log("Conflicting request ID correctly returned HTTP 409")
}

