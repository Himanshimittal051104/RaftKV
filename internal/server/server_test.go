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
	stateMachines := make([]raft.StateMachine, n)
	persisters := make([]raft.Persister, n)

	for i := 0; i < n; i++ {
		engines[i] = storage.NewMemEngine()
		stateMachines[i] = NewKVStateMachine(engines[i])
		applyChs[i] = make(chan raft.ApplyMsg, 100)
		persisters[i] = raft.NewMemPersister()

		var err error
		nodes[i], err = raft.NewRaftNode(
			i,
			n,
			applyChs[i],
			persisters[i],
			stateMachines[i],
		)
		if err != nil {
			t.Fatalf("failed to create Raft node %d: %v", i, err)
		}
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

	// Wait for leader election and discover the leader through HTTP.
	t.Log("Waiting for cluster leader election over test network...")

	leaderIdx := -1
	deadline := time.Now().Add(3 * time.Second)

	for time.Now().Before(deadline) {
		for i := 0; i < n; i++ {
			body, err := json.Marshal(WriteRequest{
				ClientID:  1,
				SeqNumber: 1,
				Key:       "ping",
				Value:     "pong",
			})
			if err != nil {
				t.Fatalf("failed to marshal leader probe: %v", err)
			}

			resp, err := http.Post(
				testServers[i].URL+"/kv",
				"application/json",
				bytes.NewReader(body),
			)
			if err != nil {
				continue
			}

			status := resp.StatusCode
			resp.Body.Close()

			if status == http.StatusOK {
				leaderIdx = i
				break
			}
		}

		if leaderIdx != -1 {
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	if leaderIdx == -1 {
		t.Fatal("failed to identify leader server via network")
	}

	t.Logf("Leader HTTP server identified at node %d", leaderIdx)

	// Test 1: A follower must reject writes with HTTP 503.
	followerIdx := (leaderIdx + 1) % n

	body, err := json.Marshal(WriteRequest{
		ClientID:  2,
		SeqNumber: 1,
		Key:       "hello",
		Value:     "world",
	})
	if err != nil {
		t.Fatalf("failed to marshal follower request: %v", err)
	}

	resp, err := http.Post(
		testServers[followerIdx].URL+"/kv",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("failed to send request to follower: %v", err)
	}

	status := resp.StatusCode
	resp.Body.Close()

	if status != http.StatusServiceUnavailable {
		t.Fatalf("expected HTTP 503 on follower, got %d", status)
	}

	t.Log("Follower correctly rejected write with HTTP 503")

	// Test 2: Write through the leader.
	putBody, err := json.Marshal(WriteRequest{
		ClientID:  3,
		SeqNumber: 1,
		Key:       "framework",
		Value:     "RaftKV",
	})
	if err != nil {
		t.Fatalf("failed to marshal PUT request: %v", err)
	}

	resp, err = http.Post(
		testServers[leaderIdx].URL+"/kv",
		"application/json",
		bytes.NewReader(putBody),
	)
	if err != nil {
		t.Fatalf("failed PUT request: %v", err)
	}

	status = resp.StatusCode
	resp.Body.Close()

	if status != http.StatusOK {
		t.Fatalf("expected HTTP 200 for PUT, got %d", status)
	}

	t.Log("HTTP PUT successful on leader server")

	// Test 3: GET the value through the leader.
	// Use the same client ID and the next sequence number.
	getBody, err := json.Marshal(WriteRequest{
		ClientID:  3,
		SeqNumber: 2,
		Key:       "framework",
	})
	if err != nil {
		t.Fatalf("failed to marshal GET request: %v", err)
	}

	getReq, err := http.NewRequest(
		http.MethodGet,
		testServers[leaderIdx].URL+"/kv",
		bytes.NewReader(getBody),
	)
	if err != nil {
		t.Fatalf("failed to create GET request: %v", err)
	}
	getReq.Header.Set("Content-Type", "application/json")

	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("failed GET request: %v", err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP 200 for GET, got %d", getResp.StatusCode)
	}

	var getResult Response
	if err := json.NewDecoder(getResp.Body).Decode(&getResult); err != nil {
		t.Fatalf("failed to decode GET response: %v", err)
	}

	if !getResult.Found || getResult.Value != "RaftKV" {
		t.Fatalf(
			"expected value %q, got found=%v, value=%q",
			"RaftKV",
			getResult.Found,
			getResult.Value,
		)
	}

	t.Logf("HTTP GET verified: framework = %q", getResult.Value)
}

func TestKVServerRejectsConflictingRequestID(t *testing.T) {
	applyCh := make(chan raft.ApplyMsg, 10)
	engine := storage.NewMemEngine()
	stateMachine := NewKVStateMachine(engine)
	persister := raft.NewMemPersister()

	node, err := raft.NewRaftNode(
		0,
		1,
		applyCh,
		persister,
		stateMachine,
	)
	if err != nil {
		t.Fatalf("failed to create Raft node: %v", err)
	}

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
		t.Fatalf("failed to marshal first request: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	var firstResp *http.Response

	for time.Now().Before(deadline) {
		resp, requestErr := http.Post(
			server.URL+"/kv",
			"application/json",
			bytes.NewReader(firstBody),
		)

		if requestErr == nil {
			if resp.StatusCode == http.StatusOK {
				firstResp = resp
				break
			}
			resp.Body.Close()
		}

		time.Sleep(50 * time.Millisecond)
	}

	if firstResp == nil {
		t.Fatal("first request did not succeed before the deadline")
	}
	firstResp.Body.Close()

	// Second request: same request ID, but a different value.
	secondBody, err := json.Marshal(WriteRequest{
		ClientID:  10,
		SeqNumber: 1,
		Key:       "framework",
		Value:     "DifferentValue",
	})
	if err != nil {
		t.Fatalf("failed to marshal conflicting request: %v", err)
	}

	secondResp, err := http.Post(
		server.URL+"/kv",
		"application/json",
		bytes.NewReader(secondBody),
	)
	if err != nil {
		t.Fatalf("failed to send conflicting request: %v", err)
	}
	defer secondResp.Body.Close()

	if secondResp.StatusCode != http.StatusConflict {
		t.Fatalf(
			"expected conflicting request status 409, got %d",
			secondResp.StatusCode,
		)
	}

	// Ensure the original value remains unchanged.
	value, found, err := stateMachine.Get([]byte("framework"))
	if err != nil {
		t.Fatalf("failed to read stored value: %v", err)
	}

	if !found || string(value) != "RaftKV" {
		t.Fatalf(
			"expected original value %q, got found=%v, value=%q",
			"RaftKV",
			found,
			string(value),
		)
	}

	t.Log("Conflicting request ID correctly returned HTTP 409")
}
