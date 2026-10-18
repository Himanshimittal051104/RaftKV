package raft

import (
	"RaftKV/internal/storage"
	"bytes"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// type failingPersister struct{}

// func (p *failingPersister) SaveRaftState(state []byte) error {
// 	return fmt.Errorf("simulated persistence failure")
// }

// func (p *failingPersister) ReadRaftState() ([]byte, error) {
// 	return nil, nil
// }

type failingPersister struct {
	saveCalls int
}

func (p *failingPersister) SaveRaftState(state []byte) error {
	p.saveCalls++

	if p.saveCalls == 2 {
		return fmt.Errorf("simulated log persistence failure")
	}

	return nil
}

func (p *failingPersister) ReadRaftState() ([]byte, error) {
	return nil, nil
}

func TestFollowerDoesNotAcknowledgeEntryWhenPersistenceFails(t *testing.T) {
	persister := &failingPersister{}

	follower := NewRaftNodeWithPersister(
		1, 2,
		make(chan ApplyMsg, 10),
		nil,
		persister,
	)
	defer close(follower.stopCh)

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{
				Index: 1,
				Term:  1,
				Command: Command{
					Op:    "SET",
					Key:   "language",
					Value: "Go",
				},
			},
		},
		LeaderCommit: 0,
	}

	var reply AppendEntriesReply
	follower.AppendEntries(&args, &reply)

	if reply.Success {
		t.Fatal("follower acknowledged an entry despite persistence failure")
	}

	follower.mu.Lock()
	defer follower.mu.Unlock()

	if len(follower.log) != 1 {
		t.Fatalf("log length = %d, want 1 (dummy entry only)", len(follower.log))
	}
}

func TestMemPersisterSaveAndRead(t *testing.T) {
	p := NewMemPersister()

	original := []byte("raft-state")

	if err := p.SaveRaftState(original); err != nil {
		t.Fatalf("SaveRaftState failed: %v", err)
	}

	restored, err := p.ReadRaftState()
	if err != nil {
		t.Fatalf("ReadRaftState failed: %v", err)
	}

	if !bytes.Equal(restored, original) {
		t.Fatalf("restored state = %q, want %q", restored, original)
	}
}

func TestMemPersisterCopiesState(t *testing.T) {
	p := NewMemPersister()

	original := []byte("raft-state")

	if err := p.SaveRaftState(original); err != nil {
		t.Fatalf("SaveRaftState failed: %v", err)
	}

	original[0] = 'X'

	restored, err := p.ReadRaftState()
	if err != nil {
		t.Fatalf("ReadRaftState failed: %v", err)
	}

	if bytes.Equal(restored, original) {
		t.Fatalf("persister state changed when caller modified original")
	}

	expected := []byte("raft-state")

	if !bytes.Equal(restored, expected) {
		t.Fatalf("restored state = %q, want %q", restored, expected)
	}
}

func TestPersisterRoundTrip(t *testing.T) {
	p := NewMemPersister()

	originalLog := []LogEntry{
		{Index: 1, Term: 1, Command: Command{
			ClientID:  101,
			SeqNumber: 1,
			Op:        "SET",
			Key:       "name",
			Value:     "Himanshi",
		}},
		{Index: 2, Term: 2, Command: Command{
			ClientID:  101,
			SeqNumber: 2,
			Op:        "DELETE",
			Key:       "old-key",
		}},
	}

	// Step 1: Encode the state.
	encoded, err := encodeRaftState(2, 1, originalLog, 0)
	if err != nil {
		t.Fatalf("encoding failed: %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("encoded state is empty")
	}

	// Step 2: Save the encoded bytes.
	if err := p.SaveRaftState(encoded); err != nil {
		t.Fatalf("saving state failed: %v", err)
	}

	// Step 3: Read the saved bytes.
	saved, err := p.ReadRaftState()
	if err != nil {
		t.Fatalf("reading saved state failed: %v", err)
	}
	if !reflect.DeepEqual(encoded, saved) {
		t.Fatal("saved bytes differ from encoded bytes")
	}

	// Step 4: Decode the saved bytes.
	term, vote, recoveredLog, commitIndex, err := decodeRaftState(saved)
	if err != nil {
		t.Fatalf("decoding saved state failed: %v", err)
	}

	// Step 5: Verify the recovered state.
	if term != 2 {
		t.Errorf("term: got %d, want 2", term)
	}
	if vote != 1 {
		t.Errorf("votedFor: got %d, want 1", vote)
	}

	if commitIndex != 0 {
		t.Errorf("commitIndex = %d, want 0", commitIndex)
	}

	if !reflect.DeepEqual(originalLog, recoveredLog) {
		t.Errorf("recovered log does not match original log")
	}
}

func TestRaftNodeRestoresPersistedState(t *testing.T) {
	persister := NewMemPersister()

	originalLog := []LogEntry{
		{Index: 0, Term: 0},
		{
			Index: 1,
			Term:  2,
			Command: Command{
				Op:    "SET",
				Key:   "language",
				Value: "Go",
			},
		},
	}

	// Simulate state saved by the first node.
	encoded, err := encodeRaftState(2, 1, originalLog, 0)
	if err != nil {
		t.Fatalf("encode state: %v", err)
	}

	if err := persister.SaveRaftState(encoded); err != nil {
		t.Fatalf("save state: %v", err)
	}

	// Construct a new node using the same persister.
	node := NewRaftNodeWithPersister(
		0,
		1,
		make(chan ApplyMsg),
		nil,
		persister,
	)

	defer close(node.stopCh)

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.currentTerm != 2 {
		t.Errorf("currentTerm = %d, want 2", node.currentTerm)
	}

	if node.votedFor != 1 {
		t.Errorf("votedFor = %d, want 1", node.votedFor)
	}

	if len(node.log) != len(originalLog) {
		t.Fatalf("log length = %d, want %d", len(node.log), len(originalLog))
	}

	if !reflect.DeepEqual(node.log, originalLog) {
		t.Errorf("restored log does not match original log")
	}
}

func TestFilePersisterSaveAndRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.bin")

	p1, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("create persister: %v", err)
	}

	want := []byte("persistent-raft-state")

	if err := p1.SaveRaftState(want); err != nil {
		t.Fatalf("save state: %v", err)
	}

	updated := []byte("updated-raft-state")

	if err := p1.SaveRaftState(updated); err != nil {
		t.Fatalf("save updated state: %v", err)
	}

	// Simulate constructing a new persister after a restart.
	p2, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("recreate persister: %v", err)
	}

	got, err := p2.ReadRaftState()
	if err != nil {
		t.Fatalf("read state: %v", err)
	}

	if !bytes.Equal(got, updated) {
		t.Fatalf("read state = %q, want %q", got, updated)
	}
}

func TestFilePersisterRestoresRaftState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.bin")

	p1, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("create persister: %v", err)
	}

	originalLog := []LogEntry{
		{Index: 0, Term: 0},
		{
			Index: 1,
			Term:  3,
			Command: Command{
				Op:    "SET",
				Key:   "city",
				Value: "Delhi",
			},
		},
	}

	// Encode and persist actual Raft state.
	encoded, err := encodeRaftState(3, 1, originalLog, 0)
	if err != nil {
		t.Fatalf("encode state: %v", err)
	}

	if err := p1.SaveRaftState(encoded); err != nil {
		t.Fatalf("save state: %v", err)
	}

	// Recreate the persister and restore the saved state.
	p2, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("recreate persister: %v", err)
	}

	saved, err := p2.ReadRaftState()
	if err != nil {
		t.Fatalf("read state: %v", err)
	}

	term, vote, recoveredLog, commitIndex, err := decodeRaftState(saved)
	if err != nil {
		t.Fatalf("decode state: %v", err)
	}

	if term != 3 {
		t.Errorf("term = %d, want 3", term)
	}
	if vote != 1 {
		t.Errorf("votedFor = %d, want 1", vote)
	}

	if commitIndex != 0 {
		t.Errorf("commitIndex = %d, want 0", commitIndex)
	}
	if !reflect.DeepEqual(recoveredLog, originalLog) {
		t.Errorf("recovered log does not match original log")
	}
}

func TestRaftNodeRecoversFromFilePersister(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.bin")

	p1, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("create persister: %v", err)
	}

	originalLog := []LogEntry{
		{Index: 0, Term: 0},
		{
			Index: 1,
			Term:  4,
			Command: Command{
				Op:    "SET",
				Key:   "language",
				Value: "Go",
			},
		},
	}

	encoded, err := encodeRaftState(4, 2, originalLog, 1)
	if err != nil {
		t.Fatalf("encode state: %v", err)
	}

	if err := p1.SaveRaftState(encoded); err != nil {
		t.Fatalf("save state: %v", err)
	}

	// Recreate the persister, simulating a process restart.
	p2, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("recreate persister: %v", err)
	}

	node := NewRaftNodeWithPersister(
		0,
		3,
		make(chan ApplyMsg, 10),
		nil,
		p2,
	)

	defer close(node.stopCh)

	node.mu.Lock()
	defer node.mu.Unlock()

	if node.currentTerm != 4 {
		t.Errorf("currentTerm = %d, want 4", node.currentTerm)
	}

	if node.votedFor != 2 {
		t.Errorf("votedFor = %d, want 2", node.votedFor)
	}

	if node.commitIndex != 0 {
		t.Errorf("commitIndex = %d, want 0 for multi-node restart",
			node.commitIndex)
	}

	if !reflect.DeepEqual(node.log, originalLog) {
		t.Errorf("recovered log does not match original log")
	}
}

func TestPersistentNodeRecoversStateMachine(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "raft-state.bin")

	// First node: write a key to the state machine.
	engine1 := storage.NewMemEngine()
	applyCh1 := make(chan ApplyMsg, 100)

	node1, err := NewPersistentRaftNode(
		0, 1, applyCh1, engine1, stateFile,
	)
	if err != nil {
		t.Fatalf("create first node: %v", err)
	}

	node1.StartBackground()
	defer node1.Stop()

	// Wait for the single-node election to finish.
	deadline := time.Now().Add(3 * time.Second)
	for {
		node1.mu.Lock()
		isLeader := node1.role == Leader
		node1.mu.Unlock()

		if isLeader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first node did not become leader")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if _, err := node1.Put("language", "Go"); err != nil {
		t.Fatalf("put language=Go: %v", err)
	}

	// Second node: same persisted file, but a fresh empty engine.
	engine2 := storage.NewMemEngine()
	applyCh2 := make(chan ApplyMsg, 100)

	node2, err := NewPersistentRaftNode(
		0, 1, applyCh2, engine2, stateFile,
	)
	if err != nil {
		t.Fatalf("create recovered node: %v", err)
	}
	defer node2.Stop()

	value, found, err := node2.Engine.Get([]byte("language"))
	if err != nil {
		t.Fatalf("get recovered value: %v", err)
	}
	if !found {
		t.Fatal("expected language key to be recovered")
	}
	if string(value) != "Go" {
		t.Fatalf("expected Go, got %q", string(value))
	}
}

func TestPersistentNodeDoesNotRecoverUncommittedEntries(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "raft-state.bin")

	log := []LogEntry{
		{Index: 0}, // Dummy entry
		{
			Index: 1,
			Term:  1,
			Command: Command{
				Op:    "SET",
				Key:   "committed",
				Value: "yes",
			},
		},
		{
			Index: 2,
			Term:  1,
			Command: Command{
				Op:    "SET",
				Key:   "uncommitted",
				Value: "no",
			},
		},
	}

	// Persist both entries, but mark only index 1 as committed.
	state, err := encodeRaftState(1, -1, log, 1)
	if err != nil {
		t.Fatalf("encode state: %v", err)
	}

	persister, err := NewFilePersister(stateFile)
	if err != nil {
		t.Fatalf("create persister: %v", err)
	}
	if err := persister.SaveRaftState(state); err != nil {
		t.Fatalf("save state: %v", err)
	}

	engine := storage.NewMemEngine()
	node, err := NewPersistentRaftNode(
		0, 1, make(chan ApplyMsg, 10), engine, stateFile,
	)
	if err != nil {
		t.Fatalf("recover node: %v", err)
	}
	defer node.Stop()

	// The committed entry must be restored.
	value, found, err := engine.Get([]byte("committed"))
	if err != nil {
		t.Fatalf("get committed key: %v", err)
	}
	if !found || string(value) != "yes" {
		t.Fatalf("committed key = (%q, %v), want (%q, true)",
			string(value), found, "yes")
	}

	// The uncommitted entry must not be applied.
	_, found, err = engine.Get([]byte("uncommitted"))
	if err != nil {
		t.Fatalf("get uncommitted key: %v", err)
	}
	if found {
		t.Fatal("uncommitted entry was incorrectly recovered")
	}

	if node.commitIndex != 1 {
		t.Fatalf("commitIndex = %d, want 1", node.commitIndex)
	}
}

func TestFilePersisterMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-state.bin")

	p, err := NewFilePersister(path)
	if err != nil {
		t.Fatalf("create persister: %v", err)
	}

	state, err := p.ReadRaftState()
	if err != nil {
		t.Fatalf("read missing state file: %v", err)
	}

	if state != nil {
		t.Fatalf("state = %v, want nil for a missing file", state)
	}
}

func TestFollowerAppliesEntryAfterRestartAndLeaderCommit(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "raft-state.bin")

	log := []LogEntry{
		{Index: 0, Term: 0},
		{
			Index: 1,
			Term:  1,
			Command: Command{
				Op:    "SET",
				Key:   "language",
				Value: "Go",
			},
		},
	}

	// Persist the follower's log, but no commit index.
	state, err := encodeRaftState(1, -1, log, 0)
	if err != nil {
		t.Fatalf("encode state: %v", err)
	}

	persister, err := NewFilePersister(stateFile)
	if err != nil {
		t.Fatalf("create persister: %v", err)
	}
	if err := persister.SaveRaftState(state); err != nil {
		t.Fatalf("save state: %v", err)
	}

	engine := storage.NewMemEngine()
	applyCh := make(chan ApplyMsg, 10)

	// Restart the follower using the persisted state.
	follower := NewRaftNodeWithPersister(
		1, 2, applyCh, engine, persister,
	)
	defer close(follower.stopCh)

	// The entry exists in the log but is not yet committed.
	if follower.commitIndex != 0 {
		t.Fatalf("commitIndex = %d, want 0", follower.commitIndex)
	}

	// Simulate the leader informing the restarted follower
	// that entry 1 is committed.
	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: 1,
	}
	var reply AppendEntriesReply
	follower.AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept valid AppendEntries")
	}

	// Process the ApplyMsg as the apply loop normally would.
	select {
	case msg := <-applyCh:
		if err := follower.applyMessage(msg); err != nil {
			t.Fatalf("apply committed entry: %v", err)
		}
	default:
		t.Fatal("expected committed entry to be queued for application")
	}

	value, found, err := engine.Get([]byte("language"))
	if err != nil {
		t.Fatalf("get recovered value: %v", err)
	}
	if !found || string(value) != "Go" {
		t.Fatalf("value = (%q, %v), want (%q, true)",
			string(value), found, "Go")
	}

	follower.mu.Lock()
	defer follower.mu.Unlock()
	if follower.commitIndex != 1 {
		t.Fatalf("commitIndex = %d, want 1", follower.commitIndex)
	}
}

func TestFollowerPersistsNewLogEntryBeforeAcknowledging(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "raft-state.bin")

	persister, err := NewFilePersister(stateFile)
	if err != nil {
		t.Fatalf("create persister: %v", err)
	}

	follower := NewRaftNodeWithPersister(
		1, 2,
		make(chan ApplyMsg, 10),
		nil,
		persister,
	)
	defer close(follower.stopCh)

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{
				Index: 1,
				Term:  1,
				Command: Command{
					Op:    "SET",
					Key:   "language",
					Value: "Go",
				},
			},
		},
		LeaderCommit: 0,
	}

	var reply AppendEntriesReply
	follower.AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should acknowledge the valid entry")
	}

	// Simulate a restart by creating a new node from the same file.
	restarted := NewRaftNodeWithPersister(
		1, 2,
		make(chan ApplyMsg, 10),
		nil,
		persister,
	)
	defer close(restarted.stopCh)

	restarted.mu.Lock()
	defer restarted.mu.Unlock()

	if len(restarted.log) != 2 {
		t.Fatalf("recovered log length = %d, want 2", len(restarted.log))
	}

	entry := restarted.log[1]
	if entry.Command.Key != "language" ||
		entry.Command.Value != "Go" ||
		entry.Term != 1 {
		t.Fatalf("unexpected recovered entry: %+v", entry)
	}

	if restarted.commitIndex != 0 {
		t.Fatalf("commitIndex = %d, want 0", restarted.commitIndex)
	}
}

func TestFilePersisterRejectsEmptyPath(t *testing.T) {
	p, err := NewFilePersister("")

	if err == nil {
		t.Fatal("expected error for empty state file path")
	}

	if p != nil {
		t.Fatal("expected nil persister for empty state file path")
	}
}

func TestFollowerPersistsRepairedConflictAcrossRestart(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "raft-state.bin")

	persister, err := NewFilePersister(stateFile)
	if err != nil {
		t.Fatalf("create persister: %v", err)
	}

	// Persist a follower log containing a conflicting entry X at index 2.
	oldLog := []LogEntry{
		{Index: 0, Term: 0},
		{
			Index: 1,
			Term:  1,
			Command: Command{
				Op:    "SET",
				Key:   "x",
				Value: "A",
			},
		},
		{
			Index: 2,
			Term:  3,
			Command: Command{
				Op:    "SET",
				Key:   "x",
				Value: "X",
			},
		},
	}

	state, err := encodeRaftState(3, -1, oldLog, 0)
	if err != nil {
		t.Fatalf("encode old state: %v", err)
	}
	if err := persister.SaveRaftState(state); err != nil {
		t.Fatalf("save old state: %v", err)
	}

	follower := NewRaftNodeWithPersister(
		1, 2, make(chan ApplyMsg, 10), nil, persister,
	)
	defer close(follower.stopCh)

	// The leader sends its replacement entry B at index 2.
	args := AppendEntriesArgs{
		Term:         4,
		LeaderID:     0,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{
				Index: 2,
				Term:  4,
				Command: Command{
					Op:    "SET",
					Key:   "x",
					Value: "B",
				},
			},
		},
		LeaderCommit: 0,
	}

	var reply AppendEntriesReply
	follower.AppendEntries(&args, &reply)

	if !reply.Success {
		t.Fatal("follower should accept the replacement entry")
	}

	// Simulate restart by loading the same state file into a new node.
	restarted := NewRaftNodeWithPersister(
		1, 2, make(chan ApplyMsg, 10), nil, persister,
	)
	defer close(restarted.stopCh)

	restarted.mu.Lock()
	defer restarted.mu.Unlock()

	if len(restarted.log) != 3 {
		t.Fatalf("recovered log length = %d, want 3", len(restarted.log))
	}

	entry := restarted.log[2]
	if entry.Term != 4 || entry.Command.Value != "B" {
		t.Fatalf("recovered entry = %+v, want term 4 and value B", entry)
	}
}

func TestGrantedVoteSurvivesRestart(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "raft-state.bin")

	persister, err := NewFilePersister(stateFile)
	if err != nil {
		t.Fatalf("create persister: %v", err)
	}

	follower := NewRaftNodeWithPersister(
		0, 3, make(chan ApplyMsg, 10), nil, persister,
	)
	defer close(follower.stopCh)

	args := RequestVoteArgs{
		Term:         1,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	var reply RequestVoteReply
	follower.RequestVote(&args, &reply)

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	// Reconstruct the follower using the same disk-backed state.
	restarted := NewRaftNodeWithPersister(
		0, 3, make(chan ApplyMsg, 10), nil, persister,
	)
	defer close(restarted.stopCh)

	restarted.mu.Lock()
	if restarted.currentTerm != 1 {
		t.Errorf("currentTerm = %d, want 1", restarted.currentTerm)
	}
	if restarted.votedFor != 1 {
		t.Errorf("votedFor = %d, want 1", restarted.votedFor)
	}
	restarted.mu.Unlock()

	// The restarted follower must not vote for another candidate
	// in the same term.
	secondRequest := RequestVoteArgs{
		Term:         1,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	var secondReply RequestVoteReply
	restarted.RequestVote(&secondRequest, &secondReply)

	if secondReply.VoteGranted {
		t.Fatal("follower granted a second vote in the same term")
	}
}

func TestFollowerDoesNotGrantVoteWhenPersistenceFails(t *testing.T) {
	persister := &failingPersister{
		saveCalls: 0,
	}

	follower := NewRaftNodeWithPersister(
		0, 3, make(chan ApplyMsg, 10), nil, persister,
	)
	defer close(follower.stopCh)

	args := RequestVoteArgs{
		Term:         1,
		CandidateID:  1,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	var reply RequestVoteReply
	follower.RequestVote(&args, &reply)

	if reply.VoteGranted {
		t.Fatal("follower granted vote despite persistence failure")
	}

	follower.mu.Lock()
	defer follower.mu.Unlock()

	if follower.currentTerm != 1 {
		t.Errorf("currentTerm = %d, want 1", follower.currentTerm)
	}
	if follower.votedFor != -1 {
		t.Errorf("votedFor = %d, want -1", follower.votedFor)
	}
}

func TestPersistentNodeRecoversDeletedKey(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "raft-state.bin")

	log := []LogEntry{
		{Index: 0, Term: 0},
		{
			Index: 1,
			Term:  1,
			Command: Command{
				Op:    "SET",
				Key:   "language",
				Value: "Go",
			},
		},
		{
			Index: 2,
			Term:  1,
			Command: Command{
				Op:  "DELETE",
				Key: "language",
			},
		},
	}

	// Both entries are committed.
	state, err := encodeRaftState(1, -1, log, 2)
	if err != nil {
		t.Fatalf("encode state: %v", err)
	}

	persister, err := NewFilePersister(stateFile)
	if err != nil {
		t.Fatalf("create persister: %v", err)
	}
	if err := persister.SaveRaftState(state); err != nil {
		t.Fatalf("save state: %v", err)
	}

	engine := storage.NewMemEngine()
	node, err := NewPersistentRaftNode(
		0, 1, make(chan ApplyMsg, 10), engine, stateFile,
	)
	if err != nil {
		t.Fatalf("recover persistent node: %v", err)
	}
	defer node.Stop()

	_, found, err := engine.Get([]byte("language"))
	if err != nil {
		t.Fatalf("get recovered key: %v", err)
	}
	if found {
		t.Fatal("deleted key should not exist after recovery")
	}
}

type failingRecoveryEngine struct{}

func (e *failingRecoveryEngine) Put(key, value []byte) error {
	return fmt.Errorf("simulated Put failure")
}

func (e *failingRecoveryEngine) Get(key []byte) ([]byte, bool, error) {
	return nil, false, nil
}

func (e *failingRecoveryEngine) Delete(key []byte) error {
	return nil
}

func (e *failingRecoveryEngine) Close() error {
	return nil
}

func TestRecoveryReturnsErrorWhenPutFails(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "raft-state.bin")

	log := []LogEntry{
		{Index: 0, Term: 0},
		{
			Index: 1,
			Term:  1,
			Command: Command{
				Op:    "SET",
				Key:   "language",
				Value: "Go",
			},
		},
	}

	state, err := encodeRaftState(1, -1, log, 1)
	if err != nil {
		t.Fatalf("encode state: %v", err)
	}

	persister, err := NewFilePersister(stateFile)
	if err != nil {
		t.Fatalf("create persister: %v", err)
	}
	if err := persister.SaveRaftState(state); err != nil {
		t.Fatalf("save state: %v", err)
	}

	_, err = NewPersistentRaftNode(
		0, 1,
		make(chan ApplyMsg, 10),
		&failingRecoveryEngine{},
		stateFile,
	)

	if err == nil {
		t.Fatal("expected recovery to fail when Engine.Put fails")
	}
}

type failingRecoveryDeleteEngine struct{}

func (e *failingRecoveryDeleteEngine) Put(key, value []byte) error {
	return nil
}

func (e *failingRecoveryDeleteEngine) Get(key []byte) ([]byte, bool, error) {
	return nil, false, nil
}

func (e *failingRecoveryDeleteEngine) Delete(key []byte) error {
	return fmt.Errorf("simulated Delete failure")
}

func (e *failingRecoveryDeleteEngine) Close() error {
	return nil
}

func TestRecoveryReturnsErrorWhenDeleteFails(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "raft-state.bin")

	log := []LogEntry{
		{Index: 0, Term: 0},
		{
			Index: 1,
			Term:  1,
			Command: Command{
				Op:  "DELETE",
				Key: "language",
			},
		},
	}

	state, err := encodeRaftState(1, -1, log, 1)
	if err != nil {
		t.Fatalf("encode state: %v", err)
	}

	persister, err := NewFilePersister(stateFile)
	if err != nil {
		t.Fatalf("create persister: %v", err)
	}
	if err := persister.SaveRaftState(state); err != nil {
		t.Fatalf("save state: %v", err)
	}

	_, err = NewPersistentRaftNode(
		0, 1,
		make(chan ApplyMsg, 10),
		&failingRecoveryDeleteEngine{},
		stateFile,
	)

	if err == nil {
		t.Fatal("expected recovery to fail when Engine.Delete fails")
	}
}
