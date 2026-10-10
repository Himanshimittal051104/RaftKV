package raft

import (
    "bytes"
    "testing"
    "reflect"
)

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
			ClientID: 101,
			SeqNumber: 1,
			Op: "SET",
			Key: "name",
			Value: "Himanshi",
		}},
		{Index: 2, Term: 2, Command: Command{
			ClientID: 101,
			SeqNumber: 2,
			Op: "DELETE",
			Key: "old-key",
		}},
	}

	// Step 1: Encode the state.
	encoded, err := encodeRaftState(2, 1, originalLog)
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
	term, vote, recoveredLog, err := decodeRaftState(saved)
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
            Term: 2,
            Command: Command{
                Op:    "SET",
                Key:   "language",
                Value: "Go",
            },
        },
    }

    // Simulate state saved by the first node.
    encoded, err := encodeRaftState(2, 1, originalLog)
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
