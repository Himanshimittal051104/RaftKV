package raft

import (
    "bytes"
    "testing"
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