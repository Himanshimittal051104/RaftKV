package raft

import (
    "reflect"
    "testing"
)

func TestEncodeDecodeRaftState(t *testing.T) {
    originalTerm := 5
    originalVotedFor := 2

    originalLog := []LogEntry{
        {
            Index: 0,
            Term: 0,
        },
        {
            Index: 1,
            Term: 1,
            Command: Command{
                ClientID: 10,
                SeqNumber: 1,
                Op: "SET",
                Key: "name",
                Value: "Himanshi",
            },
        },
        {
            Index: 2,
            Term: 5,
            Command: Command{
                ClientID: 10,
                SeqNumber: 2,
                Op: "DELETE",
                Key: "name",
            },
        },
    }

    data, err := encodeRaftState(
        originalTerm,
        originalVotedFor,
        originalLog,
    )

    if err != nil {
        t.Fatalf("encodeRaftState failed: %v", err)
    }

    term, votedFor, log, err := decodeRaftState(data)

    if err != nil {
        t.Fatalf("decodeRaftState failed: %v", err)
    }

    if term != originalTerm {
        t.Fatalf("term = %d, want %d", term, originalTerm)
    }

    if votedFor != originalVotedFor {
        t.Fatalf("votedFor = %d, want %d", votedFor, originalVotedFor)
    }

    if !reflect.DeepEqual(log, originalLog) {
        t.Fatalf("log was not restored correctly")
    }
}