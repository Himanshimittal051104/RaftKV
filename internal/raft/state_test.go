package raft

import (
	"reflect"
	"testing"
)

func TestEncodeDecodeRaftState(t *testing.T) {
	originalTerm := 5
	originalVotedFor := 2
	originalCommitIndex := 2

	originalLog := []LogEntry{
		{
			Index: 0,
			Term:  0,
		},
		{
			Index: 1,
			Term:  1,
			Command: Command{
				ClientID:  10,
				SeqNumber: 1,
				Op:        "SET",
				Key:       "name",
				Value:     "Himanshi",
			},
		},
		{
			Index: 2,
			Term:  5,
			Command: Command{
				ClientID:  10,
				SeqNumber: 2,
				Op:        "DELETE",
				Key:       "name",
			},
		},
	}

	data, err := encodeRaftState(
		originalTerm,
		originalVotedFor,
		originalLog,
		originalCommitIndex,
	)
	if err != nil {
		t.Fatalf("encodeRaftState failed: %v", err)
	}

	term, votedFor, log, commitIndex, err := decodeRaftState(data)
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

	if commitIndex != originalCommitIndex {
		t.Fatalf("commitIndex = %d, want %d", commitIndex, originalCommitIndex)
	}
}

func TestDecodeRaftStateRejectsInvalidState(t *testing.T) {
	validLog := []LogEntry{
		{Index: 0, Term: 0},
		{Index: 1, Term: 1, Command: Command{
			Op:    "SET",
			Key:   "language",
			Value: "Go",
		}},
		{Index: 2, Term: 2, Command: Command{
			Op:  "DELETE",
			Key: "old-key",
		}},
	}

	tests := []struct {
		name        string
		currentTerm int
		votedFor    int
		log         []LogEntry
		commitIndex int
	}{
		{
			name:        "negative current term",
			currentTerm: -1,
			votedFor:    -1,
			log:         validLog,
			commitIndex: 0,
		},
		{
			name:        "votedFor below negative one",
			currentTerm: 2,
			votedFor:    -2,
			log:         validLog,
			commitIndex: 0,
		},
		{
			name:        "missing dummy entry",
			currentTerm: 2,
			votedFor:    -1,
			log:         validLog[1:],
			commitIndex: 0,
		},
		{
			name:        "non-contiguous log indices",
			currentTerm: 2,
			votedFor:    -1,
			log: []LogEntry{
				{Index: 0, Term: 0},
				{Index: 2, Term: 1},
			},
			commitIndex: 0,
		},
		{
			name:        "commit index beyond log",
			currentTerm: 2,
			votedFor:    -1,
			log:         validLog,
			commitIndex: 3,
		},
		{
			name:        "decreasing log terms",
			currentTerm: 2,
			votedFor:    -1,
			log: []LogEntry{
				{Index: 0, Term: 0},
				{Index: 1, Term: 2},
				{Index: 2, Term: 1},
			},
			commitIndex: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := encodeRaftState(
				tt.currentTerm,
				tt.votedFor,
				tt.log,
				tt.commitIndex,
			)
			if err != nil {
				t.Fatalf("encodeRaftState failed: %v", err)
			}

			_, _, _, _, err = decodeRaftState(data)
			if err == nil {
				t.Fatal("decodeRaftState succeeded; want validation error")
			}
		})
	}
}
