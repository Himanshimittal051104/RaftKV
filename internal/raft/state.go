package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
	CommitIndex int
}

func encodeRaftState(currentTerm int, votedFor int, log []LogEntry, commitIndex int) ([]byte, error) {
	state := persistentState{
		CurrentTerm: currentTerm,
		VotedFor:    votedFor,
		Log:         log,
		CommitIndex: commitIndex,
	}

	var buffer bytes.Buffer //A bytes.Buffer is an in-memory object that can accumulate bytes. At this point, nothing has been written to disk. The buffer is simply a destination for the encoded data.

	encoder := gob.NewEncoder(&buffer) //Go's encoding/gob package can encode Go values into a binary representation.The encoder writes into buffer, rather than directly to a file.

	if err := encoder.Encode(state); err != nil { //gob converts the Go structure into bytes.
		return nil, err
	}

	return buffer.Bytes(), nil
}

func decodeRaftState(data []byte) (int, int, []LogEntry, int, error) {
	var state persistentState

	decoder := gob.NewDecoder(bytes.NewReader(data))
	//bytes.NewReader(data) lets the decoder read from the byte slice.
	//gob.NewDecoder(...) interprets the Gob-encoded representation.

	if err := decoder.Decode(&state); err != nil { //The decoder reconstructs the structure's fields.
		return 0, -1, nil, 0, err
	}

	if err := validatePersistentState(state); err != nil {
		return 0, -1, nil, 0, err
	}
	return state.CurrentTerm, state.VotedFor, state.Log, state.CommitIndex, nil
}

func validatePersistentState(state persistentState) error {
	if state.CurrentTerm < 0 {
		return fmt.Errorf("invalid current term %d: must be nonnegative", state.CurrentTerm)
	}

	if state.VotedFor < -1 {
		return fmt.Errorf("invalid votedFor %d: must be -1 or a candidate ID", state.VotedFor)
	}

	if len(state.Log) == 0 {
		return fmt.Errorf("invalid log: missing dummy entry at index 0")
	}

	if state.CommitIndex < 0 || state.CommitIndex >= len(state.Log) {
		return fmt.Errorf(
			"invalid commit index %d for log length %d",
			state.CommitIndex,
			len(state.Log),
		)
	}

	if state.Log[0].Index != 0 || state.Log[0].Term != 0 {
		return fmt.Errorf("invalid dummy log entry at index 0")
	}

	for i, entry := range state.Log {
		if entry.Index != i {
			return fmt.Errorf(
				"invalid log index at position %d: entry has index %d",
				i,
				entry.Index,
			)
		}

		if entry.Term < 0 || entry.Term > state.CurrentTerm {
			return fmt.Errorf(
				"invalid term %d at log index %d for current term %d",
				entry.Term,
				i,
				state.CurrentTerm,
			)
		}

		if i > 0 && entry.Term < state.Log[i-1].Term {
			return fmt.Errorf(
				"log term decreases from %d to %d at index %d",
				state.Log[i-1].Term,
				entry.Term,
				i,
			)
		}
	}

	return nil
}
