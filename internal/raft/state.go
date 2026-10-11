package raft

import (
	"bytes"
	"encoding/gob"
)

type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
	CommitIndex int
}

func encodeRaftState(currentTerm int, votedFor int, log []LogEntry,
	commitIndex int) ([]byte, error) {
	state := persistentState{
		CurrentTerm: currentTerm,
		VotedFor:    votedFor,
		Log:         log,
		CommitIndex: commitIndex,
	}

	var buffer bytes.Buffer

	encoder := gob.NewEncoder(&buffer)

	if err := encoder.Encode(state); err != nil { //gob converts the Go structure into bytes.
		return nil, err
	}

	return buffer.Bytes(), nil
}

func decodeRaftState(data []byte) (int, int, []LogEntry, int, error) {
	var state persistentState

	decoder := gob.NewDecoder(bytes.NewReader(data))

	if err := decoder.Decode(&state); err != nil {
		return 0, -1, nil, 0, err
	}

	return state.CurrentTerm, state.VotedFor, state.Log, state.CommitIndex, nil
}
