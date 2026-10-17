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

	var buffer bytes.Buffer //A bytes.Buffer is an in-memory object that can accumulate bytes. At this point, nothing has been written to disk. The buffer is simply a destination for the encoded data.

	encoder := gob.NewEncoder(&buffer)//Go's encoding/gob package can encode Go values into a binary representation.The encoder writes into buffer, rather than directly to a file.

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

	if err := decoder.Decode(&state); err != nil {//The decoder reconstructs the structure's fields.
		return 0, -1, nil, 0, err
	}

	return state.CurrentTerm, state.VotedFor, state.Log, state.CommitIndex, nil
}
