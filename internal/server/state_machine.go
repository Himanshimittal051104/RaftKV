package server

import (
	"RaftKV/internal/raft"
	"RaftKV/internal/storage"
)

type KVStateMachine struct {
	engine storage.Engine
}

func NewKVStateMachine(engine storage.Engine) *KVStateMachine {
	return &KVStateMachine{
		engine: engine,
	}
}

func (sm *KVStateMachine) Apply(msg raft.ApplyMsg) error {
	if !msg.CommandValid {
		return nil
	}

	cmd := msg.Command

	switch cmd.Op {
	case "SET":
		return sm.engine.Put(
			[]byte(cmd.Key),
			[]byte(cmd.Value),
		)

	case "DELETE":
		return sm.engine.Delete(
			[]byte(cmd.Key),
		)

	case "GET":
		// GET is a read operation and should not modify the state machine.
		return nil

	default:
		return nil
	}
}