package server

import (
	"RaftKV/internal/raft"
	"RaftKV/internal/storage"
	"fmt"
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
	case "NOOP":
		return nil

	case "SET":
		return sm.engine.Put(
			[]byte(cmd.Key),
			[]byte(cmd.Value),
		)

	case "DELETE":
		return sm.engine.Delete(
			[]byte(cmd.Key),
		)

	default:
		return fmt.Errorf("unsupported operation %q", msg.Command.Op)
	}
}

func (sm *KVStateMachine) Get(key []byte) ([]byte, bool, error) {
	return sm.engine.Get(key)
}
