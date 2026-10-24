package server

import (
	"RaftKV/internal/raft"
	"RaftKV/internal/storage"
	"fmt"
)

type KVStateMachine struct {
	Engine storage.Engine
}

func NewKVStateMachine(Engine storage.Engine) *KVStateMachine {
	return &KVStateMachine{
		Engine: Engine,
	}
}

func (sm *KVStateMachine) Apply(msg raft.ApplyMsg) error {
	if !msg.CommandValid {
		return nil
	}

	cmd := msg.Command

	switch cmd.Op {
	case "NOOP":
		return sm.Engine.Noop(msg.CommandIndex)

	case "SET":
		return sm.Engine.Put(msg.CommandIndex, []byte(cmd.Key), []byte(cmd.Value))

	case "DELETE":
		return sm.Engine.Delete(msg.CommandIndex, []byte(cmd.Key))

	default:
		return fmt.Errorf("unsupported operation %q", msg.Command.Op)
	}
}

func (sm *KVStateMachine) Get(key []byte) ([]byte, bool, error) {
	return sm.Engine.Get(key)
}

func (sm *KVStateMachine) LastApplied() (int, error) {
	return sm.Engine.LastApplied()
}

func (sm *KVStateMachine) IsClosed() bool {
	return sm.Engine.IsClosed()
}
func (sm *KVStateMachine) IsFailed() bool {
	return sm.Engine.IsFailed()
}
func (sm *KVStateMachine) Open() error {
	return sm.Engine.Open()
}
