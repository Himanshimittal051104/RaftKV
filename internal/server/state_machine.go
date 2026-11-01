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
		if err := sm.Engine.Noop(msg.CommandIndex); err != nil {
			return err
		}

	case "SET":
		if err := sm.Engine.Put(msg.CommandIndex, []byte(cmd.Key), []byte(cmd.Value)); err != nil {
			return err
		}

	case "DELETE":
		if err := sm.Engine.Delete(msg.CommandIndex, []byte(cmd.Key)); err != nil {
			return err
		}

	default:
		return fmt.Errorf("unsupported operation %q", msg.Command.Op)
	}

	sm.Engine.MaybeSaveSnapshot()
	return nil
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
