package raft

import (
	"RaftKV/internal/storage"
	"fmt"
)

type TestStateMachine struct {
	Engine storage.Engine
}

func NewTestStateMachine(Engine storage.Engine) *TestStateMachine {
	return &TestStateMachine{
		Engine: Engine,
	}
}

func (sm *TestStateMachine) Apply(msg ApplyMsg) error {
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

func (sm *TestStateMachine) Get(key []byte) ([]byte, bool, error) {
	return sm.Engine.Get(key)
}

func (sm *TestStateMachine) LastApplied() (int, error) {
	return sm.Engine.LastApplied()
}

func (sm *TestStateMachine) IsClosed() bool {
	return sm.Engine.IsClosed()
}
func (sm *TestStateMachine) IsFailed() bool {
	return sm.Engine.IsFailed()
}
func (sm *TestStateMachine) Open() error {
	return sm.Engine.Open()
}
