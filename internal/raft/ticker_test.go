package raft

import (
	"RaftKV/internal/storage"
	"testing"
	"time"
)


func TestStopReturnsWhenApplyChannelIsBlocked(t *testing.T) {
    applyCh := make(chan ApplyMsg)
	persister := NewMemPersister() // Use your actual constructor.
    stateMachine := NewTestStateMachine(storage.NewMemEngine()) // Use your existing test helper.

    rn, err := NewRaftNode(
        0,
        1,
        applyCh,
        persister,
        stateMachine,
    )
    if err != nil {
        t.Fatalf("NewRaftNode(): %v", err)
    }

    if err := rn.SetPeers([]*RaftNode{rn}); err != nil {
        t.Fatalf("SetPeers(): %v", err)
    }

    if err := rn.StartBackground(); err != nil {
        t.Fatalf("StartBackground(): %v", err)
    }

    senderStarted := make(chan struct{})
    allowSend := make(chan struct{})

    spawned := rn.spawn(func() {
        close(senderStarted)

        // Hold the goroutine here until the test releases it.
        <-allowSend

        select {
        case applyCh <- ApplyMsg{}:
        case <-rn.stopCh:
            return
        }
    })

    if !spawned {
        t.Fatal("spawn() unexpectedly rejected the goroutine")
    }

    <-senderStarted

    // Release the goroutine so it attempts the unbuffered send.
    close(allowSend)

    done := make(chan struct{})
    go func() {
        rn.Stop()
        close(done)
    }()

    select {
    case <-done:
        // Stop returned successfully.
    case <-time.After(2 * time.Second):
        t.Fatal("Stop() did not return while applyCh was blocked")
    }
}