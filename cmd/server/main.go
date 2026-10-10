package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"RaftKV/internal/raft"
	"RaftKV/internal/server"
	"RaftKV/internal/storage"
)

func main() {
	// Keep persistent state in a stable directory.
	dataDir := "data/node-0"

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("create data directory: %v", err)
	}

	stateFile := filepath.Join(dataDir, "raft-state.bin")

	// Create the storage engine.
	engine := storage.NewMemEngine()

	// Create a disk-backed Raft node.
	node, err := raft.NewPersistentRaftNode(
		0,
		1,
		make(chan raft.ApplyMsg, 100),
		engine,
		stateFile,
	)
	if err != nil {
		log.Fatalf("create Raft node: %v", err)
	}

	// Start Raft's background loops.
	node.StartBackground()

	// Expose the key-value HTTP API.
	kvServer := server.NewKVServer(node)

	log.Printf("RaftKV server listening on :8080")
	log.Printf("Raft state file: %s", stateFile)

	if err := http.ListenAndServe(":8080", kvServer); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
