package main

import (
	"log"          //Logs startup information and fatal errors.
	"net/http"     //Runs the HTTP server.
	"os"           //Creates directories and interacts with the operating system.
	"path/filepath"     //Builds file paths using the operating system's path conventions.
	"RaftKV/internal/raft" //Creates the Raft node and its apply channel.
	"RaftKV/internal/server"   //Exposes the key-value API over HTTP.
	"RaftKV/internal/storage"   //Creates the key-value storage engine.
)

func main() {
	// Keep persistent state in a stable directory.
	dataDir := "data/node-0"  //specifies the directory for node 0's persistent state.

	if err := os.MkdirAll(dataDir, 0755); err != nil { //creates the directory, including missing parent directories.
		//is the directory permission mode on systems where Unix-style permissions apply, basically allowing the owner to read/write/execute(4+2+1) and others to read/execute(4+1);  Go is cross-platform: the same code can run on Windows, Linux, and macOS..	
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
	kvServer := server.NewKVServer(node) //creates your KV server object. But it does not start listening for HTTP requests yet.

	log.Printf("RaftKV server listening on :8080")
	log.Printf("Raft state file: %s", stateFile)

	if err := http.ListenAndServe(":8080", kvServer); err != nil {//This starts listening on port 8080 and passes incoming HTTP requests to kvServer for handling.
		log.Fatalf("HTTP server failed: %v", err)
	}

// http: ->The protocol used to communicate with the server. Here, it's HTTP.
// localhost->Means your own computer. It's the hostname that refers to your local machine.
// :8080->The port your application listens on.
}
