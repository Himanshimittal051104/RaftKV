# RaftKV

> A fault-tolerant distributed key-value store built from scratch in Go using the Raft consensus algorithm.

RaftKV is a learning-oriented distributed key-value store designed to explore distributed systems, consensus, replicated state machines, and fault-tolerant storage.

The project implements Raft-based leader election, heartbeats, log replication, conflict resolution, majority-based commitment, ReadIndex-based linearizable reads, request deduplication, an HTTP API, file-based Raft state persistence, and a WAL-backed file storage engine with recovery support.

**Development status** : Core Raft consensus, ReadIndex-based linearizable reads, request deduplication, file-based Raft state persistence, and a WAL-backed key-value storage engine are implemented. The existing Raft, server, and storage test suites have been reported as passing. Safe shutdown, snapshots, log compaction, and broader fault testing remain on the roadmap.

---

## Features

### Raft Consensus

- Leader election using randomized election timeouts
- Candidate self-voting and Majority-based leader election
- Term-based leadership
- `RequestVote` RPC
- `AppendEntries` RPC
- Heartbeats
- Log replication
- Log conflict detection and resolution
- `nextIndex` and `matchIndex` tracking
- Majority-based log commitment
- Ordered application of committed entries
- Higher-term detection and leader step-down
- Election timer handling and stale-term protection
- NOOP entries to help a newly elected leader establish a current-term commitment point

### Linearizable Reads

RaftKV implements leader-based linearizable reads using a ReadIndex-style mechanism.


A `GET` request does not create a new Raft log entry.


Instead, the leader:

1. Verifies that it is currently the leader.
2. Sends lightweight `ReadProbe` RPCs to other nodes.
3. Obtains confirmation from a majority of the cluster.
4. Establishes a `readIndex` from the committed state.
5. Waits until the local state machine has applied through that index.
6. Performs the read from the local storage engine using state-machine.
7. Re-checks leadership before returning the result.


The read path is therefore:

```text
Client
  │
  ▼
GET
  │
  ▼
Leader Check
  │
  ▼
ReadIndex
  │
  ▼
ReadProbe Quorum
  │
  ▼
Committed Read Index
  │
  ▼
Wait for Application
  │
  ▼
State Machine
  │
  ▼
Storage Engine
  │
  ▼
Response
```

A dedicated `ReadProbe` RPC is used instead of AppendEntries so that read confirmation does not perform unnecessary log-prefix matching or log repair.

### Key-Value Store

Supported Operations:

- `PUT` — create or update a key
- `GET` — retrieve a value
- `DELETE` — remove a value

The key-value state machine is separated from the Raft consensus layer through a storage abstraction.

### Write Request Deduplication

Write requests use a client ID and sequence number to identify individual requests.


- Identical repeated requests can reuse an existing log entry.
- Reusing a request ID with a different command is rejected.
- Conflicting request-ID reuse returns HTTP 409 Conflict.


Tests cover repeated requests across leadership changes and restart, including rejection of conflicting request-ID reuse.


### Persistence and Recovery

RaftKV implements two separate persistence mechanisms:

**Raft State Persistence**
- Persists `currentTerm`, `votedFor`, and the `Raft log` using a file-based persister.
- Writes persisted state to a temporary file, synchronizes and closes it, and renames it into place.
- Validates persisted state during startup and returns an error if the state is invalid.
- commitIndex is not persisted; commitment information must be re-established through the Raft protocol and recovery logic after restart.

**WAL-Backed Key-Value Storage**
- Implements a dedicated file-backed `FileEngine` that persists key-value operations using a write-ahead log (WAL).
- Records `SET`, `DELETE`, and `NOOP` operations in the WAL.
- Flushes and synchronizes WAL writes to disk.
- Replays WAL records during startup to reconstruct key-value state.
- Detects and truncates an incomplete final WAL record during recovery.

**Recovery and Testing**

The Raft recovery path restores persisted consensus state and replays committed entries where supported by the current startup flow. The storage engine independently recovers its key-value data from the WAL.

Tests cover persisted-state validation, restart recovery, WAL recovery, persistence failures, and relevant Raft safety properties. Broader multi-node crash-recovery and fault-injection testing remain useful areas for further validation.

**Important distinction:** Raft state persistence and WAL-backed key-value persistence are implemented separately. Their end-to-end recovery behavior must remain consistent across crashes and restarts.


### HTTP API

The key-value store is exposed through an HTTP server at /kv.

The current handlers accept a JSON WriteRequest containing:

- ClientID
- SeqNumber
- Key
- Value — used for write operations

The request ID is validated before processing.

HTTP responses include:

- `200 OK` — successful operation
- `400 Bad Request` — invalid or malformed request
- `409 Conflict` — request ID reused with a different command
- `500 Internal Server Error` — unexpected internal failure
- `503 Service Unavailable` — request sent to a node that is not the leader

The current implementation expects JSON request bodies for POST, GET, and DELETE. GET request bodies are supported by the implementation and its tests, but some HTTP clients and intermediaries handle them inconsistently.

Example write request shape (use the exact JSON field names defined by the WriteRequest struct):
```json
{
  "ClientID": 10,
  "SeqNumber": 1,
  "Key": "name",
  "Value": "RaftKV"
}
```
A successful write returns a response indicating success. GET responses include the retrieved value and a found field. The exact response fields are defined by the server's Response type.

---

## Architecture
At a high level, RaftKV follows this architecture:

```text

                              Client
                                 |
                                 v
                          HTTP KV Server
                                 |
                    +------------+-------------+
                    |                          |
                    v                          v
               Write Request            ReadIndex-based GET
                    |                          |
                    v                          v
                 Raft Node                Leader Check
                    |                          |
                    v                          v
                 Raft Log                ReadProbe Quorum
                    |                          |
           +--------+---------+                v
           |                  |            Read Index
           v                  v                |
   Replication to       Committed Entries      v
     Followers                |            Wait for Apply
           |                  |                |
           v                  v                v
 broadcastAppendEntries()  applyLoop()     KVStateMachine
           |                  |                |
           v                  v                v
  Majority Acknowledgment  ApplyMsg          Get()
           |                  |                |
           v                  v                v
 Advance CommitIndex     KVStateMachine      Engine.Get()
                              |                |
                              v                v
                            Apply()          FileEngine
                              |                |
                              v                v
                 +------------+------------+  Response
                 |            |            |
                 v            v            v
                SET         DELETE        NOOP
                 |            |            |
                 v            v            v
            Engine.Put() Engine.Delete()  Engine.Noop()
                 |            |             |
                 +------+-----+-----+-------+
                              |
                              v
                         FileEngine
                              |
                              v
                         WAL (wal.log)
                             |
                             v
                            Disk


```
### Write Path
A write follows :
```text

Client
  |
  v
HTTP KV Server
  |
  v
Raft Node (Leader)
  |
  v
Append command to Raft log
  |
  v
Replicate to followers
  |
  v
Majority replication
  |
  v
Advance commitIndex
  |
  v
applyLoop() receives ApplyMsg
  |
  v
stateMachine.apply()
  |
  +-------------------+-------------------+
  |                   |                   |
  v                   v                   v
 SET                 DELETE              NOOP
  |                   |                   |
  v                   v                   v
Engine.Put()     Engine.Delete()       No storage
  |                   |                 mutation
  +-------------------+-------------------+
                      |
                      v
                  FileEngine
                      |
                      v
                  WAL (wal.log)
                      |
                      v
                    Disk
                      |
                      v
               Client response

```

The system distinguishes between:
```text
Append ≠ Commit ≠ Apply
```

### Read Path
Reads use a different path from writes:
```text

Client
  |
  v
HTTP KV Server
  |
  v
ReadIndex-based GET
  |
  v
Leader confirms quorum using ReadProbe
  |
  v
Establish safe readIndex
  |
  v
Wait for local application through readIndex
  |
  v
KVStateMachine
  |
  v
Engine.Get()
  |
  v
FileEngine
  |
  v
Return value / found status
  |
  v
HTTP Response to Client

```

GET requests are not appended to the Raft log and are read directly from the engine after the ReadIndex checks.

---

## Raft Node State
Each Raft node maintains the following state.

### Persistent State
The current implementation keeps the following state in memory:
```text
currentTerm
votedFor
log
```

### Volatile State
```text
commitIndex
lastQueued
```

### Leader-Specific Volatile State
```text
nextIndex[]
matchIndex[]
```

These categories describe the intended Raft state model. Refer to the implementation for the exact fields and lifecycle behavior.

---

## Raft Log
Each log entry contains:
```go
type LogEntry struct {
    Index   int
    Term    int
    Command Command
}
```
Commands contain:
```go
type Command struct {
    ClientID  int64
    SeqNumber int64
    Op        string
    Key       string
    Value     string
}
```
The log stores client commands together with the Raft term and log index. The implementation also uses a dummy entry at index zero.

---

## Leader Election
Each node starts as a follower.
If a follower does not receive valid communication from a leader before its randomized election timeout expires, it starts an election.


The candidate:
1. Changes its role to Candidate.
2. Increments its current term.
3. Votes for itself.
4. Resets its election timer.
5. Requests votes from other nodes.
6. Becomes Leader after receiving votes from a majority.


For a cluster of N nodes, a majority is:
```text
floor(N / 2) + 1
```
The candidate's log is compared with the voter's log using:
1. Last log term.
2. Last log index if the terms are equal.

---

## Log Replication
The leader replicates entries using AppendEntries.


For each follower, the leader maintains:

- `nextIndex` — represents the next log position the leader will attempt to replicate to that follower.
- `matchIndex` — represents the highest log index known to be replicated on that follower.


Before accepting entries, a follower verifies:
```text
PrevLogIndex
PrevLogTerm
```

If the previous entry does not match, replication fails and the leader moves nextIndex backwards and retries.

If a conflicting entry is found, the follower removes the conflicting suffix and appends the leader's entries.

---

## Commitment and Application
A log entry becomes eligible for commitment when it has been replicated on a majority of the cluster, subject to Raft's current-term commitment rule.


The leader advances commitIndex, and committed entries are applied in order. The current application path is:
```text

Committed Log Entry
        |
        v
    applyLoop()
        |
        v
     ApplyMsg
        |
        v
   State Machine
        |
        +---- SET ------> Engine.Put()
        |                      |
        |                      v
        |                  FileEngine
        |                      |
        |                      v
        |                  WAL (wal.log)
        |
        +---- DELETE ---> Engine.Delete()
        |                      |
        |                      v
        |                  FileEngine
        |                      |
        |                      v
        |                  WAL (wal.log)
        |
        +---- NOOP -----> Engine.Noop()
                               |
                               v
                           FileEngine
                               |
                               v
                           WAL (wal.log)

```

`applyLoop()` receives ApplyMsg messages from the Raft node, buffers out-of-order messages by command index, and coordinates their sequential processing through the state machine. It retrieves the current lastApplied index from the state machine and processes only the next expected command.

---

## Storage Engine
The storage abstraction is defined by:
```go
type Engine interface {
	Put(index int, key, value []byte) error
	Get(key []byte) ([]byte, bool, error)
	Delete(index int, key []byte) error
	Noop(index int) error
	LastApplied() (int, error)
	Open() error
	Close() error
	IsClosed() bool
	IsFailed() bool
}
```
The storage package includes `MemEngine` and a file-backed `FileEngine`. `MemEngine` stores values in memory using a Go map, while `FileEngine` persists `SET`, `DELETE`, and `NOOP` records in a write-ahead log (WAL), flushes and synchronizes writes, and replays the WAL during recovery.

The server initializes `FileEngine` using `data/node-0/wal.log` and passes it to `KVStateMachine`. The state machine applies committed commands through the storage engine abstraction, while the file-based Raft persister separately persists Raft consensus state in `data/node-0/raft-state.bin`.



---


## Project Structure
```text
RaftKV/
├── cmd/
│   └── server/
│       └── main.go
│
├── data/
|   └── node-0
│        ├── raft-state.bin
│        └── wal.log
│
├── docs/
│   └── invariants.md
│
├── internal/
│   ├── raft/
│   │   ├── handlers.go
│   │   ├── persister.go
│   │   ├── persister_test.go
│   │   ├── raft.go
│   │   ├── raft_test.go
│   │   ├── rpc_types.go
│   │   ├── state.go
│   │   ├── state_test.go
│   │   ├── ticker.go
│   │   └── types.go
│   │
│   ├── server/
│   │   ├── server.go
│   │   ├── server_test.go
│   │   └── state_machine.go
│   │
│   └── storage/
│       ├── engine.go
│       ├── mem_engine.go
|       ├── file_engine.go
│       └── file_engine_test.go
│
├── proto/
├── go.mod
└── README.md
```

---

## Current Implementation Status

### Implemented
- [x] Raft node structure and state management
- [x] Leader election and heartbeats
- [x] `RequestVote` RPC
- [x] `AppendEntries` RPC
- [x] Log replication and conflict resolution
- [x] Majority-based commitment
- [x] Ordered application of committed entries
- [x] In-memory key-value storage engine
- [x] HTTP API for key-value operations
- [x] Election and term-transition handling
- [x] Replication failure-path handling and tests
- [x] Dedicated `ReadProbe` RPC
- [x] Majority-based ReadIndex
- [x] Linearizable-read path and application fence
- [x] Leadership-loss handling during reads
- [x] File-based persistence of Raft state
- [x] Persisted-state validation during startup
- [x] Recovery of committed state on supported startup paths
- [x] Write request deduplication using client request IDs
- [x] Rejection of conflicting request-ID reuse
- [x] Deduplication tests across restart and leadership changes
- [x] HTTP error handling for wrong-leader and conflicting-request responses
- [x] WAL-backed file storage engine (FileEngine)
- [x] WAL replay and incomplete-final-record recovery handling

### In Progress
- [ ] Safe goroutine lifecycle management and graceful shutdown
- [ ] Broader multi-node crash-recovery validation
- [ ] Additional failure-injection testing around persistence and restart boundaries
- [ ] End-to-end validation of recovery and durability across failure scenarios


### Planned
- [ ] Snapshots and log compaction
- [ ] Fault injection and chaos testing
- [ ] Broader linearizability and correctness testing
- [ ] Observability and metrics
- [ ] Benchmarking and profiling
- [ ] Production hardening

---

## Correctness Invariants
RaftKV is being developed around explicit correctness invariants.


Some of the current invariants include:
- A server grants at most one vote per term.
- A node rejects RPCs from stale terms.
- A node steps down when it discovers a higher term.
- A candidate's log must be sufficiently up-to-date to receive a vote.
- A follower accepts log entries only when `PrevLogIndex` and `PrevLogTerm` match.
- Committed entries are applied in log order.
- `lastApplied` never exceeds `commitIndex`.
- Log commitment respects majority replication and Raft's current-term rule.
- Commit index never moves backwards.
- A committed entry must not be overwritten by an outdated leader.
- A ReadIndex requires confirmation from a majority in the leader's current term.
- A higher-term response invalidates the current leader's ReadIndex operation.
- A read waits until the local node has applied through the ReadIndex.
- A node that loses leadership does not complete the read through the old leader path.


Persistence and recovery invariants are partially covered by the existing test suite, including persisted-state validation, restart recovery, and persistence-failure handling. Additional failure tests will expand coverage of multi-node recovery, interrupted writes, and crash-recovery scenarios.

---

## Testing
The test suite currently covers:


- Leader election and voting rules
- Election timeout behavior
- Higher-term transitions and heartbeats
- Log replication and conflict resolution
- `nextIndex` and `matchIndex` updates
- Majority commitment and commit-index safety
- State-machine application ordering and out-of-order message handling
- Client request completion and application waiters
- Replication failure paths
- ReadProbe behavior and ReadIndex quorum confirmation
- Linearizable-read fencing and leadership loss during reads
- Repeated write request IDs and request deduplication
- Rejection of conflicting request-ID reuse
- Concurrent duplicate and distinct write requests
- HTTP API behavior, including HTTP 409 for conflicting request IDs
- Raft state persistence and persisted-state validation
- Restart recovery and committed-entry replay
- WAL recovery and incomplete-final-record handling
- Persistence-failure handling and recovery retries
- Deduplication across restart and leadership changes

Run the Raft test suite with:
```bash
go test ./internal/raft -count=1
```

Run the server tests:
```bash
go test ./internal/server -count=1
```

Run all project tests:
```bash
go test ./... -count=1
```

Run the race detector:
```bash
go test -race ./...
```

A passing test suite provides evidence for the scenarios tested; it does not by itself establish correctness under every possible distributed failure.

---

## Development Roadmap

### 1. Raft Consensus — Completed
- Leader election and heartbeats
- Log replication and conflict resolution
- Majority-based commitment and ordered application
- Linearizable reads using ReadIndex
- Leadership and term-transition handling

### 2. Persistence and Recovery — Core Implementation Completed
- File-based persistence of Raft state (`currentTerm`, `votedFor`, and log)
- Persisted-state validation during startup
- WAL-backed key-value storage and recovery
- WAL replay and incomplete-final-record recovery handling
- Recovery of committed entries on supported startup paths

**Remaining work:**
- Broader multi-node crash-recovery validation
- Additional interrupted-write and failure-injection scenarios

### 3. Storage — Core Implementation Completed
- In-memory key-value storage engine (`MemEngine`)
- WAL-backed file storage engine (`FileEngine`)
- Storage abstraction integrated with the state machine
- Separate persistence mechanisms for Raft state and key-value operations

**Remaining work:**
- Further validation of end-to-end durability across crashes and restarts

### 4. Scalability and Compaction
- Snapshot creation and restoration
- Raft log compaction
- Snapshot installation and synchronization with lagging followers

### 5. Correctness and Reliability
- Fault injection and chaos testing
- Broader linearizability and correctness testing
- Expanded multi-node failure and recovery testing
- Additional validation of retries and deduplication across leadership changes

### 6. Observability and Performance
- Metrics and operational visibility
- Throughput and latency benchmarks
- Profiling and performance optimization

### 7. Production Hardening
- Safe goroutine lifecycle management
- Graceful shutdown of Raft and HTTP server components
- Robust error handling
- Final integration and race-detector testing


---

## Goals
The primary goal of RaftKV is to understand and implement the engineering principles behind distributed systems rather than simply use an existing distributed database implementation.

The project focuses on:
- Distributed consensus
- Leader election
- Replicated logs and state application
- Log replication
- Fault tolerance
- Durable storage
- Crash recovery
- Linearizability
- Distributed-system testing
- Performance analysis


RaftKV is being built from consensus and storage primitives upward, with correctness and failure handling treated as first-class design requirements.

---

## Status

RaftKV is an active distributed-systems project built from scratch in Go.

The current implementation includes:

- A functional Raft consensus core with leader election, heartbeats, log replication, conflict resolution, and majority-based commitment.
- ReadIndex-based linearizable reads using quorum confirmation and application-index fencing.
- An HTTP API supporting `PUT`, `GET`, and `DELETE`, with write-request deduplication and conflicting request-ID detection.
- File-based Raft state persistence with startup validation.
- A WAL-backed `FileEngine` that persists key-value operations and replays its WAL during recovery.
- Ordered state-machine application, application waiters, and recovery handling for storage failures.
- Automated tests covering consensus, replication, linearizable reads, request deduplication, persistence, recovery, and concurrency.

The existing Raft, server, and storage test suites have been reported as passing.

**Next priorities** are safe goroutine lifecycle management and graceful shutdown, followed by snapshots and Raft log compaction. Broader multi-node fault testing, observability, benchmarking, and performance optimization remain future work.

RaftKV is intended as a learning-oriented distributed-systems project, with an emphasis on consensus, storage durability, recovery, and correctness under failure.

---
