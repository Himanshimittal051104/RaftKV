# RaftKV

> A fault-tolerant distributed key-value store built from scratch in Go using the Raft consensus algorithm.

RaftKV is a learning-oriented distributed key-value store designed to explore distributed systems, consensus, replicated state machines, and fault-tolerant storage.

The project currently implements Raft-based leader election, heartbeats, log replication, conflict resolution, majority-based commitment, a replicated key-value state machine, ReadIndex-based linearizable reads, file-based persistence of Raft state, basic recovery logic, and an HTTP API.

**Development status**: Persistence and basic recovery are implemented. Comprehensive crash-recovery testing and multi-node restart validation remain in progress.

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
6. Performs the read directly from the local storage engine.
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


Deduplication across leadership changes and recovery still requires further verification.

### Persistence and Recovery

RaftKV persists the following Raft state:

- currentTerm
- votedFor
- log
- commitIndex

The file persister writes to a temporary file, synchronizes and closes it, and then renames it into place.

During startup, persisted state is decoded and validated. Invalid persisted state causes initialization to return an error.

Basic recovery is implemented. The single-node startup path can reconstruct committed key-value state by replaying committed log entries.

**Important distinction**: Raft state persistence is implemented, but the current key-value storage engine (MemEngine) stores data in memory. The implemented single-node recovery path reconstructs committed key-value state from the log; a dedicated persistent key-value engine is not yet implemented.

Comprehensive crash-recovery testing, interrupted-write scenarios, and multi-node restart validation remain in progress.


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
                            v
                         Raft Node
                            |
                 +----------+----------------+
                 |                           |
                 v                           |
              Raft Log                       |
                 |                           |
          +------+-------+                   |
          |              |                   |
          v              v                   |
Replication to     Committed Entries         |
Followers                 |                  |
                          v                  |
                      applyLoop()            |
                          |                  |
                          v                  |
                       ApplyMsg              | ReadIndex-based GET
                          |                  |
                          v                  |
                      applyMessage()         |
                          |                  |
                    +-----+-----+            |
                    |           |            |
                    v           v            |
                   SET        DELETE         |
                    |           |            |
                    v           v            |
               Engine.Put  Engine.Delete     |
                    |           |            |
                    +-----+-----+------------+
                          |
                          v
                  MemEngine (In-Memory KV)
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
Leader
  |
  v
Append command to Raft log
  |
  v
Replicate to followers
  |
  v
Majority replicated
  |
  v
Commit
  |
  v
applyLoop() receives ApplyMsg
  |
  v
applyMessage()
  |
  v
Engine.Put() or Engine.Delete()
  |
  v
MemEngine
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
Wait for local application through read index
  |
  v
Read from Engine
  |
  v
Response
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
commitIndex
```

### Volatile State
```text
lastApplied
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
Committed log entry
        |
        v
    applyLoop()
        |
        v
     ApplyMsg
        |
        v
  applyMessage()
        |
        +---- SET ------> Engine.Put()
        |
        +---- DELETE ---> Engine.Delete()
        |
        +---- NOOP -----> No engine operation
```

`applyLoop()` buffers out-of-order messages and processes consecutive entries in index order. It advances `lastApplied` and notifies requests waiting for application. The `applyMessage()` function currently handles `NOOP`, `SET`, and `DELETE`. 

---

## Storage Engine
The storage abstraction is defined by:
```go
type Engine interface {
    Put(key, value []byte) error
    Get(key []byte) ([]byte, bool, error)
    Delete(key []byte) error
    Close() error
}
```
The current implementation is `MemEngine`, which stores values in memory using a Go map. `applyMessage()` calls `Engine.Put()` for `SET` commands and `Engine.Delete()` for `DELETE` commands. `GET` operations read from the engine after the ReadIndex checks.

Raft state persistence is implemented separately. On the implemented single-node recovery path, committed log entries can be replayed to reconstruct key-value state after restart. A dedicated persistent key-value engine and comprehensive durability integration remain future improvements.

---


## Project Structure
```text
RaftKV/
├── cmd/
│   └── server/
│       └── main.go
│
├── data/
│   └── node-0/
│       └── raft-state.bin
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
│       └── mem_engine.go
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
- [x] Basic committed-state reconstruction on the single-node startup path
- [x] Write request deduplication using client request IDs
- [x] Rejection of conflicting request-ID reuse
- [x] HTTP error handling for wrong-leader and conflicting-request responses

### In Progress
- [ ] Comprehensive crash-recovery validation
- [ ] Restart and recovery testing across multiple nodes
- [ ] Failure testing around interrupted persistence operations
- [ ] Verification of request deduplication across leadership changes and recovery

### Planned
- [ ] Dedicated persistent key-value storage engine
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


Persistence and recovery invariants and their failure tests will be expanded as recovery work progresses.

---

## Testing
RaftKV includes tests covering both normal operation and Raft safety properties.


The test suite currently covers:
- Leader election and voting rules
- Election timeout behavior
- Higher-term transitions and heartbeats
- Log replication and conflict resolution
- nextIndex and matchIndex updates
- Majority commitment and commit-index safety
- State-machine application ordering
- Client request completion
- Replication failure paths
- ReadProbe behavior and ReadIndex quorum confirmation
- Linearizable-read fencing and leadership loss during reads
- Repeated write request IDs
- Rejection of conflicting request-ID reuse
- HTTP API behavior, including HTTP 409 for conflicting request IDs
- Persisted-state validation and recovery behavior where covered by the current tests

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

1. **Raft Consensus — Completed**
   - Leader election and heartbeats
   - Log replication and conflict resolution
   - Commitment and ordered application
   - Linearizable reads using ReadIndex

2. **Persistence and Recovery — Implementation underway**
   - File-based persistence of Raft state
   - Persisted-state validation
   - Basic committed-state reconstruction
   - Restart tests and multi-node recovery validation
   - Testing interrupted writes and failure scenarios

3. **Storage**
   - Dedicated persistent key-value storage engine
   - Storage durability integration with consensus

4. **Scalability & Compaction**
   - Snapshots
   - Log compaction

5. **Correctness & Reliability**
   - Fault injection
   - Broader linearizability testing
   - Failure and recovery testing
   - Retry and deduplication verification across leadership changes

6. **Observability & Performance**
   - Metrics
   - Benchmarking
   - Profiling
   - Performance optimization

7. **Production Hardening**
   - Concurrency hardening
   - Graceful shutdown
   - Robust error handling
   - Final integration testing

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
RaftKV is an active distributed-systems project under development.


The current implementation contains a functional Raft consensus core, a replicated key-value operation path, an HTTP API, file-based persistence of Raft state, basic recovery logic, request-ID deduplication, and a ReadIndex-based linearizable-read path. Committed commands are applied through `applyLoop()` and `ApplyMsg` processing, with `applyMessage()` directly invoking the in-memory engine; there is no separate state-machine component in the current architecture.


The next major focus is comprehensive recovery validation, including multi-node restart and interrupted-persistence scenarios. A dedicated persistent KV engine, snapshots, broader failure testing, observability, and performance engineering follow on the roadmap..

---
