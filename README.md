# RaftKV

> A fault-tolerant distributed key-value store built from scratch in Go using the Raft consensus algorithm.

RaftKV is a distributed key-value store designed to explore the internals of
distributed systems, consensus, replicated state machines, and fault-tolerant
storage.

The system currently implements Raft-based leader election, heartbeats,
log replication, conflict resolution, majority-based commitment, and a
replicated key-value state machine exposed through an HTTP API.

---

## Features

### Raft Consensus

- Leader election using randomized election timeouts
- Candidate self-voting
- Majority-based leader election
- Term-based leadership
- RequestVote RPC
- AppendEntries RPC
- Heartbeats
- Log replication
- Log conflict detection and resolution
- `nextIndex` and `matchIndex` tracking
- Majority-based log commitment
- Ordered application of committed entries

### Key-Value Store

Supports:

- `PUT`
- `GET`
- `DELETE`

The key-value state machine is separated from the Raft consensus layer
through a storage abstraction.

### Storage Abstraction

RaftKV currently provides an in-memory storage engine through the
`storage.Engine` interface.

This abstraction allows the consensus layer to remain independent of the
underlying storage implementation.

### HTTP API

The key-value store is exposed through an HTTP server.

Current endpoint:

```text
/kv
```

Supported operations:

```text
POST   /kv
GET    /kv?key=<key>
DELETE /kv?key=<key>
```

---

## Architecture
At a high level, RaftKV follows:

```text
                    Client
                      │
                      ▼
                HTTP KV Server
                      │
                      ▼
                  Raft Node
                      │
          ┌───────────┴───────────┐
          │                       │
          ▼                       ▼
       Raft Log             State Machine
          │                       │
          │                 Storage Engine
          │                       │
          └──────────┬────────────┘
                     │
                Replication
                     │
             ┌───────┴───────┐
             ▼               ▼
          Follower         Follower
```

A write follows the general path:
```text
Client
  │
  ▼
Leader
  │
  ▼
Append command to Raft log
  │
  ▼
Replicate to followers
  │
  ▼
Majority replicated
  │
  ▼
Commit
  │
  ▼
Apply to state machine
  │
  ▼
Storage Engine
```

---

## Raft Node
Each Raft node maintains the following state.

### Persistent State
The current implementation keeps the following state in memory:
```text
currentTerm
votedFor
log
```
Durable persistence and crash recovery are planned for a later phase.

### Volatile State
```text
commitIndex
lastApplied
```

### Leader-Specific Volatile State
```text
nextIndex[]
matchIndex[]
```

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
The log stores client commands together with the Raft term and log index.

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
```text
nextIndex
matchIndex
```
`nextIndex` represents the next log position the leader will attempt to replicate to that follower.


`matchIndex` represents the highest log index known to be replicated on that follower.


Before accepting entries, a follower verifies:
```text
PrevLogIndex
PrevLogTerm
```

If the previous entry does not match, replication fails and the leader moves nextIndex backwards and retries.

If a conflicting entry is found, the follower removes the conflicting suffix and appends the leader's entries.

---

## Commitment
A log entry becomes eligible for commitment when it has been replicated on a majority of the cluster.


The leader advances:
```text
commitIndex
```
and committed entries are applied in order.


The application path is:
```text
Raft
  │
  │ committed command
  ▼
ApplyMsg
  │
  ▼
State Machine
  │
  ▼
Storage Engine
```
The distinction between these stages is important:
```text
Append ≠ Commit ≠ Apply
```
Appending an entry to the leader's local log does not mean that the operation has been committed.

---

## State Machine
RaftKV separates consensus from application logic.


The Raft layer is responsible for:
- Leader election
- Log replication
- Commitment
- Ordering
  
The state machine is responsible for applying committed commands.


The storage layer provides the underlying key-value operations.
```text
Raft
  │
  ▼
State Machine
  │
  ▼
storage.Engine
```

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
The current implementation is:
```text
MemEngine
```
which stores data in memory using a Go map.


A persistent storage engine is planned as a future phase.

---

## HTTP API
### PUT
Create or update a key:
```http
POST /kv
Content-Type: application/json

{
  "key": "name",
  "value": "RaftKV"
}
```
Example response:
```json
{
  "success": true
}
```
### GET
Retrieve a value:
```http
GET /kv?key=name
```
Example response:
```json
{
  "value": "RaftKV",
  "found": true
}
```
### DELETE
Delete a key:
```http
DELETE /kv?key=name
```
Example response:
```json
{
  "success": true
}
```

---

## Project Structure
```text
RaftKV/
├── cmd/
│   └── server/
│
├── docs/
│   └── invariants.md
│
├── internal/
│   ├── raft/
│   │   ├── handlers.go
│   │   ├── raft.go
│   │   ├── raft_test.go
│   │   ├── rpc_types.go
│   │   ├── ticker.go
│   │   └── types.go
│   │
│   ├── server/
│   │   ├── server.go
│   │   └── server_test.go
│   │
│   └── storage/
│       ├── engine.go
│       └── mem_engine.go
│
├── proto/
│
├── go.mod
└── README.md
```

---

## Current Implementation Status

### Implemented
- [x] Raft node structure and state management
- [x] Leader election
- [x] Heartbeats
- [x] RequestVote RPC
- [x] AppendEntries RPC
- [x] Log replication
- [x] Log conflict resolution
- [x] Majority-based commitment
- [x] Replicated key-value operations
- [x] In-memory storage engine
- [x] HTTP API

### In Progress
- [ ] Correctness hardening and failure testing
- [ ] Linearizable reads
- [ ] Persistent storage
- [ ] Crash recovery
- [ ] Snapshots and log compaction
- [ ] Fault injection and chaos testing

### Planned
- [ ] Custom storage engine
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
- A follower accepts log entries only when PrevLogIndex and PrevLogTerm match.
- Committed entries are applied in log order.
- lastApplied never exceeds commitIndex.
- A log entry is committed only after replication on a majority.
Additional invariants will be added as persistence, snapshots, linearizable reads, and failure testing are implemented.

---

## Development Roadmap

1. **Raft Consensus**
   - Leader election
   - Heartbeats
   - Log replication
   - Commitment and state-machine application

2. **Durability**
   - Persistent Raft state
   - Crash recovery
   - Custom storage engine

3. **Scalability & Compaction**
   - Snapshots
   - Log compaction

4. **Correctness & Reliability**
   - Fault injection
   - Linearizability testing
   - Failure recovery testing

5. **Observability & Performance**
   - Metrics
   - Benchmarking
   - Profiling
   - Performance optimization

6. **Production Hardening**
   - Concurrency hardening
   - Graceful shutdown
   - Robust error handling
   - Final integration testing

---

## Goals
The primary goal of RaftKV is to understand and implement the engineering principles behind distributed systems rather than simply using an existing distributed database implementation.


The project focuses on:
- Distributed consensus
- Leader election
- Replicated state machines
- Log replication
- Fault tolerance
- Durable storage
- Crash recovery
- Linearizability
- Distributed-system testing
- Performance analysis
RaftKV is being built from the consensus and storage primitives upward, with correctness and failure handling treated as first-class design requirements.

---

## Status
RaftKV is an active distributed-systems project under development.
The current implementation focuses on the Raft consensus core and replicated key-value state machine. Persistence, crash recovery, snapshots, fault injection, linearizability testing, observability, and performance engineering are part of the planned development roadmap.

---
