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



