# Raft & KV System Invariants

1. Election Safety: At most one leader can be elected in a given term.
2. Leader Append-Only: A leader never overwrites or truncates its own log entries; it only appends new entries.
3. Log Matching: If two logs contain an entry with the same index and term, they store the same command and are identical up to that index.
4. Leader Completeness: If a log entry is committed in a given term, that entry will be present in the logs of the leaders for all higher-numbered terms.
5. State Machine Safety: If a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.
6. Client Idempotency: Retrying a client request with the same ClientID and SequenceNumber will never execute the mutation twice.