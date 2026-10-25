package raft

import "time"

// RequestVote handles incoming vote requests from candidates.
func (rn *RaftNode) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply.VoteGranted = false

	if args.Term < rn.currentTerm {
		reply.Term = rn.currentTerm
		return
	}

	if args.Term > rn.currentTerm {
		newTerm := args.Term
		newVotedFor := -1

		if err := rn.persistState(
			newTerm,
			newVotedFor,
			rn.log,
		); err != nil {
			reply.Term = rn.currentTerm
			return
		}

		rn.currentTerm = newTerm
		rn.role = Follower
		rn.votedFor = newVotedFor
		rn.resetElectionTimeout()
	}

	reply.Term = rn.currentTerm

	canVote := rn.votedFor == -1 || rn.votedFor == args.CandidateID

	lastLogIndex := len(rn.log) - 1
	lastLogTerm := rn.log[lastLogIndex].Term

	logUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		logUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		logUpToDate = true
	}

	if !canVote || !logUpToDate {
		return
	}

	if rn.votedFor != args.CandidateID {
		if err := rn.persistState(
			rn.currentTerm,
			args.CandidateID,
			rn.log,
		); err != nil {
			return
		}

		rn.votedFor = args.CandidateID
	}

	rn.role = Follower
	rn.resetElectionTimeout()

	reply.VoteGranted = true
	reply.Term = rn.currentTerm
}

// AppendEntries handles log replication and heartbeats from the leader.

func (rn *RaftNode) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rn.mu.Lock()

	reply.Success = false
	reply.Term = rn.currentTerm

	// 1. Reject stale leaders.
	if args.Term < rn.currentTerm {
		rn.mu.Unlock()
		return
	}

	// 2. Persist a higher term before updating in-memory state.
	if args.Term > rn.currentTerm {
		if err := rn.persistState(args.Term, -1, rn.log); err != nil {
			reply.Term = rn.currentTerm
			rn.mu.Unlock()
			return
		}

		rn.currentTerm = args.Term
		rn.votedFor = -1
	}

	rn.role = Follower
	rn.resetElectionTimeout()
	reply.Term = rn.currentTerm

	// 3. Validate the previous log entry.
	if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rn.log) {
		rn.mu.Unlock()
		return
	}

	if rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rn.mu.Unlock()
		return
	}

	// 4. Reconcile the incoming entries with the local log.
	newLog := make([]LogEntry, len(rn.log))
	copy(newLog, rn.log)

	argsIndex := 0
	localIndex := args.PrevLogIndex + 1
	logChanged := false

	for argsIndex < len(args.Entries) {
		if localIndex >= len(newLog) {
			newLog = append(newLog, args.Entries[argsIndex:]...)
			logChanged = true
			break
		}

		// Matching index and term means this entry already matches.
		if newLog[localIndex].Term == args.Entries[argsIndex].Term {
			localIndex++
			argsIndex++
			continue
		}

		if localIndex <= rn.commitIndex {
			rn.mu.Unlock()
			return
		}
		// Conflict: remove this entry and all entries after it.
		newLog = newLog[:localIndex]
		newLog = append(newLog, args.Entries[argsIndex:]...)
		logChanged = true
		break
	}

	// 5. Persist only when the log actually changes.
	if logChanged {
		if err := rn.persistState(
			rn.currentTerm,
			rn.votedFor,
			newLog,
		); err != nil {
			reply.Term = rn.currentTerm
			rn.mu.Unlock()
			return
		}

		rn.log = newLog
	}

	// 6. Advance the commit index using the leader's commit index.
	var msgs []ApplyMsg

	if args.LeaderCommit > rn.commitIndex {
		rn.commitIndex = min(args.LeaderCommit, len(rn.log)-1)
		msgs = rn.collectCommittedEntries()
	}

	reply.Success = true
	reply.Term = rn.currentTerm

	rn.mu.Unlock()

	// 7. Deliver committed entries to the state machine.
	// 7. Deliver committed entries without blocking the RPC handler.
	if len(msgs) > 0 {
		rn.spawn(func() {
			for _, msg := range msgs {
				select {
				case rn.applyCh <- msg:
				case <-rn.stopCh:
					return
				}
			}
		})
	}
}

// Helper utility for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rn *RaftNode) ReadProbe(args *ReadProbeArgs, reply *ReadProbeReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if args.Term < rn.currentTerm {
		reply.Term = rn.currentTerm
		return
	}

	if args.Term > rn.currentTerm {
		if err := rn.persistState(args.Term, -1, rn.log); err != nil {
			reply.Term = rn.currentTerm
			return
		}

		rn.currentTerm = args.Term
		rn.votedFor = -1
	}

	// We received a valid current-term probe from the leader.
	rn.role = Follower
	rn.lastResetTime = time.Now()
	reply.Term = rn.currentTerm
}
