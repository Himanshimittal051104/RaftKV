package raft

import "time"

// RequestVote handles incoming vote requests from candidates.
func (rn *RaftNode) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if args.Term < rn.currentTerm {
		reply.Term = rn.currentTerm
		reply.VoteGranted = false
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
			reply.VoteGranted = false
			return
		}

		rn.currentTerm = newTerm
		rn.role = Follower
		rn.votedFor = newVotedFor
		
	}

	canVote := rn.votedFor == -1 || rn.votedFor == args.CandidateID

	lastLogIndex := len(rn.log) - 1
	lastLogTerm := rn.log[lastLogIndex].Term

	logUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		logUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		logUpToDate = true
	}

	if canVote && logUpToDate {
		newVotedFor := args.CandidateID

		if err := rn.persistState(
			rn.currentTerm,
			newVotedFor,
			rn.log,
		); err != nil {
			reply.Term = rn.currentTerm
			reply.VoteGranted = false
			return
		}
		rn.votedFor = newVotedFor
		rn.role = Follower
		rn.lastResetTime = time.Now()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rn.currentTerm
}

// AppendEntries handles log replication and heartbeats from the leader.
func (rn *RaftNode) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rn.mu.Lock()

	reply.Success = false
	reply.Term = rn.currentTerm

	// 1. Reply false if term < currentTerm
	if args.Term < rn.currentTerm {
		rn.mu.Unlock()
		return
	}

	// If term is higher, update state and become follower
	newTerm := rn.currentTerm
	newVotedFor := rn.votedFor

	if args.Term > rn.currentTerm {
		newTerm = args.Term
		newVotedFor = -1
	}


	// 2. Reply false if log doesn't contain an entry at PrevLogIndex matching PrevLogTerm
	if args.PrevLogIndex >= len(rn.log) {
		reply.Term =newTerm
		rn.mu.Unlock()
		return
	}

	if rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rn.currentTerm
		rn.mu.Unlock()
		return
	}

	newLog := make([]LogEntry, len(rn.log))
	copy(newLog, rn.log)

	// 3. Process incoming entries (Conflict resolution & appending)
	argsIndex := 0
	localIndex := args.PrevLogIndex + 1

	for argsIndex < len(args.Entries) {
		if localIndex < len(rn.log) {
			// Conflict check: if existing entry conflicts with new one, delete existing and all that follow it
			if newLog[localIndex].Term != args.Entries[argsIndex].Term {
				newLog = newLog[:localIndex]
				newLog = append(newLog, args.Entries[argsIndex:]...)
				break
			}
		} else {
			// No conflict, append remaining entries
			newLog = append(newLog, args.Entries[argsIndex:]...)
			break
		}
		localIndex++
		argsIndex++
	}

	if err := rn.persistState(newTerm, newVotedFor, newLog); err != nil {
		reply.Term = rn.currentTerm
		rn.mu.Unlock()
		return
	}

	rn.currentTerm = newTerm
	rn.votedFor = newVotedFor
	rn.log = newLog
	rn.role = Follower
	rn.lastResetTime = time.Now()
	
	var msgs []ApplyMsg

	// 4. Update commitIndex based on leaderCommit
	if args.LeaderCommit > rn.commitIndex {
		rn.commitIndex = min(args.LeaderCommit, len(rn.log)-1)
		msgs = rn.collectCommittedEntries()
	}

	reply.Success = true
	reply.Term = rn.currentTerm

	rn.mu.Unlock()

	for _, msg := range msgs {
		if rn.applyCh != nil {
			rn.applyCh <- msg
		}
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
		rn.currentTerm = args.Term
		rn.role = Follower
		rn.votedFor = -1
	}

	// We received a valid current-term probe from the leader.
	rn.role = Follower
	rn.lastResetTime = time.Now()

	reply.Term = rn.currentTerm
}
