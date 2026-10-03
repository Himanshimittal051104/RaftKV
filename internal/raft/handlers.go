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
		rn.currentTerm = args.Term
		rn.role = Follower
		rn.votedFor = -1
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
		rn.votedFor = args.CandidateID
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
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.role = Follower
		rn.votedFor = -1
	}

	rn.role = Follower
	rn.lastResetTime = time.Now()

	// 2. Reply false if log doesn't contain an entry at PrevLogIndex matching PrevLogTerm
	if args.PrevLogIndex >= len(rn.log) {
		reply.Term = rn.currentTerm
		reply.Success = false
		rn.mu.Unlock()
		return
	}

	if rn.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rn.currentTerm
		reply.Success = false
		rn.mu.Unlock()
		return
	}

	// 3. Process incoming entries (Conflict resolution & appending)
	argsIndex := 0
	localIndex := args.PrevLogIndex + 1

	for argsIndex < len(args.Entries) {
		if localIndex < len(rn.log) {
			// Conflict check: if existing entry conflicts with new one, delete existing and all that follow it
			if rn.log[localIndex].Term != args.Entries[argsIndex].Term {
				rn.log = rn.log[:localIndex]
				rn.log = append(rn.log, args.Entries[argsIndex:]...)
				break
			}
		} else {
			// No conflict, append remaining entries
			rn.log = append(rn.log, args.Entries[argsIndex:]...)
			break
		}
		localIndex++
		argsIndex++
	}

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
