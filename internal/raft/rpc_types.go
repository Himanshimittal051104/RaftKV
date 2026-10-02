package raft

type RequestVoteArgs struct {
	Term         int 
	CandidateID  int 
	LastLogIndex int 
	LastLogTerm  int 
}

type RequestVoteReply struct {
	Term        int  
	VoteGranted bool 
}

// AppendEntriesArgs contains arguments for log replication and heartbeats.
type AppendEntriesArgs struct {
	Term         int       
	LeaderID     int        
	PrevLogIndex int        
	PrevLogTerm  int        
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader's commitIndex then the follower's apply loop can apply committed entries to its state machine.
}

// AppendEntriesReply contains the response from an AppendEntries RPC.
type AppendEntriesReply struct {
	Term    int  // Current term of receiver, for leader to update itself
	Success bool // True if follower contained entry matching PrevLogIndex and PrevLogTerm
}

// LastLogIndex + LastLogTerm
//         ↓
// "Is this candidate's log up-to-date?"

// PrevLogIndex + PrevLogTerm
//         ↓
// "Does the follower's log match mine here?"

// LeaderCommit
//         ↓
// "Which entries are known to be committed?"


