package raft

import (
	"sync"
	"time"
)

// Start begins the election and heartbeat background goroutine.
func (rn *RaftNode) Start() {
	go rn.runLoop()
}

// Stop cleanly terminates the node's loop.
func (rn *RaftNode) Stop() {
	close(rn.stopCh)
}

func (rn *RaftNode) runLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rn.stopCh:
			return
		case <-ticker.C:
			rn.mu.Lock()
			role := rn.role
			elapsed := time.Since(rn.lastResetTime)
			timeout := rn.electionTimeout
			rn.mu.Unlock()

			if role != Leader && elapsed >= timeout {
				rn.startElection()
			} else if role == Leader {
				rn.broadcastHeartbeat()
			}
		}
	}
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.role = Candidate
	rn.currentTerm++
	rn.votedFor = rn.me
	rn.resetElectionTimeout()

	term := rn.currentTerm
	me := rn.me
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := rn.log[lastLogIndex].Term
	peers := rn.peers
	rn.mu.Unlock()

	var votesMu sync.Mutex
	votesReceived := 1 // Vote for self

	for i := range peers {
		if i == me {
			continue
		}

		go func(peerID int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply

			peers[peerID].RequestVote(&args, &reply)

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if rn.role != Candidate || rn.currentTerm != term {
				return
			}

			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.role = Follower
				rn.votedFor = -1
				rn.resetElectionTimeout()
				return
			}

			if reply.VoteGranted {
				votesMu.Lock()
				votesReceived++
				hasQuorum := votesReceived > len(peers)/2
				votesMu.Unlock()

				if hasQuorum && rn.role == Candidate {
					rn.role = Leader
					for j := range rn.peers {
						rn.nextIndex[j] = len(rn.log)
						rn.matchIndex[j] = 0
					}
					go rn.broadcastHeartbeat()
				}
			}
		}(i)
	}
}

func (rn *RaftNode) broadcastHeartbeat() {
	rn.mu.Lock()
	if rn.role != Leader {
		rn.mu.Unlock()
		return
	}
	term := rn.currentTerm
	me := rn.me
	peers := rn.peers
	commitIndex := rn.commitIndex
	rn.mu.Unlock()

	for i := range peers {
		if i == me {
			continue
		}

		go func(peerID int) {
			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     me,
				PrevLogIndex: len(peers[peerID].log) - 1,
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: commitIndex,
			}
			var reply AppendEntriesReply

			peers[peerID].AppendEntries(&args, &reply)

			rn.mu.Lock()
			defer rn.mu.Unlock()

			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.role = Follower
				rn.votedFor = -1
				rn.resetElectionTimeout()
			}
		}(i)
	}
}