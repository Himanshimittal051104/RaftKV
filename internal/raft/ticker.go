package raft

import (
	"sync"
	"time"
)

// Start begins the election and heartbeat background goroutine.
func (rn *RaftNode) StartBackground() {
	go rn.runLoop()
	go rn.applyLoop()
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
				rn.broadcastAppendEntries()
			}
		}
	}
}

func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	if rn.role == Leader ||
        time.Since(rn.lastResetTime) < rn.electionTimeout {
        rn.mu.Unlock()
        return
    }

	newTerm := rn.currentTerm + 1
	newVotedFor := rn.me
	if err := rn.persistState(
		newTerm,
		newVotedFor,
		rn.log,
	); err != nil {
		rn.mu.Unlock()
		return
	}


	rn.role = Candidate
	rn.currentTerm=newTerm
	rn.votedFor = newVotedFor
	rn.resetElectionTimeout()
	
	term := rn.currentTerm
	me := rn.me
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := rn.log[lastLogIndex].Term
	peers := rn.peers
	rn.mu.Unlock()
	println("Node", me, "started election for term", term)
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
				return // Suppose A starts an election:Then while waiting for votes, A receives an AppendEntries from another valid leader: then A becomes a follower and ignores the vote replies from other nodes.
			}

			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.role = Follower
				rn.votedFor = -1
				rn.resetElectionTimeout()
				return
			}

			if reply.VoteGranted {
				println("Node", me, "received vote from", peerID, "for term", term)
				votesMu.Lock()
				votesReceived++
				hasQuorum := votesReceived > len(peers)/2
				votesMu.Unlock()

				if hasQuorum && rn.role == Candidate {
					println("Node", me, "BECAME LEADER for term", term)
					rn.role = Leader

					for j := range rn.peers {
						rn.nextIndex[j] = len(rn.log)
						rn.matchIndex[j] = 0
					}
					go rn.broadcastAppendEntries()
				}
			}
		}(i)
	}
}

func (rn *RaftNode) broadcastAppendEntries() {
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
			rn.mu.Lock()
			if rn.role != Leader || rn.currentTerm != term {
				rn.mu.Unlock()
				return
			}

			prevLogIndex := rn.nextIndex[peerID] - 1
			if prevLogIndex < 0 {
				prevLogIndex = 0
			}
			prevLogTerm := rn.log[prevLogIndex].Term

			// Slice entries to send to this peer starting from nextIndex[peerID]
			var entries []LogEntry
			if rn.nextIndex[peerID] < len(rn.log) {
				entries = make([]LogEntry, len(rn.log)-rn.nextIndex[peerID])
				copy(entries, rn.log[rn.nextIndex[peerID]:])
			}

			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}
			rn.mu.Unlock()

			var reply AppendEntriesReply
			peers[peerID].AppendEntries(&args, &reply)

			rn.mu.Lock()

			if rn.role != Leader || rn.currentTerm != term {
				rn.mu.Unlock()
				return
			}

			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.role = Follower
				rn.votedFor = -1
				rn.resetElectionTimeout()
				rn.mu.Unlock()
				return
			}

			var msgs []ApplyMsg

			if reply.Success {
				// Update nextIndex and matchIndex for peer
				newMatch := prevLogIndex + len(entries)
				if newMatch > rn.matchIndex[peerID] {
					rn.matchIndex[peerID] = newMatch
				}
				rn.nextIndex[peerID] = rn.matchIndex[peerID] + 1

				// Check if we can advance commitIndex
				// A log entry is committed if stored on a majority of servers
				for N := len(rn.log) - 1; N > rn.commitIndex; N-- {
					if rn.log[N].Term == rn.currentTerm {
						count := 1 // Leader counts itself
						for j := range peers {
							if j != me && rn.matchIndex[j] >= N {
								count++
							}
						}
						if count > len(peers)/2 {
							rn.commitIndex = N
							msgs = rn.collectCommittedEntries()
							break
						}
					}
				}
			} else {
				// If append failed because of log inconsistency, decrement nextIndex and retry
				if rn.nextIndex[peerID] > 1 {
					rn.nextIndex[peerID]--
				}
			}
			rn.mu.Unlock()
			for _, msg := range msgs {
				if rn.applyCh != nil {
					rn.applyCh <- msg
				}
			}
		}(i)
	}
}
