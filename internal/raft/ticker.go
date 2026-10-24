package raft

import (
	"sync"
	"time"
)

// Start begins the election and heartbeat background goroutine.
func (rn *RaftNode) StartBackground() error {
	rn.mu.Lock()

	if rn.started {
		rn.mu.Unlock()
		return ErrAlreadyStarted
	}

	if !rn.hasValidPeers(rn.peers) {
		rn.mu.Unlock()
		return ErrInvalidPeers
	}

	select {
	case <-rn.stopCh:
		rn.stopped = true
		rn.mu.Unlock()
		return ErrNodeStopped
	default:
	}

	rn.started = true
	rn.wg.Add(2)
	rn.mu.Unlock()

	go func() {
		defer rn.wg.Done()
		rn.runLoop()
	}()

	go func() {
		defer rn.wg.Done()
		rn.applyLoop()
	}()

	return nil
}

// Stop cleanly terminates the node's loop.
func (rn *RaftNode) Stop() {
	rn.stopOnce.Do(func() {
		rn.mu.Lock()
		rn.stopped = true
		rn.mu.Unlock()

		close(rn.stopCh)
	})

	rn.wg.Wait()
}

func (rn *RaftNode) runLoop() {
	ticker := time.NewTicker(20 * time.Millisecond) //produces a tick every 20 milliseconds.
	defer ticker.Stop()                             //ensures the ticker is stopped when runLoop() returns.

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
	rn.currentTerm = newTerm
	rn.votedFor = newVotedFor
	rn.resetElectionTimeout()

	term := rn.currentTerm
	me := rn.me
	peers := append([]*RaftNode(nil), rn.peers...)
	lastLogIndex := len(rn.log) - 1
	lastLogTerm := rn.log[lastLogIndex].Term

	rn.mu.Unlock()

	println("Node", me, "started election for term", term)
	var votesMu sync.Mutex
	votesReceived := 1 // Vote for self

	// A single-node cluster already has a majority.
	if votesReceived > rn.peersCount/2 {
		rn.mu.Lock()

		if rn.role == Candidate && rn.currentTerm == term {
			println("Node", me, "BECAME LEADER for term", term)

			index := len(rn.log)
			noop := LogEntry{
				Index: index,
				Term:  term,
				Command: Command{
					Op: "NOOP",
				},
			}

			newLog := make([]LogEntry, len(rn.log)+1)
			copy(newLog, rn.log)
			newLog[index] = noop

			if err := rn.persistState(
				rn.currentTerm,
				rn.votedFor,
				newLog,
			); err != nil {
				rn.mu.Unlock()
				return
			}
			rn.role = Leader
			rn.log = newLog
			rn.rebuildRequestIndex()
			rn.commitIndex = index
			rn.matchIndex[rn.me] = index
			rn.nextIndex[rn.me] = index + 1

			msgs := rn.collectCommittedEntries()

			rn.mu.Unlock()

			for _, msg := range msgs {
				if rn.applyCh == nil {
					continue
				}

				select {
				case rn.applyCh <- msg:
				case <-rn.stopCh:
					return
				}
			}
			go rn.broadcastAppendEntries()
		} else {
			rn.mu.Unlock()
		}

		return
	}

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

			if reply.Term > rn.currentTerm {
				if err := rn.persistState(
					reply.Term,
					-1,
					rn.log,
				); err != nil {
					// Do not publish the higher term if persistence fails.
					return
				}

				rn.currentTerm = reply.Term
				rn.role = Follower
				rn.votedFor = -1
				rn.resetElectionTimeout()
				return
			}

			// Ignore stale replies from an election we no longer belong to.
			if rn.role != Candidate || rn.currentTerm != term {
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

					index := len(rn.log)
					noop := LogEntry{
						Index: index,
						Term:  term,
						Command: Command{
							Op: "NOOP",
						},
					}

					newLog := make([]LogEntry, len(rn.log)+1)
					copy(newLog, rn.log)
					newLog[index] = noop

					if err := rn.persistState(
						rn.currentTerm,
						rn.votedFor,
						newLog,
					); err != nil {
						return
					}
					rn.role = Leader
					rn.log = newLog
					rn.rebuildRequestIndex()
					rn.matchIndex[rn.me] = index
					rn.nextIndex[rn.me] = index + 1

					for j := range rn.peers {
						if j != rn.me {
							rn.nextIndex[j] = index
							rn.matchIndex[j] = 0
						}
					}

					go rn.broadcastAppendEntries()
					return
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
	peers := append([]*RaftNode(nil), rn.peers...)
	commitIndex := rn.commitIndex
	rn.mu.Unlock()

	for i := range peers {
		if i == me {
			continue
		}

		go func(peerID int) {
			rn.mu.Lock()
			sentNextIndex := rn.nextIndex[peerID]
			if rn.role != Leader || rn.currentTerm != term {
				rn.mu.Unlock()
				return
			}
			if sentNextIndex < 1 {
				sentNextIndex = 1
			}
			if sentNextIndex > len(rn.log) {
				sentNextIndex = len(rn.log)
			}

			rn.nextIndex[peerID] = sentNextIndex

			prevLogIndex := sentNextIndex - 1
			prevLogTerm := rn.log[prevLogIndex].Term

			var entries []LogEntry
			if sentNextIndex < len(rn.log) {
				entries = make([]LogEntry, len(rn.log)-sentNextIndex)
				copy(entries, rn.log[sentNextIndex:])
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
				if err := rn.persistState(reply.Term, -1, rn.log); err != nil {
					// Persistence failed; don't publish the state change.
					rn.mu.Unlock()
					return
				}
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
			} else if rn.nextIndex[peerID] == sentNextIndex {
				if rn.nextIndex[peerID] > 1 {
					rn.nextIndex[peerID]--
				}
			}
			rn.mu.Unlock()
			for _, msg := range msgs {
				if rn.applyCh == nil {
					continue
				}

				select {
				case rn.applyCh <- msg:
				case <-rn.stopCh:
					return
				}
			}
		}(i)
	}
}
