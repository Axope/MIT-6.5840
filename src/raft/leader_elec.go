package raft

import "go.uber.org/zap"

// 是否比传参新
func (rf *Raft) logNewer(index, term int) bool {
	lastLog := rf.log.getLastLog()
	if lastLog.Term != term {
		return lastLog.Term > term
	}
	return lastLog.Index > index
}

func (rf *Raft) leaderElection() {
	defer rf.Logger.Sync()
	rf.Logger.Info("start leader election")

	// rf.status = CANDIDATER
	rf.changeState(CANDIDATER)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist(nil)

	elector := 1
	lastLog := rf.log.getLastLog()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(i, args, reply) {
				return
			}

			// handle RequestVote RPC
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !rf.checkTerm(reply.Term) {
				return
			}
			if rf.status != CANDIDATER {
				return
			}
			// expire request
			if rf.currentTerm != args.Term {
				return
			}

			if reply.VoteGranted {
				rf.Logger.Sugar().Debugf("VoteGranted from = %v", i)
				elector++
				if elector > len(rf.peers)/2 && rf.status != LEADER {
					rf.Logger.Sugar().Debugf("<leader>, elector = %v", elector)
					// rf.status = LEADER
					rf.changeState(LEADER)
					// init nextIndex and matchIndex
					length := rf.log.getLastLog().Index + 1
					for i := range rf.nextIndex {
						rf.nextIndex[i] = length
						rf.matchIndex[i] = 0
					}
					rf.BroadcastHeartbeat(true)
				}
			}

		}(i)
	}

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.Logger.Sync()

	rf.Logger.Debug("recv RequestVote", zap.Any("args", args))
	defer rf.Logger.Sugar().Debugf("RequestVote end, Node = %v", rf.debug())

	if !rf.checkTerm(args.Term) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.Logger.Debug("checkTerm exit")
		return
	}

	if rf.votedFor == -1 && !rf.logNewer(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateID
		rf.persist(nil)

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		// 必须要投票成功才能重置选举超时定时器
		// rf.electionTimer.Reset(randElectionTime())
		rf.resetElectionTimer()
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}

}
