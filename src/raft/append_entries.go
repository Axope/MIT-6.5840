package raft

import (
	"go.uber.org/zap"
)

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.Logger.Sync()
	rf.Logger.Debug("AppendEntries", zap.Any("args", args))
	defer rf.Logger.Sugar().Debugf("AppendEntries end, Node = %v", rf.debug())

	if !rf.checkTerm(args.Term) {
		reply.Term = rf.currentTerm
		reply.Success = false
		// reply.FirstSameIndex = args.PrevLogIndex + 1
		rf.Logger.Debug("checkTerm exit")
		return
	}
	// rf.electionTimer.Reset(randElectionTime())
	rf.resetElectionTimer()

	if args.Empty {
		rf.Logger.Debug("empty package")
		return
	}

	if args.PrevLogIndex < rf.log.getFirstLog().Index {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Term = rf.currentTerm
	if args.PrevLogIndex > rf.log.getLastLog().Index {
		reply.Success = false
		reply.FirstConflictIndex = rf.log.getLastLog().Index + 1
		reply.ConflictTerm = -1
		rf.Logger.Sugar().Debugf("args.PrevLogIndex(%v) > rf.log.LastLog(%v), reply = %v",
			args.PrevLogIndex, rf.log.getLastLog().Index, reply)
	} else if rf.log.getLogEntryByIndex(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm, reply.FirstConflictIndex = rf.log.findFirstConflict(args.PrevLogIndex)
	} else {
		reply.Success = true

		// 删除冲突日志 追加新条目
		for i := range args.Entries {
			entryIndex := args.PrevLogIndex + 1 + i
			if entryIndex > rf.log.getLastLog().Index ||
				rf.log.getLogEntryByIndex(entryIndex).Term != args.Entries[i].Term {

				e := rf.log.getPrefixEntries(entryIndex - 1)
				e = append(e, args.Entries[i:]...)

				rf.log = LogEntries{Entries: e}
				rf.persist(nil)
				break
			}
		}

		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			rf.applyCond.Signal()
		}

	}
}

func (rf *Raft) updateCommitIndex(index int) {
	defer rf.Logger.Sync()

	if index <= rf.commitIndex {
		return
	}
	// 只能提交自己任期下的日志
	if rf.log.getLogEntryByIndex(index).Term != rf.currentTerm {
		return
	}
	// update
	cnt := 1
	for i := range rf.peers {
		if rf.matchIndex[i] >= index && i != rf.me {
			cnt++
			if cnt > len(rf.peers)/2 {
				rf.commitIndex = index
				rf.Logger.Sugar().Debugf("update commitIndex to %v", rf.commitIndex)
				rf.applyCond.Signal()
				return
			}
		}
	}

}

func (rf *Raft) sendAppendEntriesToPeer(i int, args *AppendEntriesArgs) {

	reply := &AppendEntriesReply{}
	if !rf.sendAppendEntries(i, args, reply) {
		return
	}

	// handle RequestVote RPC
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.Logger.Sync()
	rf.Logger.Debug("sendAppendEntriesToPeer", zap.Any("to", i),
		zap.Any("args", args), zap.Any("reply", reply))

	if !rf.checkTerm(reply.Term) {
		return
	}
	if rf.status != LEADER {
		return
	}
	// expire request
	if rf.currentTerm != args.Term {
		return
	}

	if reply.Success {
		if rf.nextIndex[i] < args.PrevLogIndex+len(args.Entries)+1 {
			rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[i] = rf.nextIndex[i] - 1
			rf.updateCommitIndex(rf.matchIndex[i])
		}

	} else {
		// rf.nextIndex[i]--

		// rf.nextIndex[i] = rf.log.findFirstSameIndex(args.PrevLogIndex, reply.FirstSameIndex)

		// if args.PrevLogIndex <= rf.log.getLastLog().Index {
		// 	rf.nextIndex[i] = rf.log.findFirstSameIndex(args.PrevLogIndex, reply.FirstSameIndex)
		// }

		newNextIndex := reply.FirstConflictIndex
		for i := rf.log.getLastLog().Index; i > rf.log.getFirstLog().Index; i-- {
			if term := rf.log.getLogEntryByIndex(i).Term; term == reply.ConflictTerm {
				newNextIndex = i
				break
			}
		}
		rf.nextIndex[i] = newNextIndex

		rf.Logger.Sugar().Debugf("AppendEntries failed, update nextIndex[%v] = %v", i, rf.nextIndex[i])
	}

}
