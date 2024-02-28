package raft

import "go.uber.org/zap"

func (rf *Raft) updateCommitIndexBySnapshot(index int) {
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.Logger.Sync()

	rf.Logger.Debug("InstallSnapshot", zap.Any("args", args))
	defer rf.Logger.Sugar().Debugf("InstallSnapshot end, Node = %v", rf.debug())

	if !rf.checkTerm(args.Term) {
		reply.Term = rf.currentTerm
		return
	}
	// rf.electionTimer.Reset(randElectionTime())
	rf.resetElectionTimer()

	reply.Term = rf.currentTerm
	// reject outdated(duplicated) snapshot
	if args.LastIncludedIndex <= rf.commitIndex {
		rf.Logger.Sugar().Debugf("args.LastIncludedIndex(%v) <= rf.snapshotIndex(%v)",
			args.LastIncludedIndex, rf.log.getFirstLog())
		return
	}

	// 日志中包含LastIncludedLog
	if args.LastIncludedIndex <= rf.log.getLastLog().Index &&
		rf.log.getLogEntryByIndex(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		// 保留后续日志
		rf.log = LogEntries{
			Entries: rf.log.getSuffixEntries(args.LastIncludedIndex),
		}

	} else {
		// 丢弃整个日志(rf.log[0].Command 不可用)
		rf.log = NewLogEntries()
		rf.log.setFirstLog(Entry{
			Index: args.LastIncludedIndex,
			Term:  args.LastIncludedTerm,
		})
	}

	rf.updateCommitIndexBySnapshot(args.LastIncludedIndex)
	rf.persist(args.Data)

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendInstallSnapshotToPeer(i int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	if !rf.sendInstallSnapshot(i, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.checkTerm(reply.Term) {
		return
	}
	if rf.status != LEADER {
		return
	}
	if rf.currentTerm != args.Term {
		return
	}

	rf.nextIndex[i] = args.LastIncludedIndex + 1
	rf.matchIndex[i] = args.LastIncludedIndex

}
