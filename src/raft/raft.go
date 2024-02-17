package raft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"go.uber.org/zap"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	status int

	currentTerm int
	votedFor    int
	log         LogEntries

	nextIndex   []int
	matchIndex  []int
	commitIndex int
	lastApplied int
	applyCond   *sync.Cond
	applyCh     chan ApplyMsg

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	Logger *zap.Logger
}

func (rf *Raft) debug() string {
	return fmt.Sprintf(`
		me = %v,
		status = %v,
		currentTerm = %v,
		votedFor = %v,

		log = %v,
		commitIndex = %v,
		lastApplied = %v
	`, rf.me, rf.status, rf.currentTerm, rf.votedFor, rf.log.Entries, rf.commitIndex, rf.lastApplied)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		mu:        sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,

		status: FOLLOWER,

		currentTerm: 0,
		votedFor:    -1,
		log:         NewLogEntries(),

		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		commitIndex: 0,
		lastApplied: 0,

		applyCh: applyCh,

		electionTimer:  time.NewTimer(randElectionTime()),
		heartbeatTimer: time.NewTimer(HeartbeatTime),

		Logger: NewLogger("DEBUG", fmt.Sprintf("Raft-%v.log", me)),
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.applier()

	return rf
}

func (rf *Raft) changeState(state int) {
	if rf.status == LEADER && state != LEADER {
		rf.electionTimer.Reset(randElectionTime())
	}
	rf.status = state
}

func (rf *Raft) checkTerm(term int) bool {
	// reject
	if term < rf.currentTerm {
		return false
	}
	// change to Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		// rf.status = FOLLOWER
		rf.changeState(FOLLOWER)
		rf.votedFor = -1

		rf.persist(nil)
	}

	return true
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.status == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log LogEntries
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		rf.Logger.Error("readPersist error")
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log

		rf.commitIndex = rf.log.getFirstLog().Index
		rf.lastApplied = rf.commitIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reject outdated(duplicated) snapshot
	if index <= rf.log.getFirstLog().Index {
		rf.Logger.Sugar().Debugf("index(%v) <= rf.snapshotIndex(%v)",
			index, rf.log.getFirstLog())
		return
	}

	// 日志中包含LastIncludedLog
	if index <= rf.log.getLastLog().Index {
		// 保留后续日志
		rf.log = LogEntries{
			Entries: rf.log.getSuffixEntries(index),
		}

		rf.updateCommitIndexBySnapshot(index)
		rf.persist(snapshot)
	} else {
		panic(fmt.Sprintf("Snapshot: index(%v) > rf.LastLogIndex(%v)",
			index, rf.log.getLastLog().Index))
	}

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Logger.Debug("Start()", zap.Any("command", command))

	if rf.status != LEADER {
		rf.Logger.Debug("not leader")
		return -1, -1, false
	}

	lastEntry := rf.log.getLastLog()
	e := Entry{
		Index:   lastEntry.Index + 1,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log.append([]Entry{e})
	rf.Logger.Sugar().Debugf("accept command, Node = %v", rf.debug())
	rf.persist(nil)

	return lastEntry.Index + 1, rf.currentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) needInstallSnapshot(checkIndex int) bool {
	return checkIndex < rf.log.getFirstLog().Index
}

func (rf *Raft) BroadcastHeartbeat(emptyPackage bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != LEADER {
		return
	}
	defer rf.heartbeatTimer.Reset(HeartbeatTime)

	// empty request
	if emptyPackage {
		// make empty package
		args := &AppendEntriesArgs{
			Empty:    true,
			Term:     rf.currentTerm,
			LeaderID: rf.me,
		}
		// parallel transmission
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
		return
	}

	// parallel transmission
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// check if installSnapshot needs to be sent.
		if rf.needInstallSnapshot(rf.nextIndex[i] - 1) {
			snapshotLastLog := rf.log.getFirstLog()
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderID:          rf.me,
				LastIncludedIndex: snapshotLastLog.Index,
				LastIncludedTerm:  snapshotLastLog.Term,
				Data:              rf.persister.ReadSnapshot(),
			}
			go rf.sendInstallSnapshotToPeer(i, args)
			continue
		}

		prevLog := rf.log.getLogEntryByIndex(rf.nextIndex[i] - 1)
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLog.Index,
			PrevLogTerm:  prevLog.Term,
			Entries:      rf.log.getSuffixEntries(rf.nextIndex[i]),
			LeaderCommit: rf.commitIndex,
		}
		go rf.sendAppendEntriesToPeer(i, args)
	}

}

func (rf *Raft) ticker() {
	for !rf.killed() {

		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.status != LEADER {
				rf.leaderElection()
				rf.electionTimer.Reset(randElectionTime())
			}
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.status == LEADER {
				go rf.BroadcastHeartbeat(false)
			}
			rf.mu.Unlock()

		}

	}
}

func (rf *Raft) applier() {
	for !rf.killed() {

		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		// [lastApplied+1, commitIndex]
		entries := rf.log.getRangeEntries(rf.lastApplied+1, rf.commitIndex)
		rf.mu.Unlock()

		for i := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entries[i].Command,
				CommandIndex: entries[i].Index,
			}
		}

		rf.mu.Lock()
		lastIndex := entries[len(entries)-1].Index
		if lastIndex > rf.lastApplied {
			rf.lastApplied = lastIndex
		}
		rf.mu.Unlock()

	}
}
