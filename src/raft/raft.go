package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"go.uber.org/zap"
)

const (
	LEADER = iota
	FOLLOWER
	CANDIDATER
)
const (
	HeartbeatInterval = 50 * time.Millisecond
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        *sync.Mutex         // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status         int
	currentTerm    int
	votedFor       int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	applyCh chan ApplyMsg

	log         []Entry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

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
	`, rf.me, rf.status, rf.currentTerm, rf.votedFor, rf.log, rf.commitIndex, rf.lastApplied)
}

func (rf *Raft) checkTerm(term int) bool {
	if term < rf.currentTerm {
		return false
	}
	if term > rf.currentTerm {
		rf.status = FOLLOWER
		rf.currentTerm = term
		rf.votedFor = -1
		rf.electionTimer.Reset(randDefaultTime())
		rf.persist()
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

// 遵守上锁原则, 仅对资源上锁, 之后的也是
func (rf *Raft) incrTermAndSetStatus(status int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++
	rf.status = status
	rf.persist()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry = make([]Entry, 0)
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		rf.Logger.Error("readPersist error")
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// 是否比传入的日志新
func (rf *Raft) logNewer(index, term int) bool {
	if rf.log[len(rf.log)-1].Term != term {
		return rf.log[len(rf.log)-1].Term > term
	}
	return len(rf.log)-1 > index
}

// RequestVote RPC
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Logger.Debug("recv RequestVote", zap.Any("args", args))

	if !rf.checkTerm(args.Term) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 && !rf.logNewer(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		rf.electionTimer.Reset(randDefaultTime())
		rf.persist()

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	rf.Logger.Sugar().Debugf("RequestVote end, Node = %v", rf.debug())
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool

	FirstSameIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Logger.Debug("AppendEntries", zap.Any("args", args))

	if !rf.checkTerm(args.Term) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.electionTimer.Reset(randDefaultTime())

	reply.Term = rf.currentTerm
	if len(rf.log) <= args.PrevLogIndex {
		reply.Success = false
		reply.FirstSameIndex = len(rf.log)
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		tmpTerm := rf.log[args.PrevLogIndex].Term
		firstSameIndex := args.PrevLogIndex
		for firstSameIndex-1 >= 0 && rf.log[firstSameIndex-1].Term == tmpTerm {
			firstSameIndex--
		}
		reply.FirstSameIndex = firstSameIndex
	} else {
		reply.Success = true

		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		rf.persist()

		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			if len(rf.log)-1 < rf.commitIndex {
				rf.commitIndex = len(rf.log) - 1
			}
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied,
				}
				rf.applyCh <- msg
				rf.Logger.Debug("apply", zap.Any("ApplyMsg", msg))
			}
			// rf.lastApplied = rf.commitIndex
		}
	}

	rf.Logger.Debug("AppendEntries process end", zap.Any("reply", reply))
	rf.Logger.Sugar().Debugf("Node = %v", rf.debug())
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Logger.Debug("Append command", zap.Any("command", command))

	if rf.status != LEADER {
		rf.Logger.Debug("not leader")
		return -1, -1, false
	}

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := true

	rf.log = append(rf.log, Entry{
		Term:    term,
		Command: command,
	})
	rf.persist()
	rf.Logger.Sugar().Debugf("append entry %v, return (%v, %v, %v)", rf.log[len(rf.log)-1], index, term, isLeader)
	// rf.BroadcastHeartbeat()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) BroadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {

			args := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderID: rf.me,

				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				Entries:      rf.log[rf.nextIndex[i]:],
				LeaderCommit: rf.commitIndex,
			}

			go func(i int, args *AppendEntriesArgs) {
				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(i, args, reply)

				rf.mu.Lock()
				rf.Logger.Debug("sendAppendEntries", zap.Any("to", i), zap.Any("args", args))

				if !rf.checkTerm(reply.Term) {
					rf.mu.Unlock()
					return
				}
				if rf.status != LEADER {
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					rf.Logger.Debug("AppendEntries success", zap.Any("id", i))
					rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[i] = rf.nextIndex[i] - 1

					if rf.matchIndex[i] > rf.commitIndex && rf.log[rf.matchIndex[i]].Term == rf.currentTerm {
						cnt := 1
						for j := range rf.peers {
							if rf.matchIndex[j] >= rf.matchIndex[i] && j != rf.me {
								cnt++
								if cnt > len(rf.peers)/2 {
									rf.commitIndex = rf.matchIndex[i]
									for rf.lastApplied < rf.commitIndex {
										rf.lastApplied++
										msg := ApplyMsg{
											CommandValid: true,
											Command:      rf.log[rf.lastApplied].Command,
											CommandIndex: rf.lastApplied,
										}
										rf.applyCh <- msg
										rf.Logger.Debug("apply", zap.Any("ApplyMsg", msg))
									}
									// rf.lastApplied = rf.commitIndex
									rf.Logger.Sugar().Debugf("update commitIndex to %v", rf.commitIndex)
									break
								}
							}
						}

					}

				} else {
					// index := reply.FirstSameIndex
					index := args.PrevLogIndex
					term := args.PrevLogTerm
					for rf.log[index].Term == term && index >= reply.FirstSameIndex {
						index--
					}
					rf.nextIndex[i] = index + 1
					rf.Logger.Sugar().Debugf("AppendEntries failed, update nextIndex[%v] = %v", i, rf.nextIndex[i])
				}
				rf.mu.Unlock()
			}(i, args)
		}
	}

	rf.electionTimer.Reset(randDefaultTime())
	rf.heartbeatTimer.Reset(HeartbeatInterval)
}

func (rf *Raft) leaderElection() {
	// self vote
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = rf.me
	rf.persist()
	cnt := 1 // cnt不需要上锁影响效率

	rf.Logger.Sugar().Debugf("self vote, voteFor = %v", rf.votedFor)

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,

		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	for i := range rf.peers {
		if i != rf.me {

			go func(i int, args *RequestVoteArgs) {

				reply := &RequestVoteReply{}
				if rf.sendRequestVote(i, args, reply) && reply.VoteGranted {

					rf.mu.Lock()
					if !rf.checkTerm(reply.Term) {
						rf.mu.Unlock()
						return
					}
					if rf.status == FOLLOWER {
						rf.mu.Unlock()
						return
					}

					rf.Logger.Sugar().Debugf("VoteGranted from = %v", i)
					cnt++
					if cnt > len(rf.peers)/2 && rf.status != LEADER {
						rf.Logger.Sugar().Debugf("<leader>, cnt = %v", cnt)
						rf.status = LEADER
						// init nextIndex and matchIndex
						length := len(rf.log)
						for i := range rf.nextIndex {
							rf.nextIndex[i] = length
							rf.matchIndex[i] = 0
						}
						go rf.BroadcastHeartbeat()
					}
					rf.mu.Unlock()

				}

			}(i, args)

		}
	}

}

// between 150 and 300 milliseconds.
func randDefaultTime() time.Duration {
	ms := 150 + (rand.Int63() % 150)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.Logger.Sugar().Debugf("ticke Node = %v", rf.debug())

		select {
		case <-rf.electionTimer.C:
			rf.Logger.Info("election timeout, start leader election")

			rf.incrTermAndSetStatus(CANDIDATER)
			rf.leaderElection()
			rf.electionTimer.Reset(randDefaultTime())

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			rf.Logger.Info("heartbeat")
			if rf.status == LEADER {
				go rf.BroadcastHeartbeat()
			}
			rf.mu.Unlock()
		}

	}
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
		mu:        &sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,

		status:         FOLLOWER,
		currentTerm:    0,
		votedFor:       -1,
		electionTimer:  time.NewTimer(randDefaultTime()),
		heartbeatTimer: time.NewTimer(HeartbeatInterval),

		applyCh: applyCh,

		log:         make([]Entry, 1), // 第一个是空的
		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.Logger = NewLogger("DEBUG", "Raft-"+strconv.Itoa(me)+".log")
	rf.Logger.Sugar().Infof("Raft init success, Node = %v", rf.debug())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
