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
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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

	Logger *zap.Logger
}

func (rf *Raft) debug() string {
	return fmt.Sprintf(`
		me = %v,
		status = %v,
		currentTerm = %v,
		votedFor = %v
	`, rf.me, rf.status, rf.currentTerm, rf.votedFor)
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
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
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
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
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

// RequestVote RPC
type RequestVoteArgs struct {
	Term        int
	CandidateId int
}
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			rf.electionTimer.Reset(randDefaultTime())

			reply.Term = rf.currentTerm
			reply.VoteGranted = true
		}
	} else {
		rf.status = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.electionTimer.Reset(randDefaultTime())

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}
type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	} else if args.Term == rf.currentTerm {
		rf.electionTimer.Reset(randDefaultTime())

		reply.Term = rf.currentTerm
	} else {
		rf.status = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.electionTimer.Reset(randDefaultTime())

		reply.Term = rf.currentTerm
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				args := &AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderID: rf.me,
				}
				reply := &AppendEntriesReply{}
				rf.sendAppendEntries(i, args, reply)
			}(i)
		}
	}

	rf.electionTimer.Reset(randDefaultTime())
	rf.heartbeatTimer.Reset(HeartbeatInterval)
}

func (rf *Raft) leaderElection() {
	// self vote
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.mu.Unlock()
	rf.Logger.Sugar().Debugf("self vote, voteFor = %v", rf.votedFor)
	var cnt int = 1 // cnt不需要上锁影响效率
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				args := &RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: i,
				}
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(i, args, reply) && reply.VoteGranted {
					rf.Logger.Sugar().Debugf("VoteGranted from = %v", i)
					cnt++
					if cnt > len(rf.peers)/2 {
						rf.Logger.Sugar().Debugf("<leader>, cnt = %v", cnt)

						rf.mu.Lock()
						if rf.status != LEADER {
							rf.status = LEADER
						}
						rf.mu.Unlock()

						rf.BroadcastHeartbeat()
					}
				}

			}(i)
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
			rf.Logger.Info("heartbeat")

			if _, isLeader := rf.GetState(); isLeader {
				rf.BroadcastHeartbeat()
			}
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
