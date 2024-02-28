package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"go.uber.org/zap"
)

type Op struct {
	Key   string
	Value string
	Opt   string

	ClientUUID string
	CommandID  int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	stateMechine   StateMechine
	session        Session
	responseChans  ResponseChans
	lastApplyIndex int

	Logger *zap.Logger
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := &KVServer{
		mu:      sync.Mutex{},
		me:      me,
		applyCh: make(chan raft.ApplyMsg),
		dead:    0,

		maxraftstate: maxraftstate,

		stateMechine:   NewStateMechine(),
		session:        NewSession(),
		responseChans:  NewResponseChans(),
		lastApplyIndex: 0,

		Logger: NewLogger("DEBUG", fmt.Sprintf("server-%v.log", me)),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// kv.InstallSnapshot(persister.ReadSnapshot(), kv.rf.GetSnapshotLastIncludedIndex())
	kv.InstallSnapshot(persister.ReadSnapshot(), 0)

	go kv.applier()

	return kv
}

func (kv *KVServer) Operate(args *OperateArgs, reply *OperateReply) {
	kv.mu.Lock()
	defer kv.Logger.Sync()
	kv.Logger.Debug("Operate", zap.Any("args", args))

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.Logger.Info("not leader")
		kv.mu.Unlock()
		return
	}

	// 过滤已经执行过的操作
	res := kv.session.getSessionResult(args.ClientUUID)
	if res.LastCommandId == args.CommandID && res.Err == OK {
		reply.Err = OK
		reply.Value = res.Value
		kv.Logger.Info("old result", zap.Any("reply", reply))
		kv.mu.Unlock()
		return
	}

	op := Op{
		Key:        args.Key,
		Value:      args.Value,
		Opt:        args.Opt,
		ClientUUID: args.ClientUUID,
		CommandID:  args.CommandID,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.Logger.Info("not leader")
		kv.mu.Unlock()
		return
	}

	ch := kv.responseChans.getResponseChan(index)
	kv.mu.Unlock()
	select {
	case res := <-ch:
		reply.Err = res.Err
		reply.Value = res.Value

	case <-time.After(RaftTimeout):
		reply.Err = ErrTimeout
	}
	kv.Logger.Debug("return reply", zap.Any("reply", reply))

}

func (kv *KVServer) doApplyWork(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.Logger.Sync()
	kv.Logger.Debug("doApplyWork", zap.Any("msg", msg))

	op, _ := msg.Command.(Op)
	// 旧的日志消息
	if msg.CommandIndex <= kv.lastApplyIndex {
		return
	}
	kv.lastApplyIndex = msg.CommandIndex

	// 已有结果，直接返回即可
	res := kv.session.getSessionResult(op.ClientUUID)
	if res.LastCommandId >= op.CommandID {
		return
	}

	switch op.Opt {
	case OptGet:
		kv.session.setSessionResult(op.ClientUUID, sessionResult{
			LastCommandId: op.CommandID,
			Value:         kv.stateMechine.Get(op.Key),
			Err:           OK,
		})
	case OptPut:
		kv.stateMechine.Put(op.Key, op.Value)
		kv.session.setSessionResult(op.ClientUUID, sessionResult{
			LastCommandId: op.CommandID,
			Value:         op.Value,
			Err:           OK,
		})
	case OptAppend:
		kv.stateMechine.Append(op.Key, op.Value)
		kv.session.setSessionResult(op.ClientUUID, sessionResult{
			LastCommandId: op.CommandID,
			Value:         kv.stateMechine.Get(op.Key),
			Err:           OK,
		})
	}

	if _, isLeader := kv.rf.GetState(); isLeader {
		ch := kv.responseChans.getResponseChan(msg.CommandIndex)
		ch <- OperateReply{
			Err:   OK,
			Value: kv.stateMechine.Get(op.Key),
		}
	}

	if kv.needSnapshot() {
		kv.Logger.Debug("takeSnapshot", zap.Any("index", msg.Command))
		kv.takeSnapshot(msg.CommandIndex)
	}
}

func (kv *KVServer) doSnapshotWork(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.Logger.Sync()
	kv.Logger.Debug("doSnapshotWork", zap.Any("msg", msg))

	if msg.SnapshotIndex >= kv.lastApplyIndex {
		kv.InstallSnapshot(msg.Snapshot, msg.SnapshotIndex)
	}
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.CommandValid {
			kv.doApplyWork(msg)
		} else if msg.SnapshotValid {
			kv.doSnapshotWork(msg)
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}
func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
