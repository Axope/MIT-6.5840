package kvraft

import (
	// "fmt"

	"6.5840/labrpc"
	"github.com/google/uuid"
	// "go.uber.org/zap"
)

// clerk等到上一条指令返回之后才能处理下一条指令
type Clerk struct {
	servers []*labrpc.ClientEnd

	lastLeader int
	commandID  int
	ClientUUID string

	// Logger *zap.Logger
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {

	ck := &Clerk{
		servers: servers,

		lastLeader: 0,
		commandID:  0,
		ClientUUID: uuid.New().String(),
	}
	// ck.Logger = NewLogger("DEBUG", fmt.Sprintf("client-%v.log", ck.ClientUUID))
	return ck
}

func (ck *Clerk) Operate(key string, value string, opt string) string {
	// defer ck.Logger.Sync()
	// ck.Logger.Sugar().Infof("%v(%v, %v)", opt, key, value)

	ck.commandID++
	args := &OperateArgs{
		Key:   key,
		Value: value,
		Opt:   opt,

		ClientUUID: ck.ClientUUID,
		CommandID:  ck.commandID,
	}

	for {
		reply := &OperateReply{}
		// ck.Logger.Sugar().Debugf("sendOperate(%v, %v)", ck.lastLeader, args)

		ok := ck.sendOperate(ck.lastLeader, args, reply)
		if ok && reply.Err == OK {
			// ck.Logger.Sugar().Infof("ok, return %v", reply.Value)
			return reply.Value
		} else {
			// ck.Logger.Sugar().Debug(reply)
			ck.lastLeader++
			ck.lastLeader %= len(ck.servers)
		}

	}
}

func (ck *Clerk) Get(key string) string {
	return ck.Operate(key, "", OptGet)
}

func (ck *Clerk) Put(key string, value string) {
	ck.Operate(key, value, OptPut)
}

func (ck *Clerk) Append(key string, value string) {
	ck.Operate(key, value, OptAppend)
}
