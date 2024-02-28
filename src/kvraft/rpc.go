package kvraft

type OperateArgs struct {
	Key   string
	Value string
	Opt   string

	ClientUUID string
	CommandID  int
}

type OperateReply struct {
	Err   Err
	Value string
}

func (ck *Clerk) sendOperate(server int, args *OperateArgs, reply *OperateReply) bool {
	ok := ck.servers[server].Call("KVServer.Operate", args, reply)
	return ok
}
