package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// coordinator.GetTask
type GetTaskArgs struct {
	ClientUUID string
}
type GetTaskReply struct {
	Tsk Task
}

// coordinator.Ping
type PingArgs struct {
	ClientUUID string
}
type PingReply struct {
	Msg string
}

// coordinator.PartFinish
type PartFinishArgs struct {
	FileIdx int
	PartIdx int
	Type    int
}
type PartFinishReply struct {
	Msg string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
