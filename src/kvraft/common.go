package kvraft

import "time"

const (
	OptGet    = "Get"
	OptPut    = "Put"
	OptAppend = "Append"
)

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

const RaftTimeout = time.Second

type Err string
