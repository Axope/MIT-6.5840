package mr

import "sync/atomic"

const (
	MAPTASK = iota
	REDUCETASK
)

type Task struct {
	Id         uint64
	Type       int
	InputFile  []string
	OutputFile string // map中需要加上分区号
	ReduceNum  int
	FileIdx    int
	PartIdx    int
}

var (
	TaskId uint64 = 0
)

func genId() uint64 {
	atomic.AddUint64(&TaskId, 1)
	return atomic.LoadUint64(&TaskId)
}
