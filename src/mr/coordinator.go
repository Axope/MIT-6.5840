package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"

	"go.uber.org/zap"
)

type fileIdMap struct {
	mp  map[string]int
	rmp []string
}

var fim *fileIdMap

type Coordinator struct {
	tq         *TaskQueue
	tm         *Timer
	ready      [][]bool
	partFinish []bool
}

// GetTask -- RPC
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	clientUUID := args.ClientUUID
	if c.tq.Empty() {
		Logger.Warn("GetTask: no task")
		return errors.New("no task")
	}
	task, err := c.tq.GetFrontAndPop()
	if err != nil {
		Logger.Error("tq.GetFrontAndPop()", zap.Any("err", err))
		return err
	}
	reply.Tsk = task
	c.tm.HandleClient(clientUUID, task, func(task Task) {
		Logger.Sugar().Infof("clientUUID(%v) is close", clientUUID)
		if task.Type == MAPTASK {
			fileIdx := task.FileIdx
			for _, v := range c.ready[fileIdx] {
				if !v {
					c.tq.Push(task)
					Logger.Sugar().Infof("worker crash, task(%v) reenter queue", task)
					return
				}
			}
		} else if task.Type == REDUCETASK {
			partIdx := task.PartIdx
			if !c.partFinish[partIdx] {
				c.tq.Push(task)
				Logger.Sugar().Infof("worker crash, task(%v) reenter queue", task)
				return
			}
		}
	})
	Logger.Sugar().Infof("client(%v) get task(%v)", clientUUID, task)
	return nil
}

// Ping -- RPC
func (c *Coordinator) Ping(args *PingArgs, reply *PingReply) error {
	err := c.tm.ResetTimer(args.ClientUUID)
	if err != nil {
		reply.Msg = err.Error()
		return err
	}
	reply.Msg = "pong"
	return nil
}

func checkPartFull(c *Coordinator, partIdx int) {
	ifiles := make([]string, 0)
	for i := range c.ready {
		if !c.ready[i][partIdx] {
			return
		}
		ifiles = append(ifiles, "mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(partIdx))
	}

	tsk := Task{
		Id:         genId(),
		Type:       REDUCETASK,
		InputFile:  ifiles,
		OutputFile: "mr-out-" + strconv.Itoa(partIdx),
		ReduceNum:  -1,
		FileIdx:    -1,
		PartIdx:    partIdx,
	}
	c.tq.Push(tsk)
	Logger.Sugar().Infof("add task %v", tsk)
}

// PartFinish -- RPC
func (c *Coordinator) PartFinish(args *PartFinishArgs, reply *PartFinishReply) error {
	fileIdx := args.FileIdx
	partIdx := args.PartIdx
	tp := args.Type
	if tp == MAPTASK {
		if fileIdx >= len(c.ready) {
			return fmt.Errorf("array bound %v >= len(ready)=[%v]", fileIdx, len(c.ready))
		}
		if partIdx >= len(c.ready[fileIdx]) {
			return fmt.Errorf("array bound %v >= len(ready[%v])=[%v]", partIdx, fileIdx, len(c.ready[fileIdx]))
		}
		if c.ready[fileIdx][partIdx] {
			return errors.New("task has been performed")
		}
		c.ready[fileIdx][partIdx] = true
		checkPartFull(c, partIdx)
		reply.Msg = "ok"
		return nil
	} else if tp == REDUCETASK {
		c.partFinish[partIdx] = true
		reply.Msg = "ok"
		return nil
	} else {
		reply.Msg = "type error"
		return errors.New("type error")
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _, v := range c.partFinish {
		if !v {
			return false
		}
	}

	return true
}

func makeTasks(c *Coordinator, files []string, nReduce int) {
	for _, filename := range files {
		t := Task{
			Id:         genId(),
			Type:       MAPTASK,
			InputFile:  []string{filename},
			OutputFile: "mr-" + strconv.Itoa(fim.mp[filename]) + "-",
			ReduceNum:  nReduce,
			FileIdx:    fim.mp[filename],
			PartIdx:    -1,
		}
		c.tq.Push(t)
		Logger.Sugar().Debugf("new task: %v", t)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	InitLogger("DEBUG", "./mr.log")
	// init
	c := &Coordinator{
		tq:         NewQueue(),
		tm:         NewTimer(10),
		ready:      make([][]bool, len(files)),
		partFinish: make([]bool, nReduce),
	}
	for i := range c.ready {
		c.ready[i] = make([]bool, nReduce)
	}
	fim = &fileIdMap{
		mp:  make(map[string]int),
		rmp: make([]string, len(files)),
	}

	for i, file := range files {
		fim.mp[file] = i
		fim.rmp[i] = file
	}

	// make tasks
	makeTasks(c, files, nReduce)

	c.server()
	return c
}
