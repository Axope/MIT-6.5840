package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

var (
	clientUUID string
	sig        bool
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeToFile(file string, buffer []KeyValue, FileIdx int, PartIdx int, Type int) {
	// 先放置到临时文件中，再原子重命名
	f, err := os.OpenFile(file+"_tmp", os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		Logger.Error("OpenFile error", zap.Any("err", err))
		return
	}
	for _, kv := range buffer {
		fmt.Fprintf(f, "%v %v\n", kv.Key, kv.Value)
	}
	f.Close()
	os.Rename(file+"_tmp", file)

	if err := CallPartFinish(FileIdx, PartIdx, Type); err != nil {
		Logger.Error(err.Error())
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	clientUUID = uuid.New().String()
	sig = false
	defer func() {
		sig = true
	}()

	InitLogger("DEBUG", "./worker-"+clientUUID+".log")
	Logger.Info("new worker", zap.Any("uuid", clientUUID))

	for {
		tsk, err := CallGetTask()
		if err != nil {
			Logger.Error(err.Error())
			time.Sleep(time.Second)
			continue
		}

		if tsk.Type == MAPTASK {
			Logger.Info("MAP TASK", zap.Any("task", tsk))

			// 这里本应该是从GFS中拉数据
			content, err := os.ReadFile(tsk.InputFile[0])
			if err != nil {
				Logger.Error("read file error", zap.Any("err", err))
				return
			}

			intermediateKVs := mapf(tsk.InputFile[0], string(content))

			reduceNum := tsk.ReduceNum
			parts := make([][]KeyValue, reduceNum)
			for _, kv := range intermediateKVs {
				idx := ihash(kv.Key) % reduceNum
				parts[idx] = append(parts[idx], kv)
			}

			for i := 0; i < reduceNum; i++ {
				ofile := tsk.OutputFile + strconv.Itoa(i)
				// writeToFile(ofile, parts[i], tsk.FileIdx, i, tsk.Type)
				go writeToFile(ofile, parts[i], tsk.FileIdx, i, tsk.Type)
			}

		} else if tsk.Type == REDUCETASK {
			Logger.Info("REDUCE TASK", zap.Any("task", tsk))

			// 如果数据比较大还需要进行外部排序
			data := []KeyValue{}
			for _, filename := range tsk.InputFile {
				f, err := os.Open(filename)
				if err != nil {
					Logger.Error(err.Error())
					return
				}
				defer f.Close()
				scanner := bufio.NewScanner(f)
				for scanner.Scan() {
					line := scanner.Text()
					words := strings.Fields(line)
					if len(words) == 2 {
						data = append(data, KeyValue{Key: words[0], Value: words[1]})
					}
				}
			}
			sort.Sort(ByKey(data))

			oname := tsk.OutputFile
			ofile, _ := os.Create(oname + "_tmp")

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(data) {
				j := i + 1
				for j < len(data) && data[j].Key == data[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, data[k].Value)
				}
				output := reducef(data[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", data[i].Key, output)

				i = j
			}
			ofile.Close()
			os.Rename(oname+"_tmp", oname)

			if err := CallPartFinish(tsk.FileIdx, tsk.PartIdx, tsk.Type); err != nil {
				Logger.Error(err.Error())
			}

		} else {
			Logger.Sugar().Errorf("task type error", zap.Any("task", tsk))
		}

	}
}

func pingRoutine() {
	for {
		if sig {
			sig = false
			return
		}
		msg := CallPing()
		if msg != "pong" {
			sig = true
			continue
		}
		time.Sleep(time.Second * 2)
	}
}

func CallPartFinish(FileIdx int, PartIdx int, Type int) error {
	args := PartFinishArgs{
		FileIdx: FileIdx,
		PartIdx: PartIdx,
		Type:    Type,
	}
	reply := PartFinishReply{}

	ok := call("Coordinator.PartFinish", &args, &reply)
	if ok {
		Logger.Sugar().Infof("CallPartFinish(%v, %v)", FileIdx, PartIdx)
		return nil
	}
	Logger.Sugar().Errorf("CallPartFinish(%v, %v) error", FileIdx, PartIdx)
	return errors.New(reply.Msg)
}

func CallGetTask() (Task, error) {
	args := GetTaskArgs{
		ClientUUID: clientUUID,
	}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		go pingRoutine()
		Logger.Sugar().Infof("client(%v) get task: %v", clientUUID, reply.Tsk)
		return reply.Tsk, nil
	}
	return Task{}, errors.New("call GetTask error")
}
func CallPing() string {
	args := PingArgs{
		ClientUUID: clientUUID,
	}
	reply := PingReply{}

	ok := call("Coordinator.Ping", &args, &reply)
	if ok {
		return reply.Msg
	}
	return ""
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		Logger.Error("dialing:", zap.Any("err", err))
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	Logger.Error("RPC call error",
		zap.Any("err", err),
		zap.Any("rpcname", rpcname),
		zap.Any("args", args),
		zap.Any("reply", reply),
	)
	return false
}
