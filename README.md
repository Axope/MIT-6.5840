# README

完成了 MIT6.5840（原6.824）Spring 2023 课程的 Lab1 ~ Lab3。在这里分享一下我的思路和记录一些踩过的坑，希望能帮到其他同学。



## Lab1

小试牛刀，MapReduce 词频统计。

### 1.1 整体流程

一个 coordinator 协程和多个 worker 协程，coordinator 负责任务调度，worker 负责处理任务并把结果通知 coordinator。

worker 通过 RPC 从 coordinator 获取 map 任务，生成中间结果并通知 coordinator，coordinator 检查对应的 part 是否都处理完成，是的话就往队列里添加一个 reduce 任务，等到所有任务都处理完之后结束。

### 1.2 coordinator

coordinator 维护一个 task 队列，用于给 worker 分配 task，以及当 worker 完成 map 任务的时候，生成新的 reduce 任务。

```go
// GetTask -- RPC
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
    clientUUID := args.ClientUUID
    if c.tq.Empty() {
        return errors.New("no task")
    }
    task, err := c.tq.GetFrontAndPop()
    if err != nil {
        return err
    }
    reply.Tsk = task
    c.tm.HandleClient(clientUUID, task, func(task Task) {
        if task.Type == MAPTASK {
            fileIdx := task.FileIdx
            for _, v := range c.ready[fileIdx] {
                if !v {
                    c.tq.Push(task)
                    return
                }
            }
        } else if task.Type == REDUCETASK {
            partIdx := task.PartIdx
            if !c.partFinish[partIdx] {
                c.tq.Push(task)
                return
            }
        }
    })
    return nil
}
```

检查中间结果并生成 reduce 任务：

```go
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
}
```

处理 worker 的任务完成请求：

```go
// PartFinish -- RPC
func (c *Coordinator) PartFinish(args *PartFinishArgs, reply *PartFinishReply) error {
    fileIdx := args.FileIdx
    partIdx := args.PartIdx
    tp := args.Type
    if tp == MAPTASK {
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
```

### 1.3 worker

worker 从 coordinator 获取任务并处理，完成之后通知 coordinator。

对于 MAP 任务，从 task 中指定路径拉取数据，根据 hash 值分配中间结果位置。

```go
// 论文中这里本应该是从GFS中拉数据
content, err := os.ReadFile(tsk.InputFile[0])
if err != nil {
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
    // 异步写入磁盘 完成后通知 coordinator
    go writeToFile(ofile, parts[i], tsk.FileIdx, i, tsk.Type)
}

func writeToFile(file string, buffer []KeyValue, FileIdx int, PartIdx int, Type int) {
    f, err := os.OpenFile(file+"_tmp", os.O_WRONLY|os.O_CREATE, 0600)
    if err != nil {
        return
    }
    for _, kv := range buffer {
        fmt.Fprintf(f, "%v %v\n", kv.Key, kv.Value)
    }
    f.Close()
    os.Rename(file+"_tmp", file)

    CallPartFinish(FileIdx, PartIdx, Type)
}
```

对于 reduce 任务，也是远程拉取数据，然后其进行排序，调用 reducef 处理并写入文件：

```go
data := []KeyValue{}
// remote read data...
...
// 如果数据比较大还需要进行外部排序
sort.Sort(ByKey(data))

oname := tsk.OutputFile
ofile, _ := os.Create(oname + "_tmp")

i := 0
for i < len(data) {
    // 双指针拿到相同 key 的数据
    j := i + 1
    for j < len(data) && data[j].Key == data[i].Key {
        j++
    }
    values := []string{}
    for k := i; k < j; k++ {
        values = append(values, data[k].Value)
    }
    output := reducef(data[i].Key, values)
    fmt.Fprintf(ofile, "%v %v\n", data[i].Key, output)
    i = j
}
ofile.Close()
os.Rename(oname+"_tmp", oname)

CallPartFinish(tsk.FileIdx, tsk.PartIdx, tsk.Type)
```

- 实际应用中需要远程调用读取数据，但是本 lab 为了简单直接从本地文件中读取了，因为是在一台机器上模拟 mapreduce 过程。
- 注意写数据要先放置到临时文件中，再原子重命名。这时为了防止写文件的时候 worker 崩溃。
- 此外，我额外维护一个心跳机制，用于区分是执行时间长还是 crash 掉了的任务，使得挂掉了的任务能更快被重新调度。





## Lab2

实现一个具有日志压缩的 Raft。

> 即使Lab2通过全部测试样例也不能代表一定没问题，做Lab3的时候可能还要回来改。
>

对于Raft，论文中的 Figure 2 十分关键，几乎可以把上面的每一条描述当成必须实现的点（如果不实现client重定向则不需要LeaderID）。

还有 [Students' Guide](https://thesquareplanet.com/blog/students-guide-to-raft/)，当你遇到一个不好定位的bug时不妨在这里找找。这里还有一个[对照翻译](https://mp.weixin.qq.com/s/blCp4KCY1OKiU2ljLSGr0Q)。

> 一些建议
>
> 1. 要加 `-race` 测试，一些莫名的 bug 总是锁竞争引起的。
> 2. 不能只懂 Raft 层的工作，还要知道上层如何与 Raft 交互，弄清楚从 client 发送一条指令到上层服务响应整个流程。
> 3. 最好对 log 做一个封装，如根据逻辑 Index 获取 log entry，不要等到做 2D 的时候再回来对每个下标做变换，增加 debug 难度。
> 4. 注意 slice 的 gc 内存泄漏问题，截取 slice 需要进行一次拷贝，不然原本的 slice 无法释放。
> 5. think twice, code once.

### 2.1 整体结构

首先，对于Raft结构体，除了Figure 2中需要用到的成员，还有选举定时器和心跳定时器。
```go
type Raft struct {
    mu        sync.Mutex          
    peers     []*labrpc.ClientEnd
    persister *Persister          
    me        int           
    dead      int32           

    status NodeState

    currentTerm int
    votedFor    int
    log         LogEntries

    nextIndex   []int
    matchIndex  []int
    commitIndex int
    lastApplied int
    applyCond   *sync.Cond  // 用于异步提交
    applyCh     chan ApplyMsg

    electionTimer  *time.Timer
    heartbeatTimer *time.Timer

    Logger *zap.Logger
}
```

Raft初始化之后会启动 `ticker()` 和 `applier()` 两个例行协程，ticker() 负责选举超时和心跳发送两种定时任务，applier() 负责将 ApplyMsg 异步提交到上层。

> Q：为什么异步提交？
>
> A：这是一个优化，参考[PingCAP博客](https://cn.pingcap.com/blog/optimizing-raft-in-tikv/)。由于被 committed 的 log 在什么时候被 apply 都不会再影响数据的一致性，这保证了异步提交的正确性，而使用异步提交是为了提高多 client 场景下的吞吐量（更新commitIndex之后可以立刻去处理下一个command）。

在我的实现中，选举超时定时器当节点处于 Leader 状态时不会进行重置，当节点从Leader状态切换为非Leader状态时进行重置。心跳定时器则是非 Leader 状态不维护，并且每次成功发送心跳后重置。

```go
select {
    case <-rf.electionTimer.C:
    rf.mu.Lock()
    if rf.status != LEADER {
        rf.leaderElection()
        rf.electionTimer.Reset(randElectionTime())
    }
    rf.mu.Unlock()

    case <-rf.heartbeatTimer.C:
    rf.mu.Lock()
    if rf.status == LEADER {
        rf.BroadcastHeartbeat(false)
    }
    rf.mu.Unlock()

}
```

当 commitIndex 被更新时会触发 `applyCond`，协程获取 `log[lastApplied+1, commitIndex]`，然后进行异步提交，之后再更新 lastApplied。

```go
func (rf *Raft) applier() {
    for !rf.killed() {

        rf.mu.Lock()
        for rf.lastApplied >= rf.commitIndex {
            rf.applyCond.Wait()
        }

        // [lastApplied+1, commitIndex]
        entries := rf.log.getRangeEntries(rf.lastApplied+1, rf.commitIndex)
        commitIndex := rf.commitIndex
        rf.mu.Unlock()

        for i := range entries {
            rf.applyCh <- ApplyMsg{
                CommandValid: true,
                Command:      entries[i].Command,
                CommandIndex: entries[i].Index,
            }
        }

        rf.mu.Lock()
        if commitIndex > rf.lastApplied {
            rf.lastApplied = commitIndex
        }
        rf.mu.Unlock()

    }
}
```

**一些注意的点和坑**

- 不单独开一个 `applier()` 协程，而是在 commitIndex 更新时起一个 goroutine 提交 ApplyMsg。这样是可能重复提交相同 log entry 的，由于提交时不持锁，lastApplied 就不能及时更新，在高并发下两次 commitIndex 更新可能离得比较近，就会造成重复提交。而单独开一个协程的方式只有更新完 lastApplied 之后才会处理下一次的提交。
- `applier()` 中异步提交后要引用提交前的 commitIndex（或者提交entries的最后一条），这是由于 `rf.commitIndex` 可能在提交期间发生了变化。

### 2.2 Leader选举（2A）

一旦选举定时器超时，节点就会开始一轮选主，并行发送 RequestVote，得到多数选票后成为 Leader，并向其他节点发送空包宣告自己的权威。

```go
for i := range rf.peers {
    if i == rf.me {
        continue
    }

    go func(i int) {
        reply := &RequestVoteReply{}
        if !rf.sendRequestVote(i, args, reply) {
            return
        }

        // handle RequestVote RPC
        rf.mu.Lock()
        defer rf.mu.Unlock()
        if !rf.checkTerm(reply.Term) {
            return
        }
        if rf.status != CANDIDATER {
            return
        }
        // expire request
        if rf.currentTerm != args.Term {
            return
        }

        if reply.VoteGranted {
            elector++
            if elector > len(rf.peers)/2 && rf.status != LEADER {
                rf.changeState(LEADER)
                // init nextIndex and matchIndex
                length := rf.log.getLastLog().Index + 1
                for i := range rf.nextIndex {
                    rf.nextIndex[i] = length
                    rf.matchIndex[i] = 0
                }
                // send empty package
                rf.BroadcastHeartbeat(true)
            }
        }

    }(i)
}
```

- 在收到 request 之后，首先需要对响应包的 term 进行 check，不处理过期的请求。
- 由于网络原因 RPC 可能响应很慢，还需要判断收到响应时的 term 是否和 args.term 相同（保证相同时期）。

关于投票处理，当前节点没投过票而且日志没有请求节点新才能投票。

```go
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if !rf.checkTerm(args.Term) {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        return
    }

    if rf.votedFor == -1 && !rf.logNewer(args.LastLogIndex, args.LastLogTerm) {
        rf.votedFor = args.CandidateID
        rf.persist(nil)

        reply.Term = rf.currentTerm
        reply.VoteGranted = true
        rf.electionTimer.Reset(randElectionTime())
    } else {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
    }

}
```

**一些注意的点和坑**

- 同样要 check term。
- 必须要投票成功才能重置选举超时定时器，这在不可靠网络中特别重要，具体可以看 Guide。
- 收到更高 term 时，应该把 votedFor 置空，然后在进行后续处理。因为日志可能比请求节点新而无法投票。

### 2.3 日志复制（2B）

当 Leader 收到上层传下来的指令，Leader 将这条指令作为日志条目附加到 log 中去，然后并行地发送给其他节点，当这个条目被**安全地复制**，Leader 才会把这条日志提交。

> 当日志条目被复制到多数节点之后，并且最后一个条目的 term 等于当前 Leader 的 term（防止新日志被覆盖，详见论文5.4.2节），才能把日志标记为已提交，即更新 commitIndex。
>
> 由于 Raft 的日志匹配特性，每当 AppendEntries RPC 返回成功时，Leader 就知道 Follower 的日志一定和自己的相同了。

若发生了日志冲突，Leader 需要回退对应节点的 nextIndex。

```go
func (rf *Raft) sendAppendEntriesToPeer(i int, args *AppendEntriesArgs) {

    reply := &AppendEntriesReply{}
    if !rf.sendAppendEntries(i, args, reply) {
        return
    }

    // handle RequestVote RPC
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if !rf.checkTerm(reply.Term) {
        return
    }
    if rf.status != LEADER {
        return
    }
    // expire request
    if rf.currentTerm != args.Term {
        return
    }

    if reply.Success {
        if rf.nextIndex[i] < args.PrevLogIndex+len(args.Entries)+1 {
            rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
            rf.matchIndex[i] = rf.nextIndex[i] - 1
            // 尝试更新 commitIndex
            rf.updateCommitIndex(rf.matchIndex[i])
        }
    } else {
        // 更新 nextIndex[i]
        ...
    }
}
```

- commit日志时一定要判断 term，只能提交当前任期内的日志，否则 `Figure 8` 的测试点概率 fail。

而 Follower 收到 Leader 的 commitIndex 之后也要更新自己的 commitIndex。

```go
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if !rf.checkTerm(args.Term) {
        reply.Term = rf.currentTerm
        reply.Success = false
        return
    }
    rf.electionTimer.Reset(randElectionTime())  // reset

    reply.Term = rf.currentTerm
    // 日志一致性校验
    if !matchLog() {
        reply.Success = false
    }  else {
        reply.Success = true

        // 删除冲突日志 追加新条目
        for i := range args.Entries {
            ...
        }
        // Follower 更新自己的 commitIndex
        if rf.commitIndex < args.LeaderCommit {
            rf.commitIndex = args.LeaderCommit
            rf.applyCond.Signal()
        }
    }
}
```

- 关于日志的更新，一定要按照论文中的先找到冲突点，然后再删除后续日志。我刚开始的做法是直接截断 PrevLogIndex 之后的日志，然后再 append RPC 中的日志，但其实这种做法在不稳定的网络中会出问题，比如我们收到了一个过时的 RPC，而直接截断就意味着把在这个RPC之后所添加的日志“回收”了，所以我们要保留后续的日志。
- 收到 RPC 时重置定时器

### 2.4 持久化（2C）

这个比较简单（前面都做的没问题的情况下），只要在每次 (currentTerm, votedFor, log) 变更后及时调用 persist 来持久化即可。

```go
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, snapshot)
}
```

### 2.5 日志压缩（2D）

首先是上层服务主动触发的日志压缩，截断响应 index 之前的日志。

```go
func (rf *Raft) Snapshot(index int, snapshot []byte) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // reject outdated(duplicated) snapshot
    if index <= rf.log.getFirstLog().Index {
        return
    }

    // 日志中包含LastIncludedLog
    if index <= rf.log.getLastLog().Index {
        // 保留后续日志
        rf.log = LogEntries{
            Entries: rf.log.getSuffixEntries(index),
        }

        rf.updateCommitIndexBySnapshot(index)
        rf.persist(snapshot)
    } 

}
```

- 关于 `rf.log` 的结构，设置 `rf.log[0]` 为快照最后一个包含的日志条目，默认为0，每次截断日志时都额外设置一下，不仅可以减少下标计算的失误率，而且获取该下标就能拿到快照的最后条目，不用额外持久化。

在日志复制时，如果发现我们的 PrevLog 已经被快照截断了，那么我们就需要通过 InstallSnapshot RPC 向 Follower 发送快照：

```go
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock

    if !rf.checkTerm(args.Term) {
        reply.Term = rf.currentTerm
        return
    }
    rf.electionTimer.Reset(randElectionTime())

    reply.Term = rf.currentTerm
    // reject outdated(duplicated) snapshot
    if args.LastIncludedIndex <= rf.commitIndex {
        return
    }

    // 日志中包含LastIncludedLog
    if args.LastIncludedIndex <= rf.log.getLastLog().Index &&
    rf.log.getLogEntryByIndex(args.LastIncludedIndex).Term == args.LastIncludedTerm {
        // 保留后续日志
        rf.log = LogEntries{
            Entries: rf.log.getSuffixEntries(args.LastIncludedIndex),
        }

    } else {
        // 丢弃整个日志(rf.log[0].Command 不可用)
        rf.log = NewLogEntries()
        rf.log.setFirstLog(Entry{
            Index: args.LastIncludedIndex,
            Term:  args.LastIncludedTerm,
        })
    }

    rf.updateCommitIndexBySnapshot(args.LastIncludedIndex)
    rf.persist(args.Data)

    go func() {
        rf.applyCh <- ApplyMsg{
            SnapshotValid: true,
            Snapshot:      args.Data,
            SnapshotTerm:  args.LastIncludedTerm,
            SnapshotIndex: args.LastIncludedIndex,
        }
    }()
}
```

- InstallSnapshot RPC 和 AppendEntries RPC 整体流程比较相似，包括 term 检查、重置定时器、拒绝旧 RPC 和日志更新。
- 最后异步 push 到 ApplyCh，

最后，还有心跳中发送快照和日志：PrevLog 已被快照截断则发送快照同步，否则正常添加日志。

```go
func (rf *Raft) BroadcastHeartbeat(emptyPackage bool) {
    if rf.status != LEADER {
        return
    }
    defer rf.heartbeatTimer.Reset(HeartbeatTime)

    // empty request
    if emptyPackage {
        // make empty package
        args := &AppendEntriesArgs{
            Empty:    true,
            Term:     rf.currentTerm,
            LeaderID: rf.me,
        }
        // parallel transmission
        for i := range rf.peers {
            if i == rf.me {
                continue
            }
            go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
        }
        return
    }

    // parallel transmission
    for i := range rf.peers {
        if i == rf.me {
            continue
        }

        // check if installSnapshot needs to be sent.
        if rf.needInstallSnapshot(rf.nextIndex[i] - 1) {
            snapshotLastLog := rf.log.getFirstLog()
            args := &InstallSnapshotArgs{
                Term:              rf.currentTerm,
                LeaderID:          rf.me,
                LastIncludedIndex: snapshotLastLog.Index,
                LastIncludedTerm:  snapshotLastLog.Term,
                Data:              rf.persister.ReadSnapshot(),
            }
            go rf.sendInstallSnapshotToPeer(i, args)
            continue
        }

        prevLog := rf.log.getLogEntryByIndex(rf.nextIndex[i] - 1)
        args := &AppendEntriesArgs{
            Term:         rf.currentTerm,
            LeaderID:     rf.me,
            PrevLogIndex: prevLog.Index,
            PrevLogTerm:  prevLog.Term,
            Entries:      rf.log.getSuffixEntries(rf.nextIndex[i]),
            LeaderCommit: rf.commitIndex,
        }
        go rf.sendAppendEntriesToPeer(i, args)
    }

}
```

### 2.5 一些注意点

1. 发送 RPC 和 push channel 的时候一定不能持锁，否则会死锁。
2. go 的 gc 问题，slice 的截取操作可能会导致原底层数组的内存不会被释放，从而造成内存泄漏，所以在截取 log 的时候最好 copy 一份，防止无意间内存泄漏。

### 2.6 bugs

1. `Backup2B` 错误。原因在于心跳中并行发送 appendEntries 的 args 在错误的地方进行了构造：

   ```go
   for i := range rf.peers {
       if i != rf.me {
           go func(i int) {
               rf.mu.Lock()
               // 获取rf.term rf.nextIndex等成员构造AppendEntriesArgs
               rf.mu.Unlock()
               rf.sendAppendEntries(i, args, reply)
               // 处理reply
           }
       }
   }
   ```

   在 goroutine 中获取锁的时候 rf.term 可能会发生改变。在 gorountine 外构参数造即可解决问题。

2. `Figure 8 (unreliable)` 小概率 failed。检查 handleAppendEntries 中 nextIndex 的更新是不是类似这样：

   ```go
   rf.nextIndex[i] += len(args.Entries)
   ```

   cuo这个 bug 其实和 `applier()` 中最后 commitIndex 的更新差不多，原因在于网络的不可靠性，tester 对 RPC 进行了乱序发送，由于后发送的 RPC 可能比先发送的 RPC 先到达，或者两次 RPC 的 PrevLogIndex 是相同的，直接累加导致 nextIndex 不正确，所以更新要改成：

   ```go
   rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries)
   ```

   除了上面这个原因，还有可能是论文没读完，更新 commitIndex 没有判断 term 和 Leader 的当前 term 是否相同，论文中对 `Figure 8` 这种情况明确说明 Leader 只能提交当前任期下的日志，而旧日志也会被顺便提交。

3. `snapshots (crash)` 错误。重启时没有将 `commitIndex` 和 `lastApplied` 重置到快照的位置。





## Lab3

实现一个简单的 KVserver，和 Lab2 实现的 Raft 对接。

Lab3 的流程比较简单，如果 Lab2 做的非常好，Lab3 基本上不会遇上什么 bug，反之，如果 Lab3 出现了一些莫名奇妙的 bug，很有可能是 Lab2 中的细节出了问题。

### 3.1 整体流程

client 向 server 层发送请求，server 先将该请求转化为 Op，然后交给 Raft 层同步，等同步完成后（从 ApplyCh 收到消息），server 再将操作应用到状态机，然后响应 client。

简单起见，目前没有实现 follower read 优化，对于 Get 请求也是生成一条 Raft 日志去同步，以最简单的方式实现线性一致性。同时，这么做可以把 Put Append Get 三种操作都归类为一种，而不需要分开写两种 RPC，进一步简化了代码。

### 3.2 client 实现

client中需要维护 `ClientUUID` 供 server 分辨不同的 client，维护一个 commandID 供 server 分辨同一 client 中的不同命令。

```go
type Clerk struct {
    servers []*labrpc.ClientEnd

    lastLeader int
    commandID  int
    ClientUUID string
}
```

将三种操作都归为一类去做处理：

```go
func (ck *Clerk) Operate(key string, value string, opt string) string {
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

        ok := ck.sendOperate(ck.lastLeader, args, reply)
        // 对于异常情况，更换节点尝试
        if ok && reply.Err == OK {
            return reply.Value
        } else {
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
```

### 3.3 Server实现

为了实现 exacty once，server 层需要过滤已经执行过的操作，而且是对于每个不同的 client 来说的，所以我们需要对于每个 client 维护一个 session。

> Q：为什么需要过滤已经执行过的操作：
>
> A：其实就是保证幂等。比如一次 append 操作被引用到了状态机，但是 server 响应 client 的时候 RPC 丢失，client 就会重试，从而导致 append 了两次。
>
> Q：如果写请求是幂等的，那重复执行多次也是可以满足线性一致性的？
>
> A：假设有两个 client，c1 执行 `put(x, 1)`，c2 执行 `get(x)` 和 `put(x, 2)`。对于线性一致的系统，`(get到的值, x最终的值)` 可能结果是 `(0, 2) (0, 1) (1, 2)`，但如果发生了上一个问题所述情况，结果会是 `(1, 1)`。

由于 Raft 的同步需要时间，直到将该命令应用到状态机之后才能响应 client，维护一个 ResponseChans 以获取命令执行的结果。

同时为了应对 RPC 丢失的情况，还要有超时处理。

```go
func (kv *KVServer) Operate(args *OperateArgs, reply *OperateReply) {
    kv.mu.Lock()

    _, isLeader := kv.rf.GetState()
    if !isLeader {
        reply.Err = ErrWrongLeader
        kv.mu.Unlock()
        return
    }

    // 过滤已经执行过的操作
    res := kv.session.getSessionResult(args.ClientUUID)
    if res.LastCommandId == args.CommandID && res.Err == OK {
        reply.Err = OK
        reply.Value = res.Value
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
        kv.mu.Unlock()
        return
    }

    ch := kv.responseChans.getResponseChan(index)
    kv.mu.Unlock()
    // 等待结果 超时处理
    select {
        case res := <-ch:
        reply.Err = res.Err
        reply.Value = res.Value

        case <-time.After(RaftTimeout):
        reply.Err = ErrTimeout
    }

}
```

和Raft类似的，启动一个例行协程来 apply 同步成功的命令：

```go
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
```

### 3.4 应用已同步的命令



```go
func (kv *KVServer) doApplyWork(msg raft.ApplyMsg) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    op, _ := msg.Command.(Op)
    // 旧的日志消息
    if msg.CommandIndex <= kv.lastApplyIndex {
        return
    }
    kv.lastApplyIndex = msg.CommandIndex

    // 已有结果，直接返回即可
    res := kv.session.getSessionResult(op.ClientUUID)
    if res.LastCommandId >= op.CommandID {
        if kv.needSnapshot() {
            kv.takeSnapshot(msg.CommandIndex)
        }
        return
    }

    // 应用到状态机并更新session
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

    // 将结果发送给 OperateChan 
    if _, isLeader := kv.rf.GetState(); isLeader {
        ch := kv.responseChans.getResponseChan(msg.CommandIndex)
        ch <- OperateReply{
            Err:   OK,
            Value: kv.stateMechine.Get(op.Key),
        }
    }

    if kv.needSnapshot() {
        kv.takeSnapshot(msg.CommandIndex)
    }
}
```

- 只有 Leader 节点才能将结果发送给 OperateChan。

### 3.5 快照

首先，状态机肯定是要保存到快照信息中去的；我们要保证线性一致性，所以 session 也要保存到快照中去。

server 层需要检查 Raft 层日志是否超过阈值，超过需要本地快照，这一点在 `doApplyWork()` 的最后顺便检查一下即可。

如果收到 Raft 层的 Snapshot 命令，说明收到了 Leader 发送来的快照，判断一下消息是否过时，然后应用即可

```go
func (kv *KVServer) doSnapshotWork(msg raft.ApplyMsg) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    if msg.SnapshotIndex >= kv.lastApplyIndex {
        kv.InstallSnapshot(msg.Snapshot, msg.SnapshotIndex)
    }
}

func (kv *KVServer) InstallSnapshot(data []byte, snapshotIndex int) {
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return
    }

    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var stateMechine StateMechine
    var session Session
    if d.Decode(&stateMechine) != nil || d.Decode(&session) != nil {
        return
    } else {
        kv.stateMechine = stateMechine
        kv.session = session
    }
    kv.lastApplyIndex = snapshotIndex
}
```

还有一件事，就是记得重启的时候别忘了读取快照并应用。

### 3.6 一些注意点

1. 关于 `speed3A` 超时。
   - 检查 Raft 中的内存泄漏问题。
   - Raft 的 `start()` 中写入日志之后立即触发一次心跳可以加速。
   - 心跳时间不能太短，可以参考 [这篇博客](https://www.cnblogs.com/sun-lingyu/p/14902163.html)。
2. `OnePartition3A` 一直阻塞，原因是响应 client 的时候没有判断当前节点是否为 Leader。这个 case 制造网络分区，并且将旧 Leader 分到少数节点集群里。当网络分区合并的时候，旧 Leader 变成了 Follower，这时它再提交状态机，然后尝试响应 client，但之前 client 会由于超时请求新 Leader，于是旧 Leader 变成了管道无消费者，导致管道一直阻塞。



















