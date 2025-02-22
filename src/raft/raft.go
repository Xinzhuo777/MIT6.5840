package raft

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// ApplyMsg 定义了向服务层传递的消息结构体
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// 用于2D，支持快照
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Entry 定义了 Raft 日志条目结构体
type Entry struct {
	Term int
	Cmd  interface{}
}

// 节点状态常量
const (
	Follower = iota
	Candidate
	Leader
)

const (
	HeartBeatTimeOut = 101
	ElectTimeOutBase = 450
)

// Raft 节点结构体，保存所有状态
type Raft struct {
	mu        sync.Mutex          // 保护共享状态
	peers     []*labrpc.ClientEnd // 所有节点的 RPC 端点
	persister *Persister          // 持久化状态的存储对象
	me        int                 // 自己在 peers 数组中的索引
	dead      int32               // 被 Kill() 后设置为1

	currentTerm int
	votedFor    int
	log         []Entry

	nextIndex  []int
	matchIndex []int

	// 定时器等辅助字段
	voteTimer  *time.Timer
	heartTimer *time.Timer
	rd         *rand.Rand
	role       int

	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg

	condApply *sync.Cond

	// 快照相关字段（2D）
	snapShot          []byte // 最新快照数据
	lastIncludedIndex int    // 快照包含的最后日志条目的索引
	lastIncludedTerm  int    // 快照包含的最后日志条目的 Term
}

// 获取随机选举超时时间函数
func GetRandomElectTimeOut(rd *rand.Rand) int {
	return int(rd.Float64()*150) + ElectTimeOutBase
}

// 调试打印，便于跟踪状态
func (rf *Raft) Print() {
	DPrintf("raft%v:{currentTerm=%v, role=%v, votedFor=%v}\n", rf.me, rf.currentTerm, rf.role, rf.votedFor)
}

// 重置选举定时器
func (rf *Raft) ResetVoteTimer() {
	rdTimeOut := GetRandomElectTimeOut(rf.rd)
	rf.voteTimer.Reset(time.Duration(rdTimeOut) * time.Millisecond)
}

// 重置心跳定时器
func (rf *Raft) ResetHeartTimer(timeoutMs int) {
	rf.heartTimer.Reset(time.Duration(timeoutMs) * time.Millisecond)
}

// RealLogIdx 将虚拟索引转换为实际日志数组的下标，此函数调用时必须已持有rf.mu锁
func (rf *Raft) RealLogIdx(vIdx int) int {
	return vIdx - rf.lastIncludedIndex
}

// VirtualLogIdx 将实际日志数组下标转换为虚拟索引（包含已快照部分）; 同样需在加锁状态下调用
func (rf *Raft) VirtualLogIdx(rIdx int) int {
	return rIdx + rf.lastIncludedIndex
}

// 获取当前状态：当前term和是否leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// 持久化 Raft 状态到 persister，包括 votedFor, currentTerm, log，以及快照相关信息
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// 持久化需要保存的信息
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, rf.snapShot)
}

// 从持久化数据中恢复状态
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var votedFor int
	var currentTerm int
	var log []Entry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&votedFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("server %v readPersist failed\n", rf.me)
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm

		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		DPrintf("server %v readPersist 成功\n", rf.me)
	}
}

// 从持久化快照数据中恢复状态
func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		DPrintf("server %v 读取快照失败: 无快照\n", rf.me)
		return
	}
	rf.snapShot = data
	DPrintf("server %v 读取快照成功\n", rf.me)
}

// 快照接口，应用层调用后应丢弃 index 之前的日志条目，并保存快照数据
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// index 必须在 commitIndex 内，且比当前保存的快照更新
	if rf.commitIndex < index || index <= rf.lastIncludedIndex {
		DPrintf("server %v 拒绝 Snapshot 请求, index=%v, commitIndex=%v, lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex)
		return
	}

	DPrintf("server %v 接受 Snapshot 请求, index=%v, commitIndex=%v, 原 lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex)

	// 保存快照数据
	rf.snapShot = snapshot

	// 找到待截断的日志条目，注意这里已加锁，故可直接调用
	rf.lastIncludedTerm = rf.log[rf.RealLogIdx(index)].Term
	// 截断日志，使得 index 对应的条目成为新数组的起始位置
	rf.log = rf.log[rf.RealLogIdx(index):]
	rf.lastIncludedIndex = index
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	rf.persist()
}

// Kill 将节点标记为死亡，便于退出长循环
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// killed 检查节点是否被Kill
func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// Start 提交新的命令到 Raft 日志中，非leader则返回 false
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer func() {
		rf.ResetHeartTimer(15)
		rf.mu.Unlock()
	}()
	if rf.role != Leader {
		return -1, -1, false
	}

	newEntry := Entry{Term: rf.currentTerm, Cmd: command}
	rf.log = append(rf.log, newEntry)
	DPrintf("leader %v 提交命令，准备持久化\n", rf.me)
	rf.persist()

	// 返回虚拟的日志索引
	return rf.VirtualLogIdx(len(rf.log) - 1), rf.currentTerm, true
}

// CommitChecker 定时检查 commitIndex 变化，应用已提交的日志条目
func (rf *Raft) CommitChecker() {
	DPrintf("server %v 的 CommitChecker 启动\n", rf.me)
	for !rf.killed() {
		rf.mu.Lock()
		// 等待条件变量通知有新的提交
		for rf.commitIndex <= rf.lastApplied {
			rf.condApply.Wait()
		}
		// 收集将要应用的所有日志消息
		var msgBuf []*ApplyMsg
		tmpApplied := rf.lastApplied
		for rf.commitIndex > tmpApplied {
			tmpApplied++
			// 如果该条目已在快照中，跳过应用
			if tmpApplied <= rf.lastIncludedIndex {
				continue
			}
			// 检查数组边界，防止越界
			realIdx := rf.RealLogIdx(tmpApplied)
			if realIdx >= len(rf.log) {
				DPrintf("server %v CommitChecker 数组越界: tmpApplied=%v, realIdx=%v, len(log)=%v, lastIncludedIndex=%v", rf.me, tmpApplied, realIdx, len(rf.log), rf.lastIncludedIndex)
				continue
			}
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[realIdx].Cmd,
				CommandIndex: tmpApplied,
				SnapshotTerm: rf.log[realIdx].Term,
			}
			msgBuf = append(msgBuf, msg)
		}
		rf.mu.Unlock()

		// 按序将消息发送到申请通道，并更新 lastApplied
		for _, msg := range msgBuf {
			rf.mu.Lock()
			// 保证消息顺序
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}
			DPrintf("server %v 应用日志, index=%v, term=%v, lastIncludedIndex=%v\n", rf.me, msg.CommandIndex, msg.SnapshotTerm, rf.lastIncludedIndex)
			rf.mu.Unlock()

			rf.applyCh <- *msg

			rf.mu.Lock()
			if msg.CommandIndex == rf.lastApplied+1 {
				rf.lastApplied = msg.CommandIndex
			}
			rf.mu.Unlock()
		}
	}
}

// InstallSnapshotArgs 快照安装 RPC 参数
type InstallSnapshotArgs struct {
	Term              int         // leader 的 term
	LeaderId          int         // 用于重定向客户端
	LastIncludedIndex int         // 快照包含的最后日志索引
	LastIncludedTerm  int         // 快照包含的最后日志 term
	Data              []byte      // 快照数据
	LastIncludedCmd   interface{} // 占位字段，在 log[0] 处保留
}

// InstallSnapshotReply 快照安装 RPC 回复
type InstallSnapshotReply struct {
	Term int // 当前 term，用于让 leader 更新自身
}

// sendInstallSnapshot 向指定服务器发送 InstallSnapshot RPC
func (rf *Raft) sendInstallSnapshot(serverTo int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[serverTo].Call("Raft.InstallSnapshot", args, reply)
}

// InstallSnapshot RPC 处理函数
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果收到的 term 过小，则直接回复
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		DPrintf("server %v 拒绝来自 server %v 的 InstallSnapshot，term较小\n", rf.me, args.LeaderId)
		return
	}

	// 如果 term 更高，则更新状态
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		DPrintf("server %v 接收到来自 server %v 的 InstallSnapshot，但发现更高 term\n", rf.me, args.LeaderId)
	}
	rf.role = Follower
	rf.ResetVoteTimer()
	DPrintf("server %v 接收到 leader %v 的 InstallSnapshot，重置选举定时器\n", rf.me, args.LeaderId)

	// 当快照较旧，不用处理
	if args.LastIncludedIndex < rf.lastIncludedIndex || args.LastIncludedIndex < rf.commitIndex {
		reply.Term = rf.currentTerm
		return
	}

	// 检查当前日志中是否存在和快照最后条目匹配的项
	hasEntry := false
	idxFound := 0
	for rIdx := 0; rIdx < len(rf.log); rIdx++ {
		if rf.VirtualLogIdx(rIdx) == args.LastIncludedIndex && rf.log[rIdx].Term == args.LastIncludedTerm {
			hasEntry = true
			idxFound = rIdx
			break
		}
	}

	msg := &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	// 如果已有对应条目，则保留其后的日志，否则清空日志并添加占位符
	if hasEntry {
		DPrintf("server %v InstallSnapshot：在索引 %v 处存在匹配条目，保留之后日志\n", rf.me, args.LastIncludedIndex)
		rf.log = rf.log[idxFound:]
	} else {
		DPrintf("server %v InstallSnapshot：未找到匹配条目，清空日志\n", rf.me)
		rf.log = make([]Entry, 0)
		rf.log = append(rf.log, Entry{Term: rf.lastIncludedTerm, Cmd: args.LastIncludedCmd})
	}

	rf.snapShot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	reply.Term = rf.currentTerm
	// 将快照消息发送至应用层
	rf.applyCh <- *msg
	rf.persist()
}

// handleInstallSnapshot 由 leader 用来发送快照给落后节点
func (rf *Raft) handleInstallSnapshot(serverTo int) {
	reply := &InstallSnapshotReply{}
	rf.mu.Lock()
	// 如果当前不再是 leader，直接返回
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	// 构造快照参数时，取当前本地状态
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapShot,
		LastIncludedCmd:   rf.log[0].Cmd,
	}
	rf.mu.Unlock()

	ok := rf.sendInstallSnapshot(serverTo, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果期间状态发生变化，则返回
	if rf.role != Leader || rf.currentTerm != args.Term {
		return
	}
	// 如果收到更新的 term，则转变为 Follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.ResetVoteTimer()
		rf.persist()
		return
	}
	// 更新 matchIndex 和 nextIndex
	if rf.matchIndex[serverTo] < args.LastIncludedIndex {
		rf.matchIndex[serverTo] = args.LastIncludedIndex
	}
	rf.nextIndex[serverTo] = rf.matchIndex[serverTo] + 1
}

// AppendEntriesArgs 定义了追加日志条目的 RPC 参数
type AppendEntriesArgs struct {
	Term         int     // leader 的 term
	LeaderId     int     // leader 的 ID，用于重定向
	PrevLogIndex int     // 新日志之前的日志索引
	PrevLogTerm  int     // 上一个日志条目的 term
	Entries      []Entry // 要复制的日志集；空表示心跳
	LeaderCommit int     // leader 的 commitIndex
}

// AppendEntriesReply 定义了追加日志 RPC 回复结构
type AppendEntriesReply struct {
	Term    int  // 当前 term，用于让 leader 更新状态
	Success bool // 是否成功复制日志
	XTerm   int  // Follower 中冲突位置的 term
	XIndex  int  // Follower 中第一个出现 XTerm 的日志索引
	XLen    int  // Follower 日志的虚拟长度（包含已快照部分）
}

// sendAppendEntries 向指定服务器发送追加日志 RPC
func (rf *Raft) sendAppendEntries(serverTo int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[serverTo].Call("Raft.AppendEntries", args, reply)
}

// AppendEntries RPC 处理函数
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果收到的 term 低于当前 term，则直接回复 false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("server %v 收到来自 leader %v 的旧 AppendEntries，args=%+v, 当前 term=%v\n", rf.me, args.LeaderId, args, rf.currentTerm)
		return
	}

	// 收到合法请求，先重置选举定时器
	rf.ResetVoteTimer()
	// 如果发现更高 term，则更新当前状态
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	// 处理心跳或日志复制请求
	if len(args.Entries) == 0 {
		DPrintf("server %v 收到来自 leader %v 的心跳, lastIncludedIndex=%v, PrevLogIndex=%v\n", rf.me, args.LeaderId, rf.lastIncludedIndex, args.PrevLogIndex)
	} else {
		DPrintf("server %v 收到来自 leader %v 的 AppendEntries, lastIncludedIndex=%v, PrevLogIndex=%v, Entries 数量=%v \n", rf.me, args.LeaderId, rf.lastIncludedIndex, args.PrevLogIndex, len(args.Entries))
	}

	isConflict := false

	// 检查 PrevLogIndex 的合法性
	if args.PrevLogIndex < rf.lastIncludedIndex {
		// RPC 请求落在已快照部分，直接回复成功（此时 follower已有更新快照）
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	} else if args.PrevLogIndex >= rf.VirtualLogIdx(len(rf.log)) {
		// 无对应日志条目，统计冲突详情
		reply.XTerm = -1
		reply.XLen = rf.VirtualLogIdx(len(rf.log))
		isConflict = true
		DPrintf("server %v 缺少 PrevLogIndex=%v 的日志项，当前 Log 长度=%v\n", rf.me, args.PrevLogIndex, reply.XLen)
	} else if rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
		// 找到冲突的日志条目，计算冲突的起始索引
		reply.XTerm = rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term
		i := args.PrevLogIndex
		for i > rf.commitIndex && rf.log[rf.RealLogIdx(i)].Term == reply.XTerm {
			i--
		}
		reply.XIndex = i + 1
		reply.XLen = rf.VirtualLogIdx(len(rf.log))
		isConflict = true
		DPrintf("server %v 在 PrevLogIndex=%v 处发现 Term 冲突, 请求 term=%v, 实际 term=%v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, reply.XTerm)
	}

	if isConflict {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 根据日志匹配情况追加新的日志条目
	startIdx := rf.RealLogIdx(args.PrevLogIndex) + 1
	for i, entry := range args.Entries {
		pos := startIdx + i
		if pos < len(rf.log) {
			if rf.log[pos].Term != entry.Term {
				// 冲突发生，截断日志后追加
				rf.log = rf.log[:pos]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			// 直接追加新日志条目
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	// 持久化日志
	rf.persist()

	reply.Success = true
	reply.Term = rf.currentTerm

	// 更新 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		lastNewIdx := rf.VirtualLogIdx(len(rf.log) - 1)
		if args.LeaderCommit > lastNewIdx {
			rf.commitIndex = lastNewIdx
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		DPrintf("server %v 更新 commitIndex=%v, 日志长度=%v\n", rf.me, rf.commitIndex, len(rf.log))
		rf.condApply.Signal()
	}
}

// handleAppendEntries 由 leader 使用，向指定 server 发送 AppendEntries 请求
func (rf *Raft) handleAppendEntries(serverTo int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverTo, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果状态变化（例如不再是 leader），则忽略回复
	if rf.role != Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Success {
		newMatchIdx := args.PrevLogIndex + len(args.Entries)
		if newMatchIdx > rf.matchIndex[serverTo] {
			rf.matchIndex[serverTo] = newMatchIdx
		}
		rf.nextIndex[serverTo] = rf.matchIndex[serverTo] + 1

		// 尝试更新 commitIndex（采用 leader 当前 term 的日志才有效）
		N := rf.VirtualLogIdx(len(rf.log) - 1)
		DPrintf("leader %v 准备更新 commitIndex, lastIncludedIndex=%v, 当前 commitIndex=%v\n", rf.me, rf.lastIncludedIndex, rf.commitIndex)
		for N > rf.commitIndex {
			count := 1 // 包含自己
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N && rf.log[rf.RealLogIdx(N)].Term == rf.currentTerm {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				break
			}
			N--
		}
		rf.commitIndex = N
		rf.condApply.Signal()
		return
	}

	// 如果回复中包含更新的 term，则转型为 follower
	if reply.Term > rf.currentTerm {
		DPrintf("leader %v 从 server %v 收到更高 term=%v，转换为 Follower\n", rf.me, serverTo, reply.Term)
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.ResetVoteTimer()
		rf.persist()
		return
	}

	// 处理日志冲突后，快速回退 nextIndex
	if reply.Term == rf.currentTerm && rf.role == Leader {
		if reply.XTerm == -1 {
			DPrintf("leader %v 收到 server %v 的冲突回退（日志过短），将 nextIndex 调整为 %v\n", rf.me, serverTo, reply.XLen)
			if rf.lastIncludedIndex >= reply.XLen {
				rf.nextIndex[serverTo] = rf.lastIncludedIndex
			} else {
				rf.nextIndex[serverTo] = reply.XLen
			}
			return
		}
		// 从nextIndex回退到发生冲突的合适位置
		i := rf.nextIndex[serverTo] - 1
		if i < rf.lastIncludedIndex {
			i = rf.lastIncludedIndex
		}
		for i > rf.lastIncludedIndex && rf.log[rf.RealLogIdx(i)].Term > reply.XTerm {
			i--
		}
		if i == rf.lastIncludedIndex && rf.log[rf.RealLogIdx(i)].Term > reply.XTerm {
			rf.nextIndex[serverTo] = rf.lastIncludedIndex
		} else if rf.log[rf.RealLogIdx(i)].Term == reply.XTerm {
			rf.nextIndex[serverTo] = i + 1
		} else {
			if reply.XIndex <= rf.lastIncludedIndex {
				rf.nextIndex[serverTo] = rf.lastIncludedIndex
			} else {
				rf.nextIndex[serverTo] = reply.XIndex
			}
		}
		return
	}
}

// SendHeartBeats 由 leader 定时调用，向各个 follower 发送心跳/日志复制请求
func (rf *Raft) SendHeartBeats() {
	DPrintf("leader %v 开始发送心跳\n", rf.me)
	for !rf.killed() {
		<-rf.heartTimer.C
		rf.mu.Lock()
		// 检查当前状态是否仍为 leader
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		// 遍历所有 peer 发送心跳或复制日志
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				LeaderCommit: rf.commitIndex,
			}

			sendInstallSnapshot := false
			if args.PrevLogIndex < rf.lastIncludedIndex {
				DPrintf("leader %v 对 server %v 将发送 InstallSnapshot（follower 落后），nextIndex=%v, lastIncludedIndex=%v\n", rf.me, i, rf.nextIndex[i], rf.lastIncludedIndex)
				sendInstallSnapshot = true
			} else if rf.VirtualLogIdx(len(rf.log)-1) > args.PrevLogIndex {
				args.Entries = rf.log[rf.RealLogIdx(args.PrevLogIndex+1):]
				DPrintf("leader %v 向 server %v 发送 AppendEntries, PrevLogIndex=%v, 新日志条数=%v\n", rf.me, i, args.PrevLogIndex, len(args.Entries))
			} else {
				args.Entries = make([]Entry, 0)
				DPrintf("leader %v 向 server %v 发送心跳, PrevLogIndex=%v\n", rf.me, i, args.PrevLogIndex)
			}

			if sendInstallSnapshot {
				go rf.handleInstallSnapshot(i)
			} else {
				// 为避免重复计算，将 PrevLogTerm 取出
				args.PrevLogTerm = rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term
				go rf.handleAppendEntries(i, args)
			}
		}
		rf.mu.Unlock()
		rf.ResetHeartTimer(HeartBeatTimeOut)
	}
}

// RequestVoteArgs 定义了投票请求的 RPC 参数
type RequestVoteArgs struct {
	Term         int // 候选人的 term
	CandidateId  int // 发起投票的候选人 ID
	LastLogIndex int // 候选人最后日志条目的索引（虚拟索引）
	LastLogTerm  int // 候选人最后日志条目的 term
}

// RequestVoteReply 定义了投票请求的 RPC 回复
type RequestVoteReply struct {
	Term        int  // 当前 term，用于候选人更新
	VoteGranted bool // 是否同意投票
}

// sendRequestVote 向指定服务器发送 RequestVote 请求
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// RequestVote RPC 处理函数
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("server %v 拒绝投票给 server %v，原因 term 过旧: args.term=%v, 当前 term=%v\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	// 如果发现更高 term，则更新并清空投票记录
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	// 判断候选人的日志是否至少与自己的日志一样新
	lastLog := rf.log[len(rf.log)-1]
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLog.Term ||
			(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= rf.VirtualLogIdx(len(rf.log)-1))) {
		rf.votedFor = args.CandidateId
		rf.ResetVoteTimer()
		rf.persist()
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		DPrintf("server %v 同意投票给 server %v, args=%+v\n", rf.me, args.CandidateId, args)
		return
	} else {
		DPrintf("server %v 拒绝投票给 server %v，候选人日志较旧，args=%+v\n", rf.me, args.CandidateId, args)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

// GetVoteAnswer 包装了获取单个候选人投票回复的逻辑
func (rf *Raft) GetVoteAnswer(server int, args *RequestVoteArgs) bool {
	sendArgs := *args
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &sendArgs, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果状态已经发生变化，忽略当前回复
	if rf.role != Candidate || sendArgs.Term != rf.currentTerm {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}
	return reply.VoteGranted
}

// collectVote 并发收集投票结果，同时保护 voteCount 的更新
func (rf *Raft) collectVote(serverTo int, args *RequestVoteArgs, muVote *sync.Mutex, voteCount *int) {
	if rf.GetVoteAnswer(serverTo, args) {
		muVote.Lock()
		if *voteCount > len(rf.peers)/2 {
			muVote.Unlock()
			return
		}
		*voteCount++
		if *voteCount > len(rf.peers)/2 {
			rf.mu.Lock()
			// 检查在获取票数期间状态是否已改变
			if rf.role == Candidate && rf.currentTerm == args.Term {
				DPrintf("server %v 成为新的 leader\n", rf.me)
				rf.role = Leader
				// 重置 nextIndex 和 matchIndex
				for i := 0; i < len(rf.nextIndex); i++ {
					rf.nextIndex[i] = rf.VirtualLogIdx(len(rf.log))
					rf.matchIndex[i] = rf.lastIncludedIndex
				}
				go rf.SendHeartBeats()
			}
			rf.mu.Unlock()
		}
		muVote.Unlock()
	}
}

// Elect 发起新的选举过程
func (rf *Raft) Elect() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.persist()

	voteCount := 1 // 给自己投一票
	var muVote sync.Mutex

	DPrintf("server %v 发起新一轮选举, term=%v\n", rf.me, rf.currentTerm)

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.VirtualLogIdx(len(rf.log) - 1),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.collectVote(i, args, &muVote, &voteCount)
	}
}

// ticker 定时器循环，超时则发起选举
func (rf *Raft) ticker() {
	for !rf.killed() {
		<-rf.voteTimer.C
		rf.mu.Lock()
		// 如果当前不是 leader，则启动选举
		if rf.role != Leader {
			go rf.Elect()
		}
		rf.ResetVoteTimer()
		rf.mu.Unlock()
	}
}

// Make 创建并初始化一个 Raft 节点
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DPrintf("server %v 调用 Make 启动\n", me)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.log = make([]Entry, 0)
	// 初始日志占位符，Term 设为 0
	rf.log = append(rf.log, Entry{Term: 0})

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.role = Follower
	rf.applyCh = applyCh
	rf.condApply = sync.NewCond(&rf.mu)
	rf.rd = rand.New(rand.NewSource(int64(rf.me)))
	rf.voteTimer = time.NewTimer(0)
	rf.heartTimer = time.NewTimer(0)
	rf.ResetVoteTimer()

	// 从持久化存储中恢复状态
	rf.readSnapshot(persister.ReadSnapshot())
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.VirtualLogIdx(len(rf.log))
	}

	// 启动选举定时器和日志提交检测的 goroutine
	go rf.ticker()
	go rf.CommitChecker()

	return rf
}
