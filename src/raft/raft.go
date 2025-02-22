package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// Raft server states.
const (
	Follower = iota
	Candidate
	Leader
)

// 定义心跳间隔以及选举超时时间范围
const (
	HeartbeatInterval = 50 * time.Millisecond
	// 增大选举超时窗口，减少多个节点同时超时的可能
	ElectionTimeoutMin = 300 * time.Millisecond
	ElectionTimeoutMax = 500 * time.Millisecond
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// AppendEntriesArgs 定义心跳/日志复制 RPC 请求的参数（这里只作为心跳使用）。
type AppendEntriesArgs struct {
	Term     int // 领导者的任期号
	LeaderId int // 领导者 ID，便于跟新跟随者的状态
	// 日志复制字段在 Lab2A 中不使用
}

// AppendEntriesReply 定义心跳 RPC 回复内容。
type AppendEntriesReply struct {
	Term    int  // 当前任期号，用于领导者发现自己过期
	Success bool // 回复是否成功
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 选举相关状态（Lab2A 需要的状态）
	currentTerm     int           // 最近见过的任期号
	votedFor        int           // 当前任期内投票给了哪个候选人，-1 表示还未投票
	state           int           // 节点当前状态：Follower, Candidate, Leader
	lastHeartbeat   time.Time     // 最近一次收到有效 RPC（心跳）的时间，用于判断选举超时
	electionTimeout time.Duration // 当前选举超时时间（随机值），每次选举开始时重置

}

// randomDuration 返回 [min, max) 内的随机时长
func randomDuration(min, max time.Duration) time.Duration {
	return min + time.Duration(rand.Int63n(int64(max-min)))
}

// return currentTerm and whether this server
// believes it is the leader.
// GetState 返回当前任期及本节点是否为领导者。
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int // 候选人的任期号
	CandidateId int // 请求选票的候选人 ID
	// Lab2A 中暂不需要日志匹配字段
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期号，以便候选人更新自己的任期
	VoteGranted bool // 候选人获得投票时为 true
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果候选人的任期比自己旧，则拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 若发现对方的任期更大，则更新自己的状态，转换为 Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	reply.Term = rf.currentTerm

	// 如果还未投票或已投给该候选人，则投票给他
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// 收到投票请求时重置超时计时器
		rf.lastHeartbeat = time.Now()
	} else {
		reply.VoteGranted = false
	}
}

// AppendEntries 实现了空日志复制的心跳机制。
// 当节点收到心跳时，更新选举超时计时器，回复成功。
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 若领导者任期比自己旧，则拒绝心跳请求
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 如果收到更高任期的心跳，则更新状态为 Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}
	// 重置心跳计时器
	rf.lastHeartbeat = time.Now()

	reply.Term = rf.currentTerm
	reply.Success = true
}

// broadcastHeartbeat 由领导者调用，向所有其他节点发送心跳 RPC。
func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			// 如果本节点已被 Kill，则直接返回
			if rf.killed() {
				return
			}
			args := &AppendEntriesArgs{
				Term:     currentTerm,
				LeaderId: rf.me,
			}
			var reply AppendEntriesReply
			if rf.peers[peer].Call("Raft.AppendEntries", args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
				}
			}
		}(i)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	// Lab2A 中不处理日志复制，直接返回 dummy index
	index := 0
	return index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

// killed 检查当前节点是否被 Kill。
func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// startElection 由 Follower 或 Candidate 在选举超时时调用，发起一次新的选举过程。
func (rf *Raft) startElection() {
	// 进入 Candidate 状态，更新任期、给自己投票并重置计时器
	rf.mu.Lock()
	rf.currentTerm++
	currentTerm := rf.currentTerm
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = randomDuration(ElectionTimeoutMin, ElectionTimeoutMax)
	rf.mu.Unlock()

	majority := len(rf.peers)/2 + 1
	votesCh := make(chan bool, len(rf.peers)-1)
	// 自己投票
	votes := 1

	// 向所有其他节点发送 RequestVote RPC
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			args := &RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: 0,
				LastLogTerm:  0,
			}
			var reply RequestVoteReply
			if rf.sendRequestVote(peer, args, &reply) {
				// 锁定后判断状态
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != Candidate || rf.currentTerm != currentTerm {
					// 选举已经结束，不再计票
					votesCh <- false
					return
				}
				if reply.VoteGranted {
					votesCh <- true
				} else if reply.Term > currentTerm {
					// 更新为较高任期并变为 Follower
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					votesCh <- false
				} else {
					votesCh <- false
				}
			} else {
				votesCh <- false
			}
		}(i)
	}

	// 等待投票结果，设置一个整体超时
	timeout := time.After(300 * time.Millisecond)
	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case vote := <-votesCh:
			if vote {
				votes++
				if votes >= majority {
					rf.mu.Lock()
					if rf.state == Candidate && rf.currentTerm == currentTerm {
						rf.state = Leader
						go rf.broadcastHeartbeat()
					}
					rf.mu.Unlock()
					return
				}
			}
		case <-timeout:
			// 超时后结束等待，选举将自然结束
			return
		}
	}
}

func (rf *Raft) ticker() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !rf.killed() {
		select {
		case <-ticker.C:
			rf.mu.Lock()
			currentState := rf.state
			timeout := rf.electionTimeout
			lastHB := rf.lastHeartbeat
			rf.mu.Unlock()

			if currentState == Leader {
				rf.broadcastHeartbeat()
			} else if time.Since(lastHB) >= timeout {
				rf.startElection()
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// Make 创建一个新的 Raft 服务器，并从持久化状态中恢复，同时启动后台 goroutine。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		currentTerm:   0,
		votedFor:      -1,
		state:         Follower,
		lastHeartbeat: time.Now(),
		// 初始选举超时时间为随机值
		electionTimeout: randomDuration(ElectionTimeoutMin, ElectionTimeoutMax),
	}

	// 从持久化存储中恢复状态（若有）
	rf.readPersist(persister.ReadRaftState())

	// 启动选举/心跳监控后台循环
	go rf.ticker()

	return rf
}
