package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int // 核心状态机建模
const (
	MapTask    = iota // 0 值，标识基础状态
	ReduceTask        // 保持与MapTask的严格顺序关系
	WaitTask          // 非最终状态，实现阻塞式回退
	ExitTask          // 终止状态控制
)

type TaskState int // 微观任务状态机
const (
	Idle       = iota // 初始可分配状态
	InProgress        // 原子性保证关键状态
	Completed         // 终态防重复处理
)

// 任务请求协议（空结构体实现最小通信开销）
type TaskRequest struct{} // Zero-byte 优化设计

// 任务响应协议（完备参数传递）
type TaskResponse struct {
	TaskType   TaskType // 主控制信号
	TaskID     int      // 全局唯一标识
	InputFiles []string // 数据局部性保证
	ReduceID   int      // Reduce阶段分片标识
	NReduce    int      // 跨阶段参数传递
}

type ReportRequest struct {
	TaskType TaskType // 精确状态分类报告
	TaskID   int      // 精确任务定位
}

type ReportResponse struct {
	Success bool // 原子确认信号
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
