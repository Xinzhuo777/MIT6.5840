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
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type TaskRequest struct {
}

type TaskResponse struct {
	TaskType   TaskType
	TaskID     int
	InputFiles []string
	ReduceID   int
	NReduce    int
}

type ReportRequest struct {
	TaskType TaskType
	TaskID   int
}

type ReportResponse struct {
	Success bool
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
