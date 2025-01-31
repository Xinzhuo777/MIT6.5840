package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	Type      TaskType
	ID        int
	Files     []string
	StartTime time.Time
	State     TaskState
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	phase       TaskType
	nMap        int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *TaskRequest, reply *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check for timeouts
	now := time.Now()
	if c.phase == MapTask {
		for i := range c.mapTasks {
			if c.mapTasks[i].State == InProgress &&
				now.Sub(c.mapTasks[i].StartTime) > 10*time.Second {
				c.mapTasks[i].State = Idle
			}
		}
	} else if c.phase == ReduceTask {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].State == InProgress &&
				now.Sub(c.reduceTasks[i].StartTime) > 10*time.Second {
				c.reduceTasks[i].State = Idle
			}
		}
	}

	// Assign tasks based on phase
	if c.phase == MapTask {
		// Find an idle map task
		for i := range c.mapTasks {
			if c.mapTasks[i].State == Idle {
				c.mapTasks[i].State = InProgress
				c.mapTasks[i].StartTime = now
				reply.TaskType = MapTask
				reply.TaskID = c.mapTasks[i].ID
				reply.InputFiles = c.mapTasks[i].Files
				reply.NReduce = c.nReduce
				return nil
			}
		}

		// Check if all map tasks are completed
		allCompleted := true
		for _, task := range c.mapTasks {
			if task.State != Completed {
				allCompleted = false
				break
			}
		}
		if allCompleted {
			c.phase = ReduceTask
		}
	}

	if c.phase == ReduceTask {
		// Find an idle reduce task
		for i := range c.reduceTasks {
			if c.reduceTasks[i].State == Idle {
				c.reduceTasks[i].State = InProgress
				c.reduceTasks[i].StartTime = now
				reply.TaskType = ReduceTask
				reply.TaskID = c.reduceTasks[i].ID
				reply.ReduceID = i
				reply.NReduce = c.nReduce
				return nil
			}
		}

		// Check if all reduce tasks are completed
		allCompleted := true
		for _, task := range c.reduceTasks {
			if task.State != Completed {
				allCompleted = false
				break
			}
		}
		if allCompleted {
			reply.TaskType = ExitTask
			return nil
		}
	}

	reply.TaskType = WaitTask
	return nil
}

func (c *Coordinator) ReportTask(args *ReportRequest, reply *ReportResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		if args.TaskID < len(c.mapTasks) &&
			c.mapTasks[args.TaskID].State == InProgress {
			c.mapTasks[args.TaskID].State = Completed
			reply.Success = true
		}
	} else if args.TaskType == ReduceTask {
		if args.TaskID < len(c.reduceTasks) &&
			c.reduceTasks[args.TaskID].State == InProgress {
			c.reduceTasks[args.TaskID].State = Completed
			reply.Success = true
		}
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.reduceTasks {
		if task.State != Completed {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		phase:   MapTask,
		nMap:    len(files),
	}

	// Initialize map tasks
	for i, file := range files {
		c.mapTasks = append(c.mapTasks, Task{
			Type:  MapTask,
			ID:    i,
			Files: []string{file},
			State: Idle,
		})
	}

	// Initialize reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{
			Type:  ReduceTask,
			ID:    i,
			State: Idle,
		})
	}

	c.server()
	return &c
}
