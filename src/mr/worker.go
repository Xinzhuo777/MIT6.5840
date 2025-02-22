package mr

// import "fmt"
// import "log"
// import "net/rpc"
// import "hash/fnv"
import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task := requestTask()

		switch task.TaskType {
		case MapTask:
			doMap(task, mapf)
		case ReduceTask:
			doReduce(task, reducef)
		case WaitTask:
			time.Sleep(time.Second)
			continue
		case ExitTask:
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMap(task TaskResponse, mapf func(string, string) []KeyValue) {
	filename := task.InputFiles[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	//进行map
	kva := mapf(filename, string(content))

	// Create intermediate files
	intermediates := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce //nReduce个bucket分区，每一个分区存储一个KeyValue数据
		intermediates[bucket] = append(intermediates[bucket], kv)
	}

	// Write to intermediate files
	for i := 0; i < task.NReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		tempFile, err := ioutil.TempFile("", "mr-tmp-*")
		if err != nil {
			log.Fatal("cannot create temp file", err)
		}

		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediates[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("cannot encode", err)
			}
		}
		tempFile.Close()
		os.Rename(tempFile.Name(), oname)
		//这里使用了临时文件+重命名的方式来保证写入的原子性:
		//1. 先创建临时文件
		//2. 将数据写入临时文件
		//3. 最后通过rename原子地替换为目标文件
	}

	reportTask(ReportRequest{TaskType: MapTask, TaskID: task.TaskID})
}

func doReduce(task TaskResponse, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	// Read all intermediate files for this reduce task
	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.ReduceID)
		file, err := os.Open(filename)
		if err != nil {
			continue
		}
		dec := json.NewDecoder(file) //诸葛解析k-v键值对
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate)) //将所有的key-value按照key排序，这样相同的key可以聚集在一起

	// Create output file
	oname := fmt.Sprintf("mr-out-%d", task.ReduceID)
	tempFile, err := ioutil.TempFile("", "mr-out-tmp-*")
	if err != nil {
		log.Fatal("cannot create temp file", err)
	}

	// Process each key group 它通过双指针(i和j)找到具有相同key的一组values,然后调用用户定义的reducef函数进行处理。这实现了MapReduce中的关键步骤 - 把具有相同key的所有value聚合在一起处理。
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%s %s\n", intermediate[i].Key, output)
		i = j
	}

	tempFile.Close()
	os.Rename(tempFile.Name(), oname)

	reportTask(ReportRequest{TaskType: ReduceTask, TaskID: task.ReduceID})
}
// requestTask获取新任务
// reportTask报告任务完成
func requestTask() TaskResponse {
	args := TaskRequest{}
	reply := TaskResponse{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		os.Exit(0)
	}
	return reply
}

func reportTask(args ReportRequest) {
	reply := ReportResponse{}
	call("Coordinator.ReportTask", &args, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
