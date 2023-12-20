package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"

	"github.com/google/uuid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerMateData struct {
	WorkerId string
	WorkType int // 0: map, 1: reduce
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
// worker 节点，传入 map 和 reduce 函数
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	workMateData := WorkerMateData{}
	//generate a random worker id using UUID
	workMateData.WorkerId = uuid.New().String()
	//在此方法中，需要RPC调用 master，获取任务
	//可能是Map任务，也可能是Reduce任务
	//如果是Map任务，其输出intermediate 需要以文件保存在本地
	//这里使用多线程来模拟多个worker节点
	//The reducer output needn't be sorted by key,because the "test-mr.sh"  will sort the content
	//同时，需要依据nReducer的大小来决定intermediate文件的个数
	//建议采用Json格式存储，方便Reducer读取
	//A reasonable naming convention for intermediate files is mr-X-Y, 
	//where X is the Map task number, and Y is the reduce task number.
	// CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
