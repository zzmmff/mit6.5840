package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerMateData struct {
	WorkerId string
	WorkType int // 0: map, 1: reduce
}

type ByKey []KeyValue

// for sorting by key.
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
// worker 节点，传入 map 和 reduce 函数
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
	//nReducer 决定了 reduce task 的个数，并没有决定【reduce worker】的个数，表明了在Map 计算出intermediate文件后，
	//需要分割为nReducer份，然后分发给reduce worker
	//建议采用Json格式存储，方便Reducer读取
	//A reasonable naming convention for intermediate files is mr-X-Y,
	//where X is the Map task number, and Y is the reduce task number.
	// CallExample()
	tr, err := CallAskTask(workMateData)
	if err != nil {
		log.Fatal("call AskTask failed")
	}
	fmt.Printf("tr.FileName %v\n", tr.FileName)
	if tr.WorkerType == 0 { //map
		//read file
		content, err := readFile(tr.FileName)	
		if err != nil {
			log.Fatal("cannot read %v", tr.FileName)
			//TODO handle this error
		}
		//call map function
		kv := mapf(tr.FileName, content)	
		//partition the intermediate data into nReduce files
		intermediate := []KeyValue{}
		intermediate = append(intermediate, kv...)
		intermediateFileNames := []string{}
		//sort by key
		sort.Sort(ByKey(intermediate))
		i := 0
		for i< len(intermediate) { //iterate the intermediate data
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k:=i; k<j; k++{
				values = append(values, intermediate[k].Value)
			}
			//store the intermediate data by hash(key)
			reduceTaskIndex := ihash(intermediate[i].Key)	
			fileName := intermediateFileName(tr.TaskNumberIndex, reduceTaskIndex)
			intermediateFileNames = append(intermediateFileNames,fileName)
			file, err := os.OpenFile(fileName,os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)	
			if err != nil {
				log.Fatal("cannot open %v", fileName)
				//TODO handle this error
			}
			//TODO This part can run in another thread
			enc := json.NewEncoder(file)
			for _,kv := range values {
				err := enc.Encode(&kv)	
				if (err != nil) {
					log.Fatal("cannot encode %v", kv)
					//TODO handle this error
				}
			}
			//////////////////////////////////////////
		}
		//Call the master node to report the task completion
		CallTaskCompletion(workMateData,tr.TaskNumberIndex,intermediateFileNames)
	}

}

func intermediateFileName(mapTaskIndex int, reduceTaskIndex int) string {
	return fmt.Sprintf("mr-%v-%v", mapTaskIndex, reduceTaskIndex)
}

// 读取文件中的内容，返回 string
func readFile(fileName string) (string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("cannot open %v", fileName)
		return "", err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatal("cannot read %v", fileName)
		return "", err
	}
	file.Close()
	return string(content), nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func CallAskTask(workMateData WorkerMateData) (TaskReply, error) {
	args := AskTaskArgs{
		WorkerId:   workMateData.WorkerId,
		WorkerType: workMateData.WorkType,
	}
	reply := TaskReply{}
	ok := call("Coordinator.AskTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.FileName %v\n", reply.FileName)
		fmt.Printf("reply.NReducer %v\n", reply.NReducer)
	} else {
		return reply, fmt.Errorf("call failed")
	}
	return reply, nil
}

func CallTaskCompletion(mateData WorkerMateData,taskIndexNumber int,intermediateFileNames []string) (TaskCompletionReply,error){
	args := TaskCompletion{
		WorkerType: mateData.WorkType,
		WorkerId: mateData.WorkerId,
		TaskNumberIndex: taskIndexNumber,
	}
	if args.WorkerType == 0 { //mapTask
		args.IntermediateFileNames = intermediateFileNames
	}
	reply := TaskCompletionReply{}
	ok := call("Coordinator.TaskCompletion", &args, &reply)
	if ok {
		fmt.Printf("reply.Success %v\n", reply.Success)
	} else {
		return reply, fmt.Errorf("call failed")
	}
	return reply,nil
}

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
