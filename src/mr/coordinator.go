package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)


type Coordinator struct {
	// Your definitions here
	nReducer int
	files []string
	workerTaskMap map[string]string //workerId -> fileName
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c* Coordinator) AskTask(args *AskTaskArgs, reply *TaskReply) error {
	//根据worker的类型，分配任务
	//如果是map任务，需要分配文件名
	//如果是reduce任务，需要分配文件名和nReducer
	//如果没有任务，返回空
	if args.WorkerType == 0 {
		//map任务
		if len(c.files) > 0 {
			reply.FileName = c.files[0]
			c.files = c.files[1:]
			c.workerTaskMap[args.WorkerId] = reply.FileName
		} else {
			reply.FileName = ""
		}
	} else {
		//reduce任务
		if len(c.workerTaskMap) > 0 {
			reply.FileName = c.workerTaskMap[args.WorkerId]
			reply.NReducer = c.nReducer
		} else {
			reply.FileName = ""
		}
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//每一个Mapper产生的 K-V数组，需要由nReduce个Reducer来处理.
// files 需要处理的文件名称slice
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.files = files
	c.nReducer = nReduce
	//启动mater节点的http服务
	c.server()
	return &c
}
