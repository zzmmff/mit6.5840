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
type AskTaskArgs struct {
	WorkerId   string
	WorkerType int // 0: map, 1: reduce
}

type TaskReply struct {
	FileName        string //文件名称
	WorkerType      int    // 0: map, 1: reduce
	NReducer        int    //reduce的数量
	TaskNumberIndex int    //任务编号
}

type TaskCompletion struct {
	WorkerType           int    // 0 map 1 reduce
	WorkerId 			 string
	IntermediateFileNames []string //the intermediate file name only for map worker
	TaskNumberIndex      int    //任务编号
}

type TaskCompletionReply struct {
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
