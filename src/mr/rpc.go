package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// register to get worker id

type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerId int
}

// worker need to request task

type ReqTaskArgs struct {
	WorkerId int
}

type ReqTaskReply struct {
	Task *Task
}

// when the worker finish its work, it needs to report the result to the master

type ReportTaskArgs struct {
	Done     bool
	TaskType TaskType
	WorkerId int
	Seq      int
	// 需要定位任务本身，待添加
}

type ReportTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
