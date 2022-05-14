package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

const (
	NEW_TASK     = 0 // 成功分配新任务
	WAIT         = 1 // 没有未开始的任务，但还有任务未执行完毕，先稍等一下
	ALL_FINISHED = 2 // 所有任务都已执行完毕
)

const (
	MAP    = 0
	REDUCE = 1
	EMPTY  = 2
)

const (
	SUBMIT_SUCCESS = 1
	REPEAT_SUBMIT  = 2
)

type AskTaskArgs struct {
}

type AskTaskReply struct {
	Code     int
	Type     int // Map任务还是Reduce任务
	Index    int
	Filename string
	NReduce  int
	//task	CoordinatorTask
	NMap int
}

type SubmitTaskArgs struct {
	Type  int // Map任务还是Reduce任务
	Index int
}

type SubmitTaskReply struct {
	Code int // 是否提交成功
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
