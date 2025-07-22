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
type MapJob struct {
	FileName string
	MapJobIdx int
	ReducerCount int
}

type ReduceJob struct {
	Files []string
	TaskNum string
	ReducerNum int
}

type MRArgs struct {
	Req string
}

type MRReply struct {
	MapJob *MapJob
	ReduceJob *ReduceJob
	WaitTime int
	Finish bool
}

type MRStatusArgs struct {
	IsComplete bool
	TaskType string
	FileName string
	IntermediateFiles map[int][]string
	ReduceNum int
}

type MRStatusReply struct {
	Gg bool
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
