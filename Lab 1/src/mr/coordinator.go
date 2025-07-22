package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapStatus         map[string]int 
    mapTaskId         int
    reduceStatus      map[int]int
    nReducer          int 
    intermediateFiles map[int][]string 
    mu                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetJobs(args *MRArgs, reply *MRReply) error {
	for key, value := range c.mapStatus {
		if value == 0 {
			c.mu.Lock()
			defer c.mu.Unlock()
			reply.MapJob = &MapJob{
				FileName:     key,
				MapJobIdx:    c.mapTaskId,
				ReducerCount: c.nReducer,
			}
			reply.WaitTime = 0
			reply.ReduceJob = nil
			c.mapStatus[key] = 1
			jobID := c.mapTaskId
			c.mapTaskId++

			go func(filename string, jobID int) {
				timer := time.NewTimer(10 * time.Second)
				<-timer.C
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.mapStatus[filename] == 1 {
					c.mapStatus[filename] = 0
					fmt.Printf("Job %d for file %s timed out, resetting status\n", jobID, filename)
				} else {
					fmt.Printf("Map Jobs %d Finished \n", jobID)
				}
			}(key, jobID)
			return nil
		}
	}

	for _, value := range c.mapStatus {
		if value == 1 {
			c.mu.Lock()
			defer c.mu.Unlock()
			reply.WaitTime = 2
			reply.MapJob = nil
			reply.ReduceJob = nil
			return nil
		}
	}

	for key, value := range c.reduceStatus {
		if value == 0 && len(c.intermediateFiles[key]) > 0 {
			c.mu.Lock()
			defer c.mu.Unlock()
			reply.ReduceJob = &ReduceJob{
				Files: c.intermediateFiles[key],
				TaskNum: "test",
				ReducerNum: key,
			}
			reply.WaitTime = 0
			reply.MapJob = nil
			c.reduceStatus[key] = 1

			go func(jobID int) {
				timer := time.NewTimer(10 * time.Second)
				<-timer.C
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.reduceStatus[jobID] == 1 {
					c.reduceStatus[jobID] = 0
					fmt.Printf("Reduce Job %d timed out, resetting status\n", jobID)
				} else {
					fmt.Printf("Reduce Job %d Finished \n", jobID)
				}
			}(key)

			return nil
		}
	}

	return nil
}

func (c *Coordinator) SubmitJobStatus(args *MRStatusArgs, reply *MRStatusReply) error {
	if args.IsComplete {
		if args.TaskType == "mapJob" {
			c.mu.Lock()
			defer c.mu.Unlock()
			for key, value := range args.IntermediateFiles {
				c.intermediateFiles[key] = append(c.intermediateFiles[key], value...)
			}
			c.mapStatus[args.FileName] = 2;
			reply.Gg = true
			return nil
		}

		if args.TaskType == "reduceJob" {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.reduceStatus[args.ReduceNum] = 2;
			c.intermediateFiles[args.ReduceNum] = nil
			reply.Gg = true
			return nil
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

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	// fmt.Printf("%v \n", c.mapStatus);
	// fmt.Printf("%v \n", c.reduceStatus);
	for _, value := range c.mapStatus {
		if value != 2 {
			return false
		}
	}

	for key, value := range c.reduceStatus {
		if value != 2 && c.intermediateFiles[key] != nil {
			return false
		}
	}

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapStatus: make(map[string]int),
		mapTaskId: 0,
		reduceStatus: make(map[int]int),
		nReducer: nReduce,
		intermediateFiles: make(map[int][]string),
		mu: sync.Mutex{},
	}

	// Your code here.
	for idx := 0; idx < len(files); idx++ {
		c.mapStatus[files[idx]] = 0
	}

	for idx := 0; idx < nReduce; idx++ {
		c.reduceStatus[idx] = 0
	}

	c.server()
	return &c
}
