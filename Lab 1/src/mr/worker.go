package mr

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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
// Have a loop here, ask for jobs and based on resp
// perform the task broski
// ensure we return intermediate files and save KVA to files.
		for {
			args := MRArgs{}
			args.Req = "request"
			reply := MRReply{}
			ok := call("Coordinator.GetJobs", &args, &reply)
			if ok {
				if reply.WaitTime > 0 {
					time.Sleep(time.Duration(reply.WaitTime) * time.Second)
					fmt.Printf("Waiting\n")
					continue
				}

				if reply.MapJob != nil {
					intermediate := []KeyValue{}
					intermediateFiles := make(map[int][]string)
					file, _ := os.Open(reply.MapJob.FileName)
					content, _ := ioutil.ReadAll(file)
					val := mapf(reply.MapJob.FileName, string(content))
					intermediate = append(intermediate, val...)

					output := make(map[int][]KeyValue)

					for _, value := range intermediate {
						partitionKey := ihash(value.Key) % reply.MapJob.ReducerCount
						output[partitionKey] = append(output[partitionKey], value)
					}

					for partitionIndex, kvPairs := range output {
						// Create temp file
						tmpFile, err := ioutil.TempFile("", "mr-temp-*")
						if err != nil {
							log.Fatal("Error creating temp file:", err)
						}
						tmpName := tmpFile.Name()
						
						enc := json.NewEncoder(tmpFile)
						
						for _, kv := range kvPairs {
							err := enc.Encode(&kv)
							if err != nil {
								log.Fatal("Error encoding JSON:", err)
							}
						}
						
						tmpFile.Close()
						
						filename := fmt.Sprintf("mr-%d-%d", reply.MapJob.MapJobIdx, partitionIndex)
						err = os.Rename(tmpName, filename)
						if err != nil {
							log.Fatal("Error renaming file:", err)
						}
						intermediateFiles[partitionIndex] = append(intermediateFiles[partitionIndex], filename)
					}

					statusArgs := MRStatusArgs{}
					statusReply := MRStatusReply{}

					statusArgs.IntermediateFiles = intermediateFiles
					statusArgs.IsComplete = true
					statusArgs.TaskType = "mapJob"
					statusArgs.FileName = reply.MapJob.FileName
					jobsFinished := call("Coordinator.SubmitJobStatus", &statusArgs, &statusReply);

					if !jobsFinished {
						if (statusReply.Gg) {
							fmt.Printf("Jobs finished")
						} else {
							fmt.Printf("Error finishing job")
						}
					}

					time.Sleep(time.Duration(1) * time.Second)
					continue
				}

				if reply.ReduceJob != nil {
					intermediateFiles := reply.ReduceJob.Files;
					keyValuePairs := []KeyValue{}
					for _, fileName := range intermediateFiles {
						file, _ := os.Open(fileName)
						dec := json.NewDecoder(file)
						for {
							var kv KeyValue
							if err := dec.Decode(&kv); err != nil {
								break
							}
							keyValuePairs = append(keyValuePairs, kv)
						}
					}

					sort.Sort(ByKey(keyValuePairs))

					tmpFile, err := ioutil.TempFile("", "mr-temp-*")
					if err != nil {
						log.Fatal("Error creating temp file:", err)
					}
					tmpName := tmpFile.Name()

					i := 0
					for i < len(keyValuePairs) {
						j := i + 1
						for j < len(keyValuePairs) && keyValuePairs[j].Key == keyValuePairs[i].Key {
							j++
						}
						values := []string{}
						for k := i; k < j; k++ {
							values = append(values, keyValuePairs[k].Value)
						}
						output := reducef(keyValuePairs[i].Key, values)

						// this is the correct format for each line of Reduce output.
						fmt.Fprintf(tmpFile, "%v %v\n", keyValuePairs[i].Key, output)

						i = j
					}
					filename := fmt.Sprintf("mr-out-%d", reply.ReduceJob.ReducerNum)
					err = os.Rename(tmpName, filename)
					if err != nil {
						log.Fatal("Error renaming file:", err)
					}

					statusArgs := MRStatusArgs{}
					statusReply := MRStatusReply{}
					statusArgs.IsComplete = true
					statusArgs.TaskType = "reduceJob"
					statusArgs.ReduceNum = reply.ReduceJob.ReducerNum
					jobsFinished := call("Coordinator.SubmitJobStatus", &statusArgs, &statusReply);

					if !jobsFinished {
						if (statusReply.Gg) {
							fmt.Printf("Jobs finished")
						} else {
							fmt.Printf("Error finishing job")
						}
					}

					time.Sleep(time.Duration(1) * time.Second)
					continue
				}
			}
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
