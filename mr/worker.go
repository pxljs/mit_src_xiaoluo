package mr

import (
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0
	}
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	startTime := int64(0)
	for {
		startTime = time.Now().UnixNano()
		//索要任务
		job := CallFetchJob()
		//需要等待流转到reduce
		if job.NeedWait {
			time.Sleep(5 * BackgroundInterval)
			continue
		}
		//任务做完，则停止循环
		if job.FetchCount == 0 {
			fmt.Println(logTime() + WorkerLogPrefix + "任务已做完，worker退出")
			break
		}
		//做任务
		job.DoJob(mapf, reducef)
		//做完了，提交
		CallCommitJob(&JobDoneReq{job.Job})
		fmt.Println(WorkerLogPrefix+"一次worker循环耗时[毫秒]:", (time.Now().UnixNano()-startTime)/1e6)
		startTime++
	}
}

// DoJob 开始工作
func (job *Job) DoJob(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var err error
	switch job.JobType {
	case MapJob:
		if err = job.DoMapJob(mapf); err != nil {
			fmt.Println("DoMapJob_error ", err)
		}
	case ReduceJob:
		if err = job.DoReduceJob(reducef); err != nil {
			fmt.Println("DoReduceJob_error ", err)
		}
	}
	// 成功做完修改状态
	if err == nil {
		job.JobFinished = true
	}
	fmt.Printf(WorkerLogPrefix+"DoMapJob_finished %v\n ", toJsonString(job))
}

func CallFetchJob() JobFetchResp {
	req := JobFetchReq{}
	resp := JobFetchResp{}
	call("Coordinator.JobFetch", &req, &resp)
	fmt.Printf(WorkerLogPrefix+"CallFetchJob job resp %+v\n", resp)
	return resp
}

func CallCommitJob(job *JobDoneReq) {
	fmt.Printf(WorkerLogPrefix+"CallCommitJob job req %+v\n", *job)
	resp := JobDoneResp{}
	call("Coordinator.JobDone", job, &resp)
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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (job *Job) DoMapJob(mapf func(string, string) []KeyValue) error {
	//读文件,返回值str为该文件的内容
	str, err := os.ReadFile(job.FileName)
	if err != nil {
		return err
	}
	KeyValueList := mapf(job.FileName, string(str))
	sort.Sort(ByKey(KeyValueList))
	//创建空文件
	for i := 0; i < job.ReduceNumber; i++ {
		filename := fmt.Sprint("map", i, ".txt")
		file, err := os.Create(filename)
	}
	for i := 0; i < len(KeyValueList); i++ {
		io.WriteString(, KeyValueList[i].Key)
	}
	for j := 0; j < job.ReduceNumber; j++ {
		content, _ := os.ReadFile(filelist[j].Name())
		keyValueList := mapf(filelist[j].Name(), string(content))
		sort.Sort(ByKey(keyValueList))
	}
	return nil
}

func (job *Job) DoReduceJob(reducef func(string, []string) string) error {

	return nil
}
