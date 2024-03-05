package mr

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue Map functions return a slice of KeyValue.
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
		if err = job.DoReduceJob(reducef, mapf); err != nil {
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
	fmt.Printf(WorkerLogPrefix + "CallFetchJob job req 请求任务\n")
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
	str, _ := os.ReadFile(job.FileName)
	//将整个文件转换成键值对
	KeyValueList := mapf(job.FileName, string(str))
	//创建新文件，个数为ReduceNumber
	filelist := make([]*os.File, 0)
	for i := 0; i < job.ReduceNumber; i++ {
		filename := "MapTask" + strconv.Itoa(job.ListIndex) + "--file" + strconv.Itoa(i) + ".txt"
		file, _ := os.Create(filename)
		fmt.Printf("已创建文件%v\n", filename)
		filelist = append(filelist, file)
	}
	kvlist := make([][]KeyValue, job.ReduceNumber)
	for i := 0; i < len(KeyValueList); i++ {
		index := ihash(KeyValueList[i].Key) % job.ReduceNumber
		kvlist[index] = append(kvlist[index], KeyValueList[i])
	}
	for j := 0; j < job.ReduceNumber; j++ {
		sort.Sort(ByKey(kvlist[j]))
		for _, kv := range kvlist[j] {
			filelist[j].WriteString(fmt.Sprint(kv.Key, " ", kv.Value, "\n"))
		}
		fmt.Printf("文件%v已排序完成\n", filelist[j].Name())
	}
	return nil
}

func (job *Job) DoReduceJob(reducef func(string, []string) string, mapf func(string, string) []KeyValue) error {
	//fetch，从分区0开始依次读文件
	kvlist := make([][]KeyValue, job.MapTasksNum)
	fmt.Printf("kvlist的长度为:%v\n", len(kvlist))
	for i := 0; i < job.MapTasksNum; i++ {
		filename := "MapTask" + strconv.Itoa(i) + "--file" + strconv.Itoa(job.ReduceID) + ".txt"
		str, err := os.ReadFile(filename)
		if err != nil {
			fmt.Printf("无法读取文件 %s: %v\n", filename, err)
			continue
		}
		fmt.Printf("当前打开的文件为:%v\n", filename)
		fmt.Printf("str的长度为:%v\n", len(str))
		keyValueList := mapf(filename, string(str))
		fmt.Printf("keyValueList的长度为:%v\n", len(keyValueList))
		for _, kv := range keyValueList {
			kvlist[i] = append(kvlist[i], kv)
		}
	}
	fmt.Printf("kvlist的长度为:%v\n", len(kvlist))
	res := mergeK(kvlist)
	fmt.Printf("res的长度为:%v\n", len(res))
	filename := "mr-out-" + strconv.Itoa(job.ReduceID)
	file, _ := os.Create(filename)
	fmt.Printf("文件%v已创建\n", file.Name())
	i := 0
	for i < len(res) {
		j := i + 1
		for j < len(res) && res[j].Key == res[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, res[k].Value)
		}
		output := reducef(res[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", res[i].Key, output)
		i = j
	}
	return nil
}

// 合并n个内部有序键值组，可依次合并两个，进行n-1次合并即可
func mergeK(kvlist [][]KeyValue) []KeyValue {
	var pre, cur []KeyValue
	n := len(kvlist)
	for i := 0; i < n; i++ {
		if i == 0 {
			pre = kvlist[i]
			continue
		}
		cur = kvlist[i]
		pre = merge(pre, cur)
	}
	return pre
}

func merge(l1, l2 []KeyValue) []KeyValue {
	var l []KeyValue
	i, j := 0, 0
	for i < len(l1) && j < len(l2) {
		if l1[i].Key <= l2[j].Key {
			l = append(l, l1[i])
			i++
		} else {
			l = append(l, l2[j])
			j++
		}
	}
	for i < len(l1) {
		l = append(l, l1[i])
		i++
	}
	for j < len(l2) {
		l = append(l, l2[j])
		j++
	}
	return l
}
