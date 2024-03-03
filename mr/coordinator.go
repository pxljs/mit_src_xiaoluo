package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Coordinator 协调者，即服务端，Master
type Coordinator struct {
	Files         []string   //map任务文件名
	ReduceNumber  int        //Reduce的数量，需要指定，Map任务的数量就是files的长度
	MapJobList    []*Job     //分发给worker的Map任务
	ReduceJobList []*Job     //分发给worker的Reduce任务
	JobListMutex  sync.Mutex //服务端的锁，在访问或者修改任务状态时需要加锁
	CurrentStates int32      //1-map,2-reduce,3-finish
}

// Job 客户端Job信息
type Job struct {
	//静态信息
	FileName     string //map处理文件名
	ListIndex    int    //任务列表中的index下标
	ReduceID     int    //reduce任务号，从0-（N-1）
	ReduceNumber int    //reduce任务个数
	JobType      int    //任务类型，1-map，2-reduce
	//动态信息
	JobFinished    bool  //任务是否分配正确
	StartTime      int64 //任务的分配时间（初始值为0），如果下次检查是否可分配时间超过2s,就当失败处理，重新分配
	FirstStartTime int64 //第一次被分配的时间
	FinishedTime   int64 //完成时间
	FetchCount     int   //任务分配次数，用于统计信息
}

// JobFetchReq 获取任务的请求体
type JobFetchReq struct {
	ClientID int
	// 不需要有东西。如果服务端需要记录客户端信息，可以传入客户端id等信息，微服务mesh层应该做的事情。
}

// JobFetchResp 获取任务的返回体
type JobFetchResp struct {
	NeedWait bool // 是否需要等待下次轮询任务， 因为服务端可能已经分发完map任务，但Map阶段还没结束[map任务正在被执行]
	Job           // 继承写法,相当于把Job结构体里面的所有属性写到这里
}

// JobDoneReq 任务完成提交的请求体
type JobDoneReq struct {
	Job // 继承写法,相当于把Job结构体里面的所有属性写到这里
}

// JobDoneResp 任务完成提交的返回体
type JobDoneResp struct {
	//不需要任何内容
}

// 常量信息
const (
	// InReduce 当前系统所处的mr阶段
	InReduce    = 1
	InMap       = 2
	AllFinished = 3
	// JobTimeoutSecond 任务的超时时间，超过 xx 当做客户端crash了，重新分配任务。 如果设置的太小，则 map count 检测程序不通过。
	JobTimeoutSecond = 5
	// MapJob 任务类型
	MapJob    = 1
	ReduceJob = 2
	// BackgroundInterval 后台线程的运行间隔
	BackgroundInterval = 500 * time.Millisecond //0.5s
	// MasterLogPrefix 日志前缀
	MasterLogPrefix = "master_log: "
	WorkerLogPrefix = "worker_log: "
)

// 初始化 Map 任务
func (c *Coordinator) initMapJob() {
	c.MapJobList = make([]*Job, 0, len(c.Files))
	for i, file := range c.Files {
		c.MapJobList = append(c.MapJobList, &Job{
			JobType:      MapJob,
			FileName:     file,
			ListIndex:    i,
			ReduceNumber: c.ReduceNumber,
		})
	}
	fmt.Printf(MasterLogPrefix+"master init finished %+v\n", toJsonString(c))
}

// 序列化一个结构体对象, 打印日志时候用
func toJsonString(inter interface{}) string {
	bytes, _ := json.Marshal(inter)
	return string(bytes)
}

// InitCoordinator 初始化函数
func (c *Coordinator) InitCoordinator(files []string, nReduce int) {
	c.Files = files
	c.ReduceNumber = nReduce
	c.CurrentStates = InMap
	c.initMapJob()
}

// 生成reduce任务，调用前需要持有锁
func (c *Coordinator) generateReduceMap() {
	reduceJobList := make([]*Job, 0, c.ReduceNumber)
	for i := 0; i < c.ReduceNumber; i++ {
		reduceJobList = append(reduceJobList, &Job{
			ListIndex:    i,
			ReduceID:     i,
			ReduceNumber: c.ReduceNumber,
			JobType:      ReduceJob,
		})
	}
	c.ReduceJobList = reduceJobList
	fmt.Printf(MasterLogPrefix+" generateReduceMap finished,c.ReduceJobList: %+v\n", toJsonString(reduceJobList))
}

// JobFetch 客户端索要Job，服务端处理
func (c *Coordinator) JobFetch(req *JobFetchReq, resp *JobFetchResp) error {
	c.JobListMutex.Lock()         //上锁
	defer c.JobListMutex.Unlock() //固定写法
	currentTime := time.Now().UnixMilli()
	// 看看有哪些没完成的任务，分配出去
	jobList := c.MapJobList
	switch c.CurrentStates {
	case AllFinished:
		return nil
	case InMap:
		jobList = c.MapJobList
	case InReduce:
		jobList = c.ReduceJobList
	}
	// 遍历所有Map/Reduce任务列表
	for _, job := range jobList {
		// 任务没完成，且是第一次运行或者之前超时了[防止重复分发](超过5s，认为是超时)
		if !job.JobFinished && (job.StartTime == 0 || (currentTime-job.StartTime)/1000 > int64(JobTimeoutSecond)) {
			job.FetchCount++
			job.StartTime = currentTime
			// 记录第一次运行时的时间
			if job.FirstStartTime == 0 {
				job.FirstStartTime = currentTime
			}
			// 赋值给resp，即分发给请求源:客户端
			fmt.Printf(logTime()+MasterLogPrefix+"—————————————— 分发出任务:%v，处理当前任务的是%v号客户端 \n", job.ListIndex, req.ClientID)
			resp.Job = *job
			return nil
		}
	}
	// 只要不是所有任务都是已完成状态，就让work继续等，否则无法通过early_exit测试程序
	if c.CurrentStates != AllFinished {
		resp.NeedWait = true
	}
	return nil
}

// JobDone 客户端提交任务的处理函数
func (c *Coordinator) JobDone(req *JobDoneReq, resp *JobDoneResp) error {
	c.JobListMutex.Lock()
	defer c.JobListMutex.Unlock()
	finished := req.JobFinished
	jobType := "Map任务"
	if req.JobType == ReduceJob {
		jobType = "Reduce任务"
	}
	fmt.Printf(logTime()+MasterLogPrefix+"—————————————— 完成%s任务耗时:%v 毫秒[从第一次被分发], %v毫秒[最近一次]，分配次数%v —————————————— \n",
		jobType, time.Now().UnixMilli()-req.FirstStartTime, time.Now().UnixMilli()-req.StartTime, req.FetchCount)
	switch c.CurrentStates {
	// 将任务的状态改为是否已完成，后台定时线程会扫描此状态
	case InMap:
		c.MapJobList[req.ListIndex].JobFinished = finished
	case InReduce:
		c.ReduceJobList[req.ListIndex].JobFinished = finished
	}
	return nil
}

// Background 后台扫描，任务是否都完成，是否有卡死的任务，是否进入下一个阶段，每隔100毫秒扫描一次
func (c *Coordinator) Background() {
	// 保证原子性，使用 atomic.LoadInt 进行判断
	for atomic.LoadInt32(&c.CurrentStates) != AllFinished {
		// 循环遍历任务
		c.JobListMutex.Lock()
		isAllJobDone := true
		leftCount := 0 //当前s剩余的map/reduce任务数
		switch c.CurrentStates {
		case InMap:
			for _, job := range c.MapJobList {
				if !job.JobFinished {
					isAllJobDone = false
					leftCount++
				}
			}
			fmt.Printf(logTime()+MasterLogPrefix+"—————————————— 还剩 %v 个 map 任务\n", leftCount)
			// map 任务都做完了，流转到 reduce 状态, 而且要生成 reduce 任务
			if isAllJobDone {
				leftCount = 0
				atomic.StoreInt32(&c.CurrentStates, InReduce)
				c.generateReduceMap()
				fmt.Printf(MasterLogPrefix+"background: CurrentStates change from %v to %v\n", InMap, InReduce)
			}
		case InReduce:
			for _, job := range c.ReduceJobList {
				if !job.JobFinished {
					isAllJobDone = false
					leftCount++
				}
			}
			fmt.Printf(logTime()+MasterLogPrefix+"—————————————— 还剩 %v 个 reduce 任务\n", leftCount)
			// reduce 任务都做完了，流转到 结束 状态
			if isAllJobDone {
				atomic.StoreInt32(&c.CurrentStates, AllFinished)
				fmt.Printf(MasterLogPrefix+"background: CurrentStates change from %v to %v\n", InReduce, AllFinished)
			}
		}
		c.JobListMutex.Unlock()
		time.Sleep(5 * BackgroundInterval)
	}
}

func logTime() string {
	return fmt.Sprint((time.Now().UnixNano()/1e6)%10000, " ")
}
func (c *Coordinator) Done() bool {
	ret := false
	for atomic.LoadInt32(&c.CurrentStates) != AllFinished {
		time.Sleep(300 * time.Millisecond)
	}
	ret = true
	return ret
}

// RPC handlers for the worker to call.
// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		return
	}
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

// MakeCoordinator 创建服务端
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.InitCoordinator(files, nReduce)
	go c.Background()
	c.server()
	return &c
}
