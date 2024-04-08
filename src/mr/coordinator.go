package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	TaskId   int
	NReduce  int
	NMap     int
	TaskType int
	FileName string
}

type RequestArgs struct {
	RequestNum int
	TaskId     int
}
type Coordinator struct {
	// Your definitions here.
	mapTaskChan          []*Task
	reduceTaskChan       []*Task
	mapBeginNotFinish    map[int]int64 // 存储任务开始但未完成的map任务  value为任务开始时间
	reduceBeginNotFinish map[int]int64
	files                []string
	finishMapNumber      int         // 完成的map 任务数量
	FinishReduceNumber   int         // 完成的reduce  任务数量
	nReduce              int         // reduce任务数量
	reduceTask           map[int]int // 还未开始的reduceTask
	mapTask              map[int]int // 还未开始的map task
	lock                 sync.Mutex
}

// start a thread that listens for RPCs from worker.go
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

// 添加任务  分配map任务
func (c *Coordinator) AppendMapTask() {
	for i := 0; i < len(c.files); i++ {
		_, ok := c.mapTask[i]
		if ok {
			maptask := Task{
				i,
				c.nReduce,
				len(c.files),
				0, // 表示map任务
				c.files[i],
			}
			c.mapTaskChan = append(c.mapTaskChan, &maptask)
			c.mapBeginNotFinish[i] = time.Now().Unix()
			delete(c.mapTask, i)
			break
		}
	}
}

// 分配reduce任务
func (c *Coordinator) AppendReduceTask() {
	for i := 0; i < c.nReduce; i++ {
		_, ok := c.reduceTask[i]
		if ok {
			reducetask := Task{
				i,
				c.nReduce,
				len(c.files),
				1, // 表示reduce
				"",
			}
			c.reduceTaskChan = append(c.reduceTaskChan, &reducetask)
			c.reduceBeginNotFinish[i] = time.Now().Unix()
			delete(c.reduceTask, i)
		}
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.finishMapNumber == len(c.files) && c.FinishReduceNumber == c.nReduce {
		ret = true
	}
	return ret
}

// 超时判断
func (c *Coordinator) TaskTimeout() {
	for key, value := range c.mapBeginNotFinish {
		if time.Now().Unix()-value > 10 {
			fmt.Println(time.Now().Unix(), "   ", value)
			delete(c.mapBeginNotFinish, key)
			c.mapTask[key] = 1
		}
	}

	for key, value := range c.reduceBeginNotFinish {
		if time.Now().Unix()-value > 10 {
			fmt.Println(time.Now().Unix(), "   ", value)
			delete(c.reduceBeginNotFinish, key) // 超时 删除该任务
			c.reduceTask[key] = 1               // 重新分配
		}
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *RequestArgs, reply *Task) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.TaskTimeout()
	if args.RequestNum == 1 { //map任务完成
		c.finishMapNumber++
		delete(c.mapBeginNotFinish, args.TaskId)
	} else if args.RequestNum == 2 { // reduce任务完成
		c.FinishReduceNumber++
		delete(c.reduceBeginNotFinish, args.TaskId)
	} else {
		if len(c.mapTaskChan) == 0 {
			c.AppendMapTask()
		}
		if len(c.reduceTaskChan) == 0 && c.finishMapNumber == len(c.files) {
			c.AppendReduceTask()
		}
		if len(c.mapTaskChan) > 0 {
			reply.TaskType = c.mapTaskChan[0].TaskType
			reply.NReduce = c.mapTaskChan[0].NReduce
			reply.FileName = c.mapTaskChan[0].FileName
			reply.TaskId = c.mapTaskChan[0].TaskId
			reply.NMap = c.mapTaskChan[0].NMap
			c.mapTaskChan = c.mapTaskChan[1:]
		} else if len(c.reduceTaskChan) > 0 {
			reply.TaskType = c.reduceTaskChan[0].TaskType
			reply.TaskId = c.reduceTaskChan[0].TaskId
			reply.NReduce = c.reduceTaskChan[0].NReduce
			reply.NMap = c.reduceTaskChan[0].NMap
			reply.FileName = c.reduceTaskChan[0].FileName
			c.reduceTaskChan = c.reduceTaskChan[1:]
		} else if c.Done() {
			reply.TaskType = 3
		} else {
			reply.TaskType = 2
		}

	}
	//reply.Y = args.X + 1
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTask = make(map[int]int)
	c.reduceTask = make(map[int]int)
	c.mapBeginNotFinish = make(map[int]int64)
	c.reduceBeginNotFinish = make(map[int]int64)
	for i := 0; i < len(files); i++ {
		c.reduceTask[i] = 1
	}
	c.mapTaskChan = make([]*Task, 0)
	c.reduceTaskChan = make([]*Task, 0)
	c.server()
	return &c
}
