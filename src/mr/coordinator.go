package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TODO  = 0
	DOING = 1
	DONE  = 2
)

const TIMEOUT = 10

type CoordinatorTask struct {
	Type     int // 任务类型
	Index    int
	State    int    // 任务状态
	Filename string // 文件名，只对MAP任务有效

	Timer *time.Timer
}

type Coordinator struct {
	Stage int // 当前处于什么阶段 MAP还是REDUCE

	tasks         []CoordinatorTask // 接下来要处理的任务(根据stage来确定是Map还是Reduce)
	nFinishedTask int               // 已完成的任务数，避免频繁的重复遍历

	nMap    int // Map任务数
	nReduce int // Reduce任务数

	lock sync.Mutex // 互斥锁
}

// AskTask Worker调用此函数获取任务
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Stage == EMPTY {
		// 所有任务都已执行完毕，
		log.Printf("所有任务都已执行完毕\n")
		reply.Code = ALL_FINISHED
		return nil
	}
	// 获取一个未开始的任务
	task := c.getTodoTask()
	if task == nil {
		log.Printf("没有还未开始的任务，需要稍等\n")
		reply.Code = WAIT
		return nil
	}
	// 成功获取到一个任务
	log.Printf("将分配一个任务: 类型: %v, Index: %v", getType(task.Type), task.Index)
	task.State = DOING
	reply.Code = NEW_TASK
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap
	reply.Index = task.Index
	reply.Type = task.Type
	reply.Filename = task.Filename
	task.Timer = time.NewTimer(TIMEOUT * time.Second)
	go c.addTimer(task)
	return nil
}

func (c *Coordinator) addTimer(task *CoordinatorTask) {
	<-task.Timer.C
	c.lock.Lock()
	defer c.lock.Unlock()
	task.State = TODO
}

func getType(Type int) string {
	if Type == MAP {
		return "MAP"
	}
	return "REDUCE"
}

// SubmitTask 完成任务
func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	// 首先停止相应的定时器
	index := args.Index
	task := c.tasks[index]
	// 之后需要修改任务的状态
	c.lock.Lock()
	defer c.lock.Unlock()
	if task.Timer != nil {
		task.Timer.Stop()
	}
	// 判断任务是否已提交过
	if c.Stage != task.Type {
		// 当前阶段和任务类型不匹配，不更新
		reply.Code = REPEAT_SUBMIT
		return nil
	}
	if task.State == DONE {
		reply.Code = REPEAT_SUBMIT
		return nil
	}
	// 任务是第一次提交
	task.State = DONE
	c.nFinishedTask++
	log.Printf("成功提交任务: Type: %v, Index: %v", getType(task.Type), task.Index)
	c.transit()
	reply.Code = SUBMIT_SUCCESS
	return nil
}

func (c *Coordinator) transit() {
	if c.Stage == MAP {
		if c.nFinishedTask == c.nMap {
			// 全部Map任务都已完成
			c.initReduceTasks()
			c.nFinishedTask = 0
			c.Stage = REDUCE
		}
	} else if c.Stage == REDUCE {
		if c.nFinishedTask == c.nReduce {
			c.Stage = EMPTY
			c.nFinishedTask = 0
			c.tasks = make([]CoordinatorTask, 0)
		}
	}
}

func (c *Coordinator) initReduceTasks() {
	c.tasks = make([]CoordinatorTask, c.nReduce)
	for i := range c.tasks {
		c.tasks[i] = CoordinatorTask{
			Type:  REDUCE,
			Index: i,
			State: TODO,
		}
	}
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
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.Stage == EMPTY
}

// 获取还未执行的Map任务
func (c *Coordinator) getTodoTask() *CoordinatorTask {
	for i := range c.tasks {
		task := &c.tasks[i]
		if task.State == TODO {
			return task
		}
	}
	return nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	tasks := make([]CoordinatorTask, len(files))

	for i := range tasks {
		tasks[i] = CoordinatorTask{Type: MAP, Index: i, State: TODO, Filename: files[i]}
	}

	c := Coordinator{
		Stage:         MAP,
		tasks:         tasks,
		nFinishedTask: 0,
		nMap:          len(files),
		nReduce:       nReduce,
	}

	c.server()
	return &c
}
