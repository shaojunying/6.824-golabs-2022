package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerMapTask struct {
	FileId   int // 文件序号 中间文件的格式为 mr-{FileId}-{SliceId}
	Filename string
	nReduce  int
}

type WorkerTask struct {
	Type     int // 任务类型
	Index    int
	State    int    // 任务状态
	Filename string // 文件名，只对MAP任务有效

	nMap    int
	nReduce int
}

type WorkerReduceTask struct {
	SliceId int // 同上
	NFile   int
}

const midFilenameFmt = "mr-%d-%d"

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for true {
		var reply *AskTaskReply
		reply, err := AskTask()
		if err != nil {
			continue
		}
		if reply.Code == ALL_FINISHED {
			break
		}
		if reply.Code == WAIT {
			log.Printf("睡眠 %v 秒", TIMEOUT)
			time.Sleep(TIMEOUT * time.Second)
			continue
		}
		// 构造任务对象
		task := extractTask(reply)

		// 执行任务
		if task.Type == MAP {
			_ = processMap(task, mapf)
		} else {
			_ = processReduce(task, reducef)
		}
		// 提交任务
		_ = submitTask(task)
	}
}

func processReduce(task WorkerTask, reducef func(string, []string) string) error {
	// 合并所有task.SliceId文件
	kvPairs := getKeyValueArray(task.nMap, task.Index)

	// 解析数据
	sort.Sort(ByKey(kvPairs))

	oname := fmt.Sprintf("mr-out-%d", task.Index)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kvPairs) {
		j := i + 1
		for j < len(kvPairs) && kvPairs[j].Key == kvPairs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvPairs[k].Value)
		}
		output := reducef(kvPairs[i].Key, values)
		_, _ = fmt.Fprintf(ofile, "%v %v\n", kvPairs[i].Key, output)
		i = j
	}
	err := ofile.Close()
	return err
}

// 获取sliceId对应的所有K,V对
func getKeyValueArray(nMap int, sliceId int) []KeyValue {
	var kvPairs []KeyValue
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf(midFilenameFmt, i, sliceId)
		content, _ := ioutil.ReadFile(filename)
		tmp := parseContent(string(content))
		kvPairs = append(kvPairs, tmp...)
	}
	return kvPairs
}

func processMap(task WorkerTask, mapf func(string, string) []KeyValue) error {
	content, err := ioutil.ReadFile(task.Filename)
	if err != nil {
		return err
	}
	kvPairs := mapf(task.Filename, string(content))
	// 将其放入nReduce个桶中
	buckets := make([][]string, task.nReduce)
	for _, kvPair := range kvPairs {
		line := fmt.Sprintf("%s %s", kvPair.Key, kvPair.Value)
		buckets[ihash(kvPair.Key)%task.nReduce] = append(buckets[ihash(kvPair.Key)%task.nReduce], line)
	}
	// 将内容写入到文件中
	for id, bucket := range buckets {
		content := strings.Join(bucket, "\n")
		outFilename := fmt.Sprintf(midFilenameFmt, task.Index, id)
		_ = ioutil.WriteFile(outFilename, []byte(content), 0644)
	}
	return nil
}

func submitTask(task WorkerTask) error {
	args := SubmitTaskArgs{
		Type:  task.Type,
		Index: task.Index,
	}
	reply := SubmitTaskReply{}
	ok := call("Coordinator.SubmitTask", &args, &reply)
	if ok {
		return nil
	} else {
		return errors.New("call failed!\n")
	}
}

func extractTask(reply *AskTaskReply) (task WorkerTask) {
	task = WorkerTask{
		Type:     reply.Type,
		Index:    reply.Index,
		Filename: reply.Filename,
		nMap:     reply.NMap,
		nReduce:  reply.NReduce,
	}
	return task
}

func parseContent(content string) []KeyValue {
	data := strings.Split(content, "\n")
	res := make([]KeyValue, 0)
	for _, line := range data {
		line := strings.Fields(line)
		if len(line) < 2 {
			log.Println("错误的行格式", line)
			continue
		}
		kvPair := KeyValue{Key: line[0], Value: line[1]}
		res = append(res, kvPair)
	}
	return res
}

func AskTask() (*AskTaskReply, error) {
	args := AskTaskArgs{}
	reply := AskTaskReply{}
	ok := call("Coordinator.AskTask", &args, &reply)
	if ok {
		return &reply, nil
	} else {
		return nil, errors.New("call failed!\n")
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
