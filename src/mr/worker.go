package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// 先定义一个worker结构体
type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()
	// 接下来还需要注册和运行

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func (w *worker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	if success := call("Master.RegWorker", &args, &reply); !success {
		log.Printf("worker fail to register")
	}
	w.id = reply.WorkerId
	//log.Printf("reply.WorkerId %v\n", reply.WorkerId)
}

func (w *worker) run() {
	// need to complete
	for {
		t := w.reqTask()
		if finish := w.doTask(t); finish {
			return
		}
	}
}

func (w *worker) doTask(t Task) bool {
	switch t.Type {
	case Map:
		w.doMapTask(t)
	case Reduce:
		w.doReduceTask(t)
	case Wait:
		time.Sleep(waitInterval)
	case Exit:
		//log.Printf("Worker %v get task fail, exit\n", w.id)
		return true
	default:
		//log.Printf("Worker %v get unknown task %v\n", w.id, t.Type)
	}
	return false
}

func (w *worker) reqTask() Task {
	args := ReqTaskArgs{}
	args.WorkerId = w.id
	reply := ReqTaskReply{}
	if success := call("Master.ReqTask", &args, &reply); !success {
		log.Println("Worker Request Task Fail")
		os.Exit(1)
	}
	//log.Printf("Worker %v Request Task %+v\n", w.id, reply.Task)
	return *reply.Task

}

// 执行不同任务的方法
func (w *worker) doMapTask(t Task) {
	content, err := ioutil.ReadFile(t.FileName)
	if err != nil {
		w.reportTask(t, false, err)
		return
	}
	kvs := w.mapf(t.FileName, string(content))
	reduces := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}
	// each idx refers to a reduces file
	// 对每一个读入的文件生成对应的中间文件
	for idx, kvList := range reduces {
		fileName := getReduceFileName(t.Seq, idx)
		f, err := os.Create(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range kvList {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
			}
		}
		if err := f.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	w.reportTask(t, true, nil)
}

func (w *worker) doReduceTask(t Task) {
	maps := make(map[string][]string)
	for idx := 0; idx < t.NMaps; idx++ {
		fileName := getReduceFileName(idx, t.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			log.Printf("reduceTask %+v read file error %+v\n", t.Seq, err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				//log.Printf("reduceTask %+v dec kv error %+v in file %+v\n", t.Seq, err, idx)
				break
			}
			if _, exist := maps[kv.Key]; !exist {
				// 分配好初始内存空间以避免后续不断对list进行扩展，影响执行效率
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	// 准备开始写入数据到mergeFile里面
	content := make([]string, 0, 100)
	for k, v := range maps {
		content = append(content, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	if err := ioutil.WriteFile(getMergeFileName(t.Seq), []byte(strings.Join(content, "\n")), 0600); err != nil {
		log.Printf("reduceTask %+v write kv to file error %+v\n", t.Seq, err)
		w.reportTask(t, false, err)
	}
	w.reportTask(t, true, nil)
}

func (w *worker) reportTask(t Task, done bool, err error) {
	if err != nil {
		log.Printf("worker %v do task wrong %+v\n", w.id, err)
	}
	args := ReportTaskArgs{}
	reply := ReportTaskReply{}
	args.TaskType = t.Type
	args.WorkerId = w.id
	args.Seq = t.Seq
	args.Done = done
	if success := call("Master.ReportTask", &args, &reply); !success {
		log.Printf("worker %v report task fail:%+v\n", w.id, args)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// 示例函数，没有使用
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	log.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// 获取master的socketName
	sockname := masterSock()
	// 获取一个rpc client
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
