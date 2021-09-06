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
	// 之后需要改回10
	maxTaskRunTime = time.Second * 5
)

type Master struct {
	// Your definitions here.
	files        []string // 据此可以推断出nMap
	nReduce      int
	workerNum    int
	mapTask      []Task
	mapFinish    int
	reduceFinish int
	reduceTask   []Task
	phase        TaskType // 用于辅助判断是map阶段还是reduce阶段
	mu           sync.Mutex
	done         bool
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	// 这里涉及了并发，需要加锁
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerNum += 1
	reply.WorkerId = m.workerNum
	return nil
}

func (m *Master) ReqTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.phase {
	case Map:
		// 传送map任务
		m.sendMapTask(reply)
	case Reduce:
		// 传送reduce任务
		m.sendReduceTask(reply)
	case Exit:
		// 传输结束的任务
		m.sendExitTask(reply)
	default:
		//log.Printf("can't find Master Phase %v", m.phase)
	}
	return nil
}

// sendMapTask和sendReduceTask两个函数高度相似，后续需要进行抽象
func (m *Master) sendMapTask(reply *ReqTaskReply) {
	// 根据情况可能发送wait任务或者map任务
	if m.mapFinish == len(m.mapTask) {
		task := Task{
			Type: Wait,
		}
		reply.Task = &task
	} else {
		for idx, task := range m.mapTask {
			if checkAbleToSend(task) {
				m.mapTask[idx].Seq = idx
				m.mapTask[idx].StartTime = time.Now()
				m.mapTask[idx].State = Assigned
				reply.Task = &m.mapTask[idx]
				return
			}
		}
		// 可能会遇到全部assign但是没有处理完毕的情况
		task := Task{
			Type: Wait,
		}
		reply.Task = &task
		return
	}
}

func (m *Master) sendReduceTask(reply *ReqTaskReply) {
	// 根据情况可能发送reduce任务或者exit任务
	if m.reduceFinish == len(m.reduceTask) {
		task := Task{
			Type: Exit,
		}
		reply.Task = &task
		return
	} else {
		for idx, task := range m.reduceTask {
			if checkAbleToSend(task) {
				m.reduceTask[idx].StartTime = time.Now()
				m.reduceTask[idx].State = Assigned
				reply.Task = &m.reduceTask[idx]
				return
			}
		}
		task := Task{
			Type: Wait,
		}
		reply.Task = &task
		return
	}
}

func (m *Master) sendExitTask(reply *ReqTaskReply) {
	task := Task{
		Type: Exit,
	}
	reply.Task = &task
}

func checkAbleToSend(t Task) bool {
	// 已完成的话不可再次分配
	if t.State == Finish {
		return false
	}
	// 任务分配且未超过执行时间的话也不可再次分配
	if t.State == Assigned && time.Now().Sub(t.StartTime) < maxTaskRunTime {
		return false
	}
	return true
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.Done {
		switch args.TaskType {
		case Map:
			// 进行map任务的处理,根据结果拆分成对应的reduceTask
			if m.mapTask[args.Seq].State != Finish {
				m.mapFinish += 1
				m.mapTask[args.Seq].State = Finish
			} else {
				//log.Printf("same map task!\n")
			}
		case Reduce:
			// 进行reduce返回任务的处理,通常为单纯增加数量
			if m.reduceTask[args.Seq].State != Finish {
				//log.Printf("reduce task %+v done\n",args.Seq)
				m.reduceFinish += 1
				m.reduceTask[args.Seq].State = Finish
			} else {
				//log.Printf("same reduce task!\n")
			}
		default:
			//log.Printf("%v task needn't care", args.TaskType)
		}
	}
	return nil
}

func (m *Master) addReduceTask() {
	for idx := 0; idx < m.nReduce; idx++ {
		task := Task{
			Type:    Reduce,
			State:   NotAssigned,
			NReduce: m.nReduce,
			NMaps:   len(m.files),
			Seq:     idx,
		}
		m.reduceTask = append(m.reduceTask, task)
	}
}

// 起一个协程专门用于判断是否进行状态切换以及更换整体状态为done
func (m *Master) transferPhase() {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch m.phase {
	case Map:
		// 检查完成数量进行转换
		if len(m.mapTask) == m.mapFinish {
			m.phase = Reduce
			m.addReduceTask()
		}
	case Reduce:
		// 检查完成数量进行转换
		if len(m.reduceTask) == m.reduceFinish {
			m.phase = Exit
		}
	case Exit:
		// 睡眠一段时间后退出
		time.Sleep(waitInterval)
		m.done = true
		break
	default:
		//log.Printf("unable to recognize phase %+v", m.phase)
		break
	}

}

func (m *Master) checkPhase() {
	for !m.Done() {
		go m.transferPhase()
		time.Sleep(waitInterval)
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// 凡是需要访问master内部信息的函数都加上锁以避免访问中间突然遇到问题
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

func (m *Master) initMapTask() {
	nMap := len(m.files)
	for idx, fileName := range m.files {
		task := Task{
			FileName: fileName,
			Type:     Map,
			State:    NotAssigned,
			NReduce:  m.nReduce,
			NMaps:    nMap,
			Seq:      idx,
		}
		m.mapTask = append(m.mapTask, task)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.files = files
	m.nReduce = nReduce
	m.mu = sync.Mutex{}
	m.initMapTask()
	go m.checkPhase() // 动态对状态进行进行更新
	m.server()
	return &m
}
