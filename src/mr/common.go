package mr

import (
	"fmt"
	"time"
)

type TaskType int

type TaskState int

const (
	waitInterval = 50 * time.Millisecond
)

// 用于判断任务的状态
const (
	Map    TaskType = 0
	Reduce TaskType = 1
	Wait   TaskType = 2 // 表示暂时没有任务，睡眠一会再寻找任务
	Exit   TaskType = 3 // 表示全部任务处理完毕，需要退出
)

const (
	NotAssigned TaskState = 0
	Assigned    TaskState = 1
	Finish      TaskState = 2
)

type Task struct {
	FileName  string
	Type      TaskType  // map;reduce;stop 还需要添加exit状态，需要四个状态而非三个
	State     TaskState // 用来标识完成状态
	StartTime time.Time // 用来标识任务完成的时间，以进行超时判断
	NReduce   int       // 对应reduce任务的数量
	NMaps     int       // 对应map任务的数量
	Seq       int       // 对应实际的map或reduce序号
}

// intermediate file name created after map task finished
func getReduceFileName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

// final file name created after reduce task finished
func getMergeFileName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
