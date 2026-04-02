package mr

import "os"
import "strconv"

type Args struct {
	// task-地址
	Addr string
}

type Reply struct {
	// map reduce wait exit
	T string
	// reduce任务的数量用于hash计算
	N int
	// 文件地址
	Addr string
	// reduce-task 任务索引
	Index int
	// map-task 任务索引（用于生成唯一的中间文件名）
	MapIndex int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
