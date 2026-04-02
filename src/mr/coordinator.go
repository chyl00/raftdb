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

var mu sync.Mutex

type Coordinator struct {
	// 输入文件列表（map 任务）
	mf []string
	// 标记任务状态 0 待处理 1 执行中 2 已完成
	book map[string]int
	// reduce 任务的数量
	r int
	// maptask 完成数量
	mtnum int
	// reducetask 完成数量
	rtnum int
}

func (c *Coordinator) CoordinatorHandler(args *Args, reply *Reply) error {
	mu.Lock()
	defer mu.Unlock()
	if c.mtnum < len(c.mf) {
		return c.Maptask(args, reply)
	}
	return c.Reducetask(args, reply)
}

// maptask
func (c *Coordinator) Maptask(args *Args, reply *Reply) error {
	// 检查 worker 汇报的已完成任务
	if v, ok := c.book[args.Addr]; ok && v == 1 {
		c.book[args.Addr] = 2
		c.mtnum++
	}
	// map 阶段全部完成，让 worker 等待进入 reduce 阶段
	if c.mtnum == len(c.mf) {
		reply.T = "wait"
		return nil
	}
	// 分配新的空闲任务
	for i, addr := range c.mf {
		if c.book[addr] == 0 {
			c.book[addr] = 1
			reply.T = "map"
			reply.N = c.r
			reply.Addr = addr
			reply.MapIndex = i // 传递 map 任务编号，用于生成唯一中间文件名
			go c.Timeadd(addr)
			return nil
		}
	}
	// 没有空闲任务（都在执行中），让 worker 等待
	reply.T = "wait"
	return nil
}

// reducetask
func (c *Coordinator) Reducetask(args *Args, reply *Reply) error {
	// 检查 worker 汇报的已完成任务
	if v, ok := c.book[args.Addr]; ok && v == 1 {
		c.book[args.Addr] = 2
		c.rtnum++
	}
	// reduce 阶段全部完成
	if c.rtnum == c.r {
		reply.T = "exit"
		return nil
	}
	// 分配新的空闲任务，用 "reduce-{i}" 作为唯一标识
	for i := 0; i < c.r; i++ {
		key := fmt.Sprintf("reduce-%d", i)
		if c.book[key] == 0 {
			c.book[key] = 1
			reply.T = "reduce"
			reply.Index = i
			reply.Addr = key // worker 用 Addr 向 coordinator 汇报完成
			go c.Timeadd(key)
			return nil
		}
	}
	// 没有空闲任务，让 worker 等待
	reply.T = "wait"
	return nil
}

// 计时：10 秒未完成则重置为待处理
func (c *Coordinator) Timeadd(fname string) {
	time.Sleep(10 * time.Second)
	mu.Lock()
	defer mu.Unlock()
	if c.book[fname] == 1 {
		c.book[fname] = 0
		fmt.Printf("任务超时，重新分配：%v\n", fname)
		return
	}
	fmt.Printf("任务按时完成：%v\n", fname)
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()

	return c.mtnum == len(c.mf) && c.rtnum == c.r
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mf = append(c.mf, files...)
	c.r = nReduce
	c.book = make(map[string]int)
	for _, v := range c.mf {
		c.book[v] = 0
	}
	for i := 0; i < nReduce; i++ {
		c.book[fmt.Sprintf("reduce-%d", i)] = 0
	}
	c.server()
	return &c
}
