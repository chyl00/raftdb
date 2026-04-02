package mr

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	args, reply := &Args{}, &Reply{}

	for {
		fmt.Printf("reply : %+v\n", reply)
		switch reply.T {
		case "map":
			args.Addr = reply.Addr
			if err := maptask(mapf, reply); err != nil {
				log.Printf("maptask 处理失败 %v: %v", reply.Addr, err)
				args.Addr = "" // 不汇报完成，让 coordinator 超时重新分配
			}
			if !Call(args, reply) {
				return
			}

		case "reduce":
			args.Addr = reply.Addr
			if err := reducetask(reducef, reply); err != nil {
				log.Printf("reducetask 处理失败 %v: %v", reply.Addr, err)
				args.Addr = ""
			}
			if !Call(args, reply) {
				return
			}

		case "wait":
			time.Sleep(3 * time.Second)
			args.Addr = ""
			if !Call(args, reply) {
				return
			}

		case "exit":
			fmt.Println("所有任务完成，worker 正常退出")
			return

		default:
			// 第一次请求
			args.Addr = ""
			if !Call(args, reply) {
				return
			}
		}
	}
}

// map 任务执行
func maptask(mapf func(string, string) []KeyValue, reply *Reply) error {
	// 读取输入文件
	file, err := os.Open(reply.Addr)
	if err != nil {
		return fmt.Errorf("cannot open %v: %w", reply.Addr, err)
	}
	content, err := ioutil.ReadAll(file)
	file.Close()
	if err != nil {
		return fmt.Errorf("cannot read %v: %w", reply.Addr, err)
	}

	kva := mapf(reply.Addr, string(content))

	// 先写到临时文件，全部成功后再原子 rename
	// 文件命名：map-result-{mapIndex}-{reduceIndex}
	// 不同 map 任务有不同 mapIndex，rename 不会互相覆盖
	tmpFiles := make([]*os.File, reply.N)
	tmpNames := make([]string, reply.N)
	for i := 0; i < reply.N; i++ {
		tmp, err := os.CreateTemp("", fmt.Sprintf("map-tmp-%d-%d-*", reply.MapIndex, i))
		if err != nil {
			return err
		}
		tmpFiles[i] = tmp
		tmpNames[i] = tmp.Name()
	}
	defer func() {
		for _, f := range tmpFiles {
			if f != nil {
				f.Close()
			}
		}
	}()

	for _, kv := range kva {
		index := ihash(kv.Key) % reply.N
		if _, err = fmt.Fprintf(tmpFiles[index], "%v\t%v\n", kv.Key, kv.Value); err != nil {
			return err
		}
	}

	// 全部写完后原子 rename，目标文件名带 mapIndex 保证唯一
	for i := 0; i < reply.N; i++ {
		tmpFiles[i].Close()
		tmpFiles[i] = nil
		dest := fmt.Sprintf("map-result-%d-%d", reply.MapIndex, i)
		if err := os.Rename(tmpNames[i], dest); err != nil {
			return err
		}
	}
	return nil
}

// reduce 任务执行
func reducetask(reducef func(string, []string) string, reply *Reply) error {
	// 读取所有属于该 reduce 任务的中间文件：map-result-*-{reduceIndex}
	pattern := fmt.Sprintf("map-result-*-%d", reply.Index)
	files, err := filepath.Glob(pattern)
	if err != nil || len(files) == 0 {
		return fmt.Errorf("no intermediate files found for reduce-%d", reply.Index)
	}

	var kva []KeyValue
	for _, fname := range files {
		f, err := os.Open(fname)
		if err != nil {
			return err
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, "\t", 2)
			if len(parts) != 2 {
				continue
			}
			kva = append(kva, KeyValue{parts[0], parts[1]})
		}
		f.Close()
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("scanner error on %v: %w", fname, err)
		}
	}

	// 按 Key 排序
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	// 先写临时文件，完成后原子 rename
	oname := "mr-out-" + strconv.Itoa(reply.Index)
	ofile, err := os.CreateTemp("", oname+"-tmp-*")
	if err != nil {
		return err
	}
	defer func() {
		if ofile != nil {
			ofile.Close()
		}
	}()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		if _, err = fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output); err != nil {
			return err
		}
		i = j
	}

	tmpName := ofile.Name()
	ofile.Close()
	ofile = nil
	if err := os.Rename(tmpName, oname); err != nil {
		return err
	}

	return nil
}

// Call 发起 RPC，coordinator 不可达时返回 false
func Call(args *Args, reply *Reply) bool {
	ok := call("Coordinator.CoordinatorHandler", args, reply)
	if ok {
		fmt.Printf("RPC 响应成功: %+v\n", reply)
	} else {
		fmt.Println("coordinator 已退出，worker 退出")
	}
	return ok
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("dialing failed: %v", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
