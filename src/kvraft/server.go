package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const ApplyTimeout = 500 * time.Millisecond

// ==================== Op ====================

type Op struct {
	OpType   string
	Key      string
	Value    string
	ClientId int64
	SeqId    int64
}

// ==================== applyResult ====================

type applyResult struct {
	Err      Err
	Value    string
	ClientId int64
	SeqId    int64
}

// ==================== 去重表条目 ====================

type lastReply struct {
	SeqId int64
	Reply applyResult
}

// ==================== notifyEntry：带身份标识的等待项 ====================
//
// Bug1 修复：用 clientId+seqId 标识 channel 归属。
// 新请求注册前检查 index 是否已被占用（极少发生），
// apply 时只通知 clientId+seqId 匹配的 channel，
// 避免 leader 切换后同一 index 被不同请求覆盖。
//

type notifyEntry struct {
	clientId int64
	seqId    int64
	ch       chan applyResult
}

// ==================== KVServer ====================

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxraftstate int
	persister    *raft.Persister

	kvStore     map[string]string   // 状态机 应用raft提交的日志
	dupTable    map[int64]lastReply // 幂等性保证
	notifyChans map[int]notifyEntry // index -> 等待项 防止发生了leader变更
}

// ==================== RPC Handler: Get ====================

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		OpType:   "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}
	err, value := kv.submitAndWait(op)
	reply.Err = err
	reply.Value = value
}

// ==================== RPC Handler: PutAppend ====================

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}
	err, _ := kv.submitAndWait(op)
	reply.Err = err
}

// ==================== submitAndWait ====================

func (kv *KVServer) submitAndWait(op Op) (Err, string) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader, ""
	}

	ch := make(chan applyResult, 1)

	kv.mu.Lock()
	// Bug1 修复：若该 index 已有等待项（极少情况），通知旧的 WrongLeader
	if old, exists := kv.notifyChans[index]; exists {
		old.ch <- applyResult{Err: ErrWrongLeader}
	}
	kv.notifyChans[index] = notifyEntry{
		clientId: op.ClientId,
		seqId:    op.SeqId,
		ch:       ch,
	}
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		// 只删自己注册的 entry，防止把别人的也删掉
		if e, exists := kv.notifyChans[index]; exists &&
			e.clientId == op.ClientId && e.seqId == op.SeqId {
			delete(kv.notifyChans, index)
		}
		kv.mu.Unlock()
	}()

	select {
	case result := <-ch:
		if result.ClientId != op.ClientId || result.SeqId != op.SeqId {
			return ErrWrongLeader, ""
		}
		return result.Err, result.Value
	case <-time.After(ApplyTimeout):
		return ErrTimeout, ""
	}
}

// ==================== applier ====================

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.installSnapshot(msg.Snapshot)
			}
			kv.mu.Unlock()
			continue
		}

		if !msg.CommandValid {
			continue
		}

		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}

		kv.mu.Lock()

		var result applyResult
		result.ClientId = op.ClientId
		result.SeqId = op.SeqId

		if last, dup := kv.dupTable[op.ClientId]; dup && last.SeqId >= op.SeqId {
			result.Err = last.Reply.Err
			result.Value = last.Reply.Value
		} else {
			r := kv.applyOp(op)
			result.Err = r.Err
			result.Value = r.Value
			if op.OpType != "Get" {
				kv.dupTable[op.ClientId] = lastReply{SeqId: op.SeqId, Reply: result}
			}
		}

		// Bug2 修复：先取出 channel，释放锁后再发送，避免持锁发送可能引起的死锁
		var notifyCh chan applyResult
		if e, exists := kv.notifyChans[msg.CommandIndex]; exists &&
			e.clientId == op.ClientId && e.seqId == op.SeqId {
			notifyCh = e.ch
		}

		needSnapshot := kv.maxraftstate != -1 &&
			kv.persister.RaftStateSize() >= kv.maxraftstate

		if needSnapshot {
			kv.doSnapshot(msg.CommandIndex)
		}

		kv.mu.Unlock()

		// 锁外发送，彻底避免死锁
		if notifyCh != nil {
			notifyCh <- result
		}
	}
}

func (kv *KVServer) applyOp(op Op) applyResult {
	switch op.OpType {
	case "Get":
		v, ok := kv.kvStore[op.Key]
		if !ok {
			return applyResult{Err: ErrNoKey}
		}
		return applyResult{Err: OK, Value: v}
	case "Put":
		kv.kvStore[op.Key] = op.Value
		return applyResult{Err: OK}
	case "Append":
		kv.kvStore[op.Key] += op.Value
		return applyResult{Err: OK}
	}
	return applyResult{Err: ErrNoKey}
}

// ==================== 快照 ====================

func (kv *KVServer) doSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.dupTable)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *KVServer) installSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvStore map[string]string
	var dupTable map[int64]lastReply
	if d.Decode(&kvStore) != nil || d.Decode(&dupTable) != nil {
		DPrintf("installSnapshot decode error")
		return
	}
	kv.kvStore = kvStore
	kv.dupTable = dupTable
}

// ==================== Kill ====================

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// ==================== StartKVServer ====================

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := &KVServer{
		me:           me,
		maxraftstate: maxraftstate,
		persister:    persister,
		kvStore:      make(map[string]string),
		dupTable:     make(map[int64]lastReply),
		notifyChans:  make(map[int]notifyEntry),
	}

	kv.applyCh = make(chan raft.ApplyMsg, 64)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	if snapshot := persister.ReadSnapshot(); len(snapshot) > 0 {
		kv.installSnapshot(snapshot)
	}

	go kv.applier()

	return kv
}
