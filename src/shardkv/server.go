package shardkv

// ==================== 设计说明 ====================
//
// Shard 状态机（4 个状态）：
//
//   Serving   ──config change──►  BePulling  (旧 owner：停止服务，等待新 owner 拉取并 GC)
//   Serving   ◄──OpGC applied──   BePulling
//
//   (no shard) ─config change──►  Pulling    (新 owner：需要拉取数据)
//   Pulling   ──OpInstall─────►  GCing      (新 owner：有数据可服务，等待 GC 完成)
//   GCing     ──OpGC applied──►  Serving    (新 owner：GC 已通知旧 owner，稳定)
//
// Config 推进原则：只有所有 shard 都处于稳定状态（Serving）才推进到下一个 config，
// 保证同一时刻只有一个 config 的迁移在进行。
//
// 幂等性：
//   - 客户端 op 通过 dupTable[clientId] 去重
//   - PullShard 数据连同 dupTable 一起迁移，保证迁移后历史 op 依然不重复执行

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const ApplyTimeout = 500 * time.Millisecond

// ==================== Shard 状态 ====================

type ShardState int8

const (
	Serving   ShardState = iota // 正常服务
	Pulling                     // 新 owner，等待从旧 owner 拉取数据
	BePulling                   // 旧 owner，数据被拉取中，暂停服务
	GCing                       // 新 owner，已有数据并服务，等待通知旧 owner GC
)

type Shard struct {
	State ShardState
	KV    map[string]string
}

func newShard() Shard {
	return Shard{State: Serving, KV: make(map[string]string)}
}

func (s *Shard) cloneKV() map[string]string {
	m := make(map[string]string, len(s.KV))
	for k, v := range s.KV {
		m[k] = v
	}
	return m
}

// ==================== Op 类型 ====================

const (
	OpGet     = "Get"
	OpPut     = "Put"
	OpAppend  = "Append"
	OpConfig  = "Config"
	OpInstall = "Install" // 安装迁移来的 shard 数据
	OpGC      = "GC"     // 垃圾回收：删除已迁移的 shard
)

type Op struct {
	InternalId int64 // 用于 notifyChans 身份匹配（所有 op 类型通用）

	Type     string
	ClientId int64
	SeqId    int64

	// KV ops
	Key   string
	Value string

	// OpConfig
	Config shardctrler.Config

	// OpInstall
	ConfigNum int
	Shards    map[int]map[string]string
	DupTable  map[int64]DupEntry

	// OpGC
	GCConfigNum int
	GCShardIds  []int
}

// ==================== applyResult ====================

type applyResult struct {
	InternalId int64
	Err        Err
	Value      string
}

// ==================== notifyEntry ====================

type notifyEntry struct {
	internalId int64
	ch         chan applyResult
}

// ==================== ShardKV ====================

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int
	persister    *raft.Persister
	dead         int32

	mck        *shardctrler.Clerk
	config     shardctrler.Config // 当前已 apply 的 config
	lastConfig shardctrler.Config // 上一个 config，用于确定迁移来源 gid

	shards      [shardctrler.NShards]Shard
	dupTable    map[int64]DupEntry
	notifyChans map[int]notifyEntry
}

// ==================== RPC Handler: Get ====================

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Type:     OpGet,
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}
	err, value := kv.submitOp(op)
	reply.Err = err
	reply.Value = value
}

// ==================== RPC Handler: PutAppend ====================

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}
	err, _ := kv.submitOp(op)
	reply.Err = err
}

// ==================== RPC Handler: PullShard（被旧 owner 响应） ====================
//
// 新 owner 的 migrationPuller 调用此 RPC 拉取 shard 数据。
// 只需是 leader 且 config num >= 请求的 config num 即可响应。
// 不走 Raft：只读操作，leader 持有数据，直接返回。

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num < args.ConfigNum {
		// 旧 owner 还没 apply 到对应的 config，稍后重试
		reply.Err = ErrNotReady
		return
	}

	reply.Shards = make(map[int]map[string]string)
	for _, shardId := range args.ShardIds {
		reply.Shards[shardId] = kv.shards[shardId].cloneKV()
	}

	// 连同 dupTable 一起迁移，保证幂等性历史不丢失
	reply.DupTable = make(map[int64]DupEntry)
	for k, v := range kv.dupTable {
		reply.DupTable[k] = v
	}

	reply.Err = OK
}

// ==================== RPC Handler: GCShard（被旧 owner 响应） ====================
//
// 新 owner 拿到数据后，通知旧 owner 删除。旧 owner 通过 Raft 保证所有副本一致删除。

func (kv *ShardKV) GCShard(args *GCArgs, reply *GCReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	// 如果 config 已经推进到更新的版本，说明 GC 已经完成（幂等）
	if kv.config.Num > args.ConfigNum {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	// 检查是否所有 shard 都还在 BePulling（如果已经是 Serving 说明已 GC）
	allGCed := true
	for _, shardId := range args.ShardIds {
		if kv.shards[shardId].State == BePulling {
			allGCed = false
			break
		}
	}
	if allGCed {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:        OpGC,
		GCConfigNum: args.ConfigNum,
		GCShardIds:  args.ShardIds,
	}
	err, _ := kv.submitOp(op)
	reply.Err = err
}

// ==================== submitOp：统一提交 + 等待 ====================

func (kv *ShardKV) submitOp(op Op) (Err, string) {
	op.InternalId = nrand()

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader, ""
	}

	ch := make(chan applyResult, 1)

	kv.mu.Lock()
	if old, ok := kv.notifyChans[index]; ok {
		// 同 index 有旧等待者（leader 切换重分配了同一 index），通知它失败
		old.ch <- applyResult{Err: ErrWrongLeader}
	}
	kv.notifyChans[index] = notifyEntry{internalId: op.InternalId, ch: ch}
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if e, ok := kv.notifyChans[index]; ok && e.internalId == op.InternalId {
			delete(kv.notifyChans, index)
		}
		kv.mu.Unlock()
	}()

	select {
	case result := <-ch:
		if result.InternalId != op.InternalId {
			return ErrWrongLeader, ""
		}
		return result.Err, result.Value
	case <-time.After(ApplyTimeout):
		return ErrTimeout, ""
	}
}

// ==================== applier ====================

func (kv *ShardKV) applier() {
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
		result.InternalId = op.InternalId

		// 客户端 KV op：走去重表
		if op.Type == OpGet || op.Type == OpPut || op.Type == OpAppend {
			shardId := key2shard(op.Key)
			state := kv.shards[shardId].State
			ownerGid := kv.config.Shards[shardId]

			// 修复 Bug 1：必须同时检查状态和所有权
			if ownerGid != kv.gid || (state != Serving && state != GCing) {
				result.Err = ErrWrongGroup
			} else if op.Type != OpGet {
				// 写操作去重
				if last, dup := kv.dupTable[op.ClientId]; dup && last.SeqId >= op.SeqId {
					result.Err = last.Err
					result.Value = last.Value
				} else {
					result = kv.applyKVOp(op)
					result.InternalId = op.InternalId
					kv.dupTable[op.ClientId] = DupEntry{SeqId: op.SeqId, Err: result.Err, Value: result.Value}
				}
			} else {
				// Get：不去重（幂等），直接读
				result = kv.applyKVOp(op)
				result.InternalId = op.InternalId
			}
		} else {
			// 管理 op：直接执行（配置变更、shard 迁移、GC）
			result = kv.applyAdminOp(op)
			result.InternalId = op.InternalId
		}

		// 取出 channel（锁外发送避免死锁）
		var notifyCh chan applyResult
		if e, ok := kv.notifyChans[msg.CommandIndex]; ok && e.internalId == op.InternalId {
			notifyCh = e.ch
		}

		// 快照检查：序列化在持锁时，rf.Snapshot() 在释放锁后调用，避免 data race
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			kv.doSnapshot(msg.CommandIndex)
		}
		kv.mu.Unlock()
		if notifyCh != nil {
			notifyCh <- result
		}
	}
}

// ==================== KV 状态机操作 ====================

func (kv *ShardKV) applyKVOp(op Op) applyResult {
	shardId := key2shard(op.Key)
	switch op.Type {
	case OpGet:
		v, ok := kv.shards[shardId].KV[op.Key]
		if !ok {
			return applyResult{Err: ErrNoKey}
		}
		return applyResult{Err: OK, Value: v}
	case OpPut:
		kv.shards[shardId].KV[op.Key] = op.Value
		return applyResult{Err: OK}
	case OpAppend:
		kv.shards[shardId].KV[op.Key] += op.Value
		return applyResult{Err: OK}
	}
	return applyResult{Err: ErrNoKey}
}

// ==================== 管理 op 状态机 ====================

func (kv *ShardKV) applyAdminOp(op Op) applyResult {
	switch op.Type {

	case OpConfig:
		// 只推进 +1 的 config，保证单步迁移
		if op.Config.Num != kv.config.Num+1 {
			return applyResult{Err: OK}
		}
		kv.lastConfig = kv.config
		kv.config = op.Config

		for i := 0; i < shardctrler.NShards; i++ {
			// 新配置中 该分片是否属于这组
			nowOwner := op.Config.Shards[i] == kv.gid
			// 旧配置中 该分片是否属于这组
			wasOwner := kv.lastConfig.Shards[i] == kv.gid
			// 以前的旧Gid
			prevGid := kv.lastConfig.Shards[i]

			if nowOwner && !wasOwner {
				if prevGid == 0 {
					// Config 1：从无到有，直接 Serving
					kv.shards[i].State = Serving
				} else {
					// 需要从旧 owner 拉取
					kv.shards[i].State = Pulling
				}
			}
			if !nowOwner && wasOwner {
				// 旧 owner：等待新 owner 拉取，暂停服务
				kv.shards[i].State = BePulling
			}
		}
		return applyResult{Err: OK}

	case OpInstall:
		// 只安装与当前 config 匹配的数据
		if op.ConfigNum != kv.config.Num {
			return applyResult{Err: OK}
		}
		for shardId, kvData := range op.Shards {
			if kv.shards[shardId].State == Pulling {
				newKV := make(map[string]string, len(kvData))
				for k, v := range kvData {
					newKV[k] = v
				}
				kv.shards[shardId].KV = newKV
				kv.shards[shardId].State = GCing // 有数据可服务，但还没 GC 旧 owner
			}
		}
		// 合并 dupTable：取较新的 SeqId
		for clientId, entry := range op.DupTable {
			if existing, ok := kv.dupTable[clientId]; !ok || entry.SeqId > existing.SeqId {
				kv.dupTable[clientId] = entry
			}
		}
		return applyResult{Err: OK}

	case OpGC:
		// 旧 owner：BePulling → Serving（删数据）
		// 新 owner：GCing → Serving（稳定）
		for _, shardId := range op.GCShardIds {
			switch kv.shards[shardId].State {
			case BePulling:
				if kv.config.Num == op.GCConfigNum {
					kv.shards[shardId].KV = make(map[string]string)
					kv.shards[shardId].State = Serving
				}
			case GCing:
				if kv.config.Num == op.GCConfigNum {
					kv.shards[shardId].State = Serving
				}
			}
		}
		return applyResult{Err: OK}
	}

	return applyResult{Err: OK}
}

// ==================== 后台 goroutine：config 轮询 ====================
//
// 只有当前所有 shard 都处于 Serving 状态时才推进 config。
// 保证同一时刻只有一个 config 的迁移在进行。

func (kv *ShardKV) configPoller() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			allStable := true
			for _, shard := range kv.shards {
				if shard.State != Serving {
					allStable = false
					break
				}
			}
			currentNum := kv.config.Num
			kv.mu.Unlock()

			if allStable {
				newCfg := kv.mck.Query(currentNum + 1)
				if newCfg.Num == currentNum+1 {
					kv.rf.Start(Op{
						Type:       OpConfig,
						Config:     newCfg,
						InternalId: nrand(),
					})
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ==================== 后台 goroutine：shard 拉取 ====================
//
// 收集所有 Pulling 状态的 shard，按 gid 分组，
// 向上一个 config 中的旧 owner 发送 PullShard RPC，
// 成功后提交 OpInstall 到 Raft。

func (kv *ShardKV) migrationPuller() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			pullMap := make(map[int][]int) // gid -> []shardId
			for i, shard := range kv.shards {
				if shard.State == Pulling {
					gid := kv.lastConfig.Shards[i]
					pullMap[gid] = append(pullMap[gid], i)
				}
			}
			configNum := kv.config.Num
			lastCfg := kv.lastConfig
			kv.mu.Unlock()

			var wg sync.WaitGroup
			for gid, shardIds := range pullMap {
				wg.Add(1)
				go func(gid int, shardIds []int) {
					defer wg.Done()
					servers, ok := lastCfg.Groups[gid]
					if !ok {
						return
					}
					args := PullShardArgs{ConfigNum: configNum, ShardIds: shardIds}
					for _, server := range servers {
						srv := kv.make_end(server)
						var reply PullShardReply
						if srv.Call("ShardKV.PullShard", &args, &reply) && reply.Err == OK {
							kv.rf.Start(Op{
								Type:       OpInstall,
								InternalId: nrand(),
								ConfigNum:  configNum,
								Shards:     reply.Shards,
								DupTable:   reply.DupTable,
							})
							return
						}
					}
				}(gid, shardIds)
			}
			wg.Wait()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ==================== 后台 goroutine：GC 通知 ====================
//
// 收集所有 GCing 状态的 shard，通知旧 owner 删除数据（GCShard RPC）。
// 旧 owner 确认后，提交自己的 OpGC 把 GCing → Serving。

func (kv *ShardKV) gcHandler() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			gcMap := make(map[int][]int) // gid -> []shardId
			for i, shard := range kv.shards {
				if shard.State == GCing {
					gid := kv.lastConfig.Shards[i]
					gcMap[gid] = append(gcMap[gid], i)
				}
			}
			configNum := kv.config.Num
			lastCfg := kv.lastConfig
			kv.mu.Unlock()

			var wg sync.WaitGroup
			for gid, shardIds := range gcMap {
				wg.Add(1)
				go func(gid int, shardIds []int) {
					defer wg.Done()
					servers, ok := lastCfg.Groups[gid]
					if !ok {
						return
					}
					args := GCArgs{ConfigNum: configNum, ShardIds: shardIds}
					for _, server := range servers {
						srv := kv.make_end(server)
						var reply GCReply
						if srv.Call("ShardKV.GCShard", &args, &reply) && reply.Err == OK {
							// 旧 owner 已 GC，自己也推进到 Serving
							kv.rf.Start(Op{
								Type:        OpGC,
								InternalId:  nrand(),
								GCConfigNum: configNum,
								GCShardIds:  shardIds,
							})
							return
						}
					}
				}(gid, shardIds)
			}
			wg.Wait()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ==================== 快照 ====================

func (kv *ShardKV) doSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	e.Encode(kv.dupTable)
	e.Encode(kv.config)
	e.Encode(kv.lastConfig)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) installSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var shards [shardctrler.NShards]Shard
	var dupTable map[int64]DupEntry
	var config shardctrler.Config
	var lastConfig shardctrler.Config
	if d.Decode(&shards) != nil ||
		d.Decode(&dupTable) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&lastConfig) != nil {
		return
	}
	kv.shards = shards
	kv.dupTable = dupTable
	kv.config = config
	kv.lastConfig = lastConfig
}

// ==================== Kill ====================

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// ==================== StartServer ====================

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister,
	maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *ShardKV {

	// 注册所有需要 gob 序列化的类型
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(map[int]map[string]string{})
	labgob.Register(map[int64]DupEntry{})
	labgob.Register(Shard{})

	kv := &ShardKV{
		me:           me,
		make_end:     make_end,
		gid:          gid,
		ctrlers:      ctrlers,
		maxraftstate: maxraftstate,
		persister:    persister,
		mck:          shardctrler.MakeClerk(ctrlers),
		dupTable:     make(map[int64]DupEntry),
		notifyChans:  make(map[int]notifyEntry),
	}

	// 初始化所有 shard 为 Serving 状态（空数据）
	for i := range kv.shards {
		kv.shards[i] = newShard()
	}

	kv.applyCh = make(chan raft.ApplyMsg, 64)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 从快照恢复
	if snapshot := persister.ReadSnapshot(); len(snapshot) > 0 {
		kv.installSnapshot(snapshot)
	}

	go kv.applier()
	go kv.configPoller()
	go kv.migrationPuller()
	go kv.gcHandler()

	return kv
}