package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const ApplyTimeout = 500 * time.Millisecond

// ==================== Op ====================

type OpType string

const (
	OpJoin  OpType = "Join"
	OpLeave OpType = "Leave"
	OpMove  OpType = "Move"
	OpQuery OpType = "Query"
)

type Op struct {
	Type     OpType
	ClientId int64
	SeqId    int64

	// Join
	Servers map[int][]string

	// Leave
	GIDs []int

	// Move
	Shard int
	GID   int

	// Query
	Num int
}

// ==================== applyResult ====================

type applyResult struct {
	Err      Err
	Config   Config // 仅 Query 使用
	ClientId int64
	SeqId    int64
}

// ==================== 去重表 ====================

type lastReply struct {
	SeqId int64
	Reply applyResult
}

// ==================== notifyEntry ====================

type notifyEntry struct {
	clientId int64
	seqId    int64
	ch       chan applyResult
}

// ==================== ShardCtrler ====================

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	configs  []Config             // 配置历史，index 即 config num
	dupTable map[int64]lastReply  // 幂等性去重表
	notifyChans map[int]notifyEntry
}

// ==================== RPC Handler ====================

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		Type:     OpJoin,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Servers:  args.Servers,
	}
	err, _ := sc.submitAndWait(op)
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		Type:     OpLeave,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		GIDs:     args.GIDs,
	}
	err, _ := sc.submitAndWait(op)
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		Type:     OpMove,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Shard:    args.Shard,
		GID:      args.GID,
	}
	err, _ := sc.submitAndWait(op)
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		Type:     OpQuery,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Num:      args.Num,
	}
	err, cfg := sc.submitAndWait(op)
	reply.Err = err
	reply.WrongLeader = (err == ErrWrongLeader)
	reply.Config = cfg
}

// ==================== submitAndWait ====================

func (sc *ShardCtrler) submitAndWait(op Op) (Err, Config) {
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return ErrWrongLeader, Config{}
	}

	ch := make(chan applyResult, 1)

	sc.mu.Lock()
	if old, exists := sc.notifyChans[index]; exists {
		old.ch <- applyResult{Err: ErrWrongLeader}
	}
	sc.notifyChans[index] = notifyEntry{
		clientId: op.ClientId,
		seqId:    op.SeqId,
		ch:       ch,
	}
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		if e, exists := sc.notifyChans[index]; exists &&
			e.clientId == op.ClientId && e.seqId == op.SeqId {
			delete(sc.notifyChans, index)
		}
		sc.mu.Unlock()
	}()

	select {
	case result := <-ch:
		if result.ClientId != op.ClientId || result.SeqId != op.SeqId {
			return ErrWrongLeader, Config{}
		}
		return result.Err, result.Config
	case <-time.After(ApplyTimeout):
		return ErrTimeout, Config{}
	}
}

// ==================== applier ====================

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		msg := <-sc.applyCh

		if !msg.CommandValid {
			continue
		}

		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}

		sc.mu.Lock()

		var result applyResult
		result.ClientId = op.ClientId
		result.SeqId = op.SeqId

		// 去重 + 执行状态机，用 isDup 替代 goto，消除跳过变量声明的编译错误
		isDup := false
		if op.Type != OpQuery {
			if last, dup := sc.dupTable[op.ClientId]; dup && last.SeqId >= op.SeqId {
				result.Err = last.Reply.Err
				result.Config = last.Reply.Config
				isDup = true
			}
		}
		if !isDup {
			// 执行状态机（只取 Err/Config，不覆盖 ClientId/SeqId）
			r := sc.applyOp(op)
			result.Err = r.Err
			result.Config = r.Config
			// 写操作更新去重表
			if op.Type != OpQuery {
				sc.dupTable[op.ClientId] = lastReply{SeqId: op.SeqId, Reply: result}
			}
		}

		var notifyCh chan applyResult
		if e, exists := sc.notifyChans[msg.CommandIndex]; exists &&
			e.clientId == op.ClientId && e.seqId == op.SeqId {
			notifyCh = e.ch
		}

		sc.mu.Unlock()

		// 锁外发送，避免死锁
		if notifyCh != nil {
			notifyCh <- result
		}
	}
}

// ==================== 状态机操作 ====================

func (sc *ShardCtrler) applyOp(op Op) applyResult {
	switch op.Type {

	case OpJoin:
		// 复制最新 config
		newCfg := sc.cloneLatestConfig()
		// 加入新 group
		for gid, servers := range op.Servers {
			newCfg.Groups[gid] = servers
		}
		// 重新均衡
		rebalance(&newCfg)
		sc.configs = append(sc.configs, newCfg)
		return applyResult{Err: OK}

	case OpLeave:
		newCfg := sc.cloneLatestConfig()
		// 把离开 group 的 shard 先归 0（无主）
		leaveSet := make(map[int]bool)
		for _, gid := range op.GIDs {
			leaveSet[gid] = true
			delete(newCfg.Groups, gid)
		}
		for i, gid := range newCfg.Shards {
			if leaveSet[gid] {
				newCfg.Shards[i] = 0
			}
		}
		// 重新均衡
		rebalance(&newCfg)
		sc.configs = append(sc.configs, newCfg)
		return applyResult{Err: OK}

	case OpMove:
		newCfg := sc.cloneLatestConfig()
		newCfg.Shards[op.Shard] = op.GID
		sc.configs = append(sc.configs, newCfg)
		return applyResult{Err: OK}

	case OpQuery:
		n := op.Num
		if n == -1 || n >= len(sc.configs) {
			return applyResult{Err: OK, Config: sc.configs[len(sc.configs)-1]}
		}
		return applyResult{Err: OK, Config: sc.configs[n]}
	}

	return applyResult{Err: OK}
}

// cloneLatestConfig 深拷贝最新 config，并将 Num+1
func (sc *ShardCtrler) cloneLatestConfig() Config {
	latest := sc.configs[len(sc.configs)-1]
	newCfg := Config{
		Num:    latest.Num + 1,
		Shards: latest.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range latest.Groups {
		cp := make([]string, len(servers))
		copy(cp, servers)
		newCfg.Groups[gid] = cp
	}
	return newCfg
}

// ==================== 负载均衡算法 ====================
//
// 目标：
//   1. 每个 group 分到的 shard 数尽量相等（差值 <= 1）
//   2. 移动的 shard 数尽量少
//   3. 结果必须确定性（相同输入 -> 相同输出），因此所有遍历按 gid 排序
//
// 算法：
//   avg  = NShards / nGroups    — 每个 group 至少分到 avg 个
//   extra = NShards % nGroups   — 前 extra 个 group 多分 1 个
//
//   1. 统计每个 gid 当前持有的 shard 列表
//   2. 计算每个 gid 目标数量（avg 或 avg+1）
//   3. 把 "持有超额" 的 shard 放入待分配池
//   4. 把 "持有不足" 的 gid 从池中取 shard

func rebalance(cfg *Config) {
	nGroups := len(cfg.Groups)
	if nGroups == 0 {
		// 没有 group，所有 shard 归 0
		for i := range cfg.Shards {
			cfg.Shards[i] = 0
		}
		return
	}

	// 按 gid 排序，保证确定性
	gids := sortedGIDs(cfg.Groups)

	avg := NShards / nGroups
	extra := NShards % nGroups // 前 extra 个 gid 分 avg+1

	// 统计每个 gid 当前拥有的 shard（按 shard index 排序保证确定性）
	owned := make(map[int][]int) // gid -> []shardIndex
	for _, gid := range gids {
		owned[gid] = []int{}
	}
	// gid=0 的 shard（无主）也需要分配出去
	var unassigned []int
	for shard, gid := range cfg.Shards {
		if gid == 0 || owned[gid] == nil {
			unassigned = append(unassigned, shard)
		} else {
			owned[gid] = append(owned[gid], shard)
		}
	}

	// 把超额的 shard 归入待分配池
	// 先处理分配 avg+1 的 group，再处理分配 avg 的 group
	pool := unassigned // 待分配池

	for i, gid := range gids {
		target := avg
		if i < extra {
			target = avg + 1
		}
		shards := owned[gid]
		if len(shards) > target {
			// 超额：把多余的放入 pool（从末尾取，保证确定性）
			pool = append(pool, shards[target:]...)
			owned[gid] = shards[:target]
		}
	}

	// 从 pool 补充不足的 group
	for i, gid := range gids {
		target := avg
		if i < extra {
			target = avg + 1
		}
		for len(owned[gid]) < target {
			owned[gid] = append(owned[gid], pool[0])
			pool = pool[1:]
		}
	}

	// 写回 cfg.Shards
	for gid, shards := range owned {
		for _, shard := range shards {
			cfg.Shards[shard] = gid
		}
	}
}

// sortedGIDs 返回按升序排列的 gid 列表，保证遍历确定性
func sortedGIDs(groups map[int][]string) []int {
	gids := make([]int, 0, len(groups))
	for gid := range groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	return gids
}

// ==================== Kill ====================

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// ==================== StartServer ====================

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})

	sc := &ShardCtrler{
		me:          me,
		dupTable:    make(map[int64]lastReply),
		notifyChans: make(map[int]notifyEntry),
	}

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.applyCh = make(chan raft.ApplyMsg, 64)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	go sc.applier()

	return sc
}