<div align="center">

# RaftDB

### 基于 Raft 共识算法的分布式持久化 KV 数据库

[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Status](https://img.shields.io/badge/status-WIP-yellow)]()

基于 [MIT 6.824 (Raft Lab)](https://github.com/chyl00/MIT6.824-CYL.git) 扩展实现，支持多数据结构、日志压缩与快照、线性一致读写。

</div>

---

## ✨ 特性 Features

- 🗳️ **强一致性** — 基于 Raft 协议，写操作多数派确认后提交，保证 Linearizability
- 💾 **持久化存储** — Raft 日志与 KV 数据均落盘，节点重启数据不丢失
- 📦 **多数据结构** — 支持 String / List / Hash / Set / SortedSet（可选）
- 📸 **日志压缩** — 自动 Snapshot 与 InstallSnapshot，避免日志无限增长
- 🔁 **请求去重** — ClientId + SeqNum 机制保证写操作 Exactly-Once
- 📊 **可观测性** — Prometheus Metrics、结构化日志、集群状态查询接口

---

## 📐 架构 Architecture

```
┌─────────────┐
│   Client     │
└──────┬───────┘
       │ RPC (Get/Put/...)
       ▼
┌─────────────────────────────┐
│         KVServer Layer        │
│  - 请求去重 (ClientId/SeqNum) │
│  - Leader重定向                │
│  - 线性一致读策略               │
└──────┬────────────────────────┘
       │ rf.Start(cmd) / applyCh
       ▼
┌─────────────────────────────┐
│         Raft Layer            │
│  - 选举 / 日志复制 / 心跳       │
│  - 持久化 (term/votedFor/log) │
│  - Snapshot/InstallSnapshot   │
└──────┬────────────────────────┘
       │ persist / restore
       ▼
┌─────────────────────────────┐
│       Storage Engine          │
│  (BoltDB / BadgerDB / Pebble) │
│  - raft日志区                  │
│  - kv数据区                    │
│  - 元数据区                    │
└─────────────────────────────┘
```

---

## 📋 功能需求 Functional Requirements

### 数据模型

Key 为 `string`（长度 ≤ 1KB），Value 支持以下类型：

| 类型 | 说明 | 支持操作 |
|---|---|---|
| String | 普通字符串/二进制 | `GET` `SET` `APPEND` `INCR` `DEL` |
| List | 双向链表 | `LPUSH` `RPUSH` `LPOP` `RPOP` `LRANGE` |
| Hash | 字段-值映射 | `HSET` `HGET` `HDEL` `HGETALL` |
| Set | 无序唯一集合 | `SADD` `SREM` `SMEMBERS` `SISMEMBER` |
| SortedSet *(可选)* | 带权重有序集合 | `ZADD` `ZRANGE` `ZSCORE` |

所有 Value 统一编码（带类型标签），对类型不匹配的操作（如对 String 执行 `HSET`）返回明确错误。

### 一致性与共识

- 所有写操作必须通过 Raft 日志复制到多数节点后才能应用到状态机
- 读操作默认线性一致（Linearizable Read），可降级为 Leader 本地读 / ReadIndex / Lease Read
- 客户端请求携带 `ClientId + SeqNum`，状态机层去重

### 持久化

- Raft 的 `currentTerm`、`votedFor`、日志条目持久化，重启可恢复
- KV 数据落盘，不依赖内存常驻全量数据

### 日志压缩 / 快照

- 日志超过阈值时触发状态机 Snapshot
- Snapshot 包含：全量 KV 数据 + 去重表（ClientId→SeqNum）+ lastApplied index/term
- Leader 对落后过多的 Follower 发送 InstallSnapshot

### 集群管理

- 支持成员变更（Joint Consensus 或单步变更）
- Leader 选举、心跳、故障自动切换
- 提供集群状态查询接口

### 客户端接口

```go
Get(key string) (Value, error)
Put(key string, value Value) error
Delete(key string) error

// 数据结构专用操作
LPush(key string, val string) error
HSet(key, field, val string) error
SAdd(key string, members ...string) error
```

- 支持批量操作（Batch/Pipeline）
- 客户端自动重试 + Leader 发现/重定向

---

## ⚙️ 非功能需求 Non-Functional Requirements

| 维度 | 要求 |
|---|---|
| **可靠性** | 少数节点（< N/2）宕机不影响可用性；落盘数据断电重启不丢失；CRC 校验防静默错误 |
| **性能** | 本地读 < 1ms；线性一致读 < raft 心跳周期；支持 Batching/Pipelining |
| **可观测性** | Prometheus Metrics（QPS/延迟/日志长度/Leader切换）；结构化日志；调试接口 |
| **可测试性** | Raft 核心状态转换单元测试；网络分区/丢包/重启集成测试；`go test -race` |
| **安全性** *(可选)* | 节点间 TLS；客户端 Token / mTLS 鉴权 |

---

## 🗂️ 数据编码规范

### Value 封装

```go
type ValueType uint8

const (
    TypeString ValueType = iota
    TypeList
    TypeHash
    TypeSet
    TypeZSet
)

type Value struct {
    Type ValueType
    Data []byte // gob/protobuf 序列化
}
```

### Raft 日志指令封装

```go
type Op struct {
    OpType   string // "Put","Get","LPush","HSet","SAdd",...
    Key      string
    Args     []string
    ClientId int64
    SeqNum   int64
}
```

### 存储 Key 命名规范

| 前缀 | 用途 |
|---|---|
| `r/log/<index>` | Raft 日志条目 |
| `r/meta` | term / votedFor / lastIncludedIndex |
| `kv/<key>` | 业务 KV 数据 |
| `dedup/<clientId>` | 客户端去重表 |

---

## 🛣️ 开发路线 Roadmap

- [ ] **M0** — 跑通 6.824 lab2 (Raft)，通过官方测试（含网络分区/重启）
- [ ] **M1** — 跑通 lab3（内存 KV + 线性一致），单类型 String，并发测试通过
- [ ] **M2** — Raft 持久化改造，集成嵌入式存储引擎
- [ ] **M3** — KV 数据持久化，重启后状态机数据完整
- [ ] **M4** — Snapshot / 日志压缩，InstallSnapshot 正常工作
- [ ] **M5** — 多数据结构支持（List/Hash/Set）
- [ ] **M6** — 性能优化（Batching、ReadIndex/Lease Read），压测达标
- [ ] **M7** — 可观测性与运维（Metrics、日志、集群管理接口）
- [ ] **M8** *(可选)* — 集群成员变更 / TLS / 鉴权

---

## ⚠️ 关键设计约束

1. **顺序应用** — 状态机严格按 Raft committed 顺序单线程 Apply
2. **幂等性** — List/Set 等非天然幂等操作依赖 ClientId+SeqNum 去重，去重表随 Snapshot 持久化
3. **快照一致性** — 生成 Snapshot 时与 Apply 流程同步，避免数据竞争
4. **类型安全** — 类型不匹配操作返回明确错误，而非 panic 或静默覆盖
5. **I/O 解耦** — Snapshot/Compaction 的磁盘 IO 不应阻塞 Raft 心跳

---

## 🧪 测试计划

- **单元测试**：Raft 状态转换、Value 编解码、数据结构边界条件
- **集成测试**：扩展 6.824 网络模拟框架，覆盖 Leader 崩溃日志一致性、网络分区脑裂防护、Snapshot 期间并发读写、重启数据完整性
- **压力测试**：高并发写入吞吐与延迟基线
- **混沌测试** *(可选)*：随机延迟、丢包、磁盘满故障注入

---

## 🔧 技术选型

| 组件 | 推荐方案 | 备注 |
|---|---|---|
| 存储引擎 | BadgerDB / BoltDB | BadgerDB 写性能更优且自带快照；BoltDB 更简单稳定 |
| 序列化 | Protobuf | 跨语言高性能；初期可用 gob 快速验证 |
| RPC 框架 | gRPC | 替代 6.824 自带 labrpc，更贴近生产环境 |
| Metrics | Prometheus + Grafana | 标准可观测性方案 |
| 配置管理 | YAML + 环境变量覆盖 | 便于多环境部署 |

---

## 📄 License

MIT 该项目仅用于学习 ！！！
