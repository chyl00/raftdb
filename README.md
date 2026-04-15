-----

# MIT 6.824: Distributed Systems (Golang)

这是我对 MIT 分布式系统课程 **6.824 / 6.5840** 实验部分的个人实现。本项目通过构建一系列由浅入深的系统，深入探讨了分布式环境下的共识算法、一致性模型和容错策略。

-----
<div align="center">
  <img src="https://skillicons.dev/icons?i=go,linux,github&perline=5" />
</div>

<p align="center">
  <img src="https://img.shields.io/badge/Concurrency-Goroutines-00ADD8?style=for-the-badge&logo=go" />
  <img src="https://img.shields.io/badge/Network-RPC-FFD700?style=for-the-badge" />
  <img src="https://img.shields.io/badge/Protocol-Raft-red?style=for-the-badge" />
</p>

## 🛠️ 实验模块详情

### Lab 1: MapReduce (分布式计算框架)

实现了一个类似于 Google MapReduce 论文的分布式框架。

  * **Coordinator & Worker：** 构建了一个中心化的协调器，负责任务调度（Map/Reduce 任务分配）和超时重试机制。
  * **容错设计：** 能够优雅处理 Worker 崩溃的情况，确保在动态变化的集群中任务的原子性。
  * **关键技术：** RPC 通信、Go 共享内存并发模型、文件系统原子重命名。

### Lab 2: Raft 共识算法 (核心)

从零实现 Raft 强一致性协议，这是后续所有分布式服务的基础。

  * **Leader Election：** 基于任期（Term）和心跳机制的领导者选举。
  * **Log Replication：** 确保所有节点日志的一致性，处理网络分区与日志冲突。
  * **Persistence：** 实现状态持久化，模拟服务器故障重启后的状态恢复。
  * **Log Compaction：** 基于 Snapshot 的日志压缩，解决日志无限增长问题。

### Lab 3: Fault-tolerant KV Service

基于 Raft 构建了一个可用的、强一致性的键值存储服务。

  * **Linearizability：** 确保客户端看到的执行顺序符合线性一致性。
  * **Duplicate Detection：** 通过客户端请求 ID 识别，解决网络重试导致的重复提案问题。

### Lab 4: Sharded KV Service (分片集群)

将 KV 服务扩展为支持横向缩容的分布式分片系统。

  * **Shard Controller：** 负责分片在副本组之间的负载均衡。
  * **Shard Migration：** 在不停止服务的前提下，实现分片数据在不同集群间的平滑迁移。

-----

## 🚀 性能优化与亮点

  * **精细锁粒度：** 避免了 Raft 实现中常见的死锁问题，通过分析并发竞争点（Race Condition）优化了锁的持有时间。
  * **RPC 高性能调用：** 采用 Go 原生 `net/rpc` 结合 `Channels` 实现异步处理模型。
  * **健壮性测试：** 通过了数千次 `go test -race` 压力测试，包括网络延迟、丢包、节点频繁宕机等极端情况。

-----

## 📦 如何运行

1.  **克隆仓库：**

    ```bash
    git clone https://github.com/chyl00/MIT6.824-CYL.git
    cd MIT6.824-CYL/src
    ```

2.  **运行 MapReduce 示例：**

    ```bash
    go run mrcoordinator.go pg-*.txt 
    go run mrworker.go wc.so
    ```

3.  **运行测试套件：**

    ```bash
    cd ./test_mr.sh
    ```

-----

## ⚠️ 声明 (Academic Integrity)

本项目代码仅用于个人学习记录和技术展示。如果你正在修读 MIT 6.824 或相关课程，**请务必遵守学术诚信准则**，不要直接复制或参考代码实现。

-----
