# Hudi 并发控制全链路深度解析

> 基于 Apache Hudi v1.2.0-SNAPSHOT 源码，全面剖析 Hudi 的多 Writer 并发控制机制。
> 涵盖：并发模式、锁机制、心跳机制、Marker 机制、冲突检测、冲突重试、端到端场景分析以及生产运维。

---

## 目录

- [第一部分：并发写入的问题域](#第一部分并发写入的问题域)
  - [1. 数据湖并发写入的挑战](#1-数据湖并发写入的挑战)
  - [2. WriteConcurrencyMode 三种模式完整对比](#2-writeconcurrencymode-三种模式完整对比)
- [第二部分：锁机制（Lock）](#第二部分锁机制lock)
  - [3. LockManager 完整源码解析](#3-lockmanager-完整源码解析)
  - [4. 所有 LockProvider 实现的深度对比](#4-所有-lockprovider-实现的深度对比)
  - [5. LockConfiguration 解析](#5-lockconfiguration-解析)
- [第三部分：心跳机制（Heartbeat）](#第三部分心跳机制heartbeat)
  - [6. HoodieHeartbeatClient 心跳机制](#6-hoodieheartbeatclient-心跳机制)
  - [7. 僵死 Writer 检测](#7-僵死-writer-检测)
  - [8. ClientIds 机制（Flink NBCC 场景）](#8-clientids-机制flink-nbcc-场景)
- [第四部分：Marker 机制（早期冲突检测）](#第四部分marker-机制早期冲突检测)
  - [9. WriteMarkers 体系完整源码](#9-writemarkers-体系完整源码)
  - [10. Marker 的创建时机和格式](#10-marker-的创建时机和格式)
  - [11. 基于 Marker 的早期冲突检测](#11-基于-marker-的早期冲突检测)
  - [12. MarkerDirState 和批量 Marker 创建优化](#12-markerdirstate-和批量-marker-创建优化)
- [第五部分：Pre-Commit 冲突检测（ConflictResolution）](#第五部分pre-commit-冲突检测conflictresolution)
  - [13. ConflictResolutionStrategy 接口](#13-conflictresolutionstrategy-接口)
  - [14. SimpleConcurrentFileWritesConflictResolutionStrategy](#14-simpleconcurrentfilewritesconflictresolutionstrategy)
  - [15. PreferWriterConflictResolutionStrategy](#15-preferwriterconflictresolutionstrategy)
  - [16. ConcurrentOperation](#16-concurrentoperation)
- [第六部分：冲突重试机制](#第六部分冲突重试机制)
  - [17. HoodieSparkSqlWriterInternal 的重试循环](#17-hoodiesparkqlwriterinternal-的重试循环)
  - [18. NUM_RETRIES_ON_CONFLICT_FAILURES 配置](#18-num_retries_on_conflict_failures-配置)
- [第七部分：端到端并发场景分析](#第七部分端到端并发场景分析)
  - [19. 场景 1: 两个 Spark 作业同时 upsert](#19-场景-1-两个-spark-作业同时-upsert)
  - [20. 场景 2: Spark 写入 + Compaction 并发](#20-场景-2-spark-写入--compaction-并发)
  - [21. 场景 3: Flink 多 Writer 并发](#21-场景-3-flink-多-writer-并发)
  - [22. 场景 4: NBCC 模式下的并发写入](#22-场景-4-nbcc-模式下的并发写入)
- [第八部分：生产运维](#第八部分生产运维)
  - [23. 并发控制配置完整手册](#23-并发控制配置完整手册)
  - [24. 多 Writer 架构模式推荐](#24-多-writer-架构模式推荐)
  - [25. 并发问题排查手册](#25-并发问题排查手册)

---

## 第一部分：并发写入的问题域

### 1. 数据湖并发写入的挑战

## 1. 解决什么问题
- **核心问题**:数据湖环境下多个Writer同时写入同一张表时,如何保证数据一致性和避免数据损坏
- **如果没有并发控制会有什么问题**:
  - 多个Writer同时修改同一个FileGroup,导致数据文件被覆盖或损坏
  - Timeline元数据不一致,无法正确追踪表的版本历史
  - 读取时可能看到不完整或错误的数据快照
  - 表服务(Compaction/Clustering)与数据写入冲突,导致数据丢失
- **实际应用场景**:
  - 多个Spark批处理作业同时写入不同分区
  - Flink流式作业实时写入 + Spark批处理修正历史数据
  - 数据写入与异步Compaction/Clustering并发执行
  - 多租户环境下不同团队同时操作同一张表

## 2. 有什么坑
- **误区1:认为HDFS的原子性能解决所有并发问题** - HDFS只保证单文件操作原子性,无法保证多文件事务
- **误区2:在S3上使用FileSystemBasedLockProvider** - S3不支持原子创建,会导致锁失效(源码:`StorageSchemes.isAtomicCreationSupported()`)
- **误区3:锁超时配置过短** - 默认60秒可能不够,大表的preCommit阶段可能需要更长时间
- **误区4:忽略心跳配置** - CPU争抢或GC暂停可能导致心跳过期,Writer被误判为死亡
- **性能陷阱**:
  - 过度使用早期冲突检测会增加marker创建开销
  - 锁重试次数过多导致作业长时间卡住
  - NBCC模式下读取性能下降(需要合并多个Writer的log)

## 3. 核心概念解释
- **FileGroup**:Hudi中数据组织的基本单元,由fileId唯一标识,包含base file和log files。冲突检测的粒度就是FileGroup级别
- **Timeline**:Hudi的元数据时间线,记录所有操作的Instant(REQUESTED->INFLIGHT->COMPLETED三态)
- **Instant**:表示一次写入操作,由时间戳唯一标识,经历三个状态转换
- **与其他系统对比**:
  - Delta Lake使用乐观并发控制 + 文件级冲突检测,类似Hudi的OCC模式
  - Iceberg使用乐观并发控制 + manifest文件版本控制
  - Hudi独有NBCC模式,允许多Writer写同一FileGroup(通过MOR表的log追加实现)

## 4. 设计理念
- **为什么选择FileGroup粒度**:
  - 行级锁在分布式文件系统上无法实现(没有共享内存)
  - 表级锁粒度太粗,限制并发度
  - FileGroup是数据和索引的最小管理单元,是最佳平衡点
  - 源码证据:`SimpleConcurrentFileWritesConflictResolutionStrategy.hasConflict()`通过计算FileGroup交集判断冲突
- **分层防御设计**:
  - 第1层WriteConcurrencyMode:在配置层面控制是否允许多Writer
  - 第2层Lock:通过分布式锁保护preCommit临界区
  - 第3层Heartbeat:检测Writer存活性,避免死锁
  - 第4层Marker:早期冲突检测,减少无效写入
  - 第5层ConflictResolution:最终冲突校验
  - 第6层Retry:失败后自动重试
- **架构演进**:
  - v0.x:只支持SINGLE_WRITER
  - v0.6+:引入OCC模式和LockProvider接口
  - v0.10+:引入NBCC模式支持Flink多Writer
  - v1.0+:引入StorageBasedLockProvider和早期冲突检测
- **与业界对比**:Hudi的NBCC模式是独创的,Delta Lake和Iceberg都只支持OCC模式

#### 1.1 为什么数据湖不能像数据库那样用行锁/页锁

在传统关系型数据库中，事务隔离通过行级锁（Row Lock）、页级锁（Page Lock）、表级锁（Table Lock）以及 MVCC 来实现。这些机制之所以行得通，是因为数据库拥有以下几个前提条件：

1. **数据和索引在同一进程/存储引擎中管理**：数据库引擎对每一行数据都有精确的物理地址，可以对特定行加锁。
2. **原子性操作由存储引擎保证**：B+树页面分裂、WAL 日志写入等操作都是原子的。
3. **共享内存 / 共享存储**：所有事务通过同一个 Buffer Pool 或共享存储访问数据。

而数据湖（Data Lake）的架构与此完全不同：

1. **数据以文件形式存储在分布式文件系统上**：HDFS、S3、GCS、Azure Blob Storage 等，文件是最小的操作单元。写入一个 Parquet 文件本质上就是写入一个完整的文件对象。你无法"锁住一行数据"，因为在存储层面根本没有"行"的概念。
2. **计算和存储分离**：多个 Spark 作业、Flink 作业可能运行在不同的集群上，通过网络访问同一个存储路径。它们之间没有共享内存。
3. **文件系统的原子性保证各异**：不同存储系统的语义不同，这直接影响了并发控制的策略选择。

**设计启示**：Hudi 不在行级别做并发控制，而是在 **FileGroup 级别** 做冲突检测。这是一个关键的架构决策——粒度足够粗（避免分布式锁开销过大），又足够细（不是表级锁，允许不同 FileGroup 的并发写入）。

#### 1.2 文件系统层面的并发问题

不同的存储系统在并发操作时的行为差异很大，Hudi 必须针对这些差异设计不同的锁实现：

**S3 的挑战：**
- S3 从 2020 年起提供了强一致性读，但在并发写入同一个 key 时，后写入的会覆盖前面的，没有原子 compare-and-swap。
- S3 不支持原子的 rename 操作（实际上是 copy + delete）。
- S3 不支持目录的原子创建（没有真正的目录概念）。
- 因此 S3 环境下不适合基于文件系统的锁，通常使用 DynamoDB 作为外部锁服务。

**HDFS 的特点：**
- HDFS 支持 `create(path, overwrite=false)` 的原子创建——如果文件已存在则失败。
- HDFS 的 rename 是原子的（在同一 NameNode 管理下）。
- HDFS 支持 lease 机制，可以知道哪个客户端持有文件的写权限。
- 因此 HDFS 环境下可以使用 `FileSystemBasedLockProvider`。

**云原生存储（如 Azure ADLS Gen2, GCS）：**
- ADLS Gen2 支持条件写入（conditional write），可以用 ETag 实现 CAS。
- GCS 支持 generation-match 条件写入。
- Hudi 的 `StorageBasedLockProvider` 正是为这类存储设计的。

**设计启示**：Hudi 将 `LockProvider` 设计为可插拔接口，正是因为底层存储的语义千差万别。用户根据自己的存储环境选择合适的 LockProvider 实现。

#### 1.3 数据湖表格式的事务模型

Hudi 的事务模型基于 **Timeline（时间线）**。每一次写入操作都会在时间线上创建一个 Instant，经历三个状态转换：

```
REQUESTED -> INFLIGHT -> COMPLETED
```

并发控制的核心问题就是：**当两个 Writer 同时处于 INFLIGHT 状态时，如何在 COMPLETED 之前发现并解决冲突？**

Hudi 的解决方案是一套"分层防御"体系：

```
第1层: WriteConcurrencyMode —— 决定是否允许多 Writer
第2层: LockManager + LockProvider —— 通过分布式锁保护关键临界区
第3层: Heartbeat —— 检测 Writer 是否还存活
第4层: Marker 机制 —— 在写入过程中（而非提交时）做早期冲突检测
第5层: ConflictResolutionStrategy —— 在 pre-commit 阶段做最终的冲突校验
第6层: Retry 机制 —— 冲突失败后的自动重试
```

---

### 2. WriteConcurrencyMode 三种模式完整对比

## 1. 解决什么问题
- **核心问题**:提供不同场景下的并发控制策略选择,平衡性能、一致性和可用性
- **如果没有模式选择会有什么问题**:
  - 单一策略无法满足所有场景:批处理需要高吞吐,流式需要高可用
  - 强制所有场景使用锁会降低性能
  - 不区分表类型(COW/MOR)会限制并发能力
- **实际应用场景**:
  - SINGLE_WRITER:定时ETL作业,开发测试环境
  - OCC:多个Spark作业写不同分区,数据写入+异步Compaction
  - NBCC:多个Flink流式作业实时写入,不能容忍写入失败

## 2. 有什么坑
- **坑1:在SINGLE_WRITER模式下启动多个Writer** - 不会报错,但会导致数据损坏(源码:`WriteConcurrencyMode.supportsMultiWriter()`返回false但不强制检查)
- **坑2:OCC模式下忘记配置LockProvider** - 会在运行时抛出`HoodieLockException`,作业失败
- **坑3:NBCC模式用在COW表** - 会在初始化时失败,因为NBCC依赖MOR表的log追加特性
- **坑4:误以为NBCC完全无锁** - NBCC内部仍有轻量级锁保护Timeline操作(源码:`PreferWriterConflictResolutionStrategy.isPreCommitRequired()`返回true)
- **性能陷阱**:
  - OCC模式下冲突率高时频繁重试,浪费资源
  - NBCC模式下log文件过多导致读取性能急剧下降
  - 错误选择模式导致锁竞争严重

## 3. 核心概念解释
- **SINGLE_WRITER**:同一时间只允许一个Writer,不需要任何并发控制机制
- **OCC(Optimistic Concurrency Control)**:乐观并发控制,写入时不加锁,提交时检测冲突。源码:`SimpleConcurrentFileWritesConflictResolutionStrategy`
- **NBCC(Non-Blocking Concurrency Control)**:非阻塞并发控制,允许多Writer写同一FileGroup,冲突由读取端解决。源码:`PreferWriterConflictResolutionStrategy`
- **与数据库OCC对比**:
  - 数据库OCC基于版本号或时间戳
  - Hudi OCC基于FileGroup交集检测
  - Hudi NBCC类似MVCC但冲突解决在读取端

## 4. 设计理念
- **为什么默认SINGLE_WRITER**:
  - 安全优先:避免用户误操作导致数据损坏
  - 性能最优:零并发控制开销
  - 符合大多数数据湖场景:ETL作业天然是单Writer
  - 源码证据:`HoodieWriteConfig.WRITE_CONCURRENCY_MODE.defaultValue() = "SINGLE_WRITER"`
- **为什么OCC选择FileGroup粒度**:
  - 粗粒度减少分布式锁开销
  - 细粒度允许不同FileGroup并发写入
  - 与Hudi的数据组织模型天然契合
- **为什么NBCC只支持MOR**:
  - MOR表的log文件支持追加写入
  - 不同Writer的log文件名不同(通过ClientId区分),物理上不冲突
  - COW表的base file是不可变的,无法支持多Writer同时修改
  - 源码证据:`WriteMarkers.create()`中NBCC模式会生成带ClientId的log文件名
- **架构权衡**:
  - SINGLE_WRITER牺牲并发能力换取最高性能
  - OCC牺牲部分性能(锁开销)换取并发能力
  - NBCC牺牲读取性能换取写入可用性

#### 2.1 源码位置与定义

**源码路径**：`hudi-common/src/main/java/org/apache/hudi/common/model/WriteConcurrencyMode.java`

```java
@EnumDescription("Concurrency modes for write operations.")
public enum WriteConcurrencyMode {
  // 单 Writer 模式
  @EnumFieldDescription("Only one active writer to the table. Maximizes throughput.")
  SINGLE_WRITER,

  // 乐观并发控制（OCC）
  @EnumFieldDescription("Multiple writers can operate on the table with lazy conflict resolution "
      + "using locks. This means that only one writer succeeds if multiple writers write to the "
      + "same file group.")
  OPTIMISTIC_CONCURRENCY_CONTROL,

  // 非阻塞并发控制（NBCC）
  @EnumFieldDescription("Multiple writers can operate on the table with non-blocking conflict resolution. "
      + "The writers can write into the same file group with the conflicts resolved automatically "
      + "by the query reader and the compactor.")
  NON_BLOCKING_CONCURRENCY_CONTROL;
}
```

关键辅助方法：

```java
public boolean supportsMultiWriter() {
  return this == OPTIMISTIC_CONCURRENCY_CONTROL || this == NON_BLOCKING_CONCURRENCY_CONTROL;
}

public boolean isOptimisticConcurrencyControl() {
  return this == OPTIMISTIC_CONCURRENCY_CONTROL;
}

public boolean isNonBlockingConcurrencyControl() {
  return this == NON_BLOCKING_CONCURRENCY_CONTROL;
}
```

#### 2.2 SINGLE_WRITER 模式

**约束条件**：
- 同一时间只能有一个 Writer 对表进行写操作
- 不需要配置锁提供者
- 不需要冲突检测策略

**适用场景**：
- 单个 Spark 批处理作业定时写入
- 开发/测试环境
- 对吞吐量要求极高且能保证单 Writer 的场景

**性能特征**：
- **最高吞吐量**：没有锁开销，没有冲突检测开销
- **最低延迟**：不需要等待锁获取
- 内部表服务（Compaction、Clustering、Clean）由同一个 Writer 以 inline 方式执行

**为什么这么设计**：单 Writer 模式是 Hudi 的默认模式，也是最安全、最高效的模式。在大多数数据湖场景中，ETL 作业是按计划调度执行的（例如每小时一次），天然就是单 Writer。强制要求用户配置多 Writer 支持才能使用多 Writer，避免了用户在不理解并发控制的情况下误操作导致数据损坏。

**好处**：零开销，最高性能。把复杂性留给真正需要多 Writer 的场景。

#### 2.3 OPTIMISTIC_CONCURRENCY_CONTROL（OCC）模式

**约束条件**：
- 必须配置 `LockProvider`（`hoodie.write.lock.provider` 配置项）
- 必须配置 `ConflictResolutionStrategy`（默认 `SimpleConcurrentFileWritesConflictResolutionStrategy`）
- 所有 Writer 必须使用相同的 LockProvider

**适用场景**：
- 多个 Spark 作业写入同一张 Hudi 表的不同分区
- 数据写入和表服务（如异步 Compaction）需要并行执行
- 写入冲突概率较低的场景（如按分区隔离写入）

**性能特征**：
- **写入性能**：比 SINGLE_WRITER 低，因为 pre-commit 阶段需要获取锁并做冲突检测
- **冲突处理**：如果两个 Writer 修改了同一个 FileGroup，后提交的 Writer 会失败
- **锁粒度**：表级锁，pre-commit 阶段串行化

**核心流程**：
1. Writer 正常执行写入（不加锁）
2. pre-commit 阶段获取分布式锁
3. 持锁期间刷新 Timeline，检查是否有冲突
4. 如果没有冲突，完成 commit
5. 释放锁

**为什么叫"乐观"**：因为写入阶段不加锁，"乐观地"假设不会有冲突，只在提交时才检查。这与数据库中乐观并发控制的概念一致。好处是写入阶段完全并行，只有短暂的 pre-commit 阶段需要串行化。

#### 2.4 NON_BLOCKING_CONCURRENCY_CONTROL（NBCC）模式

**约束条件**：
- **只支持 MOR（Merge On Read）表类型**：因为 NBCC 依赖 log file 追加写入
- 使用 `PreferWriterConflictResolutionStrategy`
- 冲突由读取端（query reader）和 Compaction 来解决

**适用场景**：
- Flink 多 Writer 实时写入同一张 MOR 表
- 写入端不能容忍任何失败/重试的场景
- 多个数据管道需要同时向同一个 FileGroup 追加数据

**性能特征**：
- **最高写入成功率**：写入几乎不会因冲突而失败
- **读取开销增加**：读取端需要合并多个 Writer 的 log，增加查询复杂度
- **Compaction 开销增加**：Compaction 需要处理来自多个 Writer 的 log

**为什么可以"不阻塞"**：
在 MOR 表中，每个 Writer 追加写 log 文件。不同 Writer 产生的 log 文件名不同（通过 ClientId 区分），因此在物理层面不存在冲突。冲突的解决被延迟到了读取时——Reader 会按照 ordering field 对来自不同 Writer 的记录进行合并去重。

**好处**：写入端几乎零冲突，极大简化了多 Writer 场景的运维。但代价是读取端复杂度增加。

#### 2.5 三种模式对比表

| 特性 | SINGLE_WRITER | OCC | NBCC |
|------|-------------|-----|------|
| 多 Writer 支持 | 否 | 是 | 是 |
| 需要锁 | 否 | 是 | 否（内部仍有轻量锁） |
| 冲突检测时机 | 无 | Pre-commit | 读取时/Compaction 时 |
| 写入失败可能性 | 无 | 有（文件组冲突时） | 几乎没有 |
| 支持的表类型 | COW + MOR | COW + MOR | 仅 MOR |
| 吞吐量 | 最高 | 中等 | 高 |
| 读取复杂度 | 正常 | 正常 | 较高 |
| 配置复杂度 | 最低 | 中等 | 中等 |
| 适用引擎 | Spark/Flink | Spark/Flink | 主要 Flink |

---

## 第二部分：锁机制（Lock）

### 3. LockManager 完整源码解析

## 1. 解决什么问题
- **核心问题**:统一管理LockProvider的生命周期,提供可靠的锁获取/释放接口
- **如果没有LockManager会有什么问题**:
  - 每个Writer需要自己管理LockProvider的创建和销毁,容易出错
  - 锁获取失败时的重试逻辑需要重复实现
  - 无法统一收集锁相关的metrics
  - 连接泄漏风险(忘记关闭LockProvider)
- **实际应用场景**:
  - 所有OCC模式的写入操作都通过LockManager获取锁
  - TransactionManager依赖LockManager保护preCommit临界区
  - 表服务(Compaction/Clustering)通过LockManager避免与数据写入冲突

## 2. 有什么坑
- **坑1:锁超时配置不合理** - 默认60秒可能不够,大表preCommit需要更长时间。源码:`writeConfig.getLockAcquireWaitTimeoutInMs()`
- **坑2:重试次数过多导致作业卡死** - 默认50次重试,每次等待5秒,最坏情况54分钟。源码:`maxRetries=50, maxWaitTimeInMs=5000`
- **坑3:忘记unlock导致死锁** - 必须在finally块中调用unlock。源码:`BaseHoodieClient.commitStats()`中使用try-finally
- **坑4:多线程共享LockManager** - LockManager不是线程安全的,每个Writer应该有独立实例
- **性能陷阱**:
  - 懒加载LockProvider导致首次获取锁时延迟较高
  - 每次unlock后销毁LockProvider,下次lock需要重新创建连接
  - ZK session超时后重试会导致长时间等待

## 3. 核心概念解释
- **LockProvider**:锁的具体实现接口,支持FileSystem/ZK/DynamoDB等多种实现
- **RetryHelper**:封装重试逻辑的工具类,支持指数退避和异常过滤
- **HoodieLockMetrics**:收集锁相关指标(获取次数、耗时、失败次数等)
- **懒加载**:LockProvider在首次调用`getLockProvider()`时才创建,避免不必要的连接开销
- **两级重试**:
  - 内层:LockProvider自己的重试(如ZK的`InterProcessMutex.acquire()`)
  - 外层:LockManager的RetryHelper重试

## 4. 设计理念
- **为什么使用volatile修饰lockProvider**:
  - `getLockProvider()`使用synchronized保证懒加载线程安全
  - `closeQuietly()`可能从其他线程调用,将lockProvider置为null
  - volatile保证多线程可见性
  - 源码证据:`private volatile LockProvider lockProvider;`
- **为什么每次unlock后销毁LockProvider**:
  - 确保每次锁操作使用新鲜连接,避免使用过期的ZK session
  - 防止连接泄漏
  - 让metrics准确追踪每次锁持有时间
  - 源码证据:`unlock()`调用`close()`将lockProvider置为null
- **为什么需要两级重试**:
  - 内层重试处理LockProvider自身的瞬时故障(如ZK连接抖动)
  - 外层重试处理LockProvider初始化失败、连接断开等上层故障
  - 两级参数独立调整,提供更灵活的容错能力
  - 源码证据:`lockRetryHelper.start()`包裹`getLockProvider().tryLock()`
- **为什么注入ApplicationId**:
  - 锁持有者可以被识别,方便排查死锁
  - 在锁文件或ZK节点中记录持有者信息
  - 源码证据:`lockPropsWithAppId.put(LockConfiguration.LOCK_HOLDER_APP_ID_KEY, writeConfig.getApplicationId())`

#### 3.1 源码位置与职责

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/lock/LockManager.java`

`LockManager` 是锁机制的核心管理类，它封装了 `LockProvider` 的生命周期管理，提供了统一的 `lock()` / `unlock()` 接口。它是 `TransactionManager` 的直接依赖。

#### 3.2 类结构

```java
@Slf4j
public class LockManager implements Serializable, AutoCloseable {
  private final HoodieWriteConfig writeConfig;
  private final LockConfiguration lockConfiguration;
  private final StorageConfiguration<?> storageConf;
  private final int maxRetries;
  private final long maxWaitTimeInMs;
  private final RetryHelper<Boolean, HoodieLockException> lockRetryHelper;
  private transient HoodieLockMetrics metrics;
  private volatile LockProvider lockProvider;
  // ...
}
```

**为什么使用 `volatile` 修饰 `lockProvider`**：虽然 `getLockProvider()` 方法使用了 `synchronized` 关键字来保证懒加载的线程安全，但 `closeQuietly()` 方法会将 `lockProvider` 置为 `null`，而 `close()` 可能从其他线程调用。`volatile` 保证了多线程环境下对 `lockProvider` 引用的可见性，确保一个线程将其置为 `null` 后，其他线程能立即看到更新。

#### 3.3 构造函数解析

```java
public LockManager(HoodieWriteConfig writeConfig, HoodieStorage storage, TypedProperties lockProps) {
  this.writeConfig = writeConfig;
  this.storageConf = storage.getConf().newInstance();
  TypedProperties lockPropsWithAppId = new TypedProperties();
  lockPropsWithAppId.putAll(lockProps);
  lockPropsWithAppId.put(LockConfiguration.LOCK_HOLDER_APP_ID_KEY, writeConfig.getApplicationId());
  this.lockConfiguration = new LockConfiguration(lockPropsWithAppId);
  maxRetries = lockConfiguration.getConfig().getInteger(LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP_KEY,
      Integer.parseInt(HoodieLockConfig.LOCK_ACQUIRE_CLIENT_NUM_RETRIES.defaultValue()));
  maxWaitTimeInMs = lockConfiguration.getConfig().getLong(LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY,
      Long.parseLong(HoodieLockConfig.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS.defaultValue()));
  metrics = new HoodieLockMetrics(writeConfig, storage);
  lockRetryHelper = new RetryHelper<>(maxWaitTimeInMs, maxRetries, maxWaitTimeInMs,
      Arrays.asList(HoodieLockException.class, InterruptedException.class), "acquire lock");
}
```

**设计要点**：
1. **AppId 注入**：将当前应用的 ApplicationId（如 Spark App Id）注入到锁配置中，这样锁持有者可以被识别。这在排查死锁时非常有用——你可以看到是哪个 Spark 应用持有了锁。
2. **StorageConfiguration 拷贝**：调用 `storage.getConf().newInstance()` 创建一份新的存储配置副本，避免共享可变状态。
3. **RetryHelper 初始化**：构造时就创建好重试帮手，它能处理 `HoodieLockException` 和 `InterruptedException`。

#### 3.4 lock() 方法的重试逻辑

```java
public void lock() {
  lockRetryHelper.start(() -> {
    try {
      metrics.startLockApiTimerContext();
      if (!getLockProvider().tryLock(writeConfig.getLockAcquireWaitTimeoutInMs(), TimeUnit.MILLISECONDS)) {
        metrics.updateLockNotAcquiredMetric();
        throw new HoodieLockException("Unable to acquire the lock. Current lock owner information : "
            + getLockProvider().getCurrentOwnerLockInfo());
      }
      metrics.updateLockAcquiredMetric();
      return true;
    } catch (InterruptedException e) {
      throw new HoodieLockException(e);
    }
  });
}
```

这段代码的执行流程：

1. `RetryHelper.start()` 接收一个 `Callable<Boolean>` lambda。
2. Lambda 内部首先记录锁获取计时的起始时间（metrics）。
3. 调用 `getLockProvider().tryLock()` 尝试获取锁，等待时间由 `hoodie.write.lock.wait_time_ms` 配置（默认 60 秒）。
4. 如果 `tryLock` 返回 `false`，说明在等待时间内未获取到锁，则抛出 `HoodieLockException`。
5. `RetryHelper` 捕获 `HoodieLockException` 后，等待 `maxWaitTimeInMs`（默认 5 秒），然后重试。
6. 总共重试 `maxRetries` 次（默认 50 次，由 `hoodie.write.lock.client.num_retries` 控制）。
7. 如果所有重试都失败，`RetryHelper` 抛出最终异常。

**重试总耗时计算**：默认配置下，最坏情况下的锁获取总耗时 = 50 次 * (60 秒等待 + 5 秒重试间隔) = 50 * 65 = 3250 秒，约 54 分钟。这个值在生产中需要根据实际情况调整。

**为什么需要两级重试**：
- **内层重试**（LockProvider 层面）：`tryLock(timeout)` 由具体的 LockProvider 实现，如 ZooKeeper 的 `InterProcessMutex.acquire()` 有自己的重试机制。
- **外层重试**（LockManager 层面）：`RetryHelper` 提供了更上层的重试能力，可以处理 LockProvider 初始化失败、连接断开等瞬时故障。

**好处**：两级重试机制让系统对各种瞬时故障有很强的容错能力，而且每级的参数都可以独立调整。

#### 3.5 懒加载 LockProvider

```java
public synchronized LockProvider getLockProvider() {
  if (lockProvider == null) {
    log.info("LockProvider " + writeConfig.getLockProviderClass());
    
    // 优先尝试带 metrics 的构造函数
    Class<?>[] metricsConstructorTypes = {LockConfiguration.class, StorageConfiguration.class, HoodieLockMetrics.class};
    if (ReflectionUtils.hasConstructor(writeConfig.getLockProviderClass(), metricsConstructorTypes)) {
      lockProvider = (LockProvider) ReflectionUtils.loadClass(writeConfig.getLockProviderClass(),
          metricsConstructorTypes, lockConfiguration, storageConf, metrics);
    } else {
      // 回退到标准构造函数
      lockProvider = (LockProvider) ReflectionUtils.loadClass(writeConfig.getLockProviderClass(),
              new Class<?>[] {LockConfiguration.class, StorageConfiguration.class},
              lockConfiguration, storageConf);
    }
  }
  return lockProvider;
}
```

**设计要点**：
1. **懒加载**：LockProvider 可能需要建立到 ZooKeeper / DynamoDB 等外部系统的连接。如果在构造 LockManager 时就创建连接，但实际从不需要锁（如 SINGLE_WRITER 模式），就会浪费资源。懒加载确保只有真正需要锁时才建立连接。
2. **反射加载**：通过 `ReflectionUtils.loadClass()` 动态加载 LockProvider 类，实现了完全的可插拔性。用户可以实现自己的 LockProvider 而不需要修改 Hudi 源码。
3. **向后兼容**：先尝试带 `HoodieLockMetrics` 的三参数构造函数，如果不存在则回退到两参数构造函数。这保证了老版本的 LockProvider 实现仍然可以使用。

#### 3.6 unlock() 和 close()

```java
public void unlock() {
  getLockProvider().unlock();
  try {
    metrics.updateLockHeldTimerMetrics();
  } catch (HoodieException e) {
    log.error(String.format("Exception encountered when updating lock metrics: %s", e));
  }
  metrics.updateLockReleaseSuccessMetric();
  close();
}

private void closeQuietly() {
  try {
    if (lockProvider != null) {
      lockProvider.close();
      log.info("Released connection created for acquiring lock");
      lockProvider = null;
    }
  } catch (Exception e) {
    log.error("Unable to close and release connection created for acquiring lock", e);
  }
}
```

**关键设计**：`unlock()` 在释放锁后会调用 `close()` 来销毁 LockProvider 实例（置为 null）。这意味着每次 lock/unlock 循环都会重新创建 LockProvider。这看起来似乎是"浪费"，但实际上是有意为之的：
- 确保每次锁操作都使用新鲜的连接，避免使用过时的 ZK session
- 防止连接泄漏
- 让 metrics 可以准确地追踪每次锁持有时间

---

### 4. 所有 LockProvider 实现的深度对比

## 1. 解决什么问题
- **核心问题**:适配不同存储系统和部署环境的分布式锁需求
- **如果只有一种LockProvider会有什么问题**:
  - S3环境无法使用FileSystemBasedLockProvider(不支持原子创建)
  - 没有ZK的环境无法使用ZookeeperBasedLockProvider
  - 云原生环境需要Serverless的锁服务
  - 测试环境需要轻量级的进程内锁
- **实际应用场景**:
  - InProcessLockProvider:单JVM多线程测试
  - FileSystemBasedLockProvider:HDFS环境
  - StorageBasedLockProvider:云原生存储(S3+条件写入/ADLS/GCS)
  - ZookeeperBasedLockProvider:已有ZK集群的Hadoop环境
  - DynamoDBBasedLockProvider:AWS S3环境
  - HiveMetastoreBasedLockProvider:Hive生态系统

## 2. 有什么坑
- **坑1:S3上使用FileSystemBasedLockProvider** - S3不支持原子创建,锁会失效。源码:`StorageSchemes.isAtomicCreationSupported()`
- **坑2:FileSystemBasedLockProvider锁过期未配置** - 默认0表示永不过期,Writer崩溃后锁永久残留
- **坑3:ZK session超时配置过短** - 网络抖动导致session超时,锁被意外释放
- **坑4:DynamoDB表未提前创建** - 首次使用会自动建表,可能因权限不足失败
- **坑5:StorageBasedLockProvider时钟漂移** - 不同节点时钟偏差导致锁过期判断错误
- **性能陷阱**:
  - InProcessLockProvider不支持跨进程,误用导致并发控制失效
  - HiveMetastoreBasedLockProvider延迟较高(100ms+)
  - DynamoDB锁依赖网络,延迟不稳定

## 3. 核心概念解释
- **原子创建**:文件系统的create(path, overwrite=false)操作,多进程同时创建只有一个成功
- **条件写入**:类似数据库的CAS操作,基于版本号或ETag的原子更新
- **心跳续租**:锁有有效期,持锁期间定期延长有效期,避免死锁
- **Shutdown Hook**:JVM关闭时自动释放锁的钩子函数
- **Session机制**:ZK的session超时后锁自动释放,天然处理客户端崩溃

## 4. 设计理念
- **为什么设计为可插拔接口**:
  - 底层存储语义千差万别,无法用统一实现
  - 用户可以根据环境选择合适的LockProvider
  - 支持自定义实现,不需要修改Hudi源码
  - 源码证据:`LockProvider`接口 + `ReflectionUtils.loadClass()`动态加载
- **为什么FileSystemBasedLockProvider需要检查原子创建支持**:
  - 如果文件系统不支持原子创建,多进程可能同时"成功"
  - 导致锁失效,数据损坏
  - 检查是安全保障
  - 源码证据:`if (!StorageSchemes.isAtomicCreationSupported(...)) { throw new HoodieLockException(...); }`
- **为什么StorageBasedLockProvider需要心跳续租**:
  - 锁有有效期,避免死锁
  - 长时间持锁需要定期延长有效期
  - 心跳失败说明进程卡顿,应该释放锁
  - 源码证据:`this.heartbeatManager = heartbeatManagerLoader.apply(..., this::renewLock)`
- **为什么StorageBasedLockProvider有时钟漂移缓冲**:
  - 分布式环境不同节点时钟可能有偏差
  - 加上500ms缓冲避免因时钟漂移误判锁过期
  - 源码证据:`private static final long CLOCK_DRIFT_BUFFER_MS = 500;`

Hudi 提供了 7 种 LockProvider 实现（含 2 个抽象基类和它们的具体子类，共 9 个具体实现），覆盖了从进程内到云原生的各种场景。

#### 4.1 InProcessLockProvider

**源码路径**：`hudi-common/src/main/java/org/apache/hudi/client/transaction/lock/InProcessLockProvider.java`

```java
@Slf4j
public class InProcessLockProvider implements LockProvider<ReentrantReadWriteLock>, Serializable {
  private static final Map<String, ReentrantReadWriteLock> LOCK_INSTANCE_PER_BASEPATH = new ConcurrentHashMap<>();
  private final ReentrantReadWriteLock lock;
  private final String basePath;
  private final long maxWaitTimeMillis;
}
```

**实现原理**：
- 使用 JDK 的 `ReentrantReadWriteLock` 实现进程内互斥
- 使用 `static ConcurrentHashMap` 维护每个表路径（basePath）对应的锁实例
- 同一 JVM 内所有操作同一张表的线程共享同一把锁

```java
public InProcessLockProvider(final LockConfiguration lockConfiguration, final StorageConfiguration<?> conf) {
  basePath = lockConfiguration.getConfig().getProperty(HoodieCommonConfig.BASE_PATH.key());
  lock = LOCK_INSTANCE_PER_BASEPATH.computeIfAbsent(basePath, (ignore) -> new ReentrantReadWriteLock());
  maxWaitTimeMillis = typedProperties.getLong(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, ...);
}
```

**特殊设计——不支持重入**：

```java
public boolean tryLock(long time, TimeUnit unit) {
  if (lock.isWriteLockedByCurrentThread()) {
    throw new HoodieLockException(getLogMessage(LockState.ALREADY_ACQUIRED));
  }
  // ...
}
```

如果同一个线程尝试重入（再次获取已持有的锁），会直接抛异常。这是有意为之的：Hudi 的事务模型不应该出现嵌套事务，如果出现了说明代码有 bug。

**适用场景**：
- 单 JVM 多线程场景（如同一个 Spark Driver 中多个表写入线程）
- 测试环境
- **不适合跨进程/跨节点的并发控制**

**好处**：零外部依赖，最低延迟，用于保护同一进程内的并发。

#### 4.2 FileSystemBasedLockProvider

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/lock/FileSystemBasedLockProvider.java`

**实现原理**：通过在文件系统上创建一个锁文件来实现分布式互斥。核心依赖文件系统的 **原子创建** 语义——多个进程同时尝试创建同一个文件，只有一个会成功。

```java
private void acquireLock() {
  try (OutputStream os = storage.create(this.lockFile, false)) {  // false = 不覆盖
    if (!storage.exists(this.lockFile)) {
      initLockInfo();
      os.write(StringUtils.getUTF8Bytes(lockInfo.toString()));
    }
  } catch (IOException e) {
    throw new HoodieIOException(generateLogStatement(LockState.FAILED_TO_ACQUIRE), e);
  }
}
```

**过期检测机制**：

```java
private boolean checkIfExpired() {
  if (lockTimeoutMinutes == 0) {
    return false;  // 配置为0表示永不过期
  }
  long modificationTime = storage.getPathInfo(this.lockFile).getModificationTime();
  if (System.currentTimeMillis() - modificationTime > lockTimeoutMinutes * 60 * 1000L) {
    return true;
  }
  return false;
}
```

如果一个 Writer 获取锁后崩溃了，锁文件会残留。过期检测通过检查锁文件的修改时间来判断是否可以强制释放。

**存储兼容性检查**：

```java
List<String> customSupportedFSs = lockConfiguration.getConfig().getStringList(
    HoodieCommonConfig.HOODIE_FS_ATOMIC_CREATION_SUPPORT.key(), ",", new ArrayList<>());
if (!customSupportedFSs.contains(this.storage.getScheme()) 
    && !StorageSchemes.isAtomicCreationSupported(this.storage.getScheme())) {
  throw new HoodieLockException("Unsupported scheme :" + this.storage.getScheme() 
      + ", since this fs can not support atomic creation");
}
```

**为什么要检查原子创建支持**：如果底层文件系统不支持原子创建（如 S3 在某些情况下），多个进程可能同时"成功"创建锁文件，导致锁失效。这个检查是一个安全保障。

**适用场景**：
- HDFS 环境
- 支持原子创建的文件系统

**好处**：不需要额外的外部服务（如 ZooKeeper），直接利用数据湖本身的存储系统。

#### 4.3 StorageBasedLockProvider

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/lock/StorageBasedLockProvider.java`

这是 Hudi v1.x 引入的一种更加健壮的分布式锁实现，基于**条件写入（Conditional Write）**实现，并内置了心跳续租机制。

**核心特点**：

1. **条件写入保证原子性**：通过 `StorageLockClient` 接口进行条件更新，类似于数据库的 CAS（Compare And Swap）。

2. **心跳续租**：锁有有效期，持锁期间通过心跳定期延长有效期：
```java
this.heartbeatManager = heartbeatManagerLoader.apply(
    ownerId, TimeUnit.SECONDS.toMillis(heartbeatPollSeconds), this::renewLock);
```

3. **Shutdown Hook**：注册了 JVM 关闭钩子，在进程退出时尝试释放锁：
```java
shutdownThread = new Thread(() -> shutdown(true));
Runtime.getRuntime().addShutdownHook(shutdownThread);
```

**tryLock 核心流程**：

```java
public synchronized boolean tryLock() {
  // 1. 检查是否已持有锁（支持重入）
  if (actuallyHoldsLock()) {
    return true;
  }
  
  // 2. 检查不应该有活跃的心跳
  if (this.heartbeatManager.hasActiveHeartbeat()) {
    throw new HoodieLockException("Detected broken invariant");
  }
  
  // 3. 读取当前锁状态
  Pair<LockGetResult, Option<StorageLockFile>> latestLock = this.storageLockClient.readCurrentLockFile();
  
  // 4. 检查是否有其他持有者
  if (latestLock.getLeft() == LockGetResult.SUCCESS && isLockStillValid(latestLock.getRight().get())) {
    return false;  // 锁被他人持有
  }
  
  // 5. 尝试条件写入获取锁
  StorageLockData newLockData = new StorageLockData(false, lockExpirationMs, ownerId);
  Pair<LockUpsertResult, Option<StorageLockFile>> lockUpdateStatus = 
      this.storageLockClient.tryUpsertLockFile(newLockData, latestLock.getRight());
  
  // 6. 获取成功后启动心跳
  if (lockUpdateStatus.getLeft() == LockUpsertResult.SUCCESS) {
    this.setLock(lockUpdateStatus.getRight().get());
    this.heartbeatManager.startHeartbeatForThread(Thread.currentThread());
    return true;
  }
  return false;
}
```

**时钟漂移处理**：

```java
private static final long CLOCK_DRIFT_BUFFER_MS = 500;

protected boolean isCurrentTimeCertainlyOlderThanDistributedTime(long epochMs) {
  return getCurrentEpochMs() > epochMs + CLOCK_DRIFT_BUFFER_MS;
}
```

分布式环境中不同节点的时钟可能有偏差。加上 500ms 的缓冲可以避免因时钟漂移导致错误地认为锁已过期。

**适用场景**：
- 云原生存储环境（S3 + 条件写入、ADLS Gen2、GCS）
- 需要高可靠性分布式锁的场景
- 生产环境推荐选择

**好处**：不依赖额外的外部服务（如 ZooKeeper），利用存储系统自身的条件写入能力。心跳续租机制自动处理长时间持锁的场景。Shutdown Hook 机制尽量避免崩溃后的锁残留。

#### 4.4 BaseZookeeperBasedLockProvider

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/lock/BaseZookeeperBasedLockProvider.java`

**实现原理**：使用 Apache Curator 的 `InterProcessMutex`（分布式互斥锁）来实现跨进程锁。

```java
public abstract class BaseZookeeperBasedLockProvider implements LockProvider<InterProcessMutex>, Serializable {
  private final transient CuratorFramework curatorFrameworkClient;
  private volatile InterProcessMutex lock = null;
  protected final String zkBasePath;
  protected final String lockKey;
}
```

**ZooKeeper 路径管理**：

```java
private void createPathIfNotExists() {
  String lockPath = getLockPath();  // 例如 /hudi/locks/my_table
  String[] parts = lockPath.split("/");
  StringBuilder currentPath = new StringBuilder();
  for (String part : parts) {
    if (!part.isEmpty()) {
      currentPath.append("/").append(part);
      createNodeIfNotExists(currentPath.toString());
    }
  }
}
```

会逐级创建 ZK 节点，确保路径存在。使用 `NodeExistsException` 来处理并发创建的情况。

**锁获取**：

```java
private void acquireLock(long time, TimeUnit unit) throws Exception {
  InterProcessMutex newLock = new HoodieInterProcessMutex(
      this.curatorFrameworkClient, getLockPath(), this.lockConfiguration);
  boolean acquired = newLock.acquire(time, unit);
  if (!acquired) {
    throw new HoodieLockException(...);
  }
  lock = newLock;
}
```

**Curator 连接配置**：

```java
this.curatorFrameworkClient = CuratorFrameworkFactory.builder()
    .connectString(zkConnectUrl)
    .retryPolicy(new BoundedExponentialBackoffRetry(
        retryWaitTimeMs, maxRetryWaitTimeMs, numRetries))
    .sessionTimeoutMs(sessionTimeoutMs)
    .connectionTimeoutMs(connectionTimeoutMs)
    .build();
this.curatorFrameworkClient.start();
```

**为什么使用 `BoundedExponentialBackoffRetry`**：ZK 连接失败后不应该以固定间隔重试（会造成惊群效应），指数退避可以在 ZK 集群恢复时平滑重连。

**适用场景**：
- 已有 ZooKeeper 集群的 Hadoop 环境
- 需要强一致性的分布式锁
- 锁获取延迟要求较低（ZK 延迟通常在 ms 级别）

**好处**：ZK 的分布式锁是业界标准方案，成熟可靠。Session 机制天然处理了客户端崩溃的问题——session 超时后锁自动释放。

**具体子类**：
- `ZookeeperBasedLockProvider`（`hudi-client/hudi-client-common/.../lock/ZookeeperBasedLockProvider.java`）：需要用户显式配置 ZK base path 和 lock key。
- `ZookeeperBasedImplicitBasePathLockProvider`（`hudi-client/hudi-client-common/.../lock/ZookeeperBasedImplicitBasePathLockProvider.java`）：自动从表的 base path 推导 ZK 路径，无需显式配置 base path 和 lock key。

#### 4.5 HiveMetastoreBasedLockProvider

**源码路径**：`hudi-sync/hudi-hive-sync/src/main/java/org/apache/hudi/hive/transaction/lock/HiveMetastoreBasedLockProvider.java`

**实现原理**：利用 Hive Metastore 的表级锁机制来实现分布式互斥。

```java
public void acquireLock(long time, TimeUnit unit) throws ... {
  final LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, this.databaseName);
  lockComponent.setTablename(tableName);
  acquireLockInternal(time, unit, lockComponent);
}
```

**心跳机制**：HMS 锁需要定期心跳来维持锁的有效性：

```java
Heartbeat heartbeat = new Heartbeat(hiveClient, lock.getLockid());
long heartbeatIntervalMs = lockConfiguration.getConfig()
    .getLong(LOCK_HEARTBEAT_INTERVAL_MS_KEY, DEFAULT_LOCK_HEARTBEAT_INTERVAL_MS);
future = executor.scheduleAtFixedRate(heartbeat, heartbeatIntervalMs / 2, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
```

**超时处理**：

```java
try {
  this.lock = executor.submit(() -> hiveClient.lock(lockRequestFinal)).get(time, unit);
} catch (InterruptedException | TimeoutException e) {
  if (this.lock == null || this.lock.getState() != LockState.ACQUIRED) {
    LockResponse lockResponse = this.hiveClient.checkLock(lockRequest.getTxnid());
    if (lockResponse.getState() == LockState.ACQUIRED) {
      this.lock = lockResponse;
    } else {
      throw e;
    }
  }
}
```

即使 `get()` 超时，锁可能在 HMS 端已经获取成功了，所以需要再检查一下。

**适用场景**：
- Hive 生态系统中，已有 Hive Metastore 服务
- 需要与 Hive 的其他并发控制机制协调

**好处**：复用已有的 HMS 基础设施，不需要额外部署锁服务。与 Hive 生态天然集成。

#### 4.6 DynamoDBBasedLockProviderBase

**源码路径**：`hudi-aws/src/main/java/org/apache/hudi/aws/transaction/lock/DynamoDBBasedLockProviderBase.java`

**实现原理**：使用 AWS DynamoDB 的 `AmazonDynamoDBLockClient`（基于 DynamoDB 条件写入）来实现分布式锁。

```java
this.client = new AmazonDynamoDBLockClient(
    AmazonDynamoDBLockClientOptions.builder(dynamoDB, tableName)
        .withTimeUnit(TimeUnit.MILLISECONDS)
        .withLeaseDuration(leaseDuration)
        .withHeartbeatPeriod(leaseDuration / 3)
        .withCreateHeartbeatBackgroundThread(true)
        .build());
```

**自动建表**：

```java
if (!this.client.lockTableExists()) {
  createLockTableInDynamoDB(dynamoDB, tableName);
}
```

如果 DynamoDB 表不存在，会自动创建。表结构很简单，只有一个 hash key：

```java
AttributeDefinition.builder()
    .attributeName(DYNAMODB_ATTRIBUTE_NAME)  // "key"
    .attributeType(ScalarAttributeType.S)
    .build()
```

**锁获取**：

```java
public boolean tryLock(long time, TimeUnit unit) {
  lock = client.acquireLock(AcquireLockOptions.builder(dynamoDBPartitionKey)
      .withAdditionalTimeToWaitForLock(time)
      .withTimeUnit(TimeUnit.MILLISECONDS)
      .build());
  return lock != null && !lock.isExpired();
}
```

**适用场景**：
- AWS S3 + DynamoDB 环境（这是 AWS 上使用 Hudi 的标配方案）
- S3 不支持原子文件操作，因此需要 DynamoDB 作为外部锁服务

**好处**：DynamoDB 是 Serverless 的全托管服务，无需运维。心跳由 AWS Lock Client 自动管理。与 S3 生态天然配合。

**具体子类**：
- `DynamoDBBasedLockProvider`（`hudi-aws/.../lock/DynamoDBBasedLockProvider.java`）：需要用户显式配置 DynamoDB partition key。
- `DynamoDBBasedImplicitPartitionKeyLockProvider`（`hudi-aws/.../lock/DynamoDBBasedImplicitPartitionKeyLockProvider.java`）：自动从表的 base path 推导 partition key，无需显式配置。

#### 4.7 NoopLockProvider

**源码路径**：`hudi-common/src/main/java/org/apache/hudi/client/transaction/lock/NoopLockProvider.java`

```java
public class NoopLockProvider implements LockProvider<ReentrantReadWriteLock>, Serializable {
  @Override
  public boolean tryLock(long time, @Nonnull TimeUnit unit) { return true; }
  
  @Override
  public void unlock() { /* no op */ }
  
  @Override
  public void lock() { /* no op */ }
}
```

**为什么需要 NoopLockProvider**：在 Hudi 的 upgrade/downgrade 过程中，可能会嵌套调用需要锁的操作。但不是所有的 LockProvider 都支持重入。`NoopLockProvider` 被用在这种内部嵌套场景中，避免死锁。

代码注释明确指出："This is not meant to be used as a production grade lock provider."

#### 4.8 LockProvider 实现对比表

| 实现 | 外部依赖 | 适用存储 | 延迟 | 可靠性 | 运维成本 |
|------|---------|---------|------|--------|---------|
| InProcessLockProvider | 无 | 任何 | 最低(ns) | 仅进程内 | 无 |
| FileSystemBasedLockProvider | 无 | HDFS/支持原子创建的FS | 低(ms) | 中 | 低 |
| StorageBasedLockProvider | 无 | 支持条件写入的FS | 低(ms) | 高 | 低 |
| ZookeeperBasedLockProvider | ZooKeeper | 任何 | 低(ms) | 高 | 中 |
| ZookeeperBasedImplicitBasePathLockProvider | ZooKeeper | 任何 | 低(ms) | 高 | 中 |
| HiveMetastoreBasedLockProvider | HMS + ZK | 任何 | 中(100ms+) | 高 | 中 |
| DynamoDBBasedLockProvider | DynamoDB | AWS S3 | 中(10-100ms) | 高 | 低(Serverless) |
| DynamoDBBasedImplicitPartitionKeyLockProvider | DynamoDB | AWS S3 | 中(10-100ms) | 高 | 低(Serverless) |
| NoopLockProvider | 无 | 任何 | 零 | 无保障 | 无 |

---

### 5. LockConfiguration 解析

#### 5.1 源码位置

**源码路径**：`hudi-common/src/main/java/org/apache/hudi/common/config/LockConfiguration.java`

`LockConfiguration` 是一个简单的配置 DTO（Data Transfer Object），封装了所有锁相关的配置项。之所以放在 `hudi-common` 模块，是因为 HiveMetastoreBasedLockProvider 位于 `hudi-hive-sync` 模块，需要共享这些配置。

#### 5.2 配置项分类

**通用锁配置**（前缀 `hoodie.write.lock.`）：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.write.lock.wait_time_ms_between_retry` | 1000 | 锁获取重试间隔（LockProvider 级别） |
| `hoodie.write.lock.max_wait_time_ms_between_retry` | 16000 | 重试间隔上限（指数退避上限） |
| `hoodie.write.lock.client.wait_time_ms_between_retry` | 5000 | 锁获取重试间隔（LockManager 级别） |
| `hoodie.write.lock.num_retries` | 15 | LockProvider 级别重试次数 |
| `hoodie.write.lock.client.num_retries` | 50 | LockManager 级别重试次数 |
| `hoodie.write.lock.wait_time_ms` | 60000 | 单次 tryLock 的等待超时 |
| `hoodie.write.lock.heartbeat_interval_ms` | 60000 | HMS 锁心跳间隔 |

**文件系统锁配置**（前缀 `hoodie.write.lock.filesystem.`）：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.write.lock.filesystem.path` | 无(使用表meta目录) | 锁文件存储路径 |
| `hoodie.write.lock.filesystem.expire` | 0 | 锁过期时间(分钟)，0表示不过期 |

**ZooKeeper 锁配置**（前缀 `hoodie.write.lock.zookeeper.`）：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.write.lock.zookeeper.url` | 无 | ZK 连接地址 |
| `hoodie.write.lock.zookeeper.port` | 无 | ZK 端口 |
| `hoodie.write.lock.zookeeper.base_path` | 无 | ZK 节点基础路径 |
| `hoodie.write.lock.zookeeper.lock_key` | 表名 | ZK 节点锁 key |
| `hoodie.write.lock.zookeeper.session_timeout_ms` | 60000 | ZK session 超时 |
| `hoodie.write.lock.zookeeper.connection_timeout_ms` | 15000 | ZK 连接超时 |

**Hive Metastore 锁配置**（前缀 `hoodie.write.lock.hivemetastore.`）：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.write.lock.hivemetastore.database` | 无 | Hive 数据库名 |
| `hoodie.write.lock.hivemetastore.table` | 无 | Hive 表名 |
| `hoodie.write.lock.hivemetastore.uris` | 无 | HMS URI |

**为什么锁配置有这么多层级**：不同的使用场景需要不同的细粒度控制。比如 ZK 环境下可能需要调整 session 超时来适应网络状况，而 FileSystem 环境下需要配置过期时间来处理进程崩溃。分层配置让每种 LockProvider 都能获得最适合的参数。

---

## 第三部分：心跳机制（Heartbeat）

### 6. HoodieHeartbeatClient 心跳机制

## 1. 解决什么问题
- **核心问题**:如何判断一个Writer是否还在正常运行,避免死Writer残留的INFLIGHT文件阻塞其他Writer
- **如果没有心跳机制会有什么问题**:
  - Writer崩溃后INFLIGHT文件永久残留,其他Writer无法判断是否可以回滚
  - 锁持有者死亡但锁未释放,导致永久死锁
  - 无法区分"正在运行的慢Writer"和"已经死亡的Writer"
- **实际应用场景**:
  - Writer进程OOM崩溃,心跳文件停止更新
  - 网络分区导致Writer与存储系统断开,但进程仍在运行
  - CPU争抢导致Writer长时间无法更新心跳,被其他Writer判定为死亡
  - 表服务检查INFLIGHT instant的Writer是否存活,决定是否可以回滚

## 2. 有什么坑
- **坑1:心跳间隔配置过短** - 默认60秒,如果CPU争抢严重可能导致误判。源码:`heartbeatIntervalInMs`最小1秒
- **坑2:容忍miss次数过少** - 默认2次,GC暂停可能导致连续miss。源码:`numTolerableHeartbeatMisses`
- **坑3:心跳文件未及时清理** - stop()失败会导致心跳文件残留,影响后续判断
- **坑4:依赖文件系统时间戳** - 不同节点时钟漂移可能导致误判。源码:`storage.getPathInfo().getModificationTime()`
- **性能陷阱**:
  - 心跳间隔过短增加文件系统I/O压力
  - 大量Writer同时更新心跳导致存储系统热点
  - 心跳检查需要遍历所有INFLIGHT instant的心跳文件

## 3. 核心概念解释
- **心跳文件**:存储在`.hoodie/.heartbeat/<instantTime>`的空文件,通过修改时间判断存活性
- **心跳间隔**:Writer定期更新心跳文件的时间间隔,默认60秒
- **容忍miss次数**:允许连续miss的心跳次数,默认2次,超过则判定为死亡
- **maxAllowableHeartbeatIntervalInMs**:心跳超时阈值 = 间隔 * 容忍次数,默认120秒
- **自杀机制**:Writer发现自己心跳过期时主动中断,避免继续运行导致数据损坏。源码:`updateHeartbeat()`中`Thread.currentThread().interrupt()`

## 4. 设计理念
- **为什么使用文件修改时间而非文件内容**:
  - 修改时间由文件系统自动维护,无需解析文件内容
  - 减少I/O开销,只需要stat操作而非read操作
  - 跨语言/跨引擎兼容性好
  - 源码证据:`storage.getPathInfo(heartbeatFilePath).getModificationTime()`
- **为什么首次心跳是同步的**:
  - Timer提交任务到线程,无法保证何时执行
  - 如果首次心跳延迟,其他Writer可能在心跳文件创建前就检查,导致误判
  - 同步调用确保心跳文件在写入开始前已存在
  - 源码证据:`start()`中先调用`updateHeartbeat(instantTime)`,再启动Timer
- **为什么Timer设置为daemon**:
  - `new Timer(true)`创建daemon线程
  - 即使心跳未正确停止,JVM也能正常退出
  - 避免因Timer线程导致进程挂起
  - 源码证据:`private Timer timer = new Timer(true);`
- **为什么需要自杀机制**:
  - Writer发现自己心跳过期说明进程严重卡顿(CPU争抢/长GC)
  - 继续运行可能导致数据损坏(其他Writer可能已基于"此Writer已死"做了回滚)
  - 主动中断是最安全的选择
  - 源码证据:`if (heartbeat.getLastHeartbeatTime() != null && isHeartbeatExpired(instantTime)) { Thread.currentThread().interrupt(); }`

#### 6.1 源码位置与设计目标

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/heartbeat/HoodieHeartbeatClient.java`

心跳机制的设计目标是解决一个关键问题：**如何知道一个 Writer 是否还在正常运行？**

在分布式系统中，一个 Writer 进程可能因为各种原因停止（OOM、网络断开、手动 kill 等），但它创建的 INFLIGHT 文件仍然残留在 Timeline 上。如果没有心跳机制，其他 Writer 无法区分"正在运行的 Writer"和"已经死掉的 Writer"。

#### 6.2 心跳文件的存储结构

```
<table_base_path>/.hoodie/.heartbeat/
    ├── 20240115120000000    <- instant time 1 的心跳文件
    ├── 20240115130000000    <- instant time 2 的心跳文件
    └── ...
```

心跳文件的文件名就是 instant time，文件内容为空。Hudi 通过文件的**最后修改时间（modification time）**来判断心跳是否过期。

#### 6.3 核心数据结构

```java
@Data
static class Heartbeat {
  private String instantTime;
  private boolean isHeartbeatStarted = false;
  private boolean isHeartbeatStopped = false;
  private Long lastHeartbeatTime;
  private Integer numHeartbeats = 0;
  private Timer timer = new Timer(true);  // daemon timer
}
```

**为什么 Timer 设置为 daemon**：`new Timer(true)` 创建了一个 daemon 线程的定时器。这样即使心跳没有被正确停止，JVM 也能正常退出，不会因为 Timer 线程而挂起。

#### 6.4 心跳启动

```java
public void start(String instantTime) {
  Heartbeat heartbeat = instantToHeartbeatMap.get(instantTime);
  ValidationUtils.checkArgument(heartbeat == null || !heartbeat.isHeartbeatStopped(), 
      "Cannot restart a stopped heartbeat for " + instantTime);
  if (heartbeat != null && heartbeat.isHeartbeatStarted()) {
    return;  // 幂等操作
  }

  Heartbeat newHeartbeat = new Heartbeat();
  newHeartbeat.setHeartbeatStarted(true);
  instantToHeartbeatMap.put(instantTime, newHeartbeat);
  // 首次心跳是同步的，确保心跳文件在写入开始前已经创建
  updateHeartbeat(instantTime);
  newHeartbeat.getTimer().scheduleAtFixedRate(new HeartbeatTask(instantTime), 
      this.heartbeatIntervalInMs, this.heartbeatIntervalInMs);
}
```

**为什么首次心跳是同步的**：注释说得很清楚——"Since timer submits the task to a thread, no guarantee when that thread will get CPU cycles to generate the first heartbeat." 如果首次心跳也是异步的，可能在心跳文件创建前其他 Writer 就开始检查了，导致误判。

#### 6.5 心跳更新

```java
private void updateHeartbeat(String instantTime) throws HoodieHeartbeatException {
  try {
    Long newHeartbeatTime = System.currentTimeMillis();
    OutputStream outputStream = this.storage.create(
        new StoragePath(heartbeatFolderPath, instantTime), true);  // overwrite=true
    outputStream.close();
    Heartbeat heartbeat = instantToHeartbeatMap.get(instantTime);
    if (heartbeat.getLastHeartbeatTime() != null && isHeartbeatExpired(instantTime)) {
      // 如果心跳已经过期，说明 CPU 争抢太严重，无法按时更新心跳
      Thread.currentThread().interrupt();  // 中断当前 Timer 线程
    }
    heartbeat.setLastHeartbeatTime(newHeartbeatTime);
    heartbeat.setNumHeartbeats(heartbeat.getNumHeartbeats() + 1);
  } catch (IOException io) {
    // ...
  }
}
```

**关键设计**：
1. 心跳更新就是以 overwrite 模式重新创建心跳文件，文件系统会更新 modification time。
2. 自检机制：如果发现自己的心跳已经过期了，说明当前进程已经严重卡顿，主动中断自己。这是一种"自杀机制"——与其让一个僵尸进程继续运行可能损坏数据，不如让它立即停止。

#### 6.6 过期检测

```java
public boolean isHeartbeatExpired(String instantTime) throws IOException {
  Long currentTime = System.currentTimeMillis();
  Heartbeat lastHeartbeatForWriter = instantToHeartbeatMap.get(instantTime);
  if (lastHeartbeatForWriter == null) {
    // 内存中没有记录，回退到从文件系统读取
    long lastHeartbeatForWriterTime = getLastHeartbeatTime(this.storage, basePath, instantTime);
    lastHeartbeatForWriter = new Heartbeat();
    lastHeartbeatForWriter.setLastHeartbeatTime(lastHeartbeatForWriterTime);
  }
  return currentTime - lastHeartbeatForWriter.getLastHeartbeatTime() > this.maxAllowableHeartbeatIntervalInMs;
}
```

`maxAllowableHeartbeatIntervalInMs = heartbeatIntervalInMs * numTolerableHeartbeatMisses`

默认情况下，如果心跳间隔是 60 秒，容忍的 miss 次数（由 `hoodie.client.heartbeat.tolerable.misses` 控制），那么 `maxAllowableHeartbeatIntervalInMs` 就是超时阈值。超过这个时间没有心跳更新，就认为 Writer 已经死亡。

---

### 7. 僵死 Writer 检测

#### 7.1 HoodieHeartbeatUtils

**源码路径**：`hudi-common/src/main/java/org/apache/hudi/common/heartbeat/HoodieHeartbeatUtils.java`

```java
public static Long getLastHeartbeatTime(HoodieStorage storage, String basePath,
                                        String instantTime) throws IOException {
  StoragePath heartbeatFilePath = new StoragePath(
      HoodieTableMetaClient.getHeartbeatFolderPath(basePath), instantTime);
  if (storage.exists(heartbeatFilePath)) {
    return storage.getPathInfo(heartbeatFilePath).getModificationTime();
  } else {
    return 0L;  // 心跳文件不存在，返回0（极早的时间）
  }
}

public static boolean isHeartbeatExpired(String instantTime,
                                         long maxAllowableHeartbeatIntervalInMs,
                                         HoodieStorage storage, String basePath) throws IOException {
  Long currentTime = System.currentTimeMillis();
  Long lastHeartbeatTime = getLastHeartbeatTime(storage, basePath, instantTime);
  if (currentTime - lastHeartbeatTime > maxAllowableHeartbeatIntervalInMs) {
    LOG.warn("Heartbeat expired, for instant: {}", instantTime);
    return true;
  }
  return false;
}
```

**为什么放在 `hudi-common` 模块**：因为不仅 Writer 需要检测心跳，Reader 端（用于 PreferWriterConflictResolutionStrategy 中判断请求态 instant 是否有活跃 Writer）也需要使用这个工具类。放在 common 模块让各方都能访问。

#### 7.2 HeartbeatUtils 的 abort 机制

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/heartbeat/HeartbeatUtils.java`

```java
public static void abortIfHeartbeatExpired(String instantTime, HoodieTable table,
                                           HoodieHeartbeatClient heartbeatClient, HoodieWriteConfig config) {
  ValidationUtils.checkArgument(heartbeatClient != null);
  try {
    if (config.getFailedWritesCleanPolicy().isLazy() && heartbeatClient.isHeartbeatExpired(instantTime)) {
      throw new HoodieException(
          "Heartbeat for instant " + instantTime + " has expired, last heartbeat "
              + getLastHeartbeatTime(table.getStorage(), config.getBasePath(), instantTime));
    }
  } catch (IOException io) {
    throw new HoodieException("Unable to read heartbeat", io);
  }
}
```

这个方法在 **BaseHoodieWriteClient.commitStats()** 中被调用：

```java
// BaseHoodieWriteClient.java 中的 commitStats 方法（简化）
HeartbeatUtils.abortIfHeartbeatExpired(instantTime, table, heartbeatClient, config);
this.txnManager.beginStateChange(Option.of(inflightInstant), ...);
```

**为什么在提交前检查心跳**：如果一个 Writer 因为 GC 暂停或 CPU 争抢导致心跳过期，但实际上它还在运行并试图提交，这时应该中止它。因为其他 Writer 可能已经基于"这个 Writer 已死"的判断做了回滚操作，如果让它继续提交可能导致数据不一致。

**好处**：形成了一个安全网——即使心跳过期了，Writer 自己在提交前也会检查并自我中止，避免数据损坏。

---

### 8. ClientIds 机制（Flink NBCC 场景）

#### 8.1 源码位置与设计目标

**源码路径**：`hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/util/ClientIds.java`

`ClientIds` 是 Flink 场景下特有的机制，主要用于 NBCC（Non-Blocking Concurrency Control）模式下的多 Writer 协调。

在 NBCC 模式中，每个 Flink Writer 需要一个唯一的 Client ID，用于：
1. 区分不同 Writer 产生的 log 文件
2. 检测僵死的 Flink Writer

#### 8.2 心跳文件结构

```
<table_base_path>/.hoodie/.aux/.ids/
    ├── _       <- Client ID 为空的默认 Writer
    ├── _1      <- Client ID = "1" 的 Writer
    ├── _2      <- Client ID = "2" 的 Writer
    └── _3      <- Client ID = "3" 的 Writer
```

#### 8.3 Client ID 自动分配

```java
private String nextId(Configuration conf, String basePath) {
  Path heartbeatFolderPath = new Path(getHeartbeatFolderPath(basePath));
  FileSystem fs = HadoopFSUtils.getFs(heartbeatFolderPath, ...);
  
  if (!fs.exists(heartbeatFolderPath)) {
    return INIT_CLIENT_ID;  // 空字符串
  }
  
  List<Path> sortedPaths = Arrays.stream(fs.listStatus(heartbeatFolderPath))
      .map(FileStatus::getPath)
      .sorted(Comparator.comparing(Path::getName))
      .collect(Collectors.toList());
  
  // 1. 优先复用僵死 Writer 的 ID
  List<Path> zombieHeartbeatPaths = sortedPaths.stream()
      .filter(path -> ClientIds.isHeartbeatExpired(fs, path, this.heartbeatTimeoutThresholdInMs))
      .collect(Collectors.toList());
  if (!zombieHeartbeatPaths.isEmpty()) {
    for (Path path : zombieHeartbeatPaths) {
      fs.delete(path, true);  // 清理僵尸心跳
    }
    return getClientId(zombieHeartbeatPaths.get(0));  // 复用最小的僵尸 ID
  }
  
  // 2. 否则自增
  String largestClientId = getClientId(sortedPaths.get(sortedPaths.size() - 1));
  return INIT_CLIENT_ID.equals(largestClientId) ? "1" : (Integer.parseInt(largestClientId) + 1) + "";
}
```

**为什么要复用僵尸 ID 而不是一直自增**：
1. **ID 稳定性**：如果 Flink 作业重启（如 checkpoint 恢复），可以复用之前的 ID，减少对下游的影响。
2. **ID 空间管理**：避免 ID 无限增长，保持 ID 在一个合理的范围内。
3. **资源清理**：复用之前会先删除僵尸心跳文件，起到了垃圾回收的作用。

#### 8.4 心跳更新与过期检测

```java
private void updateHeartbeat(Path heartbeatFilePath) throws HoodieHeartbeatException {
  try (OutputStream outputStream = this.fs.create(heartbeatFilePath, true)) {
    // 不写内容，只是更新文件的 modification time
  } catch (IOException io) {
    throw new HoodieHeartbeatException("Unable to generate heartbeat for file path " + heartbeatFilePath, io);
  }
}

public static boolean isHeartbeatExpired(FileSystem fs, Path path, long timeoutThreshold) {
  try {
    if (fs.exists(path)) {
      long modifyTime = fs.getFileStatus(path).getModificationTime();
      long currentTime = System.currentTimeMillis();
      return currentTime - modifyTime > timeoutThreshold;
    }
  } catch (IOException e) {
    log.error("Check heartbeat file existence error: " + path);
  }
  return false;
}
```

**默认心跳参数**：
- `DEFAULT_HEARTBEAT_INTERVAL_IN_MS = 60 * 1000`（1 分钟）
- `DEFAULT_NUM_TOLERABLE_HEARTBEAT_MISSES = 5`（容忍 5 次 miss）
- 因此默认超时阈值 = 5 分钟

**好处**：ClientIds 机制让 Flink 的 NBCC 模式可以自动发现和清理僵死的 Writer，而不需要人工干预。每个新的 Flink Writer 启动时会自动获取一个合适的 Client ID。

---

## 第四部分：Marker 机制（早期冲突检测）

### 9. WriteMarkers 体系完整源码

## 1. 解决什么问题
- **核心问题**:追踪Writer写入过程中创建的所有数据文件,支持失败回滚和早期冲突检测
- **如果没有Marker机制会有什么问题**:
  - Writer崩溃后无法知道哪些数据文件是半成品,无法清理
  - 无法在写入过程中检测冲突,只能等到preCommit阶段
  - Rollback操作无法准确定位需要删除的文件
  - 无法区分"正在写入的文件"和"已提交的文件"
- **实际应用场景**:
  - Writer写入数据前先创建marker,声明写入意图
  - Writer崩溃后,Clean操作根据marker清理半成品文件
  - 早期冲突检测通过扫描marker目录发现其他Writer的写入
  - Commit时通过marker确认哪些文件需要加入Timeline

## 2. 有什么坑
- **坑1:Direct模式下大量小文件** - 每个数据文件对应一个marker文件,大规模写入会产生海量marker文件
- **坑2:TLS模式在HDFS上失效** - HDFS不支持TLS-based markers,会自动回退到Direct模式。源码:`WriteMarkersFactory.get()`
- **坑3:Marker未及时清理** - 写入失败后marker残留,影响后续冲突检测
- **坑4:早期冲突检测误判** - 心跳过期的Writer的marker应该被忽略,否则会误报冲突
- **性能陷阱**:
  - Direct模式下marker创建是同步的,阻塞数据写入
  - TLS模式下批量刷盘延迟可能导致marker丢失
  - 早期冲突检测需要扫描所有候选instant的marker目录

## 3. 核心概念解释
- **Marker文件**:格式为`<fileName>.marker.<IOType>`,IOType包括CREATE/MERGE/APPEND
- **Direct模式**:直接在文件系统创建marker文件,每个数据文件对应一个marker
- **TLS模式**:通过Timeline Server批量管理marker,减少文件系统操作
- **早期冲突检测**:在marker创建时检查是否有其他Writer的冲突marker,而非等到preCommit
- **MarkerDirState**:TLS模式下的内存缓存,批量合并marker创建请求

## 4. 设计理念
- **为什么先创建marker后写数据**:
  - Marker是写入意图的预先声明
  - Writer崩溃后,marker可以识别半成品文件
  - 早期冲突检测依赖marker判断其他Writer的写入范围
  - 源码证据:`WriteMarkers.create()`在数据文件写入前调用
- **为什么Compaction/Clustering跳过早期冲突检测**:
  - 表服务的冲突由`ConflictResolutionStrategy.isPreCommitRequired()`在preCommit阶段处理
  - 简化实现,避免表服务与数据写入之间的锁竞争
  - 表服务通常不会与数据写入同时操作同一FileGroup
  - 源码证据:`WriteMarkers.create()`中`if (pendingCompactionTimeline.containsInstant(instantTime)) { return create(...); }`
- **为什么TLS模式使用批量刷盘**:
  - 减少文件系统RPC次数,提高吞吐
  - 多个marker请求合并为少量MARKERS文件写入
  - Round-robin文件分配提高并行度
  - 源码证据:`MarkerDirState.processMarkerCreationRequests()`批量处理
- **为什么HDFS不支持TLS模式**:
  - TLS模式将marker缓存在内存,定期批量写入
  - HDFS的写入模型(一次写入不可修改)与批量追加不兼容
  - Direct模式在HDFS上性能已足够好
  - 源码证据:`WriteMarkersFactory.get()`中`if (StorageSchemes.HDFS.getScheme().equals(...)) { return getDirectWriteMarkers(...); }`

#### 9.1 体系结构

```
WriteMarkers (抽象基类)
├── DirectWriteMarkers          (V2, Table Version >= 8，直接文件系统)
│   └── DirectWriteMarkersV1    (V1, Table Version < 8)
├── TimelineServerBasedWriteMarkers     (V2, TLS 代理)
│   └── TimelineServerBasedWriteMarkersV1  (V1, TLS 代理)
```

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/marker/`

#### 9.2 WriteMarkers 抽象基类

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/marker/WriteMarkers.java`

```java
public abstract class WriteMarkers implements Serializable {
  protected final String basePath;
  protected final transient StoragePath markerDirPath;
  protected final String instantTime;
  
  // 核心方法
  abstract Option<StoragePath> create(String partitionPath, String fileName, IOType type, boolean checkIfExists);
  public abstract boolean deleteMarkerDir(HoodieEngineContext context, int parallelism);
  public abstract boolean doesMarkerDirExist() throws IOException;
  public abstract Set<String> createdAndMergedDataPaths(HoodieEngineContext context, int parallelism) throws IOException;
  public abstract Set<String> allMarkerFilePaths() throws IOException;
}
```

**Marker 与早期冲突检测的集成**：

```java
public Option<StoragePath> create(String partitionPath, String fileName, IOType type, 
    HoodieWriteConfig writeConfig, String fileId, HoodieActiveTimeline activeTimeline) {
  if (writeConfig.getWriteConcurrencyMode().isOptimisticConcurrencyControl() 
      && writeConfig.isEarlyConflictDetectionEnable()) {
    // 如果当前是 Compaction 或 Clustering，则跳过早期冲突检测
    HoodieTimeline pendingCompactionTimeline = activeTimeline.filterPendingCompactionTimeline();
    HoodieTimeline pendingReplaceTimeline = activeTimeline.filterPendingReplaceOrClusteringTimeline();
    if (pendingCompactionTimeline.containsInstant(instantTime) 
        || pendingReplaceTimeline.containsInstant(instantTime)) {
      return create(partitionPath, fileName, type, false);
    }
    return createWithEarlyConflictDetection(partitionPath, fileName, type, false, 
        writeConfig, fileId, activeTimeline);
  }
  return create(partitionPath, fileName, type, false);
}
```

**为什么 Compaction/Clustering 跳过早期冲突检测**：表服务（Table Service）的冲突由 `ConflictResolutionStrategy.isPreCommitRequired()` 在 pre-commit 阶段处理，不需要在 marker 创建时做早期检测。这简化了实现并避免了表服务与数据写入之间的锁竞争。

#### 9.3 WriteMarkersFactory

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/marker/WriteMarkersFactory.java`

```java
public static WriteMarkers get(MarkerType markerType, HoodieTable table, String instantTime) {
  switch (markerType) {
    case DIRECT:
      return getDirectWriteMarkers(table, instantTime);
    case TIMELINE_SERVER_BASED:
      if (!table.getConfig().isEmbeddedTimelineServerEnabled() && !table.getConfig().isRemoteViewStorageType()) {
        // TLS 未启用，回退到 Direct
        return getDirectWriteMarkers(table, instantTime);
      }
      if (StorageSchemes.HDFS.getScheme().equals(...)) {
        // HDFS 不支持 TLS-based markers，回退到 Direct
        return getDirectWriteMarkers(table, instantTime);
      }
      return new TimelineServerBasedWriteMarkers(table, instantTime);
    default:
      throw new HoodieException("The marker type \"" + markerType.name() + "\" is not supported.");
  }
}
```

**为什么 HDFS 不支持 TLS-based markers**：TLS-based markers 将 marker 数据缓存在 Timeline Server 的内存中，定期批量写入文件。HDFS 的写入模型（一次写入不可修改）与这种批量追加模式不太兼容。

---

### 10. Marker 的创建时机和格式

#### 10.1 Marker 文件命名规则

Marker 文件名格式：`[file_name].marker.[IO_type]`

```java
protected static String getMarkerFileName(String fileName, IOType type) {
  return String.format("%s%s.%s", fileName, HoodieTableMetaClient.MARKER_EXTN, type.name());
}
```

其中 `MARKER_EXTN = ".marker"`，`IOType` 有三种：
- `CREATE`：创建新的数据文件（insert）
- `MERGE`：合并已有文件和新数据（upsert 对 base file）
- `APPEND`：追加 log 文件（MOR 表的 upsert）

例如：
```
partition_path/file_id_1_0_20240115120000.parquet.marker.CREATE
partition_path/file_id_2_0_20240115120000.parquet.marker.MERGE
partition_path/.file_id_3_20240115120000.log.0_0-0-0.marker.APPEND
```

#### 10.2 Marker 文件存储位置

```
<table_base_path>/.hoodie/.temp/<instant_time>/
    ├── partition1/
    │   ├── file1.parquet.marker.CREATE
    │   └── file2.parquet.marker.MERGE
    └── partition2/
        └── file3.log.marker.APPEND
```

对于 V1（表版本 < 8），路径类似但具体布局略有不同。

#### 10.3 Marker 创建时机

Marker 在数据文件创建之前被创建。以 `DirectWriteMarkers.create()` 为例：

```java
private Option<StoragePath> create(StoragePath markerPath, boolean checkIfExists) {
  HoodieTimer timer = HoodieTimer.start();
  try {
    if (checkIfExists && storage.exists(markerPath)) {
      return Option.empty();
    }
    storage.create(markerPath, false).close();  // 创建空文件
  } catch (IOException e) {
    throw new HoodieException("Failed to create marker file " + markerPath, e);
  }
  log.info("[direct] Created marker file {} in {} ms", markerPath, timer.endTimer());
  return Option.of(markerPath);
}
```

**创建时序**：
1. Writer 接收到写入任务（如一个 partition 的 upsert 数据）
2. 确定目标文件（新建或合并）
3. **创建对应的 marker 文件**
4. 写入实际数据文件
5. 提交时，通过 marker 文件确认哪些文件被写入

**为什么先创建 marker 后写数据**：如果 Writer 在写数据过程中崩溃，marker 文件可以用来识别那些"半成品"数据文件，在后续的 rollback 中清理它们。这就是 marker 的核心价值——**预先声明写入意图**。

---

### 11. 基于 Marker 的早期冲突检测

#### 11.1 设计动机

传统的 OCC 模式下，冲突检测发生在 pre-commit 阶段。这意味着一个 Writer 可能写了几个小时的数据，最后在提交时才发现冲突，所有工作都白费了。

早期冲突检测（Early Conflict Detection）的目标是：**在写入过程中就检测冲突，尽早失败，减少浪费。**

#### 11.2 SimpleTransactionDirectMarkerBasedDetectionStrategy

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/marker/SimpleTransactionDirectMarkerBasedDetectionStrategy.java`

这是直接使用文件系统 API 做冲突检测的策略。

```java
public class SimpleTransactionDirectMarkerBasedDetectionStrategy
    extends SimpleDirectMarkerBasedDetectionStrategy {

  @Override
  public void detectAndResolveConflictIfNecessary() throws HoodieEarlyConflictDetectionException {
    DirectMarkerTransactionManager txnManager =
        new DirectMarkerTransactionManager((HoodieWriteConfig) config, storage, partitionPath, fileId);
    InstantGenerator instantGenerator = TimelineLayout.fromVersion(
        activeTimeline.getTimelineLayoutVersion()).getInstantGenerator();
    try {
      // 获取文件级别的锁
      txnManager.beginTransaction(instantTime, instantGenerator);
      // 检查是否有其他 Writer 的 marker 冲突
      super.detectAndResolveConflictIfNecessary();
    } catch (Exception e) {
      throw e;
    } finally {
      txnManager.endTransaction(instantTime, instantGenerator);
      txnManager.close();
    }
  }
}
```

**关键设计**：使用 `DirectMarkerTransactionManager` 来获取**文件级别**的 ZK 锁。锁的 key 是 `partitionPath/fileId`，这意味着不同 FileGroup 的 marker 检查可以并行进行。

#### 11.3 DirectMarkerBasedDetectionStrategy 的 checkMarkerConflict 方法

**源码路径**：`hudi-common/src/main/java/org/apache/hudi/common/conflict/detection/DirectMarkerBasedDetectionStrategy.java`

```java
public boolean checkMarkerConflict(String basePath, long maxAllowableHeartbeatIntervalInMs) throws IOException {
  String tempFolderPath = basePath + StoragePath.SEPARATOR + HoodieTableMetaClient.TEMPFOLDER_NAME;

  // 1. 获取候选 instant（排除已完成的和心跳过期的）
  List<String> candidateInstants = MarkerUtils.getCandidateInstants(activeTimeline,
      storage.listDirectEntries(new StoragePath(tempFolderPath)).stream()
          .map(StoragePathInfo::getPath)
          .collect(Collectors.toList()),
      instantTime, maxAllowableHeartbeatIntervalInMs, storage, basePath);

  // 2. 在候选 instant 的 marker 目录中搜索相同 fileId 的 marker
  long res = candidateInstants.stream().flatMap(currentMarkerDirPath -> {
    try {
      StoragePath markerPartitionPath = new StoragePath(currentMarkerDirPath, partitionPath);
      if (!storage.exists(markerPartitionPath)) {
        return Stream.empty();
      }
      return storage.listDirectEntries(markerPartitionPath).stream().parallel()
          .filter((path) -> path.toString().contains(fileId));
    } catch (IOException e) {
      throw new HoodieIOException("IOException occurs during checking marker file conflict");
    }
  }).count();

  if (res != 0L) {
    LOG.warn("Detected conflict marker files: {}/{} for {}", partitionPath, fileId, instantTime);
    return true;
  }
  return false;
}
```

**冲突检测逻辑**：
1. 列出 `.temp/` 目录下所有的 instant marker 目录
2. 过滤出"候选 instant"——不包括自己的 instant，也不包括心跳过期的 instant
3. 在候选 instant 的对应 partition 目录下查找是否存在相同 fileId 的 marker
4. 如果找到了，说明有其他活跃的 Writer 正在写同一个 FileGroup

**为什么这里也要检查心跳**：如果一个 Writer 已经崩溃（心跳过期），它残留的 marker 不应该被视为冲突。否则已死的 Writer 会永远阻止其他 Writer 写入同一个 FileGroup。

#### 11.4 AsyncTimelineServerBasedDetectionStrategy

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/marker/AsyncTimelineServerBasedDetectionStrategy.java`

这是基于 Timeline Server 的异步冲突检测策略。

```java
public class AsyncTimelineServerBasedDetectionStrategy extends TimelineServerBasedDetectionStrategy {
  private final AtomicBoolean hasConflict = new AtomicBoolean(false);
  private ScheduledExecutorService asyncDetectorExecutor;

  @Override
  public void startAsyncDetection(Long initialDelayMs, Long periodMs, String markerDir,
                                  String basePath, Long maxAllowableHeartbeatIntervalInMs,
                                  HoodieStorage storage, Object markerHandler,
                                  Set<HoodieInstant> completedCommits) {
    hasConflict.set(false);
    asyncDetectorExecutor = Executors.newSingleThreadScheduledExecutor();
    asyncDetectorExecutor.scheduleAtFixedRate(
        new MarkerBasedEarlyConflictDetectionRunnable(
            hasConflict, (MarkerHandler) markerHandler, markerDir, basePath,
            storage, maxAllowableHeartbeatIntervalInMs, completedCommits, checkCommitConflict),
        initialDelayMs, periodMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void detectAndResolveConflictIfNecessary() throws HoodieEarlyConflictDetectionException {
    if (hasMarkerConflict()) {
      resolveMarkerConflict(basePath, markerDir, markerName);
    }
  }
}
```

**与直接策略的区别**：
- 直接策略在每次创建 marker 时同步检查冲突（阻塞）
- 异步策略在后台定期检查冲突，创建 marker 时只检查标志位（非阻塞）
- 异步策略的检测有延迟，但不会阻塞数据写入

**好处**：异步策略适合大规模写入场景，写入吞吐不受冲突检测的影响。缺点是检测有延迟，可能会在检测到冲突前已经写了一些数据。

---

### 12. MarkerDirState 和批量 Marker 创建优化

#### 12.1 源码位置与设计动机

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/marker/MarkerDirState.java`

在大规模写入场景下，如果每个 marker 都单独创建一个文件（如 Direct 模式），会产生大量小文件和文件系统 RPC。Timeline Server Based 模式通过 `MarkerDirState` 将多个 marker 请求批量合并，写入少量的"MARKERS*"文件中。

#### 12.2 核心数据结构

```java
public class MarkerDirState implements Serializable {
  // 所有 marker 的内存缓存
  private final Set<String> allMarkers = new HashSet<>();
  // 每个底层文件的 marker 内容缓存
  private final Map<Integer, StringBuilder> fileMarkersMap = new HashMap<>();
  // 文件使用状态（用于线程分配）
  private final List<Boolean> threadUseStatus;
  // 待处理的 marker 创建请求
  private final List<MarkerCreationFuture> markerCreationFutures = new ArrayList<>();
}
```

#### 12.3 批量创建流程

1. **请求入队**：
```java
public void addMarkerCreationFuture(MarkerCreationFuture future) {
  synchronized (markerCreationFutures) {
    markerCreationFutures.add(future);
  }
}
```

2. **获取文件槽位**（round-robin）：
```java
public Option<Integer> getNextFileIndexToUse() {
  synchronized (markerCreationProcessingLock) {
    for (int i = 0; i < threadUseStatus.size(); i++) {
      int index = (lastFileIndexUsed + 1 + i) % threadUseStatus.size();
      if (!threadUseStatus.get(index)) {
        fileIndex = index;
        threadUseStatus.set(index, true);
        break;
      }
    }
    // ...
  }
}
```

3. **批量处理**：
```java
public void processMarkerCreationRequests(
    final List<MarkerCreationFuture> pendingMarkerCreationFutures, int fileIndex) {
  synchronized (markerCreationProcessingLock) {
    for (MarkerCreationFuture future : pendingMarkerCreationFutures) {
      String markerName = future.getMarkerName();
      boolean exists = allMarkers.contains(markerName);
      if (!exists) {
        // 冲突检测
        if (conflictDetectionStrategy.isPresent()) {
          conflictDetectionStrategy.get().detectAndResolveConflictIfNecessary();
        }
        allMarkers.add(markerName);
        // 追加到对应的文件缓冲
        appendMarkerToFile(markerName, fileIndex);
        shouldFlushMarkers = true;
      }
      future.setResult(/* ... */);
    }
  }
  if (shouldFlushMarkers) {
    flushMarkersToFile(fileIndex);
  }
}
```

**设计要点**：
1. **线程池 + Round-Robin 文件分配**：多个线程可以同时写不同的 MARKERS 文件，提高并行度。
2. **内存缓存 + 批量刷盘**：marker 先缓存在内存中，积攒一批后一次性写入文件，大幅减少 I/O 操作。
3. **冲突检测集成**：在将 marker 加入缓存前，先做冲突检测。

**好处**：将 N 个 marker 创建请求合并为少数几次文件写入，显著减少文件系统操作次数。例如一个有 1000 个 partition 的写入，Direct 模式需要创建 1000+ 个 marker 文件，而 TLS 模式可能只需要写入几个 MARKERS 文件。

---

## 第五部分：Pre-Commit 冲突检测（ConflictResolution）

### 13. ConflictResolutionStrategy 接口

## 1. 解决什么问题
- **核心问题**:定义冲突检测和解决的统一接口,支持不同场景的冲突处理策略
- **如果没有策略接口会有什么问题**:
  - 冲突检测逻辑硬编码,无法适应不同场景
  - OCC和NBCC模式无法共存,需要重复实现
  - 无法扩展自定义冲突解决策略
  - 表服务与数据写入的冲突处理逻辑混乱
- **实际应用场景**:
  - SimpleConcurrentFileWritesConflictResolutionStrategy:OCC模式的默认策略
  - PreferWriterConflictResolutionStrategy:NBCC模式的策略,写入优先于表服务
  - BucketIndexConcurrentFileWritesConflictResolutionStrategy:Bucket Index场景的特殊策略
  - 用户自定义策略:实现特定业务逻辑的冲突处理

## 2. 有什么坑
- **坑1:getCandidateInstants范围过大** - 检查过多历史instant导致性能下降
- **坑2:hasConflict逻辑过于严格** - 误判冲突导致写入失败率过高
- **坑3:resolveConflict直接抛异常** - 某些场景可以自动合并而非失败
- **坑4:忘记检查心跳** - 已死Writer的instant不应该被视为冲突
- **性能陷阱**:
  - 遍历所有候选instant的metadata文件
  - 计算FileGroup交集时使用低效的集合操作
  - Rollback冲突检查需要读取rollbackPlan

## 3. 核心概念解释
- **CandidateInstants**:需要检查冲突的instant集合,通常是lastSuccessfulInstant之后的completed instant
- **ConcurrentOperation**:封装一个操作的所有冲突检测信息(operationType, mutatedFileIds等)
- **FileGroup交集**:两个操作修改的FileGroup集合的交集,非空则冲突
- **三步检测流程**:
  1. getCandidateInstants():确定检查范围
  2. hasConflict():判断是否冲突
  3. resolveConflict():决策处理(抛异常/忽略/合并)

## 4. 设计理念
- **为什么分成三个方法**:
  - 职责分离:范围确定、冲突判断、冲突处理是三个独立关注点
  - 策略模式:不同策略可以独立改变每一步的行为
  - 可复用:PreferWriterConflictResolutionStrategy重写getCandidateInstants但复用hasConflict
  - 源码证据:`ConflictResolutionStrategy`接口定义三个抽象方法
- **为什么基于FileGroup而非文件名**:
  - FileGroup是Hudi数据组织的基本单元
  - 同一FileGroup的base file和log files逻辑上是一个整体
  - 简化冲突检测逻辑,只需要比较fileId集合
  - 源码证据:`ConcurrentOperation.getMutatedPartitionAndFileIds()`返回`Set<Pair<partition, fileId>>`
- **为什么需要检查completedDuringWrite**:
  - 写入开始时记录pending instants
  - 写入结束时某些pending可能已completed
  - 这些instant不在getCandidateInstants结果中(时间戳早于lastSuccessful)
  - 但确实在本次写入期间完成,可能冲突
  - 源码证据:`TransactionUtils.resolveWriteConflictIfAny()`中`Stream.concat(candidates, completedDuringWrite)`
- **为什么Compaction可以被允许通过**:
  - Compaction只是将log合并到base file,不引入新数据
  - 如果Compaction时间戳早于当前写入,当前写入的数据会写到新log中
  - 不会被Compaction影响,是安全的
  - 源码证据:`SimpleConcurrentFileWritesConflictResolutionStrategy.resolveConflict()`中特殊处理COMPACT

#### 13.1 源码位置与设计

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/ConflictResolutionStrategy.java`

```java
public interface ConflictResolutionStrategy {
  
  // 获取需要检查冲突的候选 instant 流
  Stream<HoodieInstant> getCandidateInstants(
      HoodieTableMetaClient metaClient, HoodieInstant currentInstant, 
      Option<HoodieInstant> lastSuccessfulInstant);
  
  // 判断两个操作是否冲突
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation);
  
  // 解决冲突（通常是抛异常或返回合并后的 metadata）
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  Option<HoodieCommitMetadata> resolveConflict(HoodieTable table,
      ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) 
      throws HoodieWriteConflictException;
  
  // 表服务是否需要 preCommit
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  boolean isPreCommitRequired();
}
```

**三个核心方法的职责分离**：
1. `getCandidateInstants()`：**确定检查范围**。并非时间线上所有 instant 都需要检查，只有"可能冲突的"才需要。
2. `hasConflict()`：**判断是否冲突**。将判断逻辑与决策逻辑分离。
3. `resolveConflict()`：**决策处理**。冲突了怎么办——抛异常、忽略、还是合并。

**为什么要分成三步**：这是策略模式的经典应用。不同的冲突解决策略可以独立改变每一步的行为。例如 `PreferWriterConflictResolutionStrategy` 重写了 `getCandidateInstants()` 来包含 inflight 的 instant（用于检测 Clustering 与 Ingestion 的冲突），但复用了 `hasConflict()` 的逻辑。

#### 13.2 调用入口

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieClient.java`

```java
protected void resolveWriteConflict(HoodieTable table, HoodieCommitMetadata metadata, 
                                    Set<String> pendingInflightAndRequestedInstants) {
  Timer.Context conflictResolutionTimer = metrics.getConflictResolutionCtx();
  try {
    TransactionUtils.resolveWriteConflictIfAny(table, 
        this.txnManager.getCurrentTransactionOwner(),
        Option.of(metadata), config, 
        txnManager.getLastCompletedTransactionOwner(), true, 
        pendingInflightAndRequestedInstants);
    metrics.emitConflictResolutionSuccessful();
  } catch (HoodieWriteConflictException e) {
    metrics.emitConflictResolutionFailed();
    throw e;
  }
}
```

**TransactionUtils.resolveWriteConflictIfAny** 的核心逻辑：

```java
public static Option<HoodieCommitMetadata> resolveWriteConflictIfAny(...) {
  if (config.needResolveWriteConflict(operationType, table.isMetadataTable(), config, tableConfig)) {
    // 1. 刷新 Timeline
    table.getMetaClient().reloadActiveTimeline();
    
    // 2. 获取在本次写入期间完成的 instant
    Stream<HoodieInstant> completedDuringWrite = 
        getCompletedInstantsDuringCurrentWriteOperation(metaClient, pendingInstants);
    
    // 3. 获取冲突检测策略的候选 instant
    ConflictResolutionStrategy resolutionStrategy = config.getWriteConflictResolutionStrategy();
    Stream<HoodieInstant> candidates = resolutionStrategy.getCandidateInstants(
        metaClient, currentInstant, lastSuccessfulInstant, Option.of(config));
    
    // 4. 合并两个 instant 流，逐一检查冲突
    Stream<HoodieInstant> instantStream = Stream.concat(candidates, completedDuringWrite);
    final ConcurrentOperation thisOperation = new ConcurrentOperation(currentInstant, commitMetadata);
    
    instantStream.forEach(instant -> {
      ConcurrentOperation otherOperation = new ConcurrentOperation(instant, metaClient);
      if (resolutionStrategy.hasConflict(thisOperation, otherOperation)) {
        resolutionStrategy.resolveConflict(table, thisOperation, otherOperation);
      }
    });
  }
  return thisCommitMetadata;
}
```

**为什么要检查 `completedInstantsDuringCurrentWriteOperation`**：在写入开始时记录了当时的 pending instants。写入结束时，有些 pending instant 可能已经变成 completed。这些 instant 不在 `getCandidateInstants()` 的结果中（因为它们的时间戳可能早于 lastSuccessfulInstant），但确实在本次写入期间完成了提交，可能与本次写入冲突。所以需要额外检查。

---

### 14. SimpleConcurrentFileWritesConflictResolutionStrategy

## 1. 解决什么问题
- **核心问题**:OCC模式下的默认冲突解决策略,基于FileGroup交集检测冲突
- **如果没有这个策略会有什么问题**:
  - 无法判断两个Writer是否真正冲突
  - 无法处理Compaction与数据写入的并发
  - 无法检测Rollback冲突
  - 表服务与数据写入无法安全并发
- **实际应用场景**:
  - 多个Spark作业写入不同分区,偶尔有FileGroup重叠
  - 数据写入与异步Compaction并发执行
  - Clustering操作与数据写入的冲突检测
  - Rollback操作与正在提交的instant冲突

## 2. 有什么坑
- **坑1:MOR表的COMMIT_ACTION被误判** - 需要特殊处理,COMMIT是Compaction产生的,不应该与数据写入冲突
- **坑2:pending clustering未被检查** - V8+版本需要包含pending replace/clustering instant
- **坑3:Compaction时间戳判断错误** - 只有早于当前写入的Compaction才能通过
- **坑4:Rollback冲突未检测** - 如果有人正在回滚当前instant,必须立即停止
- **性能陷阱**:
  - 计算FileGroup交集时使用HashSet.retainAll(),大集合性能差
  - 读取每个候选instant的metadata文件
  - V8之前版本检查范围过大

## 3. 核心概念解释
- **FileGroup交集检测**:两个操作修改的`(partition, fileId)`集合的交集,非空则冲突
- **Rollback冲突**:有人正在回滚当前instant,当前instant不能提交
- **Compaction特殊处理**:Compaction时间戳早于当前写入时允许通过
- **V8版本优化**:使用completionTime而非requestedTime筛选候选instant,更精确

## 4. 设计理念
- **为什么MOR表的COMMIT_ACTION要特殊处理**:
  - MOR表中COMMIT_ACTION对应Compaction操作
  - Compaction只是将log合并到base file,不引入新数据
  - 与数据写入不存在业务级冲突
  - 源码证据:`getCandidateInstantsV8AndAbove()`中`.filter(instant -> !isMoRTable || !instant.getAction().equals(HoodieTimeline.COMMIT_ACTION))`
- **为什么要包含pending clustering**:
  - Clustering会重组FileGroup
  - 如果Writer正在写入一个正在被Clustering的FileGroup,会冲突
  - 必须在preCommit阶段检测出来
  - 源码证据:`getCandidateInstantsV8AndAbove()`中包含`filterPendingReplaceOrClusteringTimeline()`
- **为什么Compaction可以通过**:
  - Compaction时间戳早于当前写入,说明Compaction已经开始
  - 当前写入的数据会写到新log中,不会被Compaction影响
  - 这是"后写入优先"策略
  - 源码证据:`resolveConflict()`中`if (otherOperation.getOperationType() == WriteOperationType.COMPACT && compareTimestamps(..., LESSER_THAN, ...)) { return ...; }`
- **为什么需要Rollback冲突检测**:
  - Writer A正在提交instant T1
  - Writer B正在回滚T1(认为A已死)
  - 两者会冲突,A应该立即停止
  - 源码证据:`isRollbackConflict()`检查`otherOperation.getRolledbackCommit().equals(thisCommitTimestamp)`

#### 14.1 源码位置

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/SimpleConcurrentFileWritesConflictResolutionStrategy.java`

这是 OCC 模式的默认冲突解决策略。

#### 14.2 getCandidateInstants

```java
@Override
public Stream<HoodieInstant> getCandidateInstants(HoodieTableMetaClient metaClient, 
    HoodieInstant currentInstant, Option<HoodieInstant> lastSuccessfulInstant) {
  if (metaClient.getTableConfig().getTableVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
    return getCandidateInstantsV8AndAbove(metaClient, currentInstant, lastSuccessfulInstant);
  } else {
    return getCandidateInstantsPreV8(metaClient, currentInstant, lastSuccessfulInstant);
  }
}
```

**V8+ 版本的候选 instant 获取**：

```java
private Stream<HoodieInstant> getCandidateInstantsV8AndAbove(...) {
  // 1. 获取 lastSuccessfulInstant 之后完成的所有 commit
  //    但排除 MOR 表的 COMMIT_ACTION（因为那是 Compaction 产生的）
  Stream<HoodieInstant> completedCommitsInstantStream = activeTimeline
      .getCommitsTimeline()
      .filterCompletedInstants()
      .filter(instant -> !isMoRTable || !instant.getAction().equals(HoodieTimeline.COMMIT_ACTION))
      .findInstantsAfter(lastSuccessfulInstant...)
      .getInstantsAsStream();

  // 2. 获取 pending 的 replace/clustering instant
  Stream<HoodieInstant> clusteringAndReplaceCommitInstants = activeTimeline
      .filterPendingReplaceOrClusteringTimeline()
      .filter(isClusteringOrRecentlyRequestedInstant(...))
      .getInstantsAsStream();

  return Stream.concat(completedCommitsInstantStream, clusteringAndReplaceCommitInstants);
}
```

**为什么要特殊处理 MOR 表的 COMMIT_ACTION**：在 MOR 表中，`COMMIT_ACTION` 对应的是 Compaction 操作。Compaction 只是将 log 合并到 base file，不会引入新数据，所以与数据写入不存在业务级冲突。

**为什么要包含 pending clustering/replace**：Clustering 操作会重组 FileGroup，如果一个 Writer 正在写入一个正在被 Clustering 的 FileGroup，会产生冲突。必须在 pre-commit 阶段检测出来。

#### 14.3 hasConflict：文件级冲突检测

```java
@Override
public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
  // 1. 先检查 rollback 冲突
  if (isRollbackConflict(thisOperation, otherOperation)) {
    return true;
  }

  // 2. 检查文件组交集
  Set<Pair<String, String>> partitionAndFileIdsSetForFirstInstant = thisOperation.getMutatedPartitionAndFileIds();
  Set<Pair<String, String>> partitionAndFileIdsSetForSecondInstant = otherOperation.getMutatedPartitionAndFileIds();
  Set<Pair<String, String>> intersection = new HashSet<>(partitionAndFileIdsSetForFirstInstant);
  intersection.retainAll(partitionAndFileIdsSetForSecondInstant);
  if (!intersection.isEmpty()) {
    log.info("Found conflicting writes between first operation = " + thisOperation
        + ", second operation = " + otherOperation + " , intersecting file ids " + intersection);
    return true;
  }
  return false;
}
```

**冲突检测的核心逻辑**极其简洁：
1. 从两个操作中分别提取修改的 `(partition, fileId)` 集合
2. 计算交集
3. 交集非空 = 冲突

这就是 Hudi 的 FileGroup 级别冲突检测。**两个操作只要修改了同一个 FileGroup，就认为冲突**。

**Rollback 冲突检测**：

```java
private boolean isRollbackConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
  if (isRollbackOperation(otherOperation)) {
    String rolledbackCommit = otherOperation.getRolledbackCommit();
    String thisCommitTimestamp = thisOperation.getInstantTimestamp();
    if (rolledbackCommit != null && rolledbackCommit.equals(thisCommitTimestamp)) {
      return true;  // 有人正在回滚我要提交的 instant！
    }
  }
  return false;
}
```

**为什么需要 rollback 冲突检测**：如果 Writer A 正在尝试提交 instant T1，而 Writer B 正在回滚 T1（可能因为认为 A 已经死了），两者会冲突。这种情况下 A 应该立即停止提交。

#### 14.4 resolveConflict：冲突解决

```java
@Override
public Option<HoodieCommitMetadata> resolveConflict(HoodieTable table,
    ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
  // Compaction 冲突：如果 Compaction 的时间戳早于当前写入，可以安全通过
  if (otherOperation.getOperationType() == WriteOperationType.COMPACT) {
    if (compareTimestamps(otherOperation.getInstantTimestamp(), LESSER_THAN, thisOperation.getInstantTimestamp())) {
      return thisOperation.getCommitMetadataOption();
    }
  }
  // Log Compaction 冲突：可以与 delta commit 共存
  else if (HoodieTimeline.LOG_COMPACTION_ACTION.equals(thisOperation.getInstantActionType())) {
    return thisOperation.getCommitMetadataOption();
  }
  
  // 其他情况：直接抛出冲突异常
  throw new HoodieWriteConflictException(new ConcurrentModificationException(
      "Cannot resolve conflicts for overlapping writes between first operation = " + thisOperation
      + ", second operation = " + otherOperation));
}
```

**为什么 Compaction 可以被允许通过**：
- Compaction 本质上是将 log 合并到 base file，不引入新数据
- 如果 Compaction 的时间戳早于当前写入，说明 Compaction 已经开始了，当前写入的数据会写到新的 log 中，不会被 Compaction 影响
- 这是一种"后写入优先"的策略

**为什么 Log Compaction 总是允许通过**：Log Compaction 是 MOR 表特有的操作，它重写 log 文件但不改变数据语义。Reader 会按照 ordering field 来正确合并。

---

### 15. PreferWriterConflictResolutionStrategy

## 1. 解决什么问题
- **核心问题**:NBCC模式下的冲突解决策略,确保数据写入优先于表服务
- **如果没有这个策略会有什么问题**:
  - Clustering可能与正在进行的数据写入冲突,导致数据丢失
  - 表服务无法判断是否有活跃的数据写入
  - NBCC模式下的写入优先原则无法实现
  - Compaction可能与数据写入产生不必要的冲突
- **实际应用场景**:
  - 多个Flink流式作业实时写入,Clustering定期执行
  - 数据写入与Compaction并发,Compaction需要等待写入完成
  - REQUESTED状态的instant有活跃心跳,Clustering应该等待
  - 表服务检测是否有pending ingestion,决定是否可以执行

## 2. 有什么坑
- **坑1:Clustering阻塞配置未启用** - 默认不阻塞,需要显式配置`isClusteringBlockForPendingIngestion=true`
- **坑2:心跳超时配置不合理** - REQUESTED instant的心跳检查依赖正确的超时配置
- **坑3:误以为NBCC完全无冲突** - 表服务与数据写入之间仍然会冲突
- **坑4:completionTime判断错误** - 使用completionTime而非requestedTime筛选,容易混淆
- **性能陷阱**:
  - 检查所有REQUESTED instant的心跳,增加I/O开销
  - Clustering被频繁阻塞,无法及时执行
  - 心跳检查失败导致Clustering永久等待

## 3. 核心概念解释
- **写入优先原则**:数据写入(Ingestion)优先于表服务(Clustering/Compaction)
- **completionTime筛选**:基于完成时间而非请求时间筛选候选instant,更精确
- **REQUESTED instant检查**:Clustering需要检查REQUESTED状态的ingestion instant是否有活跃心跳
- **isPreCommitRequired**:NBCC模式下表服务也需要做preCommit检查

## 4. 设计理念
- **为什么写入优先于表服务**:
  - 数据写入是业务核心,不能因表服务而阻塞
  - 表服务可以延迟执行,数据写入通常有实时性要求
  - NBCC模式的设计目标就是高写入可用性
  - 源码证据:策略名称`PreferWriterConflictResolutionStrategy`
- **为什么Clustering要检查REQUESTED instant**:
  - REQUESTED instant表明Writer已准备好开始写入
  - 如果心跳活跃,说明Writer即将开始写入
  - Clustering重组FileGroup后,Writer会找不到目标文件
  - 必须等待Writer完成或心跳过期
  - 源码证据:`getCandidateInstantsForTableServicesCommits()`中`.filter(i -> { if (i.isRequested()) { return !HoodieHeartbeatUtils.isHeartbeatExpired(...); } })`
- **为什么使用completionTime**:
  - requestedTime是instant创建时间
  - completionTime是instant完成时间
  - 基于completionTime更精确地找到"在本次写入期间完成的"操作
  - 避免检查过多历史instant
  - 源码证据:`getCandidateInstantsForNonTableServicesCommits()`中使用`findInstantsModifiedAfterByCompletionTime()`
- **为什么isPreCommitRequired返回true**:
  - NBCC模式下数据写入之间几乎不冲突
  - 但表服务与数据写入之间仍可能冲突
  - 表服务需要在提交前做preCommit检查
  - 源码证据:`@Override public boolean isPreCommitRequired() { return true; }`

#### 15.1 源码位置与设计目标

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/PreferWriterConflictResolutionStrategy.java`

这是 NBCC 模式下的冲突解决策略，核心理念是：**写入（Ingestion）优先于表服务（Table Service）**。

#### 15.2 getCandidateInstants 的区别

```java
@Override
public Stream<HoodieInstant> getCandidateInstants(HoodieTableMetaClient metaClient, 
    HoodieInstant currentInstant, Option<HoodieInstant> lastSuccessfulInstant, 
    Option<HoodieWriteConfig> writeConfigOpt) {
  
  boolean isCurrentOperationClustering = ClusteringUtils.isClusteringInstant(
      activeTimeline, currentInstant, metaClient.getInstantGenerator());
  
  if (isCurrentOperationClustering || COMPACTION_ACTION.equals(currentInstant.getAction())) {
    // 如果当前操作是表服务（Clustering/Compaction），需要检查是否有 inflight 的数据写入
    return getCandidateInstantsForTableServicesCommits(activeTimeline, currentInstant, 
        isCurrentOperationClustering, metaClient, writeConfigOpt);
  } else {
    // 如果当前操作是数据写入，只需要检查 completionTime > currentInstant.requestedTime 的 commit
    return Stream.concat(
        getCandidateInstantsForNonTableServicesCommits(activeTimeline, currentInstant),
        getCandidateInstantsForRollbackConflict(activeTimeline, currentInstant));
  }
}
```

**关键区别**：
- **数据写入**：使用 `findInstantsModifiedAfterByCompletionTime` 而非 `findInstantsAfter`。这意味着基于完成时间而非请求时间来筛选，更精确地找到"在本次写入期间完成的"操作。
- **表服务**：不仅检查已完成的 commit，还检查 inflight 的 ingestion commit。

#### 15.3 Clustering 阻塞 pending ingestion 的特殊逻辑

```java
if (isClusteringBlockForPendingIngestion) {
  inflightIngestionCommitsStream = activeTimeline
      .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION))
      .filterInflightsAndRequested()  // 包括 REQUESTED 状态！
      .getInstantsAsStream()
      .filter(i -> !ClusteringUtils.isClusteringInstant(...))
      .filter(i -> {
        if (i.isRequested()) {
          // 对于 REQUESTED 状态，检查心跳是否过期
          return !HoodieHeartbeatUtils.isHeartbeatExpired(
              i.requestedTime(), maxHeartbeatIntervalMs, metaClient.getStorage(), ...);
        }
        return i.isInflight();  // INFLIGHT 状态直接包含
      });
}
```

**为什么 Clustering 需要检查 REQUESTED 的 ingestion instant**：如果一个 ingestion Writer 已经创建了 requested instant（表明它已经准备好开始写入），且其心跳仍然活跃，那么 Clustering 操作就不应该继续。因为一旦 Clustering 重组了 FileGroup，后续的 ingestion 写入会找不到目标文件。

**为什么要检查心跳**：如果 REQUESTED instant 对应的 Writer 已经死了（心跳过期），那这个 REQUESTED instant 最终会被清理，不应该阻塞 Clustering。

#### 15.4 hasConflict 和 resolveConflict 的覆写

```java
@Override
public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
  if (isClusteringBlockForPendingIngestion
      && WriteOperationType.CLUSTER.equals(thisOperation.getOperationType())
      && isRequestedIngestionInstant(otherOperation)) {
    return true;  // Clustering 与 pending ingestion 冲突
  }
  return super.hasConflict(thisOperation, otherOperation);  // 其他情况复用父类逻辑
}

@Override
public Option<HoodieCommitMetadata> resolveConflict(HoodieTable table,
    ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
  if (isClusteringBlockForPendingIngestion
      && WriteOperationType.CLUSTER.equals(thisOperation.getOperationType())
      && isRequestedIngestionInstant(otherOperation)) {
    throw new HoodieWriteConflictException(
        HoodieWriteConflictException.ConflictCategory.TABLE_SERVICE_VS_INGESTION,
        "Pending ingestion instant with active heartbeat may conflict with clustering");
  }
  return super.resolveConflict(table, thisOperation, otherOperation);
}
```

#### 15.5 isPreCommitRequired

```java
@Override
public boolean isPreCommitRequired() {
  return true;  // NBCC 模式下表服务也需要做 preCommit
}
```

**为什么 NBCC 模式下 isPreCommitRequired 返回 true**：虽然 NBCC 模式下数据写入之间几乎不冲突，但表服务（Clustering、Compaction）与数据写入之间仍然可能冲突。因此表服务在提交前需要做 preCommit 检查。

**好处**：PreferWriterConflictResolutionStrategy 让 NBCC 模式在保持高写入吞吐的同时，仍然对表服务提供了安全保障。

---

### 16. ConcurrentOperation

## 1. 解决什么问题
- **核心问题**:封装一个操作的所有冲突检测相关信息,作为ConflictResolutionStrategy的输入
- **如果没有ConcurrentOperation会有什么问题**:
  - 冲突检测需要重复解析CommitMetadata
  - 无法统一处理不同类型的操作(COMMIT/COMPACTION/CLUSTERING/ROLLBACK)
  - 代码重复,难以维护
  - 无法扩展新的操作类型
- **实际应用场景**:
  - 从Timeline读取其他Writer的操作信息
  - 从当前提交的metadata构造当前操作
  - 提取操作修改的FileGroup列表
  - 判断操作类型(数据写入/表服务/回滚)

## 2. 有什么坑
- **坑1:REPLACE_COMMIT未包含replaceFileIds** - Clustering操作会替换旧FileGroup,必须包含在冲突检测中
- **坑2:ROLLBACK未读取rollbackPlan** - 无法知道被回滚的commit,无法检测rollback冲突
- **坑3:依赖metadata文件存在** - 如果metadata文件损坏或缺失,会抛异常
- **坑4:fileId后缀未去除** - 需要去除文件扩展名,只保留fileId本身
- **性能陷阱**:
  - 每次构造ConcurrentOperation都需要读取metadata文件
  - 大量FileGroup时集合操作性能差
  - 重复构造相同instant的ConcurrentOperation

## 3. 核心概念解释
- **WriteOperationType**:操作类型枚举(INSERT/UPSERT/COMPACT/CLUSTER等)
- **mutatedPartitionAndFileIds**:操作修改的`(partition, fileId)`集合
- **rolledbackCommit**:如果是ROLLBACK操作,记录被回滚的commit时间戳
- **HoodieMetadataWrapper**:封装Avro或Orc格式的metadata,统一访问接口

## 4. 设计理念
- **为什么有两个构造函数**:
  - 从Timeline读取(用于"其他操作"):需要从文件系统读取metadata
  - 从当前提交元数据(用于"当前操作"):metadata已在内存中
  - 避免重复读取,提高性能
  - 源码证据:`new ConcurrentOperation(instant, metaClient)` vs `new ConcurrentOperation(instant, commitMetadata)`
- **为什么REPLACE_COMMIT要包含replaceFileIds**:
  - Clustering不仅创建新FileGroup(writeStats)
  - 还会"替换"旧FileGroup(replaceFileIds)
  - 如果另一Writer正在写入即将被替换的FileGroup,就是冲突
  - 源码证据:`this.mutatedPartitionAndFileIds.addAll(CommitUtils.flattenPartitionToReplaceFileIds(...))`
- **为什么ROLLBACK需要读取rollbackPlan**:
  - Rollback本身不直接修改数据文件
  - 但它会回滚某个commit
  - 如果正在提交的instant正好是被回滚的,就冲突
  - 需要从rollbackPlan提取被回滚的commit time
  - 源码证据:`HoodieRollbackPlan plan = metaClient.getActiveTimeline().readRollbackPlan(requested); this.rolledbackCommit = plan.getInstantToRollback().getCommitTime();`
- **为什么需要去除fileId后缀**:
  - 文件名包含扩展名(.parquet/.log)
  - 冲突检测基于fileId而非完整文件名
  - 同一fileId的base file和log files逻辑上是一个FileGroup
  - 源码证据:`getPartitionAndFileIdWithoutSuffix()`方法

#### 16.1 源码位置与职责

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/ConcurrentOperation.java`

`ConcurrentOperation` 封装了一个操作（instant）的所有冲突检测相关信息，是 `ConflictResolutionStrategy` 的输入参数。

#### 16.2 核心字段

```java
public class ConcurrentOperation {
  private WriteOperationType operationType;
  private final HoodieMetadataWrapper metadataWrapper;
  private final Option<HoodieCommitMetadata> commitMetadataOption;
  private final String actionState;   // REQUESTED, INFLIGHT, COMPLETED
  private final String actionType;    // COMMIT, DELTA_COMMIT, REPLACE_COMMIT, etc.
  private final String instantTime;
  private Set<Pair<String, String>> mutatedPartitionAndFileIds = Collections.emptySet();
  private String rolledbackCommit;  // 如果是 rollback 操作，记录被回滚的 commit
}
```

#### 16.3 从 CommitMetadata 提取修改的文件列表

`ConcurrentOperation` 有两个构造函数：
1. **从 Timeline 读取**（用于"其他操作"）：`new ConcurrentOperation(instant, metaClient)`
2. **从当前提交元数据**（用于"当前操作"）：`new ConcurrentOperation(instant, commitMetadata)`

关键是 `init()` 方法中对不同 action type 的处理：

```java
private void init(HoodieInstant instant) throws IOException {
  if (this.metadataWrapper.isAvroMetadata()) {
    switch (getInstantActionType()) {
      case COMPACTION_ACTION:
        this.operationType = WriteOperationType.COMPACT;
        this.mutatedPartitionAndFileIds = plan.getOperations().stream()
            .map(op -> Pair.of(op.getPartitionPath(), op.getFileId()))
            .collect(Collectors.toSet());
        break;
        
      case COMMIT_ACTION:
      case DELTA_COMMIT_ACTION:
        this.mutatedPartitionAndFileIds = getPartitionAndFileIdWithoutSuffix(
            commitMeta.getPartitionToWriteStats());
        this.operationType = WriteOperationType.fromValue(commitMeta.getOperationType());
        break;
        
      case REPLACE_COMMIT_ACTION:
      case CLUSTERING_ACTION:
        if (instant.isCompleted()) {
          // 包括 writeStats 和 replaceFileIds
          this.mutatedPartitionAndFileIds = getPartitionAndFileIdWithoutSuffix(
              replaceCommitMeta.getPartitionToWriteStats());
          this.mutatedPartitionAndFileIds.addAll(
              CommitUtils.flattenPartitionToReplaceFileIds(
                  replaceCommitMeta.getPartitionToReplaceFileIds()));
        }
        break;
        
      case ROLLBACK_ACTION:
        if (!instant.isCompleted()) {
          HoodieRollbackPlan plan = metaClient.getActiveTimeline().readRollbackPlan(requested);
          this.rolledbackCommit = plan.getInstantToRollback().getCommitTime();
        }
        break;
    }
  }
}
```

**为什么 REPLACE_COMMIT 要包含 replaceFileIds**：Clustering 操作不仅会创建新的 FileGroup（writeStats），还会"替换"旧的 FileGroup（replaceFileIds）。如果另一个 Writer 正在写入一个即将被替换的 FileGroup，就是冲突。

**为什么 ROLLBACK 需要读取 rollbackPlan**：Rollback 操作本身不直接修改数据文件，但它会回滚某个 commit。如果正在提交的 instant 正好是被回滚的那个，就产生了冲突。所以需要从 rollbackPlan 中提取被回滚的 commit time。

---

### 16.4 BucketIndexConcurrentFileWritesConflictResolutionStrategy

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/BucketIndexConcurrentFileWritesConflictResolutionStrategy.java`

对于使用 Bucket Index 的表，冲突检测粒度更粗——以 **bucket** 为单位：

```java
@Override
public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
  Set<String> partitionBucketIdSetForFirstInstant = thisOperation.getMutatedPartitionAndFileIds().stream()
      .map(partitionAndFileId -> BucketIdentifier.partitionBucketIdStr(
          partitionAndFileId.getLeft(), BucketIdentifier.bucketIdFromFileId(partitionAndFileId.getRight())))
      .collect(Collectors.toSet());
  Set<String> partitionBucketIdSetForSecondInstant = otherOperation.getMutatedPartitionAndFileIds().stream()
      .map(partitionAndFileId -> BucketIdentifier.partitionBucketIdStr(
          partitionAndFileId.getLeft(), BucketIdentifier.bucketIdFromFileId(partitionAndFileId.getRight())))
      .collect(Collectors.toSet());
  
  Set<String> intersection = new HashSet<>(partitionBucketIdSetForFirstInstant);
  intersection.retainAll(partitionBucketIdSetForSecondInstant);
  return !intersection.isEmpty();
}
```

**为什么 Bucket Index 需要特殊处理**：Bucket Index 中，一个 bucket 可能包含多个 FileGroup（在 resize 场景下）。冲突检测应该以 bucket 为粒度，而不是 fileId。两个不同的 fileId 可能属于同一个 bucket，此时仍然算冲突。

---

## 第六部分：冲突重试机制

### 17. HoodieSparkSqlWriterInternal 的重试循环

## 1. 解决什么问题
- **核心问题**:OCC模式下冲突失败后自动重试,提高写入成功率
- **如果没有重试机制会有什么问题**:
  - 偶发冲突导致整个作业失败,需要人工重新提交
  - 浪费已完成的计算资源(数据已写入但提交失败)
  - 降低系统可用性,增加运维成本
  - 无法应对瞬时冲突(如两个Writer几乎同时提交)
- **实际应用场景**:
  - 多个Spark作业写入不同分区,偶尔有FileGroup重叠
  - 网络抖动导致锁获取失败,重试后成功
  - 两个Writer几乎同时提交,后者重试后成功
  - 表服务与数据写入偶发冲突,重试避免失败

## 2. 有什么坑
- **坑1:重试次数配置过多** - 默认0不重试,配置过多(如100次)导致作业长时间卡住
- **坑2:在SINGLE_WRITER模式下配置重试** - 单Writer模式下冲突说明配置错误,重试无意义
- **坑3:重试不会跳过数据写入** - 每次重试都是完整的写入流程,不是只重试提交
- **坑4:只捕获HoodieWriteConflictException** - 其他异常不会触发重试,会直接失败
- **性能陷阱**:
  - 每次重试都重新写入数据,浪费计算资源
  - 高冲突场景下频繁重试,作业耗时大幅增加
  - 重试期间持有Executor资源,影响其他作业

## 3. 核心概念解释
- **完整重试**:每次重试都重新执行整个writeInternal(),包括获取新instant、创建marker、写入数据、提交
- **HoodieWriteConflictException**:冲突检测失败时抛出的专用异常
- **NUM_RETRIES_ON_CONFLICT_FAILURES**:最大重试次数配置,默认0
- **supportsMultiWriter检查**:只有多Writer模式才重试,单Writer模式直接抛异常

## 4. 设计理念
- **为什么每次重试都是完整流程**:
  - 冲突发生后,之前写入的数据文件可能需要回滚
  - 新的instant time确保不与已提交的instant冲突
  - marker需要重新创建,反映新的写入意图
  - 从头开始是最安全的选择
  - 源码证据:`toReturn = writeInternal(sqlContext, mode, optParams, sourceDf, ...)`完整调用
- **为什么只捕获HoodieWriteConflictException**:
  - 其他异常(如OOM/网络错误)不是冲突导致的
  - 重试无法解决非冲突异常
  - 避免无意义的重试,快速失败
  - 源码证据:`catch { case e: HoodieWriteConflictException => ... }`
- **为什么检查supportsMultiWriter**:
  - 单Writer模式下冲突说明配置错误(有其他Writer在运行)
  - 重试不会成功,应该立即失败
  - 提示用户检查配置
  - 源码证据:`if (WriteConcurrencyMode.supportsMultiWriter(writeConcurrencyMode) && counter < maxRetry) { ... } else { throw e }`
- **为什么默认值是0**:
  - 保守的默认值,避免无意义的重试
  - 重试会重新执行整个写入,可能非常耗时
  - 某些场景重试永远不会成功(如持续写同一FileGroup)
  - 用户应该主动评估是否需要重试
  - 源码证据:`HoodieWriteConfig.NUM_RETRIES_ON_CONFLICT_FAILURES.defaultValue() = 0`

#### 17.1 源码位置

**源码路径**：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieSparkSqlWriter.scala`

#### 17.2 重试逻辑

```scala
class HoodieSparkSqlWriterInternal {

  def write(sqlContext: SQLContext, mode: SaveMode, optParams: Map[String, String],
            sourceDf: DataFrame, ...): (...) = {

    val retryWrite: () => (...) = () => {
      var succeeded = false
      var counter = 0
      val maxRetry: Integer = Integer.parseInt(
        optParams.getOrElse(HoodieWriteConfig.NUM_RETRIES_ON_CONFLICT_FAILURES.key(), 
                           HoodieWriteConfig.NUM_RETRIES_ON_CONFLICT_FAILURES.defaultValue().toString))
      var toReturn: (...) = null

      while (counter <= maxRetry && !succeeded) {
        try {
          toReturn = writeInternal(sqlContext, mode, optParams, sourceDf, ...)
          if (counter > 0) {
            log.info(s"Write Succeeded after $counter attempts")
          }
          succeeded = true
        } catch {
          case e: HoodieWriteConflictException =>
            val writeConcurrencyMode = optParams.getOrElse(
              HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), 
              HoodieWriteConfig.WRITE_CONCURRENCY_MODE.defaultValue())
            if (WriteConcurrencyMode.supportsMultiWriter(writeConcurrencyMode) && counter < maxRetry) {
              counter += 1
              log.warn(s"Conflict found. Retrying again for attempt no $counter")
            } else {
              throw e
            }
        }
      }
      toReturn
    }
    retryWrite()
  }
}
```

**重试流程**：
1. 调用 `writeInternal()` 执行完整的写入流程
2. 如果在 pre-commit 阶段抛出 `HoodieWriteConflictException`
3. 检查是否是多 Writer 模式且还有剩余重试次数
4. 如果是，递增计数器，**重新执行整个 writeInternal()**
5. 重新执行意味着：获取新的 instant time，重新创建 marker，重新写入数据，重新做 pre-commit

**关键设计**：
- 每次重试都是**完整的写入流程**，不是只重试提交。因为冲突发生后，之前写入的数据文件可能需要回滚（由 marker 机制处理），需要从头开始。
- 只捕获 `HoodieWriteConflictException`，其他异常不会触发重试。
- 只有在 `supportsMultiWriter` 时才重试——单 Writer 模式下冲突异常说明配置错误，重试没有意义。

---

### 18. NUM_RETRIES_ON_CONFLICT_FAILURES 配置

#### 18.1 配置定义

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieWriteConfig.java`

```java
public static final ConfigProperty<Integer> NUM_RETRIES_ON_CONFLICT_FAILURES = ConfigProperty
    .key("hoodie.write.num.retries.on.conflict.failures")
    .defaultValue(0)
    .markAdvanced()
    .sinceVersion("0.14.0")
    .withDocumentation("Maximum number of times to retry a batch on conflict failure.");
```

#### 18.2 默认值和调优建议

**默认值为 0**，意味着不自动重试。这是一个保守的默认值，原因是：
- 重试会重新执行整个写入流程，可能非常耗时
- 在某些场景下，重试可能永远不会成功（如两个 Writer 持续写入同一个 FileGroup）
- 用户应该主动评估是否需要重试，以及重试次数

**调优建议**：

| 场景 | 建议值 | 原因 |
|------|--------|------|
| 批处理 + 偶尔冲突 | 1-3 | 重试通常能成功，因为冲突概率低 |
| 高频率写入 + 分区隔离 | 0-1 | 分区隔离下冲突极少，即使有也可能重试成功 |
| 同一 FileGroup 的频繁并发写入 | 0 | 重试大概率还是冲突，应该改用 NBCC 模式 |
| Streaming + OCC | 1-2 | 流式作业需要一定的容错能力 |

**配合使用的其他配置**：
- `hoodie.write.concurrency.mode`：必须是 OCC 或 NBCC
- `hoodie.write.lock.provider`：必须正确配置
- `hoodie.write.lock.wait_time_ms`：锁等待时间应足够长，否则重试也获取不到锁

---

## 第七部分：端到端并发场景分析

### 19. 场景 1: 两个 Spark 作业同时 upsert

**场景**：Spark Job A 和 Spark Job B 同时对同一张 COW 表执行 upsert，且写入了部分相同的 FileGroup。

**配置**：
```properties
hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider
hoodie.write.lock.conflict.resolution.strategy=org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy
```

**时序图**：

```
时间 -->

Job A:  [获取 instant T1] -> [心跳启动] -> [创建 markers] -> [写入数据文件] -> [preCommit: 获取锁] -> [检查冲突] -> [commit T1] -> [释放锁]
                                                                                          ↑ 锁等待
Job B:  [获取 instant T2] -> [心跳启动] -> [创建 markers] -> [写入数据文件] ------> [preCommit: 等待锁] -> [获取锁] -> [检查冲突: 发现T1] -> [冲突!] -> [释放锁] -> [HoodieWriteConflictException]
```

**详细流程**：

1. **T=0**：Job A 获取 instant time T1，Job B 获取 instant time T2（T2 > T1）
2. **T=1~10**：两个 Job 并行写入数据（不加锁），分别创建各自的 marker 文件
3. **T=10**：Job A 进入 preCommit
   - `txnManager.beginStateChange(T1, lastCompleted)` -> `lockManager.lock()`
   - 成功获取锁
   - 刷新 Timeline，发现没有新的 completed commit
   - `resolveWriteConflictIfAny()` 通过，无冲突
   - `commit(T1)` 成功
   - `txnManager.endStateChange(T1)` -> `lockManager.unlock()`
4. **T=11**：Job B 进入 preCommit
   - `txnManager.beginStateChange(T2, lastCompleted)` -> `lockManager.lock()`
   - 等待 Job A 释放锁（如果 A 还没释放），或者立即获取锁（如果 A 已释放）
   - 刷新 Timeline，发现 T1 已经 completed
   - `getCandidateInstants()` 返回 [T1]
   - `hasConflict(T2, T1)` 计算两者修改的 FileGroup 交集 -> 非空 -> 冲突
   - `resolveConflict()` 抛出 `HoodieWriteConflictException`
   - `lockManager.unlock()`
5. **T=12**：Job B 的冲突异常传播到 HoodieSparkSqlWriterInternal
   - 如果 `NUM_RETRIES_ON_CONFLICT_FAILURES > 0`，重试整个写入
   - 否则作业失败

---

### 20. 场景 2: Spark 写入 + Compaction 并发

**场景**：Spark 数据写入作业和异步 Compaction 作业同时运行，且 Compaction 涉及的 FileGroup 与写入有重叠。

**配置**：OCC 模式 + `SimpleConcurrentFileWritesConflictResolutionStrategy`

**时序图**：

```
时间 -->

Ingestion:  [instant T1] -> [写入 log files to FG-1, FG-2] -> [preCommit: 获取锁] -> [检查冲突: 发现Compaction on FG-1] -> [resolveConflict: Compaction时间早于T1, 允许通过] -> [commit T1] -> [释放锁]

Compaction: [instant TC (TC < T1)] -> [合并 FG-1 的 log] -> [preCommit: 等待锁] -> [获取锁] -> [检查冲突: 发现T1] -> [T1涉及FG-1, 但T1是数据写入, Compaction不产生新数据] -> [commit TC] -> [释放锁]
```

**关键处理**：

在 `resolveConflict()` 中：

```java
if (otherOperation.getOperationType() == WriteOperationType.COMPACT) {
  if (compareTimestamps(otherOperation.getInstantTimestamp(), LESSER_THAN, thisOperation.getInstantTimestamp())) {
    return thisOperation.getCommitMetadataOption();  // 允许通过！
  }
}
```

**为什么写入和 Compaction 可以共存**：
- Compaction 的输入是旧的 log files
- 写入的输出是新的 log files
- 两者操作的物理文件不同
- Compaction 完成后，新的 log files 仍然存在，下次 Compaction 会处理它们

---

### 21. 场景 3: Flink 多 Writer 并发

**场景**：两个 Flink 流式作业同时写入同一张 MOR 表的 NBCC 模式。

**配置**：
```properties
hoodie.write.concurrency.mode=NON_BLOCKING_CONCURRENCY_CONTROL
```

**流程**：

1. **ClientIds 分配**：
   - Flink Job A 启动，`ClientIds.nextId()` 返回 "" (空字符串)
   - Flink Job B 启动，`ClientIds.nextId()` 返回 "1"

2. **心跳维持**：
   - Job A 维护 `.hoodie/.aux/.ids/_` 心跳文件
   - Job B 维护 `.hoodie/.aux/.ids/_1` 心跳文件

3. **数据写入**：
   - Job A 写入 log 文件：`partition/.file_id_20240115.log.0_0-0-0`
   - Job B 写入 log 文件：`partition/.file_id_20240115.log.1_0-0-0`（注意 log 文件的版本号不同）
   - 两个 Job 写入的 log 文件名不同，物理上不冲突

4. **提交**：
   - 使用 `PreferWriterConflictResolutionStrategy`
   - 数据写入之间几乎不冲突（因为写的是不同的 log 文件）
   - Compaction 会合并来自两个 Writer 的 log

5. **读取**：
   - Reader 需要读取同一 FileGroup 的所有 log 文件（包括来自不同 Writer 的）
   - 通过 ordering field 进行去重和合并

**为什么 NBCC 可以做到无冲突**：核心在于 MOR 表的追加写入特性。每个 Writer 追加自己的 log 文件，文件名中包含了 Writer 特有的标识（通过 ClientId 区分）。物理层面不存在冲突。数据语义的一致性交给读取端来保证。

---

### 22. 场景 4: NBCC 模式下的并发写入

**场景**：在 NBCC 模式下，两个 Writer 同时写入同一个 FileGroup 的同一条记录（key 相同但值不同）。

**关键问题**：如何保证最终一致性？

**解答**：

1. Writer A 写入 record (key=1, value=A, ts=100)
2. Writer B 写入 record (key=1, value=B, ts=200)
3. 两个 record 分别在不同的 log 文件中
4. 读取时，Reader 扫描所有 log 文件，按照 ordering field（如 ts）排序
5. ts=200 的记录覆盖 ts=100 的记录
6. 最终结果：value=B

**Compaction 的角色**：
1. Compaction 读取所有 log 文件
2. 按照 ordering field 合并去重
3. 生成新的 base file
4. 之后的读取直接从 base file 读取，不再需要扫描所有 log

**注意事项**：NBCC 模式要求数据中有可靠的 ordering field（如事件时间戳）。如果两条记录的 ordering field 相同但值不同，结果是不确定的。

---

## 第八部分：生产运维

### 23. 并发控制配置完整手册

#### 23.1 核心配置

| 配置项 | 默认值 | 说明 | 模块 |
|--------|--------|------|------|
| `hoodie.write.concurrency.mode` | `SINGLE_WRITER` | 并发模式 | HoodieWriteConfig |
| `hoodie.write.lock.provider` | 无 | LockProvider 实现类 | HoodieLockConfig |
| `hoodie.write.lock.conflict.resolution.strategy` | `SimpleConcurrentFileWritesConflictResolutionStrategy` | 冲突解决策略 | HoodieLockConfig |
| `hoodie.write.num.retries.on.conflict.failures` | 0 | 冲突重试次数 | HoodieWriteConfig |

#### 23.2 锁获取相关配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.write.lock.wait_time_ms` | 60000 | 单次 tryLock 等待超时(ms) |
| `hoodie.write.lock.num_retries` | 15 | LockProvider 级别重试次数 |
| `hoodie.write.lock.wait_time_ms_between_retry` | 1000 | LockProvider 重试间隔(ms) |
| `hoodie.write.lock.max_wait_time_ms_between_retry` | 16000 | LockProvider 重试间隔上限(ms) |
| `hoodie.write.lock.client.num_retries` | 50 | LockManager 级别重试次数 |
| `hoodie.write.lock.client.wait_time_ms_between_retry` | 5000 | LockManager 重试间隔(ms) |

#### 23.3 心跳相关配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.client.heartbeat.interval_in_ms` | 60000 | 心跳间隔(ms) |
| `hoodie.client.heartbeat.tolerable.misses` | 2 | 可容忍的心跳 miss 次数 |
| `hoodie.write.lock.heartbeat_interval_ms` | 60000 | HMS 锁心跳间隔(ms) |

#### 23.4 早期冲突检测配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.write.early.conflict.detection.enable` | false | 是否启用早期冲突检测 |
| `hoodie.write.early.conflict.detection.strategy` | 见下文 | 早期冲突检测策略类 |

#### 23.5 FileSystem 锁配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.write.lock.filesystem.path` | 表 meta 目录 | 锁文件路径 |
| `hoodie.write.lock.filesystem.expire` | 0 | 锁过期时间(分钟) |

#### 23.6 ZooKeeper 锁配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.write.lock.zookeeper.url` | 无（必填） | ZK 连接地址 |
| `hoodie.write.lock.zookeeper.port` | 无 | ZK 端口 |
| `hoodie.write.lock.zookeeper.base_path` | 无 | ZK 基础路径 |
| `hoodie.write.lock.zookeeper.lock_key` | 表名 | ZK 锁 key |
| `hoodie.write.lock.zookeeper.session_timeout_ms` | 60000 | ZK session 超时(ms) |
| `hoodie.write.lock.zookeeper.connection_timeout_ms` | 15000 | ZK 连接超时(ms) |

#### 23.7 HiveMetastore 锁配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.write.lock.hivemetastore.database` | 无（必填） | Hive 数据库名 |
| `hoodie.write.lock.hivemetastore.table` | 无（必填） | Hive 表名 |
| `hoodie.write.lock.hivemetastore.uris` | 无 | HMS URI |

#### 23.8 DynamoDB 锁配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.write.lock.dynamodb.table` | 无（必填） | DynamoDB 表名 |
| `hoodie.write.lock.dynamodb.region` | 无（必填） | AWS Region |
| `hoodie.write.lock.dynamodb.partition_key` | 无（必填） | 分区 key |
| `hoodie.write.lock.dynamodb.billing_mode` | PAY_PER_REQUEST | 计费模式 |
| `hoodie.write.lock.dynamodb.endpoint_url` | 无 | 自定义 endpoint |

---

### 24. 多 Writer 架构模式推荐

#### 24.1 模式一：分区隔离 + OCC

**适用场景**：多个 ETL 管道写入同一张表的不同分区。

**架构**：
```
Spark Job A -> 写入 partition=2024-01-15
Spark Job B -> 写入 partition=2024-01-16
Spark Job C -> 异步 Compaction/Clustering
```

**配置**：
```properties
hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider
# 或使用 ZK/DynamoDB
hoodie.write.num.retries.on.conflict.failures=1
```

**优势**：
- 分区隔离天然减少冲突概率
- 即使冲突也是小概率事件，1 次重试通常足够
- 支持 COW 和 MOR 表

**注意事项**：
- 确保不同 Job 不会写入同一分区
- Compaction/Clustering 作为独立 Job 运行，与数据写入有短暂的锁竞争
- 锁超时要足够长（建议 60-120 秒）

#### 24.2 模式二：同分区写入 + OCC + 高重试

**适用场景**：多个数据源需要写入同一分区（如实时流 + 离线修正）。

**配置**：
```properties
hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
hoodie.write.lock.zookeeper.url=zk-host:2181
hoodie.write.lock.zookeeper.base_path=/hudi/locks
hoodie.write.num.retries.on.conflict.failures=3
hoodie.write.early.conflict.detection.enable=true
```

**优势**：
- 早期冲突检测可以尽早发现冲突，减少浪费
- 多次重试增加写入成功率
- ZK 提供高可靠的分布式锁

**注意事项**：
- 冲突率可能较高，需要监控重试次数
- 如果持续冲突，考虑切换到 NBCC 模式
- ZK 集群的可用性直接影响写入可用性

#### 24.3 模式三：NBCC 模式（Flink 推荐）

**适用场景**：多个 Flink 流式作业同时写入，不能容忍任何写入失败。

**配置**：
```properties
hoodie.write.concurrency.mode=NON_BLOCKING_CONCURRENCY_CONTROL
hoodie.table.type=MERGE_ON_READ
# Flink 特有配置
write.precombine.field=ts  # 必须有可靠的 ordering field
```

**优势**：
- 写入几乎不会失败
- 不需要分布式锁
- 写入吞吐高

**注意事项**：
- 只支持 MOR 表
- 读取延迟增加（需要合并多个 Writer 的 log）
- 必须有可靠的 ordering field
- Compaction 成为必须的维护操作

#### 24.4 模式选择决策树

```
需要多 Writer 吗？
├── 否 -> SINGLE_WRITER（默认，最佳性能）
└── 是 ->
    ├── 不同 Writer 写不同分区？
    │   └── 是 -> OCC + 分区隔离（模式一）
    ├── 可以容忍写入失败和重试？
    │   └── 是 -> OCC + 重试（模式二）
    └── 不能容忍写入失败？
        ├── 只使用 MOR 表？
        │   └── 是 -> NBCC（模式三）
        └── 否 -> OCC + 高重试 + 早期冲突检测
```

---

### 25. 并发问题排查手册

#### 25.1 常见问题一：锁获取超时

**错误信息**：
```
org.apache.hudi.exception.HoodieLockException: Unable to acquire the lock. Current lock owner information : ...
```

**排查步骤**：
1. 检查当前锁持有者信息（错误信息中会包含）
2. 确认锁持有者是否仍在运行：
   - 检查 Spark UI / Flink Dashboard
   - 检查心跳文件：`ls <table_path>/.hoodie/.heartbeat/`
3. 如果锁持有者已死但锁未释放：
   - FileSystemBasedLockProvider：检查 `<table_path>/.hoodie/.metadata/lock` 文件，手动删除
   - ZK：检查 ZK 节点，等待 session 超时或手动删除
   - DynamoDB：检查 DynamoDB 表中的锁记录
4. 调整锁超时参数：
   - 增大 `hoodie.write.lock.wait_time_ms`
   - 增大 `hoodie.write.lock.client.num_retries`

#### 25.2 常见问题二：写入冲突

**错误信息**：
```
org.apache.hudi.exception.HoodieWriteConflictException: Cannot resolve conflicts for overlapping writes
between first operation = ..., second operation = ..., intersecting file ids [...]
```

**排查步骤**：
1. 查看错误信息中的 intersecting file ids，确认是哪些 FileGroup 冲突
2. 确认为什么多个 Writer 写入了同一个 FileGroup：
   - 是否 key distribution 导致同一 key 被路由到同一 FileGroup
   - 是否 partition 隔离没做好
3. 解决方案：
   - 增大 `hoodie.write.num.retries.on.conflict.failures`
   - 优化 key 分布，减少 FileGroup 冲突
   - 考虑切换到 NBCC 模式
   - 启用早期冲突检测，减少无用写入

#### 25.3 常见问题三：心跳过期导致写入中止

**错误信息**：
```
org.apache.hudi.exception.HoodieException: Heartbeat for instant xxx has expired, last heartbeat yyy
```

**排查步骤**：
1. 检查 Driver 节点的 CPU 和内存使用情况
2. 检查 GC 日志，是否有长时间 GC 暂停
3. 检查网络是否有延迟或抖动（影响心跳文件写入）
4. 调整心跳参数：
   - 增大 `hoodie.client.heartbeat.interval_in_ms`（如 120000）
   - 增大 `hoodie.client.heartbeat.tolerable.misses`（如 5）

#### 25.4 常见问题四：Compaction 与写入冲突

**错误信息**：
通常表现为 Clustering 操作失败，错误信息中提到 inflight ingestion instant。

**排查步骤**：
1. 确认 Clustering 和 Ingestion 是否在同一时间运行
2. 使用 `PreferWriterConflictResolutionStrategy` 确保 Ingestion 优先
3. 配置 `hoodie.clustering.fail.on.pending.ingestion.during.conflict.resolution=true`
4. 错开 Clustering 和 Ingestion 的执行时间窗口

#### 25.5 常见问题五：NBCC 模式下读取数据不一致

**现象**：读取到的数据看起来"旧"或"不完整"。

**排查步骤**：
1. 检查 ordering field 是否正确配置
2. 检查 Compaction 是否正常运行
3. 检查读取端是否使用了 snapshot query（而非 read optimized query）
4. 确认所有 Writer 使用了相同的 precombine field

#### 25.6 常见问题六：死锁

**现象**：多个 Writer 互相等待锁，导致所有写入卡住。

**排查步骤**：
1. Hudi 使用表级锁，正常情况下不会死锁（因为不存在多资源的循环依赖）
2. 如果出现锁等待，通常是因为某个 Writer 持锁时间过长：
   - 检查持锁 Writer 的 preCommit 阶段耗时
   - 检查 Timeline 文件读取是否有延迟
   - 检查 Schema 冲突解决是否耗时
3. 解决方案：
   - 减小 `hoodie.write.lock.wait_time_ms` 让等待者更快失败
   - 优化 Timeline 读取性能
   - 增大集群资源

#### 25.7 监控指标

Hudi 通过 `HoodieLockMetrics` 暴露了以下关键指标：

| 指标 | 含义 | 告警阈值建议 |
|------|------|-------------|
| lock.acquired.count | 锁成功获取次数 | - |
| lock.not.acquired.count | 锁获取失败次数 | > 0 触发告警 |
| lock.acquire.time.ms | 锁获取耗时 | > 30s 告警 |
| lock.held.time.ms | 锁持有时间 | > 60s 告警 |
| lock.release.success.count | 锁释放成功次数 | - |
| lock.release.failure.count | 锁释放失败次数 | > 0 触发告警 |
| conflict.resolution.success.count | 冲突解决成功次数 | - |
| conflict.resolution.failure.count | 冲突解决失败次数 | 持续增长告警 |

**建议的监控策略**：
1. **锁获取成功率**：`acquired / (acquired + not_acquired)` 应该 > 95%
2. **锁持有时间**：平均值应在秒级，如果持续增长说明 preCommit 阶段有问题
3. **冲突率**：`conflict_failure / total_commits`，如果 > 10% 考虑优化架构或切换模式
4. **心跳检查**：定期扫描 `.hoodie/.heartbeat/` 目录，检查是否有超时的心跳文件

---

## 总结

### 并发控制全链路架构回顾

```
 用户写入请求
       |
       v
 [WriteConcurrencyMode 检查]
       |
       +---> SINGLE_WRITER: 不需要并发控制
       |
       +---> OCC/NBCC: 启用并发控制
              |
              v
         [心跳启动 - HoodieHeartbeatClient]
              |
              v
         [Marker 创建 - WriteMarkers]
              |
              +---> 早期冲突检测 (可选)
              |     [DirectMarkerBasedDetectionStrategy]
              |     [AsyncTimelineServerBasedDetectionStrategy]
              |
              v
         [数据文件写入]
              |
              v
         [preCommit 阶段]
              |
              v
         [获取分布式锁 - LockManager -> LockProvider]
              |
              v
         [刷新 Timeline]
              |
              v
         [冲突检测 - ConflictResolutionStrategy]
              |     getCandidateInstants()
              |     hasConflict()
              |     resolveConflict()
              |
              +---> 无冲突: commit 成功
              |
              +---> 有冲突:
                    |
                    +---> OCC: 抛出 HoodieWriteConflictException
                    |         -> 可能触发重试 (NUM_RETRIES_ON_CONFLICT_FAILURES)
                    |
                    +---> NBCC: 大部分情况下写入优先，极少数情况下表服务失败
              |
              v
         [释放分布式锁]
              |
              v
         [心跳停止 + 清理 Marker]
```

### 设计哲学总结

1. **分层防御**：从 WriteConcurrencyMode 到 Lock 到 Heartbeat 到 Marker 到 ConflictResolution 到 Retry，每一层都是独立的安全保障。即使某一层出问题，其他层仍然可以保护数据一致性。

2. **可插拔性**：LockProvider、ConflictResolutionStrategy、EarlyConflictDetectionStrategy 都是可插拔接口。用户可以根据自己的环境和需求选择合适的实现，甚至提供自定义实现。

3. **保守默认**：默认 SINGLE_WRITER、默认不重试、默认不启用早期冲突检测。安全性优先于功能性。用户必须显式启用多 Writer 支持。

4. **FileGroup 粒度**：冲突检测在 FileGroup 级别进行，这是在"太粗（表级）"和"太细（行级）"之间的最佳平衡点。粗到可以用简单的集合交集来检测，细到允许不同 FileGroup 的完全并行。

5. **乐观优先**：OCC 模式在写入阶段不加锁，只在短暂的提交阶段加锁。这最大化了写入并行度，是数据湖批处理场景的最佳选择。

6. **读写分离（NBCC）**：NBCC 模式将冲突解决从写入端转移到读取端，是一种典型的"写优化"策略。适合写入频率高、读取能容忍额外开销的场景。
