# Hudi Compaction、Clustering 与小文件治理深度解析

> 基于 Apache Hudi 源码深度分析（已纠错 + 扩展）
> 文档版本：3.0
> 源码版本：v1.2.0-SNAPSHOT (master)
> 最后更新：2026-04-15

---

## 目录

1. [概述 — Hudi 的文件管理哲学](#1-概述)
2. [小文件问题的本质与 Hudi 的多层治理](#2-小文件问题的本质与-hudi-的多层治理)
3. [写入时小文件治理 — UpsertPartitioner](#3-写入时小文件治理)
4. [Compaction 深度解析](#4-compaction-深度解析)
5. [Clustering 深度解析](#5-clustering-深度解析)
6. [Clean 操作详解](#6-clean-操作详解)
7. [Archival 归档机制](#7-archival-归档机制)
8. [调度与执行模式](#8-调度与执行模式)
9. [生产运维最佳实践](#9-生产运维最佳实践)
10. [表服务源码深度剖析](#10-表服务源码深度剖析)

---

## 1. 概述

### 1.1 为什么需要表服务？—— 数据湖的熵增定律

数据湖的文件系统没有传统数据库的"后台 daemon"来维护数据布局。随着持续写入，**文件系统的熵必然增加**：

```
写入 1 次:   partition/file1.parquet (128MB)     ← 整齐
写入 100 次: partition/file1.parquet (128MB)
             partition/file2.parquet (5MB)        ← 小文件！
             partition/file3.parquet (3MB)        ← 小文件！
             partition/.file1.log.1               ← MOR log 堆积
             partition/.file1.log.2
             partition/.file1.log.3               ← 读放大！
             partition/file_old_v1.parquet         ← 过期文件占空间
             .hoodie/T1.commit ... T500.commit    ← Timeline 膨胀
```

**不治理的后果**：
- **小文件泛滥** → 查询引擎打开大量文件句柄 → 查询延迟飙升
- **MOR log 堆积** → 读取需要合并大量 log → 读放大线性增长
- **过期文件不清理** → 存储成本无限增长
- **Timeline 膨胀** → MetaClient 初始化变慢 → 影响所有读写操作

### 1.2 Hudi 的四大表服务

Hudi 的表服务本质上是一个**后台维护系统**，解决数据湖的"无人管理"问题：

| 服务 | 解决什么问题 | 类比传统数据库 |
|------|------------|--------------|
| **Compaction** | MOR 表 log 堆积 → 合并为新 Base File | InnoDB 的后台 merge |
| **Clustering** | 文件大小不均 + 数据布局差 → 重组文件 | MySQL 的 OPTIMIZE TABLE |
| **Clean** | 过期文件占空间 → 删除旧版本 | PostgreSQL 的 VACUUM |
| **Archival** | Timeline 文件过多 → 归档旧 instant | 任何数据库的日志轮转 |

**设计哲学**：Hudi 将这些维护操作内化为表的一部分（而不是外部脚本），通过 Timeline 记录和管理，与数据写入享受同样的事务保证。

---

## 2. 小文件问题的本质与 Hudi 的多层治理

### 2.1 小文件是怎么产生的？

```
原因 1: 高频写入（每次只写少量数据）
  每 1 分钟写入 1000 条 → 每次产生 < 1MB 的文件 → 1 天产生 1440 个小文件

原因 2: 高基数分区（大量分区，每分区数据量少）
  按 user_id 分区 → 100 万用户 → 每用户只有几条记录 → 100 万个微小文件

原因 3: 数据倾斜
  大部分 FileGroup 正常大小，少数 FileGroup 在 INSERT 时溢出为多个小文件

原因 4: Clustering/Compaction 产生的中间文件
  Compaction 输出的新 Base File 可能不满
```

### 2.2 Hudi 的三层小文件治理体系

```
Layer 1: 写入时治理 (UpsertPartitioner) — 预防
    ├── INSERT 记录优先填充已有的小文件
    ├── 基于 averageRecordSize 自动计算每个 bucket 的记录数
    └── 避免产生新的小文件

Layer 2: Clustering 治理 — 事后修复
    ├── 将多个小文件合并为大文件
    ├── 按指定字段排序，优化数据局部性
    └── 重新组织文件布局

Layer 3: Clean 治理 — 空间回收
    └── 清理 Clustering/Compaction 后的旧文件
```

### 2.3 写入时小文件治理 — UpsertPartitioner 源码解析

**源码位置**：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/UpsertPartitioner.java`

这是 Hudi 小文件治理的**第一道防线**，在写入阶段就尽量避免小文件产生。

```java
public class UpsertPartitioner<T> extends SparkHoodiePartitioner<T> {
    // 已识别的小文件列表
    protected List<SmallFile> smallFiles = new ArrayList<>();
    // 每个 bucket 的信息（UPDATE 桶 or INSERT 桶）
    protected final HashMap<Integer, BucketInfo> bucketInfoMap;

    // 构造函数中的核心逻辑：
    public UpsertPartitioner(WorkloadProfile profile, ...) {
        // Step 1: 为 UPDATE 记录分配 bucket（每个 FileGroup 一个）
        assignUpdates(profile);

        // Step 2: ★ 为 INSERT 记录分配 bucket（含小文件填充逻辑）
        assignInserts(profile, context);
    }
}
```

**assignInserts 的核心算法（小文件填充）**：

```
assignInserts(partition) 流程:

Step 1: 计算平均记录大小
    averageRecordSize = 历史 commit 的总字节数 / 总记录数
    // ★ 注意：只用 commit/deltacommit/replacecommit，不用 clustering
    // 为什么？因为 Clustering 后的文件记录密度更高，会低估实际记录大小

Step 2: 识别小文件
    for each fileSlice in partition:
        if fileSlice.baseFile.size < hoodie.parquet.small.file.limit:
            smallFiles.add(fileSlice)  // 小于阈值 → 标记为小文件
    // ★ 过滤掉正在 Clustering 的文件（避免冲突）

Step 3: 优先填充小文件
    for each smallFile in smallFiles:
        // 计算还能塞多少记录
        recordsToAppend = (maxFileSize - smallFile.size) / averageRecordSize
        // 将 INSERT 记录分配到这个小文件对应的 bucket
        assignToBucket(smallFile.fileId, recordsToAppend)
        totalUnassignedInserts -= recordsToAppend
        if totalUnassignedInserts <= 0: break

Step 4: 剩余 INSERT 创建新 bucket
    if totalUnassignedInserts > 0:
        // 每个新 bucket 容纳 maxFileSize/averageRecordSize 条记录
        insertRecordsPerBucket = maxFileSize / averageRecordSize
        insertBuckets = ceil(totalUnassignedInserts / insertRecordsPerBucket)
        for i in range(insertBuckets):
            createNewInsertBucket()  // 创建新的 FileGroup

Step 5: 按权重分配
    // 每个 bucket 按容量占比设置权重
    // INSERT 记录通过 hash(recordKey) 按权重路由到对应 bucket
```

**为什么这个设计很精妙？**
- **先填旧再建新**：INSERT 记录优先填充已有的小文件，而不是一律创建新文件
- **基于统计的自适应**：通过历史 commit 的平均记录大小自动估算，不需要用户手动调参
- **与 Clustering 协调**：过滤掉正在 Clustering 的文件，避免写入冲突
- **权重路由**：多个可填充的小文件按容量占比分配 INSERT，保证均匀填充

**关键配置**：

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `hoodie.parquet.max.file.size` | `125829120` (120MB) | 目标文件大小（单位字节）|
| `hoodie.parquet.small.file.limit` | `104857600` (100MB) | 小于此值视为小文件（单位字节）|
| `hoodie.copyonwrite.insert.auto.split` | `true` | 是否自动计算每 bucket 记录数 |
| `hoodie.copyonwrite.insert.split.size` | `500000` | 手动模式下每 bucket 记录数 |

**运维建议**：
- 设 `small.file.limit = 0` → **完全关闭小文件填充**，适合 bulk_insert 场景（首次灌入大量数据时不需要填充）
- 设 `small.file.limit` 接近 `max.file.size` → **激进填充**，适合高频小批量写入
- 默认值（100MB / 120MB）适合大多数场景

---

## 3. Compaction 深度解析（原第 2 章，扩展）

### 3.1 为什么需要 Compaction？—— MOR 表的读放大困境

```
MOR 表写入 10 次后:
  FileGroup-A:
    base.parquet (128MB, T1)
    .log.1 (2MB, T2)
    .log.2 (3MB, T3)
    ...
    .log.9 (1MB, T10)

读取 FileGroup-A 的成本:
  1. 读取 base.parquet → 128MB I/O
  2. 读取 9 个 log files → 额外 ~15MB I/O
  3. 构建 ExternalSpillableMap（所有 log records 按 key 合并）→ 内存/磁盘开销
  4. 遍历 base records，逐一检查是否有 log 更新 → CPU 开销

Compaction 后:
  FileGroup-A:
    new_base.parquet (130MB, T11) — 已合并所有 log
  
读取成本: 直接读 Parquet → 130MB I/O，无合并开销
```

**Compaction 的本质**：用一次批量合并的 CPU+IO 成本，换取后续每次读取的零合并成本。是**写入时间 vs 读取时间的权衡**。

### 3.2 两阶段设计的深层原因

```
阶段 1: Schedule Compaction → 生成 CompactionPlan
    .hoodie/T11.compaction.requested (包含计划内容)

阶段 2: Execute Compaction → 执行合并
    .hoodie/T11.compaction.inflight → T11.compaction (completed)
```

**为什么要分两阶段？** 这不是过度设计，而是为了解决三个实际问题：

1. **跨引擎执行**：Flink 写入数据，Spark 执行 Compaction。Schedule 产生计划（JSON 格式），任何引擎都能读取和执行。

2. **异步解耦**：Schedule 在写入路径中执行（轻量），Execute 可以异步/独立执行（重量）。写入不被 Compaction 阻塞。

3. **故障恢复**：如果 Execute 失败，CompactionPlan 仍然存在（REQUESTED 状态），可以直接重试，不需要重新 Schedule。

### 3.3 Compaction 策略——源码完整梳理

**源码位置**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/strategy/`

```
继承层次:
CompactionStrategy (抽象基类)
    ├── BoundedIOCompactionStrategy (限制 I/O 上限)
    │   ├── LogFileSizeBasedCompactionStrategy (★ 默认策略)
    │   ├── LogFileNumBasedCompactionStrategy (按 log 文件数)
    │   └── DayBasedCompactionStrategy (按天选分区)
    │       └── BoundedPartitionAwareCompactionStrategy (有界分区感知)
    ├── UnBoundedCompactionStrategy (不限制 I/O)
    ├── UnBoundedPartitionAwareCompactionStrategy (不限制，按分区感知)
    ├── CompositeCompactionStrategy (组合策略，串联多个策略)
    └── PartitionRegexBasedCompactionStrategy (按正则匹配分区)
```

**纠错**：原文档遗漏了 `LogFileNumBasedCompactionStrategy`（按 log 文件数量筛选）、`BoundedPartitionAwareCompactionStrategy`（有界分区感知）、`CompositeCompactionStrategy`（组合策略）和 `PartitionRegexBasedCompactionStrategy`（正则分区）。同时 `LogFileSizeBasedCompactionStrategy` 和 `DayBasedCompactionStrategy` 实际继承自 `BoundedIOCompactionStrategy` 而非 `UnBoundedCompactionStrategy` 或直接继承 `CompactionStrategy`。

| 策略 | 筛选条件 | 排序方式 | I/O 限制 | 适用场景 |
|------|---------|---------|---------|---------|
| `LogFileSizeBasedCompactionStrategy` (默认) | log 总大小 ≥ 阈值 | log 大小降序 | 有（继承 BoundedIO） | **通用场景** |
| `LogFileNumBasedCompactionStrategy` | log 文件数 ≥ 阈值 | log 文件数降序 | 有（继承 BoundedIO） | 文件数敏感场景 |
| `DayBasedCompactionStrategy` | 按天过滤分区 | 分区路径排序 | 有（继承 BoundedIO） | 按天调度 |
| `BoundedPartitionAwareCompactionStrategy` | 按天过滤 + 分区感知 | 分区路径排序 | 有（继承 DayBased→BoundedIO） | 限制 I/O 的分区感知 |
| `BoundedIOCompactionStrategy` | 无 | 无特殊排序 | 有 | 控制资源消耗 |
| `UnBoundedCompactionStrategy` | 无 | 无特殊排序 | 无 | 全量 Compaction |
| `UnBoundedPartitionAwareCompactionStrategy` | 按分区感知 | 无特殊排序 | 无 | 不限 I/O 的分区感知 |
| `CompositeCompactionStrategy` | 组合多个策略 | 依赖子策略 | 依赖子策略 | 组合场景 |
| `PartitionRegexBasedCompactionStrategy` | 按正则匹配分区名 | 无特殊排序 | 无 | 按正则选择分区 |

**默认策略 LogFileSizeBasedCompactionStrategy 的工作流程**：

```java
// 源码还原
public Pair<List<HoodieCompactionOperation>, List<String>> orderAndFilter(
    HoodieWriteConfig writeConfig,
    List<HoodieCompactionOperation> operations,
    List<HoodieCompactionPlan> pendingCompactionPlans) {

    long threshold = writeConfig.getCompactionLogFileSizeThreshold();

    // Step 1: 过滤 — 只保留 log 总大小 ≥ 阈值的 FileSlice
    List<HoodieCompactionOperation> filtered = operations.stream()
        .filter(e -> e.getMetrics().get(TOTAL_LOG_FILE_SIZE) >= threshold)
        .sorted(this)  // Step 2: 排序 — log 大小降序（大的优先压缩）
        .collect(toList());

    // Step 3: 限制 — 调用父类 BoundedIOCompactionStrategy 限制总 I/O
    return super.orderAndFilter(writeConfig, filtered, pendingCompactionPlans);
}
```

**为什么大的优先？** 因为 log 越大的 FileSlice，读放大越严重，Compaction 的收益越高。这是一种**贪心策略**——每次 Compaction 的资源有限，应该优先处理收益最大的。

### 3.4 Compaction 关键配置与调优

| 参数 | 默认值 | 调优建议 |
|------|--------|---------|
| `hoodie.compact.inline` | `false` | 生产环境保持 false，使用异步 |
| `hoodie.compact.inline.max.delta.commits` | `5` | 触发频率，越小越频繁 |
| `hoodie.compaction.strategy` | `LogFileSizeBased...` | 大多数场景用默认即可 |
| `hoodie.compaction.target.io` | `512000` (MB) | 单次 Compaction I/O 上限 |
| `hoodie.compaction.logfile.size.threshold` | `0` | log 大小阈值（0=不过滤，单位字节）|
| `hoodie.compaction.logfile.num.threshold` | `0` | log 文件数阈值（0=不过滤）|

**运维经验**：
- `target.io` 太小 → 每次只压缩少量文件，积压严重
- `target.io` 太大 → 单次 Compaction 耗时过长，可能超时
- 推荐：根据集群资源设置，一般 Compaction 作业占集群 20-30% 资源

---

## 4. Clustering 深度解析（原第 3 章，扩展）

### 4.1 为什么需要 Clustering？—— Compaction 解决不了的问题

```
Compaction 能做的:
  base + log → 新 base（合并单个 FileGroup 的历史版本）

Compaction 做不到的:
  ✗ 跨 FileGroup 合并小文件
  ✗ 按查询常用字段排序
  ✗ 调整文件大小到目标值
  ✗ 优化数据局部性（相近的数据在同一文件中）
```

**Clustering 的核心价值**：

```
Before Clustering:
  partition/fg1.parquet (10MB, city=[Beijing,Shanghai,Shenzhen])
  partition/fg2.parquet (500MB, city=[Beijing,Shanghai,...])
  partition/fg3.parquet (5MB, city=[Guangzhou])
  → 查 WHERE city='Beijing' 需要读 fg1 + fg2，因为 city 值散布在多个文件中

After Clustering (sort by city):
  partition/fg_new1.parquet (128MB, city=[Beijing])
  partition/fg_new2.parquet (128MB, city=[Guangzhou,Shanghai])
  partition/fg_new3.parquet (128MB, city=[Shenzhen,...])
  → 查 WHERE city='Beijing' 只需读 fg_new1
  → 文件大小均匀，Column Stats 的 min/max 范围更窄 → Data Skipping 更有效
```

### 4.2 Clustering 两阶段 —— Plan 与 Execute

```
阶段 1: ClusteringPlanStrategy.generateClusteringPlan()
    ├── 选择需要 Clustering 的分区和文件
    ├── 将文件分成 ClusteringGroup（一组一起重写）
    ├── Timeline: .replacecommit.requested
    └── ★ 使用 replacecommit 而不是 commit
        为什么？因为 Clustering 会替换旧 FileGroup 为新 FileGroup

阶段 2: ClusteringExecutionStrategy.performClustering()
    ├── 读取 ClusteringGroup 中所有文件的数据
    ├── 按 sort columns 排序
    ├── 按 target file size 切分
    ├── 写入新文件
    └── Timeline: .replacecommit.inflight → .replacecommit (completed)
```

### 4.3 Clustering Plan Strategy——源码完整梳理

```
继承层次:
ClusteringPlanStrategy (抽象基类)
    ├── PartitionAwareClusteringPlanStrategy (分区感知)
    ├── CommitBasedClusteringPlanStrategy (按 commit 选文件)
    ├── BaseConsistentHashingBucketClusteringPlanStrategy (一致性哈希桶调整)
    └── (引擎特定实现)
        ├── SparkSizeBasedClusteringPlanStrategy (★ Spark 默认)
        │   ├── SparkStreamCopyClusteringPlanStrategy
        │   └── SparkSingleFileSortPlanStrategy (单文件排序)
        ├── SparkConsistentBucketClusteringPlanStrategy
        ├── FlinkSizeBasedClusteringPlanStrategy (Flink 默认)
        │   ├── FlinkSkipSingleFileClusteringPlanStrategy
        │   └── FlinkSizeBasedClusteringPlanStrategyRecently
        ├── FlinkConsistentBucketClusteringPlanStrategy
        └── JavaSizeBasedClusteringPlanStrategy
```

**纠错**：原文档遗漏了多个 Clustering Plan Strategy 实现，特别是 Flink 侧的 `FlinkSkipSingleFileClusteringPlanStrategy`、`FlinkSizeBasedClusteringPlanStrategyRecently` 和 `FlinkConsistentBucketClusteringPlanStrategy`，以及 Spark 侧的 `SparkSingleFileSortPlanStrategy`。同时修正了继承关系：`SparkStreamCopyClusteringPlanStrategy` 和 `SparkSingleFileSortPlanStrategy` 实际继承自 `SparkSizeBasedClusteringPlanStrategy`。

### 4.4 排序策略 —— 为什么排序很重要？

**排序对 Data Skipping 的影响**：

```
不排序:
  file1: city min=Anhui, max=Zhejiang      ← 范围极宽，几乎所有查询都需要读
  file2: city min=Beijing, max=Yunnan       ← 同上

按 city 排序:
  file1: city min=Anhui, max=Chongqing      ← 范围窄
  file2: city min=Fuzhou, max=Nanjing       ← 范围窄
  file3: city min=Shanghai, max=Zhejiang    ← 范围窄
  → WHERE city='Beijing' 只命中 file1 → Data Skipping 效果极佳
```

**三种排序策略**：

| 排序策略 | 适用场景 | 原理 |
|---------|---------|------|
| **Linear Sort** | 单列范围查询 | 按一列自然排序 |
| **Z-Order** | 多列组合查询 | 多列值交错编码，保持多维局部性 |
| **Hilbert Curve** | 多列组合查询 | Hilbert 空间填充曲线，比 Z-Order 更好的局部性 |

**为什么 Hilbert 比 Z-Order 好？**
Z-Order 存在"跳跃"——在某些维度上相邻的值可能在编码后距离很远。Hilbert 曲线通过更复杂的编码避免了这种跳跃，保证相邻区域的值在一维编码后仍然相邻。

### 4.5 Clustering 关键配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `hoodie.clustering.inline` | `false` | 内联 Clustering |
| `hoodie.clustering.inline.max.commits` | `4` | 触发间隔 |
| `hoodie.clustering.async.enabled` | `false` | 异步 Clustering |
| `hoodie.clustering.plan.strategy.target.file.max.bytes` | `1073741824` (1GB) | 目标文件大小 |
| `hoodie.clustering.plan.strategy.small.file.limit` | `629145600` (600MB) | 小文件阈值 |
| `hoodie.clustering.plan.strategy.sort.columns` | - | 排序列 |
| `hoodie.clustering.plan.strategy.max.num.groups` | `30` | 最大 ClusteringGroup 数 |
| `hoodie.clustering.plan.strategy.max.bytes.per.group` | `2147483648` (2GB) | 每组最大字节 |
| `hoodie.clustering.plan.partition.filter.mode` | `NONE` | 分区过滤模式 |

---

## 5. Clean 操作详解

### 5.1 为什么需要 Clean？

```
写入 T1: fileA_v1.parquet
写入 T2: fileA_v2.parquet (COW 重写)
写入 T3: fileA_v3.parquet (COW 重写)

此时存储:
  fileA_v1.parquet — 可能有 reader 在读（时间旅行查询 T1）
  fileA_v2.parquet — 可能有 reader 在读（时间旅行查询 T2）
  fileA_v3.parquet — 当前最新版本

如果保留所有版本:
  100 个 FileGroup × 100 个版本 = 10000 个文件，存储成本 10 倍

Clean 的目标: 在不影响活跃 reader 的前提下，删除过期文件
```

### 5.2 三种 Clean 策略

```java
public enum HoodieCleaningPolicy {
    KEEP_LATEST_COMMITS,         // 保留最近 N 次 commit 的文件
    KEEP_LATEST_FILE_VERSIONS,   // 保留每个文件的最近 N 个版本
    KEEP_LATEST_BY_HOURS         // 保留最近 N 小时内的文件
}
```

| 策略 | 适用场景 | 优点 | 缺点 |
|------|---------|------|------|
| `KEEP_LATEST_COMMITS` (默认) | 增量消费 | 保证增量消费者不丢数据 | 提交频率变化时保留时间不可控 |
| `KEEP_LATEST_FILE_VERSIONS` | 时间旅行 | 每个文件版本数固定 | 与增量消费兼容性差 |
| `KEEP_LATEST_BY_HOURS` | 合规要求 | 精确的时间窗口 | 高频写入时文件数可能很多 |

**运维建议**：
- 增量消费场景 → `KEEP_LATEST_COMMITS`，`commits.retained` ≥ 增量消费延迟的 commit 数
- 时间旅行场景 → `KEEP_LATEST_FILE_VERSIONS`，`fileversions.retained = 3`
- 合规场景（如"数据保留 72 小时"）→ `KEEP_LATEST_BY_HOURS`，`hours.retained = 72`

### 5.3 Clean 的安全保证

**Clean 如何避免删除正在被读取的文件？**

```
Clean 决策流程:
1. 获取 Active Timeline 上最早的 INFLIGHT instant → earlistInflightInstant
2. 只清理 earlistInflightInstant 之前的过期文件
3. 这保证了:
   - 任何正在执行的写入（INFLIGHT）引用的文件不会被清理
   - 任何可能正在读取的 Snapshot 引用的文件不会被清理
```

---

## 6. Archival 归档机制

### 6.1 为什么需要 Archival？

```
每次写入产生 3 个 Timeline 文件:
  T1.commit.requested → T1.commit.inflight → T1.commit

写入 10000 次 → 30000 个 Timeline 文件

影响:
  HoodieTableMetaClient 初始化 → 需要 LIST .hoodie/ 目录 → 30000 文件的 LIST 操作
  在 S3 上: 30000 / 1000(每页) = 30 次 LIST API 调用 → 显著延迟
```

**Archival 的目标**：将旧的 Timeline 文件移到归档目录，控制活跃 Timeline 的大小。

### 6.2 关键配置

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `hoodie.keep.min.commits` | `20` | 活跃 Timeline 最少保留 commit 数 |
| `hoodie.keep.max.commits` | `30` | 超过此数触发 Archival |
| `hoodie.archive.automatic` | `true` | 是否自动归档 |

**约束**: `min.commits` 必须 > `cleaner.commits.retained`，否则 Clean 可能引用已归档的 instant。

**运维建议**: 如果增量消费延迟大，需要同时增大 `cleaner.commits.retained` 和 `keep.min.commits`。

---

## 7. 调度与执行模式

### 7.1 三种模式对比

```
模式 1: Inline（内联同步）
  write → commit → schedule → execute → next write
  ├── 最简单，无需额外部署
  ├── 但写入延迟 = 写入时间 + 表服务时间
  └── 适合: 小规模表，批量写入

模式 2: Async（异步，同进程）
  write → commit → schedule → next write
                       ↓
            后台线程: execute
  ├── 不阻塞写入
  ├── 但 execute 和 write 共享资源
  └── 适合: 中规模表，流式写入

模式 3: Separate Job（独立作业）
  Job 1 (Writer):  write → commit → [可选: schedule]
  Job 2 (Service): schedule + execute (读取 pending plans)
  ├── 完全资源隔离
  ├── 可独立扩缩容
  └── 适合: 大规模表，生产环境
```

### 7.2 Flink 中的表服务调度

Flink 流式写入有专门的 Compaction/Clustering Operator：

```
Flink DAG:
  Source → StreamWriteFunction → (Checkpoint)
                                      ↓
                            CompactionPlanOperator (Schedule)
                                      ↓
                            CompactionCommitSink (Execute)
```

**为什么 Flink 不用 Async 模式？** 因为 Flink 的 Checkpoint 机制要求所有操作在 Checkpoint 边界上是一致的。Compaction 需要与 Checkpoint 协调，独立的 Operator 更容易实现这种协调。

---

## 8. 生产运维最佳实践

### 8.1 Compaction 运维

```
问题: Compaction 积压（pending compaction 越来越多）
诊断: 查看 .hoodie/timeline/ 目录中 .compaction.requested 文件数量
原因: Compaction 速度 < 写入产生 log 的速度
解决:
  1. 增大 Compaction 作业资源（executor 数/内存）
  2. 增大 hoodie.compaction.target.io（每次处理更多）
  3. 降低 hoodie.compact.inline.max.delta.commits（更频繁触发）
  4. 考虑独立 Compaction 作业（Separate Job 模式）
```

### 8.2 Clustering 运维

```
问题: 查询性能下降
诊断: 
  - 检查文件大小分布: 大量 < 10MB 的文件 → 小文件问题
  - 检查 Column Stats 的 min/max 范围: 范围很宽 → 数据局部性差
解决:
  1. 启用 Clustering + 设置 sort.columns
  2. 选择查询最频繁的过滤列作为 sort.columns
  3. 多列查询考虑 Z-Order 或 Hilbert 排序
```

### 8.3 小文件治理运维

```
预防措施:
  1. 写入时: 保持 hoodie.parquet.small.file.limit 合理（默认 100MB）
  2. 避免高基数分区（如按 user_id 分区）
  3. Bulk Insert 场景设 small.file.limit=0（跳过填充）

事后治理:
  1. 启用 Clustering（定期合并小文件）
  2. 设置合理的 target.file.max.bytes（如 128MB 或 256MB）
  3. 配置 partition.filter.mode=RECENT_DAYS（只处理最近的分区）
```

### 8.4 常见问题速查

| 问题 | 可能原因 | 解决方案 |
|------|---------|---------|
| 写入变慢 | Inline Compaction/Clustering 阻塞 | 改为 Async 或 Separate Job |
| 查询变慢 | 小文件多 / MOR log 堆积 | Clustering + Compaction |
| 存储持续增长 | Clean 未开启或配置不当 | 检查 `hoodie.clean.automatic=true` |
| MetaClient 初始化慢 | Timeline 文件过多 | 检查 Archival 配置 |
| Compaction OOM | 单个 FileSlice 的 log 太大 | 增大 executor 内存，降低 Compaction 间隔 |
| Clustering 失败 | 与写入冲突 | 检查是否有 pending compaction 覆盖同一文件 |

---

## 10. 表服务源码深度剖析

### 10.1 HoodieCompactor 的完整子类体系 —— 引擎适配的典范设计

**基类源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/HoodieCompactor.java`

HoodieCompactor 采用经典的**模板方法模式**（Template Method Pattern），基类定义了 Compaction 的完整执行框架，而将引擎差异化的行为延迟到子类实现。其继承体系如下：

```
HoodieCompactor<T, I, K, O> (抽象基类，定义核心模板)
    ├── HoodieSparkMergeOnReadTableCompactor<T>
    │     泛型: <T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>>
    │     源码: hudi-client/hudi-spark-client/.../HoodieSparkMergeOnReadTableCompactor.java
    │
    ├── HoodieFlinkMergeOnReadTableCompactor<T>
    │     泛型: <T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>>
    │     源码: hudi-client/hudi-flink-client/.../HoodieFlinkMergeOnReadTableCompactor.java
    │
    └── HoodieJavaMergeOnReadTableCompactor<T>
          泛型: <T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>>
          源码: hudi-client/hudi-java-client/.../HoodieJavaMergeOnReadTableCompactor.java
```

**三个子类必须实现的抽象方法及其差异**：

| 方法 | Spark 实现 | Flink 实现 | Java 实现 |
|------|-----------|-----------|-----------|
| `preCompact()` | 直接检查 REQUESTED 状态是否存在，不存在则抛异常 | 检查 INFLIGHT 状态，若存在则主动回滚后再执行（因 Flink Checkpoint 可能导致残留） | 类似 Flink，检查 INFLIGHT 并回滚；不支持 Log Compaction |
| `maybePersist()` | 调用 `writeStatus.persist()` 将中间结果缓存到 Spark 存储级别 | 空操作（No-Op），因 Flink 的流式执行不需要缓存中间 RDD | 空操作（No-Op），Java 引擎直接用内存 List |
| `getEngineRecordType()` | 返回 `HoodieRecordType.SPARK` | 返回 `HoodieRecordType.FLINK` | 返回 `HoodieRecordType.AVRO` |

**基类 `compact()` 方法的核心流程**（L84-L148）：

```java
// 1. 状态转换: REQUESTED → INFLIGHT
timeline.transitionCompactionRequestedToInflight(instant);

// 2. 解析 Schema（使用 TableSchemaResolver 获取表 Schema，而非 config.getSchema()）
//    为什么？因为 MergeInto 场景下 config.getSchema 可能与表 Schema 不一致

// 3. 将 CompactionPlan 中的操作转换为 CompactionOperation POJO
List<CompactionOperation> operations = compactionPlan.getOperations().stream()
    .map(CompactionOperation::convertFromAvroRecordInstance).collect(toList());

// 4. 并行执行合并（Compact 或 LogCompact）
//    对于普通 Compact: 使用 HoodieMergeHandle 合并 base + log → 新 base
//    对于 LogCompact: 使用 FileGroupReaderBasedAppendHandle 合并 log → 新 log
return context.parallelize(operations).map(operation -> compact(...)).flatMap(List::iterator);
```

**为什么这么设计？** Hudi 的引擎抽象核心思想是"**公共逻辑在基类，差异化逻辑在子类**"。Compaction 的核心合并逻辑（读取 base + log → merge → 写新 base）是引擎无关的，所以放在基类；而 Spark 需要 RDD 持久化、Flink 需要处理 Checkpoint 残留、Java 引擎不支持 Log Compaction 等差异，由各自子类处理。这种设计的好处是：**新增引擎支持时只需实现三个简单方法，而不需要重写整个 Compaction 流程**。

特别值得关注的是 Flink Compactor 的 `preCompact()` 设计。Flink 的 Checkpoint 机制可能导致 Compaction 被执行到一半然后 Task 被重启，留下 INFLIGHT 状态的 Instant。Spark 是批处理，失败后会整体重试，所以只检查 REQUESTED 是否存在即可。而 Flink 必须先回滚残留的 INFLIGHT，才能安全地重新执行——这体现了对不同引擎执行语义的深刻理解。

---

### 10.2 Compaction 计划生成器的完整架构 —— FileSlice 选择逻辑

**核心入口**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/ScheduleCompactionActionExecutor.java`

**计划生成器继承体系**：

```
BaseHoodieCompactionPlanGenerator (抽象基类)
    ├── HoodieCompactionPlanGenerator (普通 Compaction，使用 CompactionStrategy 排序过滤)
    └── HoodieLogCompactionPlanGenerator (Log Compaction，按 log block 数量筛选)
```

**完整的 FileSlice 选择流程**（`BaseHoodieCompactionPlanGenerator.generateCompactionPlan()`，L77-L189）：

```
Step 1: 获取分区列表
    ├── 如果启用增量表服务 (IncrementalPartitionAwareStrategy)
    │     → 只获取最近有变更的分区（详见 10.4）
    └── 否则 → 全表扫描所有分区

Step 2: 分区过滤 (filterPartitionPathsByStrategy)
    └── 由 CompactionStrategy 实现具体过滤逻辑

Step 3: 构建排除集合 (fgIdsInPendingCompactionAndClustering)
    ├── 排除正在 Pending Compaction 的 FileGroup
    ├── 排除正在 Pending Clustering 的 FileGroup  ★ 关键！
    └── 排除正在 Pending Log Compaction 的 FileGroup（仅普通 Compaction 会排除）

Step 4: 遍历每个分区的 FileSlice，执行过滤
    ├── filterFileSlice() 基本条件:
    │     a) FileSlice 必须有 log 文件 (logFiles.count > 0)
    │     b) FileSlice 的 FileGroupId 不在 Step 3 的排除集合中
    │
    ├── 过滤 pending log 文件 (关键安全逻辑):
    │     // 只保留 completionTime < compactionInstant 的 log 文件
    │     s.filterLogFiles(logFile ->
    │         completionTimeQueryView.isCompletedBefore(compactionInstant, logFile.getDeltaCommitTime()))
    │     // 为什么？避免包含未完成 delta commit 的 log 文件，防止读取不一致
    │
    └── 过滤后仍须有 log 文件 (FileSlice::hasLogFiles)

Step 5: 构建 CompactionOperation
    └── 每个 FileSlice → 一个 CompactionOperation (含 base file + log files + metrics)

Step 6: CompactionStrategy 排序过滤
    └── 由具体策略（如 LogFileSizeBasedCompactionStrategy）决定优先级和 I/O 上限

Step 7: 安全校验
    └── 确保最终计划中没有任何 FileGroup 与 pending compaction/clustering 冲突
```

**关键源码片段 —— log 文件的时间安全过滤**（BaseHoodieCompactionPlanGenerator L133-L147）：

```java
// 这段注释解释了为什么要过滤 pending log 文件:
// 假设 t10 发起 delta commit, t20 调度 compaction (此时 t10 还在 inflight),
// 如果将 t10 的 log 文件包含进 compaction plan,
// 当 t30 执行 compaction 时 t10 已完成，reader 会认为该 log 有效，
// 但 compaction 也合并了它 → 数据被重复读取！
s.filterLogFiles(logFile ->
    completionTimeQueryView.isCompletedBefore(compactionInstant, logFile.getDeltaCommitTime()));
```

**为什么排除 pending clustering 的文件？** Clustering 会替换整个 FileGroup（旧 FileGroup 被标记为 replaced，产生新的 FileGroup）。如果 Compaction 同时处理这些文件，会导致两种操作的输出文件冲突——Compaction 输出的新 base file 引用了即将被 Clustering 替换的 FileGroup，导致数据不一致。这体现了 Hudi 在表服务间的**冲突协调机制**：通过排除集合保证同一时刻同一 FileGroup 只被一种表服务处理。

**Log Compaction 的特殊筛选逻辑**（HoodieLogCompactionPlanGenerator）：

Log Compaction 是一种轻量级的 Compaction，它不合并 base + log，而是将多个小 log 文件合并为更少的大 log 文件。其额外筛选条件为：

```java
// 满足以下任一条件的 FileSlice 才会被选中:
// 1. log 文件数量 >= hoodie.log.compaction.blocks.threshold (默认 5)
// 2. 所有 log 文件中的 block 总数 >= threshold
```

这种设计的好处是：当 log 文件数量还不多时不浪费资源做 log compaction，只在 log 碎片化严重时才触发。

---

### 10.3 ClusteringExecutionStrategy 子类体系 —— 排序与重写的具体执行

**基类源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/cluster/strategy/ClusteringExecutionStrategy.java`

ClusteringExecutionStrategy 定义了 Clustering 执行阶段的可插拔接口，其完整继承体系如下：

```
ClusteringExecutionStrategy<T, I, K, O> (抽象基类)
    │
    ├── [Spark 引擎]
    │   ├── MultipleSparkJobExecutionStrategy<T> (多 Spark Job 执行，为每个 ClusteringGroup 提交独立的异步作业)
    │   │   ├── SparkSortAndSizeExecutionStrategy<T> (★ Spark 默认，排序+大小优化)
    │   │   │   ├── SparkBinaryCopyClusteringExecutionStrategy<T> (二进制流拷贝优化)
    │   │   │   │   └── SparkStreamCopyClusteringExecutionStrategy<T> (流式拷贝，跳过 Schema 检查)
    │   │   ├── SparkConsistentBucketClusteringExecutionStrategy<T> (一致性哈希桶 Clustering)
    │   │   └── SparkSingleFileSortExecutionStrategy<T> (单文件排序策略)
    │   │
    │   └── SingleSparkJobExecutionStrategy<T> (单 Spark Job 执行，所有 ClusteringGroup 在一个作业中)
    │       └── SingleSparkJobConsistentHashingExecutionStrategy<T> (单 Job 一致性哈希)
    │
    ├── [Java 引擎]
    │   └── JavaExecutionStrategy<T> (Java 引擎基类)
    │       └── JavaSortAndSizeExecutionStrategy<T> (Java 默认，与 Spark 版本逻辑类似)
    │
    └── [Flink 引擎]
        └── ClusteringOperator (Flink 直接在 Operator 中执行，不使用此继承体系)
```

**SparkSortAndSizeExecutionStrategy 的执行逻辑**（核心实现）：

```
输入: ClusteringGroup (一组待重写的 FileSlice)
输出: 新的 Parquet 文件

执行流程:
  1. 构建新的 WriteConfig:
     ├── bulkInsertParallelism = numOutputGroups (控制输出文件数)
     └── parquetMaxFileSize = clusteringTargetFileMaxBytes (目标文件大小)

  2. 选择分区器 (Partitioner):
     ├── 如果指定了 sort.columns:
     │   ├── LINEAR → RowCustomColumnsSortPartitioner (单列排序)
     │   ├── ZORDER → RowSpatialCurveSortPartitioner (Z-Order 空间曲线)
     │   └── HILBERT → RowSpatialCurveSortPartitioner (Hilbert 空间曲线)
     └── 未指定 sort.columns:
         └── 使用默认的 BulkInsert Partitioner (不排序，只控制大小)

  3. 数据重分区:
     repartitionedRecords = partitioner.repartitionRecords(inputRecords, numOutputGroups)
     // 这一步完成了排序 + 按目标文件大小切分

  4. 使用 BulkInsert 写入新文件:
     HoodieDatasetBulkInsertHelper.bulkInsert(repartitionedRecords, ...)
     // BulkInsert 内部使用 CreateHandleFactory 创建新的 Parquet 文件
```

**为什么使用 BulkInsert 而不是普通 Write？** BulkInsert 是 Hudi 中最高效的批量写入模式——它跳过了 UpsertPartitioner 的小文件检测和 FileGroup 匹配逻辑，直接创建新文件。对于 Clustering 来说，目标就是重写一组全新的文件，不需要与已有文件关联，所以 BulkInsert 是最合适的选择。

**SparkBinaryCopyClusteringExecutionStrategy 的优化设计**：

这是一个极具创意的优化策略。当满足以下条件时，它直接在二进制层面拷贝 Parquet 文件的 Row Group，而不需要反序列化再序列化：

```
启用条件:
  1. 表类型必须是 COW (因为 MOR 需要合并 log)
  2. 不能指定排序列 (排序需要读取并重排数据)
  3. 文件格式必须是 Parquet
  4. 所有文件的 Schema、BloomFilter 类型、列类型必须兼容
```

当条件不满足时，自动降级为父类 `SparkSortAndSizeExecutionStrategy` 的常规逻辑。这种"**条件优化 + 优雅降级**"的设计保证了安全性的同时最大化了性能：二进制拷贝比先读后写快数倍，因为省去了 Parquet 的解码和编码开销。

**MultipleSparkJobExecutionStrategy vs SingleSparkJobExecutionStrategy**：

- **MultipleSparkJobExecutionStrategy**：为每个 ClusteringGroup 提交一个独立的 CompletableFuture，使用线程池并行执行，最后 union 所有结果。优点是每个 Group 可以独立失败和重试，且支持 Row 模式和 RDD 模式两条执行路径。
- **SingleSparkJobExecutionStrategy**：将所有 ClusteringGroup 用一个 `parallelize().map()` 处理。优点是提交到 Spark 的是单个 Job，适合 ClusteringGroup 数量非常大（如数千个）的场景，避免过多 Job 带来的调度开销。

**Flink 的特殊实现 —— ClusteringOperator**：

Flink 引擎没有使用 ClusteringExecutionStrategy 继承体系，而是在 `ClusteringOperator`（位于 `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/sink/clustering/ClusteringOperator.java`）中直接实现了 Clustering 执行逻辑。这是因为 Flink 的流式执行模型要求 Clustering 操作内嵌在 Flink DAG 中，而不是像 Spark 那样以独立 Job 形式存在。Flink 的 ClusteringOperator 使用 `BulkInsertWriterHelper` 写入数据，并支持通过 `SortOperatorGen` 生成排序算子，与 Flink 的算子链无缝集成。

---

### 10.4 IncrementalPartitionAwareStrategy —— 增量表服务的精妙设计

**接口源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/IncrementalPartitionAwareStrategy.java`

**核心实现逻辑**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/BaseTableServicePlanActionExecutor.java`

**问题背景**：在一个拥有数万个分区的大表上，每次调度 Compaction 或 Clustering 都需要扫描所有分区来发现需要处理的文件。在云存储（如 S3）上，LIST 操作本身就很昂贵——1 万个分区 = 1 万次 LIST 请求，仅分区发现就可能耗时数分钟。

**解决方案 —— 只扫描"最近有变更的分区"**：

```java
// IncrementalPartitionAwareStrategy 是一个 Marking Interface（标记接口）
public interface IncrementalPartitionAwareStrategy {
    Pair<List<String>, List<String>> filterPartitionPaths(
        HoodieWriteConfig writeConfig, List<String> partitions);
}
```

`CompactionStrategy` 抽象类直接实现了此接口，这意味着**所有 CompactionStrategy 的子类都自动具备增量分区感知能力**。

**增量分区获取的完整流程**（`BaseTableServicePlanActionExecutor.getIncrementalPartitions()`，L106-L136）：

```
Step 1: 获取上次成功的表服务 Instant
    ├── Compaction → 获取最近完成的 COMMIT (compaction 产出)
    ├── Log Compaction → 同上
    └── Clustering → 获取最近完成的 Clustering Instant

Step 2: 获取 "Missing Partitions"（上次被策略过滤掉的分区）
    └── 从上次的 CompactionPlan/ClusteringPlan 的 missingSchedulePartitions 字段读取
    // 为什么？某些分区上次被策略过滤了（如 log 太小），但不代表这次也该跳过

Step 3: 计算时间窗口 [leftBoundary, rightBoundary)
    ├── leftBoundary = 上次完成的表服务 Instant 的 requestedTime
    └── rightBoundary = 当前 Instant 的 requestedTime

Step 4: 扫描时间窗口内所有已完成 commit 的 metadata
    ├── 遍历 [leftBoundary, rightBoundary) 内的每个 commit/deltacommit/replacecommit
    ├── 读取每个 commit 的 HoodieCommitMetadata
    ├── 提取 writeStats 中涉及的 partitionPath
    └── 汇总为增量分区集合

Step 5: 合并增量分区 + Missing Partitions
    └── 最终分区列表 = 步骤 4 的增量分区 ∪ 步骤 2 的 Missing Partitions
```

**三种降级场景（Fallback to Full Partition Scan）**：

1. **首次执行**：没有历史表服务 Instant → 无法确定增量起点 → 全量扫描
2. **历史 Instant 已归档**：上次的表服务 Instant 已被 Archival 移走 → 无法读取 → 全量扫描
3. **任何异常**：增量分区获取过程中抛异常 → catch 后降级为全量扫描

**"Missing Partitions" 的精妙设计**：

这是增量表服务中最巧妙的部分。假设分区 A 在第一次 Compaction 调度时有少量 log（被策略过滤掉了），第二次调度时没有新写入。如果只看增量 commit，分区 A 不会出现在增量列表中，但它实际上仍需要 Compaction。为此，Hudi 在每次生成计划时记录"被过滤掉的分区"到 `missingSchedulePartitions` 字段，下次调度时会重新检查这些分区。这保证了增量模式不会遗漏任何需要处理的分区。

**好处**：对于一个 10000 分区的表，如果每次只有 100 个分区有新写入，增量模式将 LIST 请求从 10000 次降低到约 100 次，调度耗时从分钟级降低到秒级。

---

### 10.5 Clean 与 Savepoint 的协调 —— 保护关键数据不被删除

**核心源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/clean/CleanPlanner.java`

**Savepoint 的本质**：Savepoint 是对表在某个时间点的快照的"书签"。创建 Savepoint 后，该时间点引用的所有数据文件必须被保留，即使按正常 Clean 策略它们已经过期。这类似于数据库的 `SAVEPOINT`——你可以随时回滚到这个点。

**Savepoint 感知的实现机制**：

CleanPlanner 在构造函数中就收集了所有 Savepoint 的时间戳：

```java
// CleanPlanner 构造函数 (L99-L117)
this.savepointedTimestamps = hoodieTable.isMetadataTable()
    ? Collections.emptyList()
    : (hoodieTable.isPartitioned()
        ? new ArrayList<>(hoodieTable.getSavepointTimestamps())
        : Collections.emptyList());
```

**注意**：Metadata Table 和非分区表不收集 Savepoint 信息。这是因为 Metadata Table 有自己的生命周期管理，而非分区表的 Savepoint 保护通过其他路径实现。

**三种 Clean 策略中 Savepoint 保护的具体实现**：

**策略 1: KEEP_LATEST_FILE_VERSIONS** (`getFilesToCleanKeepingLatestVersions`, L354-L405)

```java
// 1. 收集所有 Savepoint 引用的文件
List<String> savepointedFiles = hoodieTable.getSavepointTimestamps().stream()
    .flatMap(this::getSavepointedDataFiles)
    .collect(Collectors.toList());

// 2. 遍历超出保留版本数的 FileSlice
while (fileSliceIterator.hasNext()) {
    FileSlice nextSlice = fileSliceIterator.next();
    // ★ 核心保护逻辑：如果该 FileSlice 存在于 Savepoint 引用的文件列表中，跳过不删
    if (isFileSliceExistInSavepointedFiles(nextSlice, savepointedFiles)) {
        continue;  // do not clean up a savepoint data file
    }
    deletePaths.addAll(getCleanFileInfoForSlice(nextSlice));
}
```

**策略 2: KEEP_LATEST_COMMITS** (`getFilesToCleanKeepingLatestCommits`, L428-L507)

同样的保护逻辑：

```java
for (FileSlice aSlice : fileSliceList) {
    if (isFileSliceExistInSavepointedFiles(aSlice, savepointedFiles)) {
        continue;  // do not clean up a savepoint data file
    }
    // ... 正常的过期判断逻辑
}
```

**被替换文件的 Savepoint 保护**（`getReplacedFilesEligibleToClean`, L545-L557）：

Clustering 产生的 replaced FileGroup 也需要检查 Savepoint：

```java
replacedGroups.flatMap(HoodieFileGroup::getAllFileSlices)
    // ★ 即使文件已被 Clustering 替换，只要被 Savepoint 引用就不能删
    .filter(slice -> !isFileSliceExistInSavepointedFiles(slice, savepointedFiles))
    .flatMap(slice -> getCleanFileInfoForSlice(slice).stream())
    .collect(Collectors.toList());
```

**FileSlice 的 Savepoint 检查方法**（`isFileSliceExistInSavepointedFiles`, L337-L347）：

```java
private boolean isFileSliceExistInSavepointedFiles(FileSlice fs, List<String> savepointedFiles) {
    // 检查 base file
    if (fs.getBaseFile().isPresent()
        && savepointedFiles.contains(fs.getBaseFile().get().getFileName())) {
        return true;
    }
    // 检查 log files
    for (HoodieLogFile hoodieLogFile : fs.getLogFiles().collect(Collectors.toList())) {
        if (savepointedFiles.contains(hoodieLogFile.getFileName())) {
            return true;
        }
    }
    return false;
}
```

**Savepoint 与增量 Clean 的交互**（`isAnySavepointDeleted`, L246-L257）：

增量 Clean 模式下，如果上次 Clean 以来有 Savepoint 被删除了（通过对比上次 Clean 记录的 savepointedTimestamps 和当前的），则必须降级为全量 Clean。为什么？因为 Savepoint 的删除意味着之前受保护的文件现在可能可以被清理了，而增量模式可能不会扫描到这些文件所在的分区。

```java
private boolean isAnySavepointDeleted(HoodieCleanMetadata cleanMetadata) {
    List<String> savepointedTimestampsFromLastClean = ...;
    List<String> removedSavepointedTimestamps = new ArrayList<>(savepointedTimestampsFromLastClean);
    removedSavepointedTimestamps.removeAll(savepointedTimestamps);
    return !removedSavepointedTimestamps.isEmpty();
}
```

**好处**：Savepoint 保护机制使得用户可以安全地在数据湖上创建"恢复点"，同时 Clean 操作可以正常进行而不影响这些恢复点。这在生产环境中非常重要——例如数据迁移前创建 Savepoint，迁移失败后可以回滚到 Savepoint 对应的数据版本。

---

### 10.6 Archival V1 vs V2 —— 从 Avro Log 到 LSM-Tree 的演进

**V1 源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/timeline/versioning/v1/TimelineArchiverV1.java`

**V2 源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/timeline/versioning/v2/TimelineArchiverV2.java`

**LSMTimelineWriter**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/timeline/versioning/v2/LSMTimelineWriter.java`

两个版本都实现了 `HoodieTimelineArchiver` 接口，但内部存储格式和管理机制有根本性区别。

#### V1: Avro Log 文件格式

**归档存储格式**：将 Timeline Instant 的元数据序列化为 `HoodieArchivedMetaEntry` Avro 记录，追加写入到一个 Avro Log 文件中。

```
.hoodie/archived/
    .commits_.archive.1  (HoodieLogFormat，包含 HoodieAvroDataBlock)
    .commits_.archive.2
    ...
```

**核心写入逻辑** (`TimelineArchiverV1.archive()`, L430-L454):

```java
public void archive(HoodieEngineContext context, List<HoodieInstant> instants) {
    Schema wrapperSchema = HoodieArchivedMetaEntry.getClassSchema();
    List<IndexedRecord> records = new ArrayList<>();
    for (HoodieInstant hoodieInstant : instants) {
        records.add(convertToAvroRecord(hoodieInstant));
        if (records.size() >= config.getCommitArchivalBatchSize()) {
            writeToFile(wrapperSchema, records);  // 批量写入 Avro Data Block
        }
    }
    writeToFile(wrapperSchema, records);  // 写入剩余记录
}
```

**V1 的问题**：
- Avro Log 文件是追加写的，随着归档增多文件会越来越大
- 读取归档 Timeline 需要顺序扫描整个 Log 文件
- V1 永久禁用了归档文件合并（源码注释: "We permanently disable merging archive files"）

#### V2: LSM-Tree 格式

**归档存储格式**：将 Timeline Instant 的元数据序列化为 `HoodieLSMTimelineInstant` Avro 记录，写入到 Parquet 文件中，文件组织为 LSM-Tree 结构。

```
.hoodie/archived/
    20240101_20240110_0.parquet   (L0 层)
    20240111_20240120_0.parquet   (L0 层)
    20240101_20240120_1.parquet   (L1 层，由 L0 合并产生)
    _version                      (版本文件)
    manifest_3                    (清单文件)
```

**LSM-Tree 的文件命名规则**: `${min_instant}_${max_instant}_${level}.parquet`

- `min_instant` / `max_instant`: 文件包含的 Instant 时间范围
- `level`: LSM 层级（0 为最新层，数字越大越老）

**V2 的 Compaction 与 Cleaning 机制** (`LSMTimelineWriter.compactAndClean()`, L255-L270):

```java
public void compactAndClean(HoodieEngineContext context) throws IOException {
    HoodieLSMTimelineManifest latestManifest = LSMTimeline.latestSnapshotManifest(metaClient, archivePath);
    int layer = 0;
    // 从 L0 层开始，递归触发 compaction
    Option<String> compactedFileName = doCompact(latestManifest, layer);
    while (compactedFileName.isPresent()) {
        latestManifest.addFile(getFileEntry(compactedFileName.get()));
        compactedFileName = doCompact(latestManifest, ++layer);
    }
    // compaction 完成后触发 cleaning
    clean(context, layer);
}
```

**Compaction 触发条件**: 当某一层的文件数 >= `hoodie.timeline.compaction.batch.size` (默认 10) 时，合并该层最老的 N 个文件为下一层的一个文件。

**Manifest 管理机制**:

V2 使用 Manifest + Version 文件来管理 LSM-Tree 的一致性视图：

```
写入新归档文件的流程:
  1. 写入 Parquet 数据文件
  2. 读取当前最新 Manifest → 追加新文件名 → 写入新 Manifest
  3. 更新 Version 文件指向新 Manifest
```

这种设计保证了即使在步骤 2 或 3 失败时，也能通过列举 Manifest 文件找到最新的有效快照。

#### V1 vs V2 的关键差异对比

| 维度 | V1 (Avro Log) | V2 (LSM-Tree Parquet) |
|------|--------------|----------------------|
| **存储格式** | Avro Log Block (追加写) | Parquet 文件 (LSM-Tree 组织) |
| **读取方式** | 顺序扫描整个 Log | 利用 Parquet 的列式读取 + LSM 分层 |
| **Compaction** | 永久禁用 | 自动触发 L0→L1→L2... 逐层合并 |
| **Schema** | HoodieArchivedMetaEntry | HoodieLSMTimelineInstant |
| **一致性管理** | 无 (单文件追加) | Manifest + Version 文件 |
| **Clean/Rollback 归档** | 独立处理 (getCleanInstantsToArchive) | 与 Commit 一起归档 (以 latestCommitInstantToArchive 为边界) |
| **ActiveAction 概念** | 无 (直接操作 HoodieInstant) | 有 (ActiveAction 封装一个 Instant 的所有状态文件) |

#### 归档候选 Instant 选择逻辑（两版本共有）

两个版本在选择哪些 Instant 可以归档时，都遵循相同的**五重保护**规则：

```
一个 Instant 只有同时满足以下所有条件才能被归档:

1. 超出保留阈值: completedCommits.count > maxInstantsToKeep
2. 不影响 Pending 写入: 在最老的 pending instant 之前
3. 不影响 Compaction 调度: 在 Compaction 需要的最老 delta commit 之前
4. 不影响 Clustering: 在 Clustering 需要的最老 replaced 文件相关 commit 之前
5. 不超过 MDT Compaction: 在 Metadata Table 最新 compaction 时间之前
   (否则 MDT 无法通过 replay 构建元数据)

额外保护:
6. Savepoint 保护:
   ├── shouldArchiveBeyondSavepoint = true → 跳过 savepoint 对应的 commit 继续归档
   └── shouldArchiveBeyondSavepoint = false → 在第一个 savepoint 处停止归档
```

**为什么从 V1 演进到 V2？** V1 的 Avro Log 追加写模式在归档数据量大时（如运行数年的表，可能有数百万个归档 Instant）面临严重的读取性能问题——每次读取归档 Timeline 都需要反序列化整个 Log 文件。V2 的 LSM-Tree + Parquet 方案解决了这个问题：Parquet 的列式存储和 Predicate Pushdown 使得范围查询归档 Timeline 非常高效，而 LSM-Tree 的分层合并避免了小文件过多。此外，V2 的 Manifest 机制提供了原子性的视图管理，支持并发归档操作。

---

**文档版本**: 4.0（新增第 10 章：表服务源码深度剖析）
**创建日期**: 2026-04-14
**最后更新**: 2026-04-15
**基于 Hudi 版本**: v1.2.0-SNAPSHOT (master)
