# Apache Hudi 面试题库

> 本文档从面试官视角整理了 Apache Hudi 核心知识点，涵盖表模型、索引机制、事务与并发控制、Compaction/Clustering、Flink/Spark 集成、元数据管理等关键领域。
>
> **难度标记**: 🟢 基础 | 🟡 中级 | 🟠 高级 | 🔴 专家

---

## 目录（按题目导航）

### 一、表模型与存储架构
- [COW 和 MOR 表的核心区别是什么？各自适用什么场景？](#q-1-1) 🟢
- [什么是 FileGroup？FileSlice 与 FileGroup 是什么关系？](#q-1-2) 🟢
- [MOR 表的 Log File 内部结构是什么？支持哪些 Block 类型？](#q-1-3) 🟡
- [Hudi 的 Pre-combine 机制是什么？为什么需要它？](#q-1-4) 🟡
- [Hudi 如何保证同一 FileGroup 内记录的唯一性？](#q-1-5) 🟠

### 二、索引机制
- [Hudi 为什么需要索引？索引在写入流程中扮演什么角色？](#q-2-1) 🟢
- [Bloom Index 的工作原理是什么？它的误判问题如何处理？](#q-2-2) 🟡
- [Bucket Index 的两种引擎（SIMPLE vs CONSISTENT_HASHING）有什么区别？](#q-2-3) 🟡
- [Record Level Index (RLI) 是什么？与 Bloom/Bucket Index 相比有什么优势？](#q-2-4) 🟠
- [如何选择合适的索引类型？给出一个超大表的索引选型方案。](#q-2-5) 🟠
- [Bucket Index 的 isImplicitWithStorage() 返回 true 意味着什么？对写入流程有何影响？](#q-2-6) 🔴

### 三、事务与并发控制
- [Hudi 的 Timeline 机制如何保证 ACID 特性？](#q-3-1) 🟡
- [两个 Spark 作业同时写入同一个 Hudi 表时会发生什么？乐观并发控制如何工作？](#q-3-2) 🟠
- [HoodieInstant 的三种状态（REQUESTED/INFLIGHT/COMPLETED）的转换流程是什么？](#q-3-3) 🟡
- [Hudi 的 Rollback 机制如何工作？INFLIGHT 状态的 commit 如何恢复？](#q-3-4) 🟠
- [Hudi 的 Multi-Writer 如何实现？Lock Provider 有哪些实现？](#q-3-5) 🔴

### 四、Compaction 与 Clustering
- [Compaction 和 Clustering 的区别是什么？](#q-4-1) 🟢
- [Compaction 为什么分为 Schedule 和 Execute 两个阶段？这样设计有什么好处？](#q-4-2) 🟡
- [Clustering 的排序策略有哪些？Z-Order 排序相比线性排序有什么优势？](#q-4-3) 🟠
- [Compaction 策略如何选择？LogFileSizeBasedCompactionStrategy 的排序逻辑是什么？](#q-4-4) 🟡
- [Clustering 操作会改变 FileGroup ID 吗？这对索引有什么影响？](#q-4-5) 🟠
- [异步 Compaction 和内联 Compaction 的实现差异是什么？生产环境应如何选择？](#q-4-6) 🔴

### 五、Spark 集成
- [Spark 写入 Hudi 的完整调用链是什么？从 DataFrame.write 到最终 commit 经历了哪些步骤？](#q-5-1) 🟡
- [COW 表的 HoodieMergeHandle 和 MOR 表的 HoodieAppendHandle 有什么核心区别？](#q-5-2) 🟠
- [Spark 读取 MOR 表时，HoodieFileGroupReader 如何合并 base file 和 log files？](#q-5-3) 🟠
- [Hudi 的三种查询类型（snapshot/incremental/read_optimized）分别适用什么场景？](#q-5-4) 🟢
- [HoodieFileIndex 如何实现多级数据裁剪（分区裁剪 → 列统计 → Bloom Filter → RLI）？](#q-5-5) 🟠
- [Spark SQL 中 `CALL run_compaction` 的执行路径是什么？它如何找到 pending compaction plan？](#q-5-6) 🔴

### 六、Flink 集成
- [Flink 写入 Hudi 的核心组件有哪些？StreamWriteFunction 的角色是什么？](#q-6-1) 🟡
- [Flink Checkpoint 如何与 Hudi 事务集成实现 Exactly-Once？](#q-6-2) 🟠
- [BucketStreamWriteFunction 和 StreamWriteFunction 的区别是什么？](#q-6-3) 🟡
- [Flink State Index 如何工作？与其他索引类型相比有什么优劣？](#q-6-4) 🟠
- [Flink 写入 Hudi 时的数据缓冲机制是什么？SnapshotPreBarrier 在其中扮演什么角色？](#q-6-5) 🔴

### 七、元数据管理
- [Metadata Table 包含哪些分区？各分区的作用是什么？](#q-7-1) 🟡
- [Metadata Table 本身也是一个 Hudi MOR 表，它如何保证与主表的一致性？](#q-7-2) 🟠
- [Column Stats Index 如何实现 Data Skipping？举例说明。](#q-7-3) 🟡
- [Timeline V1 和 V2 格式的主要区别是什么？V2 为什么引入 completion time？](#q-7-4) 🟠
- [HoodieTableMetaClient 的核心职责是什么？它如何加载和缓存 Timeline？](#q-7-5) 🔴

### 八、Clean 与 Archival
- [Clean 操作的三种策略各适用什么场景？](#q-8-1) 🟢
- [Clean 如何保证不删除正在被读取的文件？](#q-8-2) 🟡
- [Archival 归档的触发条件是什么？对增量查询有什么影响？](#q-8-3) 🟡

### 九、Schema Evolution
- [Hudi 的 InternalSchema 如何实现无副作用的 Schema 演进？](#q-9-1) 🟡
- [读时 Schema 对齐是如何工作的？FileGroupReaderSchemaHandler 的逻辑是什么？](#q-9-2) 🟠
- [Hudi 与 Iceberg 的 Schema Evolution 机制有什么异同？](#q-9-3) 🟠

### 十、Hudi vs Iceberg vs Delta Lake
- [从架构层面对比 Hudi、Iceberg 和 Delta Lake 的核心设计差异。](#q-10-1) 🟠
- [三者在写入路径上的主要区别是什么？Hudi 的 Index + Tag 模式有什么优势？](#q-10-2) 🟠
- [三者在文件管理策略上有什么不同？各自如何解决小文件问题？](#q-10-3) 🟡

### 十一、源码级深度问题
- [解释 HoodieFileGroupReader 的合并读取流程，base file 和 log files 的合并是如何实现的？RecordMerger 在其中扮演什么角色？](#q-11-1) 🔴
- [Hudi 的 Timeline V2 相比 V1 解决了什么问题？从 HoodieInstant 的 completionTime 字段说起。](#q-11-2) 🔴
- [解释 UpsertPartitioner 中小文件填充的完整算法。averageRecordSize 是如何估算的？为什么排除了 clustering 产生的 commit？](#q-11-3) 🔴
- [Hudi 的 Expression Index 是什么？它解决了 Column Stats Index 的什么局限？举一个 date_format 的例子。](#q-11-4) 🔴
- [Hudi 支持哪些空间填充曲线排序策略？Z-Order 和 Hilbert 曲线的本质区别是什么？为什么 Hilbert 通常更好？](#q-11-5) 🟠

### 十二、架构对比深度问题
- [Hudi 的 Metadata Table 为什么选择用 MOR 表实现？相比 Iceberg 的 Manifest File 有什么优劣？](#q-12-1) 🟠
- [对比 Hudi 的 Bucket Index 和 Iceberg 的分区裁剪，在大规模 upsert 场景下谁更高效？为什么？](#q-12-2) 🟠
- [Hudi、Iceberg、Delta Lake 三者的并发控制策略有何不同？各自的 OCC 实现有什么差异？](#q-12-3) 🔴

### 十三、运维实战问题
- [生产环境中 Compaction 积压了 50 个 pending plan，你会如何排查和解决？](#q-13-1) 🟠
- [一个 MOR 表的 Snapshot 查询突然变慢了 10 倍，你的排查思路是什么？](#q-13-2) 🟠
- [如何安全地将一个运行中的 Hudi 表从表版本 7 升级到版本 9？](#q-13-3) 🔴

---

## 一、表模型与存储架构

<a id="q-1-1"></a>
### 1.1 COW vs MOR

**[绿] 问题: COW 和 MOR 表的核心区别是什么？各自适用什么场景？**

**答案:**

| 维度 | COW (Copy-On-Write) | MOR (Merge-On-Read) |
|------|---------------------|---------------------|
| **写入方式** | 重写整个 base file | 追加到 log file |
| **写入延迟** | 高（需要读旧文件+合并+写新文件） | 低（只需追加 log） |
| **读取方式** | 直接读 base file | base + log 合并读取 |
| **读取延迟** | 低 | 依赖 Compaction 频率 |
| **写放大** | 高（更新一行也要重写整个文件） | 低 |
| **文件类型** | 只有 Parquet/ORC base files | Parquet base + Avro log files |

**适用场景:**
- **COW**: 读多写少、对查询延迟敏感、批量 ETL
- **MOR**: 写多读少、流式写入（Flink）、CDC 同步

**源码证据:**
- COW 写入: `HoodieMergeHandle` — 读取旧 Parquet → 合并 → 写新 Parquet
  - `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieMergeHandle.java`
- MOR 写入: `HoodieAppendHandle` — 直接追加到 log file
  - `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieAppendHandle.java`

---

<a id="q-1-2"></a>
### 1.2 FileGroup 与 FileSlice

**[绿] 问题: 什么是 FileGroup？FileSlice 与 FileGroup 是什么关系？**

**答案:**

```
FileGroup (文件组): 由 fileId 唯一标识，包含同一组数据的所有历史版本
    │
    ├── FileSlice @T1: base_file_T1.parquet
    │                  + .log.1 (T2 的增量)
    │                  + .log.2 (T3 的增量)
    │
    └── FileSlice @T4: base_file_T4.parquet  (Compaction 后的新 base)
                       + .log.3 (T5 的增量)
```

- **FileGroup**: 逻辑概念，由 `fileId` 标识。一个分区内有多个 FileGroup。
- **FileSlice**: FileGroup 在某个 base commit 时间点的视图。包含一个 base file（可选）+ 若干 log files。
- 每次 Compaction 产生新的 base file，开启新的 FileSlice。
- 读取时选择最新的 FileSlice。

**源码位置:**
- `hudi-common/src/main/java/org/apache/hudi/common/model/HoodieFileGroup.java`
- `hudi-common/src/main/java/org/apache/hudi/common/model/FileSlice.java`

---

<a id="q-1-3"></a>
### 1.3 Log File 内部结构

**[黄] 问题: MOR 表的 Log File 内部结构是什么？支持哪些 Block 类型？**

**答案:**

Log File 使用 Hudi 自定义的日志格式，由多个 `HoodieLogBlock` 组成：

```
Log File Structure:
├── Magic Bytes (6 bytes)
├── HoodieLogBlock 1
│   ├── Header (version, block type, content length, ...)
│   ├── Content (实际数据)
│   └── Footer (checksum)
├── HoodieLogBlock 2
│   └── ...
└── EOF
```

**Block 类型:**

| Block 类型 | 描述 | 包含内容 |
|-----------|------|---------|
| `AVRO_DATA_BLOCK` | Avro 格式数据块 | INSERT/UPDATE 记录 |
| `HFILE_DATA_BLOCK` | HFile 格式数据块 | 排序的 key-value 对 |
| `PARQUET_DATA_BLOCK` | Parquet 格式数据块 | 列式存储的记录 |
| `DELETE_BLOCK` | 删除标记块 | 要删除的 record keys |
| `COMMAND_BLOCK` | 命令块 | 回滚等控制命令 |

**源码位置:**
- `hudi-common/src/main/java/org/apache/hudi/common/table/log/HoodieLogFormat.java`
- `hudi-common/src/main/java/org/apache/hudi/common/table/log/block/`

---

<a id="q-1-4"></a>
### 1.4 Pre-combine 机制

**[黄] 问题: Hudi 的 Pre-combine 机制是什么？为什么需要它？**

**答案:**

Pre-combine 是 Hudi 处理同一批写入中**重复 record key** 的策略。

**场景:** 同一批数据中可能包含同一个 record key 的多条记录（如 CDC 数据中同一行的多次变更）。Pre-combine 在写入前合并这些重复记录。

**工作原理:**
1. 用户指定 `hoodie.datasource.write.precombine.field`（如 `ts` 时间戳字段）
2. 对于相同 record key 的多条记录，**取 pre-combine field 值最大的那条**

```sql
-- 示例: CDC 数据中同一用户的多次变更
record1: (id=1, name="Alice", ts=100)
record2: (id=1, name="Alice_v2", ts=200)

-- Pre-combine 后只保留 ts=200 的记录
result: (id=1, name="Alice_v2", ts=200)
```

**关键配置:**
- `hoodie.datasource.write.precombine.field`: Pre-combine 字段
- 默认使用 `DefaultHoodieRecordPayload`，按 pre-combine field 比较大小

---

<a id="q-1-5"></a>
### 1.5 记录唯一性保证

**[橙] 问题: Hudi 如何保证同一 FileGroup 内记录的唯一性？**

**答案:**

Hudi 通过**索引 + 写入时合并**的两阶段机制保证唯一性：

1. **Index.tagLocation()**: 查找记录是否已存在
   - 存在 → 标记为 UPDATE，记录目标 FileGroup
   - 不存在 → 标记为 INSERT

2. **Write Phase**:
   - COW: `HoodieMergeHandle` 读取旧文件，按 record key 合并新旧记录
   - MOR: 直接追加到 log file，读取时由 `HoodieMergedLogRecordScanner` 合并

**唯一性范围取决于索引类型:**
- `BLOOM/SIMPLE/BUCKET`: 分区内唯一
- `GLOBAL_BLOOM/GLOBAL_SIMPLE/GLOBAL_RECORD_LEVEL_INDEX`: 全表唯一

---

## 二、索引机制

<a id="q-2-1"></a>
### 2.1 索引的核心作用

**[绿] 问题: Hudi 为什么需要索引？索引在写入流程中扮演什么角色？**

**答案:**

索引的核心作用是**将 record key 映射到 FileGroup**，使 upsert/delete 操作能高效定位记录。

**写入流程中的位置:**
```
records → Index.tagLocation() → Tagged Records → Write → Index.updateLocation()
```

没有索引，每次 upsert 都需要扫描所有文件来判断记录是否存在（O(N)）。有了索引，可以快速定位（O(1) 或 O(logN)）。

**这是 Hudi 与 Iceberg 的核心区别之一。** Iceberg 没有内置索引机制，更新操作依赖引擎层面的 JOIN 或 Delete Files。

---

<a id="q-2-2"></a>
### 2.2 Bloom Index

**[黄] 问题: Bloom Index 的工作原理是什么？它的误判问题如何处理？**

**答案:**

**工作原理:**
1. 每个 Parquet 文件的 footer 中存储了该文件所有 record keys 的 Bloom Filter
2. 写入时，用 Bloom Filter 判断传入的 record key 是否可能存在于某个文件
3. BF 返回 FALSE → 一定不存在（INSERT）
4. BF 返回 TRUE → 可能存在（需要精确查找确认）

**误判处理:**
- Bloom Filter 有假阳性（False Positive），不存在的 key 可能被判定为存在
- Hudi 对 BF 返回 TRUE 的 key 执行**精确查找**：读取文件中的实际 key 进行匹配
- 通过 Key Range 裁剪（`min_record_key` / `max_record_key`）预先排除不可能包含目标 key 的文件

**源码证据:**
```java
// HoodieBloomIndex.java 中的精确查找逻辑
// 先用 key range 过滤，再用 Bloom Filter 过滤，最后精确匹配
```

---

<a id="q-2-3"></a>
### 2.3 Bucket Index 两种引擎

**[黄] 问题: Bucket Index 的两种引擎（SIMPLE vs CONSISTENT_HASHING）有什么区别？**

**答案:**

| 维度 | SIMPLE | CONSISTENT_HASHING |
|------|--------|-------------------|
| 桶数量 | 固定，建表时确定 | 动态，可通过 Clustering 调整 |
| 数据倾斜 | 可能（固定哈希） | 自动处理（桶分裂/合并） |
| 表类型 | COW + MOR | 仅 MOR |
| 复杂度 | 简单 | 较复杂 |
| 适用场景 | 数据分布均匀 | 数据分布不均匀或随时间变化 |

**CONSISTENT_HASHING 的桶调整:**
```
桶太大 → SPLIT → 分裂成两个桶（通过 Clustering 执行）
桶太小 → MERGE → 合并相邻桶（通过 Clustering 执行）
```

---

<a id="q-2-4"></a>
### 2.4 Record Level Index

**[橙] 问题: Record Level Index (RLI) 是什么？与 Bloom/Bucket Index 相比有什么优势？**

**答案:**

RLI 在 Metadata Table 的 `record_index` 分区中存储精确的 `record key → (partition, fileId)` 映射。

**优势对比:**

| 维度 | Bloom | Bucket | RLI |
|------|-------|--------|-----|
| 查找复杂度 | O(N files) | O(1) | O(1) |
| 误判 | 有 | 无 | 无 |
| 额外存储 | 无 | 无 | Metadata Table |
| 动态扩展 | 自然 | 受桶数限制 | 自然 |
| 超大表支持 | 差 | 中 | 好（分片） |

RLI 适合超大规模表（10亿+ 行），支持 Metadata Table 分片以应对海量索引数据。

---

<a id="q-2-5"></a>
### 2.5 索引选型

**[橙] 问题: 如何选择合适的索引类型？给出一个超大表的索引选型方案。**

**答案:**

**决策树:**
```
Flink 流式写入? → FLINK_STATE
数据量 > 10亿行?
  ├── 需要全局唯一 → GLOBAL_RECORD_LEVEL_INDEX
  └── 分区内唯一 → BUCKET (SIMPLE) 或 RECORD_LEVEL_INDEX
数据量 1亿-10亿?
  ├── 高频写入 → BUCKET
  └── 低频写入 → BLOOM
数据量 < 1亿? → SIMPLE
```

**超大表方案示例:**
- 表规模: 100亿行，1000 分区
- 写入频率: 每分钟 100万条 upsert
- 推荐: `BUCKET Index (SIMPLE engine), numBuckets=64`
- 理由: O(1) 查找，无 I/O 开销，每个桶约 150万行（100亿/1000分区/64桶），文件大小约 200MB，合理

---

<a id="q-2-6"></a>
### 2.6 isImplicitWithStorage

**[红] 问题: Bucket Index 的 isImplicitWithStorage() 返回 true 意味着什么？对写入流程有何影响？**

**答案:**

`isImplicitWithStorage() = true` 意味着**索引隐含在存储结构中**，不需要显式的 `updateLocation()` 操作。

**影响:**
1. 写入流程中，`Index.updateLocation()` 变成 no-op
2. 记录的 FileGroup 由哈希函数直接确定，不需要查询任何外部索引
3. 节省了写入后更新索引的开销

**这也意味着:**
- 如果桶数改变（SIMPLE 引擎不支持），索引映射失效
- CONSISTENT_HASHING 通过 Clustering 重新分配桶时，需要特殊处理索引一致性

**源码:**
```java
// HoodieBucketIndex.java
@Override
public boolean isImplicitWithStorage() {
  return true;
}
```

---

## 三、事务与并发控制

<a id="q-3-1"></a>
### 3.1 Timeline 与 ACID

**[黄] 问题: Hudi 的 Timeline 机制如何保证 ACID 特性？**

**答案:**

- **原子性 (Atomicity)**: Instant 状态从 INFLIGHT → COMPLETED 是原子操作（文件系统的 rename 操作）。如果提交失败，INFLIGHT 文件会被后续的 Rollback 清理。

- **一致性 (Consistency)**: 只有 COMPLETED 状态的 instant 对读取可见。INFLIGHT 写入的文件不会被 FileSystemView 包含。

- **隔离性 (Isolation)**: 通过 Timeline 实现快照隔离。每个查询基于特定的 instant 构建 FileSystemView，不受并发写入影响。

- **持久性 (Durability)**: Commit metadata 持久化到文件系统（HDFS/S3）。Timeline 文件是持久的。

---

<a id="q-3-2"></a>
### 3.2 乐观并发控制

**[橙] 问题: 两个 Spark 作业同时写入同一个 Hudi 表时会发生什么？乐观并发控制如何工作？**

**答案:**

Hudi 使用乐观并发控制（OCC）处理多 Writer 场景：

```
Writer A:                    Writer B:
  write data                   write data
  acquire lock                 acquire lock (等待)
  check conflicts              
  commit (成功)                check conflicts
  release lock                   ├── 无冲突: commit (成功)
                                 └── 有冲突: abort + retry/fail
```

**冲突检测逻辑:**
- 检查 Writer A 和 Writer B 是否修改了相同 FileGroup 的文件
- 如果写入了相同的 FileGroup → 冲突
- 如果写入了不同的 FileGroup → 无冲突，可以并行提交

**Lock Provider 实现:**
- `ZookeeperBasedLockProvider`: 基于 ZooKeeper 的分布式锁
- `HiveMetastoreBasedLockProvider`: 基于 Hive Metastore 的锁
- `InProcessLockProvider`: 进程内锁（测试用）
- `DynamoDBBasedLockProvider`: 基于 AWS DynamoDB 的锁

---

<a id="q-3-3"></a>
### 3.3 Instant 状态转换

**[黄] 问题: HoodieInstant 的三种状态（REQUESTED/INFLIGHT/COMPLETED）的转换流程是什么？**

**答案:**

```
REQUESTED → INFLIGHT → COMPLETED (正常流程)
                ↓
              ROLLBACK (异常恢复)
```

以 commit 操作为例：
1. `REQUESTED`: `startCommit()` 创建 `<instant>.commit.request` 文件
2. `INFLIGHT`: 写入开始时创建 `<instant>.commit.inflight` 文件
3. `COMPLETED`: 写入成功后创建 `<instant>.commit` 文件

每次状态转换都对应文件系统中一个新文件的创建（或旧文件的重命名）。

---

<a id="q-3-4"></a>
### 3.4 Rollback 机制

**[橙] 问题: Hudi 的 Rollback 机制如何工作？INFLIGHT 状态的 commit 如何恢复？**

**答案:**

当 Writer 发现存在 INFLIGHT 状态的旧 instant 时，会触发 Rollback：

1. **检测 INFLIGHT instant**: 启动时检查是否有长时间未完成的 INFLIGHT instant
2. **读取 Write Markers**: 通过 Marker 文件确定 INFLIGHT 写入产生了哪些文件
3. **删除脏文件**: 删除 INFLIGHT 写入创建的所有数据文件
4. **清理 Timeline**: 删除 INFLIGHT instant 文件
5. **创建 Rollback instant**: 在 Timeline 上记录 Rollback 操作

**Marker 机制:**
```
每次写入文件时，先创建一个 Marker 文件:
.hoodie/.temp/<instant>/<partition>/<fileId>.marker.CREATE

Rollback 时根据 Marker 找到所有待清理的文件
```

---

<a id="q-3-5"></a>
### 3.5 Multi-Writer

**[红] 问题: Hudi 的 Multi-Writer 如何实现？Lock Provider 有哪些实现？**

**答案:**

Multi-Writer 通过 `TransactionManager` + `LockProvider` + `ConflictResolutionStrategy` 三层实现：

1. **TransactionManager**: 管理事务生命周期，协调锁获取/释放
2. **LockProvider**: 提供分布式锁能力
3. **ConflictResolutionStrategy**: 冲突时的解决策略

**Lock Provider 实现:**
| Provider | 后端 | 适用场景 |
|----------|------|---------|
| `ZookeeperBasedLockProvider` | ZooKeeper | 通用分布式场景 |
| `HiveMetastoreBasedLockProvider` | Hive Metastore | 已有 HMS 的环境 |
| `DynamoDBBasedLockProvider` | AWS DynamoDB | AWS 云环境 |
| `FileSystemBasedLockProvider` | HDFS/S3 | 简单场景 |

---

## 四、Compaction 与 Clustering

<a id="q-4-1"></a>
### 4.1 Compaction vs Clustering

**[绿] 问题: Compaction 和 Clustering 的区别是什么？**

**答案:**

| 维度 | Compaction | Clustering |
|------|-----------|-----------|
| **目标** | 合并 log files 到 base file | 重组文件布局（大小均匀化+排序） |
| **适用表类型** | 仅 MOR | COW + MOR |
| **操作范围** | 单个 FileSlice 内 | 跨多个 FileGroup |
| **Timeline Action** | `compaction` | `replacecommit` |
| **文件 ID 变化** | 不变（同一 FileGroup） | 变化（创建新 FileGroup） |
| **数据排序** | 保持原序 | 可重新排序 |

---

<a id="q-4-2"></a>
### 4.2 Compaction 两阶段设计

**[黄] 问题: Compaction 为什么分为 Schedule 和 Execute 两个阶段？这样设计有什么好处？**

**答案:**

**好处:**
1. **异构引擎协作**: Flink 写入产生 Compaction Plan，Spark 执行 Compaction
2. **异步执行**: 写入和 Compaction 解耦，不阻塞写入
3. **故障恢复**: Plan 持久化后，执行失败可以重试，不需要重新计划
4. **资源隔离**: Plan 和 Execute 可以在不同的集群/资源池执行

---

<a id="q-4-3"></a>
### 4.3 Clustering 排序策略

**[橙] 问题: Clustering 的排序策略有哪些？Z-Order 排序相比线性排序有什么优势？**

**答案:**

| 排序策略 | 描述 | 适用查询 |
|---------|------|---------|
| Linear Sort | 按指定列线性排序 | 单列范围查询 |
| Z-Order | 多列交错编码，保持多维局部性 | 多列组合查询 |
| Hilbert | Hilbert 空间填充曲线 | 多维查询（比 Z-Order 更优） |

**Z-Order 优势:**
```
线性排序 (ORDER BY city, ts):
  文件1: city=[Anhui..Fujian], ts=[整个范围]
  文件2: city=[Gansu..Hunan], ts=[整个范围]
  → WHERE city='Beijing' 高效，但 WHERE ts > X 需要扫描所有文件

Z-Order (Z-ORDER BY city, ts):
  文件1: city=[Anhui..Fujian], ts=[2024-01..2024-06]
  文件2: city=[Anhui..Fujian], ts=[2024-07..2024-12]
  → WHERE city='Beijing' AND ts > '2024-06' 可以跳过文件1
```

---

## 五、Spark 集成

<a id="q-5-1"></a>
### 5.1 写入调用链

**[黄] 问题: Spark 写入 Hudi 的完整调用链是什么？**

**答案:**

```
DataFrame.write.format("hudi").save(path)
  → DefaultSource.createRelation()
    → HoodieSparkSqlWriter.write()
      → SparkRDDWriteClient.upsert()
        → HoodieTable.upsert()
          → Step 1: Index.tagLocation()  — 标记每条记录的位置
          → Step 2: WorkloadProfile     — 统计 insert/update 分布
          → Step 3: Partitioner         — 分配到 FileGroup
          → Step 4: handleUpsertPartition()
             ├── UPDATE → HoodieMergeHandle (COW) / HoodieAppendHandle (MOR)
             └── INSERT → HoodieCreateHandle
          → Step 5: Index.updateLocation() — 更新索引
      → commit() → Timeline.saveAsComplete()
    → syncHive()  — 同步元数据到 Hive
```

---

<a id="q-5-2"></a>
### 5.2 MergeHandle vs AppendHandle

**[橙] 问题: COW 表的 HoodieMergeHandle 和 MOR 表的 HoodieAppendHandle 有什么核心区别？**

**答案:**

**HoodieMergeHandle (COW):**
```
1. 读取旧 Parquet base file
2. 在内存中维护 keyToNewRecords Map
3. 遍历旧文件每条记录:
   - 如果 key 在 Map 中 → 合并输出
   - 如果 key 不在 Map 中 → 原样输出
4. 输出 Map 中剩余的 INSERT 记录
5. 写入全新的 Parquet 文件
→ 写放大: 更新 1 行也要重写整个文件
```

**HoodieAppendHandle (MOR):**
```
1. 不读取旧文件
2. 直接将新记录序列化为 Avro LogBlock
3. 追加到 log file
→ 写放大: 极低，只写增量
→ 代价: 读取时需要合并 base + log
```

---

<a id="q-5-3"></a>
### 5.3 FileGroupReader 合并逻辑

**[橙] 问题: Spark 读取 MOR 表时，HoodieFileGroupReader 如何合并 base file 和 log files？**

**答案:**

```
HoodieFileGroupReader.read(fileSlice):
  1. 读取 base file → 构建 base records 迭代器
  2. HoodieMergedLogRecordScanner 扫描所有 log files
     → 将 log records 按 record key 存入 ExternalSpillableMap
     → 相同 key 的多条 log records 按顺序合并
  3. 遍历 base records:
     - 如果 log records Map 中有该 key:
       → RecordMerger.merge(baseRecord, logRecord)
       → 如果 logRecord 是 DELETE → 跳过
     - 如果没有: 直接输出 base record
  4. 输出 log records Map 中剩余的 INSERT 记录
```

**关键组件:**
- `ExternalSpillableMap`: 内存不足时自动溢写到磁盘
- `RecordMerger`: 可自定义的合并策略（默认按 ordering field 取最新）

---

<a id="q-5-4"></a>
### 5.4 三种查询类型

**[绿] 问题: Hudi 的三种查询类型分别适用什么场景？**

**答案:**

| 查询类型 | COW 表行为 | MOR 表行为 | 适用场景 |
|---------|-----------|-----------|---------|
| **snapshot** | 读最新 base files | 合并 base + log files | 需要最新数据 |
| **read_optimized** | = snapshot | 只读 base files (跳过 log) | 牺牲新鲜度换性能 |
| **incremental** | 读指定时间范围的变更 | 读变更 + log 合并 | 增量 ETL/CDC 消费 |

**MOR 表的 read_optimized 特点:**
- 只读 base files，不合并 log files
- 数据新鲜度取决于最近一次 Compaction 的时间
- 查询性能与 COW 表相当

---

<a id="q-5-5"></a>
### 5.5 多级数据裁剪

**[橙] 问题: HoodieFileIndex 如何实现多级数据裁剪？**

**答案:**

```
Level 1: 分区裁剪 (Partition Pruning)
  — WHERE dt = '2024-01-01' → 只扫描 dt=2024-01-01 分区
  — 剪枝粒度: 分区级别

Level 2: 列统计裁剪 (Column Stats / Data Skipping)
  — 利用 Metadata Table 中的 min/max 统计
  — WHERE city = 'Beijing' → 检查每个文件的 city.min/city.max
  — 跳过 min > 'Beijing' 或 max < 'Beijing' 的文件

Level 3: Bloom Filter 裁剪
  — 对 record key 的等值查询优化
  — WHERE id = '12345' → Bloom Filter 判断文件是否包含该 key

Level 4: Record Level Index
  — 精确定位: WHERE id = '12345' → 直接查 RLI 得到文件位置
```

---

## 六至十部分...（略）

> 注: 六至十部分的详细答案请参考对应的深度解析文档:
> - Flink 集成 → 《Flink流式写入Hudi完整流程分析.md》
> - 元数据管理 → 《Hudi元数据与Timeline深度解析.md》
> - Clean 与 Archival → 《Hudi_Compaction与Clustering深度解析.md》
> - Schema Evolution → 《Spark读写Hudi源码分析.md》
> - Hudi vs Iceberg → 对比部分参见各文档末尾

---

## 十、Hudi vs Iceberg vs Delta Lake

<a id="q-10-1"></a>
### 10.1 架构对比

**[橙] 问题: 从架构层面对比 Hudi、Iceberg 和 Delta Lake 的核心设计差异。**

**答案:**

| 维度 | Hudi | Iceberg | Delta Lake |
|------|------|---------|------------|
| **核心抽象** | Timeline + FileGroup | Snapshot + Manifest | Transaction Log |
| **更新模型** | Index + Tag + Write | Append + Delete Files | Append + Delete Vector |
| **表类型** | COW / MOR | 统一（类似 MOR） | 统一（类似 COW） |
| **索引机制** | 内置（Bloom/Bucket/RLI） | 无内置索引 | 无内置索引 |
| **元数据存储** | Timeline 文件 + Metadata Table | metadata.json + Manifest | _delta_log/ JSON/Parquet |
| **文件组织** | FileGroup（base + log 绑定）| 扁平文件 + Manifest | 扁平文件 + Log |
| **Schema Evolution** | InternalSchema（ID追踪） | Schema（ID追踪） | Schema（名称追踪） |
| **引擎耦合** | 较紧密（Spark/Flink Client） | 引擎无关 | 紧密依赖 Spark |

<a id="q-10-2"></a>
### 10.2 写入路径差异

**[橙] 问题: 三者在写入路径上的主要区别是什么？**

**答案:**

```
Hudi:  records → Index.tagLocation() → 区分 INSERT/UPDATE → 写入对应 FileGroup
       优势: 精确定位更新文件，写放大可控

Iceberg: records → 直接写新文件 → 如果有删除/更新，额外写 Delete Files
         优势: 写入简单，无需索引

Delta:  records → 直接写新文件 → 如果有删除/更新，重写受影响的文件
        优势: 实现简单，与 Spark 深度集成
```

Hudi 的 Index + Tag 模式在**高频 upsert** 场景有明显优势，因为它能精确定位需要更新的 FileGroup，避免全表扫描。

<a id="q-10-3"></a>
### 10.3 小文件处理

**[黄] 问题: 三者在文件管理策略上有什么不同？各自如何解决小文件问题？**

**答案:**

| 维度 | Hudi | Iceberg | Delta Lake |
|------|------|---------|------------|
| **小文件合并** | Compaction + Clustering | RewriteDataFiles Action | OPTIMIZE 命令 |
| **写入时控制** | 小文件限制策略（自动向小文件追加） | 无（依赖后续合并） | 自动优化 |
| **文件过期清理** | Clean 操作 | ExpireSnapshots | VACUUM 命令 |
| **元数据优化** | Metadata Table Compaction | RewriteManifests | Checkpoint |

Hudi 的**独特优势**: 写入时通过 `hoodie.parquet.small.file.limit` 配置，自动将新数据追加到已有的小文件中，从源头减少小文件产生。

---

## 十一、源码级深度问题

<a id="q-11-1"></a>
### 11.1 HoodieFileGroupReader 的合并读取流程

**[红] 问题: 解释 HoodieFileGroupReader 的合并读取流程，base file 和 log files 的合并是如何实现的？RecordMerger 在其中扮演什么角色？**

**答案:**

HoodieFileGroupReader 是 Hudi 统一的 FileGroup 读取器，所有引擎（Spark/Flink/Java）通过插入不同的 `HoodieReaderContext<T>` 实现来复用同一套合并逻辑。

**完整读取流程:**

```
HoodieFileGroupReader.getClosableIterator()
  → initRecordIterators()
     1. makeBaseFileIterator()          // 构建 base file 迭代器
     2. recordBufferLoader.getRecordBuffer()  // 扫描所有 log files，将记录存入 buffer
        → HoodieMergedLogRecordReader 扫描 log blocks
           → 每遇到 DataBlock → processDataBlock()
           → 每遇到 DeleteBlock → processDeleteBlock()
        → 记录存入 ExternalSpillableMap<Serializable, BufferedRecord<T>>
     3. recordBuffer.setBaseFileIterator(baseFileIterator)  // 关联 base 迭代器
  → 迭代输出:
     recordBuffer.hasNext() → doHasNext()
       ├── 遍历 base file 每条记录:
       │     → 从 records Map 中 remove(recordKey)
       │     → 如果 log 中有该 key:
       │         → bufferedRecordMerger.finalMerge(baseRecord, logRecord)
       │         → 如果结果是 DELETE → 跳过
       │     → 如果 log 中没有: 直接输出 base record (INSERT)
       └── base file 遍历完后:
             → hasNextLogRecord() 输出 records Map 中剩余记录 (log-only INSERT)
```

**两种 RecordBuffer 实现:**

| Buffer 类型 | 说明 | 索引键 |
|------------|------|-------|
| `KeyBasedFileGroupRecordBuffer` | 基于 record key 的合并 | String (record key) |
| `PositionBasedFileGroupRecordBuffer` | 基于行位置的合并（利用 Parquet row index） | Long (row position) |

Position-based 合并是 Hudi 的优化，当 base file 是 Parquet 格式且 log block 中记录了 `RECORD_POSITION` 时，可以避免解析 record key 进行匹配，性能更高。

**RecordMerger 的角色:**

RecordMerger 定义了两条记录如何合并，它贯穿整个读取流程的两个阶段：

1. **Delta Merge（log 内部合并）**: `BufferedRecordMerger.deltaMerge(newRecord, existingRecord)` — 同一 key 的多条 log record 按顺序合并
2. **Final Merge（base + log 合并）**: `BufferedRecordMerger.finalMerge(baseRecord, logRecord)` — base 记录与 log 合并结果进行最终合并

Hudi 支持三种内置 merge 策略：

| 策略 UUID | 合并逻辑 |
|----------|---------|
| `EVENT_TIME_BASED` | 按 ordering field（如 ts）取较大值 |
| `COMMIT_TIME_BASED` | 始终取较新的 commit 时间记录 |
| `PAYLOAD_BASED` | 委托给 HoodieRecordPayload 的遗留实现 |

HoodieRecordMerger 要求合并操作满足**结合律**: `f(a, f(b, c)) = f(f(a, b), c)`，这保证了无论 log block 的处理顺序如何，最终合并结果一致。

**源码证据:**
- `hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieFileGroupReader.java` — 主读取器
- `hudi-common/src/main/java/org/apache/hudi/common/table/read/buffer/FileGroupRecordBuffer.java` — 抽象 buffer 基类，`hasNextBaseRecord()` 实现 base+log 合并
- `hudi-common/src/main/java/org/apache/hudi/common/table/read/buffer/KeyBasedFileGroupRecordBuffer.java` — 基于 key 的合并实现，`doHasNext()` 方法
- `hudi-common/src/main/java/org/apache/hudi/common/table/read/buffer/PositionBasedFileGroupRecordBuffer.java` — 基于位置的合并实现
- `hudi-common/src/main/java/org/apache/hudi/common/table/read/BufferedRecordMerger.java` — 合并接口定义
- `hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordMerger.java` — RecordMerger 接口

---

<a id="q-11-2"></a>
### 11.2 Timeline V2 与 completionTime

**[红] 问题: Hudi 的 Timeline V2 相比 V1 解决了什么问题？从 HoodieInstant 的 completionTime 字段说起。**

**答案:**

**Timeline V1 的核心问题：requestedTime 无法反映真实完成顺序**

在 Timeline V1 中，HoodieInstant 只有 `requestedTime`（即 instant 开始时间），completed instant 的文件名格式为 `<requestedTime>.commit`。这导致一个根本性问题：

```
Writer A: requestedTime=100, 实际完成时间=300
Writer B: requestedTime=200, 实际完成时间=250

Timeline V1 排序: A(100) < B(200)
但实际完成顺序: B(250) < A(300)
```

这意味着当 Reader 在 t=260 读取时，如果按 V1 排序认为 A 已完成（因为 100 < 260），但实际上 A 直到 t=300 才完成，可能读到不一致的数据。

**Timeline V2 的解决方案：引入 completionTime**

V2 在 HoodieInstant 中增加了 `completionTime` 字段，completed instant 的文件名格式变为 `<requestedTime>_<completionTime>.commit`。

```java
// HoodieInstant.java
private final String requestedTime;    // instant 开始时间
private final String completionTime;   // instant 完成时间
```

**V2 的三个关键 Comparator:**

| Comparator | 排序依据 | 用途 |
|-----------|---------|------|
| `RequestedTimeBasedComparator` | requestedTime | 按发起顺序排列 |
| `CompletionTimeBasedComparator` | completionTime（优先）→ requestedTime（回退） | 按完成顺序排列 |
| `ActionComparator` | action 类型 | 同一时间点的不同 action 排序 |

**CompletionTimeBasedComparator 的核心逻辑:**
```java
// InstantComparators.CompletionTimeBasedComparator
public int compare(HoodieInstant instant1, HoodieInstant instant2) {
    if (instant1.getCompletionTime() == null && instant2.getCompletionTime() != null)
        return 1;  // 未完成的排在后面
    if (instant2.getCompletionTime() == null && instant1.getCompletionTime() != null)
        return -1;
    if (both null) return compareByRequestedTime();
    int res = instant1.getCompletionTime().compareTo(instant2.getCompletionTime());
    if (res == 0) return compareByRequestedTime();
    return res;
}
```

**对增量查询的重大改进:**

V1 的增量查询用 requestedTime 过滤，可能漏掉 "开始早、完成晚" 的 commit。V2 用 completionTime 过滤，保证了增量查询的完整性和正确性。

**Timeline Layout 版本对应关系:**

| 表版本 | Timeline Layout | Hudi 发布版本 |
|--------|----------------|-------------|
| SIX (6) | V1 | 0.14.0 |
| SEVEN (7) | V1 | 0.16.0 (桥接版本) |
| EIGHT (8) | V2 | 1.0.0 |
| NINE (9) | V2 | 1.1.0 |

**源码证据:**
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieInstant.java` — `completionTime` 字段定义
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/versioning/common/InstantComparators.java` — `CompletionTimeBasedComparator`
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/versioning/v2/InstantComparatorV2.java` — V2 comparator 注册
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/versioning/v2/InstantFileNameGeneratorV2.java` — V2 文件名格式 `requestedTime_completionTime`
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/TimelineLayout.java` — V1/V2 Layout 注册
- `hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTableVersion.java` — 表版本与 Timeline Layout 的映射

---

<a id="q-11-3"></a>
### 11.3 UpsertPartitioner 小文件填充算法

**[红] 问题: 解释 UpsertPartitioner 中小文件填充的完整算法。averageRecordSize 是如何估算的？为什么排除了 clustering 产生的 commit？**

**答案:**

**UpsertPartitioner 的小文件填充算法分为三步：**

**Step 1: 识别小文件**
```java
// UpsertPartitioner.getSmallFiles()
List<HoodieBaseFile> allFiles = table.getBaseFileOnlyView()
    .getLatestBaseFilesBeforeOrOn(partitionPath, latestCommitTime);
for (HoodieBaseFile file : allFiles) {
    if (file.getFileSize() < config.getParquetSmallFileLimit()) {
        smallFiles.add(sf);  // 默认阈值 104857600 (100MB)
    }
}
```

同时排除正处于 pending clustering 的文件组，因为 clustering 不支持 update 路径：
```java
smallFiles = filterSmallFilesInClustering(pendingClusteringFileGroupsId, smallFiles);
```

**Step 2: 将 INSERT 记录分配到小文件**
```java
for (SmallFile smallFile : smallFiles) {
    // 计算可以追加的记录数
    long recordsToAppend = Math.min(
        (config.getParquetMaxFileSize() - smallFile.sizeBytes) / averageRecordSize,
        totalUnassignedInserts);
    if (recordsToAppend > 0) {
        // 复用已有 update bucket 或创建新 bucket
        bucket = addUpdateBucket(partitionPath, smallFile.location.getFileId());
        totalUnassignedInserts -= recordsToAppend;
    }
}
```

**Step 3: 剩余 INSERT 创建新 bucket**
```java
if (totalUnassignedInserts > 0) {
    long insertRecordsPerBucket = config.shouldAutoTuneInsertSplits()
        ? (long) Math.ceil(config.getParquetMaxFileSize() / averageRecordSize)
        : config.getCopyOnWriteInsertSplitSize();
    int insertBuckets = (int) Math.ceil(totalUnassignedInserts / insertRecordsPerBucket);
    // 为每个新 bucket 创建新的 fileId
}
```

最后通过**累积权重 + 二分搜索**将 INSERT 记录按比例分配到各 bucket。

**averageRecordSize 的估算逻辑（AverageRecordSizeEstimator）:**

```java
// AverageRecordSizeEstimator.averageBytesPerRecord()
// 1. 从最近 N 个 commit 的 metadata 中获取统计
Iterator<HoodieInstant> instants = commitTimeline
    .filterCompletedInstants()
    .getReverseOrderedInstants()
    .filter(s -> RECORD_SIZE_ESTIMATE_ACTIONS.contains(s.getAction()))
    .limit(maxCommits);

// 2. 对于每个 commit，计算 totalBytesWritten / totalRecordsWritten
// 对 DELTA_COMMIT：只统计 base files（排除 log files）
if (instant.getAction().equals(DELTA_COMMIT_ACTION)) {
    commitMetadata.getWriteStats().stream()
        .filter(stat -> FSUtils.isBaseFile(new StoragePath(stat.getPath())))
        .forEach(stat -> {
            totalBytesWritten += stat.getTotalWriteBytes() - metadataSizeEstimate;
            totalRecordsWritten += stat.getNumWrites();
        });
}

// 3. 如果总字节数超过阈值，返回平均值
if (totalBytesWritten > commitSizeThreshold && totalRecordsWritten > 0) {
    return (long) Math.ceil(totalBytesWritten / totalRecordsWritten);
}
```

**为什么排除 clustering 产生的 commit？**

```java
// AverageRecordSizeEstimator 中的关键注释:
// "replacecommit can be created by clustering, which has smaller average record size,
//  which affects assigning inserts and may result in OOM"
private static final Set<String> RECORD_SIZE_ESTIMATE_ACTIONS =
    CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, COMPACTION_ACTION);
// 注意：不包含 REPLACE_COMMIT_ACTION
```

Clustering 会重新排序和重组数据，产生高度压缩的文件。这些文件的 `bytesWritten / recordsWritten` 比正常写入小得多（因为排序后列式压缩效率更高）。如果用 clustering 的统计来估算 averageRecordSize，会**低估**实际的记录大小，导致每个 bucket 分配过多记录，最终可能导致 Spark executor OOM。

**但在 UpsertPartitioner.assignInserts() 中，获取 timeline 时仍然包含了 REPLACE_COMMIT_ACTION:**
```java
// UpsertPartitioner.assignInserts() 第 177 行
long averageRecordSize = recordSizeEstimator.averageBytesPerRecord(
    table.getMetaClient().getActiveTimeline()
        .getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION))
        .filterCompletedInstants(), layout.getCommitMetadataSerDe());
```
这里传入了包含 REPLACE_COMMIT_ACTION 的 timeline，但 AverageRecordSizeEstimator 内部的 RECORD_SIZE_ESTIMATE_ACTIONS filter 会在遍历时排除它们。

**源码证据:**
- `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/UpsertPartitioner.java` — 小文件填充主逻辑
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/estimator/AverageRecordSizeEstimator.java` — averageRecordSize 估算实现
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/estimator/RecordSizeEstimator.java` — 估算器接口

---

<a id="q-11-4"></a>
### 11.4 Expression Index

**[红] 问题: Hudi 的 Expression Index 是什么？它解决了 Column Stats Index 的什么局限？举一个 date_format 的例子。**

**答案:**

**Column Stats Index 的局限:**

Column Stats Index 只能存储原始列的 min/max/null_count 统计。对于**查询条件涉及函数变换**的场景，Column Stats 无法帮助裁剪。例如：

```sql
-- 原始列是 timestamp 类型的 ts 字段
-- Column Stats 存储: ts.min=2024-01-01 00:00:00, ts.max=2024-12-31 23:59:59

-- 查询1: 按日期字符串过滤（对 ts 做了 date_format 变换）
SELECT * FROM table WHERE date_format(ts, 'yyyy-MM') = '2024-06'
-- Column Stats 无法利用 ts 的 min/max 来裁剪此查询
```

**Expression Index 的解决方案:**

Expression Index 允许用户对列的**函数变换结果**建立索引。它将变换后的值的统计信息（min/max 或 Bloom Filter）存储在 Metadata Table 的专门分区中。

**核心接口设计:**

```java
// HoodieExpressionIndex<S, T>
public interface HoodieExpressionIndex<S, T> {
    String getIndexName();          // 索引名称
    String getIndexFunction();      // 函数名（如 "date_format"）
    List<String> getOrderedSourceFields();  // 源列（如 ["ts"]）
    T apply(List<S> orderedSourceValues);   // 执行变换
}
```

Spark 实现 `HoodieSparkExpressionIndex` 将函数名映射为 Spark 内置函数：

```java
// ExpressionIndexSparkFunctions 中注册的函数
SPARK_FUNCTION_MAP.put("date_format", new SparkDateFormatFunction(){});
SPARK_FUNCTION_MAP.put("year", new SparkYearFunction(){});
SPARK_FUNCTION_MAP.put("month", new SparkMonthFunction(){});
SPARK_FUNCTION_MAP.put("day", new SparkDayFunction(){});
SPARK_FUNCTION_MAP.put("hour", new SparkHourFunction(){});
SPARK_FUNCTION_MAP.put("upper", new SparkUpperFunction(){});
SPARK_FUNCTION_MAP.put("lower", new SparkLowerFunction(){});
SPARK_FUNCTION_MAP.put("substring", new SparkSubstringFunction(){});
// ... 共 20+ 种内置函数
```

**date_format 的完整例子:**

```sql
-- 创建 Expression Index
CREATE INDEX idx_ts_month ON table USING column_stats (ts)
OPTIONS (expr='date_format', format='yyyy-MM');

-- 索引存储的内容（在 Metadata Table 的 expr_index_idx_ts_month 分区中）:
-- 文件1: date_format(ts,'yyyy-MM').min = '2024-01', .max = '2024-03'
-- 文件2: date_format(ts,'yyyy-MM').min = '2024-04', .max = '2024-06'
-- 文件3: date_format(ts,'yyyy-MM').min = '2024-07', .max = '2024-09'

-- 查询时的 Data Skipping:
SELECT * FROM table WHERE date_format(ts, 'yyyy-MM') = '2024-06'
-- 只需读取文件2，跳过文件1和文件3
```

**Expression Index 的类型:**

Expression Index 在 MetadataPartitionType 中以 `expr_index_` 为前缀注册：
```java
EXPRESSION_INDEX(PARTITION_NAME_EXPRESSION_INDEX, "expr-index-", 0) {
    // 每个 Expression Index 对应 Metadata Table 中的一个独立分区
}
```

支持两种索引类型：
1. **column_stats** 类型：存储变换后的 min/max 统计
2. **bloom_filters** 类型：存储变换后的值的 Bloom Filter

**源码证据:**
- `hudi-common/src/main/java/org/apache/hudi/index/expression/HoodieExpressionIndex.java` — 表达式索引接口
- `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/index/expression/HoodieSparkExpressionIndex.java` — Spark 实现
- `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/index/expression/ExpressionIndexSparkFunctions.java` — Spark 函数映射（20+ 种内置函数）
- `hudi-common/src/main/java/org/apache/hudi/metadata/MetadataPartitionType.java` — `EXPRESSION_INDEX` 分区类型
- `hudi-common/src/main/java/org/apache/hudi/common/model/HoodieIndexDefinition.java` — 索引定义模型

---

<a id="q-11-5"></a>
### 11.5 空间填充曲线排序策略

**[橙] 问题: Hudi 支持哪些空间填充曲线排序策略？Z-Order 和 Hilbert 曲线的本质区别是什么？为什么 Hilbert 通常更好？**

**答案:**

**Hudi 支持三种布局优化策略:**

```java
// HoodieClusteringConfig.LayoutOptimizationStrategy
public enum LayoutOptimizationStrategy {
    LINEAR,   // 线性排序（ORDER BY col1, col2, ...）
    ZORDER,   // Z-Order 空间填充曲线
    HILBERT   // Hilbert 空间填充曲线
}
```

**Z-Order 曲线实现原理:**

Z-Order（也称 Morton Code）通过**交错（interleaving）各维度的比特位**来产生一维排序键：

```java
// SpaceCurveSortingHelper.createZCurveSortedRDD()
byte[][] zBytes = fieldMap.entrySet().stream()
    .map(entry -> mapColumnValueTo8Bytes(row, index, field.dataType()))
    .toArray(byte[][]::new);
// 交错比特位
byte[] zOrdinalBytes = BinaryUtil.interleaving(zBytes, 8);
```

例如两个 3-bit 坐标 (x=5=101, y=3=011)：
```
x bits: 1 0 1
y bits: 0 1 1
Z-order: 10 01 11 = 100111 (interleaving)
```

**Hilbert 曲线实现原理:**

Hilbert 曲线将 N 维坐标映射到一维位置，使用 `davidmoten/hilbert-curve` 库：

```java
// SpaceCurveSortingHelper.createHilbertSortedRDD()
HilbertCurve hilbertCurve = HilbertCurve.bits(63).dimensions(fieldMap.size());
long[] longs = fieldMap.entrySet().stream()
    .mapToLong(entry -> mapColumnValueToLong(row, index, field.dataType()))
    .toArray();
byte[] hilbertCurvePosBytes = HilbertCurveUtils.indexBytes(hilbertCurve, longs, 63);
```

**Z-Order vs Hilbert 的本质区别:**

| 维度 | Z-Order | Hilbert |
|------|---------|---------|
| **映射方式** | 比特交错（简单位运算） | 递归子区间映射（复杂算法） |
| **连续性** | 不连续 — 存在"跳跃" | 连续 — 相邻点始终相邻 |
| **聚簇质量** | 较好 | 更好 |
| **计算开销** | 低（简单位运算） | 较高（需要库计算） |
| **实现** | 自研 `BinaryUtil.interleaving()` | 第三方库 `org.davidmoten.hilbert` |

**为什么 Hilbert 通常更好？**

Z-Order 的核心缺陷在于**跳跃不连续性**。在 Z 曲线上，两个在多维空间中相邻的点，映射到一维后可能相距很远。这意味着排序后的文件中，本应聚集在一起的数据被分散到不同文件，降低了 Data Skipping 的效率。

```
Z-Order 的跳跃示例（2D, 2-bit）:
  0──1  6──7       Z 曲线在 (1,0)→(2,0) 处有大跳跃
  │  │  │  │       位置 3 到 4 在一维上连续
  3──2  5──4       但在二维空间中跨越了较大距离
  │           │
  C──D  A──B       Hilbert 曲线没有这种跳跃
  │  │  │  │       每一步都移动到相邻的格子
  F──E  9──8
```

Hilbert 曲线保证了：**一维上相邻的点在多维空间中也相邻**（具有最优的局部性保持特性）。这意味着：

1. 文件内的数据在多维空间中聚集度更高
2. 每个文件的 column stats (min/max) 范围更窄
3. Data Skipping 跳过更多不相关文件
4. 多列组合查询效率显著提升

**单列场景的退化:**

当只有一个排序列时，`SpaceCurveSortingHelper` 会自动退化为线性排序，跳过空间曲线计算：
```java
if (orderByCols.size() == 1) {
    return df.repartitionByRange(targetPartitionCount, new Column(orderByColName));
}
```

**源码证据:**
- `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/sort/SpaceCurveSortingHelper.java` — 统一入口，Z-Order/Hilbert 分发
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/optimize/HilbertCurveUtils.java` — Hilbert 工具类
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieClusteringConfig.java` — `LayoutOptimizationStrategy` 枚举

---

## 十二、架构对比深度问题

<a id="q-12-1"></a>
### 12.1 Metadata Table 为什么选择 MOR 表

**[橙] 问题: Hudi 的 Metadata Table 为什么选择用 MOR 表实现？相比 Iceberg 的 Manifest File 有什么优劣？**

**答案:**

**Metadata Table 选择 MOR 的原因:**

Metadata Table 本身就是一个 Hudi MOR 表，存储在 `<table_path>/.hoodie/metadata/` 路径下。选择 MOR 的核心原因：

1. **高频写入需求**: 每次主表 commit 都需要更新 Metadata Table。MOR 的 append-only log 写入延迟极低（O(1)），而 COW 需要重写整个 base file（O(N)）。

2. **自我管理的 Compaction**: Metadata Table 内置了自己的 Compaction 调度（`hoodie.metadata.compact.max.delta.commits`），log files 积累到一定数量后自动合并，在写入效率和读取效率之间取得平衡。

3. **多分区设计**: Metadata Table 包含多个分区，各分区独立管理：

| 分区 | 内容 | 记录数量级 |
|------|------|----------|
| `files` | 文件列表 | 分区数 x 文件数 |
| `column_stats` | 列统计 | 文件数 x 列数 |
| `bloom_filters` | Bloom Filter | 文件数 |
| `record_index` | Record Key → FileGroup 映射 | 记录数（可能数十亿） |
| `expr_index_*` | Expression Index 数据 | 文件数 |
| `secondary_index_*` | 二级索引数据 | 变化 |
| `partition_stats` | 分区级统计 | 分区数 |

4. **事务一致性**: MOR 表的 Timeline 机制保证 Metadata Table 与主表的事务原子性 — 主表 commit 和 Metadata Table 更新在同一事务中完成。

**对比 Iceberg 的 Manifest File:**

| 维度 | Hudi Metadata Table | Iceberg Manifest File |
|------|--------------------|--------------------|
| **存储格式** | MOR 表（base + log） | Avro Manifest 文件 |
| **更新方式** | 追加 log block | 重写整个 Manifest |
| **索引能力** | 多分区索引（Bloom, Column Stats, RLI, Expression Index） | Manifest 内 partition summary |
| **写入开销** | 低（append log） | 中（rewrite manifest） |
| **读取开销** | 需要 Compaction 维护 | 直接读取 |
| **扩展性** | 通过 MOR Compaction 和分片扩展 | 通过 Manifest List 扩展 |
| **独立查询** | 可以独立查询 Metadata Table | Manifest 不可独立查询 |
| **维护成本** | 需要 Compaction 调度 | 需要 Manifest Rewrite |

**Hudi Metadata Table 的优势:**
- 统一的索引存储，支持更丰富的索引类型
- 增量更新，写入开销更低
- 支持 Record Level Index（Iceberg 没有类似能力）
- 可以独立查询和诊断

**Hudi Metadata Table 的劣势:**
- 额外的 Compaction 维护成本
- Metadata Table 本身可能出现一致性问题（需要专门的恢复机制）
- 增加了系统复杂度

**源码证据:**
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/metadata/HoodieBackedTableMetadataWriter.java` — Metadata Table 写入器
- `hudi-common/src/main/java/org/apache/hudi/metadata/MetadataPartitionType.java` — 分区类型定义
- `hudi-common/src/main/java/org/apache/hudi/metadata/HoodieMetadataPayload.java` — Metadata Table 的 payload 实现

---

<a id="q-12-2"></a>
### 12.2 Bucket Index vs Iceberg 分区裁剪

**[橙] 问题: 对比 Hudi 的 Bucket Index 和 Iceberg 的分区裁剪，在大规模 upsert 场景下谁更高效？为什么？**

**答案:**

**核心区别: 写入路径 vs 读取路径优化**

Hudi 的 Bucket Index 主要是一个**写入路径**优化，通过哈希函数将 record key 直接映射到固定的 FileGroup，使得 upsert 操作无需任何 I/O 就能确定目标文件。

Iceberg 的分区裁剪主要是一个**读取路径**优化，通过 Manifest 中的 partition summary 在查询时跳过不相关的分区。

**大规模 upsert 场景对比:**

| 维度 | Hudi Bucket Index | Iceberg 分区裁剪 |
|------|-------------------|-----------------|
| **定位复杂度** | O(1) 哈希计算 | 需要引擎 JOIN/MERGE |
| **是否需要索引 I/O** | 不需要（`isImplicitWithStorage()=true`） | 不需要 |
| **定位准确性** | 精确到 FileGroup | 精确到分区 |
| **分区内定位** | 确定（哈希直达） | 不确定（需扫描分区内所有文件） |
| **写入流程** | `hash(key) % numBuckets → fileId` | 写入新文件 → 生成 Delete File |

**Hudi Bucket Index 在 upsert 场景下更高效的原因:**

1. **零 I/O 索引查找**: Bucket Index 通过 `BucketIdentifier.getBucketId(recordKey, numBuckets)` 直接计算出目标 bucket，不读取任何索引数据。

2. **精确的 FileGroup 定位**: 每个 bucket 对应一个固定的 FileGroup，update 操作直接追加到对应的 log file（MOR）或合并到对应的 base file（COW）。

3. **无需全表扫描**: Iceberg 的 upsert 通常需要引擎层面做 JOIN（如 MERGE INTO），至少需要读取 Delete Files 或全分区扫描来确定已有记录的位置。

4. **避免 Delete Files 膨胀**: Iceberg 的 upsert 会产生 Delete Files（Position Delete 或 Equality Delete），随着 upsert 频率增加，Delete Files 累积，读取时合并成本增加。而 Hudi Bucket Index 将更新直接路由到正确的 FileGroup，不产生额外的删除标记文件。

**Iceberg 分区裁剪的优势场景:**

1. **append-only 场景**: 如果只有 INSERT（无 UPDATE），Iceberg 不需要索引，直接写新文件更简单
2. **查询密集场景**: Iceberg 的 Manifest + Partition Summary 在点查和范围查询时可以高效裁剪
3. **Schema 灵活性**: Iceberg 支持 Hidden Partitioning（如按 `day(ts)` 分区），查询时无需感知分区字段

**结论**: 在大规模 upsert 场景下（如 CDC 同步、实时数仓），Hudi Bucket Index 显著更高效。在 append-heavy、查询密集的分析场景中，两者差距不大。

**源码证据:**
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/bucket/HoodieBucketIndex.java` — Bucket Index 实现
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/bucket/BucketIdentifier.java` — 哈希计算

---

<a id="q-12-3"></a>
### 12.3 三者的并发控制策略对比

**[红] 问题: Hudi、Iceberg、Delta Lake 三者的并发控制策略有何不同？各自的 OCC 实现有什么差异？**

**答案:**

**三者并发控制架构对比:**

| 维度 | Hudi | Iceberg | Delta Lake |
|------|------|---------|------------|
| **并发模型** | OCC + Lock Provider | OCC（基于 Snapshot） | OCC + WAL |
| **锁机制** | 外部锁（ZooKeeper/DynamoDB/HMS） | 无外部锁（CAS on metadata.json） | 无外部锁（rename 语义） |
| **冲突粒度** | FileGroup 级别 | File 级别 | File 级别 |
| **冲突检测时机** | Commit 前获取锁 | Commit 时 CAS 竞争 | Commit 时 rename 竞争 |
| **多 Writer** | 原生支持（Lock Provider） | 原生支持（快照隔离） | 原生支持 |

**Hudi 的 OCC 实现:**

```
Writer A:                        Writer B:
  写数据                           写数据
  获取锁 (LockProvider.lock())     等待锁
  检查冲突:                        
    → getCandidateInstants()       
    → 对比 FileGroup 集合           
  提交 (成功)                       获取锁
  释放锁                           检查冲突
                                     ├── 无 FileGroup 交集 → 提交成功
                                     └── 有 FileGroup 交集 → 抛出 HoodieWriteConflictException
```

Hudi 的核心冲突检测逻辑在 `SimpleConcurrentFileWritesConflictResolutionStrategy` 中。对于表版本 8+，使用 completionTime 来获取候选冲突 instant：

```java
// SimpleConcurrentFileWritesConflictResolutionStrategy
// V8+ 使用 completionTime 过滤
private Stream<HoodieInstant> getCandidateInstantsV8AndAbove(metaClient, currentInstant, lastSuccessfulInstant) {
    // 获取自上次成功写入以来所有已完成的 commit
    Stream<HoodieInstant> completedCommitsInstantStream = activeTimeline
        .getCommitsTimeline()
        .filterCompletedInstants()...
}
```

**Iceberg 的 OCC 实现:**

Iceberg 基于快照的乐观并发：
```
Writer A: 基于 Snapshot S0 写入 → 尝试 CAS(metadata.json, S0→S1)
Writer B: 基于 Snapshot S0 写入 → 尝试 CAS(metadata.json, S0→S2)
  ├── A 先完成: CAS 成功 (S0→S1)
  └── B 后完成: CAS 失败 → 基于 S1 重试冲突检测
       ├── 不冲突: commit S1→S2
       └── 冲突: 失败
```

Iceberg 不需要外部锁，依赖底层存储的原子操作（如 S3 条件写入、HDFS rename）来实现 CAS。冲突检测基于文件集合的交集判断。

**Delta Lake 的 OCC 实现:**

Delta Lake 使用 WAL (Write-Ahead Log) 模式：
```
_delta_log/
  00000000000000000001.json  ← 版本1
  00000000000000000002.json  ← 版本2
```

Writer 通过原子 rename（`_commit_temp → 00000000000000000003.json`）来竞争。冲突检测检查 action 级别的兼容性。

**关键差异总结:**

1. **Hudi 依赖外部锁**: 更显式、更可控，但增加了运维复杂度（需要部署 ZooKeeper 或 DynamoDB）
2. **Iceberg 无锁 CAS**: 更轻量，但依赖存储层的原子语义（部分对象存储不支持）
3. **Delta Lake rename 语义**: 最简单，但 rename 在某些存储上不是原子的（如 S3）
4. **冲突粒度**: Hudi 的 FileGroup 级别冲突检测比 Iceberg/Delta 的 file 级别更精细，减少了虚假冲突

**源码证据:**
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/SimpleConcurrentFileWritesConflictResolutionStrategy.java` — 冲突检测策略
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/TransactionManager.java` — 事务管理器
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/lock/` — 各 LockProvider 实现

---

## 十三、运维实战问题

<a id="q-13-1"></a>
### 13.1 Compaction 积压排查

**[橙] 问题: 生产环境中 Compaction 积压了 50 个 pending plan，你会如何排查和解决？**

**答案:**

**第一步: 确认现状**

```bash
# 查看 pending compaction 数量
hudi-cli> compactions show all

# 或通过 Timeline 文件直接检查
ls .hoodie/*.compaction.requested
ls .hoodie/*.compaction.inflight
```

检查 pending compaction 的 instant 时间分布，确认是某段时间集中产生还是持续积累。

**第二步: 排查根因**

常见原因及对应检查：

| 原因 | 检查方式 | 典型表现 |
|------|---------|---------|
| **Compaction 执行失败** | 检查 inflight 数量和错误日志 | 有大量 inflight 文件 |
| **资源不足** | 检查 executor 内存/CPU 使用 | OOM 或 task 超时 |
| **调度间隔太短** | 检查 `hoodie.compact.inline.max.delta.commits` | 每 N 个 commit 就产生一个 plan |
| **执行速度跟不上** | 对比 plan 产生速度和执行速度 | plan 持续增长 |
| **Compaction 策略不当** | 检查 `hoodie.compaction.strategy` | 每次选择太多 FileSlice |
| **数据倾斜** | 检查各分区的 log 文件数量 | 个别分区文件极多 |

**第三步: 解决方案**

**方案1: 紧急消化积压**

```bash
# 使用独立 Spark 作业执行 pending compaction
spark-submit --class org.apache.hudi.utilities.HoodieCompactor \
  --master yarn --executor-memory 8g --num-executors 20 \
  hudi-utilities-bundle.jar \
  --base-path /path/to/table \
  --table-name my_table \
  --instant-time <specific_instant>  # 指定执行某个 plan

# 或批量执行
# CALL run_compaction(op => 'execute', path => '/path/to/table')
```

**方案2: 调整 Compaction 策略**

```properties
# 增大触发间隔，减少 plan 产生频率
hoodie.compact.inline.max.delta.commits=10

# 使用 BoundedIOCompactionStrategy 限制每次 Compaction 的 I/O
hoodie.compaction.strategy=org.apache.hudi.table.action.compact.strategy.BoundedIOCompactionStrategy
hoodie.compaction.target.io=512000  # 500GB

# 增大 Compaction 并行度
hoodie.compaction.strategy.target.partitions=10
```

**方案3: 异步 Compaction（推荐生产方案）**

```properties
# 关闭内联 Compaction
hoodie.compact.inline=false

# 启动独立的异步 Compaction 服务
hoodie.compact.schedule.inline=true   # 仍然内联调度 plan
# 由独立的 Spark/Flink 作业定期执行 plan
```

**方案4: 清理无法执行的 Compaction Plan**

如果某些 plan 因为数据已被清理而无法执行：
```bash
# Rollback 无法执行的 inflight compaction
hudi-cli> compaction rollback --instant <instant_time>
```

**第四步: 持续监控**

关键监控指标：
- `hoodie.compaction.pending.count` — pending plan 数量
- `hoodie.compaction.execution.time` — 单次执行时间
- `hoodie.log.files.per.fileslice` — 每个 FileSlice 的 log 文件数

**源码证据:**
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/ScheduleCompactionActionExecutor.java` — Compaction 调度
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/strategy/` — Compaction 策略目录
- `hudi-utilities/src/main/java/org/apache/hudi/utilities/HoodieCompactor.java` — 独立 Compactor 工具

---

<a id="q-13-2"></a>
### 13.2 MOR 表 Snapshot 查询突然变慢

**[橙] 问题: 一个 MOR 表的 Snapshot 查询突然变慢了 10 倍，你的排查思路是什么？**

**答案:**

**排查框架: 从宏观到微观，从数据到引擎**

**第一层: 确认 Compaction 状态**

MOR 表 Snapshot 查询变慢的最常见原因是 **Compaction 积压导致 log 文件堆积**。

```sql
-- 检查每个 FileSlice 的 log 文件数量
SELECT partition_path, file_id, count(log_files) as log_count
FROM table_metadata
GROUP BY partition_path, file_id
ORDER BY log_count DESC;
```

正常情况下，每个 FileSlice 应该只有少量 log files（<5）。如果发现大量 FileSlice 有 20+ 个 log files，说明 Compaction 严重滞后。

**原理**: Snapshot 查询需要执行 `base file + all log files` 的合并读取。log 文件越多，需要扫描和合并的数据量越大。FileGroupRecordBuffer 需要将所有 log records 加载到 ExternalSpillableMap 中，内存压力增大，甚至触发磁盘溢写。

**第二层: 检查数据量变化**

```bash
# 对比变慢前后的数据量
# 检查分区文件数量
hadoop fs -count /path/to/table/partition=xxx/

# 检查最近 commit 写入的数据量
hudi-cli> commits show
```

可能原因：
- 上游数据量突增（如全量同步事件）
- 新增大量分区导致文件列表膨胀

**第三层: 检查 Data Skipping 效果**

```sql
-- 查看查询计划中的 file pruning 效果
EXPLAIN SELECT * FROM table WHERE condition;
```

如果 Column Stats Index 不准确或未启用，查询可能需要读取大量不必要的文件。

可能的退化原因：
- Metadata Table 一致性问题（需要重建）
- 查询条件变化导致无法利用现有索引
- Clustering 后 Column Stats 未及时更新

**第四层: 检查查询引擎层面**

```
# Spark UI 检查:
# 1. Task 数量是否异常增多
# 2. Shuffle 数据量是否异常
# 3. 是否有大量 GC 时间
# 4. 是否有 disk spill
```

关键检查项：
- `ExternalSpillableMap` 是否溢写到磁盘（`hoodie.memory.merge.max.size` 配置）
- 是否有大量小文件导致 Task 数量爆炸
- Schema Evolution 导致的全量列读取

**第五层: 特殊场景排查**

1. **Schema 变更**: 如果最近做了 Schema Evolution（加列/改类型），FileGroupReaderSchemaHandler 需要对齐新旧 Schema，增加开销
2. **Bootstrap 表**: 如果是 Bootstrap 表，需要同时读取 skeleton file 和 data file，IO 翻倍
3. **Partition 数据倾斜**: 查询涉及的某个分区数据量远超其他分区

**快速修复方案:**

```sql
-- 1. 立即执行 Compaction 消除 log 积压
CALL run_compaction(op => 'scheduleandexecute', path => '/path/to/table');

-- 2. 如果问题在列统计，重建 Metadata Table
-- (需要先禁用再重新启用 metadata table)

-- 3. 临时切换为 read_optimized 查询（牺牲实时性换取性能）
SET hoodie.datasource.query.type=read_optimized;
```

**源码证据:**
- `hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieFileGroupReader.java` — 合并读取主流程
- `hudi-common/src/main/java/org/apache/hudi/common/table/read/buffer/FileGroupRecordBuffer.java` — ExternalSpillableMap 使用
- `hudi-common/src/main/java/org/apache/hudi/common/table/read/FileGroupReaderSchemaHandler.java` — Schema 对齐逻辑

---

<a id="q-13-3"></a>
### 13.3 表版本升级（7 到 9）

**[红] 问题: 如何安全地将一个运行中的 Hudi 表从表版本 7 升级到版本 9？**

**答案:**

**背景知识: 表版本体系**

```java
// HoodieTableVersion.java
SEVEN(7, "0.16.0", LAYOUT_VERSION_1),  // 0.16.0 桥接版本
EIGHT(8, "1.0.0",  LAYOUT_VERSION_2),  // 1.0 正式版
NINE(9,  "1.1.0",  LAYOUT_VERSION_2);  // 1.1 当前最新
```

从 7 升级到 9 需要经历两步：`7 → 8 → 9`，每步有专门的 UpgradeHandler。

**升级前准备（关键步骤）:**

1. **停止所有写入作业**: 确保没有 INFLIGHT 的 commit

2. **完成所有 pending 操作**:
```bash
# 确保没有 pending compaction 和 clustering
hudi-cli> compactions show all
hudi-cli> clustering show all
```

3. **备份 hoodie.properties**:
```bash
hadoop fs -cp /path/to/table/.hoodie/hoodie.properties /backup/
```

**升级执行（自动触发）:**

Hudi 的升级是自动的 — 当使用新版本 Hudi（1.1.0+）首次写入旧版本表时，`UpgradeDowngrade.run()` 会自动检测并执行升级：

```java
// UpgradeDowngrade.run()
while (fromVersion.versionCode() < toVersion.versionCode()) {
    HoodieTableVersion nextVersion = fromVersionCode(fromVersion.versionCode() + 1);
    upgrade(fromVersion, nextVersion, instantTime);
    fromVersion = nextVersion;
}
```

**Step 1: SevenToEightUpgradeHandler (7 → 8)**

这是最关键的一步，涉及 Timeline 格式从 V1 到 V2 的迁移：

1. **前置操作**: 自动 rollback 所有 INFLIGHT instant 并执行 pending compaction
   ```java
   UPGRADE_HANDLERS_REQUIRING_ROLLBACK_AND_COMPACT = {
       Pair.of(7, 8),  // 7→8 需要先 rollback + compact
       Pair.of(8, 9)   // 8→9 也需要
   };
   ```

2. **创建 V2 Timeline 目录结构**:
   ```java
   HoodieTableMetaClient.createTableLayoutOnStorage(
       context.getStorageConf(), new StoragePath(config.getBasePath()),
       config.getProps(), TimelineLayoutVersion.VERSION_2, false);
   ```

3. **重写 Active Timeline**: 将所有 V1 格式的 instant 文件重写为 V2 格式（文件名加入 completionTime）

4. **转换归档 Timeline**: 将旧的归档 Timeline 转换为 LSM Timeline 格式
   ```java
   upgradeToLSMTimeline(table, context, config);
   ```

5. **升级表属性**:
   - 设置 `hoodie.timeline.path` 
   - 升级 partition fields 格式
   - 设置 merge mode（根据 payload 类推断）
   - 设置 key generator type
   - 设置 initial version

**Step 2: EightToNineUpgradeHandler (8 → 9)**

主要处理 merge mode 和 payload 类的规范化：

1. **Payload 类迁移**: 将旧的 payload 类（如 `DefaultHoodieRecordPayload`）迁移到新的 merge mode 体系
   ```java
   // EventTimeAvroPayload → EVENT_TIME_ORDERING merge mode
   // OverwriteWithLatestAvroPayload → COMMIT_TIME_ORDERING merge mode
   // PartialUpdateAvroPayload → EVENT_TIME_ORDERING + PARTIAL_UPDATE_MODE
   ```

2. **移除 `hoodie.compaction.payload.class`**: 转为 `hoodie.legacy.payload.class`

3. **设置 `hoodie.table.partial.update.mode`**: 根据原 payload 类推断

4. **设置 merge properties**: 如 `hoodie.record.merge.property.deleteKey` 等

5. **升级 ordering fields**: 将 `hoodie.datasource.write.precombine.field` 转为 `hoodie.table.ordering.fields`

6. **补全索引版本信息**: 为缺少 version 的索引定义补充默认版本

**安全升级的最佳实践:**

```bash
# 1. 停止写入
# 2. 确保无 pending 操作
# 3. 备份

# 4. 设置写入版本目标
spark.conf.set("hoodie.write.table.version", "9")
# 或让 Hudi 自动升级（默认行为）
spark.conf.set("hoodie.write.auto.upgrade", "true")  # 默认 true

# 5. 执行一次写入触发升级
df.write.format("hudi").options(...).save("/path/to/table")

# 6. 验证升级结果
# 检查 hoodie.properties 中的 hoodie.table.version=9
hadoop fs -cat /path/to/table/.hoodie/hoodie.properties | grep table.version

# 7. 恢复所有写入作业（使用新版本 Hudi）
```

**注意事项:**

1. **不可跳版本降级**: 从 1.x 只能降级到版本 6+，不能降级到更低版本
2. **Metadata Table 同步升级**: UpgradeDowngrade 会自动递归升级 Metadata Table
3. **不可回退性**: V1→V2 Timeline 迁移后，旧版本 Hudi 无法读取（除非显式降级）
4. **如果自动升级失败**: 设置 `hoodie.write.auto.upgrade=false` 可以跳过升级，保持旧版本运行

**源码证据:**
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/upgrade/UpgradeDowngrade.java` — 升级降级主流程
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/upgrade/SevenToEightUpgradeHandler.java` — 7→8 升级处理
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/upgrade/EightToNineUpgradeHandler.java` — 8→9 升级处理
- `hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTableVersion.java` — 表版本定义

---

## 面试技巧总结

### 高分答题要点

1. **源码意识**: 回答时提到关键类名和源码路径，展示深度
2. **对比思维**: 主动对比 Hudi/Iceberg/Delta Lake 的设计选择
3. **场景导向**: 结合实际场景说明设计决策的合理性
4. **权衡分析**: 每种设计都有 trade-off，展示对权衡的理解

### 核心记忆点

- Hudi = **Timeline + FileGroup + Index**
- Iceberg = **Snapshot + Manifest + Spec**
- Delta Lake = **Transaction Log + Checkpoint**

---

**文档版本**: v2.0
**最后更新**: 2026-04-15
