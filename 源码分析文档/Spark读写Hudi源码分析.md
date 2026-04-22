# Spark 读写 Hudi 源码分析

> 基于 Apache Hudi 源码深度分析（已纠错 + 扩展 + 深度剖析）
> 文档版本：4.1
> 源码版本：v1.2.0-SNAPSHOT (master)
> 最后更新：2026-04-22

---

## 目录

1. [概述与架构总览](#1-概述与架构总览)
2. [Spark 写入 Hudi 源码分析](#2-spark-写入-hudi-源码分析)
3. [Spark 读取 Hudi 源码分析](#3-spark-读取-hudi-源码分析)
4. [COW 与 MOR 的读写差异](#4-cow-与-mor-的读写差异)
5. [Spark SQL 扩展与 Catalog 集成](#5-spark-sql-扩展与-catalog-集成)
6. [Schema Evolution 支持](#6-schema-evolution-支持)
7. [Spark Structured Streaming 集成](#7-spark-structured-streaming-集成)
8. [关键配置参数](#8-关键配置参数)
9. [性能优化深度解析](#9-性能优化深度解析)
10. [Spark 读写源码深度剖析](#10-spark-读写源码深度剖析)

---

## 1. 概述与架构总览

### 1.0 前置理解

#### 1.0.1 解决什么问题

Hudi Spark 集成架构要解决的核心问题是：**如何让 Spark 用户以最小的学习成本使用 Hudi 的增量更新能力，同时保持与 Spark 生态的深度兼容**。

**核心业务问题**：
1. **传统数据湖的更新困境**：在 Hudi 出现之前，数据湖（如 HDFS 上的 Parquet 文件）只支持追加写入，无法高效地进行记录级别的更新和删除。要更新一条记录，必须重写整个分区，代价极高。
2. **流批一体的需求**：业务需要同时支持批处理（每天 ETL）和流式处理（实时 CDC），但传统架构需要维护两套系统。
3. **查询性能与写入延迟的权衡**：纯追加写入（Append-only）查询快但无法更新；完全重写（Full Rewrite）支持更新但写入慢。

**如果没有这个设计会有什么问题**：
- 用户需要学习全新的 API，无法复用现有的 Spark 代码和工具链
- 无法利用 Spark 的优化器（AQE、DPP、向量化读取等）
- 与 Spark SQL、Spark Streaming、Spark Catalog 等生态组件割裂

**实际应用场景举例**：
```scala
// 场景1：用户画像表的增量更新（每天更新数亿用户的标签）
spark.read.format("hudi").load("/user_profile")
  .join(newLabels, "user_id")
  .write.format("hudi")
  .option("hoodie.datasource.write.operation", "upsert")
  .save("/user_profile")

// 场景2：订单表的实时 CDC 同步（MySQL binlog → Hudi）
kafkaStream.writeStream
  .format("hudi")
  .option("hoodie.table.name", "orders")
  .option("checkpointLocation", "/checkpoints/orders")
  .start("/orders")

// 场景3：GDPR 合规删除（根据用户请求删除个人数据）
spark.sql("DELETE FROM hudi_table WHERE user_id IN (...)")
```

**源码证据**：
- `DefaultSource` 实现了 Spark DataSource V1 的所有核心接口（`RelationProvider`、`CreatableRelationProvider`、`StreamSinkProvider`、`StreamSourceProvider`），确保与 Spark 生态无缝集成（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DefaultSource.scala:51-58`）
- `HoodieSparkSessionExtension` 通过 Spark 的扩展机制注入自定义规则，让 Hudi 表支持标准 SQL DML 语句（`INSERT`/`UPDATE`/`DELETE`/`MERGE INTO`），用户无需学习新语法（源码位置：`hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/HoodieSparkSessionExtension.scala`）

#### 1.0.2 有什么坑

**坑1：DataSource V2 被禁用导致的性能回退**
- **现象**：用户升级 Spark 版本后发现 Hudi 读取性能没有提升，甚至某些场景下变慢
- **原因**：Hudi 刻意禁用了 DataSource V2 API（HUDI-4178），因为 V2 在某些场景下存在性能回退问题。`BaseDefaultSource` 中注释掉了 `TableProvider` 接口的实现（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/BaseDefaultSource.scala`）
- **影响**：无法使用 Spark 3.x 的某些 V2 专属优化（如 Runtime Filter）
- **规避方法**：当前版本只能使用 V1 API，等待社区修复 V2 性能问题

**坑2：并行度设置不当导致小文件泛滥或 OOM**
- **现象**：写入后产生大量小文件（< 10MB），或者写入时 Executor OOM
- **原因**：`hoodie.upsert.shuffle.parallelism` 默认值在 0.13.0 之前是 200，很多用户沿用旧配置。如果数据量小（如 1GB），200 个并行度会产生 200 个小文件；如果数据量大（如 1TB），200 个并行度会导致每个 task 处理 5GB 数据，容易 OOM
- **源码证据**：`UpsertPartitioner` 根据并行度和平均记录大小计算每个 bucket 的容量（源码位置：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/UpsertPartitioner.java:90-109`）
- **正确配置**：
  ```properties
  # 0.13.0+ 推荐设置为 0（自动推断）
  hoodie.upsert.shuffle.parallelism=0
  # 或手动计算：并行度 ≈ 数据量 / 目标文件大小
  # 例：100GB / 128MB ≈ 800
  ```

**坑3：MOR 表的 Read Optimized 查询数据不一致**
- **现象**：查询结果缺少最近写入的数据
- **原因**：用户误用了 `read_optimized` 查询类型，该模式只读 base file，跳过 log file，导致最近的增量数据不可见
- **源码证据**：`DefaultSource.createRelation` 中，MOR 表的 `read_optimized` 查询使用 `HoodieCopyOnWriteSnapshotHadoopFsRelationFactory`，与 COW 表行为一致，只读 base file（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DefaultSource.scala:443-446`）
- **规避方法**：MOR 表默认使用 `snapshot` 查询（读取 base + log 合并后的最新数据），只有在明确可以容忍数据延迟时才使用 `read_optimized`

**坑4：Schema Evolution 后旧数据读取失败**
- **现象**：添加新列后，查询旧分区报错 `Column not found` 或返回 NULL
- **原因**：用户没有启用 `InternalSchema` 机制，Hudi 无法自动对齐不同版本的 Schema
- **源码证据**：`FileGroupReaderSchemaHandler` 负责读时 Schema 对齐，但需要表配置中启用 `hoodie.schema.on.read.enable=true`（源码位置：`hudi-common/src/main/java/org/apache/hudi/common/table/read/FileGroupReaderSchemaHandler.java`）
- **正确配置**：
  ```properties
  hoodie.schema.on.read.enable=true
  hoodie.table.version=8  # 表版本 >= 8 才支持 InternalSchema
  ```

**坑5：多 Writer 并发写入冲突**
- **现象**：多个 Spark 作业同时写入同一张表，部分作业失败并抛出 `HoodieWriteConflictException`
- **原因**：默认的并发控制模式是 `SINGLE_WRITER`，不支持多 Writer
- **源码证据**：`HoodieSparkSqlWriterInternal.write()` 中包含冲突重试循环，但默认重试次数为 0（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieSparkSqlWriter.scala:200-214`）
- **正确配置**：
  ```properties
  hoodie.write.concurrency.mode=optimistic_concurrency_control
  hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
  hoodie.write.lock.zookeeper.url=zk1:2181,zk2:2181
  hoodie.write.num.retries.on.conflict.failures=3
  ```

#### 1.0.3 核心概念解释

**DataSource V1 vs V2**：
- **V1**：Spark 2.x 引入的数据源 API，基于 RDD 和 DataFrame，接口简单但扩展性有限
- **V2**：Spark 3.x 引入的新 API，支持更细粒度的优化（如 Runtime Filter、Dynamic Partition Pruning），但 Hudi 因性能问题暂未采用
- **Hudi 的选择**：当前使用 V1 API（`RelationProvider`、`CreatableRelationProvider`），通过 `HoodieFileGroupReaderBasedFileFormat` 继承 `ParquetFileFormat` 来享受 Spark 的列式读取优化

**FileGroup**：
- **定义**：Hudi 的核心存储单元，由一个 base file（Parquet/ORC）和多个 log file（Avro）组成，共享同一个 `fileId`
- **生命周期**：
  ```
  T1: fg-001_T1.parquet (base file)
  T2: fg-001_T1.parquet + .fg-001_T2.log.1 (追加 log)
  T3: fg-001_T1.parquet + .fg-001_T2.log.1 + .fg-001_T3.log.2
  T4: fg-001_T4.parquet (Compaction 后产生新 base file，旧文件被清理)
  ```
- **与 Iceberg 的对比**：Iceberg 没有 FileGroup 概念，文件之间是扁平关系，通过 Manifest 文件关联；Hudi 的 FileGroup 将 base 和 log 绑定，便于增量合并

**Index（索引）**：
- **作用**：在 upsert 操作中，根据 Record Key 快速定位记录所在的 FileGroup，避免全表扫描
- **类型**：
  - `SIMPLE`：基于内存的 HashMap，适合小表（< 1 亿记录）
  - `BLOOM`：基于 Bloom Filter，适合中等规模表（1-10 亿记录）
  - `BUCKET`：基于哈希分桶，适合大表（> 10 亿记录），写入性能最优
  - `RECORD_LEVEL_INDEX`：基于 Metadata Table 的精确索引，查询性能最优
- **源码证据**：`SparkRDDWriteClient.createIndex()` 通过 `SparkHoodieIndexFactory` 创建索引实例（源码位置：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/SparkRDDWriteClient.java:87-89`）

**Timeline（时间线）**：
- **定义**：Hudi 表的所有写入操作（commit、deltacommit、compaction、clustering 等）按时间顺序组成的序列
- **Instant 状态**：
  - `REQUESTED`：操作已请求，尚未开始
  - `INFLIGHT`：操作进行中
  - `COMPLETED`：操作已完成
- **作用**：
  1. 提供 ACID 事务保证（通过原子性地将 INFLIGHT 转为 COMPLETED）
  2. 支持时间旅行查询（`as of timestamp`）
  3. 支持增量查询（读取两个 commit 之间的变更）
- **源码证据**：`HoodieActiveTimeline.saveAsComplete()` 实现原子性提交（源码位置：`hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieActiveTimeline.java`）

**Metadata Table**：
- **定义**：Hudi 0.11+ 引入的内部表，存储文件列表、列统计信息、Bloom Filter 等元数据，避免昂贵的文件系统 LIST 操作
- **分区类型**：
  - `FILES`：文件列表（替代 `fs.listStatus()`）
  - `COLUMN_STATS`：列级统计信息（min/max/null_count），用于 Data Skipping
  - `BLOOM_FILTERS`：Bloom Filter 索引
  - `RECORD_INDEX`：记录级索引（精确定位 Record Key 所在的 FileGroup）
- **性能提升**：在大表（> 10 万个文件）上，启用 Metadata Table 可将查询规划时间从分钟级降低到秒级

#### 1.0.4 设计理念

**理念1：最小化用户学习成本**
- **设计决策**：复用 Spark 的 DataSource API 和 SQL 语法，而不是发明新的 API
- **实现方式**：
  - 通过 `format("hudi")` 让用户像使用 Parquet 一样使用 Hudi
  - 通过 `HoodieSparkSessionExtension` 让标准 SQL DML 语句（`INSERT`/`UPDATE`/`DELETE`/`MERGE INTO`）直接作用于 Hudi 表
- **权衡**：牺牲了一些灵活性（如无法使用某些 Hudi 特有的高级功能），但换来了极低的上手门槛

**理念2：读写分离的架构**
- **设计决策**：写入逻辑在 `hudi-client` 模块（引擎无关），读取逻辑在 `hudi-spark-datasource` 模块（引擎特定）
- **好处**：
  1. 写入逻辑可以在 Spark、Flink、Java 客户端之间复用
  2. 读取逻辑可以深度集成 Spark 的优化器（如向量化读取、谓词下推）
- **源码证据**：
  - `SparkRDDWriteClient` 继承自 `BaseHoodieWriteClient`（引擎无关）
  - `HoodieFileGroupReader` 是引擎无关的合并读取器，Spark 通过 `SparkFileFormatInternalRowReaderContext` 适配

**理念3：索引驱动的写入优化**
- **设计决策**：在写入流程中引入 Index.tagLocation() 步骤，提前标记每条记录的目标位置
- **好处**：
  1. 避免在 Partitioner 阶段重复查找记录位置
  2. 支持小文件自动合并（INSERT 记录优先填充小文件）
  3. 支持 Bucket Index（预分桶，写入时无需查找索引）
- **源码证据**：`HoodieTable.upsert()` 的第一步就是 `index.tagLocation()`（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/commit/BaseCommitActionExecutor.java`）

**理念4：统一的文件格式抽象**
- **设计决策**：引入 `HoodieFileGroupReaderBasedFileFormat`，统一处理 COW/MOR、Snapshot/Incremental 等所有读取场景
- **演进历史**：
  - **旧架构（< 0.14）**：每种查询类型有独立的 Relation 实现（`BaseFileOnlyRelation`、`MergeOnReadSnapshotRelation` 等），代码重复度高
  - **当前架构（≥ 0.14）**：通过 `HadoopFsRelationFactory` 模式，所有场景都使用 `HadoopFsRelation` + `HoodieFileGroupReaderBasedFileFormat`
- **好处**：
  1. 代码复用度高，易于维护
  2. 可以享受 Spark 对 `ParquetFileFormat` 的所有优化（因为 `HoodieFileGroupReaderBasedFileFormat` 继承自 `ParquetFileFormat`）
- **源码证据**：`DefaultSource.createRelation()` 中的 pattern matching 逻辑，所有分支最终都调用 `Factory.build()` 返回 `HadoopFsRelation`（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DefaultSource.scala:441-465`）

**理念5：表服务的内联与异步执行**
- **设计决策**：Compaction、Clustering、Clean、Archive 等表服务可以配置为内联（inline）或异步（async）执行
- **权衡**：
  - **内联执行**：写入延迟增加，但保证表的健康状态（如 MOR 表的 log file 数量不会无限增长）
  - **异步执行**：写入延迟低，但需要额外的调度系统（如 Airflow）定期运行表服务
- **生产实践**：
  - 流式写入场景：关闭内联 Compaction（`hoodie.compact.inline=false`），通过独立的 Compaction 作业定期执行
  - 批处理场景：开启内联 Compaction（`hoodie.compact.inline=true`），每次写入后自动触发
- **源码证据**：`BaseHoodieTableServiceClient.runTableServicesInline()` 根据配置决定是否执行表服务（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieTableServiceClient.java`）

### 1.1 Hudi Spark 集成架构

Apache Hudi 通过 **Spark DataSource V1 API** 提供与 Spark 的深度集成（V2 API 因性能回归问题被刻意禁用，参见 HUDI-4178）。

**核心模块分布：**

```
hudi-spark-datasource/
    ├── hudi-spark-common/               # Spark 通用逻辑（读写核心）
    │   ├── DefaultSource.scala          # DataSource V1 入口（读+写+流）
    │   ├── BaseDefaultSource.scala      # 注册 shortName "hudi"
    │   ├── HoodieSparkSqlWriter.scala   # 写入协调器
    │   ├── HoodieHadoopFsRelationFactory.scala  # ★ 读取关系工厂（核心）
    │   ├── HoodieBaseRelation.scala     # 读取基类（旧路径）
    │   ├── HoodieFileIndex.scala        # 文件索引（分区裁剪+数据跳过）
    │   └── HoodieFileGroupReaderBasedFileFormat.scala  # ★ 统一读取格式
    ├── hudi-spark3-common/              # Spark 3.x 通用适配
    ├── hudi-spark3.3.x ~ 3.5.x/        # Spark 版本特定实现
    ├── hudi-spark4-common/              # Spark 4.x 通用适配
    ├── hudi-spark4.0.x/                 # Spark 4.0 适配
    └── hudi-spark/                      # 主模块（含 SQL 扩展 + Catalog）
        └── HoodieSparkSessionExtension.scala

hudi-client/
    ├── hudi-client-common/              # 引擎无关的写客户端核心
    │   ├── BaseHoodieWriteClient.java   # 写客户端基类
    │   ├── HoodieTable.java             # 表操作抽象
    │   └── HoodieIndex.java             # 索引抽象
    └── hudi-spark-client/               # Spark 写客户端
        ├── SparkRDDWriteClient.java     # Spark 写客户端实现
        └── HoodieSparkTable.java        # Spark 表实现（COW/MOR 各有子类）
```

### 1.2 类继承关系纠正

**纠错**：原文档写 `DefaultSource extends BaseDefaultSource`，实际上是反过来的：

```
BaseDefaultSource extends DefaultSource with DataSourceRegister
    └── shortName() = "hudi"
    └── DataSource V2 TableProvider 被注释掉了（HUDI-4178）

DefaultSource extends RelationProvider
                 with SchemaRelationProvider
                 with CreatableRelationProvider
                 with DataSourceRegister     // shortName() = "hudi_v1"
                 with StreamSinkProvider     // Structured Streaming Sink
                 with StreamSourceProvider   // Structured Streaming Source
                 with SparkAdapterSupport
                 with Serializable
```

`BaseDefaultSource` 仅仅是一个薄包装，注册 `shortName = "hudi"`，实际逻辑全在 `DefaultSource` 中。

### 1.3 写入流程总览

```
DataFrame.write.format("hudi").save()
    ↓
DefaultSource.createRelation(sqlContext, mode, optParams, df)
    ↓
HoodieSparkSqlWriter.write()  (object 方法)
    ↓
new HoodieSparkSqlWriterInternal().write()  (实际执行，含冲突重试)
    ↓
SparkRDDWriteClient.upsert/insert/bulkInsert/delete()
    ↓
HoodieTable.upsert/insert/bulkInsert/delete()
    ├── 1. Index.tagLocation() — 标记记录位置
    ├── 2. WorkloadProfile — 分析工作负载
    ├── 3. Partitioner.partition() — 文件分配
    ├── 4. handleUpsertPartition() — 执行写入
    └── 5. Index.updateLocation() — 更新索引
    ↓
commit() — 提交事务
    ↓
syncHive() — 同步外部元数据
```

### 1.4 读取流程总览（重大架构变更）

**纠错**：当前 Hudi 的 Spark 读取架构已从直接创建 Relation 演变为 **RelationFactory + HadoopFsRelation + HoodieFileGroupReaderBasedFileFormat** 的三层架构：

```
spark.read.format("hudi").load(path)
    ↓
DefaultSource.createRelation(sqlContext, parameters, schema)
    ↓
DefaultSource.createRelation(sqlContext, metaClient, schema, options)  (object 方法)
    ↓
根据 (tableType, queryType, isBootstrap) 匹配:
    ├── COW Snapshot  → HoodieCopyOnWriteSnapshotHadoopFsRelationFactory
    ├── MOR Snapshot  → HoodieMergeOnReadSnapshotHadoopFsRelationFactory
    ├── COW Incremental (V2) → HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV2
    ├── MOR Incremental (V2) → HoodieMergeOnReadIncrementalHadoopFsRelationFactoryV2
    ├── Read Optimized → HoodieCopyOnWriteSnapshotHadoopFsRelationFactory
    ├── CDC Query → HoodieCopyOnWriteCDCHadoopFsRelationFactory / MOR变体
    └── 表版本 < 8 的增量查询 → V1 变体
    ↓
Factory.build() → HadoopFsRelation
    ├── fileIndex: HoodieFileIndex (分区裁剪 + 数据跳过)
    ├── fileFormat: HoodieFileGroupReaderBasedFileFormat (★ 统一读取格式)
    ├── dataSchema / partitionSchema
    └── options
    ↓
Spark 执行引擎调用 FileFormat.buildReaderWithPartitionValues()
    ↓
HoodieFileGroupReaderBasedFileFormat 使用 HoodieFileGroupReader 读取
    ↓
返回 InternalRow 迭代器 → DataFrame
```

---

## 2. Spark 写入 Hudi 源码分析

### 2.0 前置理解

#### 2.0.1 解决什么问题

Spark 写入 Hudi 的核心问题是：**如何在保证 ACID 事务的前提下，高效地完成记录级别的 upsert（插入或更新）操作**。

**核心业务问题**：
1. **记录去重与合并**：同一批数据中可能包含同一个 Record Key 的多条记录（如 CDC 日志中的多次更新），需要在写入前合并
2. **文件大小控制**：避免产生过多小文件（影响读取性能）或过大文件（影响并行度）
3. **并发写入冲突**：多个 Spark 作业同时写入同一张表时，如何检测和解决冲突
4. **写入失败恢复**：写入过程中 Executor 失败，如何保证不产生脏数据

**如果没有这个设计会有什么问题**：
- 无法支持 upsert 语义，只能做追加写入（Append-only）
- 小文件泛滥导致读取性能下降（每次查询需要打开成千上万个小文件）
- 并发写入导致数据损坏或丢失
- 写入失败后留下半成品文件，需要手动清理

**实际应用场景举例**：
```scala
// 场景1：用户行为日志的去重写入（同一用户在同一秒内可能有多条日志）
val logs = spark.read.parquet("/raw_logs")
  .withColumn("record_key", concat($"user_id", $"timestamp"))
logs.write.format("hudi")
  .option("hoodie.datasource.write.recordkey.field", "record_key")
  .option("hoodie.datasource.write.precombine.field", "event_time")
  .option("hoodie.datasource.write.operation", "upsert")
  .save("/user_behavior")

// 场景2：维度表的全量覆盖（每天全量同步一次）
dimTable.write.format("hudi")
  .option("hoodie.datasource.write.operation", "bulk_insert")
  .mode(SaveMode.Overwrite)
  .save("/dim_table")

// 场景3：事实表的增量更新（只更新变更的记录）
changes.write.format("hudi")
  .option("hoodie.datasource.write.operation", "upsert")
  .mode(SaveMode.Append)
  .save("/fact_table")
```

**源码证据**：
- `HoodieSparkSqlWriter.write()` 是写入的统一入口，内部包含冲突重试循环（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieSparkSqlWriter.scala:192-214`）
- `SparkRDDWriteClient.upsert()` 实现了完整的 upsert 流程：Index.tagLocation() → WorkloadProfile → UpsertPartitioner → HoodieMergeHandle/HoodieAppendHandle → commit()（源码位置：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/SparkRDDWriteClient.java`）

#### 2.0.2 有什么坑

**坑1：Pre-combine 字段选择不当导致数据错乱**
- **现象**：同一个 Record Key 的多条记录，最终保留的不是最新的那条
- **原因**：`hoodie.datasource.write.precombine.field` 指定的字段不是单调递增的时间戳，或者字段类型不支持比较（如 String 类型的时间戳 "2024-01-01" 比 "2024-12-31" 大）
- **源码证据**：`RecordMerger.merge()` 使用 pre-combine 字段的值进行比较，决定保留哪条记录（源码位置：`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordMerger.java`）
- **正确配置**：
  ```properties
  # 使用 Long 类型的时间戳（毫秒）
  hoodie.datasource.write.precombine.field=update_time_ms
  # 或使用 Timestamp 类型
  hoodie.datasource.write.precombine.field=update_timestamp
  ```

**坑2：KeyGenerator 配置错误导致分区数据错乱**
- **现象**：数据写入到错误的分区，或者分区字段为 NULL
- **原因**：`hoodie.datasource.write.partitionpath.field` 指定的字段在 DataFrame 中不存在，或者 KeyGenerator 类型选择错误
- **常见错误**：
  ```scala
  // 错误：分区字段名拼写错误
  .option("hoodie.datasource.write.partitionpath.field", "dt")  // 实际字段名是 "date"
  
  // 错误：多级分区使用了错误的 KeyGenerator
  .option("hoodie.datasource.write.partitionpath.field", "year,month,day")
  .option("hoodie.datasource.write.keygenerator.class", "SimpleKeyGenerator")  // 应该用 ComplexKeyGenerator
  ```
- **源码证据**：`HoodieSparkSqlWriterInternal.writeInternal()` 中会校验 KeyGenerator 配置（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieSparkSqlWriter.scala:1026`）

**坑3：小文件合并导致写入延迟增加**
- **现象**：写入延迟突然增加，查看日志发现大量 "Merging small file" 信息
- **原因**：`UpsertPartitioner` 默认会将 INSERT 记录优先填充到小文件中，如果小文件数量很多，会导致大量的文件读取和合并操作
- **源码证据**：`UpsertPartitioner.assignInserts()` 中遍历所有小文件，计算每个小文件还能容纳多少记录（源码位置：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/UpsertPartitioner.java:152-180`）
- **规避方法**：
  ```properties
  # 方法1：提高小文件阈值，减少被识别为小文件的数量
  hoodie.parquet.small.file.limit=104857600  # 100MB（默认）
  
  # 方法2：关闭小文件合并（MOR 表）
  hoodie.merge.small.file.group.candidates.limit=0
  
  # 方法3：使用 bulk_insert 操作（不做小文件合并）
  hoodie.datasource.write.operation=bulk_insert
  ```

**坑4：Marker 文件清理失败导致后续写入失败**
- **现象**：写入失败并报错 "Failed to delete marker directory"，后续写入也失败
- **原因**：写入过程中创建的 marker 文件（用于追踪未完成的写入）在 commit 后没有被正确清理，导致下次写入时检测到 "僵尸" marker
- **源码证据**：`BaseHoodieWriteClient.postCommit()` 中调用 `WriteMarkersFactory.quietDeleteMarkerDir()` 清理 marker（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieWriteClient.java:1525-1527`）
- **规避方法**：
  ```bash
  # 手动清理 marker 目录
  hdfs dfs -rm -r /path/to/table/.hoodie/.temp/<instant_time>
  
  # 或使用 Hudi CLI
  hudi> connect --path /path/to/table
  hudi:table> marker delete --instant <instant_time>
  ```

**坑5：并发写入时的 Schema 不一致**
- **现象**：多个 Spark 作业并发写入，其中一个作业修改了 Schema（如添加新列），导致其他作业写入失败
- **原因**：Hudi 的 Schema 演进需要在 commit 时更新表的 Schema，如果多个作业同时修改 Schema，会产生冲突
- **源码证据**：`BaseHoodieWriteClient.commit()` 中调用 `saveInternalSchema()` 更新 Schema（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieWriteClient.java:1605`）
- **规避方法**：
  1. 使用统一的 Schema 管理服务（如 Schema Registry）
  2. 在写入前显式指定 Schema，避免自动推断
  3. 启用 Schema 兼容性检查：`hoodie.avro.schema.validate=true`

#### 2.0.3 核心概念解释

**WriteOperationType（写入操作类型）**：
- **UPSERT**：插入或更新，根据 Record Key 判断记录是否存在
  - 存在 → 更新（COW 重写文件，MOR 追加 log）
  - 不存在 → 插入（创建新文件或填充小文件）
- **INSERT**：纯插入，不检查记录是否存在，性能优于 UPSERT
  - 适用场景：确定数据中没有重复 Record Key（如首次导入）
- **BULK_INSERT**：批量插入，按排序键重新分区后写入，不做小文件合并
  - 适用场景：大批量数据导入，追求最高写入吞吐量
- **DELETE**：删除，根据 Record Key 删除记录
  - COW：从 base file 中移除记录，重写文件
  - MOR：在 log file 中写入 DELETE 标记
- **INSERT_OVERWRITE**：覆盖写入，删除目标分区的所有数据后插入新数据
- **INSERT_OVERWRITE_TABLE**：覆盖整张表

**源码证据**：`DataSourceUtils.doWriteOperation()` 根据 operation 类型分发到不同的写入方法（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DataSourceUtils.scala`）

**WorkloadProfile（工作负载画像）**：
- **定义**：在写入前，通过统计每个分区的 INSERT/UPDATE 记录数量，生成工作负载画像
- **作用**：
  1. 指导 `UpsertPartitioner` 进行文件分配（决定创建多少个新文件，哪些记录填充小文件）
  2. 估算写入后的文件大小，避免产生过大或过小的文件
- **数据结构**：
  ```java
  class WorkloadStat {
      Map<String, Pair<String, Long>> updateLocationToCount;  // fileId → (commitTime, 记录数)
      long numInserts;  // 该分区的 INSERT 记录数
  }
  ```
- **源码证据**：`BaseSparkCommitActionExecutor.buildProfile()` 通过 `countByKey()` 统计每个 `(partitionPath, location)` 的记录数（源码位置：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/BaseSparkCommitActionExecutor.java:111-114`）

**UpsertPartitioner（Upsert 分区器）**：
- **定义**：Spark 的自定义 Partitioner，决定每条记录应该分配到哪个 RDD partition（即哪个 Spark task）
- **核心逻辑**：
  1. **UPDATE 记录**：根据 `record.getCurrentLocation().getFileId()` 分配到对应的 bucket（一个 FileGroup 对应一个 bucket）
  2. **INSERT 记录**：
     - 优先填充小文件（小于 `hoodie.parquet.small.file.limit` 的文件）
     - 如果小文件填满，创建新的 INSERT bucket
     - 使用 MD5 hash + 二分查找将记录均匀分配到各个 INSERT bucket
- **源码证据**：`UpsertPartitioner.getPartition()` 实现了上述分配逻辑（源码位置：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/UpsertPartitioner.java:162-172`）

**HoodieWriteHandle（写入句柄）**：
- **定义**：负责实际的文件写入操作，每个 Spark task 持有一个或多个 WriteHandle
- **类型**：
  - `HoodieCreateHandle`：创建新的 base file（INSERT 操作）
  - `HoodieWriteMergeHandle`：合并旧 base file 和新记录，产生新 base file（COW UPDATE 操作）
  - `HoodieAppendHandle`：追加 log file（MOR INSERT/UPDATE 操作）
- **生命周期**：
  ```
  构造 → write(record) × N → close() → 返回 WriteStatus
  ```
- **源码证据**：`HoodieWriteHandle` 是抽象基类，定义了 `write()`、`close()` 等核心方法（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieWriteHandle.java`）

**WriteStatus（写入状态）**：
- **定义**：每个 WriteHandle 关闭后返回的写入结果，包含写入统计信息和错误信息
- **关键字段**：
  ```java
  class WriteStatus {
      String fileId;
      String partitionPath;
      long totalWriteBytes;
      long totalWriteErrors;
      List<HoodieRecord> writtenRecordDelegates;  // 写入的记录（用于更新索引）
      HoodieWriteStat stat;  // 详细统计信息
  }
  ```
- **作用**：
  1. 汇总所有 WriteStatus 生成 `HoodieCommitMetadata`
  2. 更新索引（`Index.updateLocation()`）
  3. 检测写入错误（如果 `totalWriteErrors > 0`，commit 失败）

#### 2.0.4 设计理念

**理念1：Index-driven 写入优化**
- **设计决策**：在写入流程的最开始就调用 `Index.tagLocation()`，为每条记录标记目标位置
- **好处**：
  1. 后续的 Partitioner 和 WriteHandle 可以直接使用标记结果，无需重复查找
  2. 支持小文件自动合并（INSERT 记录可以填充到已有的小文件中）
  3. 支持 Bucket Index（预分桶，写入时无需查找索引，性能最优）
- **权衡**：Index.tagLocation() 本身有开销（特别是 BLOOM Index 需要读取 Bloom Filter），但这个开销被后续的优化收益抵消
- **源码证据**：`HoodieTable.upsert()` 的第一步就是 `index.tagLocation()`（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/commit/BaseCommitActionExecutor.java`）

**理念2：小文件自动治理**
- **设计决策**：`UpsertPartitioner` 在分配 INSERT 记录时，优先填充小文件
- **好处**：
  1. 避免小文件泛滥（小文件会导致读取时打开大量文件句柄，影响性能）
  2. 自动维护文件大小在目标范围内（`hoodie.parquet.max.file.size`）
- **权衡**：小文件合并需要读取旧文件，增加写入延迟；但如果不合并，读取性能会持续下降
- **源码证据**：`UpsertPartitioner.assignInserts()` 中遍历小文件并计算剩余容量（源码位置：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/UpsertPartitioner.java:152-180`）

**理念3：冲突重试与乐观并发控制**
- **设计决策**：在 `HoodieSparkSqlWriterInternal.write()` 层面实现冲突重试循环，而不是在 `SparkRDDWriteClient` 层面
- **好处**：
  1. 每次重试都重新获取 instant time，避免使用已被其他 writer 占用的时间戳
  2. 重试逻辑与引擎无关，Flink/Java 客户端也可以复用
- **权衡**：重试会增加写入延迟，但保证了多 Writer 场景下的数据一致性
- **源码证据**：`HoodieSparkSqlWriterInternal.write()` 中的 while 循环（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieSparkSqlWriter.scala:200-214`）

**理念4：Marker 机制保证写入原子性**
- **设计决策**：每次创建或追加数据文件时，同时创建对应的 marker 文件
- **作用**：
  1. 追踪未完成的写入（如果 Executor 失败，marker 文件会残留）
  2. Rollback 时根据 marker 文件清理半成品文件
  3. 心跳机制检测 "僵尸" 写入（如果心跳过期，其他进程可以安全地 rollback）
- **源码证据**：
  - `HoodieCreateHandle` 构造函数中调用 `createPartitionMetadataAndMarkerFile()`（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieCreateHandle.java`）
  - `BaseHoodieWriteClient.postCommit()` 中调用 `quietDeleteMarkerDir()` 清理 marker（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieWriteClient.java:1525-1527`）

**理念5：分层的事务保证**
- **设计决策**：事务保证分为三层：
  1. **文件级原子性**：通过 HDFS 的原子性 rename 操作（`.tmp` → 正式文件名）
  2. **Instant 级原子性**：通过原子性地将 `.inflight` 文件重命名为 `.commit` 文件
  3. **表级原子性**：通过分布式锁（Zookeeper/DynamoDB）保证多 Writer 场景下的串行化
- **好处**：即使在分布式环境下，也能保证 ACID 事务语义
- **源码证据**：
  - `HoodieActiveTimeline.saveAsComplete()` 实现 Instant 级原子性（源码位置：`hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieActiveTimeline.java`）
  - `TransactionManager.beginStateChange()` 获取分布式锁（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/TransactionManager.java`）

### 2.1 入口：DefaultSource

**源码位置**：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DefaultSource.scala`

```scala
class DefaultSource extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider
    with DataSourceRegister
    with StreamSinkProvider      // ★ 同时支持 Streaming Sink
    with StreamSourceProvider    // ★ 同时支持 Streaming Source
    with SparkAdapterSupport
    with Serializable {

  override def shortName(): String = "hudi_v1"

  // 写入入口
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      optParams: Map[String, String],
      df: DataFrame): BaseRelation = {
    if (optParams.get(OPERATION.key).contains(BOOTSTRAP_OPERATION_OPT_VAL)) {
      HoodieSparkSqlWriter.bootstrap(sqlContext, mode, optParams, df)
    } else {
      val (success, _, _, _, _, _) = HoodieSparkSqlWriter.write(sqlContext, mode, optParams, df)
      if (!success) throw new HoodieException("Failed to write to Hudi")
    }
    HoodieSparkSqlWriter.cleanup()
    new HoodieEmptyRelation(sqlContext, df.schema)
  }
}
```

**源码洞察**：
- `DefaultSource` 的 `shortName` 是 `"hudi_v1"`，而 `BaseDefaultSource` 的 `shortName` 是 `"hudi"`。用户写 `format("hudi")` 实际匹配到 `BaseDefaultSource`。
- 写入完成后返回 `HoodieEmptyRelation`——Spark 不需要写入操作返回真正的 Relation（参见 `SaveIntoDataSourceCommand.run()`）

### 2.2 核心：HoodieSparkSqlWriter

**源码位置**：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieSparkSqlWriter.scala`

实际分为两层：
- `HoodieSparkSqlWriter` (object)：薄包装，创建 `HoodieSparkSqlWriterInternal` 实例
- `HoodieSparkSqlWriterInternal` (class)：实际写入逻辑，**含冲突重试机制**

```scala
// object 层：委托给 internal
object HoodieSparkSqlWriter {
  def write(sqlContext, mode, optParams, sourceDf, ...):
    (Boolean, HOption[String], ..., SparkRDDWriteClient[_], HoodieTableConfig) = {
    new HoodieSparkSqlWriterInternal().write(sqlContext, mode, optParams, sourceDf, ...)
  }
}

// internal 层：包含重试循环
class HoodieSparkSqlWriterInternal {
  def write(...) = {
    val maxRetry = optParams.getOrElse(NUM_RETRIES_ON_CONFLICT_FAILURES.key, ...)
    var counter = 0
    while (counter <= maxRetry && !succeeded) {
      try {
        // 实际写入逻辑
        toReturn = writeInternal(...)
        succeeded = true
      } catch {
        case e: HoodieWriteConflictException if counter < maxRetry =>
          counter += 1
          // 冲突重试：重新获取 instant time，重新执行写入
      }
    }
  }
}
```

**源码洞察**：冲突重试是在 `HoodieSparkSqlWriterInternal` 层面实现的，而不是在 `SparkRDDWriteClient` 层面。这意味着每次重试都是完整的写入流程，包括重新分配 instant time。

### 2.3 SparkRDDWriteClient.upsert() — 实际执行

**源码位置**：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/SparkRDDWriteClient.java`

```java
public class SparkRDDWriteClient<T> extends
    BaseHoodieWriteClient<T, JavaRDD<HoodieRecord<T>>,
                          JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  @Override
  public JavaRDD<WriteStatus> upsert(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    // 1. 初始化表（含版本升级检查）
    HoodieTable table = initTable(WriteOperationType.UPSERT, Option.ofNullable(instantTime));

    // 2. Schema 验证
    table.validateUpsertSchema();

    // 3. 预写（心跳注册、marker 创建）
    preWrite(instantTime, WriteOperationType.UPSERT, table.getMetaClient(), Option.of(records));

    // 4. ★ 执行 upsert（核心逻辑在 HoodieTable 中）
    HoodieWriteMetadata<HoodieData<WriteStatus>> result =
        table.upsert(context, instantTime, HoodieJavaRDD.of(records));

    // 5. 后处理（指标收集）
    return postWrite(resultRDD, instantTime, table);
  }
}
```

### 2.4 HoodieTable.upsert() 的五步流程

以 COW 表为例，`HoodieSparkCopyOnWriteTable.upsert()` 的完整流程：

```
HoodieSparkCopyOnWriteTable.upsert(context, instantTime, records)
    ↓
HoodieWriteHelper.write(table, instantTime, records, partitioner)
    ↓
Step 1: index.tagLocation(records, context, table)
    ├── 查找每条记录的 Record Key 是否已存在
    ├── 存在 → 标记 UPDATE + 目标 FileGroup
    └── 不存在 → 标记 INSERT
    ↓
Step 2: WorkloadProfile = new WorkloadProfile(taggedRecords)
    ├── 统计每个分区的 insert 数量 / update 数量
    ├── 确定 WorkloadStat（全局写入统计）
    └── 用于后续文件分配决策
    ↓
Step 3: partitioner.partition(taggedRecords, workloadProfile)
    ├── UpsertPartitioner:
    │   ├── UPDATE 记录 → 分配到已有 FileGroup（对应的 bucket）
    │   ├── INSERT 记录 → 先尝试填充小文件，再创建新 FileGroup
    │   └── 每个 bucket 对应一个 Spark partition
    └── BulkInsertPartitioner: 按排序键重新分区
    ↓
Step 4: handleUpsertPartition(instantTime, partition, recordIterator)
    ├── 对每个 partition 中的记录:
    │   ├── UPDATE 记录 → HoodieMergeHandle
    │   │   ├── 读取旧 base file (Parquet)
    │   │   ├── 遍历旧记录，遇到匹配 key 则合并
    │   │   ├── 写入新 base file（完全重写）
    │   │   └── 最后写入剩余的新 INSERT 记录
    │   └── INSERT 记录 → HoodieCreateHandle
    │       └── 直接创建新的 base file
    └── 返回 WriteStatus（写入结果统计）
    ↓
Step 5: index.updateLocation(writeStatuses, context, table)
    └── 将新的 recordKey → fileId 映射更新到索引
```

### 2.5 写入 IO 层详解

#### 2.5.1 COW 写入 — HoodieMergeHandle

**源码位置**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieMergeHandle.java`

```
HoodieMergeHandle 工作流程:

1. 初始化:
   ├── 打开旧 base file 的 reader
   ├── 创建新 base file 的 writer
   └── 将所有新记录放入 keyToNewRecords (内存 Map)

2. 遍历旧 base file:
   for each oldRecord in baseFile:
       if keyToNewRecords.contains(oldRecord.key):
           // ★ 合并逻辑（使用 RecordMerger）
           mergedRecord = recordMerger.merge(oldRecord, newRecord, schema, config)
           write(mergedRecord)
           keyToNewRecords.remove(oldRecord.key)
       else:
           write(oldRecord)  // 保留不变的旧记录

3. 写入剩余 INSERT:
   for each remainingRecord in keyToNewRecords:
       write(remainingRecord)

4. 关闭 writer，返回 WriteStatus
```

**关键性能特征**：
- COW UPDATE 的写放大 = `旧文件大小 + 新记录大小`（因为需要完全重写）
- 即使只更新 1 条记录，也需要重写整个 base file
- 优化：调整 `hoodie.parquet.max.file.size` 控制目标文件大小，小文件会被自动合并

#### 2.5.2 MOR 写入 — HoodieAppendHandle

**源码位置**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieAppendHandle.java`

```
HoodieAppendHandle 工作流程:

1. 初始化:
   └── 打开 LogFile writer (HoodieLogFormatWriter)

2. 批量写入:
   ├── 将记录收集到 recordList
   ├── 达到批大小时 → 创建 HoodieAvroDataBlock
   ├── 写入 Block 到 log file
   └── 重置 recordList

3. 关闭 writer，返回 WriteStatus

Log File 内部结构:
├── Magic (6 bytes)
├── Block 1 (HoodieAvroDataBlock)
│   ├── Block Header (schema, instantTime, 记录数)
│   └── Records (Avro 序列化)
├── Block 2 (HoodieDeleteBlock)
│   └── Deleted Keys
├── Block 3 (HoodieCommandBlock)
│   └── Rollback Command
└── ...
```

**关键性能特征**：
- MOR 写入只需追加，不读取旧数据，写入延迟极低
- Log File 是 Avro 格式，不支持列式读取
- 随着 Log Files 增多，读放大线性增长 → 需要 Compaction

### 2.6 事务提交完整流程

```
BaseHoodieWriteClient.commitStats(instantTime, writeStatuses, metadata)
    ↓
Step 1: 构建 HoodieCommitMetadata
    ├── 每个 WriteStatus 包含:
    │   ├── fileId, partitionPath
    │   ├── totalWriteBytes, totalWriteErrors
    │   ├── writtenRecordDelegates (写入的记录)
    │   └── stat (HoodieWriteStat)
    └── 汇总所有 WriteStatus → CommitMetadata
    ↓
Step 2: TransactionManager.beginStateChange()
    └── 获取锁（仅在 isLockRequired = true 时）
    ↓
Step 3: preCommit(commitMetadata)
    ├── 冲突检测 (ConflictResolutionStrategy)
    │   ├── 获取候选冲突 Instant
    │   ├── 检查文件级冲突 (FileGroup 交集)
    │   └── 冲突则抛 HoodieWriteConflictException
    ├── Schema 兼容性校验
    └── 用户自定义 pre-commit validator
    ↓
Step 4: HoodieActiveTimeline.saveAsComplete(inflight, commitMetadata)
    └── 原子性写入：INFLIGHT → COMPLETED
    ↓
Step 5: postCommit(metadata, writeStatuses)
    ├── 更新 Metadata Table
    ├── 运行 Inline Table Services:
    │   ├── Compaction (if hoodie.compact.inline=true)
    │   ├── Clustering (if hoodie.clustering.inline=true)
    │   ├── Clean (if hoodie.clean.automatic=true)
    │   └── Archive (if hoodie.archive.automatic=true)
    └── 同步外部元数据 (Hive/Glue/DataHub)
    ↓
Step 6: TransactionManager.endStateChange()
    └── 释放锁
```

---

## 3. Spark 读取 Hudi 源码分析

### 3.0 前置理解

#### 3.0.1 解决什么问题

Spark 读取 Hudi 的核心问题是：**如何在保证数据一致性的前提下，高效地读取 COW/MOR 表的最新数据，并享受 Spark 的所有查询优化**。

**核心业务问题**：
1. **MOR 表的实时合并**：MOR 表的数据分散在 base file 和多个 log file 中，如何高效地合并这些文件并返回最新数据
2. **分区裁剪与数据跳过**：大表（> 10 万个文件）如何快速定位需要读取的文件，避免全表扫描
3. **Schema 演进的兼容性**：表的 Schema 在不断演进（添加列、删除列、重命名列），如何保证旧数据仍然可读
4. **增量查询的一致性**：读取两个 commit 之间的变更时，如何保证不丢失数据（特别是并发写入场景）

**如果没有这个设计会有什么问题**：
- MOR 表的查询性能极差（需要在查询时实时合并大量 log file）
- 大表的查询规划时间过长（需要 LIST 所有文件）
- Schema 变更后旧数据无法读取，或者读取结果不正确
- 增量查询在并发写入场景下可能丢失数据

**实际应用场景举例**：
```scala
// 场景1：MOR 表的 Snapshot 查询（读取最新数据）
val latestData = spark.read.format("hudi")
  .option("hoodie.datasource.query.type", "snapshot")
  .load("/mor_table")
  .where("dt >= '2024-01-01'")  // 分区裁剪
  .where("amount > 1000")       // 数据跳过（利用列统计信息）

// 场景2：增量查询（读取两个 commit 之间的变更）
val changes = spark.read.format("hudi")
  .option("hoodie.datasource.query.type", "incremental")
  .option("hoodie.datasource.read.begin.instanttime", "20240101000000")
  .option("hoodie.datasource.read.end.instanttime", "20240102000000")
  .load("/hudi_table")

// 场景3：时间旅行查询（读取历史快照）
val historicalData = spark.read.format("hudi")
  .option("as.of.instant", "20240101000000")
  .load("/hudi_table")

// 场景4：Read Optimized 查询（牺牲新鲜度换取性能）
val optimizedData = spark.read.format("hudi")
  .option("hoodie.datasource.query.type", "read_optimized")
  .load("/mor_table")  // 只读 base file，跳过 log file
```

**源码证据**：
- `DefaultSource.createRelation()` 根据表类型和查询类型路由到不同的 `HadoopFsRelationFactory`（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DefaultSource.scala:441-465`）
- `HoodieFileGroupReader` 实现了引擎无关的 base + log 合并逻辑（源码位置：`hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieFileGroupReader.java:70-123`）

#### 3.0.2 有什么坑

**坑1：MOR 表的 log file 过多导致查询超时**
- **现象**：MOR 表的 Snapshot 查询非常慢，甚至超时失败
- **原因**：长时间没有执行 Compaction，导致某些 FileGroup 积累了大量 log file（> 100 个），合并开销极大
- **源码证据**：`HoodieFileGroupReader.initRecordIterators()` 中会扫描所有 log file 并加载到 `HoodieFileGroupRecordBuffer` 中（源码位置：`hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieFileGroupReader.java:128-140`）
- **规避方法**：
  ```properties
  # 方法1：定期执行 Compaction
  hoodie.compact.inline=true
  hoodie.compact.inline.max.delta.commits=5  # 每 5 次 deltacommit 触发一次 Compaction
  
  # 方法2：使用 Read Optimized 查询（只读 base file）
  hoodie.datasource.query.type=read_optimized
  
  # 方法3：启用 Log Compaction（合并 log file，不产生新 base file）
  hoodie.log.compaction.enable=true
  ```

**坑2：Metadata Table 未启用导致查询规划慢**
- **现象**：查询执行很快，但查询规划阶段耗时很长（几分钟甚至更久）
- **原因**：没有启用 Metadata Table，Spark 需要调用 `fs.listStatus()` 遍历所有分区和文件，在大表上非常慢
- **源码证据**：`HoodieFileIndex.listFiles()` 优先使用 Metadata Table 的 FILES 分区获取文件列表（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieFileIndex.scala`）
- **正确配置**：
  ```properties
  hoodie.metadata.enable=true
  hoodie.metadata.index.column.stats.enable=true  # 启用列统计信息
  hoodie.enable.data.skipping=true                # 启用数据跳过
  ```

**坑3：增量查询丢失数据（V1 vs V2）**
- **现象**：增量查询返回的数据量少于预期，或者某些 commit 的数据丢失
- **原因**：表版本 < 8 时使用 V1 增量查询（基于 `requestedTime`），在并发写入场景下可能丢失数据
- **示例**：
  ```
  Writer A: 开始写入 T1（requestedTime=100）
  Writer B: 开始写入 T2（requestedTime=101）
  Writer B: 完成写入 T2（completionTime=102）
  Writer A: 完成写入 T1（completionTime=103）
  
  增量查询 [100, 102]：
  - V1（基于 requestedTime）：只返回 T1 和 T2
  - V2（基于 completionTime）：只返回 T2（T1 的 completionTime=103 超出范围）
  ```
- **源码证据**：`DefaultSource.createRelation()` 根据表版本选择 V1 或 V2 增量查询实现（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DefaultSource.scala:449-454`）
- **规避方法**：
  ```properties
  # 升级表版本到 8（支持 completionTime）
  hoodie.table.version=8
  # 或使用 V2 增量查询（即使表版本 < 8）
  hoodie.datasource.read.incr.fallback.fulltablescan.enable=true
  ```

**坑4：Data Skipping 不生效**
- **现象**：查询带有 WHERE 条件，但仍然扫描了大量不相关的文件
- **原因**：
  1. 没有启用列统计信息（`hoodie.metadata.index.column.stats.enable=false`）
  2. WHERE 条件中的列没有被索引（默认只索引前 10 列）
  3. 列的数据分布不均匀（如性别字段只有两个值，min/max 无法有效过滤）
- **源码证据**：`HoodieFileIndex.listFiles()` 中使用 `COLUMN_STATS` 分区的 min/max 值进行数据跳过（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieFileIndex.scala`）
- **正确配置**：
  ```properties
  hoodie.metadata.index.column.stats.enable=true
  hoodie.metadata.index.column.stats.column.list=user_id,amount,create_time  # 指定需要索引的列
  ```

**坑5：Schema 演进后查询报错**
- **现象**：添加新列后，查询旧分区报错 `Column 'new_column' not found`
- **原因**：没有启用 Schema Evolution 支持，Hudi 无法自动填充新列的默认值
- **源码证据**：`FileGroupReaderSchemaHandler` 负责读时 Schema 对齐，但需要启用 `hoodie.schema.on.read.enable=true`（源码位置：`hudi-common/src/main/java/org/apache/hudi/common/table/read/FileGroupReaderSchemaHandler.java`）
- **正确配置**：
  ```properties
  hoodie.schema.on.read.enable=true
  hoodie.table.version=8  # 表版本 >= 8 才支持 InternalSchema
  ```

#### 3.0.3 核心概念解释

**查询类型（Query Type）**：
- **snapshot**：快照查询，读取最新的完整数据
  - COW 表：直接读取最新的 base file
  - MOR 表：读取 base file + log file 合并后的数据
- **read_optimized**：读优化查询，只读 base file，跳过 log file
  - 适用场景：可以容忍数据延迟（最多延迟到上次 Compaction）
  - 性能：等同于原生 Parquet 读取
- **incremental**：增量查询，读取两个 commit 之间的变更
  - V1（表版本 < 8）：基于 `requestedTime` 排序
  - V2（表版本 ≥ 8）：基于 `completionTime` 排序，保证因果一致性
- **time_travel**：时间旅行查询，读取指定时间点的历史快照
  - 通过 `as.of.instant` 参数指定时间点

**FileSlice（文件切片）**：
- **定义**：一个 FileGroup 在某个时间点的快照，包含一个 base file 和多个 log file
- **示例**：
  ```
  FileGroup: fg-001
  ├── FileSlice (baseInstant=T1, maxInstant=T3)
  │   ├── base: fg-001_T1.parquet
  │   ├── log: .fg-001_T2.log.1
  │   └── log: .fg-001_T3.log.2
  └── FileSlice (baseInstant=T4, maxInstant=T4)
      └── base: fg-001_T4.parquet (Compaction 后产生)
  ```
- **作用**：读取时，Spark 为每个 FileSlice 创建一个 `PartitionedFile`，分配到一个 task 处理

**HoodieFileGroupRecordBuffer（文件组记录缓冲区）**：
- **定义**：MOR 表读取时，用于缓存和合并 base file 和 log file 的记录
- **工作原理**：
  1. 扫描所有 log file，将记录按 record key 组织成 `ExternalSpillableMap`
  2. 遍历 base file 的每条记录，检查 `logRecordMap` 中是否有更新
  3. 如果有更新，使用 `RecordMerger` 合并；如果是删除，跳过该记录
  4. 输出合并后的记录 + log-only 记录（只存在于 log 中的 INSERT）
- **源码证据**：`HoodieFileGroupReader.initRecordIterators()` 中创建 `HoodieFileGroupRecordBuffer`（源码位置：`hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieFileGroupReader.java:134-139`）

**Data Skipping（数据跳过）**：
- **定义**：利用文件级别的统计信息（min/max/null_count）跳过不满足 WHERE 条件的文件
- **层次**：
  1. **分区裁剪**：根据分区字段过滤分区（Spark 原生支持）
  2. **列统计裁剪**：根据列的 min/max 值过滤文件（Hudi Metadata Table 提供）
  3. **Bloom Filter 裁剪**：根据 Bloom Filter 判断文件是否可能包含指定的 Record Key
  4. **Record Level Index**：精确定位 Record Key 所在的 FileGroup
- **示例**：
  ```sql
  SELECT * FROM hudi_table WHERE amount > 1000
  
  文件统计信息：
  file1: min=0, max=500     → 跳过（max < 1000）
  file2: min=800, max=2000  → 保留（可能包含 > 1000 的记录）
  file3: min=1500, max=3000 → 保留（min > 1000，一定包含）
  ```
- **源码证据**：`HoodieFileIndex.listFiles()` 中使用 `COLUMN_STATS` 分区进行数据跳过（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieFileIndex.scala`）

**Record Position（记录位置）**：
- **定义**：表版本 >= 8 时，Hudi 会在 log block header 中记录每条记录在 base file 中的位置（行号）
- **作用**：读取时可以基于位置进行快速合并，避免全键查找
- **工作原理**：
  ```
  传统合并（基于 Record Key）：
  for each baseRecord:
      if logRecordMap.contains(baseRecord.key):  // O(log n) 查找
          merge(baseRecord, logRecord)
  
  位置优化合并：
  for each baseRecord at position i:
      if logRecordMap.contains(position i):      // O(1) 查找
          merge(baseRecord, logRecord)
  ```
- **源码证据**：`HoodieFileGroupReader` 中通过 `shouldUseRecordPosition` 参数控制是否启用位置优化（源码位置：`hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieFileGroupReader.java:115`）

#### 3.0.4 设计理念

**理念1：统一的读取抽象（HoodieFileGroupReader）**
- **设计决策**：将 base + log 合并逻辑封装在引擎无关的 `HoodieFileGroupReader` 中
- **好处**：
  1. Spark、Flink、Java 客户端共享同一套合并逻辑，减少代码重复
  2. 合并逻辑的优化（如 record position）可以惠及所有引擎
  3. 便于测试和维护
- **实现方式**：通过 `HoodieReaderContext` 接口适配不同引擎的记录表示（Spark 的 `InternalRow`、Flink 的 `RowData`）
- **源码证据**：`HoodieFileGroupReader` 的构造函数接受 `HoodieReaderContext<T>` 参数（源码位置：`hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieFileGroupReader.java:91-123`）

**理念2：接入 Spark 原生的文件扫描框架**
- **设计决策**：`HoodieFileGroupReaderBasedFileFormat` 继承 `ParquetFileFormat`，而不是自定义 RDD
- **好处**：
  1. 享受 Spark 的所有优化：向量化读取、自适应查询执行（AQE）、动态分区裁剪（DPP）
  2. 与 Spark 的执行计划深度集成，便于调试和性能分析
  3. 未来 Spark 的新优化可以自动惠及 Hudi
- **演进历史**：
  - **旧架构（< 0.14）**：使用自定义 RDD（`HoodieMergeOnReadRDD`），无法享受 Spark 优化
  - **当前架构（≥ 0.14）**：使用 `HadoopFsRelation` + `HoodieFileGroupReaderBasedFileFormat`
- **源码证据**：`HoodieFileGroupReaderBasedFileFormat extends ParquetFileFormat`（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/HoodieFileGroupReaderBasedFileFormat.scala:522`）

**理念3：分层的元数据索引**
- **设计决策**：Metadata Table 支持多种分区类型（FILES、COLUMN_STATS、BLOOM_FILTERS、RECORD_INDEX），用户可以按需启用
- **好处**：
  1. 灵活性：用户可以根据查询模式选择合适的索引（如点查场景启用 RECORD_INDEX，范围查询场景启用 COLUMN_STATS）
  2. 成本控制：索引的维护有开销（写入时需要更新索引），用户可以只启用必要的索引
- **权衡**：
  - FILES 分区：几乎无开销，强烈推荐启用
  - COLUMN_STATS 分区：中等开销，适合大表的范围查询
  - BLOOM_FILTERS 分区：中等开销，适合点查场景
  - RECORD_INDEX 分区：高开销，适合频繁点查的场景
- **源码证据**：`MetadataPartitionType` 枚举定义了所有分区类型（源码位置：`hudi-common/src/main/java/org/apache/hudi/metadata/MetadataPartitionType.java`）

**理念4：增量查询的因果一致性（V2）**
- **设计决策**：表版本 >= 8 时，使用 `completionTime` 而不是 `requestedTime` 进行增量查询
- **问题背景**：在并发写入场景下，`requestedTime` 无法保证因果一致性
  ```
  Writer A: 开始写入 T1（requestedTime=100）
  Writer B: 开始写入 T2（requestedTime=101）
  Writer B: 完成写入 T2（completionTime=102）
  Writer A: 完成写入 T1（completionTime=103）
  
  增量查询 [100, 102]：
  - V1（requestedTime）：返回 T1 和 T2（但 T1 实际在 103 才完成）
  - V2（completionTime）：只返回 T2（T1 的 completionTime=103 超出范围）
  ```
- **V2 的保证**：如果 commit T2 依赖于 commit T1 的数据，那么 T2 的 completionTime 一定大于 T1 的 completionTime
- **源码证据**：`HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV2` 使用 `completionTime` 过滤 commit（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieIncrementalRelationV2.scala`）

**理念5：读时 Schema 对齐**
- **设计决策**：通过 `FileGroupReaderSchemaHandler` 在读取时自动对齐不同版本的 Schema
- **支持的演进操作**：
  - 添加列：填充 NULL 或默认值
  - 删除列：跳过该列（不读取）
  - 重命名列：通过 column ID 映射（需要 InternalSchema）
  - 类型宽化：安全的类型转换（int → long、float → double）
- **好处**：
  1. 用户无需手动处理 Schema 不一致问题
  2. 旧数据仍然可读，无需重写
  3. 支持在线 Schema 演进（无需停机）
- **源码证据**：`FileGroupReaderSchemaHandler` 的 `getOutputConverter()` 方法返回 Schema 对齐的转换器（源码位置：`hudi-common/src/main/java/org/apache/hudi/common/table/read/FileGroupReaderSchemaHandler.java`）

### 3.1 读取架构演进（重要）

Hudi 的 Spark 读取架构经历了重大演进：

```
旧架构 (< 0.14):
  DefaultSource → 直接创建 BaseFileOnlyRelation / MergeOnReadSnapshotRelation
  └── 自定义 RDD 读取 (HoodieMergeOnReadRDD)

当前架构 (≥ 0.14, 1.x):
  DefaultSource → HadoopFsRelationFactory → HadoopFsRelation
  └── HoodieFileGroupReaderBasedFileFormat (extends ParquetFileFormat)
      └── 利用 Spark 原生的文件扫描框架 + HoodieFileGroupReader
```

**核心变化**：读取不再使用自定义 RDD，而是接入 Spark 原生的 `FileSourceScanExec` → `FileFormat.buildReaderWithPartitionValues()` 执行链路。这让 Hudi 读取可以享受 Spark 的所有优化（向量化读取、自适应查询执行 AQE、动态分区裁剪 DPP 等）。

### 3.2 读取入口：DefaultSource.createRelation()

**源码位置**：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DefaultSource.scala`

```scala
object DefaultSource {
  def createRelation(sqlContext: SQLContext,
                     metaClient: HoodieTableMetaClient,
                     schema: StructType,
                     parameters: Map[String, String]): BaseRelation = {

    val tableType = metaClient.getTableType
    val queryType = parameters(QUERY_TYPE.key)
    val isBootstrappedTable = metaClient.getTableConfig.getBootstrapBasePath.isPresent
    val tableVersion = metaClient.getTableConfig.getTableVersion.versionCode()
    val hoodieTableSupportsCompletionTime = tableVersion >= HoodieTableVersion.EIGHT.versionCode()

    // ★ 核心路由逻辑（pattern matching）
    (tableType, queryType, isBootstrappedTable) match {
      // COW Snapshot / Read Optimized / MOR Read Optimized
      case (COPY_ON_WRITE, QUERY_TYPE_SNAPSHOT_OPT_VAL, false) |
           (COPY_ON_WRITE, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, false) |
           (MERGE_ON_READ, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, false) =>
        new HoodieCopyOnWriteSnapshotHadoopFsRelationFactory(...).build()

      // COW Incremental
      case (COPY_ON_WRITE, QUERY_TYPE_INCREMENTAL_OPT_VAL, _) =>
        if (hoodieTableSupportsCompletionTime)
          new HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV2(...).build()
        else
          new HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV1(...).build()

      // MOR Snapshot
      case (MERGE_ON_READ, QUERY_TYPE_SNAPSHOT_OPT_VAL, false) =>
        new HoodieMergeOnReadSnapshotHadoopFsRelationFactory(...).build()

      // MOR Incremental
      case (MERGE_ON_READ, QUERY_TYPE_INCREMENTAL_OPT_VAL, _) =>
        if (hoodieTableSupportsCompletionTime)
          new HoodieMergeOnReadIncrementalHadoopFsRelationFactoryV2(...).build()
        else
          new HoodieMergeOnReadIncrementalHadoopFsRelationFactoryV1(...).build()
    }
  }
}
```

**源码洞察**：
- MOR 表的 `Read Optimized` 查询使用的是 `CopyOnWriteSnapshotHadoopFsRelationFactory`（因为只读 Base Files，与 COW Snapshot 行为一致）
- 增量查询根据表版本（是否支持 completion time）选择 V1 或 V2 实现
- 还支持 **CDC 查询**（`INCREMENTAL_FORMAT_CDC_VAL`）和 **TimelineRelation / FileSystemRelation**（元数据查询）

### 3.3 HoodieHadoopFsRelationFactory — 读取关系工厂

**源码位置**：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieHadoopFsRelationFactory.scala`

```scala
trait HoodieHadoopFsRelationFactory {
  def build(): HadoopFsRelation      // ★ 构建最终的 HadoopFsRelation
  def buildFileIndex(): FileIndex     // 文件索引（分区裁剪）
  def buildFileFormat(): FileFormat   // 文件格式（读取逻辑）
  def buildPartitionSchema(): StructType
  def buildDataSchema(): StructType
  def buildBucketSpec(): Option[BucketSpec]
  def buildOptions(): Map[String, String]
}
```

**关键实现类：**

| Factory 类 | 用途 | FileFormat |
|-----------|------|-----------|
| `HoodieCopyOnWriteSnapshotHadoopFsRelationFactory` | COW 快照 + Read Optimized | `HoodieFileGroupReaderBasedFileFormat` |
| `HoodieMergeOnReadSnapshotHadoopFsRelationFactory` | MOR 快照 | `HoodieFileGroupReaderBasedFileFormat` (isMOR=true) |
| `HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV2` | COW 增量 (表版本≥8) | `HoodieFileGroupReaderBasedFileFormat` |
| `HoodieMergeOnReadIncrementalHadoopFsRelationFactoryV2` | MOR 增量 (表版本≥8) | `HoodieFileGroupReaderBasedFileFormat` |
| V1 变体 | 旧表增量读取 | `HoodieFileGroupReaderBasedFileFormat` |

所有 Factory 最终都使用 `HoodieFileGroupReaderBasedFileFormat` 作为 FileFormat——这就是 Hudi 读取的**统一入口**。

### 3.4 HoodieFileGroupReaderBasedFileFormat — 统一读取格式

**源码位置**：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/HoodieFileGroupReaderBasedFileFormat.scala`

```scala
class HoodieFileGroupReaderBasedFileFormat(
    tablePath: String,
    tableSchema: HoodieTableSchema,
    tableName: String,
    queryTimestamp: String,          // 查询时间点
    mandatoryFields: Seq[String],    // 必须读取的字段
    isMOR: Boolean,                  // ★ 是否 MOR 表
    isBootstrap: Boolean,
    isIncremental: Boolean,          // ★ 是否增量查询
    validCommits: String,            // 有效 commit 列表
    shouldUseRecordPosition: Boolean,// 是否使用 record position 优化
    requiredFilters: Seq[Filter],
    isMultipleBaseFileFormatsEnabled: Boolean,
    hoodieFileFormat: HoodieFileFormat)
  extends ParquetFileFormat            // ★ 继承 Spark 原生 Parquet 格式
  with SparkAdapterSupport
  with HoodieFormatTrait
  with Logging
  with Serializable {

  // 核心方法：构建记录 reader
  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {

    // 对每个 PartitionedFile:
    (file: PartitionedFile) => {
      // ★ 核心：使用 HoodieFileGroupReader 读取
      val fileGroupReader = new HoodieFileGroupReader[InternalRow](
          readerContext,         // SparkFileFormatInternalRowReaderContext
          storageConf,
          tablePath,
          latestCommitTimestamp,
          fileSlice,             // 从 PartitionedFile 提取
          dataSchema,
          requiredSchema,
          ...)

      fileGroupReader.initRecordIterators()
      // 返回 InternalRow 迭代器
      fileGroupReader.getClosableIterator()
    }
  }
}
```

**源码洞察**：
- 继承 `ParquetFileFormat` 意味着 Spark 可以把 Hudi 表当作 Parquet 数据源来优化（向量化读取、谓词下推等）
- `isMOR` 参数决定 `HoodieFileGroupReader` 是否需要合并 log files
- 对于 COW 表（`isMOR=false`），如果 FileSlice 没有 log files，就直接使用 Parquet 列式读取，性能等同于原生 Parquet

### 3.5 HoodieFileIndex — 分区裁剪与数据跳过

**源码位置**：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieFileIndex.scala`

```
HoodieFileIndex.listFiles(partitionFilters, dataFilters)
    ↓
Level 1: 分区裁剪 (Partition Pruning)
    ├── 根据 WHERE 条件中的分区字段过滤分区
    ├── 利用 Metadata Table 的 FILES 分区获取分区列表（避免 LIST 操作）
    └── 示例: WHERE dt='2024-01-01' → 只保留 dt=2024-01-01 分区
    ↓
Level 2: 列统计裁剪 (Data Skipping via Column Stats)
    ├── 利用 Metadata Table 的 COLUMN_STATS 分区
    ├── 比较 WHERE 条件与文件的 min/max 值
    └── 示例: WHERE amount > 1000
              file1: min=0, max=500 → 跳过
              file2: min=800, max=2000 → 保留
    ↓
Level 3: Bloom Filter 裁剪 (针对 Record Key 点查)
    ├── 利用 Metadata Table 的 BLOOM_FILTERS 分区
    └── 示例: WHERE id='user123' → BF 判断哪些文件可能包含此 key
    ↓
Level 4: Record Level Index (精确定位)
    ├── 利用 Metadata Table 的 RECORD_INDEX 分区
    └── 示例: WHERE id='user123' → 直接定位到 FileGroup-X
    ↓
输出: 需要读取的 FileSlice 列表（附带分区值）
```

### 3.6 HoodieFileGroupReader — 核心合并读取

**源码位置**：`hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieFileGroupReader.java`

这是一个**引擎无关**的组件，Spark/Flink/Java 都使用同一套合并逻辑：

```
HoodieFileGroupReader.read(fileSlice)
    ↓
Phase 1: 读取 Base File (如果存在)
    ├── 使用引擎特定的 reader (Spark: SparkParquetReader)
    └── 返回 base records 迭代器
    ↓
Phase 2: 读取 Log Files
    ├── 创建 HoodieMergedLogRecordScanner
    ├── 扫描所有 log files 的所有 block
    ├── 将 log records 按 record key 组织成 ExternalSpillableMap
    │   ├── 内存不足时自动溢写到磁盘
    │   └── 多条相同 key 的记录 → 用 RecordMerger 合并
    └── 返回 logRecordMap: Map<String, HoodieRecord>
    ↓
Phase 3: 合并 (Merge)
    for each baseRecord in baseRecords:
        if logRecordMap.contains(baseRecord.key):
            logRecord = logRecordMap.remove(baseRecord.key)
            if (logRecord.isDelete):
                skip  // 删除操作
            else:
                yield recordMerger.merge(baseRecord, logRecord)
        else:
            yield baseRecord  // 无更新
    ↓
Phase 4: 输出 Log-only 记录
    for each remaining logRecord in logRecordMap:
        if (!logRecord.isDelete):
            yield logRecord  // 只存在于 log 中的 INSERT 记录
```

### 3.7 HoodieMergedLogRecordScanner — Log 文件扫描

**源码位置**：`hudi-common/src/main/java/org/apache/hudi/common/table/log/HoodieMergedLogRecordScanner.java`

```java
public class HoodieMergedLogRecordScanner extends AbstractHoodieLogRecordScanner {
    // ★ 核心数据结构：record key → 最新的 log record
    private final ExternalSpillableMap<String, HoodieRecord> records;

    @Override
    protected void processNextRecord(HoodieRecord<?> hoodieRecord) {
        String key = hoodieRecord.getRecordKey();
        if (records.containsKey(key)) {
            // 相同 key 的多次更新 → 合并
            HoodieRecord oldRecord = records.get(key);
            HoodieRecord mergedRecord = recordMerger.merge(oldRecord, hoodieRecord);
            records.put(key, mergedRecord);
        } else {
            records.put(key, hoodieRecord);
        }
    }
}
```

**ExternalSpillableMap 机制**：
```
ExternalSpillableMap<K, V>
    ├── inMemoryMap (HashMap)  — 先在内存中存储
    │   └── 大小超过 maxInMemorySizeInBytes 时
    ├── diskBasedMap (DiskBasedMap) — 溢写到磁盘
    │   ├── 使用临时文件存储
    │   └── Key 仍在内存中索引，Value 存磁盘
    └── 配置: hoodie.common.spillable.diskmap.type (BITCASK / ROCKS_DB)
```

### 3.8 三种查询类型详解

| 查询类型 | COW 表 | MOR 表 | 场景 |
|---------|--------|--------|------|
| **snapshot** | 读最新 Base Files | 读 Base + Log Files 合并 | 默认查询方式 |
| **read_optimized** | 等同 snapshot | 只读 Base Files（跳过 Log）| 牺牲新鲜度换取性能 |
| **incremental** | 读指定时间范围的变更 | 读变更文件 + Log 合并 | CDC / 增量同步 |

**增量查询的 V1 vs V2**：

| 维度 | V1 (表版本 < 8) | V2 (表版本 ≥ 8) |
|------|----------------|----------------|
| 排序依据 | requestedTime | completionTime |
| 数据丢失风险 | 并发写入时可能丢失 | 因果一致，不会丢失 |
| 实现类 | `IncrementalRelationV1` | `IncrementalRelationV2` |
| 性能 | 需要读取 instant 文件获取 completionTime | 从文件名直接解析 |

---

## 4. COW 与 MOR 的读写差异

### 4.0 前置理解

#### 4.0.1 解决什么问题

COW 和 MOR 两种表类型要解决的核心问题是：**如何在写入延迟和读取性能之间取得平衡**。

**核心业务问题**：
1. **写入延迟 vs 读取性能的权衡**：
   - COW：写入时重写整个文件，写入慢但读取快
   - MOR：写入时追加 log，写入快但读取慢（需要合并）
2. **存储成本 vs 查询成本**：
   - COW：存储成本低（只有 Parquet 文件），查询成本低
   - MOR：存储成本高（Parquet + Avro log），查询成本高（需要合并）
3. **实时性 vs 吞吐量**：
   - COW：适合批处理场景（每小时/每天更新一次）
   - MOR：适合流式场景（每秒/每分钟更新一次）

**如果没有这个设计会有什么问题**：
- 只有 COW：流式写入场景下写入延迟过高，无法满足实时性要求
- 只有 MOR：批处理场景下读取性能差，查询成本过高

**实际应用场景举例**：
```scala
// 场景1：用户画像表（每天全量更新一次）→ 选择 COW
// 原因：批处理场景，写入延迟不敏感，但查询频繁（需要高性能）
spark.read.parquet("/raw_user_profile")
  .write.format("hudi")
  .option("hoodie.table.type", "COPY_ON_WRITE")
  .option("hoodie.datasource.write.operation", "bulk_insert")
  .mode(SaveMode.Overwrite)
  .save("/user_profile")

// 场景2：订单表（实时 CDC 同步）→ 选择 MOR
// 原因：流式场景，写入延迟敏感，查询相对较少
kafkaStream.writeStream
  .format("hudi")
  .option("hoodie.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.operation", "upsert")
  .option("checkpointLocation", "/checkpoints/orders")
  .start("/orders")

// 场景3：日志表（只追加，不更新）→ 选择 COW + INSERT
// 原因：无需 upsert 能力，COW 的 INSERT 性能等同于原生 Parquet
logData.write.format("hudi")
  .option("hoodie.table.type", "COPY_ON_WRITE")
  .option("hoodie.datasource.write.operation", "insert")
  .save("/logs")
```

**源码证据**：
- `HoodieSparkCopyOnWriteTable` 和 `HoodieSparkMergeOnReadTable` 是两种表类型的实现类（源码位置：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/`）
- `HoodieWriteMergeHandle`（COW）和 `HoodieAppendHandle`（MOR）是两种写入 Handle 的实现（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/`）

#### 4.0.2 有什么坑

**坑1：MOR 表忘记执行 Compaction 导致查询性能持续下降**
- **现象**：MOR 表刚创建时查询很快，但随着时间推移越来越慢
- **原因**：log file 数量不断增加，每次查询需要合并越来越多的 log file
- **源码证据**：`HoodieFileGroupReader` 需要扫描所有 log file 并加载到内存中合并（源码位置：`hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieFileGroupReader.java:134-139`）
- **规避方法**：
  ```properties
  # 方法1：启用内联 Compaction
  hoodie.compact.inline=true
  hoodie.compact.inline.max.delta.commits=5  # 每 5 次 deltacommit 触发一次
  
  # 方法2：定期执行异步 Compaction
  spark.read.format("hudi").load("/table")
    .write.format("hudi")
    .option("hoodie.datasource.write.operation", "compact")
    .save("/table")
  
  # 方法3：使用 Read Optimized 查询（只读 base file）
  spark.read.format("hudi")
    .option("hoodie.datasource.query.type", "read_optimized")
    .load("/table")
  ```

**坑2：COW 表的小更新导致写放大严重**
- **现象**：只更新了 1% 的记录，但写入时间和全量重写差不多
- **原因**：COW 表的 UPDATE 操作需要重写整个 base file，即使只更新一条记录
- **示例**：
  ```
  base file 大小：128MB（100 万条记录）
  更新记录数：1000 条（0.1%）
  写入数据量：128MB（写放大 = 128MB / (1000 * 平均记录大小) ≈ 1000 倍）
  ```
- **源码证据**：`HoodieWriteMergeHandle.doMerge()` 需要读取整个旧 base file 并重写（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieWriteMergeHandle.java`）
- **规避方法**：
  ```properties
  # 方法1：改用 MOR 表
  hoodie.table.type=MERGE_ON_READ
  
  # 方法2：减小文件大小（减少写放大）
  hoodie.parquet.max.file.size=67108864  # 64MB
  
  # 方法3：使用 Clustering 优化文件布局
  hoodie.clustering.inline=true
  ```

**坑3：MOR 表的 Read Optimized 查询数据不一致**
- **现象**：查询结果缺少最近写入的数据，或者数据版本不是最新的
- **原因**：Read Optimized 查询只读 base file，跳过 log file，导致最近的增量数据不可见
- **适用场景**：只有在明确可以容忍数据延迟时才使用（如离线报表、数据分析）
- **源码证据**：`DefaultSource.createRelation()` 中，MOR 表的 `read_optimized` 查询使用 `HoodieCopyOnWriteSnapshotHadoopFsRelationFactory`，只读 base file（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DefaultSource.scala:443-446`）

**坑4：COW 表的并发写入冲突率高**
- **现象**：多个 Spark 作业并发写入 COW 表，频繁出现 `HoodieWriteConflictException`
- **原因**：COW 表的 UPDATE 操作会重写整个 base file，文件级冲突的概率远高于 MOR 表（MOR 只追加 log，不修改 base file）
- **示例**：
  ```
  COW 表：
  Writer A 更新 partition=2024-01-01 的 file-001（重写整个文件）
  Writer B 更新 partition=2024-01-01 的 file-001（冲突！）
  
  MOR 表：
  Writer A 更新 partition=2024-01-01 的 file-001（追加 log-001）
  Writer B 更新 partition=2024-01-01 的 file-001（追加 log-002，无冲突）
  ```
- **规避方法**：
  1. 使用 MOR 表（冲突率更低）
  2. 使用 Bucket Index（预分桶，避免动态文件分配导致的冲突）
  3. 增加分区粒度（减少同一分区的并发写入）

**坑5：MOR 表的 Compaction 失败导致表不可用**
- **现象**：Compaction 作业失败后，表无法读取或写入
- **原因**：Compaction 产生的 `.compaction.inflight` 文件阻塞了后续的写入和查询
- **源码证据**：`HoodieActiveTimeline` 在检测到 pending compaction 时会阻塞某些操作（源码位置：`hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieActiveTimeline.java`）
- **规避方法**：
  ```bash
  # 方法1：回滚失败的 Compaction
  hudi> connect --path /path/to/table
  hudi:table> compaction rollback --instant <compaction_instant>
  
  # 方法2：手动删除 .compaction.inflight 文件（谨慎操作）
  hdfs dfs -rm /path/to/table/.hoodie/<instant>.compaction.inflight
  ```

#### 4.0.3 核心概念解释

**写放大（Write Amplification）**：
- **定义**：实际写入的数据量 / 逻辑更新的数据量
- **COW 表的写放大**：
  ```
  更新 1 条记录（1KB）
  → 读取整个 base file（128MB）
  → 合并后写入新 base file（128MB）
  → 写放大 = 128MB / 1KB = 131072 倍
  ```
- **MOR 表的写放大**：
  ```
  更新 1 条记录（1KB）
  → 追加到 log file（1KB）
  → 写放大 = 1KB / 1KB = 1 倍
  ```
- **影响**：写放大会增加 I/O 开销、网络传输、存储成本

**读放大（Read Amplification）**：
- **定义**：实际读取的数据量 / 逻辑查询的数据量
- **COW 表的读放大**：
  ```
  查询 1 条记录
  → 读取 1 个 base file（128MB）
  → 读放大 = 128MB / 1KB = 131072 倍（但有列式读取优化）
  ```
- **MOR 表的读放大**：
  ```
  查询 1 条记录
  → 读取 1 个 base file（128MB）+ 10 个 log file（10MB）
  → 读放大 = 138MB / 1KB = 141312 倍
  ```
- **影响**：读放大会增加查询延迟、CPU 开销、内存压力

**Compaction（压缩）**：
- **定义**：将 MOR 表的 base file 和 log file 合并，产生新的 base file
- **作用**：
  1. 减少 log file 数量，降低读放大
  2. 将 Avro 格式的 log 转换为 Parquet 格式，提高查询性能
  3. 清理已删除的记录，释放存储空间
- **类型**：
  - **Inline Compaction**：写入时自动触发，增加写入延迟
  - **Async Compaction**：独立的 Compaction 作业，不影响写入延迟
  - **Schedule Compaction**：只调度 Compaction 计划，不执行（由独立作业执行）
- **源码证据**：`BaseHoodieTableServiceClient.runTableServicesInline()` 中根据配置决定是否执行 Compaction（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieTableServiceClient.java:1559-1562`）

**Log Compaction（日志压缩）**：
- **定义**：合并多个 log file 为一个 log file，不产生新的 base file
- **与 Compaction 的区别**：
  - Compaction：base + log → 新 base（Parquet）
  - Log Compaction：log1 + log2 + ... → 新 log（Avro）
- **适用场景**：
  1. log file 数量过多，但不想触发完整的 Compaction（避免重写 base file）
  2. 流式写入场景，需要快速减少 log file 数量
- **源码证据**：`BaseHoodieTableServiceClient.inlineLogCompact()` 实现 Log Compaction（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieTableServiceClient.java:1569-1572`）

**Delta Commit vs Commit**：
- **Delta Commit**：MOR 表的写入操作，产生 `.deltacommit` 文件
  - 表示增量数据已写入 log file
  - 不保证数据已合并到 base file
- **Commit**：COW 表的写入操作，产生 `.commit` 文件
  - 表示数据已写入 base file
  - 保证数据立即可查询
- **Compaction Commit**：MOR 表的 Compaction 操作，产生 `.commit` 文件
  - 表示 log file 已合并到 base file
  - 之后的 Read Optimized 查询可以看到这些数据

#### 4.0.4 设计理念

**理念1：写入延迟与读取性能的可配置权衡**
- **设计决策**：提供 COW 和 MOR 两种表类型，让用户根据业务场景选择
- **权衡矩阵**：
  | 场景 | 写入频率 | 查询频率 | 推荐表类型 | 原因 |
  |------|---------|---------|-----------|------|
  | 批处理 ETL | 低（每小时/每天） | 高 | COW | 写入延迟不敏感，查询性能优先 |
  | 实时 CDC | 高（每秒/每分钟） | 中 | MOR | 写入延迟敏感，可容忍查询开销 |
  | 日志归档 | 高（只追加） | 低 | COW + INSERT | 无需 upsert，COW INSERT 性能高 |
  | 用户画像 | 中（每小时） | 高 | COW | 查询频繁，需要高性能 |
  | 订单表 | 高（实时更新） | 中 | MOR | 写入频繁，查询可用 RO 模式 |

**理念2：MOR 表的多级查询模式**
- **设计决策**：MOR 表提供 Snapshot 和 Read Optimized 两种查询模式
- **好处**：
  1. Snapshot 模式：保证数据一致性，适合需要最新数据的场景
  2. Read Optimized 模式：牺牲新鲜度换取性能，适合离线分析场景
- **实现方式**：
  - Snapshot：`HoodieMergeOnReadSnapshotHadoopFsRelationFactory` → 读取 base + log
  - Read Optimized：`HoodieCopyOnWriteSnapshotHadoopFsRelationFactory` → 只读 base
- **源码证据**：`DefaultSource.createRelation()` 中的 pattern matching 逻辑（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/DefaultSource.scala:441-465`）

**理念3：Compaction 的灵活调度**
- **设计决策**：Compaction 可以配置为内联、异步、或手动触发
- **权衡**：
  - **内联 Compaction**：
    - 优点：自动维护表健康状态，无需额外调度
    - 缺点：增加写入延迟（可能增加 2-5 倍）
  - **异步 Compaction**：
    - 优点：不影响写入延迟
    - 缺点：需要额外的调度系统（如 Airflow）
  - **手动 Compaction**：
    - 优点：完全可控，可以在低峰期执行
    - 缺点：需要人工介入，容易遗忘
- **生产实践**：
  ```properties
  # 流式写入场景：关闭内联，使用异步 Compaction
  hoodie.compact.inline=false
  hoodie.compact.schedule.inline=true  # 只调度，不执行
  # 独立的 Compaction 作业定期执行
  
  # 批处理场景：开启内联 Compaction
  hoodie.compact.inline=true
  hoodie.compact.inline.max.delta.commits=5
  ```

**理念4：COW 表的小文件自动合并**
- **设计决策**：`UpsertPartitioner` 在分配 INSERT 记录时，优先填充小文件
- **好处**：
  1. 避免小文件泛滥（小文件会导致读取时打开大量文件句柄）
  2. 自动维护文件大小在目标范围内
- **权衡**：小文件合并需要读取旧文件，增加写入延迟；但如果不合并，读取性能会持续下降
- **源码证据**：`UpsertPartitioner.assignInserts()` 中遍历小文件并计算剩余容量（源码位置：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/UpsertPartitioner.java:152-180`）

**理念5：MOR 表的 Log File 格式选择**
- **设计决策**：MOR 表的 log file 支持多种格式（Avro、Parquet、HFile）
- **格式对比**：
  | 格式 | 写入性能 | 读取性能 | 压缩率 | 适用场景 |
  |------|---------|---------|--------|---------|
  | Avro | 高（行式写入） | 低（行式读取） | 中 | 默认选择，平衡性能 |
  | Parquet | 中（列式写入） | 高（列式读取） | 高 | 查询频繁的场景 |
  | HFile | 高（KV 写入） | 中（KV 读取） | 中 | HBase 集成场景 |
- **配置**：
  ```properties
  hoodie.log.block.format=avro  # 默认
  # 或
  hoodie.log.block.format=parquet  # 查询性能优先
  ```
- **源码证据**：`HoodieAppendHandle` 根据配置创建不同类型的 `HoodieLogBlock`（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieAppendHandle.java:43-48`）

### 4.1 完整对比

| 维度 | COW (Copy-On-Write) | MOR (Merge-On-Read) |
|------|---------------------|---------------------|
| **写入方式** | 重写整个 base file | 追加到 log file |
| **写入 Action** | `commit` | `deltacommit` |
| **写入 Handle** | `HoodieMergeHandle` | `HoodieAppendHandle` |
| **写入延迟** | 高（读旧+合并+写新） | 低（仅追加） |
| **写放大** | 高 | 低 |
| **读取方式** | 直接读 Parquet | base + log 合并 |
| **读取延迟** | 低 | 较高（取决于 log 数量） |
| **数据新鲜度** | 写入即可见 | Snapshot 查询可见，RO 查询依赖 Compaction |
| **需要 Compaction** | 否 | 是（减少读放大） |
| **适用场景** | 读多写少、批量 ETL | 写多读少、流式写入、CDC |
| **存储格式** | 仅 Parquet/ORC | Parquet/ORC + Avro Log |

### 4.2 文件组织结构

**COW 表文件结构**：
```
table-path/
    ├── .hoodie/
    │   ├── hoodie.properties
    │   └── timeline/
    │       └── T1.commit, T2.commit, ...
    └── partition=2024-01-01/
        ├── fg1-0_0-100-200_T1.parquet     # T1 版本
        └── fg1-0_0-300-400_T2.parquet     # T2 版本（重写后的新文件）
```

**MOR 表文件结构**：
```
table-path/
    ├── .hoodie/
    │   ├── hoodie.properties
    │   └── timeline/
    │       └── T1.deltacommit, T2.deltacommit, T3.compaction, ...
    └── partition=2024-01-01/
        ├── fg1-0_0-100-200_T1.parquet       # Base File (T1 或 Compaction 产出)
        ├── .fg1-0_T2.log.1_0-200-300        # Log File (T2 增量)
        └── .fg1-0_T3.log.2_0-300-400        # Log File (T3 增量)
```

### 4.3 读取路径差异图

```
COW Snapshot Query:
  HoodieFileGroupReaderBasedFileFormat (isMOR=false)
      → HoodieFileGroupReader
          → 只有 BaseFile → 直接 Parquet Reader（向量化读取）
          → 性能 ≈ 原生 Parquet

MOR Snapshot Query:
  HoodieFileGroupReaderBasedFileFormat (isMOR=true)
      → HoodieFileGroupReader
          → BaseFile + LogFiles
          → HoodieMergedLogRecordScanner 扫描 Log
          → 合并 Base + Log Records
          → 性能 < 原生 Parquet（合并开销）

MOR Read Optimized Query:
  HoodieCopyOnWriteSnapshotHadoopFsRelationFactory (复用 COW 逻辑)
      → HoodieFileGroupReader
          → 只看 BaseFile（忽略 LogFiles）
          → 性能 ≈ 原生 Parquet，但数据不是最新
```

---

## 5. Spark SQL 扩展与 Catalog 集成

### 5.1 HoodieSparkSessionExtension

**源码位置**：`hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/HoodieSparkSessionExtension.scala`

启用方式：
```
spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog
```

提供的能力：
- **解析器扩展**：支持 Hudi 特有的 SQL 语法（如 `CALL run_compaction`）
- **分析器扩展**：处理 Hudi 表的 Schema 解析
- **优化器规则**：Hudi 特有的查询优化（如分区裁剪增强）
- **计划器策略**：将逻辑计划转换为物理计划

### 5.2 两种使用方式

**方式一：DataFrame API（DataSource V1）**
```scala
// 写入
df.write.format("hudi")
  .option("hoodie.table.name", "my_table")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .mode(SaveMode.Append)
  .save("/path/to/table")

// 读取
spark.read.format("hudi").load("/path/to/table")
```

**方式二：Spark SQL（需要 HoodieSparkSessionExtension）**
```sql
-- 建表
CREATE TABLE hudi_table (id INT, name STRING, ts LONG)
USING hudi
TBLPROPERTIES (
  primaryKey = 'id',
  type = 'mor',
  preCombineField = 'ts'
);

-- 写入
INSERT INTO hudi_table VALUES (1, 'Alice', 1000);
UPDATE hudi_table SET name='Bob' WHERE id=1;
DELETE FROM hudi_table WHERE id=1;

-- 调用表服务
CALL run_compaction(table => 'hudi_table', op => 'schedule');
CALL run_clustering(table => 'hudi_table');
```

---

## 6. Schema Evolution 支持

### 6.1 InternalSchema 机制

**源码位置**：`hudi-common/src/main/java/org/apache/hudi/internal/schema/InternalSchema.java`

Hudi 使用 InternalSchema 实现**无副作用的 Schema 演进**：每个列都有一个唯一的 column ID，名称变更不影响数据读取。

```java
public class InternalSchema implements Serializable {
    private int maxColumnId;           // 全局最大列 ID
    private Types.RecordType record;   // 列定义（基于 ID）
    private long versionId;            // Schema 版本
}
```

### 6.2 支持的演进操作

| 操作 | 说明 | 安全性 |
|------|------|--------|
| ADD | 添加新列（分配新 ID） | 安全 |
| DROP | 删除列 | 安全（旧文件仍有该列数据，读取时跳过）|
| RENAME | 重命名列 | 安全（通过 ID 追踪，不依赖名称）|
| UPDATE TYPE | 类型宽化（如 int → long） | 安全（向上兼容）|
| REORDER | 调整列顺序 | 安全 |

### 6.3 读时 Schema 对齐流程

```
FileGroupReaderSchemaHandler
    ↓
1. 获取文件的写入时 Schema (fileSchema)
2. 获取当前表 Schema (tableSchema)
3. 比较两者差异:
   ├── 新增列 → 填充 NULL 或默认值
   ├── 删除列 → 跳过该列（不读取）
   ├── 类型变更 → 安全的类型转换 (int→long, float→double)
   └── 重命名 → 通过 column ID 映射
4. 生成对齐后的 projectedSchema
5. 读取时使用 projectedSchema
```

---

## 7. Spark Structured Streaming 集成

### 7.1 架构

`DefaultSource` 同时实现了 `StreamSinkProvider` 和 `StreamSourceProvider`：

**Streaming Sink（写入）**：
```scala
override def createSink(sqlContext, optParams, partitionColumns, outputMode): Sink = {
  new HoodieStreamingSink(sqlContext, optParams, partitionColumns, outputMode)
}
```
- `HoodieStreamingSink` 每个 micro-batch 调用一次 `HoodieSparkSqlWriter.write()`
- 支持 `Append` 和 `Complete` 两种 Output Mode

**Streaming Source（读取）**：
```scala
override def createSource(sqlContext, metadataPath, schema, providerName, parameters): Source = {
  // 根据表版本选择 V1 或 V2
  if (readTableVersion >= HoodieTableVersion.EIGHT.versionCode())
    new HoodieStreamSourceV2(...)  // 基于 completionTime
  else
    new HoodieStreamSourceV1(...)  // 基于 requestedTime
}
```
- 支持 `earliest`、`latest`、指定 instantTime 三种起始偏移量
- 表版本 ≥ 8 使用 V2 Source（基于 completionTime，因果一致）

### 7.2 多 Writer 场景

Streaming 多 Writer 需要配置：
```properties
hoodie.write.concurrency.mode=optimistic_concurrency_control
hoodie.datasource.streaming.checkpoint.identifier=writer_1  # 每个 writer 不同
```

---

## 8. 关键配置参数

### 8.1 写入参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `hoodie.table.type` | `COPY_ON_WRITE` | 表类型 |
| `hoodie.datasource.write.operation` | `upsert` | 写入操作类型 |
| `hoodie.datasource.write.recordkey.field` | - | Record Key 字段 |
| `hoodie.datasource.write.partitionpath.field` | - | 分区字段 |
| `hoodie.datasource.write.precombine.field` | - | Pre-combine 字段 |
| `hoodie.upsert.shuffle.parallelism` | `0` | Upsert 并行度（0表示自动推断）|
| `hoodie.insert.shuffle.parallelism` | `0` | Insert 并行度（0表示自动推断）|
| `hoodie.bulkinsert.shuffle.parallelism` | `0` | Bulk Insert 并行度（0表示自动推断）|
| `hoodie.parquet.max.file.size` | `125829120` (120MB) | Parquet 文件目标大小 |
| `hoodie.merge.small.file.group.candidates.limit` | `1` | 小文件组候选数量限制（仅 MOR 表）|
| `hoodie.write.num.retries.on.conflict.failures` | `0` | 冲突重试次数 |

### 8.2 读取参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `hoodie.datasource.query.type` | `snapshot` | 查询类型 |
| `hoodie.datasource.read.begin.instanttime` | - | 增量读取起始时间 |
| `hoodie.datasource.read.end.instanttime` | - | 增量读取结束时间 |
| `hoodie.enable.data.skipping` | `true` | 是否启用数据跳过 |
| `hoodie.metadata.enable` | `true` | 是否启用 Metadata Table |
| `hoodie.datasource.read.use.new.parquet.file.format` | `true` | 是否使用 FileGroupReader |

### 8.3 索引参数（影响写入性能）

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `hoodie.index.type` | `SIMPLE` (Spark/Java) / `INMEMORY` (Flink) | 索引类型（引擎相关）|
| `hoodie.index.bucket.engine` | `SIMPLE` | Bucket 引擎类型 |
| `hoodie.bucket.index.num.buckets` | `256` | 桶数量 |

---

## 9. 性能优化深度解析

### 9.0 前置理解

#### 9.0.1 解决什么问题

性能优化要解决的核心问题是：**如何在保证数据一致性和功能完整性的前提下，最大化 Hudi 表的读写吞吐量和最小化查询延迟**。

**核心业务问题**：
1. **写入吞吐量瓶颈**：大规模数据写入时（TB 级别），如何提高写入速度
2. **查询延迟过高**：大表查询（> 10 万个文件）时，如何减少查询规划和执行时间
3. **资源利用率低**：Spark 集群资源（CPU、内存、网络）没有充分利用
4. **小文件问题**：频繁写入导致小文件泛滥，影响读取性能

**如果没有这个设计会有什么问题**：
- 写入速度慢，无法满足实时性要求
- 查询延迟高，影响用户体验
- 资源浪费，增加成本
- 小文件导致元数据膨胀，影响整个集群性能

**实际应用场景举例**：
```scala
// 场景1：大规模批量导入（1TB 数据）
// 优化前：写入时间 2 小时，产生 10 万个小文件
// 优化后：写入时间 30 分钟，产生 8000 个文件
rawData.write.format("hudi")
  .option("hoodie.datasource.write.operation", "bulk_insert")
  .option("hoodie.bulkinsert.shuffle.parallelism", "8000")  // 根据数据量计算
  .option("hoodie.parquet.max.file.size", "134217728")      // 128MB
  .save("/large_table")

// 场景2：高频实时写入（每秒 10 万条记录）
// 优化前：写入延迟 10 秒，查询延迟 5 秒
// 优化后：写入延迟 1 秒，查询延迟 500ms
kafkaStream.writeStream.format("hudi")
  .option("hoodie.table.type", "MERGE_ON_READ")             // 使用 MOR 降低写入延迟
  .option("hoodie.index.type", "BUCKET")                    // 使用 Bucket Index 避免索引查找
  .option("hoodie.bucket.index.num.buckets", "256")
  .option("hoodie.compact.inline=false")                    // 关闭内联 Compaction
  .start("/realtime_table")

// 场景3：大表点查（10 亿条记录，查询单条记录）
// 优化前：查询延迟 30 秒（全表扫描）
// 优化后：查询延迟 100ms（精确定位）
spark.read.format("hudi")
  .option("hoodie.metadata.enable", "true")
  .option("hoodie.metadata.index.record.index.enable", "true")  // 启用 Record Level Index
  .load("/large_table")
  .where("user_id = '12345'")
```

**源码证据**：
- `UpsertPartitioner` 根据并行度和平均记录大小自动计算每个 bucket 的容量（源码位置：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/UpsertPartitioner.java:90-109`）
- `HoodieFileIndex` 利用 Metadata Table 的多种分区类型进行查询优化（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieFileIndex.scala`）

#### 9.0.2 有什么坑

**坑1：并行度设置过高导致小文件泛滥**
- **现象**：写入后产生大量小文件（< 10MB），查询性能下降
- **原因**：并行度设置过高，每个 task 处理的数据量太少
- **示例**：
  ```
  数据量：100GB
  并行度：10000
  每个 task 处理：100GB / 10000 = 10MB
  → 产生 10000 个 10MB 的小文件
  ```
- **正确配置**：
  ```properties
  # 并行度 = 数据量 / 目标文件大小
  # 例：100GB / 128MB ≈ 800
  hoodie.upsert.shuffle.parallelism=800
  # 或使用自动推断（0.13.0+）
  hoodie.upsert.shuffle.parallelism=0
  ```

**坑2：Metadata Table 未启用导致查询规划慢**
- **现象**：查询执行很快，但查询规划阶段耗时很长（几分钟）
- **原因**：没有启用 Metadata Table，Spark 需要调用 `fs.listStatus()` 遍历所有文件
- **示例**：
  ```
  表文件数：10 万个
  LIST 操作延迟：10ms/文件
  总延迟：10 万 × 10ms = 1000 秒 ≈ 16 分钟
  ```
- **正确配置**：
  ```properties
  hoodie.metadata.enable=true
  hoodie.metadata.index.column.stats.enable=true
  hoodie.enable.data.skipping=true
  ```

**坑3：索引类型选择不当导致写入慢**
- **现象**：写入延迟很高，查看日志发现大量时间花在 `Index.tagLocation()` 阶段
- **原因**：使用了不适合的索引类型（如大表使用 SIMPLE Index）
- **索引选择指南**：
  | 数据量 | 推荐索引 | 原因 |
  |--------|---------|------|
  | < 1 亿 | SIMPLE | 内存占用小，查找快 |
  | 1-10 亿 | BLOOM | 平衡内存和性能 |
  | > 10 亿 | BUCKET | 无需查找索引，性能最优 |
  | 频繁点查 | RECORD_LEVEL_INDEX | 精确定位，查询最快 |
- **源码证据**：`SparkHoodieIndexFactory.createIndex()` 根据配置创建不同类型的索引（源码位置：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/index/SparkHoodieIndexFactory.java`）

**坑4：小文件合并导致写入延迟增加**
- **现象**：写入延迟突然增加，查看日志发现大量 "Merging small file" 信息
- **原因**：`UpsertPartitioner` 默认会将 INSERT 记录优先填充到小文件中
- **权衡**：
  - 开启小文件合并：写入延迟增加，但读取性能好
  - 关闭小文件合并：写入延迟低，但小文件泛滥
- **配置**：
  ```properties
  # 方法1：提高小文件阈值（减少被识别为小文件的数量）
  hoodie.parquet.small.file.limit=104857600  # 100MB
  
  # 方法2：关闭小文件合并（MOR 表）
  hoodie.merge.small.file.group.candidates.limit=0
  
  # 方法3：使用 bulk_insert（不做小文件合并）
  hoodie.datasource.write.operation=bulk_insert
  ```

**坑5：MOR 表的 Compaction 阻塞写入**
- **现象**：MOR 表写入时偶尔会卡住很久，查看日志发现在执行 Compaction
- **原因**：启用了内联 Compaction（`hoodie.compact.inline=true`），Compaction 在写入线程中同步执行
- **影响**：
  ```
  正常写入延迟：1 秒
  Compaction 延迟：60 秒
  → 每 5 次写入就有一次延迟 60 秒
  ```
- **规避方法**：
  ```properties
  # 关闭内联 Compaction，使用异步 Compaction
  hoodie.compact.inline=false
  hoodie.compact.schedule.inline=true  # 只调度，不执行
  # 独立的 Compaction 作业定期执行
  ```

#### 9.0.3 核心概念解释

**并行度（Parallelism）**：
- **定义**：Spark 写入时的 RDD partition 数量，决定了有多少个 task 并行执行
- **影响**：
  - 并行度过低：无法充分利用集群资源，写入慢
  - 并行度过高：产生大量小文件，增加调度开销
- **计算公式**：
  ```
  最优并行度 = 数据量 / 目标文件大小
  例：100GB / 128MB ≈ 800
  ```
- **自动推断**（0.13.0+）：
  ```properties
  hoodie.upsert.shuffle.parallelism=0  # 使用 Spark 推断的并行度
  ```
- **源码证据**：`HoodieSparkSqlWriterInternal.writeInternal()` 中根据并行度配置决定 shuffle partition 数量（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieSparkSqlWriter.scala`）

**Data Skipping（数据跳过）**：
- **定义**：利用文件级别的统计信息（min/max/null_count）跳过不满足 WHERE 条件的文件
- **层次**：
  1. **分区裁剪**：根据分区字段过滤分区（Spark 原生支持）
  2. **列统计裁剪**：根据列的 min/max 值过滤文件（Hudi Metadata Table 提供）
  3. **Bloom Filter 裁剪**：根据 Bloom Filter 判断文件是否可能包含指定的 Record Key
  4. **Record Level Index**：精确定位 Record Key 所在的 FileGroup
- **性能提升**：
  ```
  查询：SELECT * FROM table WHERE amount > 1000
  
  无 Data Skipping：
  - 扫描文件数：10000
  - 查询时间：60 秒
  
  有 Data Skipping：
  - 扫描文件数：500（95% 的文件被跳过）
  - 查询时间：3 秒（20 倍提升）
  ```
- **源码证据**：`HoodieFileIndex.listFiles()` 中使用 `COLUMN_STATS` 分区进行数据跳过（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieFileIndex.scala`）

**Bucket Index（桶索引）**：
- **定义**：预先将数据分配到固定数量的桶（bucket）中，每个桶对应一个 FileGroup
- **优势**：
  1. 写入时无需查找索引（直接根据 hash 值确定桶号）
  2. 避免了 Index.tagLocation() 的开销
  3. 支持高并发写入（不同桶之间无冲突）
- **工作原理**：
  ```
  Record Key: "user_12345"
  Hash: MD5("user_12345") % 256 = 123
  → 分配到 bucket-123
  → 写入 FileGroup: fg-123
  ```
- **适用场景**：
  - 大表（> 10 亿记录）
  - 高频写入（每秒 > 1 万条记录）
  - 点查场景（根据 Record Key 查询）
- **配置**：
  ```properties
  hoodie.index.type=BUCKET
  hoodie.bucket.index.num.buckets=256  # 桶数量（建议 2 的幂次）
  hoodie.bucket.index.hash.field=user_id  # 用于 hash 的字段
  ```
- **源码证据**：`SparkBucketIndexPartitioner` 根据 hash 值分配记录到桶（源码位置：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/index/bucket/SparkBucketIndexPartitioner.java`）

**Record Level Index（记录级索引）**：
- **定义**：在 Metadata Table 的 RECORD_INDEX 分区中，为每个 Record Key 维护精确的 FileGroup 映射
- **优势**：
  1. 点查性能最优（直接定位到 FileGroup，无需扫描文件）
  2. 支持全局去重（跨分区的 Record Key 唯一性）
- **劣势**：
  1. 写入开销高（每次写入需要更新索引）
  2. 存储开销高（索引大小 ≈ 记录数 × 平均 Record Key 长度）
- **适用场景**：
  - 频繁点查（根据 Record Key 查询）
  - 需要全局去重的场景
- **配置**：
  ```properties
  hoodie.metadata.enable=true
  hoodie.metadata.index.record.index.enable=true
  ```
- **性能对比**：
  ```
  查询：SELECT * FROM table WHERE user_id = '12345'
  
  无 Record Level Index：
  - 扫描文件数：10000（全表扫描）
  - 查询时间：30 秒
  
  有 Record Level Index：
  - 扫描文件数：1（精确定位）
  - 查询时间：100ms（300 倍提升）
  ```

**Clustering（聚簇）**：
- **定义**：根据指定的排序键重新组织文件布局，将相关数据聚集在一起
- **作用**：
  1. 提高 Data Skipping 效果（相同值的记录聚集在一起，min/max 范围更窄）
  2. 减少文件数量（合并小文件）
  3. 优化查询性能（减少扫描的文件数）
- **示例**：
  ```
  Clustering 前：
  file1: user_id [1, 1000]
  file2: user_id [500, 1500]
  file3: user_id [1, 2000]
  → 查询 user_id=1200 需要扫描 3 个文件
  
  Clustering 后（按 user_id 排序）：
  file1: user_id [1, 500]
  file2: user_id [501, 1000]
  file3: user_id [1001, 2000]
  → 查询 user_id=1200 只需扫描 1 个文件
  ```
- **配置**：
  ```properties
  hoodie.clustering.inline=true
  hoodie.clustering.inline.max.commits=4  # 每 4 次 commit 触发一次
  hoodie.clustering.plan.strategy.sort.columns=user_id,create_time
  ```
- **源码证据**：`BaseHoodieTableServiceClient.inlineClustering()` 实现内联 Clustering（源码位置：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieTableServiceClient.java:1574-1577`）

#### 9.0.4 设计理念

**理念1：分层的性能优化策略**
- **设计决策**：性能优化分为三个层次：
  1. **写入优化**：索引选择、并行度调整、小文件合并
  2. **存储优化**：Compaction、Clustering、文件格式选择
  3. **查询优化**：Metadata Table、Data Skipping、向量化读取
- **好处**：用户可以根据业务场景选择合适的优化策略，而不是一刀切
- **权衡**：某些优化会增加写入开销（如 Clustering），但换来查询性能的提升

**理念2：自适应的并行度推断**
- **设计决策**：0.13.0+ 版本支持自动推断并行度（`parallelism=0`）
- **好处**：
  1. 用户无需手动计算并行度
  2. 根据数据量动态调整，避免小文件或资源浪费
- **实现方式**：使用 Spark 推断的 shuffle partition 数量（基于输入数据的 partition 数量）
- **源码证据**：`HoodieSparkSqlWriterInternal.writeInternal()` 中根据并行度配置决定 shuffle partition 数量（源码位置：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieSparkSqlWriter.scala`）

**理念3：Metadata Table 的渐进式启用**
- **设计决策**：Metadata Table 的不同分区可以独立启用
- **好处**：
  1. 用户可以根据需求选择启用哪些分区（如只启用 FILES 分区，不启用 COLUMN_STATS）
  2. 降低写入开销（不需要的分区不维护）
- **推荐启用顺序**：
  1. FILES 分区（几乎无开销，强烈推荐）
  2. COLUMN_STATS 分区（中等开销，适合大表的范围查询）
  3. BLOOM_FILTERS 分区（中等开销，适合点查场景）
  4. RECORD_INDEX 分区（高开销，适合频繁点查的场景）
- **源码证据**：`MetadataPartitionType` 枚举定义了所有分区类型（源码位置：`hudi-common/src/main/java/org/apache/hudi/metadata/MetadataPartitionType.java`）

**理念4：索引的可插拔设计**
- **设计决策**：Hudi 支持多种索引类型（SIMPLE、BLOOM、BUCKET、RECORD_LEVEL_INDEX），用户可以根据场景选择
- **好处**：
  1. 灵活性：不同场景使用不同索引
  2. 可扩展性：社区可以贡献新的索引实现
- **索引选择矩阵**：
  | 场景 | 数据量 | 写入频率 | 查询模式 | 推荐索引 |
  |------|--------|---------|---------|---------|
  | 小表 | < 1 亿 | 低 | 任意 | SIMPLE |
  | 中表 | 1-10 亿 | 中 | 范围查询 | BLOOM |
  | 大表 | > 10 亿 | 高 | 任意 | BUCKET |
  | 点查 | 任意 | 低 | 点查 | RECORD_LEVEL_INDEX |
- **源码证据**：`SparkHoodieIndexFactory.createIndex()` 根据配置创建不同类型的索引（源码位置：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/index/SparkHoodieIndexFactory.java`）

**理念5：表服务的异步执行**
- **设计决策**：Compaction、Clustering、Clean、Archive 等表服务可以配置为异步执行
- **好处**：
  1. 不影响写入延迟（表服务在独立的作业中执行）
  2. 可以在低峰期执行（节省资源）
  3. 可以使用不同的资源配置（如 Compaction 使用更多内存）
- **权衡**：需要额外的调度系统（如 Airflow），增加运维复杂度
- **生产实践**：
  ```properties
  # 流式写入场景：关闭内联，使用异步表服务
  hoodie.compact.inline=false
  hoodie.compact.schedule.inline=true  # 只调度，不执行
  hoodie.clustering.inline=false
  hoodie.clustering.schedule.inline=true
  
  # 批处理场景：开启内联表服务
  hoodie.compact.inline=true
  hoodie.clustering.inline=true
  ```

### 9.1 写入性能优化

**1. 选择合适的索引**（最关键）
```
数据量 < 1 亿:    SIMPLE（简单高效）
数据量 1-10 亿:   BLOOM 或 BUCKET
数据量 > 10 亿:   BUCKET (SIMPLE engine) 或 RECORD_LEVEL_INDEX
Flink 流式写入:   FLINK_STATE（默认）
```

**2. 调整并行度**
```properties
# 从 Hudi 0.13.0 开始，默认值为 0（自动推断，基于 Spark 源数据并行度）
# 0.13.0 之前默认值为 200
# 如需手动指定：并行度 ≈ 数据量 / 目标文件大小
# 例：100GB 数据 / 128MB 文件 ≈ 800
hoodie.upsert.shuffle.parallelism=800
# 注意：设置为 0 时，Hudi 会自动使用 Spark 推断的并行度，通常是最优选择
```

**3. 小文件合并**
```properties
# 调整目标文件大小（影响小文件判断阈值）
hoodie.parquet.max.file.size=134217728  # 128MB
# MOR 表限制小文件组候选数量（设为 0 可关闭小文件合并）
hoodie.merge.small.file.group.candidates.limit=0
```

### 9.2 读取性能优化

**1. 启用 Metadata Table + Data Skipping**
```properties
hoodie.metadata.enable=true
hoodie.metadata.index.column.stats.enable=true
hoodie.enable.data.skipping=true
```

**2. MOR 表优化**
```
定期 Compaction → 减少 log files 数量 → 降低读放大
Clustering + 排序 → 提高数据局部性 → Data Skipping 更有效
```

**3. 向量化读取**
```
COW 表自动享受 Spark Parquet 向量化读取
MOR 表在 Base File 部分也支持向量化，Log 合并部分需要回退到行模式
```

### 9.3 与 Iceberg 读写流程的关键差异

| 维度 | Hudi | Iceberg |
|------|------|---------|
| **写入模型** | Index + Tag + Write（基于索引定位）| Append-only + Delete files |
| **更新方式** | COW 重写 / MOR 追加 log | Copy-on-Write / Merge-on-Read (v2) |
| **索引机制** | 内置多种（Bloom/Bucket/RLI） | 无内置索引，依赖 Manifest 统计 |
| **文件组织** | FileGroup（base + log 绑定）| 扁平文件 + Manifest 关联 |
| **读取框架** | HadoopFsRelation + FileGroupReader | HadoopFsRelation + Iceberg Reader |
| **Spark 集成深度** | SessionExtension + Catalog | SessionExtension + Catalog |
| **删除实现** | COW 合并 / MOR log 标记 | Position Delete / Equality Delete |
| **Compaction** | 内置（Hudi Table Service）| 内置（Iceberg RewriteDataFilesAction）|

---

## 10. Spark 读写源码深度剖析

### 10.1 HoodieSparkSqlWriterInternal.writeInternal 完整流程

**源码位置**：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieSparkSqlWriter.scala`

`writeInternal` 是整个 Spark 写入 Hudi 的核心枢纽方法，从参数解析到最终 commit 的所有逻辑都在这里协调完成。以下是经过源码验证的完整分步解析。

**第一步：路径校验与表存在性检查**

```scala
assert(optParams.get("path").exists(!StringUtils.isNullOrEmpty(_)), "'path' must be set")
val path = optParams("path")
val basePath = new Path(path)
val fs = basePath.getFileSystem(sparkContext.hadoopConfiguration)
tableExists = fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
```

方法首先检查 `path` 参数是否存在，然后通过检测 `.hoodie` 元数据目录判断表是否已经存在。这个 `tableExists` 布尔值在后续的表自动创建和 SaveMode 处理中起关键作用。

**第二步：参数合并与 KeyGenerator 校验**

```scala
var tableConfig = getHoodieTableConfig(sparkContext, path, mode, ...)
val paramsWithoutDefaults = HoodieWriterUtils.getParamsWithAlternatives(optParams)
validateKeyGeneratorConfig(originKeyGeneratorClassName, tableConfig)
validateTableConfig(sparkSession, optParams, tableConfig, mode == SaveMode.Overwrite)
var (parameters, hoodieConfig) = mergeParamsAndGetHoodieConfig(optParams, tableConfig, mode, ...)
```

这里有一个重要的设计决策：Hudi 会将用户传入的参数（`optParams`）与已有表配置（`tableConfig`）进行合并，确保已有表的配置不被意外覆盖。同时 `validateKeyGeneratorConfig` 会校验数据源层指定的 KeyGenerator 与表配置中的 KeyGenerator 是否一致，防止 Record Key 生成方式不匹配导致数据错乱。

**第三步：操作类型推断（deduceOperation）**

```scala
val operation = deduceOperation(hoodieConfig, paramsWithoutDefaults, sourceDf)
```

`deduceOperation` 方法有一段精妙的自动推断逻辑：如果用户没有指定 `recordkey.field`，也没有显式设置 `operation`，且 DataFrame 中不包含 `_hoodie_record_key` 元数据字段，则自动将操作类型设为 `BULK_INSERT`。这是因为没有 Record Key 意味着无法做 upsert（需要根据 key 定位记录），只能做纯追加写入。这个设计让 Hudi 在面对无主键数据（如日志流）时依然能正常工作。

**第四步：表自动创建（initTable / createTableIfNotExists）**

```scala
val tableMetaClient = if (tableExists) {
  HoodieTableMetaClient.builder.setConf(...).setBasePath(path).build()
} else {
  HoodieTableMetaClient.newTableBuilder()
    .setTableType(tableType)
    .setTableVersion(tableVersion)
    .setDatabaseName(databaseName)
    .setTableName(tblName)
    .setBaseFileFormat(baseFileFormat)
    .setRecordKeyFields(hoodieConfig.getString(RECORDKEY_FIELD))
    .setPartitionFields(partitionColumnsForKeyGenerator)
    // ... 约 20 个配置项
    .initTable(HadoopFSUtils.getStorageConfWithCopy(...), path)
}
```

当表不存在时，`writeInternal` 会通过 `HoodieTableMetaClient.newTableBuilder()` 自动创建表。这个 builder 模式非常关键，它一次性设置了表的所有核心属性（表类型、Record Key 字段、分区字段、KeyGenerator 类、RecordMergeMode 等），然后调用 `initTable` 在目标路径创建 `.hoodie` 目录和 `hoodie.properties` 文件。**为什么这么设计**：让 DataFrame API 用户无需手动建表，降低使用门槛；所有表元数据集中在 `hoodie.properties` 中，确保后续读写的一致性。

**第五步：Schema 推断与对齐**

```scala
val latestTableSchemaOpt = getLatestTableSchema(tableMetaClient, schemaFromCatalog)
val sourceSchema = convertStructTypeToHoodieSchema(df.schema, avroRecordName, avroRecordNamespace)
val writerSchema = HoodieSchemaUtils.deduceWriterSchema(
  sourceSchema, latestTableSchemaOpt, internalSchemaOpt, parameters)
```

Schema 推断分三层：(1) 从 DataFrame 获取源 Schema；(2) 从表的最近一次 commit 获取已有表 Schema；(3) 通过 `deduceWriterSchema` 进行三方调和——如果启用了 Schema Evolution，还会考虑 `InternalSchema` 的列 ID 映射。这里 `avroRecordName` 和 `avroRecordNamespace` 的处理确保了 Avro Schema 在 Catalyst StructType 和 Avro Schema 之间转换时不丢失名称信息，这对 Schema 兼容性检查至关重要。

**第六步：HoodieRecord RDD 构建**

```scala
val hoodieRecords: JavaRDD[HoodieRecord[_]] = HoodieCreateRecordUtils.createHoodieRecordRdd(
  HoodieCreateRecordUtils.createHoodieRecordRddArgs(df, writeConfig, parameters, avroRecordName,
    avroRecordNamespace, writerSchema, processedDataSchema, operation, instantTime, ...))
```

这一步将 Spark DataFrame 转换为 `JavaRDD[HoodieRecord]`。内部会使用 `KeyGenerator` 为每条记录生成 `HoodieKey`（包含 Record Key 和 Partition Path），并将 Spark 的 `InternalRow` 转换为 Hudi 的内部记录表示。对于 prepped write（SQL 层预处理过的写入），会跳过 KeyGenerator，直接从元数据字段中提取 key。

**第七步：执行写入与 Commit**

```scala
instantTime = client.startCommit(commitActionType)
val writeResult = DataSourceUtils.doWriteOperation(client, dedupedHoodieRecords, instantTime, operation, ...)
val (writeSuccessful, compactionInstant, clusteringInstant) =
  commitAndPerformPostOperations(sparkSession, df.schema, writeResult, parameters, writeClient, ...)
```

`startCommit` 在 Timeline 上创建一个 REQUESTED -> INFLIGHT 的 instant，`doWriteOperation` 根据 operation 类型分发到 `client.upsert()` / `client.insert()` / `client.bulkInsert()` 等方法，最后 `commitAndPerformPostOperations` 完成 commit 并触发后续的 MetaSync 和异步 Compaction/Clustering 调度。

**设计优势**：将整个写入流程封装在一个方法中，配合外层的冲突重试循环（`HoodieSparkSqlWriterInternal.write`），实现了"失败-重试"的原子性语义。每次重试都会重新获取 `instantTime`，避免使用已被其他 writer 占用的时间戳。

---

### 10.2 WorkloadProfile 与 UpsertPartitioner 的协作

**核心源码位置**：
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/WorkloadProfile.java`
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/WorkloadStat.java`
- `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/UpsertPartitioner.java`
- `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/BaseSparkCommitActionExecutor.java`

#### WorkloadProfile 的构建过程

WorkloadProfile 在 `BaseSparkCommitActionExecutor.execute()` 中通过 `prepareWorkloadProfile()` 构建。其内部调用 `buildProfile()` 方法统计每个分区的 INSERT/UPDATE 分布：

```java
// BaseSparkCommitActionExecutor.buildProfile()
Map<Tuple2<String, Option<HoodieRecordLocation>>, Long> partitionLocationCounts = inputRecords
    .mapToPair(record -> Pair.of(
        new Tuple2<>(record.getPartitionPath(), Option.ofNullable(record.getCurrentLocation())), record))
    .countByKey();
```

这里的关键在于 `record.getCurrentLocation()`——经过 `Index.tagLocation()` 标记后，已存在的记录会携带 `HoodieRecordLocation`（包含 fileId 和 commitTime），新记录的 location 为 null。`countByKey()` 会触发一次 Spark action，统计出每个 `(partitionPath, location)` 组合的记录数量。

`WorkloadStat` 内部维护两个核心数据结构：
- `updateLocationToCount: HashMap<String, Pair<String, Long>>` —— fileId 到 (commitTime, 记录数) 的映射，精确记录每个 FileGroup 需要更新多少条记录
- `numInserts: long` —— 该分区的新增记录总数

**为什么这么设计**：将工作负载分析与实际写入解耦，让 Partitioner 能够基于精确的统计信息做出最优的文件分配决策，而不是盲目地按记录顺序写入。

#### UpsertPartitioner 的文件分配策略

UpsertPartitioner 的构造函数是整个文件分配逻辑的核心入口：

```java
public UpsertPartitioner(WorkloadProfile profile, HoodieEngineContext context,
                         HoodieTable table, HoodieWriteConfig config, WriteOperationType operationType) {
    // ...
    assignUpdates(profile);   // 第一步：为 UPDATE 记录分配 bucket
    if (!WriteOperationType.isPreppedWriteOperation(operationType) || totalInserts > 0) {
        assignInserts(profile, context);  // 第二步：为 INSERT 记录分配 bucket
    }
}
```

**UPDATE 分配（assignUpdates）**：每个需要更新的 FileGroup 对应一个独立的 bucket（即一个 Spark partition）。这意味着同一个 FileGroup 的所有更新记录会被路由到同一个 task 处理，确保 `HoodieWriteMergeHandle` 能够一次性完成该文件的合并。

```java
private int addUpdateBucket(String partitionPath, String fileIdHint) {
    int bucket = totalBuckets;
    updateLocationToBucket.put(fileIdHint, bucket);
    BucketInfo bucketInfo = new BucketInfo(BucketType.UPDATE, fileIdHint, partitionPath);
    bucketInfoMap.put(totalBuckets, bucketInfo);
    totalBuckets++;
    return bucket;
}
```

**INSERT 分配（assignInserts）**：这是小文件治理的核心逻辑，分为三个阶段：

1. **小文件填充**：遍历该分区所有小于 `hoodie.parquet.small.file.limit`（默认 100MB）的文件，计算每个小文件还能容纳多少记录：`recordsToAppend = (maxFileSize - currentFileSize) / averageRecordSize`。将这些 INSERT 记录分配到已有的小文件 bucket 中（可能复用 UPDATE bucket）。

2. **新文件创建**：如果填充小文件后仍有剩余 INSERT 记录，则创建新的 INSERT bucket。每个 bucket 的容量通过 `shouldAutoTuneInsertSplits()` 自动计算：`insertRecordsPerBucket = ceil(maxFileSize / averageRecordSize)`。

3. **权重分配**：为所有 INSERT bucket 计算累积权重（cumulative weight），用于在 `getPartition()` 时通过 hash + 二分查找将记录均匀分配到各个 bucket：

```java
public int getPartition(Object key) {
    if (keyLocation._2().isPresent()) {
        // UPDATE：直接根据 fileId 查找 bucket
        return updateLocationToBucket.get(location.getFileId());
    } else {
        // INSERT：基于 recordKey 的 MD5 hash 和权重分配
        final long hashOfKey = NumericUtils.getMessageDigestHash("MD5", keyLocation._1().getRecordKey());
        final double r = 1.0 * Math.floorMod(hashOfKey, totalInserts) / totalInserts;
        int index = Collections.binarySearch(targetBuckets, new InsertBucketCumulativeWeightPair(..., r));
        // ... 返回对应的 bucket number
    }
}
```

**设计优势**：
1. **小文件自动治理**：INSERT 优先填充小文件，避免产生过多碎片文件，减少后续读取时的 LIST 操作开销
2. **UPDATE 局部性**：同一个 FileGroup 的更新集中在一个 task，减少不必要的文件读取
3. **自适应分桶**：基于历史 commit 的平均记录大小自动调整每个 bucket 的记录数，避免 OOM 或文件过小
4. **排除 Pending Clustering 文件**：通过 `filterSmallFilesInClustering()` 避免向正在 Clustering 的文件组写入，防止冲突

---

### 10.3 HoodieCreateHandle vs HoodieWriteMergeHandle vs HoodieAppendHandle 的完整对比

**源码位置**：
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieCreateHandle.java`（继承 `BaseCreateHandle`）
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieWriteMergeHandle.java`（继承 `HoodieAbstractMergeHandle`，实现 `HoodieMergeHandle` 接口）
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieAppendHandle.java`（继承 `HoodieWriteHandle`）

#### HoodieCreateHandle（BaseCreateHandle）—— 创建新 Base File

**适用场景**：INSERT bucket（UpsertPartitioner 分配的 `BucketType.INSERT`），以及 Compaction 产出新的 base file。

**构造逻辑**：
```java
// HoodieCreateHandle 构造函数
public HoodieCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable hoodieTable,
                          String partitionPath, String fileId, ...) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, ...);
    createPartitionMetadataAndMarkerFile();   // 创建分区元数据和 marker 文件
    this.fileWriter = initializeFileWriter(); // 初始化 Parquet/ORC Writer
}
```

`createPartitionMetadataAndMarkerFile()` 做两件事：(1) 在分区目录下创建 `.hoodie_partition_metadata` 文件记录分区格式；(2) 创建 marker 文件用于追踪未完成的写入（如果写入失败，rollback 会根据 marker 清理半成品文件）。

**写入逻辑**（`BaseCreateHandle.doWrite()`）：
```java
protected void doWrite(HoodieRecord record, HoodieSchema schema, TypedProperties props) {
    if (!HoodieOperation.isDelete(record.getOperation()) && !record.isDelete(...)) {
        writeRecordToFile(record, schema);  // 直接写入 Parquet file
        record.setNewLocation(newRecordLocation);
        recordsWritten++;
        insertRecordsWritten++;
    } else {
        recordsDeleted++;  // DELETE 记录直接跳过（不写入文件）
    }
}
```

**关闭逻辑**（`BaseCreateHandle.close()`）：关闭 fileWriter（触发 Parquet footer 写入），通过文件系统获取实际文件大小，填充 `WriteStatus` 统计信息。

**性能特征**：
- 无需读取任何旧数据，纯顺序写入
- 写入效率等同于原生 Parquet Writer
- 文件大小受 `hoodie.parquet.max.file.size` 控制

#### HoodieWriteMergeHandle —— COW 表的 Base File 合并重写

**适用场景**：UPDATE bucket（COW 表更新已有 FileGroup），以及 Compaction 路径。

**构造逻辑**：
```java
public HoodieWriteMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable hoodieTable,
                              Iterator<HoodieRecord<T>> recordItr, String partitionPath, String fileId, ...) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier, baseFile, ...);
    populateIncomingRecordsMap(recordItr);   // 将新记录放入 ExternalSpillableMap
    initMarkerFileAndFileWriter(fileId, partitionPath);  // 创建新文件的 Writer
}
```

`populateIncomingRecordsMap` 将所有传入的新记录加载到 `keyToNewRecords`（`ExternalSpillableMap`）中。这是一个支持磁盘溢写的 Map，当内存不足时自动将 value 溢写到磁盘（使用 BitCask 或 RocksDB），key 仍保留在内存中用于快速查找。

**合并逻辑**（`doMerge()` -> `HoodieMergeHelper.runMerge()`）：
遍历旧 base file 的每一条记录，检查 `keyToNewRecords` 中是否有匹配的新记录。如果有，使用 `RecordMerger` 合并两条记录（支持自定义合并策略），将合并结果写入新文件。如果没有，直接将旧记录写入新文件。遍历完毕后，将 `keyToNewRecords` 中剩余的记录（纯 INSERT）也写入新文件。

**关闭逻辑**（`close()`）：关闭 fileWriter 和 baseFileReader，填充 WriteStatus。

**性能特征**：
- **写放大严重**：即使只更新 1 条记录，也需要读取整个旧 base file 并重写
- **内存压力**：所有新记录需要加载到内存 Map（或溢写到磁盘）
- **好处是读取零开销**：写入完成后产出的就是最终的 Parquet 文件，读取不需要任何合并

**MergeHandle 的变体家族**（通过 `HoodieMergeHandleFactory` 选择）：

| 实现类 | 使用场景 | 特点 |
|--------|---------|------|
| `HoodieWriteMergeHandle` | 默认的 COW 更新 | 标准合并重写 |
| `HoodieSortedMergeHandle` | 要求记录排序的表 | 合并后对记录排序再写入 |
| `HoodieMergeHandleWithChangeLog` | CDC 启用时 | 额外写入 CDC log 记录变更详情 |
| `HoodieConcatHandle` | INSERT 操作且允许重复 | 不做合并，直接追加新记录到旧文件末尾 |
| `FileGroupReaderBasedMergeHandle` | 基于 FileGroupReader 的合并 | 使用统一的 FileGroupReader 读取 base+log 后合并 |

`HoodieMergeHandleFactory.getMergeHandleClassesWrite()` 中的选择逻辑：如果表需要排序 -> `HoodieSortedMergeHandle`；如果是非变更操作且允许重复 -> `HoodieConcatHandle`；如果启用了 CDC -> `HoodieMergeHandleWithChangeLog`；否则使用配置中指定的 merge handle 类（默认 `FileGroupReaderBasedMergeHandle`）。

#### HoodieAppendHandle —— MOR 表的 Log File 追加

**适用场景**：MOR 表的 delta commit（INSERT 和 UPDATE 都追加到 log file）。

**构造逻辑**：
```java
private HoodieAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable hoodieTable,
                           String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr, ...) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, ...);
    this.recordItr = recordItr;
    this.sizeEstimator = getSizeEstimator();
    this.statuses = new ArrayList<>();
}
```

AppendHandle 的构造非常轻量，不需要读取任何旧文件。`writer`（`HoodieLogFormat.Writer`）在第一次调用 `init()` 时延迟初始化。

**写入逻辑**（`doAppend()`）：
```java
public void doAppend() {
    while (recordItr.hasNext()) {
        HoodieRecord record = recordItr.next();
        init(record);                              // 延迟初始化 log writer
        flushToDiskIfRequired(record, false);      // 达到 block 大小阈值时刷盘
        writeToBuffer(record);                     // 将记录缓存到 recordList
    }
    appendDataAndDeleteBlocks(header, true);        // 将剩余记录作为最后一个 block 写入
}
```

核心的 `flushToDiskIfRequired` 方法实现了按 block 大小自动分割：
```java
if (numberOfRecords >= (maxBlockSize / averageRecordSize)) {
    appendDataAndDeleteBlocks(header, appendDeleteBlocks);
    estimatedNumberOfBytesWritten += averageRecordSize * numberOfRecords;
    numberOfRecords = 0;
}
```

`appendDataAndDeleteBlocks` 将缓存的记录构建为 `HoodieLogBlock`（`HoodieAvroDataBlock` / `HoodieParquetDataBlock` / `HoodieHFileDataBlock`），然后通过 `writer.appendBlocks()` 追加到 log file。DELETE 记录会被构建为 `HoodieDeleteBlock`，单独存放。

**关闭逻辑**（`close()`）：刷入剩余数据、关闭 log writer、获取 log file 实际大小更新到 WriteStatus。如果写入过程中 log file 发生了 rollover（超过最大大小），会产生多个 WriteStatus。

**性能特征**：
- **写入延迟极低**：纯追加写入，不需要读取任何旧数据
- **零写放大**：只写入变更的数据，不重写整个文件
- **读放大代价**：随着 log file 增多，MOR Snapshot 查询需要合并越来越多的 log block
- **内存友好**：记录在内存中按 block 大小批量缓存，不需要加载整个文件
- **record position 支持**：表版本 >= 8 时，AppendHandle 会在 log block header 中写入 `BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS`，用于读取时基于位置的快速合并（避免全键查找）

#### 三者对比总结

| 维度 | HoodieCreateHandle | HoodieWriteMergeHandle | HoodieAppendHandle |
|------|-------------------|----------------------|-------------------|
| **IO 类型** | CREATE（新建文件） | UPDATE（合并重写） | APPEND（追加 log） |
| **适用表类型** | COW / MOR | COW（以及 Compaction） | MOR |
| **是否读旧数据** | 否 | 是（读整个 base file） | 否 |
| **输出格式** | Parquet base file | Parquet base file | Avro/Parquet/HFile log block |
| **写放大** | 无 | 高（完整文件重写） | 无（纯追加） |
| **内存需求** | 低 | 高（ExternalSpillableMap） | 中（按 block 缓存） |
| **读取开销** | 无 | 无（输出即最终文件） | 有（需合并 log） |
| **marker 文件** | 有 | 有 | 无（log 文件自描述） |
| **文件大小控制** | canWrite() 检查 | 由 base file 大小决定 | canWrite() 估算压缩后大小 |

---

### 10.4 Spark SQL INSERT/UPDATE/DELETE/MERGE INTO 的执行路径

**核心源码位置**：
- `hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/HoodieSparkSessionExtension.scala`
- `hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/analysis/HoodieAnalysis.scala`
- `hudi-spark-datasource/hudi-spark3-common/src/main/scala/org/apache/spark/sql/hudi/command/InsertIntoHoodieTableCommand.scala`
- `hudi-spark-datasource/hudi-spark3-common/src/main/scala/org/apache/spark/sql/hudi/command/UpdateHoodieTableCommand.scala`
- `hudi-spark-datasource/hudi-spark3-common/src/main/scala/org/apache/spark/sql/hudi/command/DeleteHoodieTableCommand.scala`
- `hudi-spark-datasource/hudi-spark3-common/src/main/scala/org/apache/spark/sql/hudi/command/MergeIntoHoodieTableCommand.scala`

#### HoodieSparkSessionExtension 的注入机制

`HoodieSparkSessionExtension` 通过 `SparkSessionExtensions` API 注入四类扩展：

```scala
class HoodieSparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // 1. SQL 解析器扩展
    extensions.injectParser { (session, parser) =>
      new HoodieCommonSqlParser(session, parser)
    }
    // 2. Resolution 阶段规则
    HoodieAnalysis.customResolutionRules.foreach { ruleBuilder =>
      extensions.injectResolutionRule(ruleBuilder(_))
    }
    // 3. Post-Hoc Resolution 阶段规则
    HoodieAnalysis.customPostHocResolutionRules.foreach { ruleBuilder =>
      extensions.injectPostHocResolutionRule(ruleBuilder(_))
    }
    // 4. 优化器规则
    HoodieAnalysis.customOptimizerRules.foreach { ruleBuilder =>
      extensions.injectOptimizerRule(ruleBuilder(_))
    }
    // 5. 表函数注入
    sparkAdapter.injectTableFunctions(extensions)
  }
}
```

这些规则的执行顺序至关重要：

**Resolution 阶段规则**（按顺序）：
1. `AdaptIngestionTargetLogicalRelations` —— 处理 Hudi 表的元字段，在 INSERT INTO / MERGE INTO / UPDATE 等操作中，将目标表的 `_hoodie_*` 元字段从解析输出中剥离，避免影响 Spark 的列解析
2. `HoodieDataSourceV2ToV1Fallback` —— 将 DataSource V2 关系降级为 V1（因为 Hudi 目前使用 V1 API），这个规则必须在 ResolveReferences 之前执行
3. `ResolveReferences` —— 解析 Hudi 特有的引用
4. `ResolveImplementationsEarly` —— 将 `InsertIntoStatement` 和 `CreateTable` 转换为 Hudi 实现，**必须在 Spark 的 `DataSourceAnalysis` 之前执行**，否则 Spark 会将其转换为原生实现

**Post-Hoc Resolution 阶段规则**：
1. `ResolveImplementations` —— 将 `MergeIntoTable`、`UpdateTable`、`DeleteFromTable` 等已完全解析的逻辑计划转换为 Hudi 自定义 Command
2. `HoodiePostAnalysisRule` —— 重写 `CreateDataSourceTableCommand`、`DropTableCommand`、`AlterTable*` 等 DDL 命令

#### INSERT INTO 执行路径

```
SQL: INSERT INTO hudi_table VALUES (1, 'Alice', 1000)
  ↓ Spark Parser
InsertIntoStatement(relation, query)
  ↓ ResolveImplementationsEarly (Resolution 阶段)
  ↓ 判断 relation 是否 ResolvesToHudiTable
InsertIntoHoodieTableCommand(logicalRelation, alignedQuery, partitionSpec, overwrite)
  ↓ run(sparkSession, plan)
InsertIntoHoodieTableCommand.run(sparkSession, table, plan, partitionSpec, overwrite)
  ↓ buildHoodieInsertConfig(catalogTable) 构建配置
  ↓ HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, config, df)
  ↓ → 最终走到 writeInternal()
```

关键转换发生在 `ResolveImplementationsEarly` 中：
```scala
case iis @ MatchInsertIntoStatement(relation @ ResolvesToHudiTable(_), ...) if query.resolved =>
  relation match {
    case lr: LogicalRelation =>
      val hoodieCatalogTable = new HoodieCatalogTable(spark, lr.catalogTable.get)
      val alignedQuery = alignQueryOutput(query, hoodieCatalogTable, partition, ...)
      new InsertIntoHoodieTableCommand(lr, alignedQuery, partition, overwrite)
  }
```

`alignQueryOutput` 会确保 INSERT 数据的列顺序和类型与目标表对齐，包括添加缺失列的 NULL 默认值、类型转换等。

#### UPDATE 执行路径

```
SQL: UPDATE hudi_table SET name='Bob' WHERE id=1
  ↓ Spark Parser
UpdateTable(relation, assignments, condition)
  ↓ ResolveImplementations (Post-Hoc 阶段)
  ↓ 判断 plan ResolvesToHudiTable
UpdateHoodieTableCommand(ut, inputPlan)
  ↓ run(sparkSession, plan)
  ↓ buildHoodieConfig(catalogTable) 构建 upsert 配置
  ↓ HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, config, df)
```

`UpdateHoodieTableCommand` 的核心是构建 `inputPlan`：它会读取匹配 WHERE 条件的目标表记录，然后根据 SET 子句应用字段更新，生成新的 DataFrame。这个 DataFrame 包含所有需要更新的完整记录（不仅仅是变更的字段），然后通过 `HoodieSparkSqlWriter.write()` 以 upsert 模式写入。

#### DELETE 执行路径

```
SQL: DELETE FROM hudi_table WHERE id=1
  ↓ Spark Parser
DeleteFromTable(relation, condition)
  ↓ ResolveImplementations (Post-Hoc 阶段)
DeleteHoodieTableCommand(catalogTable, plan, config)
  ↓ run(sparkSession, queryPlan)
  ↓ buildHoodieDeleteTableConfig(catalogTable) 配置 operation=delete
  ↓ HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, config, df)
  ↓ → writeInternal() 中匹配 WriteOperationType.DELETE 分支
```

DELETE 的特殊之处在于：`buildHoodieDeleteTableConfig` 会将 `operation` 设置为 `delete`，`writeInternal` 中会走到 `DELETE` 分支，使用 `DataSourceUtils.doDeleteOperation()` 提取出需要删除记录的 `HoodieKey`，然后调用 `client.delete()`。

#### MERGE INTO 执行路径

```
SQL: MERGE INTO target USING source ON target.id = source.id
     WHEN MATCHED THEN UPDATE SET ...
     WHEN NOT MATCHED THEN INSERT ...
  ↓ Spark Parser
MergeIntoTable(target, source, condition, matchedActions, notMatchedActions)
  ↓ ResolveImplementations (Post-Hoc 阶段)
MergeIntoHoodieTableCommand(mergeInto)
  ↓ run(sparkSession)
  ↓ 将 WHEN MATCHED / WHEN NOT MATCHED 子句序列化为 ExpressionPayload 表达式
  ↓ 将 source 和 target 做 LEFT OUTER JOIN
  ↓ HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, config, joinedDf)
```

MERGE INTO 是最复杂的 DML 操作。`MergeIntoHoodieTableCommand` 使用 `ExpressionPayload` 作为特殊的 payload 实现，将 WHEN MATCHED / NOT MATCHED 子句编码为 Base64 序列化的表达式。在执行阶段，source 表和 target 表做 LEFT OUTER JOIN，join 结果作为一个 DataFrame 通过 `HoodieSparkSqlWriter.write()` 写入。`ExpressionPayload` 在 `combineAndGetUpdateValue()` 中执行匹配条件判断和更新/删除逻辑，在 `getInsertValue()` 中执行 INSERT 逻辑。

**为什么这么设计**：所有 DML 操作最终都收敛到 `HoodieSparkSqlWriter.write()` 这一个入口点，复用了完整的 Index、Partitioner、Handle 和 Commit 流水线。区别仅在于配置参数（operation 类型、是否 prepped 等）和 DataFrame 的构造方式。这种统一设计极大简化了维护成本，新功能只需在 `writeInternal` 中添加一次逻辑即可。

---

### 10.5 BaseHoodieWriteClient.commitStats 与 postCommit 的完整逻辑

**源码位置**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieWriteClient.java`

commit 后的所有动作分布在 `commitStats()` 方法的后半段以及 `postCommit()` 方法中。以下是经过源码验证的完整执行序列。

#### commitStats 方法中 commit 之后的逻辑

```java
public boolean commitStats(String instantTime, TableWriteStats tableWriteStats, ...) {
    // --- 前半段：构建元数据、加锁、preCommit、保存 commit ---
    HoodieTable table = createTable(config);
    HoodieCommitMetadata metadata = CommitMetadataResolverFactory.get(...)
        .reconcileMetadataForMissingFiles(config, context, table, instantTime, ...);
    this.txnManager.beginStateChange(Option.of(inflightInstant), ...);
    try {
        preCommit(metadata);
        commit(table, commitActionType, instantTime, metadata, tableWriteStats, ...);
    } finally {
        this.txnManager.endStateChange(Option.of(inflightInstant));
        releaseResources(instantTime);
    }

    // --- 后半段：postCommit + Clean + Archive + 内联表服务 ---
    boolean postCommitStatus = true;
    HoodieTimer postCommitTimer = HoodieTimer.start();
    try {
        postCommit(table, metadata, instantTime, extraMetadata);      // 1. postCommit
        mayBeCleanAndArchive(table);                                   // 2. Clean + Archive
        runTableServicesInline(table, metadata, extraMetadata);        // 3. 内联表服务
    } catch (Exception e) {
        postCommitStatus = false;
        if (config.canIgnorePostCommitFailures()) {
            LOG.error("Ignoring exception during post-commit...", e);
        } else {
            throw e;
        }
    } finally {
        long duration = postCommitTimer.endTimer();
        metrics.updatePostCommitMetrics(postCommitStatus, duration);
    }

    emitCommitMetrics(instantTime, metadata, commitActionType);        // 4. 指标上报
    if (config.writeCommitCallbackOn()) {                              // 5. 回调通知
        commitCallback.call(new HoodieWriteCommitCallbackMessage(...));
    }
    return true;
}
```

注意，**commit 完成后锁已释放**（`endStateChange`），后续的 postCommit、Clean、Archive 和内联表服务都在**锁外执行**。这是有意为之——这些操作可能非常耗时（特别是 Compaction 和 Clustering），在锁内执行会阻塞其他 writer。

#### 第一步：postCommit() —— Marker 清理与心跳停止

```java
protected void postCommit(HoodieTable table, HoodieCommitMetadata metadata,
                          String instantTime, Option<Map<String, String>> extraMetadata) {
    try {
        // 删除 marker 目录
        WriteMarkersFactory.get(config.getMarkersType(), table, instantTime)
            .quietDeleteMarkerDir(context, config.getMarkersDeleteParallelism());
        // 更新表服务 instant 的指标
        metrics.updateTableServiceInstantMetrics(table.getActiveTimeline());
    } finally {
        // 停止该 instant 的心跳
        this.heartbeatClient.stop(instantTime);
    }
}
```

Marker 文件在写入过程中用于追踪未完成的文件写入（每次 create/append 一个数据文件时都会创建对应的 marker）。commit 成功后，这些 marker 不再需要，必须清理。心跳机制用于检测长时间运行的写入是否还活着——如果心跳过期，其他进程可以安全地 rollback 该写入。

#### 第二步：mayBeCleanAndArchive() —— 自动清理与归档

```java
protected void mayBeCleanAndArchive(HoodieTable table) {
    autoCleanOnCommit();     // 清理旧版本文件
    autoArchiveOnCommit(table);  // 归档旧的 timeline instant
}
```

**Clean（清理）**：根据 `hoodie.clean.automatic`（默认 true）和 `hoodie.clean.async`（默认 false）决定同步/异步执行。Clean 操作根据保留策略（如保留最近 N 个 commit 的文件版本）删除旧的 base file 和 log file，释放存储空间。

**Archive（归档）**：根据 `hoodie.archive.automatic`（默认 true）和 `hoodie.archive.async`（默认 false）决定同步/异步执行。Archive 操作将超过保留期限的 timeline instant 从活跃时间线（`.hoodie/timeline/`）移动到归档时间线（`.hoodie/archived/`），防止时间线文件过多影响性能。

#### 第三步：runTableServicesInline() —— 内联表服务

```java
// BaseHoodieTableServiceClient.runTableServicesInline()
protected void runTableServicesInline(HoodieTable table, HoodieCommitMetadata metadata,
                                      Option<Map<String, String>> extraMetadata) {
    // 1. 内联 Compaction（MOR 表）
    if (config.inlineCompactionEnabled()) {
        metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT.key(), "true");
        inlineCompaction(table, extraMetadata);
    }
    // 2. 内联调度 Compaction（仅调度，不执行）
    if (!config.inlineCompactionEnabled() && config.scheduleInlineCompaction()
        && table.getActiveTimeline().getWriteTimeline().filterPendingCompactionTimeline().empty()) {
        inlineScheduleCompaction(extraMetadata);
    }
    // 3. 内联 Log Compaction
    if (config.inlineLogCompactionEnabled()) {
        runAnyPendingLogCompactions(table);
        inlineLogCompact(extraMetadata);
    }
    // 4. 内联 Clustering
    if (config.inlineClusteringEnabled()) {
        metadata.addMetadata(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true");
        inlineClustering(table, extraMetadata);
    }
    // 5. 内联调度 Clustering
    if (!config.inlineClusteringEnabled() && config.scheduleInlineClustering()
        && !table.getActiveTimeline().getLastPendingClusterInstant().isPresent()) {
        inlineScheduleClustering(extraMetadata);
    }
    // 6. 分区 TTL 管理
    if (config.isInlinePartitionTTLEnable()) {
        String instantTime = startDeletePartitionCommit(table.getMetaClient()).requestedTime();
        table.managePartitionTTL(table.getContext(), instantTime);
    }
}
```

内联表服务的执行有严格的前置条件检查：
- Compaction：仅在 `hoodie.compact.inline=true` 时执行，且会检查是否已有 pending compaction
- Clustering：仅在 `hoodie.clustering.inline=true` 时执行，且会检查是否已有 pending clustering
- Log Compaction：先执行已有的 pending log compaction，再调度新的
- 分区 TTL：自动清理过期分区

#### 第四步：Metadata Table 更新（在 commit 方法内部）

值得注意的是，Metadata Table 的更新发生在 `commit()` 内部（而非 `postCommit()`）：

```java
protected void commit(HoodieTable table, String commitActionType, String instantTime,
                      HoodieCommitMetadata metadata, TableWriteStats tableWriteStats, ...) {
    finalizeWrite(table, instantTime, tableWriteStats.getDataTableWriteStats());
    saveInternalSchema(table, instantTime, metadata);
    // ★ Metadata Table 更新
    writeToMetadataTable(skipStreamingWritesToMetadataTable, table, instantTime,
        tableWriteStats.getMetadataTableWriteStats(), metadata);
    // 保存 commit
    activeTimeline.saveAsComplete(...);
}
```

Metadata Table 更新在主表 commit 保存之前执行，确保 Metadata Table 和主表的一致性。如果 Metadata Table 更新失败，整个 commit 会失败并回滚。

#### 第五步：MetaSync（在 Spark 层执行）

MetaSync 实际上不在 `postCommit()` 中，而是在 Spark 层的 `commitAndPerformPostOperations()` 中执行：

```scala
// HoodieSparkSqlWriter.commitAndPerformPostOperations()
val metaSyncSuccess = metaSync(spark, HoodieWriterUtils.convertMapToHoodieConfig(parameters),
    tableInstantInfo.basePath, schema)
```

MetaSync 将 Hudi 表的元数据同步到外部 catalog（如 Hive Metastore、AWS Glue、DataHub 等），包括分区信息和 Schema 变更。它在 commit 成功后、异步 Compaction/Clustering 调度之后执行。

**设计优势**：
1. **锁外执行 postCommit**：避免长时间持锁，提高多 Writer 场景下的并发度
2. **可配置的失败策略**：`hoodie.fail.writes.on.inline.table.service.exception` 控制表服务失败是否导致整个写入失败
3. **分层职责**：Metadata Table 更新在 commit 原子操作内，保证一致性；Clean/Archive/表服务在 commit 后异步或同步执行，不影响写入延迟
4. **心跳保护**：通过心跳机制检测 "僵尸" 写入，`postCommit` 中及时停止心跳，让清理机制能够识别和回滚真正失败的写入

---

## 总结

### Spark 写入 Hudi 核心调用链

```
DataFrame.write.format("hudi").save()
    → DefaultSource.createRelation(mode=Append)
        → HoodieSparkSqlWriter.write()  [含冲突重试]
            → HoodieSparkSqlWriterInternal.write()
                → SparkRDDWriteClient.upsert()
                    → HoodieTable.upsert()
                        → Index.tagLocation()
                        → WorkloadProfile + UpsertPartitioner
                        → HoodieMergeHandle (COW) / HoodieAppendHandle (MOR)
                        → Index.updateLocation()
                    → commitStats()
                        → TransactionManager.beginStateChange()
                        → preCommit() [冲突检测]
                        → Timeline.saveAsComplete()
                        → TransactionManager.endStateChange()
                        → postCommit() [表服务 + MetaSync]
```

### Spark 读取 Hudi 核心调用链

```
spark.read.format("hudi").load()
    → DefaultSource.createRelation(parameters)
        → DefaultSource.createRelation(metaClient, schema, options)  [object]
            → HadoopFsRelationFactory.build()
                → HadoopFsRelation(
                      fileIndex = HoodieFileIndex,        [分区裁剪 + 数据跳过]
                      fileFormat = HoodieFileGroupReaderBasedFileFormat  [统一读取]
                  )
                → Spark FileSourceScanExec
                    → FileFormat.buildReaderWithPartitionValues()
                        → HoodieFileGroupReader
                            → COW: 直接 Parquet 读取
                            → MOR: Base + HoodieMergedLogRecordScanner → 合并
```

---

**文档版本**: 4.1（纠错 + 大幅扩展 + 深度剖析）
**创建日期**: 2026-04-14
**最后更新**: 2026-04-22
**基于 Hudi 版本**: v1.2.0-SNAPSHOT (master)
