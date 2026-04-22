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
