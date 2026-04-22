# Hudi 数据质量保障与高级特性深度解析

> 基于 Apache Hudi v1.2.0-SNAPSHOT 源码，深入剖析 Pre-commit Validator、CDC、Time Travel、Bootstrap、
> Multi-Table Streamer、Savepoint/Restore、Record Payload 类型体系以及 HoodieStreamer 架构。

---

## 目录

1. [Pre-commit Validator 机制](#1-pre-commit-validator-机制)
2. [CDC (Change Data Capture) 支持](#2-cdc-change-data-capture-支持)
3. [Time Travel 查询](#3-time-travel-查询)
4. [Bootstrap 机制](#4-bootstrap-机制)
5. [Multi-Table 写入 (HoodieMultiTableStreamer)](#5-multi-table-写入-hoodiemultitablestreamer)
6. [Savepoint 与 Restore 的完整实现](#6-savepoint-与-restore-的完整实现)
7. [Record Payload 类型全景](#7-record-payload-类型全景)
8. [Hudi Streamer (DeltaStreamer) 架构](#8-hudi-streamer-deltastreamer-架构)

---

## 1. Pre-commit Validator 机制

### 1.1 为什么需要 Pre-commit Validator

在数据湖场景中，一次错误的写入可能污染下游几十个消费方。传统做法是"先写后查、发现问题再修"，
代价极高。Hudi 的 Pre-commit Validator 将质量检查前置到 **commit 之前**——数据虽然已经写入物理文件，
但尚未完成 Timeline 上的 commit 转换。如果校验失败，写入会被回滚，脏数据永远不会对读者可见。

**设计好处**：
- 零窗口暴露：校验发生在 inflight 状态，读者看不到未提交的数据
- 可插拔：通过类名配置，支持任意自定义校验逻辑
- 并行执行：多个 Validator 通过 CompletableFuture 并行运行，不串行等待

### 1.2 核心类层级

```
SparkPreCommitValidator<T,I,K,O> (hudi-spark-client，Spark 特化抽象基类)
  └── SqlQueryPreCommitValidator<T,I,K,O> (基于 SQL 查询的抽象基类)
        ├── SqlQueryEqualityPreCommitValidator     (前后相等校验)
        ├── SqlQueryInequalityPreCommitValidator    (前后不等校验)
        └── SqlQuerySingleResultPreCommitValidator  (单值结果校验)

BasePreCommitValidator (hudi-common，引擎无关的新框架基类，与上述体系独立)
```

注意：`BasePreCommitValidator` 是 hudi-common 中新引入的引擎无关验证框架基类，
它与 Spark 特化的 `SparkPreCommitValidator` 体系是**独立并行**的两条路径。
`SparkPreCommitValidator` 不继承 `BasePreCommitValidator`。
在未来的 Phase 3 中，两者可能会进行整合。

**源码位置**：
- `hudi-common/.../client/validator/BasePreCommitValidator.java`
- `hudi-client/hudi-spark-client/.../client/validator/SparkPreCommitValidator.java`
- `hudi-client/hudi-spark-client/.../client/utils/SparkValidatorUtils.java`

### 1.3 配置体系 (HoodiePreCommitValidatorConfig)

所有配置项定义在 `HoodiePreCommitValidatorConfig` 中：

| 配置项 | 说明 |
|--------|------|
| `hoodie.precommit.validators` | 逗号分隔的 Validator 类名列表 |
| `hoodie.precommit.validators.equality.sql.queries` | 等值校验 SQL，用 `;` 分隔多条 |
| `hoodie.precommit.validators.inequality.sql.queries` | 不等值校验 SQL |
| `hoodie.precommit.validators.single.value.sql.queries` | 单值校验 SQL，格式: `query1#result1;query2#result2` |
| `hoodie.precommit.validators.failure.policy` | 失败策略: `FAIL`(阻断) 或 `WARN_LOG`(仅告警)，默认 FAIL |
| `hoodie.precommit.validators.streaming.offset.tolerance.percentage` | 流式偏移校验容忍度百分比，默认 0.0 |

**为什么设计 WARN_LOG 策略？** 在灰度上线数据质量规则时，团队可能不确定规则是否过于严格。
WARN_LOG 模式允许规则先"观察"一段时间，收集日志和指标，确认稳定后再切到 FAIL 模式。

### 1.4 校验触发流程 (SparkValidatorUtils.runValidators)

**重要说明**：Pre-commit Validator 目前仅在 Spark 引擎中完整实现。在 `BaseCommitActionExecutor.runPrecommitValidators()` 
中，非 Spark 引擎会抛出 `HoodieIOException("Precommit validation not implemented for all engines yet")`。

Spark 引擎的校验逻辑位于 `SparkValidatorUtils.runValidators()`：

```
1. 检查 config.getPreCommitValidators() 是否非空
2. 收集 writeMetadata 中被修改的分区列表 partitionsModified
3. 刷新 Timeline 以确保看到最新的异步操作（如 clustering/compaction/rollback）
4. 构建"当前提交后的状态快照" afterState (通过 getRecordsFromPendingCommits)
5. 构建"当前提交前的状态快照" beforeState (通过 getRecordsFromCommittedFiles)
6. 通过反射实例化所有配置的 Validator 类
7. 用 CompletableFuture 并行运行每个 Validator
8. 收集结果：全部成功则继续，任一失败则抛出 HoodieValidationException
```

**为什么构建"两个快照"？** 这是 Pre-commit Validator 的精髓——在 commit 动作尚未完成时，
通过读取 pending commit 的文件构建"假如 commit 成功后"的数据视图，
与"当前已提交"的数据视图形成对比。Equality Validator 可以断言某些聚合值不变，
Inequality Validator 可以断言记录数确实增长了。

### 1.5 三种内置 SQL Validator 详解

**SqlQueryEqualityPreCommitValidator**：
- 对 before 和 after 快照分别执行相同 SQL
- 用 `prevRows.intersect(newRows).count() == prevRows.count()` 判断结果集是否一致
- 适用场景：校验维度表的记录总数在写入后不变，或某些聚合指标保持稳定

**SqlQueryInequalityPreCommitValidator**：
- 与 Equality 逻辑相反：断言 before 和 after 的结果不同
- 适用场景：确保每次写入确实带来了新数据（防止空写入或重复写入）

**SqlQuerySingleResultPreCommitValidator**：
- 只对 after 快照执行 SQL，断言结果是单个值且等于预期
- 配置格式：`query1#expectedResult1;query2#expectedResult2`，例如 `select count(*) from <TABLE_NAME> where id is null#0`
- 适用场景：NULL 值检测、数据完整性约束（如某列不允许出现特定值）

### 1.6 实践示例

```properties
# 配置 Pre-commit Validator
hoodie.precommit.validators=org.apache.hudi.client.validator.SqlQuerySingleResultPreCommitValidator,\
  org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator

# NULL 值检测：确保 user_id 列无 NULL
hoodie.precommit.validators.single.value.sql.queries=\
  select count(*) from <TABLE_NAME> where user_id is null#0

# 记录数守恒校验（适用于 compaction 等场景）
hoodie.precommit.validators.equality.sql.queries=\
  select count(*) from <TABLE_NAME>
```

---

## 2. CDC (Change Data Capture) 支持

### 2.1 为什么 Hudi 需要原生 CDC

传统 CDC 方案（如 Debezium）捕获的是数据库层面的变更日志，而数据湖中的 CDC 需求是：
**给定一个时间范围，精确还原每条记录的变更历史（insert/update/delete）**。

Hudi 原生 CDC 的优势在于：
- 无需额外中间件，直接从 Hudi 表的 Timeline 和数据文件中提取变更
- 支持三种补充日志模式，在存储开销和读取效率之间灵活权衡
- 与 Hudi 的 COW/MOR 表模型深度融合

### 2.2 三种 CDC 补充日志模式 (HoodieCDCSupplementalLoggingMode)

定义在 `hudi-common/.../table/cdc/HoodieCDCSupplementalLoggingMode.java`：

| 模式 | 存储内容 | 存储开销 | 读取效率 |
|------|----------|----------|----------|
| `OP_KEY_ONLY` | 仅操作类型 + 记录键 | 最小 | 最低（需回溯前后文件还原完整变更） |
| `DATA_BEFORE` | 操作类型 + 记录键 + before-image | 中等 | 中等（需从当前文件获取 after-image） |
| `DATA_BEFORE_AFTER` | 操作类型 + 时间戳 + before-image + after-image | 最大 | 最高（直接读取即可） |

**为什么提供三种模式？** 这是经典的存储-计算权衡：
- 高吞吐写入场景选 `OP_KEY_ONLY`，牺牲读取性能换取极低的写放大
- 对 CDC 消费延迟敏感的场景选 `DATA_BEFORE_AFTER`，空间换时间
- `DATA_BEFORE` 是折中方案

### 2.3 CDC 数据的 Schema 定义 (HoodieCDCUtils)

CDC 记录的标准字段定义在 `HoodieCDCUtils` 中：

```java
CDC_OPERATION_TYPE = "op"        // 操作类型: i(insert), u(update), d(delete)
CDC_COMMIT_TIMESTAMP = "ts_ms"   // 变更时间戳
CDC_BEFORE_IMAGE = "before"      // 变更前的完整记录
CDC_AFTER_IMAGE = "after"        // 变更后的完整记录
CDC_RECORD_KEY = "record_key"    // 记录键
```

`HoodieCDCUtils.schemaBySupplementalLoggingMode()` 根据不同模式生成对应的 CDC Schema：
- `OP_KEY_ONLY` 模式：包含 `op` 和 `record_key`
- `DATA_BEFORE` 模式：包含 `op`、`record_key`、`before`
- `DATA_BEFORE_AFTER` 模式：包含 `op`、`ts_ms`、`before`、`after`

**注意**：`ts_ms` 字段仅在 `DATA_BEFORE_AFTER` 模式下存在，其他模式不包含时间戳字段。

### 2.4 CDC 数据的写入 (HoodieCDCLogger)

`HoodieCDCLogger` 负责在数据写入过程中捕获变更并写入 CDC 日志文件（后缀为 `.cdc`）。

核心写入逻辑在 `put()` 方法中：

```
1. 根据 oldRecord 和 newRecord 判断操作类型：
   - oldRecord == null && newRecord 存在 → INSERT
   - oldRecord 存在 && newRecord 存在 → UPDATE
   - newRecord 为空 → DELETE
2. 通过 CDCTransformer 将数据转换为 CDC 格式
3. 存入 ExternalSpillableMap（支持内存溢出到磁盘）
4. 当内存使用达到 maxBlockSize 时，flush 为 HoodieCDCDataBlock 写入日志文件
```

**为什么使用 ExternalSpillableMap？** CDC 日志在 merge handle 过程中生成，
可能涉及大量记录。ExternalSpillableMap 在内存不足时自动溢出到磁盘，
避免 OOM，同时保持内存中的高性能处理。

### 2.5 CDC 数据的读取 (HoodieCDCExtractor + HoodieCDCLogRecordIterator)

读取 CDC 数据分为两个阶段：

**阶段一：提取 CDC 文件分片 (HoodieCDCExtractor)**

`HoodieCDCExtractor` 分析 Timeline 中指定时间范围内的 commit 元数据，
为每个文件组生成 `HoodieCDCFileSplit`：

```java
// 五种 CDC 推断场景 (HoodieCDCInferenceCase)
AS_IS            // 直接使用 CDC 日志文件
BASE_FILE_INSERT // 新增的 Base 文件，所有记录标记为 INSERT
BASE_FILE_DELETE // 被清空的文件组，前一版本所有记录标记为 DELETE
LOG_FILE         // MOR 表的增量日志，需与前一文件切片对比
REPLACE_COMMIT   // replacecommit（如 INSERT_OVERWRITE），被替换文件组标记为 DELETE
```

**为什么需要五种推断场景？** 因为 Hudi 的变更可能发生在不同层面：
- 有 CDC 日志时直接使用（AS_IS）
- 无 CDC 日志时需要从数据文件反推变更——新文件意味着 INSERT，
  空文件意味着 DELETE，MOR 日志需要与前一切片 merge 对比

**阶段二：迭代读取 CDC 记录 (HoodieCDCLogRecordIterator)**

`HoodieCDCLogRecordIterator` 实现了 `ClosableIterator<IndexedRecord>`，
采用惰性加载策略：逐个打开 CDC 日志文件 → 逐个读取数据块 → 逐条返回记录。

### 2.6 Spark 层面的 CDC 集成

在 Spark 侧，`HoodieCDCFileIndex` 和 `CDCFileGroupIterator` 将底层 CDC 数据
转换为 Spark DataFrame，供用户通过 SQL 或 DataFrame API 消费：

```scala
// 使用 hudi_table_changes TVF 查询 CDC 数据
spark.sql("""
  SELECT * FROM hudi_table_changes('my_table', 'latest_state', 'earliest')
""")
```

---

## 3. Time Travel 查询

### 3.1 为什么 Hudi 天然支持 Time Travel

Hudi 的 Timeline 机制记录了每一次 commit 的元数据，每个文件都带有对应的 instant 时间戳。
这意味着只要 Timeline 上的信息完整，就可以精确重建任意历史时间点的数据状态。
这与传统数据库的 MVCC 概念类似，但实现在文件系统层面。

**好处**：
- 数据审计：查看任意时间点的数据快照
- 错误恢复：对比正确版本和错误版本，快速定位问题
- 可重复性：确保分析结果可复现

### 3.2 Spark SQL 中的 Time Travel 语法路由

Hudi 通过 `HoodieSparkSessionExtension` 注册自定义 SQL 解析器，
拦截 Spark 的 `SELECT ... AS OF TIMESTAMP ...` 语法。

**解析链路**：

```
1. Spark SQL Parser 解析 `AS OF TIMESTAMP` 语句
2. Hudi 扩展解析器 (HoodieSpark3_xExtendedSqlAstBuilder) 识别时间旅行子句
3. 生成 TimeTravelRelation 逻辑计划节点
4. ResolveReferences 分析规则匹配 TimeTravelRelation
5. 将 timestamp 转换为 DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT (实际为 HoodieCommonConfig.TIMESTAMP_AS_OF)
6. 创建 DataSource 并传入时间参数
7. 解析为 LogicalRelation（标准 Spark 关系）
```

核心路由逻辑在 `HoodieSparkBaseAnalysis.scala` 的 `ResolveReferences` 规则中：

```scala
case TimeTravelRelation(ResolvesToHudiTable(table), timestamp, version) =>
  // 不支持 version 表达式，仅支持 timestamp
  val dataSource = DataSource(spark, ...,
    options = Map(
      DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key -> timestamp.get.toString()
    ))
  LogicalRelation(dataSource.resolveRelation(), table)
```

**为什么只支持 timestamp 不支持 version？** Hudi 的 Timeline 是基于时间戳的，
不像 Delta Lake 那样有线性递增的版本号。timestamp 语义更自然且与 Hudi 的底层设计一致。

### 3.3 FileSystemView 的时间点重建

当 `TIME_TRAVEL_AS_OF_INSTANT` 参数被传入后，`HoodieFileIndex` 会将其作为
`specifiedQueryInstant` 传给 `SparkHoodieTableFileIndex`：

```scala
class HoodieFileIndex(...) extends SparkHoodieTableFileIndex(
  specifiedQueryInstant = options.get(
    DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key
  ).map(HoodieSqlCommonUtils.formatQueryInstant),
  ...)
```

在底层的 `AbstractTableFileSystemView` 中，文件过滤逻辑只返回
**在指定时间点之前或等于该时间点已完成的 commit 所对应的文件切片**：

```
getLatestFileSlicesBeforeOrOn(partitionPath, queryInstant)
  → 遍历所有文件组
  → 对每个文件组，找到 instant <= queryInstant 的最新文件切片
  → 返回该切片（包括 base file + log files）
```

**为什么这个设计高效？** Timeline 是有序的，文件名中编码了 instant 时间戳，
因此文件过滤操作可以在元数据层面完成，不需要扫描文件内容。
如果开启了元数据表，过滤操作甚至不需要访问数据分区目录。

### 3.4 两种使用方式

**方式一：Spark SQL AS OF 语法**
```sql
SELECT * FROM my_hudi_table TIMESTAMP AS OF '2024-01-15 10:30:00';
```

**方式二：DataFrame API 指定参数**
```scala
spark.read
  .format("hudi")
  .option("as.of.instant", "20240115103000")
  .load("/path/to/hudi_table")
```

两种方式最终都会被转换为 `TIME_TRAVEL_AS_OF_INSTANT` 参数传入 `HoodieFileIndex`。

---

## 4. Bootstrap 机制

### 4.1 为什么需要 Bootstrap

企业中大量存量数据以 Parquet/ORC 格式存储在 HDFS/S3 上。如果要将这些数据迁移到 Hudi 表，
朴素做法是"全量读取 → 全量写入"，对于 PB 级数据来说时间和资源成本极高。

Bootstrap 机制允许**不重写原始数据文件**，而是通过建立索引映射关系，
让 Hudi 表直接引用原始 Parquet 文件。这将迁移成本从"全量复制"降低到"仅写元数据"。

### 4.2 两种 Bootstrap 模式

定义在 `BootstrapMode` 枚举中：

| 模式 | 说明 | 使用场景 |
|------|------|----------|
| `METADATA_ONLY` | 仅创建索引，不复制数据 | 存量数据规模大、不需要立即改写的分区 |
| `FULL_RECORD` | 完整读取并重写为 Hudi 格式 | 需要立即具备 Hudi 全部能力（如 upsert）的分区 |

**为什么提供两种模式？** 在实际迁移中，不同分区的需求可能不同：
- 冷数据分区可以用 `METADATA_ONLY` 快速接管，后续按需逐步转换
- 热数据分区需要立即支持 upsert/delete 操作，适合 `FULL_RECORD`

### 4.3 Bootstrap 执行器 (SparkBootstrapCommitActionExecutor)

位于 `hudi-client/hudi-spark-client/.../action/bootstrap/SparkBootstrapCommitActionExecutor.java`。

**执行流程**：

```
1. validate() - 校验 bootstrap 源路径和分区选择器已配置
2. 检查 Active Timeline 为空（不允许在已有数据的表上 bootstrap）
3. listAndProcessSourcePartitions():
   a. 列举源路径下所有分区及文件
   b. 通过 HoodieSparkBootstrapSchemaProvider 获取 bootstrap schema
   c. 通过 BootstrapModeSelector 将分区分为 METADATA_ONLY 和 FULL_RECORD 两组
4. metadataBootstrap(METADATA_ONLY 分区):
   a. 创建 METADATA_BOOTSTRAP_INSTANT_TS 时间点
   b. 并行处理每个分区中的文件，生成 BootstrapWriteStatus
   c. 更新索引并提交
   d. 写入 BootstrapIndex（记录源文件到 Hudi 文件的映射关系）
5. fullBootstrap(FULL_RECORD 分区):
   a. 通过 FullRecordBootstrapDataProvider 读取源数据
   b. 使用 BulkInsert 方式写入 Hudi 表
   c. 创建 FULL_BOOTSTRAP_INSTANT_TS 时间点并提交
```

### 4.4 BootstrapModeSelector 的可插拔设计

`BootstrapModeSelector` 是一个抽象类，通过 `select()` 方法决定每个分区的 bootstrap 模式。

```java
public abstract Map<BootstrapMode, List<String>> select(
    List<Pair<String, List<HoodieFileStatus>>> partitions);
```

用户可以自定义实现，例如：
- 按分区日期选择：最近 7 天的分区用 FULL_RECORD，其余用 METADATA_ONLY
- 按分区大小选择：小分区直接全量重写，大分区仅元数据接管

**为什么这样设计？** Bootstrap 涉及大量数据搬迁决策，不同企业、不同表、不同分区的需求差异巨大。
可插拔的 Selector 让用户能够根据自身业务逻辑灵活定制迁移策略。

### 4.5 BootstrapIndex 的作用

Bootstrap 完成后，`BootstrapIndex`（基于 HFile）记录了源文件到 Hudi 文件的映射关系。
当 Hudi 读取 `METADATA_ONLY` 模式的分区时，会通过 BootstrapIndex 找到原始 Parquet 文件路径，
直接读取原始文件内容，同时合并 Hudi 的元数据信息（record key 等）。

```java
// SparkBootstrapCommitActionExecutor.commit() 中写入 BootstrapIndex
try (BootstrapIndex.IndexWriter indexWriter = BootstrapIndex
    .getBootstrapIndex(metaClient)
    .createWriter(metaClient.getTableConfig().getBootstrapBasePath().get())) {
  indexWriter.begin();
  bootstrapSourceAndStats.forEach((key, value) ->
    indexWriter.appendNextPartition(key, value.stream()
      .map(Pair::getKey).collect(Collectors.toList())));
  indexWriter.finish();
}
```

---

## 5. Multi-Table 写入 (HoodieMultiTableStreamer)

### 5.1 为什么需要 Multi-Table 写入

在实际数据平台中，一个 Kafka 集群的多个 topic 需要分别写入多张 Hudi 表。
如果每个表启动一个独立的 Spark 作业，会导致：
- 资源浪费：每个作业需要独立的 Driver 和最小 Executor 资源
- 运维复杂：需要管理几十甚至上百个独立作业
- 难以协调：多个表的摄入进度难以统一监控

`HoodieMultiTableStreamer` 允许在单个 Spark 作业中顺序写入多张 Hudi 表。

### 5.2 核心类结构

```
HoodieMultiTableStreamer (入口类)
  ├── Config (命令行参数)
  ├── List<TableExecutionContext> (每张表的执行上下文)
  └── sync() (顺序执行每张表的摄入)
```

位于 `hudi-utilities/.../streamer/HoodieMultiTableStreamer.java`。

### 5.3 配置体系

Multi-Table 的配置采用**公共配置 + 表级覆盖**的分层设计：

```
公共配置文件 (common.properties):
  hoodie.streamer.ingestion.tablesToBeIngested=db1.table1,db1.table2
  hoodie.streamer.ingestion.db1.table1.configFile=/path/to/table1_config.properties
  hoodie.streamer.ingestion.db1.table2.configFile=/path/to/table2_config.properties

表级配置文件 (table1_config.properties):
  hoodie.streamer.source.kafka.topic=topic1
  hoodie.datasource.write.recordkey.field=id
  # 其他表特定配置...
```

**为什么采用分层配置？** 多表之间通常有大量共同配置（如 Kafka 集群地址、安全配置等），
分层设计避免了配置重复。表级配置文件中未设置的项会自动继承公共配置。

### 5.4 表执行上下文的构建过程

`populateTableExecutionContextList()` 方法的处理流程：

```
1. 从公共配置中读取 hoodie.streamer.ingestion.tablesToBeIngested
2. 对每个表：
   a. 解析 database.table 格式
   b. 查找对应的配置文件路径
   c. 加载表级配置，并合并公共配置中表级未覆盖的项
   d. 构建 HoodieStreamer.Config 对象
   e. 设置 targetBasePath = basePathPrefix/database/table
   f. 填充 Transformer 和 SchemaProvider 配置
   g. 创建 TableExecutionContext 并加入列表
```

### 5.5 同步执行逻辑

```java
public void sync() {
  for (TableExecutionContext context : tableExecutionContexts) {
    try {
      streamer = new HoodieStreamer(context.getConfig(), jssc,
          Option.ofNullable(context.getProperties()));
      streamer.sync();
      successTables.add(getTableWithDatabase(context));
    } catch (Exception e) {
      failedTables.add(getTableWithDatabase(context));
    } finally {
      if (streamer != null) streamer.shutdownGracefully();
    }
  }
}
```

**为什么是顺序执行而不是并行？** 这是一个有意的设计选择：
- 单个 Spark 作业中 Executor 资源有限，并行写入多张表可能导致资源争抢
- 顺序执行使每张表都能利用全部 Executor 资源，吞吐量更优
- 表之间可能有依赖关系（如表 A 的输出是表 B 的输入），顺序执行更容易管理

### 5.6 失败处理

每张表的写入是独立的。如果表 A 失败，不影响表 B 和 C 的写入。
执行结束后通过 `getSuccessTables()` 和 `getFailedTables()` 获取状态汇总。

---

## 6. Savepoint 与 Restore 的完整实现

### 6.1 为什么需要 Savepoint/Restore

Hudi 的 Clean 操作会自动清理过期的文件版本以回收存储空间。但有时需要将表回退到某个历史版本，
如果该版本的文件已被 Clean 清理，就无法恢复了。

Savepoint 机制为指定的 commit 打上"保护标记"，阻止 Clean 清理该 commit 涉及的文件。
Restore 机制则在 Savepoint 的基础上，将表真正回退到指定时间点。

### 6.2 Savepoint 的实现 (SavepointActionExecutor)

位于 `hudi-client/hudi-client-common/.../action/savepoint/SavepointActionExecutor.java`。

**执行流程**：

```
1. 验证目标 instantTime 在已完成的 commits 时间线上存在
2. getLastCommitRetained():
   - 从最近的 Clean instant 元数据中获取 earliestCommitToRetain
   - 如果没有 clean instant，使用第一个完成的 commit
   - 确保 savepoint 时间 >= 最后保留的 commit（不允许对已被清理的 commit 建 savepoint）
3. 收集目标时间点的所有最新文件列表:
   - 如果元数据表可用：通过 getAllLatestFileSlicesBeforeOrOn(instantTime) 批量获取
   - 如果元数据表不可用：并行遍历所有分区，使用 getLatestFileSlicesBeforeOrOn 逐分区列举
   - 同时收集 base files 和 log files 的文件名
4. 构建 HoodieSavepointMetadata（包含 user、comment、分区到文件的映射）
5. 在 Timeline 上创建 SAVEPOINT action 的 inflight instant
6. 将 metadata 保存并标记为 completed
```

**为什么区分元数据表可用和不可用两种路径？** 性能优化：
- 元数据表可用时：直接通过 `getAllLatestFileSlicesBeforeOrOn()` 批量获取，
  利用元数据表的快速查找能力，避免多次读取
- 元数据表不可用时：并行遍历所有分区，在文件系统上逐分区列举文件，
  通过 Spark 并行化提升性能

### 6.3 Savepoint 与 Clean 的协调

Clean 操作在决定清理哪些文件版本时，会检查 Savepoint：

```
Clean 逻辑:
  对每个文件组：
    获取所有文件版本 (file slices)
    保留最新 N 个版本（根据 hoodie.cleaner.commits.retained 配置）
    对于更老的版本：
      如果该版本被某个 Savepoint 引用 → 保留
      否则 → 标记为可清理
```

Savepoint 的 metadata 中记录了精确的文件列表（分区 → 文件名列表），
Clean 可以据此判断某个文件是否受 Savepoint 保护。

### 6.4 Restore 的实现 (BaseRestoreActionExecutor)

位于 `hudi-client/hudi-client-common/.../action/restore/BaseRestoreActionExecutor.java`。

**执行流程**：

```
1. 在 restore timeline 上查找当前 instantTime 对应的 pending restore instant
2. 从 restore plan (HoodieRestorePlan) 中获取需要回滚的 instants 列表
3. 将 restore instant 从 REQUESTED 转换为 INFLIGHT
4. 逆序遍历 instantsToRollback，对每个 instant 执行 rollbackInstant():
   - 调用引擎特定的回滚逻辑（CopyOnWrite 或 MergeOnRead）
   - 生成 HoodieRollbackMetadata
   - 跳过已在之前的恢复尝试中回滚成功的 instant
5. finishRestore():
   a. 构建 HoodieRestoreMetadata
   b. 更新元数据表（在数据表锁内完成）
   c. 将 restore instant 标记为 COMPLETED
   d. 清理 savepoint 时间点之后的所有 pending rollback instants
6. 输出日志：Restored table to {savepointTimestamp}
```

**为什么 Restore 需要逐个回滚 instant？** 因为每个 instant 可能涉及不同的分区和文件组，
且可能是不同类型的操作（commit、deltacommit、replacecommit）。
逐个回滚确保每个操作都被正确撤销，数据文件被正确清理。

### 6.5 COW vs MOR 的 Restore 差异

- `CopyOnWriteRestoreActionExecutor`：回滚时删除 commit 写入的数据文件
- `MergeOnReadRestoreActionExecutor`：除了处理数据文件，还需要处理日志文件的回滚

### 6.6 Restore 的幂等性设计

```java
// getInstantsToRollback 中的幂等检查
Option<HoodieInstant> rollbackInstantOpt = table.getActiveTimeline()
    .getWriteTimeline()
    .filter(instant -> instant.requestedTime().equals(instantInfo.getCommitTime()))
    .firstInstant();
if (rollbackInstantOpt.isPresent()) {
  instantsToRollback.add(rollbackInstantOpt.get());
} else {
  log.info("Ignoring already rolledback instant {}", instantInfo.toString());
}
```

如果 Restore 过程中发生崩溃，重新执行时会跳过已经回滚成功的 instants。
这种幂等性设计确保了 Restore 操作的可靠性。

---

## 7. Record Payload 类型全景

### 7.1 为什么需要 Payload 抽象

在数据合并场景中，同一条记录可能有多个版本（如来自不同批次或不同分区的更新）。
"哪个版本应该胜出"是一个业务决策，不同场景需要不同的合并策略。

`HoodieRecordPayload` 接口抽象了三个核心操作：
- `preCombine(T oldValue)` — 同一批次内的去重（在写入之前）
- `combineAndGetUpdateValue(currentValue, schema)` — 新旧记录合并（upsert 时）
- `getInsertValue(schema)` — 获取插入值

### 7.2 HoodieRecordPayload 接口

位于 `hudi-common/.../model/HoodieRecordPayload.java`。

```java
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public interface HoodieRecordPayload<T extends HoodieRecordPayload> extends Serializable {
  T preCombine(T oldValue);
  Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema);
  Option<IndexedRecord> getInsertValue(Schema schema);
  default Comparable<?> getOrderingValue() { return 0; }
  default Option<Map<String, String>> getMetadata() { return Option.empty(); }
}
```

**为什么返回 Option？** 返回 `Option.empty()` 表示该记录应被跳过（如删除记录），
这是一种优雅的空值处理方式，避免了特殊的删除标记或异常抛出。

### 7.3 全部实现类详解

#### 7.3.1 OverwriteWithLatestAvroPayload

**合并语义**：新值直接覆盖旧值，不做任何比较。

```java
public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue) {
  return this;  // 直接返回新值
}

public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) {
  return getInsertValue(schema);  // 忽略 currentValue，直接用新值
}
```

**适用场景**：
- 数据源保证每条记录是最终状态（如定期全量快照导入）
- COMMIT_TIME_ORDERING 合并模式（最后写入的永远胜出）

**为什么这是最简单的实现？** 它不需要比较排序字段，也不需要读取存储中的旧记录值进行比较，
因此写入性能最高。适用于"永远以最新到达的数据为准"的场景。

#### 7.3.2 DefaultHoodieRecordPayload

**合并语义**：基于排序字段（ordering field）选择胜出者。

```java
public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue) {
  if (oldValue.isEmptyRecord()) {
    return this;  // 旧值是空记录，使用新值
  }
  if (oldValue.orderingVal.compareTo(orderingVal) > 0) {
    return oldValue;  // 旧值排序值更大，保留旧值
  }
  return this;
}
```

`combineAndGetUpdateValue` 额外处理：
- 从存储中的记录提取排序字段值与新记录比较
- 支持自定义删除标记（通过 `DELETE_KEY` 和 `DELETE_MARKER` 配置）
- 支持 Schema 演进（排序字段在旧 Schema 中可能不存在）

**适用场景**：
- EVENT_TIME_ORDERING 合并模式
- 数据源中有事件时间字段，需要确保"最新事件"的记录胜出

**为什么 DefaultHoodieRecordPayload 是推荐的默认选择？** 它在保证数据正确性的同时
提供了灵活的配置能力——通过 ordering field 控制合并行为，通过 delete marker 支持软删除，
是大多数 upsert 场景的最佳选择。

#### 7.3.3 EventTimeAvroPayload

继承自 `DefaultHoodieRecordPayload`，主要区别在于：

1. **简化的 combineAndGetUpdateValue**：不执行删除标记检查，直接基于 ordering field 比较
2. **getMetadata() 返回 Option.empty()**：不追踪事件时间元数据

```java
@Override
public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {
  Option<IndexedRecord> incomingRecord = isEmptyRecord() || isDeletedRecord ? Option.empty() : getRecord(schema);
  
  if (!needUpdatingPersistedRecord(currentValue, incomingRecord, properties)) {
    return Option.of(currentValue);  // 旧记录更新，保留旧值
  }
  
  return incomingRecord;  // 新记录更新，使用新值
}

@Override
public Option<Map<String, String>> getMetadata() {
  return Option.empty();  // 不追踪事件时间元数据
}
```

**为什么单独一个类？** 相比 `DefaultHoodieRecordPayload`，`EventTimeAvroPayload` 跳过了删除标记检查和元数据追踪，
在高吞吐场景中减少不必要的开销。适用于明确不需要软删除功能且对性能敏感的场景。

#### 7.3.4 OverwriteNonDefaultsWithLatestAvroPayload

**合并语义**：部分覆盖——只用新记录中"非默认值"的字段覆盖旧记录。

```java
protected void setField(GenericRecord baseRecord, GenericRecord mergedRecord,
    GenericRecordBuilder builder, Schema.Field field) {
  Object value = baseRecord.get(field.name());
  Object defaultValue = field.defaultVal();
  if (!overwriteField(value, defaultValue)) {
    builder.set(field, value);        // 新值非默认，使用新值
  } else {
    builder.set(field, mergedRecord.get(field.name()));  // 新值是默认值，保留旧值
  }
}
```

**preCombine 逻辑**：与 `DefaultHoodieRecordPayload` 相同，基于 ordering field 比较，但会检查空记录。

**适用场景**：
- 部分更新（Partial Update）：只发送变化的字段，其他字段保持 Schema 默认值
- CDC 场景中的字段级增量更新

**为什么这个设计很巧妙？** 它利用 Avro Schema 的 default value 机制来区分
"显式设置的值"和"未设置的值"。只有显式设置了非默认值的字段才会覆盖存储中的旧值。

#### 7.3.5 EmptyHoodieRecordPayload

**合并语义**：永远返回空值，用于表示删除。

```java
public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) {
  return Option.empty();  // 无论如何都返回空
}

public Option<IndexedRecord> getInsertValue(Schema schema) {
  return Option.empty();  // 无论如何都返回空
}
```

**适用场景**：
- 硬删除操作
- 当表的 Delete 操作类型被触发时，记录会被包装为 EmptyHoodieRecordPayload

#### 7.3.6 HoodieAvroPayload

**合并语义**：与 OverwriteWithLatestAvroPayload 类似，但使用 bytes 存储而非 GenericRecord。

```java
private final byte[] recordBytes;  // 将 GenericRecord 序列化为 bytes

public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
  if (recordBytes.length == 0) return Option.empty();
  return Option.of(HoodieAvroUtils.bytesToAvro(recordBytes, schema));
}
```

**适用场景**：
- Compaction 过程中的记录重写
- 需要更高内存效率的场景（bytes 不保存 Schema，更紧凑）

**为什么不直接用 GenericRecord？** 注释解释了两个原因：
1. 不存储 Schema，内存更高效
2. byte[] 天然支持 Java 序列化，方便在 Spark 任务间传输

#### 7.3.7 RewriteAvroPayload

**合并语义**：直接返回传入的 GenericRecord，不做任何处理。

```java
public RewriteAvroPayload preCombine(RewriteAvroPayload another) {
  throw new UnsupportedOperationException("precombine is not expected for rewrite payload");
}
```

**适用场景**：
- Compaction/Clustering 等内部重写操作
- Schema 不变的场景下避免序列化/反序列化开销

#### 7.3.8 HoodieJsonPayload

**合并语义**：与 OverwriteWithLatestAvroPayload 类似，但数据以压缩 JSON 格式存储。

```java
private final byte[] jsonDataCompressed;  // 使用 Deflater 压缩

public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
  MercifulJsonConverter jsonConverter = new MercifulJsonConverter();
  return Option.of(jsonConverter.convert(getJsonData(), HoodieSchema.fromAvroSchema(schema)));
}
```

**适用场景**：
- 数据源是 JSON 格式（如 Kafka JSON topic）
- 无需解析为 Avro 再序列化回去的场景

**为什么使用 BEST_COMPRESSION？** JSON 文本通常有很高的压缩比，
使用最高压缩级别可以显著减少内存占用。

### 7.4 Payload 选择决策树

```
是否需要排序字段比较？
  ├── 否 → 是否是 commit time ordering？
  │       ├── 是 → OverwriteWithLatestAvroPayload
  │       └── 否 → 是否是删除操作？
  │               ├── 是 → EmptyHoodieRecordPayload
  │               └── 否 → HoodieAvroPayload / RewriteAvroPayload
  └── 是 → 是否需要部分更新？
          ├── 是 → OverwriteNonDefaultsWithLatestAvroPayload
          └── 否 → DefaultHoodieRecordPayload (推荐)
                    或 EventTimeAvroPayload (高吞吐优化)
```

### 7.5 RecordMergeMode 与 Payload 的关系

从 v1.0 开始，Hudi 引入了 `RecordMergeMode` 来简化配置：

```java
static String getAvroPayloadForMergeMode(RecordMergeMode mergeMode, String payloadClassName) {
  switch (mergeMode) {
    case EVENT_TIME_ORDERING: return DefaultHoodieRecordPayload.class.getName();
    case COMMIT_TIME_ORDERING: return OverwriteWithLatestAvroPayload.class.getName();
    case CUSTOM: return payloadClassName;
  }
}
```

这意味着大多数情况下用户只需要配置 `RecordMergeMode`，不再需要直接指定 Payload 类名。

---

## 8. Hudi Streamer (DeltaStreamer) 架构

### 8.1 为什么需要 HoodieStreamer

虽然用户可以直接通过 Spark DataFrame API 写入 Hudi 表，但在生产环境中，
数据摄入需要一个完整的框架来处理：
- 源数据读取和 checkpoint 管理
- Schema 获取和演进
- 数据转换
- 写入和提交
- 异步 Compaction/Clustering
- 目录同步（如 Hive Metastore）
- 失败重试和恢复

HoodieStreamer（原名 DeltaStreamer）就是这样一个"开箱即用"的端到端数据摄入工具。

### 8.2 三层抽象架构

```
Source (数据读取层)
  ↓ InputBatch<T>
Transformer (数据转换层)
  ↓ Dataset<Row>
Writer (StreamSync 写入层)
  ↓ WriteStatus
Post-commit hooks (Compaction / Clustering / MetaSync)
```

**为什么采用三层解耦？**
- Source 只负责"从哪里读、读多少"，不关心数据格式转换
- Transformer 只负责"数据长什么样"，不关心来源和去向
- Writer 只负责"怎么写入 Hudi"，不关心数据是怎么来的
- 每一层都可以独立替换和扩展

### 8.3 Source 层详解

#### 8.3.1 Source 基类

位于 `hudi-utilities/.../sources/Source.java`。

```java
public abstract class Source<T> implements SourceCommitCallback, Serializable {
  public enum SourceType { JSON, AVRO, ROW, PROTO }

  // 核心方法：根据 checkpoint 获取新数据
  public final InputBatch<T> fetchNext(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    Option<Checkpoint> translated = translateCheckpoint(lastCheckpoint);
    InputBatch<T> batch = readFromCheckpoint(translated, sourceLimit);
    batch.getBatch().ifPresent(this::persist);
    return overriddenSchemaProvider == null ? batch
        : new InputBatch<>(batch.getBatch(), batch.getCheckpointForNextBatch(), overriddenSchemaProvider);
  }
}
```

**Checkpoint 机制**：每次读取返回 `InputBatch`，其中包含数据和下一次的 checkpoint。
下次读取时传入上一次的 checkpoint，实现增量读取。

#### 8.3.2 支持的 Source 类型

| 分类 | Source 类 | 说明 |
|------|-----------|------|
| **Kafka** | `JsonKafkaSource` | 从 Kafka 读取 JSON 消息 |
| | `AvroKafkaSource` | 从 Kafka 读取 Avro 消息（配合 Schema Registry） |
| | `ProtoKafkaSource` | 从 Kafka 读取 Protobuf 消息 |
| **DFS** | `JsonDFSSource` | 从 HDFS/S3 读取 JSON 文件 |
| | `AvroDFSSource` | 从 HDFS/S3 读取 Avro 文件 |
| | `ParquetDFSSource` | 从 HDFS/S3 读取 Parquet 文件 |
| | `CsvDFSSource` | 从 HDFS/S3 读取 CSV 文件 |
| | `ORCDFSSource` | 从 HDFS/S3 读取 ORC 文件 |
| **Database** | `JdbcSource` | 通过 JDBC 从关系数据库读取 |
| | `SqlSource` | 执行 SQL 查询获取数据 |
| **Cloud Events** | `S3EventsSource` | 监听 S3 事件（通过 SQS） |
| | `GcsEventsSource` | 监听 GCS 事件（通过 Pub/Sub） |
| | `S3EventsHoodieIncrSource` | 基于 S3 事件的 Hudi 增量源 |
| | `GcsEventsHoodieIncrSource` | 基于 GCS 事件的 Hudi 增量源 |
| **Hudi** | `HoodieIncrSource` | 增量读取另一张 Hudi 表 |
| **Hive** | `HiveIncrPullSource` | 增量读取 Hive 表 |
| **Pulsar** | `PulsarSource` | 从 Apache Pulsar 读取消息 |
| **Debezium** | `MysqlDebeziumSource` | 读取 MySQL 的 Debezium CDC 事件 |
| | `PostgresDebeziumSource` | 读取 PostgreSQL 的 Debezium CDC 事件 |

**为什么 Source 类型如此丰富？** 数据湖的数据来源极其多样化，
从消息队列到对象存储到关系数据库都有。丰富的内置 Source 减少了用户的开发工作量。

#### 8.3.3 SourceType 的四种数据格式

- `JSON`：数据以 JSON 字符串 RDD 形式返回
- `AVRO`：数据以 GenericRecord RDD 形式返回
- `ROW`：数据以 Spark Dataset\<Row\> 形式返回（最通用）
- `PROTO`：数据以 Protobuf Message RDD 形式返回

### 8.4 Transformer 层详解

位于 `hudi-utilities/.../transform/Transformer.java`。

```java
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public interface Transformer {
  Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession,
                     Dataset<Row> rowDataset, TypedProperties properties);
}
```

**内置 Transformer**：
- `SqlQueryBasedTransformer` — 通过 SQL 转换数据
- `FlatteningTransformer` — 将嵌套结构展平
- `ChainedTransformer` — 串联多个 Transformer

**为什么 Transformer 是可选的？** 很多场景下源数据已经符合目标 Schema，
不需要转换。将 Transformer 设计为可选组件，避免了不必要的 Spark shuffle 操作。

配置方式：
```properties
hoodie.streamer.transformer.class=org.apache.hudi.utilities.transform.SqlQueryBasedTransformer
hoodie.streamer.transformer.sql=SELECT *, concat(city, ',', state) as address FROM <SRC>
```

支持链式 Transformer，用逗号分隔多个类名，它们按顺序依次执行。

### 8.5 Writer 层 (StreamSync) 详解

`StreamSync` 是 HoodieStreamer 的核心类，负责协调 Source、Transformer 和 Write Client：

```
StreamSync.syncOnce():
  1. 从 Source 拉取数据 → InputBatch
  2. 通过 SchemaProvider 获取/验证 Schema
  3. 如果配置了 Transformer，执行转换
  4. 将 Dataset<Row> 转换为 HoodieRecord RDD
  5. 通过 SparkRDDWriteClient 执行写入操作（upsert/insert/bulk_insert 等）
  6. 处理 WriteStatus，检查错误
  7. 如果开启了 meta sync，同步到 Hive/DataHub 等
  8. 更新 checkpoint
```

### 8.6 运行模式

**单次模式**：
```bash
spark-submit --class org.apache.hudi.utilities.streamer.HoodieStreamer \
  --target-base-path /path/to/hudi_table \
  --table-type MERGE_ON_READ \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --props /path/to/config.properties
```

**连续模式**（添加 `--continuous`）：
```
While (true):
  syncOnce()
  if (minSyncIntervalSeconds > 0):
    sleep(剩余时间)
  schedule compaction if needed
  schedule clustering if needed
```

连续模式下，DeltaStreamer 在一个无限循环中持续运行，每次迭代执行一次同步。
同时异步调度 Compaction 和 Clustering 操作。

### 8.7 异步服务集成

HoodieStreamer 在连续模式下可以启动异步服务：

```java
// HoodieStreamer 中的初始化
this.ingestionService = Option.ofNullable(
    cfg.runBootstrap ? null : new StreamSyncService(cfg, sparkEngineContext, ...));
```

- `SparkAsyncCompactService` — 异步执行 Compaction
- `SparkAsyncClusteringService` — 异步执行 Clustering

这些服务使用 Spark 的 Fair Scheduler Pool 与主写入任务共享 Executor 资源，
通过不同的 scheduling weight 控制资源分配。

### 8.8 错误处理和恢复

HoodieStreamer 的错误恢复机制：

1. **Checkpoint 恢复**：每次成功的 commit 都会在 Timeline 元数据中记录 checkpoint，
   重启后自动从最后一个成功的 checkpoint 继续

2. **commitOnErrors 配置**：当部分记录写入失败时，可以选择忽略错误继续提交
   （适用于对数据完整性要求不高的场景）

3. **Error Table**：失败的记录可以被路由到一个单独的错误表，后续修复后重新处理

4. **回滚保护**：如果写入过程中崩溃，下次启动时 Hudi 的 rollback 机制会自动清理
   未完成的写入

---

## 总结

本文深入分析了 Hudi 的八大高级特性，它们共同构成了 Hudi 作为生产级数据湖平台的关键能力：

| 特性 | 核心价值 |
|------|----------|
| Pre-commit Validator | 数据质量前置保障，将问题拦截在 commit 之前 |
| CDC | 精确追踪数据变更历史，支持增量消费和审计 |
| Time Travel | 任意时间点数据回溯，支持审计和错误恢复 |
| Bootstrap | 零拷贝迁移存量数据，降低迁移成本数个数量级 |
| Multi-Table Streamer | 单作业多表摄入，简化运维复杂度 |
| Savepoint/Restore | 企业级数据保护和灾难恢复能力 |
| Record Payload | 灵活的合并策略体系，适配多样化业务需求 |
| HoodieStreamer | 端到端数据摄入框架，开箱即用 |

这些特性的共同设计理念是：**可插拔、可配置、引擎无关**。无论是 Validator、Source、Transformer
还是 Payload，都通过接口抽象 + 反射加载的方式实现了高度可扩展性。
用户可以在不修改 Hudi 源码的前提下，通过自定义实现类来满足特定的业务需求。
