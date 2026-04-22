# Flink 流式读取 Hudi 深度解析

> 基于 Apache Hudi v1.2.0-SNAPSHOT 源码，全面解析 Flink 引擎读取 Hudi 表的完整架构、核心实现和优化机制。

---

## 目录

- [第一部分：Flink 批量读取 Hudi](#第一部分flink-批量读取-hudi)
  - [1. HoodieTableSource -- DynamicTableSource 核心入口](#1-hoodietablesource----dynamictablesource-核心入口)
  - [2. InputFormat 体系](#2-inputformat-体系)
  - [3. FileIndex 在 Flink 中的实现](#3-fileindex-在-flink-中的实现)
- [第二部分：Flink 流式增量读取](#第二部分flink-流式增量读取)
  - [4. HoodieTableSource 的 Streaming 模式](#4-hoodietablesource-的-streaming-模式)
  - [5. IncrementalInputSplits 与 StreamReadMonitoringFunction](#5-incrementalinputsplits-与-streamreadmonitoringfunction)
  - [6. CDC 读取](#6-cdc-读取)
  - [7. StreamReadOperator 的 Checkpoint 协调](#7-streamreadoperator-的-checkpoint-协调)
- [第三部分：Flink 读取优化](#第三部分flink-读取优化)
  - [8. 谓词下推](#8-谓词下推)
  - [9. 列裁剪](#9-列裁剪)
  - [10. MOR 表读取的合并机制](#10-mor-表读取的合并机制)
- [第四部分：Flink 读写联动](#第四部分flink-读写联动)
  - [11. 端到端链路分析](#11-端到端链路分析)
  - [12. 读取与 Compaction 的协调](#12-读取与-compaction-的协调)
- [第五部分：Flink vs Spark 读取架构对比](#第五部分flink-vs-spark-读取架构对比)
  - [13. 全维度对比](#13-全维度对比)
  - [14. Flink 读取配置参数完整手册](#14-flink-读取配置参数完整手册)

---

## 第一部分：Flink 批量读取 Hudi

### 1. HoodieTableSource -- DynamicTableSource 核心入口

#### 1.1 类定义与接口实现

`HoodieTableSource` 是 Flink 读取 Hudi 表的核心入口类，位于：

```
hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/HoodieTableSource.java
```

它的类声明如下：

```java
public class HoodieTableSource extends FileIndexReader implements
    ScanTableSource,
    SupportsProjectionPushDown,
    SupportsLimitPushDown,
    SupportsFilterPushDown,
    LookupTableSource,
    SupportsReadingMetadata,
    Serializable {
```

**说明**：源码中 `HoodieTableSource` 显式实现了 `Serializable` 接口（见 `HoodieTableSource.java:142-149`），父类 `FileIndexReader` 也实现了 `Serializable`，所有字段都为可序列化类型。

**为什么这么设计？**

Flink Table/SQL 的数据源抽象基于 `DynamicTableSource` 接口体系。`HoodieTableSource` 通过实现多个 `Supports*` 接口，告诉 Flink 优化器它支持哪些下推优化。这种"声明式能力注册"的好处是：

1. **ScanTableSource**: 声明自己是一个可扫描的数据源，Flink 会调用 `getScanRuntimeProvider()` 来获取实际的数据读取逻辑。
2. **SupportsProjectionPushDown**: 支持列裁剪，避免读取不需要的列，减少 I/O。
3. **SupportsLimitPushDown**: 支持 LIMIT 下推，查询 `SELECT * FROM t LIMIT 10` 时可以提前停止读取。
4. **SupportsFilterPushDown**: 支持谓词下推，将 WHERE 条件下推到 Hudi 层执行。
5. **LookupTableSource**: 支持 Lookup Join（维表关联），可以在流式 JOIN 中用 Hudi 作为维表。
6. **SupportsReadingMetadata**: 支持读取元数据列（如 `_hoodie_commit_time` 等）。

这种多接口实现的设计使得 Hudi 可以最大限度地参与 Flink 的查询优化，而不是被当作一个"黑盒"数据源。

#### 1.2 核心字段解析

```java
private final StorageConfiguration<org.apache.hadoop.conf.Configuration> hadoopConf;
private final HoodieTableMetaClient metaClient;
private final long maxCompactionMemoryInBytes;
private final SerializableSchema schema;
private final RowType tableRowType;
private final StoragePath path;
private final List<String> partitionKeys;
private final Configuration conf;
private final InternalSchemaManager internalSchemaManager;

private int[] requiredPos;          // 列裁剪后需要读取的列位置
private long limit;                 // LIMIT 下推值
private List<Predicate> predicates; // 谓词下推的条件
private ColumnStatsProbe columnStatsProbe;       // 列统计信息探针（用于 Data Skipping）
private PartitionPruners.PartitionPruner partitionPruner; // 分区裁剪器
private Option<Function<Integer, Integer>> dataBucketFunc; // Bucket 索引裁剪函数
```

**为什么需要 `InternalSchemaManager`？**

它用于处理 Schema Evolution（模式演进）。Hudi 支持列的增加、删除、重命名和类型变更。`InternalSchemaManager` 负责在读取时将存储层的历史 schema 转换为当前查询需要的 schema，保证向前/向后兼容。

#### 1.3 getScanRuntimeProvider -- 读取入口路由

这是 Flink 调用的核心方法，它决定了如何实际读取数据：

```java
@Override
public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    return new DataStreamScanProviderAdapter() {
        @Override
        public boolean isBounded() {
            return !conf.get(FlinkOptions.READ_AS_STREAMING);
        }

        @Override
        public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
            if (conf.get(FlinkOptions.READ_SOURCE_V2_ENABLED)) {
                return produceNewSourceDataStream(execEnv);
            } else {
                return produceLegacySourceDataStream(execEnv, typeInfo);
            }
        }
    };
}
```

**关键设计决策：**

- **`isBounded()` 方法**: 根据 `read.streaming.enabled` 配置判断是否为有界数据源。这决定了 Flink 使用批处理模式还是流处理模式。
- **双版本 Source 支持**: 通过 `read.source-v2.enabled` 配置决定使用 Legacy Source（基于 SourceFunction）还是 FLIP-27 新版 Source API。新版 API 具有更好的 Checkpoint 协调和动态 Split 分配能力。

#### 1.4 批量读取路径详解

`getBatchInputFormat()` 方法根据查询类型（`hoodie.datasource.query.type`）选择不同的读取路径：

```java
private InputFormat<RowData, ?> getBatchInputFormat() {
    final String queryType = this.conf.get(FlinkOptions.QUERY_TYPE);
    switch (queryType) {
        case FlinkOptions.QUERY_TYPE_SNAPSHOT:       // "snapshot"
            // 根据表类型选择 MOR 或 COW 的 InputFormat
            final HoodieTableType tableType = ...;
            switch (tableType) {
                case MERGE_ON_READ:
                    return mergeOnReadInputFormat(...);
                case COPY_ON_WRITE:
                    return baseFileOnlyInputFormat();
            }
        case FlinkOptions.QUERY_TYPE_READ_OPTIMIZED:  // "read_optimized"
            return baseFileOnlyInputFormat();
        case FlinkOptions.QUERY_TYPE_INCREMENTAL:      // "incremental"
            // 增量读取走 IncrementalInputSplits 逻辑
            IncrementalInputSplits incrementalInputSplits = ...;
            final IncrementalInputSplits.Result result = incrementalInputSplits.inputSplits(metaClient, cdcEnabled);
            if (cdcEnabled) {
                return cdcInputFormat(...);
            } else {
                return mergeOnReadInputFormat(...);
            }
    }
}
```

**三种查询类型的设计哲学：**

| 查询类型 | 读取范围 | 读取文件 | 适用场景 |
|---------|---------|---------|---------|
| `snapshot` | 最新快照 | base files + log files（MOR）/ base files only（COW） | 获取表的最新完整视图 |
| `read_optimized` | 最新已合并数据 | 仅 base files（Parquet） | 只读已 compacted 的数据，速度最快但可能不是最新 |
| `incremental` | 增量数据 | 指定时间范围内变更的文件 | 增量同步场景 |

**为什么 `read_optimized` 对 COW 和 MOR 表使用同一个 `baseFileOnlyInputFormat()`？**

因为 read_optimized 的语义就是"只读 base file"。对于 COW 表，每次写入直接更新 base file，所以 read_optimized 等同于 snapshot。对于 MOR 表，read_optimized 跳过了尚未被 compaction 合并的 log files，牺牲实时性换取读取性能。

---

### 2. InputFormat 体系

#### 2.1 CopyOnWriteInputFormat -- COW 表的读取

```
hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/format/cow/CopyOnWriteInputFormat.java
```

`CopyOnWriteInputFormat` 继承自 Flink 的 `FileInputFormat<RowData>`，专门用于读取 Parquet base files：

```java
public class CopyOnWriteInputFormat extends FileInputFormat<RowData> {
    private final String[] fullFieldNames;
    private final DataType[] fullFieldTypes;
    private final int[] selectedFields;     // 列裁剪
    private final List<Predicate> predicates; // 谓词下推
    private final long limit;                // LIMIT 下推
    private final InternalSchemaManager internalSchemaManager; // Schema 演进
    private transient ClosableIterator<RowData> itr;
}
```

**核心读取流程：**

1. `open(FileInputSplit)`: 打开一个文件分片
   - 解析分区路径提取分区值
   - 创建 Parquet 记录迭代器
2. `nextRecord()`: 逐条返回 RowData
3. `reachedEnd()`: 判断是否读完（考虑 limit）
4. `close()`: 关闭资源

```java
@Override
public void open(FileInputSplit fileSplit) throws IOException {
    LinkedHashMap<String, Object> partObjects = FilePathUtils.generatePartitionSpecs(
        fileSplit.getPath().getPath(), ...);
    this.itr = RecordIterators.getParquetRecordIterator(
        internalSchemaManager, utcTimestamp, true, conf.conf(),
        fullFieldNames, fullFieldTypes, partObjects,
        selectedFields, 2048, fileSplit.getPath(),
        fileSplit.getStart(), fileSplit.getLength(), predicates);
}
```

**为什么重写 `createInputSplits` 方法？**

原始 `FileInputFormat.createInputSplits()` 使用标准 Flink 的文件系统 API 创建分片。Hudi 重写此方法是为了使用 `HadoopFSUtils.getFs()` 获取插件化的文件系统实现，从而支持 S3、HDFS、OSS 等多种存储后端。

#### 2.2 MergeOnReadInputFormat -- MOR 表的读取

```
hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/format/mor/MergeOnReadInputFormat.java
```

`MergeOnReadInputFormat` 是 MOR 表读取的核心类，继承自 `RichInputFormat`：

```java
public class MergeOnReadInputFormat extends RichInputFormat<RowData, MergeOnReadInputSplit> {
    protected final Configuration conf;
    protected final MergeOnReadTableState<MergeOnReadInputSplit> tableState;
    private transient ClosableIterator<RowData> iterator; // 统一迭代器
    private final List<Predicate> predicates;
    private final long limit;
    protected boolean emitDelete;  // 是否发送 DELETE 消息
    protected final InternalSchemaManager internalSchemaManager;
}
```

**核心设计 -- 统一迭代器视图：**

MOR 表的读取需要合并 base file（Parquet）和 log files（Avro），`MergeOnReadInputFormat` 将这种复杂性隐藏在一个统一的 `ClosableIterator<RowData>` 后面。

```java
@Override
public void open(MergeOnReadInputSplit split) throws IOException {
    this.iterator = initIterator(split);
    mayShiftInputSplit(split); // 恢复消费位移
}

protected ClosableIterator<RowData> initIterator(MergeOnReadInputSplit split) throws IOException {
    // 决定合并类型
    String mergeType = split.getMergeType();
    if (!split.getBasePath().isPresent()) {
        if (OptionsResolver.emitDeletes(conf)) {
            mergeType = FlinkOptions.REALTIME_SKIP_MERGE; // 跳过合并
        } else {
            mergeType = FlinkOptions.REALTIME_PAYLOAD_COMBINE; // 有效载荷合并
        }
    }
    return getSplitRowIterator(split, tableSchema, requiredSchema, mergeType, emitDelete);
}
```

**两种合并策略：**

| 合并类型 | 常量 | 行为 | 适用场景 |
|---------|------|------|---------|
| `payload_combine` | `REALTIME_PAYLOAD_COMBINE` | 将 log 记录与 base 记录合并，产出最新值 | 默认模式，保证数据正确性 |
| `skip_merge` | `REALTIME_SKIP_MERGE` | 不做合并，直接输出所有记录 | 流式模式下需要发送 DELETE 消息时 |

**`emitDelete` 的设计意图：**

在流式读取模式下（`read.streaming.enabled=true`），下游 Flink 算子可能需要 DELETE 消息来撤回之前的累加结果。例如，一个 `COUNT(*)` 聚合算子需要在记录被删除时减少计数。因此 MOR 表的流式读取默认设置 `emitDelete=true`，而批量读取默认为 `false`。

**`mayShiftInputSplit` -- 消费位移恢复：**

```java
private void mayShiftInputSplit(MergeOnReadInputSplit split) throws IOException {
    if (split.isConsumed()) {
        for (long i = 0; i < split.getConsumed() && !reachedEnd(); i++) {
            nextRecord(null);
        }
    }
}
```

当从 Checkpoint 恢复时，一个分片可能已经被部分消费。`mayShiftInputSplit` 通过跳过已消费的记录数来恢复到正确的位置。虽然这种方式（逐条跳过）效率不高，但保证了 exactly-once 语义的正确性。

#### 2.3 MergeOnReadInputSplit -- 输入分片抽象

```
hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/format/mor/MergeOnReadInputSplit.java
```

```java
public class MergeOnReadInputSplit implements InputSplit {
    private final int splitNum;
    private final Option<String> basePath;           // base file 路径（可能不存在）
    private final Option<List<String>> logPaths;     // log file 路径列表
    private final String latestCommit;               // 用于限制 log 读取范围
    private final String tablePath;
    private final long maxCompactionMemoryInBytes;
    private final String mergeType;
    private final Option<InstantRange> instantRange; // 增量读取的时间范围
    private final String partitionPath;
    protected String fileId;
    private long consumed = 0L; // 已消费记录数（用于 Checkpoint 恢复）
}
```

**为什么 `basePath` 和 `logPaths` 都是 Option 类型？**

这是因为 MOR 表的 file slice 可能只有 log files 没有 base file（新写入但尚未 compaction 的数据），也可能只有 base file 没有 log files（刚 compaction 完的数据）。Option 类型准确地表达了这种"可能存在也可能不存在"的语义。

#### 2.4 与 Spark 的 InputFormat 设计差异

| 维度 | Flink | Spark |
|------|-------|-------|
| 读取抽象 | `InputFormat<RowData, InputSplit>` | `FileFormat` / `HoodieFileGroupReaderBasedFileFormat` |
| 数据类型 | `RowData`（Flink 内部二进制格式） | `InternalRow`（Spark 内部行格式） |
| 分片管理 | `MergeOnReadInputSplit` 封装 file slice | `PartitionedFile` + `HoodieFileGroupReader` |
| 流式支持 | 原生支持，`emitDelete` 控制 DELETE 消息 | 不原生支持流式读取 |
| 合并执行 | 通过 `HoodieFileGroupReader`（引擎无关） | 同样使用 `HoodieFileGroupReader` |

两者在底层都收敛到了 `HoodieFileGroupReader`，这是 Hudi "引擎抽象"设计模式的体现：核心读取合并逻辑在 `hudi-client-common` 中实现，Flink 和 Spark 只需要提供各自的 `ReaderContext`。

---

### 3. FileIndex 在 Flink 中的实现

#### 3.1 FileIndex 类

```
hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/FileIndex.java
```

`FileIndex` 是 Flink 侧的文件索引实现，负责：

1. **分区路径发现**: 列出所有分区
2. **分区裁剪**: 根据 WHERE 条件过滤不相关的分区
3. **文件裁剪**: 基于列统计信息和 Record Level Index 跳过不相关的文件
4. **Bucket 裁剪**: 基于 Bucket Index 的精确文件定位

```java
public class FileIndex implements Serializable, AutoCloseable {
    private final StoragePath path;
    private final boolean tableExists;
    private final HoodieMetadataConfig metadataConfig;
    private final Option<PartitionPruners.PartitionPruner> partitionPruner;
    private final ColumnStatsProbe colStatsProbe;
    private final Function<String, Integer> partitionBucketIdFunc;
    private List<String> partitionPaths;               // 分区路径缓存
    private final FileStatsIndex fileStatsIndex;       // 列统计索引
    private final Option<RecordLevelIndex> recordLevelIndex; // 记录级索引
}
```

#### 3.2 分区路径发现与缓存

```java
public List<String> getOrBuildPartitionPaths() {
    if (this.partitionPaths != null) {
        return this.partitionPaths;
    }
    List<String> allPartitionPaths = this.tableExists
        ? FSUtils.getAllPartitionPaths(new HoodieFlinkEngineContext(hadoopConf), metaClient, metadataConfig)
        : Collections.emptyList();
    // 应用分区裁剪
    this.partitionPaths = partitionPruner
        .map(pruner -> pruner.filter(allPartitionPaths).stream().collect(Collectors.toList()))
        .orElse(allPartitionPaths);
    return this.partitionPaths;
}
```

**为什么缓存分区路径？**

分区路径的获取涉及文件系统元数据操作（可能通过 Metadata Table 或直接列目录）。缓存避免了在同一次查询中重复获取。值得注意的是，当启用 Metadata Table（`metadata.enabled=true`）时，分区路径的获取从 O(n) 的目录遍历变为 O(1) 的元数据查找，这对大量分区的表（如数万个分区）有极大的性能提升。

#### 3.3 多层裁剪机制

`filterFileSlices()` 方法实现了三层递进式裁剪：

```java
public List<FileSlice> filterFileSlices(List<FileSlice> fileSlices) {
    List<FileSlice> filteredFileSlices;

    // 第一层：Bucket 裁剪
    if (this.partitionBucketIdFunc != null) {
        filteredFileSlices = fileSlices.stream().filter(fileSlice -> {
            String bucketIdStr = BucketIdentifier.bucketIdStr(
                partitionBucketIdFunc.apply(fileSlice.getPartitionPath()));
            return fileSlice.getFileGroupId().getFileId().contains(bucketIdStr);
        }).collect(Collectors.toList());
    }

    // 第二层：Record Level Index 裁剪
    if (recordLevelIndex.isPresent()) {
        filteredFileSlices = recordLevelIndex.get()
            .computeCandidateFileSlices(filteredFileSlices);
    }

    // 第三层：Column Stats 裁剪
    Set<String> candidateFiles = fileStatsIndex
        .computeCandidateFiles(colStatsProbe, allFiles);
    if (candidateFiles != null) {
        result = filteredFileSlices.stream()
            .filter(fs -> fs.getAllFileNames().stream().anyMatch(candidateFiles::contains))
            .collect(Collectors.toList());
    }
    return result;
}
```

**三层裁剪的设计理由：**

1. **Bucket 裁剪**: 如果查询条件包含所有 Bucket Index 字段的等值条件（如 `WHERE id = 123`），可以精确定位到唯一的 bucket 文件，裁剪率极高（从 N 个文件变为 1 个）。
2. **Record Level Index 裁剪**: 通过 Metadata Table 中的 Record Level Index 确定某条记录在哪个文件中，适合点查场景。
3. **Column Stats 裁剪**: 通过列的 min/max/null_count 统计信息，跳过整个不包含目标数据的文件。

这三层裁剪层层递进，每层的输入是上一层的输出，形成一个漏斗式的裁剪管道。

#### 3.4 与 Spark 的 HoodieFileIndex 对比

| 维度 | Flink FileIndex | Spark HoodieFileIndex |
|------|----------------|----------------------|
| 分区发现 | `FSUtils.getAllPartitionPaths` 通过 Metadata Table | 同样通过 Metadata Table |
| Data Skipping | `FileStatsIndex` + `ColumnStatsProbe` | `ColumnStatsIndexSupport` |
| Record Index | `RecordLevelIndex` 支持 | `RecordLevelIndexSupport` |
| Bucket 裁剪 | `PartitionBucketIdFunc` | `BucketIndexSupport` |
| 缓存策略 | 实例级缓存，每次 plan 重新创建 | Spark Session 级别缓存 |
| 统计信息格式 | Flink RowData | Spark InternalRow |

核心逻辑一致，但 Spark 版本有更多优化（如统计信息的 Spark Session 级别缓存、与 Catalyst 优化器的深度集成）。

---

## 第二部分：Flink 流式增量读取

### 4. HoodieTableSource 的 Streaming 模式

#### 4.1 批流一体的设计

Flink Hudi 的批量读取和流式读取共享同一个 `HoodieTableSource` 类。通过 `read.streaming.enabled` 配置来区分：

```java
@Override
public boolean isBounded() {
    return !conf.get(FlinkOptions.READ_AS_STREAMING);
}
```

**为什么采用批流一体设计？**

1. **代码复用**: 谓词下推、列裁剪、Schema 演进等优化逻辑在批量和流式模式之间完全复用。
2. **统一的表注册**: 同一张 Hudi 表可以同时被批量查询和流式查询使用，不需要注册两次。
3. **配置驱动**: 通过一个配置项就能切换模式，降低用户心智负担。

#### 4.2 Legacy Source vs FLIP-27 Source V2

Hudi v1.2.0 同时支持两套流式读取架构：

**Legacy Source（默认）：**

```java
private DataStream<RowData> produceLegacySourceDataStream(...) {
    if (conf.get(FlinkOptions.READ_AS_STREAMING)) {
        // 流式路径：MonitoringFunction + StreamReadOperator
        StreamReadMonitoringFunction monitoringFunction = new StreamReadMonitoringFunction(...);
        InputFormat<RowData, ?> inputFormat = getInputFormat(true);
        OneInputStreamOperatorFactory<MergeOnReadInputSplit, RowData> factory =
            StreamReadOperator.factory((MergeOnReadInputFormat) inputFormat);
        // 构建 DAG: monitor(并行度1) -> keyBy/partition -> split_reader(多并行度)
    } else {
        // 批量路径
        InputFormatSourceFunctionAdapter<RowData> func = new InputFormatSourceFunctionAdapter<>(getInputFormat(), typeInfo);
        DataStreamSource<RowData> source = execEnv.addSource(func, asSummaryString(), typeInfo);
    }
}
```

**FLIP-27 Source V2（通过 `read.source-v2.enabled=true` 启用）：**

```java
private DataStream<RowData> produceNewSourceDataStream(StreamExecutionEnvironment execEnv) {
    HoodieSource<RowData> hoodieSource = createHoodieSource();
    DataStreamSource<RowData> source = execEnv.fromSource(
        hoodieSource, WatermarkStrategy.noWatermarks(), "hudi_source");
    return source.name(getSourceOperatorName("hudi_source"))
        .uid(Pipelines.opUID("hudi_source", conf))
        .setParallelism(conf.get(FlinkOptions.READ_TASKS));
}
```

**两种架构的对比：**

| 维度 | Legacy Source | FLIP-27 Source V2 |
|------|-------------|-------------------|
| 分片发现 | `StreamReadMonitoringFunction`（并行度1的 SourceFunction） | `HoodieContinuousSplitEnumerator`（在 JobManager 侧运行） |
| 分片读取 | `StreamReadOperator`（自定义 OneInputStreamOperator） | `HoodieSourceReader`（基于 SingleThreadMultiplexSourceReaderBase） |
| 分片分发 | `keyBy(fileId)` / 自定义 Partitioner | `SplitAssigner` 机制（支持 Bucket/Number/Default 三种策略） |
| Checkpoint | 手动管理 state（ListState） | 框架级集成（SplitEnumerator 自动 checkpoint） |
| 背压处理 | MailboxExecutor + mini-batch 读取 | 框架级背压传导 |

**为什么引入 FLIP-27 Source V2？**

1. **更好的 Checkpoint 协调**: Source V2 将分片发现和分片读取分离到不同的组件（Enumerator 和 Reader），框架自动处理它们的 checkpoint 对齐。
2. **动态分片分配**: Source V2 的 Enumerator 可以根据 Reader 的处理速度动态分配分片，实现更好的负载均衡。
3. **统一 API**: 符合 Flink 社区推荐的现代 Source API，长期来看是标准化方向。

#### 4.3 ChangelogMode 的设计

```java
@Override
public ChangelogMode getChangelogMode() {
    return OptionsResolver.emitChangelog(conf)
        ? ChangelogModes.FULL
        : ChangelogMode.insertOnly();
}
```

`ChangelogModes.FULL` 定义如下：

```java
public static final ChangelogMode FULL = ChangelogMode.newBuilder()
    .addContainedKind(RowKind.INSERT)
    .addContainedKind(RowKind.UPDATE_BEFORE)
    .addContainedKind(RowKind.UPDATE_AFTER)
    .addContainedKind(RowKind.DELETE)
    .build();
```

**何时使用 FULL changelog 模式？**

- 当 `read.streaming.enabled=true` 且 `changelog.enabled=true` 时
- 当 `cdc.enabled=true` 时

FULL 模式意味着 Hudi source 会发出完整的变更流（INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE），下游可以用这些消息维护准确的物化视图。

#### 4.4 文件分发策略

在 Legacy Source 架构中，MonitoringFunction 发现的分片需要分发给多个并行的 StreamReadOperator。分发策略根据写入模式自动选择：

```java
private DataStream<MergeOnReadInputSplit> addFileDistributionStrategy(
    SingleOutputStreamOperator<MergeOnReadInputSplit> source) {
    if (OptionsResolver.isMorWithBucketIndexUpsert(conf)) {
        return source.partitionCustom(
            new StreamReadBucketIndexPartitioner(conf),
            new StreamReadBucketIndexKeySelector());
    } else if (OptionsResolver.isAppendMode(conf)) {
        return source.partitionCustom(
            new StreamReadAppendPartitioner(conf.get(FlinkOptions.READ_TASKS)),
            new StreamReadAppendKeySelector());
    } else {
        return source.keyBy(split -> split.getFileId());
    }
}
```

**三种分发策略的设计意图：**

1. **Bucket Index 分发** (`StreamReadBucketIndexPartitioner`): 根据 bucket id 和分区路径计算目标 subtask，确保同一个 bucket 的数据总是发到同一个 reader。这避免了 MOR 表的 changelog 模式下 UPDATE_BEFORE 和 UPDATE_AFTER 被不同 reader 处理导致的乱序问题。

2. **Append 模式分发** (`StreamReadAppendPartitioner`): 基于 split 编号取模分发，实现简单的轮询负载均衡。Append 模式下没有 update/delete，所以不需要按 fileId 分组。

3. **默认分发** (`keyBy(fileId)`): 按 fileId 做 keyBy，确保同一个 file group 的所有分片由同一个 reader 处理。这对 MOR 表的 changelog 模式至关重要，因为需要保证同一条记录的 before/after image 的顺序性。

---

### 5. IncrementalInputSplits 与 StreamReadMonitoringFunction

#### 5.1 IncrementalInputSplits -- 增量分片生成器

```
hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/IncrementalInputSplits.java
```

`IncrementalInputSplits` 是增量/流式读取的核心工具类，负责：

1. 分析增量时间范围内的 commit
2. 解析变更文件列表
3. 过滤分区
4. 构建 `MergeOnReadInputSplit` 列表

**增量分片生成的完整流程：**

```java
public Result inputSplits(HoodieTableMetaClient metaClient, boolean cdcEnabled) {
    // Step 1: 构建 IncrementalQueryAnalyzer 分析增量查询范围
    IncrementalQueryAnalyzer analyzer = IncrementalQueryAnalyzer.builder()
        .metaClient(metaClient)
        .startCompletionTime(this.conf.get(FlinkOptions.READ_START_COMMIT))
        .endCompletionTime(this.conf.get(FlinkOptions.READ_END_COMMIT))
        .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
        .skipCompaction(skipCompaction)
        .skipClustering(skipClustering)
        .skipInsertOverwrite(skipInsertOverwrite)
        .readCdcFromChangelog(this.conf.get(FlinkOptions.READ_CDC_FROM_CHANGELOG))
        .build();

    IncrementalQueryAnalyzer.QueryContext analyzingResult = analyzer.analyze();

    // Step 2: 获取变更文件列表
    if (instantRange.isEmpty()) {
        // 从最早开始读 -> 全表扫描
        FileIndex fileIndex = getFileIndex(metaClient);
        // ...
    } else {
        // 增量读取 -> 从 commit metadata 中获取变更文件
        List<MergeOnReadInputSplit> inputSplits = getIncInputSplits(
            metaClient, hadoopConf, commitTimeline, analyzingResult, instantRange.get(), endInstant, cdcEnabled);
    }
}
```

**关键设计细节：**

**`startCompletionTime` vs `startInstantTime` 的区别：**

Hudi v1.x 引入了 completion time 的概念。一个 instant 的 request time（开始时间）和 completion time（完成时间）可能不同。使用 completion time 作为消费偏移量可以避免"空洞提交"（hollow commit）导致的数据遗漏问题。

**`InstantRange.RangeType` 的选择：**

在 `IncrementalInputSplits.inputSplits()` 方法中，默认使用 `CLOSED_CLOSED` 范围类型。而在流式读取的 `StreamReadMonitoringFunction` 中，会根据是否首次读取动态调整：
- 首次读取：使用 `CLOSED_CLOSED`，包含起始 instant
- 续读：使用 `OPEN_CLOSED`，排除已经处理过的起始 instant

这保证了 exactly-once 语义：每个 commit 恰好被消费一次。

**跳过 Compaction/Clustering 的设计：**

```java
.skipCompaction(conf.get(FlinkOptions.READ_STREAMING_SKIP_COMPACT))     // 默认 true
.skipClustering(conf.get(FlinkOptions.READ_STREAMING_SKIP_CLUSTERING))  // 默认 true
```

为什么默认跳过？

- **Compaction**: Compaction 将 log files 合并到 base file，但不产生新数据。如果不跳过，流式读取会重复读取已经通过 delta commit 消费过的数据。
- **Clustering**: Clustering 重新组织文件但不改变数据内容。读取 clustering commit 的文件同样会产生重复。

#### 5.2 StreamReadMonitoringFunction -- 流式监控函数

```
hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/StreamReadMonitoringFunction.java
```

这是 Legacy Source 架构中的"心脏"，以并行度 1 运行，周期性检查新的 commit 并生成分片：

```java
public class StreamReadMonitoringFunction
    extends RichSourceFunctionAdapter<MergeOnReadInputSplit>
    implements CheckpointedFunction {

    private final Path path;
    private final long interval;          // 检查间隔（秒）
    private final boolean cdcEnabled;
    private final int splitsLimit;        // 每轮最多发送的分片数

    private String issuedInstant;         // 已处理到的 instant
    private String issuedOffset;          // 已处理到的 offset（completion time）
    private transient List<MergeOnReadInputSplit> remainingSplits; // 剩余未发送的分片

    private transient ListState<String> instantState;
    private transient ListState<SplitState> inputSplitsState;
}
```

**注意**：`RichSourceFunctionAdapter` 是 Hudi 对 Flink 不同版本 SourceFunction API 的适配器类。

**核心执行循环：**

```java
@Override
public void run(SourceContext<MergeOnReadInputSplit> context) throws Exception {
    checkpointLock = context.getCheckpointLock();
    while (isRunning) {
        synchronized (checkpointLock) {
            monitorDirAndForwardSplits(context);
        }
        TimeUnit.SECONDS.sleep(interval);
    }
}
```

**`monitorDirAndForwardSplits` 详解：**

```java
public void monitorDirAndForwardSplits(SourceContext<MergeOnReadInputSplit> context) {
    HoodieTableMetaClient metaClient = getOrCreateMetaClient();

    // 获取新的分片（如果有剩余分片则使用剩余的，否则发现新的）
    IncrementalInputSplits.Result result = remainingSplits.isEmpty()
        ? incrementalInputSplits.inputSplits(metaClient, this.issuedOffset, this.cdcEnabled)
        : IncrementalInputSplits.Result.instance(remainingSplits, issuedInstant, issuedOffset);

    List<MergeOnReadInputSplit> inputSplits = result.getInputSplits();

    // 限流：每轮最多发送 splitsLimit 个分片
    int endIndex = Math.min(splitsLimit, inputSplits.size());
    for (int index = 0; index < endIndex; index++) {
        context.collect(inputSplits.get(index));
    }

    // 保存剩余分片
    remainingSplits = inputSplits.stream().skip(endIndex).collect(Collectors.toList());

    // 更新消费位移
    this.issuedInstant = result.getEndInstant();
    this.issuedOffset = result.getOffset();
}
```

**注意**：`IncrementalInputSplits` 提供了两个 `inputSplits()` 重载方法：
- `inputSplits(HoodieTableMetaClient, boolean)` - 通过配置 `FlinkOptions.READ_START_COMMIT` 控制起始位置
- `inputSplits(HoodieTableMetaClient, String issuedOffset, boolean)` - 直接传入 `issuedOffset` 参数

`StreamReadMonitoringFunction` 使用第二个重载版本（见源码第253行），直接传入 `issuedOffset` 来控制增量读取的起始位置。
```

**`splitsLimit` 限流设计的好处：**

配置项 `read.splits.limit`（默认 `Integer.MAX_VALUE`，即不限制）控制每轮最多发送多少个分片。这个设计解决了以下问题：

1. **防止内存溢出**: 如果一个 commit 产生了大量文件（如大规模数据导入），一次性将所有分片发送给下游可能导致 `StreamReadOperator` 的队列无限增长。
2. **背压传导**: 限制每轮发送量，让下游有时间消化。未发送的分片保存在 `remainingSplits` 中，在下一轮继续发送。
3. **Checkpoint 友好**: 分片量可控意味着 checkpoint 的 state 大小也可控。

#### 5.3 FLIP-27 架构下的分片发现

在 Source V2 架构中，分片发现由 `HoodieContinuousSplitEnumerator` 负责：

```
hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/enumerator/HoodieContinuousSplitEnumerator.java
```

```java
public class HoodieContinuousSplitEnumerator extends AbstractHoodieSplitEnumerator {
    private final AtomicReference<HoodieEnumeratorPosition> position;

    @Override
    public void start() {
        super.start();
        // 使用 callAsync 注册周期性的分片发现任务
        enumeratorContext.callAsync(
            this::discoverSplits,
            this::processDiscoveredSplits,
            0L,
            scanContext.getScanInterval().toMillis());
    }

    private HoodieContinuousSplitBatch discoverSplits() {
        int pendingSplitNumber = splitProvider.pendingSplitCount();
        if (pendingSplitNumber > scanContext.getMaxPendingSplits()) {
            // 如果待处理的分片太多，暂停发现
            return HoodieContinuousSplitBatch.EMPTY;
        }
        return splitDiscover.discoverSplits(
            position.get().getIssuedOffset().isPresent() 
                ? position.get().getIssuedOffset().get() 
                : null);
    }
}
```

**与 Legacy Source 的关键差异：**

1. **分片分配机制**: Legacy Source 通过 `keyBy` 将分片分发到 reader；Source V2 通过 `SplitAssigner` 按需分配，reader 完成一个 split 后主动请求下一个。
2. **负载均衡**: Source V2 的 `HoodieSplitAssigners` 工厂根据写入模式选择不同的分配策略：

```java
public static HoodieSplitAssigner createHoodieSplitAssigner(Configuration config, int parallelism) {
    if (OptionsResolver.isMorWithBucketIndexUpsert(config)) {
        return new HoodieSplitBucketAssigner(parallelism, config);
    } else if (OptionsResolver.isAppendMode(config)) {
        return new HoodieSplitNumberAssigner(parallelism);
    }
    return new DefaultHoodieSplitAssigner(parallelism);
}
```

---

### 6. CDC 读取

#### 6.1 CDC 读取概述

Hudi 的 CDC（Change Data Capture）功能允许读取数据的变更历史，而不仅仅是最新快照。这对于数据同步、审计追踪和增量 ETL 非常有用。

**相关源码路径：**
- `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/format/cdc/CdcInputFormat.java`
- `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/format/cdc/CdcIterators.java`
- `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/format/cdc/CdcImageManager.java`
- `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/reader/function/HoodieCdcSplitReaderFunction.java`

#### 6.2 CdcInputFormat -- CDC 读取入口

```java
public class CdcInputFormat extends MergeOnReadInputFormat {
    @Override
    protected ClosableIterator<RowData> initIterator(MergeOnReadInputSplit split) throws IOException {
        if (split instanceof CdcInputSplit) {
            // CDC 专用分片，包含 HoodieCDCFileSplit 数组
            HoodieCDCSupplementalLoggingMode mode = OptionsResolver.getCDCSupplementalLoggingMode(conf);
            CdcImageManager manager = new CdcImageManager(
                tableState.getRowType(), FlinkWriteClients.getHoodieClientConfig(conf),
                this::getFileSliceIterator);
            // 创建 CDC 文件分片迭代器
            return new CdcIterators.CdcFileSplitsIterator(
                ((CdcInputSplit) split).getChanges(), manager, recordIteratorFunc);
        } else {
            // 非 CDC 分片（如全表扫描的初始快照），走标准 MOR 读取逻辑
            return super.initIterator(split);
        }
    }
}
```

**为什么 CdcInputFormat 继承自 MergeOnReadInputFormat？**

因为 CDC 读取在某些场景下需要回退到标准的 MOR 读取逻辑（例如初始全表扫描时没有 CDC 日志）。继承关系让这种回退变得自然，同时复用了 MOR 的所有基础设施。

#### 6.3 CDC 推断类型 (Infer Cases)

Hudi 的 CDC 实现根据文件变更类型推断出不同的 CDC 事件，由 `HoodieCDCFileSplit.getCdcInferCase()` 返回：

```java
private ClosableIterator<RowData> getRecordIterator(..., HoodieCDCFileSplit fileSplit, ...) {
    switch (fileSplit.getCdcInferCase()) {
        case BASE_FILE_INSERT:
            // 新的 base file 被写入 -> 所有记录标记为 INSERT
            return new CdcIterators.AddBaseFileIterator(getBaseFileIterator(path));

        case BASE_FILE_DELETE:
            // base file 被删除 -> 所有记录标记为 DELETE
            return new CdcIterators.RemoveBaseFileIterator(..., getFileSliceIterator(inputSplit));

        case AS_IS:
            // CDC 日志文件已经包含变更信息，按 supplemental logging mode 处理
            switch (mode) {
                case DATA_BEFORE_AFTER: return new CdcIterators.BeforeAfterImageIterator(...);
                case DATA_BEFORE:       return new CdcIterators.BeforeImageIterator(...);
                case OP_KEY_ONLY:       return new CdcIterators.RecordKeyImageIterator(...);
            }

        case LOG_FILE:
            // 普通 log file -> 需要与 before image 做 diff 来推导变更
            return new CdcIterators.DataLogFileIterator(...);

        case REPLACE_COMMIT:
            // Replace Commit（如 Insert Overwrite）-> before slice 的所有记录标记为 DELETE
            return new CdcIterators.ReplaceCommitIterator(...);
    }
}
```

#### 6.4 三种 Supplemental Logging Mode

这是 Hudi CDC 的核心配置（`cdc.supplemental.logging.mode`），决定了 CDC 日志中记录多少信息：

| 模式 | 日志内容 | 存储开销 | 读取效率 |
|------|---------|---------|---------|
| `DATA_BEFORE_AFTER` | op + ts + before_image + after_image | 最高（每条记录存储完整的前后镜像） | 最高（直接读取，不需要额外查找） |
| `DATA_BEFORE` | op + key + before_image | 中等 | 中等（after image 需要从当前 file slice 查找） |
| `OP_KEY_ONLY` | op + key | 最低 | 最低（before 和 after image 都需要从 file slice 查找） |

**`DATA_BEFORE_AFTER` 模式的读取（`BeforeAfterImageIterator`）：**

```java
public static class BeforeAfterImageIterator extends BaseImageIterator {
    @Override
    protected RowData getAfterImage(RowKind rowKind, GenericRecord cdcRecord) {
        return resolveAvro(rowKind, (GenericRecord) cdcRecord.get(3)); // index 3 = after_image
    }

    @Override
    protected RowData getBeforeImage(RowKind rowKind, GenericRecord cdcRecord) {
        return resolveAvro(rowKind, (GenericRecord) cdcRecord.get(2)); // index 2 = before_image
    }
}
```

**`OP_KEY_ONLY` 模式需要 `CdcImageManager`：**

当 CDC 日志只包含操作类型和 key 时，需要 `CdcImageManager` 从实际的 file slice 加载 before/after image。这使用了 `ExternalSpillableMap` 进行内存+磁盘的混合存储，避免在大数据量下内存溢出。

#### 6.5 DataLogFileIterator -- LOG_FILE 推断类型

这是最复杂的 CDC 迭代器，需要将 log 文件中的记录与 before image 做对比来推导变更类型：

```java
public static class DataLogFileIterator implements ClosableIterator<RowData> {
    @Override
    public boolean hasNext() {
        while (logRecordIterator.hasNext()) {
            HoodieRecord<RowData> record = logRecordIterator.next();
            RowData existed = imageManager.removeImageRecord(record.getRecordKey(), beforeImages);

            if (isDelete(record)) {
                if (existed != null) {
                    existed.setRowKind(RowKind.DELETE);
                    currentImage = existed;
                    return true;
                }
            } else {
                if (existed == null) {
                    // 新记录，标记为 INSERT
                    record.getData().setRowKind(RowKind.INSERT);
                    currentImage = record.getData();
                    return true;
                } else {
                    // 更新记录，发送 UPDATE_BEFORE + UPDATE_AFTER
                    existed.setRowKind(RowKind.UPDATE_BEFORE);
                    currentImage = existed;
                    mergedRow.setRowKind(RowKind.UPDATE_AFTER);
                    sideImage = mergedRow;
                    return true;
                }
            }
        }
        return false;
    }
}
```

**关键设计 -- `sideImage` 机制：**

对于 UPDATE 操作，需要连续发出两条记录：`UPDATE_BEFORE`（旧值）和 `UPDATE_AFTER`（新值）。但 `hasNext()/next()` 接口每次只能返回一条。解决方案是用 `sideImage` 缓存第二条记录，下次 `hasNext()` 时直接返回。

#### 6.6 Source V2 下的 CDC 读取

在 FLIP-27 Source V2 架构中，CDC 读取由 `HoodieCdcSplitReaderFunction` 实现：

```java
public class HoodieCdcSplitReaderFunction extends AbstractSplitReaderFunction {
    @Override
    public RecordsWithSplitIds<HoodieRecordWithPosition<RowData>> read(HoodieSourceSplit split) {
        if (!(split instanceof HoodieCdcSourceSplit)) {
            // 非 CDC 分片，回退到标准 MOR 读取
            return getFallbackReaderFunction().read(split);
        }
        HoodieCdcSourceSplit cdcSplit = (HoodieCdcSourceSplit) split;
        // 创建 CDC 迭代器（复用 CdcIterators 中的所有迭代器实现）
        currentIterator = new CdcIterators.CdcFileSplitsIterator(
            cdcSplit.getChanges(), imageManager, recordIteratorFunc);
        return BatchRecords.forRecords(split.splitId(), currentIterator, ...);
    }
}
```

**`IncrementalInputSplits` 被两套架构复用的设计优势：**

`IncrementalInputSplits` 类是一个独立的工具类，不依赖于特定的 Source 框架。这使得 Legacy Source 和 Source V2 可以共享增量分片生成的核心逻辑。在 Source V2 架构中，`HoodieContinuousSplitDiscover` 内部也使用 `IncrementalInputSplits` 来生成分片。

**`CdcIterators` 被两套架构复用的设计优势：**

注意 `CdcIterators` 类的 Javadoc：

```java
/**
 * Shared iterator implementations for CDC record reading, used by both
 * {@link CdcInputFormat} and the Source V2 CDC split reader.
 */
public final class CdcIterators {
```

所有的 CDC 迭代器（`AddBaseFileIterator`、`RemoveBaseFileIterator`、`DataLogFileIterator` 等）都是独立的工具类，不依赖于特定的 Source 框架。这使得 Legacy Source 和 Source V2 可以共享 CDC 读取的核心逻辑。

---

### 7. StreamReadOperator 的 Checkpoint 协调

#### 7.1 StreamReadOperator 架构

```
hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/StreamReadOperator.java
```

`StreamReadOperator` 是 Legacy Source 架构中的分片读取算子：

```java
public class StreamReadOperator extends AbstractStreamOperator<RowData>
    implements OneInputStreamOperator<MergeOnReadInputSplit, RowData> {

    private static final int MINI_BATCH_SIZE = 2048;
    private final MailboxExecutor executor;
    private MergeOnReadInputFormat format;
    private transient Queue<MergeOnReadInputSplit> splits; // 待处理分片队列
    private transient volatile SplitState currentSplitState; // IDLE 或 RUNNING
}
```

#### 7.2 MailboxExecutor + Mini-Batch 读取

```java
private void processSplits() throws IOException {
    MergeOnReadInputSplit split = splits.peek();
    if (split == null) {
        currentSplitState = SplitState.IDLE;
        return;
    }

    if (format.isClosed()) {
        format.open(split);
    }

    try {
        consumeAsMiniBatch(split);
    } finally {
        currentSplitState = SplitState.IDLE;
    }

    enqueueProcessSplits();
}

private void consumeAsMiniBatch(MergeOnReadInputSplit split) throws IOException {
    for (int i = 0; i < MINI_BATCH_SIZE; i++) {
        if (!format.reachedEnd()) {
            sourceContext.collect(format.nextRecord(null));
            split.consume(); // 记录消费进度
        } else {
            format.close();
            splits.poll();
            break;
        }
    }
}
```

**为什么使用 Mini-Batch 模式（每次最多读 2048 条）而不是一次读完整个分片？**

这是一个精妙的设计：

1. **Checkpoint 不阻塞**: 读取任务和 checkpoint 任务运行在同一个线程（Flink 的 Mailbox 线程）。如果一次性读完整个分片可能需要很长时间，期间 checkpoint barrier 无法被处理。Mini-batch 模式确保每读完 2048 条就会让出 mailbox，给 checkpoint barrier 处理的机会。

2. **背压响应**: 如果下游处理速度跟不上，output buffer 会满。Mini-batch 读取让 operator 能及时感知背压。

3. **内存可控**: 每次最多 2048 条记录在 pipeline 中流动，避免内存峰值。

**`enqueueProcessSplits` 的精巧之处：**

```java
private void enqueueProcessSplits() {
    if (currentSplitState == SplitState.IDLE && !splits.isEmpty()) {
        currentSplitState = SplitState.RUNNING;
        executor.execute(this::processSplits, "process input split");
    }
}
```

使用 `SplitState` 枚举确保同一时间只有一个读取任务在 mailbox 队列中。这避免了多个读取任务累积在 checkpoint 任务前面，导致 checkpoint 超时。

#### 7.3 State 管理与 Exactly-Once 语义

```java
@Override
public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    inputSplitsState.clear();
    inputSplitsState.addAll(new ArrayList<>(splits));
}

@Override
public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    inputSplitsState = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>("splits", new JavaSerializer<>()));
    splits = new LinkedBlockingDeque<>();
    if (context.isRestored()) {
        for (MergeOnReadInputSplit split : inputSplitsState.get()) {
            splits.add(split);
        }
    }
    enqueueProcessSplits();
}
```

**Exactly-Once 语义的保证链路：**

1. **StreamReadMonitoringFunction** 在 `snapshotState` 中保存 `issuedInstant` 和 `issuedOffset`
2. **StreamReadOperator** 在 `snapshotState` 中保存未完成的分片（包含消费进度 `consumed`）
3. 当从 checkpoint 恢复时：
   - MonitoringFunction 从上次的 `issuedOffset` 继续发现分片
   - StreamReadOperator 恢复未完成的分片并通过 `mayShiftInputSplit` 跳过已消费的记录

**端到端 exactly-once 的条件：**

- Flink checkpoint 必须启用
- 上游的 `MergeOnReadInputSplit.consumed` 准确记录了消费进度
- 下游 sink 支持 exactly-once（如使用两阶段提交的 sink）

#### 7.4 MonitoringFunction 的 State 管理

```java
@Override
public void snapshotState(FunctionSnapshotContext context) throws Exception {
    this.instantState.clear();
    if (this.issuedInstant != null) {
        this.instantState.add(this.issuedInstant);
    }
    if (this.issuedOffset != null) {
        this.instantState.add(this.issuedOffset);
    }
    // 保存剩余未发送的分片
    SplitState splitState = new SplitState(this.totalSplits, this.remainingSplits);
    inputSplitsState.clear();
    inputSplitsState.add(splitState);
}
```

注意 `instantState` 中存储了两个值（`issuedInstant` 和 `issuedOffset`），通过列表的顺序来区分。恢复时：

```java
if (retrievedStates.size() == 1) {
    this.issuedInstant = retrievedStates.get(0); // 向前兼容老版本
} else if (retrievedStates.size() == 2) {
    this.issuedInstant = retrievedStates.get(0);
    this.issuedOffset = retrievedStates.get(1);
}
```

这种设计保证了从旧版本升级时的向前兼容性。

---

## 第三部分：Flink 读取优化

### 8. 谓词下推

#### 8.1 SupportsFilterPushDown 实现

```java
@Override
public Result applyFilters(List<ResolvedExpression> filters) {
    // Step 1: 过滤出简单的 Call 表达式（Hudi 不支持复杂的子查询谓词）
    List<ResolvedExpression> simpleFilters = filterSimpleCallExpression(filters);

    // Step 2: 将过滤条件分为分区列条件和数据列条件
    Tuple2<List<ResolvedExpression>, List<ResolvedExpression>> splitFilters =
        splitExprByPartitionCall(simpleFilters, this.partitionKeys, this.tableRowType);

    // Step 3: 构建数据列谓词（用于 Parquet 行组级过滤）
    this.predicates = ExpressionPredicates.fromExpression(splitFilters.f0);

    // Step 4: 构建列统计探针（用于 Data Skipping）
    this.columnStatsProbe = ColumnStatsProbe.newInstance(splitFilters.f0);

    // Step 5: 构建分区裁剪器
    this.partitionPruner = createPartitionPruner(splitFilters.f1, columnStatsProbe);

    // Step 6: 构建 Bucket 裁剪函数
    this.dataBucketFunc = getDataBucketFunc(splitFilters.f0);

    // 告诉 Flink：只有分区条件被完全消费，数据条件仍需 Flink 重新过滤
    return SupportsFilterPushDown.Result.of(
        new ArrayList<>(splitFilters.f1),  // accepted filters（分区条件）
        new ArrayList<>(filters));          // remaining filters（所有条件仍保留）
}
```

**为什么要将所有 filter 都放在 remaining 中？**

注意第二个参数 `new ArrayList<>(filters)` 包含了原始的所有过滤条件。这意味着 Hudi 告诉 Flink："我会尽力在内部过滤，但不保证完全过滤干净，请你在上层再过滤一次"。

这种保守策略的原因是：
1. Parquet 的行组级过滤是近似的（基于 min/max 统计），可能会漏网
2. 某些复杂谓词 Hudi 无法完全下推
3. Schema 演进可能导致类型不匹配的边界情况

#### 8.2 ExpressionPredicates -- 谓词转换

```
hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/ExpressionPredicates.java
```

这个类负责将 Flink 的 `ResolvedExpression` 转换为 Hudi 内部的 `Predicate` 对象：

```java
public static Predicate fromExpression(CallExpression callExpression) {
    FunctionDefinition functionDefinition = callExpression.getFunctionDefinition();
    // 支持的谓词类型：
    // - EQUALS, NOT_EQUALS
    // - GREATER_THAN, GREATER_THAN_OR_EQUAL
    // - LESS_THAN, LESS_THAN_OR_EQUAL
    // - IS_NULL, IS_NOT_NULL
    // - IN
    // - NOT
    // - AND, OR
}
```

这些 `Predicate` 对象会在两个地方使用：
1. **Parquet 行组过滤**: 转换为 Parquet 的 `FilterPredicate` 用于跳过不匹配的 Row Group
2. **Data Skipping**: 转换为 `ExpressionEvaluators.Evaluator` 用于基于列统计信息跳过文件

#### 8.3 分区裁剪的多策略实现

```
hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/prune/PartitionPruners.java
```

Hudi 提供了四种分区裁剪策略，可以链式组合：

1. **DynamicPartitionPruner**: 运行时动态评估分区是否匹配

```java
public static class DynamicPartitionPruner implements PartitionPruner {
    private boolean evaluate(String partition) {
        // 解析分区路径为 key=value 对
        String[] partStrArray = FilePathUtils.extractPartitionKeyValues(...);
        // 构造列统计信息（分区值的 min = max = value）
        Map<String, ColumnStats> partStats = new LinkedHashMap<>();
        for (int idx = 0; idx < partitionKeys.length; idx++) {
            ColumnStats columnStats = new ColumnStats(partVal, partVal, partVal == null ? 1 : 0);
            partStats.put(partKey, columnStats);
        }
        // 评估所有谓词
        return partitionEvaluator.stream().allMatch(evaluator -> evaluator.eval(partStats));
    }
}
```

2. **StaticPartitionPruner**: 编译期确定的分区集合

3. **ColumnStatsPartitionPruner**: 基于 Metadata Table 中的分区统计信息

```java
public static class ColumnStatsPartitionPruner implements PartitionPruner {
    @Override
    public Set<String> filter(Collection<String> partitions) {
        Set<String> candidatePartitions =
            partitionStatsIndex.computeCandidatePartitions(probe, new ArrayList<>(partitions));
        return partitions.stream().filter(candidatePartitions::contains).collect(Collectors.toSet());
    }
}
```

4. **ChainedPartitionPruner**: 链式组合多个裁剪器

```java
public static class ChainedPartitionPruner implements PartitionPruner {
    @Override
    public Set<String> filter(Collection<String> partitions) {
        for (PartitionPruner pruner: pruners) {
            partitions = pruner.filter(partitions); // 每层输出是下层输入
        }
        return new HashSet<>(partitions);
    }
}
```

Builder 中根据配置自动组合：

```java
public PartitionPruner build() {
    PartitionPruner staticPruner = ...;
    PartitionPruner dynamicPruner = ...;
    PartitionPruner columnStatsPruner = ...;
    List<PartitionPruner> pruners = Stream.of(staticPruner, dynamicPruner, columnStatsPruner)
        .filter(Objects::nonNull).collect(Collectors.toList());
    if (pruners.size() < 2) return pruners.get(0);
    return new ChainedPartitionPruner(pruners);
}
```

---

### 9. 列裁剪

#### 9.1 SupportsProjectionPushDown 实现

```java
@Override
public boolean supportsNestedProjection() {
    return false; // 不支持嵌套列裁剪（如 struct.field）
}

@Override
public void applyProjection(int[][] projections, DataType producedDataType) {
    this.requiredPos = Arrays.stream(projections).mapToInt(array -> array[0]).toArray();
}
```

**为什么不支持嵌套列裁剪？**

Hudi 使用 Avro 作为内部序列化格式，Avro 的列式读取对嵌套结构的支持有限。Parquet 虽然原生支持嵌套列裁剪，但 Hudi 的 Schema Evolution 机制（`InternalSchemaManager`）需要在字段级别做映射，嵌套裁剪会大幅增加复杂度。因此当前版本只支持顶层列裁剪。

#### 9.2 列裁剪的传递链路

`requiredPos` 数组记录了用户查询实际需要的列位置，它被传递到：

1. **CopyOnWriteInputFormat**: `selectedFields` -> Parquet reader 只读取需要的列
2. **MergeOnReadInputFormat**: `tableState.getRequiredPositions()` -> base file 读取和 log merge 都只处理需要的列
3. **MergeOnReadTableState**: 存储 `requiredRowType` 和 `requiredSchema`

```java
private DataType getProducedDataType() {
    String[] schemaFieldNames = this.schema.getColumnNames().toArray(new String[0]);
    DataType[] schemaTypes = this.schema.getColumnDataTypes().toArray(new DataType[0]);
    return DataTypes.ROW(Arrays.stream(this.requiredPos)
        .mapToObj(i -> DataTypes.FIELD(schemaFieldNames[i], schemaTypes[i]))
        .toArray(DataTypes.Field[]::new))
        .bridgedTo(RowData.class);
}
```

**列裁剪的性能收益：**

- **减少 I/O**: Parquet 是列式格式，只读取需要的列可以显著减少磁盘读取量
- **减少反序列化**: 不需要的列不会被反序列化为 Java 对象
- **减少网络传输**: 在分布式环境下，较少的列意味着较少的序列化/反序列化和网络传输

---

### 10. MOR 表读取的合并机制

#### 10.1 HoodieFileGroupReader -- 引擎无关的合并核心

Flink 和 Spark 的 MOR 表合并都收敛到了同一个核心组件 `HoodieFileGroupReader`：

```java
// FormatUtils.java
public static HoodieFileGroupReader<RowData> createFileGroupReader(
    HoodieTableMetaClient metaClient,
    HoodieWriteConfig writeConfig,
    InternalSchemaManager internalSchemaManager,
    FileSlice fileSlice,
    HoodieSchema tableSchema,
    HoodieSchema requiredSchema,
    String latestInstant,
    String mergeType,
    boolean emitDelete,
    List<ExpressionPredicates.Predicate> predicates,
    Option<InstantRange> instantRangeOption) {

    final FlinkRowDataReaderContext readerContext = new FlinkRowDataReaderContext(
        metaClient.getStorageConf(),
        () -> internalSchemaManager,
        predicates,
        metaClient.getTableConfig(),
        instantRangeOption);

    return HoodieFileGroupReader.<RowData>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metaClient)
        .withLatestCommitTime(latestInstant)
        .withFileSlice(fileSlice)
        .withDataSchema(tableSchema)
        .withRequestedSchema(requiredSchema)
        .withProps(typedProps)
        .withShouldUseRecordPosition(false)
        .withEmitDelete(emitDelete)
        .build();
}
```

**`FlinkRowDataReaderContext` 的角色：**

这是 Flink 引擎提供给 `HoodieFileGroupReader` 的"适配器"，实现了：
- Parquet 文件的读取（返回 `RowData`）
- Log 文件的读取（返回 `RowData`）
- 记录合并逻辑
- 谓词过滤

#### 10.2 合并流程详解

`HoodieFileGroupReader` 的合并流程：

1. **读取 Base File**: 如果存在，读取 Parquet base file 中的记录
2. **读取 Log Files**: 读取所有 log files 中的记录
3. **合并**: 根据 `mergeType` 决定合并策略
   - `payload_combine`: 使用 `RecordMerger` 将 base 和 log 记录合并
   - `skip_merge`: 直接输出所有记录（base + log），不做去重合并
4. **过滤**: 根据 `InstantRange` 过滤增量数据
5. **投影**: 根据 `requiredSchema` 只输出需要的列

**两种 mergeType 的使用场景：**

| mergeType | 使用场景 | 行为 |
|-----------|---------|------|
| `payload_combine` | 快照查询、增量查询 | 根据 record key 去重，保留最新版本 |
| `skip_merge` | 流式读取（需要发 DELETE） | 输出所有记录，保留变更历史 |

#### 10.3 与 Spark 的 MOR 合并差异

| 维度 | Flink | Spark |
|------|-------|-------|
| 核心组件 | `HoodieFileGroupReader` | `HoodieFileGroupReader`（同一组件） |
| ReaderContext | `FlinkRowDataReaderContext` | `SparkFileFormatInternalRowReaderContext` |
| 输出格式 | `RowData` | `InternalRow` |
| 位置感知合并 | `shouldUseRecordPosition = false` | 可以使用 record position 加速合并 |
| 流式支持 | `emitDelete = true` 发送 DELETE 消息 | 不支持流式 DELETE |

**为什么 Flink 设置 `shouldUseRecordPosition = false`？**

Record position 是 Hudi 的一个优化机制，通过记录在 base file 中的物理位置来加速 merge。但这需要 base file reader 支持按位置查找（random access），Flink 的 Parquet reader 当前不支持这个特性，因此设为 false。

---

## 第四部分：Flink 读写联动

### 11. 端到端链路分析

#### 11.1 一条数据从写入到被消费的完整链路

```
[Writer] -> [DeltaCommit] -> [Timeline] -> [MonitoringFunction] -> [StreamReadOperator] -> [Consumer]
```

详细步骤：

1. **Writer 写入**:
   - Flink Writer 将数据写入 log files（MOR 表）或 base files（COW 表）
   - 数据被缓冲在内存中，checkpoint 时触发 flush

2. **DeltaCommit 提交**:
   - Checkpoint 成功后，Coordinator 将写入的文件信息提交为一个 deltacommit instant
   - instant 的 completion time 被记录

3. **Timeline 更新**:
   - Hudi 的 `.hoodie` 目录下出现新的 instant 文件
   - Timeline Server 可以被通知到新的 commit

4. **MonitoringFunction 发现**:
   - `StreamReadMonitoringFunction` 每隔 `read.streaming.check-interval` 秒检查一次
   - 调用 `metaClient.reloadActiveTimeline()` 重新加载时间线
   - `IncrementalInputSplits` 分析新的 commit，生成 `MergeOnReadInputSplit`

5. **分片分发**:
   - 通过 `keyBy(fileId)` 或自定义 Partitioner 分发到 StreamReadOperator

6. **StreamReadOperator 读取**:
   - 使用 `MergeOnReadInputFormat` 打开分片
   - `HoodieFileGroupReader` 合并 base + log files
   - 以 mini-batch (2048条) 方式输出 RowData

7. **下游消费**:
   - 下游算子接收 RowData 进行处理
   - 如果是 changelog 模式，可以接收 INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE

#### 11.2 延迟构成分析

```
总端到端延迟 = 写入延迟 + Commit 延迟 + 发现延迟 + 读取延迟 + 处理延迟
```

| 延迟组成 | 典型值 | 影响因素 | 优化手段 |
|---------|-------|---------|---------|
| 写入延迟 | 由 checkpoint 间隔决定 | Checkpoint interval | 减小 checkpoint interval |
| Commit 延迟 | 毫秒级 | 文件系统写入速度 | 使用高性能文件系统 |
| 发现延迟 | 0 ~ `check-interval` 秒 | `read.streaming.check-interval` | 减小检查间隔（但增加文件系统负载） |
| 读取延迟 | 取决于文件大小和数量 | 文件数量、大小、合并复杂度 | 增加 `read.tasks` 并行度 |
| 处理延迟 | 取决于下游逻辑 | 下游算子复杂度 | 增加下游并行度 |

**关键瓶颈 -- 发现延迟：**

默认 `read.streaming.check-interval = 60`（秒），意味着新 commit 最多需要 60 秒才能被发现。对于实时性要求高的场景，可以减小到 5-10 秒。但需要注意：

- 检查间隔太小会增加文件系统的元数据操作压力
- 如果使用 Metadata Table，元数据查找效率很高，可以设置较小的间隔
- 如果直接列目录，大量分区会导致每次检查非常耗时

#### 11.3 流式读取的 Metrics 监控

```
hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/metrics/FlinkStreamReadMetrics.java
```

```java
public class FlinkStreamReadMetrics extends HoodieFlinkMetrics {
    private long issuedInstant;          // MonitoringFunction 已发出的最新 instant
    private long issuedInstantDelay;     // 已发出 instant 与当前时间的延迟
    private long splitLatestCommit;      // StreamReadOperator 正在处理的 split 的 commit 时间
    private long splitLatestCommitDelay; // 正在处理的 commit 与当前时间的延迟
}
```

通过监控 `issuedInstantDelay` 和 `splitLatestCommitDelay`，运维人员可以了解读取的延迟情况，判断是否需要增加读取并行度或调整配置。

---

### 12. 读取与 Compaction 的协调

#### 12.1 流式读取默认跳过 Compaction

```java
public static final ConfigOption<Boolean> READ_STREAMING_SKIP_COMPACT = ConfigOptions
    .key("read.streaming.skip_compaction")
    .booleanType()
    .defaultValue(true)
    .withDescription("Whether to skip compaction instants and avoid reading compacted base files "
        + "for streaming read to improve read performance.");
```

**为什么默认跳过？**

考虑以下时间线：

```
deltacommit1 -> deltacommit2 -> compaction1 -> deltacommit3
```

如果不跳过 compaction1，流式读取会同时读到：
- deltacommit1 和 deltacommit2 的增量数据（通过之前的增量读取已经消费）
- compaction1 产生的 base file（包含 deltacommit1 和 deltacommit2 的合并结果）

这会导致**数据重复**。

默认跳过 compaction 的读取意味着：流式读取只关注 deltacommit，忽略 compaction 产生的文件。数据完整性通过增量读取 delta 来保证。

#### 12.2 读取如何感知 Compaction 产生的新文件

虽然流式读取跳过 compaction commit，但在构建 FileSystemView 时，compaction 产生的 base file 仍然是最新的文件版本。这意味着：

1. **增量读取的 log 过滤**: `InstantRange` 确保只读取指定时间范围内的 log 记录，已被 compaction 合并的旧 log 不会被重复读取
2. **全表扫描路径**: 如果从最早开始读（`read.start-commit=earliest`），会走全表扫描路径，此时 compaction 产生的 base file 会被正确读取

**`IncrementalInputSplits` 中的关键逻辑：**

```java
IncrementalQueryAnalyzer analyzer = IncrementalQueryAnalyzer.builder()
    .metaClient(metaClient)
    .skipCompaction(skipCompaction)      // true -> 分析时跳过 compaction instant
    .skipClustering(skipClustering)      // true -> 分析时跳过 clustering instant
    .skipInsertOverwrite(skipInsertOverwrite)
    .build();
```

#### 12.3 跳过 Clustering 的设计

类似 compaction，clustering 也会重组文件但不产生新数据。默认跳过：

```java
public static final ConfigOption<Boolean> READ_STREAMING_SKIP_CLUSTERING = ConfigOptions
    .key("read.streaming.skip_clustering")
    .booleanType()
    .defaultValue(true)
    .withDescription("Whether to skip clustering instants to avoid reading base files of "
        + "clustering operations for streaming read to improve read performance.");
```

#### 12.4 Insert Overwrite 的特殊处理

```java
public static final ConfigOption<Boolean> READ_STREAMING_SKIP_INSERT_OVERWRITE = ConfigOptions
    .key("read.streaming.skip_insertoverwrite")
    .booleanType()
    .defaultValue(false) // 默认不跳过！
    .withDescription("Whether to skip insert overwrite instants to avoid reading base files "
        + "of insert overwrite operations for streaming read.");
```

**为什么 Insert Overwrite 默认不跳过？**

Insert Overwrite 通常用于数据修复或全量覆盖，其语义是"用新数据完全替换旧数据"。跳过它意味着下游不会感知到数据修复，可能导致数据不一致。因此默认不跳过。

但在某些流式场景中，如果 Insert Overwrite 只是用于初始化数据，用户可以手动设置 `skip_insertoverwrite=true`。

---

## 第五部分：Flink vs Spark 读取架构对比

### 13. 全维度对比

#### 13.1 整体架构对比

| 维度 | Flink | Spark |
|------|-------|-------|
| **入口类** | `HoodieTableSource` (DynamicTableSource) | `HoodieFileIndex` + `HoodieFileGroupReaderBasedFileFormat` |
| **批量读取** | `InputFormat` 体系 (CopyOnWriteInputFormat / MergeOnReadInputFormat) | `FileFormat.buildReaderWithPartitionValues()` |
| **流式读取** | 原生支持 (StreamReadMonitoringFunction + StreamReadOperator 或 FLIP-27 Source) | 不原生支持（需要 Spark Structured Streaming + HoodieStreamSource） |
| **合并核心** | `HoodieFileGroupReader`（引擎无关） | `HoodieFileGroupReader`（同一组件） |
| **谓词下推** | `SupportsFilterPushDown` -> Parquet FilterPredicate | Catalyst 优化器 -> Parquet FilterPredicate |
| **列裁剪** | `SupportsProjectionPushDown` (仅顶层) | Catalyst 优化器 (支持嵌套) |
| **分区裁剪** | `PartitionPruners` (Dynamic/Static/ColumnStats/Chained) | `HoodieFileIndex.listMatchingPartitionPaths` |
| **Data Skipping** | `FileStatsIndex` + `ColumnStatsProbe` | `ColumnStatsIndexSupport` + Catalyst Expression |
| **Record Level Index** | `RecordLevelIndex` | `RecordLevelIndexSupport` |
| **CDC** | 完整支持，三种 Supplemental Logging Mode | 部分支持 |
| **Schema Evolution** | `InternalSchemaManager` | InternalSchema 直接集成到 reader context |

#### 13.2 InputFormat vs FileFormat

**Flink InputFormat 的特点：**
- `InputFormat<RowData, InputSplit>` 是 Flink 的底层数据读取抽象
- 每个 InputSplit 对应一个读取任务
- 支持 checkpoint 恢复（通过 `MergeOnReadInputSplit.consumed`）
- 流式和批量共用同一套 InputFormat，通过 `emitDelete` 区分行为

**Spark FileFormat 的特点：**
- `FileFormat` 是 Spark SQL 的文件格式抽象
- 通过 `PartitionedFile` 描述读取范围
- 与 Catalyst 优化器深度集成
- 不支持原生流式读取

#### 13.3 流式读取能力对比

| 流式能力 | Flink | Spark |
|---------|-------|-------|
| 增量读取 | 原生支持，监控 timeline | Structured Streaming micro-batch |
| Changelog 输出 | 完整的 INSERT/UB/UA/DELETE | 仅支持 Append 模式 |
| CDC 读取 | 完整支持三种模式 | 有限支持 |
| Checkpoint/Offset 管理 | 精确到记录级别的 offset | 基于 instant time 的 offset |
| 读取并发 | 可配置 `read.tasks` | 取决于 file 数量和 partition 数量 |
| 延迟 | 秒级（`check-interval` 可调到 5s） | 分钟级（micro-batch interval） |

#### 13.4 MOR 合并对比

两者都使用 `HoodieFileGroupReader`，核心合并逻辑完全一致。差异在于：

| 合并差异 | Flink | Spark |
|---------|-------|-------|
| ReaderContext 实现 | `FlinkRowDataReaderContext` | `SparkFileFormatInternalRowReaderContext` |
| 输出数据类型 | `RowData`（Flink 内部二进制格式） | `InternalRow`（Spark 内部行格式） |
| Position-based merge | 不支持 (`shouldUseRecordPosition=false`) | 支持（可利用 record position 加速） |
| 内存管理 | `ExternalSpillableMap`（堆外溢写） | `ExternalSpillableMap`（同样） |

#### 13.5 分区裁剪对比

| 裁剪维度 | Flink | Spark |
|---------|-------|-------|
| 分区值过滤 | `DynamicPartitionPruner` 运行时评估 | `HoodieFileIndex` 编译期 + 运行期 |
| Partition Stats | `ColumnStatsPartitionPruner` 基于 Metadata Table | `PartitionStatsIndex` |
| Bucket 裁剪 | `PartitionBucketIdFunc` | `BucketIndexSupport` |
| 缓存 | 实例级缓存 | Session 级缓存（性能更好） |

---

### 14. Flink 读取配置参数完整手册

#### 14.1 基础读取配置

| 参数 | 默认值 | 说明 | 调优建议 |
|------|-------|------|---------|
| `hoodie.datasource.query.type` | `snapshot` | 查询类型：`snapshot` / `read_optimized` / `incremental` | 流式读取用 `snapshot`，性能优先用 `read_optimized` |
| `read.tasks` | 执行环境并行度 | 读取任务并行度 | 根据数据量和读取延迟需求调整，建议 = 文件数 / 10 |
| `read.utc-timezone` | `true` | 时间戳是否使用 UTC 时区 | Hive 3.x 用 `true`，Hive 0.x/1.x/2.x 用 `false` |

#### 14.2 流式读取配置

| 参数 | 默认值 | 说明 | 调优建议 |
|------|-------|------|---------|
| `read.streaming.enabled` | `false` | 是否以流式模式读取 | 流式场景设为 `true` |
| `read.streaming.check-interval` | `60` | 检查新 commit 的间隔（秒） | 实时性要求高可设为 5-10，注意文件系统负载 |
| `read.streaming.skip_compaction` | `true` | 是否跳过 compaction instant | 建议保持 `true`，避免数据重复 |
| `read.streaming.skip_clustering` | `true` | 是否跳过 clustering instant | 建议保持 `true` |
| `read.streaming.skip_insertoverwrite` | `false` | 是否跳过 insert overwrite instant | 如需忽略数据修复操作可设为 `true` |

#### 14.3 增量读取配置

| 参数 | 默认值 | 说明 | 调优建议 |
|------|-------|------|---------|
| `read.start-commit` | 无（最新 instant） | 增量读取起始 commit 时间 | 格式 `yyyyMMddHHmmss`，或 `earliest` 从最早开始 |
| `read.end-commit` | 无（最新 instant） | 增量读取结束 commit 时间 | 批量增量查询时设置 |
| `read.commits.limit` | 无限制 | 每次检查最多读取的 commit 数量 | 大数据量场景设置，避免单次处理太多 |
| `read.splits.limit` | `Integer.MAX_VALUE` | 每次检查最多发送的分片数量 | 控制下游背压，建议根据 reader 处理能力设置 |

#### 14.4 合并配置

| 参数 | 默认值 | 说明 | 调优建议 |
|------|-------|------|---------|
| `hoodie.datasource.merge.type` | `payload_combine` | 合并类型 | 通常不需要改，changelog 模式自动切换 |
| `compaction.max_memory` | `100` | Compaction/Merge 最大内存（MB） | 大文件场景可增大到 200-500 |

#### 14.5 Changelog / CDC 配置

| 参数 | 默认值 | 说明 | 调优建议 |
|------|-------|------|---------|
| `changelog.enabled` | `false` | 是否开启 changelog 模式 | 需要完整变更流时启用 |
| `cdc.enabled` | `false` | 是否开启 CDC | 需要 before/after image 时启用 |
| `cdc.supplemental.logging.mode` | `DATA_BEFORE_AFTER` | CDC 日志模式 | `DATA_BEFORE_AFTER` 读性能最好但存储开销最大 |
| `read.cdc.from.changelog` | `true` | CDC 是否从 changelog 文件读取 | `true` 直接读 CDC 文件，`false` 从文件依赖推断 |

#### 14.6 Data Skipping 配置

| 参数 | 默认值 | 说明 | 调优建议 |
|------|-------|------|---------|
| `read.data.skipping.enabled` | `false` | 是否启用 Data Skipping | 需同时启用 `metadata.enabled=true` |
| `read.data.skipping.rli.keys.max.num` | `8` | Record Level Index 最大查询 key 数 | 点查场景可适当增大 |
| `metadata.enabled` | `true` | 是否启用 Metadata Table | Data Skipping 的前提条件 |

#### 14.7 Source V2 配置

| 参数 | 默认值 | 说明 | 调优建议 |
|------|-------|------|---------|
| `read.source-v2.enabled` | `false` | 是否使用 FLIP-27 Source V2 | 新部署建议启用，更好的 checkpoint 集成 |

#### 14.8 Schema 相关配置

| 参数 | 默认值 | 说明 | 调优建议 |
|------|-------|------|---------|
| `source.avro-schema.path` | 无 | Avro Schema 文件路径 | 指定外部 schema 文件 |
| `source.avro-schema` | 无 | Avro Schema 字符串 | 直接指定 schema 内容 |

#### 14.9 Lookup Join 配置

| 参数 | 默认值 | 说明 | 调优建议 |
|------|-------|------|---------|
| `lookup.join.cache.ttl` | `60min` | Lookup Join 缓存 TTL | 根据数据变更频率调整 |
| `lookup.async` | `false` | 是否异步 Lookup | 高 QPS 场景启用 |
| `lookup.async-thread-number` | `16` | 异步线程数 | 根据 CPU 核数调整 |

#### 14.10 生产环境推荐配置

**低延迟流式读取场景：**

```sql
CREATE TABLE hudi_stream_source (
  ...
) WITH (
  'connector' = 'hudi',
  'path' = '...',
  'read.streaming.enabled' = 'true',
  'read.streaming.check-interval' = '10',
  'read.start-commit' = 'earliest',
  'read.tasks' = '4',
  'read.streaming.skip_compaction' = 'true',
  'read.streaming.skip_clustering' = 'true',
  'changelog.enabled' = 'true',
  'metadata.enabled' = 'true'
);
```

**CDC 增量同步场景：**

```sql
CREATE TABLE hudi_cdc_source (
  ...
) WITH (
  'connector' = 'hudi',
  'path' = '...',
  'read.streaming.enabled' = 'true',
  'read.streaming.check-interval' = '30',
  'cdc.enabled' = 'true',
  'cdc.supplemental.logging.mode' = 'DATA_BEFORE_AFTER',
  'read.cdc.from.changelog' = 'true',
  'read.tasks' = '4'
);
```

**大表批量快照查询场景：**

```sql
CREATE TABLE hudi_batch_source (
  ...
) WITH (
  'connector' = 'hudi',
  'path' = '...',
  'hoodie.datasource.query.type' = 'snapshot',
  'read.tasks' = '16',
  'read.data.skipping.enabled' = 'true',
  'metadata.enabled' = 'true'
);
```

**Read Optimized 高性能查询场景：**

```sql
CREATE TABLE hudi_ro_source (
  ...
) WITH (
  'connector' = 'hudi',
  'path' = '...',
  'hoodie.datasource.query.type' = 'read_optimized',
  'read.tasks' = '8'
);
```

---

## 附录：核心源码路径速查表

| 组件 | 源码路径 |
|------|---------|
| HoodieTableSource | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/HoodieTableSource.java` |
| CopyOnWriteInputFormat | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/format/cow/CopyOnWriteInputFormat.java` |
| MergeOnReadInputFormat | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/format/mor/MergeOnReadInputFormat.java` |
| MergeOnReadInputSplit | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/format/mor/MergeOnReadInputSplit.java` |
| CdcInputFormat | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/format/cdc/CdcInputFormat.java` |
| CdcIterators | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/format/cdc/CdcIterators.java` |
| CdcImageManager | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/format/cdc/CdcImageManager.java` |
| FormatUtils | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/format/FormatUtils.java` |
| FileIndex | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/FileIndex.java` |
| IncrementalInputSplits | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/IncrementalInputSplits.java` |
| StreamReadMonitoringFunction | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/StreamReadMonitoringFunction.java` |
| StreamReadOperator | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/StreamReadOperator.java` |
| HoodieSource (FLIP-27) | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/HoodieSource.java` |
| HoodieScanContext | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/HoodieScanContext.java` |
| HoodieContinuousSplitEnumerator | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/enumerator/HoodieContinuousSplitEnumerator.java` |
| AbstractHoodieSplitEnumerator | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/enumerator/AbstractHoodieSplitEnumerator.java` |
| HoodieSourceReader | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/reader/HoodieSourceReader.java` |
| HoodieSplitReaderFunction | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/reader/function/HoodieSplitReaderFunction.java` |
| HoodieCdcSplitReaderFunction | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/reader/function/HoodieCdcSplitReaderFunction.java` |
| PartitionPruners | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/prune/PartitionPruners.java` |
| ExpressionPredicates | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/ExpressionPredicates.java` |
| FileStatsIndex | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/stats/FileStatsIndex.java` |
| RecordLevelIndex | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/stats/RecordLevelIndex.java` |
| StreamReadBucketIndexPartitioner | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/rebalance/partitioner/StreamReadBucketIndexPartitioner.java` |
| HoodieSplitAssigners | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/source/split/assign/HoodieSplitAssigners.java` |
| FlinkOptions | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/configuration/FlinkOptions.java` |
| OptionsResolver | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/configuration/OptionsResolver.java` |
| ChangelogModes | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/util/ChangelogModes.java` |
| FlinkStreamReadMetrics | `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/metrics/FlinkStreamReadMetrics.java` |
