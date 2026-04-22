# Apache Hudi 工程架构全面解析

> **版本**: 1.2.0-SNAPSHOT (master 分支)  
> **文档日期**: 2026-04-21  
> **说明**: 本文档基于源码深度分析，系统性阐述 Hudi 的工程架构设计、模块职责、设计模式与核心特性。

---

## 一、项目概览

Apache Hudi（Hadoop Upserts Deletes and Incrementals）是一个开源的**数据湖仓平台（Data Lakehouse Platform）**，构建在高性能的开放表格式（Open Table Format）之上，支持在多种云数据环境中对数据进行摄取、索引、存储、服务、转换和管理。

### 1.1 核心定位

```
传统数仓 ←→ Hudi 数据湖仓 ←→ 数据湖
```

Hudi 的核心价值在于**在数据湖的开放存储之上，叠加了数据仓库的事务性和管理能力**：
- **开放格式**: 所有数据和元数据都以 Parquet/Avro/HFile 等开放格式存储在云存储上
- **事务语义**: 提供 ACID 事务保证，支持原子性 commit/rollback
- **流批一体**: 同时支持批处理和流式处理，是真正的流批一体引擎

### 1.2 技术栈概览

| 技术领域 | 选型 |
|---------|------|
| 编程语言 | Java 11+ (核心)、Scala 2.12/2.13 (Spark 集成) |
| 构建工具 | Maven 多模块 (3014 行 pom.xml) |
| 序列化框架 | Apache Avro (主要)、Protobuf (元数据)、Kryo (Spark 序列化) |
| 存储格式 | Parquet (列式)、Avro (行式/日志)、HFile (索引) |
| 计算引擎 | Apache Spark 3.3~4.0、Apache Flink 1.17~2.1 |
| 元数据同步 | Hive Metastore、AWS Glue、DataHub、ADB |
| 分布式锁 | ZooKeeper、DynamoDB、文件系统锁 |
| 云平台 | AWS (S3/Glue/DynamoDB)、GCP (GCS/PubSub/BigQuery) |

---

## 二、工程模块架构

### 2.1 整体模块拓扑

Hudi 采用经典的**分层架构 + 多模块 Maven 工程**，根 pom.xml 中定义了约 50 个模块（包含顶级模块、子模块和 packaging 模块）。按职责可分为以下六层：

```
┌─────────────────────────────────────────────────────────────────┐
│                      应用接入层 (Application Layer)              │
│  hudi-spark-datasource │ hudi-flink-datasource │ hudi-kafka-connect │
├─────────────────────────────────────────────────────────────────┤
│                      写入客户端层 (Client Layer)                 │
│  hudi-client-common │ hudi-spark-client │ hudi-flink-client │ hudi-java-client │
├─────────────────────────────────────────────────────────────────┤
│                      表服务层 (Table Service Layer)              │
│  Compaction │ Clustering │ Clean │ TTL │ Index │ Archive │ Rollback │
├─────────────────────────────────────────────────────────────────┤
│                      核心公共层 (Common Layer)                   │
│  hudi-common: Timeline │ FileSystemView │ Table │ Model │ Config │ Schema │
├─────────────────────────────────────────────────────────────────┤
│                      I/O 存储抽象层 (Storage Abstraction Layer)    │
│  hudi-io: HoodieStorage │ StoragePath │ HFile Reader │ Log Format │
├─────────────────────────────────────────────────────────────────┤
│                      外围生态层 (Ecosystem Layer)                 │
│  hudi-sync │ hudi-utilities │ hudi-cli │ hudi-timeline-service │
│  hudi-platform-service │ hudi-aws │ hudi-gcp │ hudi-kafka-connect │
│  hudi-trino-plugin (独立构建) │ hudi-examples │ hudi-tests-common │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 核心模块详解

#### 2.2.1 `hudi-io` — 存储抽象层

**设计意图**: 将 Hudi 与具体的文件系统实现（HDFS、S3、GCS 等）**完全解耦**。

```
hudi-io/
├── storage/
│   ├── HoodieStorage.java          # 统一存储抽象接口
│   ├── StoragePath.java            # 路径抽象（替代 Hadoop Path）
│   ├── StorageConfiguration.java   # 存储配置抽象
│   ├── StoragePathInfo.java        # 文件信息抽象
│   └── StorageSchemes.java         # 支持的存储协议枚举
├── io/
│   ├── hfile/                      # 自研 HFile Reader
│   └── ...
```

**为什么这么设计**:
- **去 Hadoop 化**: Hudi 正在逐步减少对 Hadoop API 的直接依赖，`HoodieStorage` 接口使得底层可以接入任何对象存储
- **轻量化**: 自研 HFile Reader 避免了引入庞大的 HBase 依赖，仅实现 Hudi Metadata Table 所需的读取功能
- **跨引擎通用**: 该层不依赖任何计算引擎，可被 Spark/Flink/Java 独立客户端复用

#### 2.2.2 `hudi-common` — 核心公共层

**设计意图**: 承载所有跨引擎共享的核心抽象，是整个项目的**基石**。

```
hudi-common/src/main/java/org/apache/hudi/
├── common/
│   ├── model/          # 数据模型：HoodieRecord, HoodieKey, FileSlice, HoodieBaseFile 等
│   ├── table/          # 表格式核心：HoodieTableMetaClient, HoodieTableConfig
│   │   ├── timeline/   # ★ Timeline 机制：HoodieTimeline, HoodieInstant, HoodieActiveTimeline
│   │   ├── view/       # ★ 文件系统视图：TableFileSystemView, HoodieTableFileSystemView
│   │   ├── log/        # 日志格式：HoodieLogFormat, HoodieLogFileReader, LogBlock
│   │   ├── read/       # 统一读取器：HoodieFileGroupReader
│   │   └── cdc/        # CDC 变更数据捕获
│   ├── engine/         # 引擎抽象：HoodieEngineContext, HoodieReaderContext
│   ├── config/         # 配置体系
│   ├── lock/           # 锁抽象：LockProvider
│   ├── bloom/          # 布隆过滤器
│   └── schema/         # Schema 管理与演进
├── metadata/           # ★ Metadata Table 子系统
├── index/              # 索引抽象（record index, expression index, secondary index）
├── timeline/           # Timeline 元数据
├── config/             # 通用配置
└── keygen/             # Key 生成器
```

**关键设计模式**:

1. **Timeline 机制** — Hudi 最核心的设计创新
   - 每次写操作对应一个 `HoodieInstant`（包含 timestamp + action + state）
   - `HoodieTimeline` 维护所有 instant 的有序序列，是**事务日志**的抽象
   - 分为 `ActiveTimeline`（活跃 instant）和 `ArchivedTimeline`（归档 instant）

2. **FileSystemView** — 多种实现的文件视图
   - `HoodieTableFileSystemView`: 基于内存的默认实现
   - `RocksDbBasedFileSystemView`: 基于 RocksDB 的大表实现
   - `SpillableMapBasedFileSystemView`: 可溢出到磁盘的实现
   - `RemoteHoodieTableFileSystemView`: 远程 Timeline Server 上的实现
   - `PriorityBasedFileSystemView`: 融合本地和远程的优先级实现

3. **引擎上下文抽象** (`HoodieEngineContext`)
   ```java
   // 让核心逻辑无需感知底层是 Spark RDD / Flink DataStream / Java Stream
   public abstract class HoodieEngineContext {
       abstract <I, O> List<O> map(List<I> data, SerializableFunction<I, O> func, int parallelism);
       abstract <I, O> List<O> flatMap(List<I> data, SerializableFunction<I, Stream<O>> func, int parallelism);
       // ...
   }
   ```

#### 2.2.3 `hudi-client` — 写入客户端层

**设计意图**: 采用**模板方法 + 策略模式**，将写入逻辑的公共流程抽取到 `hudi-client-common`，引擎特定实现下沉到子模块。

```
hudi-client/
├── hudi-client-common/    # ★ 引擎无关的写入逻辑
│   ├── client/
│   │   ├── BaseHoodieWriteClient.java       # 核心写入客户端（约 83KB 大文件）
│   │   ├── BaseHoodieTableServiceClient.java # 表服务客户端基类
│   │   └── transaction/                      # 事务管理 & 冲突解决
│   ├── table/
│   │   ├── HoodieTable.java                 # 表抽象（分发 action 到具体执行器）
│   │   └── action/                          # ★ 表服务动作执行器
│   │       ├── commit/     # 提交
│   │       ├── compact/    # 压缩
│   │       ├── cluster/    # 聚簇
│   │       ├── clean/      # 清理
│   │       ├── rollback/   # 回滚
│   │       ├── restore/    # 恢复
│   │       ├── savepoint/  # 保存点
│   │       ├── index/      # 索引构建
│   │       └── ttl/        # 数据过期
│   └── index/              # 索引实现（Bloom, Bucket, Simple, InMemory）
├── hudi-spark-client/     # Spark 引擎适配
├── hudi-flink-client/     # Flink 引擎适配
└── hudi-java-client/      # 纯 Java 引擎适配
```

**为什么这么分层**:
- `BaseHoodieWriteClient` 定义了完整的写入流程模板（start commit → write → index → commit/rollback）
- 各引擎子模块只需实现数据分区、shuffle、并行执行等引擎特定逻辑
- **避免了在 Spark/Flink/Java 三套代码中重复实现事务管理、索引更新、表服务调度等核心逻辑**

#### 2.2.4 `hudi-spark-datasource` — Spark 数据源集成

**设计意图**: 通过**版本适配层 (shim) 模式**，支持多个 Spark 版本共存。

```
hudi-spark-datasource/
├── hudi-spark-common/     # 所有 Spark 版本共享的逻辑
├── hudi-spark3-common/    # Spark 3.x 共享逻辑
├── hudi-spark3.3.x/       # Spark 3.3 特定适配
├── hudi-spark3.4.x/       # Spark 3.4 特定适配
├── hudi-spark3.5.x/       # Spark 3.5 特定适配（当前默认）
├── hudi-spark4-common/    # Spark 4.x 共享逻辑
├── hudi-spark4.0.x/       # Spark 4.0 特定适配
└── hudi-spark/            # 统一入口模块
```

**这种设计的优势**:
- **渐进式演进**: 新增 Spark 版本支持只需增加一个 `hudi-sparkX.Y.x` 模块
- **最大化代码复用**: 95% 以上的 Spark 集成逻辑在 `common` 模块中共享
- **编译隔离**: 不同 Spark 版本的 API 不兼容部分通过 Maven Profile 隔离

#### 2.2.5 `hudi-flink-datasource` — Flink 数据源集成

与 Spark 数据源采用相同的分层策略：

```
hudi-flink-datasource/
├── hudi-flink/            # 核心 Flink 集成（source, sink, table, streamer）
├── hudi-flink1.17.x/      # Flink 1.17 适配
├── hudi-flink1.18.x/      # Flink 1.18 适配
├── hudi-flink1.19.x/      # Flink 1.19 适配
├── hudi-flink1.20.x/      # Flink 1.20 适配（当前默认）
├── hudi-flink2.0.x/       # Flink 2.0 适配
└── hudi-flink2.1.x/       # Flink 2.1 适配
```

#### 2.2.6 外围生态模块

| 模块 | 职责 | 为什么需要 |
|------|------|-----------|
| `hudi-sync` | 将 Hudi 表元数据同步到外部 Catalog | 让 Hive/Presto/Trino 等查询引擎感知 Hudi 表 |
| `hudi-utilities` | 内置数据摄取工具（DeltaStreamer/HoodieStreamer） | 提供开箱即用的 CDC、Kafka、S3 等数据源接入能力 |
| `hudi-cli` | 命令行管理工具 | 运维场景下查看表状态、触发 compaction/clustering 等 |
| `hudi-timeline-service` | Timeline Server 服务 | 减少大规模表的文件系统 listing 开销 |
| `hudi-platform-service` | 平台服务（包含 hudi-metaserver） | 元数据服务器，提供集中式元数据管理 |
| `hudi-aws` | AWS 平台集成 | S3、Glue Catalog、DynamoDB Lock Provider |
| `hudi-gcp` | GCP 平台集成 | GCS、BigQuery、PubSub |
| `hudi-trino-plugin` | Trino 查询引擎插件（独立构建） | 让 Trino 直接读取 Hudi 表，需通过 profile 激活 |
| `hudi-kafka-connect` | Kafka Connect Sink | 让 Kafka 数据直接写入 Hudi |

#### 2.2.7 `packaging` — Bundle 打包

**设计意图**: 通过 Maven Shade Plugin 将复杂的依赖树打包成**单一 fat jar**。

```
packaging/
├── hudi-spark-bundle/              # Spark 环境的一体化 jar
├── hudi-flink-bundle/              # Flink 环境的一体化 jar
├── hudi-utilities-bundle/          # Utilities 一体化 jar
├── hudi-utilities-slim-bundle/     # Utilities 精简版 jar
├── hudi-hive-sync-bundle/          # Hive 同步专用 jar
├── hudi-datahub-sync-bundle/       # DataHub 同步专用 jar
├── hudi-kafka-connect-bundle/      # Kafka Connect 专用 jar
├── hudi-presto-bundle/             # Presto 查询专用 jar
├── hudi-trino-bundle/              # Trino 查询专用 jar
├── hudi-hadoop-mr-bundle/          # Hadoop MapReduce 专用 jar
├── hudi-cli-bundle/                # CLI 工具专用 jar
├── hudi-aws-bundle/                # AWS 集成专用 jar
├── hudi-gcp-bundle/                # GCP 集成专用 jar
├── hudi-timeline-server-bundle/    # Timeline Server 独立部署 jar
├── hudi-metaserver-server-bundle/  # MetaServer 独立部署 jar
├── hudi-integ-test-bundle/         # 集成测试专用 jar
└── bundle-validation/              # Bundle 验证工具
```

**为什么需要 Bundle**:
- 大数据生态中**依赖冲突**极其严重（Jackson、Avro、Protobuf 版本冲突）
- Bundle 通过 **shade relocation** 将冲突依赖重命名到 `org.apache.hudi.*` 命名空间下
- 用户只需引入一个 jar 就能使用 Hudi，极大降低了集成成本

---

## 三、核心设计理念与特性

### 3.1 表格式设计：COW vs MOR

Hudi 支持两种表类型，这是其最核心的设计决策：

```
┌─────────────────────────────────────────────────────────────┐
│                 Copy-On-Write (COW)                         │
│                                                             │
│  写入时: Parquet₁ + ΔData = Parquet₂ (重写整个文件)           │
│  读取时: 直接读 Parquet (极快)                               │
│                                                             │
│  适用: 读多写少，批处理场景                                    │
├─────────────────────────────────────────────────────────────┤
│                 Merge-On-Read (MOR)                          │
│                                                             │
│  写入时: ΔData → Avro Log File (追加写，极快)                 │
│  读取时: Parquet (Base) + Log Files → 合并读取                │
│  后台: Compaction 定期将 Log 合并回 Parquet                   │
│                                                             │
│  适用: 写多读少，近实时流式场景                                 │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 数据模型层次

```
HoodieTable
  └── Partition (分区)
       └── FileGroup (文件组：由 fileId 标识)
            └── FileSlice (文件切片：一个 Base File + 若干 Log Files)
                 ├── HoodieBaseFile   (.parquet)
                 └── HoodieLogFile[]  (.log.*)
```

每条记录由 `HoodieRecord<T>` 表示，包含：
- `HoodieKey`: recordKey + partitionPath（全局唯一标识）
- `T data`: Payload 数据（泛型，支持 Avro/Spark InternalRow/Flink RowData）
- `HoodieRecordLocation`: 当前和新的存储位置（用于索引映射）

### 3.3 Timeline 机制

Timeline 是 Hudi 的**事务日志系统**——所有的写操作都被建模为一个有序的 instant 序列：

```
Timeline: t1.commit → t2.commit → t3.deltacommit → t4.compaction → t5.clean
              │            │             │               │             │
           COMPLETED    COMPLETED    INFLIGHT        REQUESTED     COMPLETED
```

**每个 Instant 包含**:
- `timestamp`: 精确到毫秒的时间戳（支持时钟偏移校正 `SkewAdjustingTimeGenerator`）
- `action`: 操作类型（commit, deltacommit, compaction, clean, rollback, savepoint...）
- `state`: 状态机（REQUESTED → INFLIGHT → COMPLETED）

**这个设计的价值**:
- 实现了**时间旅行查询** (Time-Travel Query)
- 支持**增量查询** (Incremental Query)：只读取某个时间点之后变更的数据
- 提供**原子性提交**和**崩溃恢复**

### 3.4 索引体系

Hudi 的索引体系是其**高效 upsert 能力**的关键。索引通过不同的实现类提供多种策略：

```
HoodieIndex (抽象基类: hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/HoodieIndex.java)
├── HoodieBloomIndex           # 布隆过滤器索引（分区内唯一）
├── HoodieGlobalBloomIndex     # 全局布隆过滤器索引
├── HoodieSimpleIndex          # 简单 Join 索引（分区内唯一）
├── HoodieGlobalSimpleIndex    # 全局简单索引
├── HoodieBucketIndex          # 桶索引（哈希分桶）
│   ├── Simple Bucket Engine           # 固定桶数
│   └── Consistent Hashing Bucket     # 一致性哈希，支持动态扩缩容
├── HoodieInMemoryHashIndex    # 内存索引
├── FlinkStateIndex            # Flink 状态后端索引（Flink 专用）
├── HoodieInternalProxyIndex   # 内部代理索引（用于 Metadata Table 的 RECORD_INDEX）
└── 其他自定义索引实现
```

**为什么提供这么多索引类型**:
- **BLOOM**: 空间效率高，适合中等规模数据，但有假阳性
- **BUCKET**: O(1) 查找复杂度，适合超大规模数据；一致性哈希解决数据倾斜
- **RECORD_INDEX (Metadata Table)**: 利用 Metadata Table 存储精确映射，最精确但空间开销更大
- 不同场景下的**读写 trade-off** 完全不同，多索引类型让用户按需选择

### 3.5 Metadata Table（元数据表）

Metadata Table 是一个**内置的 Hudi MOR 表**，用于加速元数据查询：

```
MetadataPartitionType (hudi-common/src/main/java/org/apache/hudi/metadata/MetadataPartitionType.java:91)
├── FILES                  # 文件列表索引（替代 fs.list），优先级 2
├── COLUMN_STATS           # 列统计信息（min/max/count/null count），优先级 3
├── BLOOM_FILTERS          # 布隆过滤器，优先级 4
├── RECORD_INDEX           # 记录级索引，优先级 5
├── EXPRESSION_INDEX       # 表达式索引（前缀 expr-index-），优先级 -1（动态）
├── SECONDARY_INDEX        # 二级索引（前缀 secondary-index-），优先级 7
├── PARTITION_STATS        # 分区级统计信息，优先级 6
└── ALL_PARTITIONS         # 全分区虚拟类型，内部协助查询所有分区
```

**为什么需要单独的 Metadata Table**:
- 云存储（S3/GCS）的 listing 操作延迟高且昂贵
- 将文件列表以 HFile 格式存储在 Metadata Table 中，可以将 O(n) list 转化为 O(1) 点查
- 列统计信息支持查询时的文件级裁剪（Data Skipping）

### 3.6 并发控制机制

```
WriteConcurrencyMode (hudi-common/src/main/java/org/apache/hudi/common/model/WriteConcurrencyMode.java)
├── SINGLE_WRITER                 # 单写者模式（默认，无锁）
├── OPTIMISTIC_CONCURRENCY_CONTROL # 乐观并发控制（OCC）
└── NON_BLOCKING_CONCURRENCY_CONTROL # 非阻塞并发控制（NBCC）

LockProvider (hudi-common/src/main/java/org/apache/hudi/common/lock/LockProvider.java 接口)
├── ZookeeperBasedLockProvider
├── DynamoDBBasedLockProvider  
├── FileSystemBasedLockProvider
└── ...
```

**OCC 工作原理**: 写者在提交时检查是否存在冲突（通过 `ConflictResolutionStrategy`），如有冲突则回滚——适合读多写少的**关系型数据模型**。

**NBCC 工作原理**: 支持无序、延迟数据的写入，不阻塞并发写者——适合**流式数据模型**。

### 3.7 表服务自动调度

Hudi 内置了丰富的表服务，可以集成到写入流程中自动执行，也可以独立运行：

| 表服务 | 作用 | 触发条件 |
|--------|------|----------|
| **Compaction** | 将 Log Files 合并为 Parquet（MOR 表专有） | 基于策略（文件大小、时间、提交数量） |
| **Clustering** | 重新组织数据布局，优化查询性能 | 基于策略（Z-Order、Hilbert 空间填充曲线） |
| **Cleaning** | 清理旧版本文件，回收存储空间 | 基于保留策略（保留 N 个 commit 或 N 小时） |
| **TTL** | 数据过期管理 | 基于分区级过期时间 |
| **Archival** | 归档旧的 Timeline Instant | 防止 Timeline 无限增长 |
| **Index** | 异步构建/重建索引 | 手动或自动触发 |

### 3.8 查询类型

基于同一张 Hudi 表，支持五种查询方式：

```
┌────────────────────┬────────────────────────────────────────────────┐
│ Snapshot Query      │ 读取最新已提交数据的完整视图                      │
│ Incremental Query   │ 读取某时间点之后的增量变更记录                     │
│ CDC Query           │ 读取变更流（insert/update/delete + before/after） │
│ Time-Travel Query   │ 读取历史某个时间点的数据快照                      │
│ Read-Optimized Query│ 仅读取 Parquet 文件（跳过未 compact 的 Log）      │
└────────────────────┴────────────────────────────────────────────────┘
```

---

## 四、关键设计模式总结

### 4.1 模板方法模式

`BaseHoodieWriteClient` 定义了写入流程的骨架，各引擎子类只需实现引擎特定的步骤：

```
startCommitWithTime() → write(records) → index.tagLocation() → upsert/insert 
→ index.updateLocation() → commit() / rollback()
```

### 4.2 策略模式

大量组件通过策略模式实现可插拔：
- `HoodieIndex`: 多种索引策略
- `HoodieRecordMerger`: 多种记录合并策略
- `ConflictResolutionStrategy`: 多种冲突解决策略
- `CleaningPolicy`: 多种清理策略
- `CompactionStrategy`: 多种压缩策略

### 4.3 Shim/Adapter 模式

用于兼容不同版本的 Spark/Flink API：
- `hudi-spark3.3.x` / `hudi-spark3.4.x` / `hudi-spark3.5.x` 各自实现版本特定的 adapter
- 通过 Maven Profile 在编译时选择正确的 shim 模块

### 4.4 泛型记录抽象

```java
// HoodieRecord<T> 的 T 可以是:
// - Avro GenericRecord (引擎无关场景)
// - Spark InternalRow  (Spark 原生性能)  
// - Flink RowData      (Flink 原生性能)
public enum HoodieRecordType { AVRO, SPARK, HIVE, FLINK }
```

这种设计让 Hudi 可以在不同引擎中使用**原生数据格式**，避免了跨格式序列化开销。

### 4.5 Bundle 打包 + Shade Relocation

通过 Maven Shade Plugin 将依赖重定位，解决大数据生态中的依赖地狱：
```xml
<relocation>
  <pattern>org.apache.http.</pattern>
  <shadedPattern>org.apache.hudi.org.apache.http.</shadedPattern>
</relocation>
```

---

## 五、模块依赖关系图

```mermaid
graph TB
    subgraph "应用接入层"
        SPARK_DS[hudi-spark-datasource]
        FLINK_DS[hudi-flink-datasource]
        KAFKA_CONN[hudi-kafka-connect]
        UTILITIES[hudi-utilities]
    end

    subgraph "写入客户端层"
        CLIENT_COMMON[hudi-client-common]
        SPARK_CLIENT[hudi-spark-client]
        FLINK_CLIENT[hudi-flink-client]
        JAVA_CLIENT[hudi-java-client]
    end

    subgraph "核心公共层"
        COMMON[hudi-common]
        HADOOP_COMMON[hudi-hadoop-common]
    end

    subgraph "存储抽象层"
        IO[hudi-io]
    end

    subgraph "外围生态"
        SYNC[hudi-sync]
        CLI[hudi-cli]
        TIMELINE_SVC[hudi-timeline-service]
        PLATFORM_SVC[hudi-platform-service]
        AWS[hudi-aws]
        GCP[hudi-gcp]
    end

    SPARK_DS --> SPARK_CLIENT
    FLINK_DS --> FLINK_CLIENT
    KAFKA_CONN --> JAVA_CLIENT
    UTILITIES --> SPARK_CLIENT

    SPARK_CLIENT --> CLIENT_COMMON
    FLINK_CLIENT --> CLIENT_COMMON
    JAVA_CLIENT --> CLIENT_COMMON

    CLIENT_COMMON --> COMMON
    COMMON --> IO
    HADOOP_COMMON --> IO

    SYNC --> COMMON
    CLI --> CLIENT_COMMON
    TIMELINE_SVC --> COMMON
    PLATFORM_SVC --> COMMON
    AWS --> COMMON
    GCP --> COMMON
```

---

## 六、构建系统设计

### 6.1 Maven Profile 驱动的多版本支持

Hudi 通过 Maven Profile 实现同一代码库支持多个引擎版本：

```bash
# Spark 3.5 + Flink 1.20 (默认)
mvn clean package -DskipTests

# Spark 3.3 + Flink 1.17
mvn clean package -DskipTests -Dspark3.3 -Dflink1.17

# Spark 4.0 + Scala 2.13 (需要 Java 17)
mvn clean package -DskipTests -Dspark4.0

# Scala 2.13 构建
mvn clean package -DskipTests -Dspark3.5 -Dscala-2.13
```

### 6.2 代码质量保证

| 工具 | 用途 |
|------|------|
| Checkstyle | Java 代码风格检查 |
| ScalaStyle | Scala 代码风格检查 |
| Apache RAT | License 头文件检查 |
| JaCoCo | 代码覆盖率 |
| Maven Enforcer | 禁止冲突依赖（特别是 logging 框架冲突） |

---

## 七、总结：Hudi 架构的核心优势

| 特性 | 实现方式 | 价值 |
|------|----------|------|
| **流批一体** | COW + MOR 双表类型 | 一套架构覆盖批处理和流式场景 |
| **ACID 事务** | Timeline + 原子 commit | 数据湖上的可靠性保证 |
| **高效 Upsert** | 多种索引 + 记录级合并 | 比全量重写快 10~100x |
| **增量处理** | Incremental Query + CDC | 避免全量扫描，降低计算成本 |
| **引擎无关** | 分层抽象 + 泛型记录 | Spark/Flink/Java 任选 |
| **多版本兼容** | Shim 模式 + Maven Profile | 单一代码库支持 10+ 引擎版本 |
| **自动管理** | 内置表服务自动调度 | Compaction/Clean/Cluster 免运维 |
| **查询加速** | Metadata Table + Data Skipping | 减少 I/O，加速查询 |
| **开放格式** | Parquet + Avro + HFile | 无供应商锁定 |
| **云原生** | 存储抽象 + 云平台集成模块 | 适配 AWS/GCP/Azure |

Hudi 的工程架构通过**分层解耦**、**策略可插拔**和**多引擎适配**三大设计原则，在保证核心写入/读取逻辑一致性的同时，实现了对多种计算引擎、存储系统和云平台的灵活支持。其 Timeline 机制和 Metadata Table 子系统是区别于其他数据湖表格式（如 Delta Lake、Iceberg）的核心设计创新。

---

## 八、核心抽象层深度解析

### 8.1 HoodieEngineContext 抽象：计算引擎无关的并行执行框架

> **源码位置**: `hudi-common/src/main/java/org/apache/hudi/common/engine/HoodieEngineContext.java`

#### 8.1.1 设计动机

Hudi 的核心写入逻辑（索引查找、记录合并、文件写入、表服务调度）需要进行大规模的并行计算。然而，不同计算引擎对并行计算的 API 完全不同：Spark 使用 RDD/DataFrame，Flink 使用 DataStream，纯 Java 场景只有 Stream API。如果核心逻辑直接依赖某一种引擎 API，就无法实现跨引擎复用。`HoodieEngineContext` 正是为解决这一问题而设计的**计算引擎抽象层**。

#### 8.1.2 核心能力

`HoodieEngineContext` 是一个抽象类，定义了以下核心能力：

```java
// 文件: hudi-common/src/main/java/org/apache/hudi/common/engine/HoodieEngineContext.java

public abstract class HoodieEngineContext {
    // 存储配置（引擎无关）
    private final StorageConfiguration<?> storageConf;
    // 任务上下文供应器（获取 partitionId, stageId, attemptId 等）
    protected TaskContextSupplier taskContextSupplier;

    // === 并行数据容器 ===
    public abstract <T> HoodieData<T> emptyHoodieData();           // 创建空的并行数据容器
    public abstract <T> HoodieData<T> parallelize(List<T> data, int parallelism); // 本地集合 → 并行容器

    // === 并行计算原语 ===
    public abstract <I, O> List<O> map(List<I> data, SerializableFunction<I, O> func, int parallelism);
    public abstract <I, O> List<O> flatMap(List<I> data, SerializableFunction<I, Stream<O>> func, int parallelism);
    public abstract <I> void foreach(List<I> data, SerializableConsumer<I> consumer, int parallelism);

    // === 聚合与归约 ===
    public abstract <I, K, V> List<V> mapToPairAndReduceByKey(...);
    public abstract <I, K, V> List<V> reduceByKey(...);
    public abstract <I, O> O aggregate(HoodieData<I> data, O zeroValue, ...);

    // === 引擎管理 ===
    public abstract HoodieAccumulator newAccumulator();             // 分布式累加器
    public abstract void setJobStatus(String activeModule, String activityDescription);
    public abstract void cancelJob(String jobId);

    // === 分组处理 ===
    public <K, V, R> HoodieData<R> mapGroupsByKey(HoodiePairData<K, V> data, ...);
}
```

#### 8.1.3 三种引擎实现的差异

| 实现类 | 所在模块 | map() 的底层实现 | parallelize() 的底层实现 | 累加器类型 |
|--------|----------|-----------------|------------------------|-----------|
| `HoodieSparkEngineContext` | `hudi-spark-client` | `JavaSparkContext.parallelize(data).map(func).collect()` | `JavaRDD` | Spark `LongAccumulator` |
| `HoodieFlinkEngineContext` | `hudi-flink-client` | `ForkJoinPool` + `parallelStream().map()` | `HoodieListData`（内存） | `AtomicLong` |
| `HoodieJavaEngineContext` | `hudi-java-client` | `data.stream().parallel().map()` | `HoodieListData`（内存） | `AtomicLong` |
| `HoodieLocalEngineContext` | `hudi-common` | `data.stream().parallel().map()` | `HoodieListData`（内存） | `AtomicLong` |

**Spark 实现的特殊之处**：`HoodieSparkEngineContext` 内部持有 `JavaSparkContext` 引用，所有并行操作都真正通过 Spark 集群分布式执行。它还额外实现了分布式 Metrics Registry（`DistributedRegistry`），可以在 Spark Executor 上收集并聚合指标数据。此外，Spark 实现重写了 `mapGroupsByKey` 方法，使用了自定义的 `ConditionalRangePartitioner` 进行基于采样的 Range 分区，避免数据倾斜。

```java
// 文件: hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/common/HoodieSparkEngineContext.java
@Override
public <I, O> List<O> map(List<I> data, SerializableFunction<I, O> func, int parallelism) {
    final Map<String, Registry> registries = DISTRIBUTED_REGISTRY_MAP;
    return javaSparkContext.parallelize(data, parallelism).map(i -> {
        setRegistries(registries);  // 在 Executor 上注册分布式指标
        return func.apply(i);
    }).collect();
}
```

**Flink 实现的特殊之处**：`HoodieFlinkEngineContext` 的 `map`/`flatMap`/`reduceByKey` 等操作使用了专用的 `ForkJoinPool` 来控制并行度，而非直接使用公共 ForkJoinPool，避免与其他任务竞争线程资源。

```java
// 文件: hudi-client/hudi-flink-client/src/main/java/org/apache/hudi/client/common/HoodieFlinkEngineContext.java
private static <E, O> O executeParallelStream(Stream<E> paralelStream, Function<Stream<E>, O> transform, int parallelism) {
    ForkJoinPool pool = new ForkJoinPool(parallelism);
    try {
        return pool.submit(() -> transform.apply(paralelStream)).get();
    } finally {
        pool.shutdown();
    }
}
```

#### 8.1.4 为什么这么设计，好处是什么

1. **核心逻辑零重复**：`hudi-client-common` 中的索引查找、Compaction 计划生成、Clustering 策略等核心逻辑只需调用 `engineContext.map()`、`engineContext.reduceByKey()` 等抽象方法，完全无需感知底层是 Spark RDD 还是 Java Stream。
2. **新引擎接入成本极低**：如果未来需要支持新的计算引擎（如 Trino 写入），只需新增一个 `HoodieEngineContext` 子类并实现 10 余个抽象方法即可。
3. **测试便利**：`HoodieJavaEngineContext` 基于纯 Java Stream API，无需启动任何分布式集群即可进行单元测试，极大降低了测试复杂度和 CI 成本。

---

### 8.2 HoodieData 抽象层：引擎无关的数据容器

> **源码位置**:
> - `hudi-common/src/main/java/org/apache/hudi/common/data/HoodieData.java`
> - `hudi-common/src/main/java/org/apache/hudi/common/data/HoodiePairData.java`

#### 8.2.1 设计动机

`HoodieEngineContext` 解决了"如何发起并行计算"的问题，但核心写入流程中还有大量数据需要在多个步骤之间传递。例如：写入流程中，`index.tagLocation()` 返回的已标记记录集合需要传递给下游的 `upsert/insert` 操作。如果这个中间数据集合直接用 `JavaRDD<HoodieRecord>` 表示，那核心逻辑就被绑定到 Spark 了。`HoodieData<T>` 和 `HoodiePairData<K,V>` 就是为了解决这个问题而设计的**引擎无关数据容器**。

#### 8.2.2 HoodieData 接口设计

`HoodieData<T>` 定义了类似 RDD 的惰性计算链式 API：

```java
// 文件: hudi-common/src/main/java/org/apache/hudi/common/data/HoodieData.java
public interface HoodieData<T> extends Serializable {
    // 转换操作（惰性，非终端）
    <O> HoodieData<O> map(SerializableFunction<T, O> func);
    <O> HoodieData<O> flatMap(SerializableFunction<T, Iterator<O>> func);
    <O> HoodieData<O> mapPartitions(SerializableFunction<Iterator<T>, Iterator<O>> func, boolean preservesPartitioning);
    HoodieData<T> filter(SerializableFunction<T, Boolean> filterFunc);
    HoodieData<T> distinct();
    HoodieData<T> union(HoodieData<T> other);

    // 转换为键值对容器
    <K, V> HoodiePairData<K, V> mapToPair(SerializablePairFunction<T, K, V> func);
    <K, V> HoodiePairData<K, V> flatMapToPair(SerializableFunction<T, Iterator<? extends Pair<K, V>>> func);

    // 缓存管理
    void persist(String level);
    void unpersist();

    // 终端操作
    List<T> collectAsList();
    long count();
    boolean isEmpty();

    // 分区管理
    HoodieData<T> repartition(int parallelism);
    HoodieData<T> coalesce(int parallelism);
}
```

`HoodiePairData<K,V>` 则专门处理键值对数据，提供了 `groupByKey()`、`reduceByKey()`、`leftOuterJoin()`、`join()` 等 Shuffle 类操作。

#### 8.2.3 实现矩阵

| 接口 | Spark 实现 | 内存（Flink/Java）实现 |
|------|-----------|---------------------|
| `HoodieData<T>` | `HoodieJavaRDD<T>` | `HoodieListData<T>` |
| `HoodiePairData<K,V>` | `HoodieJavaPairRDD<K,V>` | `HoodieListPairData<K,V>` |

**HoodieJavaRDD** 实现（Spark）:

```java
// 文件: hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/data/HoodieJavaRDD.java
public class HoodieJavaRDD<T> implements HoodieData<T> {
    private final JavaRDD<T> rddData;  // 底层是 Spark RDD

    @Override
    public <O> HoodieData<O> map(SerializableFunction<T, O> func) {
        return HoodieJavaRDD.of(rddData.map(func::apply));  // 委托给 RDD.map()
    }

    @Override
    public void persist(String level) {
        rddData.persist(StorageLevel.fromString(level));  // 委托给 RDD.persist()
    }

    @Override
    public List<T> collectAsList() {
        return rddData.collect();  // 委托给 RDD.collect()
    }
}
```

**HoodieListData** 实现（内存）:

```java
// 文件: hudi-common/src/main/java/org/apache/hudi/common/data/HoodieListData.java
public class HoodieListData<T> extends HoodieBaseListData<T> implements HoodieData<T> {
    // 支持两种执行语义：eager（立即执行）和 lazy（延迟执行）
    public static <T> HoodieListData<T> eager(List<T> listData) { ... }
    public static <T> HoodieListData<T> lazy(List<T> listData)  { ... }

    @Override
    public <O> HoodieData<O> map(SerializableFunction<T, O> func) {
        return new HoodieListData<>(asStream().map(throwingMapWrapper(func)), lazy);
    }

    @Override
    public void persist(String level) { /* No OP - 内存实现无需缓存 */ }

    @Override
    public HoodieData<T> repartition(int parallelism) { return this; /* No OP */ }
}
```

#### 8.2.4 为什么这么设计，好处是什么

1. **写入流程完全引擎无关**：`BaseHoodieWriteClient` 中的写入逻辑全部使用 `HoodieData<HoodieRecord>` 作为中间数据载体。同一套 Compaction/Clustering/Clean 逻辑，无论跑在 Spark 还是 Flink 上，代码路径完全一致。
2. **惰性计算语义对齐**：`HoodieListData` 的 lazy 模式模拟了 RDD 的惰性计算特性，确保非终端操作不会提前触发计算。这使得在内存模式下测试核心逻辑时，行为与分布式执行一致。
3. **缓存管理透明化**：`HoodieJavaRDD` 的 `persist()/unpersist()` 直接映射到 Spark 的 RDD 缓存；而 `HoodieListData` 的 `persist()` 是 No-OP。核心逻辑可以放心调用缓存 API，无需判断当前运行环境。
4. **数据生命周期追踪**：`HoodieDataCacheKey`（由 `basePath + instantTime` 组成）机制允许在多写者场景下精确追踪和清理各写入操作的缓存数据，避免内存泄漏。

---

### 8.3 Storage 抽象层：去 Hadoop 化的文件系统接口

> **源码位置**:
> - `hudi-io/src/main/java/org/apache/hudi/storage/HoodieStorage.java`
> - `hudi-io/src/main/java/org/apache/hudi/storage/StoragePath.java`
> - `hudi-io/src/main/java/org/apache/hudi/storage/StorageConfiguration.java`
> - `hudi-io/src/main/java/org/apache/hudi/storage/StorageSchemes.java`

#### 8.3.1 设计动机

在 Hadoop 生态中，文件操作通常直接依赖 `org.apache.hadoop.fs.FileSystem` 和 `org.apache.hadoop.fs.Path`。然而，这带来了几个严重问题：（1）Hadoop 依赖非常重量级，拉入大量传递依赖；（2）一些轻量级部署场景（如嵌入式 Java 客户端、Trino 插件）不希望引入 Hadoop；（3）随着云原生趋势，越来越多的存储系统不基于 Hadoop FileSystem API。Hudi 的 Storage 抽象层正是为了**渐进式去 Hadoop 化**而设计的。

#### 8.3.2 核心组件

**StoragePath** — 路径抽象（替代 `org.apache.hadoop.fs.Path`）:

```java
// 文件: hudi-io/src/main/java/org/apache/hudi/storage/StoragePath.java
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public class StoragePath implements Comparable<StoragePath>, Serializable {
    private URI uri;
    // 兼容 Hadoop Path 的路径解析逻辑（scheme://authority/path）
    // 但不依赖任何 Hadoop 类
}
```

**StorageConfiguration** — 配置抽象（替代 `org.apache.hadoop.conf.Configuration`）:

```java
// 文件: hudi-io/src/main/java/org/apache/hudi/storage/StorageConfiguration.java
public abstract class StorageConfiguration<T> implements Serializable {
    public abstract StorageConfiguration<T> newInstance();  // 创建配置副本
    public abstract T unwrap();                              // 获取原生配置对象
    public abstract void set(String key, String value);      // 设置键值对
    public abstract Option<String> getString(String key);    // 获取值
    public abstract StorageConfiguration<T> getInline();     // 获取 Inline 存储配置

    // 类型安全的转换方法
    public final <U> U unwrapAs(Class<U> clazz) { ... }     // 转为具体配置类型
}
```

**HoodieStorage** — 文件系统操作抽象（替代 `FileSystem`）:

```java
// 文件: hudi-io/src/main/java/org/apache/hudi/storage/HoodieStorage.java
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public abstract class HoodieStorage implements Closeable {
    // 文件创建与写入
    public abstract OutputStream create(StoragePath path, boolean overwrite) throws IOException;
    public abstract OutputStream append(StoragePath path) throws IOException;

    // 文件读取
    public abstract InputStream open(StoragePath path) throws IOException;
    public abstract SeekableDataInputStream openSeekable(StoragePath path, int bufferSize, boolean wrapStream);

    // 文件与目录管理
    public abstract boolean exists(StoragePath path) throws IOException;
    public abstract boolean rename(StoragePath oldPath, StoragePath newPath) throws IOException;
    public abstract boolean deleteFile(StoragePath path) throws IOException;
    public abstract boolean deleteDirectory(StoragePath path) throws IOException;
    public abstract boolean createDirectory(StoragePath path) throws IOException;

    // 文件列表与信息
    public abstract List<StoragePathInfo> listDirectEntries(StoragePath path) throws IOException;
    public abstract List<StoragePathInfo> listFiles(StoragePath path) throws IOException;
    public abstract StoragePathInfo getPathInfo(StoragePath path) throws IOException;
    public abstract List<StoragePathInfo> globEntries(StoragePath pathPattern, StoragePathFilter filter);

    // 原子文件创建（带临时文件 + rename 策略）
    public final void createImmutableFileInPath(StoragePath path, Option<HoodieInstantWriter> contentWriter) { ... }
}
```

**StorageSchemes** — 支持的存储协议枚举:

```java
// 文件: hudi-io/src/main/java/org/apache/hudi/storage/StorageSchemes.java
public enum StorageSchemes {
    FILE("file", false, true, null),
    HDFS("hdfs", false, true, null),
    S3A("s3a", true, null, "org.apache.hudi.aws.transaction.lock.S3StorageLockClient"),
    S3("s3", true, null, "org.apache.hudi.aws.transaction.lock.S3StorageLockClient"),
    GCS("gs", true, null, "org.apache.hudi.gcp.transaction.lock.GCSStorageLockClient"),
    ABFS("abfs", null, null, null),     // Azure ADLS Gen2
    ABFSS("abfss", null, null, null),
    OSS("oss", null, null, null),       // 阿里云 OSS
    COSN("cosn", null, null, null),     // 腾讯云 COS
    OBS("obs", null, null, null),       // 华为云 OBS
    // ... 共 32 种存储协议
}
```

每种 Scheme 记录了三个关键属性：`isWriteTransactional`（写入是否事务性）、`supportAtomicCreation`（是否支持原子创建）、`storageLockClass`（存储级锁实现类）。这些属性直接影响 Hudi 的并发控制策略：例如，S3 的写入是事务性的（put 是原子的），但不支持原子文件创建（没有类似 HDFS 的 `createNewFile` 语义），因此在 S3 上 Hudi 会使用不同的 Timeline Instant 创建策略。

#### 8.3.3 Hadoop 桥接实现

目前唯一的生产级实现是 `HoodieHadoopStorage`，位于 `hudi-hadoop-common` 模块：

```java
// 文件: hudi-hadoop-common/src/main/java/org/apache/hudi/storage/hadoop/HoodieHadoopStorage.java
public class HoodieHadoopStorage extends HoodieStorage {
    private final FileSystem fs;  // Hadoop FileSystem 实例

    public HoodieHadoopStorage(StoragePath path, StorageConfiguration<?> conf) {
        super(conf);
        this.fs = HadoopFSUtils.getFs(path, conf.unwrapAs(Configuration.class));
    }
    // 所有操作都委托给 Hadoop FileSystem
}
```

#### 8.3.4 为什么这么设计，好处是什么

1. **渐进式去 Hadoop 化**：`hudi-io` 模块是整个项目的最底层，它不依赖 Hadoop。Hadoop 依赖被隔离在 `hudi-hadoop-common` 中。未来如果出现基于云原生 SDK 的 `HoodieStorage` 实现（如直接使用 AWS S3 SDK），可以完全绕过 Hadoop。
2. **存储语义精确建模**：`StorageSchemes` 枚举精确记录了每种存储协议的事务特性。`createImmutableFileInPath` 方法根据 `needCreateTempFile()`（只有 HDFS/ViewFS/本地文件系统需要临时文件）来决定是否使用"先写临时文件再 rename"的原子写入策略，体现了对不同存储语义的精细适配。
3. **跨模块无缝使用**：`hudi-common`、`hudi-client-common` 等上层模块只依赖 `HoodieStorage` 接口，完全不感知底层是 HDFS、S3 还是本地文件系统。切换存储系统只需更换 `StorageConfiguration` 的配置，无需修改任何业务代码。
4. **轻量部署**：Trino 插件（`hudi-trino-plugin`）和 Kafka Connect（`hudi-kafka-connect`）等轻量级组件可以只引入必要的 Storage 实现，不必拖入完整的 Hadoop 运行时。

---

### 8.4 Bundle 打包策略：解决大数据生态中的依赖地狱

> **源码位置**: `packaging/` 目录下的 17 个 bundle 子模块

#### 8.4.1 设计动机

大数据生态系统的依赖冲突问题臭名昭著。一个典型的生产集群中，Spark 自带 Jackson 2.12、Avro 1.11、Protobuf 3.x，而 Hudi 可能依赖不同版本的这些库。如果直接将 Hudi 的 jar 放到 Spark classpath 中，极易出现 `NoSuchMethodError`、`ClassNotFoundException` 等运行时错误。Bundle 打包策略通过 **Maven Shade Plugin 的 relocation 机制**，将冲突依赖重命名到 Hudi 的命名空间下，从根本上消除了冲突。

#### 8.4.2 Bundle 类型全景

Hudi 在 `packaging/` 下维护了 16 个 bundle 模块（不含 README.md 和 bundle-validation 验证工具目录），按用途可分为以下几类：

| 类别 | Bundle 名称 | 用途 |
|------|------------|------|
| **计算引擎** | `hudi-spark-bundle` | Spark 环境一体化 jar（含 Hive Sync） |
| | `hudi-flink-bundle` | Flink 环境一体化 jar |
| **工具** | `hudi-utilities-bundle` | HoodieStreamer 等数据摄取工具 |
| | `hudi-utilities-slim-bundle` | 精简版（不含 Spark runtime） |
| | `hudi-cli-bundle` | CLI 命令行工具 |
| **同步** | `hudi-hive-sync-bundle` | 独立 Hive Metastore 同步 |
| | `hudi-datahub-sync-bundle` | DataHub 同步 |
| **查询引擎** | `hudi-presto-bundle` | Presto 查询 |
| | `hudi-trino-bundle` | Trino 查询 |
| | `hudi-hadoop-mr-bundle` | MapReduce/Hive 查询 |
| **连接器** | `hudi-kafka-connect-bundle` | Kafka Connect Sink |
| **云平台** | `hudi-aws-bundle` | AWS 集成 |
| | `hudi-gcp-bundle` | GCP 集成 |
| **服务** | `hudi-timeline-server-bundle` | Timeline Server 独立部署 |
| | `hudi-metaserver-server-bundle` | MetaServer 独立部署 |
| **测试** | `hudi-integ-test-bundle` | 集成测试 |

#### 8.4.3 Shade Relocation 策略详解

以 `hudi-spark-bundle` 为例，其 `pom.xml` 中定义了以下关键 relocation 规则：

```xml
<!-- 文件: packaging/hudi-spark-bundle/pom.xml -->
<relocations>
  <!-- Avro Spark 扩展：避免与 spark-avro 包冲突 -->
  <relocation>
    <pattern>org.apache.spark.sql.avro.</pattern>
    <shadedPattern>org.apache.hudi.org.apache.spark.sql.avro.</shadedPattern>
  </relocation>
  <!-- Servlet API：避免与 Spark 自带的 Jetty 冲突 -->
  <relocation>
    <pattern>javax.servlet.</pattern>
    <shadedPattern>org.apache.hudi.javax.servlet.</shadedPattern>
  </relocation>
  <!-- Jetty：Hudi Timeline Service 使用的版本与 Spark 自带版本不同 -->
  <relocation>
    <pattern>org.eclipse.jetty.</pattern>
    <shadedPattern>org.apache.hudi.org.apache.jetty.</shadedPattern>
  </relocation>
  <!-- Hive 相关：避免与 Spark 内置 Hive 冲突 -->
  <relocation>
    <pattern>org.apache.hive.jdbc.</pattern>
    <shadedPattern>${spark.bundle.hive.shade.prefix}org.apache.hive.jdbc.</shadedPattern>
  </relocation>
  <!-- Metrics 库 -->
  <relocation>
    <pattern>com.codahale.metrics.</pattern>
    <shadedPattern>org.apache.hudi.com.codahale.metrics.</shadedPattern>
  </relocation>
  <!-- Commons IO/Codec -->
  <relocation>
    <pattern>org.apache.commons.io.</pattern>
    <shadedPattern>org.apache.hudi.org.apache.commons.io.</shadedPattern>
  </relocation>
</relocations>
```

`hudi-flink-bundle` 的 relocation 策略与 Spark Bundle 有显著差异，因为 Flink 运行时的依赖树不同：

```xml
<!-- 文件: packaging/hudi-flink-bundle/pom.xml -->
<relocations>
  <!-- Flink 不自带 Avro，需要 shade 进 bundle -->
  <relocation>
    <pattern>org.apache.avro.</pattern>
    <shadedPattern>org.apache.hudi.org.apache.avro.</shadedPattern>
  </relocation>
  <!-- Flink 不自带 Jackson，需要 shade 进 bundle -->
  <relocation>
    <pattern>com.fasterxml.jackson.</pattern>
    <shadedPattern>org.apache.hudi.com.fasterxml.jackson.</shadedPattern>
  </relocation>
  <!-- Kryo：Flink 自带 Kryo，但版本可能不兼容 -->
  <relocation>
    <pattern>com.esotericsoftware.kryo.</pattern>
    <shadedPattern>org.apache.hudi.com.esotericsoftware.kryo.</shadedPattern>
  </relocation>
</relocations>
```

#### 8.4.4 Spark Bundle vs Flink Bundle 的关键差异

| 方面 | Spark Bundle | Flink Bundle |
|------|-------------|--------------|
| **Avro** | Spark runtime 自带 Avro，不需要 shade | 必须 shade Avro（Flink 不自带） |
| **Jackson** | Spark runtime 自带 Jackson，不需要 shade | 必须 shade Jackson |
| **Kryo** | 不需要 shade（使用 Spark 自带的） | 必须 shade（Flink 的 Kryo 版本不兼容） |
| **Parquet** | 只需 `parquet-avro` | 需要完整的 Parquet 库（column/common/encoding/format-structures） |
| **Hive** | 通过 Profile 控制是否 shade | 默认包含独立的 Hive Metastore 客户端 |

#### 8.4.5 常见依赖冲突问题

1. **Jackson 版本冲突**：Spark 3.x 自带 Jackson 2.12/2.13，Flink 可能使用不同版本。Flink Bundle 通过 shade Jackson 解决。
2. **Avro 版本冲突**：Hudi 使用 Avro 1.11+，但部分 Hadoop 环境可能只有 Avro 1.8。
3. **Hive Metastore 冲突**：Spark 内置 Hive 2.3 客户端，而用户环境可能运行 Hive 3.x。Spark Bundle 通过 `spark-bundle-shade-hive` Profile 将 Hive 依赖 shade 到 `org.apache.hudi.` 前缀下。
4. **Jetty 版本冲突**：Hudi Timeline Service 使用 Eclipse Jetty 9/11，而 Spark 内置 Mortbay Jetty。两者 API 完全不兼容，必须 shade。
5. **Protobuf 版本冲突**：Spark 3.x 自带 Protobuf 2.5/3.x，Hudi 的序列化可能需要不同版本。

#### 8.4.6 为什么这么设计，好处是什么

1. **一个 jar 解决所有问题**：用户只需将一个 bundle jar 放到 `--jars` 参数或 Flink `lib/` 目录，无需手动管理数十个依赖。
2. **运行时隔离**：Shade relocation 在字节码层面将冲突类重命名，实现了**同一 JVM 中共存多个版本的同一库**——这是大数据生态中唯一可靠的依赖冲突解决方案。
3. **按需裁剪**：`slim-bundle` 提供了不含 Spark/Flink runtime 的精简版本，适合已有集群环境的场景。
4. **Profile 驱动的灵活性**：通过 Maven Profile（如 `spark-bundle-shade-hive`），同一个 bundle 模块可以生成不同配置的 jar，适配不同的部署环境。

---

### 8.5 多 Spark 版本适配机制：SparkAdapter 与 Shim 模式

> **源码位置**:
> - `hudi-client/hudi-spark-client/src/main/scala/org/apache/spark/sql/hudi/SparkAdapter.scala`
> - `hudi-client/hudi-spark-client/src/main/scala/org/apache/hudi/SparkAdapterSupport.scala`
> - `hudi-spark-datasource/hudi-spark3-common/src/main/scala/org/apache/spark/sql/adapter/BaseSpark3Adapter.scala`
> - `hudi-spark-datasource/hudi-spark4-common/src/main/scala/org/apache/spark/sql/adapter/BaseSpark4Adapter.scala`
> - `hudi-spark-datasource/hudi-spark3.3.x/src/main/scala/org/apache/spark/sql/adapter/Spark3_3Adapter.scala`
> - `hudi-spark-datasource/hudi-spark4.0.x/src/main/scala/org/apache/spark/sql/adapter/Spark4_0Adapter.scala`

#### 8.5.1 设计动机

Spark 从 3.3 到 4.0 的版本演进中，Catalyst 内部 API 频繁变动。例如：`DeleteFromTable` 节点的 `condition` 字段在 Spark 3.3 中从 `Option[Expression]` 变为 `Expression`（SPARK-38626）；`FileScanRDD` 的构造函数在 Spark 3.3 中增加了参数（SPARK-37273）；`UTF8String` 在 Spark 4.0 中不再支持 `compareTo`（SPARK-46832）；Spark 4.0 引入了全新的 `VariantType` 数据类型。这些 API 差异使得同一份代码无法同时编译通过多个 Spark 版本。

#### 8.5.2 层次化适配架构

Hudi 采用了三层适配架构：

```
SparkAdapter (trait)                           ← 定义在 hudi-spark-client，引擎无关接口
    ├── BaseSpark3Adapter (abstract)           ← 定义在 hudi-spark3-common，Spark 3.x 公共逻辑
    │   ├── Spark3_3Adapter                    ← 定义在 hudi-spark3.3.x
    │   ├── Spark3_4Adapter                    ← 定义在 hudi-spark3.4.x
    │   └── Spark3_5Adapter                    ← 定义在 hudi-spark3.5.x
    └── BaseSpark4Adapter (abstract)           ← 定义在 hudi-spark4-common，Spark 4.x 公共逻辑
        └── Spark4_0Adapter                    ← 定义在 hudi-spark4.0.x
```

#### 8.5.3 SparkAdapter Trait：55 个抽象方法

`SparkAdapter` 定义了 Hudi 需要适配的所有 Spark 版本差异点：

```scala
// 文件: hudi-client/hudi-spark-client/src/main/scala/org/apache/spark/sql/hudi/SparkAdapter.scala
trait SparkAdapter extends Serializable {
  // Catalyst 相关
  def getCatalystExpressionUtils: HoodieCatalystExpressionUtils
  def getCatalystPlanUtils: HoodieCatalystPlansUtils
  def getSchemaUtils: HoodieSchemaUtils
  def createInterpretedPredicate(e: Expression): InterpretedPredicate
  def resolveHoodieTable(plan: LogicalPlan): Option[CatalogTable]

  // Avro 序列化/反序列化
  def createAvroSerializer(rootCatalystType: DataType, rootType: HoodieSchema, nullable: Boolean): HoodieAvroSerializer
  def createAvroDeserializer(rootType: HoodieSchema, rootCatalystType: DataType): HoodieAvroDeserializer

  // SQL 解析器
  def createExtendedSparkParser(spark: SparkSession, delegate: ParserInterface): HoodieExtendedParserInterface

  // 文件读取
  def createParquetFileReader(vectorized: Boolean, sqlConf: SQLConf, options: Map[String, String], hadoopConf: Configuration): SparkColumnarFileReader
  def createOrcFileReader(...): SparkColumnarFileReader
  def createLanceFileReader(...): Option[SparkColumnarFileReader]
  def createHoodieFileScanRDD(...): FileScanRDD

  // Spark 4.0 新特性
  def getVariantDataType: Option[DataType]      // Spark 3.x 返回 None, Spark 4.x 返回 Some(VariantType)
  def isVariantType(dataType: DataType): Boolean // Spark 3.x 返回 false
  def createVariantValueWriter(...): BiConsumer[SpecializedGetters, Integer]

  // Spark 内部行
  def createInternalRow(metaFields: Array[UTF8String], sourceRow: InternalRow, sourceContainsMetaFields: Boolean): HoodieInternalRow
  def getUTF8StringFactory: HoodieUTF8StringFactory
  def getUnsafeUtils: HoodieUnsafeUtils

  // ... 还有 30+ 个其他方法
}
```

#### 8.5.4 运行时动态加载机制

`SparkAdapterSupport` 通过检测当前运行时的 Spark 版本，动态加载对应的 Adapter 实现：

```scala
// 文件: hudi-client/hudi-spark-client/src/main/scala/org/apache/hudi/SparkAdapterSupport.scala
object SparkAdapterSupport {
  lazy val sparkAdapter: SparkAdapter = {
    val adapterClass = if (HoodieSparkUtils.isSpark4_0) {
      "org.apache.spark.sql.adapter.Spark4_0Adapter"
    } else if (HoodieSparkUtils.isSpark3_5) {
      "org.apache.spark.sql.adapter.Spark3_5Adapter"
    } else if (HoodieSparkUtils.isSpark3_4) {
      "org.apache.spark.sql.adapter.Spark3_4Adapter"
    } else {
      "org.apache.spark.sql.adapter.Spark3_3Adapter"
    }
    getClass.getClassLoader.loadClass(adapterClass)
      .newInstance().asInstanceOf[SparkAdapter]
  }
}
```

这里使用了**反射 + 类名字符串**的方式加载，而非直接 `new`，是因为在编译 `hudi-spark-client` 模块时，版本特定的 Adapter 类（如 `Spark3_3Adapter`）尚未编译出来——它们位于 `hudi-spark3.3.x` 等子模块中，通过 Maven Profile 在最终的 bundle 中才被组装到一起。

#### 8.5.5 版本差异的具体适配示例

**示例 1：VariantType（Spark 4.0 新特性）**

```scala
// Spark 3.x 的 BaseSpark3Adapter：
override def getVariantDataType: Option[DataType] = None  // 不支持
override def isVariantType(dataType: DataType): Boolean = false

// Spark 4.0 的 BaseSpark4Adapter：
override def getVariantDataType: Option[DataType] = Some(VariantType)  // 支持
override def isVariantType(dataType: DataType): Boolean = dataType.isInstanceOf[VariantType]
```

**示例 2：Lance 文件格式（Spark 4.0 独有）**

```scala
// Spark 3.3 的 Spark3_3Adapter：
override def createLanceFileReader(...): Option[SparkColumnarFileReader] = None

// Spark 4.0 的 Spark4_0Adapter：
override def createLanceFileReader(...): Option[SparkColumnarFileReader] = Some(new SparkLanceReaderBase(vectorized))
```

**示例 3：SparkContext 停止方式**

```scala
// Spark 3.3: jssc.stop()
override def stopSparkContext(jssc: JavaSparkContext, exitCode: Int): Unit = jssc.stop()

// Spark 4.0: jssc.sc.stop(exitCode)  — 支持传入退出码
override def stopSparkContext(jssc: JavaSparkContext, exitCode: Int): Unit = jssc.sc.stop(exitCode)
```

**示例 4：HoodieInternalRow 版本差异**

```scala
// Spark 3.x: 使用 Spark3HoodieInternalRow
override def createInternalRow(metaFields: Array[UTF8String], sourceRow: InternalRow,
    sourceContainsMetaFields: Boolean): HoodieInternalRow =
  new Spark3HoodieInternalRow(metaFields, sourceRow, sourceContainsMetaFields)

// Spark 4.x: 使用 Spark4HoodieInternalRow
override def createInternalRow(metaFields: Array[UTF8String], sourceRow: InternalRow,
    sourceContainsMetaFields: Boolean): HoodieInternalRow =
  new Spark4HoodieInternalRow(metaFields, sourceRow, sourceContainsMetaFields)
```

#### 8.5.6 Maven Profile 驱动的编译隔离

在构建时，Maven Profile 决定哪些版本特定模块参与编译：

```bash
# 编译 Spark 3.3 版本
mvn clean package -Dspark3.3  # 激活 spark3.3 profile，编译 hudi-spark3.3.x 模块

# 编译 Spark 4.0 版本
mvn clean package -Dspark4.0  # 激活 spark4.0 profile，编译 hudi-spark4.0.x 模块
```

每个 Profile 会设置 `hudi.spark.module` 和 `hudi.spark.common.module` 等 Maven 属性，这些属性在 bundle `pom.xml` 的 `<artifactSet>` 中被引用，确保最终 bundle 中只包含对应版本的 Adapter 实现。

#### 8.5.7 为什么这么设计，好处是什么

1. **单一代码库支持 4 个 Spark 版本**：Hudi 只维护一套代码仓库，通过 Shim 模式同时支持 Spark 3.3/3.4/3.5/4.0 四个版本，避免了分支维护的巨大成本。
2. **最大化代码复用**：真正的版本差异代码只占 5% 左右。以 `Spark3_3Adapter` 为例，它只有约 200 行代码，其余 95% 的 Spark 集成逻辑在 `hudi-spark-common` 和 `hudi-spark3-common` 中共享。
3. **渐进式新版本支持**：新增 Spark 版本支持只需：（a）创建一个 `hudi-sparkX.Y.x` 模块；（b）实现对应的 `SparkX_YAdapter`，覆盖有差异的方法；（c）在 `SparkAdapterSupport` 中添加版本判断。大部分方法直接继承自 `BaseSpark3Adapter` 或 `BaseSpark4Adapter`。
4. **编译时安全**：版本特定代码在独立模块中编译，不会因为引用了不存在的 API 而导致其他版本的编译失败。运行时通过反射加载正确的 Adapter，确保类型安全。
5. **向前兼容设计**：`SparkAdapter` 中的新方法（如 `getVariantDataType`、`isVariantShreddingStruct`）在旧版本 Adapter 中提供合理的默认返回值（None/false/UnsupportedOperationException），确保新功能只在支持它的 Spark 版本上生效，不影响旧版本的正常运行。
