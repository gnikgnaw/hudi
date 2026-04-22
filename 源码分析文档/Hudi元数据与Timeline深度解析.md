# Hudi 元数据与 Timeline 深度解析

> 基于 Apache Hudi 源码深度分析
> 文档版本：5.0
> 源码版本：v1.2.0-SNAPSHOT (master)
> 最后更新：2026-04-21

---

## 目录

1. [Hudi 元数据架构总览](#1-hudi-元数据架构总览)
2. [Timeline 机制深度解析](#2-timeline-机制深度解析)
3. [HoodieInstant — 时间点](#3-hoodieinstant-时间点)
4. [文件系统视图 — FileSystemView](#4-文件系统视图-filesystemview)
5. [Metadata Table 深度解析](#5-metadata-table-深度解析)
6. [HoodieTableMetaClient — 元数据客户端](#6-hoodietablemetaclient-元数据客户端)
7. [Timeline Server — 嵌入式元数据服务](#7-timeline-server-嵌入式元数据服务)
8. [与 Iceberg 元数据体系的对比](#8-与-iceberg-元数据体系的对比)
9. [元数据源码深度剖析](#9-元数据源码深度剖析)

---

## 1. Hudi 元数据架构总览

### 1.1 三层元数据结构

**为什么需要三层？** 不同层解决不同问题：

```
Layer 1: Table Properties（不可变属性）
    ├── 存储: .hoodie/hoodie.properties
    ├── 内容: 表类型、表名、key fields、表版本
    ├── 生命周期: 建表确定，只能通过升级变更
    └── 为什么单独一层？
        → 这些属性需要在任何操作之前就能读取
        → 不需要 Timeline 也能获取表的基本信息

Layer 2: Timeline（操作历史）
    ├── 存储: .hoodie/timeline/ (V2) 或 .hoodie/ (V1)
    ├── 内容: 所有操作的有序记录（commit/compaction/clean/...）
    ├── 生命周期: 每次操作产生，Archival 归档旧的
    └── 为什么独立于数据文件？
        → Timeline 是"日志"，数据文件是"状态"
        → 读取 Timeline 可以重建任意时间点的表状态
        → 支持时间旅行、增量查询、故障恢复

Layer 3: Metadata Table（元数据表）
    ├── 存储: .hoodie/metadata/ (本身也是 MOR 表)
    ├── 内容: 文件列表、列统计、Bloom Filter、Record Index、二级索引
    ├── 生命周期: 随主表 commit 同步更新
    └── 为什么用一个 Hudi 表来存元数据？
        → 避免昂贵的文件系统 LIST 操作（S3 上极慢）
        → 利用 Hudi 自身的 MOR 写入能力增量更新
        → 利用 Hudi 自身的 Compaction 控制元数据文件大小
```

### 1.2 完整目录结构

```
<basePath>/
├── .hoodie/                              # 元数据根目录
│   ├── hoodie.properties                 # Layer 1: 表属性
│   ├── timeline/                         # Layer 2: Active Timeline (V2)
│   │   ├── T1_T1c.commit                # 已完成的写入
│   │   ├── T2_T2c.deltacommit           # MOR 增量提交
│   │   ├── T3.compaction.requested       # 待执行的 Compaction
│   │   └── T4.clean                      # 已完成的 Clean
│   ├── archived/                         # Layer 2: Archived Timeline
│   │   └── commits_1.avro (V1) 或 LSM文件 (V2)
│   ├── metadata/                         # Layer 3: Metadata Table
│   │   ├── .hoodie/                      # MT 自身的 Timeline
│   │   ├── files/                        # 文件列表分区
│   │   ├── column_stats/                 # 列统计分区
│   │   ├── bloom_filters/                # Bloom Filter 分区
│   │   ├── record_index/                 # Record Level Index
│   │   ├── partition_stats/              # 分区统计
│   │   ├── secondary_index_xxx/          # 二级索引
│   │   └── expr_index_xxx/              # 表达式索引
│   ├── .temp/                            # Marker 文件 + 临时文件
│   ├── .heartbeat/                       # Writer 心跳
│   ├── .locks/                           # 文件系统锁
│   ├── .schema/                          # Schema 历史
│   ├── .index_defs/index.json            # 索引定义
│   └── .bucket_index/                    # Bucket Index 元数据
└── <partition>/                          # 数据分区
    ├── <fileId>_<token>_<instant>.parquet
    └── .<fileId>_<instant>.log.<ver>_<token>
```

---

## 2. Timeline 机制深度解析

### 2.1 为什么 Timeline 是 Hudi 的核心？

Timeline 不仅是操作日志，更是 Hudi 实现以下能力的基础：

| 能力 | 依赖 Timeline 的什么 |
|------|---------------------|
| **ACID 原子性** | Instant 状态转换（INFLIGHT→COMPLETED 是原子操作）|
| **时间旅行** | 按 Instant Time 重建任意时间点的文件视图 |
| **增量查询** | 按时间范围过滤 Instant，提取变更文件 |
| **故障恢复** | 检测 INFLIGHT Instant → Rollback 或 Resume |
| **并发控制** | Instant 之间的冲突检测 |
| **表服务协调** | Compaction/Clustering 计划存储为 REQUESTED Instant |

### 2.2 HoodieActiveTimeline — 接口与实现

`HoodieActiveTimeline` 是一个**接口**，定义了 Active Timeline 的核心操作。

**源码位置**：`hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieActiveTimeline.java`

```java
public interface HoodieActiveTimeline extends HoodieTimeline {
    void createNewInstant(HoodieInstant instant);
    <T> HoodieInstant saveAsComplete(HoodieInstant instant, Option<T> metadata);
    void transitionState(HoodieInstant fromInstant, HoodieInstant toInstant, ...);
    void deleteInstantFileIfExists(HoodieInstant instant);
    HoodieTimeline reload();  // 从存储重新加载
}
```

**实现层次**：
```
HoodieActiveTimeline (接口)
    ↑ implements
ActiveTimelineV1 (类) extends BaseTimelineV1  ← 表版本 ≤ 7
ActiveTimelineV2 (类) extends BaseTimelineV2  ← 表版本 ≥ 8

BaseTimelineV1/V2 (抽象类)
    ├── 继承自 BaseHoodieTimeline
    ├── 管理 Instant 列表
    ├── 文件命名规则
    └── 序列化/反序列化
```

### 2.3 Timeline V1 vs V2 — 文件命名与排序

| 维度 | V1 (表版本 ≤ 7) | V2 (表版本 ≥ 8) |
|------|-----------------|-----------------|
| **文件名** | `<instantTime>.<action>[.<state>]` | `<requestedTime>_<completionTime>.<action>[.<state>]` |
| **completionTime** | 存在 Instant 文件内容中 | 直接编码在文件名中 |
| **排序** | 只能按 requestedTime | 可按 completionTime（更准确） |
| **增量查询** | 按 requestedTime，有数据丢失风险 | 按 completionTime，因果一致 |

**V2 解决的核心问题 — 为什么 completionTime 在文件名中很重要？**

```
场景: 两个并发 Writer

V1 的问题:
  Writer A: requested=T1, 写入很慢..., completed=T5
  Writer B: requested=T2, 写入很快, completed=T3

  增量消费者按 requestedTime 排序: T1 → T2
  消费者读到 T2 后 checkpoint=T2
  但 T1 在 T2 之后才完成！
  → T1 的数据被遗漏

V2 的解决:
  文件名: T1_T5.commit, T2_T3.commit
  按 completionTime 排序: T3(B) → T5(A)
  消费者按完成顺序消费 → 不会遗漏

  而且不需要读文件内容就能知道 completionTime → 性能提升
```

### 2.4 Active Timeline vs Archived Timeline

```
                    hoodie.keep.max.commits = 30
                           ↓ (超过时触发 Archival)
Active Timeline ──────────────────── Archived Timeline
(最近 20-30 个 instant)              (历史 instant)
   │                                     │
   ├── 高频访问                          ├── 低频访问
   ├── 文件系统文件                      ├── Avro/LSM 压缩存储
   ├── 日常读写操作使用                  ├── 时间旅行/审计使用
   └── 控制参数:                         └── 自动管理
       hoodie.keep.min.commits=20
       hoodie.keep.max.commits=30
```

**为什么要归档？** S3 等对象存储的 LIST 操作是按前缀扫描的。如果 `.hoodie/timeline/` 下有 10000 个文件，每次 MetaClient 初始化都要 LIST 10000 个文件。归档后只保留 20-30 个，初始化速度提升 100 倍以上。

---

## 3. HoodieInstant — 时间点

### 3.1 结构

**源码位置**：`hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieInstant.java`

```java
public class HoodieInstant implements Serializable, Comparable<HoodieInstant> {
    private final State state;            // REQUESTED / INFLIGHT / COMPLETED / NIL
    private final String action;          // commit / deltacommit / compaction / ...
    private final String requestedTime;   // 请求时间（唯一标识）
    private final String completionTime;  // 完成时间（V2 新增，可为 null）
    private boolean isLegacy = false;     // 表版本 < 7 的旧格式标记
    private final Comparator<HoodieInstant> comparator;
}

// State 枚举定义
public enum State {
    REQUESTED,   // 请求状态（Compaction/Clustering 计划）
    INFLIGHT,    // 执行中
    COMPLETED,   // 已完成
    NIL          // 无效状态
}

// equals() 只比较 state + action + requestedTime，不比较 completionTime
// 这意味着同一个操作在不同状态下（如 INFLIGHT vs COMPLETED）是不同的 Instant
```

### 3.2 Action 类型全景

| Action | 说明 | 适用表类型 | 对应的 Commit Metadata 类 |
|--------|------|-----------|-------------------------|
| `commit` | COW 写入 | COW | `HoodieCommitMetadata` |
| `deltacommit` | MOR 写入 | MOR | `HoodieCommitMetadata` |
| `compaction` | Compaction | MOR | `HoodieCompactionPlan` (requested), `HoodieCommitMetadata` (completed) |
| `logcompaction` | Log Compaction（合并 log 文件） | MOR | 类似 compaction |
| `clean` | 文件清理 | 两种 | `HoodieCleanerPlan` / `HoodieCleanMetadata` |
| `rollback` | 回滚 | 两种 | `HoodieRollbackPlan` / `HoodieRollbackMetadata` |
| `savepoint` | 保存点 | 两种 | `HoodieSavepointMetadata` |
| `restore` | 恢复到保存点 | 两种 | `HoodieRestorePlan` / `HoodieRestoreMetadata` |
| `replacecommit` | 替换提交(Clustering/INSERT_OVERWRITE) | 两种 | `HoodieReplaceCommitMetadata` |
| `clustering` | Clustering（V2 独立 action，V1 使用 replacecommit） | 两种 | `HoodieReplaceCommitMetadata` |
| `indexing` | 索引构建 | 两种 | `HoodieIndexPlan` / `HoodieIndexCommitMetadata` |
| `schemacommit` | Schema 保存（仅用于 schema 变更记录） | 两种 | - |

---

## 4. 文件系统视图 — FileSystemView

### 4.1 为什么需要 FileSystemView？

**问题**：Timeline 记录的是操作历史，但查询需要知道"当前应该读哪些文件"。FileSystemView 就是从 Timeline 推导出的**文件快照视图**。

```
Timeline:
  T1.commit (写入 fg1_v1, fg2_v1)
  T2.deltacommit (追加 fg1.log.1)
  T3.compaction (fg1_v1 + log → fg1_v2)
  T4.clean (删除 fg1_v1)

FileSystemView 推导:
  当前最新视图:
    fg1 → FileSlice(base=fg1_v2, logs=[])
    fg2 → FileSlice(base=fg2_v1, logs=[])

  T2 时刻的视图（时间旅行）:
    fg1 → FileSlice(base=fg1_v1, logs=[fg1.log.1])
    fg2 → FileSlice(base=fg2_v1, logs=[])
```

### 4.2 实现类全景

**源码位置**：`hudi-common/src/main/java/org/apache/hudi/common/table/view/`

```
TableFileSystemView (接口)
    ├── BaseFileOnlyView — 只看 Base Files
    └── SliceView — 看完整 FileSlice（base + logs）

实现类层次:
AbstractTableFileSystemView (抽象基类)
    └── IncrementalTimelineSyncFileSystemView (抽象类，增量同步)
        ├── HoodieTableFileSystemView        ← ★ 基于内存 HashMap
        │   └── SpillableMapBasedFileSystemView  ← 内存+磁盘溢写
        └── RocksDbBasedFileSystemView       ← 基于 RocksDB

RemoteHoodieTableFileSystemView          ← 远程视图（调用 Timeline Server API）

PriorityBasedFileSystemView              ← ★ 组合视图：先查远程，失败再查本地
    ├── primary: RemoteHoodieTableFileSystemView
    └── secondary: HoodieTableFileSystemView

HoodieTablePreCommitFileSystemView       ← Pre-commit 视图（含未提交的数据）

FileSystemViewManager                    ← 工厂，根据配置创建合适的 View
```

**为什么有这么多实现？**

| 实现 | 适用场景 | 为什么 |
|------|---------|--------|
| `HoodieTableFileSystemView` | 小表，Spark Driver | 全部缓存在内存，最快 |
| `SpillableMapBasedFileSystemView` | 大表 | 内存不足时溢写磁盘 |
| `RocksDbBasedFileSystemView` | 超大表 | RocksDB 持久化，重启不丢 |
| `RemoteHoodieTableFileSystemView` | 多 Executor 共享 | 调 Timeline Server，避免每个 Executor 都构建视图 |
| `PriorityBasedFileSystemView` | 生产推荐 | 先查 Timeline Server（快），失败降级本地（可靠）|

### 4.3 FileSystemView 构建流程

```
FileSystemViewManager.createViewManager()
    ↓
1. 加载 Active Timeline
    → 读取 .hoodie/timeline/ 下所有 instant 文件
    → 构建有序的 Timeline
    ↓
2. 获取文件列表
    ├── 优先: 从 Metadata Table 的 FILES 分区获取（O(1) 查表，无 LIST）
    └── 降级: 直接 LIST 文件系统（S3 上很慢）
    ↓
3. 构建 FileGroup 映射
    ├── 按 fileId 分组为 HoodieFileGroup
    ├── 按 instantTime 排序构建 FileSlice 链
    └── 关联 base files + log files
    ↓
4. 过滤无效文件
    ├── 排除 INFLIGHT 写入的文件（未完成）
    ├── 排除已 ROLLBACK 的文件
    └── 排除 pending compaction/clustering 锁定的文件
```

---

## 5. Metadata Table 深度解析

### 5.1 为什么 Metadata Table 用 MOR 表实现？

```
传统方式存储元数据（如 JSON 文件）:
  每次更新 → 全量重写 → O(N) 写放大

用 Hudi MOR 表存储元数据:
  每次更新 → 追加 log record → O(1) 写放大
  定期 Compaction → 合并为新 base file

好处:
  1. 增量更新（不需要全量重写文件列表）
  2. 事务保证（Metadata Table 的更新与主表 commit 在同一事务中）
  3. 利用 Hudi 自身的 Compaction 控制文件大小
  4. 利用 Hudi 自身的 HFile 格式做快速 key 查找
```

### 5.2 Metadata Table 分区类型

**源码位置**：`hudi-common/src/main/java/org/apache/hudi/metadata/MetadataPartitionType.java`

```java
public enum MetadataPartitionType {
    FILES(PARTITION_NAME_FILES, "files-", 2),
    COLUMN_STATS(PARTITION_NAME_COLUMN_STATS, "col-stats-", 3),
    BLOOM_FILTERS(PARTITION_NAME_BLOOM_FILTERS, "bloom-filters-", 4),
    RECORD_INDEX(PARTITION_NAME_RECORD_INDEX, "record-index-", 5),
    EXPRESSION_INDEX(PARTITION_NAME_EXPRESSION_INDEX_PREFIX, "expr-index-", -1),
    SECONDARY_INDEX(PARTITION_NAME_SECONDARY_INDEX_PREFIX, "secondary-index-", 7),
    PARTITION_STATS(PARTITION_NAME_PARTITION_STATS, "partition-stats-", 6),
    ALL_PARTITIONS(PARTITION_NAME_FILES, "files-", 1);  // 复用 FILES 分区
}
```

**说明**：
- 共 8 种分区类型，每种有对应的 Record Type（整数标识）
- `ALL_PARTITIONS` 是特殊类型，与 `FILES` 共享同一物理分区（都是 `files/`），只是记录类型不同（1 vs 2）
- `EXPRESSION_INDEX` 的 Record Type 为 -1，表示动态类型
- 每个分区类型有独立的 fileId 前缀（如 `files-`、`col-stats-` 等）

### 5.3 FILES 分区 — 替代文件系统 LIST

```
作用: 存储每个分区下的文件列表

Key:   partitionPath (如 "dt=2024-01-01")
Value: Map<fileName, HoodieMetadataFileInfo>
    HoodieMetadataFileInfo:
        ├── size: long         # 文件大小
        └── isDeleted: boolean # 是否已删除

查询: 获取某分区下所有文件
  → 在 FILES 分区中查找 key=partitionPath → 直接返回文件列表
  → vs 文件系统 LIST: S3 上 1000 文件需要 1 次 LIST API（~200ms）
  → FILES 分区查找: ~10ms
```

### 5.4 Metadata Table 的同步更新机制

```
主表 commit 流程中的 Metadata Table 更新:

BaseHoodieWriteClient.commitStats()
    ↓
postCommit()
    ↓
HoodieTableMetadataWriter.updateFromWriteStatuses()
    ├── 提取 WriteStatus 中的新文件信息
    ├── 计算新文件的 Column Stats / Bloom Filter
    ├── 构建 Metadata Table 的 HoodieRecord
    └── 写入 Metadata Table（作为 deltacommit）

关键: 主表 commit 和 Metadata Table 更新是原子的
  → 如果主表 commit 失败，Metadata Table 更新也回滚
  → 保证元数据与数据的一致性
```

### 5.5 配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `hoodie.metadata.enable` | `true` | 启用 Metadata Table |
| `hoodie.metadata.index.column.stats.enable` | `false` | 启用列统计 |
| `hoodie.metadata.index.bloom.filter.enable` | `false` | 启用 BF 索引 |
| `hoodie.metadata.record.index.enable` | `false` | 启用 Record Index |
| `hoodie.metadata.compact.max.delta.commits` | `10` | MT Compaction 间隔 |

---

## 6. HoodieTableMetaClient — 元数据统一入口

### 6.1 核心字段

```java
public class HoodieTableMetaClient implements Serializable {
    protected StoragePath basePath;                    // 表根路径
    protected StoragePath metaPath;                    // .hoodie/ 路径
    private transient HoodieStorage storage;
    private HoodieTableType tableType;
    protected HoodieTableConfig tableConfig;           // hoodie.properties
    private TimelineLayoutVersion timelineLayoutVersion;
    private TimelineLayout timelineLayout;             // ★ Timeline 布局策略
    private StoragePath timelinePath;                  // timeline 目录
    private StoragePath timelineHistoryPath;            // archived 目录
    protected HoodieActiveTimeline activeTimeline;
    private Option<HoodieIndexMetadata> indexMetadataOpt;
    private HoodieTableFormat tableFormat;             // 表格式
}
```

### 6.2 MetaClient 的懒加载设计

**为什么 Timeline 是懒加载的？**
```java
// 构造函数中:
if (loadActiveTimelineOnLoad) {
    this.activeTimeline = ...;  // 立即加载
} else {
    // 延迟到首次访问时加载
}
```

Timeline 加载涉及文件系统 LIST 操作（尤其是 V1 格式），在 S3 上可能需要数秒。懒加载允许快速构建 MetaClient 用于获取表配置等不需要 Timeline 的信息。

---

## 7. Timeline Server — 嵌入式元数据服务

### 7.1 为什么需要 Timeline Server？

```
没有 Timeline Server:
  Spark Driver: 构建 FileSystemView → 加载 Timeline + LIST 文件系统
  Executor 1: 需要文件视图 → 重新构建 FileSystemView
  Executor 2: 需要文件视图 → 重新构建 FileSystemView
  ...
  Executor N: 需要文件视图 → 重新构建 FileSystemView
  → N+1 次重复的 Timeline 加载和文件 LIST

有 Timeline Server:
  Spark Driver: 启动 Timeline Server（Javalin HTTP 服务）
  Driver: 构建 FileSystemView → 缓存在 Timeline Server 中
  Executor 1~N: 调用 Timeline Server API → 直接获取文件视图
  → 1 次构建，N 次复用
```

### 7.2 配置

```properties
hoodie.embed.timeline.server=true           # 启用嵌入式 Timeline Server
hoodie.embed.timeline.server.port=0         # 自动分配端口
hoodie.embed.timeline.server.threads=32     # 服务线程数
```

---

## 8. 与 Iceberg 元数据体系的对比

| 维度 | Hudi | Iceberg |
|------|------|---------|
| **元数据根** | `.hoodie/` | `metadata/` |
| **操作历史** | Timeline（有状态的 Instant 序列） | Snapshot 链（metadata.json 版本链）|
| **状态追踪** | REQUESTED→INFLIGHT→COMPLETED | 无中间状态（提交即完成）|
| **原子提交** | 文件重命名/原子写入 | metadata.json 原子写入 |
| **文件索引** | Metadata Table 的 FILES 分区 | Manifest File 层级 |
| **列统计** | Metadata Table 的 COLUMN_STATS 分区 | Manifest File 内嵌 |
| **元数据存储** | Hudi MOR 表（增量更新） | Avro Manifest 文件（追加式）|
| **元数据服务** | 嵌入式 Timeline Server | Catalog Server（REST/JDBC）|

**Hudi 的设计优势**：
- Timeline 有状态 → 支持故障恢复（INFLIGHT 可以 rollback）
- Metadata Table 用 MOR → 增量更新成本低
- 内置 Timeline Server → 多 Executor 共享视图
- Record Level Index → O(1) 点查能力

**Iceberg 的设计优势**：
- Manifest 自包含统计 → 无需额外 Metadata Table
- metadata.json 原子更新 → 更简单的一致性模型
- 无 Timeline Server 依赖 → 架构更简单

---

## 9. 元数据源码深度剖析

### 9.1 TimelineLayout 与 TimelineFactory — 版本化的 Timeline 策略体系

**源码位置**：
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/TimelineLayout.java`
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/TimelineFactory.java`
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/versioning/TimelineLayoutVersion.java`
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/versioning/v1/TimelineV1Factory.java`
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/versioning/v2/TimelineV2Factory.java`

**设计思路**：Hudi 需要同时支持 0.x（旧版本）和 1.x（新版本）两种表格式，Timeline 的文件命名规则、Instant 排序逻辑、序列化方式都有本质差异。如何让一套代码同时支持两种格式？Hudi 采用了**策略模式 + 工厂模式**的组合。

`TimelineLayout` 是抽象基类，内部通过静态 `LAYOUT_MAP` 维护了三个版本的实例：

```java
public abstract class TimelineLayout implements Serializable {
  private static final Map<TimelineLayoutVersion, TimelineLayout> LAYOUT_MAP = new HashMap<>();
  public static final TimelineLayout TIMELINE_LAYOUT_V0 = new TimelineLayoutV0();  // pre 0.5.1
  public static final TimelineLayout TIMELINE_LAYOUT_V1 = new TimelineLayoutV1();  // 0.x (无重命名)
  public static final TimelineLayout TIMELINE_LAYOUT_V2 = new TimelineLayoutV2();  // 1.x (completionTime 编码在文件名)

  public static TimelineLayout fromVersion(TimelineLayoutVersion version) {
    return LAYOUT_MAP.get(version);
  }
}
```

`TimelineLayout` 对外提供七个抽象方法，分别返回不同的策略组件：

| 方法 | 返回组件 | 职责 |
|------|---------|------|
| `getInstantGenerator()` | `InstantGeneratorV1/V2` | 从文件名解析出 HoodieInstant |
| `getInstantFileNameGenerator()` | `InstantFileNameGeneratorV1/V2` | 从 HoodieInstant 生成文件名 |
| `getTimelineFactory()` | `TimelineV1Factory/V2Factory` | 创建 ActiveTimeline、ArchivedTimeline 等 |
| `getInstantComparator()` | `InstantComparatorV1/V2` | Instant 排序规则 |
| `getInstantFileNameParser()` | `InstantFileNameParserV2` | 文件名解析器 |
| `getCommitMetadataSerDe()` | `CommitMetadataSerDeV1/V2` | Commit 元数据序列化/反序列化 |
| `getTimelinePathProvider()` | `TimelinePathProviderV1/V2` | Timeline 目录路径 |

`TimelineFactory` 是抽象工厂，`TimelineV1Factory` 和 `TimelineV2Factory` 分别创建对应版本的 `ActiveTimeline`、`ArchivedTimeline`、`CompletionTimeQueryView` 等实例。例如 `TimelineV2Factory` 会创建 `ActiveTimelineV2`（支持 completionTime 编码在文件名中），而 `TimelineV1Factory` 创建 `ActiveTimelineV1`。

**版本选择的入口**在 `HoodieTableMetaClient` 的构造函数中：

```java
// HoodieTableMetaClient 构造函数
this.timelineLayoutVersion = layoutVersion.orElseGet(tableConfigVersion::get);
this.timelineLayout = TimelineLayout.fromVersion(timelineLayoutVersion);
this.timelinePath = timelineLayout.getTimelinePathProvider()
    .getTimelinePath(tableConfig, this.basePath);
```

**V1 vs V2 的核心区别**还体现在 `filterHoodieInstants` 方法上。V0 不做任何过滤（保留所有 Instant），V1/V2 都会调用 `filterHoodieInstantsByLatestState`，按 `(requestedTime, comparableAction)` 分组后只保留最高状态（COMPLETED > INFLIGHT > REQUESTED）的 Instant。但 V1 和 V2 使用不同的 `comparableAction` 映射：V1 将 `compaction` 映射为 `commit`（因为 0.x 中 compaction 完成后写入的文件名是 `.commit`），V2 同样如此但额外处理了 `clustering` 这一在 1.x 中独立出来的 action。

**为什么这么设计？** 这种设计的好处是**开闭原则**：添加新的 Timeline 版本只需新增一个 `TimelineLayoutVx` 子类并注册到 `LAYOUT_MAP`，不需要修改任何已有代码。整个系统通过 `TimelineLayout.fromVersion()` 一个入口就能切换所有相关行为，避免在代码中出现大量 `if (version == 1) ... else ...` 的判断。

**另一个重要区别是 TimelinePathProvider**：V1 中 Timeline 目录就是 `.hoodie/`（Instant 文件直接放在 `.hoodie/` 下），V2 中 Timeline 目录是 `.hoodie/timeline/`（通过 `tableConfig.getTimelinePath()` 配置），这使得 V2 的目录结构更清晰，也避免了 Instant 文件与 `hoodie.properties` 等文件混在同一目录导致的 LIST 性能问题。

---

### 9.2 HoodieCommitMetadata 的完整结构

**源码位置**：
- `hudi-common/src/main/java/org/apache/hudi/common/model/HoodieCommitMetadata.java`
- `hudi-common/src/main/java/org/apache/hudi/common/model/HoodieReplaceCommitMetadata.java`
- `hudi-common/src/main/java/org/apache/hudi/common/model/HoodieWriteStat.java`
- `hudi-common/src/main/java/org/apache/hudi/common/model/WriteOperationType.java`

`HoodieCommitMetadata` 是每次 commit/deltacommit 操作完成后存储在 Instant 文件中的核心元数据。它记录了这次写入操作的全部详情：

```java
public class HoodieCommitMetadata implements Serializable {
  // 核心字段
  protected Map<String, List<HoodieWriteStat>> partitionToWriteStats; // 按分区组织的写入统计
  protected Boolean compacted;                                         // 是否是 Compaction 产生的
  protected Map<String, String> extraMetadata;                         // 扩展元数据（schema、checkpoint 等）
  protected WriteOperationType operationType;                          // 操作类型（INSERT/UPSERT/DELETE/...）
}
```

**partitionToWriteStats** 是最核心的字段，它是一个 `Map<分区路径, List<HoodieWriteStat>>`。每个 `HoodieWriteStat` 包含了一个文件的完整写入统计：

```java
public class HoodieWriteStat extends HoodieReadStats {
  private String fileId;           // 文件组 ID
  private String path;             // 相对于 basePath 的文件路径
  private String prevCommit;       // 前一个版本的 commit（null 表示新文件）
  private long numWrites;          // 写入的总记录数
  private long numUpdateWrites;    // 更新的记录数
  private long totalWriteBytes;    // 写入的总字节数
  private long totalWriteErrors;   // 写入错误数
  private String partitionPath;    // 分区路径
  private long fileSizeInBytes;    // 文件最终大小
  private Long minEventTime;       // 最早事件时间（用于计算延迟）
  private Long maxEventTime;       // 最晚事件时间（用于计算新鲜度）
  private RuntimeStats runtimeStats; // 运行时统计（扫描/创建/Upsert 耗时）
  private Map<String, HoodieColumnRangeMetadata<Comparable>> recordsStats; // 列级别统计
}
```

**extraMetadata** 存储了可扩展的键值对，常见的 key 包括：
- `schema`：本次写入使用的 Avro Schema（JSON 格式），用于 Schema Evolution 追踪
- 用户自定义的 checkpoint 信息（如 DeltaStreamer 的 Kafka offset）
- 任何通过 `HoodieWriteConfig.EXTRA_METADATA_PREFIX` 传入的自定义元数据

**operationType** 是一个枚举，覆盖了 Hudi 支持的所有写入操作类型：`INSERT`、`UPSERT`、`BULK_INSERT`、`DELETE`、`INSERT_OVERWRITE`、`DELETE_PARTITION`、`INSERT_OVERWRITE_TABLE`、`CLUSTER`、`COMPACT`、`LOG_COMPACT`、`ALTER_SCHEMA`、`INDEX`、`BUCKET_RESCALE` 等共 17 种。

**HoodieReplaceCommitMetadata** 继承自 `HoodieCommitMetadata`，额外增加了一个关键字段：

```java
protected Map<String, List<String>> partitionToReplaceFileIds;
```

这个字段记录了在 Clustering 或 INSERT_OVERWRITE 操作中，哪些分区中的哪些文件被替换掉了。FileSystemView 在构建文件视图时，会利用这个信息将被替换的旧文件从视图中排除。

**这些信息如何被使用？**

1. **查询优化**：`HoodieCommitMetadata.fetchTotalFilesInsert()`、`fetchTotalFilesUpdated()` 等方法通过遍历 `partitionToWriteStats` 统计新增和更新的文件数量。Spark 的 `HoodieROPathFilter` 使用这些信息过滤出有效文件。列级别统计（`recordsStats`）支撑了列统计索引（Column Stats Index）的构建，从而在查询时实现文件级别的 Data Skipping。

2. **表服务（Table Services）**：Clean 操作需要知道每个文件最后被哪个 commit 引用（通过遍历 `partitionToWriteStats` 中的 `fileId`），以决定哪些旧版本文件可以安全删除。Compaction 调度器读取 `deltacommit` 的元数据来统计每个 FileSlice 的 log 文件数量和大小，从而决定哪些 FileSlice 需要 Compaction。

3. **Metadata Table 更新**：`HoodieBackedTableMetadataWriter.update(HoodieCommitMetadata, instantTime)` 直接使用 `partitionToWriteStats` 中的文件信息来更新 Metadata Table 的 FILES 分区，同时提取 `recordsStats` 来更新 COLUMN_STATS 分区。

4. **增量查询**：增量消费者通过读取 commit metadata 中的 `partitionToWriteStats` 获知哪些分区和文件被变更了，从而只读取变更部分的数据。

---

### 9.3 Metadata Table 的 HFile 格式 — 为什么不用 Parquet

**源码位置**：
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/metadata/HoodieBackedTableMetadataWriter.java` (第 1046 行设置 `HFILE` 格式)
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/metadata/HoodieMetadataWriteUtils.java` (第 136、233 行)
- `hudi-common/src/main/java/org/apache/hudi/io/storage/HoodieNativeAvroHFileReader.java`
- `hudi-common/src/main/java/org/apache/hudi/io/storage/HoodieAvroHFileReaderImplBase.java`
- `hudi-common/src/main/java/org/apache/hudi/metadata/HoodieMetadataPayload.java`

Metadata Table 在初始化时显式指定了 `HFile` 作为 base file 格式：

```java
// HoodieBackedTableMetadataWriter.java 第 1046 行
.setBaseFileFormat(HoodieFileFormat.HFILE.toString())
```

同时配置了 10GB 的超大文件大小上限，确保每个分区的文件组不会被拆分：

```java
// HoodieMetadataWriteUtils.java 第 136 行
private static final long MDT_MAX_HFILE_SIZE_BYTES = 10 * 1024 * 1024 * 1024L; // 10GB
```

**为什么选择 HFile 而不是 Parquet？** 这是 Metadata Table 最关键的设计决策之一。

| 维度 | HFile | Parquet |
|------|-------|---------|
| **数据模型** | Key-Value（排序的键值对） | 列式（Column-oriented） |
| **点查能力** | 支持 O(log N) 二分查找 seekTo | 不支持单行点查，需全文件扫描或读取 Row Group |
| **前缀查找** | 原生支持有序遍历（prefix scan） | 不支持 |
| **Bloom Filter** | 内置于 HFile meta block 中 | 需要额外存储 |
| **写入模式** | 追加写入，键有序 | 批量写入，列式编码 |

Metadata Table 的核心使用场景是**按 key 查找**：
- FILES 分区：key 是 partitionPath，查找某个分区的文件列表
- COLUMN_STATS 分区：key 是 `hash(column) + hash(partition) + hash(file)` 的组合
- RECORD_INDEX 分区：key 是记录的主键，查找记录位于哪个文件
- BLOOM_FILTERS 分区：key 是 `hash(partition) + hash(file)`

这些场景都是**点查或前缀查找**，Parquet 的列式存储结构完全不适合这种访问模式。

**HFile 的 key-value 查找实现**可以在 `HoodieNativeAvroHFileReader` 中看到：

```java
// HoodieNativeAvroHFileReader.java - RecordByKeyIterator
UTF8StringKey key = new UTF8StringKey(rawKey);
if (reader.seekTo(key) == HFileReader.SEEK_TO_FOUND) {
    // Key is found, 读取对应的 value
    KeyValue keyValue = reader.getKeyValue().get();
    // 反序列化为 Avro GenericRecord
}
```

HFile 内部按 key 有序存储，并在每个 data block（默认 64KB）之间维护了一个索引层（Block Index）。`seekTo` 操作首先通过 Block Index 定位到目标 key 所在的 data block（O(log N)），然后在 block 内部进行线性或二分查找。对于 Record Index 这种 key 是唯一记录主键的场景，整个查找过程接近 O(log N)。

此外，HFile 还内嵌了 Bloom Filter（存储在 meta block 中），`RecordByKeyIterator` 在执行 `seekTo` 之前会先检查 Bloom Filter：

```java
if (bloomFilterOption.isPresent() && !bloomFilterOption.get().mightContain(rawKey)) {
    continue; // Bloom Filter 排除，跳过磁盘 seekTo
}
```

这进一步将不存在的 key 的查找优化到了 O(1) -- 只需要一次内存中的 Bloom Filter 检查。对于 Record Index 中的 Upsert 场景（需要判断记录是否已存在），Bloom Filter 能极大减少不必要的磁盘 IO。

**HFile 的 Block Cache 机制**也是关键优化。Metadata Table 配置了 HFile Block Cache（通过 `HoodieReaderConfig.HFILE_BLOCK_CACHE_ENABLED` 等配置），频繁访问的 Block Index 和 Bloom Filter block 会被缓存在内存中，进一步降低查找延迟。

---

### 9.4 HoodieTableMetadataWriter 的更新流程

**源码位置**：
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/metadata/HoodieTableMetadataWriter.java`
- `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/metadata/HoodieBackedTableMetadataWriter.java`

`HoodieTableMetadataWriter` 是一个接口，定义了 Metadata Table 更新的四种触发方式：

```java
public interface HoodieTableMetadataWriter<I, O> extends Serializable, AutoCloseable {
  void update(HoodieCommitMetadata commitMetadata, String instantTime);  // 主表 COMMIT
  void update(HoodieCleanMetadata cleanMetadata, String instantTime);    // CLEAN 操作
  void update(HoodieRestoreMetadata restoreMetadata, String instantTime); // RESTORE 操作
  void update(HoodieRollbackMetadata rollbackMetadata, String instantTime); // ROLLBACK 操作
}
```

**核心实现类**是 `HoodieBackedTableMetadataWriter`，一个抽象类，包含了引擎无关的更新逻辑。以主表 commit 触发的更新为例，完整流程如下：

```
1. update(HoodieCommitMetadata, instantTime) 被调用
   │
   ├── mayBeReinitMetadataReader()  -- 确保 Metadata Table 读取器已初始化
   ├── maybeInitializeNewFileGroupsForPartitionedRLI()  -- 如果启用了分区化 Record Index，
   │       为新出现的分区初始化文件组
   │
   └── processAndCommit(instantTime, convertMetadataFunction)
       │
       ├── getMetadataPartitionsToUpdate()  -- 从 tableConfig 获取需要更新的分区集合
       │       包括已完成的分区 + inflight 中的分区（支持异步索引构建）
       │
       ├── convertMetadataFunction.convertMetadata()  -- 将 CommitMetadata 转换为
       │       Map<分区名, HoodieData<HoodieRecord>> 格式的 Metadata Table 记录
       │       例如：FILES 分区的记录包含 key=partitionPath, value=文件列表更新
       │
       └── commit(instantTime, partitionRecordsMap)
           │
           └── commitInternal(instantTime, partitionRecordsMap, ...)
               │
               ├── tagRecordsWithLocation(partitionRecordsMap)  -- 为每条记录打上
               │       HoodieRecordLocation（确定写入哪个文件组）
               │
               ├── convertHoodieDataToEngineSpecificData(preppedRecords)
               │       -- 转换为引擎特定格式（Spark RDD / Flink DataStream）
               │
               ├── rollbackFailedWrites(...)  -- 回滚 Metadata Table 上的失败写入
               │
               └── writeClient.upsert(preppedRecordInputs, instantTime)
                   -- 通过内部 WriteClient 将记录写入 Metadata Table
                   -- Metadata Table 是 MOR 表，所以实际写入的是 log 文件
```

**事务一致性保证**是 Metadata Table 更新机制最关键的设计考量。Hudi 采用了以下策略来保证主表与 Metadata Table 之间的一致性：

1. **同步更新（非异步）**：Metadata Table 的更新发生在主表 `postCommit()` 阶段，是主表 commit 流程的一部分。主表 commit 成功后立即更新 Metadata Table，而不是异步的。

2. **幂等性设计**：`commitInternal` 方法在写入前会检查 `metadataMetaClient.getActiveTimeline().containsInstant(instantTime)`，如果该 instantTime 已经在 Metadata Table 的 Timeline 中存在，说明是重试场景，会跳过重复写入。

3. **失败回滚**：每次 commit 前都会调用 `rollbackFailedWrites` 来清理 Metadata Table 上可能存在的 INFLIGHT 但未完成的写入，确保 Metadata Table 的状态是干净的。

4. **Metadata Table 独立的表服务**：`performTableServices()` 方法在每次更新后触发 Metadata Table 自身的 Compaction、Clean、Archival 操作。Metadata Table 的 Compaction 间隔默认是 10 个 delta commits（通过 `hoodie.metadata.compact.max.delta.commits` 配置），比主表更频繁，以保持查找性能。

5. **单 Writer 保证**：Metadata Table 的写入始终由主表的 Writer 完成，不存在并发写入的问题。即使主表开启了多 Writer（OCC），Metadata Table 的更新也是在获得锁之后串行执行的。

**为什么不采用两阶段提交（2PC）？** 因为 Metadata Table 的读取可以容忍短暂的不一致：如果 Metadata Table 落后于主表（例如主表 commit 成功但 Metadata Table 更新失败），下次写入时 `rollbackFailedWrites` 会修复这种不一致。而 Metadata Table 超前于主表的情况不会发生（因为 Metadata Table 更新在主表 commit 之后）。这种"最终一致"的简化设计避免了 2PC 的复杂性和性能开销。

---

### 9.5 ConsistencyGuard 机制 — 最终一致性存储上的元数据保护

**源码位置**：
- `hudi-common/src/main/java/org/apache/hudi/common/fs/ConsistencyGuard.java`
- `hudi-common/src/main/java/org/apache/hudi/common/fs/NoOpConsistencyGuard.java`
- `hudi-common/src/main/java/org/apache/hudi/common/fs/FailSafeConsistencyGuard.java`
- `hudi-common/src/main/java/org/apache/hudi/common/fs/OptimisticConsistencyGuard.java`
- `hudi-common/src/main/java/org/apache/hudi/common/fs/ConsistencyGuardConfig.java`

`ConsistencyGuard` 是 Hudi 为应对最终一致性存储（如早期的 AWS S3）设计的保护机制。其核心接口非常简洁：

```java
public interface ConsistencyGuard {
  enum FileVisibility { APPEAR, DISAPPEAR }

  void waitTillFileAppears(StoragePath filePath) throws IOException, TimeoutException;
  void waitTillFileDisappears(StoragePath filePath) throws IOException, TimeoutException;
  void waitTillAllFilesAppear(String dirPath, List<String> files) throws IOException, TimeoutException;
  void waitTillAllFilesDisappear(String dirPath, List<String> files) throws IOException, TimeoutException;
}
```

**三种实现策略**：

**1. NoOpConsistencyGuard**（默认）：所有方法都是空实现，不做任何等待。适用于提供强一致性保证的存储系统（如 HDFS、本地文件系统，以及 2020 年 12 月之后的 AWS S3 -- S3 已经提供了 read-after-write 强一致性）。

```java
public class NoOpConsistencyGuard implements ConsistencyGuard {
  public void waitTillFileAppears(StoragePath filePath) { }  // 空实现
  public void waitTillFileDisappears(StoragePath filePath) { }  // 空实现
}
```

**2. FailSafeConsistencyGuard**：采用**指数退避重试**策略。写入文件后反复检查文件是否在存储上可见，每次等待时间翻倍，直到文件出现或达到最大重试次数：

```java
// 核心逻辑
long waitMs = consistencyGuardConfig.getInitialConsistencyCheckIntervalMs(); // 默认 400ms
int attempt = 0;
while (attempt < consistencyGuardConfig.getMaxConsistencyChecks()) { // 默认 6 次
    if (checkFileVisibility(filePath, visibility)) return;  // 文件可见，返回
    sleepSafe(waitMs);
    waitMs = waitMs * 2;  // 指数退避：400ms → 800ms → 1600ms → ...
    waitMs = Math.min(waitMs, consistencyGuardConfig.getMaxConsistencyCheckIntervalMs()); // 上限 20s
    attempt++;
}
throw new TimeoutException("Timed-out waiting for the file to " + visibility.name());
```

这意味着最长等待时间约为 400 + 800 + 1600 + 3200 + 6400 + 12800 = 25200ms（约 25 秒）。如果超时仍不可见，则抛出 `TimeoutException`，导致写入失败并触发 rollback。

**3. OptimisticConsistencyGuard**：一种更乐观的策略，继承自 `FailSafeConsistencyGuard` 但大幅简化了逻辑。对于 APPEAR 事件，只做一次检查 + 一次固定时间的 sleep（默认 500ms）；对于 DISAPPEAR 事件完全不等待（no-op）：

```java
public class OptimisticConsistencyGuard extends FailSafeConsistencyGuard {
  public void waitTillFileAppears(StoragePath filePath) {
    if (!checkFileVisibility(filePath, FileVisibility.APPEAR)) {
      Thread.sleep(consistencyGuardConfig.getOptimisticConsistencyGuardSleepTimeMs()); // 500ms
    }
  }
  public void waitTillFileDisappears(StoragePath filePath) { } // no-op
}
```

**为什么需要三种实现？** 这体现了 Hudi 在不同存储环境下的适配策略：

- 在强一致性存储上（HDFS、新版 S3），使用 `NoOpConsistencyGuard` 避免不必要的等待
- 在早期 S3（最终一致性）上，使用 `FailSafeConsistencyGuard` 确保写入的文件在读取前已可见
- `OptimisticConsistencyGuard` 是 `FailSafeConsistencyGuard` 的优化版本，基于"绝大多数情况下一致性延迟很短"的假设，减少等待时间提升吞吐

**重要提示**：`ConsistencyGuardConfig` 中的配置项 `hoodie.consistency.check.enabled` 自 0.7.0 起已标记为 `@Deprecated`，文档明确指出 "S3 is NOT eventually consistent anymore!"。这意味着在现代云存储环境下，ConsistencyGuard 机制更多是一种历史遗留的防御性设计，默认不启用。

**ConsistencyGuard 在 Hudi 中的使用场景**主要是在 `HoodieTable.finalizeWrite()` 中，确保写入的数据文件在标记为 committed 之前已经对所有读取者可见。具体来说，在 marker files 指向的数据文件与实际写入的文件之间可能存在不一致（进程崩溃导致 marker 存在但文件未写入），ConsistencyGuard 帮助检测并处理这种情况。

---

### 9.6 InstantGenerator 与 InstantFileNameGenerator — 时间戳生成与文件名编码

**源码位置**：
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieInstantTimeGenerator.java`
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/InstantGenerator.java`
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/versioning/v1/InstantGeneratorV1.java`
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/versioning/v2/InstantGeneratorV2.java`
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/versioning/v1/InstantFileNameGeneratorV1.java`
- `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/versioning/v2/InstantFileNameGeneratorV2.java`

#### 9.6.1 Instant Time 的生成策略

`HoodieInstantTimeGenerator` 负责生成全局唯一的 Instant 时间戳。时间戳格式为 `yyyyMMddHHmmssSSS`（17 位，毫秒精度），例如 `20260415143025123`。

```java
public static final String MILLIS_INSTANT_TIMESTAMP_FORMAT = "yyyyMMddHHmmssSSS";
public static final DateTimeFormatter MILLIS_INSTANT_TIME_FORMATTER =
    new DateTimeFormatterBuilder().appendPattern("yyyyMMddHHmmss")
        .appendValue(ChronoField.MILLI_OF_SECOND, 3).toFormatter();
```

生成新 Instant Time 的核心方法使用了 **CAS（Compare-And-Swap）机制**确保单调递增：

```java
private static final AtomicReference<String> LAST_INSTANT_TIME = new AtomicReference<>(...);

public static String createNewInstantTime(boolean shouldLock, TimeGenerator timeGenerator, long milliseconds) {
  return LAST_INSTANT_TIME.updateAndGet((oldVal) -> {
    String newCommitTime;
    do {
      Date d = new Date(timeGenerator.generateTime(!shouldLock) + milliseconds);
      newCommitTime = formatDateBasedOnTimeZone(d);
    } while (compareTimestamps(newCommitTime, LESSER_THAN_OR_EQUALS, oldVal));
    return newCommitTime;
  });
}
```

**关键设计点**：

1. **单调递增保证**：通过 `AtomicReference` 和 `updateAndGet` 实现 CAS，确保在同一 JVM 内不会生成重复或回退的时间戳。如果当前系统时间生成的时间戳 <= 上一个时间戳，会循环重试直到生成一个更大的值。

2. **时区支持**：支持 `LOCAL`（JVM 默认时区）和 `UTC` 两种时区模式，通过 `hoodie.timeline.timezone` 配置。UTC 模式避免了因时区变化（如夏令时调整）导致时间戳回退的问题。

3. **向后兼容**：早期版本使用秒级精度（14 位格式 `yyyyMMddHHmmss`），`fixInstantTimeCompatibility` 方法会自动将旧格式补齐为毫秒精度（追加 `999`），确保新旧格式可以正确比较。

#### 9.6.2 InstantGenerator — 从文件名到 HoodieInstant

`InstantGenerator` 接口定义了如何从存储文件创建 `HoodieInstant` 对象。V1 和 V2 的核心差异在于**文件名正则解析模式**：

**V1 的正则**（适用于 0.x）：
```java
// 格式: <timestamp>.<action>[.<state>]
Pattern.compile("^(\\d+)(\\.\\w+)(\\.\\D+)?$")
// 示例: 20260415143025123.commit        → COMPLETED
//       20260415143025123.commit.requested → REQUESTED
```

**V2 的正则**（适用于 1.x）：
```java
// 格式: <requestedTime>[_<completionTime>].<action>[.<state>]
Pattern.compile("^(\\d+(_\\d+)?)(\\.\\w+)(\\.\\D+)?$")
// 示例: 20260415143025123_20260415143030456.commit → COMPLETED (completionTime 在文件名中)
//       20260415143025123.commit.requested          → REQUESTED
```

V2 的 `createNewInstant(StoragePathInfo)` 方法从文件名中同时提取 requestedTime 和 completionTime：

```java
String[] timestamps = matcher.group(1).split(HoodieInstant.UNDERSCORE);
timestamp = timestamps[0];               // requestedTime
if (state == HoodieInstant.State.COMPLETED) {
  if (timestamps.length > 1) {
    completionTime = timestamps[1];       // completionTime 从文件名获取
  } else {
    // 向后兼容 0.x：从文件修改时间推断
    completionTime = HoodieInstantTimeGenerator.formatDate(new Date(pathInfo.getModificationTime()));
    isLegacy = true;
  }
}
```

#### 9.6.3 InstantFileNameGenerator — 从 HoodieInstant 到文件名

`InstantFileNameGenerator` 负责反向操作：根据 `HoodieInstant` 生成存储文件的文件名。

**V1 的 getFileName**（简单拼接）：

```java
// REQUESTED: 20260415143025123.commit.requested
// INFLIGHT:  20260415143025123.commit.inflight (或 20260415143025123.inflight 对于 commit)
// COMPLETED: 20260415143025123.commit
```

V1 中 `getFileName(String completionTime, HoodieInstant instant)` 方法直接忽略了 `completionTime` 参数，因为 0.x 不在文件名中编码完成时间。

**V2 的 getFileName**（编码 completionTime）：

```java
private String getCompleteFileName(HoodieInstant instant, String completionTime) {
  ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(completionTime));
  String timestampWithCompletionTime = instant.isLegacy()
      ? instant.requestedTime()
      : instant.requestedTime() + "_" + completionTime;
  switch (instant.getAction()) {
    case HoodieTimeline.COMMIT_ACTION:
    case HoodieTimeline.COMPACTION_ACTION:
      return makeCommitFileName(timestampWithCompletionTime);
    // ... 其他 action 类型
  }
}
```

V2 中已完成的 Instant 文件名格式为 `<requestedTime>_<completionTime>.<extension>`，如 `20260415143025123_20260415143030456.commit`。但对于 `isLegacy=true` 的旧格式 Instant，仍然只使用 requestedTime 以保持向后兼容。

**V1 vs V2 的另一个关键差异**是 Clustering 操作的处理。在 V1（0.x）中，Clustering 和 Replace Commit 共享同一个文件名后缀（`.replacecommit`），所以 `makeRequestedClusteringFileName` 直接委托给了 `makeRequestedReplaceFileName`。但在 V2（1.x）中，Clustering 有了独立的 action 名称和文件名后缀（`.clustering`），通过 `HoodieTimeline.REQUESTED_CLUSTERING_COMMIT_EXTENSION` 和 `HoodieTimeline.INFLIGHT_CLUSTERING_COMMIT_EXTENSION` 定义。

**为什么要把 completionTime 编码在文件名中？** 这是 Timeline V2 最重要的优化。好处有三：

1. **无需读取文件内容就能获取 completionTime**：在构建 Timeline 时，V1 需要读取每个 Instant 文件的内容来获取 completionTime，而 V2 只需要解析文件名。对于 S3 等对象存储，读取文件内容需要额外的 GET 请求，而 LIST 操作已经包含了文件名。这将 Timeline 加载的 IO 开销从 O(N) 次 GET 降低到 0 次额外 GET。

2. **支持基于 completionTime 的排序**：增量查询消费者可以按 completionTime 排序消费，彻底解决了 V1 中并发 Writer 场景下按 requestedTime 排序导致的数据丢失问题（详见 2.3 节的说明）。

3. **CompletionTimeQueryView 的高效实现**：V2 的 `CompletionTimeQueryViewV2` 可以直接从文件名构建 requestedTime -> completionTime 的映射，无需额外的文件读取或元数据缓存。

---

**文档版本**: 5.0（技术细节验证完成）
**创建日期**: 2026-04-14
**最后更新**: 2026-04-21
**基于 Hudi 版本**: v1.2.0-SNAPSHOT (master)
