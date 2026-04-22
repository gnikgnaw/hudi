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

### 解决什么问题

Hudi 的三层元数据架构解决了数据湖场景下的三个核心痛点:

1. **表属性的快速获取**: 在任何操作之前,需要快速知道表的基本信息(表类型、主键字段等),而不依赖于复杂的元数据加载
2. **操作历史的可追溯性**: 数据湖不像传统数据库有 WAL,需要一种机制记录所有变更历史,支持时间旅行、增量查询、故障恢复
3. **文件列表的高效获取**: 对象存储(S3/OSS)的 LIST 操作极慢(1000 文件需 200ms+),直接 LIST 会严重影响查询性能

**如果没有这个设计会有什么问题**:
- 每次查询都需要 LIST 整个表目录,S3 上大表的查询延迟会增加数秒甚至数十秒
- 无法实现增量查询和时间旅行,只能全表扫描
- 故障恢复困难,无法判断哪些操作是未完成的
- 并发写入冲突检测无法实现

**实际应用场景**:
- 实时数仓场景: DeltaStreamer 每分钟写入一次,查询引擎需要快速获取最新文件列表而不触发昂贵的 LIST
- 增量 ETL: 下游消费者按 Timeline 增量读取变更数据,避免全表扫描
- 故障恢复: Spark 任务失败后,通过 Timeline 检测 INFLIGHT 状态的 Instant 并自动 Rollback

### 有什么坑

1. **Metadata Table 未启用导致性能退化**: 默认 `hoodie.metadata.enable=true`,但如果手动关闭或升级时未初始化 Metadata Table,每次查询都会退化为文件系统 LIST,S3 上性能下降 10-100 倍
   - **排查方法**: 检查 `.hoodie/metadata/` 目录是否存在,查看 Spark UI 中是否有大量 S3 LIST 调用
   - **解决方案**: 运行 `HoodieMetadataTableValidator` 初始化 Metadata Table

2. **Timeline 文件过多导致初始化慢**: 如果 `hoodie.keep.max.commits` 设置过大(如 1000),每次 MetaClient 初始化都要 LIST 1000 个文件,S3 上可能需要 5-10 秒
   - **生产建议**: 保持默认值 30,最多不超过 50
   - **监控指标**: MetaClient 初始化耗时,如果超过 1 秒需要检查 Timeline 文件数量

3. **Archived Timeline 查询失败**: 时间旅行查询历史数据时,如果目标 Instant 已被归档,需要读取 `.hoodie/archived/` 下的 Avro 文件,但默认配置下归档文件可能被 Clean 删除
   - **配置建议**: 设置 `hoodie.cleaner.policy=KEEP_LATEST_FILE_VERSIONS` 并保留足够的归档文件

4. **V1 vs V2 混用导致 Timeline 损坏**: 0.x 升级到 1.x 后,如果同时有旧版本 Writer 写入,会产生 V1 格式的 Instant 文件,导致 completionTime 解析错误
   - **升级建议**: 升级时确保所有 Writer 同时升级,避免混合版本写入

### 核心概念解释

**Layer 1 - Table Properties (hoodie.properties)**:
- 不可变的表级配置,存储表类型(COW/MOR)、表名、主键字段、分区字段、表版本等
- 生命周期: 建表时创建,只能通过表升级(Upgrade/Downgrade)变更
- 读取时机: MetaClient 初始化时首先读取,无需 Timeline 即可获取

**Layer 2 - Timeline**:
- 有序的操作日志,每个操作对应一个 HoodieInstant(时间点)
- 分为 Active Timeline(最近 20-30 个)和 Archived Timeline(历史归档)
- 支持三种状态: REQUESTED(计划) → INFLIGHT(执行中) → COMPLETED(已完成)
- 是 Hudi 实现 ACID、时间旅行、增量查询的基础

**Layer 3 - Metadata Table**:
- 本身是一个 MOR 表,存储在 `.hoodie/metadata/` 下
- 包含 8 种分区类型: FILES(文件列表)、COLUMN_STATS(列统计)、BLOOM_FILTERS、RECORD_INDEX 等
- 与主表事务同步更新,保证一致性
- 使用 HFile 格式支持 O(log N) 的 key 查找

**三层之间的关系**:
```
hoodie.properties (Layer 1) → 告诉 MetaClient 表的基本信息和版本
    ↓
Timeline (Layer 2) → 记录所有操作历史,推导出文件视图
    ↓
Metadata Table (Layer 3) → 缓存文件列表和索引,加速查询
```

### 设计理念

**为什么分三层而不是一层?**

Hudi 的设计哲学是**分层解耦 + 渐进增强**:

1. **Layer 1 的设计权衡**: 表属性必须在任何操作之前就能读取,所以不能依赖 Timeline。使用简单的 Properties 文件而不是复杂的元数据存储,保证了初始化的极致性能(< 1ms)

2. **Layer 2 的设计权衡**: Timeline 采用"文件即日志"的设计,每个 Instant 对应一个文件。这种设计的好处是:
   - 原子性: 文件的创建/重命名在文件系统层面是原子的
   - 可见性: 文件系统的 LIST 操作天然提供了 Timeline 的有序视图
   - 简单性: 不需要额外的元数据存储(如数据库)
   - 缺点是文件数量多时 LIST 慢,所以引入了 Archival 机制

3. **Layer 3 的设计权衡**: Metadata Table 是后来引入的优化层,解决了对象存储 LIST 慢的问题。为什么用 Hudi 表而不是其他存储?
   - **自举(Bootstrapping)**: 利用 Hudi 自身的 MOR 能力实现增量更新,避免全量重写
   - **一致性**: 与主表共享同一套事务机制,无需额外的一致性协议
   - **可扩展**: 天然支持分区,可以存储任意类型的索引(列统计、Bloom Filter、Record Index)

**与业界其他方案的对比**:

| 方案 | 元数据存储 | 操作历史 | 文件索引 | 优缺点 |
|------|----------|---------|---------|--------|
| **Hudi** | 三层(Properties + Timeline + MT) | Timeline 有状态 | Metadata Table | 优: 支持故障恢复,增量更新成本低<br>缺: 架构复杂,学习曲线陡 |
| **Iceberg** | metadata.json 版本链 | Snapshot 链 | Manifest 文件 | 优: 架构简单,元数据自包含<br>缺: 无中间状态,故障恢复依赖外部机制 |
| **Delta Lake** | _delta_log/ JSON 文件 | Transaction Log | Log 内嵌文件列表 | 优: 简单易懂<br>缺: JSON 解析慢,大表性能差 |

**架构演进历史**:
- **0.5.x**: 只有 Layer 1 + Layer 2,文件列表通过 LIST 获取
- **0.7.0**: 引入 Metadata Table(Layer 3),但默认关闭
- **0.9.0**: Metadata Table 默认开启,只包含 FILES 分区
- **0.11.0**: 新增 COLUMN_STATS 和 BLOOM_FILTERS 分区
- **0.12.0**: 新增 RECORD_INDEX 分区,支持 O(1) 点查
- **1.0.0**: Timeline V2 发布,completionTime 编码在文件名中

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

### 解决什么问题

Timeline 解决了数据湖场景下的四个核心问题:

1. **ACID 事务的实现**: 传统数据库通过 WAL(Write-Ahead Log)实现事务,但数据湖基于对象存储,没有 WAL。Timeline 通过"文件即日志"的设计,将每个操作记录为一个 Instant 文件,实现了类似 WAL 的功能
2. **并发写入的冲突检测**: 多个 Writer 同时写入时,如何检测冲突?Timeline 通过 Instant 的原子创建(文件系统的原子性)实现了乐观并发控制(OCC)
3. **故障恢复**: Writer 崩溃后如何恢复?Timeline 记录了 INFLIGHT 状态的操作,重启后可以检测并 Rollback 或 Resume
4. **增量数据消费**: 下游如何高效获取变更数据?Timeline 提供了有序的操作历史,支持按时间范围增量读取

**如果没有 Timeline 会有什么问题**:
- 无法实现 ACID:写入一半失败后,无法判断哪些文件是有效的
- 并发写入会产生数据损坏:两个 Writer 同时写入同一个 FileGroup,互相覆盖
- 故障恢复困难:崩溃后无法判断哪些操作是未完成的
- 增量查询无法实现:只能全表扫描

**实际应用场景**:
- **实时数仓**: DeltaStreamer 每分钟写入一次,下游 Spark 任务按 Timeline 增量消费,避免重复处理
- **多 Writer 场景**: 多个 Flink 任务并发写入不同分区,通过 Timeline 的 OCC 机制检测冲突
- **故障恢复**: Spark 任务失败后,通过检测 INFLIGHT 的 Instant 自动 Rollback,保证数据一致性

### 有什么坑

1. **Timeline 文件过多导致初始化慢**: 如果 `hoodie.keep.max.commits` 设置过大(如 500),每次 MetaClient 初始化都要 LIST 500 个文件,S3 上可能需要 10-30 秒
   - **排查方法**: 查看 Spark UI 中 MetaClient 初始化耗时,检查 `.hoodie/timeline/` 下的文件数量
   - **解决方案**: 保持默认值 30,最多不超过 50。如果需要长期保留历史,依赖 Archived Timeline
   - **监控指标**: `MetaClient.loadActiveTimeline()` 耗时,如果超过 1 秒需要检查

2. **V1 vs V2 混用导致 Timeline 损坏**: 0.x 升级到 1.x 后,如果有旧版本 Writer 仍在写入,会产生 V1 格式的 Instant 文件(没有 completionTime 编码在文件名中),导致增量查询数据丢失
   - **现象**: 增量消费者发现数据缺失,或者 Timeline 加载时报错 "Invalid instant file name"
   - **解决方案**: 升级时确保所有 Writer 同时升级,避免混合版本写入。可以通过 `hoodie.table.version` 检查表版本
   - **预防措施**: 在升级前先停止所有 Writer,升级表版本后再启动新版本 Writer

3. **Archived Timeline 查询失败**: 时间旅行查询历史数据时,如果目标 Instant 已被归档到 `.hoodie/archived/`,但归档文件被 Clean 删除,会导致查询失败
   - **错误信息**: "Instant not found in active or archived timeline"
   - **配置建议**: 设置 `hoodie.cleaner.policy=KEEP_LATEST_FILE_VERSIONS` 并保留足够的归档文件,或者使用 Savepoint 保护关键时间点
   - **生产实践**: 对于需要长期保留的历史数据,使用 Savepoint 而不是依赖 Archived Timeline

4. **completionTime 与 requestedTime 的混淆**: 在 V2 中,Instant 有两个时间戳,很多用户不理解区别,导致增量查询逻辑错误
   - **常见错误**: 使用 `getTimestamp()` (返回 requestedTime)而不是 `getCompletionTime()` 来过滤增量数据
   - **正确做法**: 增量查询必须使用 completionTime 排序,否则在并发写入场景下会丢失数据(详见 2.3 节)

5. **INFLIGHT Instant 未清理导致表服务阻塞**: 如果 Writer 崩溃后没有正确 Rollback,INFLIGHT 的 Instant 会一直存在,阻塞后续的 Compaction/Clustering
   - **现象**: Compaction 一直处于 REQUESTED 状态,无法执行
   - **排查方法**: 检查 Timeline 中是否有长期存在的 INFLIGHT Instant
   - **解决方案**: 手动删除 INFLIGHT 文件或使用 `HoodieCleaner` 的 `--repair` 模式

### 核心概念解释

**Timeline 的本质**: Timeline 是一个**有序的、不可变的操作日志**,类似于数据库的 WAL(Write-Ahead Log),但实现方式完全不同:
- 数据库 WAL: 顺序写入的二进制日志文件,支持快速追加
- Hudi Timeline: 每个操作对应一个独立的文件,通过文件系统的原子性保证一致性

**Instant 的三态模型**:
```
REQUESTED (计划) → INFLIGHT (执行中) → COMPLETED (已完成)
     ↓                  ↓                    ↓
  .requested         .inflight           (无后缀)
```

- **REQUESTED**: 表服务(Compaction/Clustering)的计划阶段,记录了要处理的文件列表
- **INFLIGHT**: 操作正在执行,文件正在写入
- **COMPLETED**: 操作已完成,文件已提交

**Active Timeline vs Archived Timeline**:
- **Active Timeline**: 最近 20-30 个 Instant,存储为独立文件,高频访问
- **Archived Timeline**: 历史 Instant,压缩存储为 Avro 文件(V1)或 LSM 文件(V2),低频访问

**Timeline 的有序性保证**:
- V1: 按 requestedTime 排序(文件名中的时间戳)
- V2: 可按 completionTime 排序(文件名中编码的完成时间)

**Timeline 与 FileSystemView 的关系**:
- Timeline 是"操作历史"(What happened)
- FileSystemView 是"当前状态"(What files exist now)
- FileSystemView 通过重放 Timeline 推导出文件视图

### 设计理念

**为什么采用"文件即日志"的设计?**

Hudi 的 Timeline 设计与传统数据库的 WAL 有本质区别。传统 WAL 是顺序追加的二进制文件,而 Hudi 将每个操作记录为一个独立的文件。这种设计的权衡:

**优势**:
1. **原子性**: 文件系统的文件创建/重命名操作是原子的,天然提供了事务保证
2. **可见性**: 文件系统的 LIST 操作天然提供了 Timeline 的有序视图,无需额外的索引
3. **简单性**: 不需要额外的元数据存储(如数据库),降低了系统复杂度
4. **分布式友好**: 多个 Reader 可以并发读取不同的 Instant 文件,无需中心化的日志服务

**劣势**:
1. **文件数量多**: 每个操作一个文件,导致 `.hoodie/timeline/` 下文件数量快速增长
2. **LIST 性能**: 对象存储(S3)的 LIST 操作较慢,文件多时初始化慢
3. **无法原子批量操作**: 不能原子地创建多个 Instant(但 Hudi 也不需要这个能力)

**为什么引入 Archival 机制?**

Archival 是对"文件即日志"设计的补充优化。当 Active Timeline 中的 Instant 数量超过阈值时,将旧的 Instant 压缩存储到归档文件中,解决了文件数量过多的问题:
- V1: 归档为 Avro 文件(`.hoodie/archived/commits_*.avro`)
- V2: 归档为 LSM 文件(支持更高效的范围查询)

**为什么需要三态模型(REQUESTED/INFLIGHT/COMPLETED)?**

三态模型是 Hudi 实现故障恢复的关键:
- **REQUESTED**: 表服务的计划阶段,可以被取消或重新调度
- **INFLIGHT**: 操作正在执行,如果 Writer 崩溃,可以检测到并 Rollback
- **COMPLETED**: 操作已完成,数据对读取者可见

对比其他系统:
- **Iceberg**: 只有 COMPLETED 状态,没有中间状态。故障恢复依赖外部机制(如 Spark 的 task 重试)
- **Delta Lake**: 类似 Hudi,但使用 JSON 文件记录操作,解析性能较差

**V2 的设计动机 — 为什么要把 completionTime 编码在文件名中?**

这是 Timeline V2 最重要的改进,解决了 V1 的两个核心问题:
1. **性能问题**: V1 需要读取每个 Instant 文件的内容才能获取 completionTime,S3 上需要 N 次 GET 请求。V2 只需要 LIST 操作(返回文件名)即可获取,IO 开销降低到 0
2. **正确性问题**: V1 按 requestedTime 排序,在并发写入场景下会导致增量消费者丢失数据(详见 2.3 节的例子)。V2 按 completionTime 排序,保证了因果一致性

**架构演进历史**:
- **0.5.x**: Timeline V0,无 Archival,文件数量无限增长
- **0.6.0**: 引入 Archival 机制,解决文件数量问题
- **0.9.0**: Timeline V1 稳定,支持 REQUESTED/INFLIGHT/COMPLETED 三态
- **1.0.0**: Timeline V2 发布,completionTime 编码在文件名中
- **1.2.0**: Timeline V2 优化,支持 LSM 格式的归档文件

### 2.1 为什么 Timeline 是 Hudi 的核心?

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

### 解决什么问题

HoodieInstant 作为 Timeline 的基本单元,解决了以下问题:

1. **操作的唯一标识**: 每个 Instant 通过 `(requestedTime, action, state)` 三元组唯一标识一个操作,支持精确的操作追踪和查询
2. **操作状态的追踪**: 通过 State 枚举(REQUESTED/INFLIGHT/COMPLETED)追踪操作的生命周期,支持故障检测和恢复
3. **操作类型的区分**: 通过 Action 字段区分不同类型的操作(commit/compaction/clean等),支持针对性的处理逻辑
4. **时间旅行的基础**: 通过 requestedTime 和 completionTime 支持按时间点查询历史数据

**如果没有 HoodieInstant 会有什么问题**:
- 无法区分不同的操作:所有操作混在一起,无法单独处理
- 无法追踪操作状态:不知道哪些操作是未完成的,故障恢复困难
- 无法实现时间旅行:没有时间戳,无法查询历史数据
- 无法实现增量查询:无法按时间范围过滤操作

**实际应用场景**:
- **故障恢复**: 检测 INFLIGHT 状态的 Instant,判断是否需要 Rollback
- **增量查询**: 按 Instant 的时间范围过滤,获取变更数据
- **表服务调度**: 检测 REQUESTED 状态的 Compaction/Clustering Instant,执行计划
- **时间旅行**: 根据 Instant Time 重建历史时间点的文件视图

### 有什么坑

1. **equals() 不比较 completionTime**: `HoodieInstant.equals()` 只比较 `(state, action, requestedTime)`,不比较 `completionTime`。这意味着同一个操作在不同状态下(如 INFLIGHT vs COMPLETED)是不同的 Instant
   - **常见错误**: 使用 `instant1.equals(instant2)` 判断是否是同一个操作,但忽略了状态差异
   - **正确做法**: 如果要判断是否是同一个操作(忽略状态),应该比较 `requestedTime` 和 `action`

2. **Action 类型的版本差异**: 不同 Hudi 版本支持的 Action 类型不同,例如 `clustering` 在 0.x 中使用 `replacecommit`,在 1.x 中是独立的 action
   - **升级陷阱**: 0.x 升级到 1.x 后,旧的 `replacecommit` 可能被误识别为 Clustering
   - **解决方案**: 使用 `HoodieTimeline.getTimelineOfActions()` 而不是硬编码 action 名称

3. **Instant Time 的格式混淆**: Instant Time 有两种格式:14 位秒级精度(`yyyyMMddHHmmss`)和 17 位毫秒精度(`yyyyMMddHHmmssSSS`)
   - **常见错误**: 手动构造 Instant Time 时使用错误的格式,导致排序错误
   - **正确做法**: 始终使用 `HoodieInstantTimeGenerator.createNewInstantTime()` 生成

4. **completionTime 为 null 的情况**: REQUESTED 和 INFLIGHT 状态的 Instant,completionTime 为 null。直接调用 `getCompletionTime()` 会抛出 NPE
   - **安全做法**: 使用 `instant.isCompleted()` 判断后再获取 completionTime

5. **isLegacy 标记的误用**: `isLegacy=true` 表示这是 0.x 版本的 Instant(没有 completionTime 编码在文件名中)。很多代码需要特殊处理 legacy Instant
   - **常见错误**: 忽略 `isLegacy` 标记,导致解析 0.x 表时出错
   - **正确做法**: 在处理 completionTime 时检查 `isLegacy`,如果为 true 则从文件修改时间推断

### 核心概念解释

**HoodieInstant 的三元组标识**:
```
(requestedTime, action, state) → 唯一标识一个操作
```
- **requestedTime**: 操作请求的时间戳,全局唯一,单调递增
- **action**: 操作类型,如 commit/deltacommit/compaction/clean 等
- **state**: 操作状态,REQUESTED/INFLIGHT/COMPLETED/NIL

**State 的生命周期**:
```
REQUESTED → INFLIGHT → COMPLETED
   ↓           ↓           ↓
计划阶段    执行阶段    完成阶段
```
- **REQUESTED**: 只用于表服务(Compaction/Clustering/Clean),记录计划信息
- **INFLIGHT**: 操作正在执行,文件正在写入
- **COMPLETED**: 操作已完成,数据对读取者可见
- **NIL**: 无效状态,用于表示不存在的 Instant

**Action 类型的分类**:
1. **数据写入**: commit(COW), deltacommit(MOR)
2. **表服务**: compaction, logcompaction, clean, clustering
3. **元数据操作**: savepoint, restore, rollback
4. **索引操作**: indexing
5. **Schema 操作**: schemacommit

**requestedTime vs completionTime**:
- **requestedTime**: 操作开始的时间,用于唯一标识操作
- **completionTime**: 操作完成的时间,用于增量查询排序
- **为什么需要两个时间?** 在并发写入场景下,操作的开始顺序和完成顺序可能不同。按 completionTime 排序可以保证因果一致性

**Instant 与文件名的映射**:
- V1: `<requestedTime>.<action>[.<state>]`
  - 例如: `20260415143025123.commit` (COMPLETED)
  - 例如: `20260415143025123.commit.inflight` (INFLIGHT)
- V2: `<requestedTime>[_<completionTime>].<action>[.<state>]`
  - 例如: `20260415143025123_20260415143030456.commit` (COMPLETED)
  - 例如: `20260415143025123.commit.inflight` (INFLIGHT)

### 设计理念

**为什么使用三元组而不是单一 ID?**

Hudi 的 Instant 设计与传统数据库的事务 ID 有本质区别:
- **传统数据库**: 使用单一的事务 ID(如自增整数),所有信息存储在事务日志中
- **Hudi**: 使用 `(requestedTime, action, state)` 三元组,信息编码在文件名中

这种设计的好处:
1. **自描述**: 文件名本身就包含了操作的关键信息,无需读取文件内容
2. **分布式友好**: 不需要中心化的 ID 生成器,每个 Writer 可以独立生成 Instant Time
3. **可读性**: 文件名可读,方便调试和运维

**为什么 equals() 不比较 completionTime?**

这是一个深思熟虑的设计决策:
- **同一个操作的不同状态应该被视为不同的 Instant**: 例如 `T1.commit.inflight` 和 `T1.commit` 是同一个操作的不同阶段,但在 Timeline 中是两个不同的 Instant
- **completionTime 是派生属性**: completionTime 是操作完成后才确定的,不是操作的固有属性
- **向后兼容**: 0.x 版本的 Instant 没有 completionTime,如果 equals() 比较 completionTime 会导致兼容性问题

**为什么需要 isLegacy 标记?**

这是 Hudi 实现向后兼容的关键机制:
- **0.x 表**: Instant 文件名中没有 completionTime,需要从文件修改时间推断
- **1.x 表**: Instant 文件名中编码了 completionTime
- **混合场景**: 1.x 的 Hudi 需要能够读取 0.x 的表

`isLegacy` 标记让代码可以区分这两种情况,采用不同的处理逻辑。

**Action 类型的演进**:
- **0.5.x**: 只有 commit/clean/savepoint/restore
- **0.6.0**: 新增 compaction(MOR 表支持)
- **0.7.0**: 新增 rollback(显式回滚操作)
- **0.9.0**: 新增 replacecommit(Clustering/INSERT_OVERWRITE)
- **1.0.0**: 新增 clustering(从 replacecommit 中独立出来)
- **1.1.0**: 新增 indexing(索引构建)
- **1.2.0**: 新增 logcompaction(Log 文件压缩)

每个新 Action 的引入都是为了支持新的功能,同时保持向后兼容。

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

### 解决什么问题

FileSystemView 解决了从 Timeline 到查询的核心转换问题:

1. **文件快照的推导**: Timeline 记录的是操作历史,但查询需要知道"当前应该读哪些文件"。FileSystemView 从 Timeline 推导出文件快照视图
2. **FileSlice 的构建**: MOR 表的一个 FileGroup 包含 base file + 多个 log files,FileSystemView 负责将它们组装成 FileSlice
3. **时间旅行的支持**: 根据指定的 Instant Time,重建历史时间点的文件视图
4. **视图的缓存和共享**: 避免每个 Executor 都重复构建视图,通过 Timeline Server 共享视图

**如果没有 FileSystemView 会有什么问题**:
- 每次查询都需要重新扫描文件系统,性能极差
- 无法正确处理 MOR 表的 FileSlice(base + logs)
- 时间旅行查询无法实现
- 多 Executor 场景下重复构建视图,浪费资源

**实际应用场景**:
- **查询优化**: Spark 查询时,通过 FileSystemView 获取需要读取的文件列表,避免全表扫描
- **Compaction 调度**: 通过 FileSystemView 获取每个 FileSlice 的 log 文件数量,决定哪些需要 Compaction
- **Clean 操作**: 通过 FileSystemView 判断哪些旧版本文件可以安全删除
- **时间旅行**: 根据历史 Instant Time 重建文件视图,支持历史数据查询

### 有什么坑

1. **视图未刷新导致读取旧数据**: FileSystemView 是缓存的,如果不刷新,会读取到过期的文件列表
   - **常见场景**: Spark 长时间运行的任务,中途有新的 commit,但 FileSystemView 未刷新
   - **解决方案**: 定期调用 `reload()` 刷新视图,或者使用 `PriorityBasedFileSystemView` 自动刷新
   - **配置建议**: 设置 `hoodie.filesystem.view.remote.timeout.secs` 控制远程视图的超时时间

2. **内存溢出**: `HoodieTableFileSystemView` 将所有文件信息缓存在内存中,大表(百万级文件)会导致 OOM
   - **排查方法**: 查看 Spark Driver 的堆内存使用,检查 FileSystemView 占用的内存
   - **解决方案**: 使用 `SpillableMapBasedFileSystemView`(内存+磁盘)或 `RocksDbBasedFileSystemView`(持久化)
   - **配置建议**: 设置 `hoodie.filesystem.view.spillable.mem` 控制内存阈值

3. **Timeline Server 不可用导致查询失败**: 如果使用 `RemoteHoodieTableFileSystemView`,但 Timeline Server 未启动或崩溃,查询会失败
   - **错误信息**: "Failed to connect to Timeline Server"
   - **解决方案**: 使用 `PriorityBasedFileSystemView`,先查远程,失败降级本地
   - **生产建议**: 始终启用 `hoodie.embed.timeline.server=true`

4. **Pending Compaction 文件被误读**: 如果一个 FileSlice 有 pending compaction(REQUESTED 状态),其 log files 不应该被读取,但某些视图实现会误读
   - **现象**: 读取到重复数据或脏数据
   - **排查方法**: 检查 Timeline 中是否有 REQUESTED 状态的 compaction Instant
   - **解决方案**: 使用 `getLatestMergedFileSlicesBeforeOrOn()` 而不是 `getLatestFileSlices()`

5. **Replaced FileGroup 未过滤**: Clustering 或 INSERT_OVERWRITE 操作会替换旧的 FileGroup,但某些视图实现未正确过滤被替换的文件
   - **现象**: 读取到已被替换的旧文件,导致数据重复
   - **排查方法**: 检查 `HoodieReplaceCommitMetadata.partitionToReplaceFileIds`
   - **解决方案**: 确保使用最新版本的 Hudi,旧版本可能有 bug

### 核心概念解释

**FileSystemView 的本质**: FileSystemView 是 Timeline 的"投影",将操作历史转换为文件快照:
```
Timeline (操作历史) → FileSystemView (文件快照)
  T1.commit: 写入 fg1_v1, fg2_v1
  T2.deltacommit: 追加 fg1.log.1
  T3.compaction: fg1_v1 + log → fg1_v2
  ↓
FileSystemView:
  fg1 → FileSlice(base=fg1_v2, logs=[])
  fg2 → FileSlice(base=fg2_v1, logs=[])
```

**核心数据结构**:
- **HoodieFileGroup**: 一个 fileId 对应的所有版本文件,按 Instant Time 排序
- **FileSlice**: 某个时间点的文件切片,包含 base file + log files
- **HoodieBaseFile**: Parquet/ORC/HFile 格式的 base file
- **HoodieLogFile**: Avro 格式的 log file

**视图的层次结构**:
```
TableFileSystemView (接口)
  ├── BaseFileOnlyView — 只看 Base Files(COW 表)
  └── SliceView — 看完整 FileSlice(MOR 表)

实现类:
  ├── HoodieTableFileSystemView — 内存缓存
  ├── SpillableMapBasedFileSystemView — 内存+磁盘
  ├── RocksDbBasedFileSystemView — RocksDB 持久化
  ├── RemoteHoodieTableFileSystemView — 远程调用 Timeline Server
  └── PriorityBasedFileSystemView — 组合视图(远程优先,本地降级)
```

**视图的刷新机制**:
- **同步刷新**: 调用 `reload()` 立即从存储重新加载 Timeline 和文件列表
- **增量刷新**: `IncrementalTimelineSyncFileSystemView` 只加载新增的 Instant,避免全量重建
- **远程刷新**: `RemoteHoodieTableFileSystemView` 从 Timeline Server 获取最新视图

**Pending Compaction 的处理**:
- 如果一个 FileSlice 有 pending compaction(REQUESTED 状态),其 log files 正在被 compaction 任务处理
- 查询时应该跳过这些 log files,只读取 base file
- `getLatestMergedFileSlicesBeforeOrOn()` 会自动过滤 pending compaction 的 FileSlice

### 设计理念

**为什么需要多种 FileSystemView 实现?**

不同场景对视图的需求不同:
- **小表(< 10万文件)**: 使用 `HoodieTableFileSystemView`,全内存缓存,性能最优
- **大表(10万-100万文件)**: 使用 `SpillableMapBasedFileSystemView`,内存不足时溢写磁盘
- **超大表(> 100万文件)**: 使用 `RocksDbBasedFileSystemView`,持久化到 RocksDB,重启不丢失
- **多 Executor 场景**: 使用 `RemoteHoodieTableFileSystemView`,调用 Timeline Server,避免重复构建
- **生产环境**: 使用 `PriorityBasedFileSystemView`,远程优先,本地降级,兼顾性能和可靠性

**为什么 FileSystemView 需要缓存?**

构建 FileSystemView 的成本很高:
1. **加载 Timeline**: 需要 LIST `.hoodie/timeline/` 下的所有文件
2. **获取文件列表**: 需要从 Metadata Table 或文件系统获取所有数据文件
3. **构建 FileGroup 映射**: 需要按 fileId 分组,按 Instant Time 排序
4. **关联 base + logs**: 需要将 base file 和 log files 关联成 FileSlice

在 S3 上,这个过程可能需要数秒甚至数十秒。缓存可以将后续访问的延迟降低到毫秒级。

**为什么需要 Timeline Server?**

在 Spark 等分布式引擎中,每个 Executor 都需要 FileSystemView:
- **没有 Timeline Server**: 每个 Executor 都构建一次视图,N 个 Executor 就是 N 次重复构建
- **有 Timeline Server**: Driver 构建一次视图,Executor 通过 HTTP API 获取,1 次构建 + N 次网络调用

Timeline Server 的好处:
1. **减少重复构建**: 避免每个 Executor 都加载 Timeline 和文件列表
2. **减少存储压力**: 避免 N 个 Executor 同时 LIST 文件系统
3. **统一视图**: 所有 Executor 看到的是同一个视图,避免不一致

**为什么需要增量刷新?**

全量刷新的成本很高,尤其是大表。增量刷新只加载新增的 Instant,大幅降低刷新成本:
```
全量刷新:
  1. 重新 LIST Timeline → O(N) 个文件
  2. 重新构建 FileGroup 映射 → O(N) 个文件

增量刷新:
  1. 只 LIST 新增的 Instant → O(M) 个文件(M << N)
  2. 只更新变更的 FileGroup → O(M) 个文件
```

`IncrementalTimelineSyncFileSystemView` 维护了一个 `lastInstantTime`,每次刷新只处理比它新的 Instant。

**与 Iceberg 的对比**:

| 维度 | Hudi FileSystemView | Iceberg Manifest |
|------|---------------------|------------------|
| **构建方式** | 从 Timeline 推导 | 从 metadata.json 读取 |
| **缓存位置** | 内存/RocksDB | 无缓存(每次读 manifest) |
| **刷新成本** | 增量刷新,成本低 | 全量读取,成本高 |
| **共享机制** | Timeline Server | 无(每个 Executor 独立读取) |

Hudi 的 FileSystemView 设计更复杂,但在大表和多 Executor 场景下性能更优。

### 4.1 为什么需要 FileSystemView?

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

### 解决什么问题

Metadata Table 解决了对象存储上的性能瓶颈问题:

1. **LIST 操作的性能问题**: 对象存储(S3/OSS/COS)的 LIST 操作极慢,1000 个文件需要 200ms+,大表的 LIST 可能需要数秒甚至数十秒
2. **文件列表的快速获取**: 查询需要快速知道某个分区下有哪些文件,直接 LIST 会严重影响查询性能
3. **列统计的存储**: 支持 Data Skipping,需要存储每个文件的列级别统计信息(min/max/null_count)
4. **索引的存储**: 支持 Bloom Filter、Record Index、二级索引等,需要高效的 key-value 存储

**如果没有 Metadata Table 会有什么问题**:
- 每次查询都需要 LIST 整个表目录,S3 上大表的查询延迟增加数秒甚至数十秒
- 无法实现 Data Skipping,查询需要扫描所有文件
- 无法实现 O(1) 的点查(Record Index)
- 并发查询会产生大量的 LIST 请求,触发 S3 限流

**实际应用场景**:
- **实时查询**: BI 工具查询 Hudi 表,通过 Metadata Table 快速获取文件列表,查询延迟从 10 秒降低到 1 秒
- **Data Skipping**: 查询带 WHERE 条件时,通过 Column Stats 跳过不符合条件的文件,扫描量减少 90%+
- **Upsert 优化**: 通过 Record Index 快速定位记录所在的文件,避免全表扫描
- **二级索引**: 对非主键字段建立索引,支持高效的点查和范围查询

### 有什么坑

1. **Metadata Table 未启用导致性能退化**: 虽然默认 `hoodie.metadata.enable=true`,但如果手动关闭或升级时未初始化 Metadata Table,每次查询都会退化为文件系统 LIST
   - **排查方法**: 检查 `.hoodie/metadata/` 目录是否存在,查看 Spark UI 中是否有大量 S3 LIST 调用
   - **解决方案**: 运行 `HoodieMetadataTableValidator` 初始化 Metadata Table
   - **监控指标**: 查询中的 LIST 调用次数,如果 > 0 说明未使用 Metadata Table

2. **Metadata Table 与主表不一致**: 如果主表 commit 成功但 Metadata Table 更新失败,会导致读取到过期的文件列表
   - **现象**: 查询结果缺少最新写入的数据,或者读取到已删除的文件
   - **排查方法**: 比较主表和 Metadata Table 的最新 Instant Time,检查是否一致
   - **解决方案**: 运行 `HoodieMetadataTableValidator --repair` 修复不一致
   - **预防措施**: 确保 Writer 有足够的权限写入 `.hoodie/metadata/` 目录

3. **Column Stats 索引未启用**: 默认只启用 FILES 分区,Column Stats 需要手动启用。如果未启用,Data Skipping 不生效
   - **配置**: 设置 `hoodie.metadata.index.column.stats.enable=true`
   - **注意**: 启用后需要重新构建索引,可能需要数小时(取决于表大小)
   - **监控**: 查看查询计划中是否有 "Data Skipping" 信息

4. **Metadata Table Compaction 不及时**: Metadata Table 是 MOR 表,如果 Compaction 不及时,log files 过多会导致查询变慢
   - **现象**: Metadata Table 查询延迟逐渐增加,从 10ms 增加到 100ms+
   - **排查方法**: 检查 `.hoodie/metadata/files/` 下的 log 文件数量
   - **解决方案**: 降低 `hoodie.metadata.compact.max.delta.commits`(默认 10),更频繁地 Compaction
   - **生产建议**: 设置为 5-10,确保 log files 不超过 10 个

5. **HFile Block Cache 未启用**: Metadata Table 使用 HFile 格式,如果 Block Cache 未启用,每次查询都需要读取磁盘
   - **配置**: 设置 `hoodie.metadata.hfile.block.cache.enabled=true`
   - **内存配置**: 设置 `hoodie.metadata.hfile.block.cache.size` 控制缓存大小(默认 100MB)
   - **效果**: 启用后,热点数据的查询延迟从 50ms 降低到 5ms

6. **Record Index 的写放大**: Record Index 记录每个记录的位置,对于高频更新的表,写放大严重
   - **现象**: 写入吞吐量下降 50%+,Metadata Table 的大小快速增长
   - **适用场景**: 只在需要 O(1) 点查的场景下启用,不要默认启用
   - **替代方案**: 对于范围查询,使用 Column Stats 而不是 Record Index

### 核心概念解释

**Metadata Table 的本质**: Metadata Table 本身是一个 MOR 表,存储在 `.hoodie/metadata/` 下,与主表共享同一套 Timeline 机制:
```
主表:
  basePath/
    ├── .hoodie/
    │   └── timeline/
    └── data/

Metadata Table:
  basePath/.hoodie/metadata/
    ├── .hoodie/
    │   └── timeline/  ← Metadata Table 自己的 Timeline
    └── files/         ← FILES 分区
    └── column_stats/  ← COLUMN_STATS 分区
    └── ...
```

**8 种分区类型**:
1. **FILES**: 存储每个分区的文件列表,替代文件系统 LIST
2. **COLUMN_STATS**: 存储每个文件的列级别统计(min/max/null_count),支持 Data Skipping
3. **BLOOM_FILTERS**: 存储每个文件的 Bloom Filter,支持快速判断记录是否存在
4. **RECORD_INDEX**: 存储每个记录的位置(fileId + position),支持 O(1) 点查
5. **PARTITION_STATS**: 存储每个分区的统计信息(文件数量、记录数量、大小)
6. **SECONDARY_INDEX**: 用户自定义的二级索引,支持非主键字段的快速查询
7. **EXPRESSION_INDEX**: 基于表达式的索引,支持复杂查询条件
8. **ALL_PARTITIONS**: 存储所有分区的列表,与 FILES 共享同一物理分区

**HFile 格式的选择**:
- Metadata Table 使用 HFile 而不是 Parquet,因为 HFile 是 key-value 存储,支持 O(log N) 的点查
- HFile 内部按 key 有序存储,并维护了 Block Index,支持高效的二分查找
- HFile 内嵌了 Bloom Filter,可以快速判断 key 是否存在

**同步更新机制**:
- Metadata Table 的更新发生在主表 `postCommit()` 阶段,是主表 commit 流程的一部分
- 主表 commit 成功后立即更新 Metadata Table,保证一致性
- 如果 Metadata Table 更新失败,下次写入时会自动修复

**事务一致性保证**:
- 主表和 Metadata Table 共享同一个 Instant Time
- Metadata Table 的更新是幂等的,重试不会产生重复数据
- 每次更新前会回滚 Metadata Table 上的 INFLIGHT 写入,确保状态干净

### 设计理念

**为什么用 Hudi MOR 表而不是其他存储?**

Metadata Table 的设计哲学是**自举(Bootstrapping)**,利用 Hudi 自身的能力来存储元数据:

**优势**:
1. **增量更新**: 利用 MOR 的 log files 实现增量更新,避免全量重写文件列表
2. **事务保证**: 与主表共享同一套事务机制,无需额外的一致性协议
3. **Compaction 控制**: 利用 Hudi 自身的 Compaction 控制元数据文件大小
4. **可扩展**: 天然支持分区,可以存储任意类型的索引

**劣势**:
1. **复杂性**: Metadata Table 本身也是一个表,增加了系统复杂度
2. **依赖性**: Metadata Table 的正确性依赖于主表的正确性
3. **调试困难**: Metadata Table 损坏时,需要理解 Hudi 的内部机制才能修复

**为什么选择 HFile 而不是 Parquet?**

这是 Metadata Table 最关键的设计决策:

| 维度 | HFile | Parquet |
|------|-------|---------|
| **数据模型** | Key-Value(排序) | 列式存储 |
| **点查能力** | O(log N) 二分查找 | 不支持,需全文件扫描 |
| **前缀查找** | 原生支持 | 不支持 |
| **Bloom Filter** | 内置 | 需额外存储 |
| **写入模式** | 追加写入,键有序 | 批量写入,列式编码 |

Metadata Table 的核心使用场景是**按 key 查找**,HFile 的 key-value 模型完美匹配这个需求。

**为什么需要 8 种分区类型?**

不同的索引类型有不同的数据模型和访问模式:
- **FILES**: key 是 partitionPath,value 是文件列表(Map)
- **COLUMN_STATS**: key 是 `hash(column) + hash(partition) + hash(file)`,value 是统计信息
- **RECORD_INDEX**: key 是记录的主键,value 是文件位置

将它们分开存储,可以:
1. **独立启用**: 用户可以只启用需要的索引,避免不必要的存储开销
2. **独立优化**: 不同索引可以使用不同的 Compaction 策略
3. **独立扩展**: 新增索引类型不影响已有索引

**为什么 Metadata Table 的 Compaction 更频繁?**

Metadata Table 的 Compaction 间隔默认是 10 个 delta commits,比主表更频繁(主表默认 5-10 个)。原因:
1. **查询性能**: Metadata Table 的查询延迟直接影响主表的查询性能,必须保持低延迟
2. **文件大小**: Metadata Table 的文件较小(通常 < 1GB),Compaction 成本低
3. **更新频率**: Metadata Table 每次主表 commit 都会更新,log files 增长快

**与 Iceberg 的对比**:

| 维度 | Hudi Metadata Table | Iceberg Manifest |
|------|---------------------|------------------|
| **存储格式** | HFile(key-value) | Avro(列表) |
| **更新方式** | 增量更新(log files) | 追加新 manifest |
| **查询方式** | O(log N) 点查 | O(N) 扫描 |
| **索引类型** | 8 种(FILES/COLUMN_STATS/...) | 1 种(文件列表) |
| **一致性** | 与主表同步更新 | 独立更新 |

Hudi 的 Metadata Table 更复杂,但功能更强大,支持更多类型的索引。

**架构演进历史**:
- **0.5.x**: 无 Metadata Table,直接 LIST 文件系统
- **0.7.0**: 引入 Metadata Table,只包含 FILES 分区,默认关闭
- **0.9.0**: Metadata Table 默认开启,性能优化
- **0.11.0**: 新增 COLUMN_STATS 和 BLOOM_FILTERS 分区
- **0.12.0**: 新增 RECORD_INDEX 分区,支持 O(1) 点查
- **1.0.0**: 新增 SECONDARY_INDEX 和 EXPRESSION_INDEX,支持用户自定义索引
- **1.2.0**: 新增 PARTITION_STATS,优化分区裁剪

### 5.1 为什么 Metadata Table 用 MOR 表实现?

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

### 解决什么问题

HoodieTableMetaClient 作为元数据的统一入口,解决了以下问题:

1. **元数据的统一访问**: 提供了访问表属性、Timeline、FileSystemView 的统一接口,避免直接操作文件系统
2. **版本兼容性**: 自动识别表版本(V1/V2),加载对应的 Timeline 实现,支持向后兼容
3. **懒加载优化**: Timeline 等重量级对象采用懒加载,避免不必要的初始化开销
4. **缓存管理**: 缓存 Timeline 和 FileSystemView,避免重复加载

**如果没有 MetaClient 会有什么问题**:
- 每个组件都需要直接操作文件系统,代码重复且容易出错
- 版本兼容性难以维护,需要在每个地方判断表版本
- 无法统一管理缓存,导致重复加载和内存浪费
- 难以实现懒加载,初始化性能差

**实际应用场景**:
- **查询引擎集成**: Spark/Flink 通过 MetaClient 获取表信息和 Timeline,无需关心底层实现
- **表服务**: Compaction/Clean/Clustering 通过 MetaClient 获取 Timeline 和 FileSystemView
- **工具类**: HoodieCLI、DeltaStreamer 通过 MetaClient 访问表元数据
- **多版本兼容**: 1.x 的 Hudi 通过 MetaClient 读取 0.x 的表

### 有什么坑

1. **Timeline 懒加载导致的延迟**: 如果 `loadActiveTimelineOnLoad=false`,首次访问 Timeline 时会触发加载,可能需要数秒
   - **现象**: 第一次查询很慢,后续查询正常
   - **排查方法**: 查看日志中 "Loading active timeline" 的耗时
   - **解决方案**: 对于需要立即访问 Timeline 的场景,设置 `loadActiveTimelineOnLoad=true`
   - **权衡**: 立即加载会增加 MetaClient 初始化时间,但避免首次访问延迟

2. **MetaClient 未刷新导致读取旧数据**: MetaClient 缓存了 Timeline,如果不刷新,会读取到过期的 Instant
   - **常见场景**: 长时间运行的 Spark 任务,中途有新的 commit,但 MetaClient 未刷新
   - **解决方案**: 定期调用 `reloadActiveTimeline()` 刷新 Timeline
   - **自动刷新**: 使用 `FileSystemViewManager` 的自动刷新机制

3. **表版本识别错误**: 如果 `hoodie.properties` 中的 `hoodie.table.version` 配置错误,会导致加载错误的 Timeline 实现
   - **现象**: Timeline 加载失败,或者 Instant 解析错误
   - **排查方法**: 检查 `hoodie.properties` 中的 `hoodie.table.version`,确认是否与实际表版本一致
   - **解决方案**: 手动修正 `hoodie.table.version`,或者运行表升级工具

4. **并发访问导致的竞态条件**: 多个线程同时访问 MetaClient,可能导致 Timeline 重复加载或缓存不一致
   - **现象**: 偶发的 NPE 或 ConcurrentModificationException
   - **解决方案**: 确保 MetaClient 的访问是线程安全的,或者每个线程使用独立的 MetaClient 实例
   - **注意**: MetaClient 本身不是线程安全的,需要外部同步

5. **Storage 对象未正确关闭**: MetaClient 持有 Storage 对象(文件系统连接),如果未正确关闭,会导致资源泄漏
   - **现象**: 文件句柄泄漏,最终导致 "Too many open files" 错误
   - **解决方案**: 使用 try-with-resources 或手动调用 `close()` 关闭 MetaClient
   - **注意**: MetaClient 实现了 `AutoCloseable` 接口

### 核心概念解释

**MetaClient 的职责**: MetaClient 是 Hudi 表元数据的统一入口,负责:
1. **加载表配置**: 读取 `hoodie.properties`,获取表类型、主键字段等配置
2. **识别表版本**: 根据 `hoodie.table.version` 识别表版本,加载对应的 Timeline 实现
3. **管理 Timeline**: 加载和缓存 Active Timeline,提供刷新接口
4. **提供路径信息**: 提供 basePath、metaPath、timelinePath 等路径信息

**核心字段**:
```java
protected StoragePath basePath;           // 表根路径
protected StoragePath metaPath;           // .hoodie/ 路径
private HoodieTableType tableType;        // COW/MOR
protected HoodieTableConfig tableConfig;  // hoodie.properties
private TimelineLayoutVersion timelineLayoutVersion; // 表版本
private TimelineLayout timelineLayout;    // Timeline 布局策略
protected HoodieActiveTimeline activeTimeline; // Active Timeline
```

**懒加载机制**:
- **Timeline**: 默认懒加载,首次访问时才加载
- **FileSystemView**: 不由 MetaClient 管理,由 `FileSystemViewManager` 管理
- **IndexMetadata**: 懒加载,只在需要时读取 `.hoodie/.index_defs/index.json`

**版本识别流程**:
```
1. 读取 hoodie.properties
2. 获取 hoodie.table.version(默认为 7,表示 V2)
3. 根据版本号创建 TimelineLayout(V1/V2)
4. 根据 TimelineLayout 创建对应的 ActiveTimeline
```

**Timeline 刷新机制**:
- `reloadActiveTimeline()`: 重新加载 Active Timeline,丢弃旧缓存
- `getActiveTimeline().reload()`: 增量刷新,只加载新增的 Instant
- 区别: 前者重新创建 Timeline 对象,后者只刷新 Instant 列表

**MetaClient 的生命周期**:
```
创建 → 使用 → 刷新 → 关闭
  ↓      ↓      ↓      ↓
new   getXxx  reload  close
```

### 设计理念

**为什么需要 MetaClient 这一层抽象?**

MetaClient 是 Hudi 实现**关注点分离**的关键:
- **表配置**: 由 `HoodieTableConfig` 管理
- **Timeline**: 由 `HoodieActiveTimeline` 管理
- **FileSystemView**: 由 `FileSystemViewManager` 管理
- **MetaClient**: 作为统一入口,协调这些组件

这种设计的好处:
1. **单一职责**: 每个组件只负责一个方面,代码清晰
2. **可测试性**: 可以单独测试每个组件,无需依赖整个系统
3. **可扩展性**: 新增功能只需扩展对应的组件,不影响其他部分

**为什么 Timeline 采用懒加载?**

Timeline 加载涉及文件系统 LIST 操作,在 S3 上可能需要数秒。懒加载的好处:
1. **快速初始化**: 只读取 `hoodie.properties`,无需加载 Timeline
2. **按需加载**: 只在需要时加载 Timeline,避免不必要的开销
3. **灵活性**: 某些操作(如获取表类型)不需要 Timeline,懒加载避免浪费

**为什么需要版本化的 TimelineLayout?**

Hudi 需要同时支持 0.x(V1)和 1.x(V2)两种表格式,Timeline 的文件命名、排序逻辑、序列化方式都有本质差异。TimelineLayout 采用**策略模式**,将版本相关的逻辑封装在不同的实现中:
- `TimelineLayoutV1`: 0.x 表,Instant 文件名不包含 completionTime
- `TimelineLayoutV2`: 1.x 表,Instant 文件名包含 completionTime

MetaClient 根据表版本自动选择对应的 TimelineLayout,上层代码无需关心版本差异。

**为什么 MetaClient 不是线程安全的?**

这是一个深思熟虑的设计决策:
1. **性能考虑**: 线程安全需要加锁,会降低性能
2. **使用模式**: 大多数场景下,每个线程使用独立的 MetaClient 实例
3. **简化设计**: 不需要考虑并发控制,代码更简单

如果需要多线程共享 MetaClient,应该在外部加锁或使用 `ThreadLocal`。

**与 Iceberg 的对比**:

| 维度 | Hudi MetaClient | Iceberg TableMetadata |
|------|-----------------|----------------------|
| **职责** | 统一入口,协调多个组件 | 直接管理元数据 |
| **Timeline** | 独立的 ActiveTimeline 对象 | 内嵌在 TableMetadata 中 |
| **版本兼容** | 通过 TimelineLayout 策略 | 通过版本号判断 |
| **懒加载** | 支持 Timeline 懒加载 | 不支持,立即加载 |
| **线程安全** | 不保证 | 不保证 |

Hudi 的 MetaClient 设计更复杂,但提供了更好的灵活性和扩展性。

**架构演进历史**:
- **0.5.x**: MetaClient 直接管理所有元数据,职责过重
- **0.7.0**: 引入 TimelineLayout,支持版本化
- **0.9.0**: Timeline 懒加载,优化初始化性能
- **1.0.0**: 引入 TimelineFactory,进一步解耦
- **1.2.0**: 支持 IndexMetadata,统一索引管理

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

### 解决什么问题

Timeline Server 解决了分布式查询引擎中的元数据重复加载问题:

1. **避免重复构建 FileSystemView**: 在 Spark 等分布式引擎中,每个 Executor 都需要 FileSystemView。没有 Timeline Server,每个 Executor 都要重复构建,浪费资源
2. **减少存储压力**: 避免 N 个 Executor 同时 LIST 文件系统,减少对 S3 等对象存储的压力
3. **统一视图**: 所有 Executor 看到的是同一个视图,避免不一致
4. **降低延迟**: Executor 通过 HTTP API 获取视图,比本地构建快 10-100 倍

**如果没有 Timeline Server 会有什么问题**:
- 每个 Executor 都要加载 Timeline 和构建 FileSystemView,N 个 Executor 就是 N 次重复构建
- N 个 Executor 同时 LIST 文件系统,可能触发 S3 限流
- 每个 Executor 都缓存 FileSystemView,内存浪费严重
- Executor 之间的视图可能不一致,导致查询结果错误

**实际应用场景**:
- **Spark 查询**: Driver 启动 Timeline Server,Executor 通过 HTTP API 获取文件列表,避免重复构建
- **Flink 流式写入**: TaskManager 通过 Timeline Server 获取最新的 FileSystemView,避免每个 Task 都加载 Timeline
- **多 Writer 场景**: 多个 Writer 共享同一个 Timeline Server,减少元数据加载开销
- **大表查询**: 大表的 FileSystemView 构建可能需要数十秒,Timeline Server 可以将延迟降低到毫秒级

### 有什么坑

1. **Timeline Server 端口冲突**: 如果多个 Spark 任务在同一台机器上运行,可能出现端口冲突
   - **错误信息**: "Address already in use"
   - **解决方案**: 设置 `hoodie.embed.timeline.server.port=0`,让系统自动分配端口
   - **注意**: 自动分配端口后,需要通过日志或 Spark UI 查看实际端口号

2. **Timeline Server 未启动导致查询失败**: 如果配置了 `RemoteHoodieTableFileSystemView`,但 Timeline Server 未启动,查询会失败
   - **错误信息**: "Failed to connect to Timeline Server"
   - **排查方法**: 检查 Driver 日志中是否有 "Timeline Server started" 信息
   - **解决方案**: 确保 `hoodie.embed.timeline.server=true`,或者使用 `PriorityBasedFileSystemView` 降级到本地

3. **Timeline Server 内存溢出**: Timeline Server 缓存了 FileSystemView,大表可能导致 Driver OOM
   - **现象**: Driver 内存使用持续增长,最终 OOM
   - **排查方法**: 查看 Driver 堆内存使用,检查 FileSystemView 占用的内存
   - **解决方案**: 增加 Driver 内存,或者使用 `SpillableMapBasedFileSystemView` 溢写到磁盘
   - **配置建议**: 设置 `hoodie.filesystem.view.spillable.mem` 控制内存阈值

4. **Timeline Server 响应慢**: 如果 Timeline Server 的线程池不足,高并发查询时会出现响应慢
   - **现象**: Executor 等待 Timeline Server 响应,查询延迟增加
   - **排查方法**: 查看 Timeline Server 的线程池使用情况
   - **解决方案**: 增加 `hoodie.embed.timeline.server.threads`(默认 32)
   - **生产建议**: 根据 Executor 数量调整,建议设置为 Executor 数量的 2-3 倍

5. **Timeline Server 未刷新导致读取旧数据**: Timeline Server 缓存了 FileSystemView,如果不刷新,Executor 会读取到过期的文件列表
   - **现象**: 查询结果缺少最新写入的数据
   - **解决方案**: 设置 `hoodie.filesystem.view.remote.timeout.secs` 控制缓存超时时间
   - **自动刷新**: Timeline Server 会定期刷新 FileSystemView,但可能有延迟

6. **Timeline Server 与 Executor 的网络问题**: 如果 Executor 与 Driver 之间的网络不稳定,Timeline Server 调用可能失败
   - **错误信息**: "Connection timeout" 或 "Connection refused"
   - **解决方案**: 使用 `PriorityBasedFileSystemView`,失败时降级到本地构建
   - **生产建议**: 始终使用 `PriorityBasedFileSystemView`,而不是纯 `RemoteHoodieTableFileSystemView`

### 核心概念解释

**Timeline Server 的本质**: Timeline Server 是一个嵌入在 Driver 中的 HTTP 服务,基于 Javalin 框架实现,提供 RESTful API 供 Executor 访问:
```
Driver:
  ├── Spark Application
  ├── Timeline Server (Javalin HTTP Server)
  │   ├── FileSystemView Cache
  │   └── RESTful API
  └── Executor 1~N 通过 HTTP 调用
```

**核心 API**:
1. **GET /v1/hoodie/view/latest-file-slices/{partition}**: 获取某个分区的最新 FileSlice
2. **GET /v1/hoodie/view/latest-file-slices-before-or-on/{partition}/{maxInstantTime}**: 获取某个时间点之前的 FileSlice(时间旅行)
3. **GET /v1/hoodie/view/all-file-groups/{partition}**: 获取某个分区的所有 FileGroup
4. **POST /v1/hoodie/view/refresh**: 刷新 FileSystemView

**工作流程**:
```
1. Driver 启动时,创建 Timeline Server
2. Driver 构建 FileSystemView,缓存在 Timeline Server 中
3. Executor 需要文件列表时,调用 Timeline Server API
4. Timeline Server 返回缓存的 FileSystemView
5. 主表有新 commit 时,Timeline Server 自动刷新 FileSystemView
```

**与 RemoteHoodieTableFileSystemView 的关系**:
- `RemoteHoodieTableFileSystemView` 是客户端,负责调用 Timeline Server API
- Timeline Server 是服务端,负责提供 API 和缓存 FileSystemView
- 两者通过 HTTP 协议通信

**PriorityBasedFileSystemView 的降级机制**:
```
PriorityBasedFileSystemView:
  ├── primary: RemoteHoodieTableFileSystemView (优先)
  └── secondary: HoodieTableFileSystemView (降级)

工作流程:
  1. 先尝试调用 Timeline Server
  2. 如果失败(超时/连接失败),降级到本地构建
  3. 本地构建的结果不缓存到 Timeline Server
```

**Timeline Server 的线程模型**:
- Javalin 使用 Jetty 作为底层 HTTP 服务器
- 线程池大小由 `hoodie.embed.timeline.server.threads` 控制
- 每个请求由一个线程处理,支持并发访问

### 设计理念

**为什么采用嵌入式设计而不是独立服务?**

Timeline Server 采用**嵌入式设计**,运行在 Driver 进程中,而不是独立的服务。这种设计的权衡:

**优势**:
1. **简单性**: 无需额外部署和管理独立服务,降低运维成本
2. **生命周期一致**: Timeline Server 与 Spark 任务同生共死,无需担心服务残留
3. **无状态**: Timeline Server 不持久化状态,重启后自动重建,无需考虑状态恢复
4. **低延迟**: Driver 与 Executor 通常在同一集群内,网络延迟低

**劣势**:
1. **单点故障**: Driver 崩溃后,Timeline Server 也不可用,Executor 需要降级到本地构建
2. **资源竞争**: Timeline Server 与 Driver 共享内存和 CPU,可能影响 Driver 性能
3. **扩展性**: Timeline Server 的性能受限于 Driver 的资源,无法独立扩展

**为什么选择 Javalin 而不是其他框架?**

Javalin 是一个轻量级的 Java Web 框架,特点:
1. **轻量**: 只有几百 KB,依赖少,启动快
2. **简单**: API 简洁,易于使用和维护
3. **性能**: 基于 Jetty,性能优秀
4. **嵌入式友好**: 设计上就是为嵌入式场景优化的

对比其他框架:
- **Spring Boot**: 太重,启动慢,依赖多
- **Netty**: 太底层,需要手动处理 HTTP 协议
- **Jetty**: 需要手动配置,Javalin 是 Jetty 的高级封装

**为什么需要 PriorityBasedFileSystemView?**

这是 Timeline Server 设计中最重要的容错机制。纯 `RemoteHoodieTableFileSystemView` 的问题:
- Timeline Server 不可用时,查询直接失败
- 网络抖动时,查询不稳定

`PriorityBasedFileSystemView` 通过**降级机制**解决了这个问题:
1. **优先远程**: 先尝试调用 Timeline Server,性能最优
2. **降级本地**: 失败时降级到本地构建,保证可用性
3. **透明切换**: 上层代码无需感知,自动切换

这种设计体现了**可用性优于性能**的原则。

**为什么 Timeline Server 需要定期刷新?**

Timeline Server 缓存了 FileSystemView,但主表可能有新的 commit。如果不刷新,Executor 会读取到过期的文件列表。刷新策略:
1. **被动刷新**: Executor 调用 `/refresh` API 触发刷新
2. **主动刷新**: Timeline Server 定期检查 Timeline,发现新 Instant 时自动刷新
3. **超时刷新**: 缓存超过一定时间后自动失效,强制刷新

**与 Iceberg 的对比**:

| 维度 | Hudi Timeline Server | Iceberg Catalog Server |
|------|---------------------|----------------------|
| **部署方式** | 嵌入式(Driver 内) | 独立服务 |
| **职责** | 提供 FileSystemView | 提供 Metadata + 事务协调 |
| **状态** | 无状态(缓存) | 有状态(持久化) |
| **容错** | 降级到本地 | 依赖高可用部署 |
| **扩展性** | 受限于 Driver | 可独立扩展 |

Hudi 的 Timeline Server 更轻量,但功能相对简单。Iceberg 的 Catalog Server 更强大,但需要额外的运维成本。

**架构演进历史**:
- **0.5.x**: 无 Timeline Server,每个 Executor 独立构建 FileSystemView
- **0.6.0**: 引入 Timeline Server,基于 Javalin 实现
- **0.7.0**: 新增 PriorityBasedFileSystemView,支持降级
- **0.9.0**: 优化线程池和缓存策略,提升性能
- **1.0.0**: 支持增量刷新,减少刷新开销
- **1.2.0**: 支持多租户,一个 Driver 可以服务多个表

### 7.1 为什么需要 Timeline Server?

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

**operationType** 是一个枚举，覆盖了 Hudi 支持的所有写入操作类型：`INSERT`、`INSERT_PREPPED`、`UPSERT`、`UPSERT_PREPPED`、`BULK_INSERT`、`BULK_INSERT_PREPPED`、`DELETE`、`DELETE_PREPPED`、`BOOTSTRAP`、`INSERT_OVERWRITE`、`BUCKET_RESCALE`、`CLUSTER`、`DELETE_PARTITION`、`INSERT_OVERWRITE_TABLE`、`COMPACT`、`INDEX`、`ALTER_SCHEMA`、`LOG_COMPACT`、`UNKNOWN` 等共 19 种。

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
