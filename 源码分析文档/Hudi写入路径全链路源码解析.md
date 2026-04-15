# Hudi 写入路径全链路源码解析

> 基于 Apache Hudi v1.2.0-SNAPSHOT 源码，从一条记录进入 Hudi 到最终持久化到文件系统的完整链路逐层拆解。

---

## 目录

1. [HoodieRecord 的生命周期](#1-hoodierecord-的生命周期)
2. [KeyGenerator 体系](#2-keygenerator-体系)
3. [RecordMerger 体系](#3-recordmerger-体系)
4. [WriteHandle 完整体系](#4-writehandle-完整体系)
5. [IO 层：HoodieIOFactory / HoodieFileWriter / HoodieFileReader](#5-io-层hoodieioFactory--hoodiefilewriter--hoodiefilereader)
6. [Marker 机制完整解析](#6-marker-机制完整解析)
7. [写入操作类型全景](#7-写入操作类型全景)
8. [全链路串联：一条记录的完整写入之旅](#8-全链路串联一条记录的完整写入之旅)

---

## 1. HoodieRecord 的生命周期

### 1.1 核心三元组：HoodieKey / HoodieRecordLocation / HoodieRecord

Hudi 对每条记录的管理围绕三个核心类展开。理解这三个类的职责和关系是读懂整个写入链路的前提。

#### HoodieKey — 记录的全局唯一标识

源码路径：`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieKey.java`

```java
@AllArgsConstructor
@NoArgsConstructor
@Data
public class HoodieKey implements Serializable {
  private String recordKey;
  private String partitionPath;
}
```

HoodieKey 只有两个字段：`recordKey`（记录主键）和 `partitionPath`（分区路径）。这两个字段联合构成了一条记录在整张 Hudi 表中的全局唯一标识。

**为什么这么设计？**

- Hudi 是面向"行级别更新"的数据湖框架，必须能精确定位到"表中哪个分区下的哪条记录"。将 recordKey 与 partitionPath 绑定为一个不可变对象，使得后续 Index 查找、Merge 操作都可以通过这个 key 一步定位。
- HoodieKey 实现了 `Serializable`，且注释中明确说明不允许修改其结构（因为 `HoodieDeleteBlock` 使用 Kryo 序列化了 HoodieKey，修改结构会破坏向后兼容性，参见 HUDI-5760）。

**好处：** 简单而稳定。整个系统中所有对"某条记录"的引用都统一为 HoodieKey，避免了各模块各自定义主键格式带来的混乱。

#### HoodieRecordLocation — 记录的物理位置

源码路径：`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordLocation.java`

```java
@AllArgsConstructor
@NoArgsConstructor
@Data
public class HoodieRecordLocation implements Serializable, KryoSerializable {
  public static final long INVALID_POSITION = -1L;
  protected String instantTime;
  protected String fileId;
  @EqualsAndHashCode.Exclude
  protected long position;
}
```

HoodieRecordLocation 描述了一条记录在文件系统上的物理位置：

| 字段 | 含义 |
|------|------|
| `instantTime` | 记录所属的提交时间（instant），标识了这条记录是在哪次写入中产生的 |
| `fileId` | 文件组 ID（File Group ID），Hudi 以 fileId 为粒度管理数据文件 |
| `position` | 记录在文件中的行位置（如 Parquet 文件中的行号），从 0 开始，-1 表示无效 |

**为什么这么设计？**

- `instantTime + fileId` 就可以唯一确定一个 base file 或者一个 log file。这与 Hudi 的 Timeline（时间线）架构完美契合：每次写入都有一个 instant，每个 instant 下对某个 fileId 只产生一个文件。
- `position` 字段是从 Table Version 8 开始引入的，用于支持基于行位置的更精确合并（无需全文件扫描即可定位到具体行），是性能优化的关键。
- 注意 `position` 字段在 `equals` 和 `hashCode` 中被排除（`@EqualsAndHashCode.Exclude`），因为两个指向同一 instant + fileId 的位置应该被认为是"同一位置"，position 只是读取优化用的辅助信息。

**好处：** 通过 instantTime + fileId 的组合，Hudi 不需要维护一个全局的"记录 -> 文件偏移量"的索引，而是将定位问题简化为 Timeline 上的查找问题。

#### HoodieRecord — 抽象基类

源码路径：`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecord.java`

这是 Hudi 中最核心的抽象类，定义了一条记录在整个生命周期中携带的所有信息：

```java
public abstract class HoodieRecord<T> implements HoodieRecordCompatibilityInterface, 
    KryoSerializable, Serializable {
  
  protected HoodieKey key;                    // 记录的全局标识
  protected T data;                           // 实际数据载荷（泛型，支持不同引擎）
  protected HoodieRecordLocation currentLocation;  // 当前在存储中的位置（由 Index 填充）
  protected HoodieRecordLocation newLocation;       // 写入后的新位置
  protected boolean ignoreIndexUpdate;         // 是否跳过索引更新
  private boolean sealed;                      // 是否已密封（不可修改）
  protected HoodieOperation operation;         // CDC 操作类型
  protected Option<Map<String, String>> metaData;  // 记录级元数据
  protected transient Comparable<?> orderingValue; // 排序值（用于 merge 决策）
  protected Boolean isDelete;                  // 是否为删除记录
}
```

### 1.2 HoodieRecord 的子类体系

HoodieRecord 通过 `HoodieRecordType` 枚举区分不同引擎的实现：

```java
public enum HoodieRecordType {
  AVRO, SPARK, HIVE, FLINK
}
```

| 子类 | RecordType | 说明 |
|------|------------|------|
| `HoodieAvroRecord<T extends HoodieRecordPayload>` | AVRO | 基于 Avro 的实现，payload 为 `HoodieRecordPayload`，最通用 |
| `HoodieAvroIndexedRecord` | AVRO | 直接包装 `IndexedRecord`，不使用 payload 模式 |
| `HoodieSparkRecord` | SPARK | 包装 Spark 的 `InternalRow`，避免 Spark<->Avro 转换开销 |
| `HoodieFlinkRecord` | FLINK | 包装 Flink 的 `RowData`，同样避免序列化开销 |

**为什么这么设计？**

早期 Hudi 只有 `HoodieAvroRecord`，所有引擎都要先将数据转换为 Avro 格式。这在 Spark 中意味着 `InternalRow -> GenericRecord -> InternalRow` 的双重转换，性能损失巨大。引入引擎原生的 Record 类型后，写入路径可以直接使用引擎原生格式，大幅减少序列化/反序列化开销。

**好处：** 引擎无关的接口 + 引擎特化的实现 = 最大性能 + 最大通用性。

### 1.3 记录的五个元数据字段

每条写入 Hudi 的记录都会被自动添加 5 个元数据字段（如果启用了 `populateMetaFields`）：

```java
public static final List<String> HOODIE_META_COLUMNS = CollectionUtils.createImmutableList(
    "_hoodie_commit_time",      // 提交时间
    "_hoodie_commit_seqno",     // 提交序列号（全局唯一）
    "_hoodie_record_key",       // 记录主键
    "_hoodie_partition_path",   // 分区路径
    "_hoodie_file_name"         // 文件名
);
```

此外还有一个可选的 `_hoodie_operation` 字段，用于 CDC 场景标记操作类型（INSERT/UPDATE/DELETE）。

**为什么这么设计？**

- 这些元字段使得每个 Parquet 文件都是"自描述"的，不依赖外部元数据即可回答"这条记录的 key 是什么、属于哪个分区、由哪次 commit 写入"等问题。
- 特别是 `_hoodie_commit_seqno` 提供了全局唯一的序列号，在 MOR 表的 log 合并过程中用于确定记录的先后顺序。

### 1.4 记录的四阶段生命周期

一条记录从进入 Hudi 到最终落盘，经历以下四个阶段：

```
阶段 1: 创建 (Created)
  ┌──────────────────────────────────┐
  │ key = HoodieKey(recordKey, path) │
  │ data = 原始数据                   │
  │ currentLocation = null           │  <-- 还不知道这条记录在存储中是否已存在
  │ newLocation = null               │
  │ sealed = false                   │
  └──────────────────────────────────┘
           │
           ▼ Index 查找（tagLocation）
阶段 2: 已标记 (Tagged)
  ┌──────────────────────────────────┐
  │ currentLocation = (instant, fid) │  <-- Index 告知记录已存在于某个文件中
  │ 或 currentLocation = null        │  <-- Index 告知这是新记录
  │ sealed = true                    │
  └──────────────────────────────────┘
           │
           ▼ WriteHandle 写入
阶段 3: 已写入 (Written)
  ┌──────────────────────────────────┐
  │ newLocation = (instant, new_fid) │  <-- 记录被写入了新的文件
  │ data = null (已 deflate)          │  <-- 数据已落盘，释放内存
  └──────────────────────────────────┘
           │
           ▼ Commit 成功
阶段 4: 已提交 (Committed)
  ┌──────────────────────────────────┐
  │ 记录信息写入 commit metadata       │
  │ Index 更新为 newLocation          │
  └──────────────────────────────────┘
```

**关键方法解析：**

1. **setCurrentLocation** — 由 Index 调用，标记记录在存储中的已有位置（如果记录已存在）
2. **setNewLocation** — 由 WriteHandle 调用，标记记录被写入后的新位置
3. **deflate** — 写入成功后释放 data 字段的内存引用（`this.data = null`）
4. **seal/unseal** — 控制记录的可修改性，防止在不该修改的时候被意外修改

**为什么需要 seal 机制？**

在分布式计算环境中，HoodieRecord 可能被 Shuffle 到不同节点。seal 机制确保在 Shuffle 过程中记录不会被意外修改。只有在需要设置 location 时才通过 `unseal()` 临时打开修改权限，设置完毕后立即 `seal()`。

**deflate 机制的好处：**

一旦记录数据已经持久化到文件系统，内存中就不再需要持有完整的数据负载。通过 `deflate()` 将 data 设为 null，可以在写入大批量数据时显著降低内存压力，避免 OOM。

---

## 2. KeyGenerator 体系

### 2.1 设计理念

KeyGenerator 的职责是从原始数据记录中提取出 `recordKey`（记录主键）和 `partitionPath`（分区路径），然后构造出 `HoodieKey`。这是写入链路的第一个环节——数据还没有被分配文件，甚至还没有进入 Hudi 的处理管道，KeyGenerator 就需要先确定"这条记录的身份是什么、应该放到哪个分区"。

**为什么将 Key 生成设计为可插拔的？**

不同业务场景对主键和分区的定义千差万别：

- 有的系统用单个字段作为主键（如 `user_id`），分区也是单个字段（如 `dt`）
- 有的系统用复合字段作为主键（如 `user_id + order_id`），分区也是多字段（如 `year/month/day`）
- 有的系统需要将时间戳格式化为日期作为分区路径
- 有的系统根本没有分区

将 KeyGenerator 设计为可插拔的抽象，让用户可以通过配置选择不同的实现，而不需要修改任何核心代码。

### 2.2 继承体系

```
KeyGeneratorInterface (接口)
  │
  └── KeyGenerator (抽象类, hudi-common)
        │
        └── BaseKeyGenerator (抽象类, hudi-common)
              │
              ├── SimpleAvroKeyGenerator (hudi-client-common)
              │     └── TimestampBasedAvroKeyGenerator (hudi-client-common)
              │
              ├── ComplexAvroKeyGenerator (hudi-client-common)
              │
              ├── NonpartitionedAvroKeyGenerator (hudi-client-common)
              │
              ├── CustomAvroKeyGenerator (hudi-client-common)
              │
              ├── GlobalAvroDeleteKeyGenerator (hudi-client-common)
              │
              └── AutoRecordGenWrapperAvroKeyGenerator (hudi-client-common)

Spark 特化版本（位于 hudi-spark-client）:
  BuiltinKeyGenerator (抽象类)
    ├── SimpleKeyGenerator
    ├── ComplexKeyGenerator
    ├── TimestampBasedKeyGenerator
    ├── NonpartitionedKeyGenerator
    ├── CustomKeyGenerator
    ├── GlobalDeleteKeyGenerator
    └── AutoRecordGenWrapperKeyGenerator
```

**为什么有 Avro 版和 Spark 版两套？**

这与 HoodieRecord 的引擎多态策略一致。Avro 版本（`*AvroKeyGenerator`）接受 `GenericRecord` 作为输入，适用于所有引擎；Spark 版本（`BuiltinKeyGenerator` 体系）直接操作 Spark 的 `Row`/`InternalRow`，避免了 Spark -> Avro 的转换开销。

### 2.3 KeyGenerator 抽象基类

源码路径：`hudi-common/src/main/java/org/apache/hudi/keygen/KeyGenerator.java`

```java
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public abstract class KeyGenerator implements KeyGeneratorInterface {
  public static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
  public static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";
  
  protected final TypedProperties config;
  
  public abstract HoodieKey getKey(GenericRecord record);
}
```

核心方法 `getKey()` 接收一条 Avro GenericRecord，返回 HoodieKey。

对于复合主键，有一个关键的静态方法 `constructRecordKey()`：

```java
public static String constructRecordKey(String[] recordKeyFields, 
    BiFunction<String, Integer, String> recordValueFunction) {
  // 格式: "field1:value1,field2:value2,..."
  // 如果所有字段都是 null 或空，抛出 HoodieKeyException
}
```

**为什么 null 值用 `__null__` 占位而不是直接用空字符串？**

因为在复合主键场景下，需要区分"字段值为空"和"字段值为 null"两种情况，同时确保序列化后的 key 是可逆解析的。使用特定占位符可以避免歧义。

### 2.4 BaseKeyGenerator — 中间层抽象

源码路径：`hudi-common/src/main/java/org/apache/hudi/keygen/BaseKeyGenerator.java`

```java
public abstract class BaseKeyGenerator extends KeyGenerator {
  protected List<String> recordKeyFields;      // 记录主键字段列表
  protected List<String> partitionPathFields;  // 分区路径字段列表
  protected final boolean hiveStylePartitioning;  // 是否使用 Hive 风格分区 (field=value)
  protected final boolean encodePartitionPath;    // 是否 URL 编码分区路径
  protected final boolean slashSeparatedDatePartitioning; // 是否使用斜杠分隔日期
  
  public abstract String getRecordKey(GenericRecord record);
  public abstract String getPartitionPath(GenericRecord record);
  
  @Override
  public final HoodieKey getKey(GenericRecord record) {
    return new HoodieKey(getRecordKey(record), getPartitionPath(record));
  }
}
```

BaseKeyGenerator 将 `getKey()` 拆分为 `getRecordKey()` 和 `getPartitionPath()` 两个独立方法，并从配置中读取各种分区格式选项。注意 `getKey()` 被声明为 `final`，子类只需要分别实现 recordKey 和 partitionPath 的生成逻辑。

**hiveStylePartitioning 的作用：**

| 设置 | 分区路径示例 |
|------|-------------|
| false | `2024/01/15` |
| true | `year=2024/month=01/day=15` |

Hive 风格分区使得 Hive Metastore 可以自动识别分区字段，与 Hive 生态无缝兼容。

### 2.5 各 KeyGenerator 实现详解

#### SimpleAvroKeyGenerator — 最基础的实现

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/keygen/SimpleAvroKeyGenerator.java`

适用场景：单字段 recordKey + 单字段 partitionPath。这是最常见的场景。

```java
public class SimpleAvroKeyGenerator extends BaseKeyGenerator {
  @Override
  public String getRecordKey(GenericRecord record) {
    return KeyGenUtils.getRecordKey(record, getRecordKeyFieldNames().get(0), 
        isConsistentLogicalTimestampEnabled());
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    return KeyGenUtils.getPartitionPath(record, getPartitionPathFields().get(0), 
        hiveStylePartitioning, encodePartitionPath, slashSeparatedDatePartitioning, 
        isConsistentLogicalTimestampEnabled());
  }
}
```

**好处：** 零开销——直接从记录中取一个字段值，没有任何拼接或格式化逻辑。对于简单场景性能最优。

#### ComplexAvroKeyGenerator — 复合键支持

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/keygen/ComplexAvroKeyGenerator.java`

适用场景：多字段 recordKey 和/或多字段 partitionPath。

```java
public class ComplexAvroKeyGenerator extends BaseKeyGenerator {
  // recordKey 格式: "field1:value1,field2:value2"
  // partitionPath 格式: "value1/value2/value3"
}
```

**设计亮点：** 当只有一个 recordKey 字段时，ComplexAvroKeyGenerator 会自动退化为 Simple 模式（不带字段名前缀），保持向后兼容性。这通过 `getRecordKeyFunc()` 方法中的条件判断实现。

#### TimestampBasedAvroKeyGenerator — 时间戳分区

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/keygen/TimestampBasedAvroKeyGenerator.java`

适用场景：分区路径基于时间戳字段，需要格式化为日期字符串。

这是所有 KeyGenerator 中最复杂的实现。它支持多种时间戳类型：

```java
public enum TimestampType {
  UNIX_TIMESTAMP,     // 秒级时间戳
  DATE_STRING,        // 日期字符串
  MIXED,              // 混合格式
  EPOCHMILLISECONDS,  // 毫秒时间戳
  EPOCHMICROSECONDS,  // 微秒时间戳
  SCALAR              // 自定义时间单位
}
```

**getPartitionPath 的核心逻辑：**

1. 从记录中提取时间戳字段值
2. 根据 `TimestampType` 和输入格式，将值统一转换为毫秒时间戳
3. 使用输出日期格式化器，将毫秒时间戳格式化为分区路径字符串
4. 根据配置决定是否 URL 编码、是否 Hive 风格

**为什么这么复杂？**

因为现实世界中时间戳的表示方式极其多样：有的数据源用秒、有的用毫秒、有的用日期字符串、还有的用 Avro 的 LogicalType。TimestampBasedAvroKeyGenerator 承担了"时间格式归一化"的职责，让用户只需要配置输入/输出格式，而不需要自己写时间转换代码。

#### CustomAvroKeyGenerator — 灵活组合

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/keygen/CustomAvroKeyGenerator.java`

适用场景：不同分区字段需要不同的处理策略（有的用 SIMPLE、有的用 TIMESTAMP）。

配置格式：`hoodie.datasource.write.partitionpath.field=field1:simple,field2:timestamp`

```java
public class CustomAvroKeyGenerator extends BaseKeyGenerator {
  private final List<BaseKeyGenerator> partitionKeyGenerators;  // 每个分区字段一个 generator
  private final BaseKeyGenerator recordKeyGenerator;            // recordKey 用 Simple 或 Complex

  @Override
  public String getPartitionPath(GenericRecord record) {
    StringBuilder partitionPath = new StringBuilder();
    for (int i = 0; i < partitionKeyGenerators.size(); i++) {
      partitionPath.append(partitionKeyGenerators.get(i).getPartitionPath(record));
      if (i != partitionKeyGenerators.size() - 1) {
        partitionPath.append("/");
      }
    }
    return partitionPath.toString();
  }
}
```

**设计精髓：** 组合模式（Composite Pattern）。CustomAvroKeyGenerator 本身并不实现具体的分区路径生成逻辑，而是将每个分区字段委托给对应的 SimpleAvroKeyGenerator 或 TimestampBasedAvroKeyGenerator。这让用户可以在同一张表中对不同分区字段使用不同的格式化策略。

#### NonpartitionedAvroKeyGenerator — 无分区表

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/keygen/NonpartitionedAvroKeyGenerator.java`

```java
public class NonpartitionedAvroKeyGenerator extends BaseKeyGenerator {
  @Override
  public String getPartitionPath(GenericRecord record) {
    return EMPTY_PARTITION;  // 始终返回空字符串
  }
}
```

适用场景：不需要分区的表。所有数据写入同一个"根分区"。

### 2.6 KeyGenerator 工厂

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/keygen/factory/HoodieAvroKeyGeneratorFactory.java`

KeyGenerator 的实例化通过工厂类完成，根据配置项 `hoodie.datasource.write.keygenerator.class` 或 `hoodie.datasource.write.keygenerator.type` 自动选择合适的实现。

**为什么需要工厂？**

因为 KeyGenerator 的选择涉及配置解析、类加载、版本兼容等复杂逻辑，不适合分散在各处。工厂模式将这些复杂性集中管理。

---

## 3. RecordMerger 体系

### 3.1 设计理念

RecordMerger 解决的核心问题是：当同一条记录（相同 recordKey）出现多个版本时，如何决定最终保留哪个版本？

这在数据湖场景中极为常见：

- **UPSERT**：新数据可能和存储中的已有数据产生冲突
- **preCombine**：同一批次中可能出现相同 key 的重复数据
- **MOR 读取**：需要将 base file 中的记录与 delta log 中的更新合并

### 3.2 HoodieRecordMerger 接口

源码路径：`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecordMerger.java`

```java
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public interface HoodieRecordMerger extends Serializable {
  
  // 四种合并策略 ID
  String EVENT_TIME_BASED_MERGE_STRATEGY_UUID = "eeb8d96f-...";
  String COMMIT_TIME_BASED_MERGE_STRATEGY_UUID = "ce9acb64-...";
  String CUSTOM_MERGE_STRATEGY_UUID = "1897ef5f-...";
  String PAYLOAD_BASED_MERGE_STRATEGY_UUID = "00000000-...";

  // 核心合并方法
  <T> BufferedRecord merge(BufferedRecord<T> older, BufferedRecord<T> newer, 
      RecordContext<T> recordContext, TypedProperties props) throws IOException;

  // 部分更新合并（可选）
  default <T> BufferedRecord<T> partialMerge(...) throws IOException {
    throw new UnsupportedOperationException(...);
  }

  HoodieRecordType getRecordType();
  String getMergingStrategy();
}
```

**核心设计原则：**

1. **结合律（Associativity）：** 合并操作必须满足 `f(a, f(b, c)) = f(f(a, b), c)`。这意味着无论合并的顺序如何，最终结果一致。这个性质对于分布式系统至关重要——因为在不同节点上处理数据的顺序可能不同。

2. **无状态：** Merger 是无状态组件，所有决策信息（如 ordering value）都从记录本身提取。

### 3.3 RecordMergeMode 三种模式

源码路径：`hudi-common/src/main/java/org/apache/hudi/common/config/RecordMergeMode.java`

```java
public enum RecordMergeMode {
  COMMIT_TIME_ORDERING,  // 事务时间排序：后写入的覆盖先写入的
  EVENT_TIME_ORDERING,   // 事件时间排序：按业务时间排序决定哪条胜出
  CUSTOM                 // 自定义合并逻辑
}
```

| 模式 | 对应的 Merger | 适用场景 |
|------|--------------|---------|
| COMMIT_TIME_ORDERING | OverwriteWithLatestMerger | 简单场景，最新写入即最终状态 |
| EVENT_TIME_ORDERING | 引擎内置 EventTime Merger | 有乱序数据，需要按业务时间排序 |
| CUSTOM | HoodieAvroRecordMerger 等 | 复杂业务逻辑，如条件更新 |

### 3.4 各 Merger 实现详解

#### OverwriteWithLatestMerger — 最简策略

源码路径：`hudi-common/src/main/java/org/apache/hudi/common/model/OverwriteWithLatestMerger.java`

```java
public class OverwriteWithLatestMerger implements HoodieRecordMerger {
  @Override
  public <T> BufferedRecord<T> merge(BufferedRecord<T> older, BufferedRecord<T> newer, 
      RecordContext<T> recordContext, TypedProperties props) throws IOException {
    return newer;   // 永远选择更新的记录
  }

  @Override
  public String getMergingStrategy() {
    return COMMIT_TIME_BASED_MERGE_STRATEGY_UUID;
  }
}
```

**为什么需要这么简单的实现？**

大量场景下（如日志采集、全量同步），业务语义就是"最新值覆盖旧值"，不需要任何比较逻辑。此时使用 OverwriteWithLatestMerger 可以避免所有不必要的 ordering value 计算和比较开销。

#### HoodieAvroRecordMerger — 基于 Payload 的合并

源码路径：`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieAvroRecordMerger.java`

这是最传统也最灵活的 Merger，通过 `HoodieRecordPayload` 接口委托实际的合并逻辑：

```java
public class HoodieAvroRecordMerger implements HoodieRecordMerger {
  @Override
  public <T> BufferedRecord<T> merge(BufferedRecord<T> older, BufferedRecord<T> newer, 
      RecordContext<T> recordContext, TypedProperties props) throws IOException {
    
    // 特殊处理：commit time ordering 的删除
    if (HoodieRecordMerger.isCommitTimeOrderingDelete(older, newer)) {
      return newer;
    }
    
    // 加载 Payload 并调用 combineAndGetUpdateValue
    HoodieRecordPayload payload = HoodieRecordUtils.loadPayload(
        payloadClass, newerAvroRecord, newer.getOrderingValue());
    
    if (previousAvroData == null) {
      return newer;  // 没有旧数据，直接返回新数据
    } else {
      Option<IndexedRecord> updatedValue = payload.combineAndGetUpdateValue(
          previousAvroData, schema, props);
      // 根据 combineAndGetUpdateValue 的返回值决定结果...
    }
  }
}
```

**合并决策的三种返回值：**

| 返回值 | 含义 |
|--------|------|
| `Option.of(updatedRecord)` | 合并成功，使用更新后的记录 |
| `Option.empty()` | 合并结果为删除 |
| `Option.of(SENTINEL)` | 跳过此记录（由 ExpressionPayload 使用） |

**为什么要通过 Payload 间接委托？**

Payload 模式（`HoodieRecordPayload`）是 Hudi 早期的设计遗产。它允许用户通过自定义 Payload 类来控制合并逻辑，例如 `OverwriteWithLatestAvroPayload`（用最新值覆盖）、`DefaultHoodieRecordPayload`（基于 ordering value 比较）、`ExpressionPayload`（基于 SQL 表达式条件更新）。

这种设计虽然灵活，但由于 Payload 基于 Avro，在 Spark/Flink 引擎中会产生不必要的序列化开销。因此 Hudi 引入了 RecordMerger 接口作为更高效的替代方案，同时通过 HoodieAvroRecordMerger 保持对旧 Payload 模式的兼容。

#### HoodiePreCombineAvroRecordMerger — 去重用

源码路径：`hudi-common/src/main/java/org/apache/hudi/common/model/HoodiePreCombineAvroRecordMerger.java`

```java
public class HoodiePreCombineAvroRecordMerger extends HoodieAvroRecordMerger {
  @Override
  public <T> BufferedRecord<T> merge(...) throws IOException {
    // 调用 Payload 的 preCombine 方法（而非 combineAndGetUpdateValue）
    HoodieRecordPayload payload = newerPayload.preCombine(olderPayload, schema, props);
    ...
  }
}
```

**preCombine 与 combineAndGetUpdateValue 的区别：**

| 方法 | 使用场景 | 语义 |
|------|---------|------|
| `preCombine` | 同一批次内的去重 | 两条记录地位平等，选择"更优"的一条 |
| `combineAndGetUpdateValue` | 新记录与存储中旧记录的合并 | 新记录尝试"更新"旧记录，可能产生删除 |

这种区分非常重要：在 preCombine 阶段，如果两条记录都是新数据，不应该产生删除语义；而在 merge 阶段，新记录可能表示"删除已有记录"。

### 3.5 isCommitTimeOrderingDelete 机制

```java
static <T> boolean isCommitTimeOrderingDelete(BufferedRecord<T> oldRecord, 
    BufferedRecord<T> newRecord) {
  return newRecord.isCommitTimeOrderingDelete() || oldRecord.isCommitTimeOrderingDelete();
}
```

这是一个特殊的短路逻辑：如果一条记录是 commit time ordering 的删除（没有 ordering value，纯粹按事务时间排序），则无论另一条记录的 ordering value 是什么，都直接采用新记录。这确保了"删除操作"在任何合并策略下都能正确生效。

---

## 4. WriteHandle 完整体系

### 4.1 设计理念

WriteHandle 是写入路径中实际执行"将记录写入文件系统"的组件。不同的写入场景（新建文件、合并更新、追加日志）需要不同的写入策略，WriteHandle 通过继承体系将这些策略封装起来。

### 4.2 完整继承树

```
HoodieIOHandle (基类 - 持有 config, instantTime, hoodieTable)
  │
  └── HoodieWriteHandle (写入基类)
        │
        ├── BaseCreateHandle (创建新文件)
        │     ├── HoodieCreateHandle (通用创建)
        │     ├── HoodieUnboundedCreateHandle (不限大小创建)
        │     ├── HoodieBootstrapHandle (Bootstrap 创建)
        │     └── HoodieBinaryCopyHandle (二进制复制)
        │     
        │     Flink 特化:
        │     ├── FlinkCreateHandle
        │     ├── HoodieRowDataCreateHandle
        │     └── HoodieRowCreateHandle (Spark Row)
        │
        ├── HoodieAbstractMergeHandle (合并基类, 实现 HoodieMergeHandle 接口)
        │     │
        │     └── HoodieWriteMergeHandle (读旧文件 + 合并 + 写新文件)
        │           │
        │           ├── HoodieSortedMergeHandle (排序合并)
        │           ├── HoodieSortedMergeHandleWithChangeLog (排序合并 + CDC)
        │           ├── HoodieMergeHandleWithChangeLog (合并 + CDC)
        │           ├── HoodieConcatHandle (直接拼接，不合并)
        │           │
        │           └── FileGroupReaderBasedMergeHandle (基于 FileGroupReader)
        │
        │     Flink 特化:
        │     ├── FlinkMergeHandle
        │     ├── FlinkMergeHandleWithChangeLog
        │     ├── FlinkIncrementalMergeHandle
        │     ├── FlinkIncrementalMergeHandleWithChangeLog
        │     ├── FlinkIncrementalConcatHandle
        │     └── FlinkFileGroupReaderBasedMergeHandle
        │
        └── HoodieAppendHandle (追加日志文件)
              │
              └── FileGroupReaderBasedAppendHandle
              
              Flink 特化:
              └── FlinkAppendHandle
```

### 4.3 IOType — 三种底层 IO 类型

源码路径：`hudi-common/src/main/java/org/apache/hudi/common/model/IOType.java`

```java
public enum IOType {
  MERGE,    // 读取旧 base file + 合并新数据 + 写入新 base file
  CREATE,   // 直接创建新 base file
  APPEND    // 追加写入 delta log file (仅 Table Version 6 及以下)
}
```

**这三种 IO 类型与表类型的关系：**

| 表类型 | INSERT 操作 | UPSERT（新记录） | UPSERT（已有记录） |
|--------|------------|-----------------|-------------------|
| COW | CREATE | CREATE | MERGE |
| MOR | CREATE | CREATE | APPEND |

### 4.4 HoodieWriteHandle — 写入基类

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieWriteHandle.java`

关键字段和职责：

```java
public abstract class HoodieWriteHandle<T, I, K, O> extends HoodieIOHandle<T, I, K, O> {
  // Schema 管理
  protected final HoodieSchema writeSchema;               // 写入 schema
  protected final HoodieSchema writeSchemaWithMetaFields;  // 带元字段的写入 schema

  // 合并器
  protected final HoodieRecordMerger recordMerger;
  
  // 状态追踪
  protected WriteStatus writeStatus;           // 写入状态（成功/失败记录数等）
  protected HoodieRecordLocation newRecordLocation; // 新的记录位置

  // 位置信息
  protected final String partitionPath;  
  protected final String fileId;
  protected final String writeToken;     // 写入令牌，格式: partitionId-stageId-attemptId
}
```

**writeToken 的设计：**

`writeToken` 由 `partitionId-stageId-attemptId` 三个部分组成，用于在文件名中唯一标识一个写入任务。这确保了即使在 Spark/Flink 任务重试的情况下，不同 attempt 产生的文件也不会冲突。

**createMarkerFile 方法：**

```java
protected void createMarkerFile(String partitionPath, String dataFileName) {
  WriteMarkersFactory.get(config.getMarkersType(), hoodieTable, instantTime)
      .create(partitionPath, dataFileName, getIOType(), config, fileId, 
          hoodieTable.getMetaClient().getActiveTimeline());
}
```

每次创建数据文件之前，都会先创建对应的 marker 文件。这是故障恢复的关键（详见第 6 节）。

### 4.5 BaseCreateHandle / HoodieCreateHandle — 创建新文件

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/BaseCreateHandle.java`

BaseCreateHandle 负责创建全新的 base file（如 Parquet 文件）。

**核心写入流程 (doWrite 方法)：**

```
doWrite(record, schema, props)
  │
  ├── 判断是否为删除操作
  │     ├── 是 → recordsDeleted++，不写入文件
  │     └── 否 → 继续
  │
  ├── 判断是否应忽略 (shouldIgnore)
  │     ├── 是 → 直接返回
  │     └── 否 → 继续
  │
  ├── writeRecordToFile(record, schema)
  │     ├── preserveMetadata=true → 更新文件名后直接写入
  │     └── preserveMetadata=false → 先 prependMetaFields，再 writeWithMetadata
  │
  ├── record.unseal() → setNewLocation → seal()
  │     └── 标记记录写入了新的位置
  │
  ├── recordsWritten++, insertRecordsWritten++
  │
  ├── writeStatus.markSuccess(record, metadata)
  │
  └── record.deflate()   // 释放内存
```

**canWrite 方法：**

```java
public boolean canWrite(HoodieRecord record) {
  return (fileWriter.canWrite() && record.getPartitionPath().equals(writeStatus.getPartitionPath()))
      || layoutControlsNumFiles();
}
```

这个方法决定当前 handle 是否还能接受更多记录。判断依据：
1. 底层 FileWriter 是否还可以写（文件未达到大小限制）
2. 记录的 partitionPath 是否与当前 handle 的分区一致
3. 存储布局是否控制文件数量（如 Bucket Index 场景）

**好处：** 通过 canWrite 机制，上层代码可以透明地实现文件滚动——当一个文件写满时，创建新的 handle 继续写。

### 4.6 HoodieWriteMergeHandle — COW 表的更新核心

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieWriteMergeHandle.java`

这是 Copy-On-Write 表处理 UPSERT 的核心 Handle。其工作原理可以用一个简洁的例子说明：

```
已有数据 (base file):  rec1_v1, rec2_v1, rec3_v1, rec4_v1
新入数据:              rec1_v2, rec4_v2, rec5_v1, rec6_v1

合并过程:
  遍历已有文件中的每条记录:
    rec1_v1 → 发现有新数据 rec1_v2 → merge(rec1_v1, rec1_v2) → 写入 rec1_v2
    rec2_v1 → 无新数据 → 直接写入 rec2_v1
    rec3_v1 → 无新数据 → 直接写入 rec3_v1
    rec4_v1 → 发现有新数据 rec4_v2 → merge(rec4_v1, rec4_v2) → 写入 rec4_v2
  
  遍历剩余新数据（不在已有文件中的）:
    rec5_v1 → 作为新插入写入
    rec6_v1 → 作为新插入写入

最终新文件: rec1_v2, rec2_v1, rec3_v1, rec4_v2, rec5_v1, rec6_v1
```

**关键实现细节：**

1. **ExternalSpillableMap：** 新入数据存储在 `ExternalSpillableMap` 中（key → record），当内存不足时自动溢写到磁盘。这防止了大批量更新时的 OOM。

```java
this.keyToNewRecords = new ExternalSpillableMap<>(
    memoryForMerge,               // 最大内存
    config.getSpillableMapBasePath(),  // 溢写路径
    new DefaultSizeEstimator<>(), 
    new HoodieRecordSizeEstimator<>(writeSchema),
    config.getCommonConfig().getSpillableDiskMapType(),
    ...);
```

2. **doMerge：** 实际的遍历和合并由 `HoodieMergeHelper` 完成：

```java
@Override
public void doMerge() throws IOException {
  HoodieMergeHelper.newInstance().runMerge(hoodieTable, this);
}
```

3. **writeUpdateRecord：** 合并新旧记录后的写入：

```java
protected boolean writeUpdateRecord(HoodieRecord<T> newRecord, HoodieRecord<T> oldRecord, 
    HoodieRecord combineRecord, HoodieSchema writerSchema) throws IOException {
  boolean isDelete = false;
  if (oldRecord.getData() != combineRecord.getData()) {
    // 选择了新记录
    isDelete = isDeleteRecord(combineRecord);
    if (!isDelete) updatedRecordsWritten++;
  } else {
    // 选择了旧记录（新记录被丢弃）
    return false;
  }
  return writeRecord(newRecord, oldRecord, combineRecord, writerSchema, props, isDelete);
}
```

**为什么 COW 表的 UPSERT 效率相对较低？**

因为即使只更新一条记录，也需要读取整个 base file，逐行判断是否需要合并，然后写出一个完整的新文件。这就是 COW 的"写放大"问题。MOR 表通过 AppendHandle 避免了这个问题。

### 4.7 HoodieConcatHandle — 跳过合并的拼接

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieConcatHandle.java`

当操作类型为 INSERT 且配置了 `allowDuplicateInserts=true` 时，使用 ConcatHandle 替代 MergeHandle。

```java
// 对已有记录：直接写入，不做任何合并
@Override
public void write(HoodieRecord oldRecord) {
  writeToFile(new HoodieKey(key, partitionPath), oldRecord, oldSchema, props, true);
  recordsWritten++;
}

// 对新记录：直接追加写入
@Override
protected void writeIncomingRecords() throws IOException {
  while (recordItr.hasNext()) {
    HoodieRecord<T> record = recordItr.next();
    record.unseal();
    record.setNewLocation(new HoodieRecordLocation(instantTime, fileId));
    record.seal();
    writeInsertRecord(record);
  }
}
```

**好处：** 完全跳过了"旧记录在新数据中是否存在"的查找和比较，性能远高于 MergeHandle。适用于确定没有重复数据的纯插入场景。

### 4.8 HoodieAppendHandle — MOR 表的追加核心

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/io/HoodieAppendHandle.java`

AppendHandle 不会读取和重写已有的 base file，而是将更新记录追加到 delta log 文件中。

**关键设计：**

1. **内存缓冲 + 批量刷盘：**

```java
protected final List<HoodieRecord> recordList = new ArrayList<>();  // 缓冲区
private final long maxBlockSize = config.getLogFileDataBlockMaxSize();  // 单个 block 最大大小
```

记录先缓冲在内存中，当缓冲区达到 `maxBlockSize` 时批量刷入 log 文件。这种批量写入减少了 IO 次数。

2. **bufferRecord 方法——判断 Insert 还是 Update：**

```java
private void bufferRecord(HoodieRecord<T> hoodieRecord) {
  boolean isUpdateRecord = isUpdateRecord(hoodieRecord);  // currentLocation != null 即为 update
  recordProperties.put(HoodiePayloadProps.PAYLOAD_IS_UPDATE_RECORD_FOR_MOR, 
      String.valueOf(isUpdateRecord));
  
  if (!record.isDelete(deleteContext, recordProperties)) {
    bufferInsertAndUpdate(schema, hoodieRecord, isUpdateRecord);
  } else {
    bufferDelete(hoodieRecord);  // 删除记录单独缓冲
  }
}
```

3. **Log Block 类型：**

AppendHandle 写入的 log 文件由多个 block 组成，每个 block 有不同类型：

| Block 类型 | 说明 |
|-----------|------|
| `HoodieAvroDataBlock` | Avro 格式的数据 block |
| `HoodieParquetDataBlock` | Parquet 格式的数据 block |
| `HoodieHFileDataBlock` | HFile 格式的数据 block |
| `HoodieDeleteBlock` | 记录删除操作的 block |

**为什么 MOR 表要用 log 文件而不是直接修改 base file？**

这是 MOR 表的核心设计哲学：**将"读放大"换成"写优化"**。追加到 log 文件是顺序写操作，比读取整个 base file + 重写新 file 快得多。代价是读取时需要合并 base file 和 log file，但这个合并可以延迟到 Compaction 或查询时进行。

### 4.9 Flink 特化 Handle

Flink 环境下有一套独立的 Handle 体系，主要区别在于：

| Handle | 功能 |
|--------|------|
| `FlinkCreateHandle` | Flink 的创建 handle，使用 RowData 格式 |
| `FlinkMergeHandle` | Flink 的合并 handle |
| `FlinkAppendHandle` | Flink 的追加 handle |
| `FlinkIncrementalMergeHandle` | 增量合并，只处理变更的记录而非全量 |
| `FlinkIncrementalConcatHandle` | 增量拼接 |
| `FlinkMergeHandleWithChangeLog` | 合并 + 产生 CDC changelog |

**FlinkIncrementalMergeHandle 的设计亮点：**

与 HoodieWriteMergeHandle 需要读取全量 base file 不同，FlinkIncrementalMergeHandle 只处理发生变化的记录。这在 Flink 流式写入场景下特别重要——每次 checkpoint 间隔内通常只有少量记录变化，没必要重写整个文件。

### 4.10 WriteHandleFactory — Handle 工厂体系

```
WriteHandleFactory (接口)
  ├── CreateHandleFactory         → 创建 HoodieCreateHandle
  ├── SingleFileHandleCreateFactory → 单文件创建
  ├── AppendHandleFactory         → 创建 HoodieAppendHandle
  ├── FlinkWriteHandleFactory     → Flink 写入 handle 工厂
  └── ExplicitWriteHandleFactory  → 显式指定 handle 的工厂
```

---

## 5. IO 层：HoodieIOFactory / HoodieFileWriter / HoodieFileReader

### 5.1 设计理念

IO 层是 Hudi 写入链路的最底层——所有上层的 Handle 最终都通过 IO 层将数据写入实际的文件格式（Parquet/ORC/HFile/Lance）。IO 层通过抽象工厂模式封装了不同文件格式的差异，上层代码完全不需要关心底层用的是哪种格式。

### 5.2 HoodieIOFactory — 抽象工厂

源码路径：`hudi-common/src/main/java/org/apache/hudi/io/storage/HoodieIOFactory.java`

```java
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public abstract class HoodieIOFactory {
  protected final HoodieStorage storage;

  // 静态工厂方法，通过反射加载具体实现
  public static HoodieIOFactory getIOFactory(HoodieStorage storage) {
    String ioFactoryClass = storage.getConf()
        .getString(HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.key())
        .orElse(HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.defaultValue());
    return (HoodieIOFactory) ReflectionUtils.loadClass(ioFactoryClass, ...);
  }

  // 三个核心工厂方法
  public abstract HoodieFileReaderFactory getReaderFactory(HoodieRecordType recordType);
  public abstract HoodieFileWriterFactory getWriterFactory(HoodieRecordType recordType);
  public abstract FileFormatUtils getFileFormatUtils(HoodieFileFormat fileFormat);
}
```

**为什么通过反射加载 IOFactory？**

因为 `hudi-common` 模块不能直接依赖 Hadoop/Parquet 等具体实现库。通过配置 `hoodie.io.factory.class` 和反射加载，可以在运行时注入具体的 IO 实现，实现了编译时的解耦。

**getFileFormatUtils 的格式分发逻辑：**

```java
public final FileFormatUtils getFileFormatUtils(StoragePath path) {
  if (path.getFileExtension().equals(PARQUET.getFileExtension())) {
    return getFileFormatUtils(HoodieFileFormat.PARQUET);
  } else if (path.getFileExtension().equals(ORC.getFileExtension())) {
    return getFileFormatUtils(HoodieFileFormat.ORC);
  } else if (path.getFileExtension().equals(HFILE.getFileExtension())) {
    return getFileFormatUtils(HoodieFileFormat.HFILE);
  } else if (path.getFileExtension().equals(LANCE.getFileExtension())) {
    return getFileFormatUtils(HoodieFileFormat.LANCE);
  }
  throw new UnsupportedOperationException(...);
}
```

Hudi 支持四种文件格式：Parquet、ORC、HFile、Lance。通过文件扩展名自动选择对应的工具类。

### 5.3 HoodieFileWriter — 文件写入接口

源码路径：`hudi-common/src/main/java/org/apache/hudi/io/storage/HoodieFileWriter.java`

```java
public interface HoodieFileWriter extends AutoCloseable {
  // 是否还能继续写入（文件未达到大小限制）
  boolean canWrite();

  // 带元数据的写入（会自动填充 _hoodie_* 字段）
  void writeWithMetadata(HoodieKey key, HoodieRecord record, 
      HoodieSchema schema, Properties props) throws IOException;

  // 不带元数据的写入
  void write(String recordKey, HoodieRecord record, 
      HoodieSchema schema, Properties props) throws IOException;

  // 获取文件格式元数据（用于生成列统计信息）
  default Object getFileFormatMetadata() {
    throw new UnsupportedOperationException(...);
  }
}
```

**canWrite 的判断逻辑：**

对于 Parquet 格式，canWrite 检查当前文件大小是否超过配置的最大文件大小（`hoodie.parquet.max.file.size`，默认 120MB）。这个机制确保了 Hudi 产出的文件不会过大，有利于查询性能。

**writeWithMetadata vs write 的区别：**

| 方法 | 适用场景 | 说明 |
|------|---------|------|
| `writeWithMetadata` | 新记录插入 | 会自动设置 `_hoodie_commit_time`、`_hoodie_record_key` 等元字段 |
| `write` | 记录迁移/Compaction | 元字段已存在，直接写入即可 |

### 5.4 HoodieFileWriterFactory — 文件写入工厂

源码路径：`hudi-common/src/main/java/org/apache/hudi/io/storage/HoodieFileWriterFactory.java`

```java
public class HoodieFileWriterFactory {
  // 静态方法：根据文件路径扩展名自动选择 writer
  public static HoodieFileWriter getFileWriter(
      String instantTime, StoragePath path, HoodieStorage storage, 
      HoodieConfig config, HoodieSchema schema,
      TaskContextSupplier taskContextSupplier, HoodieRecordType recordType) {
    
    final String extension = FSUtils.getFileExtension(path.getName());
    HoodieFileWriterFactory factory = HoodieIOFactory.getIOFactory(storage)
        .getWriterFactory(recordType);
    return factory.getFileWriterByFormat(extension, instantTime, path, config, schema, 
        taskContextSupplier);
  }

  // 根据扩展名分发到具体的 writer 创建方法
  protected HoodieFileWriter getFileWriterByFormat(String extension, ...) {
    if (PARQUET.getFileExtension().equals(extension)) {
      return newParquetFileWriter(instantTime, path, config, schema, taskContextSupplier);
    }
    if (HFILE.getFileExtension().equals(extension)) {
      return newHFileFileWriter(...);
    }
    if (ORC.getFileExtension().equals(extension)) {
      return newOrcFileWriter(...);
    }
    if (LANCE.getFileExtension().equals(extension)) {
      return newLanceFileWriter(...);
    }
    throw new UnsupportedOperationException(extension + " format not supported yet.");
  }
}
```

**为什么 newParquetFileWriter 等方法是 protected 并且抛出 UnsupportedOperationException？**

因为 `HoodieFileWriterFactory` 在 `hudi-common` 模块中，而 Parquet/ORC/HFile 的具体写入实现在 `hudi-hadoop-common` 等下游模块中。`hudi-common` 中的基类提供了分发框架，具体实现由子类（如 `HoodieAvroFileWriterFactory`）Override。

### 5.5 HoodieFileReader — 文件读取接口

源码路径：`hudi-common/src/main/java/org/apache/hudi/io/storage/HoodieFileReader.java`

```java
public interface HoodieFileReader<T> extends AutoCloseable {
  // 读取 BloomFilter（用于 Bloom Index）
  BloomFilter readBloomFilter();
  
  // 读取 min/max record key（用于快速过滤）
  String[] readMinMaxRecordKeys();
  
  // 过滤候选 key
  Set<Pair<String, Long>> filterRowKeys(Set<String> candidateRowKeys);
  
  // 获取记录迭代器
  ClosableIterator<HoodieRecord<T>> getRecordIterator(HoodieSchema readerSchema, 
      HoodieSchema requestedSchema) throws IOException;
  
  // 获取 record key 迭代器（比全量读取轻量）
  ClosableIterator<String> getRecordKeyIterator() throws IOException;
  
  HoodieSchema getSchema();
  long getTotalRecords();
}
```

**为什么 Reader 需要 BloomFilter 和 min/max key 的方法？**

这些是 Bloom Index 的核心依赖。在 UPSERT 操作中，Index 需要判断一条新记录是否已存在于某个文件中：

1. 先读取 BloomFilter，快速判断 key 是否**可能**存在（假阳性率低）
2. 如果 BloomFilter 命中，再检查 min/max key 范围进一步过滤
3. 最后用 filterRowKeys 做精确匹配

这个三级过滤机制大大减少了实际需要读取的数据量。

### 5.6 BloomFilter 在写入中的角色

```java
public static BloomFilter createBloomFilter(HoodieConfig config) {
  return BloomFilterFactory.createBloomFilter(
      config.getIntOrDefault(HoodieStorageConfig.BLOOM_FILTER_NUM_ENTRIES_VALUE),
      config.getDoubleOrDefault(HoodieStorageConfig.BLOOM_FILTER_FPP_VALUE),
      config.getIntOrDefault(HoodieStorageConfig.BLOOM_FILTER_DYNAMIC_MAX_ENTRIES),
      config.getStringOrDefault(HoodieStorageConfig.BLOOM_FILTER_TYPE));
}

public static boolean enableBloomFilter(boolean populateMetaFields, HoodieConfig config) {
  return populateMetaFields && (
      config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED)
      || config.getString("hoodie.index.type").contains("BLOOM"));
}
```

在写入 Parquet 文件时，如果启用了 BloomFilter，每条记录的 recordKey 会被添加到 BloomFilter 中，最终作为文件 footer 的一部分持久化。后续读取时可以直接从 footer 中加载 BloomFilter 而不需要扫描文件内容。

---

## 6. Marker 机制完整解析

### 6.1 设计理念

Marker（标记）机制是 Hudi 实现**原子写入**和**故障恢复**的关键基础设施。核心思想是：

> 在写入任何数据文件之前，先创建一个对应的 marker 文件；在 commit 成功后，清理所有 marker 文件。如果写入过程中发生故障，通过扫描残留的 marker 文件，就能知道哪些数据文件是"未完成的"，需要被清理。

### 6.2 Marker 文件格式

Marker 文件名格式：`{data_file_name}.marker.{IO_TYPE}`

例如：
- `file-abc-0_0-1-100_20240115120000.parquet.marker.CREATE`
- `file-def-0_0-1-101_20240115120000.parquet.marker.MERGE`
- `file-ghi-0_0-1-102_.log.1_0-1-103.marker.CREATE`（log 文件的 marker）

Marker 文件存放在：`{basePath}/.hoodie/.temp/{instantTime}/` 目录下，保持与数据文件相同的分区目录结构。

### 6.3 WriteMarkers 抽象类

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/marker/WriteMarkers.java`

```java
public abstract class WriteMarkers implements Serializable {
  protected final String basePath;
  protected final transient StoragePath markerDirPath;
  protected final String instantTime;

  // 创建 marker（不检查是否存在）
  public Option<StoragePath> create(String partitionPath, String fileName, IOType type);
  
  // 创建 marker（如果不存在才创建）
  public Option<StoragePath> createIfNotExists(String partitionPath, String fileName, IOType type);
  
  // 创建带早期冲突检测的 marker
  public Option<StoragePath> create(String partitionPath, String fileName, IOType type,
      HoodieWriteConfig writeConfig, String fileId, HoodieActiveTimeline activeTimeline);
  
  // 为 log 文件创建 marker
  public Option<StoragePath> createLogMarkerIfNotExists(...);
  
  // 删除 marker 目录
  public abstract boolean deleteMarkerDir(HoodieEngineContext context, int parallelism);
  
  // 获取所有 CREATE 和 MERGE 类型的数据路径
  public abstract Set<String> createdAndMergedDataPaths(HoodieEngineContext context, int parallelism);
  
  // 获取所有 marker 文件路径
  public abstract Set<String> allMarkerFilePaths() throws IOException;
}
```

### 6.4 Marker 的创建时机

```
写入流程:
                    创建 Marker
  WriteHandle 初始化 ──────────── → 创建数据文件 → 写入数据 → 关闭文件
                    (先于数据文件)

具体来说:
  HoodieCreateHandle: 在构造函数中调用 createPartitionMetadataAndMarkerFile()
  HoodieWriteMergeHandle: 在 initMarkerFileAndFileWriter() 中调用 createMarkerFile()
  HoodieAppendHandle: 通过 LogFileCreationCallback.preFileCreation() 在 log 文件创建前调用
```

**为什么 Marker 必须在数据文件之前创建？**

这保证了一个不变式：**如果数据文件存在，那么对应的 marker 文件一定存在**。故障恢复时，只需扫描 marker 文件就能找到所有可能需要清理的数据文件，而不会遗漏。如果先创建数据文件、再创建 marker，那么在两者之间发生故障就会导致数据文件无法被追踪。

### 6.5 早期冲突检测

WriteMarkers 支持基于 marker 的早期冲突检测（Early Conflict Detection）。在多 writer 场景下，如果两个 writer 同时尝试写入同一个 fileId，marker 机制可以在写入数据之前就检测到冲突。

```java
public Option<StoragePath> create(String partitionPath, String fileName, IOType type,
    HoodieWriteConfig writeConfig, String fileId, HoodieActiveTimeline activeTimeline) {
  if (writeConfig.getWriteConcurrencyMode().isOptimisticConcurrencyControl() 
      && writeConfig.isEarlyConflictDetectionEnable()) {
    // 排除 compaction 和 clustering 操作
    HoodieTimeline pendingCompactionTimeline = activeTimeline.filterPendingCompactionTimeline();
    HoodieTimeline pendingReplaceTimeline = activeTimeline.filterPendingReplaceOrClusteringTimeline();
    if (pendingCompactionTimeline.containsInstant(instantTime) 
        || pendingReplaceTimeline.containsInstant(instantTime)) {
      return create(partitionPath, fileName, type, false);
    }
    return createWithEarlyConflictDetection(...);
  }
  return create(partitionPath, fileName, type, false);
}
```

**好处：** 在 OCC（乐观并发控制）模式下，早期冲突检测可以在写入阶段就失败，而不是等到 commit 阶段才发现冲突。这避免了大量无用的写入工作。

### 6.6 DirectWriteMarkers — 直接文件系统实现

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/marker/DirectWriteMarkers.java`

```java
public class DirectWriteMarkers extends WriteMarkers {
  // 每个 marker 都是文件系统上的一个独立文件
  // 存放在 {basePath}/.hoodie/.temp/{instantTime}/ 下
  
  @Override
  Option<StoragePath> create(String partitionPath, String fileName, 
      IOType type, boolean checkIfExists) {
    StoragePath markerPath = getMarkerPath(partitionPath, fileName, type);
    // 直接在文件系统上创建空文件
    ...
  }
}
```

**优点：** 实现简单，可靠性高，不依赖任何额外服务。

**缺点：** 当写入任务并行度很高时（如几百个 Spark partition），会在 `.temp` 目录下创建大量小文件，对 HDFS NameNode 造成压力。对于对象存储（如 S3），大量小文件的创建和删除操作会非常慢。

### 6.7 TimelineServerBasedWriteMarkers — Timeline Server 代理实现

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/marker/TimelineServerBasedWriteMarkers.java`

```java
public class TimelineServerBasedWriteMarkers extends WriteMarkers {
  private final TimelineServiceClient timelineServiceClient;
  
  // 通过 HTTP 请求发送 marker 信息到 Timeline Server
  // Timeline Server 将多个 marker 聚合到少量文件中批量写入
}
```

**设计思想：** 将大量 marker 的创建请求收拢到 Timeline Server，由 Server 端将多个 marker 信息合并到少量文件中批量写入。例如，1000 个 Spark 分区产生的 1000 个 marker，在 Server 端可能只写入几个文件。

**好处：**
- 大幅减少文件系统的文件数量，对 NameNode / 对象存储友好
- 减少了 `listFiles` 操作的开销（marker 清理时需要列举所有 marker 文件）
- 在云存储（S3/GCS/Azure Blob）场景下性能提升特别显著

**限制：**
- 需要 Embedded Timeline Server 运行
- 不支持 HDFS（因为 HDFS 上 Timeline Server 的 HTTP 通信可能不稳定）

### 6.8 WriteMarkersFactory — 工厂类

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/marker/WriteMarkersFactory.java`

```java
public class WriteMarkersFactory {
  public static WriteMarkers get(MarkerType markerType, HoodieTable table, String instantTime) {
    switch (markerType) {
      case DIRECT:
        return getDirectWriteMarkers(table, instantTime);
      case TIMELINE_SERVER_BASED:
        if (!table.getConfig().isEmbeddedTimelineServerEnabled() 
            && !table.getConfig().isRemoteViewStorageType()) {
          // 降级为 Direct 模式
          return getDirectWriteMarkers(table, instantTime);
        }
        // 检查是否为 HDFS（不支持 Timeline Server 模式）
        if (isHDFS(basePath)) {
          return getDirectWriteMarkers(table, instantTime);
        }
        // 根据表版本选择实现
        return tableVersion >= 8 
            ? new TimelineServerBasedWriteMarkers(table, instantTime)
            : new TimelineServerBasedWriteMarkersV1(table, instantTime);
    }
  }
}
```

注意工厂中的**优雅降级**逻辑：如果配置了 Timeline Server 模式但条件不满足（Server 未启动、HDFS 环境），自动降级为 Direct 模式，确保系统始终可用。

### 6.9 Marker 在故障恢复中的作用

```
正常流程:
  1. 开始写入 → 创建 marker 目录和 marker 文件
  2. 写入数据文件
  3. Commit 成功 → 删除 marker 目录
  4. 完成

故障恢复流程:
  1. 检测到未完成的 instant（有 .inflight 但无 .commit）
  2. 扫描对应的 marker 目录
  3. 通过 createdAndMergedDataPaths() 找到所有 CREATE/MERGE 类型的数据文件
  4. 删除这些"脏"数据文件
  5. 回滚 instant
  6. 清理 marker 目录
```

**为什么只清理 CREATE 和 MERGE 类型？**

因为 APPEND 类型对应的是 log 文件的追加，这些追加操作是基于已有 log 文件的，回滚时只需要将 log 文件截断到追加前的大小即可，不需要删除整个文件。而 CREATE 和 MERGE 类型对应的是全新创建的 base 文件，需要完整删除。

---

## 7. 写入操作类型全景

### 7.1 WriteOperationType 枚举

源码路径：`hudi-common/src/main/java/org/apache/hudi/common/model/WriteOperationType.java`

```java
public enum WriteOperationType {
  INSERT("insert"),
  INSERT_PREPPED("insert_prepped"),
  UPSERT("upsert"),
  UPSERT_PREPPED("upsert_prepped"),
  BULK_INSERT("bulk_insert"),
  BULK_INSERT_PREPPED("bulk_insert_prepped"),
  DELETE("delete"),
  DELETE_PREPPED("delete_prepped"),
  BOOTSTRAP("bootstrap"),
  INSERT_OVERWRITE("insert_overwrite"),
  BUCKET_RESCALE("bucket_rescale"),
  CLUSTER("cluster"),
  DELETE_PARTITION("delete_partition"),
  INSERT_OVERWRITE_TABLE("insert_overwrite_table"),
  COMPACT("compact"),
  INDEX("index"),
  ALTER_SCHEMA("alter_schema"),
  LOG_COMPACT("log_compact"),
  UNKNOWN("unknown");
}
```

### 7.2 各操作类型详解

#### INSERT — 纯插入

- **语义：** 所有记录都是新记录，直接写入
- **Index：** 不查找已有记录（但仍会更新索引）
- **Handle：** `HoodieCreateHandle`（COW 和 MOR）
- **适用场景：** 确定数据没有重复时的批量导入

**为什么不走 Index？** 因为调用方已经保证了没有重复，跳过 Index 查找可以避免大量的 IO 开销。

#### UPSERT — 更新+插入

- **语义：** 如果 key 已存在则更新，否则插入
- **Index：** 必须查找（tagLocation）
- **Handle（COW）：** 已有记录用 `HoodieWriteMergeHandle`，新记录用 `HoodieCreateHandle`
- **Handle（MOR）：** 已有记录用 `HoodieAppendHandle`，新记录用 `HoodieCreateHandle`
- **适用场景：** 最通用的写入方式，适用于 CDC 同步、流式摄入等

**为什么 UPSERT 是最常用的操作？** 因为在数据湖场景中，很少有数据源能保证"绝对不会产生重复"。UPSERT 以一定的性能代价（Index 查找）换取了数据正确性保证。

#### BULK_INSERT — 批量导入

- **语义：** 大批量数据的快速写入
- **特点：** 使用全局排序和 RDD 分区来优化写入性能，不做去重
- **Handle：** 通常直接写 Parquet 文件，绕过单条记录的 Handle 机制
- **适用场景：** 首次建表、历史数据回灌

**为什么 BULK_INSERT 比 INSERT 快？** BULK_INSERT 会根据 partitionPath 对数据进行全局排序，使得同一分区的数据连续写入同一文件，减少了文件切换开销。同时跳过了 Index 查找和单条记录的 WriteStatus 追踪。

#### DELETE — 删除

- **语义：** 删除指定 key 的记录
- **Index：** 必须查找（找到记录所在位置才能删除）
- **Handle（COW）：** `HoodieWriteMergeHandle`（遍历旧文件时跳过被删除的 key）
- **Handle（MOR）：** `HoodieAppendHandle`（写入 DeleteBlock）
- **适用场景：** GDPR 合规、数据纠正

#### INSERT_OVERWRITE — 静态分区覆写

- **语义：** 用新数据完全替换指定分区的旧数据
- **特点：** 不需要 Index 查找，直接用新文件替换旧文件
- **适用场景：** ETL 管道中的分区级别全量刷新

**好处：** 相比 UPSERT，INSERT_OVERWRITE 跳过了 Index 查找和 Merge 过程，对于"重写某个分区"的场景效率高得多。

#### INSERT_OVERWRITE_TABLE — 动态分区覆写

- **语义：** 用新数据完全替换整张表中涉及到的分区
- **与 INSERT_OVERWRITE 的区别：** INSERT_OVERWRITE 需要用户显式指定分区；INSERT_OVERWRITE_TABLE 根据新数据中出现的分区自动决定覆盖哪些分区
- **适用场景：** 数据源产出的分区是动态的，无法提前知道

#### DELETE_PARTITION — 删除分区

- **语义：** 删除整个分区的数据
- **适用场景：** 数据过期清理、分区级别的数据撤回

### 7.3 _PREPPED 变体的意义

每种主要操作都有对应的 `_PREPPED` 变体（如 `INSERT_PREPPED`、`UPSERT_PREPPED`）：

```java
public static boolean isPreppedWriteOperation(WriteOperationType operationType) {
  return operationType == BULK_INSERT_PREPPED 
      || operationType == INSERT_PREPPED 
      || operationType == UPSERT_PREPPED 
      || operationType == DELETE_PREPPED;
}
```

**_PREPPED 的含义：** 数据已经被"预处理"过了——Key 已经提取、Index 已经 tagged、分区已经确定。Hudi 可以跳过这些步骤直接写入。

**为什么需要 PREPPED？**

在某些场景下（如 Hudi 内部的 Compaction、Clustering），数据已经是 Hudi 格式的，不需要再次进行 Key 生成和 Index 查找。使用 PREPPED 变体可以跳过这些不必要的步骤。

### 7.4 操作分类辅助方法

WriteOperationType 提供了一系列静态方法用于操作分类：

```java
// 是否会改变已有记录
static boolean isChangingRecords(WriteOperationType op) {
  return op == UPSERT || op == UPSERT_PREPPED || op == DELETE || op == DELETE_PREPPED;
}

// 是否是覆写操作
static boolean isOverwrite(WriteOperationType op) {
  return op == INSERT_OVERWRITE || op == INSERT_OVERWRITE_TABLE;
}

// 是否是纯插入（不替换已有数据）
static boolean isInsertWithoutReplace(WriteOperationType op) {
  return op == INSERT || op == INSERT_PREPPED || op == BULK_INSERT || op == BULK_INSERT_PREPPED;
}

// 是否可以更新 schema
static boolean canUpdateSchema(WriteOperationType op) {
  return !(op == CLUSTER || op == COMPACT || op == INDEX || op == LOG_COMPACT);
}
```

**为什么 CLUSTER/COMPACT/INDEX/LOG_COMPACT 不能更新 Schema？**

这些是内部维护操作，它们操作的是已有数据，不应该改变表的 Schema。Schema 变更只应该由用户显式的写入操作（INSERT/UPSERT 等）触发。

### 7.5 内部维护操作

| 操作 | 说明 |
|------|------|
| `COMPACT` | 将 MOR 表的 delta log 合并到 base file |
| `LOG_COMPACT` | 将多个小 log 文件合并为大 log 文件 |
| `CLUSTER` | 重新组织文件布局（如按排序键重排数据） |
| `INDEX` | 构建/更新索引 |
| `BOOTSTRAP` | 将已有的非 Hudi 数据接入 Hudi 管理 |
| `ALTER_SCHEMA` | Schema 变更操作 |
| `BUCKET_RESCALE` | Bucket Index 的缩扩容 |

---

## 8. 全链路串联：一条记录的完整写入之旅

让我们以一个 UPSERT 操作为例，从一条 Spark DataFrame 中的 Row 开始，追踪它从头到尾经历的每一个步骤。

### 8.1 阶段一：数据准备与 Key 生成

```
用户调用: df.write.format("hudi").mode("append").option("hoodie.datasource.write.operation", "upsert").save(path)

Step 1: SparkDataSource 接收到写入请求
  → 解析配置，确定操作类型为 UPSERT
  → 调用 HoodieSparkSqlWriter

Step 2: KeyGenerator 提取 HoodieKey
  → SimpleKeyGenerator.getKey(record)
  → recordKey = record.getString("user_id")  // "user_123"
  → partitionPath = record.getString("dt")   // "2024/01/15"
  → return new HoodieKey("user_123", "2024/01/15")

Step 3: 构建 HoodieRecord
  → new HoodieSparkRecord(key, internalRow)
  → currentLocation = null  (还不知道记录是否已存在)
  → newLocation = null
  → sealed = false
```

### 8.2 阶段二：Index 查找与标记

```
Step 4: Index.tagLocation()
  → 根据 recordKey "user_123" 和 partitionPath "2024/01/15"
  → 查找 BloomFilter / Bucket Index / 其他 Index
  
  情况 A (记录已存在):
    → record.setCurrentLocation(new HoodieRecordLocation("20240114", "file-abc"))
    → 标记为 UPDATE
  
  情况 B (记录不存在):
    → currentLocation 保持 null
    → 标记为 INSERT

Step 5: record.seal()
  → 防止在后续 Shuffle 过程中被意外修改
```

### 8.3 阶段三：分桶与路由

```
Step 6: 根据 currentLocation 分桶
  
  已有记录 (currentLocation != null):
    → 路由到 file-abc 所在的 partition (Spark partition)
    → 由 MergeHandle 或 AppendHandle 处理
  
  新记录 (currentLocation == null):
    → 路由到可以接受新插入的文件组
    → 由 CreateHandle 处理
```

### 8.4 阶段四：Marker 创建

```
Step 7: Handle 初始化时创建 Marker

  对于 CreateHandle:
    → WriteMarkersFactory.get(markerType, table, instantTime)
    → markers.create("2024/01/15", "file-new-0_0-1-100_20240115120000.parquet", IOType.CREATE)
    → 在 .hoodie/.temp/20240115120000/2024/01/15/ 下创建 marker 文件

  对于 MergeHandle (COW):
    → markers.create("2024/01/15", "file-abc-0_0-2-101_20240115120000.parquet", IOType.MERGE)

  对于 AppendHandle (MOR):
    → 通过 LogFileCreationCallback 在 log 文件创建前创建 marker
```

### 8.5 阶段五：数据写入

```
Step 8: Handle 执行写入

  === COW MergeHandle 路径 ===
  
  8a. 读取旧 base file (file-abc.parquet)
  8b. 遍历旧文件中的每条记录:
      → 检查 keyToNewRecords 中是否有对应的 key
      → 有: 调用 RecordMerger.merge(oldRecord, newRecord, ...)
           → 将 merge 结果写入新文件
      → 无: 直接将旧记录写入新文件
  8c. 遍历 keyToNewRecords 中剩余的记录（纯新插入）
      → 写入新文件
  8d. 关闭文件

  === MOR AppendHandle 路径 ===
  
  8a. 初始化 LogWriter
  8b. 将新记录缓冲到 recordList
  8c. 当缓冲达到 maxBlockSize:
      → 构建 HoodieDataBlock (Parquet/Avro/HFile 格式)
      → 通过 LogWriter 追加到 delta log 文件
  8d. 如果有删除记录:
      → 构建 HoodieDeleteBlock
      → 追加到 delta log 文件
  8e. 关闭 LogWriter

  === 底层 FileWriter 路径 (以 Parquet 为例) ===
  
  8x. HoodieFileWriterFactory.getFileWriter(instantTime, path, ...)
      → 根据文件扩展名选择 Parquet Writer
  8y. parquetWriter.writeWithMetadata(key, record, schema, props)
      → 填充 _hoodie_commit_time, _hoodie_record_key 等元字段
      → 将记录序列化为 Parquet 格式
      → 如果启用了 BloomFilter: bloomFilter.add(recordKey)
      → 写入 Parquet Row Group
  8z. parquetWriter.close()
      → 将 BloomFilter 写入 Parquet footer
      → 将文件 flush 到文件系统
```

### 8.6 阶段六：记录状态更新

```
Step 9: 更新记录状态

  record.unseal()
  record.setNewLocation(new HoodieRecordLocation("20240115120000", "file-new"))
  record.seal()
  
  writeStatus.markSuccess(record, metadata)
  record.deflate()  // data = null，释放内存
```

### 8.7 阶段七：Commit

```
Step 10: 收集所有 WriteStatus

  → 每个 Handle 返回 List<WriteStatus>
  → 聚合所有分区的 WriteStatus

Step 11: 生成 CommitMetadata

  → 包含每个文件的写入统计（recordsWritten, recordsDeleted, fileSize 等）
  → 包含 WriteOperationType, Schema 信息

Step 12: 写入 Timeline

  → 将 .inflight 文件重命名为 .commit（原子操作）
  → 此时 commit 正式生效

Step 13: 更新 Index

  → 将 newLocation 信息更新到 Index 中
  → 下次写入时 Index 可以返回最新的位置

Step 14: 清理 Marker

  → WriteMarkersFactory.get(...).deleteMarkerDir(context, parallelism)
  → 删除 .hoodie/.temp/{instantTime}/ 目录
```

### 8.8 故障场景分析

如果在 Step 8（数据写入）过程中发生故障：

```
恢复流程:
  1. 检测到 instant "20240115120000" 有 .inflight 但无 .commit
  2. 扫描 .hoodie/.temp/20240115120000/ 下的所有 marker 文件
  3. 找到:
     - file-new-0_0-1-100_20240115120000.parquet.marker.CREATE
     - file-abc-0_0-2-101_20240115120000.parquet.marker.MERGE
  4. 删除对应的数据文件:
     - 删除 2024/01/15/file-new-0_0-1-100_20240115120000.parquet
     - 删除 2024/01/15/file-abc-0_0-2-101_20240115120000.parquet
  5. 将 .inflight 标记为 .rollback
  6. 清理 marker 目录
  7. 表恢复到写入前的一致状态
```

---

## 总结

Hudi 的写入路径是一个精心设计的分层架构：

| 层级 | 组件 | 职责 |
|------|------|------|
| **数据模型层** | HoodieRecord / HoodieKey / HoodieRecordLocation | 定义记录的身份、数据和位置 |
| **Key 生成层** | KeyGenerator 体系 | 从原始数据提取主键和分区路径 |
| **合并决策层** | RecordMerger 体系 | 决定相同 key 的多个版本如何合并 |
| **写入执行层** | WriteHandle 体系 | 执行具体的文件创建、合并、追加操作 |
| **IO 层** | HoodieIOFactory / FileWriter / FileReader | 屏蔽不同文件格式的差异 |
| **可靠性层** | Marker 机制 | 保证写入的原子性和故障可恢复性 |
| **操作语义层** | WriteOperationType | 定义不同写入操作的语义和行为 |

每一层都遵循了**接口抽象 + 可插拔实现**的设计原则，使得 Hudi 能够：

1. 支持多种计算引擎（Spark/Flink/Java）而不改变核心逻辑
2. 支持多种文件格式（Parquet/ORC/HFile/Lance）而不影响上层
3. 支持多种合并策略而不需要修改写入路径
4. 支持多种 Index 类型而不影响数据写入
5. 在任何故障场景下都能恢复到一致状态

理解了这条从 Record 到 File 的完整链路，就掌握了 Hudi 写入模块的核心骨架。后续深入研究 Index、Compaction、Clustering 等子系统时，都可以在这个骨架上定位各个子系统的切入点和交互方式。
