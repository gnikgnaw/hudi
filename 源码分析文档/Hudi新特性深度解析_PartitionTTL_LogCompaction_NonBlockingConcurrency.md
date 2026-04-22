# Hudi 新特性深度解析：Partition TTL / Log Compaction / Non-Blocking Concurrency Control

> 基于 Apache Hudi v1.2.0-SNAPSHOT 源码分析
> 文档覆盖范围：Partition TTL、Log Compaction、NBCC、Record Position Merge、Shredded Variant、VECTOR Search、RFC 机制

---

## 目录

- [第一部分：Partition TTL — 分区自动过期清理](#第一部分partition-ttl--分区自动过期清理)
- [第二部分：Log Compaction — 轻量级日志压缩](#第二部分log-compaction--轻量级日志压缩)
- [第三部分：Non-Blocking Concurrency Control (NBCC)](#第三部分non-blocking-concurrency-control-nbcc)
- [第四部分：Record Position Merge — 基于位置的合并优化](#第四部分record-position-merge--基于位置的合并优化)
- [第五部分：Shredded Variant / VECTOR Search 等前沿特性](#第五部分shredded-variant--vector-search-等前沿特性)
- [第六部分：新特性路线图与 RFC](#第六部分新特性路线图与-rfc)

---

# 第一部分：Partition TTL — 分区自动过期清理

## 1. 解决什么问题

Partition TTL 解决的核心问题是**时序数据的自动化生命周期管理**。

### 业务痛点

在大规模时序数据场景（日志分析、IoT 数据、广告点击流）中，数据按日期分区存储，随着时间推移会产生以下问题：

1. **存储成本失控**：历史分区可能占用总存储的 90%+，但业务只关心最近 N 天数据
2. **元数据操作变慢**：分区数量过多导致 FSView 构建、分区列表获取等操作耗时增加
3. **表服务调度复杂**：Compaction/Clustering 需要扫描所有分区，大量过期分区拖慢调度

### 传统方案的局限

源码证据：`PartitionTTLStrategy.java` 第 34-37 行注释说明了设计动机：
```java
/**
 * Strategy for partition-level ttl management.
 */
public abstract class PartitionTTLStrategy implements TTLStrategy, Serializable
```

传统做法是手动编写脚本调用 `DELETE_PARTITION`，但存在以下问题：
- 需要额外的调度系统（Airflow/DolphinScheduler）
- 策略逻辑分散在外部脚本中，与 Hudi 表配置脱节
- 无法与 Hudi 的表服务（Table Service）协调，可能与 Compaction/Clustering 冲突

### 实际应用场景

1. **日志分析平台**：保留最近 90 天日志，自动删除更早的分区
2. **IoT 数据湖**：设备数据按天分区，只保留最近 180 天用于实时分析
3. **广告点击流**：点击数据按小时分区，保留最近 30 天用于报表生成

## 2. 有什么坑

### 坑1：TTL 配置默认不启用

源码证据：`HoodieTTLConfig.java` 第 66-71 行：
```java
public static final ConfigProperty<Integer> DAYS_RETAIN = ConfigProperty
    .key(PARTITION_TTL_STRATEGY_PARAM_PREFIX + "days.retain")
    .defaultValue(-1)  // 默认 -1 表示不启用
    .sinceVersion("1.0.0")
    .markAdvanced()
    .withDocumentation("Partition ttl management KEEP_BY_TIME strategy days retain");
```

**陷阱**：即使设置了 `hoodie.partition.ttl.inline=true`，如果 `days.retain=-1`（默认值），TTL 不会执行任何操作。必须显式设置正数。

### 坑2：单次删除数量限制

源码证据：`KeepByTimeStrategy.java` 第 60-64 行：
```java
List<String> expiredPartitions = getExpiredPartitionsForTimeStrategy(getPartitionPathsForTTL());
int limit = writeConfig.getPartitionTTLMaxPartitionsToDelete();
log.info("Total expired partitions count {}, limit {}", expiredPartitions.size(), limit);
return expiredPartitions.stream()
    .limit(limit) // Avoid a single replace commit too large
    .collect(Collectors.toList());
```

**陷阱**：默认最多删除 1000 个分区（`HoodieTTLConfig.MAX_PARTITION_TO_DELETE`）。如果首次启用 TTL 时有大量过期分区，需要多次执行才能全部清理。

### 坑3：前置条件检查严格

源码证据：`KeepByTimeStrategy.java` 第 54-58 行：
```java
Option<HoodieInstant> lastCompletedInstant = hoodieTable.getActiveTimeline().filterCompletedInstants().lastInstant();
if (!lastCompletedInstant.isPresent() || ttlInMilis <= 0
    || !hoodieTable.getMetaClient().getTableConfig().getPartitionFields().isPresent()) {
  return Collections.emptyList();
}
```

**陷阱**：三个条件任一不满足都会跳过 TTL：
- 表还没有完成过任何 commit
- TTL 配置无效（≤0）
- 表未启用分区（非分区表）

### 坑4：Late-arriving data 的影响

源码证据：`KeepByTimeStrategy.java` 第 82-92 行：
```java
private Map<String, Option<String>> getLastCommitTimeForPartitions(List<String> partitionPaths) {
    return hoodieTable.getContext().map(partitionPaths, partitionPath -> {
      Option<String> partitionLastModifiedTime = hoodieTable.getHoodieView()
          .getLatestFileSlicesBeforeOrOn(partitionPath, instantTime, true)
          .map(FileSlice::getBaseInstantTime)
          .max(Comparator.naturalOrder())  // 取最大的 instantTime
          .map(Option::ofNullable)
          .orElse(Option.empty());
      return Pair.of(partitionPath, partitionLastModifiedTime);
    }, statsParallelism).stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
}
```

**陷阱**：使用 `KeepByTimeStrategy` 时，如果旧分区收到迟到数据（Late-arriving data），其最后修改时间会更新，导致该分区不会被过期。如果需要"无论是否更新，超过 N 天一律删除"的语义，应使用 `KeepByCreationTimeStrategy`。

### 坑5：TTL 与 Clean 的协调

**陷阱**：TTL 删除分区后生成 `REPLACE_COMMIT`，但被替换的旧文件需要 Clean 服务来实际释放存储空间。如果 Clean 配置不当（如 `commits.retained` 过大），存储空间可能不会立即释放。

### 性能陷阱

源码证据：`PartitionTTLStrategy.java` 第 61-73 行：
```java
protected List<String> getPartitionPathsForTTL() {
    String partitionSelected = writeConfig.getPartitionTTLPartitionSelected();
    HoodieTimer timer = HoodieTimer.start();
    List<String> partitionsForTTL;
    if (StringUtils.isNullOrEmpty(partitionSelected)) {
      // Return all partition paths.
      partitionsForTTL = FSUtils.getAllPartitionPaths(hoodieTable.getContext(), 
          hoodieTable.getMetaClient(), writeConfig.getMetadataConfig());
    } else {
      partitionsForTTL = Arrays.asList(partitionSelected.split(","));
    }
    log.info("Get partitions for ttl cost {} ms", timer.endTimer());
    return partitionsForTTL;
}
```

**陷阱**：如果表有数万个分区，`getAllPartitionPaths()` 会扫描所有分区目录，耗时可能达到分钟级。建议通过 `partition.selected` 配置只对特定分区启用 TTL。

## 3. 核心概念解释

### TTLStrategy 接口层次

源码证据：`TTLStrategy.java` 是一个空标记接口（Marker Interface）：
```java
/**
 * Strategy for ttl management.
 */
public interface TTLStrategy {
}
```

这是面向未来扩展的设计，为未来可能出现的 Record-Level TTL 预留空间。

### PartitionTTLStrategy 抽象基类

源码证据：`PartitionTTLStrategy.java` 第 37-54 行：
```java
public abstract class PartitionTTLStrategy implements TTLStrategy, Serializable {
  protected final HoodieTable hoodieTable;
  protected final HoodieWriteConfig writeConfig;
  protected final String instantTime;

  /**
   * Get expired partition paths for a specific partition ttl strategy.
   */
  public abstract List<String> getExpiredPartitionPaths();
}
```

**核心方法**：
- `getExpiredPartitionPaths()`：由子类实现，返回过期分区列表
- `getPartitionPathsForTTL()`：模板方法，提供获取候选分区的通用逻辑

### 两种内建策略

| 策略 | 参考时间 | 信息来源 | 适用场景 |
|------|---------|---------|---------|
| KeepByTimeStrategy | 分区最后修改时间 | FileSlice 的 baseInstantTime | 按数据活跃度过期 |
| KeepByCreationTimeStrategy | 分区创建时间 | HoodiePartitionMetadata 文件 | 按绝对年龄过期 |

### 与其他系统的对比

- **Hive RETENTION**：仅元数据标记，不删除数据
- **Iceberg Expire Snapshots**：删除快照，但不删除分区
- **Delta Lake VACUUM**：删除旧版本文件，但不删除分区
- **Hudi Partition TTL**：真正删除整个分区，生成 REPLACE_COMMIT

## 4. 设计理念

### 声明式管理 vs 命令式脚本

源码证据：`HoodieTTLConfig.java` 的配置项设计体现了声明式理念：
```java
public static final ConfigProperty<Boolean> INLINE_PARTITION_TTL = ConfigProperty
    .key("hoodie.partition.ttl.inline")
    .defaultValue(false)
    .sinceVersion("1.0.0")
    .markAdvanced()
    .withDocumentation("When enabled, the partition ttl management service is invoked immediately after each commit, "
        + "to delete exipired partitions");
```

**设计权衡**：
- **优势**：策略跟随表元数据演进，与表服务天然协调，配置集中管理
- **劣势**：灵活性不如外部脚本（如无法根据业务逻辑动态调整）

### 可扩展策略框架

源码证据：`PartitionTTLStrategyType.java` 枚举 + `HoodiePartitionTTLStrategyFactory.java` 工厂模式：
```java
public static PartitionTTLStrategy createStrategy(
    HoodieTable hoodieTable, TypedProperties props, String instantTime) throws IOException {
  String strategyClassName = getPartitionTTLStrategyClassName(props);
  try {
    return (PartitionTTLStrategy) ReflectionUtils.loadClass(strategyClassName,
        new Class<?>[] {HoodieTable.class, String.class}, hoodieTable, instantTime);
  } catch (Throwable e) {
    throw new IOException("Could not load partition ttl management strategy class " + strategyClassName, e);
  }
}
```

**设计哲学**：
- 内建策略通过枚举类型配置（`KEEP_BY_TIME` / `KEEP_BY_CREATION_TIME`）
- 自定义策略通过完整类名配置（`hoodie.partition.ttl.strategy.class`）
- 类名配置优先级高于枚举类型，确保用户自定义策略不被覆盖

### 与表服务的协调

源码证据：`SparkPartitionTTLActionExecutor.java` 第 355-359 行：
```java
} catch (HoodieDeletePartitionPendingTableServiceException e) {
  // 如果分区正在被其他 Table Service 操作，跳过
  LOG.info("Partition is under table service, do nothing, call delete partition next time.");
  return emptyResult;
}
```

**设计理念**：TTL 操作感知正在进行的 Compaction/Clustering，通过异常机制优雅跳过冲突分区，而非强制中断其他表服务。

### 架构演进历史

- **0.x 版本**：无内建 TTL 支持，依赖外部脚本
- **1.0.0 版本**：引入 Partition TTL（RFC-65），支持 Inline 和 Async 两种模式
- **未来方向**：可能扩展到 Record-Level TTL（TTLStrategy 接口预留了扩展空间）

## 1.1 为什么需要 Partition TTL

### 传统 Clean 的局限性

在 Hudi 的传统数据管理体系中，Clean（清理）服务的职责是清理旧版本文件——即删除不再被任何查询引用的过期文件版本（FileVersion）。例如，当你配置 `hoodie.cleaner.commits.retained=3` 时，Clean 会保留最近 3 个 commit 所引用的文件，删除更早的文件版本。

但请注意一个关键问题：**Clean 操作的粒度是文件版本（File Version），而不是分区（Partition）**。它永远不会删除一整个分区目录。

在大量时序数据场景中（如日志分析、IoT 数据、广告点击流），数据通常按日期分区：

```
/data/hudi_table/dt=2024-01-01/
/data/hudi_table/dt=2024-01-02/
...
/data/hudi_table/dt=2025-04-15/
```

随着时间推移，历史分区越来越多，但业务方通常只关心最近 N 天的数据。这些过期分区：
- **占用大量存储空间**（可能占总存储的 90%+）
- **拖慢元数据操作**（如 FSView 构建、Partition 列表获取）
- **增加 Compaction/Clustering 的调度复杂度**

传统做法是手动编写脚本，定期调用 `DELETE_PARTITION` 来删除过期分区。但这种方式：
- 需要额外的调度系统（Airflow/DolphinScheduler）
- 策略逻辑分散在外部脚本中，与 Hudi 表配置脱节
- 无法与 Hudi 的表服务（Table Service）协调

### Partition TTL 的设计目标

Partition TTL（RFC-65，JIRA: HUDI-5823）的设计目标是：

1. **声明式管理**：通过配置参数声明分区保留策略，Hudi 自动执行
2. **可扩展策略框架**：支持不同的过期判定策略（按最后修改时间、按创建时间等）
3. **与表服务协调**：TTL 操作能感知正在进行的 Compaction/Clustering/Clean
4. **支持 Inline 和 Async 两种模式**：像 Clean/Compaction 一样，可以内联执行或异步执行

> **为什么这么设计？** 将分区生命周期管理纳入 Hudi 的 Table Service 体系，而非依赖外部脚本，可以确保：（1）过期策略跟随表元数据一起演进；（2）与其他 Table Service 的并发控制天然协调；（3）配置集中管理，降低运维复杂度。

## 1.2 TTLStrategy 接口设计

### 策略接口继承体系

```
TTLStrategy (顶层标记接口)
    └── PartitionTTLStrategy (分区级别 TTL 抽象基类)
            ├── KeepByTimeStrategy (按最后修改时间保留)
            └── KeepByCreationTimeStrategy (按创建时间保留)
```

#### TTLStrategy 顶层接口

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/ttl/strategy/TTLStrategy.java`

```java
/**
 * Strategy for ttl management.
 */
public interface TTLStrategy {
}
```

这是一个纯标记接口（Marker Interface），没有定义任何方法。

> **为什么是空接口？** 这是面向未来扩展的设计。TTLStrategy 的设计考虑了未来可能出现的非分区级别 TTL（如 Record-Level TTL），因此顶层接口保持空白，让不同粒度的 TTL 策略各自定义所需的抽象方法。这种 "thin interface + abstract class" 的模式在 Hudi 中很常见（类比 IndexType 和 HoodieIndex 的关系）。

#### PartitionTTLStrategy 抽象基类

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/ttl/strategy/PartitionTTLStrategy.java`

```java
@Slf4j
public abstract class PartitionTTLStrategy implements TTLStrategy, Serializable {

  protected final HoodieTable hoodieTable;
  protected final HoodieWriteConfig writeConfig;
  protected final String instantTime;

  public PartitionTTLStrategy(HoodieTable hoodieTable, String instantTime) {
    this.writeConfig = hoodieTable.getConfig();
    this.hoodieTable = hoodieTable;
    this.instantTime = instantTime;
  }

  /**
   * Get expired partition paths for a specific partition ttl strategy.
   */
  public abstract List<String> getExpiredPartitionPaths();

  /**
   * Scan and list all partitions for partition ttl management.
   */
  protected List<String> getPartitionPathsForTTL() {
    String partitionSelected = writeConfig.getPartitionTTLPartitionSelected();
    HoodieTimer timer = HoodieTimer.start();
    List<String> partitionsForTTL;
    if (StringUtils.isNullOrEmpty(partitionSelected)) {
      // Return all partition paths.
      partitionsForTTL = FSUtils.getAllPartitionPaths(
          hoodieTable.getContext(), hoodieTable.getMetaClient(), writeConfig.getMetadataConfig());
    } else {
      partitionsForTTL = Arrays.asList(partitionSelected.split(","));
    }
    log.info("Get partitions for ttl cost {} ms", timer.endTimer());
    return partitionsForTTL;
  }
}
```

关键设计要点：

1. **`getExpiredPartitionPaths()`**：核心抽象方法，由子类实现，返回过期分区路径列表
2. **`getPartitionPathsForTTL()`**：模板方法，提供获取候选分区列表的通用逻辑
3. **支持选择性分区管理**：通过 `partitionSelected` 配置，可以只对特定分区启用 TTL，而非全表扫描
4. **使用 `HoodieTimer` 计时**：用于监控 TTL 扫描性能

> **好处是什么？** 这种 Template Method 设计让所有子类共享分区列表获取逻辑，同时允许不同策略自定义"过期"的判定规则。新增策略只需实现 `getExpiredPartitionPaths()` 即可。

## 1.3 KeepByTimeStrategy 实现详解

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/ttl/strategy/KeepByTimeStrategy.java`

### 整体逻辑流程

```
1. 计算 ttlInMilis = 配置天数 * 86400000 (毫秒)
2. 获取所有候选分区列表
3. 对每个分区，找到其最后一次 commit 时间
4. 判断: 当前时间 - 最后commit时间 > TTL? 如果是，标记为过期
5. 按配置的最大删除数量截断
```

### 核心代码解析

```java
@Slf4j
public class KeepByTimeStrategy extends PartitionTTLStrategy {

  protected final long ttlInMilis;

  public KeepByTimeStrategy(HoodieTable hoodieTable, String instantTime) {
    super(hoodieTable, instantTime);
    // 将天数配置转换为毫秒
    this.ttlInMilis = writeConfig.getPartitionTTLStrategyDaysRetain() * 1000 * 3600 * 24;
  }

  @Override
  public List<String> getExpiredPartitionPaths() {
    // 前置条件检查
    Option<HoodieInstant> lastCompletedInstant =
        hoodieTable.getActiveTimeline().filterCompletedInstants().lastInstant();
    if (!lastCompletedInstant.isPresent() || ttlInMilis <= 0
        || !hoodieTable.getMetaClient().getTableConfig().getPartitionFields().isPresent()) {
      return Collections.emptyList();
    }
    // 获取过期分区
    List<String> expiredPartitions = getExpiredPartitionsForTimeStrategy(getPartitionPathsForTTL());
    int limit = writeConfig.getPartitionTTLMaxPartitionsToDelete();
    log.info("Total expired partitions count {}, limit {}", expiredPartitions.size(), limit);
    return expiredPartitions.stream()
        .limit(limit)  // 避免单次 replace commit 过大
        .collect(Collectors.toList());
  }
```

**设计亮点分析：**

1. **前置条件检查的三重保障**：
   - `lastCompletedInstant` 不存在 → 表还没有完成过任何 commit，不执行 TTL
   - `ttlInMilis <= 0` → TTL 配置无效（默认 -1 天），不执行
   - `partitionFields` 不存在 → 表未启用分区，分区 TTL 没有意义

2. **`limit` 截断机制**：防止一次性删除过多分区导致单个 REPLACE_COMMIT 过大，默认最多删 1000 个分区

### 获取分区最后修改时间

```java
private Map<String, Option<String>> getLastCommitTimeForPartitions(List<String> partitionPaths) {
    int statsParallelism = Math.min(partitionPaths.size(), 200);
    return hoodieTable.getContext().map(partitionPaths, partitionPath -> {
      Option<String> partitionLastModifiedTime = hoodieTable.getHoodieView()
          .getLatestFileSlicesBeforeOrOn(partitionPath, instantTime, true)
          .map(FileSlice::getBaseInstantTime)
          .max(Comparator.naturalOrder())
          .map(Option::ofNullable)
          .orElse(Option.empty());
      return Pair.of(partitionPath, partitionLastModifiedTime);
    }, statsParallelism).stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
}
```

**深度解析这段代码：**

1. **`statsParallelism = Math.min(partitionPaths.size(), 200)`**：控制并行度上限为 200，避免分区数过多时创建过多线程
2. **`getLatestFileSlicesBeforeOrOn(partitionPath, instantTime, true)`**：通过 FileSystemView 获取分区在指定时间点之前的最新 FileSlice
3. **`FileSlice::getBaseInstantTime`**：每个 FileSlice 的 baseInstantTime 就是创建该 FileSlice 的 commit 时间
4. **`max(Comparator.naturalOrder())`**：取所有 FileSlice 中最大的 instantTime，即分区的最后修改时间

> **为什么用 FileSlice 的 baseInstantTime 而不是直接查 commit 元数据？** 因为 commit 元数据只记录了哪些 partition 被修改，但不记录每个 partition 的最后修改时间。而 FileSlice 的 baseInstantTime 天然包含了这个信息，且可以通过 FileSystemView 高效获取，不需要扫描所有 commit 的元数据。

### 过期判定逻辑

```java
protected boolean isPartitionExpired(String referenceTime) {
    String expiredTime = instantTimePlusMillis(referenceTime, ttlInMilis);
    return fixInstantTimeCompatibility(instantTime).compareTo(expiredTime) > 0;
}
```

这段代码的语义是：`referenceTime + TTL < currentTime`，即"参考时间加上 TTL 后的时间点"如果早于当前时间，则分区已过期。

`fixInstantTimeCompatibility` 处理 Hudi 不同版本间 instant time 格式的兼容性问题（旧版本用 `yyyyMMddHHmmss`，新版本用毫秒级时间戳）。

## 1.4 KeepByCreationTimeStrategy 实现

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/ttl/strategy/KeepByCreationTimeStrategy.java`

```java
public class KeepByCreationTimeStrategy extends KeepByTimeStrategy {

  @Override
  protected List<String> getExpiredPartitionsForTimeStrategy(List<String> partitionPathsForTTL) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    return partitionPathsForTTL.stream().parallel().filter(part -> {
      HoodiePartitionMetadata hoodiePartitionMetadata =
          new HoodiePartitionMetadata(metaClient.getStorage(),
              FSUtils.constructAbsolutePath(metaClient.getBasePath(), part));
      Option<String> instantOption = hoodiePartitionMetadata.readPartitionCreatedCommitTime();
      if (instantOption.isPresent()) {
        String instantTime = instantOption.get();
        return isPartitionExpired(instantTime);
      }
      return false;
    }).collect(Collectors.toList());
  }
}
```

与 `KeepByTimeStrategy` 的核心区别：

| 维度 | KeepByTimeStrategy | KeepByCreationTimeStrategy |
|------|-------------------|---------------------------|
| 参考时间 | 分区最后一次被修改的时间 | 分区首次创建的时间 |
| 信息来源 | FileSlice 的 baseInstantTime | HoodiePartitionMetadata 文件 |
| 适用场景 | 按数据活跃度过期 | 按绝对年龄过期 |

> **为什么需要两种策略？** 考虑一个场景：某个旧分区 `dt=2024-01-01` 在 2025-04-15 因为 Late-arriving data 被更新了。使用 KeepByTimeStrategy，它的最后修改时间会更新，不会被过期。但如果你的需求是"无论是否有更新，超过 90 天的分区一律删除"，那就应该使用 KeepByCreationTimeStrategy。

## 1.5 PartitionTTLStrategyType 枚举

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/ttl/strategy/PartitionTTLStrategyType.java`

```java
public enum PartitionTTLStrategyType {
  KEEP_BY_TIME("org.apache.hudi.table.action.ttl.strategy.KeepByTimeStrategy"),
  KEEP_BY_CREATION_TIME("org.apache.hudi.table.action.ttl.strategy.KeepByCreationTimeStrategy");

  @Getter
  private final String className;
  
  PartitionTTLStrategyType(String className) {
    this.className = className;
  }
  
  public static PartitionTTLStrategyType fromClassName(String className) {
    for (PartitionTTLStrategyType type : PartitionTTLStrategyType.values()) {
      if (type.getClassName().equals(className)) {
        return type;
      }
    }
    throw new IllegalArgumentException("No PartitionTTLStrategyType found for class name: " + className);
  }
}
```

只有两种内建策略类型。用户也可以通过 `hoodie.partition.ttl.strategy.class` 配置自定义策略类。

> **源码Bug警告：** 在 v1.2.0-SNAPSHOT 中，`PartitionTTLStrategyType.getPartitionTTLStrategyClassName()` 方法的第70行错误地使用了 `KeyGeneratorType.valueOf()` 而不是 `PartitionTTLStrategyType.valueOf()`。这会导致通过 `hoodie.partition.ttl.management.strategy.type` 配置策略类型时失败。实际使用中应该通过 `HoodiePartitionTTLStrategyFactory` 来创建策略实例，该工厂类正确实现了策略解析逻辑（使用 `PartitionTTLStrategyType.valueOf(strategyType.toUpperCase(Locale.ROOT))`）。

## 1.6 HoodiePartitionTTLStrategyFactory 工厂

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/ttl/strategy/HoodiePartitionTTLStrategyFactory.java`

```java
public static PartitionTTLStrategy createStrategy(
    HoodieTable hoodieTable, TypedProperties props, String instantTime) throws IOException {
  String strategyClassName = getPartitionTTLStrategyClassName(props);
  try {
    return (PartitionTTLStrategy) ReflectionUtils.loadClass(strategyClassName,
        new Class<?>[] {HoodieTable.class, String.class}, hoodieTable, instantTime);
  } catch (Throwable e) {
    throw new IOException("Could not load partition ttl management strategy class " + strategyClassName, e);
  }
}
```

工厂类的策略解析优先级：
1. **优先使用 `PARTITION_TTL_STRATEGY_CLASS_NAME`**（完整类名）
2. **其次使用 `PARTITION_TTL_STRATEGY_TYPE`**（枚举名称 → 映射到类名）

> **这种"类名优先"的设计好处是什么？** 确保用户自定义的策略类不会被枚举类型覆盖。这与 Hudi 中 KeyGenerator、CompactionStrategy 等工厂的设计模式一致。

## 1.7 SparkPartitionTTLActionExecutor — Spark 引擎执行器

源码路径：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/SparkPartitionTTLActionExecutor.java`

```java
public class SparkPartitionTTLActionExecutor<T> extends BaseSparkCommitActionExecutor<T> {

  public SparkPartitionTTLActionExecutor(HoodieEngineContext context, HoodieWriteConfig config,
                                         HoodieTable table, String instantTime) {
    // 注意：operationType 是 DELETE_PARTITION
    super(context, config, table, instantTime, WriteOperationType.DELETE_PARTITION);
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
    HoodieWriteMetadata<HoodieData<WriteStatus>> emptyResult = new HoodieWriteMetadata<>();
    emptyResult.setPartitionToReplaceFileIds(Collections.emptyMap());
    emptyResult.setWriteStatuses(context.emptyHoodieData());
    try {
      // 1. 创建策略实例
      PartitionTTLStrategy strategy = HoodiePartitionTTLStrategyFactory
          .createStrategy(table, config.getProps(), instantTime);
      // 2. 获取过期分区列表
      List<String> expiredPartitions = strategy.getExpiredPartitionPaths();
      if (expiredPartitions.isEmpty()) {
        return emptyResult;
      }
      LOG.info("Partition ttl find the following expired partitions to delete: {}",
          String.join(",", expiredPartitions));
      // 3. 委托给 SparkDeletePartitionCommitActionExecutor 执行实际删除
      return new SparkAutoCommitExecutor(
          new SparkDeletePartitionCommitActionExecutor<>(
              context, config, table, instantTime, expiredPartitions)).execute();
    } catch (HoodieDeletePartitionPendingTableServiceException e) {
      // 4. 如果分区正在被其他 Table Service 操作，跳过
      LOG.info("Partition is under table service, do nothing, call delete partition next time.");
      return emptyResult;
    } catch (IOException e) {
      throw new HoodieIOException("Error executing hoodie partition ttl: ", e);
    }
  }
}
```

**执行流程深度解析：**

1. **策略计算阶段**：通过工厂创建策略 → 计算过期分区列表
2. **执行阶段**：委托给 `SparkDeletePartitionCommitActionExecutor`，这与手动调用 `DELETE_PARTITION` 走的是同一条代码路径
3. **自动提交**：通过 `SparkAutoCommitExecutor` 包装，确保删除操作会自动 commit
4. **冲突处理**：如果分区正在被 Compaction/Clustering 处理，捕获 `HoodieDeletePartitionPendingTableServiceException` 并优雅跳过

> **为什么 TTL 使用 DELETE_PARTITION 而不是 DELETE？** 因为 TTL 的语义是删除整个分区，而非删除分区内的部分记录。DELETE_PARTITION 操作在 timeline 上生成的是 `REPLACE_COMMIT`，它会将分区内所有 file groups 替换为空，效果等同于删除整个分区。

## 1.8 FlinkPartitionTTLActionExecutor — Flink 引擎执行器

源码路径：`hudi-client/hudi-flink-client/src/main/java/org/apache/hudi/table/action/commit/FlinkPartitionTTLActionExecutor.java`

Flink 版本的执行器逻辑与 Spark 版本几乎完全一致，主要区别在于：

1. 返回类型是 `HoodieWriteMetadata<List<WriteStatus>>`（Flink 不使用 RDD 抽象）
2. 委托给 `FlinkDeletePartitionCommitActionExecutor` 和 `FlinkAutoCommitActionExecutor`

这体现了 Hudi **引擎抽象**的核心设计原则：策略层（TTLStrategy）是引擎无关的，只有执行层（ActionExecutor）需要引擎特定实现。

## 1.9 TTL 与 Clean/Archival 的协调

TTL 删除分区后，会在 timeline 上产生一个 `REPLACE_COMMIT` instant。这个 instant 对 Clean 和 Archival 来说就是一个普通的 commit：

- **Clean**：会在后续清理中删除被替换掉的旧文件（如果 Clean 策略配置了足够的保留数量）
- **Archival**：达到归档条件时，会将 TTL 生成的 `REPLACE_COMMIT` 归档到 archived timeline

> **为什么不需要特殊协调？** TTL 生成的 REPLACE_COMMIT 与手动 DELETE_PARTITION 或 Clustering 生成的 REPLACE_COMMIT 是同一类型，Hudi 的 Clean/Archival 已经有成熟的处理逻辑。

## 1.10 TTL 配置参数与生产实践

### 核心配置参数

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieTTLConfig.java`

| 配置项 | 默认值 | 说明 |
|-------|-------|------|
| `hoodie.partition.ttl.inline` | `false` | 是否在每次 commit 后内联执行 TTL |
| `hoodie.partition.ttl.management.strategy.type` | `KEEP_BY_TIME` | TTL 策略类型 |
| `hoodie.partition.ttl.strategy.class` | 无 | 自定义 TTL 策略类（优先级高于 type） |
| `hoodie.partition.ttl.strategy.days.retain` | `-1` | 分区保留天数（-1 表示不启用） |
| `hoodie.partition.ttl.strategy.partition.selected` | 无 | 指定参与 TTL 的分区（逗号分隔） |
| `hoodie.partition.ttl.strategy.max.delete.partitions` | `1000` | 单次 TTL 最多删除的分区数 |

### 生产实践配置示例

```properties
# 启用内联 TTL
hoodie.partition.ttl.inline=true

# 使用按最后修改时间保留策略
hoodie.partition.ttl.management.strategy.type=KEEP_BY_TIME

# 保留最近 90 天数据
hoodie.partition.ttl.strategy.days.retain=90

# 单次最多删除 500 个分区（防止大量删除导致性能问题）
hoodie.partition.ttl.strategy.max.delete.partitions=500

# 只对特定分区目录做 TTL（可选）
# hoodie.partition.ttl.strategy.partition.selected=dt=2024-01-01,dt=2024-01-02
```

### 独立 TTL Job

除了 Inline 模式，Hudi 还提供了独立的 `HoodieTTLJob` 工具。

源码路径：`hudi-utilities/src/main/java/org/apache/hudi/utilities/HoodieTTLJob.java`

```java
public void run() {
    try (SparkRDDWriteClient<HoodieRecordPayload> client =
             UtilHelpers.createHoodieClient(jsc, cfg.basePath, ...)) {
      String instantTime = client.startDeletePartitionCommit(metaClient);
      HoodieWriteResult result = client.managePartitionTTL(instantTime);
      client.commit(instantTime, result.getWriteStatuses(), Option.empty(),
          HoodieTimeline.REPLACE_COMMIT_ACTION,
          result.getPartitionToReplaceFileIds(), Option.empty());
    }
}
```

可以通过 `spark-submit` 定期调度执行，适合不想在写入路径中增加 TTL 开销的场景。

### Spark SQL Procedure 调用

源码路径：`hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/command/procedures/RunTTLProcedure.scala`

```sql
-- 通过 Spark SQL Procedure 手动触发 TTL
CALL run_ttl(table => 'my_hudi_table')
```

### 生产建议

1. **首次启用 TTL 前，先 dry-run**：设置 `max.delete.partitions=0`，通过日志查看哪些分区会被标记为过期
2. **配合 Async Clean 使用**：TTL 删除分区后，被替换的文件需要 Clean 来实际清理
3. **监控 REPLACE_COMMIT 大小**：如果一次 TTL 删除大量分区，REPLACE_COMMIT 的元数据会比较大
4. **大表首次 TTL 分批执行**：通过 `max.delete.partitions` 限制每次删除的分区数

---

# 第二部分：Log Compaction — 轻量级日志压缩

## 1. 解决什么问题

Log Compaction 解决的核心问题是**MOR 表的写放大（Write Amplification）和读取性能平衡**。

### 业务痛点

在 MOR（Merge on Read）表中，传统 Compaction 存在严重的写放大问题：

源码证据：`HoodieLogCompactionPlanGenerator.java` 第 55 行注释：
```java
.setCompactorClassName("org.apache.hudi.table.action.compact.LogCompactionExecutionHelper") 
// added for compatibility between releases
```

**写放大场景**：
- Base File 128MB + Log Files 5MB → 传统 Compaction 需要写出 128MB+ 新 Base File
- 即使只有 5MB 更新，也要重写整个 Base File
- I/O 成本：读取 138MB + 写入 128MB = 266MB

**影响场景**：
1. **Metadata Table**：Hudi 元数据表是 MOR 表，每次 delta commit 产生 log blocks，频繁全量 Compaction 写放大严重
2. **高频流式写入**：每分钟 commit 一次，log files 累积快，但频繁全量 Compaction 消耗大量 I/O
3. **大 Base File 场景**：Base File 几百 MB 甚至 GB，少量更新也需要重写整个文件

### 如果没有 Log Compaction 会怎样

1. **文件句柄数量爆炸**：多个零散 Log File 导致读取时打开大量文件句柄
2. **读取性能下降**：扫描多个小 Log File 的随机 I/O 开销大
3. **Compaction 频率两难**：频繁 Compaction → 写放大严重；不频繁 Compaction → 读取性能差

### 实际应用场景

1. **实时数据湖**：Flink 流式写入每分钟产生 log files，Log Compaction 每小时合并一次，传统 Compaction 每天执行
2. **CDC 同步**：数据库 CDC 产生大量小更新，Log Compaction 减少 log 文件数量，降低读取开销
3. **元数据表优化**：Hudi 自身的 Metadata Table 默认启用 Log Compaction（`ENABLE_LOG_COMPACTION` since 0.14.0）

## 2. 有什么坑

### 坑1：Log Compaction 和传统 Compaction 的配置混淆

源码证据：`HoodieCompactionConfig.java` 第 65-70 行：
```java
public static final ConfigProperty<String> ENABLE_LOG_COMPACTION = ConfigProperty
    .key("hoodie.log.compaction.enable")
    .defaultValue("false")
    .markAdvanced()
    .sinceVersion("0.14.0")
    .withDocumentation("By enabling log compaction through this config, log compaction will also get enabled for the metadata table.");
```

**陷阱**：`hoodie.log.compaction.enable` 和 `hoodie.log.compaction.inline` 是两个独立配置：
- `enable=true`：启用 Log Compaction 功能（包括元数据表）
- `inline=true`：在写入时内联执行 Log Compaction
- 必须两者都设置为 `true` 才能在写入时自动触发

### 坑2：触发阈值的理解偏差

源码证据：`HoodieLogCompactionPlanGenerator.java` 第 92-112 行：
```java
private boolean isFileSliceEligibleForLogCompaction(FileSlice fileSlice, String maxInstantTime,
                                                    Option<InstantRange> instantRange) {
    log.info("Checking if fileId {} and partition {} eligible for log compaction.", 
        fileSlice.getFileId(), fileSlice.getPartitionPath());
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    long numLogFiles = fileSlice.getLogFiles().count();
    if (numLogFiles >= writeConfig.getLogCompactionBlocksThreshold()) {
      log.info("Total logs files ({}) is greater than log blocks threshold is {}", 
          numLogFiles, writeConfig.getLogCompactionBlocksThreshold());
      return true;
    }
    HoodieLogBlockMetadataScanner scanner = new HoodieLogBlockMetadataScanner(metaClient, 
        fileSlice.getLogFiles()
        .sorted(HoodieLogFile.getLogFileComparator())
        .collect(Collectors.toList()),
        writeConfig.getMaxDFSStreamBufferSize(),
        maxInstantTime,
        instantRange);
    int totalBlocks = scanner.getCurrentInstantLogBlocks().size();
    log.info("Total blocks seen are {}, log blocks threshold is {}", 
        totalBlocks, writeConfig.getLogCompactionBlocksThreshold());
    return totalBlocks >= writeConfig.getLogCompactionBlocksThreshold();
}
```

**陷阱**：`hoodie.log.compaction.blocks.threshold=5` 有两层含义（满足任一即触发）：
1. Log **文件数量** ≥ 5
2. Log **blocks 总数** ≥ 5

很多用户误以为只看文件数量，实际上单个文件内的 blocks 也会计入。

### 坑3：Log Compaction 后旧文件不会立即删除

源码证据：RFC-48 描述的行为：
> "当 LogCompaction 执行后，产生 log.4，Reader 在扫描时会看到 4 个 log blocks，但会**只考虑 log block 4**（因为它包含了 block 1-3 的合并结果）。"

**陷阱**：
- Log Compaction 生成新的合并 Log File，但旧的 Log File 物理上仍存在
- 旧文件在 Timeline 上被"覆盖"，Reader 会跳过
- 最终由 Clean 服务删除旧文件
- 如果 Clean 配置不当，存储空间不会立即释放

### 坑4：与传统 Compaction 的协调

源码证据：`ScheduleCompactionActionExecutor.java` 中的触发逻辑：
```java
private boolean needLogCompact(Pair<Integer, String> latestDeltaCommitInfoSinceCompact) {
    Option<Pair<Integer, String>> latestDeltaCommitInfoSinceLogCompactOption =
        getLatestDeltaCommitInfoSinceLogCompaction();
    int numDeltaCommitsSinceLatestCompaction = latestDeltaCommitInfoSinceCompact.getLeft();
    int numDeltaCommitsSinceLatestLogCompaction = latestDeltaCommitInfoSinceLogCompactOption.isPresent()
        ? latestDeltaCommitInfoSinceLogCompactOption.get().getLeft() : 0;

    int numDeltaCommitsSince = Math.min(
        numDeltaCommitsSinceLatestCompaction, numDeltaCommitsSinceLatestLogCompaction);
    boolean shouldLogCompact = numDeltaCommitsSince >= config.getLogCompactionBlocksThreshold();
    return shouldLogCompact;
}
```

**陷阱**：如果刚做完传统 Compaction，Log Compaction 不会立即触发（取较小值逻辑）。这可能导致用户困惑："为什么配置了 Log Compaction 但没有执行？"

### 坑5：只适用于 MOR 表

源码证据：`ScheduleCompactionActionExecutor.java` 第 579 行：
```java
ValidationUtils.checkArgument(
    this.table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ, ...);
```

**陷阱**：COW 表没有 Log File，配置 Log Compaction 会在调度阶段失败。

### 性能陷阱

**陷阱**：Log Compaction 虽然减少了写放大，但仍需要读取所有 Log Files 并写出合并后的 Log File。如果 Log Files 总大小很大（如几百 MB），Log Compaction 的 I/O 成本也不低。建议配合传统 Compaction 使用，而非完全替代。

## 3. 核心概念解释

### Log Compaction vs 传统 Compaction

| 维度 | 传统 Compaction | Log Compaction |
|------|----------------|----------------|
| 输入 | Base File + Log Files | 仅 Log Files |
| 输出 | 新的 Base File (.parquet) | 新的 Log File (.log) |
| Timeline Action | `compaction` → `commit` | `logcompaction` → `deltacommit` |
| I/O 成本 | 高（需重写 Base File） | 低（只合并 Log） |
| 读取性能改善 | 最优（无需 merge） | 中等（减少了 log 数量） |
| 写放大 | 高 | 低 |
| Handle 类型 | MergeHandle | AppendHandle |

源码证据：`HoodieTimeline.java` 定义了独立的 Action 类型：
```java
String LOG_COMPACTION_ACTION = "logcompaction";
```

### Minor Compaction vs Major Compaction

- **Minor Compaction**：Log Compaction 的别名，只合并 Log Files
- **Major Compaction**：传统 Compaction，合并 Base File + Log Files

这个术语来自 LSM-Tree 数据库（如 HBase、RocksDB）的设计。

### Log Block 的概念

Log File 内部由多个 Log Block 组成：
- **Data Block**：包含实际数据记录
- **Delete Block**：包含删除标记
- **Rollback Block**：包含回滚信息

Log Compaction 会合并多个 Data Block 和 Delete Block，生成一个新的 Data Block。

### 与其他系统的对比

- **HBase Minor Compaction**：合并多个 HFile，不触碰 MemStore
- **RocksDB Level Compaction**：合并同一 Level 的 SST 文件
- **Hudi Log Compaction**：合并同一 File Group 的 Log Files

## 4. 设计理念

### 延迟合并（Deferred Merging）

源码证据：`HoodieCompactor.java` 中的执行路径分离：
```java
if (operationType == WriteOperationType.LOG_COMPACT) {
    // Log Compaction: 转发到 logCompact 方法
    return context.parallelize(operations).map(
        operation -> logCompact(config, operation, compactionInstantTime,
            instantRange, table, taskContextSupplier))
        .flatMap(List::iterator);
} else {
    // 传统 Compaction: 转发到 compact 方法
    return context.parallelize(operations).map(
        operation -> compact(config, operation, compactionInstantTime,
            readerContextFactory.getContext(), table, maxInstantTime, taskContextSupplier))
        .flatMap(List::iterator);
}
```

**设计哲学**：
- Log Compaction 是"轻量级优化"，延迟完整的 Base File 重写
- 通过多次 Log Compaction 逐步改善读取性能
- 最终由传统 Compaction 完成彻底合并

### 空间换时间的权衡

**设计权衡**：
- **优势**：减少写放大，提高写入吞吐
- **劣势**：读取时仍需 merge（虽然 log 数量减少了），不如传统 Compaction 后的纯 Base File 读取快

### 与 LSM-Tree 的相似性

Hudi 的 MOR 表 + Log Compaction 设计借鉴了 LSM-Tree 的思想：
- **MemTable** → Hudi 的 Log File（追加写）
- **SSTable** → Hudi 的 Base File（不可变）
- **Minor Compaction** → Hudi 的 Log Compaction
- **Major Compaction** → Hudi 的传统 Compaction

### 架构演进历史

- **0.x 版本**：只有传统 Compaction，写放大严重
- **0.13.0 版本**：引入 Log Compaction（RFC-48），支持 Inline 模式
- **0.14.0 版本**：为 Metadata Table 默认启用 Log Compaction
- **未来方向**：可能引入更智能的 Compaction 策略选择（基于 I/O 成本模型）

### 为什么使用 AppendHandle 而非 MergeHandle

源码证据：`HoodieCompactor.java` 中的 Handle 选择：
```java
// 传统 Compaction: 使用 MergeHandle，输出 Base File
public List<WriteStatus> compact(...) {
    HoodieMergeHandle mergeHandle = HoodieMergeHandleFactory.create(...);
    mergeHandle.doMerge();
    return mergeHandle.close();
}

// Log Compaction: 使用 AppendHandle，输出 Log File
public List<WriteStatus> logCompact(...) {
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(...);
    FileGroupReaderBasedAppendHandle appendHandle =
        new FileGroupReaderBasedAppendHandle<>(writeConfig, instantTime,
            table, operation, taskContextSupplier, readerContext);
    appendHandle.doAppend();
    return appendHandle.close();
}
```

**设计理念**：
- MergeHandle：创建新的 Parquet Base File，需要完整的 schema 和 Parquet 写入器
- AppendHandle：追加到 Log File，只需要序列化记录到 Avro 格式
- AppendHandle 的写入路径更轻量，符合 Log Compaction 的"轻量级"定位

## 2.1 为什么需要 Log Compaction

### 传统 Compaction 的写放大问题

在 MOR（Merge on Read）表中，传统 Compaction 的工作流程是：

```
输入：Base File (Parquet) + Log Files (.log.1, .log.2, ..., .log.N)
输出：新的 Base File (Parquet)
```

即 Compaction 读取 Base File 和所有 Log File，合并后写出一个全新的 Parquet Base File。假设 Base File 大小 128MB，Log Files 总共只有 5MB，Compaction 依然需要写出一个完整的 128MB+ Base File。

**这就是典型的写放大（Write Amplification）问题。**

在以下场景中，写放大尤为严重：
- **Metadata Table**：Hudi 的元数据表是一个 MOR 表，每次 delta commit 都会产生 log blocks。如果频繁做全量 Compaction，写放大会非常严重
- **高频写入场景**：如流式写入每分钟 commit 一次，log files 累积很快，但频繁全量 Compaction 消耗大量 I/O
- **大 Base File 场景**：当 Base File 很大（几百 MB 甚至 GB），即使只有少量更新，也需要重写整个文件

### Log Compaction 的核心思想

RFC-48（JIRA: HUDI-3580）提出了 Log Compaction（也称为 Minor Compaction）的概念：

```
传统 Compaction:
  Base File + Log Files → 新 Base File  (重量级)

Log Compaction:
  多个 Log Files → 一个合并的 Log File  (轻量级)
```

Log Compaction 只将多个零散的 Log File 合并成一个大的 Log File，**不触碰 Base File**。

> **为什么这么设计？** 核心动机是减少文件句柄数量和读取开销，同时避免全量重写 Base File 带来的 I/O 成本。这是一种 "空间换时间" 和 "延迟合并" 的设计哲学——通过轻量级的 log 合并来改善读取性能，将完整的 Base File 重写留给真正需要时（如大量数据变更后）。

### 对比示意

```
场景：Base File = 128MB, 5 个 Log Files 共 10MB

传统 Compaction:
  读取: 128MB + 10MB = 138MB
  写入: ~128MB (新 Base File)
  I/O 总量: 266MB

Log Compaction:
  读取: 10MB (只读 Log Files)
  写入: ~10MB (合并后的 Log File)
  I/O 总量: 20MB

I/O 节省: 92%+
```

## 2.2 Log Compaction 的 Timeline Action

在 Hudi Timeline 中，Log Compaction 有独立的 Action 类型：

源码路径：`hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieTimeline.java`

```java
String LOG_COMPACTION_ACTION = "logcompaction";
```

与传统 Compaction 对比：

| 属性 | 传统 Compaction | Log Compaction |
|------|---------------|----------------|
| Timeline Action | `compaction` | `logcompaction` |
| 完成后的 Action | `commit` | `deltacommit` |
| 输出文件 | 新的 Base File (Parquet) | 新的 Log File |
| 请求文件后缀 | `.compaction.requested` | `.logcompaction.requested` |
| Inflight 文件后缀 | `.compaction.inflight` | `.logcompaction.inflight` |

> **为什么 Log Compaction 完成后是 deltacommit 而不是 commit？** 因为 Log Compaction 的产出物是 Log File（而不是 Base File），这与 MOR 表的 delta commit 语义一致。commit（COMMIT_ACTION）是 COW 表或 Compaction 创建 Base File 时使用的。

## 2.3 Schedule 阶段 — ScheduleCompactionActionExecutor

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/ScheduleCompactionActionExecutor.java`

`ScheduleCompactionActionExecutor` 同时处理传统 Compaction 和 Log Compaction 的调度。通过 `WriteOperationType` 区分：

```java
public ScheduleCompactionActionExecutor(..., WriteOperationType operationType) {
    checkArgument(operationType == WriteOperationType.COMPACT
        || operationType == WriteOperationType.LOG_COMPACT,
        "Only COMPACT and LOG_COMPACT is supported");
    initPlanGenerator(context, config, table);
}

private void initPlanGenerator(...) {
    if (WriteOperationType.COMPACT.equals(operationType)) {
        String planGeneratorClass = ConfigUtils.getStringWithAltKeys(
            config.getProps(), HoodieCompactionConfig.COMPACTION_PLAN_GENERATOR, true);
        planGenerator = createCompactionPlanGenerator(planGeneratorClass, table, context, config);
    } else {
        // Log Compaction 使用专门的 Plan Generator
        planGenerator = new HoodieLogCompactionPlanGenerator(table, context, config, this);
    }
}
```

### 调度执行流程

```java
@Override
public Option<HoodieCompactionPlan> execute() {
    // 1. 检查表类型必须是 MOR
    ValidationUtils.checkArgument(
        this.table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ, ...);

    // 2. 生成 Compaction Plan
    HoodieCompactionPlan plan = scheduleCompaction();
    Option<HoodieCompactionPlan> option = Option.empty();

    if (plan != null && nonEmpty(plan.getOperations())) {
        extraMetadata.ifPresent(plan::setExtraMetadata);
        if (operationType.equals(WriteOperationType.COMPACT)) {
            // 传统 Compaction: 创建 .compaction.requested
            HoodieInstant compactionInstant = ...createNewInstant(
                HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
            table.getActiveTimeline().saveToCompactionRequested(compactionInstant, plan);
        } else {
            // Log Compaction: 创建 .logcompaction.requested
            HoodieInstant logCompactionInstant = ...createNewInstant(
                HoodieInstant.State.REQUESTED, HoodieTimeline.LOG_COMPACTION_ACTION, instantTime);
            table.getActiveTimeline().saveToLogCompactionRequested(logCompactionInstant, plan);
        }
        option = Option.of(plan);
    }
    return option;
}
```

### Log Compaction 的触发条件

```java
private boolean needLogCompact(Pair<Integer, String> latestDeltaCommitInfoSinceCompact) {
    Option<Pair<Integer, String>> latestDeltaCommitInfoSinceLogCompactOption =
        getLatestDeltaCommitInfoSinceLogCompaction();
    int numDeltaCommitsSinceLatestCompaction = latestDeltaCommitInfoSinceCompact.getLeft();
    int numDeltaCommitsSinceLatestLogCompaction = latestDeltaCommitInfoSinceLogCompactOption.isPresent()
        ? latestDeltaCommitInfoSinceLogCompactOption.get().getLeft() : 0;

    int numDeltaCommitsSince = Math.min(
        numDeltaCommitsSinceLatestCompaction, numDeltaCommitsSinceLatestLogCompaction);
    boolean shouldLogCompact = numDeltaCommitsSince >= config.getLogCompactionBlocksThreshold();
    if (shouldLogCompact) {
      log.info("There have been {} delta commits since last compaction or log compaction, "
          + "triggering log compaction.", numDeltaCommitsSince);
    }
    return shouldLogCompact;
}
```

核心逻辑：取"自上次 Compaction 以来的 delta commits 数"和"自上次 Log Compaction 以来的 delta commits 数"的较小值，如果超过阈值则触发。

> **为什么取较小值？** 因为如果距离上次 Compaction 已经很近（说明最近刚做过全量 Compaction），那 Log Compaction 没必要。只有在两者都超过阈值时才需要 Log Compaction。

## 2.4 HoodieLogCompactionPlanGenerator — Plan 生成

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/plan/generators/HoodieLogCompactionPlanGenerator.java`

```java
public class HoodieLogCompactionPlanGenerator<T extends HoodieRecordPayload, I, K, O>
    extends BaseHoodieCompactionPlanGenerator<T, I, K, O> {

  private final HoodieCompactionStrategy compactionStrategy;

  public HoodieLogCompactionPlanGenerator(...) {
    super(table, engineContext, writeConfig, executor);
    this.compactionStrategy = HoodieCompactionStrategy.newBuilder()
        .setStrategyParams(getStrategyParams())
        // 设置特殊的 Compactor 类名，用于区分 Log Compaction 和传统 Compaction
        .setCompactorClassName(
            "org.apache.hudi.table.action.compact.LogCompactionExecutionHelper")
        .build();
  }
```

### FileSlice 资格判定

```java
private boolean isFileSliceEligibleForLogCompaction(
    FileSlice fileSlice, String maxInstantTime, Option<InstantRange> instantRange) {
    log.info("Checking if fileId {} and partition {} eligible for log compaction.",
        fileSlice.getFileId(), fileSlice.getPartitionPath());
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    long numLogFiles = fileSlice.getLogFiles().count();
    // 条件1: Log 文件数量 >= 阈值
    if (numLogFiles >= writeConfig.getLogCompactionBlocksThreshold()) {
      log.info("Total logs files ({}) is greater than log blocks threshold is {}",
          numLogFiles, writeConfig.getLogCompactionBlocksThreshold());
      return true;
    }
    // 条件2: Log blocks 总数 >= 阈值
    HoodieLogBlockMetadataScanner scanner = new HoodieLogBlockMetadataScanner(
        metaClient, fileSlice.getLogFiles()
            .sorted(HoodieLogFile.getLogFileComparator())
            .collect(Collectors.toList()), ...);
    int totalBlocks = scanner.getCurrentInstantLogBlocks().size();
    log.info("Total blocks seen are {}, log blocks threshold is {}",
        totalBlocks, writeConfig.getLogCompactionBlocksThreshold());
    return totalBlocks >= writeConfig.getLogCompactionBlocksThreshold();
}
```

**两个判定条件（满足任一即可）：**
1. **Log 文件数量 >= 阈值**：文件数量多意味着打开的文件句柄多，影响读取性能
2. **Log blocks 总数 >= 阈值**：即使文件数量不多，单个文件内的 blocks 也可能很多

默认阈值为 5（`hoodie.log.compaction.blocks.threshold=5`）。

## 2.5 Execute 阶段 — RunCompactionActionExecutor

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/RunCompactionActionExecutor.java`

执行阶段同样复用了 `RunCompactionActionExecutor`，通过 `operationType` 区分处理逻辑：

```java
@Override
public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
    HoodieTimeline pendingMajorOrMinorCompactionTimeline =
        WriteOperationType.COMPACT.equals(operationType)
            ? table.getActiveTimeline().filterPendingCompactionTimeline()
            : table.getActiveTimeline().filterPendingLogCompactionTimeline();
    compactor.preCompact(table, pendingMajorOrMinorCompactionTimeline,
        this.operationType, instantTime);

    // 获取 Compaction Plan
    HoodieCompactionPlan compactionPlan = operationType.equals(WriteOperationType.COMPACT)
        ? CompactionUtils.getCompactionPlan(table.getMetaClient(), instantTime)
        : CompactionUtils.getLogCompactionPlan(table.getMetaClient(), instantTime);

    // ... schema evolution handling ...

    HoodieData<WriteStatus> statuses = compactor.compact(
        context, operationType, compactionPlan, table, configCopy, instantTime);
    // ...
}
```

### HoodieCompactor 中的 Log Compaction 执行

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/HoodieCompactor.java`

```java
public HoodieData<WriteStatus> compact(...) {
    // ...
    if (operationType == WriteOperationType.LOG_COMPACT) {
        // Log Compaction: 转发到 logCompact 方法
        return context.parallelize(operations).map(
            operation -> logCompact(config, operation, compactionInstantTime,
                instantRange, table, taskContextSupplier))
            .flatMap(List::iterator);
    } else {
        // 传统 Compaction: 转发到 compact 方法
        return context.parallelize(operations).map(
            operation -> compact(config, operation, compactionInstantTime,
                readerContextFactory.getContext(), table, maxInstantTime, taskContextSupplier))
            .flatMap(List::iterator);
    }
}
```

关键区别在于单个 FileSlice 的处理方式：

```java
// 传统 Compaction: 使用 MergeHandle，输出 Base File
public List<WriteStatus> compact(...) {
    HoodieMergeHandle mergeHandle = HoodieMergeHandleFactory.create(...);
    mergeHandle.doMerge();
    return mergeHandle.close();
}

// Log Compaction: 使用 AppendHandle，输出 Log File
public List<WriteStatus> logCompact(...) {
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(...);
    FileGroupReaderBasedAppendHandle appendHandle =
        new FileGroupReaderBasedAppendHandle<>(writeConfig, instantTime,
            table, operation, taskContextSupplier, readerContext);
    appendHandle.doAppend();
    return appendHandle.close();
}
```

> **核心区别：** 传统 Compaction 使用 `HoodieMergeHandle`（创建新的 Parquet Base File），Log Compaction 使用 `FileGroupReaderBasedAppendHandle`（创建新的 Log File）。前者是"合并写"，后者是"追加写"。

## 2.6 Log Compaction vs 传统 Compaction 对比

| 维度 | 传统 Compaction | Log Compaction |
|------|----------------|----------------|
| 输入 | Base File + Log Files | 仅 Log Files |
| 输出 | 新的 Base File (.parquet) | 新的 Log File (.log) |
| Timeline Action | `compaction` → `commit` | `logcompaction` → `deltacommit` |
| I/O 成本 | 高（需重写 Base File） | 低（只合并 Log） |
| 读取性能改善 | 最优（无需 merge） | 中等（减少了 log 数量） |
| 写放大 | 高 | 低 |
| 适用场景 | 读多写少 | 写多读少、频繁小批量更新 |
| Handle 类型 | MergeHandle | AppendHandle |
| Since 版本 | 0.x | 0.13.0 |

## 2.7 Log Compaction 配置

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieCompactionConfig.java`

| 配置项 | 默认值 | 说明 | Since 版本 |
|-------|-------|------|-----------|
| `hoodie.log.compaction.enable` | `false` | 是否启用 Log Compaction（也会为元数据表启用） | 0.14.0 |
| `hoodie.log.compaction.inline` | `false` | 是否内联执行 Log Compaction | 0.13.0 |
| `hoodie.log.compaction.blocks.threshold` | `5` | 触发 Log Compaction 的 log blocks/files 阈值 | 0.13.0 |

> **注意：** `hoodie.log.compaction.enable` 和 `hoodie.log.compaction.inline` 是两个独立的配置项。前者控制是否启用 Log Compaction 功能，后者控制是否在写入时内联执行。通常两者需要同时设置为 `true`。

### 生产实践

```properties
# 启用 Log Compaction
hoodie.log.compaction.enable=true
hoodie.log.compaction.inline=true

# 当 log blocks 达到 5 个时触发 Log Compaction
hoodie.log.compaction.blocks.threshold=5

# 同时保留传统 Compaction（二者互补）
hoodie.compact.inline=true
hoodie.compact.inline.max.delta.commits=10
```

> **最佳实践：** Log Compaction 和传统 Compaction 应该配合使用。Log Compaction 作为"短期优化"减少读取时的文件句柄数量，传统 Compaction 作为"长期优化"最终将所有数据合并到 Base File 中。

---

# 第三部分：Non-Blocking Concurrency Control (NBCC)

## 1. 解决什么问题

NBCC 解决的核心问题是**高并发流式写入场景下的写写冲突和重试风暴**。

### 业务痛点

源码证据：`WriteConcurrencyMode.java` 第 42-46 行的注释清晰描述了 NBCC 的价值：
```java
// Multiple writer can perform write ops on a MOR table with non-blocking conflict resolution
@EnumFieldDescription("Multiple writers can operate on the table with non-blocking conflict resolution. "
    + "The writers can write into the same file group with the conflicts resolved automatically "
    + "by the query reader and the compactor.")
NON_BLOCKING_CONCURRENCY_CONTROL;
```

传统 OCC（Optimistic Concurrency Control）在高并发场景下的问题：

1. **Bloom Filter Index 场景**：
   - 多个 Writer 倾向于写入小文件（small file handling）
   - 同一个小文件会被多个 Writer 同时选中
   - 冲突率极高，后提交的 Writer 必须回滚重试

2. **Hash Index 场景**：
   - 记录通过 hash 均匀分布到所有 bucket
   - 每个 Writer 都可能触碰所有 bucket
   - 冲突几乎不可避免

3. **重试风暴**：
   - 冲突后回滚重试，但重试时可能再次冲突
   - 恶性循环导致写入吞吐急剧下降

### 如果没有 NBCC 会怎样

**实际案例**（RFC-66 描述）：
- 10 个 Flink 作业同时写入同一张 Hudi 表
- 使用 OCC 模式，冲突率 > 80%
- 平均每个作业需要重试 3-5 次才能成功
- 写入延迟从秒级上升到分钟级

### 实际应用场景

1. **多源数据汇入**：多个 Flink/Spark 流从不同数据源（Kafka、Pulsar、数据库 CDC）写入同一张 Hudi 表
2. **实时数据 JOIN**：替代 Flink 的 State-Based JOIN，用 Hudi 表作为 JOIN 的物化视图，多个流同时更新
3. **微服务数据湖**：多个微服务实例并发写入同一张事件表

## 2. 有什么坑

### 坑1：必须使用 MOR + Bucket Index

源码证据：`HoodieWriteConfig.java` 第 3753-3757 行的硬性约束：
```java
if (writeConcurrencyMode == WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL) {
    boolean isMetadataTable = HoodieTableMetadata.isMetadataTable(writeConfig.getBasePath());
    checkArgument(
        writeConfig.getTableType().equals(HoodieTableType.MERGE_ON_READ)
            && (isMetadataTable || writeConfig.isSimpleBucketIndex()),
        "Non-blocking concurrency control requires the MOR table with simple bucket index "
            + "or it has to be Metadata table");
}
```

**陷阱**：
- COW 表不支持 NBCC（会抛出 `IllegalArgumentException`）
- 必须使用 Simple Bucket Index（Bloom Filter Index / Global Index 不支持）
- 配置错误会在 `HoodieWriteConfig` 构建时失败，而非运行时

### 坑2：Bucket 数量不可变

源码证据：`BucketIdentifier.java` 第 40-42 行的确定性哈希：
```java
public static int getBucketId(List<String> hashKeyFields, int numBuckets) {
    return (hashKeyFields.hashCode() & Integer.MAX_VALUE) % numBuckets;
}
```

**陷阱**：
- Bucket 数量一旦设定，无法动态调整（Simple Bucket Index 限制）
- 如果数据量增长，无法通过增加 bucket 来扩展
- 需要在表创建时根据数据量预估合理设置 `hoodie.bucket.index.num.buckets`

### 坑3：BULK_INSERT 仍需冲突检测

源码证据：`HoodieWriteConfig.java` 的 `needResolveWriteConflict` 方法：
```java
public boolean needResolveWriteConflict(WriteOperationType operationType,
    boolean isMetadataTable, HoodieWriteConfig config, HoodieTableConfig tableConfig) {
    WriteConcurrencyMode mode = getWriteConcurrencyMode();
    switch (mode) {
      case SINGLE_WRITER:
        return false;
      case OPTIMISTIC_CONCURRENCY_CONTROL:
        return true;
      case NON_BLOCKING_CONCURRENCY_CONTROL: {
        if (isMetadataTable) {
          return false;
        } else {
          // NBCC 只在 BULK_INSERT 时需要冲突检测
          return WriteOperationType.BULK_INSERT == operationType;
        }
      }
    }
}
```

**陷阱**：BULK_INSERT 可能创建新的 Base File（而非追加 Log File），两个 Writer 同时 BULK_INSERT 同一个 partition 可能产生文件冲突。

### 坑4：仍需要 Lock Provider

**陷阱**：虽然数据写入（Ingestion）之间不需要锁，但以下场景仍需要锁：
- Table Service（Compaction、Clustering）与数据写入的协调
- Timeline 操作（创建 instant、提交 commit）的原子性
- 必须配置 `hoodie.write.lock.provider`（如 ZooKeeper、DynamoDB）

### 坑5：Clustering 与 Pending Ingestion 的冲突

源码证据：`PreferWriterConflictResolutionStrategy.java` 第 1137-1148 行：
```java
@Override
public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    if (isClusteringBlockForPendingIngestion
        && WriteOperationType.CLUSTER.equals(thisOperation.getOperationType())
        && isRequestedIngestionInstant(otherOperation)) {
      log.info("Clustering operation {} conflicts with pending ingestion instant {} "
          + "that has an active heartbeat", thisOperation, otherOperation);
      return true;
    }
    return super.hasConflict(thisOperation, otherOperation);
}
```

**陷阱**：Clustering 会重写整个 File Group，如果有 Ingestion 正在向同一个 File Group 追加 Log File，Clustering 完成后这些 Log File 会变成"孤儿"。Clustering 遇到 Pending Ingestion 时会主动失败。

### 坑6：必须使用 LAZY Clean 策略

源码证据：RFC-66 的前置条件要求：
> "Lazy Cleaning Strategy (必须)"

**陷阱**：EAGER Clean 策略会在 commit 后立即清理旧文件，可能删除其他 Writer 正在读取的 Log File。必须配置 `hoodie.clean.failed.writes.policy=LAZY`。

### 性能陷阱

**陷阱**：NBCC 下多个 Writer 会产生大量 Log File，需要更频繁的 Compaction。建议配合 Log Compaction 使用：
```properties
hoodie.log.compaction.enable=true
hoodie.log.compaction.inline=true
hoodie.log.compaction.blocks.threshold=5
```

## 3. 核心概念解释

### WriteConcurrencyMode 三种模式

源码证据：`WriteConcurrencyMode.java` 第 30-46 行定义了三种模式：

| 模式 | 多写者 | 冲突处理 | 写入吞吐 | 适用场景 |
|------|-------|---------|---------|---------|
| SINGLE_WRITER | 否 | 无 | 最高 | 单一 ETL Pipeline |
| OCC | 是 | Lock + 回滚重试 | 中等（冲突多时下降） | 低频批量写入 |
| NBCC | 是 | 无需处理（架构设计避免冲突） | 高 | 高频流式多写者 |

### 确定性哈希分桶（Deterministic Hashing）

源码证据：`BucketIdentifier.java` 第 28-30 行的关键设计：
```java
public class BucketIdentifier implements Serializable {
  // Ensure the same records keys from different writers are desired to be distributed into the same bucket
  private static final String CONSTANT_FILE_ID_SUFFIX = "-0000-0000-0000-000000000000";
```

**核心概念**：
- 相同 Record Key 的记录，无论哪个 Writer 处理，都路由到同一个 Bucket
- Bucket ID 通过 `(hashKeyFields.hashCode() & Integer.MAX_VALUE) % numBuckets` 计算
- File ID 使用固定后缀 `CONSTANT_FILE_ID_SUFFIX`，确保不同 Writer 对同一 Bucket 使用相同的 File ID

### Log File 的 Writer 隔离

**核心概念**：在 MOR 表中，不同 Writer 写入的是不同的 Log File：
```
Writer A writes: filegroup_X.log.1_0-1-001_20250415010000
Writer B writes: filegroup_X.log.1_0-2-002_20250415010100
```

Log File 命名包含：
- File Group ID（相同）
- Log Version（相同）
- Writer ID（不同）
- Instant Time（不同）

### PreferWriter 策略

源码证据：`PreferWriterConflictResolutionStrategy.java` 的设计哲学：

**核心概念**：
- **数据写入 vs Table Service 冲突**：Table Service 让步（回滚），数据写入优先通过
- **数据写入 vs 数据写入冲突**：通过 NBCC 架构设计避免（MOR + Bucket Index）
- **Table Service vs Table Service 冲突**：正常的文件级冲突检测

### 与其他系统的对比

- **PostgreSQL MVCC**：多版本并发控制，读不阻塞写，写不阻塞读
- **HBase MVCC**：每个 Cell 有多个版本，通过时间戳区分
- **Hudi NBCC**：每个 Record 的多个版本分散在不同 Log File 中，通过 Compaction 或读取时合并

## 4. 设计理念

### 架构约束代替运行时检测

源码证据：`WriteConcurrencyMode.java` 第 48-50 行的方法：
```java
public boolean supportsMultiWriter() {
    return this == OPTIMISTIC_CONCURRENCY_CONTROL || this == NON_BLOCKING_CONCURRENCY_CONTROL;
}
```

**设计哲学**：
- OCC：运行时检测冲突，冲突后回滚重试（Pessimistic）
- NBCC：通过架构约束（MOR + Bucket Index）使冲突根本不会发生（Optimistic）

这是一种"设计时解决问题"而非"运行时解决问题"的思想。

### Merge on Read 的延迟冲突解决

**设计理念**：
- 写入时：不同 Writer 创建不同的 Log File，无冲突
- 读取时：Reader 扫描所有 Log Files，按 commit time / event time 选择最新版本
- Compaction 时：Compactor 合并所有 Log Files，解决多版本冲突

这是一种"延迟冲突解决"（Deferred Conflict Resolution）策略。

### 为什么 COW 表不支持 NBCC

源码证据：`HoodieWriteConfig.java` 的约束检查注释：
```java
checkArgument(
    writeConfig.getTableType().equals(HoodieTableType.MERGE_ON_READ)
        && (isMetadataTable || writeConfig.isSimpleBucketIndex()),
    "Non-blocking concurrency control requires the MOR table with simple bucket index "
        + "or it has to be Metadata table");
```

**设计权衡**：
- COW 表每次写入创建新的 Base File 替换旧的
- 两个 Writer 同时修改同一个 File Group 会产生两个新 Base File
- 文件系统无法处理这种冲突（不允许两个文件同时作为同一个 File Group 的最新 Base File）
- MOR 表的 Log File 是追加式的，不同 Writer 创建不同的 Log File，天然支持并发

### 架构演进历史

- **0.x 版本**：只支持 SINGLE_WRITER 和 OCC
- **0.14.0 版本**：引入 NBCC（RFC-66），支持 MOR + Bucket Index 场景
- **未来方向**：
  - RFC-42（Consistent Hashing Index）：支持动态调整 bucket 数量
  - RFC-91（Storage-based lock provider）：消除外部锁依赖

### 与 MVCC 的相似性和差异

**相似性**：
- 都是多版本并发控制
- 都通过版本号（Hudi 用 instant time）区分不同版本
- 都在读取时或后台任务中解决冲突

**差异**：
- MVCC（如 PostgreSQL）：版本存储在同一个数据页中，通过 tuple header 区分
- Hudi NBCC：版本存储在不同的 Log File 中，通过文件名区分

## 3.1 为什么需要 NBCC

### OCC 在高并发场景下的问题

Hudi 传统的多写者并发控制是 OCC（Optimistic Concurrency Control，乐观并发控制）。OCC 的工作原理：

```
Writer A: 开始写入 → ... → 准备提交 → 检查冲突 → 提交/回滚
Writer B: 开始写入 → ... → 准备提交 → 检查冲突 → 提交/回滚
```

冲突检测的粒度是 **File Group**：如果两个 Writer 修改了同一个 File Group 的文件，就认为有冲突，后提交的 Writer 必须回滚重试。

在高并发流式写入场景中，这种机制面临严重问题：

1. **Bloom Filter Index 场景**：多个 Writer 倾向于写入小文件（small file handling），同一个小文件会被多个 Writer 同时选中 → 冲突率极高
2. **Hash Index 场景**：记录通过 hash 均匀分布到所有 bucket，每个 Writer 都可能触碰所有 bucket → 冲突几乎不可避免
3. **重试风暴**：冲突后回滚重试，但重试时可能再次冲突 → 恶性循环

RFC-66（JIRA: HUDI-5672）描述了这个问题：

> "Apache Hudi community has seen instant time based OCC could cause serious write-write conflicts for important application scenarios, e.g., multi-writer ingestion."

### NBCC 的核心思想

NBCC（Non-Blocking Concurrency Control）的核心思想是：**通过巧妙的架构约束（MOR + Bucket Index），使得多个 Writer 即使同时写入同一个 File Group，也不会产生真正的冲突。**

关键洞察：在 MOR 表中，不同 Writer 写入的是不同的 Log File（因为 Log File 带有 Writer 标识和时间戳）。同一个记录的多个版本分散在不同的 Log File 中，合并在读取时（Merge on Read）或 Compaction 时进行。

```
Writer A writes: filegroup_X.log.1_0-1-001_20250415010000
Writer B writes: filegroup_X.log.1_0-2-002_20250415010100

两个 Writer 写入同一个 File Group 的不同 Log File → 无冲突！
```

## 3.2 WriteConcurrencyMode 枚举

源码路径：`hudi-common/src/main/java/org/apache/hudi/common/model/WriteConcurrencyMode.java`

```java
@EnumDescription("Concurrency modes for write operations.")
public enum WriteConcurrencyMode {
  // 单写者模式：最大吞吐量
  @EnumFieldDescription("Only one active writer to the table. Maximizes throughput.")
  SINGLE_WRITER,

  // OCC：多写者 + 惰性冲突解决
  @EnumFieldDescription("Multiple writers can operate on the table with lazy conflict resolution "
      + "using locks. This means that only one writer succeeds if multiple writers write to the "
      + "same file group.")
  OPTIMISTIC_CONCURRENCY_CONTROL,

  // NBCC：多写者 + 非阻塞冲突解决
  @EnumFieldDescription("Multiple writers can operate on the table with non-blocking conflict resolution. "
      + "The writers can write into the same file group with the conflicts resolved automatically "
      + "by the query reader and the compactor.")
  NON_BLOCKING_CONCURRENCY_CONTROL;

  public boolean supportsMultiWriter() {
    return this == OPTIMISTIC_CONCURRENCY_CONTROL || this == NON_BLOCKING_CONCURRENCY_CONTROL;
  }

  public boolean isNonBlockingConcurrencyControl() {
    return this == NON_BLOCKING_CONCURRENCY_CONTROL;
  }
}
```

三种模式的对比：

| 模式 | 多写者 | 冲突处理 | 写入吞吐 | 适用场景 |
|------|-------|---------|---------|---------|
| SINGLE_WRITER | 否 | 无 | 最高 | 单一 ETL Pipeline |
| OCC | 是 | Lock + 回滚重试 | 中等（冲突多时下降） | 低频批量写入 |
| NBCC | 是 | 无需处理（架构设计避免冲突） | 高 | 高频流式多写者 |

## 3.3 NBCC 的约束条件

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieWriteConfig.java`（第 3753-3757 行）

```java
if (writeConcurrencyMode == WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL) {
    boolean isMetadataTable = HoodieTableMetadata.isMetadataTable(writeConfig.getBasePath());
    checkArgument(
        writeConfig.getTableType().equals(HoodieTableType.MERGE_ON_READ)
            && (isMetadataTable || writeConfig.isSimpleBucketIndex()),
        "Non-blocking concurrency control requires the MOR table with simple bucket index "
            + "or it has to be Metadata table");
}
```

NBCC 有两个硬性约束：

### 约束 1：必须使用 MOR 表

> **为什么？** COW 表每次写入都会创建新的 Base File 来替换旧的。如果两个 Writer 同时修改同一个 File Group，它们各自会创建一个新的 Base File，这就产生了不可调和的冲突——文件系统上不允许两个文件同时作为同一个 File Group 的最新 Base File。
>
> 而 MOR 表的写入产出物是 Log File，不同 Writer 可以创建不同的 Log File 挂在同一个 File Group 下。多个 Log File 的合并可以延迟到读取时或 Compaction 时，天然支持"先写后合并"。

### 约束 2：必须使用 Simple Bucket Index（或是 Metadata Table）

> **为什么需要确定性的桶分配？** NBCC 的前提是"相同 Record Key 的不同版本最终能合并"。Simple Bucket Index 使用确定性哈希算法：`bucket_id = hash(record_key) % num_buckets`。无论哪个 Writer 处理同一条记录，它必然被路由到同一个 Bucket（即同一个 File Group）。
>
> 如果使用 Bloom Filter Index 或其他非确定性索引，不同 Writer 可能将同一条记录写入不同的 File Group，导致数据不一致（同一条记录出现在两个地方）。

## 3.4 BucketIdentifier — 确定性哈希分桶

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/bucket/BucketIdentifier.java`

```java
public class BucketIdentifier implements Serializable {
  // 固定的 File ID 后缀，确保不同 Writer 对同一个 bucket 使用相同的 File ID
  private static final String CONSTANT_FILE_ID_SUFFIX = "-0000-0000-0000-000000000000";

  public static int getBucketId(String recordKey, List<String> indexKeyFields, int numBuckets) {
    return getBucketId(getHashKeys(recordKey, indexKeyFields), numBuckets);
  }

  public static int getBucketId(List<String> hashKeyFields, int numBuckets) {
    return (hashKeyFields.hashCode() & Integer.MAX_VALUE) % numBuckets;
  }

  public static String partitionBucketIdStr(String partition, int bucketId) {
    // 格式: {partition}_{bucket_id}
    // bucket id 是 8 位数字，前面补零
    StringBuilder sb = new StringBuilder()
        .append(partition)
        .append('_');
    // ...
  }
}
```

**关键设计：**

1. **确定性哈希**：`hashKeyFields.hashCode() & Integer.MAX_VALUE) % numBuckets`，相同的 Record Key 永远路由到同一个 Bucket
2. **固定 File ID 后缀**：`CONSTANT_FILE_ID_SUFFIX = "-0000-0000-0000-000000000000"`，确保不同 Writer 对同一个 Bucket 生成相同的 File ID，这样它们的 Log File 会挂在同一个 File Group 下

> **这是 NBCC 能工作的数据路由基础。** 如果 File ID 不确定，不同 Writer 就会创建不同的 File Group，数据就散落了。

## 3.5 冲突避免机制深度解析

NBCC 的冲突避免不是通过检测和重试来实现的，而是通过架构设计让冲突根本不会发生：

```
Writer A (instant_time = t1):
  record_key="user_001" → hash → bucket_5 → filegroup_5.log.1_t1

Writer B (instant_time = t2):
  record_key="user_001" → hash → bucket_5 → filegroup_5.log.1_t2

两个 Writer 写入同一个 File Group 的不同 Log File。
没有文件覆盖，没有冲突。

读取时：
  Reader 扫描 filegroup_5 的所有 Log Files
  → 发现 user_001 有两个版本 (t1 和 t2)
  → 按照 event time / commit time 选择最新版本
  → 返回给用户

Compaction 时：
  Compactor 合并 base file + 所有 log files
  → 解决 user_001 的多版本冲突
  → 写出最终的 Base File
```

### needResolveWriteConflict 方法

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieWriteConfig.java`

```java
public boolean needResolveWriteConflict(WriteOperationType operationType,
    boolean isMetadataTable, HoodieWriteConfig config, HoodieTableConfig tableConfig) {
    WriteConcurrencyMode mode = getWriteConcurrencyMode();
    switch (mode) {
      case SINGLE_WRITER:
        return false;  // 单写者，无需冲突检测
      case OPTIMISTIC_CONCURRENCY_CONTROL:
        return true;   // OCC 总是需要冲突检测
      case NON_BLOCKING_CONCURRENCY_CONTROL: {
        if (isMetadataTable) {
          return false; // MDT 的 NBCC 无需冲突检测
        } else {
          // NBCC 只在 BULK_INSERT 时需要冲突检测
          return WriteOperationType.BULK_INSERT == operationType;
        }
      }
    }
}
```

> **为什么 BULK_INSERT 在 NBCC 下也需要冲突检测？** BULK_INSERT 可能会创建新的 Base File（而非追加 Log File），这时如果两个 Writer 同时 BULK_INSERT 同一个 partition，可能会产生文件冲突。

## 3.6 ConflictResolutionStrategy 接口

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/ConflictResolutionStrategy.java`

```java
public interface ConflictResolutionStrategy {
  /**
   * 获取需要检查冲突的候选 instant 流
   */
  Stream<HoodieInstant> getCandidateInstants(
      HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
      Option<HoodieInstant> lastSuccessfulInstant);

  /**
   * 判断两个并发操作是否冲突
   */
  boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation);

  /**
   * 解决两个并发操作之间的冲突
   */
  Option<HoodieCommitMetadata> resolveConflict(
      HoodieTable table, ConcurrentOperation thisOperation,
      ConcurrentOperation otherOperation) throws HoodieWriteConflictException;

  /**
   * 是否需要在提交前执行冲突检查
   */
  boolean isPreCommitRequired();
}
```

### 策略继承体系

```
ConflictResolutionStrategy (接口)
    └── SimpleConcurrentFileWritesConflictResolutionStrategy (基于文件级冲突检测)
            ├── PreferWriterConflictResolutionStrategy (优先保证写入方)
            └── BucketIndexConcurrentFileWritesConflictResolutionStrategy (基于 Bucket 级冲突检测)
```

## 3.7 SimpleConcurrentFileWritesConflictResolutionStrategy

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/SimpleConcurrentFileWritesConflictResolutionStrategy.java`

```java
@Override
public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    // 1. 先检查回滚冲突
    if (isRollbackConflict(thisOperation, otherOperation)) {
      return true;
    }

    // 2. 检查文件级别冲突：两个操作修改的 (partition, fileId) 是否有交集
    Set<Pair<String, String>> partitionAndFileIdsSetForFirstInstant =
        thisOperation.getMutatedPartitionAndFileIds();
    Set<Pair<String, String>> partitionAndFileIdsSetForSecondInstant =
        otherOperation.getMutatedPartitionAndFileIds();
    Set<Pair<String, String>> intersection = new HashSet<>(partitionAndFileIdsSetForFirstInstant);
    intersection.retainAll(partitionAndFileIdsSetForSecondInstant);
    if (!intersection.isEmpty()) {
      log.info("Found conflicting writes between first operation = " + thisOperation
          + ", second operation = " + otherOperation
          + " , intersecting file ids " + intersection);
      return true;
    }
    return false;
}
```

**冲突检测逻辑：** 如果两个并发操作修改了同一个 `(partition, fileId)` 对，则认为有冲突。

### resolveConflict — Log Compaction 的特殊处理

```java
@Override
public Option<HoodieCommitMetadata> resolveConflict(
    HoodieTable table, ConcurrentOperation thisOperation,
    ConcurrentOperation otherOperation) throws HoodieWriteConflictException {
    // Compaction 的冲突处理
    if (otherOperation.getOperationType() == WriteOperationType.COMPACT) {
      if (compareTimestamps(otherOperation.getInstantTimestamp(), LESSER_THAN,
          thisOperation.getInstantTimestamp())) {
        return thisOperation.getCommitMetadataOption();
      }
    }
    // Log Compaction 的特殊处理：允许与其他 delta commit 共存
    else if (HoodieTimeline.LOG_COMPACTION_ACTION.equals(thisOperation.getInstantActionType())) {
      return thisOperation.getCommitMetadataOption();
    }
    // 其他情况：抛出冲突异常
    throw new HoodieWriteConflictException(...);
}
```

> **为什么 Log Compaction 可以与其他 delta commit 共存？** 因为 Log Compaction 是一种"重写"操作——它将多个旧 Log Block 合并成一个新的 Log Block。即使有其他 Writer 同时写入新的 Log File，Log Compaction 的结果和新的 Log File 可以在读取时通过 commit ordering 正确合并。

## 3.8 PreferWriterConflictResolutionStrategy

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/PreferWriterConflictResolutionStrategy.java`

这是 NBCC 模式下使用的冲突解决策略，核心思想是**优先保证数据写入（Ingestion），让 Table Service（Compaction/Clustering）让步**。

```java
public class PreferWriterConflictResolutionStrategy
    extends SimpleConcurrentFileWritesConflictResolutionStrategy {

  @Override
  public Stream<HoodieInstant> getCandidateInstants(
      HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
      Option<HoodieInstant> lastSuccessfulInstant, Option<HoodieWriteConfig> writeConfigOpt) {
    HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();
    boolean isCurrentOperationClustering = ClusteringUtils.isClusteringInstant(
        activeTimeline, currentInstant, metaClient.getInstantGenerator());

    if (isCurrentOperationClustering || COMPACTION_ACTION.equals(currentInstant.getAction())) {
      // Table Service (Clustering/Compaction): 需要检查所有已完成和 inflight 的写入
      return getCandidateInstantsForTableServicesCommits(
          activeTimeline, currentInstant, isCurrentOperationClustering, metaClient, writeConfigOpt);
    } else {
      // 数据写入 (Ingestion): 只检查已完成的写入 + 回滚冲突
      return Stream.concat(
          getCandidateInstantsForNonTableServicesCommits(activeTimeline, currentInstant),
          getCandidateInstantsForRollbackConflict(activeTimeline, currentInstant));
    }
  }
}
```

**设计哲学：**

1. **数据写入 vs Table Service 冲突**：Table Service 让步（回滚），数据写入优先通过
2. **数据写入 vs 数据写入冲突**：通过 NBCC 架构设计避免（MOR + Bucket Index）
3. **Table Service vs Table Service 冲突**：正常的文件级冲突检测

### Clustering vs Pending Ingestion 的特殊处理

```java
@Override
public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    if (isClusteringBlockForPendingIngestion
        && WriteOperationType.CLUSTER.equals(thisOperation.getOperationType())
        && isRequestedIngestionInstant(otherOperation)) {
      log.info("Clustering operation {} conflicts with pending ingestion instant {} "
          + "that has an active heartbeat", thisOperation, otherOperation);
      return true;
    }
    return super.hasConflict(thisOperation, otherOperation);
}
```

> **为什么 Clustering 需要对 Pending Ingestion 特殊处理？** Clustering 会重写整个 File Group（创建新的 Base File + 删除旧文件），如果有 Ingestion 正在向同一个 File Group 追加 Log File，Clustering 完成后这些 Log File 会变成"孤儿"（所属的 File Group 已被 Clustering 替换）。所以 Clustering 遇到 Pending Ingestion 时需要主动失败。

## 3.9 BucketIndexConcurrentFileWritesConflictResolutionStrategy

源码路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/BucketIndexConcurrentFileWritesConflictResolutionStrategy.java`

```java
@Override
public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    // 将 (partition, fileId) 转换为 (partition, bucketId)
    Set<String> partitionBucketIdSetForFirstInstant = thisOperation
        .getMutatedPartitionAndFileIds().stream()
        .map(partitionAndFileId -> BucketIdentifier.partitionBucketIdStr(
            partitionAndFileId.getLeft(),
            BucketIdentifier.bucketIdFromFileId(partitionAndFileId.getRight())))
        .collect(Collectors.toSet());

    Set<String> partitionBucketIdSetForSecondInstant = otherOperation
        .getMutatedPartitionAndFileIds().stream()
        .map(partitionAndFileId -> BucketIdentifier.partitionBucketIdStr(
            partitionAndFileId.getLeft(),
            BucketIdentifier.bucketIdFromFileId(partitionAndFileId.getRight())))
        .collect(Collectors.toSet());

    Set<String> intersection = new HashSet<>(partitionBucketIdSetForFirstInstant);
    intersection.retainAll(partitionBucketIdSetForSecondInstant);
    if (!intersection.isEmpty()) {
      log.info("Found conflicting writes ... intersecting bucket ids " + intersection);
      return true;
    }
    return false;
}
```

与 `SimpleConcurrentFileWritesConflictResolutionStrategy` 的核心区别：**冲突检测的粒度从 File ID 级别提升到 Bucket ID 级别**。

> **为什么？** 在 Bucket Index 下，不同 Writer 对同一个 Bucket 写入的 Log File 具有不同的 File ID（因为 Log File 的 File ID 包含 Writer 标识），但它们归属于同一个 Bucket。所以冲突检测应该基于 Bucket ID 而非 File ID。

## 3.10 NBCC 配置与生产实践

```properties
# 启用 NBCC
hoodie.write.concurrency.mode=NON_BLOCKING_CONCURRENCY_CONTROL

# 必须使用 MOR 表
hoodie.table.type=MERGE_ON_READ

# 必须使用 Simple Bucket Index
hoodie.index.type=BUCKET
hoodie.index.bucket.engine=SIMPLE
hoodie.bucket.index.num.buckets=128

# 必须使用 LAZY 清理策略
hoodie.clean.failed.writes.policy=LAZY

# 推荐配合 Log Compaction 使用
hoodie.log.compaction.enable=true
hoodie.log.compaction.inline=true
```

### 生产注意事项

1. **Bucket 数量规划**：需要根据数据量预估，一旦设定不能轻易修改（Simple Bucket Index 不支持动态调整）
2. **Compaction 频率调整**：NBCC 下多个 Writer 会产生大量 Log File，需要更频繁的 Compaction
3. **Lock Provider 仍然需要**：NBCC 模式下仍然需要 Lock Provider 来协调 Compaction/Clustering 等 Table Service 的并发
4. **不支持 BULK_INSERT 的无冲突写入**：BULK_INSERT 操作仍然需要冲突检测

---

# 第四部分：Record Position Merge — 基于位置的合并优化

## 1. 解决什么问题

Record Position Merge 解决的核心问题是**MOR 表读取路径中 Key-Based Merge 的性能瓶颈**。

### 业务痛点

源码证据：`PositionBasedFileGroupRecordBuffer.java` 第 59-63 行的类注释：
```java
/**
 * A buffer that is used to store log records by {@link org.apache.hudi.common.table.log.HoodieMergedLogRecordReader}
 * by calling the {@link #processDataBlock} and {@link #processDeleteBlock} methods into record position based map.
 * Here the position means that record position in the base file.
 */
public class PositionBasedFileGroupRecordBuffer<T> extends KeyBasedFileGroupRecordBuffer<T>
```

传统 Key-Based Merge 的性能瓶颈：

1. **HashMap 内存消耗**：
   - Log File 中的更新记录存入 `HashMap<RecordKey, Record>`
   - 如果 Log File 有大量更新，HashMap 消耗大量 JVM 堆内存
   - 复合 Key（如 `user_id:order_id`）的字符串构造和存储开销更大

2. **字符串比较开销**：
   - Record Key 通常是字符串类型
   - `hashCode()` 和 `equals()` 操作相对昂贵
   - 每条 Base File 记录都需要提取 Key 并在 HashMap 中查找

3. **序列化/反序列化开销**：
   - 从 Log Block 读取记录需要完整反序列化
   - 提取 Record Key 需要访问特定字段

### 如果没有 Position-Based Merge 会怎样

**性能对比**（理论分析）：
- Key-Based：`O(N)` 次字符串 hashCode + HashMap 查找
- Position-Based：`O(N)` 次整数比较 + RoaringBitmap 查找

在大规模数据集（百万级记录）上，Position-Based 可以带来 2-5 倍的性能提升。

### 实际应用场景

1. **大表增量读取**：Base File 几百 MB，Log Files 几十 MB，Position-Based 显著减少内存占用
2. **复合主键场景**：Record Key 是多字段组合（如 `region:city:user_id`），字符串操作开销大
3. **高频查询场景**：Presto/Trino 查询 MOR 表，Position-Based 减少查询延迟

## 2. 有什么坑

### 坑1：Base File 被 Compaction 重写后失效

源码证据：`PositionBasedFileGroupRecordBuffer.java` 的 `extractRecordPositions` 方法第 331-336 行：
```java
// 检查 log block 中记录的 Base File InstantTime 是否与当前 File Group 匹配
String blockBaseFileInstantTime = logBlock.getBaseFileInstantTimeOfPositions();
if (StringUtils.isNullOrEmpty(blockBaseFileInstantTime)
    || !baseFileInstantTime.equals(blockBaseFileInstantTime)) {
  LOG.debug("The record positions cannot be used because the base file instant time "
      + "is either missing or different from the base file to merge.");
  return null;  // 位置信息无效
}
```

**陷阱**：
- Log Block 中记录的位置是基于写入时的 Base File
- 如果 Base File 被 Compaction 重写，行号会发生变化
- 此时 Position-Based Merge 会自动降级到 Key-Based Merge
- 用户无感知，但性能优势消失

### 坑2：旧版本 Log Block 缺少位置信息

源码证据：`PositionBasedFileGroupRecordBuffer.java` 第 97-102 行：
```java
// Extract positions from data block.
List<Long> recordPositions = extractRecordPositions(dataBlock, baseFileInstantTime);
if (recordPositions == null) {
  LOG.debug("Falling back to key based merge for data block");
  fallbackToKeyBasedBuffer();
  super.processDataBlock(dataBlock, keySpecOpt);
  return;
}
```

**陷阱**：
- 旧版本 Hudi（< 1.0）写入的 Log Block 可能没有 record position 元数据
- 读取这些旧 Log Block 时会降级到 Key-Based Merge
- 升级到新版本后，需要等待 Compaction 重写才能享受 Position-Based 优势

### 坑3：RoaringBitmap 为空的情况

源码证据：`PositionBasedFileGroupRecordBuffer.java` 的 `extractRecordPositions` 方法第 339-343 行：
```java
// 从 RoaringBitmap 中提取位置列表
Roaring64NavigableMap positions = logBlock.getRecordPositions();
if (positions == null || positions.isEmpty()) {
  LOG.info("No record position info is found...");
  return null;
}
```

**陷阱**：
- 如果 Log Block 的 RoaringBitmap 为空（可能是写入时的 bug 或特殊场景）
- Position-Based Merge 会降级
- 日志只打印 INFO 级别，容易被忽略

### 坑4：混合策略（Hybrid Strategy）的复杂性

源码证据：`PositionBasedFileGroupRecordBuffer.java` 第 71 行：
```java
private boolean needToDoHybridStrategy = false;
```

以及 `fallbackToKeyBasedBuffer` 方法第 1419-1432 行：
```java
private void fallbackToKeyBasedBuffer() {
    readerContext.setShouldMergeUseRecordPosition(false);
    ArrayList<Serializable> positions = new ArrayList<>(records.keySet());
    for (Serializable position : positions) {
      BufferedRecord<T> entry = records.get(position);
      String recordKey = entry.getRecordKey();
      if (!entry.isDelete() || recordKey != null) {
        records.put(recordKey, entry);
        records.remove(position);
      } else {
        needToDoHybridStrategy = true;
      }
    }
}
```

**陷阱**：
- 如果某些删除记录没有 Key 信息，会启用"混合策略"
- 混合策略同时使用位置和 Key 两种方式进行合并
- 代码逻辑复杂，容易出现边界情况的 bug

### 坑5：不适用于 COW 表

**陷阱**：
- COW 表没有 Log File，Position-Based Merge 无用武之地
- 配置相关参数不会报错，但也不会生效

### 性能陷阱

**陷阱**：Position-Based Merge 的性能优势在以下场景下不明显：
- Base File 很小（< 10MB）
- Log Files 更新记录很少（< 1000 条）
- Record Key 是简单类型（如整数）

在这些场景下，Key-Based Merge 的开销本身就不大，Position-Based 的优化效果有限。

## 3. 核心概念解释

### Record Position 的定义

源码证据：`PositionBasedFileGroupRecordBuffer.java` 第 67-68 行：
```java
private static final String ROW_INDEX_COLUMN_NAME = "row_index";
public static final String ROW_INDEX_TEMPORARY_COLUMN_NAME = "_tmp_metadata_" + ROW_INDEX_COLUMN_NAME;
```

**核心概念**：
- Record Position 是记录在 Base File（Parquet）中的行号（从 0 开始）
- Parquet 文件天然有行号（Row Index）
- Log Block 写入时记录每条更新对应的 Base File 行号

### RoaringBitmap 的使用

源码证据：`PositionBasedFileGroupRecordBuffer.java` 第 44 行导入：
```java
import org.roaringbitmap.longlong.Roaring64NavigableMap;
```

**核心概念**：
- Hudi 使用 `Roaring64NavigableMap`（64 位 Roaring Bitmap）存储位置信息
- Roaring Bitmap 是一种高效的压缩位图数据结构
- 特别适合存储稀疏的整数集合（如文件中被更新的行号）

**优势**：
- 空间效率：相比 `HashSet<Long>`，内存占用可减少 10-100 倍
- 查询效率：O(1) 查找
- 迭代效率：支持有序迭代
- 序列化效率：压缩格式适合存储在 Log Block 头部

### Position-Based vs Key-Based 对比

| 维度 | Key-Based Merge | Position-Based Merge |
|------|----------------|---------------------|
| 关联键 | Record Key（字符串） | Row Position（整数） |
| 数据结构 | HashMap<String, Record> | Map<Long, Record> + RoaringBitmap |
| 内存占用 | 高（字符串 + HashMap 开销） | 低（整数 + Bitmap 压缩） |
| 查找效率 | O(1) 但常数较大 | O(1) 且常数很小 |
| 适用条件 | 总是可用 | 需要位置信息有效 |
| 降级机制 | 无 | 自动降级到 Key-Based |

### 降级触发条件

源码证据：`PositionBasedFileGroupRecordBuffer.java` 的多处降级检查：

Position-Based Merge 在以下情况下会降级到 Key-Based Merge：

1. **Base File 被 Compaction 重写**：`baseFileInstantTime` 不匹配
2. **Log Block 缺少位置信息**：`recordPositions == null`
3. **Instant Time 不匹配**：`blockBaseFileInstantTime != baseFileInstantTime`
4. **RoaringBitmap 为空**：`positions.isEmpty()`

### 与其他系统的对比

- **Parquet Row Group Filtering**：基于行组级别的统计信息过滤
- **ORC Predicate Pushdown**：基于 Stripe 级别的索引
- **Hudi Position-Based Merge**：基于行级别的位置索引，用于 merge 而非过滤

## 4. 设计理念

### 整数比较 vs 字符串比较

源码证据：`PositionBasedFileGroupRecordBuffer.java` 第 146 行：
```java
long recordPosition = recordPositions.get(recordIndex++);
```

以及第 405 行：
```java
BufferedRecord<T> logRecordInfo = records.remove(nextRecordPosition++);
```

**设计哲学**：
- 整数比较（`position == 42`）比字符串比较（`"user_001".equals(key)`）快一个数量级
- 整数作为 Map 的 key，hashCode 计算是 O(1)
- 字符串作为 Map 的 key，hashCode 计算是 O(length)

### 优雅降级（Graceful Degradation）

源码证据：`PositionBasedFileGroupRecordBuffer.java` 继承自 `KeyBasedFileGroupRecordBuffer`：
```java
public class PositionBasedFileGroupRecordBuffer<T> extends KeyBasedFileGroupRecordBuffer<T>
```

**设计理念**：
- Position-Based 是 Key-Based 的"优化版本"，而非"替代版本"
- 当优化条件不满足时，无缝退回到 Key-Based
- 用户无需关心底层使用了哪种合并策略
- 保证了正确性优先，性能优化其次

### 空间换时间的权衡

**设计权衡**：
- **额外空间**：Log Block 需要存储 RoaringBitmap（通常几 KB 到几十 KB）
- **时间节省**：读取时避免字符串操作，节省 CPU 和内存

在大规模数据集上，这个权衡是值得的。

### 为什么不总是使用 Position-Based

源码证据：`PositionBasedFileGroupRecordBuffer.java` 第 92-95 行：
```java
if (!readerContext.getShouldMergeUseRecordPosition()) {
  super.processDataBlock(dataBlock, keySpecOpt);
  return;
}
```

**设计考虑**：
- Position-Based 的适用条件比较严格（需要位置信息有效）
- 如果强制使用，可能导致读取失败
- 通过配置开关（`shouldMergeUseRecordPosition`）让用户选择

### 架构演进历史

- **0.x 版本**：只有 Key-Based Merge
- **1.0.0 版本**：引入 Position-Based Merge，作为可选优化
- **未来方向**：
  - 自动检测是否适合使用 Position-Based（基于统计信息）
  - 支持更多的位置索引格式（如 Bloom Filter + Position）

### 与 Parquet Row Index 的关系

**设计理念**：
- Parquet 文件内部有 Row Group 和 Row Index 的概念
- Hudi 利用 Parquet 的 Row Index 作为 Position-Based Merge 的基础
- 不需要额外的索引结构，直接复用 Parquet 的元数据

这是一种"利用现有基础设施"的设计思想，而非"重新发明轮子"。

## 4.1 传统 Key-Based Merge 的性能瓶颈

在 MOR 表的读取路径中，核心操作是将 Base File 中的记录与 Log File 中的更新记录进行合并（Merge）。传统的 Key-Based Merge 流程如下：

```
1. 扫描所有 Log Files，将更新记录存入 HashMap<RecordKey, Record>
2. 遍历 Base File 的每条记录
3. 对每条记录：
   a. 提取 Record Key
   b. 在 HashMap 中查找是否有对应的更新
   c. 如果有更新，执行合并；否则直接返回
```

**性能瓶颈：**

1. **HashMap 内存消耗**：如果 Log File 中有大量更新，HashMap 会消耗大量 JVM 堆内存
2. **字符串比较开销**：Record Key 通常是字符串类型，hashCode 和 equals 操作相对昂贵
3. **复合 Key 的额外开销**：如果 Record Key 是复合键（如 `user_id:order_id`），字符串构造和比较更加昂贵
4. **序列化/反序列化**：从 Log Block 中读取记录需要完整反序列化

## 4.2 Position-Based Merge 的原理

Position-Based Merge 的核心思想是：**用记录在 Base File 中的行号（Row Position/Row Index）来替代 Record Key 作为合并的关联键。**

在 Parquet 文件中，每条记录天然有一个行号（从 0 开始的位置索引）。如果写入 Log File 时同时记录了"这条更新对应 Base File 的第 N 行"，那么合并时就可以直接用行号匹配，无需字符串比较。

```
传统 Key-Based:
  Base File 记录: {key="user_001", name="Alice"} → 在 HashMap 查找 "user_001"
  Log File 记录:  {key="user_001", name="Alice_Updated"}

Position-Based:
  Base File 记录: 第 42 行 → 检查 BitMap 位置 42 是否有更新
  Log File 记录:  position=42, {name="Alice_Updated"}
```

### 为什么 Position-Based 更快？

1. **整数比较 vs 字符串比较**：`position == 42` 比 `"user_001".equals(key)` 快一个数量级
2. **RoaringBitmap vs HashMap**：用 RoaringBitmap 标记哪些行有更新，内存占用仅为 HashMap 的几十分之一
3. **无需 Record Key 提取**：读取 Base File 时不需要反序列化 Record Key 列
4. **顺序访问**：Base File 是按行号顺序扫描的，Position-Based 匹配完全是顺序的

## 4.3 PositionBasedFileGroupRecordBuffer 源码解析

源码路径：`hudi-common/src/main/java/org/apache/hudi/common/table/read/buffer/PositionBasedFileGroupRecordBuffer.java`

### 类定义

```java
public class PositionBasedFileGroupRecordBuffer<T> extends KeyBasedFileGroupRecordBuffer<T> {
  private static final String ROW_INDEX_COLUMN_NAME = "row_index";
  public static final String ROW_INDEX_TEMPORARY_COLUMN_NAME = "_tmp_metadata_" + ROW_INDEX_COLUMN_NAME;
  protected final String baseFileInstantTime;
  private long nextRecordPosition = 0L;
  private boolean needToDoHybridStrategy = false;
  // ...
}
```

**设计亮点：继承自 `KeyBasedFileGroupRecordBuffer`**。这意味着 Position-Based 可以在必要时降级到 Key-Based Merge，保证了正确性。

### processDataBlock — 处理数据块

```java
@Override
public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException {
    if (!readerContext.getShouldMergeUseRecordPosition()) {
      // 如果不使用位置合并，降级到 Key-Based
      super.processDataBlock(dataBlock, keySpecOpt);
      return;
    }

    // 从数据块头部提取记录位置信息
    List<Long> recordPositions = extractRecordPositions(dataBlock, baseFileInstantTime);
    if (recordPositions == null) {
      // 位置信息不可用，降级到 Key-Based
      LOG.debug("Falling back to key based merge for data block");
      fallbackToKeyBasedBuffer();
      super.processDataBlock(dataBlock, keySpecOpt);
      return;
    }

    // 遍历数据块中的记录，用位置索引存储
    try (ClosableIterator<T> recordIterator = dataBlock.getEngineRecordIterator(readerContext)) {
      int recordIndex = 0;
      while (recordIterator.hasNext()) {
        T nextRecord = recordIterator.next();
        // ...
        long recordPosition = recordPositions.get(recordIndex++);
        T evolvedNextRecord = schemaTransformerWithEvolvedSchema.getLeft().apply(nextRecord);
        boolean isDelete = readerContext.getRecordContext().isDeleteRecord(evolvedNextRecord, deleteContext);
        BufferedRecord<T> bufferedRecord = BufferedRecords.fromEngineRecord(
            evolvedNextRecord, schema, readerContext.getRecordContext(),
            orderingFieldNames, isDelete);
        // 以位置（Long）作为 key 存入 records map
        processNextDataRecord(bufferedRecord, recordPosition);
      }
    }
}
```

### extractRecordPositions — 提取位置信息

```java
protected static List<Long> extractRecordPositions(
    HoodieLogBlock logBlock, String baseFileInstantTime) throws IOException {
    List<Long> blockPositions = new ArrayList<>();

    // 检查 log block 中记录的 Base File InstantTime 是否与当前 File Group 匹配
    String blockBaseFileInstantTime = logBlock.getBaseFileInstantTimeOfPositions();
    if (StringUtils.isNullOrEmpty(blockBaseFileInstantTime)
        || !baseFileInstantTime.equals(blockBaseFileInstantTime)) {
      LOG.debug("The record positions cannot be used because the base file instant time "
          + "is either missing or different from the base file to merge.");
      return null;  // 位置信息无效
    }

    // 从 RoaringBitmap 中提取位置列表
    Roaring64NavigableMap positions = logBlock.getRecordPositions();
    if (positions == null || positions.isEmpty()) {
      LOG.info("No record position info is found...");
      return null;
    }

    Iterator<Long> iterator = positions.iterator();
    while (iterator.hasNext()) {
      blockPositions.add(iterator.next());
    }
    return blockPositions;
}
```

> **为什么要检查 baseFileInstantTime？** 如果 Base File 被 Compaction 重写过，行号会发生变化。此时 Log Block 中记录的位置是基于旧 Base File 的，对新 Base File 无效，必须降级到 Key-Based Merge。这是 Position-Based Merge 正确性保证的关键。

### processDeleteBlock — 处理删除块

```java
@Override
public void processDeleteBlock(HoodieDeleteBlock deleteBlock) throws IOException {
    if (!readerContext.getShouldMergeUseRecordPosition()) {
      super.processDeleteBlock(deleteBlock);
      return;
    }

    List<Long> recordPositions = extractRecordPositions(deleteBlock, baseFileInstantTime);
    if (recordPositions == null) {
      fallbackToKeyBasedBuffer();
      super.processDeleteBlock(deleteBlock);
      return;
    }

    switch (recordMergeMode) {
      case COMMIT_TIME_ORDERING:
        int commitTimeBasedRecordIndex = 0;
        DeleteRecord[] deleteRecords = deleteBlock.getRecordsToDelete();
        for (Long recordPosition : recordPositions) {
          DeleteRecord deleteRecord = deleteRecords[commitTimeBasedRecordIndex++];
          BufferedRecord<T> record = BufferedRecords.fromDeleteRecord(
              deleteRecord, readerContext.getRecordContext());
          records.put(recordPosition, record);
        }
        return;
      case EVENT_TIME_ORDERING:
      case CUSTOM:
      default:
        // ...
    }
}
```

### hasNextBaseRecord — 合并阶段

```java
@Override
protected boolean hasNextBaseRecord(T baseRecord) throws IOException {
    if (!readerContext.getShouldMergeUseRecordPosition()) {
      return doHasNextFallbackBaseRecord(baseRecord);
    }

    // 从 Base File 记录中提取行号
    nextRecordPosition = readerContext.getRecordContext().extractRecordPosition(
        baseRecord, readerSchema, ROW_INDEX_TEMPORARY_COLUMN_NAME, nextRecordPosition);
    // 用行号在 records map 中查找并移除对应的 log 记录
    BufferedRecord<T> logRecordInfo = records.remove(nextRecordPosition++);
    return super.hasNextBaseRecord(baseRecord, logRecordInfo);
}
```

**合并流程：**
1. 从 Base File 的当前记录提取行号 `nextRecordPosition`
2. 在 `records` Map 中查找该行号是否有 Log 更新
3. 如果有更新，执行合并逻辑
4. 如果没有更新，直接返回 Base File 的原始记录

### fallbackToKeyBasedBuffer — 降级机制

```java
private void fallbackToKeyBasedBuffer() {
    readerContext.setShouldMergeUseRecordPosition(false);
    ArrayList<Serializable> positions = new ArrayList<>(records.keySet());
    for (Serializable position : positions) {
      BufferedRecord<T> entry = records.get(position);
      String recordKey = entry.getRecordKey();
      if (!entry.isDelete() || recordKey != null) {
        records.put(recordKey, entry);
        records.remove(position);
      } else {
        needToDoHybridStrategy = true;
      }
    }
}
```

降级时，将已经按位置存储的记录转换为按 Key 存储。如果某些删除记录没有 Key 信息，则启用"混合策略"（Hybrid Strategy），同时使用位置和 Key 两种方式进行合并。

> **这种优雅降级的设计好处是什么？** Position-Based Merge 的适用条件比较严格（需要位置信息有效），当条件不满足时，能无缝退回到 Key-Based Merge，保证读取操作不会失败。用户无需关心底层使用了哪种合并策略。

## 4.4 适用条件和降级触发场景

Position-Based Merge 在以下情况下会降级到 Key-Based Merge：

1. **Base File 被 Compaction 重写过**：Log Block 中的位置基于旧 Base File，不再有效
2. **Log Block 缺少位置信息**：旧版本写入的 Log Block 可能没有 record position 元数据
3. **Instant Time 不匹配**：Log Block 的 baseFileInstantTime 与当前 File Group 的 Base File 不一致
4. **RoaringBitmap 为空**：Log Block 的位置信息被标记为空

### RoaringBitmap 的使用

```java
Roaring64NavigableMap positions = logBlock.getRecordPositions();
```

Hudi 使用 `Roaring64NavigableMap`（64 位 Roaring Bitmap）来存储位置信息。Roaring Bitmap 是一种高效的压缩位图数据结构，特别适合存储稀疏的整数集合（如文件中被更新的行号）。

**优势：**
- 空间效率：相比 HashSet<Long>，内存占用可减少 10-100 倍
- 查询效率：O(1) 查找
- 迭代效率：支持有序迭代
- 序列化效率：压缩格式适合存储在 Log Block 头部

---

# 第五部分：Shredded Variant / VECTOR Search 等前沿特性

## 5.0 Shredded Variant — 半结构化数据优化

### 1. 解决什么问题

Shredded Variant 解决的核心问题是**半结构化数据（JSON）的存储效率和查询性能**。

#### 业务痛点

传统做法是将 JSON 存为 STRING 列，但存在以下问题：

1. **无法利用列式存储优势**：
   - JSON 字符串作为整体存储，无法享受列式压缩
   - 查询时需要解析整个 JSON 字符串
   - 无法做列裁剪（Column Pruning）

2. **查询性能差**：
   - 每次查询都需要 JSON 解析（如 `get_json_object()`）
   - 无法利用 Parquet 的谓词下推（Predicate Pushdown）
   - 统计信息（min/max）无法用于过滤

3. **存储空间浪费**：
   - JSON 字符串压缩率低
   - 重复的字段名占用大量空间

#### 实际应用场景

1. **日志分析**：应用日志包含固定字段（timestamp、level、message）和可变字段（自定义属性）
2. **IoT 数据**：设备数据有通用字段（device_id、timestamp）和设备特定字段
3. **事件追踪**：用户行为事件有公共字段（user_id、event_type）和事件特定属性

### 2. 有什么坑

#### 坑1：Unshredded Variant 不支持列统计

源码证据：`HoodieTableMetadataUtil.java` 的注释：
```java
// VARIANT (unshredded) type is excluded because it stores semi-structured data as opaque binary blobs,
// ...
// TODO: For shredded, we are able to store colstats, explore that: #17988
```

**陷阱**：
- Unshredded Variant 存储为二进制 blob，无法计算 min/max
- 查询优化器无法利用统计信息进行过滤
- Shredded Variant 的 `typed_value` 字段未来可以支持列统计（待实现）

#### 坑2：Shredded 字段选择需要权衡

**陷阱**：
- 如果 shred 太多字段，Parquet schema 会变得很宽，影响写入性能
- 如果 shred 太少字段，查询性能提升有限
- 需要根据查询模式（Query Pattern）选择热点字段

#### 坑3：Schema Evolution 的复杂性

**陷阱**：
- Shredded Variant 的 schema 变更比普通列更复杂
- 新增 shredded 字段需要重写历史数据（或使用 schema evolution）
- 删除 shredded 字段可能导致旧数据无法读取

#### 坑4：只在 Spark 4.0+ 支持

源码证据：测试类路径 `hudi-spark-datasource/hudi-spark4.0.x/src/test/java/org/apache/hudi/io/storage/row/TestHoodieRowParquetWriteSupportVariant.java`

**陷阱**：
- Variant 类型是 Spark 4.0 引入的新特性
- Spark 3.x 不支持 Variant 类型
- 需要升级到 Spark 4.0+ 才能使用

### 3. 核心概念解释

#### Variant Type 的定义

Variant 是一种特殊的数据类型，用于存储半结构化数据（如 JSON）。它允许不同行的同一列包含不同结构的数据。

#### Unshredded vs Shredded

| 维度 | Unshredded Variant | Shredded Variant |
|------|-------------------|------------------|
| 存储格式 | metadata + value（REQUIRED BINARY） | metadata + value（OPTIONAL BINARY） + typed_value（GROUP） |
| 列式存储 | 否（整体存储） | 部分是（shredded 字段） |
| 查询性能 | 差（需要解析） | 好（shredded 字段直接访问） |
| 存储空间 | 大 | 中等（shredded 字段压缩） |
| Schema 复杂度 | 低 | 高 |

#### Shredded Variant 的结构

源码证据：`HoodieRowParquetWriteSupport.java` 的实现：
```
Variant 列 "v":
  ├── metadata (REQUIRED BINARY)   -- Variant 元数据（schema 信息）
  ├── value (OPTIONAL BINARY)      -- 未被 shred 的值（fallback）
  └── typed_value (GROUP)          -- 被 shred 出来的原生列
        ├── field_a (INT64)
        ├── field_b (BINARY/UTF8)
        └── ...
```

#### 与其他系统的对比

- **Databricks Delta Lake Variant**：Hudi 的 Variant 设计借鉴了 Delta Lake
- **Snowflake VARIANT**：类似的半结构化数据类型
- **PostgreSQL JSONB**：二进制 JSON 存储，支持索引

### 4. 设计理念

#### 热点字段提取

**设计哲学**：
- 半结构化数据中往往有一些字段（如 timestamp、user_id）在几乎所有记录中都存在且被频繁查询
- Shredded Variant 允许将这些"热点字段"提取为原生列式存储
- 享受列式压缩和谓词下推的性能优势
- 同时保留 Variant 的灵活性来存储其他不规则字段

#### 渐进式优化

**设计理念**：
- 初期使用 Unshredded Variant（简单，快速上线）
- 根据查询模式分析，识别热点字段
- 逐步迁移到 Shredded Variant（性能优化）

#### 与 Parquet 的深度集成

源码证据：`HoodieRowParquetWriteSupport.java` 的实现直接操作 Parquet 的 schema 和 writer。

**设计理念**：
- 不是在 Hudi 层面模拟 Variant，而是直接利用 Parquet 的 GROUP 类型
- Shredded 字段作为 Parquet 的原生列，享受所有 Parquet 优化
- 这是一种"利用底层存储能力"的设计思想

## 5.1 VECTOR Search — 数据湖上的向量搜索

### 1. 解决什么问题

VECTOR Search 解决的核心问题是**AI/ML 场景下的向量相似度搜索与数据湖的集成**。

#### 业务痛点

传统做法是将向量数据从数据湖导出到专门的向量数据库（如 Pinecone、Milvus），但存在以下问题：

1. **数据一致性问题**：
   - 两个系统间的数据同步延迟
   - 数据湖更新后，向量数据库可能不同步
   - 需要额外的 ETL Pipeline

2. **额外的基础设施成本**：
   - 向量数据库的部署和维护成本
   - 数据存储双份（数据湖 + 向量数据库）
   - 运维复杂度增加

3. **查询割裂**：
   - 向量搜索在向量数据库中执行
   - 结构化查询在数据湖中执行
   - 无法在一个 SQL 中完成混合查询

#### 实际应用场景

1. **语义搜索**：在文档数据湖中搜索与查询文本语义相似的文档
2. **推荐系统**：在用户行为数据湖中找到相似用户或相似商品
3. **图像检索**：在图像特征数据湖中搜索相似图像

### 2. 有什么坑

#### 坑1：只支持 Brute Force 算法

源码证据：`HoodieVectorSearchTableValuedFunction.scala` 第 1582-1584 行：
```scala
object SearchAlgorithm extends Enumeration {
  val BRUTE_FORCE = Value  // 目前只支持暴力搜索
}
```

**陷阱**：
- 暴力搜索的时间复杂度是 O(N)，N 是数据集大小
- 大规模数据集（百万级以上）性能不如专业向量数据库
- 不支持 HNSW、IVF 等近似最近邻算法

#### 坑2：元素类型必须严格匹配

源码证据：`VectorDistanceUtils.scala` 的 `validateEmbeddingColumn` 方法：
```scala
field.dataType match {
  case ArrayType(FloatType, _) | ArrayType(DoubleType, _) | ArrayType(ByteType, _) => // valid
  case other => throw new HoodieAnalysisException(
    s"Embedding column '$colName' has type $other, " +
      "expected array<float>, array<double>, or array<byte>")
}
```

**陷阱**：
- corpus 和 query 必须使用相同的元素类型
- 不支持自动类型提升（如 float → double）
- 这是有意为之的设计，避免精度差异

#### 坑3：批量查询的 Cross Join 开销

源码证据：`BruteForceSearchAlgorithm.scala` 的 `buildBatchQueryPlan` 方法：
```scala
// 1. Cross Join: corpus x broadcast(query)
val scored = filteredCorpus.crossJoin(broadcast(renamedQuery))
  .withColumn(DISTANCE_COL,
    distanceUdf(col(corpusEmbeddingCol), col(QUERY_EMB_ALIAS)))
```

**陷阱**：
- Cross Join 产生 O(|corpus| * |queries|) 行中间数据
- 如果 query 表很大（几千个查询），中间数据会爆炸
- 适合小到中等规模的查询集（几十到几百个查询）

#### 坑4：向量维度必须匹配

源码证据：`BruteForceSearchAlgorithm.scala` 的 `validateQueryVectorDimension` 方法会检查维度。

**陷阱**：
- corpus 的 embedding 维度和 query 的 embedding 维度必须一致
- 如果维度不匹配，会在查询时抛出异常
- 需要在数据写入时确保维度一致性

#### 坑5：不支持增量索引

**陷阱**：
- 每次查询都是全表扫描（Brute Force）
- 无法利用增量索引加速
- 数据更新后，查询性能不会自动优化

### 3. 核心概念解释

#### 三种距离度量

源码证据：`VectorDistanceUtils.scala` 第 1700-1712 行：

| 距离度量 | 公式 | 值域 | 适用场景 |
|---------|------|------|---------|
| Cosine | 1 - cos(a, b) | [0, 2] | 文本语义相似度 |
| L2 (Euclidean) | sqrt(sum((a-b)^2)) | [0, +inf) | 图像特征匹配 |
| Dot Product | -(a . b) | (-inf, +inf) | 推荐系统 |

#### Table-Valued Function (TVF)

**核心概念**：
- TVF 是 SQL 标准中的一种函数，返回一个表（而非标量值）
- Spark SQL 支持 TVF 扩展
- Hudi 通过 TVF 机制实现向量搜索

#### 单查询 vs 批量查询

- **单查询模式**：一个查询向量 → 返回 top-K 最相似记录
- **批量查询模式**：多个查询向量 → 每个向量返回 top-K

批量查询使用 Window Function 实现分组 top-K。

#### 与专业向量数据库的对比

| 维度 | Hudi VECTOR Search | Pinecone/Milvus |
|------|-------------------|----------------|
| 搜索算法 | Brute Force | HNSW/IVF/PQ |
| 时间复杂度 | O(N) | O(log N) |
| 数据一致性 | 强一致（事务保证） | 最终一致 |
| SQL 集成 | 原生支持 | 需要额外集成 |
| 基础设施 | 无需额外部署 | 需要独立部署 |
| 适用规模 | 中小规模（< 百万） | 大规模（> 千万） |

### 4. 设计理念

#### 数据湖原生（Lake-Native）

**设计哲学**：
- 向量搜索直接在数据湖上执行，无需数据导出
- 享受 Hudi 的事务保证、增量查询、数据管理能力
- 与结构化查询无缝集成（同一个 SQL）

#### 可插拔算法框架

源码证据：`HoodieVectorSearchPlanBuilder.scala` 的 `VectorSearchAlgorithm` trait：
```scala
trait VectorSearchAlgorithm {
  def name: String

  def buildSingleQueryPlan(...): LogicalPlan

  def buildBatchQueryPlan(...): LogicalPlan
}
```

**设计理念**：
- 目前只实现了 Brute Force
- 框架设计为可扩展的
- 未来可以添加 HNSW、IVF 等近似最近邻算法
- 添加新算法只需实现 trait 并注册

#### 利用 Spark 优化器

源码证据：`BruteForceSearchAlgorithm.scala` 的执行计划：
```
Scan(corpus_table)
  → Filter(embedding IS NOT NULL)
  → Project(*, _hudi_distance = UDF(embedding))
  → TakeOrderedAndProject(k, orderBy=_hudi_distance ASC)
```

**设计理念**：
- 不是在 Hudi 层面实现排序，而是生成 Spark 逻辑计划
- Spark 会将 `orderBy + limit` 优化为 `TakeOrderedAndProject`
- 这是一种部分排序算法，复杂度为 O(N * log(k))，远优于全排序的 O(N * log(N))

#### 架构演进方向

- **当前版本**：Brute Force，适合中小规模数据集
- **未来方向**：
  - 引入 HNSW 等近似算法（需要额外的索引结构）
  - 支持增量索引更新
  - 与 Hudi 的 Secondary Index 集成

## 5.1 Variant Type 支持

### 什么是 Variant Type

Variant（变体类型）是数据湖/数据仓库领域近年来的热门话题，由 Databricks 在 Delta Lake 中率先引入。Variant 类型用于存储半结构化数据（如 JSON），允许不同行的同一列包含不同结构的数据。

传统做法是将 JSON 存为 STRING 列，但这样：
- 无法利用列式存储的压缩优势
- 查询时需要 JSON 解析，性能差
- 无法做列裁剪（Column Pruning）

### Hudi 的 Variant 类型定义

源码路径：`hudi-common/src/main/java/org/apache/hudi/common/schema/HoodieSchema.java`

Hudi 在其 `HoodieSchema` 类型系统中定义了对 Variant 类型的支持，包括两种形式：

1. **Unshredded Variant**：不拆解的变体类型，将整个半结构化数据以二进制格式（metadata + value）存储
2. **Shredded Variant**：拆解的变体类型，将 JSON 中频繁访问的字段提取为 Parquet 原生列，其余部分仍以二进制存储

### Shredded Variant 的写入优化

源码路径：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/io/storage/row/HoodieRowParquetWriteSupport.java`

Shredded Variant 的写入支持在 `HoodieRowParquetWriteSupport` 中实现。其核心思想是在 Parquet 写入时，将 Variant 类型的字段拆解为：

```
Variant 列 "v":
  ├── metadata (REQUIRED BINARY)   -- Variant 元数据（schema 信息）
  ├── value (OPTIONAL BINARY)      -- 未被 shred 的值（fallback）
  └── typed_value (GROUP)          -- 被 shred 出来的原生列
        ├── field_a (INT64)
        ├── field_b (BINARY/UTF8)
        └── ...
```

**设计思路：**
- `metadata` 始终存在，描述 Variant 的内部 schema
- 对于 Unshredded，`value` 是 REQUIRED，包含完整数据
- 对于 Shredded，`value` 是 OPTIONAL（部分数据被提取到 `typed_value` 中）
- `typed_value` 是一个 Parquet Group，其中每个字段对应 JSON 中一个被"shredded"出来的字段

### 测试验证

源码路径：`hudi-spark-datasource/hudi-spark4.0.x/src/test/java/org/apache/hudi/io/storage/row/TestHoodieRowParquetWriteSupportVariant.java`

测试类验证了三种场景：
1. **Unshredded Variant**：metadata + value（REQUIRED）
2. **Shredded Object Variant**：metadata + value（OPTIONAL）+ typed_value（GROUP with fields）
3. **Shredded Scalar Variant**：metadata + value（OPTIONAL）+ typed_value（原始类型如 INT64）

> **为什么需要 Shredded Variant？** 半结构化数据（如 JSON 日志）中往往有一些字段（如 timestamp、user_id）在几乎所有记录中都存在且被频繁查询。Shredded Variant 允许将这些"热点字段"提取为原生列式存储，享受列式压缩和谓词下推的性能优势，同时保留 Variant 的灵活性来存储其他不规则字段。

### Variant 与 Column Statistics 的关系

源码路径：`hudi-common/src/main/java/org/apache/hudi/metadata/HoodieTableMetadataUtil.java`

```java
// VARIANT (unshredded) type is excluded because it stores semi-structured data as opaque binary blobs,
// ...
// TODO: For shredded, we are able to store colstats, explore that: #17988
```

当前 Unshredded Variant 不支持列统计信息（Column Stats），因为二进制 blob 无法计算 min/max。但对于 Shredded Variant 的 `typed_value` 字段，未来可以支持列统计，这是待探索的方向（Issue #17988）。

## 5.2 VECTOR Search TVF — 向量搜索表值函数

### 背景与动机

随着 AI/ML 的兴起，向量搜索（Vector Search / Similarity Search）成为数据平台的重要能力。传统做法是将向量数据从数据湖导出到专门的向量数据库（如 Pinecone、Milvus），但这带来：
- 数据一致性问题（两个系统间的数据同步延迟）
- 额外的基础设施成本
- 运维复杂度增加

Hudi 的 VECTOR Search TVF（Table-Valued Function）让用户可以直接在数据湖上执行 KNN（K-Nearest Neighbors）搜索，无需额外的向量数据库。

### 架构设计

Hudi 的 Vector Search 通过 Spark SQL 的 TVF（Table-Valued Function）机制实现，涉及以下关键组件：

#### 1. TVF 定义与注册

源码路径：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/analysis/TableValuedFunctions.scala`

```scala
object TableValuedFunctions {
  val funcs = Seq(
    // ... 其他 TVF ...
    (
      FunctionIdentifier(HoodieVectorSearchTableValuedFunction.FUNC_NAME),  // "hudi_vector_search"
      new ExpressionInfo(...),
      (args: Seq[Expression]) => new HoodieVectorSearchTableValuedFunction(args)
    ),
    (
      FunctionIdentifier(HoodieVectorSearchBatchTableValuedFunction.FUNC_NAME),  // "hudi_vector_search_batch"
      new ExpressionInfo(...),
      (args: Seq[Expression]) => new HoodieVectorSearchBatchTableValuedFunction(args)
    )
  )
}
```

注册了两个 TVF：
- `hudi_vector_search`：单查询模式，一个查询向量 → 返回 top-K 最相似记录
- `hudi_vector_search_batch`：批量查询模式，多个查询向量 → 每个向量返回 top-K

#### 2. TVF 参数解析

源码路径：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/HoodieVectorSearchTableValuedFunction.scala`

```scala
object HoodieVectorSearchTableValuedFunction {
  val FUNC_NAME = "hudi_vector_search"

  // 距离度量类型
  object DistanceMetric extends Enumeration {
    val COSINE, L2, DOT_PRODUCT = Value
  }

  // 搜索算法
  object SearchAlgorithm extends Enumeration {
    val BRUTE_FORCE = Value  // 目前只支持暴力搜索
  }

  case class ParsedArgs(
    table: String,        // 表名
    embeddingCol: String, // 向量列名
    queryVectorExpr: Expression,  // 查询向量
    k: Int,               // 返回 top-K
    metric: DistanceMetric.Value,        // 距离度量
    algorithm: SearchAlgorithm.Value     // 搜索算法
  )
}
```

SQL 用法示例：

```sql
-- 单查询模式：找与 [1.0, 2.0, 3.0] 最相似的 10 条记录
SELECT * FROM hudi_vector_search(
  'my_table', 'embedding_col', ARRAY(1.0, 2.0, 3.0), 10
)

-- 指定距离度量和算法
SELECT * FROM hudi_vector_search(
  'my_table', 'embedding_col', ARRAY(1.0, 2.0, 3.0), 10, 'cosine', 'brute_force'
)

-- 批量查询模式：用 query_table 中每行的向量去 corpus_table 中搜索
SELECT * FROM hudi_vector_search_batch(
  'corpus_table', 'corpus_col', 'query_table', 'query_col', 5
)
```

#### 3. 搜索算法框架

源码路径：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/analysis/HoodieVectorSearchPlanBuilder.scala`

```scala
trait VectorSearchAlgorithm {
  def name: String

  def buildSingleQueryPlan(
      spark: SparkSession,
      corpusDf: DataFrame,
      embeddingCol: String,
      queryVector: Array[Double],
      k: Int,
      metric: DistanceMetric.Value): LogicalPlan

  def buildBatchQueryPlan(
      spark: SparkSession,
      corpusDf: DataFrame,
      corpusEmbeddingCol: String,
      queryDf: DataFrame,
      queryEmbeddingCol: String,
      k: Int,
      metric: DistanceMetric.Value): LogicalPlan
}
```

> **为什么设计成可插拔算法接口？** 目前只实现了 Brute Force（暴力搜索），但框架设计为可扩展的。未来可以添加 HNSW（Hierarchical Navigable Small World）、IVF（Inverted File Index）等近似最近邻算法。添加新算法只需三步：（1）创建 trait 实现；（2）在 SearchAlgorithm 枚举中添加值；（3）在 resolveAlgorithm 中注册。

#### 4. BruteForceSearchAlgorithm — 暴力搜索实现

```scala
object BruteForceSearchAlgorithm extends VectorSearchAlgorithm {
  override val name: String = "brute_force"

  override def buildSingleQueryPlan(
      spark: SparkSession, corpusDf: DataFrame,
      embeddingCol: String, queryVector: Array[Double],
      k: Int, metric: DistanceMetric.Value): LogicalPlan = {
    // 1. 验证 embedding 列类型
    validateEmbeddingColumn(corpusDf, embeddingCol)
    // 2. 验证向量维度匹配
    validateQueryVectorDimension(corpusDf, embeddingCol, queryVector.length)

    val elemType = getElementType(corpusDf, embeddingCol)
    val filteredDf = corpusDf.filter(col(embeddingCol).isNotNull)

    // 3. 创建距离计算 UDF
    val distanceUdf = VectorDistanceUtils.createSingleQueryDistanceUdf(
        metric, elemType, queryVector)

    // 4. 计算距离 → 排序 → 取 top-K
    val result = filteredDf
      .withColumn(DISTANCE_COL, distanceUdf(col(embeddingCol)))
      .drop(embeddingCol)
      .orderBy(col(DISTANCE_COL).asc)
      .limit(k)

    result.queryExecution.analyzed
  }
}
```

**单查询模式的执行计划：**
```
Scan(corpus_table)
  → Filter(embedding IS NOT NULL)
  → Project(*, _hudi_distance = UDF(embedding))
  → TakeOrderedAndProject(k, orderBy=_hudi_distance ASC)
```

Spark 会将 `orderBy + limit` 优化为 `TakeOrderedAndProject`，这是一种部分排序算法，复杂度为 O(N * log(k))，远优于全排序的 O(N * log(N))。

#### 5. VectorDistanceUtils — 距离计算

源码路径：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/analysis/VectorDistanceUtils.scala`

支持三种距离度量：

```scala
private def resolveDistanceFn(
    metric: DistanceMetric.Value): (DenseVector, DenseVector, Double) => Double =
  metric match {
    case DistanceMetric.COSINE => (a, b, bNorm) =>
      val aNorm = Vectors.norm(a, 2.0)
      val denom = aNorm * bNorm
      if (denom == 0.0) 1.0 else math.min(2.0, math.max(0.0, 1.0 - (a.dot(b) / denom)))
    case DistanceMetric.L2 => (a, b, _) =>
      math.sqrt(Vectors.sqdist(a, b))
    case DistanceMetric.DOT_PRODUCT => (a, b, _) =>
      -(a.dot(b))  // 取负数使得更大的内积对应更小的距离
  }
```

| 距离度量 | 公式 | 值域 | 适用场景 |
|---------|------|------|---------|
| Cosine | 1 - cos(a, b) | [0, 2] | 文本语义相似度 |
| L2 (Euclidean) | sqrt(sum((a-b)^2)) | [0, +inf) | 图像特征匹配 |
| Dot Product | -(a . b) | (-inf, +inf) | 推荐系统 |

**优化细节：** 单查询模式下，查询向量的 DenseVector 和 norm 被预计算并通过闭包捕获，避免每行重复计算。

#### 6. 批量查询模式

批量查询模式使用 Cross Join + Window Function 实现：

```scala
override def buildBatchQueryPlan(...): LogicalPlan = {
    // 1. Cross Join: corpus x broadcast(query)
    val scored = filteredCorpus.crossJoin(broadcast(renamedQuery))
      .withColumn(DISTANCE_COL,
        distanceUdf(col(corpusEmbeddingCol), col(QUERY_EMB_ALIAS)))

    // 2. Window Function: 每个 query 取 top-K
    val window = Window.partitionBy(QUERY_ID_COL).orderBy(col(DISTANCE_COL).asc)
    val result = scored
      .withColumn(RANK_COL, row_number().over(window))
      .filter(col(RANK_COL) <= k)
      .drop(RANK_COL)
      .orderBy(col(QUERY_ID_COL), col(DISTANCE_COL))

    result.queryExecution.analyzed
}
```

> **为什么使用 broadcast cross join？** 查询表通常很小（几十到几百个查询向量），广播到所有 executor 可以避免 shuffle。Cross join 产生 O(|corpus| * |queries|) 行中间数据，适合小到中等规模的查询集。

#### 7. 维度和类型验证

```scala
private[analysis] def validateEmbeddingColumn(df: DataFrame, colName: String): Unit = {
    field.dataType match {
      case ArrayType(FloatType, _) | ArrayType(DoubleType, _) | ArrayType(ByteType, _) => // valid
      case other => throw new HoodieAnalysisException(
        s"Embedding column '$colName' has type $other, " +
          "expected array<float>, array<double>, or array<byte>")
    }
}
```

支持三种 embedding 元素类型：
- `array<float>`：最常见，32 位浮点（如 OpenAI Embedding）
- `array<double>`：64 位浮点，高精度场景
- `array<byte>`：量化后的向量（如 int8 量化），最省空间

> **严格类型匹配的设计决策：** 代码注释中明确说明，Hudi 选择了严格类型匹配（corpus 和 query 必须使用相同的元素类型），而非自动类型提升。这是有意为之的设计，避免了 float→double 转换可能带来的精度差异。未来可能放宽为自动宽化（byte→float→double）。

---

# 第六部分：新特性路线图与 RFC

## 6.1 Hudi RFC 机制

### RFC 流程概述

Hudi 社区使用 RFC（Request for Comments）机制来管理重大设计变更。RFC 文档存放在仓库的 `rfc/` 目录下。

RFC 的生命周期状态：

| 状态 | 含义 |
|------|------|
| UNDER REVIEW | RFC 已提出，社区正在讨论 |
| IN PROGRESS | 初始实现阶段正在进行 |
| ONGOING | 部分工作已落地，持续改进中 |
| COMPLETED | 所有工作已完成 |
| ABANDONED | 由于各种原因未实施 |

### 关键 RFC 与本文相关

| RFC # | 标题 | 状态 | 与本文的关联 |
|-------|------|------|------------|
| RFC-48 | LogCompaction for MOR tables | COMPLETED | Log Compaction 的设计提案 |
| RFC-65 | Partition TTL Management | COMPLETED | Partition TTL 的设计提案 |
| RFC-66 | Non-Blocking Concurrency Control | COMPLETED | NBCC 的设计提案 |
| RFC-69 | Hudi 1.X | COMPLETED | Hudi 1.0 的整体架构愿景 |
| RFC-42 | Consistent Hashing Index | ONGOING | 动态桶索引，与 NBCC 相关 |
| RFC-77 | Secondary Index | COMPLETED | 二级索引支持 |
| RFC-80 | Column Groups | IN PROGRESS | 列族支持 |
| RFC-82 | Concurrent Schema Evolution | COMPLETED | 并发 Schema 变更检测 |
| RFC-83 | Incremental Table Service | COMPLETED | 增量表服务 |

### RFC-69: Hudi 1.X 的核心愿景

源码路径：`rfc/rfc-69/rfc-69.md`

RFC-69 由 Vinoth Chandar（Hudi 创始人）提出，描述了 Hudi 1.X 的整体架构演进方向：

1. **深度查询引擎集成**：充分利用 Hudi 的多模索引能力进行查询规划和执行
2. **通用化数据模型**：从 KV 存储模式向关系模型演进
3. **服务化架构**：混合架构——表元数据使用服务端组件，数据处理保持 Serverless
4. **超越结构化数据**：支持 JSON、图像、视频、ML/AI 格式等
5. **更强的自管理能力**：反向流、快照管理、诊断报告、跨区域复制等

RFC-69 还特别提到了并发控制的演进方向：

> "This RFC proposes Hudi should pursue a more general purpose non-blocking MVCC-based concurrency control"

这正是 RFC-66（NBCC）的理论基础。

## 6.2 RFC-48: Log Compaction 设计要点

RFC-48 的核心设计思想：

1. **新增 Timeline Action**: `logcompaction`，作为一个独立的表服务操作
2. **Schedule + Execute 两阶段**：与传统 Compaction 一致
3. **产出物是 Log File**：而非 Base File，这是与传统 Compaction 的本质区别
4. **完成后发布 deltacommit**：因为产出物是 Log File

关于 Log Block 的处理，RFC-48 描述了一个关键场景：

> "当 LogCompaction 执行后，产生 log.4，Reader 在扫描时会看到 4 个 log blocks，但会**只考虑 log block 4**（因为它包含了 block 1-3 的合并结果）。"

这意味着 Log Compaction 后，旧的 log blocks 虽然物理上仍存在，但在逻辑上被新的合并 block 所替代。

## 6.3 RFC-66: NBCC 设计要点

RFC-66 描述了 NBCC 的前置条件和基本工作流：

**前置条件：**
1. MOR Table Type（必须）
2. 确定性分桶策略（必须）
3. Lazy Cleaning Strategy（必须）

**基本工作流：**
- 每个 Writer 按顺序将 log files 写入各自的版本号序列
- 不同 Writer 写入同一个 File Group 的不同 log file
- Compaction 负责最终的冲突解决（合并同一 key 的多个版本）
- Reader 在读取时也可以进行冲突解决（Merge on Read）

**两个重要使用场景：**
1. **多源数据汇入**：多个 Flink/Spark 流从不同数据源写入同一张 Hudi 表
2. **实时数据 JOIN**：替代 Flink 的 State-Based JOIN，用 Hudi 表作为 JOIN 的物化视图

## 6.4 Hudi 1.x 版本新特性总结

### Hudi 1.0 核心新特性

Hudi 1.0 是一个里程碑式的版本，引入了大量架构级变更：

1. **Table Version 8**：新的表版本，支持更丰富的元数据和更高效的 Timeline
2. **新 Timeline 格式**：从 v1 演进到 v2，支持 completion time 排序
3. **Record Merge Mode**：替代旧的 Payload 机制，提供更灵活的记录合并策略
   - `COMMIT_TIME_ORDERING`：按提交时间排序
   - `EVENT_TIME_ORDERING`：按事件时间排序
   - `CUSTOM`：自定义合并逻辑
4. **Partition TTL**（RFC-65）：分区自动过期清理
5. **Non-Blocking Concurrency Control**（RFC-66）：高并发无冲突写入
6. **Log Compaction**（RFC-48）：轻量级日志压缩
7. **Secondary Index**（RFC-77）：二级索引支持
8. **Expression Index**（RFC-63）：表达式索引
9. **Auto Record Key Generation**（RFC-76）：自动主键生成
10. **Position-Based Merge**：基于位置的高效合并

### Hudi 1.1 新特性

1. **Incremental Table Service**（RFC-83）：增量表服务，优化 Compaction/Clustering 的增量执行
2. **Concurrent Schema Evolution Detection**（RFC-82）：并发 Schema 变更的检测和处理
3. **HoodieStorage 抽象**（RFC-74）：存储抽象层，支持更多存储后端
4. **优化的 SerDe**（RFC-84）：Flink DataStream 的优化序列化/反序列化

### Hudi 1.2 前沿探索

1. **Shredded Variant 写入支持**（PR #18036）：半结构化数据的高效存储
2. **VECTOR Search TVF**（PR #18432）：数据湖上的向量搜索能力
3. **Column Groups**（RFC-80）：列族支持，优化宽表场景
4. **Pluggable Table Formats**（RFC-93）：可插拔表格式
5. **HoodieRecordMerger API 更新**（RFC-101）：更灵活的记录合并 API

### 重要的 IN PROGRESS / UNDER REVIEW RFC

| RFC # | 标题 | 意义 |
|-------|------|------|
| RFC-80 | Column Groups | 支持列族存储，优化宽表和部分列更新 |
| RFC-87 | Avro elimination for Flink writer | Flink 写入路径去 Avro 依赖 |
| RFC-89 | Dynamic Partition Level Bucket Index | 动态分区级桶索引 |
| RFC-91 | Storage-based lock provider | 基于存储的锁提供者（消除外部锁依赖） |
| RFC-92 | Support Bitmap Index | 位图索引支持 |
| RFC-93 | Pluggable Table Formats | 可插拔表格式（Hudi/Iceberg/Delta 互通） |
| RFC-96 | Introduce Unified Bucket Index | 统一桶索引 |
| RFC-98 | Spark Datasource V2 Read | Spark V2 数据源读取集成 |
| RFC-99 | Hudi Type System Redesign | 类型系统重设计 |
| RFC-100 | Unstructured Data Storage | 非结构化数据存储（图像、视频等） |
| RFC-101 | Updates to HoodieRecordMerger API | 记录合并器 API 更新 |
| RFC-103 | Hudi LSM tree layout | LSM 树布局 |

---

## 附录 A：源码文件路径汇总

### Partition TTL 相关

```
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/ttl/strategy/TTLStrategy.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/ttl/strategy/PartitionTTLStrategy.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/ttl/strategy/KeepByTimeStrategy.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/ttl/strategy/KeepByCreationTimeStrategy.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/ttl/strategy/PartitionTTLStrategyType.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/ttl/strategy/HoodiePartitionTTLStrategyFactory.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieTTLConfig.java
hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/action/commit/SparkPartitionTTLActionExecutor.java
hudi-client/hudi-flink-client/src/main/java/org/apache/hudi/table/action/commit/FlinkPartitionTTLActionExecutor.java
hudi-utilities/src/main/java/org/apache/hudi/utilities/HoodieTTLJob.java
hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/command/procedures/RunTTLProcedure.scala
rfc/rfc-65/rfc-65.md
```

### Log Compaction 相关

```
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/plan/generators/HoodieLogCompactionPlanGenerator.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/plan/generators/BaseHoodieCompactionPlanGenerator.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/ScheduleCompactionActionExecutor.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/RunCompactionActionExecutor.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/HoodieCompactor.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieCompactionConfig.java
hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieTimeline.java
rfc/rfc-48/rfc-48.md
```

### NBCC 相关

```
hudi-common/src/main/java/org/apache/hudi/common/model/WriteConcurrencyMode.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieWriteConfig.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/ConflictResolutionStrategy.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/SimpleConcurrentFileWritesConflictResolutionStrategy.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/PreferWriterConflictResolutionStrategy.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/transaction/BucketIndexConcurrentFileWritesConflictResolutionStrategy.java
hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/bucket/BucketIdentifier.java
rfc/rfc-66/rfc-66.md
```

### Record Position Merge 相关

```
hudi-common/src/main/java/org/apache/hudi/common/table/read/buffer/PositionBasedFileGroupRecordBuffer.java
hudi-common/src/main/java/org/apache/hudi/common/table/read/buffer/KeyBasedFileGroupRecordBuffer.java
hudi-common/src/main/java/org/apache/hudi/common/table/read/HoodieFileGroupReader.java
```

### Variant / Vector Search 相关

```
hudi-common/src/main/java/org/apache/hudi/common/schema/HoodieSchema.java
hudi-common/src/main/java/org/apache/hudi/common/schema/HoodieSchemaType.java
hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/io/storage/row/HoodieRowParquetWriteSupport.java
hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/HoodieVectorSearchTableValuedFunction.scala
hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/analysis/HoodieVectorSearchPlanBuilder.scala
hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/analysis/VectorDistanceUtils.scala
hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/analysis/TableValuedFunctions.scala
```

### RFC 相关

```
rfc/README.md
rfc/rfc-48/rfc-48.md
rfc/rfc-65/rfc-65.md
rfc/rfc-66/rfc-66.md
rfc/rfc-69/rfc-69.md
rfc/rfc-77/rfc-77.md
rfc/rfc-80/rfc-80.md
rfc/rfc-82/rfc-82.md
rfc/rfc-83/rfc-83.md
```

---

## 附录 B：特性关联与协同工作

```
                    ┌─────────────────────────────────────┐
                    │         Hudi Table Service Layer     │
                    │                                     │
  ┌──────────┐      │  ┌─────────┐  ┌────────────────┐   │
  │ Partition │      │  │  Clean  │  │   Archival     │   │
  │   TTL     │──────┼─▶│         │  │                │   │
  │(删除分区) │      │  │(清理旧  │  │ (归档 timeline)│   │
  └──────────┘      │  │ 文件版本)│  │                │   │
                    │  └─────────┘  └────────────────┘   │
                    │                                     │
  ┌──────────┐      │  ┌─────────────────────────────┐   │
  │   Log    │      │  │    Traditional Compaction     │   │
  │Compaction│◀────▶│  │  (Base + Logs → New Base)     │   │
  │(轻量合并)│      │  └─────────────────────────────┘   │
  └──────────┘      │                                     │
                    └─────────────────────────────────────┘

                    ┌─────────────────────────────────────┐
                    │       Concurrency Control Layer      │
                    │                                     │
  ┌──────────┐      │  ┌────────────────────────────┐    │
  │  NBCC    │──────┼─▶│  Bucket Index (确定性路由)   │    │
  │(无冲突   │      │  ├────────────────────────────┤    │
  │ 多写者)  │      │  │  MOR (Log File 隔离)       │    │
  └──────────┘      │  ├────────────────────────────┤    │
                    │  │  PreferWriter Strategy      │    │
                    │  │  (写入优先于 Table Service)  │    │
                    │  └────────────────────────────┘    │
                    └─────────────────────────────────────┘

                    ┌─────────────────────────────────────┐
                    │          Read Optimization Layer     │
                    │                                     │
  ┌──────────┐      │  ┌────────────────────────────┐    │
  │ Position │──────┼─▶│  RoaringBitmap (位图索引)    │    │
  │ Based    │      │  ├────────────────────────────┤    │
  │ Merge    │      │  │  FileGroupReader           │    │
  │(高效合并)│      │  │  (顺序扫描 + 位置匹配)     │    │
  └──────────┘      │  └────────────────────────────┘    │
                    └─────────────────────────────────────┘

                    ┌─────────────────────────────────────┐
                    │       New Data Type / AI Layer       │
                    │                                     │
  ┌──────────┐      │  ┌────────────────────────────┐    │
  │ Shredded │──────┼─▶│  HoodieSchema Type System   │    │
  │ Variant  │      │  │  (Variant / Vector 类型)    │    │
  └──────────┘      │  └────────────────────────────┘    │
                    │                                     │
  ┌──────────┐      │  ┌────────────────────────────┐    │
  │ VECTOR   │──────┼─▶│  Spark TVF + UDF Framework  │    │
  │ Search   │      │  │  (KNN 搜索 + 距离计算)      │    │
  └──────────┘      │  └────────────────────────────┘    │
                    └─────────────────────────────────────┘
```

### 特性之间的协同关系

1. **NBCC + Log Compaction**：NBCC 会产生大量 Log File（多个 Writer 同时追加），Log Compaction 可以周期性合并这些 Log File，减少读取时的文件句柄数量
2. **NBCC + Position-Based Merge**：在 NBCC 场景下，多个 Writer 的 Log File 被合并时，Position-Based Merge 可以加速合并过程
3. **Partition TTL + Clean**：TTL 生成的 REPLACE_COMMIT 会触发后续的 Clean 来实际释放存储空间
4. **Log Compaction + Traditional Compaction**：二者互补——Log Compaction 短期优化读取性能，Traditional Compaction 长期消除所有 Log File
5. **Shredded Variant + Column Statistics**：Shredded 出来的列可以利用 Column Stats 进行查询加速
6. **Vector Search + Hudi 数据管理**：在 Hudi 表上直接做向量搜索，享受 Hudi 的事务保证、增量查询、数据管理等能力

---

## 附录 C：各特性的版本引入时间线

```
Hudi 0.13.0 (2023)
  └── Log Compaction 初始引入 (hoodie.log.compaction.*)

Hudi 0.14.0 (2023)
  └── NBCC 初始支持
  └── NUM_RETRIES_ON_CONFLICT_FAILURES 配置

Hudi 1.0.0 (2024)
  └── Partition TTL (RFC-65, 完整实现)
  └── Table Version 8
  └── Record Merge Mode
  └── Position-Based Merge
  └── Secondary Index (RFC-77)

Hudi 1.1.0 (2025)
  └── Incremental Table Service (RFC-83)
  └── Concurrent Schema Evolution (RFC-82)

Hudi 1.2.0-SNAPSHOT (2025, 当前开发版)
  └── Shredded Variant 写入支持 (#18036)
  └── VECTOR Search TVF (#18432)
  └── Column Groups 进行中 (RFC-80)
```

---

## 附录 D：面试高频问题

### Q1: Partition TTL 和 Hive 的 RETENTION 有什么区别？

Hive 的 RETENTION 是元数据层面的标记，不会自动删除数据。Hudi 的 Partition TTL 是真正的数据管理操作，会生成 REPLACE_COMMIT 来删除分区数据，并通过 Clean 释放存储。

### Q2: Log Compaction 后，旧的 Log File 怎么处理？

旧的 Log File 不会立即删除。它们在 Timeline 上被 Log Compaction 的 deltacommit "覆盖"。Reader 在扫描时会根据 instantTime 和 logcompaction 状态跳过旧的 log blocks。旧文件最终由 Clean 服务删除。

### Q3: NBCC 为什么不支持 COW 表？

COW 表每次写入创建新的 Base File 替换旧的。两个 Writer 同时写同一个 File Group 会产生两个新 Base File，文件系统无法处理这种冲突。MOR 表写入的是 Log File，不同 Writer 创建不同的 Log File，不会冲突。

### Q4: Position-Based Merge 什么时候会降级？

当 Log Block 中记录的 Base File InstantTime 与当前 File Group 的 Base File 不一致时（通常是因为 Compaction 重写了 Base File），Position 信息失效，会自动降级到 Key-Based Merge。

### Q5: Hudi 的 Vector Search 和专业向量数据库相比优劣？

**优势**：数据一致性好（事务保证）、无需额外基础设施、支持 SQL 查询、与数据管理能力（TTL/Clean/Compaction）集成。
**劣势**：目前只支持 Brute Force 算法（O(N) 复杂度），不支持 HNSW 等近似算法，大规模数据集性能不如专业向量数据库。适合中小规模数据集或不追求极致搜索性能的场景。

### Q6: 如何选择 KeepByTimeStrategy 还是 KeepByCreationTimeStrategy？

- 如果允许 Late-arriving data 延长分区寿命 → 用 KeepByTimeStrategy（按最后修改时间）
- 如果不管是否有更新，固定时间后必须过期 → 用 KeepByCreationTimeStrategy（按创建时间）
- 大多数场景推荐 KeepByTimeStrategy（默认值），它更安全

### Q7: NBCC 模式下还需要 Lock Provider 吗？

需要。虽然数据写入（Ingestion）之间不需要锁，但 Table Service（Compaction、Clustering）仍然需要锁来与数据写入协调。此外，Timeline 操作（创建 instant、提交 commit）也需要锁来保证原子性。
