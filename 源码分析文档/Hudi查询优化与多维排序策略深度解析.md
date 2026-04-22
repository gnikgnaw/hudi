# Hudi 查询优化与多维排序策略深度解析

> 基于 Apache Hudi 源码深度分析
> 文档版本：1.1
> 源码版本：v1.2.0-SNAPSHOT (master)
> 最后更新：2026-04-21

---

## 目录

1. [查询优化全景 — Hudi 如何让查询变快](#1-查询优化全景)
2. [Data Skipping 机制深度解析](#2-data-skipping-机制深度解析)
3. [Column Stats Index — 列统计索引](#3-column-stats-index)
4. [多维排序策略 — Linear / Z-Order / Hilbert](#4-多维排序策略)
5. [Expression Index — 表达式索引](#5-expression-index)
6. [Partition Stats Index — 分区级统计](#6-partition-stats-index)
7. [Secondary Index — 二级索引](#7-secondary-index)
8. [排序 + 索引的协同优化](#8-排序与索引的协同优化)
9. [与 Iceberg/Delta 查询优化对比](#9-与-iceberg-delta-查询优化对比)
10. [场景实战：从慢查询到秒级响应](#10-场景实战)
11. [源码深度剖析](#11-源码深度剖析)
    - 11.1 [SpaceCurveSortingHelper.orderDataFrameByMappingValues 完整解析](#111-spacecurvesortinghelperorderdataframebymappingvalues-完整解析)
    - 11.2 [DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr 完整解析](#112-dataskippingutilstranslateintocolumnstatsindexfilterexpr-完整解析)
    - 11.3 [ColumnStatsIndexSupport.computeCandidateFileNames 完整解析](#113-columnstatsindexsupportcomputecandidatefilenames-完整解析)
    - 11.4 [RangeSampleSort（采样排序）vs 直接映射排序](#114-rangesamplesort采样排序vs-直接映射排序)
    - 11.5 [HoodieFileIndex 与 Spark Catalyst 的集成](#115-hoodiefileindex-与-spark-catalyst-的集成)

---

## 1. 查询优化全景

### 1.1 为什么数据湖查询天然就慢？

传统数据库有 B-Tree/Hash 索引，查询时直接定位到目标行。数据湖是**一堆文件的集合**，查询的朴素方式是**全表扫描**：

```
SELECT * FROM hudi_table WHERE city = 'Beijing' AND amount > 1000

朴素执行:
  扫描全部 10000 个 Parquet 文件 → 读取 1TB 数据 → 过滤得到 10MB 结果
  实际需要的数据: 10MB / 1TB = 0.001% → 99.999% 的 I/O 是浪费
```

### 1.2 Hudi 的五层查询优化体系

```
Layer 1: 分区裁剪 (Partition Pruning)
    WHERE dt='2024-01-01' → 直接跳过其他分区目录
    效果: 通常可过滤 90%+ 的数据
    ↓
Layer 2: Data Skipping (文件级裁剪)
    利用 Column Stats (min/max) 跳过不相关文件
    效果: 排序良好时可再过滤 80%+ 的文件
    ↓
Layer 3: Bloom Filter 裁剪 (点查优化)
    利用 Bloom Filter 快速判断 Record Key 是否在文件中
    效果: 点查场景可精确到 1-2 个文件
    ↓
Layer 4: 谓词下推 (Predicate Pushdown)
    将过滤条件下推到 Parquet/ORC reader，跳过不匹配的 Row Group
    效果: 减少列和行的读取量
    ↓
Layer 5: 列裁剪 (Column Pruning)
    只读取查询需要的列
    效果: 列式存储的天然优势
```

**关键洞察**：Layer 1-3 是**文件级别**的过滤（决定读哪些文件），Layer 4-5 是**文件内部**的过滤。文件级过滤的收益远大于文件内部过滤——**跳过一个文件节省的 I/O 等于跳过该文件内所有行**。

### 1.3 排序为什么是查询优化的基础？

```
不排序的文件:
  file1: city=[Anhui, Beijing, Shanghai, Zhejiang]   min=Anhui, max=Zhejiang
  file2: city=[Beijing, Guangzhou, Shenzhen, Wuhan]   min=Beijing, max=Wuhan
  file3: city=[Beijing, Nanjing, Tianjin, Xian]       min=Beijing, max=Xian

  WHERE city='Beijing' → 3 个文件的 min/max 范围都包含 Beijing → 全部都要读

按 city 排序后:
  file1: city=[Anhui, Beijing, Beijing, Chongqing]    min=Anhui, max=Chongqing
  file2: city=[Fuzhou, Guangzhou, Nanjing, Shanghai]   min=Fuzhou, max=Shanghai
  file3: city=[Shenzhen, Tianjin, Wuhan, Xian]        min=Shenzhen, max=Xian

  WHERE city='Beijing' → 只有 file1 的范围包含 Beijing → 只读 1 个文件！
```

**排序让 min/max 范围变窄 → Data Skipping 效果指数级提升**。这就是为什么 Clustering + 排序是 Hudi 查询优化的核心手段。

---

## 2. Data Skipping 机制深度解析

### 2.1 核心原理

Data Skipping 的本质是**用元数据预判来避免读取不必要的文件**。

**源码位置**：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/DataSkippingUtils.scala`

```
查询: WHERE city = 'Beijing'

Data Skipping 翻译过程:
  原始过滤:  city = 'Beijing'
      ↓
  翻译为 Column Stats 过滤:
      city_minValue <= 'Beijing' AND city_maxValue >= 'Beijing'
      ↓
  对每个文件的 Column Stats 执行过滤:
      file1: minValue='Anhui', maxValue='Chongqing'
             'Anhui' <= 'Beijing' ✓ AND 'Chongqing' >= 'Beijing' ✓ → 候选 ✓
      file2: minValue='Fuzhou', maxValue='Shanghai'
             'Fuzhou' <= 'Beijing' ✗ → 排除 ✗ (不可能包含 Beijing)
```

### 2.2 支持的过滤表达式翻译

`DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr()` 将 Spark SQL 的过滤表达式翻译为 Column Stats 过滤：

| 原始查询 | 翻译后的 Column Stats 过滤 | 说明 |
|---------|--------------------------|------|
| `col = val` | `min <= val AND max >= val` | 等值过滤 |
| `col > val` | `max > val` | 大于过滤 |
| `col < val` | `min < val` | 小于过滤 |
| `col >= val` | `max >= val` | 大于等于 |
| `col <= val` | `min <= val` | 小于等于 |
| `col IS NULL` | `nullCount > 0` | NULL 值过滤 |
| `col IS NOT NULL` | `valueCount > 0` | 非 NULL 过滤 |
| `col IN (v1, v2)` | `min <= max(v1,v2) AND max >= min(v1,v2)` | IN 列表 |
| `col1=v1 AND col2=v2` | 两个条件分别翻译再 AND | 联合过滤 |
| `col1=v1 OR col2=v2` | 两个条件分别翻译再 OR | 联合过滤 |

### 2.3 Data Skipping 的生效条件

```
必须满足:
  1. hoodie.metadata.enable = true (Metadata Table 启用)
  2. hoodie.metadata.index.column.stats.enable = true (Column Stats 启用)
  3. hoodie.enable.data.skipping = true (Data Skipping 启用)
  4. 查询的 WHERE 条件中包含被索引的列
  5. 文件的 min/max 范围足够窄（排序后效果最佳）

Data Skipping 失效的场景:
  - WHERE 条件中的列未被 Column Stats 索引
  - 文件中数据分布太散（min/max 范围极宽）→ 所有文件都是候选
  - 使用了 Column Stats 不支持的表达式（如自定义 UDF）
  - Metadata Table 损坏或未初始化
```

---

## 3. Column Stats Index

### 3.1 存储结构

**源码位置**：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/ColumnStatsIndexSupport.scala`

注：`ColumnStatsIndexSupport` 是一个 class，定义为：
```scala
class ColumnStatsIndexSupport(spark: SparkSession,
                              tableSchema: StructType,
                              schema: HoodieSchema,
                              metadataConfig: HoodieMetadataConfig,
                              metaClient: HoodieTableMetaClient,
                              allowCaching: Boolean = false)
  extends SparkBaseIndexSupport(spark, metadataConfig, metaClient)
```

Column Stats Index 存储在 Metadata Table 的 `column_stats` 分区中：

```
每条 Column Stats 记录包含:
    Key: columnName + partitionPath + fileName
    Value:
        ├── minValue          # 列最小值（支持各种数据类型）
        ├── maxValue          # 列最大值
        ├── nullCount         # NULL 值数量
        ├── valueCount        # 非 NULL 值数量
        ├── totalSize         # 列总大小（字节）
        ├── totalUncompressedSize  # 未压缩大小
        └── isTightBound     # ★ 是否紧边界（Log Files 可能导致边界不紧）
```

**为什么有 isTightBound？** MOR 表中，Log Files 可能包含 Column Stats 范围之外的值。如果 `isTightBound = false`，Data Skipping 需要更保守（不能完全信任 min/max）。

### 3.2 Column Stats 的更新时机

```
主表每次 commit/deltacommit:
  → 同步更新 Metadata Table 的 column_stats 分区
  → 对新写入的文件计算 min/max/nullCount 等
  → 追加为 Metadata Table 的 log record

Compaction 后:
  → 新 Base File 的统计替换旧的
  → Column Stats 自动更新

Clustering 后:
  → 新文件的 Column Stats 替换旧文件的
  → ★ 因为 Clustering 排序了数据，新的 min/max 范围通常更窄
```

### 3.3 配置与调优

```properties
# 启用 Column Stats
hoodie.metadata.index.column.stats.enable=true

# ★ 指定要收集统计的列（性能关键！）
# 不要收集所有列 — 每个列都增加 Metadata Table 的写入和存储开销
# 只收集查询过滤条件中最常出现的列
hoodie.metadata.index.column.stats.column.list=city,ts,amount,status

# Column Stats 内存投影阈值（超过则溢写磁盘）
hoodie.metadata.index.column.stats.in.memory.projection.threshold=100000
```

**为什么不收集所有列？**
- 100 列的表 × 10000 个文件 = 100 万条 Column Stats 记录
- 每次写入都需要更新所有被索引列的统计 → 写入开销线性增长
- 大多数列在查询中很少作为过滤条件 → 白白浪费

---

## 4. 多维排序策略

### 4.1 LayoutOptimizationStrategy 枚举

**源码位置**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieClusteringConfig.java`（第 828-837 行）

```java
@EnumDescription("Determines ordering strategy for records layout optimization.")
public enum LayoutOptimizationStrategy {
    @EnumFieldDescription("Orders records lexicographically")
    LINEAR,
    
    @EnumFieldDescription("Orders records along Z-order spatial-curve.")
    ZORDER,
    
    @EnumFieldDescription("Orders records along Hilbert's spatial-curve.")
    HILBERT
}
```

### 4.2 LINEAR Sort — 线性排序

**原理**：按一列或多列的字典序排序，多列时按声明顺序优先级递减。

```
数据: (Beijing, 100), (Shanghai, 50), (Beijing, 200), (Shanghai, 150)

sort by city, amount:
  (Beijing, 100), (Beijing, 200), (Shanghai, 50), (Shanghai, 150)
  → city 是主排序键，amount 是次排序键
```

**文件 min/max 效果**：
```
sort by city:
  file1: city=[Anhui, Chongqing]       ← 范围窄，city 过滤高效
         amount=[10, 50000]             ← 范围宽，amount 过滤无效

sort by city, amount:
  file1: city=[Anhui, Chongqing]       ← 城市相关查询高效
         amount=[10, 50000]             ← 金额范围仍然宽（因为每个城市内金额范围大）
```

**适用场景**：查询几乎总是按同一列过滤（如按时间戳过滤的时序数据）

**局限**：多列查询时，只有第一列的 min/max 范围窄，后续列效果急剧下降。

### 4.3 Z-Order — Z 曲线排序

**核心问题**：LINEAR 排序只能优化一个维度。如果查询经常同时按 `city` 和 `amount` 过滤，怎么办？

**Z-Order 的思想**：将多个维度的值**交错编码**为一维值，使得多维空间中相近的点在一维编码后仍然相近。

```
二维 Z-Order 编码原理:

  city 二进制:   1 0 1 1
  amount 二进制: 0 1 1 0
                 ↓ ↓ ↓ ↓
  交错编码:      10 01 11 10 = 01011110 (Z值)

  相邻的 city + amount 组合 → 相近的 Z值 → 排序后在同一文件中
```

**文件 min/max 效果**：
```
Z-Order sort by (city, amount):
  file1: city=[Anhui, Fuzhou], amount=[0, 500]        ← 两个维度范围都窄！
  file2: city=[Guangzhou, Nanjing], amount=[100, 800]  ← 两个维度范围都窄！
  file3: city=[Shanghai, Zhejiang], amount=[200, 1000] ← 两个维度范围都窄！

  WHERE city='Beijing' AND amount > 500
  → 只有 file1 和 file2 可能匹配 → Data Skipping 高效
```

**源码实现**：

**源码位置**：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/sort/SpaceCurveSortingHelper.java`（第 84-139 行）

```java
public static Dataset<Row> orderDataFrameByMappingValues(
    Dataset<Row> df,
    HoodieClusteringConfig.LayoutOptimizationStrategy layoutOptStrategy,
    List<String> orderByCols,
    int targetPartitionCount) {
    
    // 验证排序列是否存在
    Map<String, StructField> columnsMap = Arrays.stream(df.schema().fields())
        .collect(Collectors.toMap(StructField::name, Function.identity()));
    
    // ★ 关键优化：如果只有一列，直接退化为 LINEAR 排序
    if (orderByCols.size() == 1) {
        String orderByColName = orderByCols.get(0);
        return df.repartitionByRange(targetPartitionCount, new Column(orderByColName));
    }

    // 多列场景：根据策略创建排序后的 RDD
    JavaRDD<Row> sortedRDD;
    switch (layoutOptStrategy) {
        case ZORDER:
            sortedRDD = createZCurveSortedRDD(df.toJavaRDD(), fieldMap, fieldNum, targetPartitionCount);
            break;
        case HILBERT:
            sortedRDD = createHilbertSortedRDD(df.toJavaRDD(), fieldMap, fieldNum, targetPartitionCount);
            break;
        default:
            throw new UnsupportedOperationException(...);
    }
    
    // 构建新 StructType 并创建 DataFrame
    StructType newStructType = composeOrderedRDDStructType(df.schema());
    return df.sparkSession().createDataFrame(sortedRDD, newStructType).drop("Index");
}
```

**Z-Order 的局限**：

```
Z-Order 的"跳跃"问题:

  二维空间中:
    (0,0) → (1,0) → (0,1) → (1,1) → (2,0) → (3,0) → (2,1) → (3,1)

    注意: (1,0) 到 (0,1) 是一个"跳跃"
    空间中相邻的 (1,0) 和 (1,1) 在 Z 曲线上不相邻

  后果: 某些相近的数据点可能被分配到不同文件 → min/max 范围不够紧
```

### 4.4 Hilbert 曲线排序 — 比 Z-Order 更好的选择

**为什么 Hilbert 比 Z-Order 更好？** Hilbert 曲线消除了 Z-Order 的"跳跃"问题。

```
Z-Order 路径 (2D):
  ┌─→─┐
  │   │
  └─←─┘ ┌─→─┐    ← 在这里有一个大跳跃
         │   │
         └─←─┘

Hilbert 路径 (2D):
  ┌─→─┐
  │   │
  ↓   ↓
  │   │
  └─→─┘              ← 连续，无跳跃
```

**源码实现**：使用 `org.davidmoten.hilbert.HilbertCurve` 库

**源码位置**：`hudi-common/src/main/java/org/apache/hudi/optimize/HilbertCurveUtils.java`

```java
public static byte[] indexBytes(HilbertCurve hilbertCurve, long[] points, int paddingNum) {
    BigInteger index = hilbertCurve.index(points);  // 计算 Hilbert 索引值
    return paddingToNByte(index.toByteArray(), paddingNum);
}
```

**Hilbert vs Z-Order 实际效果**：

| 维度 | Z-Order | Hilbert |
|------|---------|---------|
| 空间连续性 | 有跳跃 | 无跳跃 |
| min/max 紧密度 | 好 | **更好** |
| 计算成本 | 低（位交错） | 较高（递归映射） |
| Data Skipping 效果 | 好 | **更好 10-20%** |
| 适用维度数 | 2-8 | 2-8 |

### 4.5 三种排序策略对比与选择

| 排序策略 | 适用场景 | 不适用场景 |
|---------|---------|-----------|
| **LINEAR** | 几乎总是按同一列过滤（时序数据按 ts 查询） | 多列组合查询 |
| **Z-Order** | 2-4 列的组合查询，计算资源有限 | 单列查询（退化为 LINEAR 即可）|
| **Hilbert** | 2-4 列的组合查询，追求最佳效果 | 维度数 > 8 时效果递减 |

**选择决策**：
```
查询模式分析:
  总是按 1 列过滤 → LINEAR
  经常按 2-4 列组合过滤 → HILBERT (首选) 或 ZORDER
  过滤列数 > 4 或不确定 → HILBERT
  计算资源极其紧张 → ZORDER (比 Hilbert 计算更快)
```

### 4.6 排序配置

```properties
# 在 Clustering 中指定排序列
hoodie.clustering.plan.strategy.sort.columns=city,ts,amount

# 指定排序策略
hoodie.layout.optimize.strategy=HILBERT
# 可选值: LINEAR / ZORDER / HILBERT

# 在 Bulk Insert 中也可以使用空间曲线排序
hoodie.bulkinsert.sort.mode=GLOBAL_SORT
hoodie.layout.optimize.strategy=ZORDER
```

---

## 5. Expression Index

### 5.1 为什么需要 Expression Index？—— 解决派生查询问题

```
问题场景:
  表有字段 ts (TIMESTAMP), 查询经常按 date_format(ts, 'yyyy-MM') 过滤

  Column Stats 只收集了 ts 的 min/max:
    file1: ts min=2024-01-01 00:00:00, max=2024-01-31 23:59:59
    file2: ts min=2024-01-15 00:00:00, max=2024-02-15 23:59:59

  WHERE date_format(ts, 'yyyy-MM') = '2024-01'
  → Column Stats 无法直接过滤！因为索引的是 ts 而不是 date_format(ts)

Expression Index 的解决方案:
  索引 date_format(ts, 'yyyy-MM') 这个表达式的 min/max
  → file1: expr_min='2024-01', expr_max='2024-01' → 命中 ✓
  → file2: expr_min='2024-01', expr_max='2024-02' → 命中 ✓ (需要读)
```

### 5.2 核心设计

**源码位置**：
- 定义：`hudi-common/src/main/java/org/apache/hudi/index/expression/HoodieExpressionIndex.java`
- Spark 实现：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/ExpressionIndexSupport.scala`

```java
public interface HoodieExpressionIndex<S, T> extends Serializable {
    String getIndexName();            // 索引名称
    String getIndexFunction();        // 表达式函数名
    List<String> getOrderedSourceFields(); // 源字段列表
    T apply(List<S> orderedSourceValues);  // 执行表达式转换
}
```

### 5.3 支持的表达式

从 `ExpressionIndexSupport.scala` 的导入可以看到支持的 Spark 表达式：

| 表达式类型 | 示例 | 索引什么 |
|-----------|------|---------|
| `DateFormatClass` | `date_format(ts, 'yyyy-MM')` | 格式化后的日期字符串 |
| `FromUnixTime` | `from_unixtime(epoch_ts)` | 时间戳转字符串 |
| `Substring` | `substring(name, 1, 3)` | 子串 |
| `StringTrim` | `trim(city)` | 去空格后的字符串 |
| `ParseToDate` | `to_date(str_ts)` | 解析后的日期 |
| `DateAdd/DateSub` | `date_add(dt, 7)` | 日期偏移 |
| `RegExpExtract` | `regexp_extract(url, pattern)` | 正则提取 |
| `UnixTimestamp` | `unix_timestamp(ts)` | 时间戳转数值 |
| `identity` | 原始值 | Bloom Filter 场景 |

### 5.4 Expression Index 的两种索引类型

Expression Index 存储在 Metadata Table 中，支持两种底层类型：

```
类型 1: Column Stats 类型 (min/max/null_count)
  → 适合范围查询: WHERE date_format(ts) >= '2024-01'
  → 存储每个文件中该表达式值的 min/max

类型 2: Bloom Filter 类型
  → 适合等值查询: WHERE date_format(ts) = '2024-01'
  → 存储每个文件中该表达式值的 Bloom Filter
```

### 5.5 创建 Expression Index

```sql
-- 创建基于 date_format 的 Expression Index
CREATE INDEX expr_idx_month ON hudi_table
USING column_stats (ts)
OPTIONS (expr='date_format', format='yyyy-MM');

-- 创建基于 Bloom Filter 的 Expression Index
CREATE INDEX expr_idx_city_trim ON hudi_table
USING bloom_filters (city)
OPTIONS (expr='trim');
```

---

## 6. Partition Stats Index

### 6.1 设计动机

Column Stats 是**文件级别**的统计，Partition Stats 是**分区级别**的统计。

```
Column Stats: 每个文件的 min/max
  → 10000 文件 → 10000 条统计记录
  → 加载和查询有一定开销

Partition Stats: 每个分区的 min/max (所有文件的聚合)
  → 1000 分区 → 1000 条统计记录
  → 更轻量，适合粗粒度过滤

使用场景:
  先用 Partition Stats 粗过滤分区 → 再用 Column Stats 细过滤文件
  → 两级过滤，效率更高
```

### 6.2 存储位置

```
Metadata Table 的 partition_stats 分区:
  Key: partitionPath + columnName
  Value: 该分区所有文件的聚合 min/max/nullCount
```

---

## 7. Secondary Index

### 7.1 设计动机 —— 解决非 Record Key 列的点查

```
Record Level Index: recordKey → fileId (只能按 Record Key 查找)
Secondary Index:    arbitraryColumn → recordKey → fileId (任意列查找)

场景: 表的 Record Key 是 order_id，但经常按 user_id 查询
  → Record Level Index 只能定位 order_id
  → Secondary Index 可以定位 user_id → order_id → fileId
```

### 7.2 二级索引的存储

```
Metadata Table 的 secondary_index_xxx 分区:
  Key: 被索引列的值 + recordKey
  Value: 是否删除标记 (isDeleted)

查询流程:
  1. 在 Secondary Index 中查找 user_id='user_001' → 得到所有匹配的 recordKey
  2. 在 Record Index 中查找 recordKey → 得到 fileId
  3. 直接读取目标文件
```

### 7.3 配置

```sql
-- 创建二级索引
CREATE INDEX idx_user_id ON hudi_table (user_id);
```

---

## 8. 排序与索引的协同优化

### 8.1 协同效果矩阵

```
排序 + Column Stats = Data Skipping 效果最大化

排序前:
  10000 个文件，Column Stats min/max 范围极宽
  → Data Skipping 只能跳过 10% 的文件

排序后 (HILBERT by city, amount):
  10000 个文件，Column Stats min/max 范围极窄
  → Data Skipping 可以跳过 95% 的文件

排序 + Column Stats + Expression Index:
  常规查询用 Column Stats
  派生查询用 Expression Index
  → 覆盖更多查询模式
```

### 8.2 最佳实践组合

| 查询模式 | 推荐组合 |
|---------|---------|
| 单列范围查询 | LINEAR Sort + Column Stats |
| 多列组合查询 | HILBERT Sort + Column Stats |
| 派生字段查询 | Expression Index |
| Record Key 点查 | Record Level Index |
| 非 Key 列点查 | Secondary Index |
| 分区过滤 + 文件过滤 | Partition Stats + Column Stats |

### 8.3 配置模板：高性能分析查询

```properties
# 1. 启用 Metadata Table
hoodie.metadata.enable=true

# 2. 启用 Column Stats（只选查询热点列）
hoodie.metadata.index.column.stats.enable=true
hoodie.metadata.index.column.stats.column.list=city,ts,amount

# 3. 启用 Data Skipping
hoodie.enable.data.skipping=true

# 4. Clustering 排序（定期执行）
hoodie.clustering.plan.strategy.sort.columns=city,ts,amount
hoodie.layout.optimize.strategy=HILBERT

# 5. Clustering 目标文件大小
hoodie.clustering.plan.strategy.target.file.max.bytes=134217728  # 128MB
```

---

## 9. 与 Iceberg/Delta 查询优化对比

| 维度 | Hudi | Iceberg | Delta Lake |
|------|------|---------|------------|
| **文件级统计** | Column Stats Index (Metadata Table) | Manifest 内嵌统计 | Transaction Log 统计 |
| **Data Skipping** | 支持（需手动启用 Column Stats） | 默认支持 | 默认支持 |
| **排序策略** | LINEAR / Z-Order / Hilbert (Clustering) | Z-Order / 排序 (RewriteDataFiles) | Z-Order (OPTIMIZE) |
| **表达式索引** | ★ Expression Index (独有) | 无 | Generated Columns (间接) |
| **二级索引** | ★ Secondary Index (独有) | 无 | 无 |
| **Record 级索引** | ★ Record Level Index (独有) | 无 | 无 |
| **Bloom Filter** | 文件 footer + Metadata Table | 无内置 | 无内置 |
| **分区统计** | Partition Stats Index | Partition Summary | Partition Stats |

**Hudi 的优势**：
- Expression Index 和 Secondary Index 是 Hudi 独有的能力
- Record Level Index 支持 O(1) 点查
- 多种索引类型可组合使用

**Iceberg/Delta 的优势**：
- Column Stats 默认内嵌在 Manifest/Log 中，无需额外配置
- 配置更简单（开箱即用）

---

## 10. 场景实战

### 场景 1: 电商订单查询优化

```
表结构: order_id (PK), user_id, city, amount, ts, status
查询模式:
  - WHERE city = 'Beijing' AND ts > '2024-01-01'     (80%)
  - WHERE user_id = 'user_xxx'                        (15%)
  - WHERE order_id = 'order_xxx'                      (5%)

优化方案:
  1. 分区: 按 dt (日期) 分区
  2. 排序: HILBERT(city, ts) — 因为 80% 查询按 city+ts
  3. Column Stats: city, ts, amount
  4. Record Level Index: 优化 order_id 点查
  5. Secondary Index on user_id: 优化 user_id 查询

预期效果:
  - city+ts 查询: 从全表扫描 → 读取 < 5% 的文件
  - user_id 查询: 从全表扫描 → 精确定位到几个文件
  - order_id 查询: O(1) 定位
```

### 场景 2: 日志分析查询优化

```
表结构: log_id (PK), service_name, level, message, ts
查询模式:
  - WHERE ts BETWEEN '2024-01-01' AND '2024-01-02' AND level = 'ERROR'
  - WHERE service_name = 'payment' AND ts > '2024-01-01'
  - WHERE date_format(ts, 'yyyy-MM-dd HH') = '2024-01-01 14'

优化方案:
  1. 分区: 按 dt (日期) 分区
  2. 排序: LINEAR(ts) — 因为几乎所有查询都含 ts
  3. Column Stats: ts, level, service_name
  4. Expression Index: date_format(ts, 'yyyy-MM-dd HH')
  5. 不需要 Record Level Index（很少点查 log_id）

预期效果:
  - ts 范围查询: 分区裁剪 + 文件级 Data Skipping
  - date_format 查询: Expression Index 直接匹配
```

### 场景 3: 不排序反而更好的情况

```
表结构: sensor_id, temperature, ts
写入模式: 每秒 100 万条，流式写入
查询模式: 几乎不查询，只做下游 ETL 消费

这种情况:
  ✗ 不要排序 — 排序需要 shuffle，极大增加写入延迟
  ✗ 不要启用 Column Stats — 增加写入开销，但没人查
  ✓ 使用 MOR + Bucket Index — 最低写入延迟
  ✓ Compaction 频率设低 — 减少后台负载
```

---

## 11. 源码深度剖析

### 11.1 SpaceCurveSortingHelper.orderDataFrameByMappingValues 完整解析

**源码位置**：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/sort/SpaceCurveSortingHelper.java`

这个方法是 Hudi 多维排序的核心入口，负责将 DataFrame 按照指定的空间曲线策略（Z-Order 或 Hilbert）进行排序。理解它的完整逻辑，就理解了 Hudi 多维排序的全貌。

#### 11.1.1 方法总体流程

```
orderDataFrameByMappingValues(df, strategy, orderByCols, targetPartitionCount)
    │
    ├── Step 1: 验证排序列是否存在于 Schema 中
    │   如果列不存在，打印错误日志并原样返回 df
    │
    ├── Step 2: 单列优化 — 直接退化为 repartitionByRange
    │   if (orderByCols.size() == 1)
    │       return df.repartitionByRange(targetPartitionCount, col)
    │   ★ 为什么这么设计：单列时空间曲线与线性排序完全等价（1维曲线就是直线），
    │     直接使用 Spark 原生的 repartitionByRange 避免了 map + sortBy 的开销
    │
    ├── Step 3: 构建 fieldMap — 映射排序列索引到 StructField
    │   Map<Integer, StructField>，key 是列在 schema 中的位置索引
    │
    ├── Step 4: 根据策略创建排序后的 RDD
    │   ├── ZORDER  → createZCurveSortedRDD()
    │   └── HILBERT → createHilbertSortedRDD()
    │
    └── Step 5: 构建新 StructType（追加 "Index" 列），创建 DataFrame 后 drop("Index")
```

#### 11.1.2 类型映射 — 各数据类型如何转为 long / byte[]

Z-Order 和 Hilbert 曲线的计算需要统一的数值表示。Hudi 提供了两套映射方法：

**Z-Order 使用 `mapColumnValueTo8Bytes`** — 将任意类型映射为 8 字节的 `byte[]`：

```java
// 源码位置: SpaceCurveSortingHelper.java, 第 206-235 行
@Nonnull
private static byte[] mapColumnValueTo8Bytes(Row row, int index, DataType dataType) {
    if (dataType instanceof LongType) {
        return BinaryUtil.longTo8Byte(row.isNullAt(index) ? Long.MAX_VALUE : row.getLong(index));
    } else if (dataType instanceof DoubleType) {
        return BinaryUtil.doubleTo8Byte(row.isNullAt(index) ? Double.MAX_VALUE : row.getDouble(index));
    } else if (dataType instanceof IntegerType) {
        return BinaryUtil.intTo8Byte(row.isNullAt(index) ? Integer.MAX_VALUE : row.getInt(index));
    } else if (dataType instanceof StringType) {
        return BinaryUtil.utf8To8Byte(row.isNullAt(index) ? "" : row.getString(index));
    }
    // ... 支持 12 种数据类型（Long, Double, Int, Float, String, Date, Timestamp, Byte, Short, Decimal, Boolean, Binary）
}
```

**Hilbert 使用 `mapColumnValueToLong`** — 将任意类型映射为 `long`：

```java
// 源码位置: SpaceCurveSortingHelper.java, 第 237-266 行
private static long mapColumnValueToLong(Row row, int index, DataType dataType) {
    if (dataType instanceof LongType) {
        return row.isNullAt(index) ? Long.MAX_VALUE : row.getLong(index);
    } else if (dataType instanceof DoubleType) {
        return row.isNullAt(index) ? Long.MAX_VALUE : Double.doubleToLongBits(row.getDouble(index));
    } else if (dataType instanceof StringType) {
        return row.isNullAt(index) ? Long.MAX_VALUE : BinaryUtil.convertStringToLong(row.getString(index));
    }
    // ... 同样支持 12 种数据类型
}
```

**NULL 值的统一处理策略**：所有类型的 NULL 值都映射到 `Long.MAX_VALUE`（或对应类型最大值），这确保 NULL 值在排序时被推到最后，不会干扰正常数据的空间局部性。

**关键类型映射表**：

| 数据类型 | to 8 bytes (Z-Order) | to long (Hilbert) | 设计考量 |
|---------|---------------------|-------------------|---------|
| Long | `longTo8Byte`: XOR 符号位 | 直接返回 | XOR 符号位使负数在字节序中正确排列 |
| Double | `doubleTo8Byte`: 正数翻转符号位，负数全部取反 | `Double.doubleToLongBits` | 保证 IEEE 754 浮点数的字节序与数值序一致 |
| Int | `intTo8Byte`: XOR 符号位 + 补零到 8 字节 | 直接强转 long | 补零保证所有类型长度统一 |
| String | `utf8To8Byte`: 取 UTF-8 前 8 字节 | 前 8 字节转 long | 字符串截断是有损映射，但保留了前缀排序性 |
| Date/Timestamp | 取 `getTime()` 毫秒值后同 Long 处理 | 取 `getTime()` 毫秒值 | 时间类型天然是有序数值 |
| Boolean | true=1, false=0 后同 Int 处理 | true=MAX_VALUE, false=0 | 二值类型不适合空间曲线排序，但为了兼容性支持 |
| Decimal | `longValue()` 取整后同 Long 处理 | `longValue()` 取整 | 有精度损失，大 Decimal 可能溢出 |

**为什么 `longTo8Byte` 要 XOR 符号位？**

```java
// BinaryUtil.java, 第 169-172 行
public static byte[] longTo8Byte(long a) {
    long temp = a;
    temp = temp ^ (1L << 63);  // ★ 翻转最高位（符号位）
    return toBytes(temp);
}
```

Java 的 long 是补码表示，负数的最高位是 1。在字节序比较中，`-1` 的字节表示 `0xFF...FF` 会大于 `0` 的 `0x00...00`。XOR 符号位后，负数最高位变 0，正数最高位变 1，使得字节序比较与数值大小一致。这对 Z-Order 的位交错至关重要——只有各维度的字节表示能正确反映大小关系，交错后的排序才有意义。

#### 11.1.3 Z-Order 编码 — 位交错实现

```java
// SpaceCurveSortingHelper.java, 第 150-166 行
private static JavaRDD<Row> createZCurveSortedRDD(JavaRDD<Row> originRDD, Map<Integer, StructField> fieldMap, 
                                                   int fieldNum, int fileNum) {
    return originRDD.map(row -> {
        byte[][] zBytes = fieldMap.entrySet().stream()
            .map(entry -> {
                int index = entry.getKey();
                StructField field = entry.getValue();
                return mapColumnValueTo8Bytes(row, index, field.dataType());
            })
            .toArray(byte[][]::new);
        
        // ★ 核心：位交错生成 Z-Order 编码
        byte[] zOrdinalBytes = BinaryUtil.interleaving(zBytes, 8);
        return appendToRow(row, zOrdinalBytes);
    }).sortBy(f -> new ByteArraySorting((byte[]) f.get(fieldNum)), true, fileNum);
}
```

`BinaryUtil.interleaving` 的位交错算法（`hudi-common/src/main/java/org/apache/hudi/common/util/BinaryUtil.java` 第 91-108 行）：

```
假设 2 列，每列 8 字节 = 64 位
输入: col1 = [b1_0, b1_1, ..., b1_63], col2 = [b2_0, b2_1, ..., b2_63]
输出: [b1_0, b2_0, b1_1, b2_1, ..., b1_63, b2_63] → 共 128 位 = 16 字节

遍历顺序:
  外层循环: bitStep = 0..63 (遍历每一位)
  内层循环: i = 0..candidateSize-1 (遍历每一列)
  
  对于 bitStep=0: 取 col1 第 0 位 → result 第 0 位; 取 col2 第 0 位 → result 第 1 位
  对于 bitStep=1: 取 col1 第 1 位 → result 第 2 位; 取 col2 第 1 位 → result 第 3 位
  ...
```

**为什么位交错能实现多维空间局部性？** 位交错将每个维度的"贡献"均匀散布在最终编码中。最高有效位（MSB）来自所有维度的最高位，这意味着在 Z 值排序后，相邻的 Z 值对应的多维坐标在每个维度上都是接近的——高位决定了大致范围，低位决定了精确位置。

输出的 `byte[]` 长度 = 列数 x 8 字节。对于 3 列排序，Z 值为 24 字节；4 列排序，Z 值为 32 字节。最后通过 `ByteArraySorting`（字节字典序比较）排序整个 RDD。

#### 11.1.4 Hilbert 曲线调用方式

```java
// SpaceCurveSortingHelper.java, 第 168-198 行
private static JavaRDD<Row> createHilbertSortedRDD(JavaRDD<Row> originRDD, Map<Integer, StructField> fieldMap,
                                                    int fieldNum, int fileNum) {
    // ★ 使用 mapPartitions 而非 map，每个分区只创建一个 HilbertCurve 实例
    return originRDD.mapPartitions(rows -> {
        HilbertCurve hilbertCurve = HilbertCurve.bits(63).dimensions(fieldMap.size());
        return new Iterator<Row>() {
            @Override
            public boolean hasNext() {
                return rows.hasNext();
            }
            
            @Override
            public Row next() {
                Row row = rows.next();
                long[] longs = fieldMap.entrySet().stream()
                    .mapToLong(entry -> {
                        int index = entry.getKey();
                        StructField field = entry.getValue();
                        return mapColumnValueToLong(row, index, field.dataType());
                    })
                    .toArray();
                
                // 计算 N 维坐标在 Hilbert 曲线上的位置
                byte[] hilbertCurvePosBytes = HilbertCurveUtils.indexBytes(hilbertCurve, longs, 63);
                return appendToRow(row, hilbertCurvePosBytes);
            }
        };
    }).sortBy(f -> new ByteArraySorting((byte[]) f.get(fieldNum)), true, fileNum);
}
```

**关键设计决策**：

1. **`bits(63)`**：Hilbert 曲线的精度为 63 位，意味着每个维度的取值范围为 [0, 2^63)，与 long 的正数范围对应。精度越高，空间划分越细，但编码结果的字节数也越大（63 字节的 padding）。

2. **`mapPartitions` 而非 `map`**：使用 `mapPartitions` 确保 `HilbertCurve` 实例只在每个 Spark 分区初始化一次。`HilbertCurve.bits(63).dimensions(N)` 的构造涉及预计算查找表，代价不可忽视。

3. **`HilbertCurveUtils.indexBytes`** 调用了 `org.davidmoten.hilbert` 库：

```java
// HilbertCurveUtils.java, 第 30-33 行
public static byte[] indexBytes(HilbertCurve hilbertCurve, long[] points, int paddingNum) {
    BigInteger index = hilbertCurve.index(points);  // ★ 计算 N 维坐标在 Hilbert 曲线上的位置
    return paddingToNByte(index.toByteArray(), paddingNum);  // 填充到 63 字节
}
```

`hilbertCurve.index(points)` 返回一个 `BigInteger`，表示 N 维坐标在 Hilbert 曲线上的一维位置。`paddingToNByte` 确保所有结果长度统一为 63 字节，使得字节序比较有意义。

**为什么 Hilbert 用 `long` 而 Z-Order 用 `byte[]`？** Z-Order 的位交错可以直接在字节级别操作，不需要中间数值表示；而 Hilbert 曲线的数学计算需要数值坐标作为输入，因此使用 `long`。这也是 Hilbert 比 Z-Order 计算开销更大的原因之一——需要一次额外的数值映射步骤和更复杂的曲线位置计算。

---

### 11.2 DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr 完整解析

**源码位置**：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/DataSkippingUtils.scala`

这个方法是 Data Skipping 的"翻译引擎"，将用户的 SQL WHERE 条件翻译为对 Column Stats Index 的过滤条件。它的设计体现了 Hudi 在正确性和性能之间的精妙平衡。

#### 11.2.1 总体架构

```scala
// 第 51-60 行
def translateIntoColumnStatsIndexFilterExpr(
    dataTableFilterExpr: Expression,
    isExpressionIndex: Boolean = false,
    indexedCols: Seq[String] = Seq.empty,
    hasNonIndexedCols: AtomicBoolean = new AtomicBoolean(false)
): Expression = {
    try {
        createColumnStatsIndexFilterExprInternal(dataTableFilterExpr, ...)
    } catch {
        case e: AnalysisException =>
            throw e  // 分析异常上抛，不吞掉
    }
}
```

核心逻辑在 `tryComposeIndexFilterExpr` 方法中，使用 Scala 的模式匹配对 Spark Catalyst 表达式树进行递归翻译。当某个表达式无法翻译时返回 `None`，外层将其转为 `TrueLiteral`（即不过滤），确保永远不会错误地跳过包含目标数据的文件。

**为什么无法翻译时返回 `TrueLiteral` 而不是 `FalseLiteral`？** Data Skipping 的核心约束是**保守性**——宁可多读（false positive），绝不能少读（false negative）。如果一个表达式无法准确翻译，返回 `TrueLiteral` 意味着"所有文件都可能匹配"，等于放弃了这一层的优化，但保证了结果的正确性。

#### 11.2.2 AllowedTransformationExpression 守卫

在翻译每种操作之前，源码使用了 `AllowedTransformationExpression` 提取器来验证表达式是否安全：

```scala
// 第 535-551 行
object AllowedTransformationExpression {
    def unapply(expr: Expression): Option[AttributeReference] = {
        // 检查 1: 不能包含子查询
        // 检查 2: 必须恰好引用 1 个属性（列）
        if (SubqueryExpression.hasSubquery(expr) || expr.references.size != 1) {
            None
        } else {
            // 检查 3: 必须是保序变换（如 Cast、数学运算等）
            exprUtils.tryMatchAttributeOrderingPreservingTransformation(expr)
        }
    }
}
```

**保序变换（ordering-preserving transformation）** 是关键概念：如果变换 `T` 满足 `a1 <= a2 ==> T(a1) <= T(a2)`，则 `min(T(colA)) = T(min(colA))`，我们可以安全地将 `T` 应用到 Column Stats 的 min/max 上。例如 `CAST(col AS BIGINT)` 是保序的，但 `ABS(col)` 不是（因为 `-2 < 1` 但 `ABS(-2) > ABS(1)`）。

这就是 `swapAttributeRefInExpr` 方法的作用——将原表达式中引用的列属性替换为 Column Stats 的 min/max 列：

```scala
// 第 500-505 行
def swapAttributeRefInExpr(sourceExpr: Expression, from: AttributeReference, to: Expression): Expression = {
    checkState(sourceExpr.references.size == 1)
    sourceExpr.transformDown {
        case attrRef: AttributeReference if attrRef.sameRef(from) => to
    }
}
```

#### 11.2.3 NOT 表达式的处理

NOT 表达式的处理尤其精妙，因为 Data Skipping 的"不等于"语义与直觉不同：

```scala
// NOT(colA = B) 的翻译 (第 145-161 行)
case Not(EqualTo(sourceExpr @ AllowedTransformationExpression(attrRef), value)) =>
    Not(genColumnOnlyValuesEqualToExpression(colName, value, targetExprBuilder))
    // 翻译为: NOT(min = B AND max = B)
    // 含义: 排除那些"列中只包含值 B"的文件
    // ★ 注意: 这不是 "colA = B" 翻译的简单取反！
```

**为什么 `NOT(colA = B)` 不等于 `NOT(min <= B AND max >= B)`？**

假设文件包含值 [1, 2, 3]，查询 `WHERE colA != 2`：
- 如果翻译为 `NOT(min <= 2 AND max >= 2)` = `NOT(1 <= 2 AND 3 >= 2)` = `NOT(true)` = `false` → 文件被排除！但实际上文件中的 1 和 3 都满足 `colA != 2`，排除这个文件是错误的。
- 正确翻译是 `NOT(min = 2 AND max = 2)` = `NOT(1 = 2 AND 3 = 2)` = `NOT(false)` = `true` → 文件被保留。

只有当文件中**所有值都等于 B**（即 min = max = B）时，才能确定文件中没有满足 `!= B` 的行。

#### 11.2.4 LIKE (StartsWith) 表达式处理

```scala
// 第 322-334 行
case StartsWith(sourceExpr @ AllowedTransformationExpression(attrRef), v @ Literal(_: UTF8String, _)) =>
    genColumnValuesEqualToExpression(colName, v, targetExprBuilder)
    // 翻译为: min <= 'xxx' AND max >= 'xxx'
```

Hudi 只处理了 `LIKE 'prefix%'` 的形式（由 Spark Catalyst 自动优化为 `StartsWith`）。对于通用的 `LIKE '%middle%'` 模式，无法进行有效的 min/max 裁剪，因此不予翻译（回退到 `TrueLiteral`）。

前缀匹配之所以可以翻译，是因为字符串字典序保证了：如果文件的 `min <= prefix <= max`，则文件中可能存在以 `prefix` 开头的字符串。

**NOT LIKE 的处理同样精巧**：

```scala
// 第 338-347 行
case Not(StartsWith(sourceExpr, value)) =>
    Not(And(StartsWith(minValueExpr, value), StartsWith(maxValueExpr, value)))
    // 翻译为: NOT(min LIKE 'xxx%' AND max LIKE 'xxx%')
    // 含义: 排除那些 min 和 max 都以 prefix 开头的文件（即文件中所有值都匹配）
```

#### 11.2.5 AND/OR 复合表达式的递归处理

```scala
// OR 处理 (第 349-356 行)
case or: Or =>
    val resLeft = createColumnStatsIndexFilterExprInternal(or.left, ...)
    val resRight = createColumnStatsIndexFilterExprInternal(or.right, ...)
    // ★ 关键: 如果任意一侧是 TrueLiteral，则整个 OR 变为 None
    // 因为: TRUE OR x = TRUE, 无法裁剪任何文件
    if (resLeft.equals(LITERAL_TRUE_EXPR) || resRight.equals(LITERAL_TRUE_EXPR)) {
        None  // 回退到 TrueLiteral
    } else {
        Option(Or(resLeft, resRight))
    }

// AND 处理 (第 358-374 行)
case and: And =>
    val resLeft = ...
    val resRight = ...
    // ★ AND 更宽容: 只要一侧可翻译就有价值
    // TRUE AND x = x, 仍然可以利用 x 侧的条件
    if (isLeftLiteralTrue && isRightLiteralTrue) None
    else if (isLeftLiteralTrue) Option(resRight)  // 保留可翻译的右侧
    else if (isRightLiteralTrue) Option(resLeft)  // 保留可翻译的左侧
    else Option(And(resLeft, resRight))
```

**为什么 OR 和 AND 的策略不同？** 这源于布尔逻辑的性质。AND 是收紧条件——即使一侧无法翻译，另一侧仍能缩小范围。OR 是放宽条件——任何一侧无法翻译就意味着"可能匹配所有文件"，整体优化失效。

#### 11.2.6 NOT 的德摩根展开

```scala
// 第 381-387 行
case Not(And(left, right)) =>
    // NOT(A AND B) = NOT(A) OR NOT(B)  → 德摩根定律
    createColumnStatsIndexFilterExprInternal(Or(Not(left), Not(right)), ...)

case Not(Or(left, right)) =>
    // NOT(A OR B) = NOT(A) AND NOT(B)  → 德摩根定律
    createColumnStatsIndexFilterExprInternal(And(Not(left), Not(right)), ...)
```

通过德摩根展开，将 `NOT(复合表达式)` 转换为基本 NOT 表达式的组合，从而复用前面定义的 `NOT(EqualTo)`、`NOT(In)` 等翻译规则。这是编译器优化中常见的"规范化（normalization）"技巧。

**BETWEEN 表达式的处理**：Spark Catalyst 在分析阶段会将 `col BETWEEN a AND b` 自动展开为 `col >= a AND col <= b`，因此 DataSkippingUtils 不需要单独处理 BETWEEN——它会自然地命中 `GreaterThanOrEqual` 和 `LessThanOrEqual` 的翻译规则。

---

### 11.3 ColumnStatsIndexSupport.computeCandidateFileNames 完整解析

**源码位置**：`hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/ColumnStatsIndexSupport.scala`

`computeCandidateFileNames` 是 Column Stats Index 与 Data Skipping 的核心连接点，它将"有哪些过滤条件"翻译为"需要读哪些文件"。

#### 11.3.1 完整流程

```scala
// 第 85-106 行
override def computeCandidateFileNames(
    fileIndex: HoodieFileIndex,
    queryFilters: Seq[Expression],
    queryReferencedColumns: Seq[String],
    prunedPartitionsAndFileSlices: Seq[...],
    shouldPushDownFilesFilter: Boolean
): Option[Set[String]] = {

    // 前置检查: 索引可用 + 有过滤条件 + 有查询引用列
    if (isIndexAvailable && queryFilters.nonEmpty && queryReferencedColumns.nonEmpty) {

        // Step 1: 决定是否在内存中处理 Column Stats
        val readInMemory = shouldReadInMemory(fileIndex, queryReferencedColumns, inMemoryProjectionThreshold)

        // Step 2: 从已裁剪的分区和文件切片中提取分区名和文件名
        val (prunedPartitions, prunedFileNames) = getPrunedPartitionsAndFileNames(...)

        // Step 3: 加载 Column Stats Index 并执行转置
        loadTransposed(queryReferencedColumns, readInMemory, Some(prunedPartitions), prunedFileNamesOpt) {
            transposedColStatsDF =>
                // Step 4: 在转置后的 DataFrame 上执行过滤
                Some(getCandidateFiles(transposedColStatsDF, queryFilters, prunedFileNames, ...))
        }
    } else {
        Option.empty  // 返回 None 表示无法裁剪，HoodieFileIndex 将读取所有文件
    }
}
```

#### 11.3.2 内存/集群执行决策

```scala
// SparkBaseIndexSupport.scala, 第 150-163 行
protected def shouldReadInMemory(...): Boolean = {
    Option(metadataConfig.getColumnStatsIndexProcessingModeOverride) match {
        case Some(mode) => mode == IN_MEMORY  // 用户强制指定
        case None =>
            // 启发式判断: 文件切片数 * 查询列数 < 阈值(默认 100000)
            fileIndex.getFileSlicesCount * queryReferencedColumns.length < inMemoryProjectionThreshold
    }
}
```

**为什么需要这个决策？** 如果 Column Stats 数据量小（如 1000 个文件 x 3 个列 = 3000 条记录），在 Driver 端内存中处理更快（避免 Spark Job 的调度开销）。但如果数据量大（如 100000 个文件 x 10 个列 = 100 万条记录），分布式处理更高效。阈值默认为 100000，这是一个经验值。

#### 11.3.3 Column Stats Index 的转置（transpose）

这是整个流程中计算最密集的步骤。Metadata Table 中 Column Stats 的原始格式是**行式**的——每条记录描述一个 (文件, 列) 对：

```
文件 A, 列 city, min=Anhui, max=Chongqing, nullCount=0
文件 A, 列 amount, min=100, max=5000, nullCount=2
文件 B, 列 city, min=Fuzhou, max=Shanghai, nullCount=0
```

`transpose` 方法将其转为**列式**——每行对应一个文件，所有列的统计平铺在同一行中：

```
文件 A, city_min=Anhui, city_max=Chongqing, city_nullCount=0, amount_min=100, amount_max=5000, amount_nullCount=2
文件 B, city_min=Fuzhou, city_max=Shanghai, city_nullCount=0, ...
```

```scala
// 第 253-317 行（核心逻辑简化）
val transposedRows = colStatsRecords
    .filter(r => sortedTargetColumnsSet.contains(r.getColumnName))  // 只保留查询引用的列
    .mapToPair(r => (r.getFileName, r))  // 按文件名分组
    .groupByKey()
    .map(p => {
        val columnRecordsMap = p.getValue.map(r => (r.getColumnName, r)).toMap
        // 按目标列顺序对齐，缺失的列填 null
        val alignedRecords = targetIndexedColumns.map(columnRecordsMap.get)
        // 展平为一行: [fileName, valueCount, col1_min, col1_max, col1_null, col2_min, ...]
        Row(coalescedValues: _*)
    })
```

**为什么要转置？** 转置后的 DataFrame 可以直接用 Spark SQL 的 `where()` 方法应用过滤条件。DataSkippingUtils 翻译出的表达式引用的是 `city_minValue`、`city_maxValue` 这样的列名，这恰好对应转置后的 Schema。一次 `where` 操作就能同时对多个列的统计执行过滤，优雅且高效。

#### 11.3.4 候选文件计算 — getCandidateFiles

```scala
// SparkBaseIndexSupport.scala, 第 104-144 行
protected def getCandidateFiles(indexDf: DataFrame, queryFilters: Seq[Expression], ...): Set[String] = {
    // Step 1: 将查询过滤器翻译为 Column Stats 过滤器
    val indexFilter = queryFilters
        .map(translateIntoColumnStatsIndexFilterExpr(_, isExpressionIndex, validIndexedColumns))
        .reduce(And)

    if (indexFilter.equals(TrueLiteral)) {
        fileNamesFromPrunedPartitions  // 无法优化，返回全部文件
    } else {
        // Step 2: 在转置后的 Column Stats DataFrame 上执行过滤
        val prunedCandidateFileNames = indexDf
            .where(sparkAdapter.createColumnFromExpression(indexFilter))
            .select("fileName")
            .collect()
            .map(_.getString(0))
            .toSet

        // Step 3: ★ 关键安全网 — 添加未被索引的文件
        val allIndexedFileNames = indexDf.select("fileName").collect().map(_.getString(0)).toSet
        val notIndexedFileNames = fileNamesFromPrunedPartitions -- allIndexedFileNames
        prunedCandidateFileNames ++ notIndexedFileNames
    }
}
```

**Step 3 是正确性的关键保证**。Column Stats Index 可能不包含所有文件的统计信息（异步 Clustering 可能只处理了部分文件）。对于没有统计信息的文件，我们必须将它们全部包含在候选集中——因为无法判断它们是否匹配查询条件。`notIndexedFileNames = 全部文件 - 已索引文件` 正是这一"安全网"的实现。

**整体性能优势**：通过 `loadTransposed` 的缓存机制（`cachedColumnStatsIndexViews`），同一查询中多次引用 Column Stats 不会重复加载。`withPersistedData` 确保中间 RDD 被持久化到内存，避免在 `prunedCandidateFileNames` 和 `allIndexedFileNames` 两次 `collect` 时重复计算。

---

### 11.4 RangeSampleSort（采样排序）vs 直接映射排序

**源码位置**：
- 采样排序入口：`SpaceCurveSortingHelper.orderDataFrameBySamplingValues` (第 268-275 行)
- 采样排序实现：`hudi-client/hudi-spark-client/src/main/scala/org/apache/spark/sql/hudi/execution/RangeSample.scala`

Hudi 提供了两种空间曲线排序的实现方式，对应 `SpaceCurveSortingHelper` 中的两个方法。

#### 11.4.1 直接映射排序（orderDataFrameByMappingValues）

```
数据值 → 固定映射函数(类型映射) → 空间曲线编码 → 字节序排序
```

- 映射方式：`mapColumnValueTo8Bytes` / `mapColumnValueToLong`
- 特点：直接将原始值映射为数值坐标，不需要预扫描数据
- 限制：只支持基本数据类型（long, int, short, double, float, string, timestamp, decimal, date, byte, boolean, binary），共 12 种
- String 类型只取 UTF-8 前 8 字节，不同前缀的长字符串无法区分

#### 11.4.2 采样排序（orderDataFrameBySamplingValues）

```
数据值 → 采样 → 计算分位数边界 → 值映射为排名(rank) → 空间曲线编码 → 字节序排序
```

采样排序的核心思想是**用采样确定每个维度的值分布，然后将原始值转为其在分布中的排名**。

```scala
// RangeSample.scala, 第 258-451 行
def sortDataFrameBySample(df: DataFrame, layoutOptStrategy: LayoutOptimizationStrategy, ...): DataFrame = {
    // Step 1: 对每列采样并计算分位数边界
    val sample = new RangeSample(zOrderBounds, sampleRdd)
    val rangeBounds = sample.getRangeBounds()

    // Step 2: 对每列独立确定边界
    val sampleBounds = (0 to candidateColNumber - 1).map { i =>
        sample.determineBound(colRangeBound, math.min(zOrderBounds, rangeBounds.length), ordering)
    }

    // Step 3: ★ 扩展边界以平衡各维度的分辨率
    val maxLength = sampleBounds.map(_.length).max
    val expandSampleBoundsWithFactor = sampleBounds.map { bound =>
        val fillFactor = maxLength / bound.size
        // 如果某列的边界数远少于最大值，插值扩展
        ...
    }

    // Step 4: 广播边界，将每个值映射为其排名
    val indexRdd = rawRdd.mapPartitions { iter =>
        iter.map { row =>
            val values = zFields.map { case ((index, field), rawIndex) =>
                // ★ getRank: 通过二分查找将值映射为排名
                getRank(rawIndex, value, isNull)
            }
            // Step 5: 对排名进行空间曲线编码
            val mapValues = layoutOptStrategy match {
                case HILBERT => HilbertCurveUtils.indexBytes(hilbertCurve, ...)
                case ZORDER  => BinaryUtil.interleaving(values.map(BinaryUtil.intTo8Byte(_)), 8)
            }
            Row.fromSeq(row.toSeq ++ Seq(mapValues))
        }
    }.sortBy(...)
}
```

#### 11.4.3 两种方式的核心区别

| 维度 | 直接映射 (Mapping) | 采样排序 (Sampling) |
|------|------------------|-------------------|
| **数据分布感知** | 不感知。值域映射是静态的 | 感知。通过采样了解数据分布 |
| **维度平衡** | 各维度取值范围可能差异巨大。如 city 有 30 个值，amount 有 100 万个值 | 各维度统一映射为排名 [0, N)，天然平衡 |
| **类型支持** | 仅 12 种基本类型 | 支持所有 Spark 数据类型（包括复杂类型） |
| **预处理开销** | 无（直接映射） | 需要一次采样 Job（额外的 Spark Stage） |
| **精度** | String 只取前 8 字节，大 Decimal 可能溢出 | 基于排名，精度取决于采样点数 |
| **适用场景** | 数据分布均匀、类型简单的场景 | 数据倾斜严重、包含复杂类型的场景 |

**为什么采样排序能解决维度不平衡问题？**

假设 `city` 列有 10 个不同值，`amount` 列有 100 万个不同值。直接映射时：
- city 映射到 [0, 10] 的范围（很窄）
- amount 映射到 [0, 1000000] 的范围（很宽）

在 Z-Order 位交错中，amount 的高位几乎独占了编码的高位，city 的影响被"淹没"了。结果就是排序几乎只按 amount 排，退化为单维排序。

采样排序将两者都映射为排名 [0, N)：
- city 的 10 个值均匀映射到 [0, N) 的 10 个区间
- amount 的 100 万个值也均匀映射到 [0, N) 的 N 个区间

两个维度的"分辨率"被强制对齐，空间曲线的多维平衡性得到保证。

**采样排序中的随机性（防止条纹现象）**：

```scala
// RangeSample.scala, 第 374-377 行
if (factor > 1) {
    doubleDecisionBound.getBound(value + (threadLocalRandom.nextInt(factor) + 1)*(1 / factor.toDouble), expandBound)
} else {
    longDecisionBound.getBound(value, expandBound)
}
```

当某列的边界数远少于另一列时（`factor > 1`），采样排序会在排名中引入随机扰动。这防止了"条纹效应"——如果 city 只有 10 个值，不加扰动时同一城市的所有记录会有完全相同的排名，在 Z-Order 编码中形成条纹状分布，破坏空间局部性。

---

### 11.5 HoodieFileIndex 与 Spark Catalyst 的集成

**源码位置**：
- `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieFileIndex.scala`
- `hudi-spark-datasource/hudi-spark3-common/src/main/scala/org/apache/spark/sql/hudi/analysis/Spark3HoodiePruneFileSourcePartitions.scala`
- `hudi-spark-datasource/hudi-spark/src/main/scala/org/apache/spark/sql/hudi/HoodieSparkSessionExtension.scala`

HoodieFileIndex 是 Hudi 与 Spark 查询引擎之间的关键桥梁。它实现了 Spark 的 `FileIndex` 接口，但在内部实现了远超标准 `FileIndex` 的优化逻辑。

#### 11.5.1 集成的总体架构

```
Spark SQL 查询执行流程:

1. SQL 解析 → 逻辑计划
2. 分析（Analysis）→ 解析后的逻辑计划
3. ★ 优化（Optimizer）:
   ├── Spark 原生规则 (如 ConstantFolding, PushDownPredicates)
   ├── HoodiePruneFileSourcePartitions ← Hudi 注入的分区裁剪规则
   │   调用 HoodieFileIndex.filterFileSlices(dataFilters, partitionFilters)
   │       ├── 分区裁剪
   │       ├── Partition Stats 过滤
   │       └── ★ Data Skipping (通过 lookupCandidateFilesInMetadataTable)
   └── 其他优化规则
4. 物理计划 → 调用 HoodieFileIndex.listFiles(partitionFilters, dataFilters)
5. 执行 → 只读取候选文件
```

#### 11.5.2 HoodieSparkSessionExtension — 注入点

```scala
// HoodieSparkSessionExtension.scala
class HoodieSparkSessionExtension extends (SparkSessionExtensions => Unit) {
    override def apply(extensions: SparkSessionExtensions): Unit = {
        // 注入 SQL 解析器
        extensions.injectParser { (session, parser) => new HoodieCommonSqlParser(session, parser) }

        // 注入分析规则（Resolution Rules）
        HoodieAnalysis.customResolutionRules.foreach { ... }

        // 注入优化器规则（Optimizer Rules）
        HoodieAnalysis.customOptimizerRules.foreach { ... }
        // ★ 其中包含 HoodiePruneFileSourcePartitions
    }
}
```

`HoodieSparkSessionExtension` 通过 Spark 的 `SparkSessionExtensions` 机制注入自定义规则。用户需要在 Spark 配置中添加：

```
spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
```

**为什么不使用 Spark 原生的 `PruneFileSourcePartitions`？** Spark 原生规则只支持分区裁剪（基于分区列的过滤），而 Hudi 需要在同一阶段同时执行 Data Skipping（基于数据列的文件级裁剪）。自定义规则 `HoodiePruneFileSourcePartitions` 在分区裁剪的基础上额外调用了 `lookupCandidateFilesInMetadataTable`，实现了分区+文件的两级裁剪。

#### 11.5.3 HoodiePruneFileSourcePartitions — 优化器规则

```scala
// Spark3HoodiePruneFileSourcePartitions.scala, 第 41-82 行
case class Spark3HoodiePruneFileSourcePartitions(spark: SparkSession) extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
        // ★ 模式匹配: 找到包含 HoodieFileIndex 且尚未推送谓词的 LogicalRelation
        case op @ PhysicalOperation(projects, filters, lr @ LogicalRelation(HoodieRelationMatcher(fileIndex), ...))
            if !fileIndex.hasPredicatesPushedDown =>

            // Step 1: 过滤确定性谓词（排除包含子查询的非确定性表达式）
            val deterministicFilters = filters.filter(f => f.deterministic && !SubqueryExpression.hasSubquery(f))

            // Step 2: 分离分区过滤和数据过滤
            val (partitionPruningFilters, dataFilters) =
                getPartitionFiltersAndDataFilters(fileIndex.partitionSchema, normalizedFilters)

            // Step 3: ★ 核心调用 — 在 HoodieFileIndex 上执行文件过滤
            fileIndex.filterFileSlices(dataFilters, partitionPruningFilters, isPartitionPruned = true)

            // Step 4: 更新统计信息供 CBO 使用
            if (partitionPruningFilters.nonEmpty) {
                val filteredStats = FilterEstimation(Filter(partitionPruningFilters.reduce(And), lr)).estimate
                val tableWithStats = lr.catalogTable.map(_.copy(
                    stats = Some(CatalogStatistics(sizeInBytes = BigInt(fileIndex.sizeInBytes), ...))
                ))
                val prunedLogicalRelation = lr.copy(catalogTable = tableWithStats)
                rebuildPhysicalOperation(projects, filters, prunedLogicalRelation)
            }
    }
}
```

**这个规则被注入到 Spark Optimizer 的哪个阶段？** 根据 `HoodieAnalysis.customOptimizerRules` 的注释，它作为 Optimizer 规则执行，位于 CBO（基于代价的优化）之前。这意味着：
- 它能访问到与 CBO 相同的统计信息
- 它在实际物理计划生成之前执行，确保裁剪后的文件列表能被后续优化阶段利用
- 它先于 Spark 原生的 `customEarlyScanPushDownRules` 执行

#### 11.5.4 HoodieFileIndex.filterFileSlices — 核心调度

```scala
// HoodieFileIndex.scala, 第 223-294 行
def filterFileSlices(dataFilters: Seq[Expression], partitionFilters: Seq[Expression], ...):
    Seq[(Option[PartitionPath], Seq[FileSlice])] = {

    // Step 1: 分区裁剪
    val (isPruned, prunedPartitionsAndFileSlices) = prunePartitionsAndGetFileSlices(dataFilters, partitionFilters)

    // Step 2: Data Skipping
    if (prunedPartitionsAndFileSlices.nonEmpty && dataFilters.nonEmpty && !isPartitionPruned) {
        val candidateFilesNamesOpt = lookupCandidateFilesInMetadataTable(dataFilters, ...) match {
            case Success(opt) => opt
            case Failure(e) =>
                // ★ 优雅降级: 根据配置决定是回退还是报错
                spark.sqlContext.getConf(DataSkippingFailureMode.configName, "fallback") match {
                    case "fallback" => Option.empty  // Data Skipping 失败时回退到全文件扫描
                    case "strict"   => throw new HoodieException(e)
                }
        }
        // Step 3: 按候选文件名过滤文件切片
        prunedPartitionsAndFileSlices.map { case (partitionOpt, fileSlices) =>
            val candidateFileSlices = fileSlices.filter(fs => {
                // 检查文件切片中的任意文件（base file 或 log file）是否在候选集中
                fileSliceFiles.stream().filter(candidateFilesNamesOpt.get.contains(_)).findAny().isPresent
            })
            (partitionOpt, candidateFileSlices)
        }
    }
}
```

#### 11.5.5 多索引级联查询

```scala
// HoodieFileIndex.scala, 第 391-418 行
private def lookupCandidateFilesInMetadataTable(...): Try[Option[Set[String]]] = Try {
    if (isDataSkippingEnabled) {
        // ★ 按顺序尝试所有可用索引
        for (indexSupport <- indicesSupport) {
            if (indexSupport.isIndexAvailable && indexSupport.supportsQueryType(options)) {
                val prunedFileNames = indexSupport.computeCandidateIsStrict(...)
                if (prunedFileNames.nonEmpty) {
                    return Try(prunedFileNames)  // ★ 第一个返回结果的索引即生效
                }
            }
        }
    }
    Option.empty
}
```

`indicesSupport` 的顺序定义在 HoodieFileIndex 初始化时（第 114-126 行）：

```scala
@transient private lazy val indicesSupport: List[SparkBaseIndexSupport] = List(
    new RecordLevelIndexSupport(...),     // 1. 记录级索引（点查最快）
    new PartitionBucketIndexSupport(...), // 2. 分区桶索引（或普通桶索引）
    new SecondaryIndexSupport(...),       // 3. 二级索引
    new ExpressionIndexSupport(...),      // 4. 表达式索引
    new BloomFiltersIndexSupport(...),    // 5. 布隆过滤器索引
    new ColumnStatsIndexSupport(...)      // 6. 列统计索引
)
```

**为什么顺序这么设计？** 越精确的索引优先级越高。RecordLevelIndex 能精确到单条记录所在的文件，效果最好；而 ColumnStatsIndex 是基于 min/max 的范围过滤，是最"粗粒度"的索引。第一个返回非空结果的索引就直接返回，避免了不必要的索引查询开销。

#### 11.5.6 listFiles — Spark 物理计划的入口

```scala
// HoodieFileIndex.scala, 第 174-181 行
override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val slices = filterFileSlices(dataFilters, partitionFilters).flatMap(...)
    prepareFileSlices(slices)
}
```

Spark 在生成物理执行计划时调用 `FileIndex.listFiles()`。Hudi 在这里返回已经经过分区裁剪和 Data Skipping 过滤后的文件列表。从 Spark 的视角看，它"以为"表只有这些文件——这是 Hudi 实现查询优化对 Spark 完全透明的关键。

**sizeInBytes 的重要性**：

```scala
// 第 433-443 行
override def sizeInBytes: Long = {
    val size = getTotalCachedFilesSize
    if (size == 0 && !enableHoodieExtension) {
        // ★ 如果没有启用 HoodieExtension，返回 Long.MaxValue 避免 Broadcast Join
        Long.MaxValue
    } else {
        size
    }
}
```

`sizeInBytes` 被 Spark 的 CBO（Cost-Based Optimizer）用于判断是否采用 Broadcast Join。如果 Data Skipping 将表大小从 1TB 降低到 10MB，CBO 可能自动选择 Broadcast Join，进一步加速查询。但如果没有启用 `HoodieSparkSessionExtension`，`HoodiePruneFileSourcePartitions` 规则不会执行，`sizeInBytes` 将不准确，因此返回 `Long.MaxValue` 以安全地禁用 Broadcast Join。

**整体协作总结**：

```
用户查询: SELECT * FROM orders WHERE city='Beijing' AND amount > 1000

1. HoodieSparkSessionExtension 在 SparkSession 启动时注入 HoodiePruneFileSourcePartitions 规则

2. Spark Optimizer 执行到 HoodiePruneFileSourcePartitions:
   ├── 匹配到 HoodieFileIndex
   ├── 分离过滤条件: partitionFilters=[], dataFilters=[city='Beijing', amount>1000]
   └── 调用 fileIndex.filterFileSlices(dataFilters, partitionFilters)

3. HoodieFileIndex.filterFileSlices:
   ├── prunePartitionsAndGetFileSlices → 获取所有分区和文件切片
   └── lookupCandidateFilesInMetadataTable:
       ├── RecordLevelIndex: 不适用（非 Record Key 查询）→ None
       ├── BucketIndex: 不适用 → None
       ├── SecondaryIndex: 不适用 → None
       ├── ExpressionIndex: 不适用 → None
       ├── BloomFiltersIndex: 不适用 → None
       └── ColumnStatsIndex: ★ 命中!
           ├── loadTransposed → 加载并转置 Column Stats
           ├── translateIntoColumnStatsIndexFilterExpr:
           │   city='Beijing' → city_min <= 'Beijing' AND city_max >= 'Beijing'
           │   amount > 1000  → amount_max > 1000
           │   组合: AND(city条件, amount条件)
           ├── indexDf.where(indexFilter) → 过滤得到候选文件
           └── 添加未索引文件 → 返回候选文件集合

4. 只保留候选文件集合中的文件切片 → 返回给 Spark

5. Spark 物理计划只扫描候选文件，跳过了 95% 的文件
```

---

---

## 审查记录

**审查日期**: 2026-04-21
**审查内容**: 
1. 验证了所有类名、方法名和源码位置的准确性
2. 确认了配置项名称的正确性
3. 补充了源码行号和完整方法签名
4. 修正了类型定义（ColumnStatsIndexSupport 是 class 而非 trait）

**主要修正**:
- 补充了 `LayoutOptimizationStrategy` 枚举的完整注解
- 更新了 `orderDataFrameByMappingValues` 方法的完整签名
- 补充了 `createZCurveSortedRDD` 和 `createHilbertSortedRDD` 的完整实现
- 添加了 `HilbertCurveUtils` 的源码位置
- 明确了 `ColumnStatsIndexSupport` 的类型定义

**文档版本**: 1.1
**创建日期**: 2026-04-15
**最后审查**: 2026-04-21
**基于 Hudi 版本**: v1.2.0-SNAPSHOT (master)
