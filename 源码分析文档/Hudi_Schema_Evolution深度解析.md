# Hudi Schema Evolution 深度解析

> 基于 Apache Hudi v1.2.0-SNAPSHOT 源码分析
> 核心源码路径: 
> - `/hudi-common/src/main/java/org/apache/hudi/internal/schema/`
> - `/hudi-common/src/main/java/org/apache/hudi/common/schema/`

---

## 目录

1. [总体架构概览](#1-总体架构概览)
2. [InternalSchema 核心设计](#2-internalschema-核心设计)
3. [AvroSchemaEvolutionUtils 与 Schema 对齐算法](#3-avroschemaevolutionutils-与-schema-对齐算法)
4. [TableSchemaResolver：Schema 版本推导](#4-tableschemaresolver-schema-版本推导)
5. [支持的 Schema 变更操作](#5-支持的-schema-变更操作)
6. [读时 Schema 对齐（FileGroupReaderSchemaHandler）](#6-读时-schema-对齐filegroupreaderschemahandler)
7. [写时 Schema 兼容性检查](#7-写时-schema-兼容性检查)
8. [与 Spark/Flink 的 Schema Evolution 集成](#8-与-sparkflink-的-schema-evolution-集成)
9. [与 Iceberg/Delta 的 Schema Evolution 对比](#9-与-icebergdelta-的-schema-evolution-对比)

---

## 1. 总体架构概览

## 1. 解决什么问题

Schema Evolution 要解决数据湖场景中最核心的矛盾:**业务需求不断变化导致表结构频繁调整,但历史数据文件已经按旧 Schema 写入且不可变**。

**核心业务问题**:
- **历史数据兼容性**: 表结构变更后,如何正确读取使用旧 Schema 写入的 Parquet/ORC 文件?
- **数据完整性保障**: 新增列时,历史文件中缺失的列如何填充? 删除列时,如何避免误读旧数据?
- **语义正确性**: 列重命名后,如何保证旧文件中的数据能正确映射到新列名? 如何避免"删除再添加同名列"导致的数据混淆?
- **类型安全性**: 列类型变更(如 int -> long)时,如何在读取时进行安全的类型转换?

**如果没有 Schema Evolution 会有什么问题**:
1. **无法新增列**: 任何新增列都会导致旧文件无法读取,或者读取时新列全部为 NULL 且无法区分是"真实 NULL"还是"列不存在"
2. **无法重命名列**: 重命名列会导致旧文件中的数据丢失,因为 Parquet 按列名匹配,找不到新列名就认为数据不存在
3. **无法删除列**: 删除列后,旧文件中仍包含该列的数据,可能导致读取错误或数据泄露
4. **无法类型升级**: 业务需要将 int 升级为 long 时,必须重写所有历史数据,成本极高

**实际应用场景举例**:

**场景 1: 用户行为分析表的演进**
```
初始 Schema (2024-01-01):
  user_id: int, event_type: string, timestamp: long

业务变更 1 (2024-03-01): 新增用户画像字段
  + age: int, gender: string

业务变更 2 (2024-06-01): user_id 从 int 升级为 long (支持更大用户量)
  user_id: int -> long

业务变更 3 (2024-09-01): 重命名字段以符合新规范
  event_type -> action_type
```
没有 Schema Evolution,每次变更都需要:
- 停止写入
- 重写所有历史数据(可能数百 TB)
- 更新所有下游任务
- 重新启动写入

有了 Schema Evolution:
- 变更立即生效
- 历史数据无需重写
- 查询自动处理 Schema 差异

**场景 2: 实时数据湖的 Schema 漂移**

Kafka 消息的 Schema 在不断演进,每天可能有多个版本:
```
Day 1: {id: int, name: string}
Day 2: {id: int, name: string, email: string}  // 新增 email
Day 3: {id: int, name: string, email: string, phone: string}  // 新增 phone
```
如果没有 Schema Evolution,要么:
- 拒绝写入新 Schema 的数据(业务中断)
- 为每个 Schema 版本创建独立的表(查询复杂度爆炸)

有了 Schema Evolution:
- 自动检测并合并 Schema 变更
- 单表存储所有版本数据
- 查询时自动填充缺失列

## 2. 有什么坑

### 坑 1: 新增列强制为 nullable,无法添加 required 列

**源码证据** (`TableChanges.ColumnAddChange.addColumns`):
```java
// 第 644 行
Types.Field newField = Types.Field.get(nextId, true, name, typeWithNewId, doc);
//                                      ^^^^ 强制为 true(optional)
```

**问题**: 即使业务上新列不应该为 NULL,也无法在 Schema Evolution 中添加 required 列。

**原因**: 历史文件不包含新列,读取时必须填充 NULL。如果新列是 required,历史文件就违反了约束。

**规避方法**:
- 在应用层做非空校验,而非依赖 Schema 约束
- 使用默认值填充(需要自定义 RecordMerger)

### 坑 2: 类型变更后读取性能下降

**源码证据** (`InternalSchemaMerger` 第 52-53 行):
```java
// spark parquetReader need the original column type to read data
private boolean useColumnTypeFromFileSchema = true;
```

**问题**: 列从 int 升级为 long 后,读取旧文件时需要两步:
1. 用原始类型(int)读取 Parquet 文件
2. 将读出的数据从 int 转换为 long

这导致额外的类型转换开销,尤其在大规模扫描时明显。

**性能陷阱**: 如果表中有多个列发生类型变更,且查询涉及这些列,类型转换开销会累加。

**规避方法**:
- 通过 Compaction 重写旧文件为新 Schema,消除类型转换开销
- 避免在热路径列上做类型变更

### 坑 3: 重命名列后过滤条件失效

**源码证据** (`InternalSchemaMerger.dealWithRename` 第 173 行):
```java
renamedFields.put(querySchema.findFullName(fieldId), nameFromFileSchema);
```

**问题**: 列重命名后,如果查询带有过滤条件(如 `WHERE new_name = 'value'`),读取旧文件时过滤条件需要重写为旧列名,否则无法下推到 Parquet Reader。

**Spark 中的处理** (`ParquetSchemaEvolutionUtils.rebuildFilterFromParquet`):
- 如果列被重命名,将过滤条件中的新列名替换为旧列名
- 如果列是新增的(旧文件中不存在),过滤条件变为 `AlwaysTrue`(无法下推)

**性能影响**: 新增列的过滤条件无法下推,导致全表扫描。

**规避方法**:
- 重命名列后,通过 Compaction 重写旧文件
- 避免在新增列上建立频繁查询的过滤条件

### 坑 4: 嵌套结构变更的复杂性

**源码证据** (`AvroSchemaEvolutionUtils.reconcileSchema` 第 88-101 行):
```java
// Remove redundancy from diffFromEvolutionSchema
// for example, now we add a struct col, the struct col is "user struct<name:string, age:int>"
// when we do diff operation: user, user.name, user.age will appear in the resultSet which is redundancy
```

**问题**: 添加嵌套结构(如 `struct<name:string, age:int>`)时,如果不去重,会重复添加父字段和所有子字段,导致 Schema 错乱。

**容易踩的坑**:
- 手动构造 Schema 变更时,忘记处理嵌套结构的层级关系
- 删除嵌套结构的某个子字段后,父结构变为空 struct,可能导致读取异常

**规避方法**:
- 使用 Hudi 提供的 DDL 接口(ALTER TABLE),而非手动构造 InternalSchema
- 删除嵌套字段前,检查是否会导致父结构为空

### 坑 5: Schema 缓存不一致导致的读取错误

**源码证据** (`InternalSchemaCache` 第 98-100 行):
```java
if (historicalSchemas.keySet().stream().max(Long::compareTo).map(maxVersionId -> versionID > maxVersionId).orElse(false)) {
    historicalSchemas = getHistoricalSchemas(metaClient);
    HISTORICAL_SCHEMA_CACHE.put(tablePath, historicalSchemas);
}
```

**问题**: 如果查询的 versionID 大于缓存中的最大版本,会重新加载历史 Schema。但在分布式环境中,不同 Executor 的缓存可能不一致。

**生产环境问题**:
- Executor A 缓存了版本 1-10
- 此时写入了版本 11
- Executor B 查询版本 11 的文件,缓存未更新,回退到版本 10 的 Schema,导致读取错误

**规避方法**:
- 使用 Caffeine Cache 的 `weakValues()`,内存紧张时自动回收缓存
- 在 Schema 变更后,重启查询任务以清空缓存

### 坑 6: RECONCILE_SCHEMA 配置的误用

**源码证据** (`HoodieCommonConfig` 第 1015-1027 行):
```java
@Deprecated
public static final ConfigProperty<Boolean> RECONCILE_SCHEMA = ...
    .defaultValue(false)
    .deprecatedAfter("0.14.1")
```

**问题**: 此配置已在 0.14.1 版本后标记为 `@Deprecated`,但很多用户仍在使用。

**误用场景**:
- 用户期望通过设置 `RECONCILE_SCHEMA=true` 来启用 Schema Evolution,但实际上这只是写入时的 Schema 协调,不等同于完整的 Schema Evolution 功能
- 与 `SET_NULL_FOR_MISSING_COLUMNS` 配置混淆,导致预期外的 nullable 变更

**正确做法**:
- 使用 DDL 显式管理 Schema 变更
- 理解 `RECONCILE_SCHEMA` 只处理"缺失列"和"新增列",不处理删除和重命名

## 3. 核心概念解释

### 3.1 InternalSchema vs Avro Schema

**InternalSchema**: Hudi 自建的 Schema 表示,核心特征是基于 **Column ID** 追踪字段。

**Avro Schema**: Avro 原生的 Schema 表示,基于 **字段名称** 匹配。

**关键区别**:

| 维度 | InternalSchema | Avro Schema |
|------|---------------|-------------|
| 字段标识 | Column ID (整数,全局唯一) | 字段名称 (字符串) |
| 重命名支持 | 原生支持 (ID 不变) | 不支持 (名称变化导致匹配失败) |
| 删除再添加同名列 | 安全 (新列获得新 ID) | 不安全 (新列匹配到旧数据) |
| 嵌套结构 | 每层都有 ID | 只有顶层有名称 |

**源码证据** (`InternalSchema` 第 59-62 行):
```java
private transient Map<Integer, Field> idToField = null;    // ID -> Field 映射
private transient Map<String, Integer> nameToId = null;    // 全限定名 -> ID 映射
private transient Map<Integer, String> idToName = null;    // ID -> 全限定名 映射
```

### 3.2 Column ID 的分配机制

**源码证据** (`InternalSchema` 第 56-57 行):
```java
@Getter
@Setter
private int maxColumnId;
```

**分配规则**:
1. 新表创建时,从 0 开始为每个字段分配连续的 ID
2. 添加新列时,新列 ID = `maxColumnId + 1`
3. 删除列时,`maxColumnId` 不变,被删除的 ID 永不复用
4. 嵌套结构中,子字段也拥有独立的 ID

**为什么 ID 不复用?**

如果删除列后复用 ID,可能导致:
```
版本 1: id=5 -> column_a (int)
版本 2: 删除 column_a
版本 3: 添加 column_b (string), 复用 id=5
```
读取版本 1 的文件时,id=5 会错误地映射到 column_b,导致类型不匹配。

### 3.3 Schema 版本号 (versionId)

**源码证据** (`InternalSchema` 第 137-140 行):
```java
public InternalSchema setSchemaId(long versionId) {
    this.versionId = versionId;
    return this;
}
```

**versionId 的含义**: 使用 **commit 时间戳** 作为 Schema 版本号。

**好处**:
- 文件名中自带 commit 时间,无需额外映射表就能定位文件的 Schema 版本
- 版本号天然有序,支持范围查询

**源码证据** (`FileGroupReaderSchemaHandler.getRequiredSchemaForFileAndRenamedColumns` 第 138 行):
```java
long commitInstantTime = Long.parseLong(FSUtils.getCommitTime(path.getName()));
InternalSchema fileSchema = InternalSchemaCache.searchSchemaAndCache(commitInstantTime, metaClient);
```

### 3.4 Schema 合并 (Schema Merge)

**定义**: 将文件的 Schema 与查询的 Schema 合并,生成适用于读取该文件的 merged schema。

**源码证据** (`InternalSchemaMerger` 第 84-87 行):
```java
public InternalSchema mergeSchema() {
    Types.RecordType record = (Types.RecordType) mergeType(querySchema.getRecord(), 0);
    return new InternalSchema(record);
}
```

**合并规则**:
1. **字段存在且名称未变**: 可能需要类型转换
2. **字段存在但名称不同**: 处理重命名,使用文件中的旧名称
3. **字段在文件中不存在**: 新增列,读取时填充 NULL
4. **字段在查询中不存在**: 已删除列,跳过不读

### 3.5 Schema 协调 (Schema Reconciliation)

**定义**: 写入时,将 incoming schema 与 table schema 合并,自动检测并应用 Schema 变更。

**源码证据** (`AvroSchemaEvolutionUtils.reconcileSchema` 第 67 行):
```java
public static InternalSchema reconcileSchema(Schema incomingSchema, InternalSchema oldTableSchema, boolean makeMissingFieldsNullable)
```

**与 Schema Merge 的区别**:
- **Schema Merge**: 读取时使用,目标是生成能正确读取文件的 Schema
- **Schema Reconciliation**: 写入时使用,目标是生成兼容的 evolved schema

## 4. 设计理念

### 4.1 为什么选择 Column ID 而非 Column Name?

**设计权衡**:

**方案 A: 基于 Column Name (Avro 原生方案)**
- 优点: 实现简单,与 Avro 生态无缝集成
- 缺点: 无法支持重命名,删除再添加同名列不安全

**方案 B: 基于 Column ID (Hudi/Iceberg 方案)**
- 优点: 支持重命名,语义正确性强
- 缺点: 需要维护 ID 分配机制,增加复杂度

**Hudi 的选择**: 方案 B,因为数据湖场景中 Schema 变更频繁,语义正确性比实现简单性更重要。

**源码证据** (`InternalSchemaMerger.dealWithRename` 第 161-183 行):
```java
private Types.Field dealWithRename(int fieldId, Type newType, Types.Field oldField) {
    Types.Field fieldFromFileSchema = fileSchema.findField(fieldId);
    String nameFromFileSchema = fieldFromFileSchema.name();
    String nameFromQuerySchema = querySchema.findField(fieldId).name();
    // 通过 ID 匹配,而非名称
    ...
}
```

### 4.2 为什么需要 InternalSchema 和 Avro Schema 双重表示?

**设计理念**: **分层抽象,职责分离**。

- **InternalSchema**: 引擎无关的 Schema 表示,负责 Schema Evolution 逻辑
- **Avro Schema**: 与存储格式绑定的 Schema 表示,负责数据序列化/反序列化

**好处**:
1. **引擎无关性**: Spark、Flink、Java Client 都使用同一套 InternalSchema 逻辑
2. **向后兼容性**: 旧版本 Hudi 表(没有 InternalSchema)仍可通过 Avro Schema 读取
3. **灵活性**: 可以支持 Parquet、ORC、Avro 等多种存储格式

**源码证据** (`InternalSchemaConverter`):
```java
// InternalSchema <-> HoodieSchema(Avro) 双向转换
public static InternalSchema convert(HoodieSchema schema)
public static HoodieSchema convert(InternalSchema schema, String recordName)
```

### 4.3 为什么读取时需要 useColumnTypeFromFileSchema?

**设计权衡**: **Parquet Reader 的限制 vs 类型安全**。

**问题**: Parquet Reader 必须使用文件中实际存储的类型来读取数据。如果传入升级后的类型,读取会失败。

**源码证据** (`InternalSchemaMerger` 第 45-49 行):
```java
// spark parquetReader need the original column type to read data, otherwise the parquetReader will failed.
// eg: current column type is StringType, now we changed it to decimalType,
// we should not pass decimalType to parquetReader, we must pass StringType to it
private boolean useColumnTypeFromFileSchema = true;
```

**解决方案**: 分两步处理
1. 用文件原始类型读取数据
2. 读出后进行类型转换

**为什么 Log Reader 不需要?**

Log Reader 使用 `reWriteRecordWithNewSchema` 函数,可以直接传入新类型进行重写,无需两步处理。

### 4.4 架构演进历史

**阶段 1: 无 Schema Evolution (Hudi 0.x 早期)**
- Schema 存储在 TableConfig 中,所有文件使用相同 Schema
- 任何 Schema 变更都需要重写数据

**阶段 2: 基于 Avro Schema Evolution (Hudi 0.6-0.8)**
- 利用 Avro 原生的 Schema Evolution 能力
- 局限性: 无法支持重命名,类型变更受限

**阶段 3: 引入 InternalSchema (Hudi 0.10+)**
- 自建 Column ID 追踪体系
- 支持完整的 DDL 操作(ADD/DROP/RENAME/TYPE CHANGE/REORDER)
- 与 Spark/Flink 深度集成

**阶段 4: 优化与增强 (Hudi 1.x)**
- 引入 InternalSchemaCache 提升性能
- 支持嵌套结构的 Schema Evolution
- 改进 Schema 协调算法

### 4.5 与业界其他方案的对比

**Iceberg 的方案**:
- 同样基于 Column ID
- Schema 存储在 metadata.json 中,与 snapshot 关联
- 优势: Schema Evolution 是一等公民,所有写入工具都理解 Column ID
- 劣势: 要求生态工具全部适配

**Delta Lake 的方案**:
- 早期基于 Column Name,后期引入 Column Mapping
- Column Mapping 一旦启用不可禁用
- 优势: 实现简单,基于 Parquet 原生能力
- 劣势: Column Mapping 模式有性能开销

**Hudi 的优势**:
- 与时间线深度集成,Schema 变更与数据写入共享事务机制
- 灵活的读取策略(useColumnTypeFromFileSchema/useColNameFromFileSchema)
- 支持隐式 Schema Reconciliation,适合流式写入场景

### 1.1 Schema Evolution 面临的核心挑战

在数据湖场景中，表的 Schema 不可避免地会随着业务发展而变化。Schema Evolution（模式演进）要解决的核心问题是：**当表的 Schema 发生变更后，如何正确读取历史数据文件？**

这里的挑战体现在多个层面：

- 历史 Parquet/ORC 文件使用旧 Schema 写入，新 Schema 增加了列 —— 需要为旧文件中缺失的列填充 NULL
- 历史文件中存在的列被删除 —— 读取时需要跳过这些列
- 列被重命名 —— 读取旧文件时需要将旧名称映射到新名称
- 列类型发生变化 —— 读取时需要进行类型转换（如 int -> long）

### 1.2 Hudi 的 Schema Evolution 分层架构

Hudi 构建了一套完整的 Schema Evolution 分层架构：

```
                    +------------------------------+
  用户接口层         | Spark ALTER TABLE / Flink DDL |
                    +------------------------------+
                              |
                    +------------------------------+
  Spark/Flink       | AlterTableCommand            |
  适配层             | InternalSchemaManager (Flink)|
                    +------------------------------+
                              |
                    +------------------------------+
  变更操作层         | InternalSchemaChangeApplier   |
                    | TableChanges (Add/Delete/     |
                    |   Update)                     |
                    +------------------------------+
                              |
                    +------------------------------+
  核心模型层         | InternalSchema                |
                    | Type / Types / Field          |
                    | Column ID 追踪体系              |
                    +------------------------------+
                              |
                    +------------------------------+
  持久化与           | SerDeHelper (JSON 序列化)      |
  存储层             | FileBasedInternalSchema       |
                    |   StorageManager              |
                    +------------------------------+
                              |
                    +------------------------------+
  读写对齐层         | InternalSchemaMerger (读)     |
                    | AvroSchemaEvolutionUtils (写)  |
                    | FileGroupReaderSchemaHandler   |
                    +------------------------------+
```

**为什么这么设计？** 分层架构使得 Schema Evolution 的核心逻辑与具体计算引擎（Spark/Flink）解耦。核心模型层和变更操作层完全定义在 `hudi-common` 中，是引擎无关的。不同计算引擎只需实现各自的适配层即可复用全部 Schema Evolution 能力。

---

## 2. InternalSchema 核心设计

## 1. 解决什么问题

InternalSchema 要解决 **Avro Schema Evolution 机制的根本性缺陷**:基于字段名称匹配导致的语义错误。

**核心问题**:
1. **重命名列的语义丢失**: Avro 按名称匹配,重命名后旧文件中的数据无法映射到新列名
2. **删除再添加同名列的数据混淆**: Avro 会将新列错误地匹配到旧列的数据
3. **嵌套结构变更的复杂性**: Avro 对嵌套结构的 Schema Evolution 支持有限
4. **引擎绑定**: Avro Schema 与 Avro 库深度绑定,难以支持多引擎

**实际应用场景**:

**场景: 用户表的字段重命名**
```
版本 1: user_name (string)
版本 2: 重命名为 full_name
版本 3: 新增 user_name (string) 用于存储用户名(不含姓氏)
```

使用 Avro Schema:
- 读取版本 1 的文件时,找不到 `full_name`,认为是新列,返回 NULL
- 版本 3 的 `user_name` 会错误地匹配到版本 1 的旧数据

使用 InternalSchema:
- 版本 1: `user_name` (id=0)
- 版本 2: `full_name` (id=0, 重命名)
- 版本 3: `full_name` (id=0), `user_name` (id=5, 新列)
- 读取版本 1 时,通过 id=0 正确映射到 `full_name`

## 2. 有什么坑

### 坑 1: 懒加载索引导致的首次查询延迟

**源码证据** (`InternalSchema` 第 59-62 行):
```java
private transient Map<Integer, Field> idToField = null;    // 懒加载
private transient Map<String, Integer> nameToId = null;    // 懒加载
private transient Map<Integer, String> idToName = null;    // 懒加载
```

**问题**: 索引在首次使用时构建,对于字段数量多的表(如宽表),首次查询会有明显延迟。

**性能陷阱**: 在 Spark 中,每个 Task 都会反序列化 InternalSchema,都需要重建索引。

**规避方法**:
- 避免过宽的表(建议单表字段数 < 1000)
- 使用列裁剪减少需要处理的字段数

### 坑 2: maxColumnId 的维护错误

**源码证据** (`AlterTableCommand` 中):
```java
val newSchema = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, deleteChange)
newSchema.setMaxColumnId(oldSchema.getMaxColumnId)  // 删除列时保持不变
```

**问题**: 如果手动构造 InternalSchema 时忘记更新 `maxColumnId`,会导致新列 ID 冲突。

**容易踩的坑**:
- 从 Avro Schema 转换为 InternalSchema 时,`maxColumnId` 计算错误
- 并发写入时,多个 writer 使用相同的 `maxColumnId`,导致 ID 冲突

**规避方法**:
- 使用 Hudi 提供的 API,而非手动构造 InternalSchema
- 通过 DDL 操作保证 `maxColumnId` 的原子性更新

### 坑 3: 空 Schema 的哨兵值误用

**源码证据** (`InternalSchema` 第 47-48 行):
```java
private static final InternalSchema EMPTY_SCHEMA = new InternalSchema(-1L, RecordType.get());
```

**问题**: `versionId = -1` 表示空 Schema,但如果业务代码中使用 `-1` 作为有效的版本号,会导致逻辑错误。

**规避方法**:
- 始终使用 `isEmptySchema()` 方法判断,而非直接比较 `versionId`

### 坑 4: 嵌套结构的 ID 分配顺序

**源码证据** (`InternalSchemaBuilder.refreshNewId` 第 245-257 行):
```java
// 先为当前层所有字段分配连续 ID
int currentId = nextId.get();
nextId.set(currentId + record.fields().size());
// 再递归处理每个字段的子类型
```

**问题**: 广度优先分配 ID,同一层级的字段 ID 是连续的。如果误以为是深度优先,会导致 ID 预期错误。

**实际影响**: 在手动构造嵌套结构时,ID 分配顺序与预期不符,导致 Schema 匹配失败。

**规避方法**:
- 理解 ID 分配是广度优先
- 使用 `InternalSchemaBuilder.refreshNewId` 自动分配 ID

## 3. 核心概念解释

### 3.1 Column ID 的全局唯一性

**定义**: 每个字段(包括嵌套字段)都有一个全局唯一的整数 ID,在表的整个生命周期中不变且不复用。

**源码证据** (`Types.Field` 第 195-201 行):
```java
public static class Field implements Serializable {
    private final int id;              // 全局唯一的 Column ID
    private final String name;         // 字段名
    private final Type type;           // 字段类型
    ...
}
```

**全局唯一性的保证**:
1. 新列 ID 从 `maxColumnId + 1` 开始
2. 删除列后,ID 不回收
3. 嵌套结构中,子字段 ID 与顶层字段 ID 在同一命名空间

### 3.2 Type 类型体系的层次结构

**源码证据** (文档第 164-183 行):
```
Type (接口)
  |
  +-- PrimitiveType (抽象类)
  |     +-- IntType, LongType, StringType, ...
  |
  +-- NestedType (抽象类)
        +-- RecordType  (struct)
        +-- ArrayType   (array)
        +-- MapType     (map)
```

**为什么自建类型体系?**
1. **引擎无关性**: 不依赖 Avro/Parquet 的类型系统
2. **精确控制**: 区分 `DecimalTypeFixed` 和 `DecimalTypeBytes`
3. **简化逻辑**: 统一的类型层次便于实现 Schema Evolution

### 3.3 懒加载索引的设计

**源码证据** (`InternalSchema` 第 99-104 行):
```java
private Map<Integer, String> getIdToName() {
    if (idToName == null) {
        idToName = buildIdToName(record);
    }
    return idToName;
}
```

**为什么懒加载?**
- InternalSchema 在 Spark Task 序列化传输时,只传输结构数据(RecordType)
- 索引在 Executor 端按需构建,减少序列化开销

**权衡**: 首次访问有构建开销,但避免了网络传输大量索引数据。

### 3.4 嵌套类型的 ID 分配

**ArrayType 的 ID 分配**:
```java
public static ArrayType get(int elementId, boolean isOptional, Type elementType) {
    return new ArrayType(Field.get(elementId, isOptional, "element", elementType));
}
```

**MapType 的 ID 分配**:
```java
public static MapType get(int keyId, int valueId, Type keyType, Type valueType) {
    return new MapType(
        Field.get(keyId, "key", keyType),
        Field.get(valueId, "value", valueType));
}
```

**好处**: 嵌套结构内部的 Schema 变更(如给 struct 添加字段)也能通过 ID 精确追踪。

## 4. 设计理念

### 4.1 为什么使用 versionId = commit 时间戳?

**设计理念**: **利用已有信息,避免额外映射**。

**源码证据** (`InternalSchema.setSchemaId` 第 137-140 行):
```java
public InternalSchema setSchemaId(long versionId) {
    this.versionId = versionId;
    return this;
}
```

**好处**:
1. 文件名中自带 commit 时间,无需额外的"文件 -> Schema 版本"映射表
2. 版本号天然有序,支持范围查询和二分查找
3. 与 Hudi 的时间线机制无缝集成

**权衡**: 如果同一 commit 中有多个 Schema 变更,只能记录最终状态,无法追踪中间过程。

### 4.2 为什么 maxColumnId 不回收?

**设计理念**: **安全性优先于空间效率**。

**如果回收 ID 会怎样?**
```
版本 1: column_a (id=5, type=int)
版本 2: 删除 column_a, maxColumnId 降为 4
版本 3: 添加 column_b (id=5, type=string), 复用 ID
```
读取版本 1 的文件时,id=5 会错误地映射到 column_b,导致类型不匹配。

**源码证据** (`AlterTableCommand`):
```java
newSchema.setMaxColumnId(oldSchema.getMaxColumnId)  // 删除列时保持不变
```

**权衡**: ID 空间是 int 类型(21 亿),即使每秒添加 1 个列,也能用 68 年。

### 4.3 为什么索引使用懒加载?

**设计理念**: **按需计算,减少序列化开销**。

**场景**: Spark Driver 构造 InternalSchema,序列化后发送给 1000 个 Executor。

**如果不懒加载**:
- 索引数据(idToField, nameToId, idToName)会被序列化
- 对于 1000 列的表,索引数据可能达到 MB 级别
- 1000 个 Executor 总共传输 GB 级别数据

**懒加载方案**:
- 只序列化 RecordType(结构数据)
- 每个 Executor 独立构建索引
- 总传输量减少 90%+

**源码证据** (`InternalSchema` 第 59 行):
```java
private transient Map<Integer, Field> idToField = null;  // transient 不序列化
```

### 4.4 为什么需要 nameToPosition 索引?

**设计理念**: **支持列顺序推断**。

**源码证据** (`InternalSchema.getNameToPosition` 第 267-272 行):
```java
public Map<String, Integer> getNameToPosition() {
    if (nameToPosition == null) {
        nameToPosition = InternalSchemaBuilder.getBuilder().buildNameToPosition(record);
    }
    return nameToPosition;
}
```

**用途**: 在 Schema 协调时,推断新列应插入的位置。

**示例**:
```
旧 Schema: [a, b, c]
新 Schema: [a, b, d, c]  // d 插入在 c 之前
```
通过 `nameToPosition`,可以推断出 d 的位置应该是 "BEFORE c"。

### 4.5 架构演进:从 Avro Schema 到 InternalSchema

**阶段 1: 直接使用 Avro Schema**
- 问题: 无法支持重命名,语义错误

**阶段 2: 引入 Column ID**
- 解决重命名问题,但需要维护 ID 分配机制

**阶段 3: 自建类型体系**
- 解耦 Avro 依赖,支持多引擎

**阶段 4: 优化索引构建**
- 懒加载减少序列化开销

**未来方向**:
- 支持更复杂的类型变更(如 struct -> map)
- 优化嵌套结构的 Schema Evolution 性能

---

### 2.1 为什么不直接使用 Avro Schema？

Hudi 底层数据格式使用 Avro/Parquet，Avro 本身也有 Schema Evolution 机制。那为什么 Hudi 要自建一套 InternalSchema？

**Avro Schema Evolution 的致命局限**：Avro 的 Schema 匹配完全依赖**字段名称**（name-based matching）。这带来了以下问题：

| 场景 | Avro 的行为 | 问题 |
|------|------------|------|
| 重命名列 | 找不到旧名称的字段，认为是新列 | 无法正确匹配已有数据 |
| 删除再添加同名列 | 直接匹配到旧数据 | 语义错误，本应是全新列 |
| 嵌套结构重命名 | 整个嵌套子树失去匹配 | 大量数据丢失 |

**Hudi 的解决方案**：基于 **Column ID** 的追踪机制。每个字段（无论顶层还是嵌套）都被分配一个全局唯一且不可变的整数 ID。Schema 变更操作（重命名、删除、类型变更等）始终通过 ID 来定位字段，而非名称。

**好处**：
1. 重命名列时，旧文件中的数据可以通过 ID 精确匹配到新列名
2. 删除列再添加同名列时，新列获得新的 ID，不会与旧列数据混淆
3. 嵌套结构（struct、array、map）中的字段同样拥有 ID，支持深层嵌套的 Schema Evolution

### 2.2 InternalSchema 类详解

源码位置：`hudi-common/.../internal/schema/InternalSchema.java`

```java
public class InternalSchema implements Serializable {
    private final RecordType record;       // 根记录类型，包含所有字段
    private int maxColumnId;               // 当前 Schema 中最大的 Column ID
    private long versionId;                // Schema 版本号（通常为 commit 时间戳）

    // 以下为懒加载的索引缓存
    private transient Map<Integer, Field> idToField = null;    // ID -> Field 映射
    private transient Map<String, Integer> nameToId = null;    // 全限定名 -> ID 映射
    private transient Map<Integer, String> idToName = null;    // ID -> 全限定名 映射
    private transient Map<String, Integer> nameToPosition = null; // 全限定名 -> 位置映射
}
```

**关键设计点**：

**1. versionId 与 commit 时间戳绑定**

```java
public InternalSchema setSchemaId(long versionId) {
    this.versionId = versionId;
    return this;
}
```

Schema 版本号使用 commit 时间戳作为 versionId，这样每个数据文件可以通过其 commit 时间直接定位到写入时使用的 Schema 版本。**好处**：无需额外的映射表来关联"文件 -> Schema 版本"，文件名中自带的 commit 时间就是 Schema 版本的索引键。

**2. maxColumnId 的递增机制**

每次添加新列时，新列的 ID 从 `maxColumnId + 1` 开始分配。这保证了 Column ID 的单调递增和全局唯一性。即使删除了某个 ID 的列，该 ID 也不会被复用。

```java
// TableChanges.ColumnAddChange 构造函数中
private ColumnAddChange(InternalSchema internalSchema) {
    super(internalSchema);
    this.nextId = internalSchema.getMaxColumnId() + 1;  // 新列从 maxId+1 开始
}
```

**3. 空 Schema 的哨兵值设计**

```java
private static final InternalSchema EMPTY_SCHEMA = new InternalSchema(-1L, RecordType.get());

public boolean isEmptySchema() {
    return versionId < 0;  // versionId < 0 表示空 Schema
}
```

使用 `versionId = -1` 作为空 Schema 的标识，避免使用 null，使得在整个代码中可以安全地调用方法而无需额外的 null 检查。

**4. 懒加载的索引缓存**

`idToField`、`nameToId`、`idToName` 等映射均使用懒加载（声明为 `transient`，在需要时构建）。**好处**：InternalSchema 在序列化传输（如 Spark Task 序列化到 Executor）时只传输结构数据，索引在需要时按需构建，减少了序列化开销。

### 2.3 Type 类型体系

源码位置：`hudi-common/.../internal/schema/Type.java` 和 `Types.java`

```
Type (接口)
  |
  +-- PrimitiveType (抽象类)
  |     +-- BooleanType, IntType, LongType, FloatType, DoubleType
  |     +-- DateType, TimeType, TimestampType
  |     +-- StringType, BinaryType, FixedType
  |     +-- DecimalBase (抽象类)
  |     |     +-- DecimalTypeBytes
  |     |     +-- DecimalTypeFixed
  |     |           +-- DecimalType
  |     +-- UUIDType, VectorType
  |     +-- TimeMillisType, TimestampMillisType
  |     +-- LocalTimestampMillisType, LocalTimestampMicrosType
  |
  +-- NestedType (抽象类)
        +-- RecordType  (struct 类型)
        +-- ArrayType   (数组类型)
        +-- MapType     (映射类型)
```

**为什么要自建类型体系而不复用 Avro 类型？**

1. **引擎无关性**：Avro 的类型系统与 Avro 库深度绑定，而 Hudi 需要同时支持 Spark、Flink 等多引擎，自建类型体系可以作为通用中间表示
2. **更精确的类型控制**：例如 `DecimalTypeFixed` 和 `DecimalTypeBytes` 区分了 Decimal 的底层存储方式（fixed bytes vs variable bytes），这在类型升级兼容性检查中至关重要
3. **简化 Schema Evolution 逻辑**：自建类型体系可以提供更清晰的类型层次，便于实现类型升级规则

### 2.4 Types.Field — 每个字段的完整描述

```java
public static class Field implements Serializable {
    private final boolean isOptional;  // 是否可为 null
    private final int id;              // 全局唯一的 Column ID
    private final String name;         // 字段名
    private final Type type;           // 字段类型
    private final String doc;          // 文档注释
    private final Object defaultValue; // 默认值（实验性功能）
}
```

每个 Field 都通过 `id` 而非 `name` 来唯一标识。这是 Hudi Schema Evolution 的基石。

### 2.5 嵌套类型中的 ID 分配

嵌套类型（Array、Map、Record）中的子字段同样拥有独立的 Column ID：

- **ArrayType**：element 子字段拥有自己的 `elementId`
- **MapType**：key 和 value 子字段分别拥有 `keyId` 和 `valueId`
- **RecordType**：每个 Field 都有自己的 `fieldId`

```java
// ArrayType 示例
public static ArrayType get(int elementId, boolean isOptional, Type elementType) {
    return new ArrayType(Field.get(elementId, isOptional, "element", elementType));
}

// MapType 示例
public static MapType get(int keyId, int valueId, Type keyType, Type valueType) {
    return new MapType(
        Field.get(keyId, "key", keyType),
        Field.get(valueId, "value", valueType));
}
```

**好处**：嵌套结构内部的 Schema 变更（如给 struct 类型中添加新字段）也能通过 ID 精确追踪。

### 2.6 InternalSchemaBuilder — 构建索引和遍历

源码位置：`hudi-common/.../internal/schema/InternalSchemaBuilder.java`

InternalSchemaBuilder 是一个单例工具类，提供了：

1. **buildNameToId / buildIdToName**：构建全限定名与 ID 的双向映射。使用 Visitor 模式遍历整个类型树，对嵌套字段的全限定名使用 `.` 分隔（如 `address.city`）

2. **buildIdToField**：构建 ID 到 Field 的映射

3. **index2Parents**：构建子字段 ID 到父字段 ID 的映射，用于位置变更操作中验证两个字段是否属于同一父结构

4. **refreshNewId**：为类型树中的所有字段重新分配 ID。这在添加复合类型列时使用 —— 新添加的嵌套结构中的所有子字段都需要分配新的 ID

```java
public Type refreshNewId(Type type, AtomicInteger nextId) {
    switch (type.typeId()) {
        case RECORD:
            // 先为当前层所有字段分配连续 ID
            int currentId = nextId.get();
            nextId.set(currentId + record.fields().size());
            // 再递归处理每个字段的子类型
            for (int i = 0; i < oldFields.size(); i++) {
                Type fieldType = refreshNewId(oldField.type(), nextId);
                internalFields.add(Types.Field.get(currentId++, oldField.isOptional(), 
                    oldField.name(), fieldType, oldField.doc()));
            }
            return Types.RecordType.get(internalFields);
        case ARRAY:
            int elementId = nextId.get();
            nextId.set(elementId + 1);
            Type elementType = refreshNewId(array.elementType(), nextId);
            return Types.ArrayType.get(elementId, array.isElementOptional(), elementType);
        case MAP:
            int keyId = nextId.get();
            int valueId = keyId + 1;
            nextId.set(keyId + 2);
            Type keyType = refreshNewId(map.keyType(), nextId);
            Type valueType = refreshNewId(map.valueType(), nextId);
            return Types.MapType.get(keyId, valueId, keyType, valueType, map.isValueOptional());
        default:
            return type;
    }
}
```

**为什么用广度优先分配 ID？** 当前层的字段先分配连续 ID，然后再递归处理子类型。这样同一层级的字段 ID 是连续的，便于快速定位和范围查询。

---

## 3. AvroSchemaEvolutionUtils 与 Schema 对齐算法

### 3.1 核心问题：Schema 对齐（Reconciliation）

当写入新数据时，incoming schema（新数据的 schema）可能与当前 table schema 不一致。Schema 对齐要解决的是：**如何将新数据的 schema 与已有的 table schema 合并，生成一个兼容的 evolved schema？**

### 3.2 AvroSchemaEvolutionUtils.reconcileSchema 算法

源码位置：`hudi-common/.../internal/schema/utils/AvroSchemaEvolutionUtils.java`

`reconcileSchema` 方法是 Hudi 在写入时自动演进 Schema 的核心算法。它处理四种情况：

```
情况 1：新数据缺少表中已有的列 -> 在 evolvedSchema 中保留该列（读取时填 NULL）
情况 2：新数据包含表中不存在的列 -> 添加到 evolvedSchema
情况 3：同时存在情况 1 和 2 -> 合并处理
情况 4：相同列名但类型不同 -> 尝试类型升级
```

**算法流程**：

```java
public static InternalSchema reconcileSchema(Schema incomingSchema, 
    InternalSchema oldTableSchema, boolean makeMissingFieldsNullable) {
    
    // Step 1: 将 incoming Avro Schema 转换为 InternalSchema
    InternalSchema inComingInternalSchema = convert(incomingSchema, 
        oldTableSchema.getNameToPosition());
    
    // Step 2: 计算差集
    List<String> diffFromOldSchema = ...; // 表中有但 incoming 中没有的列
    List<String> diffFromEvolutionColumns = ...; // incoming 中有但表中没有的列
    List<String> typeChangeColumns = ...; // 两边都有但类型不同的列
    
    // Step 3: 去重 - 对嵌套结构，只保留顶层父字段的添加操作
    // 例如添加 struct 类型 user(name, age)，只需要添加 user，
    // 不需要分别添加 user.name 和 user.age
    TreeMap<Integer, String> finalAddAction = new TreeMap<>();
    for (String name : diffFromEvolutionColumns) {
        String parentName = ...;
        if (!diffFromEvolutionColumns.contains(parentName)) {
            finalAddAction.put(id, name);
        }
    }
    
    // Step 4: 执行添加操作（带位置推断）
    TableChanges.ColumnAddChange addChange = TableChanges.ColumnAddChange.get(oldTableSchema);
    finalAddAction.forEach((id, name) -> {
        // 推断新列应插入的位置（BEFORE 第一个在两个 schema 中都存在的后续列）
        Optional<String> inferPosition = ...;
        addChange.addColumns(parentName, rawName, type, null);
        inferPosition.map(i -> addChange.addPositionChange(name, i, "before"));
    });
    
    // Step 5: 执行类型升级
    TableChanges.ColumnUpdateChange typeChange = ...;
    typeChangeColumns.forEach(col -> typeChange.updateColumnType(col, newType));
    
    // Step 6: 处理缺失列的 nullable 标记
    if (makeMissingFieldsNullable) {
        diffFromOldSchema.forEach(col -> typeChange.updateColumnNullability(col, true));
    }
    
    return evolvedSchema;
}
```

**关键设计决策**：

1. **位置推断**：新增列不是简单地追加到末尾，而是尝试推断其在原始 incoming schema 中的相对位置，保持列的逻辑顺序。具体做法是找到 incoming schema 中新列之后、且在 old schema 中也存在的第一个列，将新列插入到该列之前（BEFORE）。

2. **不支持隐式删除和重命名**：`reconcileSchema` 明确只处理 "missing"（缺失）和 "add"（新增）语义，不处理删除和重命名。这是因为自动推断"某列是被删除了还是只是数据中缺失"是不安全的。删除和重命名必须通过显式的 DDL 操作完成。

3. **TreeMap 保持顺序**：使用 `TreeMap<Integer, String>` 按 ID 排序来保证新增列的处理顺序稳定。

### 3.3 reconcileSchemaRequirements — 写入前的 Schema 协调

```java
public static Schema reconcileSchemaRequirements(Schema sourceSchema, 
    Schema targetSchema, boolean shouldReorderColumns) {
    // 调整 source schema 的 nullability 和数据类型以匹配 target schema
    // 例如：source 中 colA 是 required，target 中是 optional -> 输出为 optional
    // 例如：source 中 colC 是 int，target 中是 long -> 输出为 long（类型提升）
}
```

**为什么需要两个不同的 reconcile 方法？**
- `reconcileSchema`：用于自动演进 table schema（写入时发现新列自动添加）
- `reconcileSchemaRequirements`：用于协调 incoming data 与 table schema 的兼容性（确保写入的数据能被正确处理）

### 3.4 SchemaChangeUtils — 类型升级规则

源码位置：`hudi-common/.../internal/schema/utils/SchemaChangeUtils.java`

```java
public static boolean isTypeUpdateAllow(Type src, Type dst) {
    // 支持的升级路径：
    // INT    -> LONG, FLOAT, DOUBLE, STRING, DECIMAL
    // LONG   -> FLOAT, DOUBLE, STRING, DECIMAL
    // FLOAT  -> DOUBLE, STRING, DECIMAL
    // DOUBLE -> STRING, DECIMAL
    // DATE   -> STRING
    // BINARY -> STRING
    // STRING -> DATE, DECIMAL, BINARY
    // DECIMAL -> DECIMAL (precision/scale 只能扩大), STRING
}
```

**为什么只支持 "宽化" 类型变更？** 这是数据安全性的保障。类型宽化（widening）不会丢失精度，而窄化（narrowing，如 long -> int）可能导致数据截断或溢出。这与 Java 的隐式类型转换规则类似。

**Decimal 类型的特殊处理**：

```java
public boolean isWiderThan(PrimitiveType other) {
    if (other instanceof DecimalBase) {
        DecimalBase dt = (DecimalBase) other;
        return (precision - scale) >= (dt.precision - dt.scale) && scale > dt.scale;
    }
    if (other instanceof IntType) {
        return (precision - scale) >= 10 && scale > 0;
    }
    return false;
}
```

Decimal 的 `isWiderThan` 判断需要同时满足：
- 对于 DecimalBase：整数位不减少 `(precision - scale) >= (dt.precision - dt.scale)` AND 小数位严格增大 `scale > dt.scale`
- 对于 IntType：整数位至少 10 位 AND 小数位大于 0

在 `SchemaChangeUtils.isDecimalUpdateAllowInternalBase` 中，除了使用 `isWiderThan` 判断外，还额外允许 `precision >= src.precision && scale == src.scale` 的情况（即只扩大 precision 而 scale 不变）。此外，`DecimalTypeFixed` 还检查固定字节长度不能缩小。

---

## 4. TableSchemaResolver：Schema 版本推导

源码位置：`hudi-common/.../common/table/TableSchemaResolver.java`

### 4.1 核心职责

`TableSchemaResolver` 负责从 Hudi 表的元数据中推导出当前表的完整 Schema。它需要处理多个 Schema 来源的优先级：

```
Schema 来源优先级（从高到低）：
1. 最新 commit 的 CommitMetadata 中的 Schema
2. TableConfig 中的建表 Schema (hoodie.table.create.schema)
3. 数据文件中读取的 Schema
```

### 4.2 Schema 获取的多级回退策略

```java
private Option<HoodieSchema> getTableSchemaInternal(boolean includeMetadataFields, 
    Option<HoodieInstant> instantOpt) {
    Option<HoodieSchema> schema =
        // 优先从 commit metadata 获取
        (instantOpt.isPresent()
            ? getTableSchemaFromCommitMetadata(instantOpt.get(), includeMetadataFields)
            : getTableSchemaFromLatestCommitMetadata(includeMetadataFields))
        // 回退到 table config 中的建表 schema
        .or(() -> metaClient.getTableConfig().getTableCreateSchema()
            .map(tableSchema -> includeMetadataFields
                ? HoodieSchemaUtils.addMetadataFields(tableSchema, hasOperationField.get())
                : tableSchema))
        // 最终回退到从数据文件读取
        .or(() -> {
            Option<HoodieSchema> schemaFromDataFile = getTableSchemaFromDataFileInternal();
            return includeMetadataFields
                ? schemaFromDataFile
                : schemaFromDataFile.map(HoodieSchemaUtils::removeMetadataFields);
        });
    return schema;
}
```

**为什么需要多级回退？**

- 正常情况下，Schema 存储在 commit metadata 中，读取最快
- 对于新建表但还未写入数据的情况，只有建表时的 Schema 可用
- 对于非常早期版本创建的表，可能不存在 commit metadata 中的 Schema 信息，此时需要从数据文件（Parquet/ORC）中直接读取 Schema

### 4.3 InternalSchema 的获取

```java
public Option<InternalSchema> getTableInternalSchemaFromCommitMetadata() {
    HoodieTimeline completedInstants = metaClient.getActiveTimeline()
        .getCommitsTimeline().filterCompletedInstants();
    
    // 从最新 commit 开始倒序遍历，找到第一个能更新 schema 的 commit
    return Option.fromJavaOptional(completedInstants.getReverseOrderedInstants()
        .filter(instant -> {
            // 只有能更新 schema 的操作类型才会检查
            return WriteOperationType.canUpdateSchema(
                getCachedCommitMetadata(instant).getOperationType());
        })
        .findFirst())
        .flatMap(this::getTableInternalSchemaFromCommitMetadata);
}
```

**关键优化**：不是遍历所有 commit，而是从最新的 commit 开始**倒序**查找，找到第一个能更新 Schema 的 commit 就停止。这避免了在大量 commit 的表上进行全量扫描。

InternalSchema 存储在 CommitMetadata 中的 `latest_schema` 字段：

```java
private Option<InternalSchema> getTableInternalSchemaFromCommitMetadata(HoodieInstant instant) {
    HoodieCommitMetadata metadata = getCachedCommitMetadata(instant);
    String latestInternalSchemaStr = metadata.getMetadata(SerDeHelper.LATEST_SCHEMA);
    if (latestInternalSchemaStr != null) {
        return SerDeHelper.fromJson(latestInternalSchemaStr);
    }
    return Option.empty();
}
```

### 4.4 Schema 版本历史链

Schema 的完整版本历史存储在 `.hoodie/.schema/` 目录下的文件中，由 `FileBasedInternalSchemaStorageManager` 管理。

```java
// 持久化历史 Schema
public void persistHistorySchemaStr(String instantTime, String historySchemaStr) {
    cleanResidualFiles();  // 先清理残留文件
    HoodieActiveTimeline timeline = getMetaClient().getActiveTimeline();
    // 创建一个 schema commit instant
    HoodieInstant hoodieInstant = metaClient.createNewInstant(
        HoodieInstant.State.REQUESTED, SCHEMA_COMMIT_ACTION, instantTime);
    timeline.createNewInstant(hoodieInstant);
    // 写入历史 Schema 内容
    byte[] writeContent = getUTF8Bytes(historySchemaStr);
    timeline.transitionRequestedToInflight(hoodieInstant, Option.empty());
    timeline.saveAsComplete(false, ..., Option.of(writeContent));
}
```

**历史 Schema 的 JSON 格式**：

```json
{
  "schemas": [
    {
      "max_column_id": 10,
      "version_id": 20240101000000,
      "type": "record",
      "fields": [
        {"id": 0, "name": "id", "optional": false, "type": "int"},
        {"id": 1, "name": "name", "optional": true, "type": "string"}
      ]
    },
    {
      "max_column_id": 11,
      "version_id": 20240102000000,
      "type": "record",
      "fields": [
        {"id": 0, "name": "id", "optional": false, "type": "int"},
        {"id": 1, "name": "name", "optional": true, "type": "string"},
        {"id": 11, "name": "age", "optional": true, "type": "int"}
      ]
    }
  ]
}
```

**新 Schema 的追加采用字符串拼接优化**：

```java
public static String inheritSchemas(InternalSchema newSchema, String oldSchemas) {
    // 不解析整个 JSON 再重新序列化，而是直接做字符串拼接
    String checkedString = "{\"schemas\":[";
    String oldSchemasSuffix = oldSchemas.substring(checkedString.length());
    return checkedString + toJson(newSchema) + "," + oldSchemasSuffix;
}
```

**好处**：避免了反复解析和序列化大量历史 Schema 的 JSON 开销。新 Schema 直接插入到 JSON 数组的头部。

### 4.5 InternalSchemaCache — 全局缓存

源码位置：`hudi-common/.../common/util/InternalSchemaCache.java`

```java
public class InternalSchemaCache {
    // 分段锁减少竞争
    private static final Object[] LOCK_LIST = new Object[16];
    
    // 全局缓存：tablePath -> (versionId -> InternalSchema)
    private static final Cache<String, TreeMap<Long, InternalSchema>>
        HISTORICAL_SCHEMA_CACHE = Caffeine.newBuilder()
            .maximumSize(1000).weakValues().build();
    
    public static InternalSchema searchSchemaAndCache(long versionID, 
        HoodieTableMetaClient metaClient) {
        String tablePath = metaClient.getBasePath().toString();
        // 分段锁：使用 tablePath 的 hash 来选择锁
        synchronized (LOCK_LIST[tablePath.hashCode() & (LOCK_LIST.length - 1)]) {
            TreeMap<Long, InternalSchema> historicalSchemas = 
                HISTORICAL_SCHEMA_CACHE.getIfPresent(tablePath);
            if (historicalSchemas == null || 
                InternalSchemaUtils.searchSchema(versionID, historicalSchemas) == null) {
                historicalSchemas = getHistoricalSchemas(metaClient);
                HISTORICAL_SCHEMA_CACHE.put(tablePath, historicalSchemas);
            }
            return InternalSchemaUtils.searchSchema(versionID, historicalSchemas);
        }
    }
}
```

**设计亮点**：

1. **分段锁**：16 个锁对象，通过表路径 hash 选锁，避免所有表操作竞争同一把锁
2. **弱引用值**：使用 Caffeine Cache 的 `weakValues()`，当内存紧张时可以自动回收缓存的 Schema
3. **版本搜索**：使用 `TreeMap` 的有序特性，对于未精确命中的版本，回退到该版本之前最近的 Schema（`headMap(versionId).lastKey()`）

---

## 5. 支持的 Schema 变更操作

### 5.1 操作总览

| 操作 | 类 | 约束条件 |
|------|-----|---------|
| ADD COLUMN | `ColumnAddChange` | 不能添加已存在的列；嵌套添加时父字段必须是 RecordType |
| DROP COLUMN | `ColumnDeleteChange` | 不能删除不存在的列；不能删除 Hudi 元数据列 |
| RENAME COLUMN | `ColumnUpdateChange` | 新名称不能与已有列冲突；不能重命名元数据列 |
| TYPE CHANGE | `ColumnUpdateChange` | 只支持安全的类型宽化；不能将嵌套类型更改为原始类型 |
| REORDER | `ColumnUpdateChange/ColumnAddChange` | 只能在同一父结构内调整顺序；不能跨层级移动 |

### 5.2 ADD COLUMN 详解

源码位置：`hudi-common/.../internal/schema/action/TableChanges.java` 中的 `ColumnAddChange` 类

```java
public ColumnAddChange addColumns(String parent, String name, Type type, String doc) {
    // 1. 验证：如果指定了 parent，parent 必须存在且为 RecordType
    if (!parent.isEmpty()) {
        Types.Field parentField = internalSchema.findField(parent);
        if (parentField == null) {
            throw new HoodieSchemaException("parent column does not exist");
        }
        if (!(parentField.type() instanceof Types.RecordType)) {
            throw new HoodieSchemaException("can only add to struct types");
        }
    }
    
    // 2. 验证：列名不能重复
    if (internalSchema.hasColumn(name, caseSensitive)) {
        throw new HoodieSchemaException("column already exists");
    }
    
    // 3. 为新列及其子字段分配 ID（从 nextId 开始）
    AtomicInteger assignNextId = new AtomicInteger(nextId + 1);
    Type typeWithNewId = InternalSchemaBuilder.getBuilder()
        .refreshNewId(type, assignNextId);
    
    // 4. 新增列强制为 optional（可空）
    Types.Field newField = Types.Field.get(nextId, true, name, typeWithNewId, doc);
    // 记录到 parentId2AddCols 映射中
    // ...
}
```

**关键约束 —— 新增列强制为 optional**：

```java
Types.Field newField = Types.Field.get(nextId, true, name, typeWithNewId, doc);
//                                      ^^^^ 强制为 true（optional）
```

**为什么？** 因为历史已有的数据文件不包含新增的列，读取这些文件时新列必须允许为 NULL。如果允许新增 required 列，那些历史文件就无法满足约束。

### 5.3 DROP COLUMN 详解

```java
public ColumnDeleteChange deleteColumn(String name) {
    checkColModifyIsLegal(name);  // 不允许删除 Hudi 元数据列
    Types.Field field = internalSchema.findField(name);
    if (field == null) {
        throw new SchemaCompatibilityException("column does not exist");
    }
    deletes.add(field.fieldId());  // 记录要删除的 Column ID
    return this;
}
```

**删除时的安全检查**：

1. **禁止删除 Hudi 元数据列**：`_hoodie_commit_time`、`_hoodie_commit_seqno` 等
2. **禁止删除 primaryKey / orderingFields / partitionKey**：在 Spark 的 `AlterTableCommand.checkSchemaChange` 中检查
3. **禁止删除 Struct 的所有子字段**：SchemaChangeUtils 中检查 `if (fields.isEmpty()) throw`
4. **禁止删除 Array 的 element 或 Map 的 key/value**

**重要**：删除操作不改变 `maxColumnId`：

```java
// AlterTableCommand 中
val newSchema = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, deleteChange)
newSchema.setMaxColumnId(oldSchema.getMaxColumnId)  // 保持不变
```

**为什么？** 删除列不应影响 ID 分配。如果删除后降低了 maxColumnId，后续添加的新列可能复用已删除列的 ID，导致读取历史文件时数据错乱。

### 5.4 RENAME COLUMN 详解

```java
public ColumnUpdateChange renameColumn(String name, String newName) {
    checkColModifyIsLegal(name);
    Types.Field field = internalSchema.findField(name);
    // 验证列存在
    if (field == null) { throw ... }
    // 验证新名称不为空
    if (newName == null || newName.isEmpty()) { throw ... }
    // 验证新名称不与已有列冲突
    if (internalSchema.hasColumn(newName, caseSensitive)) { throw ... }
    
    // 只更新名称，保留原来的 ID、类型、doc
    updates.put(field.fieldId(), Types.Field.get(
        field.fieldId(), field.isOptional(), newName, field.type(), field.doc()));
}
```

**为什么重命名能正确工作？** 因为 Column ID 不变。当读取旧文件时，通过 Column ID 找到字段（此时名称还是旧的），然后映射到新名称。`InternalSchemaMerger` 中的 `dealWithRename` 方法专门处理这种情况。

### 5.5 TYPE CHANGE 详解

```java
public ColumnUpdateChange updateColumnType(String name, Type newType) {
    // 1. 不允许更改为嵌套类型
    if (newType.isNestedType()) {
        throw new SchemaCompatibilityException("Cannot update to nested type");
    }
    
    // 2. 检查类型升级是否允许
    if (!SchemaChangeUtils.isTypeUpdateAllow(field.type(), newType)) {
        throw new SchemaCompatibilityException("Incompatible type change");
    }
    
    // 3. 如果类型相同，不做操作
    if (field.type().equals(newType)) { return this; }
    
    // 4. 保存更新信息
    updates.put(field.fieldId(), Types.Field.get(
        field.fieldId(), field.isOptional(), field.name(), newType, field.doc()));
}
```

**完整的类型升级兼容矩阵**：

```
         -> INT  LONG  FLOAT  DOUBLE  STRING  DECIMAL  DATE  BINARY
INT      |  =    Y     Y      Y       Y       Y        -     -
LONG     |  -    =     Y      Y       Y       Y        -     -
FLOAT    |  -    -     =      Y       Y       Y        -     -
DOUBLE   |  -    -     -      =       Y       Y        -     -
STRING   |  -    -     -      -       =       Y        Y     Y
DECIMAL  |  -    -     -      -       Y       *        -     -
DATE     |  -    -     -      -       Y       -        =     -
BINARY   |  -    -     -      -       Y       -        -     =

Y = 允许   - = 不允许   = = 相同类型   * = Decimal 升级有额外约束
```

### 5.6 REORDER 详解

```java
public InternalSchema applyReOrderColPositionChange(
    String colName, String referColName,
    ColumnPositionChange.ColumnPositionType positionType) {
    
    String parentName = TableChangesHelper.getParentName(colName);
    String referParentName = TableChangesHelper.getParentName(referColName);
    
    if (positionType.equals(ColumnPositionType.FIRST)) {
        updateChange.addPositionChange(colName, "", positionType);
    } else if (parentName.equals(referParentName)) {
        // 只有同一父结构下的字段才能互相参照位置
        updateChange.addPositionChange(colName, referColName, positionType);
    } else {
        throw new IllegalArgumentException(
            "cannot reorder columns with different parent");
    }
}
```

位置变更支持三种类型：
- **FIRST**：移到同层第一个位置
- **AFTER**：移到指定列之后
- **BEFORE**：移到指定列之前

**安全约束**：
- 不允许在元数据列之间插入普通列
- 不允许跨层级移动（嵌套结构内的字段不能移出去）
- 不允许对顶层列使用 FIRST 语法

---

## 6. 读时 Schema 对齐（FileGroupReaderSchemaHandler）

### 6.1 核心问题

当读取一个 file group 时，其中的数据文件可能使用不同版本的 Schema 写入。`FileGroupReaderSchemaHandler` 负责：

1. 确定每个文件的实际 Schema
2. 将文件 Schema 与查询 Schema 合并，生成适用于读取该文件的 merged schema
3. 处理列名映射（重命名场景）
4. 处理类型转换（类型变更场景）

### 6.2 FileGroupReaderSchemaHandler 架构

源码位置：`hudi-common/.../common/table/read/FileGroupReaderSchemaHandler.java`

```java
public class FileGroupReaderSchemaHandler<T> {
    protected final HoodieSchema tableSchema;      // 当前表 Schema
    protected final HoodieSchema requestedSchema;   // 用户请求的 Schema（列裁剪后）
    protected final HoodieSchema requiredSchema;    // 实际需要读取的 Schema（含 merge 所需额外列）
    protected final InternalSchema internalSchema;  // 裁剪后的 InternalSchema
    protected final Option<InternalSchema> internalSchemaOpt; // 完整 InternalSchema
}
```

**三个 Schema 的区别**：

- `requestedSchema`：用户查询指定的列（如 `SELECT name, age FROM table`）
- `requiredSchema`：在 requestedSchema 基础上，补充了 merge 所需的额外列（如 record key、precombine field 等）
- `tableSchema`：表的完整 Schema，包含所有列

### 6.3 为每个文件生成读取 Schema

```java
public Pair<HoodieSchema, Map<String, String>> getRequiredSchemaForFileAndRenamedColumns(
    StoragePath path) {
    if (internalSchema.isEmptySchema()) {
        // Schema Evolution 未启用，直接使用 requiredSchema
        return Pair.of(requiredSchema, Collections.emptyMap());
    }
    
    // 1. 从文件名解析 commit 时间，定位文件的 Schema 版本
    long commitInstantTime = Long.parseLong(FSUtils.getCommitTime(path.getName()));
    InternalSchema fileSchema = InternalSchemaCache.searchSchemaAndCache(
        commitInstantTime, metaClient);
    
    // 2. 合并 fileSchema 和 querySchema (internalSchema)
    Pair<InternalSchema, Map<String, String>> mergedInternalSchema = 
        new InternalSchemaMerger(fileSchema, internalSchema,
            true,    // ignoreRequiredAttribute
            false,   // useColumnTypeFromFileSchema = false
            false    // useColNameFromFileSchema = false
        ).mergeSchemaGetRenamed();
    
    // 3. 转换为 HoodieSchema
    HoodieSchema mergedAvroSchema = InternalSchemaConverter.convert(
        mergedInternalSchema.getLeft(), requiredSchema.getFullName());
    
    return Pair.of(mergedAvroSchema, mergedInternalSchema.getRight());
}
```

### 6.4 InternalSchemaMerger — Schema 合并的核心引擎

源码位置：`hudi-common/.../internal/schema/action/InternalSchemaMerger.java`

InternalSchemaMerger 是读取时 Schema 对齐的核心类。它接收两个参数：
- `fileSchema`：文件实际写入时的 Schema
- `querySchema`：当前查询需要的 Schema（已根据最新 table schema 裁剪）

**三个关键布尔参数**：

```java
// 是否忽略 required 属性（解决 Spark 某些操作错误将 optional 改为 required 的 bug）
private final boolean ignoreRequiredAttribute;

// 是否使用文件 Schema 的类型（Parquet Reader 需要文件原始类型）
private boolean useColumnTypeFromFileSchema = true;

// 是否使用文件 Schema 的列名（Parquet Reader 需要文件原始列名）
private boolean useColNameFromFileSchema = true;
```

**为什么需要 `useColumnTypeFromFileSchema`？**

Parquet Reader 必须使用文件中实际存储的类型来读取数据。例如，某列从 INT 升级为 LONG，但旧文件中仍然是 INT。如果传入 LONG 类型给 Parquet Reader，读取会失败。因此，生成给 Parquet Reader 的 Schema 必须使用文件的原始类型，读出数据后再进行类型转换。

**为什么需要 `useColNameFromFileSchema`？**

类似地，如果列被重命名，Parquet 文件中的列名是旧名称。Parquet Reader 按列名匹配，必须使用旧名称才能读到数据。

### 6.5 合并算法的四种情况处理

```java
private List<Types.Field> buildRecordType(List<Types.Field> oldFields, List<Type> newTypes) {
    for (int i = 0; i < newTypes.size(); i++) {
        int fieldId = oldField.fieldId();
        String fullName = querySchema.findFullName(fieldId);
        
        if (fileSchema.findField(fieldId) != null) {
            if (fileSchema.findFullName(fieldId).equals(fullName)) {
                // 情况 1：字段存在且名称未变 -> 可能需要处理类型变更
                newFields.add(Types.Field.get(fieldId, ..., newType, ...));
            } else {
                // 情况 2：字段存在但名称不同 -> 处理重命名
                newFields.add(dealWithRename(fieldId, newType, oldField));
            }
        } else {
            // 情况 3 & 4：字段在文件中不存在
            String normalizedName = normalizeFullName(fullName);
            if (fileSchema.findField(normalizedName) != null) {
                // 情况 3：名称冲突（文件中有同名但不同 ID 的列）
                // 添加 "suffix" 后缀避免冲突
                newFields.add(Types.Field.get(fieldId, ..., name + "suffix", ...));
            } else {
                // 情况 4：新增列（文件中完全不存在）
                // 保持新列定义，读取时自动填充 NULL
                newFields.add(Types.Field.get(fieldId, true, name, newType, ...));
            }
        }
    }
}
```

**新列填充 NULL 的机制**：

当一个列在查询 Schema 中存在但在文件 Schema 中不存在时，该列在 merged schema 中仍然保留（且标记为 optional）。Parquet Reader 在读取文件时发现该列在文件中不存在，自动返回 NULL 值。这是 Parquet 格式本身的能力 —— 如果读取 Schema 中包含文件中不存在的列，该列的值为 null。

**删除列的跳过机制**：

如果某列在文件 Schema 中存在但在查询 Schema 中不存在（即该列已被删除），那么 merged schema 中不会包含该列。Parquet Reader 只读取 merged schema 中指定的列，自动跳过已删除的列。

### 6.6 requiredSchema 的构建

```java
HoodieSchema generateRequiredSchema(DeleteContext deleteContext) {
    // 1. 如果没有 log 文件，只需考虑 instant range 过滤
    if (!readerContext.getHasLogFiles()) {
        if (hasInstantRange && !requestedSchema.contains(COMMIT_TIME_FIELD)) {
            return appendField(requestedSchema, COMMIT_TIME_FIELD);
        }
        return requestedSchema;
    }
    
    // 2. 有 log 文件时，需要添加 merge 所需的额外列
    List<String> mandatoryFields = getMandatoryFieldsForMerging(...);
    // 包括：record key、precombine field、_hoodie_is_deleted 等
    
    for (String field : mandatoryFields) {
        if (!requestedSchema.contains(field)) {
            addedFields.add(getField(tableSchema, field));
        }
    }
    
    return appendFields(requestedSchema, addedFields);
}
```

**为什么 MOR 表需要额外列？** MOR 表的 file group 包含 base file 和 log files，读取时需要将两者合并（merge）。合并需要 record key 来匹配记录，需要 precombine field 来决定哪条记录更新，需要 `_hoodie_is_deleted` 来处理删除标记。

---

## 7. 写时 Schema 兼容性检查

### 7.1 写入流程中的 Schema Evolution 触发点

在 `BaseHoodieWriteClient` 中，Schema Evolution 发生在 commit 阶段：

```java
private void saveInternalSchema(HoodieTable table, String instantTime, 
    HoodieCommitMetadata metadata) {
    
    TableSchemaResolver schemaUtil = new TableSchemaResolver(table.getMetaClient());
    String historySchemaStr = schemaUtil.getTableHistorySchemaStrFromCommitMetadata()
        .orElse("");
    
    // 条件：历史 Schema 存在 OR 启用了 Schema 协调
    if (!historySchemaStr.isEmpty() || 
        Boolean.parseBoolean(config.getString(RECONCILE_SCHEMA.key()))) {
        
        InternalSchema internalSchema;
        HoodieSchema schema = createHoodieWriteSchema(config.getSchema(), ...);
        
        if (historySchemaStr.isEmpty()) {
            // 首次启用 Schema Evolution：从配置中获取或从 Avro Schema 转换
            internalSchema = SerDeHelper.fromJson(config.getInternalSchema())
                .orElseGet(() -> InternalSchemaConverter.convert(schema));
            internalSchema.setSchemaId(Long.parseLong(instantTime));
        } else {
            // 已有历史 Schema：搜索当前版本
            internalSchema = InternalSchemaUtils.searchSchema(
                Long.parseLong(instantTime), 
                SerDeHelper.parseSchemas(historySchemaStr));
        }
        
        // 核心：调用 reconcileSchema 进行 Schema 对齐
        InternalSchema evolvedSchema = AvroSchemaEvolutionUtils.reconcileSchema(
            schema.toAvroSchema(), internalSchema, 
            config.getBooleanOrDefault(SET_NULL_FOR_MISSING_COLUMNS));
        
        if (evolvedSchema.equals(internalSchema)) {
            // Schema 未变化：直接保存当前 Schema
            metadata.addMetadata(LATEST_SCHEMA, SerDeHelper.toJson(evolvedSchema));
            schemasManager.persistHistorySchemaStr(instantTime, ...);
        } else {
            // Schema 发生变化：设置新版本号并保存
            evolvedSchema.setSchemaId(Long.parseLong(instantTime));
            metadata.addMetadata(LATEST_SCHEMA, SerDeHelper.toJson(evolvedSchema));
            schemasManager.persistHistorySchemaStr(instantTime, 
                SerDeHelper.inheritSchemas(evolvedSchema, historySchemaStr));
        }
        
        // 同步更新 SCHEMA_KEY（Avro 格式的 Schema）
        metadata.addMetadata(SCHEMA_KEY, 
            InternalSchemaConverter.convert(evolvedSchema, name).toString());
    }
}
```

### 7.2 reconcileSchema 的安全保障

`reconcileSchema` 方法在处理 Schema 变更时有多层安全保障：

1. **不做隐式删除**：如果 incoming schema 缺少某列，只是将该列标记为 nullable，而非删除。删除必须通过 DDL 显式操作
2. **类型升级验证**：只允许安全的宽化操作，不允许缩窄
3. **不做隐式重命名**：列名匹配只用精确匹配，不会猜测 "这个列是不是重命名了"
4. **幂等性**：如果 evolvedSchema 与 oldTableSchema 结构相同（ignoring version），返回旧 Schema 保持一致性

```java
// 幂等性保障
if (evolvedSchema.equalsIgnoringVersion(oldTableSchema)) {
    return oldTableSchema;
}
return evolvedSchema;
```

### 7.3 RECONCILE_SCHEMA 配置

```java
// HoodieCommonConfig.java
@Deprecated
public static final ConfigProperty<Boolean> RECONCILE_SCHEMA = ConfigProperty
    .key("hoodie.datasource.write.reconcile.schema")
    .defaultValue(false)
    .markAdvanced()
    .deprecatedAfter("0.14.1")
    .withDocumentation("This config controls how writer's schema will be selected based on the incoming batch's "
        + "schema as well as existing table's one. When schema reconciliation is DISABLED, incoming batch's "
        + "schema will be picked as a writer-schema (therefore updating table's schema). When schema reconciliation "
        + "is ENABLED, writer-schema will be picked such that table's schema (after txn) is either kept the same "
        + "or extended, meaning that we'll always prefer the schema that either adds new columns or stays the same. "
        + "This enables us, to always extend the table's schema during evolution and never lose the data (when, for "
        + "ex, existing column is being dropped in a new batch)");
```

当设置 `hoodie.datasource.write.reconcile.schema=true` 时，即使没有历史 Schema Evolution 记录，也会启动 Schema 对齐流程。这是从"非 Schema Evolution 模式"迁移到"Schema Evolution 模式"的入口。

**注意**：此配置在 0.14.1 版本后已被标记为 `@Deprecated`。

### 7.4 SET_NULL_FOR_MISSING_COLUMNS 配置

```java
public static final ConfigProperty<String> SET_NULL_FOR_MISSING_COLUMNS = ConfigProperty
    .key("hoodie.write.set.null.for.missing.columns")
    .defaultValue("false")
    .markAdvanced()
    .sinceVersion("0.14.1")
    .withDocumentation("When a nullable column is missing from incoming batch during a write operation, the write "
        + " operation will fail schema compatibility check. Set this option to true will make the missing "
        + " column be filled with null values to successfully complete the write operation.");
```

当 incoming schema 缺少表中已有的列时，如果此配置为 true，缺失的列会被标记为 nullable。这在 Schema 频繁变化的流式写入场景中非常有用。

---

## 8. 与 Spark/Flink 的 Schema Evolution 集成

### 8.1 Spark 集成架构

Hudi 的 Spark Schema Evolution 集成通过 Spark SQL 扩展机制实现：

```
Spark SQL Parser
      |
      v
Spark Catalyst Optimizer (逻辑计划优化)
      |
      v
Spark35ResolveHudiAlterTableCommand (Hudi 自定义规则)
      |  拦截 AddColumns/DropColumns/RenameColumn/AlterColumn
      v
HudiAlterTableCommand (Hudi DDL 命令)
      |
      v
InternalSchema 变更操作
      |
      v
commitWithSchema（提交新 Schema 到时间线）
```

### 8.2 Spark ALTER TABLE 命令的拦截

源码位置：`hudi-spark3.5.x/.../Spark35ResolveHudiAlterTableCommand.scala`

```scala
class Spark35ResolveHudiAlterTableCommand(sparkSession: SparkSession) 
    extends Rule[LogicalPlan] {
    
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (ProvidesHoodieConfig.isSchemaEvolutionEnabled(sparkSession)) {
      plan.resolveOperatorsUp {
        // ALTER TABLE ... ADD COLUMNS -> ColumnChangeID.ADD
        case add@AddColumns(ResolvedHoodieV2TablePlan(t), _) if add.resolved =>
          HudiAlterTableCommand(t.v1Table, add.changes, ColumnChangeID.ADD)
        
        // ALTER TABLE ... DROP COLUMNS -> ColumnChangeID.DELETE
        case drop@DropColumns(ResolvedHoodieV2TablePlan(t), _, _) if drop.resolved =>
          HudiAlterTableCommand(t.v1Table, drop.changes, ColumnChangeID.DELETE)
        
        // ALTER TABLE ... RENAME COLUMN -> ColumnChangeID.UPDATE
        case rename@RenameColumn(ResolvedHoodieV2TablePlan(t), _, _) =>
          HudiAlterTableCommand(t.v1Table, rename.changes, ColumnChangeID.UPDATE)
        
        // ALTER TABLE ... ALTER COLUMN (type/comment/nullability/position)
        case alter@AlterColumn(ResolvedHoodieV2TablePlan(t), ...) =>
          HudiAlterTableCommand(t.v1Table, alter.changes, ColumnChangeID.UPDATE)
      }
    } else {
      plan  // Schema Evolution 未启用，不拦截
    }
  }
}
```

**为什么需要对不同 Spark 版本做适配？** Spark 3.3、3.4、3.5、4.0 的 Catalyst 逻辑计划节点有 API 差异（如 `AlterColumn` 的构造函数参数不同），因此每个 Spark 版本都有对应的 `ResolveHudiAlterTableCommand` 实现。

### 8.3 AlterTableCommand 的 Spark -> Hudi 映射

源码位置：`hudi-spark-common/.../command/AlterTableCommand.scala`

**ADD COLUMN 的映射**：

```scala
def applyAddAction2Schema(sparkSession: SparkSession, 
    oldSchema: InternalSchema, addChanges: Seq[AddColumn]): InternalSchema = {
    val addChange = TableChanges.ColumnAddChange.get(oldSchema)
    addChanges.foreach { addColumn =>
        val names = addColumn.fieldNames()
        val parentName = AlterTableCommand.getParentName(names)
        // Spark 类型 -> Hudi InternalSchema 类型
        val colType = SparkInternalSchemaConverter.buildTypeFromStructType(
            addColumn.dataType(), true, new AtomicInteger(0))
        addChange.addColumns(parentName, names.last, colType, addColumn.comment())
        
        // 处理 Spark 的位置指定（AFTER/FIRST）
        addColumn.position() match {
            case after: TableChange.After =>
                addChange.addPositionChange(names.mkString("."), 
                    after.column(), "after")
            case _: TableChange.First =>
                addChange.addPositionChange(names.mkString("."), "", "first")
            case _ => // 默认追加到末尾
        }
    }
    SchemaChangeUtils.applyTableChanges2Schema(oldSchema, addChange)
}
```

**UPDATE（类型变更/重命名/nullability 变更）的映射**：

```scala
def applyUpdateAction(sparkSession: SparkSession): Unit = {
    val updateChange = TableChanges.ColumnUpdateChange.get(oldSchema)
    changes.foreach {
        case updateType: TableChange.UpdateColumnType =>
            val newType = SparkInternalSchemaConverter.buildTypeFromStructType(...)
            updateChange.updateColumnType(colName, newType)
        
        case updateName: TableChange.RenameColumn =>
            checkSchemaChange(Seq(originalColName), table)  // 安全检查
            updateChange.renameColumn(originalColName, updateName.newName())
        
        case updateNullAbility: TableChange.UpdateColumnNullability =>
            updateChange.updateColumnNullability(colName, nullable)
        
        case updatePosition: TableChange.UpdateColumnPosition =>
            updateChange.addPositionChange(colName, referCol, "after"/"first")
    }
    val newSchema = SchemaChangeUtils.applyTableChanges2Schema(oldSchema, updateChange)
}
```

### 8.4 commitWithSchema — DDL 变更的提交

DDL 操作通过一个特殊的空 commit 来记录 Schema 变更：

```scala
def commitWithSchema(internalSchema: InternalSchema, historySchemaStr: String, 
    table: CatalogTable, sparkSession: SparkSession): Unit = {
    
    // 1. 将 InternalSchema 转换为 HoodieSchema
    val schema = InternalSchemaConverter.convert(internalSchema, recordName)
    
    // 2. 创建 HoodieWriteClient
    val client = DataSourceUtils.createHoodieClient(...)
    
    // 3. 创建 ALTER_SCHEMA 类型的 commit
    val commitActionType = CommitUtils.getCommitActionType(
        WriteOperationType.ALTER_SCHEMA, tableType)
    val instantTime = client.startCommit(commitActionType)
    
    // 4. 将 InternalSchema 存入 commit 的 extraMetadata
    val extraMeta = new HashMap[String, String]()
    extraMeta.put(SerDeHelper.LATEST_SCHEMA, 
        SerDeHelper.toJson(internalSchema.setSchemaId(instantTime.toLong)))
    
    // 5. 持久化历史 Schema 链
    val schemaManager = new FileBasedInternalSchemaStorageManager(metaClient)
    schemaManager.persistHistorySchemaStr(instantTime, 
        SerDeHelper.inheritSchemas(internalSchema, historySchemaStr))
    
    // 6. 提交（空 RDD，无数据写入）
    client.commit(instantTime, jsc.emptyRDD, Option.of(extraMeta))
    
    // 7. 同步到 Hive Metastore
    val dataSparkSchema = SparkInternalSchemaConverter
        .constructSparkSchemaFromInternalSchema(internalSchema)
    alterTableDataSchema(sparkSession, db, tableName, dataSparkSchema)
}
```

**为什么用空 commit？** DDL 操作不写入数据，但需要在时间线上记录 Schema 变更。空 commit 使得 Schema 变更与数据写入共享同一套时间线机制，保证了一致性和原子性。

### 8.5 Spark 读取时的 Schema Evolution

源码位置：`hudi-spark-common/.../parquet/ParquetSchemaEvolutionUtils.scala`

```scala
class ParquetSchemaEvolutionUtils(...) {
    // 1. 获取文件对应版本的 InternalSchema
    private lazy val fileSchema: InternalSchema = {
        val commitInstantTime = FSUtils.getCommitTime(filePath.getName).toLong
        InternalSchemaCache.getInternalSchemaByVersionId(
            commitInstantTime, tablePath, storage, validCommits, layout)
    }
    
    // 2. 合并 fileSchema 和 querySchema
    def getHadoopConfClone(footerFileMetaData, enableVectorizedReader): Configuration = {
        val mergedInternalSchema = new InternalSchemaMerger(
            fileSchema, querySchemaOption.get(), true, true).mergeSchema()
        val mergedSchema = SparkInternalSchemaConverter
            .constructSparkSchemaFromInternalSchema(mergedInternalSchema)
        
        // 将 merged schema 设置为 Parquet Reader 的请求 schema
        hadoopAttemptConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, 
            mergedSchema.json)
        
        // 收集需要类型转换的列
        typeChangeInfos = SparkInternalSchemaConverter
            .collectTypeChangedCols(querySchemaOption.get(), mergedInternalSchema)
    }
    
    // 3. 重建过滤条件中的列名（处理重命名）
    def rebuildFilterFromParquet(filter: Filter): Filter = {
        // 如果列被重命名，将过滤条件中的新列名替换为文件中的旧列名
        // 如果列是新增的（文件中不存在），该过滤条件变为 AlwaysTrue
    }
}
```

### 8.6 Flink 集成

源码位置：`hudi-flink-datasource/.../table/format/InternalSchemaManager.java`

Flink 的 Schema Evolution 集成思路与 Spark 类似，但有自己的特点：

```java
public class InternalSchemaManager implements Serializable {
    
    public static InternalSchemaManager get(StorageConfiguration<?> conf, 
        HoodieTableMetaClient metaClient) {
        // 检查是否启用 Schema Evolution
        if (!isSchemaEvolutionEnabled(conf)) {
            return DISABLED;
        }
        // 获取最新的 InternalSchema
        Option<InternalSchema> internalSchema = new TableSchemaResolver(metaClient)
            .getTableInternalSchemaFromCommitMetadata();
        if (!internalSchema.isPresent() || internalSchema.get().isEmptySchema()) {
            return DISABLED;
        }
        return new InternalSchemaManager(conf, internalSchema.get(), ...);
    }
    
    // 获取文件的 merged schema
    InternalSchema getMergeSchema(String fileName) {
        long commitInstantTime = Long.parseLong(FSUtils.getCommitTime(fileName));
        InternalSchema fileSchema = InternalSchemaCache.getInternalSchemaByVersionId(
            commitInstantTime, tablePath, storage, validCommits, layout, tableConfig);
        if (querySchema.equals(fileSchema)) {
            return InternalSchema.getEmptyInternalSchema(); // 无需合并
        }
        return new InternalSchemaMerger(fileSchema, querySchema, true, true)
            .mergeSchema();
    }
    
    // 获取类型变更映射 (用于 Flink 的类型转换)
    CastMap getCastMap(InternalSchema mergeSchema, 
        String[] queryFieldNames, DataType[] queryFieldTypes, int[] selectedFields) {
        // 对比 mergeSchema 和 querySchema，找出类型不一致的列
        // 构建 CastMap 用于 Flink 运行时的类型转换
        Map<Integer, Pair<Type, Type>> changedCols = 
            InternalSchemaUtils.collectTypeChangedCols(querySchema, mergeSchema);
        // ...
    }
    
    // 获取重命名映射
    String[] getMergeFieldNames(InternalSchema mergeSchema, String[] queryFieldNames) {
        Map<String, String> renamedCols = InternalSchemaUtils
            .collectRenameCols(mergeSchema, querySchema);
        return Arrays.stream(queryFieldNames)
            .map(name -> renamedCols.getOrDefault(name, name))
            .toArray(String[]::new);
    }
}
```

**Flink 与 Spark 的关键差异**：

| 方面 | Spark | Flink |
|------|-------|-------|
| DDL 入口 | 通过 Catalyst Rule 拦截 | 通过 Flink Table API |
| Schema Evolution 启用 | Spark SessionExtension | StorageConfiguration 配置 |
| 类型转换 | 通过 UnsafeProjection | 通过 CastMap |
| 列名映射 | rebuildFilterFromParquet | getMergeFieldNames |
| 缓存策略 | 共用 InternalSchemaCache | 共用 InternalSchemaCache |

---

## 9. 与 Iceberg/Delta 的 Schema Evolution 对比

### 9.1 核心实现对比

| 特性 | Hudi | Iceberg | Delta Lake |
|------|------|---------|------------|
| **列追踪机制** | Column ID (整数) | Column ID (整数) | Column Name (字符串) |
| **Schema 存储** | CommitMetadata + .schema 文件 | Metadata 文件(manifest) | Transaction Log (_delta_log) |
| **ADD COLUMN** | 支持，强制 nullable | 支持，强制 nullable | 支持，强制 nullable |
| **DROP COLUMN** | 支持 | 支持 | 支持（Spark 3.2+） |
| **RENAME COLUMN** | 支持（基于 Column ID） | 支持（基于 Column ID） | 支持（基于 Column Mapping） |
| **TYPE CHANGE** | 支持（宽化规则） | 支持（宽化规则） | 有限支持 |
| **REORDER COLUMN** | 支持（FIRST/AFTER/BEFORE） | 支持（FIRST/AFTER/BEFORE） | 不支持 |
| **嵌套结构演进** | 支持 | 支持 | 有限支持 |
| **Schema 版本化** | versionId = commit 时间戳 | snapshot-id 关联 | version 关联 |

### 9.2 设计哲学对比

**Hudi**：
- **设计哲学**：在已有的 Avro/Parquet 生态上增加一层 InternalSchema 抽象
- **优势**：与 Hudi 的时间线（Timeline）机制深度集成，Schema 变更和数据写入共享同一套事务机制
- **劣势**：需要维护 InternalSchema 与 Avro Schema 的双重表示，增加了复杂度

**Iceberg**：
- **设计哲学**：从底层重新设计的表格式，Schema Evolution 是一等公民
- **优势**：Column ID 是 Iceberg 格式的核心概念，所有数据文件（Parquet/ORC/Avro）写入时都会携带 Column ID 信息
- **劣势**：要求所有写入工具都理解 Column ID 概念

**Delta Lake**：
- **设计哲学**：基于 Parquet + Transaction Log，Schema Evolution 后期通过 Column Mapping 补充
- **优势**：实现简单，基于 Parquet 的原生 Schema Evolution 能力
- **劣势**：依赖 Column Name 匹配，RENAME 需要启用 Column Mapping 模式才能安全使用；一旦启用不能禁用

### 9.3 Column ID vs Column Name 的根本差异

**基于 Column ID（Hudi/Iceberg）**：

```
版本 1:  id=0 name="user_name" type=STRING
版本 2:  id=0 name="full_name" type=STRING  (重命名)
版本 3:  id=0 name="full_name" type=STRING, id=5 name="user_name" type=STRING (添加同名新列)
```

- 读取版本 1 的文件时，通过 id=0 正确映射到 `full_name`
- 版本 3 中新添加的 `user_name` 拥有 id=5，不会与旧的 id=0 混淆

**基于 Column Name（早期 Delta Lake）**：

```
版本 1:  name="user_name" type=STRING
版本 2:  name="full_name" type=STRING  (重命名)
版本 3:  name="full_name" type=STRING, name="user_name" type=STRING (添加同名新列)
```

- 读取版本 1 的文件时，文件中的 `user_name` 列会被错误地映射到版本 3 中新添加的 `user_name`
- 这就是为什么 Delta Lake 后来引入了 Column Mapping 特性

### 9.4 Schema 版本历史管理对比

**Hudi** 的方式：
- 完整的历史 Schema 链存储在 `.hoodie/.schema/` 目录下
- 每次 commit 时，最新的 InternalSchema 存储在 CommitMetadata 中
- 历史 Schema 使用 JSON 序列化，新 Schema 追加到 JSON 数组头部
- 通过 TreeMap 按 versionId 排序，支持范围查找（找不到精确版本时回退到最近的历史版本）

**Iceberg** 的方式：
- Schema 存储在 metadata.json 文件中
- 每个 snapshot 引用一个 schema-id
- 多个 snapshot 可以共享同一个 schema
- 历史 Schema 列表内嵌在 metadata.json 中

**Delta Lake** 的方式：
- Schema 存储在 Transaction Log 的 commit 文件中
- 每次 commit 记录当前 Schema
- 通过 Checkpoint 机制合并历史 Schema

### 9.5 读取时 Schema 对齐能力对比

| 场景 | Hudi | Iceberg | Delta Lake |
|------|------|---------|------------|
| **新列填 NULL** | InternalSchemaMerger 生成 merged schema，Parquet Reader 自动填 NULL | Iceberg Reader 根据 Column ID 映射，缺失列返回 NULL | Delta Reader 按列名匹配，缺失列返回 NULL |
| **删除列跳过** | merged schema 不包含已删除列，Reader 自动跳过 | 类似，通过 Column ID 过滤 | 类似，按列名过滤 |
| **列重命名** | InternalSchemaMerger 的 dealWithRename 方法，按 ID 匹配后使用文件中的旧名称读取 | 按 Column ID 直接匹配，无需名称映射 | 需要启用 Column Mapping |
| **类型转换** | 分两步：先用文件原始类型读取，再做类型转换（collectTypeChangedCols） | Iceberg Reader 内置类型转换 | 有限的自动类型转换 |
| **嵌套结构** | 递归处理 Record/Array/Map 的每一层 | 递归处理，Column ID 贯穿全部嵌套层 | 基于名称的有限嵌套支持 |

### 9.6 Hudi Schema Evolution 的独特优势

1. **与时间线深度集成**：Schema 变更记录在 Timeline 上，与数据 commit 共享同一套事务机制。这意味着 Schema 变更是原子的、可见性语义与数据写入一致

2. **双 Schema 表示**：同时维护 InternalSchema（用于精确的 Schema Evolution）和 Avro Schema（用于与生态兼容）。虽然增加了复杂度，但保证了向后兼容性

3. **灵活的读取策略**：`InternalSchemaMerger` 提供了 `useColumnTypeFromFileSchema` 和 `useColNameFromFileSchema` 两个旋钮，可以根据不同的 Reader（Parquet Reader vs Log Record Rewrite）选择不同的策略

4. **全局 Schema 缓存**：`InternalSchemaCache` 使用 Caffeine Cache + 分段锁，在多线程环境下高效共享 Schema 信息，减少了重复 IO

5. **隐式 Schema Reconciliation**：通过 `reconcileSchema`，Hudi 支持写入时自动检测并合并 Schema 变更，无需用户显式执行 DDL。这在 Streaming 场景中特别有用

---

## 附录：核心源码文件索引

| 文件 | 路径 | 职责 |
|------|------|------|
| InternalSchema.java | `hudi-common/.../internal/schema/` | Schema 核心模型 |
| Type.java / Types.java | `hudi-common/.../internal/schema/` | 类型系统定义 |
| InternalSchemaBuilder.java | `hudi-common/.../internal/schema/` | 索引构建和类型遍历 |
| TableChanges.java | `hudi-common/.../internal/schema/action/` | ADD/DELETE/UPDATE 变更操作 |
| TableChange.java | `hudi-common/.../internal/schema/action/` | 变更接口和位置变更定义 |
| InternalSchemaChangeApplier.java | `hudi-common/.../internal/schema/action/` | 变更操作的应用入口 |
| InternalSchemaMerger.java | `hudi-common/.../internal/schema/action/` | 读时 Schema 合并 |
| TableChangesHelper.java | `hudi-common/.../internal/schema/action/` | 位置变更辅助工具 |
| SchemaChangeUtils.java | `hudi-common/.../internal/schema/utils/` | 变更应用到类型树 + 类型升级规则 |
| AvroSchemaEvolutionUtils.java | `hudi-common/.../internal/schema/utils/` | Avro Schema 对齐算法 |
| InternalSchemaUtils.java | `hudi-common/.../internal/schema/utils/` | 列裁剪、过滤重建、重命名检测 |
| SerDeHelper.java | `hudi-common/.../internal/schema/utils/` | JSON 序列化/反序列化 |
| InternalSchemaConverter.java | `hudi-common/.../internal/schema/convert/` | HoodieSchema <-> InternalSchema 转换 |
| FileBasedInternalSchemaStorageManager.java | `hudi-common/.../internal/schema/io/` | Schema 持久化管理 |
| TableSchemaResolver.java | `hudi-common/.../common/table/` | 表 Schema 推导 |
| FileGroupReaderSchemaHandler.java | `hudi-common/.../common/table/read/` | 读时 Schema 处理 |
| InternalSchemaCache.java | `hudi-common/.../common/util/` | 全局 Schema 缓存 |
| BaseHoodieWriteClient.java | `hudi-client/hudi-client-common/...` | 写时 Schema 保存 |
| AlterTableCommand.scala | `hudi-spark-common/.../command/` | Spark DDL 命令实现 |
| Spark35ResolveHudiAlterTableCommand.scala | `hudi-spark3.5.x/...` | Spark 3.5 DDL 规则拦截 |
| ParquetSchemaEvolutionUtils.scala | `hudi-spark-common/.../parquet/` | Spark Parquet 读取时 Schema Evolution |
| InternalSchemaManager.java | `hudi-flink/.../table/format/` | Flink Schema Evolution 管理器 |

---

## 附录：Schema Evolution 端到端流程图

### DDL 变更流程（以 Spark ALTER TABLE ADD COLUMNS 为例）

```
用户执行: ALTER TABLE t ADD COLUMNS (age INT)
    |
    v
Spark Catalyst 解析为 AddColumns 逻辑计划
    |
    v
Spark35ResolveHudiAlterTableCommand 拦截
    |  识别为 Hudi 表，转换为 HudiAlterTableCommand
    v
AlterTableCommand.applyAddAction()
    |  1. 获取当前 InternalSchema 和历史 Schema 链
    |  2. SparkInternalSchemaConverter 将 Spark INT 转为 Hudi IntType
    |  3. ColumnAddChange.addColumns("", "age", IntType, null)
    |     - 检查列名不重复
    |     - 分配新 Column ID (maxColumnId + 1)
    |     - 强制设为 optional
    |  4. SchemaChangeUtils.applyTableChanges2Schema()
    |     - 递归遍历类型树，在根 RecordType 追加新 Field
    v
AlterTableCommand.commitWithSchema()
    |  1. 创建 ALTER_SCHEMA commit
    |  2. InternalSchema 序列化为 JSON，存入 CommitMetadata
    |  3. 更新历史 Schema 链（inheritSchemas）
    |  4. 持久化到 .hoodie/.schema/ 目录
    |  5. client.commit() 完成原子提交
    |  6. 同步到 Hive Metastore
    v
完成
```

### 读取流程（以 Spark 读取旧文件为例）

```
用户执行: SELECT id, age FROM t (age 是新增列，旧文件没有)
    |
    v
FileGroupReaderSchemaHandler 初始化
    |  tableSchema = (id INT, name STRING, age INT)
    |  requestedSchema = (id INT, age INT)
    |  requiredSchema = 补充 merge 所需列后的 schema
    |  internalSchema = pruneInternalSchema(requiredSchema)
    v
读取某个旧 Parquet 文件 (commit time = T1)
    |
    v
getRequiredSchemaForFileAndRenamedColumns(path)
    |  1. 从文件名解析 commitTime = T1
    |  2. InternalSchemaCache.searchSchemaAndCache(T1, metaClient)
    |     -> 返回 T1 时的 Schema: (id=0 INT, name=1 STRING)  (没有 age)
    |  3. InternalSchemaMerger(fileSchema, querySchema)
    |     - id (ID=0): 两边都有，名称相同 -> 直接保留
    |     - age (ID=2): 文件中不存在 -> 标记为新增列
    |  4. merged schema = (id INT, age INT)
    |     age 列在文件中不存在，Parquet Reader 自动返回 NULL
    v
Parquet Reader 使用 merged schema 读取文件
    |  id 列: 正常读取文件中的数据
    |  age 列: 文件中不存在，返回 NULL
    v
返回结果: (1, NULL), (2, NULL), ...
```

---

*本文档基于 Apache Hudi v1.2.0-SNAPSHOT 源码分析，涵盖了 Schema Evolution 的核心设计、实现细节和工程考量。每个设计决策都附带了"为什么这么设计"和"好处是什么"的解释，帮助读者深入理解 Hudi Schema Evolution 的设计哲学。*
