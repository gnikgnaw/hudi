# Hudi LogFile 格式与 MOR 合并机制深度解析

> 基于 Apache Hudi v1.2.0-SNAPSHOT 源码分析
> 核心源码路径：`hudi-common/src/main/java/org/apache/hudi/common/table/log/`

---

## 一、整体概述

在 Hudi 的 Merge-On-Read（MOR）表类型中，数据的更新和删除并不直接修改 Base File（Parquet），而是将增量变更以 **Log Block** 的形式追加写入到 **Log File** 中。读取时再将 Base File 与 Log File 中的记录合并，得到最新的数据视图。这种设计使得写入操作变成了顺序追加（append-only），极大地提升了写入吞吐量，同时将合并的开销推迟到了读取阶段。

本文将从底层二进制格式、Block 类型体系、读写流程、合并优化等多个维度全面解析 Hudi LogFile 的实现机制。

---

## 二、Log File 的命名规则与版本管理

### 2.1 Log File 命名格式

Log File 的命名由 `FSUtils.makeLogFileName()` 方法生成，整体格式为：

```
.<fileId>_<deltaCommitTime>.<extension>.<version>_<writeToken>
```

对应正则表达式（定义在 `FSUtils.java`）：

```java
public static final Pattern LOG_FILE_PATTERN =
    Pattern.compile("^\\.([^._]+)_([^.]*)\\.(log|archive)\\.(\\d+)(_((\\d+)-(\\d+)-(\\d+))(\\.cdc)?)?$");
```

各字段含义：

| 字段 | 说明 | 示例 |
|------|------|------|
| `.` (前缀) | Log File 固定以点号开头（`LOG_FILE_PREFIX = "."`），这是为了在某些文件系统中作为隐藏文件 | `.` |
| `fileId` | 与对应 Base File 相同的文件 ID，用于将 Log File 关联到同一个 FileSlice | `b5068208-e1a4-11e6-bf01-fe55135034f3` |
| `deltaCommitTime` | 生成该 Log File 的 delta commit 时间戳 | `20170101134598` |
| `extension` | 文件扩展类型，`log` 表示普通日志，`archive` 表示归档日志 | `log` |
| `version` | 同一 FileSlice 内同一 instant 的 Log File 版本号（从 1 开始递增） | `1` |
| `writeToken` | 格式为 `<taskPartitionId>-<stageId>-<taskAttemptId>`，用于区分不同 task 写入的 Log File | `1-0-1` |
| `.cdc` (可选后缀) | 标识该 Log File 是 CDC（Change Data Capture）日志 | `.cdc` |

### 2.2 为什么这么设计

1. **fileId 关联机制**：Log File 通过与 Base File 相同的 `fileId` 来建立关联关系。同一个 FileSlice 中的所有 Log File 和 Base File 共享相同的 `fileId`，这使得在构建 FileSlice 视图时可以快速匹配。

2. **版本号递增**：当一个 Log File 写满（超过 `sizeThreshold`，默认 512 MB）或文件已存在时，会调用 `rollOver()` 方法生成一个版本号加一的新 Log File。版本号定义在 `HoodieLogFile.LOGFILE_BASE_VERSION = 1`。

3. **writeToken 防冲突**：在多 task 并发写入场景下，`writeToken` 确保不同 task 生成的 Log File 不会冲突。格式 `taskPartitionId-stageId-taskAttemptId` 精确标识了写入者身份。

### 2.3 Log File 的排序规则

`HoodieLogFile.LogFileComparator` 定义了 Log File 的排序规则（源码 `HoodieLogFile.java:201-241`）：

```
优先级: deltaCommitTime > logVersion > logWriteToken > suffix
```

排序逻辑：
1. 首先按 `deltaCommitTime` 自然序排序（字符串比较）
2. 相同 commit time 时按 `logVersion` 升序排列
3. 相同 version 时按 `logWriteToken` 自然序比较
4. 最后按 `suffix`（如 `.cdc`）排序

**好处**：这种多级排序确保了在合并时能按照写入的时间和逻辑顺序处理 Log Block，保证数据的最终一致性。

### 2.4 rollOver 机制

当 Log File 写入过程中发生以下情况之一时，Writer 会执行 rollOver 操作（`HoodieLogFormatWriter.java:214-218`）：

```java
private void rollOver() throws IOException {
    closeStream();
    this.logFile = logFile.rollOver(rolloverLogWriteToken);
    this.closed = false;
}
```

`HoodieLogFile.rollOver()` 方法（`HoodieLogFile.java:181-188`）：

```java
public HoodieLogFile rollOver(String logWriteToken) {
    return new HoodieLogFile(new StoragePath(path.getParent(),
        FSUtils.makeLogFileName(fileId, extension, deltaCommitTime, logVersion + 1, logWriteToken)));
}
```

**触发 rollOver 的场景**：
- 文件已存在（`storage.exists(logFile.getPath())` 为 true）
- 另一个 task 正在创建同一文件（`AlreadyBeingCreatedException`）
- 当前文件大小超过阈值（`getCurrentSize() > sizeThreshold`）

---

## 三、HoodieLogFormat 文件格式

### 3.1 整体二进制布局

一个 Log File 由多个 **Log Block** 顺序拼接组成。每个 Block 的完整二进制格式如下（定义在 `HoodieLogFormatWriter.appendBlocks()` 方法中，`HoodieLogFormatWriter.java:128-188`）：

```
+==========================+
|    Magic Header (6B)     |  '#', 'H', 'U', 'D', 'I', '#'
+--------------------------+
|  Total Block Size (8B)   |  long: Block 内部所有数据的总字节数（不含 Magic Header）
+--------------------------+
|  Log Format Version (4B) |  int: 当前为 1
+--------------------------+
|  Block Type Ordinal (4B) |  int: HoodieLogBlockType 枚举序号
+--------------------------+
|  Block Header (变长)      |  序列化的 Map<HeaderMetadataType, String>
+--------------------------+
|  Content Length (8B)      |  long: 内容区的字节数
+--------------------------+
|  Content (变长)           |  实际的数据内容（记录/删除键/命令等）
+--------------------------+
|  Block Footer (变长)      |  序列化的 Map<FooterMetadataType, String>
+--------------------------+
|  Total Block Length (8B)  |  long: 整个 Block 总长度（含 Magic），用于反向遍历
+==========================+
```

### 3.2 Magic Header

```java
// HoodieLogFormat.java:51
byte[] MAGIC = new byte[] {'#', 'H', 'U', 'D', 'I', '#'};
```

Magic Header 是固定的 6 字节序列 `#HUDI#`。

**为什么需要 Magic Header**：
- 它是每个 Block 的分隔标记，Reader 通过扫描 Magic 来定位下一个 Block 的起始位置
- 当遇到损坏的 Block 时，可以通过扫描下一个 Magic 来跳过损坏区域恢复读取
- 在 `isBlockCorrupted()` 方法中，读取完一个 Block 后会验证下一个位置是否为 Magic 或 EOF

### 3.3 Block Header 的序列化格式

Header 的序列化由 `HoodieLogBlock.getLogMetadataBytes()` 方法实现（`HoodieLogBlock.java:446-457`）：

```
+------+--------+-----------+-------+--------+-----------+-------+-----+
| count| ordinal| valueLen  | value | ordinal| valueLen  | value | ... |
| (4B) | (4B)   | (4B)     | (变长) | (4B)   | (4B)     | (变长) | ... |
+------+--------+-----------+-------+--------+-----------+-------+-----+
```

格式说明：
1. **count (4B int)**: metadata 条目总数
2. 对每个条目：
   - **ordinal (4B int)**: `HeaderMetadataType` 枚举的序号
   - **valueLen (4B int)**: 值字符串的 UTF-8 字节长度
   - **value (变长 byte[])**: 值字符串的 UTF-8 编码

**为什么使用 ordinal 而非字符串**：使用枚举序号而非名称字符串可以大幅减少存储空间和序列化开销，同时确保向前兼容性（新增枚举只能加在末尾）。

### 3.4 HeaderMetadataType 枚举

定义在 `HoodieLogBlock.java:225-242`，每个枚举关联最早支持的表版本号：

| 枚举值 | 最早表版本 | 说明 |
|--------|-----------|------|
| `INSTANT_TIME` | V1 | 写入该 Block 的 instant 时间戳 |
| `TARGET_INSTANT_TIME` | V1 | 目标 instant（Command Block 中用于指定回滚目标） |
| `SCHEMA` | V1 | 数据 Block 的 Avro Schema（JSON 格式） |
| `COMMAND_BLOCK_TYPE` | V1 | 命令 Block 的类型（如 ROLLBACK） |
| `COMPACTED_BLOCK_TIMES` | V5 | Log Compaction 合并的原始 Block instant 列表 |
| `RECORD_POSITIONS` | V6 | 记录在 Base File 中的位置（RoaringBitmap 编码） |
| `BLOCK_IDENTIFIER` | V6 | Block 唯一标识符 |
| `IS_PARTIAL` | V8 | 是否为部分更新 Block |
| `BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS` | V8 | 位置信息对应的 Base File instant |

### 3.5 Block Footer

Footer 的序列化格式与 Header 完全一致，类型为 `FooterMetadataType`。在当前版本中，`FooterMetadataType` 是一个空枚举，暂无实际使用的 Footer 字段。`HoodieLogFormatVersion` 显示：仅 Version 1 的 LogFormatVersion 支持 Footer。

### 3.6 Total Block Length（反向指针）

Block 末尾写入的 `Total Block Length (8B long)` 包含了从 Magic Header 开始到该字段本身结束的所有字节数。该值有两个核心用途：

1. **反向遍历**：`HoodieLogFileReader.prev()` 方法利用该值从文件末尾向前定位 Block
2. **完整性校验**：`isBlockCorrupted()` 方法会检查 Header 中记录的 Block 大小与 Footer 中的 Total Block Length 是否一致。不一致则说明 Block 被截断

---

## 四、HoodieLogBlock 类型全景

### 4.1 类型枚举定义

所有 Block 类型定义在 `HoodieLogBlock.HoodieLogBlockType` 枚举中（`HoodieLogBlock.java:187-219`）：

```java
public enum HoodieLogBlockType {
    COMMAND_BLOCK(":command", HoodieTableVersion.ONE),
    DELETE_BLOCK(":delete", HoodieTableVersion.ONE),
    CORRUPT_BLOCK(":corrupted", HoodieTableVersion.ONE),
    AVRO_DATA_BLOCK("avro", HoodieTableVersion.ONE),
    HFILE_DATA_BLOCK("hfile", HoodieTableVersion.ONE),
    PARQUET_DATA_BLOCK("parquet", HoodieTableVersion.FOUR),
    CDC_DATA_BLOCK("cdc", HoodieTableVersion.SIX);
}
```

**关键警告**：该枚举的序号（ordinal）被序列化到磁盘上，因此新类型只能追加到末尾，不能修改已有类型的顺序。

### 4.2 HoodieAvroDataBlock

**源码**：`hudi-common/.../block/HoodieAvroDataBlock.java`

这是最早支持的数据 Block 类型，用于将记录以 Avro Binary 格式序列化存储。

**Content 内部格式**（`serializeRecords()` 方法，第 109-136 行）：

```
+---------------------+
| Block Version (4B)  |   int: 当前为 3（HoodieLogBlock.version）
+---------------------+
| Record Count  (4B)  |   int: 记录总数
+---------------------+
| Record 1 Size (4B)  |   int: 第 1 条记录的字节数
+---------------------+
| Record 1 Data (变长) |   Avro Binary 编码的记录
+---------------------+
| Record 2 Size (4B)  |
+---------------------+
| Record 2 Data (变长) |
+---------------------+
| ...                  |
+---------------------+
```

**为什么选择 Avro Binary**：Avro 是 Hudi 内部最早采用的记录格式，Avro Binary 编码紧凑、自描述（Schema 存储在 Block Header 中），且原生支持 Schema Evolution。每条记录前写入其大小，使得反序列化时可以按记录粒度定位。

**流式读取优化**：`StreamingRecordIterator` 内部类支持按缓冲区大小分批从磁盘读取记录，避免将整个 Block 内容一次性加载到内存中。

### 4.3 HoodieParquetDataBlock

**源码**：`hudi-common/.../block/HoodieParquetDataBlock.java`

从表版本 V4 开始引入，将记录以 Parquet 格式存储在 Log Block 中。

**核心特点**：
- 序列化时使用 `HoodieIOFactory` 将记录写为 Parquet 格式
- 反序列化时利用 **InLineFS**（内联文件系统）直接从 Log File 中读取嵌入的 Parquet 数据，无需解压 Block 内容到内存
- 不支持 `deserializeRecords(byte[] content, ...)` 方法（直接抛 `UnsupportedOperationException`），因为它不走传统的 inflate/deflate 路径

**为什么引入 Parquet Block**：当 Base File 为 Parquet 格式时，Log File 中也使用 Parquet 格式存储变更记录，可以复用 Parquet 的列式存储优势（压缩率更高、列裁剪）。同时，通过 InLineFS 直接读取嵌入的 Parquet 数据，避免了 Avro 和 Parquet 之间的格式转换开销。

### 4.4 HoodieHFileDataBlock

**源码**：`hudi-common/.../block/HoodieHFileDataBlock.java`

HFile 格式的数据 Block，主要用于 Hudi Metadata Table。

**核心特点**：
- 使用 HBase HFile 格式存储，天然支持按 key 的点查（Point Lookup）
- `enablePointLookups` 参数可以开启按 key 精确查找，而非全扫描
- `lookupRecords()` 和 `lookupEngineRecords()` 方法通过 `HoodieAvroHFileReaderImplBase` 实现 O(log n) 的 key 查找

**为什么 Metadata Table 使用 HFile**：Metadata Table 的访问模式以按 key 查找为主（如查询某个 fileId 的文件信息），HFile 的有序存储和 Block 索引使得点查性能远优于全扫描式的 Avro Block。

### 4.5 HoodieDeleteBlock

**源码**：`hudi-common/.../block/HoodieDeleteBlock.java`

存储一批需要删除的记录键。

**Content 内部格式**（`getContentBytes()` 方法，第 93-109 行）：

```
+--------------------+
| Block Version (4B) |   int: 当前为 3
+--------------------+
| Data Length   (4B) |   int: 序列化数据的字节长度
+--------------------+
| Serialized Data    |   DeleteRecord[] 的序列化字节
+--------------------+
```

**版本演进**：
- **V1**：序列化 `HoodieKey[]`（仅包含 recordKey 和 partitionPath）
- **V2**：序列化 `DeleteRecord[]`（增加 orderingValue 用于预合并判断）
- **V3**（当前）：使用 Avro 特定序列化 `HoodieDeleteRecordList`，更高效且可演进

**为什么需要 orderingValue**：在有预合并（pre-combine）语义的表中，一条 DELETE 消息不应该盲目地删除比它更新的数据。通过携带 `orderingValue`，Scanner 可以在合并时判断 DELETE 是否应该生效。

### 4.6 HoodieCommandBlock

**源码**：`hudi-common/.../block/HoodieCommandBlock.java`

命令 Block，用于向 Scanner 发出特定控制指令。

**当前支持的命令**：
```java
public enum HoodieCommandBlockTypeEnum {
    ROLLBACK_BLOCK
}
```

`ROLLBACK_BLOCK` 的含义：标记需要回滚的 instant。当 Scanner 扫描到 Command Block 时，会读取 `TARGET_INSTANT_TIME` Header，将该 instant 的所有 Data/Delete Block 标记为无效。

**Content**：Command Block 的内容为空（`getContentBytes()` 返回空的 `ByteArrayOutputStream`），所有信息都存储在 Block Header 中。

**为什么内容为空**：Command Block 纯粹是元数据性质的指令，不携带任何数据记录。将命令类型和目标 instant 存储在 Header 中既简洁又高效。

### 4.7 HoodieCorruptBlock

**源码**：`hudi-common/.../block/HoodieCorruptBlock.java`

不是一种"正式"的 Block 类型，而是当 Reader 检测到损坏的 Block 时生成的占位 Block。

**生成时机**（`HoodieLogFileReader.createCorruptBlock()` 方法，第 246-259 行）：
1. 无法读取 Block Size（EOF 或格式异常）
2. Block Size 与 Footer 中记录的长度不一致
3. Block 结束位置之后未找到 Magic 或 EOF

**恢复策略**：`scanForNextAvailableBlockOffset()` 方法以 1 MB 为缓冲单位扫描文件，查找下一个 Magic 标记 `#HUDI#` 的位置，然后将损坏区域封装为 `HoodieCorruptBlock`。

**好处**：这种设计使得即使 Log File 部分损坏，Reader 也能跳过损坏区域继续读取后续的有效 Block，最大程度保护数据可用性。

---

## 五、HoodieLogFormatWriter 写入流程

### 5.1 Writer 的创建

Writer 通过 Builder 模式创建（`HoodieLogFormat.WriterBuilder`，`HoodieLogFormat.java:119-294`）：

```java
HoodieLogFormat.Writer writer = HoodieLogFormat.newWriterBuilder()
    .withStorage(storage)
    .withFileId(fileId)
    .withInstantTime(instantTime)
    .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
    .onParentPath(partitionPath)
    .withSizeThreshold(sizeThreshold)    // 默认 512 MB
    .withLogWriteToken(writeToken)
    .build();
```

**版本号计算逻辑**（`WriterBuilder.build()` 方法，第 245-266 行）：
- 表版本 >= V8 且 writeToken 非空时：直接使用 `LOGFILE_BASE_VERSION = 1`（Writer 内部处理文件存在性检查）
- 否则：通过 `FSUtils.getLatestLogVersion()` 扫描存储获取最新版本号（代价较高）

### 5.2 懒初始化 OutputStream

Writer 采用懒初始化策略，直到真正调用 `appendBlock()` 时才创建输出流（`getOutputStream()` 方法，第 93-120 行）：

```java
private FSDataOutputStream getOutputStream() throws IOException {
    if (this.output == null) {
        boolean created = false;
        while (!created) {
            try {
                if (storage.exists(logFile.getPath())) {
                    rollOver(); // 文件已存在，递增版本号
                }
                createNewFile();
                created = true;
            } catch (FileAlreadyExistsException ignored) {
                rollOver(); // 竞态条件处理
            } catch (RemoteException re) {
                // AlreadyBeingCreatedException 也执行 rollOver
            }
        }
    }
    return output;
}
```

**为什么懒初始化**：避免创建 Writer 但不写入数据时产生空文件。同时，通过循环重试和 rollOver 处理并发写入冲突。

### 5.3 Block 写入流程

`appendBlocks()` 方法（`HoodieLogFormatWriter.java:128-188`）执行以下步骤：

```
for (每个 block) {
    1. 写入 Magic Header (6 bytes): #HUDI#
    2. 写入 Total Block Size (8 bytes long): 不含 Magic 的 Block 内部总长
    3. 写入 Log Format Version (4 bytes int): 当前为 1
    4. 写入 Block Type Ordinal (4 bytes int): 枚举序号
    5. 写入 Header Bytes (变长): 序列化的 Block Header
    6. 写入 Content Length (8 bytes long): 内容字节数
    7. 写入 Content (变长): Block 的实际内容
    8. 写入 Footer Bytes (变长): 序列化的 Block Footer
    9. 写入 Total Block Length (8 bytes long): 含 Magic 的完整 Block 长度
}
flush() + hsync() // 确保数据落盘
```

**Total Block Size 的计算**（`getLogBlockLength()` 方法，第 196-204 行）：

```java
private int getLogBlockLength(int contentLength, int headerLength, int footerLength) {
    return Integer.BYTES       // version (4B)
        + Integer.BYTES        // block type ordinal (4B)
        + headerLength         // header 变长
        + Long.BYTES           // content length (8B)
        + contentLength        // content 变长
        + footerLength         // footer 变长
        + Long.BYTES;          // total block length at end (8B)
}
```

### 5.4 数据持久化保证

写入完成后执行两步刷盘：
```java
output.flush();    // 刷新 Java 缓冲区
output.hsync();    // HDFS fsync，确保数据到 DataNode 磁盘
```

**为什么使用 hsync 而非 hflush**：`hflush()` 只保证数据到 DataNode 内存，`hsync()` 等同于 POSIX `fsync()`，确保数据持久化到物理磁盘，防止 DataNode 宕机时数据丢失。

### 5.5 JVM 退出安全保障

Writer 在构造时注册了 ShutdownHook（`addShutDownHook()` 方法，第 272-284 行），确保 JVM 异常退出时关闭输出流，避免数据截断或文件句柄泄漏。Writer 关闭时会移除该 ShutdownHook 以避免内存泄漏。

---

## 六、HoodieLogFileReader / HoodieLogFormatReader 读取流程

### 6.1 HoodieLogFileReader -- 单文件 Block 迭代器

**源码**：`hudi-common/.../log/HoodieLogFileReader.java`

`HoodieLogFileReader` 实现了 `HoodieLogFormat.Reader` 接口，作为单个 Log File 上的 Block 级别迭代器。

**读取单个 Block 的流程**（`readBlock()` 方法，第 128-221 行）：

```
1. readLong()  => blockSize        // 读取 Block 总大小
2. isBlockCorrupted(blockSize)     // 完整性校验
3. readVersion()                    // 读取 Log Format Version
4. tryReadBlockType(version)        // 读取 Block Type Ordinal
5. getHeaderMetadata(inputStream)   // 读取 Block Header
6. readLong()  => contentLength     // 读取内容长度
7. tryReadContent(...)              // 读取或跳过内容
8. getFooterMetadata(inputStream)   // 读取 Block Footer
9. readLong()  => totalBlockLength  // 读取反向指针
10. 根据 blockType 构造对应的 Block 实例
```

### 6.2 Block 完整性校验

`isBlockCorrupted()` 方法（第 261-305 行）执行三重校验：

1. **跳过事务性存储**：如果底层存储支持写事务（如 S3 的原子写入），则跳过校验
2. **Footer 长度比对**：seek 到 Block 末尾，读取 Footer 中的 `blockSizeFromFooter`，与 Header 中的 `blockSize` 比较（Footer 包含 Magic 长度，需减去）
3. **Magic 标记验证**：检查 Block 结束后是否紧跟一个有效的 Magic 标记或 EOF

### 6.3 损坏 Block 的恢复

当检测到损坏时，`createCorruptBlock()` 方法（第 246-259 行）：

```java
private HoodieLogBlock createCorruptBlock(long blockStartPos) throws IOException {
    inputStream.seek(blockStartPos);
    long nextBlockOffset = scanForNextAvailableBlockOffset();
    inputStream.seek(blockStartPos);
    int corruptedBlockSize = (int) (nextBlockOffset - blockStartPos);
    // 将损坏区域封装为 CorruptBlock
    ...
}
```

`scanForNextAvailableBlockOffset()` 以 1 MB 缓冲区逐步扫描文件，搜索下一个 `#HUDI#` 标记。搜索时考虑了跨缓冲区边界的情况（每次回退 Magic 长度的字节数）。

### 6.4 懒读取（Lazy Reading）优化

当 `readBlockLazily = true` 时，`HoodieLogBlock.tryReadContent()` 方法不会将内容读入内存，而是直接 seek 跳过内容区域：

```java
public static Option<byte[]> tryReadContent(SeekableDataInputStream inputStream,
    Integer contentLength, boolean readLazily) throws IOException {
    if (readLazily) {
        inputStream.seek(inputStream.getPos() + contentLength);
        return Option.empty();
    }
    byte[] content = new byte[contentLength];
    inputStream.readFully(content, 0, contentLength);
    return Option.of(content);
}
```

后续需要时调用 `inflate()` 方法从磁盘重新读取内容。这种设计将读取分为两个 I/O Pass：

```
Pass 1 (Metadata): 顺序读取所有 Block 的 Header（前向 seek）
Pass 2 (Content):  按需读取需要的 Block 内容（可能随机 seek）
```

**好处**：当 Log File 包含大量 Block 但只有部分需要处理时（如被 Rollback 的 Block），可以显著减少无用的 I/O 读取。

### 6.5 HoodieLogFormatReader -- 多文件 Block 迭代器

**源码**：`hudi-common/.../log/HoodieLogFormatReader.java`

`HoodieLogFormatReader` 是多个 `HoodieLogFileReader` 的聚合包装器，提供跨多个 Log File 的统一迭代接口。

**核心逻辑**（`hasNext()` 方法，第 79-98 行）：

```java
public boolean hasNext() {
    if (currentReader == null) return false;
    if (currentReader.hasNext()) return true;
    if (!logFiles.isEmpty()) {
        // 当前文件读完，切换到下一个文件
        HoodieLogFile nextLogFile = logFiles.remove(0);
        this.currentReader.close();
        this.currentReader = new HoodieLogFileReader(storage, nextLogFile, ...);
        return hasNext(); // 递归检查新文件
    }
    return false;
}
```

**好处**：上层的 Scanner 无需关心 Log File 的边界，可以像处理单个文件一样遍历所有 Block。

---

## 七、AbstractHoodieLogRecordScanner 扫描框架

### 7.1 整体架构

`AbstractHoodieLogRecordScanner` 是所有 Log Record Scanner 的基类（`AbstractHoodieLogRecordScanner.java`），定义了两阶段扫描框架：

```
阶段 1：前向遍历，收集所有 Block 的 instant 信息、处理 Rollback 逻辑
阶段 2：逆序遍历 instant 列表，处理 Log Compaction 合并关系，构建最终的 Block 队列
```

### 7.2 scanInternal 方法详解

`scanInternal()` 方法（第 242-456 行）是核心扫描逻辑。

**第一阶段 -- 前向扫描**：

```java
while (logFormatReaderWrapper.hasNext()) {
    HoodieLogBlock logBlock = logFormatReaderWrapper.next();
    String instantTime = logBlock.getLogBlockHeader().get(INSTANT_TIME);

    // 1. 跳过 CORRUPT_BLOCK
    // 2. 跳过超过 latestInstantTime 的 Data/Delete Block
    // 3. 对于非 COMMAND_BLOCK，检查 instant 是否已提交
    // 4. 处理 instantRange 过滤

    switch (logBlock.getBlockType()) {
        case DATA_BLOCK / DELETE_BLOCK:
            // 收集到 instantToBlocksMap 中
            break;
        case COMMAND_BLOCK:
            // ROLLBACK: 收集 targetRollbackInstants
            // 从 instantToBlocksMap 中移除被回滚的 instant
            break;
    }
}
```

**第二阶段 -- 逆序处理 Log Compaction**：

```java
for (int i = orderedInstantsList.size() - 1; i >= 0; i--) {
    String instantTime = orderedInstantsList.get(i);
    HoodieLogBlock firstBlock = instantsBlocks.get(0);

    if (firstBlock 包含 COMPACTED_BLOCK_TIMES Header) {
        // 这是一个 Log Compaction 产生的合并 Block
        // 更新 blockTimeToCompactionBlockTimeMap
    } else {
        String compactedFinalInstantTime = blockTimeToCompactionBlockTimeMap.get(instantTime);
        if (compactedFinalInstantTime == null) {
            // 未被合并，直接加入队列
        } else if (已包含最终合并 Block) {
            // 跳过（已被合并 Block 代替）
        } else {
            // 加入最终合并 Block
        }
    }
}
```

**为什么需要两阶段**：在多 Writer 场景下，Rollback Block 可能不紧跟在被回滚的 Block 后面（例如 `B1, B2, B3, B4, R1(B3), B5`）。两阶段处理确保了即使 Block 顺序被打乱，也能正确处理回滚和 Log Compaction 的合并关系。

### 7.3 Log Compaction 合并链

Log Compaction 会将多个连续的 Block 合并为一个大的 Compacted Block。合并后的 Block 的 Header 中包含 `COMPACTED_BLOCK_TIMES`，记录了被合并的所有原始 instant。

例如：
```
B1(i1) + B2(i2) => M1(i3, [i1,i2])
M1(i3) + B3(i4) + B4(i5) => M2(i6, [i3,i4,i5])
```

此时 `blockTimeToCompactionBlockTimeMap` 为：
```
i1 -> i6, i2 -> i6, i3 -> i6, i4 -> i6, i5 -> i6
```

**好处**：通过这种多层合并链，Log Compaction 可以多次执行而不影响正确性，Scanner 总能找到包含所有变更的最终合并 Block。

---

## 八、HoodieMergedLogRecordScanner vs HoodieUnMergedLogRecordScanner

### 8.1 HoodieMergedLogRecordScanner -- 合并 Scanner

**源码**：`hudi-common/.../log/HoodieMergedLogRecordScanner.java`

**核心特征**：
- 使用 `ExternalSpillableMap<String, HoodieRecord>` 存储合并后的记录
- 对相同 key 的记录执行 pre-combine 合并（通过 `recordMerger.merge()`）
- 实现了 `Iterable<HoodieRecord>` 接口，可直接遍历合并结果

**processNextRecord 合并逻辑**（第 255-283 行）：

```java
protected <T> void processNextRecord(HoodieRecord<T> newRecord) throws IOException {
    String key = newRecord.getRecordKey();
    HoodieRecord<T> prevRecord = records.get(key);
    if (prevRecord != null) {
        // 执行 pre-combine 合并
        BufferedRecord<T> combinedRecord = recordMerger.merge(prevBuffered, newBuffered, ...);
        records.put(key, latestHoodieRecord.copy());
    } else {
        records.put(key, newRecord.copy());
    }
}
```

**processNextDeletedRecord 删除逻辑**（第 286-314 行）：

```java
protected void processNextDeletedRecord(DeleteRecord deleteRecord) {
    String key = deleteRecord.getRecordKey();
    HoodieRecord oldRecord = records.get(key);
    if (oldRecord != null) {
        // 比较 orderingValue，只有 DELETE 的 orderingValue >= 现有记录时才执行删除
        Comparable curOrderingVal = oldRecord.getOrderingValue(...);
        Comparable deleteOrderingVal = deleteRecord.getOrderingValue();
        if (curOrderingVal > deleteOrderingVal) return; // DELETE 已过时
    }
    // 放入空记录标记删除
    records.put(key, emptyPayloadRecord);
}
```

**使用场景**：
- **Compaction**：将 Base File 与 Log File 合并生成新的 Base File
- **Snapshot Query**：MOR 表的快照查询需要合并所有 Log 记录
- **Metadata Table 读取**：读取 Metadata 表的 Log 记录

### 8.2 HoodieUnMergedLogRecordScanner -- 不合并 Scanner

**源码**：`hudi-common/.../log/HoodieUnMergedLogRecordScanner.java`

**核心特征**：
- 不维护任何状态 Map，不执行 pre-combine
- 通过回调函数（`LogRecordScannerCallback`）将每条记录逐条传递给调用方
- 每条记录通过 `record.copy()` 创建副本后传递

**processNextRecord 逻辑**：

```java
protected <T> void processNextRecord(HoodieRecord<T> hoodieRecord) throws Exception {
    if (callback != null) {
        callback.apply(hoodieRecord.copy());
    }
}
```

**使用场景**：
- **增量查询（Incremental Query）**：需要看到所有版本的变更记录
- **CDC（Change Data Capture）**：需要所有变更事件
- **流式消费**：Flink 等流式引擎需要按顺序消费所有变更

### 8.3 对比总结

| 维度 | MergedScanner | UnMergedScanner |
|------|--------------|-----------------|
| 合并策略 | 按 key 合并，只保留最新版本 | 不合并，保留所有版本 |
| 内存使用 | ExternalSpillableMap（内存+磁盘） | 无状态，逐条回调 |
| 输出顺序 | 按 key 组织 | 按写入顺序 |
| 适用场景 | Compaction、Snapshot Query | Incremental Query、CDC |
| pre-combine | 是 | 否 |
| 删除处理 | 放入空 payload 标记 | 回调通知删除的 key |

---

## 九、ExternalSpillableMap 完整实现

### 9.1 设计理念

**源码**：`hudi-common/.../util/collection/ExternalSpillableMap.java`

`ExternalSpillableMap` 是一个两级存储的 Map 实现：当内存使用量未超过阈值时，数据存储在内存 HashMap 中；超过阈值后，新数据自动溢写到磁盘。

**为什么需要这种设计**：MOR 表的 Log File 合并需要缓存大量记录（可能数百万甚至更多），纯内存存储会导致 OOM，纯磁盘存储性能太差。两级存储在保证不 OOM 的前提下最大化利用内存。

### 9.2 核心数据结构

```java
// 内存层：标准 HashMap
private final Map<T, R> inMemoryMap;

// 磁盘层：延迟初始化的 DiskMap
private transient volatile DiskMap<T, R> diskBasedMap;

// 内存上限（乘以 0.8 的安全系数）
private final long maxInMemorySizeInBytes;

// 当前内存估计大小
private long currentInMemoryMapSize;

// 负载大小估计值（动态调整）
private volatile long estimatedPayloadSize;
```

### 9.3 put 操作的分流策略

`put()` 方法（第 212-242 行）核心逻辑：

```java
public R put(T key, R value) {
    if (inMemoryMap.containsKey(key)) {
        // 1. key 已在内存中 => 直接更新内存 Map
        inMemoryMap.put(key, value);
    } else if (currentInMemoryMapSize < maxInMemorySizeInBytes) {
        // 2. 内存未满 => 放入内存 Map
        // 动态估算负载大小（每 100 条重新计算一次）
        if (estimatedPayloadSize == 0) {
            estimatedPayloadSize = keySizeEstimator.sizeEstimate(key)
                + valueSizeEstimator.sizeEstimate(value);
        } else if (inMemoryMap.size() % 100 == 0) {
            // 指数移动平均：90% 旧值 + 10% 新值
            estimatedPayloadSize = (long)(estimatedPayloadSize * 0.9
                + (keySize + valueSize) * 0.1);
        }
        currentInMemoryMapSize += estimatedPayloadSize;
        // 如果 key 之前在磁盘上，先从磁盘删除避免重复
        if (inDiskContainsKey(key)) {
            diskBasedMap.remove(key);
        }
        inMemoryMap.put(key, value);
    } else {
        // 3. 内存已满 => 溢写到磁盘
        if (diskBasedMap == null) {
            initDiskBasedMap(); // 延迟初始化磁盘 Map
        }
        diskBasedMap.put(key, value);
    }
    return value;
}
```

**动态负载估算**的好处：避免为每条记录都执行昂贵的大小估算，通过采样+指数平滑得到一个足够准确的估计值。0.8 的安全系数（`SIZING_FACTOR_FOR_IN_MEMORY_MAP`）为 JVM 其他对象预留了内存空间。

### 9.4 get 操作的两级查找

```java
public R get(Object key) {
    if (inMemoryMap.containsKey(key)) {
        return inMemoryMap.get(key);  // 内存命中，O(1)
    } else if (inDiskContainsKey(key)) {
        return diskBasedMap.get(key);  // 磁盘查找
    }
    return null;
}
```

### 9.5 DiskMap 的两种实现

#### BitCaskDiskMap（默认）

**源码**：`hudi-common/.../util/collection/BitCaskDiskMap.java`

灵感来源于 [Basho Bitcask](https://github.com/basho/bitcask)，是一种基于追加写入（Append-Only）的日志结构存储。

**核心数据结构**：
- **valueMetadataMap**（内存）：`ConcurrentHashMap<T, ValueMetadata>`，存储每个 key 最新值在磁盘文件中的偏移量和大小
- **writeOnlyFile**（磁盘）：单个追加写入文件

**磁盘文件条目格式**（来自 `SpillableMapUtils.spillToDisk()`）：

```
+------------+-----------+------------+-------------+-----+-------+
| CRC (8B)   | timestamp | sizeOfKey  | sizeOfValue | key | value |
| long       | (8B long) | (4B int)   | (4B int)    | 变长 | 变长   |
+------------+-----------+------------+-------------+-----+-------+
```

**写入流程**：
1. 将 value 序列化（可选 Deflate 压缩）
2. 计算 value 的 CRC 校验和
3. 将 key 序列化
4. 追加 `|crc|timestamp|keySize|valueSize|key|value|` 到磁盘文件
5. 更新 `valueMetadataMap`，记录该 key 的最新偏移量

**读取流程**：
1. 从 `valueMetadataMap` 获取偏移量和大小
2. 通过 `RandomAccessFile` 随机读取指定位置的数据
3. 校验 CRC，如果不匹配则抛出异常
4. 可选解压后返回

**ValueMetadata 存储内容**：

```java
class ValueMetadata {
    String filePath;       // 数据文件路径
    int sizeOfValue;       // value 字节数
    long offsetOfValue;    // value 在文件中的偏移量
    long timestamp;        // 写入时间戳
}
```

**好处**：
- 写入性能极高（纯顺序追加，无随机写入）
- 读取通过内存索引+随机读取实现 O(1) 查找
- 线程安全（`ConcurrentHashMap` + `ThreadLocal<RandomAccessFile>`）

**缺点**：
- remove 操作只清除内存索引，磁盘上的旧数据不会被回收
- 单文件存储，无 compaction 机制

#### RocksDbDiskMap

**源码**：`hudi-common/.../util/collection/RocksDbDiskMap.java`

基于 RocksDB 嵌入式存储引擎的 DiskMap 实现。

**核心特点**：
- 使用单个 ColumnFamily（`"rocksdb-diskmap"`）存储所有 KV
- 支持 `writeBatch` 批量写入
- 天然支持 point delete（不像 BitCask 需要额外处理）
- 内存中维护一个 `Set<T> keySet` 用于快速判断 key 是否存在

```java
public R put(T key, R value) {
    getRocksDb().put(ROCKSDB_COL_FAMILY, key, value);
    keySet.add(key);
    return value;
}

public R get(Object key) {
    if (!containsKey(key)) return null;
    return getRocksDb().get(ROCKSDB_COL_FAMILY, (T) key);
}
```

**好处**：
- RocksDB 内建 LSM-Tree compaction，自动回收废弃数据
- 更好的空间利用率（相比 BitCask 的 append-only 模式）
- 支持点删除

**选择建议**：
- 数据量较小、以顺序写入为主时选择 BitCask（默认）
- 数据量很大、有大量更新和删除时选择 RocksDB

配置项：`hoodie.common.spillable.diskmap.type`，可选值为 `BITCASK`（默认）或 `ROCKS_DB`。

### 9.6 迭代器的统一封装

`ExternalSpillableMap.iterator()` 通过 `IteratorWrapper` 将内存和磁盘两层数据的迭代器串联：

```java
public Iterator<R> iterator() {
    return diskBasedMap == null
        ? inMemoryMap.values().iterator()
        : new IteratorWrapper<>(inMemoryMap.values().iterator(), diskBasedMap.iterator());
}
```

`IteratorWrapper` 先遍历内存数据，再遍历磁盘数据，保证所有数据都被访问到。

---

## 十、Record Position Based Merge（位置合并优化）

### 10.1 传统 Key-Based Merge 的瓶颈

在传统的 MOR 合并中，Scanner 需要：
1. 遍历 Log File 中的每条记录，提取 `recordKey`
2. 将记录放入 `ExternalSpillableMap<String, HoodieRecord>` 中
3. 遍历 Base File 中的每条记录，提取 `recordKey`
4. 在 Map 中查找该 key 是否有对应的 Log 记录
5. 如果有，执行合并；如果没有，直接输出

这个过程的瓶颈在于：
- **字符串 key 的比较和哈希**：每条记录都需要提取、序列化和比较字符串 key
- **HashMap 的内存开销**：大量字符串 key 消耗大量内存
- **Map 查找开销**：对于百万级记录，HashMap 查找的常数因子不可忽略

### 10.2 Position-Based Merge 的核心思想

从表版本 V6 开始，Hudi 引入了 Record Position 概念。写入 Log Block 时，如果能确定每条记录在 Base File 中的行号（position），就将这些位置信息编码在 Block Header 的 `RECORD_POSITIONS` 字段中。

读取合并时，不再需要按 key 查找，而是直接按**行号**（position）匹配 Base File 和 Log File 中的记录。

### 10.3 位置信息的写入

位置信息的写入逻辑在 `HoodieLogBlock` 类中实现。当 Block Header 包含 `BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS` 时，会调用相关方法将记录位置编码到 Header 中：

```java
protected void addRecordPositionsToHeader(Set<Long> positionSet, int numRecords) {
    if (positionSet.size() == numRecords) {
        try {
            logBlockHeader.put(HeaderMetadataType.RECORD_POSITIONS, 
                LogReaderUtils.encodePositions(positionSet));
        } catch (IOException e) {
            LOG.error("Cannot write record positions to the log block header.", e);
        }
    } else {
        LOG.warn("There are duplicate keys in the records (number of unique positions: {}, "
                + "number of records: {}). Skip writing record positions to the log block header.",
            positionSet.size(), numRecords);
    }
}
```

位置信息使用 **Roaring64NavigableMap**（RoaringBitmap 的 64 位版本）编码，极其紧凑地存储稀疏的行号集合。

### 10.4 PositionBasedFileGroupRecordBuffer 的实现

**源码**：`hudi-common/.../read/buffer/PositionBasedFileGroupRecordBuffer.java`

**processDataBlock 流程**（第 91-153 行）：

```java
@Override
public void processDataBlock(HoodieDataBlock dataBlock, Option<KeySpec> keySpecOpt) throws IOException {
    if (!readerContext.getShouldMergeUseRecordPosition()) {
        super.processDataBlock(dataBlock, keySpecOpt); // 降级到 key-based
        return;
    }
    // Extract positions from data block.
    List<Long> recordPositions = extractRecordPositions(dataBlock, baseFileInstantTime);
    if (recordPositions == null) {
        LOG.debug("Falling back to key based merge for data block");
        fallbackToKeyBasedBuffer(); // 无位置信息，降级
        super.processDataBlock(dataBlock, keySpecOpt);
        return;
    }

    // 遍历记录，用 position 而非 key 作为 Map 的键
    try (ClosableIterator<T> recordIterator = dataBlock.getEngineRecordIterator(readerContext)) {
        int recordIndex = 0;
        while (recordIterator.hasNext()) {
            T nextRecord = recordIterator.next();
            
            // Skip a record if it is not contained in the specified keys.
            if (shouldSkip(nextRecord, isFullKey, keys, dataBlock.getSchema())) {
                recordIndex++;
                continue;
            }
            
            long recordPosition = recordPositions.get(recordIndex++);
            T evolvedNextRecord = schemaTransformerWithEvolvedSchema.getLeft().apply(nextRecord);
            boolean isDelete = readerContext.getRecordContext().isDeleteRecord(evolvedNextRecord, deleteContext);
            BufferedRecord<T> bufferedRecord = BufferedRecords.fromEngineRecord(evolvedNextRecord, schema, 
                readerContext.getRecordContext(), orderingFieldNames, isDelete);
            processNextDataRecord(bufferedRecord, recordPosition);
        }
    }
}
```

**hasNextBaseRecord 合并流程**（第 230-239 行）：

```java
@Override
protected boolean hasNextBaseRecord(T baseRecord) throws IOException {
    if (!readerContext.getShouldMergeUseRecordPosition()) {
        return doHasNextFallbackBaseRecord(baseRecord);
    }

    // 从 Base File 记录中提取行号
    nextRecordPosition = readerContext.getRecordContext().extractRecordPosition(
        baseRecord, readerSchema, ROW_INDEX_TEMPORARY_COLUMN_NAME, nextRecordPosition);

    // 直接按行号从 Map 中移除并获取对应的 Log 记录
    BufferedRecord<T> logRecordInfo = records.remove(nextRecordPosition++);

    // 执行合并
    return super.hasNextBaseRecord(baseRecord, logRecordInfo);
}
```

### 10.5 位置有效性检查与降级策略

位置信息的提取在 `extractRecordPositions()` 方法中执行严格的有效性检查：

```java
protected static List<Long> extractRecordPositions(HoodieLogBlock logBlock,
    String baseFileInstantTime) throws IOException {
    String blockBaseFileInstantTime = logBlock.getBaseFileInstantTimeOfPositions();

    // 检查 1：Base File instant time 必须匹配
    if (StringUtils.isNullOrEmpty(blockBaseFileInstantTime)
        || !baseFileInstantTime.equals(blockBaseFileInstantTime)) {
        return null; // 降级到 key-based
    }

    // 检查 2：位置信息不能为空
    Roaring64NavigableMap positions = logBlock.getRecordPositions();
    if (positions == null || positions.isEmpty()) {
        return null; // 降级到 key-based
    }

    // 提取位置列表
    ...
}
```

**为什么需要检查 Base File instant time**：如果 Base File 在写入 Log Block 之后被 Compaction 重写了，那么记录的行号就不再有效（文件内容已经变了）。通过比较 instant time 确保位置信息与当前 Base File 对应。

### 10.6 降级与混合策略

当位置信息不可用时，`fallbackToKeyBasedBuffer()` 方法（第 155-173 行）将已经按 position 存储的记录转换回 key-based 存储：

```java
private void fallbackToKeyBasedBuffer() {
    readerContext.setShouldMergeUseRecordPosition(false);
    //need to make a copy of the keys to avoid concurrent modification exception
    ArrayList<Serializable> positions = new ArrayList<>(records.keySet());
    for (Serializable position : positions) {
        BufferedRecord<T> entry = records.get(position);
        String recordKey = entry.getRecordKey();
        if (!entry.isDelete() || recordKey != null) {
            records.put(recordKey, entry);
            records.remove(position);
        } else {
            //if it's a delete record and the key is null, then we need to still use positions
            //this happens when we read the positions using logBlock.getRecordPositions()
            //instead of reading the delete records themselves
            needToDoHybridStrategy = true;
        }
    }
}
```

当存在无法转换的 delete record（没有 key 信息，只有 position）时，启用 **混合策略（Hybrid Strategy）**：同时使用 position 和 key 两种方式匹配记录。

### 10.7 性能优势

Position-Based Merge 相比 Key-Based Merge 的优势：

| 维度 | Key-Based | Position-Based |
|------|-----------|----------------|
| Map 键类型 | String（recordKey） | Long（position） |
| 内存开销 | 高（字符串对象+哈希） | 低（long 值） |
| 查找复杂度 | O(1) 但常数大 | O(1) 且常数小 |
| Base File 遍历 | 需要提取 recordKey | 只需读取行号（Parquet row index） |
| Schema 投影 | 需要包含 key 字段 | 可以完全按需投影 |

**核心收益**：避免了字符串 key 的提取、哈希和比较开销，尤其在 key 为复合字段（多列拼接）时效果更显著。同时，行号作为 Map 的键只占 8 字节，大幅降低了内存使用。

---

## 十一、HoodieMergedLogRecordReader（新版 FileGroupReader 体系）

### 11.1 与旧版 Scanner 的关系

Hudi v1.2.0 中存在两套并行的合并读取体系：

1. **旧体系**：`AbstractHoodieLogRecordScanner` -> `HoodieMergedLogRecordScanner` / `HoodieUnMergedLogRecordScanner`
   - 使用 `ExternalSpillableMap` 存储合并结果
   - 基于 HoodieRecord 层面的合并

2. **新体系**：`BaseHoodieLogRecordReader` -> `HoodieMergedLogRecordReader<T>`
   - 使用 `HoodieFileGroupRecordBuffer<T>` 存储合并结果
   - 基于引擎原生记录类型 `T`（如 Spark 的 `InternalRow`）的合并
   - 由 `HoodieFileGroupReader<T>` 统一编排

### 11.2 HoodieFileGroupReader 的编排

`HoodieFileGroupReader`（`hudi-common/.../read/HoodieFileGroupReader.java`）是面向引擎集成的统一入口。它的核心决策逻辑：

```java
// 是否使用位置合并
readerContext.setShouldMergeUseRecordPosition(
    readerParameters.useRecordPosition()       // 配置开启
    && !isSkipMerge                            // 非 skip_merge 模式
    && readerContext.getHasLogFiles()           // 有 Log File
    && inputSplit.isParquetBaseFile()           // Base File 是 Parquet
);
```

当满足所有条件时，会使用 `PositionBasedFileGroupRecordBuffer` 进行位置合并；否则降级为 `KeyBasedFileGroupRecordBuffer`。

### 11.3 RecordBuffer 的选择与加载

`FileGroupRecordBufferLoader` 负责根据配置选择合适的 RecordBuffer 实现：

```
shouldUseRecordPosition == true  =>  PositionBasedFileGroupRecordBuffer
shouldUseRecordPosition == false =>  KeyBasedFileGroupRecordBuffer
```

`PositionBasedFileGroupRecordBuffer` 继承自 `KeyBasedFileGroupRecordBuffer`，在其基础上增加了按 position 处理 Block 的逻辑，并在 position 不可用时自动降级到 key-based 行为。

---

## 十二、完整的 MOR 读取合并流程

将上述所有组件串联起来，一次完整的 MOR Snapshot Query 读取合并流程如下：

```
1. HoodieFileGroupReader.initRecordIterators()
   |
   +-- 创建 Base File Iterator（读取 Parquet/HFile 基础文件）
   |
   +-- FileGroupRecordBufferLoader.getRecordBuffer()
       |
       +-- 创建 HoodieMergedLogRecordReader
           |
           +-- BaseHoodieLogRecordReader.scanInternal()
               |
               +-- 创建 HoodieLogFormatReader（聚合多个 Log File）
               |       |
               |       +-- HoodieLogFileReader（逐 Block 迭代）
               |               |
               |               +-- readMagic() => readBlock()
               |               +-- Block 完整性校验
               |               +-- 构造 HoodieLogBlock 实例
               |
               +-- 阶段 1：前向扫描，收集 Block 和 Rollback 信息
               +-- 阶段 2：逆序处理 Log Compaction
               +-- processQueuedBlocksForInstant()
                   |
                   +-- processDataBlock() => RecordBuffer.processDataBlock()
                   |       |
                   |       +-- Position-Based: 按行号存入 Map
                   |       +-- Key-Based: 按 recordKey 存入 Map
                   |
                   +-- processDeleteBlock() => RecordBuffer.processDeleteBlock()
   |
   +-- recordBuffer.setBaseFileIterator(baseFileIterator)
   |
2. 迭代输出：
   |
   +-- recordBuffer.hasNext()
       |
       +-- 从 Base File Iterator 取一条记录
       +-- 查找 Log Map 中是否有对应的变更
       +-- 如果有：执行合并（merge / overwrite），输出合并后记录
       +-- 如果没有：直接输出原始记录
       +-- Log Map 中剩余的记录作为纯 INSERT 输出
```

---

## 十三、关键设计思想总结

### 13.1 追加写入 + 延迟合并

Log File 的追加写入模式将随机写入变为顺序写入，写入吞吐量可以达到底层存储的线性带宽。合并开销推迟到读取阶段，通过多种优化（位置合并、懒读取、键过滤）来降低实际开销。

### 13.2 多格式适配

通过 Block Type 抽象，Log File 可以包含 Avro/Parquet/HFile 三种格式的数据 Block。不同的存储格式适用于不同的访问模式（全扫描 vs 点查 vs 列裁剪），且可以在同一个 Log File 中混合使用。

### 13.3 分层溢写

ExternalSpillableMap 的两级存储策略在内存和磁盘之间取得平衡。BitCask 和 RocksDB 两种后端提供了不同的性能特性选择。动态负载估算避免了频繁的精确计算开销。

### 13.4 优雅降级

Position-Based Merge 在不可用时自动降级为 Key-Based Merge，混合策略处理部分降级场景。这种设计确保了新优化不会引入可靠性风险。

### 13.5 容错与恢复

Log File 的 Magic 标记、CRC 校验、双端长度记录（Header + Footer）、损坏 Block 跳过等机制，共同构成了完整的数据可靠性保障体系。即使存储层发生部分损坏，也能最大程度地恢复可用数据。

---

## 十四、核心源码文件索引

| 类名 | 文件路径 | 职责 |
|------|---------|------|
| `HoodieLogFormat` | `hudi-common/.../table/log/HoodieLogFormat.java` | 定义 Log File 的 Magic、版本、Writer/Reader 接口 |
| `HoodieLogFormatVersion` | `hudi-common/.../table/log/HoodieLogFormatVersion.java` | Log Format 版本的 Feature Flags |
| `HoodieLogFormatWriter` | `hudi-hadoop-common/.../table/log/HoodieLogFormatWriter.java` | Log File 的写入实现 |
| `HoodieLogFileReader` | `hudi-common/.../table/log/HoodieLogFileReader.java` | 单文件 Block 级别 Reader |
| `HoodieLogFormatReader` | `hudi-common/.../table/log/HoodieLogFormatReader.java` | 多文件聚合 Reader |
| `HoodieLogBlock` | `hudi-common/.../table/log/block/HoodieLogBlock.java` | Block 基类，定义 Header/Footer 序列化 |
| `HoodieDataBlock` | `hudi-common/.../table/log/block/HoodieDataBlock.java` | 数据 Block 基类 |
| `HoodieAvroDataBlock` | `hudi-common/.../table/log/block/HoodieAvroDataBlock.java` | Avro 格式数据 Block |
| `HoodieParquetDataBlock` | `hudi-common/.../table/log/block/HoodieParquetDataBlock.java` | Parquet 格式数据 Block |
| `HoodieHFileDataBlock` | `hudi-common/.../table/log/block/HoodieHFileDataBlock.java` | HFile 格式数据 Block |
| `HoodieDeleteBlock` | `hudi-common/.../table/log/block/HoodieDeleteBlock.java` | 删除标记 Block |
| `HoodieCommandBlock` | `hudi-common/.../table/log/block/HoodieCommandBlock.java` | 命令 Block（Rollback） |
| `HoodieCorruptBlock` | `hudi-common/.../table/log/block/HoodieCorruptBlock.java` | 损坏数据占位 Block |
| `AbstractHoodieLogRecordScanner` | `hudi-common/.../table/log/AbstractHoodieLogRecordScanner.java` | 扫描框架基类（旧体系） |
| `HoodieMergedLogRecordScanner` | `hudi-common/.../table/log/HoodieMergedLogRecordScanner.java` | 合并 Scanner（旧体系） |
| `HoodieUnMergedLogRecordScanner` | `hudi-common/.../table/log/HoodieUnMergedLogRecordScanner.java` | 不合并 Scanner |
| `HoodieMergedLogRecordReader` | `hudi-common/.../table/log/HoodieMergedLogRecordReader.java` | 合并 Reader（新体系） |
| `BaseHoodieLogRecordReader` | `hudi-common/.../table/log/BaseHoodieLogRecordReader.java` | Log Record Reader 基类（新体系） |
| `ExternalSpillableMap` | `hudi-common/.../util/collection/ExternalSpillableMap.java` | 内存+磁盘两级 Map |
| `BitCaskDiskMap` | `hudi-common/.../util/collection/BitCaskDiskMap.java` | BitCask 磁盘 Map |
| `RocksDbDiskMap` | `hudi-common/.../util/collection/RocksDbDiskMap.java` | RocksDB 磁盘 Map |
| `PositionBasedFileGroupRecordBuffer` | `hudi-common/.../table/read/buffer/PositionBasedFileGroupRecordBuffer.java` | 位置合并 Buffer |
| `KeyBasedFileGroupRecordBuffer` | `hudi-common/.../table/read/buffer/KeyBasedFileGroupRecordBuffer.java` | 键合并 Buffer |
| `HoodieFileGroupReader` | `hudi-common/.../table/read/HoodieFileGroupReader.java` | FileGroup 统一读取入口 |
| `HoodieLogFile` | `hudi-common/.../model/HoodieLogFile.java` | Log File 元数据抽象 |
| `FSUtils` | `hudi-common/.../common/fs/FSUtils.java` | 文件命名与路径工具 |
| `SpillableMapUtils` | `hudi-common/.../util/SpillableMapUtils.java` | BitCask 文件条目序列化工具 |
