# Hudi 元数据结构完整指南

## 1. 元数据系统概述

Hudi 的元数据系统采用分层架构，包括：
- **时间线层**：操作历史管理
- **提交元数据层**：每次提交的详细信息
- **文件组层**：文件的逻辑组织
- **元数据表层**：索引和统计信息
- **版本管理**：向后兼容性支持

---

## 2. 核心数据结构

### 2.1 HoodieInstant（操作时刻）

表示表上的一个操作时刻，是元数据系统的基本单位。

**JSON 表示：**
```json
{
  "timestamp": "20240422101530",
  "action": "commit",
  "state": "COMPLETED",
  "completionTime": "20240422101545"
}
```

**Java 类定义：**
```java
public class HoodieInstant {
  private final String timestamp;           // 操作请求时间 (yyyyMMddHHmmss)
  private final State state;                // 状态: REQUESTED/INFLIGHT/COMPLETED
  private final String action;              // 操作类型: commit/deltacommit/compaction等
  private final String completionTime;      // 完成时间 (V2版本)
}
```

**状态转换流程：**
```
REQUESTED → INFLIGHT → COMPLETED
    ↓
   NIL (无效)
```

---

### 2.2 HoodieCommitMetadata（提交元数据）

记录每次提交操作的详细信息。

**Avro Schema：**
```json
{
  "type": "record",
  "name": "HoodieCommitMetadata",
  "namespace": "org.apache.hudi.common.model",
  "fields": [
    {
      "name": "partitionToWriteStats",
      "type": [
        "null",
        {
          "type": "map",
          "values": {
            "type": "array",
            "items": {
              "type": "record",
              "name": "HoodieWriteStat",
              "fields": [
                {"name": "fileId", "type": "string"},
                {"name": "path", "type": "string"},
                {"name": "prevCommit", "type": ["null", "string"]},
                {"name": "numWrites", "type": "long"},
                {"name": "numUpdateWrites", "type": "long"},
                {"name": "numDeletes", "type": "long"},
                {"name": "totalWriteBytes", "type": "long"},
                {"name": "fileSizeInBytes", "type": "long"},
                {"name": "minEventTime", "type": ["null", "long"]},
                {"name": "maxEventTime", "type": ["null", "long"]},
                {"name": "cdcStats", "type": ["null", "string"]},
                {"name": "totalLogRecords", "type": ["null", "long"]},
                {"name": "totalLogSize", "type": ["null", "long"]}
              ]
            }
          }
        }
      ]
    },
    {
      "name": "compacted",
      "type": ["null", "boolean"],
      "default": null
    },
    {
      "name": "extraMetadata",
      "type": [
        "null",
        {
          "type": "map",
          "values": "string"
        }
      ]
    },
    {
      "name": "version",
      "type": ["null", "int"],
      "default": 1
    },
    {
      "name": "operationType",
      "type": ["null", "string"],
      "default": null
    }
  ]
}
```

**JSON 示例：**
```json
{
  "partitionToWriteStats": {
    "year=2024/month=04/day=22": [
      {
        "fileId": "file-1",
        "path": "year=2024/month=04/day=22/file-1_0-0-0_20240422101530.parquet",
        "prevCommit": null,
        "numWrites": 1000,
        "numUpdateWrites": 0,
        "numDeletes": 0,
        "totalWriteBytes": 102400,
        "fileSizeInBytes": 102400,
        "minEventTime": 1713787530000,
        "maxEventTime": 1713787590000,
        "cdcStats": null,
        "totalLogRecords": null,
        "totalLogSize": null
      }
    ]
  },
  "compacted": false,
  "extraMetadata": {
    "schema": "{\"type\":\"record\",\"name\":\"Record\",\"fields\":[...]}"
  },
  "version": 2,
  "operationType": "UPSERT"
}
```

---

### 2.3 HoodieFileGroup（文件组）

表示一个分区内的文件组，包含多个文件切片。

**结构：**
```json
{
  "fileGroupId": {
    "partitionPath": "year=2024/month=04/day=22",
    "fileId": "file-1"
  },
  "fileSlices": [
    {
      "baseInstantTime": "20240422101530",
      "baseFile": {
        "path": "year=2024/month=04/day=22/file-1_0-0-0_20240422101530.parquet",
        "fileLen": 102400,
        "commitTime": "20240422101530"
      },
      "logFiles": [
        {
          "path": "year=2024/month=04/day=22/.file-1_0-0-0_20240422101545.log",
          "fileLen": 51200,
          "commitTime": "20240422101545"
        }
      ]
    }
  ]
}
```

---

### 2.4 HoodiePartitionMetadata（分区元数据）

存储在每个分区目录下的 `.hoodie_partition_metadata` 文件。

**JSON 格式：**
```json
{
  "commitTime": "20240422101530",
  "partitionDepth": 3
}
```

**文件位置：**
```
table_root/
├── year=2024/
│   ├── month=04/
│   │   ├── day=22/
│   │   │   ├── .hoodie_partition_metadata
│   │   │   ├── file-1_0-0-0_20240422101530.parquet
│   │   │   └── .file-1_0-0-0_20240422101545.log
```

---

## 3. 时间线文件结构

### 3.1 文件名格式

**V1 版本（Hudi 0.x）：**
```
{timestamp}.{action}
{timestamp}.{action}.inflight
{timestamp}.{action}.requested
```

**V2 版本（Hudi 1.x+）：**
```
{timestamp}.{action}
{timestamp}__{completionTime}.{action}
{timestamp}.{action}.inflight
{timestamp}.{action}.requested
```

### 3.2 操作类型和文件扩展名

| 操作类型 | 扩展名 | 说明 | 状态转换 |
|---------|-------|------|--------|
| 提交 | `.commit` | 标准提交 | REQUESTED → INFLIGHT → COMPLETED |
| 增量提交 | `.deltacommit` | MOR表增量提交 | REQUESTED → INFLIGHT → COMPLETED |
| 压缩 | `.compaction` | 压缩操作 | REQUESTED → INFLIGHT → COMPLETED |
| 日志压缩 | `.logcompaction` | 日志压缩 | REQUESTED → INFLIGHT → COMPLETED |
| 聚类 | `.clustering` | 聚类操作 | REQUESTED → INFLIGHT → COMPLETED |
| 清理 | `.clean` | 清理过期文件 | REQUESTED → INFLIGHT → COMPLETED |
| 回滚 | `.rollback` | 回滚操作 | REQUESTED → INFLIGHT → COMPLETED |
| 替换提交 | `.replacecommit` | 替换提交 | REQUESTED → INFLIGHT → COMPLETED |
| 保存点 | `.savepoint` | 保存点 | INFLIGHT → COMPLETED |
| 恢复 | `.restore` | 恢复操作 | REQUESTED → INFLIGHT → COMPLETED |
| 索引 | `.indexing` | 索引操作 | REQUESTED → INFLIGHT → COMPLETED |
| Schema提交 | `.schemacommit` | Schema变更 | REQUESTED → INFLIGHT → COMPLETED |

### 3.3 时间线目录结构

```
table_root/
├── .hoodie/
│   ├── .hoodie_conf/
│   │   └── hoodie.properties
│   ├── .hoodie_metadata/
│   │   ├── 20240422101530.commit
│   │   ├── 20240422101530.commit.inflight
│   │   ├── 20240422101530.commit.requested
│   │   ├── 20240422101545__20240422101600.deltacommit
│   │   ├── 20240422101600.compaction
│   │   ├── 20240422101600.compaction.inflight
│   │   └── 20240422101615__20240422101630.compaction
│   ├── .hoodie_heartbeat/
│   └── .hoodie_aux/
```

---

## 4. 元数据表结构

### 4.1 元数据表分区类型

| 分区类型 | 分区路径 | 记录类型 | 功能 |
|---------|--------|--------|------|
| FILES | `__metadata/files` | 2 | 文件列表索引 |
| ALL_PARTITIONS | `__metadata/files` | 1 | 分区列表 |
| COLUMN_STATS | `__metadata/col-stats` | 3 | 列统计信息 |
| BLOOM_FILTERS | `__metadata/bloom-filters` | 4 | 布隆过滤器 |
| RECORD_INDEX | `__metadata/record-index` | 5 | 记录级索引 |
| PARTITION_STATS | `__metadata/partition-stats` | 6 | 分区统计 |
| SECONDARY_INDEX | `__metadata/secondary-index-*` | 7 | 二级索引 |

### 4.2 HoodieMetadataPayload 结构

**Avro Schema：**
```json
{
  "type": "record",
  "name": "HoodieMetadataRecord",
  "fields": [
    {
      "name": "key",
      "type": "string",
      "doc": "分区路径或文件ID"
    },
    {
      "name": "type",
      "type": "int",
      "doc": "元数据类型: 1=分区列表, 2=文件列表, 3=列统计, 4=布隆过滤器, 5=记录索引, 6=分区统计, 7=二级索引"
    },
    {
      "name": "filesystemMetadata",
      "type": [
        "null",
        {
          "type": "map",
          "values": {
            "type": "record",
            "name": "HoodieMetadataFileInfo",
            "fields": [
              {"name": "size", "type": "long"},
              {"name": "isDeleted", "type": "boolean"}
            ]
          }
        }
      ]
    },
    {
      "name": "BloomFilterMetadata",
      "type": [
        "null",
        {
          "type": "record",
          "name": "HoodieMetadataBloomFilter",
          "fields": [
            {"name": "type", "type": "string"},
            {"name": "timestamp", "type": "string"},
            {"name": "bloomFilter", "type": "bytes"},
            {"name": "isDeleted", "type": "boolean"}
          ]
        }
      ]
    },
    {
      "name": "ColumnStatsMetadata",
      "type": [
        "null",
        {
          "type": "record",
          "name": "HoodieMetadataColumnStats",
          "fields": [
            {"name": "fileName", "type": ["null", "string"]},
            {"name": "columnName", "type": ["null", "string"]},
            {"name": "minValue", "type": ["null", "string"]},
            {"name": "maxValue", "type": ["null", "string"]},
            {"name": "valueCount", "type": ["null", "long"]},
            {"name": "nullCount", "type": ["null", "long"]},
            {"name": "totalSize", "type": ["null", "long"]},
            {"name": "totalUncompressedSize", "type": ["null", "long"]},
            {"name": "isDeleted", "type": "boolean"},
            {"name": "isTightBound", "type": "boolean"}
          ]
        }
      ]
    },
    {
      "name": "recordIndexMetadata",
      "type": [
        "null",
        {
          "type": "record",
          "name": "HoodieRecordIndexInfo",
          "fields": [
            {"name": "partitionName", "type": ["null", "string"]},
            {"name": "fileId", "type": ["null", "string"]},
            {"name": "instantTime", "type": ["null", "long"]},
            {"name": "position", "type": ["null", "long"]}
          ]
        }
      ]
    }
  ]
}
```

**JSON 示例（FILES 分区）：**
```json
{
  "key": "year=2024/month=04/day=22",
  "type": 2,
  "filesystemMetadata": {
    "file-1_0-0-0_20240422101530.parquet": {
      "size": 102400,
      "isDeleted": false
    },
    ".file-1_0-0-0_20240422101545.log": {
      "size": 51200,
      "isDeleted": false
    }
  }
}
```

**JSON 示例（COLUMN_STATS 分区）：**
```json
{
  "key": "year=2024/month=04/day=22/file-1_0-0-0_20240422101530.parquet",
  "type": 3,
  "ColumnStatsMetadata": {
    "fileName": "file-1_0-0-0_20240422101530.parquet",
    "columnName": "user_id",
    "minValue": "1",
    "maxValue": "1000",
    "valueCount": 1000,
    "nullCount": 0,
    "totalSize": 10240,
    "totalUncompressedSize": 20480,
    "isDeleted": false,
    "isTightBound": true
  }
}
```

---

## 5. Schema 演进机制

### 5.1 Schema 存储位置

Schema 存储在提交元数据的 `extraMetadata` 中：

```json
{
  "extraMetadata": {
    "schema": "{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"org.apache.hudi\",\"fields\":[...]}"
  }
}
```

### 5.2 Schema 增删改操作

#### 5.2.1 Schema 添加字段

**操作前的 Schema：**
```json
{
  "type": "record",
  "name": "Record",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"}
  ]
}
```

**操作后的 Schema（添加 email 字段）：**
```json
{
  "type": "record",
  "name": "Record",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}
```

**提交元数据变化：**
```json
{
  "partitionToWriteStats": {...},
  "extraMetadata": {
    "schema": "{新的schema}",
    "schemaVersion": "2"
  },
  "operationType": "UPSERT"
}
```

#### 5.2.2 Schema 删除字段

**操作前的 Schema：**
```json
{
  "type": "record",
  "name": "Record",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "deprecated_field", "type": ["null", "string"]}
  ]
}
```

**操作后的 Schema（删除 deprecated_field）：**
```json
{
  "type": "record",
  "name": "Record",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"}
  ]
}
```

#### 5.2.3 Schema 修改字段类型

**操作前的 Schema：**
```json
{
  "type": "record",
  "name": "Record",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "age", "type": "int"}
  ]
}
```

**操作后的 Schema（age 从 int 改为 long）：**
```json
{
  "type": "record",
  "name": "Record",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "age", "type": "long"}
  ]
}
```

---

## 6. 快照机制

### 6.1 快照的定义

快照是表在某个特定时刻的完整状态，由一个 HoodieInstant 定义。

### 6.2 快照的组成

```json
{
  "snapshotTime": "20240422101530",
  "snapshotAction": "commit",
  "partitions": [
    {
      "partitionPath": "year=2024/month=04/day=22",
      "fileGroups": [
        {
          "fileGroupId": "file-1",
          "fileSlices": [
            {
              "baseInstantTime": "20240422101530",
              "baseFile": {
                "path": "year=2024/month=04/day=22/file-1_0-0-0_20240422101530.parquet",
                "fileLen": 102400
              },
              "logFiles": []
            }
          ]
        }
      ]
    }
  ],
  "schema": "{...}",
  "commitMetadata": {
    "partitionToWriteStats": {...},
    "operationType": "UPSERT"
  }
}
```

### 6.3 快照的读取流程

1. **获取最新 Instant**：从时间线中获取最新的 COMPLETED instant
2. **加载提交元数据**：读取对应的 `.commit` 文件
3. **构建文件视图**：根据提交元数据构建文件组和文件切片
4. **加载 Schema**：从提交元数据的 extraMetadata 中获取 Schema
5. **返回快照**：返回完整的表快照

### 6.4 快照的版本管理

**快照版本链：**
```
Snapshot V1 (20240422101530)
    ↓
Snapshot V2 (20240422101545)
    ↓
Snapshot V3 (20240422101600)
    ↓
Current Snapshot (20240422101615)
```

**快照之间的增量：**
```json
{
  "baseSnapshot": "20240422101530",
  "incrementalChanges": {
    "addedFiles": [
      "year=2024/month=04/day=22/file-2_0-0-0_20240422101545.parquet"
    ],
    "deletedFiles": [],
    "modifiedPartitions": ["year=2024/month=04/day=22"]
  }
}
```

---

## 7. 版本管理

### 7.1 时间线版本

| 版本 | Hudi 版本 | 特性 | 文件名格式 |
|-----|---------|------|---------|
| V0 | < 0.5.1 | 基础版本 | `{timestamp}.{action}` |
| V1 | 0.x | 无重命名 | `{timestamp}.{action}` |
| V2 | 1.x+ | 包含完成时间 | `{timestamp}__{completionTime}.{action}` |

### 7.2 提交元数据版本

| 版本 | 特性 | 序列化器 |
|-----|------|--------|
| V1 | 基础版本 | `CommitMetadataSerDeV1` |
| V2 | 支持更多字段 | `CommitMetadataSerDeV2` |

---

## 8. 实际场景示例

### 场景 1：初始化表并写入数据

**时间线变化：**
```
.hoodie/
├── 20240422101530.commit.requested
├── 20240422101530.commit.inflight
└── 20240422101530.commit
```

**提交元数据内容：**
```json
{
  "partitionToWriteStats": {
    "year=2024/month=04/day=22": [
      {
        "fileId": "file-1",
        "path": "year=2024/month=04/day=22/file-1_0-0-0_20240422101530.parquet",
        "prevCommit": null,
        "numWrites": 1000,
        "numUpdateWrites": 0,
        "numDeletes": 0,
        "totalWriteBytes": 102400,
        "fileSizeInBytes": 102400,
        "minEventTime": 1713787530000,
        "maxEventTime": 1713787590000
      }
    ]
  },
  "compacted": false,
  "extraMetadata": {
    "schema": "{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"}]}"
  },
  "version": 2,
  "operationType": "INSERT"
}
```

### 场景 2：增量更新

**时间线变化：**
```
.hoodie/
├── 20240422101530.commit
├── 20240422101545.deltacommit.requested
├── 20240422101545.deltacommit.inflight
└── 20240422101545.deltacommit
```

**新的提交元数据：**
```json
{
  "partitionToWriteStats": {
    "year=2024/month=04/day=22": [
      {
        "fileId": "file-1",
        "path": "year=2024/month=04/day=22/.file-1_0-0-0_20240422101545.log",
        "prevCommit": "20240422101530",
        "numWrites": 100,
        "numUpdateWrites": 50,
        "numDeletes": 10,
        "totalWriteBytes": 51200,
        "fileSizeInBytes": 51200,
        "minEventTime": 1713787590000,
        "maxEventTime": 1713787650000
      }
    ]
  },
  "compacted": false,
  "extraMetadata": {
    "schema": "{...}"
  },
  "version": 2,
  "operationType": "UPSERT"
}
```

### 场景 3：Schema 演进

**时间线变化：**
```
.hoodie/
├── 20240422101530.commit
├── 20240422101545.deltacommit
└── 20240422101600.schemacommit
```

**Schema 提交元数据：**
```json
{
  "partitionToWriteStats": {},
  "compacted": false,
  "extraMetadata": {
    "schema": "{\"type\":\"record\",\"name\":\"Record\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null}]}",
    "schemaVersion": "2"
  },
  "version": 2,
  "operationType": "SCHEMA_UPDATE"
}
```

### 场景 4：压缩操作

**时间线变化：**
```
.hoodie/
├── 20240422101530.commit
├── 20240422101545.deltacommit
├── 20240422101600.compaction.requested
├── 20240422101600.compaction.inflight
└── 20240422101615__20240422101630.compaction
```

**压缩后的提交元数据：**
```json
{
  "partitionToWriteStats": {
    "year=2024/month=04/day=22": [
      {
        "fileId": "file-1",
        "path": "year=2024/month=04/day=22/file-1_0-0-0_20240422101630.parquet",
        "prevCommit": "20240422101545",
        "numWrites": 1100,
        "numUpdateWrites": 50,
        "numDeletes": 10,
        "totalWriteBytes": 153600,
        "fileSizeInBytes": 153600,
        "minEventTime": 1713787530000,
        "maxEventTime": 1713787650000
      }
    ]
  },
  "compacted": true,
  "extraMetadata": {
    "schema": "{...}"
  },
  "version": 2,
  "operationType": "COMPACT"
}
```

---

## 9. 元数据查询接口

### 9.1 获取表快照

```java
HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
    .setBasePath(tablePath)
    .setConf(hadoopConf)
    .build();

HoodieTimeline timeline = metaClient.getActiveTimeline();
Option<HoodieInstant> latestInstant = timeline.lastInstant();

if (latestInstant.isPresent()) {
  HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(
      timeline.getInstantDetails(latestInstant.get()).get()
  );
  // 获取提交元数据
}
```

### 9.2 获取文件视图

```java
HoodieTableFileSystemView fileSystemView = new HoodieTableFileSystemView(
    metaClient,
    timeline
);

List<HoodieFileGroup> fileGroups = fileSystemView
    .getFileGroupsInPartition("year=2024/month=04/day=22");

for (HoodieFileGroup fileGroup : fileGroups) {
  List<FileSlice> fileSlices = fileGroup.getAllFileSlices().collect(Collectors.toList());
  // 处理文件切片
}
```

### 9.3 获取 Schema

```java
String schemaStr = metadata.getExtraMetadata().get("schema");
Schema schema = new Schema.Parser().parse(schemaStr);
```

---

## 10. 总结

Hudi 的元数据系统是一个分层、版本化的架构：

1. **时间线层**：通过 HoodieInstant 管理操作历史
2. **提交层**：HoodieCommitMetadata 记录每次提交的详细信息
3. **文件层**：HoodieFileGroup 组织文件的逻辑结构
4. **索引层**：元数据表存储各种索引和统计信息
5. **版本层**：支持多个版本，确保向后兼容性

所有元数据都以 JSON 或 Avro 格式存储，支持 Schema 演进、快照管理和增量更新。
