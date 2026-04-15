# Hudi 测试体系与源码阅读实战指南

> Apache Hudi v1.2.0-SNAPSHOT 深度源码分析文档
> 适用于想要深入理解 Hudi 内部机制、掌握测试体系、并最终成为源码专家的开发者

---

## 目录

- [第一部分：源码阅读入门指南](#第一部分源码阅读入门指南)
  - [1. 项目结构导航](#1-项目结构导航)
  - [2. 核心类入口索引](#2-核心类入口索引)
  - [3. 源码阅读路线图](#3-源码阅读路线图)
  - [4. IDE 调试配置](#4-ide-调试配置)
- [第二部分：构建与测试体系](#第二部分构建与测试体系)
  - [5. Maven 构建体系](#5-maven-构建体系)
  - [6. 测试分层体系](#6-测试分层体系)
  - [7. 测试标签（Tag）机制](#7-测试标签tag机制)
- [第三部分：测试基础设施](#第三部分测试基础设施)
  - [8. HoodieCommonTestHarness 与 HoodieWriterClientTestHarness](#8-hoodiecommontestharness-与-hoodiewriterclienttestharness)
  - [9. HoodieTestTable](#9-hoodietesttable)
  - [10. MiniCluster 集成测试](#10-minicluster-集成测试)
  - [11. SparkClientFunctionalTestHarness](#11-sparkclientfunctionaltestharness)
- [第四部分：关键测试用例解读](#第四部分关键测试用例解读)
  - [12. TestHoodieClientOnCopyOnWriteStorage](#12-testhoodieclientoncopyonwritestorage)
  - [13. TestHoodieClientMultiWriter](#13-testhoodieclientmultiwriter)
  - [14. TestHoodieCompactionStrategy](#14-testhoodiecompactionstrategy)
  - [15. TestHoodieBloomIndex](#15-testhoodiebloomindex)
- [第五部分：Checkstyle 与代码规范](#第五部分checkstyle-与代码规范)
  - [16. Checkstyle 规则](#16-checkstyle-规则)
  - [17. Import Control](#17-import-control)
  - [18. Scalastyle 规则](#18-scalastyle-规则)
- [第六部分：RFC 与设计文档](#第六部分rfc-与设计文档)
  - [19. RFC 机制](#19-rfc-机制)
  - [20. 重要 RFC 索引](#20-重要-rfc-索引)
  - [21. 如何从 RFC 理解架构决策](#21-如何从-rfc-理解架构决策)
- [第七部分：贡献者指南](#第七部分贡献者指南)
  - [22. 如何提交 PR](#22-如何提交-pr)
  - [23. 代码审查要点](#23-代码审查要点)
  - [24. 常见构建问题排查](#24-常见构建问题排查)

---

# 第一部分：源码阅读入门指南

## 1. 项目结构导航

### 1.1 顶层 pom.xml 模块声明

Hudi 采用 Maven 多模块项目结构，所有模块均在顶层 `pom.xml` 中声明。

**源码路径**: `pom.xml`（项目根目录）

顶层 pom.xml 中声明的模块如下（按编译顺序排列）：

```xml
<modules>
    <module>hudi-common</module>
    <module>hudi-cli</module>
    <module>hudi-client</module>
    <module>hudi-aws</module>
    <module>hudi-gcp</module>
    <module>hudi-hadoop-common</module>
    <module>hudi-hadoop-mr</module>
    <module>hudi-io</module>
    <module>hudi-spark-datasource</module>
    <module>hudi-timeline-service</module>
    <module>hudi-utilities</module>
    <module>hudi-sync</module>
    <module>packaging/hudi-hadoop-mr-bundle</module>
    <module>packaging/hudi-datahub-sync-bundle</module>
    <module>packaging/hudi-hive-sync-bundle</module>
    <module>packaging/hudi-aws-bundle</module>
    <module>packaging/hudi-gcp-bundle</module>
    <module>packaging/hudi-spark-bundle</module>
    <module>packaging/hudi-presto-bundle</module>
    <module>packaging/hudi-utilities-bundle</module>
    <module>packaging/hudi-utilities-slim-bundle</module>
    <module>packaging/hudi-timeline-server-bundle</module>
    <module>packaging/hudi-trino-bundle</module>
    <module>hudi-examples</module>
    <module>hudi-flink-datasource</module>
    <module>hudi-kafka-connect</module>
    <module>packaging/hudi-flink-bundle</module>
    <module>packaging/hudi-kafka-connect-bundle</module>
    <module>packaging/hudi-cli-bundle</module>
    <module>hudi-tests-common</module>
</modules>
```

**为什么这么设计？** Maven 按照声明顺序和依赖关系确定编译顺序。Hudi 将最底层的模块（如 `hudi-io`、`hudi-common`）放在前面，确保被依赖的模块先编译。打包模块（`packaging/`）放在最后，因为它们需要将所有上游模块的 class 文件打包成 uber-jar。

**好处**：
- 清晰的分层结构使得开发者可以只构建关心的模块（`-pl <module> -am`）
- Bundle 打包与业务代码完全分离，避免了打包逻辑污染核心代码
- 各引擎适配模块（Spark/Flink）互相独立，不会交叉污染

### 1.2 模块依赖关系图

Hudi 的模块依赖关系可以理解为以下层级（从下到上）：

```
第0层（基础层）:
  hudi-io                   -- 最底层 I/O 抽象，定义存储接口和公共注解

第1层（公共层）:
  hudi-common               -- 依赖 hudi-io，提供配置、Avro、元数据、时间线等公共能力
  hudi-hadoop-common         -- 依赖 hudi-common，提供 Hadoop FileSystem 兼容层

第2层（客户端核心层）:
  hudi-client/hudi-client-common  -- 依赖 hudi-common，引擎无关的写入客户端核心
  hudi-hadoop-mr                  -- 依赖 hudi-hadoop-common，MapReduce InputFormat

第3层（引擎绑定层）:
  hudi-client/hudi-spark-client   -- 依赖 client-common + Spark
  hudi-client/hudi-flink-client   -- 依赖 client-common + Flink
  hudi-client/hudi-java-client    -- 依赖 client-common，纯 Java 实现

第4层（数据源层）:
  hudi-spark-datasource/hudi-spark-common    -- Spark 公共数据源代码
  hudi-spark-datasource/hudi-spark3-common   -- Spark 3.x 公共代码
  hudi-spark-datasource/hudi-spark3.5.x      -- Spark 3.5 特定实现
  hudi-spark-datasource/hudi-spark           -- Spark 统一入口（SQL 扩展等）
  hudi-flink-datasource/hudi-flink           -- Flink 公共实现
  hudi-flink-datasource/hudi-flink1.20.x     -- Flink 1.20 特定适配

第5层（同步/工具层）:
  hudi-sync/hudi-sync-common      -- 目录同步抽象
  hudi-sync/hudi-hive-sync        -- Hive Metastore 同步
  hudi-utilities                   -- DeltaStreamer 等数据摄取工具
  hudi-timeline-service            -- Timeline HTTP 服务
  hudi-cli                         -- 命令行管理工具

第6层（打包层）:
  packaging/hudi-spark-bundle      -- Spark uber-jar
  packaging/hudi-flink-bundle      -- Flink uber-jar
  packaging/hudi-utilities-bundle  -- Utilities uber-jar
  ...其他 bundle
```

**为什么这么设计？** 这种分层设计遵循了"高内聚、低耦合"的原则：
- **hudi-io** 作为最底层模块，不依赖任何 Hudi 模块，只定义接口。这保证了 I/O 层的稳定性。
- **hudi-common** 汇聚了所有引擎共用的逻辑，避免在各引擎模块中重复实现。
- **client-common** 层实现了引擎无关的核心算法（索引、压缩、聚簇），各引擎客户端只需要实现引擎特定的"胶水代码"。
- **packaging** 层完全是部署关注点，与业务逻辑零耦合。

**好处**：
- 新增引擎支持只需在第3层新增一个客户端模块
- 核心算法修改自动对所有引擎生效
- Bundle 打包策略可以灵活调整而不影响核心代码

### 1.3 关键目录结构说明

```
hudi/
├── hudi-io/                           # I/O 抽象层
│   └── src/main/java/org/apache/hudi/
│       ├── io/                        # 存储读写接口
│       └── annotation/                # @PublicAPIClass, @PublicAPIMethod
├── hudi-common/                       # 公共库
│   └── src/main/java/org/apache/hudi/common/
│       ├── config/                    # 所有配置类：HoodieMetadataConfig, HoodieStorageConfig...
│       ├── model/                     # 数据模型：HoodieRecord, HoodieKey, FileSlice...
│       ├── table/                     # 表抽象：HoodieTableMetaClient, HoodieTableConfig
│       │   ├── timeline/              # 时间线：HoodieTimeline, HoodieInstant
│       │   ├── view/                  # 文件系统视图：HoodieTableFileSystemView
│       │   └── log/                   # 日志文件读写：HoodieLogFormat
│       ├── bloom/                     # Bloom Filter 实现
│       ├── fs/                        # 文件系统工具类
│       └── util/                      # 通用工具类
├── hudi-client/
│   ├── hudi-client-common/            # 引擎无关客户端核心
│   │   └── src/main/java/org/apache/hudi/
│   │       ├── client/                # BaseHoodieWriteClient, BaseHoodieTableServiceClient
│   │       ├── config/                # HoodieWriteConfig, HoodieIndexConfig...
│   │       ├── index/                 # 索引实现：HoodieIndex, HoodieBloomIndex...
│   │       ├── io/                    # I/O Handle：HoodieCreateHandle, HoodieMergeHandle
│   │       └── table/
│   │           └── action/            # 表操作执行器
│   │               ├── commit/        # 写入提交
│   │               ├── compact/       # Compaction
│   │               ├── clean/         # 清理
│   │               ├── cluster/       # 聚簇
│   │               └── rollback/      # 回滚
│   ├── hudi-spark-client/             # Spark 引擎绑定
│   ├── hudi-flink-client/             # Flink 引擎绑定
│   └── hudi-java-client/              # 纯 Java 引擎绑定
├── hudi-spark-datasource/             # Spark 数据源
│   ├── hudi-spark-common/             # Spark 公共代码
│   ├── hudi-spark3-common/            # Spark 3 公共代码
│   ├── hudi-spark3.3.x/              # Spark 3.3 适配
│   ├── hudi-spark3.4.x/              # Spark 3.4 适配
│   ├── hudi-spark3.5.x/              # Spark 3.5 适配（默认）
│   ├── hudi-spark4.0.x/              # Spark 4.0 适配
│   └── hudi-spark/                    # Spark 统一入口
├── hudi-flink-datasource/             # Flink 数据源
│   ├── hudi-flink/                    # Flink 公共实现
│   ├── hudi-flink1.17.x ~ hudi-flink2.1.x/  # Flink 版本适配
├── style/                             # 代码规范
│   ├── checkstyle.xml                 # Java Checkstyle 规则
│   ├── checkstyle-suppressions.xml    # Checkstyle 抑制规则
│   ├── import-control.xml             # 模块间导入约束
│   └── scalastyle.xml                 # Scala 代码规范
├── rfc/                               # RFC 设计文档
│   ├── rfc-8/                         # Record Level Index
│   ├── rfc-46/                        # Optimize Record Payload
│   ├── rfc-56/                        # Early Conflict Detection
│   ├── rfc-69/                        # Hudi 1.X
│   └── ...
└── packaging/                         # 各种 uber-jar bundle
    ├── hudi-spark-bundle/
    ├── hudi-flink-bundle/
    └── ...
```

---

## 2. 核心类入口索引

对于源码阅读来说，最关键的是知道"从哪里开始读"。以下是按功能分类的入口类清单。

### 2.1 写入流程入口

| 功能 | 入口类 | 源码路径 |
|------|--------|----------|
| **写入客户端（引擎无关）** | `BaseHoodieWriteClient` | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieWriteClient.java` |
| **Spark 写入客户端** | `SparkRDDWriteClient` | `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/SparkRDDWriteClient.java` |
| **表服务客户端** | `BaseHoodieTableServiceClient` | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieTableServiceClient.java` |
| **写入配置** | `HoodieWriteConfig` | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieWriteConfig.java` |

**为什么 BaseHoodieWriteClient 是核心入口？** 因为所有写入操作（insert、upsert、delete、bulkInsert）的统一 API 都定义在这个类中。无论使用 Spark 还是 Flink，最终都会通过这个抽象类的方法来执行写入。`SparkRDDWriteClient` 只是它的 Spark 具体实现。

**好处**：统一的 API 设计使得不同引擎的用户学习成本一致，核心写入逻辑的修改也只需要在一个地方进行。

### 2.2 表操作入口

| 功能 | 入口类 | 源码路径 |
|------|--------|----------|
| **COW 表操作（Spark）** | `HoodieSparkCopyOnWriteTable` | `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/HoodieSparkCopyOnWriteTable.java` |
| **MOR 表操作（Spark）** | `HoodieSparkMergeOnReadTable` | `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/HoodieSparkMergeOnReadTable.java` |
| **Compaction 执行器** | `RunCompactionActionExecutor` | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/RunCompactionActionExecutor.java` |
| **Clean 执行器** | `CleanActionExecutor` | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/clean/CleanActionExecutor.java` |
| **Clustering 计划执行器** | `ClusteringPlanActionExecutor` | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/cluster/ClusteringPlanActionExecutor.java` |

**为什么 COW 和 MOR 是独立的类？** 因为 Copy-On-Write 和 Merge-On-Read 的写入策略从根本上不同。COW 在写入时就完成 merge，每次写入都生成新的 base file。MOR 则将更新写入 log file，在读取时才 merge。两种策略在 insert/upsert/compact 的行为上有本质差异，需要独立的实现类。

**好处**：清晰分离两种表类型的行为，让开发者可以专注于理解一种模式的完整流程，同时也便于各自独立演进。

### 2.3 元数据与时间线入口

| 功能 | 入口类 | 源码路径 |
|------|--------|----------|
| **表元数据客户端** | `HoodieTableMetaClient` | `hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTableMetaClient.java` |
| **表配置** | `HoodieTableConfig` | `hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTableConfig.java` |
| **时间线** | `HoodieTimeline` | `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieTimeline.java` |
| **Instant** | `HoodieInstant` | `hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieInstant.java` |
| **文件系统视图** | `HoodieTableFileSystemView` | `hudi-common/src/main/java/org/apache/hudi/common/table/view/HoodieTableFileSystemView.java` |

**为什么 HoodieTableMetaClient 如此重要？** 这是 Hudi 表的"身份证"，提供了访问表元数据（表名、表类型、时间线、配置）的统一入口。几乎所有操作在开始前都需要先构建一个 MetaClient。

**好处**：将所有表级元信息聚合到一个类中，避免了散落的元数据访问逻辑，也为不同引擎提供了统一的元数据读取方式。

### 2.4 索引入口

| 功能 | 入口类 | 源码路径 |
|------|--------|----------|
| **索引抽象** | `HoodieIndex` | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/HoodieIndex.java` |
| **Bloom Index** | `HoodieBloomIndex` | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/bloom/HoodieBloomIndex.java` |
| **Record Level Index** | 参见 RFC-8 | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/` |

### 2.5 测试数据生成入口

| 功能 | 入口类 | 源码路径 |
|------|--------|----------|
| **测试数据生成器** | `HoodieTestDataGenerator` | `hudi-common/src/test/java/org/apache/hudi/common/testutils/HoodieTestDataGenerator.java` |
| **测试表构建器** | `HoodieTestTable` | `hudi-hadoop-common/src/test/java/org/apache/hudi/common/testutils/HoodieTestTable.java` |
| **Spark 可写测试表** | `HoodieSparkWriteableTestTable` | `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/testutils/HoodieSparkWriteableTestTable.java` |

---

## 3. 源码阅读路线图

### 3.1 推荐阅读顺序

源码阅读应当遵循"由简入繁、由静到动、由抽象到实现"的原则。以下是推荐的阅读路径：

**第一阶段：理解数据模型（预计 2-3 天）**

1. **HoodieRecord / HoodieKey** -- 理解 Hudi 最基本的数据单元
   - 路径：`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieRecord.java`
   - 重点：理解 record key + partition path 如何唯一标识一条记录

2. **HoodieTableType** -- 理解 COW vs MOR 的区别
   - 路径：`hudi-common/src/main/java/org/apache/hudi/common/model/HoodieTableType.java`

3. **FileSlice / HoodieBaseFile / HoodieLogFile** -- 理解文件组织方式
   - 路径：`hudi-common/src/main/java/org/apache/hudi/common/model/`
   - 重点：理解 FileGroup -> FileSlice -> BaseFile + LogFile 的层次结构

4. **HoodieTableConfig** -- 理解表级配置
   - 路径：`hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTableConfig.java`

**第二阶段：理解时间线（预计 2-3 天）**

5. **HoodieInstant** -- 理解时间线上的"事件"
   - 路径：`hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieInstant.java`
   - 重点：Requested -> Inflight -> Completed 三态转换

6. **HoodieTimeline** -- 理解时间线操作
   - 路径：`hudi-common/src/main/java/org/apache/hudi/common/table/timeline/HoodieTimeline.java`
   - 重点：filterCompletedInstants()、getInstants() 等过滤方法

7. **HoodieTableMetaClient** -- 理解表的元数据入口
   - 路径：`hudi-common/src/main/java/org/apache/hudi/common/table/HoodieTableMetaClient.java`

**第三阶段：理解写入流程（预计 3-5 天）**

8. **HoodieWriteConfig** -- 理解写入配置
   - 路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieWriteConfig.java`

9. **BaseHoodieWriteClient** -- 理解写入 API
   - 路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/BaseHoodieWriteClient.java`
   - 重点：insert()、upsert()、delete() 方法链路

10. **HoodieIndex** -- 理解如何通过索引定位记录
    - 路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/index/HoodieIndex.java`

11. **HoodieSparkCopyOnWriteTable** -- 理解 COW 写入实现
    - 路径：`hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/table/HoodieSparkCopyOnWriteTable.java`

**第四阶段：理解表服务（预计 3-5 天）**

12. **RunCompactionActionExecutor** -- 理解 Compaction
    - 路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/compact/RunCompactionActionExecutor.java`

13. **CleanActionExecutor** -- 理解清理机制
    - 路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/clean/CleanActionExecutor.java`

14. **ClusteringPlanActionExecutor** -- 理解聚簇
    - 路径：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/action/cluster/ClusteringPlanActionExecutor.java`

**第五阶段：理解测试体系（贯穿全程）**

15. 与上述每个阶段并行，阅读对应的测试类，通过测试用例理解行为

### 3.2 从测试入手的阅读技巧

**为什么强烈建议从测试代码开始阅读？**

- 测试是"活的文档"，每个测试方法都描述了一个具体的行为场景
- 测试提供了最小化的使用示例，剥离了复杂的集群环境
- 测试中的 assert 语句精确定义了"预期行为是什么"
- 测试的 setUp/tearDown 展示了初始化一个 Hudi 表需要哪些步骤

**好处**：通过测试理解行为，再回到实现代码看"如何做到"，比直接啃实现代码效率高 3-5 倍。

---

## 4. IDE 调试配置

### 4.1 IntelliJ IDEA 导入项目

1. **打开项目**：File -> Open -> 选择 Hudi 根目录
2. **Maven 识别**：IDEA 会自动识别 pom.xml，等待索引完成（首次可能需要 10-20 分钟）
3. **JDK 配置**：File -> Project Structure -> Project SDK -> 选择 JDK 11 或 17
4. **Maven Profile 配置**：在 IDEA 右侧 Maven 面板中，勾选需要的 Profile（如 `spark3.5`, `flink1.20`, `scala-2.12`）

### 4.2 运行单个测试

**方法一：IDEA 直接运行**

在测试类中，点击测试方法左边的绿色运行按钮即可。需要确保：
- VM Options 中添加：`-Xmx3g -Xms128m -XX:-OmitStackTraceInFastThrow`
- Working Directory 设置为项目根目录

**方法二：Maven 命令行**

```bash
# 运行单个测试类
mvn test -pl hudi-common -Dtest=TestHoodieTimer

# 运行单个测试方法
mvn test -pl hudi-common -Dtest=TestHoodieTimer#testTimer

# 运行 Spark 相关测试（需要指定 Spark 版本 Profile）
mvn test -pl hudi-spark-datasource/hudi-spark -Dspark3.5 \
    -Dtest=TestHoodieClientOnCopyOnWriteStorage#testAutoCommitOnInsert
```

### 4.3 调试 Spark 测试

Spark 测试会在本地启动一个迷你 SparkSession。在 IDEA 中调试时，需注意：

1. **增加内存**：在 Run/Debug Configuration 中设置 VM Options: `-Xmx3g -Xms128m`
2. **设置断点**：在目标代码处设置断点，从测试方法开始 Debug
3. **注意 Spark 的延迟执行**：很多操作在 `collect()` 或 `count()` 时才真正执行
4. **日志配置**：测试使用 `log4j2-surefire.properties` 配置日志（由 pom.xml 中 `surefire-log4j.file` 属性指定）

### 4.4 调试技巧

- **Conditional Breakpoint**：对于循环中的调试，使用条件断点过滤特定记录
- **Evaluate Expression**：利用 IDEA 的表达式求值功能查看中间变量
- **Remote Debug**：对于需要真实 Spark 集群的场景，使用 `JAVA_TOOL_OPTIONS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005` 开启远程调试

---

# 第二部分：构建与测试体系

## 5. Maven 构建体系

### 5.1 Profile 机制详解

Hudi 使用 Maven Profile 来支持多版本构建。所有 Profile 定义在顶层 `pom.xml` 中。

**源码路径**: `pom.xml`（约2500-2800行的 `<profiles>` 段落）

#### Spark 版本 Profile

| Profile ID | Spark 版本 | 激活方式 | 关键属性 |
|------------|-----------|---------|---------|
| `spark3.3` | 3.3.4 | `-Dspark3.3` | `hudi.spark.module=hudi-spark3.3.x`, `scala.binary.version=2.12` |
| `spark3.4` | 3.4.3 | `-Dspark3.4` | `hudi.spark.module=hudi-spark3.4.x`, `scala.binary.version=2.12` |
| `spark3.5` | 3.5.5 (默认) | `-Dspark3.5` | `hudi.spark.module=hudi-spark3.5.x`, `scala.binary.version=2.12` |
| `spark4.0` | 4.0.1 | `-Dspark4.0` | `hudi.spark.module=hudi-spark4.0.x`, `scala.binary.version=2.13`, 需 Java 17 |

**Spark 3.3 Profile 的实际配置**（从源码摘取）：

```xml
<profile>
  <id>spark3.3</id>
  <properties>
    <spark3.version>${spark33.version}</spark3.version>    <!-- 3.3.4 -->
    <spark.version>${spark3.version}</spark.version>
    <sparkbundle.version>3.3</sparkbundle.version>
    <scala12.version>2.12.15</scala12.version>
    <scala.version>${scala12.version}</scala.version>
    <scala.binary.version>2.12</scala.binary.version>
    <hudi.spark.module>hudi-spark3.3.x</hudi.spark.module>
    <hudi.spark.common.module>hudi-spark3-common</hudi.spark.common.module>
  </properties>
</profile>
```

**为什么需要 Profile 机制？** Spark/Flink 各版本之间存在 API 差异（特别是 Spark 3.x 到 4.0 的大版本升级）。通过 Profile 切换，编译系统可以选择性地编译对应版本的适配模块（如 `hudi-spark3.5.x` 或 `hudi-spark4.0.x`），而核心模块的代码保持不变。

**好处**：
- 一套源码支持多个引擎版本，避免了分支管理的噩梦
- 开发者只需关心自己的目标版本，不需要理解所有版本的差异
- CI 系统可以并行构建所有版本组合，确保兼容性

#### Flink 版本 Profile

| Profile ID | Flink 版本 | 激活方式 |
|------------|-----------|---------|
| `flink1.17` | 1.17.1 | `-Dflink1.17` |
| `flink1.18` | 1.18.1 | `-Dflink1.18` |
| `flink1.19` | 1.19.2 | `-Dflink1.19` |
| `flink1.20` | 1.20.1 (默认) | `-Dflink1.20` |
| `flink2.0` | 2.0.0 | `-Dflink2.0` |
| `flink2.1` | 2.1.1 | `-Dflink2.1` |

#### Scala 版本 Profile

| Profile ID | Scala 版本 | 说明 |
|------------|-----------|------|
| `scala-2.12` | 2.12.15 (默认) | 适用于 Spark 3.x |
| `scala-2.13` | 2.13.8 | 仅限 Spark 3.5+ 和 Spark 4.0 |

### 5.2 常用构建命令

```bash
# 默认构建（Spark 3.5 + Flink 1.20 + Scala 2.12）
mvn clean package -DskipTests -Dspark3.5 -Dflink1.20

# 仅构建某个模块（-pl 指定模块，-am 构建其依赖）
mvn clean package -DskipTests -pl hudi-common -am

# 构建 Spark 客户端
mvn clean package -DskipTests -pl hudi-client/hudi-spark-client -am

# 构建 Spark 4.0 版本（注意需要 Java 17）
mvn clean package -DskipTests -Dspark4.0

# 构建集成测试 bundle
mvn clean package -DskipTests -Dintegration-tests

# 生成 Javadoc
mvn javadoc:aggregate -Pjavadocs
```

**为什么推荐使用 `-pl <module> -am`？** 完整构建 Hudi 项目需要 15-30 分钟。如果你只修改了 `hudi-common` 中的代码，使用 `-pl hudi-common -am` 可以将构建时间缩短到 2-3 分钟。`-am` (also-make) 参数确保该模块的上游依赖也会被编译。

**好处**：极大加速开发迭代周期，尤其在日常开发中效果显著。

### 5.3 Surefire 与 Failsafe 插件配置

Hudi 使用两个 Maven 测试插件：

- **maven-surefire-plugin**（版本 3.5.4）: 执行单元测试和功能测试
- **maven-failsafe-plugin**（版本 3.5.4）: 执行集成测试

关键的 Surefire 配置（摘自 pom.xml）：

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-surefire-plugin</artifactId>
  <version>${maven-surefire-plugin.version}</version>
  <configuration>
    <rerunFailingTestsCount>3</rerunFailingTestsCount>  <!-- 失败自动重试 3 次 -->
    <argLine>@{argLine}</argLine>
    <trimStackTrace>false</trimStackTrace>               <!-- 不裁剪异常栈 -->
    <systemPropertyVariables>
      <log4j.configurationFile>${surefire-log4j.file}</log4j.configurationFile>
    </systemPropertyVariables>
    <useSystemClassLoader>false</useSystemClassLoader>
    <forkedProcessExitTimeoutInSeconds>30</forkedProcessExitTimeoutInSeconds>
  </configuration>
  <dependencies>
    <dependency>
      <groupId>org.apache.maven.surefire</groupId>
      <artifactId>surefire-junit-platform</artifactId>  <!-- JUnit 5 支持 -->
    </dependency>
  </dependencies>
</plugin>
```

**为什么设置 `rerunFailingTestsCount=3`？** 大数据系统的测试往往涉及并发、I/O 和时间依赖，偶尔会因为资源竞争或时序问题而 flaky。自动重试机制可以减少因偶发失败导致的 CI 误报，同时不会掩盖真正的 bug（如果一个测试连续失败 3 次，几乎可以确定存在真实问题）。

**好处**：显著减少了开发者因 flaky test 而反复触发 CI 的时间成本。

---

## 6. 测试分层体系

### 6.1 三层测试架构

Hudi 将测试分为三个层级，每个层级有独立的 Maven Profile 和运行方式。

```
┌─────────────────────────────────────────────────────────┐
│                   集成测试 (Integration Tests)            │
│  运行方式: mvn -Pintegration-tests verify                 │
│  特点: 需要 Docker/MiniCluster，运行时间最长               │
│  文件命名: IT*.java                                       │
│  Profile 属性: skipITs                                    │
├─────────────────────────────────────────────────────────┤
│                   功能测试 (Functional Tests)              │
│  运行方式: mvn -Pfunctional-tests test                    │
│  特点: 标记 @Tag("functional")，需要 Spark/Flink 环境      │
│  Profile 属性: skipFTs                                    │
│  分组: functional, functional-b, functional-c             │
├─────────────────────────────────────────────────────────┤
│                   单元测试 (Unit Tests)                    │
│  运行方式: mvn -Punit-tests test                          │
│  特点: 不需要外部依赖，运行速度最快                         │
│  Profile 属性: skipUTs                                    │
│  排除: @Tag("functional") 标记的测试                       │
└─────────────────────────────────────────────────────────┘
```

**为什么要分三层？**

- **单元测试**：验证单个类或方法的行为，不依赖外部系统。运行快（几秒到几分钟），每次提交代码前都应该运行。
- **功能测试**：验证跨类/跨模块的协作行为，需要 Spark/Flink 等引擎环境。运行慢一些（几分钟到几十分钟），在 PR 合并前运行。
- **集成测试**：验证端到端的完整场景，需要 HDFS/Hive/Docker 等外部系统。运行最慢（几十分钟到几小时），在发版前运行。

**好处**：开发者可以根据修改范围选择合适的测试层级，在快速反馈和全面覆盖之间取得平衡。

### 6.2 测试跳过机制

pom.xml 中定义了多个测试跳过属性：

```xml
<properties>
    <skipTests>false</skipTests>        <!-- 跳过所有测试 -->
    <skipUTs>${skipTests}</skipUTs>      <!-- 跳过单元测试 -->
    <skipFTs>${skipTests}</skipFTs>      <!-- 跳过功能测试 -->
    <skipITs>${skipTests}</skipITs>      <!-- 跳过集成测试 -->
</properties>
```

**使用示例**：

```bash
# 跳过所有测试（用于纯编译）
mvn clean package -DskipTests

# 只跳过功能测试和集成测试（只运行单元测试）
mvn clean test -DskipFTs=true -DskipITs=true

# 等价写法：使用 Profile
mvn clean test -Punit-tests
```

### 6.3 JVM 参数

测试执行时的 JVM 参数在 pom.xml 中定义：

```xml
<argLine>-Xmx3g -Xms128m -XX:-OmitStackTraceInFastThrow</argLine>
```

- `-Xmx3g`：最大堆内存 3GB，因为 Spark 测试需要在单 JVM 中运行 driver + executor
- `-Xms128m`：初始堆内存 128MB
- `-XX:-OmitStackTraceInFastThrow`：禁止 JVM 优化掉异常栈信息，确保调试时能看到完整的异常链

**为什么需要 3g 内存？** Spark 测试在本地模式下运行，driver 和 executor 共享同一个 JVM。Hudi 测试经常涉及大量 Avro 记录的序列化/反序列化和 Parquet 文件读写，这些操作内存消耗较大。3GB 是经过实践验证的最小安全值。

---

## 7. 测试标签（Tag）机制

### 7.1 @Tag 注解的使用

Hudi 使用 JUnit 5 的 `@Tag` 注解来标记不同层级的测试。目前使用的标签有：

- `@Tag("functional")` -- 功能测试（最常用，项目中约有 59 个测试类使用此标签）
- `@Tag("functional-b")` -- 功能测试 B 分组
- `@Tag("functional-c")` -- 功能测试 C 分组

**示例代码**（摘自 `TestHoodieClientOnCopyOnWriteStorage.java`）：

```java
@Tag("functional")
public class TestHoodieClientOnCopyOnWriteStorage extends HoodieClientTestBase {
    // ...
}
```

```java
@Tag("functional")
public class TestHoodieClientMultiWriter extends HoodieClientTestBase {
    // ...
}
```

### 7.2 Profile 如何过滤标签

**unit-tests Profile 配置**（摘自 pom.xml 第2046行）：

```xml
<profile>
  <id>unit-tests</id>
  <properties>
    <skipUTs>false</skipUTs>
    <skipFTs>true</skipFTs>
    <skipITs>true</skipITs>
  </properties>
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <configuration combine.self="append">
          <skip>${skipUTs}</skip>
          <forkedProcessExitTimeoutInSeconds>120</forkedProcessExitTimeoutInSeconds>
          <excludedGroups>functional,functional-b,functional-c</excludedGroups>
          <excludes>
            <exclude>**/IT*.java</exclude>
            <exclude>**/testsuite/**/Test*.java</exclude>
          </excludes>
        </configuration>
      </plugin>
    </plugins>
  </build>
</profile>
```

**关键配置解析**：
- `<excludedGroups>functional,functional-b,functional-c</excludedGroups>` -- 排除所有带 `@Tag("functional")` 等标签的测试
- `<excludes>` -- 排除集成测试（IT*.java）和测试套件

**functional-tests Profile 配置**（摘自 pom.xml 第2097行）：

```xml
<profile>
  <id>functional-tests</id>
  <properties>
    <skipUTs>true</skipUTs>
    <skipFTs>false</skipFTs>
    <skipITs>true</skipITs>
  </properties>
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <configuration combine.self="append">
          <skip>${skipFTs}</skip>
          <forkCount>1</forkCount>
          <reuseForks>true</reuseForks>
          <groups>functional</groups>
        </configuration>
      </plugin>
    </plugins>
  </build>
</profile>
```

**关键配置解析**：
- `<groups>functional</groups>` -- 只运行带 `@Tag("functional")` 标签的测试
- `<forkCount>1</forkCount>` + `<reuseForks>true</reuseForks>` -- 只使用 1 个 fork 进程并复用，避免 SparkSession 反复创建/销毁

**为什么功能测试要拆分成 functional/functional-b/functional-c 三组？** 功能测试通常运行时间较长（涉及 Spark/Flink 环境初始化和实际数据操作）。拆分成多组允许 CI 系统并行运行，显著缩短整体 CI 时间。

**好处**：CI 流水线可以将三组功能测试分配到不同的 CI runner 上并行执行，将原本可能需要 2-3 小时的功能测试缩短到 1 小时内。

### 7.3 integration-tests Profile

```xml
<profile>
  <id>integration-tests</id>
  <activation>
    <property>
      <name>deployArtifacts</name>
      <value>true</value>
    </property>
  </activation>
  <modules>
    <module>docker/hoodie/hadoop</module>
    <module>hudi-integ-test</module>
    <module>packaging/hudi-integ-test-bundle</module>
  </modules>
  <properties>
    <skipUTs>true</skipUTs>
    <skipFTs>true</skipFTs>
    <skipITs>${skipTests}</skipITs>
  </properties>
</profile>
```

**注意**：集成测试 Profile 会额外引入 `docker/hoodie/hadoop`、`hudi-integ-test` 等模块，这些模块在默认构建中不包含。集成测试使用 `maven-failsafe-plugin` 执行，遵循 `IT*.java` 命名规范。

---

# 第三部分：测试基础设施

## 8. HoodieCommonTestHarness 与 HoodieWriterClientTestHarness

### 8.1 HoodieCommonTestHarness -- 最基础的测试工具

**源码路径**: `hudi-hadoop-common/src/test/java/org/apache/hudi/common/testutils/HoodieCommonTestHarness.java`

这是 Hudi 测试体系中最底层的基类，提供了所有测试共需的基本能力。

```java
@Slf4j
public class HoodieCommonTestHarness {

  protected static final String BASE_FILE_EXTENSION =
      HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension();
  protected static ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = null;
  protected static final HoodieLogBlock.HoodieLogBlockType DEFAULT_DATA_BLOCK_TYPE =
      HoodieLogBlock.HoodieLogBlockType.AVRO_DATA_BLOCK;

  @Setter(AccessLevel.PROTECTED)
  protected String tableName;
  protected String basePath;
  protected URI baseUri;
  protected HoodieTestDataGenerator dataGen;
  protected HoodieTableMetaClient metaClient;
  private HoodieEngineContext engineContext;
  @TempDir
  public java.nio.file.Path tempDir;

  protected StorageConfiguration<Configuration> storageConf;
  // ... 初始化方法
}
```

**提供的关键能力**：

1. **临时目录管理**：通过 JUnit 5 的 `@TempDir` 注解自动创建和清理测试目录。每个测试方法都有独立的临时目录，避免测试间互相干扰。

2. **MetaClient 构建**：提供 `initMetaClient()` 方法自动初始化表的元数据客户端。

3. **数据生成器**：内置 `HoodieTestDataGenerator` 实例，用于生成测试用的 Avro 记录。

4. **文件系统视图**：提供创建 `HoodieTableFileSystemView` 的方法，用于验证写入后的文件状态。

5. **日志文件操作**：提供写入和读取 Hudi Log File 的工具方法，用于 MOR 表测试。

**为什么要设计这个基类？** 几乎所有 Hudi 测试都需要：创建临时目录 -> 初始化 MetaClient -> 生成测试数据 -> 执行操作 -> 验证结果 -> 清理资源。将这些公共步骤抽取到基类中，避免了每个测试类中重复编写 50-100 行的样板代码。

**好处**：新增测试时只需关注"测试什么"，而不需要操心"如何初始化环境"。同时，如果环境初始化逻辑需要调整（如 API 变更），只需修改基类即可。

### 8.2 HoodieWriterClientTestHarness -- 写入客户端测试的核心基类

**源码路径**: `hudi-client/hudi-client-common/src/test/java/org/apache/hudi/utils/HoodieWriterClientTestHarness.java`

这是所有写入客户端测试的引擎无关基类，继承自 `HoodieCommonTestHarness`。

```java
public abstract class HoodieWriterClientTestHarness extends HoodieCommonTestHarness {
  protected static int timelineServicePort =
      FileSystemViewStorageConfig.REMOTE_PORT_NUM.defaultValue();
  protected static final String CLUSTERING_FAILURE = "CLUSTERING FAILURE";
  protected static final String CLEANING_FAILURE = "CLEANING FAILURE";

  protected HoodieTestTable testTable;

  protected abstract BaseHoodieWriteClient getHoodieWriteClient(HoodieWriteConfig cfg);
  protected abstract BaseHoodieWriteClient getHoodieWriteClient(
      HoodieWriteConfig cfg, boolean shouldCloseOlderClient);
  // ...
}
```

**提供的关键能力**：

1. **WriteClient 工厂方法**：抽象方法 `getHoodieWriteClient()`，由各引擎子类实现（Spark/Flink/Java）。

2. **通用写入测试模板方法**：
   - `insertFirstBatch()` -- 插入第一批数据并验证
   - `writeBatch()` -- 执行一次写入批次（insert/upsert/delete）
   - `updateBatch()` -- 执行更新批次
   - `deleteBatch()` -- 执行删除批次

3. **Compaction/Clean/Clustering 测试**：提供测试各种表服务操作的公共方法。

4. **多 Writer 测试支持**：提供并发写入测试的工具方法。

5. **Meta 字段相关配置**：`addConfigsForPopulateMetaFields()` 方法处理是否填充元字段的配置。

**为什么要抽象 getHoodieWriteClient？** 因为不同引擎需要不同的 WriteClient 实现（`SparkRDDWriteClient` vs `FlinkWriteClient`），但测试逻辑是一样的。通过模板方法模式，核心测试逻辑只写一次，各引擎只需实现工厂方法。

**好处**：测试逻辑的 DRY (Don't Repeat Yourself) 原则得到了完美执行。新增引擎时，只需实现几个抽象方法就能复用所有已有的写入测试。

### 8.3 HoodieSparkClientTestHarness -- Spark 引擎特化

**源码路径**: `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/testutils/HoodieSparkClientTestHarness.java`

继承 `HoodieWriterClientTestHarness`，添加 Spark 特有的环境管理。

```java
@Slf4j
public abstract class HoodieSparkClientTestHarness extends HoodieWriterClientTestHarness {
  // Spark 环境
  private static transient SparkSession spark;
  private static transient SQLContext sqlContext;
  private static transient JavaSparkContext jsc;
  protected static transient HoodieSparkEngineContext context;

  // 初始化 Spark 环境
  protected void initSparkContexts() {
    // 创建 SparkConf, SparkSession, JavaSparkContext 等
  }

  // 元数据验证
  protected void validateMetadata(HoodieTableMetaClient metaClient, ...) {
    // 验证元数据表的正确性
  }
  // ...
}
```

**关键设计**：SparkSession 使用 `static` 字段并在类级别共享（`@AfterAll` 清理），避免每个测试方法都重新创建 SparkSession。这是因为 Spark 的初始化开销很大（需要启动调度器、内存管理器等），重复创建会极大拖慢测试速度。

### 8.4 HoodieClientTestBase -- Spark 客户端测试的最终基类

**源码路径**: `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/testutils/HoodieClientTestBase.java`

```java
public class HoodieClientTestBase extends HoodieSparkClientTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initResources();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  public HoodieSparkTable getHoodieTable(HoodieTableMetaClient metaClient,
      HoodieWriteConfig config) {
    HoodieSparkTable table = HoodieSparkTable.create(config, context, metaClient);
    ((SyncableFileSystemView) (table.getSliceView())).reset();
    return table;
  }
  // ...
}
```

**好处**：提供了完整的生命周期管理（`@BeforeEach` 初始化 + `@AfterEach` 清理），子类只需关注测试逻辑本身。

### 8.5 继承层级总结

```
HoodieCommonTestHarness                      (hudi-hadoop-common, 最基础的测试工具)
  └── HoodieWriterClientTestHarness          (hudi-client-common, 写入客户端测试模板)
        └── HoodieSparkClientTestHarness     (hudi-spark-client, Spark 环境管理)
              └── HoodieClientTestBase       (hudi-spark-client, 完整生命周期管理)
                    └── TestHoodieClientOnCopyOnWriteStorage  (具体测试类)
                    └── TestHoodieClientMultiWriter           (具体测试类)
                    └── ...
```

---

## 9. HoodieTestTable

### 9.1 设计目的

**源码路径**: `hudi-hadoop-common/src/test/java/org/apache/hudi/common/testutils/HoodieTestTable.java`

`HoodieTestTable` 是一个极其重要的测试工具类，它允许你用流式 API 快速构建一个带有完整时间线历史的模拟 Hudi 表。不需要实际执行写入操作，就能创建出包含 commit、compaction、clean、clustering 等各种 instant 的表状态。

```java
@Slf4j
public class HoodieTestTable implements AutoCloseable {

  public static final String PHONY_TABLE_SCHEMA = "...";
  private static final Random RANDOM = new Random();

  protected static HoodieTestTableState testTableState;
  private final List<String> inflightCommits = new ArrayList<>();

  protected final String basePath;
  protected final HoodieStorage storage;
  protected final FileSystem fs;
  protected HoodieTableMetaClient metaClient;
  protected String currentInstantTime;
  @Getter
  private boolean isNonPartitioned = false;
  protected Option<HoodieEngineContext> context;
  protected final InstantGenerator instantGenerator = new DefaultInstantGenerator();
  // ...
}
```

### 9.2 核心能力

**1. 创建 Commit**

```java
HoodieTestTable testTable = HoodieTestTable.of(metaClient);

// 创建一个完整的 commit
testTable.addCommit("001")
    .withBaseFilesInPartition("2024/01/01", "file1", "file2")
    .withBaseFilesInPartition("2024/01/02", "file3");
```

**2. 创建 Compaction**

```java
testTable.addRequestedCompaction("002",
    new HoodieCompactionPlan(/* ... */));
testTable.addInflightCompaction("002");
testTable.addCompaction("002");
```

**3. 创建 Clean**

```java
testTable.addRequestedClean("003", cleanerPlan);
testTable.addInflightClean("003");
testTable.addClean("003", cleanMetadata);
```

**4. 创建 Clustering**

```java
testTable.addRequestedClustering("004", clusteringPlan);
testTable.addInflightClustering("004");
testTable.addClustering("004");
```

**5. 创建 Rollback/Restore/Savepoint**

```java
testTable.addRollback("005", rollbackMetadata);
testTable.addRestore("006", restoreMetadata);
testTable.addSavepoint("007", savepointMetadata);
```

**为什么要设计 HoodieTestTable？** 在测试时间线相关逻辑（如 Compaction 调度、Clean 策略、文件系统视图计算）时，我们需要构建各种复杂的时间线状态。如果每次都通过实际执行写入操作来构建这些状态，不仅慢而且容易出错。`HoodieTestTable` 通过直接在文件系统上创建元数据文件来模拟表状态，速度极快且可精确控制。

**好处**：
- 构建复杂表状态只需几行代码，而不是几十行
- 可以精确模拟异常状态（如 inflight 但未完成的 compaction）
- 测试运行速度快，因为不需要实际的数据处理
- 支持流式 API，代码可读性强

### 9.3 HoodieSparkWriteableTestTable

**源码路径**: `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/testutils/HoodieSparkWriteableTestTable.java`

这是 `HoodieTestTable` 的 Spark 特化版本，除了元数据模拟外，还能实际写入 Parquet 数据文件。用于需要验证数据内容（而非仅验证时间线状态）的测试场景。

---

## 10. MiniCluster 集成测试

### 10.1 HdfsTestService

**源码路径**: `hudi-hadoop-common/src/test/java/org/apache/hudi/common/testutils/minicluster/HdfsTestService.java`

Hudi 提供了嵌入式 HDFS 集群服务，用于在不依赖外部 HDFS 的情况下运行集成测试。

```java
@Slf4j
public class HdfsTestService {

  @Getter
  private final Configuration hadoopConf;
  private final java.nio.file.Path dfsBaseDirPath;
  private MiniDFSCluster miniDfsCluster;

  public HdfsTestService() throws IOException {
    this(new Configuration());
  }

  public HdfsTestService(Configuration hadoopConf) throws IOException {
    this.hadoopConf = hadoopConf;
    this.dfsBaseDirPath = Files.createTempDirectory(
        "hdfs-test-service" + System.currentTimeMillis());
  }

  public MiniDFSCluster start(boolean format) throws IOException {
    Objects.requireNonNull(dfsBaseDirPath,
        "dfs base dir must be set before starting cluster.");

    int loop = 0;
    while (true) {
      try {
        int namenodeRpcPort = NetworkTestUtils.nextFreePort();
        int datanodePort = NetworkTestUtils.nextFreePort();
        int datanodeIpcPort = NetworkTestUtils.nextFreePort();
        int datanodeHttpPort = NetworkTestUtils.nextFreePort();

        String bindIP = "127.0.0.1";
        configureDFSCluster(hadoopConf, dfsBaseDirPath.toString(),
            bindIP, namenodeRpcPort, datanodePort,
            datanodeIpcPort, datanodeHttpPort);
        miniDfsCluster = new MiniDFSCluster.Builder(hadoopConf)
            .numDataNodes(1)
            .format(format)
            .checkDataNodeAddrConfig(true)
            .checkDataNodeHostConfig(true)
            .build();
        return miniDfsCluster;
      } catch (BindException ex) {
        ++loop;
        if (loop < 5) {
          stop();
        } else {
          throw ex;
        }
      }
    }
  }
}
```

**关键设计**：
- **自动端口分配**：使用 `NetworkTestUtils.nextFreePort()` 动态分配端口，避免端口冲突
- **重试机制**：如果端口绑定失败（`BindException`），会自动重试最多 5 次
- **临时目录隔离**：每次启动使用独立的临时目录，确保测试间不互相干扰

**为什么要嵌入式 HDFS？** 某些 Hudi 功能依赖于真实的 HDFS 语义（如原子重命名、目录锁），本地文件系统无法完全模拟。MiniDFSCluster 提供了真实的 HDFS 行为，同时又不需要外部集群。

**好处**：
- 测试环境完全自包含，CI 服务器不需要预装 HDFS
- 测试间完全隔离，不会因共享存储而互相影响
- 支持验证 HDFS 特有的行为（如副本、权限等）

### 10.2 HiveTestCluster

**源码路径**: `hudi-sync/hudi-hive-sync/src/test/java/org/apache/hudi/hive/testutils/HiveTestCluster.java`

用于测试 Hudi 与 Hive Metastore 的同步功能，启动嵌入式 Hive Server。

### 10.3 FunctionalTestHarness（带 DFS 支持）

**源码路径**: `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/testutils/FunctionalTestHarness.java`

```java
/**
 * @deprecated 已废弃。请使用 {@link SparkClientFunctionalTestHarness} 代替。
 */
public class FunctionalTestHarness implements SparkProvider, DFSProvider,
    HoodieMetaClientProvider, HoodieWriteClientProvider {

  protected static transient SparkSession spark;
  private static transient SQLContext sqlContext;
  private static transient JavaSparkContext jsc;
  protected static transient HoodieSparkEngineContext context;

  private static transient HdfsTestService hdfsTestService;
  private static transient MiniDFSCluster dfsCluster;
  private static transient HoodieStorage storage;

  protected boolean initialized = false;
  @TempDir
  protected java.nio.file.Path tempDir;
  // ...
}
```

**注意**：这个类已被标记为 `@deprecated`，新测试应使用 `SparkClientFunctionalTestHarness`。但理解其设计对于阅读旧测试代码仍有价值。它组合了 Spark 环境和 MiniDFS 集群，实现了 `SparkProvider`、`DFSProvider`、`HoodieMetaClientProvider`、`HoodieWriteClientProvider` 四个 Provider 接口。

**为什么使用 Provider 接口模式？** 不同的测试需要不同的环境组合。有些只需要 Spark，有些需要 Spark + HDFS，有些需要 Spark + Hive。通过 Provider 接口的组合，可以灵活地为不同测试提供所需的环境。

---

## 11. SparkClientFunctionalTestHarness

### 11.1 设计与实现

**源码路径**: `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/testutils/SparkClientFunctionalTestHarness.java`

这是当前推荐的 Spark 功能测试基类，替代了已废弃的 `FunctionalTestHarness`。

```java
public class SparkClientFunctionalTestHarness implements SparkProvider,
    HoodieMetaClientProvider, HoodieWriteClientProvider {

  protected static int timelineServicePort =
      FileSystemViewStorageConfig.REMOTE_PORT_NUM.defaultValue();
  private static transient SparkSession spark;
  private static transient SQLContext sqlContext;
  private static transient JavaSparkContext jsc;
  private static transient HoodieSparkEngineContext context;
  private static transient TimelineService timelineService;
  private HoodieStorage storage;
  private FileSystem fileSystem;

  protected boolean initialized = false;
  @TempDir
  protected java.nio.file.Path tempDir;

  public static Map<String, String> getSparkSqlConf() {
    Map<String, String> sqlConf = new HashMap<>();
    sqlConf.put("spark.sql.extensions",
        "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
    sqlConf.put("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog");
    return sqlConf;
  }

  public String basePath() {
    return tempDir.toAbsolutePath().toUri().toString();
  }
}
```

### 11.2 SparkSession 管理

```java
@BeforeEach
public synchronized void runBeforeEach() {
  initialized = spark != null;
  if (!initialized) {
    SparkConf sparkConf = conf();
    HoodieSparkKryoRegistrar$.MODULE$.register(sparkConf);
    SparkRDDReadClient.addHoodieSupport(sparkConf);
    spark = SparkSession.builder().config(sparkConf).getOrCreate();
    sqlContext = spark.sqlContext();
    HoodieClientTestUtils.overrideSparkHadoopConfiguration(spark.sparkContext());
    jsc = new JavaSparkContext(spark.sparkContext());
    context = new HoodieSparkEngineContext(jsc);
    timelineService = HoodieClientTestUtils.initTimelineService(
        context, basePath(), incrementTimelineServicePortToUse());
    timelineServicePort = timelineService.getServerPort();
  }
  // 清理持久化的 RDD，释放内存
  spark.sparkContext().persistentRdds().foreach(
      rdd -> rdd._2.unpersist(false));
}
```

**关键设计点**：

1. **synchronized 初始化**：SparkSession 创建使用 `synchronized` 保护，确保多线程安全
2. **惰性初始化**：只在首次 `@BeforeEach` 时创建 SparkSession，后续测试方法复用
3. **Kryo 注册**：`HoodieSparkKryoRegistrar$.MODULE$.register(sparkConf)` 注册 Hudi 的自定义 Kryo 序列化器
4. **HoodieSparkSessionExtension**：通过 Spark SQL 扩展注册 Hudi 的 DDL/DML 支持
5. **Timeline Service**：启动内嵌的 Timeline Service，提供文件系统视图服务
6. **RDD 清理**：每次测试前清理持久化的 RDD，避免内存泄漏

**为什么 SparkSession 要类级别共享？** 创建一个 SparkSession 需要 3-5 秒（包括启动 SparkContext、初始化内存管理器、创建调度器等）。如果每个测试方法都创建一个新的 SparkSession，一个有 50 个方法的测试类就需要额外花费 3-4 分钟仅在环境初始化上。共享 SparkSession 将这个开销减少到一次。

**好处**：功能测试的运行速度大幅提升，同时通过每次清理持久化 RDD 确保了测试间的数据隔离。

### 11.3 MetaClient 创建

```java
public HoodieTableMetaClient getHoodieMetaClient(
    StorageConfiguration<?> storageConf, String basePath,
    Properties props, HoodieTableType tableType) throws IOException {
  return HoodieTableMetaClient.newTableBuilder()
      .setTableName(RAW_TRIPS_TEST_NAME)
      .setTableType(tableType)
      .setPayloadClass(HoodieAvroPayload.class)
      .setTableVersion(ConfigUtils.getIntWithAltKeys(props, WRITE_TABLE_VERSION))
      .fromProperties(props)
      .initTable(storageConf.newInstance(), basePath);
}
```

### 11.4 SparkClientFunctionalTestHarnessWithHiveSupport

**源码路径**: `hudi-utilities/src/test/java/org/apache/hudi/utilities/testutils/SparkClientFunctionalTestHarnessWithHiveSupport.java`

这是 `SparkClientFunctionalTestHarness` 的扩展版本，额外提供了 Hive 支持，用于需要 Hive Metastore 交互的测试（如 DeltaStreamer 测试）。

---

# 第四部分：关键测试用例解读

## 12. TestHoodieClientOnCopyOnWriteStorage

### 12.1 概述

**源码路径**: `hudi-spark-datasource/hudi-spark/src/test/java/org/apache/hudi/client/functional/TestHoodieClientOnCopyOnWriteStorage.java`

这是 Hudi 中最核心、最全面的测试类之一，覆盖了 Copy-On-Write 表的几乎所有写入场景。

```java
@SuppressWarnings("unchecked")
@Tag("functional")
public class TestHoodieClientOnCopyOnWriteStorage extends HoodieClientTestBase {
  private static final Map<String, String> STRATEGY_PARAMS =
      Collections.singletonMap("sortColumn", "record_key");
  // ...
}
```

**继承关系**：`TestHoodieClientOnCopyOnWriteStorage` -> `HoodieClientTestBase` -> `HoodieSparkClientTestHarness` -> `HoodieWriterClientTestHarness` -> `HoodieCommonTestHarness`

### 12.2 关键测试方法解读

#### (1) testAutoCommitOnInsert -- 自动提交机制

```java
@ParameterizedTest
@MethodSource("populateMetaFieldsParams")
public void testAutoCommitOnInsert(boolean populateMetaFields) throws Exception {
  testAutoCommit(
      (writeClient, recordRDD, instantTime) ->
          writeClient.insert(recordRDD, instantTime),
      false, populateMetaFields, INSTANT_GENERATOR);
}
```

**测试目标**：验证 insert 操作的自动提交行为。当 `hoodie.auto.commit` 配置为 true 时，写入完成后应该自动将 instant 从 inflight 提交为 completed。

**为什么这个测试很重要？** 自动提交是 Hudi 的默认行为，理解它如何工作是理解整个写入流程的第一步。

#### (2) testAutoCommitOnUpsert -- Upsert 的自动提交

```java
@ParameterizedTest
@MethodSource("populateMetaFieldsParams")
public void testAutoCommitOnUpsert(boolean populateMetaFields) throws Exception {
  testAutoCommit(
      (writeClient, recordRDD, instantTime) ->
          writeClient.upsert(recordRDD, instantTime),
      false, populateMetaFields, INSTANT_GENERATOR);
}
```

**测试目标**：验证 upsert 操作的自动提交，与 insert 类似但涉及索引查找和 merge 逻辑。

#### (3) Prepped API 测试 -- 预处理 API

```java
@Test
public void testAutoCommitOnInsertPrepped() throws Exception {
  testAutoCommit(
      (writeClient, recordRDD, instantTime) ->
          writeClient.insertPreppedRecords(recordRDD, instantTime),
      true, true, INSTANT_GENERATOR);
}
```

**测试目标**：验证 "Prepped" 版本的 API。Prepped API 期望记录已经经过去重和位置标记，跳过索引查找步骤。

**为什么要有 Prepped API？** 在某些场景下（如 DeltaStreamer 的多轮写入），记录已经在上游完成了去重和位置标记，再次执行索引查找是浪费。Prepped API 允许跳过这些步骤，显著提升写入性能。

#### (4) castInsertFirstBatch / castWriteBatch -- 模板方法实现

```java
@Override
protected Object castInsertFirstBatch(...) throws Exception {
  return insertFirstBatch(writeConfig, (SparkRDDWriteClient) client,
      newCommitTime, initCommitTime, numRecordsInThisCommit,
      (writeClient, records, commitTime) ->
          (JavaRDD<WriteStatus>) writeFn.apply(writeClient, records, commitTime),
      isPreppedAPI, assertForCommit, expRecordsInThisCommit,
      filterForCommitTimeWithAssert, instantGenerator);
}
```

**设计解析**：这些方法实现了父类 `HoodieWriterClientTestHarness` 中定义的抽象模板方法。它们负责将引擎无关的测试参数适配为 Spark 特定的类型（如 `JavaRDD<WriteStatus>`）。

#### (5) 参数化测试工厂

```java
private static Stream<Arguments> conflictResolutionStrategyParams() {
  return Stream.of(
      Arguments.of(new PreferWriterConflictResolutionStrategy()),
      Arguments.of(new SimpleConcurrentFileWritesConflictResolutionStrategy())
  );
}
```

**为什么使用参数化测试？** 同一个测试逻辑需要在多种配置组合下验证。通过 JUnit 5 的 `@ParameterizedTest` + `@MethodSource`，可以用一个测试方法覆盖多种场景，避免代码重复。

#### (6) Clustering 相关测试配置

```java
private static HoodieClusteringConfig.Builder createClusteringBuilder(
    boolean isInline, int inlineNumCommits) {
  return HoodieClusteringConfig.newBuilder()
      .withClusteringMaxNumGroups(10)
      .withClusteringTargetPartitions(0)
      .withInlineClustering(isInline)
      .withInlineClusteringNumCommits(inlineNumCommits)
      .fromProperties(getDisabledRowWriterProperties());
}
```

#### (7) Lock 配置（多 Writer 场景）

```java
private static HoodieLockConfig createLockConfig(
    ConflictResolutionStrategy conflictResolutionStrategy) {
  return HoodieLockConfig.newBuilder()
      .withLockProvider(FileSystemBasedLockProviderTestClass.class)
      .withConflictResolutionStrategy(conflictResolutionStrategy)
      .build();
}
```

**为什么需要 LockConfig？** 在并发写入场景下，多个 Writer 可能同时操作同一个表。LockProvider 负责协调写入顺序，ConflictResolutionStrategy 决定冲突如何解决（是优先新写入还是用简单的文件级冲突检测）。

### 12.3 学习价值

通过阅读这个测试类，你可以深入理解：
- Hudi 的完整写入流程（insert/upsert/delete）
- 自动提交 vs 手动提交的区别
- Prepped API 的使用场景
- Clustering 的内联触发机制
- 并发写入的冲突解决策略
- 参数化测试的最佳实践

---

## 13. TestHoodieClientMultiWriter

### 13.1 概述

**源码路径**: `hudi-spark-datasource/hudi-spark/src/test/java/org/apache/hudi/client/TestHoodieClientMultiWriter.java`

这是 Hudi 多 Writer 并发控制的核心测试类，验证多个 Writer 同时操作同一个表时的行为。

```java
@Tag("functional")
public class TestHoodieClientMultiWriter extends HoodieClientTestBase {

  private Properties lockProperties = null;

  /**
   * super is not thread safe!!
   **/
  @Override
  public SparkRDDWriteClient getHoodieWriteClient(HoodieWriteConfig cfg) {
    return new SparkRDDWriteClient(context, cfg);  // 直接创建新实例
  }
}
```

**注意**：注释 `super is not thread safe!!` 揭示了一个重要的设计决策 -- 父类的 `getHoodieWriteClient` 方法会关闭旧的客户端，但在多 Writer 测试中，多个客户端需要同时存在。因此这里覆盖了该方法，每次都创建新的独立实例。

### 13.2 锁配置

```java
@BeforeEach
public void setup() throws IOException {
  if (lockProperties == null) {
    lockProperties = new Properties();
    lockProperties.setProperty(FILESYSTEM_LOCK_PATH_PROP_KEY,
        basePath + "/.hoodie/.locks");
    lockProperties.setProperty(
        LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "3000");
    lockProperties.setProperty(FILESYSTEM_LOCK_EXPIRE_PROP_KEY, "1");
    lockProperties.setProperty(
        LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP_KEY, "1000");
    lockProperties.setProperty(
        LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP_KEY, "1000");
    lockProperties.setProperty(
        LOCK_ACQUIRE_NUM_RETRIES_PROP_KEY, "3");
  }
}
```

### 13.3 测试维度组合

```java
private static final List<Class> LOCK_PROVIDER_CLASSES = Arrays.asList(
    InProcessLockProvider.class
);

private static final List<ConflictResolutionStrategy>
    CONFLICT_RESOLUTION_STRATEGY_CLASSES = Arrays.asList(
    new SimpleConcurrentFileWritesConflictResolutionStrategy(),
    new PreferWriterConflictResolutionStrategy()
);

private static Iterable<Object[]>
    providerClassResolutionStrategyAndTableType() {
  List<Object[]> opts = new ArrayList<>();
  for (Object providerClass : LOCK_PROVIDER_CLASSES) {
    for (ConflictResolutionStrategy resolutionStrategy :
        CONFLICT_RESOLUTION_STRATEGY_CLASSES) {
      opts.add(new Object[] {COPY_ON_WRITE, providerClass, resolutionStrategy});
      opts.add(new Object[] {MERGE_ON_READ, providerClass, resolutionStrategy});
    }
  }
  return opts;
}
```

**为什么测试多种锁提供者和冲突策略的组合？** 生产环境中用户可能使用不同的锁提供者（InProcess/FileSystem/Zookeeper/DynamoDB）和不同的冲突解决策略。通过组合测试确保所有合法配置都能正确工作。

**注意**：`FileSystemBasedLockProvider` 在测试中被注释掉了，因为不同 OS 的文件系统 API 对原子操作的支持不同，可能导致误报。这是一个典型的"flaky test 处理"案例。

### 13.4 并发 Schema Evolution 测试

```java
public static Stream<Arguments> concurrentAlterSchemaTestDimension() {
  Object[][] data = new Object[][] {
    // {<should create initial commit>, <table type>,
    //  <txn 1 writer schema>, <txn 2 writer schema>,
    //  <should schema conflict>, <expected table schema after resolution>}
    {true, false, MERGE_ON_READ, TRIP_EXAMPLE_SCHEMA,
     TRIP_EXAMPLE_SCHEMA, false, TRIP_EXAMPLE_SCHEMA},
    // ...更多组合
  };
}
```

**为什么要测试并发 Schema Evolution？** 在多 Writer 场景下，两个 Writer 可能使用不同的 Schema 写入。测试需要验证 Schema 冲突检测和解决机制是否正确工作，确保表的 Schema 一致性不被破坏。

### 13.5 学习价值

通过这个测试类，你可以理解：
- Hudi 的乐观并发控制 (OCC) 机制
- 多种锁提供者的配置和行为差异
- 冲突检测和解决的完整流程
- 并发 Schema Evolution 的处理方式
- 如何在测试中模拟并发场景（使用 `ExecutorService`、`CyclicBarrier`、`CountDownLatch`）

---

## 14. TestHoodieCompactionStrategy

### 14.1 概述

**源码路径**: `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/table/action/compact/strategy/TestHoodieCompactionStrategy.java`

这个测试类验证 Hudi 的各种 Compaction 策略，是理解 MOR 表 Compaction 行为的最佳入口。

```java
public class TestHoodieCompactionStrategy {

  private static final long MB = 1024 * 1024L;
  private static final Random RANDOM = new Random();
  private String[] partitionPaths = {"2017/01/01", "2017/01/02", "2017/01/03"};
}
```

### 14.2 UnBounded 策略测试

```java
@ParameterizedTest
@ValueSource(booleans = {true, false})
public void testUnBounded(boolean enableIncrTableService) {
  Map<Long, List<Long>> sizesMap = new HashMap<>();
  sizesMap.put(120 * MB, Arrays.asList(60 * MB, 10 * MB, 80 * MB));
  sizesMap.put(110 * MB, new ArrayList<>());
  sizesMap.put(100 * MB, Collections.singletonList(MB));
  sizesMap.put(90 * MB, Collections.singletonList(1024 * MB));
  UnBoundedCompactionStrategy strategy = new UnBoundedCompactionStrategy();
  HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp")
      .withIncrementalTableServiceEnabled(enableIncrTableService)
      .withCompactionConfig(HoodieCompactionConfig.newBuilder()
          .withCompactionStrategy(strategy).build())
      .build();
  List<HoodieCompactionOperation> operations =
      createCompactionOperations(writeConfig, sizesMap).getLeft();
  Pair<List<HoodieCompactionOperation>, List<String>> resPair =
      writeConfig.getCompactionStrategy()
          .orderAndFilter(writeConfig, operations, new ArrayList<>());
  List<HoodieCompactionOperation> returned = resPair.getLeft();

  assertEquals(operations, returned,
      "UnBounded should not re-order or filter");
}
```

**测试逻辑解析**：
1. 构建一组不同大小的 base file 和 log file 组合
2. 使用 `UnBoundedCompactionStrategy` 策略
3. 验证该策略不会过滤或重排序任何 compaction 操作

**为什么 UnBounded 策略不做任何过滤？** 因为它是最基础的策略，对所有待 compact 的 file slice 一视同仁。在资源不受限的场景下使用，确保所有 log file 都能被及时 compact 成 base file。

### 14.3 BoundedIO 策略测试

```java
@ParameterizedTest
@ValueSource(booleans = {true, false})
public void testBoundedIOSimple(boolean enableIncrTableService) {
  // ...
  BoundedIOCompactionStrategy strategy = new BoundedIOCompactionStrategy();
  HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp")
      .withIncrementalTableServiceEnabled(enableIncrTableService)
      .withCompactionConfig(
          HoodieCompactionConfig.newBuilder()
              .withCompactionStrategy(strategy)
              .withTargetIOPerCompactionInMB(400)
              .build())
      .build();
  // ...
  assertTrue(returned.size() < operations.size(),
      "BoundedIOCompaction should have resulted in fewer compactions");
}
```

**测试逻辑解析**：
1. 设置 target I/O 为 400MB
2. 验证 BoundedIOCompactionStrategy 会根据 I/O 预算过滤掉部分 compaction 操作
3. 返回的操作数应该少于输入的操作数

**为什么需要 BoundedIO 策略？** 在资源有限的环境中，不能一次 compact 所有待处理的 file slice。BoundedIO 策略通过限制单次 compaction 的 I/O 总量来控制资源消耗，优先处理 I/O 效益最大的 file slice。

**好处**：避免了 compaction 操作耗尽集群资源影响正常写入和查询的风险。

### 14.4 学习价值

通过这个测试类，你可以理解：
- Hudi 的 Compaction 策略插件化设计
- UnBounded vs BoundedIO vs LogFileSize 等策略的区别
- Compaction 操作的排序和过滤逻辑
- 增量表服务（Incremental Table Service）对 Compaction 的影响
- 如何通过 HoodieWriteConfig 配置 Compaction 行为

---

## 15. TestHoodieBloomIndex

### 15.1 概述

**源码路径**: `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/index/bloom/TestHoodieBloomIndex.java`

Bloom Index 是 Hudi 默认的索引实现，这个测试类全面验证了 Bloom Filter 索引的行为。

```java
public class TestHoodieBloomIndex extends TestHoodieMetadataBase {

  private static final HoodieSchema SCHEMA =
      getSchemaFromResource(TestHoodieBloomIndex.class,
          "/exampleSchema.avsc", true);
  private static final Random RANDOM = new Random(0xDEED);

  public static Stream<Arguments> configParams() {
    // rangePruning, treeFiltering, bucketizedChecking,
    // useMetadataTable, enableFileGroupIdKeySorting
    Object[][] data = new Object[][] {
      {true, true, true, false, false},
      {false, true, true, false, false},
      {true, true, false, false, false},
      {true, false, true, false, false},
      {true, true, true, true, false},
      {false, true, true, true, false},
      {true, true, false, true, false},
      // ... 更多组合
    };
  }
}
```

### 15.2 参数化配置

五个布尔参数控制 Bloom Index 的行为：

| 参数 | 含义 | 影响 |
|------|------|------|
| `rangePruning` | 范围裁剪 | 是否使用记录键的最小/最大值来排除不可能包含目标记录的文件 |
| `treeFiltering` | 树形过滤 | 是否使用内存中的区间树来加速文件定位 |
| `bucketizedChecking` | 分桶检查 | 是否对 Bloom Filter 的检查进行分桶优化 |
| `useMetadataTable` | 使用元数据表 | 是否从 Metadata Table 读取 Bloom Filter（而非每个文件的 footer） |
| `enableFileGroupIdKeySorting` | 文件组ID排序 | 是否按文件组 ID 排序来优化 I/O |

**为什么需要这么多配置组合？** Bloom Index 的性能取决于数据分布和查询模式。不同的优化策略在不同场景下效果不同：
- 小文件场景下 rangePruning 效果不明显
- 大量分区下 treeFiltering 显著减少搜索空间
- Metadata Table 模式下减少了随机 I/O，但增加了额外的元数据维护开销

**好处**：通过参数化测试覆盖所有配置组合，确保每种优化策略都能正确工作，同时也为用户选择最佳配置提供了参考。

### 15.3 测试继承体系

`TestHoodieBloomIndex` 继承自 `TestHoodieMetadataBase`，而非直接继承 `HoodieClientTestBase`。这是因为 Bloom Index 的测试需要 Metadata Table 的支持（当 `useMetadataTable=true` 时）。`TestHoodieMetadataBase` 提供了 Metadata Table 的初始化和验证能力。

### 15.4 学习价值

通过这个测试类，你可以深入理解：
- Bloom Filter 的工作原理（假阳性但无假阴性）
- Range Pruning 如何利用文件内记录键的最大/最小值
- Metadata Table 如何存储和检索 Bloom Filter
- 索引查找的完整流程：tagLocation() -> 查找记录在哪个文件中
- 各种优化策略的开启和关闭方式

---

# 第五部分：Checkstyle 与代码规范

## 16. Checkstyle 规则

### 16.1 配置文件

**源码路径**: `style/checkstyle.xml`

Hudi 的 Java 代码规范基于 Google Java Style，但做了一些定制化修改。Checkstyle 在编译阶段（`compile` phase）自动执行，违规会导致构建失败。

### 16.2 关键规则解析

#### (1) 行长度限制

```xml
<module name="LineLength">
    <property name="max" value="200"/>
    <property name="ignorePattern" value="^ *\* *[^ ]+$"/>
</module>
```

**限制**：每行最大 200 个字符，注释中的长 URL 可以超过限制。

**为什么是 200 而不是 Google Style 的 100？** 大数据系统的类名和方法名通常较长（如 `HoodieSparkCopyOnWriteTable`、`RunCompactionActionExecutor`），再加上泛型参数，100 字符的限制会导致大量不必要的换行。200 字符是一个更实用的平衡点。

#### (2) Tab 字符禁止

```xml
<module name="FileTabCharacter">
    <property name="eachLine" value="true"/>
</module>
```

**要求**：所有文件禁止使用 Tab 字符，统一使用空格缩进。

#### (3) 缩进规则

```xml
<module name="Indentation">
    <property name="basicOffset" value="2"/>
    <property name="braceAdjustment" value="0"/>
    <property name="caseIndent" value="2"/>
    <property name="throwsIndent" value="4"/>
    <property name="lineWrappingIndentation" value="4"/>
    <property name="arrayInitIndent" value="2"/>
</module>
```

**要求**：基本缩进 2 个空格，换行续行缩进 4 个空格。

#### (4) Import 规则

```xml
<module name="ImportOrder">
    <property name="groups"
        value="org.apache.hudi,*,javax,java,scala"/>
    <property name="separated" value="true"/>
    <property name="sortStaticImportsAlphabetically" value="true"/>
    <property name="option" value="bottom"/>
</module>
```

**Import 顺序**：
1. `org.apache.hudi.*` -- Hudi 自身的包
2. `*` -- 第三方库
3. `javax.*` -- Java 标准扩展
4. `java.*` -- Java 标准库
5. `scala.*` -- Scala 库
6. Static imports（放在最底部，按字母排序）

**各组之间用空行分隔**。

**为什么 Hudi 自身的包放最前面？** 这是 Apache 项目的惯例，让代码审查者一眼就能看到引用了哪些 Hudi 内部模块，便于判断是否存在不合理的跨模块依赖。

#### (5) 非法 Import 检查

```xml
<module name="IllegalImport">
    <property name="regexp" value="true"/>
    <property name="illegalPkgs"
        value="org\.apache\.commons, com\.google\.common,
               org\.apache\.log4j, org\.codehaus\.jackson"/>
    <property name="illegalClasses"
        value="^java\.util\.Optional,
               ^org\.junit\.(?!jupiter|platform|contrib|Rule|runner)(.*)"/>
</module>
```

**禁止导入的包**：
- `org.apache.commons` -- 禁止直接使用 Apache Commons（Hudi 有自己的工具类）
- `com.google.common` -- 禁止直接使用 Guava（Bundle 中不包含 Guava，使用会导致运行时错误）
- `org.apache.log4j` -- 禁止直接使用 Log4j（应使用 SLF4J）
- `org.codehaus.jackson` -- 禁止使用旧版 Jackson（应使用 com.fasterxml.jackson）
- `java.util.Optional` -- 禁止使用 Java 的 Optional（Hudi 有自己的 `org.apache.hudi.common.util.Option`）
- 旧版 JUnit (JUnit 4) -- 只允许 JUnit Jupiter (JUnit 5)

**为什么禁止 Guava 和 Apache Commons？** Hudi 的 uber-jar (bundle) 通过 Maven Shade Plugin 打包时，会对第三方依赖进行 relocation。如果直接使用 Guava 或 Commons 的类，在 bundle 中可能找不到对应的类（因为没有被 shade 进来），导致 `ClassNotFoundException`。

**为什么禁止 java.util.Optional？** `Option` 是 Hudi 自定义的 Optional 实现，支持序列化。在 Avro/Spark 序列化场景中，`java.util.Optional` 不可序列化会导致错误。

#### (6) 命名规则

```xml
<!-- 成员变量：小驼峰 -->
<module name="MemberName">
    <property name="format" value="^[a-z][a-zA-Z0-9]*$"/>
</module>

<!-- 方法名：小写字母开头 + 小驼峰 -->
<module name="MethodName">
    <property name="format" value="^[a-z][a-z0-9][a-zA-Z0-9_]*$"/>
</module>

<!-- 常量名：大写 + 下划线 -->
<module name="ConstantName"/>

<!-- 类型参数：大写字母（可选数字），或大写字母开头以T结尾 -->
<module name="ClassTypeParameterName">
    <property name="format"
        value="(^[A-Z][0-9]?)$|([A-Z][a-zA-Z0-9]*[T]$)"/>
</module>
```

#### (7) 其他重要规则

```xml
<!-- 避免星号导入 -->
<module name="AvoidStarImport"/>

<!-- Switch 必须有 default 分支 -->
<module name="MissingSwitchDefault"/>

<!-- 空 catch 块必须命名为 expected -->
<module name="EmptyCatchBlock">
    <property name="exceptionVariableName" value="expected"/>
</module>

<!-- 每行只能声明一个变量 -->
<module name="MultipleVariableDeclarations"/>

<!-- 简化布尔表达式 -->
<module name="SimplifyBooleanExpression"/>

<!-- 禁止未使用的 import -->
<module name="UnusedImports"/>

<!-- 禁止冗余 import -->
<module name="RedundantImport"/>
```

---

## 17. Import Control

### 17.1 配置文件

**源码路径**: `style/import-control.xml`

```xml
<import-control pkg="org" strategyOnMismatch="allowed">
    <disallow pkg="scala"/>
</import-control>
```

### 17.2 规则解析

当前的 import-control 规则非常简洁：**在 `hudi-client/hudi-client-common` 模块的 Java 代码中，禁止导入 Scala 包**。

```xml
<!-- 在 checkstyle.xml 中的引用 -->
<module name="ImportControl">
    <property name="file" value="${basedir}/style/import-control.xml"/>
    <property name="path"
        value="^.*[\\/]hudi-client[\\/]hudi-client-common[\\/]src[\\/].*$"/>
</module>
```

**为什么 hudi-client-common 禁止使用 Scala？**

`hudi-client-common` 是引擎无关的核心模块，它需要被 Spark 客户端、Flink 客户端和纯 Java 客户端共同依赖。如果它引入了 Scala 依赖：
- 纯 Java 客户端运行时会缺少 Scala 运行库
- Flink 客户端使用的 Scala 版本可能与 Spark 不兼容
- 增加了 Bundle 打包的复杂性

**好处**：确保核心写入逻辑的纯 Java 实现，使得所有引擎都能无缝使用。

### 17.3 策略说明

`strategyOnMismatch="allowed"` 表示对于未显式声明的包，默认允许导入。这是一个宽松的策略 -- 只禁止明确指定的违规，其他都放行。

**为什么不使用更严格的策略？** Hudi 是一个大型项目，模块间的依赖关系复杂。过于严格的 import control 会导致大量的 `<allow>` 声明，维护成本很高。当前策略聚焦于最关键的约束（Java 核心模块不依赖 Scala），是投入产出比最优的选择。

---

## 18. Scalastyle 规则

### 18.1 配置文件

**源码路径**: `style/scalastyle.xml`

Hudi 的 Scala 代码（主要在 Spark datasource 模块中）遵循独立的 Scalastyle 规范。

### 18.2 关键规则

#### (1) 文件级规则

```xml
<!-- 文件最大长度 5000 行 -->
<check level="error" class="org.scalastyle.file.FileLengthChecker" enabled="true">
  <parameters>
    <parameter name="maxFileLength"><![CDATA[5000]]></parameter>
  </parameters>
</check>

<!-- 禁止 Tab 字符 -->
<check level="error" class="org.scalastyle.file.FileTabChecker" enabled="true"/>

<!-- 禁止行尾空白 -->
<check level="error" class="org.scalastyle.file.WhitespaceEndOfLineChecker" enabled="true"/>

<!-- 文件必须以换行符结束 -->
<check level="error" class="org.scalastyle.file.NewLineAtEofChecker" enabled="true"/>
```

#### (2) Import 顺序规则

```xml
<check level="error" class="org.scalastyle.scalariform.ImportOrderChecker" enabled="true">
  <parameters>
    <parameter name="groups">hudi,3rdParty,javax,java,scala</parameter>
    <parameter name="group.hudi">org\.apache\.hudi\..*</parameter>
    <parameter name="group.3rdParty">
        (?!org\.apache\.hudi\.|javax\.|java\.|scala\.).*
    </parameter>
    <parameter name="group.javax">javax\..*</parameter>
    <parameter name="group.java">java\..*</parameter>
    <parameter name="group.scala">scala\..*</parameter>
  </parameters>
</check>
```

**顺序与 Java 一致**：hudi -> 第三方 -> javax -> java -> scala

#### (3) 非法 Import

```xml
<check level="error"
    class="org.scalastyle.scalariform.IllegalImportsChecker" enabled="true">
  <parameters>
    <parameter name="illegalImports">
        <![CDATA[sun._,java.awt._,com.google.common,org.codehaus.jackson]]>
    </parameter>
  </parameters>
</check>
```

**禁止导入**：`sun.*`、`java.awt.*`（非服务端库）、`com.google.common`（Guava）、`org.codehaus.jackson`（旧 Jackson）

#### (4) 命名和结构规则

```xml
<!-- 类名：大写字母开头 -->
<check level="error" class="org.scalastyle.scalariform.ClassNamesChecker">
  <parameters>
    <parameter name="regex"><![CDATA[[A-Z][A-Za-z]*]]></parameter>
  </parameters>
</check>

<!-- 方法名：小写字母开头 -->
<check level="error" class="org.scalastyle.scalariform.MethodNamesChecker">
  <parameters>
    <parameter name="regex"><![CDATA[^[a-z][A-Za-z0-9]*$]]></parameter>
  </parameters>
</check>

<!-- 方法参数不超过 12 个 -->
<check level="error" class="org.scalastyle.scalariform.ParameterNumberChecker">
  <parameters>
    <parameter name="maxParameters"><![CDATA[12]]></parameter>
  </parameters>
</check>

<!-- 一个类型中方法数不超过 500 -->
<check level="error"
    class="org.scalastyle.scalariform.NumberOfMethodsInTypeChecker">
  <parameters>
    <parameter name="maxMethods"><![CDATA[500]]></parameter>
  </parameters>
</check>

<!-- 禁止 return 语句（Scala 风格偏好表达式而非语句） -->
<check level="error" class="org.scalastyle.scalariform.ReturnChecker"
    enabled="true"/>

<!-- 禁止 clone -->
<check level="error" class="org.scalastyle.scalariform.NoCloneChecker"
    enabled="true"/>

<!-- 禁止 finalize -->
<check level="error" class="org.scalastyle.scalariform.NoFinalizeChecker"
    enabled="true"/>

<!-- equals 和 hashCode 必须同时重写 -->
<check level="error"
    class="org.scalastyle.scalariform.EqualsHashCodeChecker" enabled="true"/>
```

**为什么禁止 return 语句？** 在 Scala 中，函数的最后一个表达式自动作为返回值。显式的 `return` 语句不仅不必要，还可能在闭包中产生意外行为（`return` 在闭包中会触发 `NonLocalReturnControl` 异常）。

**为什么限制方法参数为 12 个？** 过多的参数是设计问题的信号，通常意味着需要将参数封装为配置对象。12 个是一个相对宽松的上限，考虑到了 Spark API 某些回调方法的参数较多的情况。

---

# 第六部分：RFC 与设计文档

## 19. RFC 机制

### 19.1 什么是 RFC

RFC (Request For Comments) 是 Hudi 社区的正式设计提案机制。任何影响 Hudi 核心架构、公共 API 或用户行为的重大变更，都应该先提交 RFC 进行讨论。

**源码路径**: `rfc/` 目录

每个 RFC 有独立的子目录（如 `rfc/rfc-69/`），包含 Markdown 格式的设计文档和可能的图片资源。

### 19.2 RFC 流程

1. **提案**：在 dev@hudi.apache.org 邮件列表发起讨论
2. **RFC 编写**：使用标准模板编写设计文档，包含 Proposers、Approvers、Status、Abstract、Background、Design 等部分
3. **社区审查**：在 GitHub PR 中讨论设计方案
4. **批准/修改**：获得 Approvers 的 +1 后合入主线
5. **实现**：按照 RFC 设计进行编码实现

### 19.3 RFC 模板结构

一个典型的 RFC 包含以下部分：

```markdown
# RFC-XX: 标题

## Proposers
- @github-username

## Approvers
- @reviewer1
- @reviewer2

## Status
JIRA: https://issues.apache.org/jira/browse/HUDI-XXXX

## Abstract
简短描述问题和解决方案

## Background
问题背景和现有方案的不足

## Design / Implementation
详细设计，包括架构图、API 设计、数据格式等

## Rollout / Adoption Plan
兼容性、迁移方案、配置变更

## Test Plan
如何验证设计的正确性
```

**为什么 RFC 机制很重要？** RFC 是连接"为什么"和"怎么做"的桥梁。通过阅读 RFC，你可以理解一个特性的设计动机（为什么需要这个特性）、方案选型（为什么选择这个方案而不是其他方案）和实现约束（需要注意哪些兼容性问题）。

---

## 20. 重要 RFC 索引

以下是 Hudi 项目中最重要的 RFC 列表，按影响范围排序：

### 核心架构类

| RFC | 标题 | 路径 | 重要性 |
|-----|------|------|--------|
| **RFC-69** | Hudi 1.X | `rfc/rfc-69/rfc-69.md` | Hudi 1.0 的整体架构升级规划，定义了 Table Version 8 的设计目标 |
| **RFC-78** | 1.0 Migration | `rfc/rfc-78/rfc-78.md` | 从 0.x 到 1.0 的迁移方案，包含表格式升降级策略 |
| **RFC-46** | Optimize Record Payload handling | `rfc/rfc-46/rfc-46.md` | 优化记录有效载荷处理，减少 Avro 序列化/反序列化的开销 |
| **RFC-53** | Use Lock-Free Message Queue Improving Hoodie Writing Efficiency | `rfc/rfc-53/rfc-53.md` | 使用无锁消息队列提升写入效率 |

### 索引与元数据类

| RFC | 标题 | 路径 | 重要性 |
|-----|------|------|--------|
| **RFC-8** | Metadata based Record Index | `rfc/rfc-8/rfc-8.md` | Record Level Index，通过元数据表实现精确的记录级索引 |
| **RFC-27** | Data skipping Index | `rfc/rfc-27/rfc-27.md` | 数据跳过索引，通过列统计信息跳过不相关的文件 |
| **RFC-37** | Metadata based Bloom Index | `rfc/rfc-37/rfc-37.md` | 将 Bloom Filter 存储在元数据表中，避免打开每个数据文件读取 Bloom Filter |
| **RFC-45** | Asynchronous Metadata Indexing | `rfc/rfc-45/rfc-45.md` | 异步元数据索引，后台构建索引不阻塞写入 |
| **RFC-63** | Expression Indexes | `rfc/rfc-63/rfc-63.md` | 表达式索引，支持对表达式（如 date_format(ts)）建立索引 |
| **RFC-77** | Secondary Indexes | `rfc/rfc-77/rfc-77.md` | 二级索引，支持非主键列的索引 |

### 并发与多 Writer 类

| RFC | 标题 | 路径 | 重要性 |
|-----|------|------|--------|
| **RFC-56** | Early Conflict Detection For Multi-writer | `rfc/rfc-56/rfc-56.md` | 多 Writer 场景的早期冲突检测，在写入过程中尽早发现冲突 |
| **RFC-73** | Multi-Table Transactions | `rfc/rfc-73/rfc-73.md` | 多表事务，支持跨多个 Hudi 表的原子性操作 |

### 表服务与压缩类

| RFC | 标题 | 路径 | 重要性 |
|-----|------|------|--------|
| **RFC-48** | Log compaction support for MOR tables | `rfc/rfc-48/rfc-48.md` | MOR 表的日志压缩，将多个 log file 合并为一个 |
| **RFC-42** | Consistent Hashing Index for Dynamic Bucket Number | `rfc/rfc-42/rfc-42.md` | 一致性哈希索引，支持动态调整桶数量 |

### 集成与同步类

| RFC | 标题 | 路径 | 重要性 |
|-----|------|------|--------|
| **RFC-34** | Hudi BigQuery Integration | `rfc/rfc-34/rfc-34.md` | Hudi 与 BigQuery 的集成 |
| **RFC-38** | Spark Datasource V2 Integration | `rfc/rfc-38/rfc-38.md` | Spark DataSource V2 API 集成 |
| **RFC-40** | Hudi Connector for Trino | `rfc/rfc-40/rfc-40.md` | Trino 原生 Connector |
| **RFC-51** | Hudi to support Change-Data-Capture | `rfc/rfc-51/rfc-51.md` | CDC 支持，可以读取增量变更流 |
| **RFC-55** | Improve hudi-sync classes design and simplify configs | `rfc/rfc-55/rfc-55.md` | 改进同步模块的类设计和配置 |
| **RFC-60** | Federated Storage Layout | `rfc/rfc-60/rfc-60.md` | 联邦存储布局，支持多存储系统 |

---

## 21. 如何从 RFC 理解架构决策

### 21.1 以 RFC-69 (Hudi 1.X) 为例

**源码路径**: `rfc/rfc-69/rfc-69.md`

RFC-69 是理解 Hudi 1.0 架构的最重要文档。让我们通过它来展示如何从 RFC 理解架构决策。

#### 步骤1：理解动机（Why）

RFC-69 提出的核心动机：
- Hudi 0.x 版本积累了大量技术债务
- 表格式需要升级以支持新特性（如更高效的时间线）
- 写入路径需要统一（Row Writer vs Classic Writer）
- 读取路径需要优化（File Group Reader）

#### 步骤2：理解设计选择（What）

RFC-69 定义了 Table Version 8 的关键变更：
- 新的时间线格式（Timeline V2）
- 统一的写入路径
- 改进的 Schema 处理
- 新的文件列表机制

#### 步骤3：在代码中验证（How）

通过 RFC 理解了设计后，可以在代码中找到对应的实现：

```java
// HoodieTableVersion.java 中定义了版本号
public enum HoodieTableVersion {
  // ...
  EIGHT;  // 对应 RFC-69 中的 Table Version 8
}

// HoodieTableMetaClient 中版本升降级逻辑
// hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/upgrade/
```

#### 步骤4：通过测试验证理解

找到相关的迁移测试，验证你对升降级逻辑的理解是否正确。

### 21.2 从 RFC 到代码的导航技巧

1. **找到 JIRA Issue**：RFC 中通常会引用 JIRA issue，通过 JIRA 可以找到所有相关的 PR
2. **搜索关键词**：在代码中搜索 RFC 中定义的新类名或配置项
3. **查看 git log**：`git log --grep="RFC-69"` 可以找到与 RFC 相关的所有提交
4. **阅读测试**：寻找名称中包含 RFC 编号或特性名称的测试类

**好处**：RFC -> JIRA -> PR -> 代码 -> 测试 的导航链路，让你可以完整地理解一个特性从设计到实现到验证的全过程。

---

# 第七部分：贡献者指南

## 22. 如何提交 PR

### 22.1 完整流程

```
1. Fork 仓库
   └── 在 GitHub 上 Fork apache/hudi 到自己的账号

2. Clone 并配置
   git clone git@github.com:<your-username>/hudi.git
   cd hudi
   git remote add upstream git@github.com:apache/hudi.git

3. 创建特性分支
   git checkout -b HUDI-XXXX-feature-description master

4. 开发与测试
   # 修改代码
   # 运行单元测试
   mvn test -pl <modified-module> -Dtest=<TestClass>
   # 运行功能测试（如果修改了核心逻辑）
   mvn -Pfunctional-tests test -pl <modified-module>

5. 确保 Checkstyle 通过
   mvn checkstyle:check -pl <modified-module>

6. 提交代码
   git add <files>
   git commit -m "[HUDI-XXXX] Brief description"

7. 推送并创建 PR
   git push origin HUDI-XXXX-feature-description
   # 在 GitHub 上创建 PR，目标分支为 apache/hudi:master

8. CI 验证
   # PR 会自动触发 CI 流水线
   # 等待 CI 通过，修复任何失败

9. 代码审查
   # 回应 Reviewer 的评审意见
   # 进行必要的修改

10. 合并
    # 获得至少 1 个 Committer 的 +1 后合并
```

### 22.2 Commit Message 规范

```
[HUDI-XXXX] 简短描述（不超过 72 个字符）

详细描述变更内容和原因。可以包含多段。
解释为什么需要这个变更，而不仅仅是做了什么。

Jira: https://issues.apache.org/jira/browse/HUDI-XXXX
```

### 22.3 PR 描述模板

一个好的 PR 描述应该包含：
- **变更摘要**：做了什么，为什么要做
- **风险评估**：这个变更可能影响哪些场景
- **测试计划**：如何验证这个变更的正确性
- **兼容性说明**：是否有向后兼容性问题

---

## 23. 代码审查要点

### 23.1 审查 Hudi PR 时需要关注的方面

#### (1) 架构一致性
- 修改是否在正确的模块中？（引擎无关的逻辑应该在 `client-common` 中）
- 是否遵循了引擎抽象的设计模式？
- 是否引入了不合理的跨模块依赖？

#### (2) 配置设计
- 新增的配置项是否遵循了 Builder 模式？
- 配置命名是否一致？（`hoodie.<category>.<name>`）
- 是否提供了合理的默认值？
- 是否有向后兼容性？

#### (3) 序列化安全
- 新增的类是否需要序列化？（Spark/Flink 环境中经常需要）
- 是否使用了不可序列化的类作为成员变量？
- Lambda 表达式是否捕获了不可序列化的变量？

#### (4) 并发安全
- 涉及共享状态的操作是否线程安全？
- 时间线操作是否考虑了并发写入？
- 锁的使用是否有死锁风险？

#### (5) 测试覆盖
- 是否为新功能添加了足够的测试？
- 测试是否覆盖了正常路径和异常路径？
- 功能测试是否使用了 `@Tag("functional")`？
- 测试是否有 flaky 风险（时间依赖、端口依赖等）？

#### (6) 代码规范
- 是否通过了 Checkstyle 检查？
- Import 顺序是否正确？
- 是否使用了禁止的依赖（Guava、Commons、java.util.Optional 等）？

### 23.2 常见审查反馈

| 问题 | 解决方案 |
|------|---------|
| "这个逻辑应该放在 client-common 中" | 将引擎无关的逻辑从 spark-client 移动到 client-common |
| "请使用 Option 而不是 java.util.Optional" | 替换为 `org.apache.hudi.common.util.Option` |
| "缺少功能测试" | 添加 `@Tag("functional")` 标记的测试 |
| "配置命名不一致" | 遵循 `hoodie.<category>.<property>` 的命名规范 |
| "可能的序列化问题" | 确保类实现 `Serializable` 或使用 `transient` 修饰 |

---

## 24. 常见构建问题排查

### 24.1 编译失败

#### 问题1：Checkstyle 失败

**症状**：编译阶段报 Checkstyle 违规错误

**解决方案**：
```bash
# 查看详细的 Checkstyle 报告
mvn checkstyle:check -pl <module>

# 常见问题：
# - 未使用的 import -> 删除
# - Tab 字符 -> 替换为空格
# - 行太长 -> 换行（每行不超过 200 字符）
# - Import 顺序错误 -> 按 hudi -> 第三方 -> javax -> java -> scala 排序
```

#### 问题2：依赖冲突

**症状**：`ClassNotFoundException` 或 `NoSuchMethodError`

**解决方案**：
```bash
# 查看依赖树
mvn dependency:tree -pl <module>

# 常见原因：
# - 多个模块引入了不同版本的同一依赖
# - Bundle 打包中遗漏了某个依赖
# - Shade 后的类路径冲突
```

#### 问题3：Scala 版本不匹配

**症状**：`NoClassDefFoundError: scala/...` 或 Scala 编译错误

**解决方案**：
```bash
# 确保 Profile 和 Scala 版本匹配
# Spark 3.x -> Scala 2.12（默认）
# Spark 4.0 -> Scala 2.13
mvn clean package -DskipTests -Dspark4.0  # 会自动设置 Scala 2.13
```

#### 问题4：Java 版本不匹配

**症状**：`UnsupportedClassVersionError` 或编译失败

**解决方案**：
```bash
# 检查 Java 版本
java -version

# Spark 3.x -> Java 11 或 17
# Spark 4.0 -> 需要 Java 17
export JAVA_HOME=/path/to/jdk17
```

### 24.2 测试失败

#### 问题1：内存不足 (OOM)

**症状**：`java.lang.OutOfMemoryError: Java heap space`

**解决方案**：
```bash
# 增加测试 JVM 内存
mvn test -pl <module> -Dargline="-Xmx4g -Xms256m"
```

#### 问题2：端口绑定失败

**症状**：`java.net.BindException: Address already in use`

**解决方案**：
```bash
# 检查端口占用
lsof -i :<port>

# 常见原因：之前的测试未正确清理，遗留了 Timeline Service 进程
# 解决：杀掉占用进程
kill -9 <pid>
```

#### 问题3：Flaky Test（间歇性失败）

**症状**：测试在 CI 中偶尔失败，本地无法重现

**常见原因**：
- **时序依赖**：测试假设某个操作在特定时间内完成，但 CI 机器较慢
- **端口冲突**：多个测试同时使用相同端口
- **临时文件未清理**：上一次测试的残留文件影响了下一次

**解决方案**：
```bash
# 多次运行测试验证稳定性
for i in {1..5}; do mvn test -pl <module> -Dtest=<TestClass>; done

# 使用 Maven 的 rerunFailingTestsCount 自动重试
mvn test -pl <module> -Dtest=<TestClass> -DrerunFailingTestsCount=3
```

#### 问题4：Spark 版本相关的测试失败

**症状**：测试在某个 Spark 版本下失败，在其他版本下通过

**常见原因**：
- Spark API 在不同版本间有差异
- 某些 Spark 功能在特定版本中被废弃或移除

**解决方案**：
```bash
# 使用版本特定的 Profile 运行测试
mvn test -pl hudi-spark-datasource/hudi-spark -Dspark3.5 -Dtest=<TestClass>

# 查看是否有版本特定的适配代码
# hudi-spark-datasource/hudi-spark3.5.x/ 中可能有版本特定的实现
```

### 24.3 常见编译加速技巧

```bash
# 跳过测试和 Javadoc
mvn clean package -DskipTests -Dmaven.javadoc.skip=true

# 只构建需要的模块（-pl 指定模块，-am 包含依赖）
mvn clean package -DskipTests -pl hudi-client/hudi-spark-client -am

# 使用 Maven 的增量构建
mvn package -DskipTests -pl <module>  # 不加 clean

# 并行构建（谨慎使用，可能引发资源竞争）
mvn clean package -DskipTests -T 4

# 跳过 Checkstyle（仅在调试时使用，提交前必须检查）
mvn clean package -DskipTests -Dcheckstyle.skip=true
```

### 24.4 Maven 依赖版本速查

以下是 Hudi v1.2.0-SNAPSHOT 使用的关键依赖版本（摘自 pom.xml）：

| 依赖 | 版本 | 用途 |
|------|------|------|
| JUnit Jupiter | 5.14.1 | 测试框架 |
| Mockito | 5.21.0 | Mock 框架 |
| Avro | 1.11.4 | 序列化框架 |
| Parquet | 1.13.1 | 列式存储 |
| Hadoop | 2.10.2 | 分布式存储 |
| Hive | 2.3.10 | Metastore 集成 |
| SLF4J | 1.7.36 | 日志门面 |
| Log4j2 | 2.25.4 | 日志实现 |
| Jackson | 2.15.2 | JSON 处理 |
| Lombok | 1.18.36 | 样板代码生成 |
| Awaitility | 3.1.2 | 异步测试工具 |
| Testcontainers | 1.21.4 | Docker 容器测试 |

---

## 附录A：测试类索引速查表

### 核心写入测试

| 测试类 | 路径 | 测试内容 |
|--------|------|---------|
| `TestHoodieClientOnCopyOnWriteStorage` | `hudi-spark-datasource/hudi-spark/src/test/java/org/apache/hudi/client/functional/` | COW 表的全面写入测试 |
| `TestHoodieClientMultiWriter` | `hudi-spark-datasource/hudi-spark/src/test/java/org/apache/hudi/client/` | 多 Writer 并发控制 |

### 索引测试

| 测试类 | 路径 | 测试内容 |
|--------|------|---------|
| `TestHoodieBloomIndex` | `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/index/bloom/` | Bloom Filter 索引 |

### 表服务测试

| 测试类 | 路径 | 测试内容 |
|--------|------|---------|
| `TestHoodieCompactionStrategy` | `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/table/action/compact/strategy/` | Compaction 策略 |
| `TestScheduleCompactionActionExecutor` | `hudi-client/hudi-client-common/src/test/java/org/apache/hudi/table/action/compact/` | Compaction 调度 |
| `TestCleanerInsertAndCleanByVersions` | `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/table/action/clean/` | Clean 操作 |

### 基础设施测试

| 测试类 | 路径 | 测试内容 |
|--------|------|---------|
| `HoodieCommonTestHarness` | `hudi-hadoop-common/src/test/java/org/apache/hudi/common/testutils/` | 基础测试工具 |
| `HoodieWriterClientTestHarness` | `hudi-client/hudi-client-common/src/test/java/org/apache/hudi/utils/` | 写入客户端测试基类 |
| `HoodieSparkClientTestHarness` | `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/testutils/` | Spark 环境测试基类 |
| `SparkClientFunctionalTestHarness` | `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/testutils/` | Spark 功能测试基类 |
| `HoodieTestTable` | `hudi-hadoop-common/src/test/java/org/apache/hudi/common/testutils/` | 模拟表构建器 |
| `HoodieTestDataGenerator` | `hudi-common/src/test/java/org/apache/hudi/common/testutils/` | 测试数据生成器 |
| `HdfsTestService` | `hudi-hadoop-common/src/test/java/org/apache/hudi/common/testutils/minicluster/` | 嵌入式 HDFS |

---

## 附录B：关键配置类速查表

| 配置类 | 路径 | 配置范围 |
|--------|------|---------|
| `HoodieWriteConfig` | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/` | 写入总配置 |
| `HoodieIndexConfig` | 同上 | 索引配置 |
| `HoodieCompactionConfig` | 同上 | Compaction 配置 |
| `HoodieClusteringConfig` | 同上 | Clustering 配置 |
| `HoodieCleanConfig` | 同上 | Clean 配置 |
| `HoodieArchivalConfig` | 同上 | 归档配置 |
| `HoodieLockConfig` | 同上 | 锁配置 |
| `HoodiePreCommitValidatorConfig` | 同上 | 预提交验证配置 |
| `HoodieMetadataConfig` | `hudi-common/src/main/java/org/apache/hudi/common/config/` | 元数据表配置 |
| `HoodieStorageConfig` | 同上 | 存储配置 |
| `FileSystemViewStorageConfig` | 同上 | 文件视图配置 |
| `LockConfiguration` | 同上 | 锁配置项 |

---

## 附录C：构建命令速查表

```bash
# ===== 基础构建 =====
# 默认构建（Spark 3.5 + Flink 1.20）
mvn clean package -DskipTests

# 构建指定 Spark 版本
mvn clean package -DskipTests -Dspark3.3
mvn clean package -DskipTests -Dspark3.4
mvn clean package -DskipTests -Dspark3.5    # 默认
mvn clean package -DskipTests -Dspark4.0    # 需要 Java 17

# 构建指定 Flink 版本
mvn clean package -DskipTests -Dflink1.17
mvn clean package -DskipTests -Dflink1.18
mvn clean package -DskipTests -Dflink1.19
mvn clean package -DskipTests -Dflink1.20   # 默认
mvn clean package -DskipTests -Dflink2.0
mvn clean package -DskipTests -Dflink2.1

# 构建指定 Scala 版本
mvn clean package -DskipTests -Dscala-2.12  # 默认
mvn clean package -DskipTests -Dscala-2.13  # 仅 Spark 3.5+

# ===== 模块构建 =====
# 只构建核心模块
mvn clean package -DskipTests -pl hudi-common -am
mvn clean package -DskipTests -pl hudi-client/hudi-spark-client -am

# ===== 测试运行 =====
# 单元测试
mvn -Punit-tests test

# 功能测试
mvn -Pfunctional-tests test

# 功能测试（B 组）
mvn -Pfunctional-tests-b test

# 功能测试（C 组）
mvn -Pfunctional-tests-c test

# 集成测试
mvn -Pintegration-tests verify

# 单个测试
mvn test -pl hudi-common -Dtest=TestHoodieTimer
mvn test -pl hudi-common -Dtest=TestHoodieTimer#testTimer

# ===== 代码检查 =====
# Checkstyle
mvn checkstyle:check -pl <module>

# 生成 Javadoc
mvn javadoc:aggregate -Pjavadocs
```

---

## 附录D：关键源码路径总结

### 核心模块

```
hudi-io/src/main/java/org/apache/hudi/
├── io/                                    # I/O 接口
└── annotation/                            # 公共 API 注解

hudi-common/src/main/java/org/apache/hudi/common/
├── config/                                # 配置系统
├── model/                                 # 数据模型
├── table/
│   ├── HoodieTableMetaClient.java         # 表元数据入口
│   ├── HoodieTableConfig.java             # 表配置
│   ├── timeline/
│   │   ├── HoodieTimeline.java            # 时间线接口
│   │   └── HoodieInstant.java             # 时间线事件
│   ├── view/
│   │   └── HoodieTableFileSystemView.java # 文件系统视图
│   └── log/
│       └── HoodieLogFormat.java           # 日志文件格式
├── bloom/                                 # Bloom Filter
└── util/                                  # 工具类

hudi-client/hudi-client-common/src/main/java/org/apache/hudi/
├── client/
│   ├── BaseHoodieWriteClient.java         # 写入客户端基类
│   └── BaseHoodieTableServiceClient.java  # 表服务客户端
├── config/
│   └── HoodieWriteConfig.java             # 写入配置
├── index/
│   ├── HoodieIndex.java                   # 索引接口
│   └── bloom/
│       └── HoodieBloomIndex.java          # Bloom 索引
├── io/                                    # I/O Handle
└── table/action/
    ├── commit/                            # 提交
    ├── compact/                           # Compaction
    ├── clean/                             # Clean
    ├── cluster/                           # Clustering
    └── rollback/                          # Rollback
```

### 测试基础设施

```
hudi-hadoop-common/src/test/java/org/apache/hudi/common/testutils/
├── HoodieCommonTestHarness.java           # 基础测试工具
├── HoodieTestTable.java                   # 模拟表构建器
└── minicluster/
    └── HdfsTestService.java               # 嵌入式 HDFS

hudi-common/src/test/java/org/apache/hudi/common/testutils/
└── HoodieTestDataGenerator.java           # 测试数据生成

hudi-client/hudi-client-common/src/test/java/org/apache/hudi/utils/
└── HoodieWriterClientTestHarness.java     # 写入测试基类

hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/testutils/
├── HoodieSparkClientTestHarness.java      # Spark 测试基类
├── HoodieClientTestBase.java              # 客户端测试基类
├── SparkClientFunctionalTestHarness.java  # 功能测试基类
└── HoodieSparkWriteableTestTable.java     # Spark 可写测试表

hudi-common/src/test/java/org/apache/hudi/common/testutils/
└── HoodieTestDataGenerator.java           # 数据生成器
```

### 代码规范

```
style/
├── checkstyle.xml                         # Java 检查规则
├── checkstyle-suppressions.xml            # 规则抑制
├── import-control.xml                     # 导入控制
└── scalastyle.xml                         # Scala 检查规则
```

### RFC

```
rfc/
├── rfc-8/rfc-8.md       # Record Index
├── rfc-27/rfc-27.md     # Data Skipping Index
├── rfc-37/rfc-37.md     # Metadata Bloom Index
├── rfc-42/rfc-42.md     # Consistent Hashing Index
├── rfc-46/rfc-46.md     # Optimize Record Payload
├── rfc-48/rfc-48.md     # Log Compaction
├── rfc-51/rfc-51.md     # CDC Support
├── rfc-53/rfc-53.md     # Lock-Free Queue
├── rfc-56/rfc-56.md     # Early Conflict Detection
├── rfc-63/rfc-63.md     # Expression Indexes
├── rfc-69/rfc-69.md     # Hudi 1.X
├── rfc-73/rfc-73.md     # Multi-Table Transactions
├── rfc-77/rfc-77.md     # Secondary Indexes
└── rfc-78/rfc-78.md     # 1.0 Migration
```

---

*本文档基于 Apache Hudi v1.2.0-SNAPSHOT 源码编写。随着项目的持续发展，部分细节可能会有所变化，请以最新源码为准。*
