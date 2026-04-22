# Hudi Timeline Service 架构与源码深度解析

> 基于 Apache Hudi v1.2.0-SNAPSHOT 源码  
> 源码路径：`/Users/wanghaofeng/IdeaProjects/hudi`

---

## 目录

- [第一部分：Timeline Service 整体架构](#第一部分timeline-service-整体架构)
  - [1.1 为什么需要 Timeline Service](#11-为什么需要-timeline-service)
  - [1.2 TimelineService 核心类解析](#12-timelineservice-核心类解析)
  - [1.3 Javalin HTTP 服务的启动流程](#13-javalin-http-服务的启动流程)
  - [1.4 端口绑定与重试策略](#14-端口绑定与重试策略)
  - [1.5 线程模型与 Jetty 配置](#15-线程模型与-jetty-配置)
  - [1.6 FileSystemView 存储类型选择](#16-filesystemview-存储类型选择)
  - [1.7 嵌入式 vs 独立部署](#17-嵌入式-vs-独立部署)
  - [1.8 EmbeddedTimelineService 深度解析](#18-embeddedtimelineservice-深度解析)
- [第二部分：核心 Handler 体系](#第二部分核心-handler-体系)
  - [2.1 RequestHandler 路由中枢](#21-requesthandler-路由中枢)
  - [2.2 Handler 基类设计](#22-handler-基类设计)
  - [2.3 TimelineHandler 解析](#23-timelinehandler-解析)
  - [2.4 BaseFileHandler 解析](#24-basefilehandler-解析)
  - [2.5 FileSliceHandler 解析](#25-fileslicehandler-解析)
  - [2.6 MarkerHandler 解析](#26-markerhandler-解析)
  - [2.7 RemotePartitionerHandler 解析](#27-remotepartitionerhandler-解析)
  - [2.8 ViewHandler 内部类与刷新检查](#28-viewhandler-内部类与刷新检查)
- [第三部分：服务端 FileSystemView 管理](#第三部分服务端-filesystemview-管理)
  - [3.1 FileSystemViewManager 全局视图管理器](#31-filesystemviewmanager-全局视图管理器)
  - [3.2 FileSystemViewStorageType 存储类型](#32-filesystemviewstoragetype-存储类型)
  - [3.3 IncrementalTimelineSyncFileSystemView 增量同步机制](#33-incrementaltimelinesyncfilesystemview-增量同步机制)
  - [3.4 PriorityBasedFileSystemView 优先级降级视图](#34-prioritybasedfilesystemview-优先级降级视图)
  - [3.5 Timeline Hash 一致性校验机制](#35-timeline-hash-一致性校验机制)
- [第四部分：Marker 管理服务](#第四部分marker-管理服务)
  - [4.1 Marker 机制概述](#41-marker-机制概述)
  - [4.2 MarkerDirState 核心状态管理](#42-markerdirstate-核心状态管理)
  - [4.3 批量 Marker 创建的完整链路](#43-批量-marker-创建的完整链路)
  - [4.4 MarkerCreationDispatchingRunnable 调度器](#44-markercreationdispatchingrunnable-调度器)
  - [4.5 BatchedMarkerCreationRunnable 批处理器](#45-batchedmarkercreationrunnable-批处理器)
  - [4.6 TimelineServerBasedWriteMarkers 客户端](#46-timelineserverbasedwritemarkers-客户端)
  - [4.7 AsyncTimelineServerBasedDetectionStrategy 冲突检测](#47-asynctimelineserverbaseddetectionstrategy-冲突检测)
  - [4.8 MarkerBasedEarlyConflictDetectionRunnable 检测实现](#48-markerbasedearlyconflictdetectionrunnable-检测实现)
- [第五部分：客户端与服务端的交互](#第五部分客户端与服务端的交互)
  - [5.1 TimelineServiceClientBase 基类](#51-timelineserviceclientbase-基类)
  - [5.2 TimelineServiceClient HTTP 客户端](#52-timelineserviceclient-http-客户端)
  - [5.3 RemoteHoodieTableFileSystemView 远程代理](#53-remotehoodietablefilesystemview-远程代理)
  - [5.4 DTO 序列化传输体系](#54-dto-序列化传输体系)
  - [5.5 请求-响应完整流程](#55-请求-响应完整流程)
- [第六部分：生产运维](#第六部分生产运维)
  - [6.1 TLS 配置参数完整手册](#61-tls-配置参数完整手册)
  - [6.2 TLS 故障排查](#62-tls-故障排查)
  - [6.3 TLS 性能调优](#63-tls-性能调优)
- [总结](#总结)

---

## 第一部分：Timeline Service 整体架构

### 1.1 为什么需要 Timeline Service

#### 问题背景：Executor 重复构建 FileSystemView 的代价

在 Apache Hudi 的写入和查询过程中，每个 Task（Spark Executor 或 Flink TaskManager）都需要知道当前表的文件系统视图（FileSystemView）。FileSystemView 记录了每个分区下的文件分组（FileGroup）、文件切片（FileSlice）、基础文件（BaseFile）和日志文件（LogFile）的信息。

如果没有 Timeline Service，每个 Executor 都需要独立执行以下操作：

1. **LIST 操作**：对文件系统执行 `listStatus`/`listFiles` 操作，扫描分区目录下的所有文件
2. **Timeline 解析**：读取 `.hoodie` 目录下的所有 instant 文件，构建 Timeline
3. **FileGroup 组装**：将文件按 FileId 分组，关联 BaseFile 和 LogFile
4. **Compaction/Clustering 状态**：解析 pending compaction 和 clustering 的计划文件

在一个有数千分区、数万文件的大表上，这些操作的代价非常高昂：

- **HDFS/S3 LIST 操作的高延迟**：云存储上的 LIST 操作通常需要数百毫秒甚至数秒
- **N 个 Executor 执行 N 次**：如果有 200 个 Executor 并发写入，就会发起 200 次几乎相同的 LIST 操作
- **文件系统压力**：大量并发 LIST 请求会对 NameNode / S3 端点造成压力
- **延迟累积**：每个 Task 的初始化延迟直接影响整体任务的完成时间

#### Timeline Service 的解决方案

Timeline Service（简称 TLS）通过"集中构建，全局共享"的思路解决了上述问题：

```
没有 TLS 时：
Executor-1 ──> LIST 文件系统 ──> 构建 FileSystemView
Executor-2 ──> LIST 文件系统 ──> 构建 FileSystemView
Executor-3 ──> LIST 文件系统 ──> 构建 FileSystemView
...
Executor-N ──> LIST 文件系统 ──> 构建 FileSystemView
（N 次重复的文件系统操作）

有 TLS 时：
TLS(Driver) ──> LIST 文件系统 ──> 构建 FileSystemView（仅 1 次）
Executor-1 ──> HTTP 请求 ──> TLS ──> 返回结果
Executor-2 ──> HTTP 请求 ──> TLS ──> 返回结果
Executor-3 ──> HTTP 请求 ──> TLS ──> 返回结果
...
Executor-N ──> HTTP 请求 ──> TLS ──> 返回结果
（N 次轻量级 HTTP 调用，仅 1 次文件系统 LIST）
```

**设计好处总结**：

| 方面 | 没有 TLS | 有 TLS |
|------|---------|--------|
| LIST 操作次数 | N 次（每个 Executor 一次） | 1 次（TLS 集中构建） |
| 文件系统压力 | 高（并发 LIST） | 低（单次 LIST） |
| 内存占用 | 每个 Executor 都缓存完整视图 | 仅 TLS 缓存一份 |
| 数据一致性 | 可能不一致（各 Executor 构建时间不同） | 一致（统一版本） |
| Marker 管理 | 每个 Executor 独立创建 marker 文件 | 批量合并，减少小文件 |

---

### 1.2 TimelineService 核心类解析

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/TimelineService.java`

TimelineService 是整个 Timeline 服务的核心入口类。它封装了 Javalin HTTP 服务器，管理 FileSystemView，并暴露 RESTful API。

```java
@Slf4j
public class TimelineService {

  private static final int START_SERVICE_MAX_RETRIES = 16;
  private static final int DEFAULT_NUM_THREADS = 250;

  @Getter
  private int serverPort;
  private final Config timelineServerConf;
  @Getter
  private final StorageConfiguration<?> storageConf;
  private transient Javalin app = null;
  private transient FileSystemViewManager fsViewsManager;
  private transient RequestHandler requestHandler;
  // ...
}
```

**核心成员解析**：

1. **`serverPort`**：HTTP 服务监听端口，支持动态分配（端口 0 表示随机选择空闲端口）
2. **`timelineServerConf`**：Timeline Service 的配置对象，包括线程数、存储类型、压缩策略等
3. **`storageConf`**：存储配置（如 Hadoop Configuration），用于访问文件系统
4. **`app`**：Javalin HTTP 框架实例，底层使用 Jetty 作为 Servlet 容器
5. **`fsViewsManager`**：全局的 FileSystemView 管理器，按 basePath 维护每张表的文件视图
6. **`requestHandler`**：HTTP 请求路由处理器，分发请求到各个 Handler

**为什么选择 Javalin 框架？**

Javalin 是一个轻量级的 Java Web 框架，底层基于 Jetty。选择它的原因：

- **轻量级**：没有过多的依赖和框架开销，适合嵌入到 Spark Driver 进程中
- **API 简洁**：注册路由只需 `app.get("/path", handler)` 一行代码
- **Jetty 底层**：可以直接配置 Jetty 的线程池、调度器等高级参数
- **嵌入式友好**：不需要额外的 Web 容器，可以在任何 JVM 进程中启动

---

### 1.3 Javalin HTTP 服务的启动流程

服务启动的核心方法是 `createApp()`，让我们详细分析：

```java
private void createApp() {
    // 如果 app 需要重建，先停止已有的
    if (app != null) {
        app.stop();
    }
    // 1. 配置线程池
    int maxThreads = timelineServerConf.numThreads > 0
        ? timelineServerConf.numThreads : DEFAULT_NUM_THREADS;
    QueuedThreadPool pool = new QueuedThreadPool(maxThreads, 8, 60_000);
    pool.setDaemon(true);

    // 2. 创建 Jetty Server
    final Server server = new Server(pool);
    ScheduledExecutorScheduler scheduler =
        new ScheduledExecutorScheduler("TimelineService-JettyScheduler", true, 8);
    server.addBean(scheduler);

    // 3. 创建 Javalin 应用
    app = Javalin.create(c -> {
        if (!timelineServerConf.compress) {
            c.compressionStrategy(
                io.javalin.core.compression.CompressionStrategy.NONE);
        }
        c.server(() -> server);
    });

    // 4. 创建请求处理器并注册路由
    requestHandler = new RequestHandler(
        app, storageConf, timelineServerConf, fsViewsManager);
    app.get("/", ctx -> ctx.result("Hello Hudi"));
    requestHandler.register();
}
```

**启动流程分解**：

**步骤 1：配置 Jetty 线程池**

- 使用 `QueuedThreadPool`（Jetty 的标准线程池实现）
- 最大线程数默认 250，最小线程数 8，空闲超时 60 秒
- `setDaemon(true)` 设为守护线程——这很重要，因为当 Spark Driver 退出时，TLS 线程不应阻止 JVM 关闭

**步骤 2：创建 Jetty Server 和调度器**

- `ScheduledExecutorScheduler` 提供定时任务支持，核心线程数为 8
- 也设为守护模式，保证不影响 JVM 正常退出

**步骤 3：配置 Javalin**

- 默认启用 GZIP 压缩（`compress = true`），减少网络传输量
- 通过 `c.server(() -> server)` 将自定义的 Jetty Server 注入 Javalin

**步骤 4：注册路由**

- 先注册一个根路径 `/`，返回 "Hello Hudi"，用于健康检查
- 然后通过 `requestHandler.register()` 注册所有业务 API 路由

**为什么这么设计？**

将线程池和服务器创建从 Javalin 中抽出来，允许：
1. 精细控制线程池参数（不依赖 Javalin 默认值）
2. 设置守护线程模式（Javalin 默认不设置）
3. 添加 Jetty 原生组件（如 ScheduledExecutorScheduler）

---

### 1.4 端口绑定与重试策略

**源码路径**：`TimelineService.java` 的 `startServiceOnPort()` 方法

```java
private int startServiceOnPort(int port) throws IOException {
    if (!(port == 0 || (1024 <= port && port < 65536))) {
        throw new IllegalArgumentException(
            String.format("startPort should be between 1024 and 65535 (inclusive), "
                + "or 0 for a random free port. but now is %s.", port));
    }
    for (int attempt = 0; attempt < START_SERVICE_MAX_RETRIES; attempt++) {
        // 端口递增并在 1024-65535 范围内循环
        int tryPort = port == 0 ? port
            : (port + attempt - 1024) % (65536 - 1024) + 1024;
        try {
            createApp();
            app.start(tryPort);
            return app.port();
        } catch (Exception e) {
            if (e instanceof JavalinBindException) {
                if (tryPort == 0) {
                    log.warn("Timeline server could not bind on a random free port.");
                } else {
                    log.warn("Timeline server could not bind on port {}. "
                        + "Attempting port {} + 1.", tryPort, tryPort);
                }
            } else {
                log.warn("Timeline server start failed on port {}. "
                    + "Attempting port {} + 1.", tryPort, tryPort, e);
            }
        }
    }
    throw new IOException(String.format(
        "Timeline server start failed on port %d, after retry %d times",
        port, START_SERVICE_MAX_RETRIES));
}
```

**端口绑定策略**：

1. **端口 0 模式**：当 `port = 0` 时，由操作系统自动分配空闲端口。这是嵌入式 TLS 的默认行为
2. **指定端口模式**：当指定了具体端口时，如果该端口被占用，会依次尝试 port+1, port+2, ...
3. **环绕策略**：端口号在 1024-65535 范围内循环，`(port + attempt - 1024) % (65536 - 1024) + 1024` 确保不会使用特权端口（0-1023）
4. **最大重试 16 次**：由 `START_SERVICE_MAX_RETRIES = 16` 控制

**为什么这么设计？**

在生产环境中，同一台机器可能运行多个 Spark 作业，每个作业都会启动自己的嵌入式 TLS。端口重试机制避免了端口冲突导致作业启动失败。使用端口 0 则完全由 OS 选择，是最安全的方式。

---

### 1.5 线程模型与 Jetty 配置

TLS 的线程模型直接影响并发处理能力。让我们分析其配置：

```java
// TimelineService.Config 中的线程配置
@Builder.Default
@Parameter(names = {"--threads", "-t"},
    description = "Number of threads to use for serving requests. Default is 250")
public int numThreads = DEFAULT_NUM_THREADS;

@Builder.Default
@Parameter(names = {"--async"},
    description = "Use asynchronous request processing")
public boolean async = false;
```

**线程池参数**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| maxThreads | 250 | QueuedThreadPool 最大线程数 |
| minThreads | 8 | QueuedThreadPool 最小线程数（硬编码） |
| idleTimeout | 60,000ms | 空闲线程的回收时间 |
| daemon | true | 守护线程模式 |

**Jetty QueuedThreadPool 的工作机制**：

- 初始状态创建 8 个核心线程
- 请求到来时，如果现有线程都在忙碌，会创建新线程，最多到 250 个
- 空闲超过 60 秒的线程会被回收（但不低于 8 个）
- 如果 250 个线程都在处理请求，新请求会排队等待

**异步处理模式**：

当 `async = true` 时，JSON 序列化操作会在独立的线程中执行，释放 Jetty 线程去处理其他请求：

```java
private void writeValueAsStringAsync(Context ctx, Object obj) {
    ctx.future(CompletableFuture.supplyAsync(() -> {
        try {
            return jsonifyResult(ctx, obj, metricsRegistry);
        } catch (JsonProcessingException e) {
            throw new HoodieException("Failed to JSON encode the value", e);
        }
    }, asyncResultService));
}
```

**为什么默认不启用异步？**

大多数 TLS 请求的 JSON 序列化非常快（毫秒级），异步化带来的上下文切换开销可能反而更大。只有在序列化耗时（如返回大量 FileSlice 数据）且并发极高时，异步模式才有优势。

---

### 1.6 FileSystemView 存储类型选择

Timeline Service 在服务端维护 FileSystemView 时，支持三种存储类型：

```java
public static FileSystemViewManager buildFileSystemViewManager(
    Config config, StorageConfiguration<?> conf) {
    // ...
    switch (config.viewStorageType) {
        case MEMORY:
            // 纯内存存储
            return FileSystemViewManager.createViewManager(...);
        case SPILLABLE_DISK:
            // 内存 + 磁盘溢出
            return FileSystemViewManager.createViewManager(...);
        case EMBEDDED_KV_STORE:
            // RocksDB 本地 KV 存储
            return FileSystemViewManager.createViewManager(...);
    }
}
```

**源码路径**：`hudi-common/src/main/java/org/apache/hudi/common/table/view/FileSystemViewStorageType.java`

```java
public enum FileSystemViewStorageType {
    MEMORY,            // 纯内存
    SPILLABLE_DISK,    // 内存 + 磁盘溢出（默认）
    EMBEDDED_KV_STORE, // RocksDB
    REMOTE_ONLY,       // 仅远程（客户端使用）
    REMOTE_FIRST       // 远程优先 + 本地降级（客户端使用）
}
```

**各存储类型对比**：

| 存储类型 | 适用场景 | 内存需求 | 性能 |
|---------|---------|---------|------|
| `MEMORY` | 小中型表 | 高 | 最快 |
| `SPILLABLE_DISK` | 大表（默认） | 可控 | 快（热数据在内存） |
| `EMBEDDED_KV_STORE` | 超大表 | 低 | 较快（RocksDB） |

**为什么独立部署的 TLS 默认使用 SPILLABLE_DISK？**

独立部署的 TLS 可能同时管理多张表，内存压力大。SPILLABLE_DISK 模式在内存充足时等同于 MEMORY，内存不足时自动溢出到磁盘，提供了最好的弹性。默认配置中，每张表允许 2048MB 的视图内存（`maxViewMemPerTableInMB = 2048`）。

---

### 1.7 嵌入式 vs 独立部署

Timeline Service 支持两种部署模式：

**模式 1：嵌入式部署（Embedded）**

TLS 以嵌入式方式运行在 Spark Driver 或 Flink JobManager 进程中。这是默认模式。

```
+----------------------------------+
| Spark Driver / Flink JobManager  |
|   +----------------------------+ |
|   | EmbeddedTimelineService    | |
|   |   +---------------------+  | |
|   |   | TimelineService     |  | |
|   |   |   Javalin HTTP      |  | |
|   |   |   FileSystemView    |  | |
|   |   +---------------------+  | |
|   +----------------------------+ |
+----------------------------------+
       ^           ^           ^
       |           |           |
  Executor-1  Executor-2  Executor-3
  (HTTP请求)   (HTTP请求)   (HTTP请求)
```

**模式 2：独立部署（Standalone）**

TLS 作为独立进程运行，可以通过命令行启动：

```java
// TimelineService.main()
public static void main(String[] args) throws Exception {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help) {
        cmd.usage();
        System.exit(1);
    }

    StorageConfiguration<?> storageConf = HadoopFSUtils.getStorageConf();
    FileSystemViewManager viewManager =
        buildFileSystemViewManager(cfg, storageConf.newInstance());
    TimelineService service = new TimelineService(
        storageConf.newInstance(), cfg, viewManager);
    service.run();
}
```

启动命令示例：
```bash
java -cp <classpath> org.apache.hudi.timeline.service.TimelineService \
  --server-port 26754 \
  --view-storage SPILLABLE_DISK \
  --max-view-mem-per-table 2048 \
  --threads 250
```

**两种模式的对比**：

| 特性 | 嵌入式 | 独立部署 |
|------|--------|---------|
| 进程 | 与 Driver 同进程 | 独立 JVM |
| 生命周期 | 随 Job 启停 | 持久运行 |
| 多表支持 | 通常单表 | 可管理多表 |
| 端口 | 动态分配（端口 0） | 固定端口 |
| 运维 | 无需额外运维 | 需要单独运维 |
| 可用性 | Job 结束后不可用 | 持续可用 |
| 适用场景 | 大多数写入场景 | 多 Job 共享、长期查询 |

**为什么嵌入式是默认模式？**

嵌入式模式不需要额外的部署运维，且每个 Job 有自己独立的 TLS，避免了多租户之间的资源竞争。对于大多数 Spark/Flink 写入作业来说，嵌入式模式已经足够。

---

### 1.8 EmbeddedTimelineService 深度解析

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/embedded/EmbeddedTimelineService.java`

EmbeddedTimelineService 是嵌入式 TLS 的管理类，负责创建、启动、复用和停止 TLS 实例。

#### 核心架构

```java
@Slf4j
public class EmbeddedTimelineService {
    // 全局锁，用于启停和修改嵌入式服务
    private static final Object SERVICE_LOCK = new Object();
    // 运行中的服务器数量统计
    private static final AtomicInteger NUM_SERVERS_RUNNING = new AtomicInteger(0);
    // 已运行的服务映射：TimelineServiceIdentifier -> EmbeddedTimelineService
    private static final Map<TimelineServiceIdentifier, EmbeddedTimelineService>
        RUNNING_SERVICES = new HashMap<>();
    
    private int serverPort;
    private String hostAddr;
    private final HoodieEngineContext context;
    private final StorageConfiguration<?> storageConf;
    private final HoodieWriteConfig writeConfig;
    private TimelineService.Config serviceConfig;
    private final TimelineServiceIdentifier timelineServiceIdentifier;
    private final Set<String> basePaths; // 使用此 TLS 的表路径集合
    
    @Getter
    private transient FileSystemViewManager viewManager;
    private transient TimelineService server;
}
```

#### 服务复用机制

当多个 Writer 在同一个 JVM 中运行时（如同一 Spark Application 写入多张 Hudi 表），EmbeddedTimelineService 支持复用同一个 TLS 实例：

```java
public static EmbeddedTimelineService getOrStartEmbeddedTimelineService(
    HoodieEngineContext context,
    String embeddedTimelineServiceHostAddr,
    HoodieWriteConfig writeConfig) throws IOException {
    
    TimelineServiceIdentifier identifier =
        getTimelineServiceIdentifier(embeddedTimelineServiceHostAddr, writeConfig);
    
    if (writeConfig.isEmbeddedTimelineServerReuseEnabled()) {
        synchronized (SERVICE_LOCK) {
            if (RUNNING_SERVICES.containsKey(identifier)) {
                // 复用已有实例，添加新的 basePath
                RUNNING_SERVICES.get(identifier).addBasePath(writeConfig.getBasePath());
                return RUNNING_SERVICES.get(identifier);
            }
            // 没有兼容实例，创建新的
            EmbeddedTimelineService service = createAndStartService(...);
            RUNNING_SERVICES.put(identifier, service);
            return service;
        }
    }
    // 不复用，直接创建新实例
    return createAndStartService(...);
}
```

**复用判定条件**（TimelineServiceIdentifier）：

```java
static class TimelineServiceIdentifier {
    private final String hostAddr;
    private final MarkerType markerType;
    private final boolean isMetadataEnabled;
    private final boolean isEarlyConflictDetectionEnable;
    
    // equals 方法：四个字段都相同才认为是同一个标识
}
```

**为什么需要 TimelineServiceIdentifier？**

不同配置的 Writer 不能共享同一个 TLS。例如：
- 一个 Writer 使用 `TIMELINE_SERVER_BASED` marker，另一个使用 `DIRECT` marker，它们的 TLS 配置不同
- 一个启用了 Metadata Table，另一个没有，FileSystemView 的数据源不同

只有在这些关键配置完全一致时，才能安全复用。

#### 服务启动配置构建

```java
private void startServer(TimelineServiceCreator timelineServiceCreator) throws IOException {
    TimelineService.Config.ConfigBuilder timelineServiceConfBuilder =
        TimelineService.Config.builder()
            .serverPort(writeConfig.getEmbeddedTimelineServerPort())
            .numThreads(writeConfig.getEmbeddedTimelineServerThreads())
            .compress(writeConfig.getEmbeddedTimelineServerCompressOutput())
            .async(writeConfig.getEmbeddedTimelineServerUseAsync());
    
    // 只有使用 TIMELINE_SERVER_BASED marker 时才配置 marker 相关参数
    if (writeConfig.getMarkersType() == MarkerType.TIMELINE_SERVER_BASED) {
        timelineServiceConfBuilder
            .enableMarkerRequests(true)
            .markerBatchNumThreads(
                writeConfig.getMarkersTimelineServerBasedBatchNumThreads())
            .markerBatchIntervalMs(
                writeConfig.getMarkersTimelineServerBasedBatchIntervalMs())
            .markerParallelism(writeConfig.getMarkersDeleteParallelism());
    }
    
    // 早期冲突检测配置
    if (writeConfig.isEarlyConflictDetectionEnable()) {
        timelineServiceConfBuilder
            .earlyConflictDetectionEnable(true)
            .earlyConflictDetectionStrategy(
                writeConfig.getEarlyConflictDetectionStrategyClassName())
            // ...更多配置
    }
    
    this.serviceConfig = timelineServiceConfBuilder.build();
    server = timelineServiceCreator.create(
        storageConf.newInstance(), serviceConfig, viewManager);
    serverPort = server.startService();
}
```

#### 客户端视图配置生成

EmbeddedTimelineService 启动后，需要生成一个配置给 Executor 使用，使它们能连接到 TLS：

```java
public FileSystemViewStorageConfig getRemoteFileSystemViewConfig(
    HoodieWriteConfig clientWriteConfig) {
    // 决定使用 REMOTE_FIRST 还是 REMOTE_ONLY
    FileSystemViewStorageType viewStorageType =
        clientWriteConfig.getClientSpecifiedViewStorageConfig()
            .shouldEnableBackupForRemoteFileSystemView()
            ? FileSystemViewStorageType.REMOTE_FIRST
            : FileSystemViewStorageType.REMOTE_ONLY;
    
    return FileSystemViewStorageConfig.newBuilder()
        .withStorageType(viewStorageType)
        .withRemoteServerHost(hostAddr)
        .withRemoteServerPort(serverPort)
        .withRemoteTimelineClientTimeoutSecs(...)
        .withRemoteTimelineClientRetry(...)
        // ...
        .build();
}
```

**为什么默认使用 REMOTE_FIRST 而不是 REMOTE_ONLY？**

`REMOTE_FIRST` 模式在 TLS 不可达时会降级到本地构建 FileSystemView，提供了更好的容错性。在生产环境中，网络瞬断、GC 暂停等原因可能导致 TLS 暂时不可用，`REMOTE_FIRST` 确保了任务不会因此失败。

#### 优雅停止

```java
public void stopForBasePath(String basePath) {
    synchronized (SERVICE_LOCK) {
        basePaths.remove(basePath);
        if (basePaths.isEmpty()) {
            RUNNING_SERVICES.remove(timelineServiceIdentifier);
        }
    }
    if (this.server != null) {
        this.server.unregisterBasePath(basePath);
    }
    // 只有当没有表在使用时才真正关闭
    if (basePaths.isEmpty() && null != server) {
        this.server.close();
        METRICS_REGISTRY.set(NUM_EMBEDDED_TIMELINE_SERVERS,
            NUM_SERVERS_RUNNING.decrementAndGet());
        this.server = null;
        this.viewManager = null;
    }
}
```

**为什么要引用计数（basePaths）？**

当复用模式启用时，一个 TLS 可能被多张表共享。只有当所有表都停止使用后，才能安全关闭 TLS。`basePaths` 集合起到了引用计数的作用。

---

## 第二部分：核心 Handler 体系

### 2.1 RequestHandler 路由中枢

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/RequestHandler.java`

RequestHandler 是所有 HTTP API 请求的路由中枢。它管理四大 Handler，负责 API 注册和请求分发。

```java
@Slf4j
public class RequestHandler {

    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper().registerModule(new AfterburnerModule());

    private final TimelineService.Config timelineServiceConfig;
    private final FileSystemViewManager viewManager;
    private final Javalin app;
    private final TimelineHandler instantHandler;
    private final FileSliceHandler sliceHandler;
    private final BaseFileHandler dataFileHandler;
    private final MarkerHandler markerHandler;
    private RemotePartitionerHandler partitionerHandler;
    private final Registry metricsRegistry =
        Registry.getRegistry("TimelineService");
    private final ScheduledExecutorService asyncResultService;
}
```

**Handler 创建逻辑**：

```java
public RequestHandler(Javalin app, StorageConfiguration<?> conf,
    TimelineService.Config timelineServiceConfig,
    FileSystemViewManager viewManager) {
    this.timelineServiceConfig = timelineServiceConfig;
    this.viewManager = viewManager;
    this.app = app;
    
    // 始终创建的 Handler
    this.instantHandler = new TimelineHandler(conf, timelineServiceConfig, viewManager);
    this.sliceHandler = new FileSliceHandler(conf, timelineServiceConfig, viewManager);
    this.dataFileHandler = new BaseFileHandler(conf, timelineServiceConfig, viewManager);
    
    // 条件创建的 Handler
    if (timelineServiceConfig.enableMarkerRequests) {
        this.markerHandler = new MarkerHandler(
            conf, timelineServiceConfig, viewManager, metricsRegistry);
    } else {
        this.markerHandler = null;
    }
    if (timelineServiceConfig.enableRemotePartitioner) {
        this.partitionerHandler = new RemotePartitionerHandler(
            conf, timelineServiceConfig, viewManager);
    }
    
    // 异步结果服务
    if (timelineServiceConfig.async) {
        this.asyncResultService = Executors.newSingleThreadScheduledExecutor();
    } else {
        this.asyncResultService = null;
    }
}
```

**为什么 MarkerHandler 是条件创建的？**

并非所有 TLS 都需要 Marker 功能。只有当 Writer 使用 `TIMELINE_SERVER_BASED` marker 类型时，才需要启用 Marker API。条件创建避免了不必要的资源占用（Marker 有独立的线程池和调度器）。

#### API 注册架构

```java
public void register() {
    registerDataFilesAPI();      // 注册 BaseFile 相关 API
    registerFileSlicesAPI();     // 注册 FileSlice 相关 API
    registerTimelineAPI();       // 注册 Timeline 相关 API
    if (markerHandler != null) {
        registerMarkerAPI();     // 注册 Marker 相关 API
    }
    if (partitionerHandler != null) {
        registerRemotePartitionerAPI(); // 注册分区器 API
    }
}
```

#### JSON 序列化优化

```java
private static final ObjectMapper OBJECT_MAPPER =
    new ObjectMapper().registerModule(new AfterburnerModule());
```

**为什么使用 AfterburnerModule？**

Jackson AfterburnerModule 使用字节码生成（ASM）来替代 Java 反射进行序列化/反序列化。在 TLS 的高频序列化场景下，这可以显著提升性能（通常 10-30% 的提升）。

#### 指标收集

RequestHandler 内置了全面的指标收集：

```java
private final Registry metricsRegistry = Registry.getRegistry("TimelineService");

// 在每个请求处理中收集指标
metricsRegistry.add("TOTAL_API_TIME", timeTakenMillis);
metricsRegistry.add("TOTAL_REFRESH_TIME", refreshCheckTimeTaken);
metricsRegistry.add("TOTAL_HANDLE_TIME", handleTimeTaken);
metricsRegistry.add("TOTAL_CHECK_TIME", finalCheckTimeTaken);
metricsRegistry.add("TOTAL_API_CALLS", 1);
metricsRegistry.add("WRITE_VALUE_CNT", 1);
metricsRegistry.add("WRITE_VALUE_TIME", jsonifyTime);
```

这些指标对于性能分析和问题排查至关重要。

---

### 2.2 Handler 基类设计

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/Handler.java`

```java
public abstract class Handler {

    protected final StorageConfiguration<?> conf;
    protected final TimelineService.Config timelineServiceConfig;
    protected final FileSystemViewManager viewManager;

    public Handler(StorageConfiguration<?> conf,
                   TimelineService.Config timelineServiceConfig,
                   FileSystemViewManager viewManager) {
        this.conf = conf.newInstance();  // 注意：创建新实例！
        this.timelineServiceConfig = timelineServiceConfig;
        this.viewManager = viewManager;
    }
}
```

**为什么 `conf.newInstance()`？**

`StorageConfiguration`（如 Hadoop Configuration）可能被多线程共享。调用 `newInstance()` 创建副本，避免并发修改问题。Handler 的方法会被多个 Jetty 线程并发调用，因此需要确保配置对象的线程安全性。

**设计模式**：

Handler 使用了**模板方法模式**。基类提供 `viewManager` 和 `conf` 等基础设施，子类专注于实现业务逻辑。所有 Handler 共享同一个 `viewManager` 实例，确保数据一致性。

---

### 2.3 TimelineHandler 解析

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/TimelineHandler.java`

TimelineHandler 处理 Timeline 相关请求，是最简单的 Handler，仅提供两个 API：

```java
public class TimelineHandler extends Handler {

    public List<InstantDTO> getLastInstant(String basePath) {
        return viewManager.getFileSystemView(basePath)
            .getLastInstant()
            .map(InstantDTO::fromInstant)
            .map(Arrays::asList)
            .orElse(Collections.emptyList());
    }

    public TimelineDTO getTimeline(String basePath) {
        return TimelineDTO.fromTimeline(
            viewManager.getFileSystemView(basePath).getTimeline());
    }
}
```

**API 端点对应关系**：

| HTTP 路径 | 方法 | 说明 |
|-----------|------|------|
| `/v1/hoodie/view/timeline/instant/last` | GET | 获取最新 Instant |
| `/v1/hoodie/view/timeline/instants/all` | GET | 获取完整 Timeline |

**API 注册代码**：

```java
private void registerTimelineAPI() {
    app.get(RemoteHoodieTableFileSystemView.LAST_INSTANT_URL,
        new ViewHandler(ctx -> {
            metricsRegistry.add("LAST_INSTANT", 1);
            List<InstantDTO> dtos =
                instantHandler.getLastInstant(getBasePathParam(ctx));
            writeValueAsString(ctx, dtos);
        }, false));  // false = 不执行 refreshCheck

    app.get(RemoteHoodieTableFileSystemView.TIMELINE_URL,
        new ViewHandler(ctx -> {
            metricsRegistry.add("TIMELINE", 1);
            TimelineDTO dto =
                instantHandler.getTimeline(getBasePathParam(ctx));
            writeValueAsString(ctx, dto);
        }, false));  // false = 不执行 refreshCheck
}
```

**为什么 Timeline API 不执行 refreshCheck？**

Timeline API 返回的是元数据信息（哪些 instant 存在），不涉及具体的文件列表。客户端拿到 Timeline 后自行判断是否需要刷新。如果连 Timeline 查询都要先刷新，就会形成循环依赖。

---

### 2.4 BaseFileHandler 解析

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/BaseFileHandler.java`

BaseFileHandler 处理基础文件（Parquet/ORC 数据文件）相关查询：

```java
public class BaseFileHandler extends Handler {

    // 获取分区下最新的 BaseFile 列表
    public List<BaseFileDTO> getLatestDataFiles(String basePath, String partitionPath) {
        return viewManager.getFileSystemView(basePath)
            .getLatestBaseFiles(partitionPath)
            .map(BaseFileDTO::fromHoodieBaseFile)
            .collect(Collectors.toList());
    }

    // 获取特定文件 ID 的最新 BaseFile
    public List<BaseFileDTO> getLatestDataFile(
        String basePath, String partitionPath, String fileId) {
        return viewManager.getFileSystemView(basePath)
            .getLatestBaseFile(partitionPath, fileId)
            .map(BaseFileDTO::fromHoodieBaseFile)
            .map(Collections::singletonList)
            .orElse(Collections.emptyList());
    }

    // 获取所有分区下的最新 BaseFile
    public List<BaseFileDTO> getLatestDataFiles(String basePath) {
        return viewManager.getFileSystemView(basePath)
            .getLatestBaseFiles()
            .map(BaseFileDTO::fromHoodieBaseFile)
            .collect(Collectors.toList());
    }

    // 获取指定 instant 之前的最新 BaseFile
    public List<BaseFileDTO> getLatestDataFilesBeforeOrOn(
        String basePath, String partitionPath, String maxInstantTime) {
        return viewManager.getFileSystemView(basePath)
            .getLatestBaseFilesBeforeOrOn(partitionPath, maxInstantTime)
            .map(BaseFileDTO::fromHoodieBaseFile)
            .collect(Collectors.toList());
    }

    // ... 更多方法
}
```

**API 端点一览**：

| HTTP 路径 | 方法 | 说明 |
|-----------|------|------|
| `/v1/hoodie/view/datafiles/latest/partition` | GET | 分区下最新 BaseFile |
| `/v1/hoodie/view/datafile/latest/partition` | GET | 分区+文件ID的最新 BaseFile |
| `/v1/hoodie/view/datafiles/all/latest/` | GET | 所有最新 BaseFile |
| `/v1/hoodie/view/datafiles/beforeoron/latest/` | GET | 指定 instant 前的最新 BaseFile |
| `/v1/hoodie/view/basefiles/all/beforeoron/` | GET | 所有分区指定 instant 前的 BaseFile |
| `/v1/hoodie/view/datafile/on/latest/` | GET | 特定 instant 的 BaseFile |
| `/v1/hoodie/view/datafiles/all` | GET | 分区下所有 BaseFile |
| `/v1/hoodie/view/datafiles/range/latest/` | GET | instant 范围内的最新 BaseFile |

**为什么需要这么多查询 API？**

不同的查询场景需要不同的文件列表：
- **读取查询**需要 `getLatestBaseFilesBeforeOrOn`（读取某一时刻的快照）
- **写入操作**需要 `getLatestBaseFiles`（确定要更新哪些文件）
- **Compaction**需要 `getAllBaseFiles`（获取某分区的全量文件信息）

每个 API 都有精确的语义，避免客户端做多余的过滤操作。

---

### 2.5 FileSliceHandler 解析

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/FileSliceHandler.java`

FileSliceHandler 是最庞大的 Handler，处理 FileSlice（BaseFile + LogFiles 组合）相关查询。对 MOR（Merge-On-Read）表来说，FileSlice 是核心查询单元。

```java
public class FileSliceHandler extends Handler {

    // 获取分区下最新的 FileSlice
    public List<FileSliceDTO> getLatestFileSlices(
        String basePath, String partitionPath) {
        return viewManager.getFileSystemView(basePath)
            .getLatestFileSlices(partitionPath)
            .map(FileSliceDTO::fromFileSlice)
            .collect(Collectors.toList());
    }

    // 获取包含 inflight 的最新 FileSlice
    public List<FileSliceDTO> getLatestFileSlicesIncludingInflight(
        String basePath, String partitionPath) {
        return viewManager.getFileSystemView(basePath)
            .getLatestFileSlicesIncludingInflight(partitionPath)
            .map(FileSliceDTO::fromFileSlice)
            .collect(Collectors.toList());
    }

    // 无状态查询（不更新内部状态）
    public List<FileSliceDTO> getLatestFileSlicesStateless(
        String basePath, String partitionPath) {
        return viewManager.getFileSystemView(basePath)
            .getLatestFileSlicesStateless(partitionPath)
            .map(FileSliceDTO::fromFileSlice)
            .collect(Collectors.toList());
    }

    // 获取 merged FileSlice（合并后的视图，含 inflight）
    public List<FileSliceDTO> getLatestMergedFileSlicesBeforeOrOnIncludingInflight(
        String basePath, String partitionPath,
        String maxInstantTime, String currentInstantTime) {
        return viewManager.getFileSystemView(basePath)
            .getLatestMergedFileSlicesBeforeOrOnIncludingInflight(
                partitionPath, maxInstantTime, currentInstantTime)
            .map(FileSliceDTO::fromFileSlice)
            .collect(Collectors.toList());
    }

    // 获取 pending compaction 操作
    public List<CompactionOpDTO> getPendingCompactionOperations(String basePath) {
        return viewManager.getFileSystemView(basePath)
            .getPendingCompactionOperations()
            .map(instantOp -> CompactionOpDTO.fromCompactionOperation(
                instantOp.getKey(), instantOp.getValue()))
            .collect(Collectors.toList());
    }

    // 获取 pending clustering 的文件组
    public List<ClusteringOpDTO> getFileGroupsInPendingClustering(String basePath) {
        return viewManager.getFileSystemView(basePath)
            .getFileGroupsInPendingClustering()
            .map(fgInstant -> ClusteringOpDTO.fromClusteringOp(
                fgInstant.getLeft(), fgInstant.getRight()))
            .collect(Collectors.toList());
    }

    // 刷新表视图（清除缓存并重新构建）
    public boolean refreshTable(String basePath) {
        viewManager.clearFileSystemView(basePath);
        return true;
    }

    // 加载所有分区
    public boolean loadAllPartitions(String basePath) {
        viewManager.getFileSystemView(basePath).loadAllPartitions();
        return true;
    }

    // 加载指定分区列表
    public boolean loadPartitions(String basePath, List<String> partitionPaths) {
        viewManager.getFileSystemView(basePath).loadPartitions(partitionPaths);
        return true;
    }
}
```

**完整 API 端点一览**：

| HTTP 路径 | 方法 | 说明 |
|-----------|------|------|
| `/v1/hoodie/view/slices/partition/latest/` | GET | 分区最新 FileSlice |
| `/v1/hoodie/view/slices/partition/latest/inflight/` | GET | 含 inflight 的最新 FileSlice |
| `/v1/hoodie/view/slices/partition/latest/stateless/` | GET | 无状态最新 FileSlice |
| `/v1/hoodie/view/slices/file/latest/` | GET | 单个文件的最新 FileSlice |
| `/v1/hoodie/view/slices/uncompacted/partition/latest/` | GET | 未压缩的最新 FileSlice |
| `/v1/hoodie/view/slices/all` | GET | 所有 FileSlice |
| `/v1/hoodie/view/slices/range/latest/` | GET | instant 范围最新 FileSlice |
| `/v1/hoodie/view/slices/merged/beforeoron/latest/` | GET | 合并后的最新 FileSlice |
| `/v1/hoodie/view/slices/merged/beforeoron/inflight/latest/` | GET | 含 inflight 合并 FileSlice |
| `/v1/hoodie/view/slice/merged/beforeoron/latest/` | GET | 单个合并 FileSlice |
| `/v1/hoodie/view/slices/beforeoron/latest/` | GET | 指定时间前的 FileSlice |
| `/v1/hoodie/view/slices/all/beforeoron/latest/` | GET | 所有分区指定时间前 FileSlice |
| `/v1/hoodie/view/compactions/pending/` | GET | Pending Compaction 操作 |
| `/v1/hoodie/view/logcompactions/pending/` | GET | Pending Log Compaction 操作 |
| `/v1/hoodie/view/filegroups/all/partition/` | GET | 分区所有 FileGroup |
| `/v1/hoodie/view/filegroups/all/partition/stateless/` | GET | 无状态分区所有 FileGroup |
| `/v1/hoodie/view/filegroups/replaced/beforeoron/` | GET | 已替换 FileGroup |
| `/v1/hoodie/view/filegroups/replaced/before/` | GET | 指定时间前已替换 FileGroup |
| `/v1/hoodie/view/filegroups/replaced/afteroron/` | GET | 指定时间后已替换 FileGroup |
| `/v1/hoodie/view/filegroups/replaced/partition/` | GET | 分区所有已替换 FileGroup |
| `/v1/hoodie/view/clustering/pending/` | GET | Pending Clustering FileGroup |
| `/v1/hoodie/view/refresh/` | POST | 刷新表视图 |
| `/v1/hoodie/view/loadallpartitions/` | POST | 加载所有分区 |
| `/v1/hoodie/view/loadpartitions/` | POST | 加载指定分区 |

**为什么有 stateless 版本的 API？**

`getLatestFileSlicesStateless` 和 `getAllFileGroupsStateless` 不会触发分区的加载或缓存更新。这对于探索性查询或轻量级检查很有用，避免了副作用。

---

### 2.6 MarkerHandler 解析

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/MarkerHandler.java`

MarkerHandler 是最复杂的 Handler，负责管理 Marker 文件的创建、查询和删除。它采用异步批量处理架构来优化 Marker 创建性能。

**整体架构**：

```
客户端请求                  MarkerHandler                    文件系统
    |                          |                               |
    |-- createMarker() -----> |                               |
    |                          |-- 加入 markerCreationFutures  |
    |                          |                               |
    |  (等待批量处理)           |                               |
    |                          |                               |
    |               dispatchingExecutorService                 |
    |                 (定时调度，50ms 间隔)                      |
    |                          |                               |
    |             MarkerCreationDispatchingRunnable             |
    |                          |                               |
    |                 batchingExecutorService                   |
    |                   (20 个工作线程)                          |
    |                          |                               |
    |              BatchedMarkerCreationRunnable                |
    |                          |-- flush markers -----------> |
    |                          |                     MARKERS0/1/2...
    |<-- 返回 Future 结果 ---  |                               |
```

**Marker API 端点**：

| HTTP 路径 | 方法 | 说明 |
|-----------|------|------|
| `/v1/hoodie/marker/all` | GET | 获取所有 Marker |
| `/v1/hoodie/marker/create-and-merge` | GET | 获取 CREATE+MERGE 类型 Marker |
| `/v1/hoodie/marker/append` | GET | 获取 APPEND 类型 Marker |
| `/v1/hoodie/marker/dir/exists` | GET | 检查 Marker 目录是否存在 |
| `/v1/hoodie/marker/create` | POST | 创建 Marker（异步） |
| `/v1/hoodie/marker/dir/delete` | POST | 删除 Marker 目录 |

**Marker 创建的核心方法**：

```java
public CompletableFuture<String> createMarker(
    Context context, String markerDir, String markerName, String basePath) {
    
    // Step1: 早期冲突检测（如果启用）
    if (timelineServiceConfig.earlyConflictDetectionEnable) {
        try {
            synchronized (earlyConflictDetectionLock) {
                if (earlyConflictDetectionStrategy == null) {
                    // 第一次：初始化冲突检测策略
                    earlyConflictDetectionStrategy = (TimelineServerBasedDetectionStrategy)
                        ReflectionUtils.loadClass(strategyClassName, ...);
                }
                
                // 当 markerDir 变化时（新的 instant），重新启动冲突检测
                if (!markerDir.equalsIgnoreCase(currentMarkerDir)) {
                    this.currentMarkerDir = markerDir;
                    // 获取已完成的 commits
                    Set<HoodieInstant> completedCommits = new HashSet<>(
                        viewManager.getFileSystemView(basePath)
                            .getTimeline()
                            .filterCompletedInstants()
                            .filter(instant -> actions.contains(instant.getAction()))
                            .getInstants());
                    
                    // 启动异步冲突检测
                    earlyConflictDetectionStrategy.startAsyncDetection(...);
                }
            }
            
            // 检查是否已检测到冲突
            earlyConflictDetectionStrategy.detectAndResolveConflictIfNecessary();
            
        } catch (HoodieEarlyConflictDetectionException he) {
            // 冲突！返回失败
            return finishCreateMarkerFuture(context, markerDir, markerName);
        }
    }
    
    // Step2: 将创建请求加入异步处理队列
    return addMarkerCreationRequestForAsyncProcessing(
        context, markerDir, markerName);
}
```

**为什么 Marker 创建是异步的？**

同步创建 Marker 意味着每个创建请求都要立即写入文件系统。在一个有数万 task 的大作业中，这会导致：
1. 大量小文件写入（每个 Marker 一个文件）
2. 文件系统瓶颈（HDFS NameNode 压力大）
3. 请求延迟高

异步批量处理将多个创建请求合并到一个文件中，大大减少了 I/O 次数。

---

### 2.7 RemotePartitionerHandler 解析

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/RemotePartitionerHandler.java`

RemotePartitionerHandler 提供远程分区索引分配服务，用于 Bucket Index 场景：

```java
public class RemotePartitionerHandler extends Handler {

    // 缓存 Map<PartitionPath, BucketStartIndex>
    private final ConcurrentHashMap<String, Integer> cache;
    private final AtomicInteger nextIndex = new AtomicInteger(0);

    public int gePartitionIndex(String numBuckets,
        String partitionPath, String partitionNum) {
        int num = Integer.parseInt(numBuckets);
        int partNum = Integer.parseInt(partitionNum);

        return cache.computeIfAbsent(partitionPath, key -> {
            int current;
            int newNext;
            int res;
            do {
                current = nextIndex.get();
                res = current;
                newNext = current + num;
                if (newNext >= partNum) {
                    newNext = newNext % partNum;
                }
            } while (!nextIndex.compareAndSet(current, newNext));
            return res;
        });
    }
}
```

**为什么需要远程分区索引？**

在 Bucket Index 场景下，需要为每个分区分配一段连续的 bucket 范围。如果每个 Executor 各自分配，可能会产生冲突。通过 TLS 集中分配，确保全局唯一。

使用 CAS（Compare-And-Set）操作的 `AtomicInteger` 确保了在并发场景下的线程安全性，同时避免了锁的开销。

---

### 2.8 ViewHandler 内部类与刷新检查

**源码路径**：`RequestHandler.java` 中的 `ViewHandler` 内部类

ViewHandler 是所有 API 请求的包装器，负责：
1. 请求前的 Timeline 一致性检查
2. 必要时触发 FileSystemView 同步
3. 请求后的二次验证
4. 指标收集和日志记录

```java
private class ViewHandler implements Handler {

    private final Handler handler;
    private final boolean performRefreshCheck;
    private final UserGroupInformation ugi;

    @Override
    public void handle(@Nonnull Context context) throws Exception {
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            boolean success = true;
            long beginTs = System.currentTimeMillis();
            boolean synced = false;
            boolean refreshCheck = performRefreshCheck
                && !isRefreshCheckDisabledInQuery(context);
            
            try {
                // 1. 请求前检查：本地视图是否落后于客户端？
                if (refreshCheck) {
                    synced = syncIfLocalViewBehind(context);
                }
                
                // 2. 执行实际的 Handler 逻辑
                handler.handle(context);
                
                // 3. 请求后检查：如果还是落后，抛出异常
                if (refreshCheck) {
                    if (isLocalViewBehind(context)) {
                        // ...可能抛出 BadRequestResponse
                    }
                }
            } catch (RuntimeException re) {
                // 处理异常
            } finally {
                // 收集指标
                metricsRegistry.add("TOTAL_API_TIME", timeTakenMillis);
            }
            return null;
        });
    }
}
```

**Timeline 一致性检查机制**：

客户端每次请求时，会带上自己知道的最新 instant timestamp 和 timeline hash：

```java
// 客户端在发送请求时附加参数
queryParameters.put(LAST_INSTANT_TS, instant.requestedTime());
queryParameters.put(TIMELINE_HASH, timeline.getTimelineHash());
```

服务端通过 `isLocalViewBehind()` 判断本地视图是否落后于客户端：

```java
private boolean isLocalViewBehind(Context ctx) {
    String lastKnownInstantFromClient = getLastInstantTsParam(ctx);
    String timelineHashFromClient = getTimelineHashParam(ctx);
    HoodieTimeline localTimeline = viewManager.getFileSystemView(basePath)
        .getTimeline()
        .filterCompletedOrMajorOrMinorCompactionInstants();

    // 本地空 + 客户端也没有 instant => 不落后
    if ((!localTimeline.getInstantsAsStream().findAny().isPresent())
        && HoodieTimeline.INVALID_INSTANT_TS.equals(lastKnownInstantFromClient)) {
        return false;
    }

    // Timeline Hash 不匹配 => 落后
    String localTimelineHash = localTimeline.getTimelineHash();
    if (!localTimelineHash.equals(timelineHashFromClient)) {
        return true;
    }

    // Hash 相同但 instant 不存在 => 落后
    return !localTimeline.containsOrBeforeTimelineStarts(
        lastKnownInstantFromClient);
}
```

**为什么需要 Timeline Hash？**

仅比较最后一个 instant 的时间戳不够安全。考虑这种场景：
- 客户端看到 instants: [001, 002, 003]
- 服务端可能因为 rollback 删除了 002，只有 [001, 003]

此时最后一个 instant 都是 003，但 Timeline 内容不同。Hash 可以检测出这种差异。

**同步操作**：

```java
private boolean syncIfLocalViewBehind(Context ctx) {
    SyncableFileSystemView view = viewManager.getFileSystemView(basePath);
    synchronized (view) {  // 对 view 加锁，避免并发同步
        if (isLocalViewBehind(ctx)) {
            view.sync();  // 执行同步
            return true;
        }
    }
    return false;
}
```

**为什么要对 view 加锁？**

多个请求可能同时检测到本地落后，但只需要同步一次。锁确保了同步操作的串行化，避免重复工作。

---

## 第三部分：服务端 FileSystemView 管理

### 3.1 FileSystemViewManager 全局视图管理器

**源码路径**：`hudi-common/src/main/java/org/apache/hudi/common/table/view/FileSystemViewManager.java`

FileSystemViewManager 是 FileSystemView 的工厂和管理器。每个 TLS 实例持有一个 FileSystemViewManager，它按 basePath（表路径）维护每张表的 FileSystemView。

```java
public class FileSystemViewManager {

    private final StorageConfiguration<?> conf;
    private final FileSystemViewStorageConfig viewStorageConfig;
    // 视图创建工厂函数
    private final Function2<HoodieTableMetaClient, FileSystemViewStorageConfig,
        SyncableFileSystemView> viewCreator;
    // 全局视图映射：basePath -> SyncableFileSystemView
    private final ConcurrentHashMap<String, SyncableFileSystemView> globalViewMap;

    // 获取某张表的 FileSystemView（懒创建）
    public SyncableFileSystemView getFileSystemView(String basePath) {
        return globalViewMap.computeIfAbsent(basePath, (path) -> {
            HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setConf(conf.newInstance())
                .setBasePath(path)
                .build();
            return viewCreator.apply(metaClient, viewStorageConfig);
        });
    }

    // 清除某张表的视图（下次请求时会重新创建）
    public void clearFileSystemView(String basePath) {
        SyncableFileSystemView view = globalViewMap.remove(basePath);
        if (view != null) {
            view.close();
        }
    }

    // 关闭所有视图
    public void close() {
        if (!this.globalViewMap.isEmpty()) {
            this.globalViewMap.values()
                .forEach(SyncableFileSystemView::close);
            this.globalViewMap.clear();
        }
    }
}
```

**为什么使用 ConcurrentHashMap + computeIfAbsent？**

多个 Jetty 线程可能同时请求同一张表的视图。`ConcurrentHashMap.computeIfAbsent` 是原子操作，确保每张表只创建一个视图实例，同时避免了显式加锁的开销。

#### 工厂方法体系

FileSystemViewManager 使用工厂模式根据不同的存储类型创建不同的 View 实现：

```java
public static FileSystemViewManager createViewManager(
    final HoodieEngineContext context,
    final HoodieMetadataConfig metadataConfig,
    final FileSystemViewStorageConfig config,
    final HoodieCommonConfig commonConfig,
    final SerializableFunctionUnchecked<HoodieTableMetaClient,
        HoodieTableMetadata> metadataCreator) {
    
    switch (config.getStorageType()) {
        case EMBEDDED_KV_STORE:
            return new FileSystemViewManager(context, config,
                (metaClient, viewConf) ->
                    createRocksDBBasedFileSystemView(...));
        case SPILLABLE_DISK:
            return new FileSystemViewManager(context, config,
                (metaClient, viewConf) ->
                    createSpillableMapBasedFileSystemView(...));
        case MEMORY:
            return new FileSystemViewManager(context, config,
                (metaClient, viewConfig) ->
                    createInMemoryFileSystemView(...));
        case REMOTE_ONLY:
            return new FileSystemViewManager(context, config,
                (metaClient, viewConfig) ->
                    createRemoteFileSystemView(viewConfig, metaClient));
        case REMOTE_FIRST:
            return new FileSystemViewManager(context, config,
                (metaClient, viewConfig) -> {
                    RemoteHoodieTableFileSystemView remoteView =
                        createRemoteFileSystemView(viewConfig, metaClient);
                    SecondaryViewCreator secondaryViewSupplier =
                        new SecondaryViewCreator(viewConfig, metaClient, ...);
                    return new PriorityBasedFileSystemView(
                        remoteView, secondaryViewSupplier, context);
                });
    }
}
```

**设计好处**：

通过将视图创建逻辑封装在 lambda 表达式中，实现了懒初始化。视图只在第一次被请求时才创建，避免了预创建所有表的视图带来的资源浪费。

---

### 3.2 FileSystemViewStorageType 存储类型

**源码路径**：`hudi-common/src/main/java/org/apache/hudi/common/table/view/FileSystemViewStorageType.java`

```java
public enum FileSystemViewStorageType {
    MEMORY,            // 纯内存（HashMap）
    SPILLABLE_DISK,    // 内存 + 磁盘溢出（ExternalSpillableMap）
    EMBEDDED_KV_STORE, // RocksDB
    REMOTE_ONLY,       // 仅远程（通过 HTTP 访问 TLS）
    REMOTE_FIRST       // 远程优先 + 本地降级
}
```

**使用场景分析**：

```
TLS 服务端使用：MEMORY / SPILLABLE_DISK / EMBEDDED_KV_STORE
    |
    |-- 构建并缓存 FileSystemView
    |-- 响应来自 Executor 的 HTTP 请求
    v

Executor/客户端使用：REMOTE_ONLY / REMOTE_FIRST
    |
    |-- REMOTE_ONLY: 只通过 HTTP 访问 TLS
    |-- REMOTE_FIRST: 优先 HTTP，失败降级到本地
    v
    
RemoteHoodieTableFileSystemView (REMOTE_ONLY)
    或
PriorityBasedFileSystemView (REMOTE_FIRST)
    |-- preferredView = RemoteHoodieTableFileSystemView
    |-- secondaryView = InMemory/Spillable/RocksDB (懒创建)
```

---

### 3.3 IncrementalTimelineSyncFileSystemView 增量同步机制

**源码路径**：`hudi-common/src/main/java/org/apache/hudi/common/table/view/IncrementalTimelineSyncFileSystemView.java`

IncrementalTimelineSyncFileSystemView 是 TLS 中实现高性能的关键组件。当 Timeline 发生变化时（新的 commit、clean、rollback 等），它不需要重建整个 FileSystemView，而是增量地应用变更。

#### 增量同步核心逻辑

```java
public abstract class IncrementalTimelineSyncFileSystemView
    extends AbstractTableFileSystemView {

    private final boolean incrementalTimelineSyncEnabled;
    private HoodieTimeline visibleActiveTimeline;

    @Override
    public void sync() {
        try {
            writeLock.lock();
            maySyncIncrementally();
            tableMetadata.reset();
        } finally {
            writeLock.unlock();
        }
    }

    protected void maySyncIncrementally() {
        HoodieTimeline oldTimeline = getTimeline();
        HoodieTimeline newTimeline = metaClient.reloadActiveTimeline()
            .filterCompletedOrMajorOrMinorCompactionInstants();
        
        try {
            if (incrementalTimelineSyncEnabled) {
                // 计算新旧 Timeline 的差异
                TimelineDiffResult diffResult =
                    TimelineDiffHelper.getNewInstantsForIncrementalSync(
                        metaClient, oldTimeline, newTimeline);
                
                if (diffResult.canSyncIncrementally()) {
                    LOG.info("Doing incremental sync");
                    refreshCompletionTimeQueryView();
                    runIncrementalSync(newTimeline, diffResult);
                    LOG.info("Finished incremental sync");
                    refreshTimeline(newTimeline);
                    return;
                }
            }
        } catch (Exception ioe) {
            LOG.error("Got exception trying to perform incremental sync. "
                + "Reverting to complete sync", ioe);
        }
        // 增量同步失败，回退到全量重建
        clear();
        init(metaClient, newTimeline);
    }
}
```

**为什么增量同步如此重要？**

对于一张有 10000 个分区的表，全量重建 FileSystemView 可能需要数分钟（需要 LIST 每个分区目录）。而增量同步只需要处理新增/修改的 instant，通常在毫秒到秒级别完成。

#### 增量同步的差异处理

```java
private void runIncrementalSync(HoodieTimeline timeline,
    TimelineDiffResult diffResult) {
    
    // 1. 处理已完成的 Compaction（从 pending 移除）
    diffResult.getFinishedCompactionInstants().forEach(instant -> {
        removePendingCompactionInstant(instant);
    });

    // 2. 处理已完成或移除的 Log Compaction
    diffResult.getFinishedOrRemovedLogCompactionInstants().forEach(instant -> {
        removePendingLogCompactionInstant(instant);
    });

    // 3. 处理新发现的 instants
    diffResult.getNewlySeenInstants().stream()
        .filter(instant -> instant.isCompleted()
            || instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION)
            || instant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION))
        .forEach(instant -> {
            if (instant.getAction().equals(HoodieTimeline.COMMIT_ACTION)
                || instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION)) {
                addCommitInstant(timeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.RESTORE_ACTION)) {
                addRestoreInstant(timeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.CLEAN_ACTION)) {
                addCleanInstant(timeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION)) {
                addPendingCompactionInstant(timeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION)) {
                addPendingLogCompactionInstant(instant);
            } else if (instant.getAction().equals(HoodieTimeline.ROLLBACK_ACTION)) {
                addRollbackInstant(timeline, instant);
            } else if (instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
                addReplaceInstant(timeline, instant);
            }
        });
}
```

**各类型 Instant 的增量处理逻辑**：

| Instant 类型 | 处理方式 |
|-------------|---------|
| COMMIT / DELTA_COMMIT | 从 metadata 中读取写入的文件信息，添加到对应分区的 View |
| CLEAN | 从 metadata 中读取清理的文件信息，从对应分区的 View 中移除 |
| COMPACTION (pending) | 添加 pending compaction 记录，创建新的空 FileSlice |
| COMPACTION (completed) | 移除 pending compaction 记录 |
| ROLLBACK | 从 metadata 中读取回滚的文件，从 View 中移除 |
| RESTORE | 类似 Rollback，但可能涉及多个 instant |
| REPLACE_COMMIT | 添加新写入文件，同时标记被替换的 FileGroup |

#### Commit 的增量处理示例

```java
private void addCommitInstant(HoodieTimeline timeline,
    HoodieInstant instant) throws IOException {
    HoodieCommitMetadata commitMetadata =
        timeline.readCommitMetadata(instant);
    updatePartitionWriteFileGroups(
        commitMetadata.getPartitionToWriteStats(), timeline, instant);
}

private void updatePartitionWriteFileGroups(
    Map<String, List<HoodieWriteStat>> partitionToWriteStats,
    HoodieTimeline timeline, HoodieInstant instant) {
    
    partitionToWriteStats.entrySet().forEach(entry -> {
        String partition = entry.getKey();
        if (isPartitionAvailableInStore(partition)) {
            // 从 WriteStat 构建 StoragePathInfo
            List<StoragePathInfo> pathInfoList = entry.getValue().stream()
                .map(p -> new StoragePathInfo(
                    new StoragePath(String.format("%s/%s",
                        metaClient.getBasePath(), p.getPath())),
                    p.getFileSizeInBytes(), false, (short) 0, 0, 0))
                .collect(Collectors.toList());
            
            // 构建 FileGroup 并添加到分区视图
            List<HoodieFileGroup> fileGroups = buildFileGroups(
                partition, pathInfoList,
                timeline.filterCompletedAndCompactionInstants(), false);
            applyDeltaFileSlicesToPartitionView(
                partition, fileGroups, DeltaApplyMode.ADD);
        }
    });
}
```

**关键设计点：不需要 LIST 文件系统！**

增量同步的精妙之处在于：新 commit 的文件信息已经记录在 commit metadata 中（`HoodieWriteStat`），不需要再次 LIST 文件系统。这使得增量同步几乎不产生 I/O 操作。

#### Delta 应用机制

```java
protected void applyDeltaFileSlicesToPartitionView(
    String partition,
    List<HoodieFileGroup> deltaFileGroups,
    DeltaApplyMode mode) {
    
    // 获取当前分区的所有 FileGroup
    List<HoodieFileGroup> fileGroups =
        fetchAllStoredFileGroups(partition).collect(Collectors.toList());
    
    // 提取现有的 DataFile 和 LogFile
    Map<String, HoodieBaseFile> viewDataFiles = ...;
    Map<String, HoodieLogFile> viewLogFiles = ...;
    
    // 提取 Delta 中的 DataFile 和 LogFile
    Map<String, HoodieBaseFile> deltaDataFiles = ...;
    Map<String, HoodieLogFile> deltaLogFiles = ...;

    switch (mode) {
        case ADD:
            viewDataFiles.putAll(deltaDataFiles);
            viewLogFiles.putAll(deltaLogFiles);
            break;
        case REMOVE:
            deltaDataFiles.keySet().forEach(viewDataFiles::remove);
            deltaLogFiles.keySet().forEach(viewLogFiles::remove);
            break;
    }

    // 重新构建分区的 FileGroup 并存储
    List<HoodieFileGroup> fgs = buildFileGroups(
        partition, viewDataFiles.values().stream(),
        viewLogFiles.values().stream(), timeline, true);
    storePartitionView(partition, fgs);
}
```

---

### 3.4 PriorityBasedFileSystemView 优先级降级视图

**源码路径**：`hudi-common/src/main/java/org/apache/hudi/common/table/view/PriorityBasedFileSystemView.java`

PriorityBasedFileSystemView 实现了"远程优先、本地降级"的策略。这是 `REMOTE_FIRST` 模式在客户端的具体实现。

```java
public class PriorityBasedFileSystemView
    implements SyncableFileSystemView, Serializable {

    private final SyncableFileSystemView preferredView;  // 主视图（远程）
    private final SerializableFunctionUnchecked<HoodieEngineContext,
        SyncableFileSystemView> secondaryViewCreator;   // 备用视图创建器
    private SyncableFileSystemView secondaryView;        // 备用视图（本地，懒创建）
    private boolean errorOnPreferredView;                // 主视图是否已出错

    // 通用执行模板（无参数版本）
    private <R> R execute(Function0<R> preferredFunction,
                          Function0<R> secondaryFunction) {
        if (errorOnPreferredView) {
            LOG.warn("Routing request to secondary file-system view");
            return secondaryFunction.apply();
        } else {
            try {
                return preferredFunction.apply();
            } catch (RuntimeException re) {
                handleRuntimeException(re);
                errorOnPreferredView = true;  // 标记主视图出错
                return secondaryFunction.apply();
            }
        }
    }

    // 示例：getLatestBaseFiles 的降级实现
    @Override
    public Stream<HoodieBaseFile> getLatestBaseFiles(String partitionPath) {
        return execute(
            partitionPath,
            preferredView::getLatestBaseFiles,
            (path) -> getSecondaryView().getLatestBaseFiles(path)
        );
    }

    // 懒创建备用视图
    synchronized SyncableFileSystemView getSecondaryView() {
        if (secondaryView == null) {
            secondaryView = secondaryViewCreator.apply(engineContext);
        }
        return secondaryView;
    }
}
```

**降级行为分析**：

```
正常情况：
  请求 ──> preferredView (RemoteHoodieTableFileSystemView)
       ──> HTTP 请求到 TLS
       ──> 返回结果

异常降级（首次出错）：
  请求 ──> preferredView ──> 抛出 RuntimeException
       ──> 标记 errorOnPreferredView = true
       ──> 懒创建 secondaryView (本地 InMemory/Spillable)
       ──> secondaryView 返回结果

后续请求（已降级）：
  请求 ──> 直接使用 secondaryView
       ──> 本地计算结果
```

**特殊的 HTTP 400 处理**：

```java
private void handleRuntimeException(RuntimeException re) {
    if (re.getCause() instanceof HttpResponseException
        && ((HttpResponseException) re.getCause()).getStatusCode()
            == HttpStatus.SC_BAD_REQUEST) {
        LOG.warn("Got error running preferred function. "
            + "Likely due to another concurrent writer in progress. "
            + "Trying secondary");
    } else {
        LOG.error("Got error running preferred function. Trying secondary", re);
    }
}
```

HTTP 400 通常意味着客户端的 Timeline 和服务端不一致（另一个 Writer 已经提交了新数据），这种情况下降级到本地构建是安全的。

**为什么降级后不自动恢复？**

注意 `errorOnPreferredView` 一旦设置为 `true`，后续所有请求都会走备用视图，直到调用 `sync()` 或 `reset()`。这是一种保守策略：既然 TLS 已经出错过一次，继续使用可能导致更多问题。通过 `sync()` 显式恢复，确保 TLS 真正可用后才切回。

---

### 3.5 Timeline Hash 一致性校验机制

Timeline Hash 是 TLS 实现数据一致性的核心机制。

**工作原理**：

1. 每个 Timeline 维护一个 Hash 值，基于所有 instant 的信息计算
2. 客户端每次请求时携带自己的 Timeline Hash
3. 服务端比较 Hash，如果不一致，触发同步

```java
// ViewHandler 中的一致性检查
private boolean isLocalViewBehind(Context ctx) {
    String timelineHashFromClient = getTimelineHashParam(ctx);
    HoodieTimeline localTimeline = viewManager.getFileSystemView(basePath)
        .getTimeline()
        .filterCompletedOrMajorOrMinorCompactionInstants();

    String localTimelineHash = localTimeline.getTimelineHash();
    // Hash 不匹配意味着 Timeline 内容不同
    if (!localTimelineHash.equals(timelineHashFromClient)) {
        return true;
    }

    // Hash 相同，再验证 instant 是否存在
    return !localTimeline.containsOrBeforeTimelineStarts(
        lastKnownInstantFromClient);
}
```

**为什么需要双重验证（Hash + Instant）？**

Hash 碰撞虽然概率极低，但理论上存在。双重验证增加了安全性：
- Hash 不同 => 一定不一致
- Hash 相同但 instant 不存在 => 也不一致（可能 Hash 碰撞或 Timeline 被截断）

---

## 第四部分：Marker 管理服务

### 4.1 Marker 机制概述

Marker（标记）文件是 Hudi 写入过程中的辅助文件，用于追踪正在写入的数据文件。每个数据文件在写入前会创建一个对应的 marker，写入完成后（commit 成功）删除所有 marker。

**Marker 的作用**：

1. **Rollback**：如果写入失败，通过 marker 文件可以找到所有需要清理的部分写入文件
2. **冲突检测**：通过比较不同 Writer 的 marker，可以检测到 FileGroup 级别的写入冲突
3. **一致性保证**：确保失败的写入不会留下垃圾数据

**两种 Marker 实现**：

| 类型 | 实现方式 | 优缺点 |
|------|---------|--------|
| DIRECT | 每个 marker 一个文件 | 简单但小文件多，HDFS 压力大 |
| TIMELINE_SERVER_BASED | TLS 集中管理，批量写入 | 高效但依赖 TLS |

**为什么 TIMELINE_SERVER_BASED 是更好的选择？**

在一个写入 10000 个文件的作业中：
- DIRECT 模式：创建 10000 个 marker 文件 + 10000 次 NameNode 操作
- TLS 模式：只创建几十个 MARKERS 文件（批量合并），NameNode 操作减少 99%+

---

### 4.2 MarkerDirState 核心状态管理

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/marker/MarkerDirState.java`

MarkerDirState 维护一个 marker 目录的完整状态，包括内存缓存和磁盘文件的映射。

```java
@Slf4j
public class MarkerDirState implements Serializable {
    // Marker 目录路径
    private final StoragePath markerDirPath;
    private final HoodieStorage storage;
    
    // 所有 marker 的内存缓存
    @Getter
    private final Set<String> allMarkers = new HashSet<>();
    
    // 每个 marker 文件中的内容（用 StringBuilder 高效追加）
    // {文件索引 -> marker 内容}
    private final Map<Integer, StringBuilder> fileMarkersMap = new HashMap<>();
    
    // 每个底层文件的使用状态（true=被工作线程占用）
    private final List<Boolean> threadUseStatus;
    
    // 待处理的 marker 创建请求队列
    private final List<MarkerCreationFuture> markerCreationFutures = new ArrayList<>();
    
    // 上次使用的文件索引（用于轮询分配）
    private int lastFileIndexUsed = -1;
    
    // marker 类型是否已写入
    private boolean isMarkerTypeWritten = false;
}
```

**构造函数——从文件系统同步**：

```java
public MarkerDirState(String markerDirPath, int markerBatchNumThreads,
    Option<TimelineServerBasedDetectionStrategy> conflictDetectionStrategy,
    HoodieStorage storage, Registry metricsRegistry, int parallelism) {
    
    this.markerDirPath = new StoragePath(markerDirPath);
    this.storage = storage;
    this.threadUseStatus = Stream.generate(() -> false)
        .limit(markerBatchNumThreads)
        .collect(Collectors.toList());
    this.conflictDetectionStrategy = conflictDetectionStrategy;
    
    // 从文件系统读取已有的 marker 文件，恢复内存状态
    syncMarkersFromFileSystem();
}
```

**为什么需要从文件系统恢复？**

TLS 可能在运行过程中重启。重启后需要从文件系统中的 MARKERS* 文件恢复已创建的 marker 状态，确保不会丢失已有的 marker 记录。

#### 文件索引的轮询分配

```java
public Option<Integer> getNextFileIndexToUse() {
    int fileIndex = -1;
    synchronized (markerCreationProcessingLock) {
        // 从上次使用的索引之后开始扫描
        for (int i = 0; i < threadUseStatus.size(); i++) {
            int index = (lastFileIndexUsed + 1 + i)
                % threadUseStatus.size();
            if (!threadUseStatus.get(index)) {
                fileIndex = index;
                threadUseStatus.set(index, true);  // 标记为使用中
                break;
            }
        }
        if (fileIndex >= 0) {
            lastFileIndexUsed = fileIndex;
            return Option.of(fileIndex);
        }
    }
    return Option.empty();  // 所有文件都在使用中
}
```

**为什么用轮询而不是随机分配？**

轮询分配确保各个 MARKERS 文件被均匀使用，避免某些文件过大而其他文件过小。均匀的文件大小有利于读取时的负载均衡。

#### Marker 处理核心方法

```java
public void processMarkerCreationRequests(
    final List<MarkerCreationFuture> pendingMarkerCreationFutures,
    int fileIndex) {
    
    if (pendingMarkerCreationFutures.isEmpty()) {
        markFileAsAvailable(fileIndex);
        return;
    }

    boolean shouldFlushMarkers = false;
    
    synchronized (markerCreationProcessingLock) {
        for (MarkerCreationFuture future : pendingMarkerCreationFutures) {
            String markerName = future.getMarkerName();
            boolean exists = allMarkers.contains(markerName);
            
            if (!exists) {
                // 冲突检测（如果启用）
                if (conflictDetectionStrategy.isPresent()) {
                    try {
                        conflictDetectionStrategy.get()
                            .detectAndResolveConflictIfNecessary();
                    } catch (HoodieEarlyConflictDetectionException he) {
                        future.setIsSuccessful(false);
                        continue;
                    }
                }
                // 添加到内存映射
                addMarkerToMap(fileIndex, markerName);
                shouldFlushMarkers = true;
            }
            future.setIsSuccessful(!exists);
        }

        // 首次创建时，写入 MARKERS.type 文件
        if (!isMarkerTypeWritten) {
            writeMarkerTypeToFile();
            isMarkerTypeWritten = true;
        }
    }
    
    // 批量刷盘（在锁外面做，减少锁持有时间）
    if (shouldFlushMarkers) {
        flushMarkersToFile(fileIndex);
    }
    markFileAsAvailable(fileIndex);

    // 完成所有 Future
    for (MarkerCreationFuture future : pendingMarkerCreationFutures) {
        future.complete(jsonifyResult(
            future.getContext(), future.isSuccessful(), metricsRegistry));
    }
}
```

**为什么将刷盘操作放在锁外面？**

`markerCreationProcessingLock` 保护的是内存状态的更新。`flushMarkersToFile` 是 I/O 操作，可能耗时较长。将它放在锁外面，可以减少锁持有时间，允许其他线程并行处理新的请求。

#### 内存中的 Marker 添加

```java
private void addMarkerToMap(int fileIndex, String markerName) {
    allMarkers.add(markerName);
    StringBuilder stringBuilder = fileMarkersMap
        .computeIfAbsent(fileIndex, k -> new StringBuilder(16384));
    stringBuilder.append(markerName);
    stringBuilder.append('\n');
}
```

**为什么初始容量为 16384？**

一个 marker 名称大约 50-100 字符。16384 字节可以容纳约 100-300 个 marker。这是一个合理的初始大小，避免频繁的 StringBuilder 扩容。

#### 刷盘实现

```java
private void flushMarkersToFile(int markerFileIndex) {
    StoragePath markersFilePath = new StoragePath(
        markerDirPath, MARKERS_FILENAME_PREFIX + markerFileIndex);
    OutputStream outputStream = null;
    BufferedWriter bufferedWriter = null;
    try {
        outputStream = storage.create(markersFilePath);
        bufferedWriter = new BufferedWriter(
            new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
        bufferedWriter.write(fileMarkersMap.get(markerFileIndex).toString());
    } catch (IOException e) {
        throw new HoodieIOException(
            "Failed to overwrite marker file " + markersFilePath, e);
    } finally {
        closeQuietly(bufferedWriter);
        closeQuietly(outputStream);
    }
}
```

**注意：每次刷盘是全量覆盖**

`storage.create()` 创建/覆盖文件，将 `fileMarkersMap` 中该索引的所有内容一次性写入。这意味着 MARKERS 文件的内容是累积增长的。这种设计简化了实现（不需要追加写），且 marker 文件的大小通常不大。

---

### 4.3 批量 Marker 创建的完整链路

让我们用一个具体的例子来展示 Marker 创建的完整链路：

```
时间轴：

T=0ms    Executor-1 发送创建 marker1 请求
T=10ms   Executor-2 发送创建 marker2 请求
T=20ms   Executor-3 发送创建 marker3 请求
T=30ms   Executor-4 发送创建 marker4 请求
T=40ms   Executor-5 发送创建 marker5 请求

T=50ms   dispatchingExecutorService 定时触发
         MarkerCreationDispatchingRunnable.run():
           1. 遍历 markerDirStateMap
           2. 获取空闲文件索引 (假设 fileIndex=0)
           3. 取出 5 个 pending futures
           4. 创建 BatchedMarkerCreationContext
           5. 提交 BatchedMarkerCreationRunnable 到 batchingExecutorService

T=51ms   BatchedMarkerCreationRunnable.run():
           1. 调用 markerDirState.processMarkerCreationRequests(futures, 0)
           2. 批量更新内存中的 allMarkers
           3. 批量追加到 fileMarkersMap[0]
           4. 一次性 flush 到 MARKERS0 文件
           5. 完成 5 个 Future

T=52ms   5 个 Executor 收到响应
```

**性能对比**：

| 操作 | 逐个创建 | 批量创建 |
|------|---------|---------|
| 文件写入次数 | 5 次 | 1 次 |
| HTTP 响应延迟 | 每个 10-50ms | 整批 50-60ms |
| NameNode 操作 | 5 次 CREATE | 1 次 CREATE |

---

### 4.4 MarkerCreationDispatchingRunnable 调度器

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/marker/MarkerCreationDispatchingRunnable.java`

调度器是批量处理的入口，定期（默认 50ms）检查是否有待处理的 marker 创建请求：

```java
public class MarkerCreationDispatchingRunnable implements Runnable {

    private final Map<String, MarkerDirState> markerDirStateMap;
    private final ExecutorService executorService;

    @Override
    public void run() {
        List<BatchedMarkerCreationContext> requestContextList = new ArrayList<>();

        for (Map.Entry<String, MarkerDirState> entry
            : markerDirStateMap.entrySet()) {
            String markerDir = entry.getKey();
            MarkerDirState markerDirState = entry.getValue();
            
            // 1. 获取空闲的文件索引
            Option<Integer> fileIndex = markerDirState.getNextFileIndexToUse();
            if (!fileIndex.isPresent()) {
                LOG.debug("All marker files are busy, skip...");
                continue;
            }
            
            // 2. 取出待处理的请求
            List<MarkerCreationFuture> futures =
                markerDirState.fetchPendingMarkerCreationRequests();
            if (futures.isEmpty()) {
                markerDirState.markFileAsAvailable(fileIndex.get());
                continue;
            }
            
            // 3. 构建批处理上下文
            requestContextList.add(new BatchedMarkerCreationContext(
                markerDir, markerDirState, futures, fileIndex.get()));
        }

        // 4. 提交批处理任务
        if (requestContextList.size() > 0) {
            executorService.execute(
                new BatchedMarkerCreationRunnable(requestContextList));
        }
    }
}
```

**调度器的三级保护机制**：

1. **文件索引检查**：所有文件都在使用中时跳过，避免争用
2. **请求队列检查**：没有待处理请求时释放文件索引，避免无谓占用
3. **多 markerDir 支持**：同时处理多个表的 marker 目录

---

### 4.5 BatchedMarkerCreationRunnable 批处理器

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/marker/BatchedMarkerCreationRunnable.java`

```java
@Slf4j
public class BatchedMarkerCreationRunnable implements Runnable {

    private final List<BatchedMarkerCreationContext> requestContextList;

    @Override
    public void run() {
        log.debug("Start processing create marker requests");
        HoodieTimer timer = HoodieTimer.start();

        for (BatchedMarkerCreationContext requestContext : requestContextList) {
            requestContext.getMarkerDirState().processMarkerCreationRequests(
                requestContext.getFutures(), requestContext.getFileIndex());
        }
        log.debug("Finish batch processing in {} ms", timer.endTimer());
    }
}
```

**BatchedMarkerCreationContext** 封装了批处理所需的所有信息：

```java
@AllArgsConstructor
@Getter
public class BatchedMarkerCreationContext {
    private final String markerDir;
    private final MarkerDirState markerDirState;
    private final List<MarkerCreationFuture> futures;
    private final int fileIndex;
}
```

**并发模型图示**：

```
                  |-----| batch interval (50ms)
Worker Thread 1  |------------------------------>| writing to MARKERS0
Worker Thread 2        |------------------------------>| writing to MARKERS1
Worker Thread 3               |------------------------------>| writing to MARKERS2
                                     |------------------------------>| writing to MARKERS3
                                            ...
```

每个 Worker Thread 操作不同的 MARKERS 文件，互不干扰。最多可以有 `markerBatchNumThreads`（默认 20）个线程并行写入。

---

### 4.6 TimelineServerBasedWriteMarkers 客户端

**源码路径**：`hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/marker/TimelineServerBasedWriteMarkers.java`

这是 Executor 端用来与 TLS 通信创建 marker 的客户端类：

```java
@Slf4j
public class TimelineServerBasedWriteMarkers extends WriteMarkers {

    private final TimelineServiceClient timelineServiceClient;

    public TimelineServerBasedWriteMarkers(HoodieTable table, String instantTime) {
        this(table.getMetaClient().getBasePath().toString(),
            table.getMetaClient().getMarkerFolderPath(instantTime),
            instantTime,
            table.getConfig().getViewStorageConfig());
    }

    // 创建 marker
    @Override
    protected Option<StoragePath> create(
        String partitionPath, String fileName, IOType type,
        boolean checkIfExists) {
        
        HoodieTimer timer = HoodieTimer.start();
        String markerFileName = getMarkerFileName(fileName, type);
        Map<String, String> paramsMap =
            getConfigMap(partitionPath, markerFileName, false);
        
        boolean success = executeCreateMarkerRequest(
            paramsMap, partitionPath, markerFileName);
        
        log.info("[timeline-server-based] Created marker file "
            + partitionPath + "/" + markerFileName
            + " in " + timer.endTimer() + " ms");
        
        if (success) {
            return Option.of(new StoragePath(
                FSUtils.constructAbsolutePath(markerDirPath, partitionPath),
                markerFileName));
        } else {
            return Option.empty();
        }
    }

    // 带早期冲突检测的创建
    @Override
    public Option<StoragePath> createWithEarlyConflictDetection(
        String partitionPath, String fileName, IOType type,
        boolean checkIfExists, HoodieWriteConfig config,
        String fileId, HoodieActiveTimeline activeTimeline) {
        
        String markerFileName = getMarkerFileName(fileName, type);
        Map<String, String> paramsMap =
            getConfigMap(partitionPath, markerFileName, true);
        
        boolean success = executeCreateMarkerRequest(
            paramsMap, partitionPath, markerFileName);
        
        if (success) {
            return Option.of(new StoragePath(...));
        } else {
            // 创建失败可能是冲突检测触发的
            throw new HoodieEarlyConflictDetectionException(
                new ConcurrentModificationException(
                    "Early conflict detected..."));
        }
    }

    // 删除 marker 目录
    @Override
    public boolean deleteMarkerDir(
        HoodieEngineContext context, int parallelism) {
        Map<String, String> paramsMap =
            Collections.singletonMap(
                MARKER_DIR_PATH_PARAM, markerDirPath.toString());
        return executeRequestToTimelineServer(
            DELETE_MARKER_DIR_URL, paramsMap,
            BOOLEAN_TYPE_REFERENCE, RequestMethod.POST);
    }

    // 获取所有已创建和合并的数据路径
    @Override
    public Set<String> createdAndMergedDataPaths(
        HoodieEngineContext context, int parallelism) throws IOException {
        Set<String> markerPaths = executeRequestToTimelineServer(
            CREATE_AND_MERGE_MARKERS_URL, paramsMap,
            SET_TYPE_REFERENCE, RequestMethod.GET);
        return markerPaths.stream()
            .map(WriteMarkers::stripMarkerSuffix)
            .collect(Collectors.toSet());
    }
}
```

**客户端通信模型**：

所有请求通过 `TimelineServiceClient` 发送 HTTP 请求到 TLS。请求参数通过 URL 查询参数传递（不是请求体），这简化了服务端的解析逻辑。

---

### 4.7 AsyncTimelineServerBasedDetectionStrategy 冲突检测

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/marker/AsyncTimelineServerBasedDetectionStrategy.java`

异步冲突检测策略在 TLS 内部运行，周期性地检查不同 Writer 的 marker 是否有 FileGroup 级别的冲突。

```java
@Slf4j
public class AsyncTimelineServerBasedDetectionStrategy
    extends TimelineServerBasedDetectionStrategy {

    private final AtomicBoolean hasConflict = new AtomicBoolean(false);
    private ScheduledExecutorService asyncDetectorExecutor;

    @Override
    public boolean hasMarkerConflict() {
        return hasConflict.get();
    }

    @Override
    public void resolveMarkerConflict(
        String basePath, String markerDir, String markerName) {
        throw new HoodieEarlyConflictDetectionException(
            new ConcurrentModificationException(
                "Early conflict detected but cannot resolve conflicts "
                + "for overlapping writes"));
    }

    @Override
    public void startAsyncDetection(
        Long initialDelayMs, Long periodMs, String markerDir,
        String basePath, Long maxAllowableHeartbeatIntervalInMs,
        HoodieStorage storage, Object markerHandler,
        Set<HoodieInstant> completedCommits) {
        
        if (asyncDetectorExecutor != null) {
            asyncDetectorExecutor.shutdown();
        }
        hasConflict.set(false);
        asyncDetectorExecutor = Executors.newSingleThreadScheduledExecutor();
        asyncDetectorExecutor.scheduleAtFixedRate(
            new MarkerBasedEarlyConflictDetectionRunnable(
                hasConflict, (MarkerHandler) markerHandler,
                markerDir, basePath, storage,
                maxAllowableHeartbeatIntervalInMs,
                completedCommits, checkCommitConflict),
            initialDelayMs, periodMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void detectAndResolveConflictIfNecessary()
        throws HoodieEarlyConflictDetectionException {
        if (hasMarkerConflict()) {
            resolveMarkerConflict(basePath, markerDir, markerName);
        }
    }

    public void stop() {
        if (asyncDetectorExecutor != null) {
            asyncDetectorExecutor.shutdown();
        }
    }
}
```

**冲突检测模型**：

```
定时调度（默认 30 秒间隔）
    |
    v
MarkerBasedEarlyConflictDetectionRunnable.run()
    |
    |-- 获取当前 instant 的所有 marker（含待处理的）
    |-- 扫描 .temp 目录下其他 instant 的 marker
    |-- 比较两组 marker 的 FileGroup ID
    |
    |-- 有交集？
    |     |-- 是 => hasConflict.set(true)
    |     |-- 否 => 继续
    v
下次 createMarker() 调用时检查 hasConflict
    |-- true => 抛出 HoodieEarlyConflictDetectionException
    |-- false => 继续创建
```

**为什么是异步而不是同步检测？**

同步检测意味着每次创建 marker 都要扫描其他 Writer 的 marker，这会显著增加每个 marker 创建的延迟。异步检测在后台运行，不影响 marker 创建的性能，代价是可能有最多一个检测周期（默认 30 秒）的检测延迟。

---

### 4.8 MarkerBasedEarlyConflictDetectionRunnable 检测实现

**源码路径**：`hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/marker/MarkerBasedEarlyConflictDetectionRunnable.java`

```java
@Slf4j
public class MarkerBasedEarlyConflictDetectionRunnable implements Runnable {

    private final MarkerHandler markerHandler;
    private final String markerDir;
    private final String basePath;
    private final HoodieStorage storage;
    private final AtomicBoolean hasConflict;
    private final long maxAllowableHeartbeatIntervalInMs;
    private final Set<HoodieInstant> completedCommits;
    private final boolean checkCommitConflict;

    @Override
    public void run() {
        // 如果已经检测到冲突，跳过
        if (hasConflict.get()) {
            return;
        }

        try {
            // 1. 获取当前 instant 的 marker（含未处理的请求）
            Set<String> pendingMarkers =
                markerHandler.getPendingMarkersToProcess(markerDir);

            if (!storage.exists(new StoragePath(markerDir))
                && pendingMarkers.isEmpty()) {
                return;
            }

            Set<String> currentInstantAllMarkers = new HashSet<>();
            currentInstantAllMarkers.addAll(
                markerHandler.getAllMarkers(markerDir));
            currentInstantAllMarkers.addAll(pendingMarkers);

            // 2. 扫描 .temp 目录下所有 instant 的 marker
            StoragePath tempPath = new StoragePath(
                basePath, HoodieTableMetaClient.TEMPFOLDER_NAME);
            List<StoragePath> instants =
                MarkerUtils.getAllMarkerDir(tempPath, storage);

            // 3. 过滤候选 instant（排除过期的心跳）
            HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setConf(storage.getConf().newInstance())
                .setBasePath(basePath)
                .setLoadActiveTimelineOnLoad(true)
                .build();
            List<String> candidate = MarkerUtils.getCandidateInstants(
                metaClient.getActiveTimeline(), instants,
                MarkerUtils.markerDirToInstantTime(markerDir),
                maxAllowableHeartbeatIntervalInMs,
                storage, basePath);

            // 4. 读取候选 instant 的所有 marker
            Set<String> tableMarkers = candidate.stream()
                .flatMap(instant -> {
                    return MarkerUtils
                        .readTimelineServerBasedMarkersFromFileSystem(
                            instant, storage,
                            new HoodieLocalEngineContext(
                                storage.getConf().newInstance()), 100)
                        .values().stream()
                        .flatMap(Collection::stream);
                })
                .collect(Collectors.toSet());

            // 5. 比较 FileGroup ID
            Set<String> currentFileIDs = currentInstantAllMarkers.stream()
                .map(MarkerUtils::makerToPartitionAndFileID)
                .collect(Collectors.toSet());
            Set<String> tableFilesIDs = tableMarkers.stream()
                .map(MarkerUtils::makerToPartitionAndFileID)
                .collect(Collectors.toSet());

            currentFileIDs.retainAll(tableFilesIDs);  // 求交集

            // 6. 检测冲突
            if (!currentFileIDs.isEmpty()
                || (checkCommitConflict && MarkerUtils.hasCommitConflict(
                    metaClient.getActiveTimeline(),
                    currentInstantAllMarkers.stream()
                        .map(MarkerUtils::makerToPartitionAndFileID)
                        .collect(Collectors.toSet()),
                    completedCommits))) {
                
                log.error("Conflict writing detected based on markers!");
                hasConflict.compareAndSet(false, true);
            }
        } catch (IOException e) {
            throw new HoodieIOException(
                "IOException occurs during checking marker conflict");
        }
    }
}
```

**冲突检测的详细步骤**：

1. **收集当前 Writer 的 marker**：包括已经写入 MARKERS 文件的和还在队列中待处理的
2. **扫描其他 Writer 的 marker 目录**：通过 `.hoodie/.temp/` 目录找到所有活跃的 instant
3. **心跳过滤**：排除心跳超时的 instant（认为对应的 Writer 已经死亡）
4. **FileGroup ID 比较**：将 marker 转换为 `partitionPath/fileId` 形式，比较两组集合的交集
5. **冲突判定**：如果有交集，说明两个 Writer 在写入同一个 FileGroup，触发冲突

**为什么还需要检查 commitConflict？**

除了 FileGroup 级别的冲突，还可能存在 commit 级别的冲突：另一个 Writer 可能已经成功提交了一个操作（如 clustering），修改了当前 Writer 正在写入的文件。`hasCommitConflict` 检查已完成的 commits 中是否有与当前 marker 关联的 FileGroup 变更。

---

## 第五部分：客户端与服务端的交互

### 5.1 TimelineServiceClientBase 基类

**源码路径**：`hudi-common/src/main/java/org/apache/hudi/timeline/TimelineServiceClientBase.java`

```java
public abstract class TimelineServiceClientBase implements Serializable {

    private RetryHelper<Response, IOException> retryHelper;

    protected TimelineServiceClientBase(FileSystemViewStorageConfig config) {
        if (config.getBooleanOrDefault(
            FileSystemViewStorageConfig.REMOTE_RETRY_ENABLE)) {
            retryHelper = new RetryHelper<>(
                config.getRemoteTimelineClientMaxRetryIntervalMs(),
                config.getRemoteTimelineClientMaxRetryNumbers(),
                config.getRemoteTimelineInitialRetryIntervalMs(),
                config.getRemoteTimelineClientRetryExceptions(),
                "Sending request to timeline server");
        }
    }

    protected abstract Response executeRequest(Request request) throws IOException;

    public Response makeRequest(Request request) throws IOException {
        return (retryHelper != null)
            ? retryHelper.start(() -> executeRequest(request))
            : executeRequest(request);
    }
}
```

**重试机制**：

当启用重试时（`hoodie.filesystem.view.remote.retry.enable = true`），`RetryHelper` 提供指数退避重试：

- **初始间隔**：`REMOTE_INITIAL_RETRY_INTERVAL_MS`（默认 100ms）
- **最大间隔**：`REMOTE_MAX_RETRY_INTERVAL_MS`（默认 2000ms）
- **最大重试次数**：`REMOTE_MAX_RETRY_NUMBERS`（默认 3 次）
- **可重试异常**：`RETRY_EXCEPTIONS`（默认所有 IOException 和 RuntimeException）

**重试序列示例**：

```
第 1 次尝试 ──> 失败
  等待 100ms
第 2 次尝试 ──> 失败
  等待 200ms（指数退避）
第 3 次尝试 ──> 失败
  等待 400ms
第 4 次尝试（超过最大次数）──> 抛出异常
```

#### Request 和 Response 设计

```java
// Request 使用 Builder 模式
public static class Request {
    private final RequestMethod method;
    private final String path;
    private final Option<Map<String, String>> queryParameters;

    public static Builder newBuilder(RequestMethod method, String path) {
        return new Builder(method, path);
    }

    public static class Builder {
        public Builder addQueryParam(String key, String value) {
            queryParameters.get().put(key, value);
            return this;
        }

        public Builder addQueryParams(Map<String, String> parameters) {
            queryParameters.get().putAll(parameters);
            return this;
        }

        public Request build() {
            return new Request(method, path, queryParameters);
        }
    }
}

// Response 封装反序列化逻辑
public static class Response {
    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper().registerModule(new AfterburnerModule());
    private final InputStream content;

    public <T> T getDecodedContent(TypeReference reference) throws IOException {
        try {
            return (T) OBJECT_MAPPER.readValue(content, reference);
        } finally {
            content.close();  // 确保关闭流
        }
    }
}
```

**为什么 Response 中也使用 AfterburnerModule？**

客户端反序列化的频率和数据量与服务端序列化对等。使用 AfterburnerModule 可以在客户端也获得序列化性能提升。

---

### 5.2 TimelineServiceClient HTTP 客户端

**源码路径**：`hudi-common/src/main/java/org/apache/hudi/timeline/TimelineServiceClient.java`

```java
@Slf4j
public class TimelineServiceClient extends TimelineServiceClientBase {

    private static final String DEFAULT_SCHEME = "http";

    protected final String timelineServerHost;
    protected final int timelineServerPort;
    protected final int timeoutMs;

    public TimelineServiceClient(FileSystemViewStorageConfig config) {
        super(config);
        this.timelineServerHost = config.getRemoteViewServerHost();
        this.timelineServerPort = config.getRemoteViewServerPort();
        this.timeoutMs = (int) TimeUnit.SECONDS.toMillis(
            config.getRemoteTimelineClientTimeoutSecs());
    }

    @Override
    protected Response executeRequest(Request request) throws IOException {
        // 构建 URL
        URIBuilder builder = new URIBuilder()
            .setHost(timelineServerHost)
            .setPort(timelineServerPort)
            .setPath(request.getPath())
            .setScheme(DEFAULT_SCHEME);

        if (request.getQueryParameters().isPresent()) {
            request.getQueryParameters().get()
                .forEach(builder::addParameter);
        }

        String url = builder.toString();
        log.debug("Sending request : ({})", url);
        
        // 执行 HTTP 请求
        org.apache.http.client.fluent.Response response =
            get(request.getMethod(), url, timeoutMs);
        return new Response(response.returnContent().asStream());
    }

    private org.apache.http.client.fluent.Response get(
        RequestMethod method, String url, int timeoutMs) throws IOException {
        switch (method) {
            case GET:
                return org.apache.http.client.fluent.Request.Get(url)
                    .connectTimeout(timeoutMs)
                    .socketTimeout(timeoutMs)
                    .execute();
            case POST:
            default:
                return org.apache.http.client.fluent.Request.Post(url)
                    .connectTimeout(timeoutMs)
                    .socketTimeout(timeoutMs)
                    .execute();
        }
    }
}
```

**HTTP 客户端选择——Apache HttpComponents Fluent API**：

使用 `org.apache.http.client.fluent` 而不是更底层的 `HttpClient`，原因：
1. API 简洁，一行代码完成请求
2. 自动处理连接管理和资源释放
3. 支持连接超时和读取超时设置

**超时设置**：

- `connectTimeout`：建立 TCP 连接的超时时间
- `socketTimeout`：等待服务端响应数据的超时时间
- 默认值：300 秒（5 分钟），在 `FileSystemViewStorageConfig.REMOTE_TIMEOUT_SECS` 中配置

**为什么超时默认这么长？**

FileSystemView 的构建可能涉及大量文件的 LIST 操作，特别是第一次请求时。5 分钟的超时确保在大表的首次加载时不会超时。

---

### 5.3 RemoteHoodieTableFileSystemView 远程代理

**源码路径**：`hudi-common/src/main/java/org/apache/hudi/common/table/view/RemoteHoodieTableFileSystemView.java`

RemoteHoodieTableFileSystemView 是 `SyncableFileSystemView` 接口的远程实现，将所有方法调用转换为 HTTP 请求。

#### URL 常量定义

```java
public class RemoteHoodieTableFileSystemView
    implements SyncableFileSystemView, Serializable {

    private static final String SCHEME = "http";
    private static final String BASE_URL = "/v1/hoodie/view";
    
    // FileSlice API
    public static final String LATEST_PARTITION_SLICES_URL =
        String.format("%s/%s", BASE_URL, "slices/partition/latest/");
    public static final String LATEST_PARTITION_SLICES_INFLIGHT_URL =
        String.format("%s/%s", BASE_URL, "slices/partition/latest/inflight/");
    // ... 更多 URL 常量

    // 请求参数名
    public static final String PARTITION_PARAM = "partition";
    public static final String BASEPATH_PARAM = "basepath";
    public static final String MAX_INSTANT_PARAM = "maxinstant";
    public static final String LAST_INSTANT_TS = "lastinstantts";
    public static final String TIMELINE_HASH = "timelinehash";
    public static final String REFRESH_OFF = "refreshoff";
    // ... 更多参数
}
```

#### 方法调用到 HTTP 请求的转换

以 `getLatestFileSlices` 为例：

```java
@Override
public Stream<FileSlice> getLatestFileSlices(String partitionPath) {
    Map<String, String> paramsMap = getParamsWithPartitionPath(partitionPath);
    return getLatestFileSlicesStreamFromParams(
        LATEST_PARTITION_SLICES_URL, paramsMap);
}

private Stream<FileSlice> getLatestFileSlicesStreamFromParams(
    String requestPath, Map<String, String> paramsMap) {
    try {
        List<FileSliceDTO> dataFiles = executeRequest(
            requestPath, paramsMap,
            FILE_SLICE_DTOS_REFERENCE, RequestMethod.GET);
        return dataFiles.stream().map(FileSliceDTO::toFileSlice);
    } catch (IOException e) {
        throw new HoodieRemoteException(e);
    }
}
```

#### 核心请求执行方法

```java
private <T> T executeRequest(String requestPath,
    Map<String, String> queryParameters,
    TypeReference<T> reference, RequestMethod method) throws IOException {
    
    ValidationUtils.checkArgument(!closed, "View already closed");

    // 自动附加 Timeline 一致性参数
    timeline.lastInstant().ifPresent(instant ->
        queryParameters.put(LAST_INSTANT_TS, instant.requestedTime()));
    queryParameters.put(TIMELINE_HASH, timeline.getTimelineHash());

    return timelineServiceClient.makeRequest(
        TimelineServiceClient.Request.newBuilder(method, requestPath)
            .addQueryParams(queryParameters)
            .build())
        .getDecodedContent(reference);
}
```

**关键设计点**：

1. **自动附加 Timeline 信息**：每个请求都会携带客户端的 `lastInstantTs` 和 `timelineHash`，服务端据此判断是否需要同步
2. **TypeReference 类型安全**：使用预定义的 TypeReference 常量，避免每次请求创建新对象
3. **closed 状态检查**：防止在 View 关闭后继续使用

#### 刷新和同步

```java
public boolean refresh() {
    Map<String, String> paramsMap = getParams();
    try {
        // 先刷新本地 Timeline
        this.timeline = metaClient.reloadActiveTimeline()
            .filterCompletedAndCompactionInstants();
        // 再通知服务端刷新
        return executeRequest(REFRESH_TABLE_URL, paramsMap,
            BOOLEAN_TYPE_REFERENCE, RequestMethod.POST);
    } catch (IOException e) {
        throw new HoodieRemoteException(e);
    }
}

@Override
public void sync() {
    refresh();
}

@Override
public void reset() {
    refresh();
}
```

**为什么 sync/reset 都调用 refresh？**

对于远程视图来说，同步和重置的效果是一样的：刷新本地 Timeline 并通知服务端清除缓存。服务端收到 refresh 后会清除该表的 FileSystemView 缓存，下次请求时重新构建。

---

### 5.4 DTO 序列化传输体系

TLS 使用 DTO（Data Transfer Object）模式在客户端和服务端之间传输数据。所有 DTO 类位于：

`hudi-common/src/main/java/org/apache/hudi/common/table/timeline/dto/`

**主要 DTO 类**：

| DTO 类 | 对应域模型 | 用途 |
|--------|-----------|------|
| `BaseFileDTO` | `HoodieBaseFile` | 传输基础文件信息 |
| `FileSliceDTO` | `FileSlice` | 传输文件切片信息 |
| `FileGroupDTO` | `HoodieFileGroup` | 传输文件分组信息 |
| `InstantDTO` | `HoodieInstant` | 传输 instant 信息 |
| `TimelineDTO` | `HoodieTimeline` | 传输 Timeline 信息 |
| `CompactionOpDTO` | `CompactionOperation` | 传输压缩操作信息 |
| `ClusteringOpDTO` | Clustering 操作 | 传输聚簇操作信息 |

**为什么需要 DTO 而不是直接序列化域模型？**

1. **解耦**：域模型可能包含不可序列化的字段（如 `HoodieStorage` 引用）
2. **精简**：DTO 只包含传输所需的字段，减少序列化开销
3. **版本兼容**：DTO 可以独立演进，不影响域模型

---

### 5.5 请求-响应完整流程

以 `getLatestFileSlices("partition1")` 为例，展示完整的请求-响应链路：

```
Executor (RemoteHoodieTableFileSystemView)
    |
    |-- 1. 构建参数: {basepath=..., partition=partition1,
    |                  lastinstantts=20240101120000, timelinehash=abc123}
    |
    |-- 2. TimelineServiceClient.makeRequest()
    |       |-- 构建 URL: http://driver-host:26754/v1/hoodie/view/slices/partition/latest/?basepath=...&partition=partition1&...
    |       |-- HTTP GET 请求
    |
    v
TLS (Javalin HTTP Server)
    |
    |-- 3. ViewHandler.handle()
    |       |-- 3a. isLocalViewBehind()? 比较 timelinehash
    |       |-- 3b. 如果落后: syncIfLocalViewBehind() -> view.sync()
    |       |-- 3c. 执行实际 handler
    |
    |-- 4. FileSliceHandler.getLatestFileSlices("basepath", "partition1")
    |       |-- viewManager.getFileSystemView("basepath")
    |       |       |-- 首次：创建 HoodieTableMetaClient + FileSystemView
    |       |       |-- 后续：从 ConcurrentHashMap 缓存获取
    |       |-- .getLatestFileSlices("partition1")
    |       |       |-- 首次加载分区：LIST 文件系统
    |       |       |-- 后续：从缓存获取
    |       |-- .map(FileSliceDTO::fromFileSlice)
    |       |-- .collect(Collectors.toList())
    |
    |-- 5. jsonifyResult(ctx, dtos, metricsRegistry)
    |       |-- ObjectMapper.writeValueAsString(dtos)
    |       |-- 使用 AfterburnerModule 加速序列化
    |
    |-- 6. ctx.result(jsonString)
    |
    v
Executor
    |
    |-- 7. response.returnContent().asStream()
    |-- 8. ObjectMapper.readValue(stream, FILE_SLICE_DTOS_REFERENCE)
    |-- 9. dtos.stream().map(FileSliceDTO::toFileSlice)
    |-- 10. 返回 Stream<FileSlice>
```

**性能特性**：

- 步骤 3b（同步）只在 Timeline 变化时触发，大部分请求会跳过
- 步骤 4 中的文件系统 LIST 只在首次加载分区时执行，后续从缓存获取
- 序列化和反序列化使用 AfterburnerModule 加速

---

## 第六部分：生产运维

### 6.1 TLS 配置参数完整手册

#### 嵌入式 TLS 配置（HoodieWriteConfig）

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.embed.timeline.server` | `true` | 是否启用嵌入式 TLS |
| `hoodie.embed.timeline.server.reuse.enabled` | `false` | 是否复用 TLS 实例（多表共享） |
| `hoodie.embed.timeline.server.port` | `0` | TLS 端口（0=自动分配） |
| `hoodie.embed.timeline.server.threads` | `-1` | TLS 线程数（-1=自动） |
| `hoodie.embed.timeline.server.gzip` | `true` | 是否启用 GZIP 压缩 |
| `hoodie.embed.timeline.server.async` | `false` | 是否异步处理请求 |

#### FileSystemView 存储配置（FileSystemViewStorageConfig）

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.filesystem.view.type` | `MEMORY` | View 存储类型 |
| `hoodie.filesystem.view.secondary.type` | `MEMORY` | 备用 View 存储类型 |
| `hoodie.filesystem.view.incr.timeline.sync.enable` | `false` | 是否启用增量同步 |
| `hoodie.filesystem.view.remote.host` | `localhost` | TLS 远程地址 |
| `hoodie.filesystem.view.remote.port` | `26754` | TLS 远程端口 |
| `hoodie.filesystem.view.remote.timeout.secs` | `300` | 客户端超时（秒） |
| `hoodie.filesystem.view.remote.retry.enable` | `false` | 是否启用重试 |
| `hoodie.filesystem.view.remote.retry.max_numbers` | `3` | 最大重试次数 |
| `hoodie.filesystem.view.remote.retry.initial_interval_ms` | `100` | 初始重试间隔（ms） |
| `hoodie.filesystem.view.remote.retry.max_interval_ms` | `2000` | 最大重试间隔（ms） |
| `hoodie.filesystem.view.remote.retry.exceptions` | `""` | 可重试的异常类 |
| `hoodie.filesystem.remote.backup.view.enable` | `true` | 是否启用备用视图 |
| `hoodie.filesystem.view.spillable.dir` | `/tmp/` | Spillable 存储目录 |
| `hoodie.filesystem.view.spillable.mem` | `104857600` (100MB) | Spillable 内存限制 |
| `hoodie.filesystem.view.spillable.compaction.mem.fraction` | `0.1` | Compaction 内存比例 |
| `hoodie.filesystem.view.spillable.log.compaction.mem.fraction` | `0.02` | Log Compaction 内存比例 |
| `hoodie.filesystem.view.spillable.replaced.mem.fraction` | `0.05` | Replaced 内存比例 |
| `hoodie.filesystem.view.spillable.clustering.mem.fraction` | `0.02` | Clustering 内存比例 |
| `hoodie.filesystem.view.rocksdb.base.path` | `/tmp/hoodie_timeline_rocksdb` | RocksDB 路径 |

#### 独立部署 TLS 命令行参数（TimelineService.Config）

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--server-port (-p)` | `26754` | 服务端口 |
| `--view-storage (-st)` | `SPILLABLE_DISK` | View 存储类型 |
| `--max-view-mem-per-table (-mv)` | `2048` (MB) | 每表最大 View 内存 |
| `--mem-overhead-fraction-pending-compaction (-cf)` | `0.001` | Compaction 内存比例 |
| `--base-store-path (-sp)` | `/tmp/` | 溢出存储路径 |
| `--rocksdb-path (-rp)` | `/tmp/hoodie_timeline_rocksdb` | RocksDB 路径 |
| `--threads (-t)` | `250` | 线程数 |
| `--async` | `false` | 异步处理 |
| `--compress` | `true` | GZIP 压缩 |
| `--enable-marker-requests (-em)` | `false` | 启用 Marker API |
| `--marker-batch-threads (-mbt)` | `20` | Marker 批处理线程数 |
| `--marker-batch-interval-ms (-mbi)` | `50` | Marker 批处理间隔（ms） |
| `--marker-parallelism (-mdp)` | `100` | Marker 读写并行度 |

#### 冲突检测配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--early-conflict-detection-enable` | `false` | 启用早期冲突检测 |
| `--early-conflict-detection-strategy` | `AsyncTimelineServerBasedDetectionStrategy` | 冲突检测策略类 |
| `--early-conflict-detection-check-commit-conflict` | `false` | 是否检查 commit 冲突 |
| `--async-conflict-detector-initial-delay-ms` | `0` | 首次检测延迟（ms） |
| `--async-conflict-detector-period-ms` | `30000` | 检测周期（ms） |
| `--early-conflict-detection-max-heartbeat-interval-ms` | `120000` | 心跳超时（ms） |

---

### 6.2 TLS 故障排查

#### 问题 1：端口冲突

**现象**：TLS 启动失败，日志中出现 `JavalinBindException`

**排查步骤**：

1. 检查日志中的端口信息：

```
WARN Timeline server could not bind on port 26754. Attempting port 26754 + 1.
WARN Timeline server could not bind on port 26755. Attempting port 26755 + 1.
...
ERROR Timeline server start failed on port 26754, after retry 16 times
```

2. 确认端口占用情况：

```bash
netstat -tlnp | grep 26754
```

**解决方案**：

- 嵌入式模式：设置 `hoodie.embed.timeline.server.port=0` 让系统自动分配
- 独立部署：更换未使用的端口
- 多作业环境：确保每个作业使用不同的端口范围

#### 问题 2：连接超时

**现象**：Executor 日志中出现 `SocketTimeoutException` 或 `ConnectTimeoutException`

**排查步骤**：

1. 确认 TLS 是否在运行：
```bash
curl http://driver-host:26754/
# 应返回 "Hello Hudi"
```

2. 检查网络连通性：Executor 到 Driver 的网络是否通畅

3. 检查 TLS 线程池是否耗尽：
```
// 日志中可能出现线程池满的警告
```

**解决方案**：

- 增大超时时间：`hoodie.filesystem.view.remote.timeout.secs=600`
- 启用重试：`hoodie.filesystem.view.remote.retry.enable=true`
- 增大线程池：`hoodie.embed.timeline.server.threads=500`
- 确保 `REMOTE_FIRST` 模式启用，自动降级

#### 问题 3：内存不足（OOM）

**现象**：TLS 或 Driver 进程 OOM

**排查步骤**：

1. 检查 FileSystemView 缓存的表数量和大小
2. 检查是否有大量分区被加载

**解决方案**：

- 使用 `SPILLABLE_DISK` 存储类型替代 `MEMORY`
- 减小内存限制：`hoodie.filesystem.view.spillable.mem=52428800`（50MB）
- 使用 `EMBEDDED_KV_STORE`（RocksDB）降低内存占用
- 增大 Driver 内存

#### 问题 4：Timeline 不一致

**现象**：日志中出现 `BadRequestResponse: Last known instant from client was xxx but server has...`

**排查步骤**：

这通常是多 Writer 并发写入导致的正常现象。客户端的 Timeline 和服务端不一致，因为另一个 Writer 已经提交了新数据。

**解决方案**：

- 启用 `REMOTE_FIRST` 模式（默认），允许降级到本地 View
- 启用冲突检测来避免并发写入冲突
- 检查是否有不必要的并发写入

#### 问题 5：Marker 创建超时

**现象**：写入时 Marker 创建请求长时间不返回

**排查步骤**：

1. 检查 Marker 批处理配置是否合理
2. 确认 TLS 的 Marker Handler 是否正常工作
3. 检查文件系统 I/O 延迟

**解决方案**：

- 减小批处理间隔：`--marker-batch-interval-ms 25`
- 增大批处理线程数：`--marker-batch-threads 40`
- 检查底层存储的写入性能

---

### 6.3 TLS 性能调优

#### 线程池调优

**关键原则**：线程数应该匹配并发请求数量

```
推荐配置公式：
  线程数 = max(200, 并发 Executor 数量 * 1.5)
```

对于大规模作业（1000+ Executor），建议：
```
hoodie.embed.timeline.server.threads=1500
```

但不建议超过 2000，因为线程上下文切换的开销会抵消并发带来的好处。

#### 缓存策略调优

**内存模式（适合中小表）**：
```properties
hoodie.filesystem.view.type=MEMORY
```

**Spillable 模式（适合大表，推荐）**：
```properties
hoodie.filesystem.view.type=SPILLABLE_DISK
hoodie.filesystem.view.spillable.mem=209715200  # 200MB
hoodie.filesystem.view.spillable.dir=/data/tmp/hudi_spillable
```

**RocksDB 模式（适合超大表）**：
```properties
hoodie.filesystem.view.type=EMBEDDED_KV_STORE
hoodie.filesystem.view.rocksdb.base.path=/data/tmp/hudi_rocksdb
```

#### 增量同步调优

启用增量同步可以显著减少 View 刷新的开销：
```properties
hoodie.filesystem.view.incr.timeline.sync.enable=true
```

**何时启用增量同步？**

- 表分区数多（>100）：增量同步避免全量重建的高成本
- 写入频率高：每次 commit 后的 View 更新更快
- 独立部署的 TLS：长期运行时受益最大

**何时不启用？**

- 小表（<10 个分区）：全量重建也很快
- Timeline 操作复杂（频繁 rollback/restore）：增量同步的 corner case 可能导致问题

#### Marker 批量创建调优

| 参数 | 小作业 | 中型作业 | 大型作业 |
|------|--------|---------|---------|
| `markerBatchNumThreads` | 10 | 20 | 40 |
| `markerBatchIntervalMs` | 100 | 50 | 25 |
| `markerParallelism` | 50 | 100 | 200 |

**调优原则**：

- **`markerBatchIntervalMs`**：间隔越小，Marker 创建延迟越低，但 I/O 频率越高。在低延迟场景设为 25ms，在高吞吐场景设为 100ms
- **`markerBatchNumThreads`**：控制并行写入 MARKERS 文件的线程数。增大可以提高吞吐，但过多会增加文件系统负担
- **`markerParallelism`**：控制 Marker 文件的读写并行度。在 HDFS 上不要超过 NameNode 的承受能力

#### GZIP 压缩调优

```properties
hoodie.embed.timeline.server.gzip=true
```

**何时关闭压缩？**

- TLS 和 Executor 在同一台机器（localhost）：网络开销为零，压缩反而增加 CPU 负担
- 返回数据量小（<1KB）：压缩头部开销可能大于压缩收益

**何时开启压缩？**

- 跨节点通信：压缩可以减少 30-70% 的传输量
- 大量 FileSlice 返回：对大响应体效果显著

#### 异步处理调优

```properties
hoodie.embed.timeline.server.async=true
```

**建议**：在大多数场景下保持默认（关闭）。只有当你观察到：
1. JSON 序列化时间占总响应时间的 30%+
2. 并发请求数持续接近线程池上限

才考虑启用异步模式。

#### 重试机制调优

对于生产环境，建议启用重试：
```properties
hoodie.filesystem.view.remote.retry.enable=true
hoodie.filesystem.view.remote.retry.max_numbers=3
hoodie.filesystem.view.remote.retry.initial_interval_ms=200
hoodie.filesystem.view.remote.retry.max_interval_ms=5000
```

这可以应对短暂的网络波动和 GC 暂停。

---

## 总结

### Timeline Service 的核心价值

1. **性能优化**：将 N 次文件系统 LIST 操作减少为 1 次，显著降低了存储系统的压力和任务的初始化延迟
2. **数据一致性**：所有 Executor 通过同一个 TLS 获取文件视图，确保了数据的一致性
3. **Marker 优化**：批量创建 Marker 将小文件问题从 O(N) 降低到 O(1) 级别
4. **冲突检测**：集中管理所有 Writer 的 Marker，实现了 FileGroup 级别的早期冲突检测

### 架构设计亮点

1. **嵌入式友好**：TLS 可以无感地嵌入到 Spark Driver 中，无需额外部署
2. **优雅降级**：`REMOTE_FIRST` + `PriorityBasedFileSystemView` 确保了 TLS 不可用时的容错
3. **增量同步**：`IncrementalTimelineSyncFileSystemView` 避免了全量重建的高成本
4. **异步批量**：Marker 的异步批量创建架构在保证正确性的同时实现了高吞吐

### 关键源码路径速查

| 组件 | 源码路径 |
|------|---------|
| TimelineService | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/TimelineService.java` |
| RequestHandler | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/RequestHandler.java` |
| Handler 基类 | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/Handler.java` |
| TimelineHandler | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/TimelineHandler.java` |
| BaseFileHandler | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/BaseFileHandler.java` |
| FileSliceHandler | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/FileSliceHandler.java` |
| MarkerHandler | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/MarkerHandler.java` |
| RemotePartitionerHandler | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/RemotePartitionerHandler.java` |
| MarkerDirState | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/marker/MarkerDirState.java` |
| MarkerCreationDispatchingRunnable | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/marker/MarkerCreationDispatchingRunnable.java` |
| BatchedMarkerCreationRunnable | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/marker/BatchedMarkerCreationRunnable.java` |
| BatchedMarkerCreationContext | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/marker/BatchedMarkerCreationContext.java` |
| MarkerCreationFuture | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/marker/MarkerCreationFuture.java` |
| AsyncTimelineServerBasedDetectionStrategy | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/marker/AsyncTimelineServerBasedDetectionStrategy.java` |
| MarkerBasedEarlyConflictDetectionRunnable | `hudi-timeline-service/src/main/java/org/apache/hudi/timeline/service/handlers/marker/MarkerBasedEarlyConflictDetectionRunnable.java` |
| EmbeddedTimelineService | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/client/embedded/EmbeddedTimelineService.java` |
| TimelineServerBasedWriteMarkers | `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/marker/TimelineServerBasedWriteMarkers.java` |
| RemoteHoodieTableFileSystemView | `hudi-common/src/main/java/org/apache/hudi/common/table/view/RemoteHoodieTableFileSystemView.java` |
| PriorityBasedFileSystemView | `hudi-common/src/main/java/org/apache/hudi/common/table/view/PriorityBasedFileSystemView.java` |
| FileSystemViewManager | `hudi-common/src/main/java/org/apache/hudi/common/table/view/FileSystemViewManager.java` |
| FileSystemViewStorageConfig | `hudi-common/src/main/java/org/apache/hudi/common/table/view/FileSystemViewStorageConfig.java` |
| FileSystemViewStorageType | `hudi-common/src/main/java/org/apache/hudi/common/table/view/FileSystemViewStorageType.java` |
| IncrementalTimelineSyncFileSystemView | `hudi-common/src/main/java/org/apache/hudi/common/table/view/IncrementalTimelineSyncFileSystemView.java` |
| TimelineServiceClient | `hudi-common/src/main/java/org/apache/hudi/timeline/TimelineServiceClient.java` |
| TimelineServiceClientBase | `hudi-common/src/main/java/org/apache/hudi/timeline/TimelineServiceClientBase.java` |
| TimelineServerBasedDetectionStrategy | `hudi-common/src/main/java/org/apache/hudi/common/conflict/detection/TimelineServerBasedDetectionStrategy.java` |
| MarkerOperation | `hudi-common/src/main/java/org/apache/hudi/common/table/marker/MarkerOperation.java` |
