# Flink-Hudi 参数调优快速参考指南

## 快速导航

### 🎯 按场景快速查找

#### 高吞吐场景（TB 级数据，追求最大吞吐）
```properties
# 缓冲区配置
write.batch.size=1024
write.task.max.size=4096

# 并行度配置
write.tasks=32
write.index_bootstrap.tasks=16

# Checkpoint 配置
execution.checkpointing.interval=120000

# Compaction 配置
compaction.async.enabled=true
compaction.delta_commits=20
```

**预期效果**：
- 吞吐量：100万+ 条/秒
- 延迟：1-2 分钟
- 小文件：少

---

#### 低延迟场景（秒级数据可见性）
```properties
# 缓冲区配置
write.batch.size=64
write.task.max.size=512

# 并行度配置
write.tasks=8
write.index_bootstrap.tasks=4

# Checkpoint 配置
execution.checkpointing.interval=30000

# Compaction 配置
compaction.async.enabled=true
compaction.delta_commits=5
```

**预期效果**：
- 吞吐量：10万+ 条/秒
- 延迟：30 秒以内
- 小文件：较多

---

#### 内存紧张场景（内存有限，需要稳定运行）
```properties
# 缓冲区配置
write.batch.size=128
write.task.max.size=512

# 并行度配置
write.tasks=4
write.index_bootstrap.tasks=2

# Checkpoint 配置
execution.checkpointing.interval=60000

# Compaction 配置
compaction.async.enabled=true
compaction.delta_commits=10
```

**预期效果**：
- 吞吐量：10万+ 条/秒
- 延迟：1 分钟
- 内存占用：< 2GB

---

#### 读多写少场景（优化读取性能）
```properties
# 缓冲区配置
write.batch.size=256
write.task.max.size=1024

# 索引配置
hoodie.index.type=BUCKET

# Compaction 配置
compaction.async.enabled=true
compaction.trigger.strategy=num_commits
compaction.delta_commits=5
```

**预期效果**：
- 写入吞吐：50万+ 条/秒
- 读取延迟：< 100ms
- 查询性能：最优

---

#### 写多读少场景（优化写入性能）
```properties
# 缓冲区配置
write.batch.size=512
write.task.max.size=2048

# 并行度配置
write.tasks=16

# Compaction 配置
compaction.async.enabled=true
compaction.delta_commits=20
```

**预期效果**：
- 写入吞吐：200万+ 条/秒
- 延迟：1-2 分钟
- 读取性能：可能较差

---

## 参数详解

### 缓冲区参数

| 参数 | 默认值 | 范围 | 说明 |
|------|--------|------|------|
| `write.batch.size` | 256（MB） | 64-2048 | 批缓冲区大小（MB），越大吞吐越高但延迟越高，见 `FlinkOptions.java:791` |
| `write.task.max.size` | 1024（MB） | 512-8192 | 单个写入任务最大内存（MB），超过阈值刷出最大 bucket，防止 OOM，见 `FlinkOptions.java:714` |

**调优建议**：
- 内存充足（16GB+）：`write.batch.size=1024`, `write.task.max.size=4096`
- 内存有限（4GB）：`write.batch.size=128`, `write.task.max.size=512`
- 低延迟：`write.batch.size=64`, `write.task.max.size=256`

---

### 并行度参数

| 参数 | 默认值 | 范围 | 说明 |
|------|--------|------|------|
| `write.tasks` | 无（取执行环境并行度） | 1-64 | 写入任务数，越多吞吐越高但资源占用越多，见 `FlinkOptions.java:707` |
| `write.index_bootstrap.tasks` | 无（同 write.tasks） | 1-32 | Bootstrap 任务数，一般为写入任务数的 1/2，见 `FlinkOptions.java:693` |
| `compaction.tasks` | 无（同 write.tasks） | 1-32 | Compaction 任务数，一般为写入任务数的 1/2，见 `FlinkOptions.java:929` |
| `index.write.tasks` | 无（同 write.tasks） | 1-32 | Index Bootstrap 阶段 index write 子任务并行度，见 `FlinkOptions.java:331` |

**调优建议**：
- 高吞吐：`write.tasks=CPU核数×2`, `bootstrap.tasks=CPU核数`, `compaction.tasks=CPU核数`
- 低资源：`write.tasks=4`, `bootstrap.tasks=2`, `compaction.tasks=2`
- 平衡：`write.tasks=CPU核数`, `bootstrap.tasks=CPU核数/2`, `compaction.tasks=CPU核数/2`

---

### Compaction 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `compaction.async.enabled` | true | 是否启用异步 Compaction（MOR 表默认开启），见 `FlinkOptions.java:922` |
| `compaction.trigger.strategy` | num_commits | Compaction 触发策略，取值：`num_commits` / `num_commits_after_last_request` / `time_elapsed` / `num_and_time` / `num_or_time`，见 `FlinkOptions.java:947` |
| `compaction.delta_commits` | 5 | 多少个 delta commit 后触发 Compaction，见 `FlinkOptions.java:959` |
| `compaction.delta_seconds` | 3600 | 多少秒后触发 Compaction（默认 1 小时），见 `FlinkOptions.java:966` |

**调优建议**：
- 生产环境：默认已启用 `compaction.async.enabled=true`
- 读多写少：`compaction.delta_commits=5`（频繁 Compaction）
- 写多读少：`compaction.delta_commits=20`（减少 Compaction）
- 实时性要求高：`compaction.trigger.strategy=time_elapsed`, `compaction.delta_seconds=300`

---

### 索引参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `index.type` | FLINK_STATE | 索引类型：FLINK_STATE（默认）/ BUCKET / BLOOM / GLOBAL_BLOOM / SIMPLE / GLOBAL_SIMPLE / RECORD_LEVEL_INDEX / GLOBAL_RECORD_LEVEL_INDEX / INMEMORY，见 `FlinkOptions.java:257`，枚举定义 `HoodieIndex.java:161` |
| `hoodie.index.bucket.engine` | SIMPLE | 桶引擎：SIMPLE / CONSISTENT_HASHING，见 `FlinkOptions.java:598`（Flink 端 key 与 `HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE` 对齐） |

**调优建议**：
- 高并发 Upsert：`index.type=BUCKET`
- 大表全局唯一场景：`index.type=RECORD_LEVEL_INDEX`
- 默认场景：`index.type=FLINK_STATE`（利用 Flink 状态后端）

---

### Checkpoint 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `execution.checkpointing.interval` | 60000ms | Checkpoint 间隔 |
| `execution.checkpointing.timeout` | 600000ms | Checkpoint 超时 |
| `execution.checkpointing.mode` | EXACTLY_ONCE | Checkpoint 模式 |

**调优建议**：
- 高吞吐：`interval=120000`, `timeout=1200000`
- 低延迟：`interval=30000`, `timeout=300000`
- 网络差：增加 `timeout` 到 `1200000`

---

## 常见问题快速解答

### Q: 吞吐量低怎么办？
**A**: 按优先级尝试：
1. 增加 `write.batch.size` 到 512
2. 增加 `write.tasks` 到 16
3. 增加 `execution.checkpointing.interval` 到 120000
4. 检查网络和磁盘 I/O

### Q: 延迟高怎么办？
**A**: 按优先级尝试：
1. 减少 `write.batch.size` 到 64
2. 减少 `execution.checkpointing.interval` 到 30000
3. 减少 `compaction.delta_commits` 到 5
4. 检查 Compaction 是否有积压

### Q: 内存溢出（OOM）怎么办？
**A**: 按优先级尝试：
1. 减少 `write.batch.size` 到 128
2. 减少 `write.task.max.size` 到 512
3. 减少 `write.tasks` 到 4
4. 增加 TaskManager 内存

### Q: 小文件太多怎么办？
**A**: 按优先级尝试：
1. 增加 `write.batch.size` 到 512
2. 减少 `compaction.delta_commits` 到 5
3. 增加 `execution.checkpointing.interval` 到 120000
4. 手动执行 Compaction

### Q: Checkpoint 超时怎么办？
**A**: 按优先级尝试：
1. 增加 `execution.checkpointing.timeout` 到 1200000
2. 减少 `write.batch.size` 到 128
3. 检查网络连接
4. 增加 Checkpoint 间隔

---

## 监控指标

### 关键指标

| 指标 | 目标值 | 过高时 | 过低时 |
|------|--------|--------|--------|
| 写入延迟 | < 1 分钟 | 减少缓冲区 | 增加缓冲区 |
| 吞吐量 | 业务需求 | 增加缓冲区 | 减少缓冲区 |
| 小文件数 | < 1000 | 增加缓冲区 | 减少缓冲区 |
| Checkpoint 时间 | < 30 秒 | 减少数据量 | 增加并行度 |
| 内存占用 | < 50% | 减少缓冲区 | 增加缓冲区 |

### 监控命令

```bash
# 查看 Flink 任务状态
flink list

# 查看任务详情
flink info <job_id>

# 查看 TaskManager 日志
tail -f logs/flink-*.log

# 查看 Hudi 表统计
hudi-cli
> show table stats
```

---

## 调优流程

### 第一步：确定优化目标
- [ ] 吞吐优先
- [ ] 延迟优先
- [ ] 平衡

### 第二步：选择基础配置
- [ ] 根据场景选择预设配置
- [ ] 记录初始配置

### 第三步：监控关键指标
- [ ] 写入延迟
- [ ] 吞吐量
- [ ] 小文件数
- [ ] 内存占用

### 第四步：逐步调整参数
- [ ] 一次只改一个参数
- [ ] 观察效果（至少 5 分钟）
- [ ] 记录结果
- [ ] 找到最优值

### 第五步：定期评估
- [ ] 每周检查一次
- [ ] 根据数据量变化调整
- [ ] 优化新的瓶颈

---

## 参考资源

- [Hudi 官方文档](https://hudi.apache.org/)
- [Flink 官方文档](https://flink.apache.org/)
- [Hudi GitHub](https://github.com/apache/hudi)
- [Flink GitHub](https://github.com/apache/flink)

---

**版本**: 1.0  
**最后更新**: 2026-04-13  
**维护者**: Hudi 社区
