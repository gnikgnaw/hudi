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
| `hoodie.write.batch.size` | 256MB | 64MB-2GB | 缓冲区大小，越大吞吐越高但延迟越高 |
| `hoodie.write.buffer.limit.bytes` | 1GB | 512MB-8GB | 缓冲区最大限制，防止 OOM |
| `hoodie.write.buffer.type` | MEMORY | MEMORY/SPILLABLE_DISK | 缓冲区类型，SPILLABLE_DISK 可溢出到磁盘 |

**调优建议**：
- 内存充足（16GB+）：`batch.size=1GB`, `limit=4GB`
- 内存有限（4GB）：`batch.size=128MB`, `limit=512MB`, `type=SPILLABLE_DISK`
- 低延迟：`batch.size=64MB`, `limit=256MB`

---

### 并行度参数

| 参数 | 默认值 | 范围 | 说明 |
|------|--------|------|------|
| `hoodie.flink.write.tasks` | 4 | 1-64 | 写入任务数，越多吞吐越高但资源占用越多 |
| `hoodie.index.bootstrap.tasks` | 4 | 1-32 | Bootstrap 任务数，一般为写入任务数的 1/2 |
| `hoodie.flink.compaction.tasks` | 4 | 1-32 | Compaction 任务数，一般为写入任务数的 1/2 |

**调优建议**：
- 高吞吐：`write.tasks=CPU核数×2`, `bootstrap.tasks=CPU核数`, `compaction.tasks=CPU核数`
- 低资源：`write.tasks=4`, `bootstrap.tasks=2`, `compaction.tasks=2`
- 平衡：`write.tasks=CPU核数`, `bootstrap.tasks=CPU核数/2`, `compaction.tasks=CPU核数/2`

---

### Compaction 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `hoodie.compaction.async.enabled` | false | 是否启用异步 Compaction |
| `hoodie.compaction.strategy` | num_commits | Compaction 触发策略 |
| `hoodie.compaction.num_commits` | 10 | 多少个 Commit 后触发 Compaction |
| `hoodie.compaction.time_elapsed.minutes` | 60 | 多少分钟后触发 Compaction |

**调优建议**：
- 生产环境：必须启用 `async.enabled=true`
- 读多写少：`num_commits=5`（频繁 Compaction）
- 写多读少：`num_commits=20`（减少 Compaction）
- 实时性要求高：`strategy=time_elapsed`, `time_elapsed.minutes=5`

---

### 索引参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `hoodie.index.streaming.write.enabled` | false | 是否启用流式索引写入 |
| `hoodie.index.type` | BLOOM | 索引类型：BLOOM/BUCKET/RECORD_INDEX |
| `hoodie.index.bucket.engine` | SIMPLE | 桶引擎：SIMPLE/CONSISTENT_HASHING |

**调优建议**：
- 高并发 Upsert：`streaming.write.enabled=true`, `type=BUCKET`
- 大表场景：`streaming.write.enabled=false`, `type=BLOOM`
- 精确查询：`type=RECORD_INDEX`

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
1. 增加 `hoodie.write.batch.size` 到 512MB
2. 增加 `hoodie.flink.write.tasks` 到 16
3. 增加 `execution.checkpointing.interval` 到 120000
4. 检查网络和磁盘 I/O

### Q: 延迟高怎么办？
**A**: 按优先级尝试：
1. 减少 `hoodie.write.batch.size` 到 64MB
2. 减少 `execution.checkpointing.interval` 到 30000
3. 增加 `hoodie.compaction.num_commits` 到 5
4. 启用 `hoodie.index.streaming.write.enabled=true`

### Q: 内存溢出（OOM）怎么办？
**A**: 按优先级尝试：
1. 减少 `hoodie.write.batch.size` 到 128MB
2. 减少 `hoodie.write.buffer.limit.bytes` 到 512MB
3. 启用 `hoodie.write.buffer.type=SPILLABLE_DISK`
4. 减少 `hoodie.flink.write.tasks` 到 4

### Q: 小文件太多怎么办？
**A**: 按优先级尝试：
1. 增加 `hoodie.write.batch.size` 到 512MB
2. 减少 `hoodie.compaction.num_commits` 到 5
3. 增加 `execution.checkpointing.interval` 到 120000
4. 手动执行 Compaction

### Q: Checkpoint 超时怎么办？
**A**: 按优先级尝试：
1. 增加 `execution.checkpointing.timeout` 到 1200000
2. 减少 `hoodie.write.batch.size` 到 128MB
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
