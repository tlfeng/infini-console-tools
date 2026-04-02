# INFINI Console 监控数据导出说明

本文档介绍从 INFINI Console 系统集群导出的监控数据类别、字段结构、配置方法和性能优化建议。

## 数据概述

INFINI Console 通过系统集群（`.infini_*` 索引）存储所有被监控 ES 集群的指标数据。导出的数据可用于：

- 集群健康状态分析
- 性能瓶颈诊断
- 容量规划
- 异常检测
- 配置优化建议

---

## 一、快速开始

### 基本使用

```bash
# 导出最近24小时的监控数据
python metrics_exporter.py -c http://localhost:9000 -u admin -p password

# 导出最近7天的数据，并行度4
python metrics_exporter.py -c http://localhost:9000 -u admin -p password --time-range 168 --parallel 4

# 使用配置文件
python metrics_exporter.py --config config.json --job "全量导出-一周"
```

### 配置文件驱动

使用 `config.json` 定义导出任务：

```json
{
  "consoleUrl": "http://localhost:9000",
  "auth": {
    "username": "admin",
    "password": "password"
  },
  "metricsExporter": {
    "jobs": [
      {
        "name": "全量导出-一周",
        "enabled": true,
        "metrics": ["cluster_health", "cluster_stats", "node_stats", "index_stats", "shard_stats"],
        "sampling": { "mode": "full" },
        "output": { "directory": "./metrics_export_full" },
        "execution": {
          "parallelMetrics": 3,
          "scrollKeepalive": "5m"
        },
        "timeRangeHours": 168,
        "maxDocs": 1000000
      }
    ]
  }
}
```

---

## 二、性能优化建议

### 1. 批次大小优化

默认批次大小根据指标类型自适应：

| 指标类型 | 默认批次 | 说明 |
|---------|---------|------|
| cluster_health | 5000 | 数据量小，大批次 |
| cluster_stats | 5000 | 数据量小，大批次 |
| node_stats | 3000 | 数据量大，中批次 |
| index_stats | 3000 | 数据量大，中批次 |
| shard_stats | 2000 | 数据量最大，小批次 |

可通过 `--batch-size` 或配置文件覆盖。

### 2. 并行导出

使用 `--parallel` 参数或 `execution.parallelMetrics` 配置并行导出多个指标类型：

```bash
# 并行导出4个指标类型
python metrics_exporter.py --parallel 4
```

**建议**：
- 2-4 核机器：parallel=2
- 4-8 核机器：parallel=3-4
- 8+ 核机器：parallel=4-6

### 3. 字段筛选

只导出需要的字段，减少数据传输量：

```bash
# 只导出关键字段
python metrics_exporter.py --fields timestamp,metadata.labels.cluster_id,payload.elasticsearch.node_stats.os
```

或在配置中指定：

```json
{
  "sourceFields": [
    "timestamp",
    "metadata.labels.cluster_id",
    "payload.elasticsearch.node_stats.jvm"
  ]
}
```

### 4. 抽样导出

对于长时间范围的数据，可使用抽样模式减少数据量：

```json
{
  "sampling": {
    "mode": "sampling",
    "interval": "1h"
  }
}
```

**重要：抽样在 ES 端完成**

本工具的所有抽样操作均在 Elasticsearch 服务端执行，而非在本地客户端进行。这意味着：

- **减少网络传输**：只有被抽中的数据才会通过网络传输到客户端
- **降低带宽占用**：尤其适用于大规模数据集的远程导出
- **缩短导出时间**：ES 端预过滤后再返回结果，避免全量数据拉取
- **降低内存占用**：客户端无需缓存和处理全部原始数据

**抽样实现原理：**

- **`interval` 抽样**：通过 ES 的 `composite` 聚合按时间字段分桶，每桶取一条文档，在服务端完成时间维度的均匀采样

抽样模式：
- `interval`: 按时间间隔抽样（如 "1h", "5m", "30s"），每个时间桶保留最新一条记录

### 5. 精简数据

使用 `--slim` 或配置 `slim` 选项删除不必要的字段，减少数据量：

```bash
# 命令行启用精简
python metrics_exporter.py --slim
```

或在配置中启用：

```json
{
  "slim": true
}
```

或更细粒度控制：

```json
{
  "slim": {
    "enabled": true,
    "removeMeta": true,
    "removeHumanReadable": true
  }
}
```

**精简删除的字段：**

| 类别 | 删除的字段 | 说明 |
|------|-----------|------|
| 排障无关 | `_id`, `agent`, `metadata.category`, `metadata.datatype`, `metadata.name` | 对集群排障无价值 |
| 冗余格式 | `store`, `estimated_size`, `limit_size` | 有对应的 `*_in_bytes` 字段可用 |

**预期效果：**
- 数据量减少 5-15%
- 保留所有对排障有用的字段

### 5. Scroll 参数调优

- `scrollKeepalive`: 默认 5m，适合长时间和大批量导出
- 小数据量或低延迟网络可按需缩短，但出现 scroll 404 时应优先延长 keepalive
- 当前批次大小已按指标类型分层，自定义调优时建议先降低并行度，再调整批次

### 6. 预估性能提升

| 优化项 | 预期提升 |
|-------|---------|
| 并行导出 (parallel=4) | 2-3x |
| 紧凑 JSON 输出 | 30-50% IO 减少 |
| 批次优化 (2000→5000) | 20-30% |
| 字段筛选 (50%字段) | 40-50% 数据量减少 |
| 抽样 (1h间隔) | 90%+ 数据量减少 |

---

## 三、配置格式说明

### Job 配置结构

```json
{
  "name": "任务名称",
  "enabled": true,
  "metrics": ["node_stats", "index_stats"],
  "targets": {
    "clusters": {
      "include": ["cluster-id-1", "*-prod-*"],
      "exclude": ["test-*"]
    }
  },
  "sampling": {
    "mode": "full"
  },
  "slim": true,
  "output": {
    "directory": "./output",
    "splitBy": "metric_type",
    "filenamePrefix": "",
    "compress": false
  },
  "execution": {
    "parallelMetrics": 2,
    "batchSize": null,
    "scrollKeepalive": "5m",
    "maxRetries": 3,
    "retryDelay": 5
  },
  "timeRangeHours": 168,
  "maxDocs": 1000000,
  "sourceFields": null,
  "includeAlerts": true,
  "alertTypes": ["alert_rules", "alert_messages"]
}
```

### 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `name` | string | 任务名称（唯一） |
| `enabled` | bool | 是否启用 |
| `metrics` | array | 要导出的指标类型 |
| `targets.clusters` | object | 集群筛选规则 |
| `sampling.mode` | string | full 或 sampling |
| `sampling.interval` | string | 抽样间隔（如 "1h"），sampling 模式必填 |
| `slim` | bool/object | 精简数据配置，删除不必要的字段 |
| `output.directory` | string | 输出目录 |
| `output.splitBy` | string | 分割方式：metric_type/cluster/none |
| `execution.parallelMetrics` | int | 并行导出数 |
| `execution.batchSize` | int | 批次大小（null=自适应） |
| `timeRangeHours` | int | 时间范围（小时） |
| `maxDocs` | int | 每类型最大文档数 |
| `sourceFields` | array | 要导出的字段列表 |
| `includeAlerts` | bool | 是否导出告警数据 |

---

## 四、监控指标数据

监控指标存储在 `.infini_metrics` 索引中，按 `metadata.name` 字段区分类型。

### 1. cluster_health（集群健康指标）

**用途**：监控集群整体健康状态

**关键字段**：
```
metadata.labels.cluster_id      - 集群ID
metadata.labels.cluster_name    - 集群名称
metadata.labels.cluster_uuid    - 集群UUID
timestamp                       - 采集时间

payload.elasticsearch.cluster_health:
  status                        - 集群状态 (green/yellow/red)
  number_of_nodes               - 节点总数
  number_of_data_nodes          - 数据节点数
  active_primary_shards         - 主分片数
  active_shards                 - 活跃分片总数
  relocating_shards             - 迁移中分片数
  initializing_shards           - 初始化分片数
  unassigned_shards             - 未分配分片数
  delayed_unassigned_shards     - 延迟未分配分片数
  number_of_pending_tasks       - 待处理任务数
  number_of_in_flight_fetch     - 进行中的fetch数
  active_shards_percent_as_number - 活跃分片百分比
```

**分析要点**：
- `status=red` 表示有主分片未分配，数据可能丢失
- `status=yellow` 表示有副本分片未分配
- `unassigned_shards > 0` 需要检查原因
- `relocating_shards` 持续不为0可能表示集群不稳定

---

### 2. cluster_stats（集群统计指标）

**用途**：集群级别的资源使用和统计信息

**关键字段**：
```
metadata.labels.cluster_id      - 集群ID
metadata.labels.cluster_name    - 集群名称
timestamp                       - 采集时间

payload.elasticsearch.cluster_stats:
  indices.count                 - 索引总数
  indices.docs.count            - 文档总数
  indices.docs.deleted          - 已删除文档数
  indices.shards.total          - 分片总数
  indices.store.size_in_bytes   - 存储大小

  nodes.count.total             - 节点总数
  nodes.count.data              - 数据节点数
  nodes.count.master            - 主节点候选数
  nodes.fs.available_in_bytes   - 可用磁盘空间

  nodes.jvm.mem.heap_used_in_bytes     - JVM堆内存使用
  nodes.jvm.mem.heap_max_in_bytes      - JVM堆内存最大值
  nodes.jvm.max_uptime_in_millis       - 最大运行时长
```

**分析要点**：
- `indices.shards.total` 过大可能影响性能（建议单节点<20个分片/GB堆内存）
- `nodes.fs.available_in_bytes` 磁盘空间不足会导致分片分配失败
- JVM堆内存使用率 > 75% 需要关注

---

### 3. node_stats（节点统计指标）

**用途**：节点级别的详细性能指标，是最重要的诊断数据

**关键字段**：
```
metadata.labels.cluster_id      - 集群ID
metadata.labels.node_id         - 节点ID
metadata.labels.node_name       - 节点名称
metadata.labels.transport_address - 传输地址
timestamp                       - 采集时间

payload.elasticsearch.node_stats:

  === 系统资源 ===
  os.cpu.percent                - OS CPU使用率
  os.cpu.load_average.1m/5m/15m - 系统负载
  os.mem.used_percent           - 系统内存使用率
  os.swap.used_in_bytes         - Swap使用量

  === 进程资源 ===
  process.cpu.percent           - ES进程CPU使用率
  process.open_file_descriptors - 打开的文件描述符数
  process.max_file_descriptors  - 最大文件描述符数

  === JVM ===
  jvm.mem.heap_used_percent     - 堆内存使用率
  jvm.mem.heap_used_in_bytes    - 堆内存使用量
  jvm.mem.heap_max_in_bytes     - 堆内存最大值
  jvm.mem.pools.young.used_in_bytes   - 年轻代使用量
  jvm.mem.pools.old.used_in_bytes     - 老年代使用量
  jvm.gc.collectors.young.collection_count  - Young GC次数
  jvm.gc.collectors.young.collection_time_in_millis - Young GC时间
  jvm.gc.collectors.old.collection_count    - Old GC次数
  jvm.gc.collectors.old.collection_time_in_millis  - Old GC时间

  === 索引操作 ===
  indices.indexing.index_total           - 索引操作总数
  indices.indexing.index_time_in_millis  - 索引操作总耗时
  indices.indexing.index_current         - 当前索引操作数
  indices.indexing.delete_total          - 删除操作总数

  === 搜索性能 ===
  indices.search.query_total             - 查询总数
  indices.search.query_time_in_millis    - 查询总耗时
  indices.search.fetch_total             - Fetch总数
  indices.search.fetch_time_in_millis    - Fetch总耗时
  indices.search.scroll_total            - Scroll总数
  indices.search.open_contexts           - 打开的搜索上下文数

  === 缓存 ===
  indices.query_cache.memory_size_in_bytes   - 查询缓存内存
  indices.query_cache.hit_count              - 查询缓存命中数
  indices.query_cache.miss_count             - 查询缓存未命中数
  indices.request_cache.memory_size_in_bytes - 请求缓存内存
  indices.fielddata.memory_size_in_bytes     - Fielddata内存

  === 存储 ===
  indices.store.size_in_bytes        - 存储大小
  indices.docs.count                 - 文档数
  indices.docs.deleted               - 已删除文档数
  indices.segments.count             - Segment数量
  indices.segments.memory_in_bytes   - Segment内存

  === 索引刷新与合并 ===
  indices.refresh.total              - Refresh次数
  indices.refresh.total_time_in_millis - Refresh耗时
  indices.flush.total                - Flush次数
  indices.flush.total_time_in_millis - Flush耗时
  indices.merges.total               - Merge次数
  indices.merges.total_time_in_millis - Merge耗时
  indices.merges.total_size_in_bytes - Merge数据量

  === 网络 ===
  transport.tx_count                 - 发送次数
  transport.rx_count                 - 接收次数
  transport.tx_size_in_bytes         - 发送字节数
  transport.rx_size_in_bytes         - 接收字节数
  http.current_open                  - 当前HTTP连接数

  === 磁盘IO ===
  fs.io_stats.total.operations       - IO操作总数
  fs.io_stats.total.read_operations  - 读操作数
  fs.io_stats.total.write_operations - 写操作数
  fs.total.total_in_bytes            - 磁盘总大小
  fs.total.available_in_bytes        - 磁盘可用大小

  === 熔断器 ===
  breakers.parent.tripped            - Parent Breaker触发次数
  breakers.fielddata.tripped         - Fielddata Breaker触发次数
  breakers.request.tripped           - Request Breaker触发次数
  breakers.in_flight_requests.tripped - In-flight Requests Breaker触发次数
```

**分析要点**：

| 指标 | 正常范围 | 异常处理 |
|------|----------|----------|
| JVM堆使用率 | < 75% | >85% 需要扩容或优化 |
| Old GC频率 | < 1次/分钟 | 频繁Full GC需排查内存泄漏 |
| CPU使用率 | < 70% | 持续高CPU需检查查询或索引 |
| 文件描述符 | < 80% | 接近上限需调整ulimit |
| Swap使用 | 0 | Swap非0会严重影响性能 |
| 搜索耗时/次数 | - | 计算平均延迟，>100ms需优化 |
| 熔断器触发 | 0 | 触发表示内存压力过大 |

---

### 4. index_stats（索引统计指标）

**用途**：索引级别的性能分析

**关键字段**：
```
metadata.labels.cluster_id      - 集群ID
metadata.labels.index_name      - 索引名称
timestamp                       - 采集时间

payload.elasticsearch.index_stats:
  total.search.query_total             - 查询总数
  total.search.query_time_in_millis    - 查询总耗时
  total.indexing.index_total           - 索引操作总数
  total.indexing.index_time_in_millis  - 索引操作总耗时
  total.docs.count                     - 文档数
  total.docs.deleted                   - 已删除文档数
  total.store.size_in_bytes            - 存储大小
  total.segments.count                 - Segment数量
  total.segments.memory_in_bytes       - Segment内存
```

**分析要点**：
- 查询平均延迟 = query_time_in_millis / query_total
- 索引平均延迟 = index_time_in_millis / index_total
- docs.deleted 过多需要考虑 force merge 或 reindex

---

### 5. shard_stats（分片统计指标）

**用途**：分片级别的详细分析

**关键字段**：
```
metadata.labels.cluster_id      - 集群ID
metadata.labels.index_name      - 索引名称
metadata.labels.shard_id        - 分片ID
metadata.labels.node_id         - 所在节点ID
timestamp                       - 采集时间

payload.elasticsearch.shard_stats:
  routing.primary                - 是否主分片
  store.size_in_bytes            - 存储大小
  docs.count                     - 文档数
  docs.deleted                   - 已删除文档数

  indexing.index_total           - 索引操作总数
  indexing.index_time_in_millis  - 索引耗时
  search.query_total             - 查询总数
  search.query_time_in_millis    - 查询耗时
  search.scroll_total            - Scroll总数

  refresh.total                  - Refresh次数
  flush.total                    - Flush次数
  merges.total                   - Merge次数
  segments.count                 - Segment数量
```

**分析要点**：
- 分片大小不均衡可能导致热点问题
- 单个分片建议 < 50GB
- 分片文档数过多会影响查询性能

---

## 五、告警数据

### 1. alert_rules（告警规则）

**用途**：配置的告警规则定义

**关键字段**：
```
id                    - 规则ID
name                  - 规则名称
enabled               - 是否启用
resource.type         - 资源类型
resource.id           - 资源ID
metrics.field         - 监控指标字段
metrics.function      - 聚合函数
conditions.expression - 触发条件表达式
schedule.interval     - 检查间隔
```

### 2. alert_messages（告警消息）

**用途**：告警触发产生的消息

**关键字段**：
```
id              - 消息ID
rule_id         - 关联的规则ID
resource_id     - 资源ID
resource_name   - 资源名称
title           - 告警标题
message         - 告警详情
status          - 状态 (alerting/ignored/recovered)
priority        - 优先级
created         - 创建时间
updated         - 更新时间
```

### 3. alert_history（告警历史）

**用途**：告警状态变更历史

**关键字段**：
```
rule_id         - 规则ID
resource_id     - 资源ID
status          - 状态变更
timestamp       - 时间戳
```

---

## 六、CLI 参数参考

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-c, --console` | Console URL | http://localhost:9000 |
| `-u, --username` | 用户名 | - |
| `-p, --password` | 密码 | - |
| `--config` | 配置文件路径 | - |
| `--job` | 指定执行的 job 名称 | - |
| `--list-jobs` | 列出所有 jobs | - |
| `--time-range` | 时间范围(小时) | 24 |
| `--max-docs` | 每类型最大文档数 | 100000 |
| `--batch-size` | 批次大小 | 自适应 |
| `--scroll-keepalive` | Scroll 保持时间 | 5m |
| `--parallel` | 并行度 | 2 |
| `--cluster-id` | 集群ID过滤 | - |
| `--metric-types` | 指标类型列表 | 全部 |
| `--fields` | 字段列表 | 全部 |
| `--slim` | 精简数据，删除不必要的字段 | false |
| `--no-alerts` | 不导出告警 | false |
| `--list-clusters` | 列出集群 | false |

---

## 七、注意事项

1. **数据量控制**
   - 默认每种类型最多导出 100000 条记录
   - 可通过 `--max-docs` 参数调整
   - 建议先用 `--list-clusters` 查看数据规模

2. **时间范围**
   - 默认导出最近 24 小时数据
   - 可通过 `--time-range` 调整（单位：小时）
   - 注意数据量随时间范围增加

3. **敏感信息**
   - 导出数据可能包含集群配置信息
   - 分享数据时请注意脱敏处理

4. **数据时效性**
   - 监控数据按 10 秒间隔采集
   - 分析时注意时间戳

5. **抽样限制**
   - 抽样模式适用于趋势分析
   - 不适用于精确计数或异常检测
   - 时间间隔抽样保留时间序列特征
