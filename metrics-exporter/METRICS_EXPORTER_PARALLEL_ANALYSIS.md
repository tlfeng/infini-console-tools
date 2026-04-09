# Metrics Exporter 并行下载性能分析

## 现状概览

**当前默认配置：**
- `DEFAULT_PARALLEL_JOBS = 2` - 同时导出2个指标类型
- `DEFAULT_PARALLEL_DEGREE = 1` - 单个指标内部无并行
- `DEFAULT_BATCH_SIZE = 3000` - 每批次读取3000条
- `DEFAULT_SCROLL_KEEPALIVE = "5m"` - Scroll上下文保活5分钟

---

## 🔴 核心瓶颈分析

### 1. **单指标内部并行度严重不足 (最主要问题)**

**代码位置:** `metrics_exporter.py:1603-1607`

```python
metric_parallel_degree = (
    max(1, parallel_degree)
    if metric_type in {"node_stats", "index_stats", "shard_stats", "cluster_stats", "cluster_health"}
    else 1
)
```

**问题：**
- 默认 `parallel_degree=1`，意味着单个指标只用1个线程导出
- 即使指定了 `-–parallel-degree=4`，也只是增加了4个并发线程在单个指标内
- 而 **指标类型之间的并行度（parallel_jobs）默认只有2**

**影响：**
当导出 `node_stats` 时：
- 指标1（node_stats）: 1个线程（或 parallel_degree 个线程）
- 指标2（index_stats）: 1个线程（或 parallel_degree 个线程）
- 其他指标都在等待队列中

**实际吞吐量计算：**
```
如果有5个指标，parallel_jobs=2：
时间1-2: 导出指标1、2（每个可用 parallel_degree 线程）
时间3-4: 导出指标3、4
时间5:   导出指标5

总耗时 ≈ 2.5 * (单个指标导出时间)
```

---

### 2. **批次大小相对较小**

**代码位置:** `metrics_exporter.py:48-90`

```python
METRIC_TYPES = {
    "node_stats": {
        "default_batch_size": 3000,  # 中等批次
    },
    "shard_stats": {
        "default_batch_size": 2000,  # 最小批次
    },
}
```

**问题：**
- `shard_stats` 默认批次只有 2000，导致 ES 查询往返次数多
- 数据量大的场景下，频繁的小批次请求会成为瓶颈
- 增加 batch_size 可以减少网络往返

**建议：**
```
- cluster_health/cluster_stats: 5000-10000 (数据量小)
- node_stats: 5000-8000 (提高20-50%)
- index_stats: 5000-8000
- shard_stats: 3000-5000 (提高50-150%)
```

---

### 3. **Scroll 上下文时间可能不够**

**代码位置:** `metrics_exporter.py:116`

```python
DEFAULT_SCROLL_KEEPALIVE = "5m"  # 5分钟
```

**风险场景：**
- 数据量特别大（>100万条）时
- 批次size相对小（2000-3000）
- 导致需要很多次scroll请求
- 5分钟可能不够，会导致 scroll context 过期
- 过期时会触发恢复逻辑 (`search_after`)，额外消耗时间

---

### 4. **告警数据完全不支持并行**

**代码位置:** `metrics_exporter.py:1684-1693`

```python
def export_alert_type(...) -> ExportResult:
    count, file_paths = self.export_with_scroll(
        config["index_pattern"],
        query,
        output_file,
        effective_batch_size,
        shard_size,
        self._make_progress_callback(alert_type, progress_reporter),
        slim_config,
        mask_ip,
        # ⚠️ 注意：这里没有传递 parallel_degree 参数！
    )
```

**问题：**
- `export_alert_type()` 方法不接受 `parallel_degree` 参数
- 告警数据（alert_rules, alert_messages, alert_history）总是单线程导出
- 这些数据量通常也不小，白白浪费并行能力

---

## 📊 性能对比场景

### 场景1: 导出5个指标 + 3个告警数据（共8项）

**当前方案（parallel_jobs=2, parallel_degree=1）:**
```
Timeline:
T0-T1:   node_stats (1线程) + index_stats (1线程)         [并行2项]
T1-T2:   shard_stats (1线程) + cluster_stats (1线程)      [并行2项]
T2-T3:   cluster_health (1线程)                            [单线程1项]
T3-T4:   alert_messages (1线程) + alert_history (1线程)  [并行2项]
T4-T5:   alert_rules (1线程)                              [单线程1项]

总时间: ~5个时间单位
```

**优化方案1（parallel_jobs=4, parallel_degree=2）:**
```
Timeline:
T0-T1:   node_stats (2线程) + index_stats (2线程) + 
         shard_stats (2线程) + cluster_stats (2线程)     [并行4项]
T1-T2:   cluster_health (2线程) + alert_messages (2线程) +
         alert_history (2线程) + alert_rules (2线程)     [并行4项]

总时间: ~2个时间单位（提升 60%+）
```

---

## 🎯 优化建议

### 优先级 1: **启用更高的并行度（立竿见影）**

**方案A - 命令行模式：**
```bash
# 原始（慢）
python metrics_exporter.py -c http://localhost:9000 -u admin -p password

# 优化（快）- 增加指标类型并行度和单指标并行度
python metrics_exporter.py \
  -c http://localhost:9000 -u admin -p password \
  --parallel 4 \           # 同时导出4个指标类型
  --parallel-degree 3      # 每个指标内部3个并行线程 (sliced scroll)
```

**方案B - 配置文件模式：**
```json
{
  "metricsExporter": {
    "jobs": [
      {
        "name": "高性能导出",
        "execution": {
          "parallelMetrics": 4,        // 从2增加到4
          "parallelDegree": 3          // 从1增加到3
        }
      }
    ]
  }
}
```

**性能提升预期：** 2-3倍

---

### 优先级 2: **增加批次大小**

**配置文件方案：**
```json
{
  "metricsExporter": {
    "jobs": [
      {
        "execution": {
          "batchSize": 5000  // 从默认3000增加到5000
        }
      }
    ]
  }
}
```

**命令行方案：**
```bash
python metrics_exporter.py \
  -c http://localhost:9000 -u admin -p password \
  --batch-size 5000
```

**性能提升预期：** 10-20%（减少ES往返次数）

---

### 优先级 3: **优化 Scroll Keepalive**

**大数据量场景：**
```bash
python metrics_exporter.py \
  -c http://localhost:9000 -u admin -p password \
  --scroll-keepalive 10m    # 从5m增加到10m
```

**注意：** 需要确保 ES 集群支持，检查 `search.max_keep_alive` 设置

---

### 优先级 4: **支持告警数据并行导出（需要代码改动）**

关键修改点：

**修改`export_alert_type()` 方法签名：**
```python
def export_alert_type(
    self,
    # ... 其他参数 ...
    parallel_degree: int = 1,        # ← 新增这个参数
    progress_reporter: Optional[ConsoleProgressReporter] = None,
    # ...
) -> ExportResult:
```

**将 parallel_degree 传递给 export_with_scroll()：**
```python
count, file_paths = self.export_with_scroll(
    config["index_pattern"],
    query,
    output_file,
    effective_batch_size,
    shard_size,
    self._make_progress_callback(alert_type, progress_reporter),
    slim_config,
    mask_ip,
    parallel_degree,  # ← 传递此参数
)
```

**性能提升预期：** 1.5-2倍（针对告警数据类）

---

## 🔧 推荐配置方案

### 方案A: 保守但均衡（推荐首先尝试）
```bash
--parallel 4 --parallel-degree 2 --batch-size 5000 --scroll-keepalive 5m
```
- 可以安全地在多数环境使用
- 预期吞吐提升：**60-100%**
- 内存占用增长：**30-50%**

### 方案B: 激进（适合大数据量）
```bash
--parallel 8 --parallel-degree 4 --batch-size 8000 --scroll-keepalive 10m
```
- 需要充足的系统资源
- 预期吞吐提升：**150-250%**
- 风险：可能导致 ES 连接数过多或客户端内存溢出
- **建议同时监控 ES 负载**

### 方案C: 微调平衡
```bash
--parallel 6 --parallel-degree 3 --batch-size 5000 --scroll-keepalive 8m
```
- 介于保守和激进之间
- 预期吞吐提升：**100-150%**

---

## 📋 诊断清单

运行导出时，检查以下指标来判断是否达到最优：

```bash
# 1. 检查导出的吞吐量（条/秒）
# 从日志输出计算: count / duration_ms * 1000

# 2. 监控 ES 集群状态
# 确保没有热点 node 或过高的 CPU/内存

# 3. 观察客户端资源使用
# 并行度过高会导致内存溢出或连接数超限

# 4. 查看实际使用的线程数
# 如果 parallel_degree > ES 分片数，会造成浪费
```

---

## 📝 配置文件完整示例

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
        "name": "高性能导出-7天",
        "enabled": true,
        "metrics": [
          "cluster_health",
          "cluster_stats", 
          "node_stats",
          "index_stats",
          "shard_stats"
        ],
        "alertTypes": [
          "alert_rules",
          "alert_messages",
          "alert_history"
        ],
        "timeRangeHours": 168,
        "shardSize": 100000,
        "output": {
          "directory": "./metrics_export"
        },
        "execution": {
          "parallelMetrics": 4,      // ← 增加到4
          "parallelDegree": 3,       // ← 增加到3
          "batchSize": 5000,         // ← 增加到5000
          "scrollKeepalive": "10m"   // ← 增加到10m
        }
      }
    ]
  }
}
```

---

## ⚡ 快速测试命令

```bash
# 导出最近1小时数据，启用高并行度，观察速度
python metrics_exporter.py \
  -c http://localhost:9000 \
  -u admin \
  -p password \
  --time-range 1 \
  --parallel 4 \
  --parallel-degree 3 \
  --batch-size 5000 \
  --metric-types node_stats,index_stats \
  -o ./test_export
```

---

## 📈 预期收益总结

| 优化项 | 配置改动 | 吞吐提升 | 难度 | 优先级 |
|------|--------|--------|------|------|
| 增加 parallel_jobs | 2→4 | **+40%** | 低 | ⭐⭐⭐ |
| 增加 parallel_degree | 1→3 | **+50%** | 低 | ⭐⭐⭐ |
| 增加 batch_size | 3000→5000 | **+15%** | 低 | ⭐⭐ |
| 延长 scroll_keepalive | 5m→10m | **+5-10%** | 低 | ⭐⭐ |
| 支持告警多线程 | 代码改动 | **+25%** | 中 | ⭐⭐ |
| **总体综合** | 全启用 | **+150-200%** | 中 | ⭐⭐⭐ |

