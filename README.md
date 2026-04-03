# INFINI Console Tools

INFINI Console 配套工具集，提供集群管理、索引采样、查询分析等功能。

## 工具列表

| 工具 | 功能 | 路径 |
|------|------|------|
| **index-sampler** | 索引采样工具 - 提取索引 Mapping 和样本文档 | `index-sampler/` |
| **metrics-exporter** | 监控数据导出工具 - 导出集群监控指标供离线分析 | `metrics-exporter/` |
| **cluster-report** | 集群报告工具 - 收集集群基本信息和统计 | `cluster-report/` |
| **query-report** | 查询报告工具 - 执行查询并生成 Markdown 报告 | `query-report/` |
| **test-runner** | 测试运行工具 - 查询性能测试和对比 | `test-runner/` |

## 快速开始

所有工具均使用 Python 3.6+ 编写，仅需标准库（除 test-runner 和 query-report 外）。

### 1. 克隆仓库

```bash
git clone <repository-url>
cd infini-console-tools
```

### 2. 安装依赖（可选）

```bash
# 基础工具（index-sampler, cluster-report）无需额外依赖
# query-report 和 test-runner 需要安装依赖
pip install -r requirements.txt
```

### 3. 使用工具

```bash
# 索引采样
python index-sampler/index_sampler.py -c http://localhost:9000 -u admin -p password

# 集群报告
python cluster-report/cluster_report.py -c http://localhost:9000 -u admin -p password

# 监控数据导出
python metrics-exporter/metrics_exporter.py -c http://localhost:9000 -u admin -p password

# 查询报告
python query-report/es_query_report.py -c http://localhost:9000 -u admin -p password \
  --cluster-id xxx -i queries.txt -o report.md

# 测试运行
python test-runner/es_test_runner.py config.json
```

## 统一配置方式

所有工具都支持**三种**统一的配置方式（优先级从高到低）：

### 1. 命令行参数

```bash
# 通用参数
-c, --console      Console URL (默认: http://localhost:9000)
-u, --username     登录用户名
-p, --password     登录密码
--timeout          请求超时时间(秒) (默认: 60)
--insecure         忽略 SSL 证书验证
--config           配置文件路径 (JSON 格式)
-o, --output       输出文件或目录路径
```

### 2. 环境变量

```bash
export CONSOLE_URL=http://localhost:9000
export CONSOLE_USERNAME=admin
export CONSOLE_PASSWORD=password
export CONSOLE_TIMEOUT=60
# query-report 特有
export CONSOLE_CLUSTER_ID=xxx
export CONSOLE_CLUSTER_NAME=my-cluster
```

### 3. 配置文件 (JSON)

创建 `config.json` 文件：

```json
{
  "consoleUrl": "http://localhost:9000",
  "auth": {
    "username": "admin",
    "password": "password"
  },
  "timeout": 60,
  "insecure": false,
  "output": "./output",
  "includeConsoleCluster": false
}
```

使用配置文件：

```bash
python index-sampler/index_sampler.py --config config.json
python cluster-report/cluster_report.py --config config.json
python query-report/es_query_report.py --config config.json -i queries.txt
```

## 公共模块

`common/` 目录包含所有工具共享的代码：

- `console_client.py` - Console API 客户端封装
  - JWT 认证
  - 集群列表获取
  - _proxy API 调用
  - 索引信息查询
  - 工具方法（字节格式化、时长格式化等）
- `config.py` - 统一配置管理
  - 配置文件加载
  - 环境变量支持
  - 参数解析

## 工具详情

### Index Sampler (索引采样工具)

从 INFINI Console 管理的所有 Elasticsearch 集群中采样索引信息。

**功能：**
- 自动发现所有管理的集群（排除 Console 系统集群）
- 获取所有非系统索引（排除 `.` 开头的索引）
- 检查集群健康状态，跳过 unavailable 集群
- 提取每个索引的 Mapping 结构
- 提取每个索引的样本文档（默认 2 条）
- 导出 JSON 和 CSV 格式的总结报告
- 为每个索引生成详细的 JSON 文件

**参数：**
```bash
-s, --sample-size        每个索引采样的文档数量 (默认: 2)
-m, --max-indices        每个集群最大采样索引数 (0=无限制)
--include-system-indices 包含系统索引(以 . 开头)
--include-console-cluster  包含 INFINI Console 系统集群
```

**输出文件：**
- `index_sampling_report.json` - 完整 JSON 报告
- `index_sampling_report.csv` - CSV 汇总表
- `details/` - 每个索引的详细 JSON 文件

### Cluster Report (集群报告工具)

收集 INFINI Console 中所有 Elasticsearch 集群的基本信息。

**功能：**
- 集群名称、版本、健康状态
- 在线时长、可用性、监控状态
- 节点数、索引数、分片数
- 文档总数、存储空间、JVM 内存

**参数：**
```bash
--summary-only           仅显示汇总统计，不生成 CSV 文件
--include-console-cluster 包含 INFINI Console 系统集群
```

**输出文件：**
- `cluster_report_YYYYMMDD_HHMMSS.csv` - 详细报告

### Metrics Exporter (监控数据导出工具)

从 INFINI Console 系统集群导出 ES 监控指标数据，供离线分析使用。

**功能：**
- 导出集群健康、节点统计、索引统计等监控指标
- 导出告警规则、告警消息、告警历史
- 支持指定时间范围和集群过滤
- **流式处理**：使用 Scroll API 分批读取，每批直接写入文件，避免内存溢出
- **并行导出**：支持多指标类型并行导出，显著提升效率
- **单指标并行**：支持 `--parallel-degree` 在单个大体量指标内并行导出（full/sampling 均可）
- **紧凑输出**：紧凑 JSON 格式，减少 IO 开销
- **字段筛选**：支持只导出指定字段
- **抽样模式**：支持时间间隔抽样和比例抽样
- **数据脱敏**：支持 IP 地址脱敏保护隐私（隐藏前两个 octet）
- **Job 配置**：支持配置文件定义多个导出任务
- **实时进度显示**：线程安全的任务级进度输出，避免并发日志互相覆盖

**参数：**
```bash
--time-range HOURS       导出时间范围(小时)，默认24
--max-docs N             每种类型最大文档数，默认100000（0=无限制）
--batch-size N           每批次读取的文档数，默认自适应 (2000-5000)
--scroll-keepalive TIME  Scroll 上下文保持时间，默认5m
--parallel N             并行导出的指标类型数，默认2
--parallel-degree N      单个指标内并行度(sliced scroll)，默认1
--cluster-id ID          只导出指定集群的数据
--metric-types TYPES     指定导出的指标类型，逗号分隔
--fields FIELDS          只导出指定字段，逗号分隔
--no-alerts              不导出告警数据
--list-clusters          只列出有监控数据的集群
--config FILE            配置文件路径
--job NAME               指定执行的 job 名称
--list-jobs              列出配置文件中的所有 jobs
--slim                   精简数据：删除不必要的字段以减少数据量
--mask-ip                脱敏IP地址：隐藏前两个octet，如 192.168.1.1 变为 *.*.1.1
```

**使用示例：**
```bash
# 导出最近24小时数据
python metrics-exporter/metrics_exporter.py -c http://localhost:9000 -u admin -p password

# 导出最近7天数据，并行度4
python metrics-exporter/metrics_exporter.py -c http://localhost:9000 -u admin -p password \
  --time-range 168 --parallel 4

# 只导出关键字段
python metrics-exporter/metrics_exporter.py -c http://localhost:9000 -u admin -p password \
  --fields timestamp,metadata.labels.cluster_id,payload.elasticsearch.node_stats.jvm

# 导出数据并脱敏IP地址（用于数据对外分享）
python metrics-exporter/metrics_exporter.py -c http://localhost:9000 -u admin -p password \
  --mask-ip --slim

# 使用配置文件执行 job
python metrics-exporter/metrics_exporter.py --config config.json --job "全量导出-一周"

# 列出可用的 jobs
python metrics-exporter/metrics_exporter.py --config config.json --list-jobs
```

**性能优化建议：**
- 使用 `--parallel 4` 并行导出，预期提升 2-3x
- 对 `node_stats`/`index_stats` 建议配合 `--parallel-degree 2~4`，提升单指标吞吐
- 使用 `--fields` 筛选关键字段，减少数据传输
- 使用抽样模式减少长时间范围的数据量
- 适当增大批次大小 (`--batch-size 5000`) 减少请求次数

**配置文件示例 (config.json)：**
```json
{
  "consoleUrl": "http://localhost:9000",
  "auth": { "username": "admin", "password": "password" },
  "metricsExporter": {
    "jobs": [
      {
        "name": "全量导出-一周",
        "enabled": true,
        "metrics": ["node_stats", "index_stats"],
        "sampling": { "mode": "full" },
        "output": { "directory": "./metrics_export_full" },
        "execution": { "parallelMetrics": 3, "parallelDegree": 2, "scrollKeepalive": "5m" },
        "timeRangeHours": 168,
        "maxDocs": 1000000
      },
      {
        "name": "抽样导出-一周",
        "enabled": false,
        "metrics": ["node_stats", "index_stats"],
        "sampling": { "mode": "sampling", "interval": "1h" },
        "output": { "directory": "./metrics_export_sampled" },
        "timeRangeHours": 168
      }
    ]
  }
}
```

**输出文件：**
- `cluster_health.json` - 集群健康指标
- `cluster_stats.json` - 集群统计指标
- `node_stats.json` - 节点统计指标
- `index_stats.json` - 索引统计指标
- `shard_stats.json` - 分片统计指标
- `alert_rules.json` - 告警规则
- `alert_messages.json` - 告警消息
- `alert_history.json` - 告警历史
- `export_summary.json` - 导出摘要（包含耗时统计）

**详细文档：** 参见 [metrics-exporter/DATA_GUIDE.md](metrics-exporter/DATA_GUIDE.md)

### Query Report (查询报告工具)

根据 Kibana 风格请求文本，自动执行查询并输出 Markdown 报告。

**功能：**
- 查询请求（多行 + 单行 body）
- 查询完整响应
- 索引概览（支持 alias 解析到真实索引）
- 索引 mappings
- 索引 settings（扁平化为 `index.xxx` 键）

**参数：**
```bash
-i, --input INPUT        输入文件路径（Kibana 格式查询）
--cluster-id CLUSTER_ID  目标集群 ID
--cluster-name CLUSTER_NAME  目标集群名称（可替代 cluster-id）
```

**输出文件：**
- `query-report.md` - Markdown 格式报告

### Test Runner (测试运行工具)

自动化 Elasticsearch 查询性能测试工具。

**功能：**
- 配置驱动：JSON 配置文件，支持批量测试用例
- 性能记录：准确记录 ES 查询响应时间（took）和时延
- Kibana 格式支持：支持 Kibana 风格的查询文件
- 性能对比测试：对比原始查询和优化查询的性能差异
- 可视化报告：生成 CSV 报告和性能对比折线图
- Search Profile：支持 ES Search Profile 分析

**运行模式：**
```bash
# 基础测试模式
python test-runner/es_test_runner.py config.json

# 性能测试模式
python test-runner/es_test_runner.py --performance config.json

# 开启 profile（基础/性能模式均可）
python test-runner/es_test_runner.py --profile config.json
python test-runner/es_test_runner.py --performance --profile config.json
```

**输出文件：**
- `test_results_*/performance-test-results-*.json` - 详细测试报告
- `test_results_*/performance_report_*.csv` - CSV 报告
- `test_results_*/performance_chart_*.png` - 性能对比图
- `test_results_*/search-profile-summary-*.json` - Profile 报告（如开启）

## 依赖安装

### 基础工具（index-sampler, cluster-report）
无需额外依赖，仅需 Python 3.6+ 标准库。

### Query Report 和 Test Runner
```bash
pip install requests matplotlib
```

或使用 requirements.txt:
```bash
pip install -r requirements.txt
```

## 系统集群过滤

默认情况下，所有工具都会自动排除 INFINI Console 系统集群（如 `INFINI_SYSTEM` 或 `infini_default_system_cluster`）。

如需包含系统集群，请使用 `--include-console-cluster` 参数。

## 许可证

MIT License
