# INFINI Console Tools

INFINI Console 配套工具集，提供集群管理、索引采样、查询分析等功能。

## 工具列表

| 工具 | 功能 | 路径 |
|------|------|------|
| **index-sampler** | 索引采样工具 - 提取索引 Mapping 和样本文档 | `index-sampler/` |
| **cluster-report** | 集群报告工具 - 收集集群基本信息和统计 | `cluster-report/` |
| **query-report** | 查询报告工具 - 执行查询并生成 Markdown 报告 | `query-report/` |
| **test-runner** | 测试运行工具 - 查询性能测试和对比 | `test-runner/` |

## 快速开始

所有工具均使用 Python 3.6+ 编写，仅需标准库（除 test-runner 外）。

### 1. 克隆仓库

```bash
git clone <repository-url>
cd infini-console-tools
```

### 2. 使用工具

```bash
# 索引采样
python index-sampler/index_sampler.py -c http://localhost:9000 -u admin -p password

# 集群报告
python cluster-report/cluster_report.py --host http://localhost:9000 -u admin -p password

# 查询报告（需要 requests）
pip install requests
python query-report/query_report.py --base-url http://localhost:9000 --cluster-name my-cluster

# 测试运行（需要 requests 和 matplotlib）
pip install requests matplotlib
python test-runner/test_runner.py --performance
```

## 公共模块

`common/` 目录包含所有工具共享的代码：

- `console_client.py` - Console API 客户端封装
  - JWT 认证
  - 集群列表获取
  - _proxy API 调用
  - 索引信息查询
  - 工具方法（字节格式化、时长格式化等）

## 工具详情

### Index Sampler (索引采样工具)

从 INFINI Console 管理的所有 Elasticsearch 集群中采样索引信息。

**功能：**
- 自动发现所有管理的集群（排除 Console 系统集群）
- 获取所有非系统索引（排除 `.` 开头的索引）
- 提取每个索引的 Mapping 结构
- 提取每个索引的样本文档（默认 2 条）
- 导出 JSON 和 CSV 格式的总结报告
- 为每个索引生成详细的 JSON 文件

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

**输出文件：**
- `cluster_report_YYYYMMDD_HHMMSS.csv` - 详细报告

### Query Report (查询报告工具)

根据 Kibana 风格请求文本，自动执行查询并输出 Markdown 报告。

**功能：**
- 查询请求（多行 + 单行 body）
- 查询完整响应
- 索引概览（支持 alias 解析到真实索引）
- 索引 mappings
- 索引 settings（扁平化为 `index.xxx` 键）

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

**输出文件：**
- `performance-test-results-*.json` - 详细测试报告
- `performance_report_*.csv` - CSV 报告
- `performance_chart_*.png` - 性能对比图

## 依赖安装

### 基础工具（index-sampler, cluster-report）
无需额外依赖，仅需 Python 3.6+ 标准库。

### Query Report
```bash
pip install requests
```

### Test Runner
```bash
pip install requests matplotlib
```

或使用 requirements.txt:
```bash
pip install -r requirements.txt
```

## 配置说明

### Console 连接配置

所有工具都支持以下方式配置 Console 连接：

1. **命令行参数**
   ```bash
   -c http://localhost:9000 -u admin -p password
   ```

2. **环境变量**
   ```bash
   export CONSOLE_BASE_URL=http://localhost:9000
   export CONSOLE_USERNAME=admin
   export CONSOLE_PASSWORD=password
   ```

3. **配置文件**（query-report, test-runner 支持）
   ```json
   {
     "baseUrl": "http://localhost:9000",
     "auth": {
       "username": "admin",
       "password": "password"
     }
   }
   ```

## 系统集群过滤

默认情况下，所有工具都会自动排除 INFINI Console 系统集群（如 `INFINI_SYSTEM` 或 `infini_default_system_cluster`）。

如需包含系统集群，请使用 `--include-console-cluster` 参数。

## 许可证

MIT License
