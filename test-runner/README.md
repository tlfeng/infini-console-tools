# ES Query Test Runner (Python 版本)

一个轻量化的 Elasticsearch 查询性能测试工具，通过 Infini Console 自动执行 ES 查询并记录响应时间。

## 🎯 特性

- ✅ **Python 单文件**：仅需一个 Python 文件，部署简单
- ✅ **配置驱动**：JSON 配置文件，支持批量测试用例
- ✅ **性能记录**：准确记录 ES 查询响应时间（took）和时延
- ✅ **Kibana 格式支持**：支持 Kibana 风格的查询文件，方便编辑
- ✅ **性能对比测试**：对比原始查询和优化查询的性能差异
- ✅ **可视化报告**：生成 CSV 报告和性能对比折线图
- ✅ **结构化输出**：生成详细的 JSON 报告
- ✅ **错误处理**：完善的错误处理和验证机制

## 📁 文件结构

```
es-test-runner-py/
├── es_test_runner.py                # 主程序文件
├── requirements.txt                 # Python 依赖包
├── config/
│   ├── test-cases.json            # 基础测试配置
│   └── performance-test-config.json  # 性能测试配置
│   └── kibana-format-*.queries  # Kibana 格式查询文件
└── README.md                      # 说明文档
```

## 🚀 快速开始

### 1. 环境要求

- Python 3.8+
- 可访问的 Infini Console
- 有效的用户名和密码

### 2. 安装依赖

```bash
cd es-test-runner-py
pip install -r requirements.txt
```

### 3. 运行测试

```bash
# 基础测试模式（使用默认配置文件）
python es_test_runner.py

# 基础测试模式（指定配置文件路径）
python es_test_runner.py config/test-cases.json

# 性能测试模式（对比原始查询和优化查询）
python es_test_runner.py --performance

# 性能测试模式（指定配置文件路径）
python es_test_runner.py --performance config/performance-test-config.json

# 开启 Search Profile（仅对 _search 请求生效）
python es_test_runner.py --profile
python es_test_runner.py --performance --profile

# 查看帮助
python es_test_runner.py -h
```

## ⚙️ 基础测试配置

配置文件位于 `config/test-cases.json`：

```json
{
  "baseUrl": "http://localhost:9000",
  "auth": {
    "username": "admin",
    "password": "Qwer1234"
  },
  "testCases": [
    {
      "name": "查看集群信息",
      "clusterId": "d5u6pm21us8vhd5mls3g",
      "path": "_cat/health",
      "method": "GET",
      "body": {}
    },
    {
      "name": "查看所有索引",
      "clusterId": "d5u6pm21us8vhd5mls3g",
      "path": "_cat/indices",
      "method": "GET",
      "body": {}
    }
  ]
}
```

## ⚡ 性能测试配置

性能测试用于对比原始查询和优化后查询的性能差异。

### 配置文件

配置文件位于 `config/performance-test-config.json`：

```json
{
  "baseUrl": "http://localhost:9000",
  "auth": {
    "username": "admin",
    "password": "Qwer1234"
  },
  "clusterId": "your_cluster_id",
  "iterations": 10,
  "warmupIterations": 2,
  "intervalSeconds": 1,
  "title": "ES查询性能对比",
  "originalQueryLabel": "原始查询",
  "optimizedQueryLabel": "优化后查询",
  "originalQueriesPath": "config/kibana-format-original.queries",
  "optimizedQueriesPath": "config/kibana-format-optimized.queries"
}
```

### 配置字段说明

| 字段 | 说明 |
|------|------|
| baseUrl | Infini Console 的地址 |
| auth | 登录凭据 (username, password) |
| clusterId | ES 集群 ID |
| iterations | 测试运行的轮次 |
| warmupIterations | 前 N 轮热身轮次（包含在 iterations 内，不计入统计，范围 `0..iterations-1`） |
| intervalSeconds | 每轮测试之间的间隔时间（秒） |
| title | 图表的标题 |
| originalQueryLabel | 原始查询的标签（用于图例） |
| optimizedQueryLabel | 优化后查询的标签（用于图例） |
| originalQueriesPath | 原始查询文件路径（Kibana 格式） |
| optimizedQueriesPath | 优化查询文件路径（Kibana 格式） |
| originalQueries | 原始查询数组（内联方式） |
| optimizedQueries | 优化查询数组（内联方式） |

### Kibana 格式查询文件

查询文件格式类似 Kibana 开发工具，方便用户编辑和测试：

```text
# 查询名称1
GET products/_search
{
  "query": {
    "match": {
      "name": "手机"
    }
  },
  "size": 10
}

# 查询名称2
GET orders/_search
{
  "query": {
    "match_all": {}
  },
  "size": 20
}
```

格式说明：
- `# 开头的行`作为查询分隔符，`#` 后的文本作为查询名称
- 支持单行或多行 JSON
- 支持多种 HTTP 方法（GET、POST、PUT、DELETE 等）

## 📊 输出格式

### 基础测试输出

运行基础测试后生成：
- **JSON 报告**：`test-results.json` - 包含详细的测试结果和统计摘要
- **Profile 关键信息**：`search-profile-summary.json`（仅 `--profile`）- 包含优化参考指标（慢分片、慢组件、阶段耗时等）

### 性能测试输出

运行性能测试后生成：
- **JSON 报告**：`performance-test-results-YYYYMMDD-HHMMSS.json` - 详细的测试报告
- **CSV 报告**：`performance_report_YYYYMMDD_HHMMSS.csv` - 包含每次查询的详细结果和统计摘要
- **Took 时间对比图**：`performance_chart_took_YYYYMMDD_HHMMSS.png` - ES 内部查询执行时间对比
- **端到端时延对比图**：`performance_chart_duration_YYYYMMDD_HHMMSS.png` - 整体响应时间对比
- **Profile 关键信息**：`search-profile-summary-YYYYMMDD_HHMMSS.json`（仅 `--profile`）- 包含每轮查询可优化的 profile 指标

### 控制台输出示例

```
============================================================
ES Query Performance Test Runner (Python)
============================================================

[1/5] Loading configuration...
✓ Configuration loaded from: config/performance-test-config.json

[2/5] Login to Infini Console...
✓ Login successful!

[3/5] Loading queries...
✓ Loading queries from files...
  - Original queries: config/kibana-format-original.queries
  - Optimized queries: config/kibana-format-optimized.queries
  - 3 original queries
  - 3 optimized queries
  - Iterations: 10
  - Interval: 1s

[4/5] Running performance tests...

  Iteration 1/10
  开始时间: 2026-02-05 10:00:00
  本轮平均 Took:
    原始查询: 150.5ms
    优化后查询: 120.2ms

  ...

[5/5] Generating report...
✓ CSV报告已保存: performance_report_20260205_100000.csv
✓ Took时间对比图已保存: performance_chart_took_20260205_100000.png
✓ 端到端时延对比图已保存: performance_chart_duration_20260205_100000.png

============================================================
Test Results Summary
============================================================
Total: 60
Passed: 60
Failed: 0
Pass Rate: 100.00%

============================================================
Performance Comparison
============================================================

  ✓ 商品搜索
    原始查询: 150.50ms
    优化后查询: 120.20ms
    Improvement: +20.13%
```

## 🛠️ 实际应用

### 批量测试 ES 查询性能

```bash
# 使用现有配置
python es_test_runner.py config/test-cases.json

# 自定义配置
python es_test_runner.py config/my-config.json
```

### 比较查询优化效果

使用性能测试模式评估索引优化带来的性能提升：

```bash
python es_test_runner.py --performance config/performance-test-config.json
```

配置文件设置：
- `iterations: 10` - 运行 10 轮测试
- `warmupIterations: 2` - 前 2 轮作为热身，不计入最终统计
- `intervalSeconds: 5` - 每轮间隔 5 秒

运行完成后，你可以通过：
- 查看 CSV 文件获取详细数据
- 查看折线图了解性能趋势
- 参考统计摘要了解平均性能提升

## 🚨 注意事项

1. **权限验证**：确保用户有访问 Console 和 ES 集群的权限
2. **网络连接**：确保可以正常访问 Console 和 ES 集群
3. **Python 版本**：需要 Python 3.8 或更高版本
4. **依赖包**：首次运行需要安装依赖包
5. **生产环境**：根据实际负载调整间隔时间，避免对生产环境造成压力

## 📄 许可证

MIT License
