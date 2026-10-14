# Cluster Report（集群报告）

收集 INFINI Console 中**所有** Elasticsearch 集群的基本信息，输出 CSV 报告和汇总统计，
适合做环境资产盘点、定期巡检留档、给客户出全貌报表。

数据全部来自 Console 自身接口（集群列表、`/elasticsearch/status`、`/elasticsearch/<id>/metrics`），
**不直连 ES**，因此 Console 监控里挂载的集群即使暂时离线也会出现在报告里（可用性列标"否"）。

注意：数值来自 Console 监控侧的聚合 summary，与集群实时值可能有出入；
单集群深度体检（配置/mapping/线程池/快照等）请用 [cluster-config-dump](../cluster-config-dump/)。

## 用法

```bash
# 基础用法（默认排除 Console 系统集群）
python cluster_report.py -c http://localhost:9000 -u admin -p password

# 使用配置文件
python cluster_report.py --config config.json

# 仅显示汇总统计，不生成 CSV
python cluster_report.py -c http://localhost:9000 --summary-only

# 包含 Console 系统集群
python cluster_report.py -c http://localhost:9000 --include-console-cluster
```

## 报告字段

| 字段 | 说明 |
|------|------|
| Console显示名称 / Console集群ID | Console 中的登记信息（ID 可供其他工具 `--cluster-id` 使用） |
| 集群名称 / 集群版本 | ES 侧的真实集群名与版本 |
| 健康状态 / 可用性 | green/yellow/red；可用性为 Console 探测结果 |
| 在线时长 | Console 监控的 uptime |
| 节点数 / 索引数 | 集群规模 |
| 主分片数 / 总分片数 / 未分配分片 | 分片概况 |
| 文档数 | 全集群文档总数 |
| 存储已用 / 存储总量 | 磁盘占用（含人读格式） |
| JVM已用 / JVM总量 / JVM使用率% | 堆内存概况 |
| 采集时间 | 本次采集时间戳 |

末尾同时打印汇总统计：集群总数、可用/监控中集群数、健康状态分布、
总节点/索引/文档数、总存储用量。

## 参数

| 参数 | 说明 |
|------|------|
| `--summary-only` | 仅显示汇总统计，不生成 CSV 文件 |
| `--include-console-cluster` | 包含 INFINI Console 系统集群 |
| `-o, --output` | 输出 CSV 路径（默认 `cluster_report_<时间戳>.csv`） |

## 输出文件

- `cluster_report_YYYYMMDD_HHMMSS.csv` - 详细报告（UTF-8 BOM，Excel 可直接打开）
