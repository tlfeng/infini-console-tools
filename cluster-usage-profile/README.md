# Cluster Usage Profile — 集群使用画像工具

通过 INFINI Console 代理接口（`_proxy`）采集**单个** Elasticsearch 集群的使用情况与使用场景画像。
适用于只能通过 Console 访问集群、无法直连 9200 的场景。

## 采集内容

| 模块 | 数据来源 | 回答的问题 |
|------|---------|-----------|
| 集群概览 | `/`、`_cluster/health`、`_cluster/settings`、`_cluster/pending_tasks` | 版本、健康、磁盘水位配置、主节点任务积压 |
| 节点画像 | `_cat/nodes` | 角色分布（data/master/ingest/coordinating）、Heap/CPU/磁盘水位、磁盘热点节点 |
| 分片画像 | `_cat/shards` | 状态分布、大小分布、最大分片、碎片分片占比、节点分片均衡度 |
| 索引与业务域画像 | `_cat/indices`、`_cat/aliases` | 规模、按命名分组（剥离日期后缀）得到业务域/时间序列占比、别名、closed 归档索引 |
| 读写热度 | `/_all/_stats` 两轮采样 | 每个索引的查询/写入速率（分片级）、活跃度分类、TOP 读/写索引 |
| API 使用画像 | `_nodes/usage` | REST 请求计数，区分 Bulk 批量写入 / Search 检索 / Get 点查等访问方式 |
| 治理画像 | `_cat/templates`、`_ilm/policy`、`_snapshot`、`_all/_settings`、`_cat/aliases` | 模板、ILM 策略、快照仓库、slowlog 配置、只读阻断索引 |
| 风险检查 | 以上数据综合 | 磁盘超水位、只读阻断、未分配分片、超大/碎片分片、无快照仓库等 |
| 场景推断 | 以上数据综合 | 读多写少还是写多读少、时间序列滚动还是固定业务索引、批量写入还是点查为主 |

兼容 6.8.x / 7.10.x：`_cat` 列缺失时自动降级重试；ILM、`_nodes/usage` 不可用时跳过并提示。

## 用法

```bash
# 按集群名采集（推荐，两轮采样默认间隔 60s，总耗时约 1 分钟）
python cluster-usage-profile/cluster_usage_profile.py -c http://localhost:9000 \
  -u admin -p password --cluster-name my-cluster

# 按集群 ID 采集，采样 120 秒，输出到指定目录
python cluster-usage-profile/cluster_usage_profile.py --config config.json \
  --cluster-id xxx --sample-interval 120 -o ./exports

# 只做静态画像，不等采样
python cluster-usage-profile/cluster_usage_profile.py --config config.json \
  --cluster-name my-cluster --sample-interval 0

# 不带 --cluster-id/--cluster-name 时会列出 Console 中所有可选集群
python cluster-usage-profile/cluster_usage_profile.py --config config.json
```

## 参数

| 参数 | 说明 |
|------|------|
| `--cluster-id` / `--cluster-name` | 目标集群（二选一，name 支持部分匹配），环境变量 `CONSOLE_CLUSTER_ID` / `CONSOLE_CLUSTER_NAME` |
| `--sample-interval` | 两轮读写采样间隔秒数，默认 60，`0` 关闭速率采样 |
| `--include-system-indices` | 包含 `.` 开头的系统索引（默认排除） |
| `--top` | 报告中 TOP 列表条数，默认 20 |
| `-o, --output` | 输出目录，默认 `./exports` |

通用 Console 连接参数（`-c/-u/-p/--timeout/--insecure/--config`）与其他工具一致。

## 输出文件

- `usage_profile_<集群名>_<时间戳>.md` — Markdown 画像报告（9 个章节，含场景推断）
- `usage_profile_<集群名>_<时间戳>_indices.csv` — 索引级明细（规模/读写计数/速率/活跃度/年龄/别名）
- `usage_profile_<集群名>_<时间戳>.json` — 完整采集结果，适合喂给下游脚本

## 注意事项

- 读写速率为**分片级 primaries 口径**（每个分片的 query/index 执行次数），客户端真实 QPS ≈ 采样值 ÷ 索引分片数，用于索引间横向对比和读写比例判断最合适。
- `_nodes/usage` 计数自节点重启起累计，受节点 uptime 影响，家族占比仅作参考。
- 两轮采样会对集群产生极轻量压力（2 次 `/_all/_stats`）；超大规模集群建议 `--timeout 120`。
