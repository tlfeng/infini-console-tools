# Console Queue Delay — 指标写入延迟排查工具

排查 INFINI Console 监控图表延迟、状态灯全红的根因。**只需要 Console 账号权限**，
不需要 ES 集群密码——所有检查都通过 Console 自身接口完成。

## 背景

非探针（agentless）模式下 Console 自己定时采集集群指标（默认 10s/轮）：
采集时打上 timestamp → 推入本地磁盘队列（metrics）→ 后台消费者异步 bulk 写入系统 ES。
监控图表和"非探针"状态灯都读文档的 timestamp。写入链路积压时，文档带着旧时间戳
晚 N 小时落库，表现为：图表永远只画到 now-N 小时、状态灯红色。

## 检查项

| 检查 | 接口 | 回答的问题 |
|------|------|-----------|
| A. Console 本地队列 | `GET /queue/stats` | metrics 队列是否有积压（lag/depth）、消费者是否卡死 |
| B. 被监控集群列表 | `GET /elasticsearch/_search` | 有哪些集群、什么采集模式 |
| C. 采集状态（红绿灯） | `GET /elasticsearch/<id>/_collection_stats` | 每类指标多久没数据了 |
| D. 指标新鲜度明细 | `_proxy` → `.infini_metrics-*/_search` | 每类指标/每个集群的最新 timestamp 延迟、近 5m/1h 文档数 |
| E. 系统 ES 负载 | `_proxy` → `_cat/thread_pool/write` 等 | 积压的常见根因（write 拒绝、磁盘/CPU） |
| F. 缺席二次诊断 | `_proxy` → 按 cluster_id 直查 `.infini_metrics` | absent 到底是积压、采集停止，还是 Console bug |

## 关于 absent（已对照 Console 源码确认）

`_collection_stats`（界面红绿灯数据源）在 Console 源码
`modules/elastic/api/cluster_overview.go: getClusterMonitorState` 中实现：

- 只查 `timestamp >= now-15m` 窗口；窗口内没有某类型的文档，响应里就没有这个类型的 key；
- Console 返回的 status 只有 `ok`（5 分钟内）/ `warning`（超过 5 分钟），
  **不存在 `absent` 值**——工具把缺席的预期类型统一标记为 `absent`，
  语义是"该类型 ≥15 分钟无数据"；
- 因此**所有集群全部 absent 的直接含义是：系统 ES 最近 15 分钟没有落库任何指标文档**。
  在写入积压环境下这是必然表现（文档带旧时间戳晚到，窗口内无数据），
  不是采集坏了，也不是工具解析错误；是否真是积压，看报告里的新鲜度明细与队列 lag。

为便于 Field 判别，工具内置**缺席二次诊断**：对报 absent 的集群按 cluster_id
直查系统 ES 的实际最新时间戳，把每个缺席类型归类为：

| 判定 | 含义 |
|------|------|
| `stale` | 有数据但最新时间戳超过 15 分钟 → 采集停止或写入积压（结合队列判定） |
| `no_data` | 该集群从未有过该类型文档 → 采集从未成功或 cluster_id 已变更 |
| `fresh_but_absent` | 数据其实新鲜（15 分钟内），状态接口却缺席 → **疑似 Console bug，建议上报** |

## 用法

```bash
# 基础排查，出报告
python console-queue-delay/console_queue_delay.py -c https://console:9000 \
  -u admin -p '<密码>'

# 用配置文件（consoleUrl/auth/output 与其他工具共用）
python console-queue-delay/console_queue_delay.py --config config.json

# 趋势观察：每 10s 采样，持续 10 分钟（可 Ctrl+C 提前结束）
python console-queue-delay/console_queue_delay.py -c ... --watch --duration 600 --interval 10

# 手动指定系统集群 ID（默认自动识别）
python console-queue-delay/console_queue_delay.py --config config.json \
  --system-cluster-id infini_default_system_cluster
```

## 参数

| 参数 | 说明 |
|------|------|
| `-c/--console` `-u/--username` `-p/--password` `--timeout` `--insecure` `--config` | 通用 Console 连接参数，与其他工具一致 |
| `-o, --output` | 报告输出目录，默认 `./exports` |
| `--system-cluster-id` | 系统集群 ID（默认从集群列表自动识别，兜底 `infini_default_system_cluster`） |
| `--watch` | 持续采样观察趋势 |
| `--interval` / `--duration` | watch 采样间隔秒（默认 10）/ 总时长秒（默认 300） |

## 输出文件

- `console_delay_report_<时间戳>.md` — 人读报告（结论、队列、集群状态、缺席诊断、新鲜度、系统 ES 负载）
- `console_delay_raw_<时间戳>.json` — 原始数据，适合二次分析
- `console_delay_trend_<时间戳>.csv` — watch 模式趋势采样

## 判读参考

- **metrics 队列 lag 持续增长 + 指标延迟大** → 写入积压：查系统 ES write 线程池 rejected、
  磁盘/CPU；缓解：调大采集间隔（监控设置 10s→30/60s）、减少 index_stats 范围。
- **队列无积压但延迟大** → 采集停了：查 Console 日志 `collect ... error` /
  `persist ... failed` / `NOT eligible for metrics collection`。
- **absent 但二次诊断 fresh_but_absent** → Console `_collection_stats` 与实际数据不一致，
  属于 Console bug。
- **健康**：延迟秒级、depth=0、消费 offset==写入 offset。

依赖：仅标准库 + 仓库 `common` 模块（登录、重试、代理传输均已复用）。
