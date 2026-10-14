# Cluster Config Dump（集群配置全量采集）

通过 INFINI Console 的 `_proxy` 通道，对指定 Elasticsearch 集群执行一批**只读 GET API**，
把每条 API 的响应落盘为 JSON，用于判断集群能否正常运行、排查问题、以及变更前后 diff。

API 目录以 elasticsearch 7.10 官方文档 Cluster APIs 为主体，并补充了 Cat APIs、
索引/模板/ILM/SLM/快照配置、License、诊断与模块健康、ML 等对"集群健康判断"有价值的
API（全部路由对照 elasticsearch v7.10.2 源码逐条核对）。

- 全部为 GET 只读操作，对集群无副作用
- 被通配父 API 完全包含的子 API 不重复采集
  （如 `/_nodes/<id>`、`/_cluster/state/<metric>`、`/_cat/indices/<index>`）
- 写操作不采集（reroute、update settings、tasks cancel 等）
- 单条 API 失败不影响后续采集，错误本身也是诊断信息，会原样落盘

## 支持的类别（共 6 类 / 76 个 API）

| 类别 | 内容 | 数量 |
|------|------|------|
| `cluster` | Cluster APIs 核心：health、state、stats、settings、pending_tasks、nodes info/stats/usage/hot_threads、remote info、tasks、allocation explain | 14 |
| `cat` | Cat APIs 人读汇总视图（format=json） | 19 |
| `index` | 索引统计/设置/mapping/alias/segments/recovery、模板、ingest pipeline、ILM/SLM、快照仓库与状态、dangling indices | 19 |
| `license` | `/_xpack`、`/_xpack/usage`、`/_license` 及状态/功能使用 | 6 |
| `diagnostic` | `/_migration/deprecations`、`/_security/_authenticate`、watcher/enrich/ccr/rollup/transform/script | 11 |
| `ml` | ML info、异常检测作业、datafeed、DataFrame analytics（未启用时报错属预期） | 7 |

标为 **[重]** 的接口（`_cluster/state`、`/_stats`、`/_segments`、`/_shard_stores`）
在大集群上响应可达数十 MB，可用 `--skip-heavy` 排除。

## 用法

```bash
# 交互式（推荐）：选择集群 → 选择类别 → 确认是否包含重接口
python cluster_config_dump.py -c http://localhost:9000 -u admin -p password

# 非交互：指定集群与类别，跳过重接口
python cluster_config_dump.py --config config.json --cluster-name prod \
    --groups cluster,cat,index --skip-heavy

# 全部类别（含重接口），--yes 跳过所有菜单
python cluster_config_dump.py -c http://localhost:9000 -u admin -p 'pw' \
    --cluster-id xxx --yes

# 全集群模式：一次采集 Console 中所有非系统集群（跳过不可达集群）
python cluster_config_dump.py --config config.json --all-clusters \
    --groups cluster,cat --skip-heavy

# 只查看 API 目录（76 个 API 及说明）
python cluster_config_dump.py --list
```

类别参数支持编号或 key，如 `--groups 1,2,3`、`--groups cluster,cat`、`--groups all`。

全集群模式说明：

- 目标参数三选一：`--cluster-id` / `--cluster-name` / `--all-clusters`
  （`--all-clusters` 与前两者互斥）
- 顺序执行，逐个预检可达性；离线/无权限的集群标记失败后继续下一个
- 所有集群共用同一时间戳，每个集群一个独立的 `config_dump_<时间戳>_<集群名>/` 目录，
  便于整批与上一次运行做 `diff -r`
- 结束后在输出根目录生成 `batch_manifest_<时间戳>.json`，记录每个集群的
  状态/计数/产物目录，可作为自动化 diff 的索引
- 退出码：至少一个集群采集成功即为 0

## 输出结构

```
cluster_config_dump/
├── batch_manifest_<时间戳>.json   # 全集群模式的批处理清单（单集群模式无此文件）
└── config_dump_<时间戳>_<集群名>/
    ├── manifest.json              # 全部 API 的状态/耗时/大小/错误（总清单）
    ├── summary.md                 # 自动体检摘要（健康信号/模块状态/统计）
    ├── 01_cluster/
    │   ├── 001_root.json
    │   ├── 002_cluster_health.json
    │   └── ...
    ├── 02_cat/
    ├── 03_index/
    ├── 04_license/
    ├── 05_diagnostic/
    └── 06_ml/
```

- 每条 API 一个编号 JSON 文件，目录按类别编号，两次运行可直接 `diff -r`
- `manifest.json` 记录每条的 `status`：
  - `ok`：正常响应
  - `es_error`：集群返回了错误响应——既包括经 `_proxy` 透传回来的非 2xx，也包括
    2xx 里带 `{"error", "status"}` 体的情况。**部分属预期**，如
    `/_cluster/allocation/explain` 在所有分片已分配时返回 400；Easysearch 未实现的
    xpack 接口（`/_license`、`/_ml/*` 等）返回 405/400。结果文件落盘的是集群原始错误体
    JSON，`es_error_status` 记录其 HTTP 状态码，是最有价值的排障线索之一
  - `skipped`：命中 API 目录中 `expected_status` 标注的状态码（如无快照仓库的 404、
    基础 license 下 CCR 的 403），按预期跳过、不算失败；结果文件里以 `es_response`
    保留集群错误体
  - `failed`：请求层面失败（网络中断、Console 自身返回错误等非集群响应），错误信息
    写入结果文件的 `_dump_error`
- `summary.md` 自动提取：健康状态、未分配分片、pending tasks 积压、JVM 堆、
  磁盘水位、circuit breaker、线程池拒绝、节点版本一致性、License 到期、
  ILM/SLM 运行模式、快照仓库、弃用配置告警等

## 健康判断速查（summary.md 第 2 节的自动检查项）

| 信号 | 判定 |
|------|------|
| `cluster_health.status` | red=❌ 主分片丢失；yellow=⚠ 副本未分配 |
| task_max_waiting_in_queue_millis | >60s 说明 master 处理不过来 |
| pending_tasks 排队时间 | 高优先级任务排队超 1 分钟需排查 |
| 节点 heap_used_percent | ≥85% 警告，≥90% 严重 |
| 节点磁盘使用率 | ≥85%（低水位，停止分配）警告，≥95%（flood stage，只读）严重 |
| circuit breaker estimated ≥ limit | ❌ 已触顶 |
| thread_pool rejected > 0 | ⚠ 有过载历史，重点看 write/search 池 |
| 节点版本集合 | >1 个版本说明滚动升级未完成 |
| license 状态/到期 | 非 active 或 30 天内到期警告 |
| ILM/SLM operation_mode | 非 RUNNING 说明生命周期管理停摆 |
| `/_snapshot` 为空 | ⚠ 未配置任何备份仓库 |

## 注意事项

1. `manifest.json` 与 `summary.md` 无论部分失败与否都会生成
2. 结果可能包含集群配置细节（settings、realm 等），按敏感资料保管
3. 大集群建议先 `--groups cluster` + `--skip-heavy` 快速体检，再决定是否全量
4. Console 请求超时默认放宽到 120s（大响应 API 需要），`--timeout` 可调
5. 7.10.2 不存在 `GET /_nodes/shutdown`（8.x）与 `/_features`（7.12+），未列入

## 环境变量

`CONSOLE_URL` / `CONSOLE_USERNAME` / `CONSOLE_PASSWORD` /
`CONSOLE_CLUSTER_ID` / `CONSOLE_CLUSTER_NAME` / `CONSOLE_DUMP_GROUPS` /
`CONSOLE_MAX_ATTEMPTS`（网络重试次数，见 common/console_client.py）

也可在 `config.json` 的 `clusterConfigDumpOptions` 段配置 `output` / `groups` /
`skipHeavy` / `allClusters` / `includeConsoleCluster`。
