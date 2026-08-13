# data-tasks — 数据工具任务报表

读取 INFINI Console 侧边栏 **数据工具 → 数据迁移** 和 **数据工具 → 数据比对**
的任务列表，尽可能多地导出每个任务的详细信息（状态、用时、涉及索引、文档数、
进度、执行节点、scroll/bulk 参数、增量/分区标记等）。

## 数据来源

对每种任务同时调用列表与详情接口：

- `GET /migration/data/_search`     — 迁移任务列表（`populateMigrationSearchTaskInfo` 已填 target_total_docs / source_total_docs / error_partitions / running_children / repeat）
- `GET /comparison/data/_search`    — 比对任务列表
- `GET /migration/data/{id}/info`   — 迁移单任务详情（`config_string.indices[]` 会被填 `percent` / `status` / `exported_percent`）
- `GET /comparison/data/{id}/info`  — 比对单任务详情（`indices[]` 会带 `scroll_percent` / `total_scroll_docs` / `total_diff_docs` / `status`）
- `GET /migration/data/{id}/info/{index}` — 每索引详情（主任务 `start_time_in_millis` 为 0 时聚合子任务开始/完成时间）
- `GET /comparison/data/{id}/info/{index}`

不需要每索引进度时可以加 `--no-info` 跳过详情调用，只用列表接口，速度更快。

## 时间与耗时

- `start_time`（开始时间）取自任务记录的 `start_time_in_millis`，时区与 `created` 一致。
- 主任务自身的 `start_time_in_millis` 经常为 0（该字段要等调度器真正开跑才写入）。
  此时脚本会按索引逐个调用每索引详情接口，对子任务时间做聚合
  （`start_time` 取各索引子任务开始时间的最小值，`completed_time` 取最大值），
  与 Console UI 口径一致。
- `duration` = 完成时间 − 开始时间（进行中的任务用当前时间 − 开始时间），
  格式与 UI 一致（`HH:MM:SS`，超过 24 小时为 `1d HH:MM:SS`，不足 1 分钟为 `12.34 s`）。
- 拿不到开始时间（如从未运行过的任务）时，`start_time` 与 `duration` 留空，
  不再用“创建时间 → 完成时间”兜底。

## 输出格式

默认同时生成两种（`--format both`）：

- `data_tasks.json` — 完整嵌套结构，字段最全（执行节点、scroll/bulk、raw_filter、
  incremental/partition 完整对象、log_info 等都在里面），适合喂给下游脚本
- `data_tasks.csv`  — 每行 `(任务, 索引)`，方便放进 Excel/BI 做筛选和聚合

只需要一种：`--format json` / `--format csv`。

## 用法

```bash
# 使用配置文件（推荐；把凭证放进项目根 config.json）
python3 data_tasks_report.py --config ../config.json

# 直接指定 Console（HTTPS + 自签证书）
python3 data_tasks_report.py -c https://localhost:9000 -u admin -p 'Qwer@12345' --insecure

# 只导出数据迁移任务，输出 JSON
python3 data_tasks_report.py --config ../config.json --kind migration --format json

# 跳过 /info 详情（快很多，但没每索引进度，且不做子任务时间聚合）
python3 data_tasks_report.py --config ../config.json --no-info

# 输出到指定目录 / 指定文件前缀
python3 data_tasks_report.py --config ../config.json -o ./exports
python3 data_tasks_report.py --config ../config.json -o ./exports/tasks   # 会生成 tasks.json 和 tasks.csv
```

主要参数：

| 参数            | 说明                                                     |
| --------------- | -------------------------------------------------------- |
| `--kind`        | `migration` / `comparison` / `all`（默认 all）           |
| `--format`      | `json` / `csv` / `both`（默认 both）                     |
| `--no-info`     | 跳过 `/info` 与每索引详情调用（不再抓每索引的实时进度，也不做子任务时间聚合） |
| `-o/--output`   | 输出目录或文件名前缀，默认 `./data_tasks_output`         |
| `--page-size`   | 每次翻页拉取的任务数，默认 200                           |
| `--config`      | JSON 配置文件路径（`consoleUrl` / `auth` / `insecure`）  |
| `-c/--console`  | Console URL，默认 `http://localhost:9000`                |
| `-u`/`-p`       | 登录用户名 / 密码                                        |
| `--insecure`    | 忽略 SSL 证书验证（HTTPS + 自签名场景）                  |

## 字段清单

### 任务级（CSV 前 42 列）

| 列                          | 含义                                     |
| --------------------------- | ---------------------------------------- |
| `kind`                      | 数据迁移 / 数据比对                      |
| `task_id`                   | 任务 ID                                  |
| `task_name`                 | 任务名称                                 |
| `status`                    | 任务状态（init / ready / running / complete / stopped / pause / error 等） |
| `task_lifecycle`            | 生命周期状态（not_started / running / complete / ...） |
| `created`                   | 创建时间                                 |
| `start_time`                | 开始运行时间（ISO，与 created 同时区；主任务 start_time_in_millis 为 0 时从子任务聚合） |
| `completed_time`            | 完成时间                                 |
| `duration` / `duration_ms`  | 用时（可读 `HH:MM:SS` + 毫秒；= 完成时间 − 开始时间）|
| `creator`                   | 创建人                                   |
| `source_cluster` / `target_cluster` (+ `_id` / `_distribution`) | 源/目标集群 |
| `task_indices_count`        | 涉及索引数                               |
| `task_completed_indices`    | 已完成索引数                             |
| `task_total_source_docs`    | 源端总文档数                             |
| `task_total_target_docs`    | 目标端总文档数（已迁移/已比对）          |
| `task_total_source_size`    | 源端总存储大小（可读格式）               |
| `task_overall_percent`      | 任务整体进度（迁移=target/source，比对=scroll/(src+tgt)） |
| `task_is_incremental`       | 是否启用增量                             |
| `task_is_partitioned`       | 是否启用分区并发                         |
| `task_is_repeat`            | 是否为重复调度类任务                     |
| `task_run_times`            | 已运行次数                               |
| `task_error_partitions`     | 错误分片数                               |
| `task_running_children`     | 运行中的子任务数                         |
| `runtime_group`             | 运行时组                                 |
| `execution_nodes`           | 执行节点（分号分隔）                     |
| `scroll_*` / `bulk_*`       | 迁移的滚动查询和批量写入参数             |
| `max_tasks_per_instance`    | 每实例最大任务数                         |
| `max_retry_times`           | 最大重试次数                             |
| `tags`                      | 任务标签（分号分隔）                     |

### 索引级（CSV 后 21 列，每行一个源索引）

| 列                          | 含义                                     |
| --------------------------- | ---------------------------------------- |
| `source_index` / `target_index` | 源/目标索引名                        |
| `doc_type`                  | 文档类型（通常 `_doc`）                  |
| `source_docs` / `target_docs`   | 索引级源/目标文档数                  |
| `source_store_size` / `_bytes`  | 索引级源端存储大小                   |
| `index_status`              | 索引级状态                               |
| `index_percent`             | 迁移进度（迁移任务）                     |
| `index_exported_percent`    | 已导出（scroll）进度（迁移任务）         |
| `index_scroll_percent`      | 比对扫描进度（比对任务）                 |
| `index_total_scroll_docs`   | 已扫描文档数（比对任务）                 |
| `index_total_diff_docs`     | 差异文档数（比对任务）                   |
| `index_error_partitions`    | 索引级错误分片数                         |
| `index_running_children`    | 索引级运行中的子任务数                   |
| `has_filter`                | 是否设置了过滤条件                       |
| `incremental_field` / `_delay` | 增量字段和延迟                        |
| `partitioned`               | 是否分区并发                             |
| `partition_field` / `_step` | 分区字段和步长                           |

JSON 里还额外包含 `raw_filter` 完整对象、`incremental` / `partition` 的完整结构、
`time_window`、`repeat`、`log_info`（.infini_logs 位置，便于去查该任务的运行日志）等。
