# metrics-analysis — INFINI Console 指标导出数据的读取与分析工具

配合 [`metrics-exporter`](../metrics-exporter)（DATA_GUIDE.md 有数据格式全集）使用：
把导出的 JSONL 一次性转成分析友好的 CSV，并给出经过实盘对账的常用分析（速率日表、重启检测、合并窗口）。
口径与公式已在数百万行量级的真实导出数据上对账验证。

## 快速开始

```bash
# 1. 解压导出包（包内自带批次时间戳目录和 alerts/）
zstd -dc metrics_export_YYYYMMDD_HHMMSS.tar.zst | tar -xf -

# 2. 全量提取（大体量 JSONL 多进程流式解析，其余类型秒级）
python extract.py /path/to/export_dir          # 输出到 export_dir/analysis/
python extract.py /path/to/export_dir -o /tmp/out --types cluster_health cluster_stats alerts  # 只提小类型

# 3. 速览
python quicklook.py analysis/node_stats.csv.gz                    # 速率日表（09-12 窗口）
python quicklook.py analysis/node_stats.csv.gz --restarts         # 真重启检测
python quicklook.py analysis/node_stats.csv.gz --profile          # 节点档案（上下线/线程数/存储）
python quicklook.py analysis/node_stats.csv.gz --day MM-DD        # 某天分钟级曲线
python quicklook.py analysis/node_stats.csv.gz --health analysis/cluster_health.csv.gz
python quicklook.py analysis/node_stats.csv.gz --alerts analysis/alert_messages.json --marker <cluster_id>

# 4. 合并窗口
python merge_windows.py analysis/node_stats.csv.gz --big 10 -o analysis/
```

依赖：`pip install pandas`（Python ≥3.10）。

## 输出文件字段

| 文件 | 说明 |
|---|---|
| `node_stats.csv.gz` | 每行 = 一个节点一个采样点。`ts`（epoch 秒）、`node`、计数器列（差分求速率）、仪表列（直读） |
| `index_stats.csv.gz` | 每行 = 一个索引一个采样点，`p_*` primaries 口径、`t_*` total 口径 |
| `cluster_stats.csv.gz` / `cluster_health.csv.gz` | 集群级，每行一个采样点 |
| `alert_messages.json` 等 | 告警原文 |
| `daily_merge_windows.csv` / `big_merge_episodes.csv` | merge_windows.py 产物：每日合并起止 / 大体积合并段 |

常用 node_stats 列：`s_completed`(search池完成) `q_total/q_time`(查询) `i_total/d_total`(写/删)
`gc_o/gc_o_time`(老年代GC) `cpu` `heap` `s_active/s_queue/s_threads`(search池) `http` `ctx`(open contexts)
`m_cur/m_cur_size`(进行中合并) `docs/docs_del/store` `seg_count` `br_*`(熔断) `fs_avail`。
完整清单见 `extract.py` 的 `NS_COLS`。

## 口径（实盘对账过的公式）

- **集群每秒速率**：按 ts 聚合，`速率 = Σ(各节点计数器增量) / median(各节点采样间隔)`。
  不要把 Σ增量 直接当每秒（那是每采样间隔的量），也不要除以 Σdt（那是每节点均值）。
- **差分过滤**：只保留采样间隔 50–200s 的相邻对；增量 <0（计数器回绕）一律剔除。
- **协调层吞吐** = `coordinate-*` 节点 `s_completed` 差分之和（≈顶层请求数）。
- **查询速率** = `data-*` 节点 `q_total` 差分之和（含副本）；**分片均耗时** = ΣΔq_time / ΣΔq_total。
- **合并瞬时值** = 单时间戳跨节点求和（跨样本求和会虚高数倍）。
- **节点分类**按名称前缀：`coordinate-` / `data-` / `master-`。

## 踩过的坑（代码里已规避，勿回退）

1. **CSV 必须带 ts、node 两列**——列清单里漏掉它们，`extrasaction="ignore"` 会静默丢掉。
2. **sort 之后禁止 reset_index**——node 前缀布尔掩码按索引对齐，reset 后行会错位（症状：协调节点出现 active=48、threads=193 这类物理不可能的值）。
3. **计数器回绕 ≠ 重启**：Console 采集毛刺也会让计数微降几千；真重启的判据是回绕后计数接近 0（`--restarts` 用 <1e6 阈值）。
4. **导出可能含双 agent 重复段**（同 ts 同节点两行）：差分管线用 dt≥50 过滤天然规避；统计行数对不上导出说明时先想到这个。
5. **`max_file_descriptors`** 在 7.x 是 int、别的版本可能是 `{limit:...}`，两种都要兼容。
6. **ts 存的是 epoch 秒**：读回用 `pd.to_datetime(ts, unit="s", utc=True)`，别让 pandas 猜格式。
7. 告警消息的 `resource_name` 都是 Console 系统集群，**真实归属要看 message 正文里的 Cluster/Node/URL**。
