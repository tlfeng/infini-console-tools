# Hot Threads Collector

定时抓取 Elasticsearch hot threads，并将每次结果写入 JSONL 文件。

- 脚本路径：`hot-threads/hot_threads_collector.py`
- 调用方式：通过 INFINI Console 的 `_proxy` 转发到 ES `/_nodes/hot_threads`
- 输出格式：JSONL（每行一条记录）

## 适用场景

- 集群出现 CPU 飙升、线程阻塞、间歇性卡顿时做现场抓样
- 以固定节奏连续采集（例如 1 小时）用于后续离线分析
- 需要记录失败重试过程与请求耗时

## 快速开始

```bash
python3 hot-threads/hot_threads_collector.py \
  -c http://localhost:9000 \
  -u admin -p password \
  --cluster-id <CLUSTER_ID> \
  --poll-interval 10 \
  --duration-minutes 60 \
  --retries 2 \
  --retry-delay 1
```

说明：
- 上述命令会每 10 秒抓取一次，运行 60 分钟后自动停止。
- 每次抓取成功或失败都会落盘，避免进程中断导致数据丢失。

## 参数说明

### 通用连接参数

- `-c, --console`：Console 地址，默认 `http://localhost:9000`
- `-u, --username`：用户名
- `-p, --password`：密码
- `--timeout`：Console 请求超时（秒），默认 `60`
- `--insecure`：忽略 SSL 证书验证
- `--config`：配置文件路径（JSON）
- `-o, --output`：输出路径，默认 `exports/hot_threads_YYYYMMDD_HHMMSS.jsonl`

### 目标集群参数

- `--cluster-id`：目标集群 ID（推荐）
- `--cluster-name`：目标集群名称（未指定 `--cluster-id` 时可用）
- `--node-id`：节点 ID/名称，逗号分隔；为空表示采集全部节点

### 采集控制参数

- `--poll-interval`：抓取间隔（秒），默认 `10`
- `--duration-minutes`：运行时长（分钟），默认 `0`（不限制）
- `--count`：抓取次数，默认 `0`（不限制）
- `--retries`：单次抓取失败后的重试次数，默认 `2`
- `--retry-delay`：重试间隔（秒），默认 `1`

### Hot Threads API 参数

- `--threads`：返回线程数量，默认 `3`
- `--snapshots`：采样次数，默认 `10`
- `--sample-interval`：采样间隔（如 `500ms`、`1s`）
- `--type`：采样类型，`cpu` / `wait` / `block`，默认 `cpu`
- `--api-timeout`：hot threads API 超时（如 `30s`）
- `--ignore-idle-threads`：忽略空闲线程（默认开启）
- `--include-idle-threads`：包含空闲线程

## 常用命令

1) 连续抓取 1 小时（推荐）

```bash
python3 hot-threads/hot_threads_collector.py \
  -c http://localhost:9000 -u admin -p password \
  --cluster-id <CLUSTER_ID> \
  --poll-interval 10 --duration-minutes 60
```

2) 仅抓取指定节点，持续 30 分钟

```bash
python3 hot-threads/hot_threads_collector.py \
  -c http://localhost:9000 -u admin -p password \
  --cluster-id <CLUSTER_ID> --node-id node-1 \
  --poll-interval 5 --duration-minutes 30 --threads 10
```

3) 固定次数抓取，便于回归测试

```bash
python3 hot-threads/hot_threads_collector.py \
  -c http://localhost:9000 -u admin -p password \
  --cluster-id <CLUSTER_ID> --count 30 --poll-interval 2
```

## 输出格式

每次抓取写入一行 JSON。成功和失败都会记录。

成功记录示例：

```json
{
  "sequence": 1,
  "attempt": 1,
  "fetch_time": "2026-05-19T00:58:15.911819+00:00",
  "elapsed_ms": 664.95,
  "cluster_id": "d82k4s5vlvcc737void0",
  "api_path": "/_nodes/hot_threads?threads=3&snapshots=10&ignore_idle_threads=true&type=cpu",
  "response": "::: {node-1} ..."
}
```

失败记录示例：

```json
{
  "sequence": 3,
  "attempt": 2,
  "fetch_time": "2026-05-19T01:00:12.001000+00:00",
  "elapsed_ms": 1012.56,
  "cluster_id": "d82k4s5vlvcc737void0",
  "api_path": "/_nodes/hot_threads?threads=3&snapshots=10&ignore_idle_threads=true&type=cpu",
  "error": "HTTP 504: gateway timeout"
}
```

## 运行日志说明

脚本会打印：

- 目标集群、请求路径、输出文件
- 每次抓取的成功/失败、尝试次数、耗时
- 结束汇总（`total/success/failed/retries`）

示例：

```text
[12] 抓取成功 attempt=1 time=... elapsed=542.30ms
[13] 抓取失败，准备重试 attempt=1/3 elapsed=1021.77ms error=...
[13] 抓取成功 attempt=2 time=... elapsed=690.12ms
...
汇总: total=360, success=358, failed=2, retries=2
```

## 排障建议

- 无法认证：确认账号、密码及 Console 地址是否正确。
- 找不到集群：优先使用 `--cluster-id`，避免同名集群歧义。
- 数据量太大：减小 `--threads`、增大 `--poll-interval` 或缩短 `--duration-minutes`。
- 失败较多：适当增大 `--timeout` 与 `--api-timeout`，并提高 `--retries`。

## 与 Elasticsearch API 对齐

脚本使用的 API 路径：

- `GET /_nodes/hot_threads`
- `GET /_nodes/{node_id}/hot_threads`

并支持常用参数：`threads`、`snapshots`、`interval`、`type`、`timeout`、`ignore_idle_threads`。
