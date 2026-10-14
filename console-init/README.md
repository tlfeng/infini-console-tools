# Console Init（Console 初始化/重置）

INFINI Console（es/easysearch 系统集群）的**初始化、重置工具**，Bash 实现，零依赖
（仅需 `bash` + `curl` + `python3` 标准库）。与 `cluster-register` 互补：本工具负责
Console 自身从零到"初始化后干净状态"的一键就绪。

| 命令 | 行为 |
|------|------|
| **`setup`**（默认） | 自动分流：已初始化则先重置再初始化，未初始化则直接初始化。无论之前在 Web UI 上操作了什么，最终都回到"初始化后"的干净状态 |
| `init` | 仅初始化（要求全新环境，已初始化会报错提示改用 reset） |
| `reset` | 重置为"初始化后"状态并重新初始化（需要 `CONSOLE_DIR`） |

`init` 流程：`/setup/_validate` 校验集群连接 → 按 distribution 初始化系统模板
（easysearch ≥1.12.1 额外初始化 rollup）→ `/setup/_initialize` 创建系统集群配置、
admin 用户与系统索引 → 轮询验证 admin 登录与集群列表。

兼容新老两种 Console：老版本要求 `/account/replay_nonce` 的 `X-Request-Nonce` 头，
新版本（如 1.30.x）无该端点时自动不带 nonce 直接调用。

## 用法

### 交互式（推荐，会提示输入所有参数）

```bash
./console-init.sh setup   # 或 init / reset
```

### 非交互式（通过环境变量传入）

```bash
CONSOLE_HOST=127.0.0.1 CONSOLE_PORT=9000 \
CLUSTER_HOST=127.0.0.1 CLUSTER_PORT=9200 CLUSTER_SCHEME=https \
CLUSTER_USER=admin CLUSTER_PASS='mypass' \
CONSOLE_USER=admin CONSOLE_PASS='Qwer@12345' \
CONSOLE_DIR=/path/to/console \
./console-init.sh setup
```

### 参数

| 参数 | 说明 |
|------|------|
| `CONSOLE_HOST` / `CONSOLE_PORT` | Console 的 host 与 port（默认 127.0.0.1:9000） |
| `CONSOLE_SCHEME` | Console 协议：`http` 或 `https`（默认 https） |
| `CLUSTER_HOST` / `CLUSTER_PORT` | 系统集群（es/easysearch）的 host 与 port |
| `CLUSTER_SCHEME` | 系统集群协议：`http` 或 `https`（必填，缺失或非法直接报错） |
| `CLUSTER_USER` / `CLUSTER_PASS` | 系统集群认证（可选，无认证可留空） |
| `CONSOLE_USER` / `CONSOLE_PASS` | Console 登录用户名/密码（默认 admin / Qwer@12345） |
| `CONSOLE_DIR` | Console 安装目录（`setup`/`reset` 需要，用于停止/重启 console 进程并清理数据） |
| `NONINTERACTIVE` | 设为任意值即关闭交互提示，缺参直接报错 |

## reset 会清理什么

- Console 本地数据：`data/console/nodes/*`（badger / keystore / `.setup_lock`）、
  `config/system_config.yml`
- 系统集群中的 `.infini_*` 索引、`.infini` 模板、`ilm_.infini_*` ILM 策略
- 然后停止 console 进程、后台重启并重新初始化（日志写入 `log/console-init.out`）

## 单元测试

`test_console_init.sh` 为纯逻辑测试（mock 掉网络/进程调用），不连接真实环境，
可在任意机器运行：

```bash
./test_console_init.sh          # 运行全部 29 个用例
./test_console_init.sh detect   # 只运行名称含 "detect" 的用例（substring 过滤）
```

覆盖：`gen_secret`、`build_cluster_json`（有/无认证）、`detect_state` 四种状态、
`do_setup` 三种分流、easysearch 模板列表、主入口命令分类。通过标志：`通过: 29 失败: 0`。

## 兼容性说明

脚本使用 `set -u`，并针对 macOS 自带 bash 3.2 做了兼容处理（`do_setup` 中临时关闭
`set -u` 以避免命令替换 + `case` 的 unbound 误报）。在 bash 3.2 与 5.x 上均已验证通过。
