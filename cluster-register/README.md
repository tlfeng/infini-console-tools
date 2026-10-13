# Cluster Register（集群自动注册）

向 INFINI Console 自动注册 Elasticsearch / Easysearch 集群，**幂等可重复执行**：
集群已存在时直接打印信息并成功退出（加 `--update` 则更新配置），适合放进初始化脚本。

接口在 Console 1.31.1 上实测核对：注册走 `POST /elasticsearch/`（注意尾部斜杠），
凭据走 `POST /credential`（basic_auth 类型，密码由 Console 加密存储），集群通过
`credential_id` 引用凭据；所有写操作自动携带 Console 1.31+ 要求的防重放 nonce。

## 用法

```bash
# 注册带认证的 ES 集群（自动创建凭据并绑定）
python register_cluster.py -c https://console:9000 -u admin -p 'pw' --insecure \
    --name es7102-test --endpoint http://10.0.0.1:9201 \
    --cluster-user elastic --cluster-pass 'secret'

# 幂等重跑（已存在则跳过，退出码 0）
python register_cluster.py ...（同上）

# 已存在时更新 endpoint/凭据
python register_cluster.py ... --update

# 列出 Console 中已注册的集群
python register_cluster.py --list
```

注册成功后工具会轮询 `_proxy` 直到 Console 元数据刷新、代理可用，并输出
**Console 集群 ID**（供 cluster-config-dump 等其他工具的 `--cluster-id` 使用）。

## 参数

| 参数 | 说明 |
|------|------|
| `--name` | 集群在 Console 中的显示名称（必填） |
| `--endpoint` | 集群 REST 地址 `http(s)://host:port`（必填；**Console 服务器**须可达该地址） |
| `--hosts` | 逗号分隔 host:port 列表（默认取 endpoint） |
| `--cluster-user` / `--cluster-pass` | 集群认证（可选；提供则创建并绑定 Credential） |
| `--no-monitored` | 不开启指标监控（默认注册即监控） |
| `--update` | 已存在时更新 endpoint/凭据 |
| `--wait-timeout` | 等待代理可用秒数（默认 60） |
| `--list` | 仅列出已注册集群 |

## 注意

- 注册后 Console 元数据有约 10s 的刷新周期，工具会自动等待；若超时仍未可用，
  检查 endpoint 可达性与凭据正确性（Console 日志会打印连接错误）
- Console 要求集群健康为 green 才接受 Easysearch 类型的注册连接，
  ES 集群 yellow/red 时注册会成功但监控会报健康异常
