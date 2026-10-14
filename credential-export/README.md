# Credential Export（集群凭据提取）

以 Console 管理员身份，提取 Console 中所有已注册集群的认证凭据（basic_auth
用户名/密码明文），用于**凭据恢复与 Console 迁移**：

- Console 把集群凭据加密存储（credential_secret 仅在初始化时生成一次），
  丢失后无法解密，迁移新 Console 时无法恢复各集群的认证信息——本工具解决
  这个"密钥只有一份"的问题
- 输出为带明文凭据的 JSON（权限自动收紧为 0600），可配合 cluster-register
  把凭据重新注册到新 Console

## 工作原理（Console 1.31.1 实测）

Console 的 `_proxy` 在集群侧返回非 2xx 时，会把发给集群的完整请求头回显在
错误包装的 `request_header` 字段里，其中 `Authorization: Basic <base64>`
解码即明文 `用户名:密码`。工具对每个集群发一个**必然 404** 的探测请求：

```
GET /_cluster/health/__cred_probe_<随机hex>__   → 404 index_not_found
```

（索引不存在必然 404，与 license、插件、集群健康状态无关。）然后从错误包装
中提取并解码凭据。集群未绑定凭据（匿名）时请求头里没有 Authorization，
会如实标记为 `未绑定凭据`。

## 用法

```bash
# 提取所有非系统集群（终端密码打码，JSON 文件为明文）
python export_credentials.py -c https://console:9000 -u admin -p pw --insecure

# 终端显示明文；包含系统集群；指定输出文件
python export_credentials.py ... --show --include-system -o my-credentials.json
```

输出文件结构：

```json
{
  "tool": "credential-export",
  "collected_at": "2026-10-14 12:00:00",
  "console_url": "https://console:9000",
  "warning": "本文件包含明文凭据，请妥善保管，勿提交代码库",
  "clusters": [
    {"cluster_id": "db6v6diflsa3ha8ohflg", "cluster_name": "es7102-test",
     "endpoint": "http://127.0.0.1:9201", "status": "ok",
     "username": "cfgdump", "password": "...", "http_status": 404, "detail": ""}
  ]
}
```

`status` 取值：`ok`（已提取）/ `no_credential`（集群未绑定凭据）/
`no_header`（代理响应不含请求头，Console 版本已抹除回显，机制失效）/
`error`（请求层面失败）。

## 安全约束（务必阅读）

1. **需要 Console 登录**，且账号对目标集群有 `_proxy` 权限——它提取的是
   操作者自己有权管理的集群凭据，无法越权获取未授权集群
2. 输出是明文口令：文件权限 0600、勿提交代码库、勿随意分发；终端默认打码
3. 该机制依赖 Console 代理错误中的请求头回显；若 INFINI 后续版本抹除该
   回显，工具会明确报 `机制不可用`（这也是个值得向厂商反馈的敏感信息
   泄漏点——本工具的存在不代表该行为是推荐依赖的接口）
