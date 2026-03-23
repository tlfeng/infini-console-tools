# ES Query Markdown Reporter

根据 Kibana 风格请求文本，自动执行查询并输出 Markdown 报告，包含：

- 查询请求（多行 + 单行 body）
- 查询完整响应
- 索引概览（支持 alias 解析到真实索引）
- 索引 mappings
- 索引 settings（扁平化为 `index.xxx` 键）

## 1. 安装依赖

```bash
cd es-query-report-py
pip install -r requirements.txt
```

## 2. 准备输入文件

默认输入文件在 `config/queries.txt`（项目已内置），示例内容：

```text
#1 adv&act
GET movies/_search
{"query":{"bool":{"must":[{"match":{"keywords":"Adventure"}},{"range":{"rating":{"gte":5}}}],"should":[{"match":{"directors":"Ernest B. Schoedsack"}}],"must_not":[{"term":{"revenue":0}}]}}}

#2 king kong
GET movies/_search
{"query":{"bool":{"must":[{"match":{"keywords":"Adventure"}},{"range":{"rating":{"gte":5}}}],"should":[{"match":{"directors":"Ernest B. Schoedsack"}}],"must_not":[{"term":{"revenue":0}}]}}}
```

## 3. 运行

默认认证文件在 `config/auth-config.json`（项目已内置），格式如下：

```json
{
  "baseUrl": "http://localhost:9000",
  "clusterName": "your_cluster_name",
  "auth": {
    "username": "admin",
    "password": "Qwer1234"
  }
}
```

```bash
python es_query_report.py
```

上面命令会默认读取：

- `config/queries.txt`
- `config/auth-config.json`

也可以通过命令行覆盖默认路径或字段：

```bash
python es_query_report.py \
  --input /path/to/queries.txt \
  --output report.md \
  --auth-config /path/to/auth-config.json \
  --cluster-name another_cluster_name
```

也可以完全使用环境变量（并保留默认 `config` 输入文件）：

```bash
export CONSOLE_BASE_URL=http://localhost:9000
export CONSOLE_CLUSTER_NAME=<your_cluster_name>
export CONSOLE_USERNAME=admin
export CONSOLE_PASSWORD='<your_password>'

python es_query_report.py --output report.md
```

## 4. 输出

输出文件为 Markdown，例如：

- `## 1 adv&act`
- `### 请求`
- `### 响应`
- `### 索引概览`
- `### 索引Mappings`
- `### 索引Settings`

## 参数说明

- `--input/-i`：输入文件路径（默认 `config/queries.txt`）；传 `-` 则从标准输入读取
- `--output/-o`：输出 Markdown 文件路径（默认 `query-report.md`）
- `--auth-config`：认证配置 JSON 文件（`baseUrl/auth.username/auth.password/clusterId/clusterName`）
- `--base-url`：Console 地址
- `--cluster-id`：集群 ID（优先级高于 `cluster-name`）
- `--cluster-name`：集群名称（当未提供 `cluster-id` 时自动解析为 ID）
- `--username`：用户名
- `--password`：密码（不传时会提示输入）
- `--timeout`：请求超时时间，默认 60 秒
- `--insecure`：关闭 HTTPS 证书校验

参数优先级：命令行参数 > 环境变量 > `--auth-config` 文件。

默认路径（未传参时）：

- `--input` 默认 `config/queries.txt`
- `--auth-config` 默认 `config/auth-config.json`
