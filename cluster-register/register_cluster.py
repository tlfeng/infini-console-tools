#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cluster Register - 向 INFINI Console 自动注册 Elasticsearch/Easysearch 集群

把一个 ES/Easysearch 集群注册进 Console（多集群管理），幂等可重复执行：
- 集群已存在且未加 --update 时，直接打印现有信息并成功退出
- 提供了 --cluster-user/--cluster-pass 时自动创建 Credential（basic_auth）
  并绑定到集群（Console 1.31+ 的凭据与集群是分开的两个对象）
- 注册后轮询 _proxy 直到 Console 元数据刷新、代理可用

接口说明（在 Console 1.31.1 上实测核对）：
- POST   /elasticsearch/            注册集群（注意带尾部斜杠）
- GET    /elasticsearch/<id>        读取集群配置（返回 {_id, _source}）
- PUT    /elasticsearch/<id>        更新集群配置（如绑定 credential_id）
- POST   /credential                创建凭据 {type:"basic_auth", payload:{...}}
- 所有写操作需要防重放 nonce，由 common.console_client.write_request 自动处理

用法:
  python register_cluster.py -c https://console:9000 -u admin -p pw \\
      --name es7102-test --endpoint http://10.0.0.1:9201 \\
      --cluster-user elastic --cluster-pass secret

  # 幂等重跑（已存在则直接跳过）
  python register_cluster.py ... （同上）

  # 已存在时更新 endpoint/凭据
  python register_cluster.py ... --update

  # 仅列出 Console 中已注册的集群
  python register_cluster.py --list
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent))
from common.console_client import (
    ConsoleClient,
    ConsoleAPIError,
    ConsoleAuthError,
    create_authenticated_client,
)
from common.config import add_common_args, load_and_merge_config, get_config_value


# ---------------------------------------------------------------------------
# 纯函数（可单测）
# ---------------------------------------------------------------------------

def split_endpoint(endpoint: str) -> Dict[str, Any]:
    """
    从 endpoint 拆出 schema 与 host:port。

    http://127.0.0.1:9201 -> {"schema": "http", "host": "127.0.0.1:9201"}
    """
    parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
    if not parsed.hostname or not parsed.port:
        raise ValueError(f"endpoint 必须形如 http(s)://host:port，收到: {endpoint}")
    return {"schema": parsed.scheme, "host": f"{parsed.hostname}:{parsed.port}"}


def build_register_body(name: str, endpoint: str, *,
                        enabled: bool = True,
                        monitored: bool = True,
                        hosts: Optional[List[str]] = None) -> Dict[str, Any]:
    """构造 POST /elasticsearch/ 的注册请求体"""
    parts = split_endpoint(endpoint)
    body: Dict[str, Any] = {
        "name": name,
        "endpoint": endpoint,
        "hosts": hosts or [parts["host"]],
        "schema": parts["schema"],
        "enabled": enabled,
        "monitored": monitored,
    }
    return body


def build_credential_body(name: str, username: str, password: str) -> Dict[str, Any]:
    """构造 POST /credential 的凭据请求体（密码由 Console 负责加密存储）"""
    return {
        "name": name,
        "type": "basic_auth",
        "tags": ["ES"],
        "payload": {"basic_auth": {"username": username, "password": password}},
    }


def find_existing(clusters: List[Dict[str, Any]], name: str,
                  endpoint: str = "") -> Optional[Dict[str, Any]]:
    """
    按名称（优先）或 endpoint 查找已注册集群，找不到返回 None。
    """
    for c in clusters:
        if str(c.get("name", "")) == name:
            return c
    if endpoint:
        for c in clusters:
            if str(c.get("endpoint", "")).rstrip("/") == endpoint.rstrip("/"):
                return c
    return None


# ---------------------------------------------------------------------------
# 注册器
# ---------------------------------------------------------------------------

class ClusterRegistrar:
    """封装注册/更新/凭据绑定/连通性等待"""

    def __init__(self, client: ConsoleClient):
        self.client = client

    def list_clusters(self) -> List[Dict[str, Any]]:
        return self.client.get_clusters()

    def register(self, body: Dict[str, Any]) -> str:
        """注册集群，返回 cluster_id"""
        resp = self.client.write_request("/elasticsearch/", "POST", body)
        cluster_id = resp.get("_id") or resp.get("id")
        if not cluster_id:
            raise ConsoleAPIError(f"注册响应缺少 _id: {resp}")
        return cluster_id

    def get_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """读取集群配置（返回 _source 内容）"""
        resp = self.client.request(f"/elasticsearch/{cluster_id}", "GET")
        return resp.get("_source", resp) if isinstance(resp, dict) else {}

    def update_cluster(self, cluster_id: str, source: Dict[str, Any]) -> None:
        self.client.write_request(
            f"/elasticsearch/{cluster_id}", "PUT", source)

    def create_credential(self, name: str, username: str, password: str) -> str:
        """创建 basic_auth 凭据，返回 credential_id"""
        resp = self.client.write_request(
            "/credential", "POST", build_credential_body(name, username, password))
        cred_id = resp.get("_id") or resp.get("id")
        if not cred_id:
            raise ConsoleAPIError(f"凭据创建响应缺少 _id: {resp}")
        return cred_id

    def bind_credential(self, cluster_id: str, credential_id: str) -> None:
        """把凭据绑定到集群（读取-修改-写回）"""
        source = self.get_cluster(cluster_id)
        source["credential_id"] = credential_id
        self.update_cluster(cluster_id, source)

    def wait_proxy_ready(self, cluster_id: str, timeout_seconds: int = 60,
                         interval: int = 5) -> Optional[str]:
        """
        轮询 _proxy 直到 Console 元数据刷新、代理可用。

        返回成功时为 "集群名 (v版本)"，超时返回 None。
        """
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                root = self.client.proxy_request(cluster_id, "GET", "/")
                cname = root.get("cluster_name", cluster_id) \
                    if isinstance(root, dict) else cluster_id
                version = (root.get("version") or {}).get("number", "?") \
                    if isinstance(root, dict) else "?"
                return f"{cname} (v{version})"
            except ConsoleAPIError:
                time.sleep(interval)
        return None


# ---------------------------------------------------------------------------
# 命令行入口
# ---------------------------------------------------------------------------

def print_clusters(clusters: List[Dict[str, Any]]) -> None:
    print(f"\nConsole 中已注册 {len(clusters)} 个集群:")
    print(f"{'ID':<30} {'名称':<34} {'endpoint':<34} 监控")
    for c in clusters:
        print(f"{str(c.get('id', '')):<30} {str(c.get('name', '')):<34} "
              f"{str(c.get('endpoint', '')):<34} {'开' if c.get('monitored') else '关'}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Cluster Register - 向 INFINI Console 注册 ES/Easysearch 集群"
                    "（幂等，可重复执行）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 注册带认证的 ES 集群
  python register_cluster.py -c https://console:9000 -u admin -p 'pw' --insecure \\
      --name es7102-test --endpoint http://10.0.0.1:9201 \\
      --cluster-user elastic --cluster-pass 'secret'

  # 幂等重跑：已存在则打印信息后成功退出；--update 则更新 endpoint/凭据
  python register_cluster.py ... --update

  # 列出已注册集群
  python register_cluster.py --list

Environment Variables:
  CONSOLE_URL / CONSOLE_USERNAME / CONSOLE_PASSWORD
        """,
    )

    parser = add_common_args(parser)

    reg_group = parser.add_argument_group("注册参数")
    reg_group.add_argument(
        "--name", default="", help="集群在 Console 中的显示名称")
    reg_group.add_argument(
        "--endpoint", default="",
        help="集群 REST 地址，形如 http(s)://host:port（Console 服务器须可达）")
    reg_group.add_argument(
        "--hosts", default="",
        help="逗号分隔的 host:port 列表（默认取 endpoint 的 host:port）")
    reg_group.add_argument(
        "--cluster-user", default="",
        help="集群认证用户名（可选；提供时自动创建 Credential 并绑定）")
    reg_group.add_argument(
        "--cluster-pass", default="",
        help="集群认证密码")
    reg_group.add_argument(
        "--no-monitored", action="store_true",
        help="不开启指标监控（默认注册即监控）")
    reg_group.add_argument(
        "--update", action="store_true",
        help="集群已存在时更新 endpoint/凭据（不加则幂等跳过）")
    reg_group.add_argument(
        "--wait-timeout", type=int, default=60,
        help="注册后等待代理可用的秒数 (默认: 60)")
    reg_group.add_argument(
        "--list", action="store_true", help="仅列出已注册集群后退出")
    return parser.parse_args()


def main():
    args = parse_args()
    config, _ = load_and_merge_config(args)

    console_url = get_config_value(
        args.console, config.get("consoleUrl"), "CONSOLE_URL", "http://localhost:9000")
    username = get_config_value(
        args.username, config.get("auth", {}).get("username"), "CONSOLE_USERNAME", "")
    password = get_config_value(
        args.password, config.get("auth", {}).get("password"), "CONSOLE_PASSWORD", "")
    timeout = int(get_config_value(
        str(args.timeout), str(config.get("timeout")), "CONSOLE_TIMEOUT", "60"))
    insecure = args.insecure or config.get("insecure", False)

    try:
        client = create_authenticated_client(
            console_url, username, password,
            timeout=timeout, verify_ssl=not insecure)
    except ConsoleAuthError as e:
        print(f"登录失败: {e}")
        sys.exit(1)

    registrar = ClusterRegistrar(client)

    if args.list:
        print_clusters(registrar.list_clusters())
        return

    # ---- 参数校验 ----
    if not args.name or not args.endpoint:
        print("必须提供 --name 与 --endpoint（或使用 --list 查看集群）")
        sys.exit(1)
    try:
        body = build_register_body(
            args.name, args.endpoint,
            enabled=True,
            monitored=not args.no_monitored,
            hosts=[h.strip() for h in args.hosts.split(",") if h.strip()] or None,
        )
    except ValueError as e:
        print(f"参数错误: {e}")
        sys.exit(1)

    if args.cluster_user and not args.cluster_pass:
        import getpass
        args.cluster_pass = getpass.getpass(
            f"请输入集群用户 {args.cluster_user} 的密码: ")

    try:
        existing = find_existing(registrar.list_clusters(), args.name, args.endpoint)
    except ConsoleAPIError as e:
        print(f"获取集群列表失败: {e}")
        sys.exit(1)

    # ---- 已注册：幂等跳过或更新 ----
    if existing:
        print(f"集群已注册: {existing['name']} (id={existing['id']}, "
              f"endpoint={existing.get('endpoint', '')})")
        if not args.update:
            print("如需更新 endpoint/凭据，请加 --update")
            return
        cluster_id = existing["id"]
        source = registrar.get_cluster(cluster_id)
        source.update({"endpoint": body["endpoint"], "hosts": body["hosts"],
                       "schema": body["schema"]})
        registrar.update_cluster(cluster_id, source)
        print(f"已更新集群 endpoint: {body['endpoint']}")
    else:
        cluster_id = registrar.register(body)
        print(f"集群注册成功: {args.name} (id={cluster_id})")

    # ---- 凭据创建与绑定 ----
    if args.cluster_user:
        cred_id = registrar.create_credential(
            f"{args.name} credential", args.cluster_user, args.cluster_pass)
        registrar.bind_credential(cluster_id, cred_id)
        print(f"凭据已创建并绑定: credential_id={cred_id}")

    # ---- 等待代理可用 ----
    print("等待 Console 元数据刷新（代理可用）...")
    ready = registrar.wait_proxy_ready(cluster_id, args.wait_timeout)
    if ready:
        print(f"代理验证通过: {ready}")
        print(f"完成。Console 集群 ID: {cluster_id}")
    else:
        print(f"警告: {args.wait_timeout}s 内代理仍不可用。"
              "集群可能尚未连通（检查 endpoint/凭据），Console 通常会在元数据"
              "刷新后自动恢复；也可重启 Console 后重试。")
        sys.exit(1)


if __name__ == "__main__":
    main()
