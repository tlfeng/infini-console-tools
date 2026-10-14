#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Credential Export - 提取 Console 中所有集群的认证凭据

用途：Console 的集群凭据（basic_auth 用户名/密码）加密存储在 Console 内部
（credential_secret 只在初始化时生成一次），一旦丢失就无法解密迁移。本工具
为 Console 管理员提供凭据恢复/迁移手段：以 Console 管理员身份登录后，把
每个已注册集群的凭据提取为明文，用于备份、迁移到新 Console 或核对。

工作原理（在 Console 1.31.1 上实测）：
Console 的 _proxy 在集群侧返回非 2xx 时，会把发给集群的完整请求头回显在
错误包装的 request_header 字段里，其中 Authorization: Basic <base64> 解码
即是明文 用户名:密码。工具对每个集群发送一个必然 404 的探测请求
（GET /_cluster/health/__cred_probe_<随机>__，索引不存在与 license/插件无关），
从错误包装中提取并解码凭据。

安全约束与提示：
- 需要 Console 管理员（或有对应集群 _proxy 权限的账号）登录才能运行——
  它提取的是你自己有权管理的集群凭据，不是越权工具
- 输出文件为明文凭据，默认权限 0600；请勿提交到代码库或随意分发
- 密码默认在终端打码，加 --show 才显示明文
- 若 Console 后续版本在代理错误中抹除请求头，本机制将失效（工具会明确提示）

用法:
  python export_credentials.py -c https://console:9000 -u admin -p pw --insecure

  # 显示明文密码（默认打码）
  python export_credentials.py ... --show

  # 包含 Console 系统集群（默认跳过）
  python export_credentials.py ... --include-system

  # 指定输出文件（默认 ./credential_export_<时间戳>.json）
  python export_credentials.py ... -o my-credentials.json
"""

import argparse
import base64
import json
import os
import re
import secrets
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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

AUTH_HEADER_RE = re.compile(
    r"(?i)authorization\s*:\s*basic\s+([A-Za-z0-9+/=_-]+)")


def build_probe_path() -> str:
    """构造必然 404 的探测路径：health 子 API + 不存在的索引名"""
    return f"/_cluster/health/__cred_probe_{secrets.token_hex(4)}__"


def extract_basic_auth(request_header: str) -> Optional[Tuple[str, str]]:
    """
    从代理回显的请求头文本中提取 Basic 凭据，解码为 (username, password)。

    没有凭据头/解码失败时返回 None。
    """
    if not request_header:
        return None
    m = AUTH_HEADER_RE.search(request_header)
    if not m:
        return None
    try:
        decoded = base64.b64decode(m.group(1), validate=True).decode("utf-8")
    except Exception:
        return None
    if ":" not in decoded:
        return None
    username, password = decoded.split(":", 1)
    return username, password


def mask_password(password: str) -> str:
    """终端展示用：密码打码，只保留首字符"""
    if not password:
        return "(空)"
    return password[0] + "***" if len(password) > 1 else "***"


def mask_authorization_header(request_header: str) -> str:
    """把请求头文本中的 Basic 凭据打码，用于安全展示原始响应"""
    def _mask(m):
        return f"Authorization: Basic {mask_password(m.group(1))}"
    return AUTH_HEADER_RE.sub(_mask, request_header or "")


# ---------------------------------------------------------------------------
# 提取器
# ---------------------------------------------------------------------------

class CredentialExporter:
    """提取 Console 中所有集群的凭据"""

    def __init__(self, client: ConsoleClient, include_system: bool = False,
                 show: bool = False):
        self.client = client
        self.include_system = include_system
        self.show = show

    def load_targets(self) -> List[Dict[str, Any]]:
        """集群列表，默认跳过系统集群"""
        clusters = self.client.get_clusters()
        if self.include_system:
            return clusters
        return [c for c in clusters
                if not ConsoleClient.is_system_cluster(
                    str(c.get("id", "")), str(c.get("name", "")))]

    def extract_one(self, cluster_id: str) -> Dict[str, Any]:
        """
        对单个集群探测并提取凭据。

        返回: {"status": "ok"|"no_credential"|"no_header"|"error",
               "username", "password", "http_status", "detail"}
        """
        probe_path = build_probe_path()
        try:
            status, wrapper = self.client.proxy_raw(cluster_id, "GET", probe_path)
        except ConsoleAPIError as e:
            return {"status": "error", "username": "", "password": "",
                    "http_status": None, "detail": str(e)[:200]}

        if not isinstance(wrapper, dict):
            return {"status": "error", "username": "", "password": "",
                    "http_status": status, "detail": f"代理响应不是 JSON: {wrapper}"}

        request_header = wrapper.get("request_header")
        if not request_header:
            # 2xx 或代理包装不含请求头：机制不可用（或探测路径意外命中）
            return {"status": "no_header", "username": "", "password": "",
                    "http_status": status,
                    "detail": "代理响应未包含 request_header，"
                              "该 Console 版本可能已抹除请求头回显"}

        creds = extract_basic_auth(request_header)
        if creds is None:
            return {"status": "no_credential", "username": "", "password": "",
                    "http_status": status,
                    "detail": "请求头中没有 Authorization: Basic，"
                              "该集群未绑定凭据（匿名访问）"}
        return {"status": "ok", "username": creds[0], "password": creds[1],
                "http_status": status, "detail": ""}

    def export_all(self) -> List[Dict[str, Any]]:
        """提取全部目标集群，返回按集群组织的记录列表"""
        records: List[Dict[str, Any]] = []
        targets = self.load_targets()
        if not targets:
            print("Console 中没有可提取的集群")
            return records

        print(f"共 {len(targets)} 个集群待提取（顺序探测）...")
        for c in targets:
            cluster_id = str(c.get("id", ""))
            name = str(c.get("name", "")) or cluster_id
            endpoint = str(c.get("endpoint", ""))
            result = self.extract_one(cluster_id)
            record = {
                "cluster_id": cluster_id,
                "cluster_name": name,
                "endpoint": endpoint,
                "status": result["status"],
                "username": result["username"],
                "password": result["password"],
                "http_status": result["http_status"],
                "detail": result["detail"],
            }
            records.append(record)

            if result["status"] == "ok":
                pw = (result["password"] if self.show
                      else mask_password(result["password"]))
                print(f"  [OK]   {name:<30} {result['username']}:{pw}")
            elif result["status"] == "no_credential":
                print(f"  [匿名] {name:<30} 未绑定凭据")
            elif result["status"] == "no_header":
                print(f"  [跳过] {name:<30} {result['detail']}")
            else:
                print(f"  [失败] {name:<30} {result['detail']}")
        return records


STATUS_LABELS = {
    "ok": "已提取",
    "no_credential": "未绑定凭据",
    "no_header": "机制不可用",
    "error": "提取失败",
}


def save_records(records: List[Dict[str, Any]], output: Path,
                 console_url: str) -> None:
    """明文凭据落盘，权限收紧到 0600"""
    payload = {
        "tool": "credential-export",
        "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "console_url": console_url,
        "warning": "本文件包含明文凭据，请妥善保管，勿提交代码库",
        "clusters": records,
    }
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.chmod(output, 0o600)


# ---------------------------------------------------------------------------
# 命令行入口
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Credential Export - 提取 Console 中所有集群的认证凭据"
                    "（管理员 own 的凭据恢复/迁移工具）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 提取所有非系统集群的凭据，密码打码展示，明文写入 JSON
  python export_credentials.py -c https://console:9000 -u admin -p pw --insecure

  # 终端显示明文 + 包含系统集群
  python export_credentials.py ... --show --include-system

Environment Variables:
  CONSOLE_URL / CONSOLE_USERNAME / CONSOLE_PASSWORD
        """,
    )
    parser = add_common_args(parser)
    parser.add_argument(
        "--include-system", action="store_true",
        help="包含 Console 系统集群（默认跳过）")
    parser.add_argument(
        "--show", action="store_true",
        help="终端显示明文密码（默认打码；JSON 文件始终是明文）")
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

    exporter = CredentialExporter(client, include_system=args.include_system,
                                  show=args.show)
    records = exporter.export_all()
    if not records:
        return

    # ---- 落盘 ----
    output = Path(args.output) if args.output else Path(
        f"credential_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    save_records(records, output, console_url)

    ok_count = sum(1 for r in records if r["status"] == "ok")
    nc_count = sum(1 for r in records if r["status"] == "no_credential")
    print()
    print("=" * 64)
    print(f"提取完成: 成功 {ok_count} / 未绑定凭据 {nc_count} / "
          f"失败或跳过 {len(records) - ok_count - nc_count}")
    print(f"输出文件: {output}（明文凭据，权限 0600，请妥善保管）")
    print("=" * 64)

    sys.exit(0 if ok_count > 0 else 1)


if __name__ == "__main__":
    main()
