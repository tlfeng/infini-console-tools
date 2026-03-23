#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import getpass
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

try:
    import requests
except ModuleNotFoundError:
    requests = None


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_DIR = SCRIPT_DIR / "config"
DEFAULT_INPUT_PATH = DEFAULT_CONFIG_DIR / "queries.txt"
DEFAULT_AUTH_CONFIG_PATH = DEFAULT_CONFIG_DIR / "auth-config.json"


@dataclass
class RequestCase:
    seq: int
    title: str
    method: str
    path: str
    body_raw: str
    body: Any
    body_is_ndjson: bool


def is_msearch_path(path: str) -> bool:
    normalized = (path or "").split("?", 1)[0].strip().lower().lstrip("/")
    return normalized.endswith("/_msearch") or normalized.endswith("_msearch")


def parse_requests(text: str) -> List[RequestCase]:
    lines = text.splitlines()
    blocks: List[Tuple[int, List[str]]] = []
    current_start: Optional[int] = None
    current_lines: List[str] = []

    for lineno, line in enumerate(lines, start=1):
        if line.strip().startswith("#"):
            if current_lines:
                blocks.append((current_start or lineno, current_lines))
            current_start = lineno
            current_lines = [line]
            continue

        if current_lines:
            current_lines.append(line)
        elif line.strip():
            raise ValueError(f"第 {lineno} 行在首个 # 标题前出现了内容: {line}")

    if current_lines:
        blocks.append((current_start or 1, current_lines))

    if not blocks:
        raise ValueError("没有解析到任何请求，请确认输入中包含 # 标题行。")

    parsed: List[RequestCase] = []
    for idx, (start_line, block_lines) in enumerate(blocks, start=1):
        i = 0
        while i < len(block_lines) and not block_lines[i].strip():
            i += 1
        if i >= len(block_lines) or not block_lines[i].strip().startswith("#"):
            raise ValueError(f"第 {start_line} 行附近缺少请求标题（# 开头）。")
        title = re.sub(r"^#+", "", block_lines[i].strip()).strip()
        if not title:
            title = f"Query {idx}"

        j = i + 1
        while j < len(block_lines) and not block_lines[j].strip():
            j += 1
        if j >= len(block_lines):
            raise ValueError(f"请求 '{title}' 缺少方法行（如 GET index/_search）。")

        method_line = block_lines[j].strip()
        m = re.match(r"^([A-Za-z]+)\s+(.+)$", method_line)
        if not m:
            raise ValueError(f"请求 '{title}' 的方法行格式错误: {method_line}")

        method = m.group(1).upper()
        path = m.group(2).strip()

        body_lines = block_lines[j + 1 :]
        while body_lines and not body_lines[0].strip():
            body_lines = body_lines[1:]
        while body_lines and not body_lines[-1].strip():
            body_lines = body_lines[:-1]
        body_raw = "\n".join(body_lines)

        parsed_body: Any = None
        ndjson = False
        if body_raw.strip():
            if is_msearch_path(path):
                ndjson = True
                parsed_body = body_raw if body_raw.endswith("\n") else f"{body_raw}\n"
            else:
                try:
                    parsed_body = json.loads(body_raw)
                except json.JSONDecodeError as exc:
                    line_no = start_line + j
                    raise ValueError(
                        f"请求 '{title}' 的 JSON body 解析失败（约第 {line_no} 行）: {exc}"
                    ) from exc

        parsed.append(
            RequestCase(
                seq=idx,
                title=title,
                method=method,
                path=path,
                body_raw=body_raw,
                body=parsed_body,
                body_is_ndjson=ndjson,
            )
        )

    return parsed


class ConsoleClient:
    def __init__(
        self,
        base_url: str,
        cluster_id: Optional[str],
        username: str,
        password: str,
        timeout: int = 60,
        verify_ssl: bool = True,
    ):
        self.base_url = base_url.rstrip("/")
        self.cluster_id = (cluster_id or "").strip()
        self.username = username
        self.password = password
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        if requests is None:
            raise RuntimeError("缺少依赖 requests，请先执行: pip install -r requirements.txt")
        self.session = requests.Session()
        self.token: Optional[str] = None

    def login(self) -> None:
        url = f"{self.base_url}/account/login"
        payload = {"username": self.username, "password": self.password}
        headers = {"Content-Type": "application/json"}

        response = self.session.post(
            url,
            headers=headers,
            json=payload,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "ok" or not data.get("access_token"):
            raise RuntimeError(f"登录失败: {data}")

        self.token = f"Bearer {data['access_token']}"

    def proxy_request(
        self,
        method: str,
        path: str,
        body: Any = None,
        content_type: str = "application/json",
    ) -> Dict[str, Any]:
        if not self.token:
            raise RuntimeError("未登录，请先调用 login()。")
        if not self.cluster_id:
            raise RuntimeError("未设置 cluster_id，无法调用 _proxy。")

        url = f"{self.base_url}/elasticsearch/{self.cluster_id}/_proxy"
        params = {"path": path.lstrip("/"), "method": method.upper()}
        headers = {"Authorization": self.token, "Content-Type": content_type}

        data = None
        if body is not None:
            if isinstance(body, str):
                data = body
            else:
                data = json.dumps(body, ensure_ascii=False)

        try:
            response = self.session.post(
                url,
                headers=headers,
                params=params,
                data=data,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
        except requests.RequestException as exc:
            return {
                "ok": False,
                "http_status": 0,
                "error": str(exc),
                "wrapper": {},
                "body": {"error": str(exc)},
                "body_raw": str(exc),
            }

        try:
            wrapper = response.json()
        except ValueError:
            raw_text = response.text
            return {
                "ok": False,
                "http_status": response.status_code,
                "error": "Console 返回了非 JSON 响应",
                "wrapper": {"raw_text": raw_text},
                "body": raw_text,
                "body_raw": raw_text,
            }

        response_body = wrapper.get("response_body")
        parsed_body: Any = None
        if isinstance(response_body, str):
            try:
                parsed_body = json.loads(response_body)
            except ValueError:
                parsed_body = response_body
        else:
            parsed_body = response_body

        return {
            "ok": response.ok and not wrapper.get("error"),
            "http_status": response.status_code,
            "error": wrapper.get("error"),
            "wrapper": wrapper,
            "body": parsed_body,
            "body_raw": response_body,
        }

    def resolve_cluster_id_by_name(self, cluster_name: str) -> str:
        if not self.token:
            raise RuntimeError("未登录，请先调用 login()。")

        name = (cluster_name or "").strip()
        if not name:
            raise ValueError("cluster_name 不能为空。")

        headers = {"Authorization": self.token}
        exact: List[Tuple[str, str]] = []
        case_insensitive: List[Tuple[str, str]] = []
        available_names: List[str] = []

        def add_match(cluster_id: str, display_name: str) -> None:
            cid = (cluster_id or "").strip()
            dname = (display_name or "").strip()
            if not cid or not dname:
                return
            pair = (cid, dname)
            if dname == name:
                if pair not in exact:
                    exact.append(pair)
            elif dname.lower() == name.lower():
                if pair not in case_insensitive:
                    case_insensitive.append(pair)
            if dname not in available_names and len(available_names) < 20:
                available_names.append(dname)

        # 1) 通过 /elasticsearch/_search 按 name 检索
        try:
            response = self.session.get(
                f"{self.base_url}/elasticsearch/_search",
                headers=headers,
                params={"name": name, "size": 200, "from": 0},
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            response.raise_for_status()
            data = response.json()
            hits = data.get("hits", {}).get("hits", [])
            if isinstance(hits, list):
                for hit in hits:
                    if not isinstance(hit, dict):
                        continue
                    cluster_id = str(hit.get("_id", "")).strip()
                    source = hit.get("_source", {})
                    if not cluster_id or not isinstance(source, dict):
                        continue
                    add_match(cluster_id, str(source.get("name", "")).strip())
        except Exception:
            pass

        if len(exact) == 1:
            return exact[0][0]
        if len(exact) > 1:
            details = ", ".join([f"{n}({_id})" for _id, n in exact[:10]])
            raise RuntimeError(f"clusterName 精确匹配到多个集群: {details}")
        if len(case_insensitive) == 1:
            return case_insensitive[0][0]
        if len(case_insensitive) > 1:
            details = ", ".join([f"{n}({_id})" for _id, n in case_insensitive[:10]])
            raise RuntimeError(f"clusterName（忽略大小写）匹配到多个集群: {details}")

        # 2) 通过 /elasticsearch/metadata 做兜底（这里返回的是 cluster_id -> metadata）
        try:
            response = self.session.get(
                f"{self.base_url}/elasticsearch/metadata",
                headers=headers,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            response.raise_for_status()
            metadata = response.json()
            if isinstance(metadata, dict):
                if name in metadata:
                    return name
                for cluster_id, meta in metadata.items():
                    if not isinstance(meta, dict):
                        continue
                    config = meta.get("config")
                    if isinstance(config, dict):
                        add_match(str(cluster_id), str(config.get("name", "")).strip())
        except Exception:
            pass

        if len(exact) == 1:
            return exact[0][0]
        if len(exact) > 1:
            details = ", ".join([f"{n}({_id})" for _id, n in exact[:10]])
            raise RuntimeError(f"clusterName 精确匹配到多个集群: {details}")
        if len(case_insensitive) == 1:
            return case_insensitive[0][0]
        if len(case_insensitive) > 1:
            details = ", ".join([f"{n}({_id})" for _id, n in case_insensitive[:10]])
            raise RuntimeError(f"clusterName（忽略大小写）匹配到多个集群: {details}")

        # 3) 兜底：用户可能填的是 cluster_id，尝试按 id 直接读取
        try:
            response = self.session.get(
                f"{self.base_url}/elasticsearch/{quote(name, safe='-_.:')}",
                headers=headers,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
            if response.status_code == 200:
                return name
        except Exception:
            pass

        hint = f"，当前可见集群名（前20）: {', '.join(available_names)}" if available_names else ""
        raise RuntimeError(f"未找到 clusterName='{name}' 对应的集群{hint}")


def extract_target_names(path: str) -> List[str]:
    clean = (path or "").strip().lstrip("/").split("?", 1)[0]
    if not clean:
        return []

    first_segment = clean.split("/", 1)[0]
    if first_segment.startswith("_"):
        return []

    return [name.strip() for name in first_segment.split(",") if name.strip()]


def resolve_indices(client: ConsoleClient, names: List[str]) -> List[str]:
    resolved: List[str] = []

    for name in names:
        encoded_name = quote(name, safe="*,-_.:")
        alias_result = client.proxy_request("GET", f"{encoded_name}/_alias")
        body = alias_result.get("body")

        if isinstance(body, dict) and body:
            for idx in body.keys():
                if idx not in resolved:
                    resolved.append(idx)
            continue

        if name not in resolved:
            resolved.append(name)

    return resolved


def fetch_index_overview(client: ConsoleClient, index_name: str) -> Dict[str, Any]:
    encoded = quote(index_name, safe="-_.:")
    path = (
        f"_cat/indices/{encoded}"
        "?format=json&h=health,status,index,pri,rep,docs.count,docs.deleted,store.size,pri.store.size"
    )
    result = client.proxy_request("GET", path)
    body = result.get("body")
    if isinstance(body, list) and body:
        return body[0]
    if isinstance(body, dict):
        return body
    return {"error": f"无法获取索引概览: {index_name}"}


def fetch_index_mapping(client: ConsoleClient, index_name: str) -> Any:
    encoded = quote(index_name, safe="-_.:")
    result = client.proxy_request("GET", f"{encoded}/_mapping")
    body = result.get("body")
    if isinstance(body, dict) and index_name in body:
        return body[index_name]
    return body


def flatten_dict_with_prefix(data: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for key, value in data.items():
        full_key = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            result.update(flatten_dict_with_prefix(value, full_key))
        else:
            result[full_key] = value
    return result


def fetch_index_settings(client: ConsoleClient, index_name: str) -> Any:
    encoded = quote(index_name, safe="-_.:")
    result = client.proxy_request("GET", f"{encoded}/_settings")
    body = result.get("body")
    if not isinstance(body, dict):
        return body

    index_settings: Optional[Dict[str, Any]] = None
    if index_name in body and isinstance(body[index_name], dict):
        settings_obj = body[index_name].get("settings")
        if isinstance(settings_obj, dict):
            if isinstance(settings_obj.get("index"), dict):
                index_settings = settings_obj["index"]
            else:
                index_settings = settings_obj

    if not index_settings:
        return body

    flattened = flatten_dict_with_prefix(index_settings, "index")
    return flattened


def format_count(value: Any) -> str:
    if value is None:
        return "-"
    text = str(value).strip()
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        return f"{int(text):,}"
    return text or "-"


def json_pretty(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)


def body_pretty(case: RequestCase) -> str:
    if case.body is None:
        return ""
    if case.body_is_ndjson:
        return case.body_raw.strip()
    if isinstance(case.body, (dict, list)):
        return json_pretty(case.body)
    return str(case.body)


def body_compact(case: RequestCase) -> str:
    if case.body is None:
        return ""
    if case.body_is_ndjson:
        return case.body_raw.strip().replace("\n", "\\n")
    if isinstance(case.body, (dict, list)):
        return json.dumps(case.body, ensure_ascii=False, separators=(",", ":"))
    return str(case.body).replace("\n", "\\n")


def build_report_markdown(report_items: List[Dict[str, Any]]) -> str:
    sections: List[str] = []

    for item in report_items:
        case: RequestCase = item["case"]
        response_body = item["response_body"]
        indices: List[str] = item["indices"]
        overview: Dict[str, Any] = item["overview"]
        mappings: Dict[str, Any] = item["mappings"]
        settings: Dict[str, Any] = item["settings"]

        lines: List[str] = []
        lines.append(f"## {case.title}")
        lines.append("### 请求")
        lines.append("```http")
        lines.append(f"{case.method} {case.path}")
        pretty_body = body_pretty(case)
        if pretty_body:
            lines.append(pretty_body)
        lines.append("```")
        lines.append("")
        lines.append("单行 body 版")
        lines.append("```http")
        lines.append(f"{case.method} {case.path}")
        compact_body = body_compact(case)
        if compact_body:
            lines.append(compact_body)
        lines.append("```")
        lines.append("")

        lines.append("### 响应")
        lines.append("```json")
        if isinstance(response_body, (dict, list)):
            lines.append(json_pretty(response_body))
        else:
            lines.append(str(response_body))
        lines.append("```")
        lines.append("")

        lines.append("### 索引概览")
        if not indices:
            lines.append("请求路径未包含索引，或无法解析索引。")
        else:
            for idx in indices:
                data = overview.get(idx, {})
                lines.append(idx)
                lines.append("")
                lines.append("| 指标 | 值 |")
                lines.append("| --- | --- |")
                if isinstance(data, dict) and data.get("error"):
                    lines.append(f"| 错误 | {data.get('error')} |")
                else:
                    lines.append(f"| 健康状态 | {data.get('health', '-')} |")
                    lines.append(f"| 状态 | {data.get('status', '-')} |")
                    lines.append(f"| 主分片数 | {data.get('pri', '-')} |")
                    lines.append(f"| 副分片数 | {data.get('rep', '-')} |")
                    lines.append(f"| 文档数 | {format_count(data.get('docs.count'))} |")
                    lines.append(f"| 删除文档数 | {format_count(data.get('docs.deleted'))} |")
                    lines.append(f"| 存储大小 | {data.get('store.size', '-')} |")
                    lines.append(f"| 主存储大小 | {data.get('pri.store.size', '-')} |")
                lines.append("")

        lines.append("### 索引Mappings")
        lines.append("```json")
        if not mappings:
            lines.append(json_pretty({"error": "未获取到 mappings"}))
        elif len(mappings) == 1:
            only_value = next(iter(mappings.values()))
            lines.append(json_pretty(only_value))
        else:
            lines.append(json_pretty(mappings))
        lines.append("```")
        lines.append("")

        lines.append("### 索引Settings")
        lines.append("```json")
        if not settings:
            lines.append(json_pretty({"error": "未获取到 settings"}))
        elif len(settings) == 1:
            only_value = next(iter(settings.values()))
            lines.append(json_pretty(only_value))
        else:
            lines.append(json_pretty(settings))
        lines.append("```")

        sections.append("\n".join(lines))

    return "\n\n".join(sections) + "\n"


def load_input_text(input_path: Optional[str]) -> str:
    if input_path == "-":
        return sys.stdin.read()
    if input_path:
        with open(input_path, "r", encoding="utf-8") as f:
            return f.read()
    return sys.stdin.read()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="按 Kibana 风格请求文件生成 ES 查询与索引信息 Markdown 报告。"
    )
    parser.add_argument(
        "--input",
        "-i",
        default=str(DEFAULT_INPUT_PATH),
        help=f"输入文件路径（默认: {DEFAULT_INPUT_PATH}）；传 '-' 则从 stdin 读取。",
    )
    parser.add_argument("--output", "-o", default="query-report.md", help="输出 Markdown 文件路径。")
    parser.add_argument(
        "--auth-config",
        default=str(DEFAULT_AUTH_CONFIG_PATH),
        help="认证配置 JSON 文件路径，支持 baseUrl/auth.username/auth.password/clusterId/clusterName。",
    )
    parser.add_argument("--base-url", default=os.getenv("CONSOLE_BASE_URL"), help="Console 地址，例如 http://localhost:9000")
    parser.add_argument("--cluster-id", default=os.getenv("CONSOLE_CLUSTER_ID"), help="目标集群 ID")
    parser.add_argument("--cluster-name", default=os.getenv("CONSOLE_CLUSTER_NAME"), help="目标集群名称（可替代 cluster-id）")
    parser.add_argument("--username", default=os.getenv("CONSOLE_USERNAME"), help="登录用户名")
    parser.add_argument("--password", default=os.getenv("CONSOLE_PASSWORD"), help="登录密码")
    parser.add_argument("--timeout", type=int, default=60, help="HTTP 超时（秒）")
    parser.add_argument("--insecure", action="store_true", help="关闭 HTTPS 证书校验")
    return parser.parse_args()


def require_arg(value: Optional[str], name: str) -> str:
    if value:
        return value
    raise ValueError(f"缺少参数: {name}")


def load_auth_config(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError as exc:
        raise ValueError(f"认证配置文件不存在: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"认证配置文件 JSON 格式错误: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError("认证配置文件根节点必须是 JSON 对象。")
    return data


def main() -> int:
    args = parse_args()
    try:
        input_text = load_input_text(args.input)
        cases = parse_requests(input_text)
    except Exception as exc:
        print(f"输入解析失败: {exc}", file=sys.stderr)
        return 1

    auth_data: Dict[str, Any] = {}
    if args.auth_config:
        try:
            auth_data = load_auth_config(args.auth_config)
        except Exception as exc:
            print(str(exc), file=sys.stderr)
            return 1

    auth_obj = auth_data.get("auth") if isinstance(auth_data.get("auth"), dict) else {}
    try:
        base_url = require_arg(
            args.base_url or auth_data.get("baseUrl"),
            "--base-url 或 CONSOLE_BASE_URL 或 --auth-config.baseUrl",
        )
        cluster_id = (args.cluster_id or auth_data.get("clusterId") or "").strip()
        cluster_name = (args.cluster_name or auth_data.get("clusterName") or "").strip()
        if not cluster_id and not cluster_name:
            raise ValueError(
                "缺少集群标识：请提供 clusterId 或 clusterName（可通过命令行、环境变量或 --auth-config）。"
            )
        username = require_arg(
            args.username or auth_obj.get("username"),
            "--username 或 CONSOLE_USERNAME 或 --auth-config.auth.username",
        )
        password = args.password or auth_obj.get("password") or getpass.getpass("Console 密码: ")
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    client = ConsoleClient(
        base_url=base_url,
        cluster_id=cluster_id,
        username=username,
        password=password,
        timeout=args.timeout,
        verify_ssl=not args.insecure,
    )

    try:
        client.login()
    except Exception as exc:
        print(f"登录失败: {exc}", file=sys.stderr)
        return 1

    if not cluster_id:
        try:
            cluster_id = client.resolve_cluster_id_by_name(cluster_name)
            client.cluster_id = cluster_id
            print(f"已根据 clusterName 解析 clusterId: {cluster_name} -> {cluster_id}")
        except Exception as exc:
            print(f"解析 clusterName 失败: {exc}", file=sys.stderr)
            return 1

    report_items: List[Dict[str, Any]] = []
    for case in cases:
        content_type = "application/x-ndjson" if case.body_is_ndjson else "application/json"
        result = client.proxy_request(case.method, case.path, case.body, content_type=content_type)
        response_body = result["body"]
        if result.get("error"):
            response_body = {
                "error": result.get("error"),
                "http_status": result.get("http_status"),
                "wrapper": result.get("wrapper"),
                "response_body": result.get("body"),
            }

        target_names = extract_target_names(case.path)
        indices = resolve_indices(client, target_names) if target_names else []

        overview: Dict[str, Any] = {}
        mappings: Dict[str, Any] = {}
        settings: Dict[str, Any] = {}
        for idx in indices:
            overview[idx] = fetch_index_overview(client, idx)
            mappings[idx] = fetch_index_mapping(client, idx)
            settings[idx] = fetch_index_settings(client, idx)

        report_items.append(
            {
                "case": case,
                "response_body": response_body,
                "indices": indices,
                "overview": overview,
                "mappings": mappings,
                "settings": settings,
            }
        )

    markdown = build_report_markdown(report_items)
    out_path = os.path.abspath(args.output)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(markdown)

    print(f"已生成报告: {out_path}")
    print(f"请求数量: {len(cases)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
