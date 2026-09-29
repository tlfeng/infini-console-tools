#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
INFINI Console API 客户端公共模块

提供统一的 Console API 访问接口，包括：
- JWT 认证
- 集群列表获取
- _proxy API 调用
- 索引信息查询
- 便捷的认证辅助函数
"""

import getpass
import hashlib
import hmac
import http.client
import json
import ssl
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from typing import Dict, List, Optional, Any

# 瞬时性网络错误的最大请求次数（1 次原始请求 + 2 次重试）
MAX_REQUEST_ATTEMPTS = 3

# 可重试的瞬时网络错误（连接被重置/截断/超时等）
_TRANSIENT_ERRORS = (
    urllib.error.URLError,
    ConnectionError,
    http.client.IncompleteRead,
    http.client.RemoteDisconnected,
    TimeoutError,
    OSError,
)


class ConsoleAuthError(Exception):
    """认证失败异常"""
    pass


class ConsoleAPIError(Exception):
    """API 调用异常"""
    pass


class ConsoleClient:
    """INFINI Console API 客户端"""

    def __init__(
        self,
        base_url: str,
        username: str = "",
        password: str = "",
        timeout: int = 60,
        verify_ssl: bool = False,
    ):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout = timeout
        self.token: Optional[str] = None
        self.login_method: Optional[str] = None  # "challenge"（1.31+）或 "plaintext"（旧版本）

        # SSL 上下文
        self.ssl_context = ssl.create_default_context()
        if not verify_ssl:
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = ssl.CERT_NONE

    @staticmethod
    def _decode_response(response) -> str:
        """读取并解码响应体，编码异常时输出可定位问题的诊断信息"""
        raw = response.read()
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as e:
            content_encoding = response.headers.get("Content-Encoding", "none")
            content_type = response.headers.get("Content-Type", "unknown")
            raise ConsoleAPIError(
                "响应体不是合法的 UTF-8，可能被中间设备（防火墙/代理）改写或压缩: "
                f"Content-Encoding={content_encoding}, Content-Type={content_type}, "
                f"size={len(raw)} bytes, 前64字节={raw[:64]!r}。原始错误: {e}"
            ) from e

    def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """发送 HTTP 请求，瞬时网络错误自动重试"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        req = urllib.request.Request(url, method=method, data=data)

        # 设置默认 headers
        req.add_header("Content-Type", "application/json")
        if headers:
            for key, value in headers.items():
                req.add_header(key, value)

        # 添加认证头
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")

        last_error = None
        for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
            try:
                with urllib.request.urlopen(
                    req, context=self.ssl_context, timeout=self.timeout
                ) as response:
                    response_data = self._decode_response(response)
                    return json.loads(response_data) if response_data else {}
            except urllib.error.HTTPError as e:
                error_body = e.read().decode("utf-8", errors="replace")
                # 5xx/429 视为瞬时错误重试，其余（4xx 等）直接抛出
                if e.code in (429, 500, 502, 503, 504) and attempt < MAX_REQUEST_ATTEMPTS:
                    last_error = e
                    time.sleep(2 ** (attempt - 1))
                    continue
                raise ConsoleAPIError(f"HTTP {e.code}: {error_body}")
            except _TRANSIENT_ERRORS as e:
                last_error = e
                if attempt < MAX_REQUEST_ATTEMPTS:
                    time.sleep(2 ** (attempt - 1))
                    continue
                break
            except UnicodeDecodeError as e:
                # 理论上不会到这里（_decode_response 已转换），保险起见不重试直接抛出
                raise ConsoleAPIError(f"响应解码失败: {e}")
            except Exception as e:
                raise ConsoleAPIError(f"Request failed: {str(e)}")

        raise ConsoleAPIError(
            f"Request failed after {MAX_REQUEST_ATTEMPTS} attempts: {last_error}"
        )

    def _raw_request(
        self,
        method: str,
        endpoint: str,
        body: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> tuple:
        """底层请求，返回 (status, parsed_body)；HTTP 错误码原样返回不抛异常，
        供登录流程按状态码降级。瞬时网络错误自动重试。"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        data = json.dumps(body).encode("utf-8") if body is not None else None

        last_error = None
        for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
            req = urllib.request.Request(url, data=data, method=method)
            req.add_header("Content-Type", "application/json")
            if headers:
                for key, value in headers.items():
                    req.add_header(key, value)
            try:
                with urllib.request.urlopen(
                    req, context=self.ssl_context, timeout=self.timeout
                ) as response:
                    raw = response.read()
                    return response.status, json.loads(raw) if raw else {}
            except urllib.error.HTTPError as e:
                raw = e.read().decode("utf-8", errors="replace")
                try:
                    return e.code, json.loads(raw)
                except json.JSONDecodeError:
                    return e.code, raw
            except _TRANSIENT_ERRORS as e:
                last_error = e
                if attempt < MAX_REQUEST_ATTEMPTS:
                    time.sleep(2 ** (attempt - 1))
                    continue
                raise ConsoleAPIError(
                    f"Request failed after {MAX_REQUEST_ATTEMPTS} attempts: {last_error}"
                )
        raise ConsoleAPIError(f"Request failed: {last_error}")

    @staticmethod
    def _extract_token(result: Any) -> Optional[str]:
        """从登录响应的不同字段结构中提取 token，找不到返回 None"""
        if not isinstance(result, dict):
            return None
        token = result.get("token") or result.get("access_token")
        if not token and isinstance(result.get("data"), dict):
            token = result["data"].get("token") or result["data"].get("access_token")
        return token

    def login(self) -> bool:
        """登录获取 JWT Token。
        Console 1.31+ 为挑战-响应登录（密码不明文出网，带防重放 nonce）；
        挑战接口不可用（旧版本）时自动降级为明文登录。"""
        if not self.username or not self.password:
            return False

        try:
            if self._login_challenge():
                return True
        except ConsoleAPIError:
            raise
        except Exception:
            pass  # 挑战接口不存在（旧版本 Console），降级明文
        return self._login_plaintext()

    def _login_challenge(self) -> bool:
        """Console 1.31+ 挑战-响应登录，对齐前端 buildPasswordProof 算法"""
        s, ch = self._raw_request(
            "POST", "/account/login/challenge", {"username": self.username}
        )
        if s != 200 or not isinstance(ch, dict) or ch.get("method") != "challenge":
            return False

        verifier = hashlib.pbkdf2_hmac(
            "sha256", self.password.encode("utf-8"),
            ch["salt"].encode("utf-8"), ch["iterations"], dklen=32,
        )
        msg = f"{self.username}:{ch['challenge_id']}:{ch['nonce']}".encode("utf-8")
        proof = hmac.new(verifier, msg, hashlib.sha256).hexdigest()

        nonce = None
        try:
            rn_s, rn = self._raw_request(
                "POST", "/account/replay_nonce",
                {"method": "POST", "path": "/account/login"},
            )
            if rn_s == 200 and isinstance(rn, dict):
                nonce = rn.get("nonce")
        except Exception:
            nonce = None

        headers = {"X-Request-Nonce": nonce} if nonce else None
        s, result = self._raw_request(
            "POST", "/account/login",
            {
                "userName": self.username,
                "type": "account",
                "challenge_id": ch["challenge_id"],
                "proof": proof,
            },
            headers=headers,
        )
        if s != 200:
            return False
        token = self._extract_token(result)
        if token:
            self.token = token
            self.login_method = "challenge"
            return True
        return False

    def _login_plaintext(self) -> bool:
        """旧版本 Console 明文登录"""
        s, result = self._raw_request(
            "POST", "/account/login",
            {"username": self.username, "password": self.password},
        )
        if s != 200:
            return False
        token = self._extract_token(result)
        if token:
            self.token = token
            self.login_method = "plaintext"
            return True
        return False

    def get_clusters(self) -> List[Dict[str, Any]]:
        """获取所有集群列表"""
        query = {"size": 1000, "query": {"match_all": {}}}
        result = self._make_request(
            "/elasticsearch/_search", "POST", json.dumps(query).encode("utf-8")
        )

        clusters = []
        hits = result.get("hits", {}).get("hits", [])
        for hit in hits:
            source = hit.get("_source", {})
            clusters.append(
                {
                    "id": hit.get("_id"),
                    "name": source.get("name", "Unknown"),
                    "version": source.get("version", "Unknown"),
                    "endpoint": source.get("endpoint", ""),
                    "enabled": source.get("enabled", False),
                    "monitored": source.get("monitored", False),
                }
            )
        return clusters

    def get_cluster_status(self, cluster_id: str) -> Dict[str, Any]:
        """获取集群状态"""
        return self._make_request(f"/elasticsearch/{cluster_id}/status", "GET")

    def get_clusters_status(self) -> Dict[str, Any]:
        """获取所有集群状态"""
        return self._make_request("/elasticsearch/status", "GET")

    def get_cluster_metrics(
        self, cluster_id: str, min_time: str = "now-1h", max_time: str = "now"
    ) -> Dict[str, Any]:
        """获取集群指标"""
        return self._make_request(
            f"/elasticsearch/{cluster_id}/metrics?min={min_time}&max={max_time}", "GET"
        )

    def get_indices(self, cluster_id: str) -> Dict[str, Any]:
        """获取集群索引列表"""
        try:
            # 使用 _proxy 直接查询 ES _cat/indices API 获取完整信息
            # proxy_request 已经帮我们解析好了 response_body
            result = self.proxy_request(
                cluster_id, "GET",
                "/_cat/indices?format=json&h=index,health,status,shards,pri,rep,docs.count,docs.deleted,store.size,pri.store.size"
            )
            if isinstance(result, list):
                return {item["index"]: item for item in result if "index" in item}
            elif isinstance(result, dict):
                return result
            return {}
        except Exception:
            # 回退到 Console 的 indices 接口
            try:
                result = self._make_request(f"/elasticsearch/{cluster_id}/indices", "GET")
                if isinstance(result, list):
                    return {item["index"]: item for item in result if "index" in item}
                elif isinstance(result, dict):
                    return result
            except Exception:
                pass
            return {}

    def proxy_request(
        self,
        cluster_id: str,
        method: str,
        path: str,
        body: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """通过 _proxy API 发送请求到 ES"""
        encoded_path = urllib.parse.quote(path, safe="/-_.:?&=")
        endpoint = f"/elasticsearch/{cluster_id}/_proxy?method={method.upper()}&path={encoded_path}"

        data = None
        if body is not None:
            if isinstance(body, str):
                data = body.encode("utf-8")
            else:
                data = json.dumps(body, ensure_ascii=False).encode("utf-8")

        # _make_request 会自动处理 Authorization header 和 HTTP 错误
        result = self._make_request(endpoint, "POST", data=data)

        # 解析 response_body
        if isinstance(result, dict) and "response_body" in result:
            response_body = result["response_body"]
            if isinstance(response_body, str):
                try:
                    response_body = json.loads(response_body)
                except json.JSONDecodeError:
                    pass
            return response_body
        return result

    def get_index_mapping(self, cluster_id: str, index_name: str) -> Optional[Dict]:
        """获取索引 mapping"""
        try:
            result = self.proxy_request(cluster_id, "GET", f"/{index_name}/_mapping")
            if isinstance(result, dict):
                # 尝试提取 mappings 部分
                for key, value in result.items():
                    if isinstance(value, dict) and "mappings" in value:
                        return value["mappings"]
                return result
            return None
        except Exception:
            return None

    def get_index_settings(self, cluster_id: str, index_name: str) -> Optional[Dict]:
        """获取索引 settings"""
        try:
            result = self.proxy_request(cluster_id, "GET", f"/{index_name}/_settings")
            if isinstance(result, dict) and index_name in result:
                settings = result[index_name].get("settings", {})
                if isinstance(settings.get("index"), dict):
                    return settings["index"]
                return settings
            return result
        except Exception:
            return None

    def search_index(
        self, cluster_id: str, index_name: str, query: Optional[Dict] = None, size: int = 10
    ) -> List[Dict]:
        """搜索索引"""
        body = query or {"query": {"match_all": {}}, "size": size}
        try:
            result = self.proxy_request(cluster_id, "POST", f"/{index_name}/_search", body)
            hits = result.get("hits", {}).get("hits", [])
            return [
                {"_id": hit.get("_id"), "_index": hit.get("_index"), "_source": hit.get("_source")}
                for hit in hits
            ]
        except Exception:
            return []

    def resolve_cluster_id_by_name(self, cluster_name: str) -> str:
        """根据集群名称解析集群 ID"""
        clusters = self.get_clusters()
        name_lower = cluster_name.lower().strip()

        # 精确匹配
        for cluster in clusters:
            if cluster["name"] == cluster_name:
                return cluster["id"]

        # 忽略大小写匹配
        for cluster in clusters:
            if cluster["name"].lower() == name_lower:
                return cluster["id"]

        # 部分匹配
        matches = [c for c in clusters if name_lower in c["name"].lower()]
        if len(matches) == 1:
            return matches[0]["id"]
        elif len(matches) > 1:
            names = ", ".join([f"{c['name']}({c['id']})" for c in matches[:5]])
            raise ConsoleAPIError(f"找到多个匹配集群: {names}")

        raise ConsoleAPIError(f"未找到集群: {cluster_name}")

    @staticmethod
    def is_system_cluster(cluster_id: str, cluster_name: str) -> bool:
        """判断是否为系统集群"""
        system_ids = ["infini_default_system_cluster"]
        system_name_patterns = ["INFINI_SYSTEM", "Slingshot"]

        if cluster_id in system_ids:
            return True

        name_upper = cluster_name.upper()
        for pattern in system_name_patterns:
            if pattern.upper() in name_upper:
                return True

        return False

    @staticmethod
    def format_bytes(bytes_val: int) -> str:
        """将字节转换为可读格式"""
        if bytes_val is None or bytes_val == 0:
            return "0 B"
        for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
            if abs(bytes_val) < 1024.0:
                return f"{bytes_val:.2f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.2f} EB"

    @staticmethod
    def format_duration(millis: int) -> str:
        """将毫秒转换为可读时长格式"""
        if millis is None or millis == 0:
            return "0s"

        seconds = millis // 1000
        days = seconds // 86400
        seconds %= 86400
        hours = seconds // 3600
        seconds %= 3600
        minutes = seconds // 60
        seconds %= 60

        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        if seconds > 0 or not parts:
            parts.append(f"{seconds}s")

        return " ".join(parts[:2])


def create_authenticated_client(
    console_url: str,
    username: str,
    password: str,
    timeout: int = 60,
    verify_ssl: bool = True,
    prompt_password: bool = True,
    verbose: bool = True,
) -> ConsoleClient:
    """
    创建 ConsoleClient 并自动完成登录的便捷辅助函数。

    统一处理：
    - 交互式密码输入（当提供了用户名但没密码时自动提示）
    - 登录错误处理
    - 特殊字符密码提示

    Args:
        console_url: Console 地址
        username: 用户名
        password: 密码（如果为空且 prompt_password=True，会交互式询问）
        timeout: 请求超时秒数
        verify_ssl: 是否验证 SSL 证书
        prompt_password: 用户名存在但密码为空时，是否交互式提示输入密码
        verbose: 是否打印进度信息

    Returns:
        已登录的 ConsoleClient 实例

    Raises:
        ConsoleAuthError: 登录失败
    """
    # 交互式密码输入
    if username and not password and prompt_password:
        password = getpass.getpass(f"请输入用户 {username} 的密码: ")

    client = ConsoleClient(
        base_url=console_url,
        username=username,
        password=password,
        timeout=timeout,
        verify_ssl=verify_ssl,
    )

    if not username or not password:
        if verbose:
            print("警告: 未提供用户名/密码，将以匿名方式请求（可能返回 403 Forbidden）", file=sys.stderr)
        return client

    # 执行登录
    try:
        if verbose:
            print(f"正在登录 {console_url} ...", file=sys.stderr)
        if not client.login():
            error_msg = "登录失败：用户名或密码错误"
            if '!' in password or '$' in password or '`' in password or '\\' in password:
                error_msg += "\n提示: 如果密码包含特殊字符（! $ ` \\ 等），请在命令行用单引号包裹密码:"
                error_msg += "\n      -p 'your!password@123'"
                error_msg += "\n  或者不要在命令行传密码，使用交互式输入。"
            raise ConsoleAuthError(error_msg)
        if verbose:
            print("登录成功", file=sys.stderr)
    except ConsoleAuthError:
        raise
    except Exception as e:
        raise ConsoleAuthError(f"登录失败: {str(e)}") from e

    return client
