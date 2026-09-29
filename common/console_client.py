#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
INFINI Console API 客户端公共模块

提供统一的 Console API 访问接口，包括：
- JWT 认证（挑战-响应 / 明文）
- 集群列表获取
- _proxy API 调用
- 索引信息查询
- 便捷的认证辅助函数

传输层特性：
- keep-alive 连接复用（按线程隔离）：避免每个请求都新建 TLS 连接，
  客户环境里"新建连接被重置"的问题因此大幅减少；连接被对端回收时自动换新连接重发
- 瞬时网络错误指数退避 + 抖动重试：避免多次重试全部落在同一个抖动窗口里
- 环境变量 CONSOLE_MAX_ATTEMPTS 调整重试次数
"""

import getpass
import hashlib
import hmac
import http.client
import json
import os
import random
import ssl
import sys
import threading
import time
import urllib.parse
from typing import Dict, List, Optional, Any

# 瞬时性网络错误的最大尝试次数（1 次原始请求 + N-1 次重试），
# 客户环境出现过间歇性连接重置（WinError 10054），重试太少覆盖不了抖动窗口
DEFAULT_MAX_ATTEMPTS = 5

# Console 固定的系统集群ID
DEFAULT_SYSTEM_CLUSTER_ID = "infini_default_system_cluster"

# 重试退避：1s、2s、4s、8s（上限）再叠加 0-0.5s 抖动
RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 8.0
RETRY_JITTER = 0.5

# 需要重试的 HTTP 状态码
_RETRYABLE_STATUS = (429, 500, 502, 503, 504)

# 可重试的瞬时网络错误（连接被重置/截断/超时等）。
# Windows 的 WinError 10054（远程主机强迫关闭了一个现有的连接）是 OSError 的子类，
# ssl.SSLError 也是 OSError 的子类，这里显式列出以便阅读。
_TRANSIENT_ERRORS = (
    http.client.HTTPException,  # RemoteDisconnected / IncompleteRead / BadStatusLine
    ConnectionError,            # ConnectionResetError / ConnectionAbortedError / BrokenPipeError
    TimeoutError,               # socket 超时
    ssl.SSLError,               # 连接被掐断时 TLS 层可能报错
    OSError,                    # socket 层错误兜底（含 WinError 10054）
)


def _is_connection_reset(err: Optional[BaseException]) -> bool:
    """判断是否为"连接被重置"（服务端未返回任何 HTTP 响应）"""
    if err is None:
        return False
    text = str(err)
    return isinstance(err, (ConnectionResetError, ConnectionAbortedError)) or any(
        s in text for s in ("10054", "10053", "forcibly closed", "远程主机强迫关闭", "Connection reset")
    )


def _describe_transient_error(err: Optional[BaseException]) -> str:
    """描述瞬时网络错误；连接被重置（无 HTTP 响应）时附上定位提示"""
    if err is None:
        return "unknown"
    if _is_connection_reset(err):
        return (
            f"{err}。连接被重置且无 HTTP 响应（服务端未返回任何状态码）："
            "多为服务端异常断开（到 Console 服务器执行 grep -i panic 查看日志）"
            "或中间设备/网关拦截，可运行 console_diag.py 进一步定位"
        )
    return str(err)


def _format_attempt_log(attempt: int, elapsed: float, reason: str) -> str:
    """单次尝试的日志片段，用于失败时还原整个重试过程"""
    return f"第{attempt}次({elapsed:.1f}s) {reason}"


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
        max_attempts: Optional[int] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout = timeout
        self.token: Optional[str] = None
        self.login_method: Optional[str] = None  # "challenge"（1.31+）或 "plaintext"（旧版本）

        # 重试次数：显式入参 > 环境变量 CONSOLE_MAX_ATTEMPTS > 默认值
        self.max_attempts = max_attempts or int(
            os.getenv("CONSOLE_MAX_ATTEMPTS", str(DEFAULT_MAX_ATTEMPTS))
        )

        # SSL 上下文
        self.ssl_context = ssl.create_default_context()
        if not verify_ssl:
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = ssl.CERT_NONE

        # keep-alive 连接按线程隔离（各工具普遍用线程池并发取数）
        self._local = threading.local()

    @staticmethod
    def _decode_body(raw: bytes, headers=None) -> str:
        """解码响应体，编码异常时输出可定位问题的诊断信息"""
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as e:
            content_encoding = headers.get("Content-Encoding", "none") if headers else "none"
            content_type = headers.get("Content-Type", "unknown") if headers else "unknown"
            raise ConsoleAPIError(
                "响应体不是合法的 UTF-8，可能被中间设备（防火墙/代理）改写或压缩: "
                f"Content-Encoding={content_encoding}, Content-Type={content_type}, "
                f"size={len(raw)} bytes, 前64字节={raw[:64]!r}。原始错误: {e}"
            ) from e

    # ---------- 连接管理：同线程内复用同一条 keep-alive 连接 ----------
    #
    # 每个请求都新建 TLS 连接会放大"新建连接被重置"这类中间设备/网关问题，
    # 而且握手开销不小（滚动导出动辄成千上万次请求）。这里按线程缓存连接；
    # 连接被服务端或网关回收时（长连接空闲超时很常见），_send_once 会自动
    # 换新连接把当前这次请求重发一遍，不消耗重试次数。

    def _connection_key(self) -> tuple:
        parts = urllib.parse.urlsplit(self.base_url)
        scheme = (parts.scheme or "http").lower()
        host = parts.hostname or "localhost"
        port = parts.port or (443 if scheme == "https" else 80)
        return scheme, host, port

    def _open_connection(self, scheme: str, host: str, port: int):
        """建立新连接（测试可替换此方法注入假连接）"""
        if scheme == "https":
            return http.client.HTTPSConnection(
                host, port, timeout=self.timeout, context=self.ssl_context
            )
        return http.client.HTTPConnection(host, port, timeout=self.timeout)

    def _release_connection(self) -> None:
        """丢弃当前线程缓存的连接（只影响本线程）"""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        self._local.conn = None
        self._local.conn_key = None
        self._local.conn_reused = False

    def close(self) -> None:
        """释放当前线程缓存的连接（其余线程的连接随线程结束释放）"""
        self._release_connection()

    def _get_connection(self):
        key = self._connection_key()
        conn = getattr(self._local, "conn", None)
        if conn is not None and getattr(self._local, "conn_key", None) == key:
            return conn
        self._release_connection()
        conn = self._open_connection(*key)
        self._local.conn = conn
        self._local.conn_key = key
        self._local.conn_reused = False
        return conn

    def _send_once(
        self,
        method: str,
        endpoint: str,
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> tuple:
        """发送一次请求，返回 (status, body_bytes, response_headers)。

        连接层错误原样抛出，交给调用方决定是否重试。
        注意：长连接被对端回收时会把当前请求在**新连接**上重发一次，
        因此这里只适用于读操作（本仓库各工具都是查询/导出，无写副作用）。
        """
        path = "/" + endpoint.lstrip("/")
        # 保持与旧版 urllib 传输一致的请求头，避免改变中间设备/网关看到的请求特征；
        # Accept-Encoding: identity 确保响应不被压缩（自己解压会掩盖中间设备的改写）
        req_headers = {
            "Content-Type": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": f"Python-urllib/{sys.version_info.major}.{sys.version_info.minor}",
        }
        if headers:
            req_headers.update(headers)
        if self.token:
            req_headers["Authorization"] = f"Bearer {self.token}"

        # 第一次用缓存连接；若它已被对端回收（长连接空闲超时），换新连接重发一次
        for stale_retry in (False, True):
            conn = self._get_connection()
            reused = bool(getattr(self._local, "conn_reused", False))
            try:
                conn.request(method, path, body=data, headers=req_headers)
                response = conn.getresponse()
                raw = response.read()
                status = response.status
                response_headers = response.headers
            except Exception:
                self._release_connection()
                if reused and not stale_retry:
                    continue
                raise
            self._local.conn_reused = True
            if getattr(response, "will_close", False):
                # 服务端要求关闭连接（如 Connection: close），不要继续复用
                self._release_connection()
            return status, raw, response_headers
        raise ConsoleAPIError("连接重发失败")  # 理论上不可达

    @staticmethod
    def _retry_delay(attempt: int) -> float:
        """指数退避 + 抖动：1s、2s、4s…上限 8s，另加 0-0.5s 随机抖动"""
        delay = min(RETRY_BASE_DELAY * (2 ** (attempt - 1)), RETRY_MAX_DELAY)
        return delay + random.uniform(0, RETRY_JITTER)

    @staticmethod
    def _describe_exception(err: BaseException) -> str:
        """把异常归类成便于判断责任方的短语"""
        text = str(err)
        if isinstance(err, TimeoutError) or "timed out" in text:
            return f"超时 {err}"
        if _is_connection_reset(err):
            return f"连接被重置 {err}"
        if isinstance(err, http.client.HTTPException):
            return f"响应不完整 {err}"
        return f"{type(err).__name__}: {text}"

    def _raise_request_failed(
        self,
        endpoint: str,
        attempts: List[str],
        elapsed: float,
        last_error: Optional[BaseException] = None,
    ) -> None:
        """重试全部失败：抛出带逐次尝试明细的错误"""
        detail = "; ".join(attempts)
        reset_hint = ""
        if _is_connection_reset(last_error):
            reset_hint = f"\n{_describe_transient_error(last_error)}"
        raise ConsoleAPIError(
            f"请求 {endpoint} 连续 {len(attempts)} 次失败（共 {elapsed:.1f}s）: {detail}{reset_hint}\n"
            "提示: 连接被重置/超时通常来自网络中间设备（防火墙/代理/负载均衡）或服务端短时抖动，"
            "而不是接口本身返回错误。可依次排查:\n"
            "  1) 到 Console 宿主机上访问 http://127.0.0.1:<Console端口> 复测同一请求；\n"
            "  2) 抓包看 RST 报文的 TTL 是否与正常报文一致（不一致多为中间设备伪造）；\n"
            "  3) 确认 Console 前面的网关/负载均衡没有健康检查抖动或连接数限制；\n"
            "  4) 调整重试预算: 环境变量 CONSOLE_MAX_ATTEMPTS（当前 "
            f"{self.max_attempts}）"
        )

    def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """发送 HTTP 请求，瞬时网络错误按指数退避 + 抖动重试"""
        attempt_logs: List[str] = []
        started = time.time()
        last_error: Optional[BaseException] = None

        for attempt in range(1, self.max_attempts + 1):
            attempt_started = time.time()
            try:
                status, raw, response_headers = self._send_once(method, endpoint, data, headers)
            except _TRANSIENT_ERRORS as e:
                last_error = e
                attempt_logs.append(
                    _format_attempt_log(
                        attempt, time.time() - attempt_started, self._describe_exception(e)
                    )
                )
                if attempt < self.max_attempts:
                    time.sleep(self._retry_delay(attempt))
                    continue
                break
            except ConsoleAPIError:
                raise
            except Exception as e:
                raise ConsoleAPIError(f"请求 {endpoint} 失败: {e}") from e

            body_text = self._decode_body(raw, response_headers)
            # 5xx/429 视为瞬时错误重试，其余（4xx 等）直接抛出
            if status in _RETRYABLE_STATUS and attempt < self.max_attempts:
                attempt_logs.append(
                    _format_attempt_log(attempt, time.time() - attempt_started, f"HTTP {status}")
                )
                time.sleep(self._retry_delay(attempt))
                continue
            if status >= 400:
                raise ConsoleAPIError(f"请求 {endpoint} 失败: HTTP {status}: {body_text}")
            return json.loads(body_text) if body_text else {}

        self._raise_request_failed(endpoint, attempt_logs, time.time() - started, last_error)

    def _raw_request(
        self,
        method: str,
        endpoint: str,
        body: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> tuple:
        """底层请求，返回 (status, parsed_body)；HTTP 错误码原样返回不抛异常，
        供登录流程按状态码降级。瞬时网络错误自动重试。"""
        data = json.dumps(body).encode("utf-8") if body is not None else None
        attempt_logs: List[str] = []
        started = time.time()
        last_error: Optional[BaseException] = None

        for attempt in range(1, self.max_attempts + 1):
            attempt_started = time.time()
            try:
                status, raw, _ = self._send_once(method, endpoint, data, headers)
            except _TRANSIENT_ERRORS as e:
                last_error = e
                attempt_logs.append(
                    _format_attempt_log(
                        attempt, time.time() - attempt_started, self._describe_exception(e)
                    )
                )
                if attempt < self.max_attempts:
                    time.sleep(self._retry_delay(attempt))
                    continue
                break
            except ConsoleAPIError:
                raise
            except Exception as e:
                raise ConsoleAPIError(f"请求 {endpoint} 失败: {e}") from e

            text = raw.decode("utf-8", errors="replace") if raw else ""
            try:
                return status, json.loads(text) if text else {}
            except json.JSONDecodeError:
                return status, text

        self._raise_request_failed(endpoint, attempt_logs, time.time() - started, last_error)

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

    def get_clusters(self, page_size: int = 500) -> List[Dict[str, Any]]:
        """获取所有集群列表（自动翻页，直到取完 total 条）

        注意：Console 的 /elasticsearch/_search 只解析 URL 查询参数（size/from），
        POST body 里的分页条件会被完全忽略——所以分页参数必须放在查询串上，
        否则不管传什么 size 都只能拿到默认的 20 条，集群数超过 20 时
        系统集群可能被挤掉，导致"未找到系统集群"。
        """
        clusters: List[Dict[str, Any]] = []
        seen_ids = set()
        offset = 0

        while True:
            endpoint = f"/elasticsearch/_search?size={page_size}&from={offset}"
            result = self._make_request(endpoint, "GET")

            hits_block = result.get("hits", {}) if isinstance(result, dict) else {}
            hits = hits_block.get("hits", []) or []
            total = hits_block.get("total")
            if isinstance(total, dict):
                total = total.get("value")

            for hit in hits:
                source = hit.get("_source", {}) or {}
                cluster_id = hit.get("_id")
                if cluster_id in seen_ids:
                    continue
                seen_ids.add(cluster_id)
                clusters.append(
                    {
                        "id": cluster_id,
                        "name": source.get("name", "Unknown"),
                        "version": source.get("version", "Unknown"),
                        "endpoint": source.get("endpoint", ""),
                        "enabled": source.get("enabled", False),
                        "monitored": source.get("monitored", False),
                    }
                )

            if len(hits) < page_size:
                break
            offset += len(hits)
            if isinstance(total, int) and offset >= total:
                break

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
        system_ids = [DEFAULT_SYSTEM_CLUSTER_ID]
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
