#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
INFINI Console API 客户端公共模块

提供统一的 Console API 访问接口，包括：
- JWT 认证
- 集群列表获取
- _proxy API 调用
- 索引信息查询
"""

import json
import ssl
import urllib.request
import urllib.error
import urllib.parse
from typing import Dict, List, Optional, Any, Tuple


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

        # SSL 上下文
        self.ssl_context = ssl.create_default_context()
        if not verify_ssl:
            self.ssl_context.check_hostname = False
            self.ssl_context.verify_mode = ssl.CERT_NONE

    def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """发送 HTTP 请求"""
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

        try:
            with urllib.request.urlopen(
                req, context=self.ssl_context, timeout=self.timeout
            ) as response:
                response_data = response.read().decode("utf-8")
                return json.loads(response_data) if response_data else {}
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            raise ConsoleAPIError(f"HTTP {e.code}: {error_body}")
        except Exception as e:
            raise ConsoleAPIError(f"Request failed: {str(e)}")

    def login(self) -> bool:
        """登录获取 JWT Token"""
        if not self.username or not self.password:
            return False

        url = f"{self.base_url}/account/login"
        login_data = {"username": self.username, "password": self.password}

        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(login_data).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )

            with urllib.request.urlopen(
                req, context=self.ssl_context, timeout=self.timeout
            ) as response:
                result = json.loads(response.read().decode("utf-8"))

                # 尝试从不同字段获取 token
                token = result.get("token") or result.get("access_token")
                if not token and "data" in result and isinstance(result["data"], dict):
                    token = result["data"].get("token") or result["data"].get("access_token")

                if token:
                    self.token = token
                    return True

                # 尝试检查 status
                if result.get("status") == "ok":
                    token = result.get("access_token")
                    if token:
                        self.token = token
                        return True

                return False

        except Exception as e:
            raise ConsoleAuthError(f"Login failed: {str(e)}")

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
            # 尝试使用 _cat/indices
            result = self._make_request(
                f"/elasticsearch/{cluster_id}/_cat/indices", "GET"
            )
            if isinstance(result, list):
                return {item["index"]: item for item in result if "index" in item}
            elif isinstance(result, dict):
                return result
            return {}
        except Exception:
            # 回退到普通 indices 接口
            result = self._make_request(f"/elasticsearch/{cluster_id}/indices", "GET")
            if isinstance(result, list):
                return {item["index"]: item for item in result if "index" in item}
            elif isinstance(result, dict):
                return result
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
        url = f"{self.base_url}/elasticsearch/{cluster_id}/_proxy?method={method.upper()}&path={encoded_path}"

        data = None
        if body is not None:
            if isinstance(body, str):
                data = body.encode("utf-8")
            else:
                data = json.dumps(body, ensure_ascii=False).encode("utf-8")

        headers = {"Authorization": f"Bearer {self.token}"} if self.token else {}

        try:
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")

            with urllib.request.urlopen(
                req, context=self.ssl_context, timeout=self.timeout
            ) as response:
                result = json.loads(response.read().decode("utf-8"))

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

        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            raise ConsoleAPIError(f"HTTP {e.code}: {error_body}")

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
