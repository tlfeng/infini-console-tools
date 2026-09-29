#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 ConsoleClient 模块
"""

import sys
import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.console_client import (
    ConsoleClient,
    ConsoleAuthError,
    ConsoleAPIError,
    DEFAULT_MAX_ATTEMPTS,
    RETRY_BASE_DELAY,
    RETRY_JITTER,
    RETRY_MAX_DELAY,
)


class FakeResponse:
    """假的 HTTP 响应对象"""

    def __init__(self, body: bytes, status: int = 200, headers=None, will_close: bool = False):
        self.status = status
        self._body = body
        self.headers = headers or {}
        self.will_close = will_close

    def read(self):
        return self._body


class FakeConnection:
    """按顺序返回预设响应/异常的假连接"""

    def __init__(self, items, name="conn"):
        self.items = list(items)
        self.name = name
        self.requests = []
        self.close_count = 0

    def request(self, method, path, body=None, headers=None):
        self.requests.append({"method": method, "path": path, "body": body, "headers": dict(headers or {})})
        if self.items and isinstance(self.items[0], BaseException):
            raise self.items.pop(0)

    def getresponse(self):
        if not self.items:
            raise ConnectionResetError("[WinError 10054] 远程主机强迫关闭了一个现有的连接。")
        item = self.items.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def close(self):
        self.close_count += 1


def json_response(payload, status: int = 200, will_close: bool = False) -> FakeResponse:
    return FakeResponse(json.dumps(payload).encode("utf-8"), status=status, will_close=will_close)


def make_client(connections, **kwargs) -> ConsoleClient:
    """构造一个客户端，_open_connection 依次交出给定的假连接"""
    client = ConsoleClient("http://localhost:9000", **kwargs)
    client.token = "test_token"
    pending = list(connections)

    def _open(scheme, host, port):
        if not pending:
            raise AssertionError("未预期的额外连接创建")
        return pending.pop(0)

    client._open_connection = _open  # type: ignore[assignment]
    return client


class TestConsoleClientInit(unittest.TestCase):
    """测试 ConsoleClient 初始化"""

    def test_default_init(self):
        """测试默认初始化"""
        client = ConsoleClient("http://localhost:9000")
        self.assertEqual(client.base_url, "http://localhost:9000")
        self.assertEqual(client.username, "")
        self.assertEqual(client.password, "")
        self.assertEqual(client.timeout, 60)
        self.assertIsNone(client.token)

    def test_custom_init(self):
        """测试自定义初始化"""
        client = ConsoleClient(
            "http://test:9000",
            username="admin",
            password="secret",
            timeout=120,
        )
        self.assertEqual(client.base_url, "http://test:9000")
        self.assertEqual(client.username, "admin")
        self.assertEqual(client.password, "secret")
        self.assertEqual(client.timeout, 120)

    def test_url_trailing_slash_removed(self):
        """测试 URL 尾部斜杠被移除"""
        client = ConsoleClient("http://localhost:9000/")
        self.assertEqual(client.base_url, "http://localhost:9000")


class TestFormatBytes(unittest.TestCase):
    """测试字节格式化"""

    def test_zero_bytes(self):
        """测试零字节"""
        result = ConsoleClient.format_bytes(0)
        self.assertEqual(result, "0 B")

    def test_bytes(self):
        """测试字节"""
        result = ConsoleClient.format_bytes(512)
        self.assertEqual(result, "512.00 B")

    def test_kilobytes(self):
        """测试 KB"""
        result = ConsoleClient.format_bytes(1024)
        self.assertEqual(result, "1.00 KB")

    def test_megabytes(self):
        """测试 MB"""
        result = ConsoleClient.format_bytes(1024 * 1024)
        self.assertEqual(result, "1.00 MB")

    def test_gigabytes(self):
        """测试 GB"""
        result = ConsoleClient.format_bytes(1024 * 1024 * 1024)
        self.assertEqual(result, "1.00 GB")

    def test_none_value(self):
        """测试 None 值"""
        result = ConsoleClient.format_bytes(None)
        self.assertEqual(result, "0 B")


class TestFormatDuration(unittest.TestCase):
    """测试时长格式化"""

    def test_zero_millis(self):
        """测试零毫秒"""
        result = ConsoleClient.format_duration(0)
        self.assertEqual(result, "0s")

    def test_seconds_only(self):
        """测试只有秒"""
        result = ConsoleClient.format_duration(5000)  # 5 seconds
        self.assertEqual(result, "5s")

    def test_minutes_seconds(self):
        """测试分钟和秒"""
        result = ConsoleClient.format_duration(65000)  # 1m 5s
        self.assertEqual(result, "1m 5s")

    def test_hours_minutes(self):
        """测试小时和分钟"""
        result = ConsoleClient.format_duration(3661000)  # 1h 1m 1s
        self.assertIn("h", result)

    def test_days(self):
        """测试天数"""
        result = ConsoleClient.format_duration(86400000 * 2)  # 2 days
        self.assertIn("d", result)

    def test_none_value(self):
        """测试 None 值"""
        result = ConsoleClient.format_duration(None)
        self.assertEqual(result, "0s")


class TestIsSystemCluster(unittest.TestCase):
    """测试系统集群判断"""

    def test_system_cluster_by_id(self):
        """测试通过 ID 判断系统集群"""
        result = ConsoleClient.is_system_cluster(
            "infini_default_system_cluster", "any_name"
        )
        self.assertTrue(result)

    def test_system_cluster_by_name(self):
        """测试通过名称判断系统集群"""
        result = ConsoleClient.is_system_cluster(
            "any_id", "INFINI_SYSTEM"
        )
        self.assertTrue(result)

    def test_slingshot_name(self):
        """测试 Slingshot 名称"""
        result = ConsoleClient.is_system_cluster(
            "any_id", "My Slingshot Cluster"
        )
        self.assertTrue(result)

    def test_normal_cluster(self):
        """测试普通集群"""
        result = ConsoleClient.is_system_cluster(
            "normal_id", "my-es-cluster"
        )
        self.assertFalse(result)

    def test_case_insensitive(self):
        """测试大小写不敏感"""
        result = ConsoleClient.is_system_cluster(
            "any_id", "infini_system"
        )
        self.assertTrue(result)


class TestProxyRequestParsing(unittest.TestCase):
    """测试 proxy_request 响应解析"""

    def test_parse_string_response_body(self):
        """测试解析字符串类型的 response_body"""
        conn = FakeConnection([json_response({"response_body": '{"hits": {"total": 10}}'})])
        client = make_client([conn])

        result = client.proxy_request("test_cluster", "GET", "/test")
        self.assertEqual(result["hits"]["total"], 10)

    def test_parse_dict_response_body(self):
        """测试解析字典类型的 response_body"""
        conn = FakeConnection([json_response({"response_body": {"hits": {"total": 20}}})])
        client = make_client([conn])

        result = client.proxy_request("test_cluster", "GET", "/test")
        self.assertEqual(result["hits"]["total"], 20)


class TestTransport(unittest.TestCase):
    """测试 keep-alive 连接复用与请求头"""

    def test_connection_is_reused_across_requests(self):
        """同一线程内连续请求复用同一条连接"""
        conn = FakeConnection([json_response({"ok": 1}), json_response({"ok": 2})])
        opened = []
        client = ConsoleClient("http://localhost:9000")
        client.token = "t"

        def _open(scheme, host, port):
            opened.append((scheme, host, port))
            return conn

        client._open_connection = _open  # type: ignore[assignment]

        self.assertEqual(client._make_request("/a"), {"ok": 1})
        self.assertEqual(client._make_request("/b"), {"ok": 2})
        self.assertEqual(len(opened), 1)
        self.assertEqual(len(conn.requests), 2)
        self.assertEqual(opened[0], ("http", "localhost", 9000))

    def test_request_headers_match_legacy_transport(self):
        """请求头保持与旧 urllib 传输一致，并带上 Bearer Token"""
        conn = FakeConnection([json_response({})])
        client = make_client([conn])
        client._make_request("/elasticsearch/_search?size=10", "POST", b"{}")

        headers = conn.requests[0]["headers"]
        self.assertEqual(headers["Authorization"], "Bearer test_token")
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertEqual(headers["Accept-Encoding"], "identity")
        self.assertTrue(headers["User-Agent"].startswith("Python-urllib/"))
        self.assertEqual(conn.requests[0]["path"], "/elasticsearch/_search?size=10")
        self.assertEqual(conn.requests[0]["body"], b"{}")

    def test_stale_kept_alive_connection_is_replaced_transparently(self):
        """长连接被对端回收时自动换新连接重发，不消耗重试次数"""
        first = FakeConnection([json_response({"ok": 1})], name="first")
        second = FakeConnection([json_response({"ok": 2})], name="second")
        client = make_client([first, second])
        # 第二次请求时，复用中的连接已被服务端回收
        first.items.append(ConnectionResetError("[WinError 10054] 远程主机强迫关闭了一个现有的连接。"))

        self.assertEqual(client._make_request("/a"), {"ok": 1})
        self.assertEqual(client._make_request("/b"), {"ok": 2})
        self.assertGreaterEqual(first.close_count, 1)
        self.assertEqual(len(second.requests), 1)

    def test_will_close_response_discards_connection(self):
        """响应声明 Connection: close 时不再复用该连接"""
        first = FakeConnection([json_response({"ok": 1}, will_close=True)], name="first")
        second = FakeConnection([json_response({"ok": 2})], name="second")
        client = make_client([first, second])

        client._make_request("/a")
        client._make_request("/b")
        self.assertEqual(first.close_count, 1)
        self.assertEqual(len(second.requests), 1)

    def test_close_releases_connection(self):
        """close() 释放连接后重新建连"""
        first = FakeConnection([json_response({})], name="first")
        second = FakeConnection([json_response({})], name="second")
        client = make_client([first, second])

        client._make_request("/a")
        client.close()
        client._make_request("/b")
        self.assertEqual(first.close_count, 1)
        self.assertEqual(len(second.requests), 1)


class TestRetry(unittest.TestCase):
    """测试瞬时错误重试策略"""

    def test_retry_delay_is_exponential_with_jitter(self):
        """退避为 1/2/4/8 秒（封顶）并带抖动"""
        deltas = [ConsoleClient._retry_delay(i) for i in range(1, 7)]
        for index, (base, delay) in enumerate(zip([1, 2, 4, 8, 8, 8], deltas)):
            self.assertGreaterEqual(delay, base, f"第{index + 1}次退避偏小")
            self.assertLessEqual(delay, base + RETRY_JITTER, f"第{index + 1}次退避偏大")
        self.assertEqual(RETRY_MAX_DELAY, 8.0)
        self.assertEqual(RETRY_BASE_DELAY, 1.0)

    @patch("common.console_client.time.sleep")
    def test_retries_until_success(self, mock_sleep):
        """前几次连接被重置，后续成功则正常返回"""
        conn = FakeConnection([
            ConnectionResetError("[WinError 10054] 远程主机强迫关闭了一个现有的连接。"),
            ConnectionResetError("[WinError 10054] 远程主机强迫关闭了一个现有的连接。"),
            json_response({"ok": True}),
        ])
        client = make_client([conn, conn, conn])

        self.assertEqual(client._make_request("/a"), {"ok": True})
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("common.console_client.time.sleep")
    def test_all_attempts_failed_reports_details(self, mock_sleep):
        """全部重试失败时，错误信息包含尝试次数与逐次耗时"""
        conns = [FakeConnection([ConnectionResetError("10054 远程主机强迫关闭了一个现有的连接。")])
                 for _ in range(DEFAULT_MAX_ATTEMPTS)]
        client = make_client(conns, max_attempts=DEFAULT_MAX_ATTEMPTS)

        with self.assertRaises(ConsoleAPIError) as ctx:
            client._make_request("/elasticsearch/_search")
        message = str(ctx.exception)
        self.assertIn(f"连续 {DEFAULT_MAX_ATTEMPTS} 次失败", message)
        self.assertIn("连接被重置", message)
        self.assertIn("第1次(", message)
        self.assertIn("CONSOLE_MAX_ATTEMPTS", message)
        # 连接被重置时的定位提示（grep panic / console_diag.py）必须保留
        self.assertIn("grep -i panic", message)
        self.assertIn("console_diag.py", message)
        self.assertEqual(mock_sleep.call_count, DEFAULT_MAX_ATTEMPTS - 1)

    @patch("common.console_client.time.sleep")
    def test_max_attempts_can_be_configured(self, mock_sleep):
        """重试次数可通过构造参数调整"""
        conns = [FakeConnection([ConnectionResetError("reset")]) for _ in range(2)]
        client = make_client(conns, max_attempts=2)
        with self.assertRaises(ConsoleAPIError):
            client._make_request("/a")
        self.assertEqual(mock_sleep.call_count, 1)

    @patch("common.console_client.time.sleep")
    def test_server_error_is_retried(self, mock_sleep):
        """5xx 视为瞬时错误重试，最终抛出 HTTP 状态码"""
        conn = FakeConnection([
            json_response({"error": "boom"}, status=503),
            json_response({"error": "boom"}, status=503),
        ])
        client = make_client([conn], max_attempts=2)
        with self.assertRaises(ConsoleAPIError) as ctx:
            client._make_request("/a")
        self.assertIn("HTTP 503", str(ctx.exception))
        self.assertEqual(mock_sleep.call_count, 1)
        self.assertEqual(len(conn.requests), 2)

    @patch("common.console_client.time.sleep")
    def test_client_error_is_not_retried(self, mock_sleep):
        """4xx 不重试，直接抛出"""
        conn = FakeConnection([json_response({"error": "forbidden"}, status=403)])
        client = make_client([conn])
        with self.assertRaises(ConsoleAPIError) as ctx:
            client._make_request("/a")
        self.assertIn("HTTP 403", str(ctx.exception))
        mock_sleep.assert_not_called()

    def test_non_utf8_body_reports_diagnostic(self):
        """响应体不是合法 UTF-8 时给出中间设备相关的诊断信息"""
        conn = FakeConnection([FakeResponse(b"\xff\xfe\x00\x01not-utf8",
                                           headers={"Content-Encoding": "gzip"})])
        client = make_client([conn])
        with self.assertRaises(ConsoleAPIError) as ctx:
            client._make_request("/a")
        message = str(ctx.exception)
        self.assertIn("不是合法的 UTF-8", message)
        self.assertIn("Content-Encoding=gzip", message)

    def test_raw_request_returns_status_without_raising(self):
        """_raw_request 保留 HTTP 状态码，供登录流程降级"""
        conn = FakeConnection([json_response({"error": "not found"}, status=404)])
        client = make_client([conn])
        status, body = client._raw_request("POST", "/account/login/challenge")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "not found"})


class TestGetClusters(unittest.TestCase):
    """测试集群列表获取（分页参数必须走 URL 查询串）"""

    @staticmethod
    def _hit(cluster_id, name="cluster"):
        return {"_id": cluster_id, "_source": {"name": name, "version": "7.10.2", "endpoint": "http://es:9200"}}

    def test_size_is_sent_as_query_parameter(self):
        """size/from 必须出现在查询串里（Console 会忽略 POST body）"""
        conn = FakeConnection([
            json_response({"hits": {"total": {"value": 21, "relation": "eq"},
                                    "hits": [self._hit(f"c{i}") for i in range(21)]}})
        ])
        client = make_client([conn])

        clusters = client.get_clusters()
        self.assertEqual(len(clusters), 21)
        self.assertEqual(len(conn.requests), 1)
        request = conn.requests[0]
        self.assertEqual(request["method"], "GET")
        self.assertEqual(request["path"], "/elasticsearch/_search?size=500&from=0")
        self.assertIsNone(request["body"])

    def test_paginates_until_total_reached(self):
        """超过一页时自动翻页并去重"""
        first_page = [self._hit(f"c{i}") for i in range(500)]
        second_page = [self._hit("c500"), self._hit("c0")]  # 第二页重复了 c0
        conn = FakeConnection([
            json_response({"hits": {"total": {"value": 501}, "hits": first_page}}),
            json_response({"hits": {"total": {"value": 501}, "hits": second_page}}),
        ])
        client = make_client([conn])

        clusters = client.get_clusters(page_size=500)
        self.assertEqual(len(clusters), 501)
        self.assertEqual([r["path"] for r in conn.requests], [
            "/elasticsearch/_search?size=500&from=0",
            "/elasticsearch/_search?size=500&from=500",
        ])

    def test_legacy_total_as_integer(self):
        """兼容旧版 ES 返回的整数 total"""
        conn = FakeConnection([
            json_response({"hits": {"total": 2, "hits": [self._hit("a"), self._hit("b")]}})
        ])
        client = make_client([conn])
        self.assertEqual([c["id"] for c in client.get_clusters()], ["a", "b"])

    def test_empty_result(self):
        """没有集群时返回空列表"""
        conn = FakeConnection([json_response({"hits": {"total": {"value": 0}, "hits": []}})])
        client = make_client([conn])
        self.assertEqual(client.get_clusters(), [])


if __name__ == "__main__":
    unittest.main()
