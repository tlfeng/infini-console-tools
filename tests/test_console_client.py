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
)


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

    def setUp(self):
        self.client = ConsoleClient("http://localhost:9000")
        self.client.token = "test_token"

    @patch("urllib.request.urlopen")
    def test_parse_string_response_body(self, mock_urlopen):
        """测试解析字符串类型的 response_body"""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "response_body": '{"hits": {"total": 10}}'
        }).encode()
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = self.client.proxy_request("test_cluster", "GET", "/test")
        self.assertEqual(result["hits"]["total"], 10)

    @patch("urllib.request.urlopen")
    def test_parse_dict_response_body(self, mock_urlopen):
        """测试解析字典类型的 response_body"""
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "response_body": {"hits": {"total": 20}}
        }).encode()
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = self.client.proxy_request("test_cluster", "GET", "/test")
        self.assertEqual(result["hits"]["total"], 20)


if __name__ == "__main__":
    unittest.main()
