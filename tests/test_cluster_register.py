#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 Cluster Register 模块
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "cluster-register"))

from common.console_client import ConsoleAPIError
from register_cluster import (
    split_endpoint,
    build_register_body,
    build_credential_body,
    find_existing,
    ClusterRegistrar,
)


class TestSplitEndpoint(unittest.TestCase):

    def test_http(self):
        self.assertEqual(
            split_endpoint("http://127.0.0.1:9201"),
            {"schema": "http", "host": "127.0.0.1:9201"})

    def test_https(self):
        self.assertEqual(
            split_endpoint("https://es.prod:9200"),
            {"schema": "https", "host": "es.prod:9200"})

    def test_invalid(self):
        with self.assertRaises(ValueError):
            split_endpoint("http://no-port")


class TestBuildRegisterBody(unittest.TestCase):

    def test_basic(self):
        body = build_register_body("es7102-test", "http://127.0.0.1:9201")
        self.assertEqual(body["name"], "es7102-test")
        self.assertEqual(body["endpoint"], "http://127.0.0.1:9201")
        self.assertEqual(body["hosts"], ["127.0.0.1:9201"])
        self.assertEqual(body["schema"], "http")
        self.assertTrue(body["enabled"])
        self.assertTrue(body["monitored"])

    def test_custom_hosts_and_flags(self):
        body = build_register_body(
            "es", "http://10.0.0.1:9200",
            enabled=False, monitored=False, hosts=["10.0.0.1:9200", "10.0.0.2:9200"])
        self.assertFalse(body["enabled"])
        self.assertFalse(body["monitored"])
        self.assertEqual(len(body["hosts"]), 2)


class TestBuildCredentialBody(unittest.TestCase):

    def test_shape(self):
        body = build_credential_body("cred-name", "elastic", "secret")
        self.assertEqual(body["type"], "basic_auth")
        self.assertEqual(body["payload"]["basic_auth"]["username"], "elastic")
        self.assertEqual(body["payload"]["basic_auth"]["password"], "secret")


class TestFindExisting(unittest.TestCase):

    CLUSTERS = [
        {"id": "a", "name": "INFINI_SYSTEM", "endpoint": "https://127.0.0.1:9200"},
        {"id": "b", "name": "es7102-test", "endpoint": "http://127.0.0.1:9201"},
    ]

    def test_by_name(self):
        self.assertEqual(find_existing(self.CLUSTERS, "es7102-test")["id"], "b")

    def test_by_endpoint(self):
        self.assertEqual(
            find_existing(self.CLUSTERS, "other-name", "http://127.0.0.1:9201/")["id"],
            "b")

    def test_not_found(self):
        self.assertIsNone(find_existing(self.CLUSTERS, "nope"))


class TestClusterRegistrar(unittest.TestCase):

    def _registrar(self):
        return ClusterRegistrar(MagicMock())

    def test_register_returns_id(self):
        r = self._registrar()
        r.client.write_request.return_value = {"_id": "abc", "result": "created"}
        self.assertEqual(r.register({"name": "x"}), "abc")
        r.client.write_request.assert_called_once_with(
            "/elasticsearch/", "POST", {"name": "x"})

    def test_register_missing_id_raises(self):
        r = self._registrar()
        r.client.write_request.return_value = {"result": "created"}
        with self.assertRaises(ConsoleAPIError):
            r.register({"name": "x"})

    def test_create_and_bind_credential(self):
        r = self._registrar()
        r.client.write_request.return_value = {"_id": "cred1", "result": "created"}
        r.client.request.return_value = {
            "_id": "clus1", "_source": {"name": "es", "enabled": True}}
        cred_id = r.create_credential("cred", "u", "p")
        self.assertEqual(cred_id, "cred1")
        r.bind_credential("clus1", cred_id)
        # 绑定时 PUT 的 body 应包含 credential_id
        args = r.client.write_request.call_args
        self.assertEqual(args[0][0], "/elasticsearch/clus1")
        self.assertEqual(args[0][1], "PUT")
        self.assertEqual(args[0][2]["credential_id"], "cred1")

    def test_wait_proxy_ready(self):
        r = self._registrar()
        r.client.proxy_request.return_value = {
            "cluster_name": "es", "version": {"number": "7.10.2"}}
        self.assertEqual(
            r.wait_proxy_ready("cid", timeout_seconds=2, interval=1),
            "es (v7.10.2)")

    def test_wait_proxy_timeout(self):
        r = self._registrar()
        r.client.proxy_request.side_effect = ConsoleAPIError("metadata not found")
        self.assertIsNone(r.wait_proxy_ready("cid", timeout_seconds=1, interval=1))


if __name__ == "__main__":
    unittest.main()
