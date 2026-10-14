#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 Credential Export 模块
"""

import base64
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "credential-export"))

from common.console_client import ConsoleAPIError
from export_credentials import (
    build_probe_path,
    extract_basic_auth,
    mask_password,
    mask_authorization_header,
    CredentialExporter,
    STATUS_LABELS,
)


def b64(user: str, password: str) -> str:
    return base64.b64encode(f"{user}:{password}".encode()).decode()


# 真机抓到的错误包装形态（节选）
REAL_REQUEST_HEADER = (
    "GET /_ccr/stats HTTP/1.1\r\n"
    "User-Agent: Python-urllib/3.9\r\n"
    "Host: 127.0.0.1:9201\r\n"
    "content-type: application/json\r\n"
    "Content-Length: 0\r\n"
    f"Authorization: Basic {b64('cfgdump', 'AiTest@2026')}\r\n"
    "Referer: http://127.0.0.1:9201/_ccr/stats\r\n\r\n"
)


class TestExtractBasicAuth(unittest.TestCase):

    def test_real_header(self):
        self.assertEqual(
            extract_basic_auth(REAL_REQUEST_HEADER),
            ("cfgdump", "AiTest@2026"))

    def test_missing_header(self):
        self.assertIsNone(extract_basic_auth(""))
        self.assertIsNone(extract_basic_auth(None))
        self.assertIsNone(
            extract_basic_auth("GET / HTTP/1.1\r\nHost: x\r\n\r\n"))

    def test_invalid_base64(self):
        self.assertIsNone(
            extract_basic_auth("Authorization: Basic !!!not-b64!!!"))

    def test_no_colon(self):
        no_colon = base64.b64encode(b"just-a-token").decode()
        self.assertIsNone(
            extract_basic_auth(f"Authorization: Basic {no_colon}"))

    def test_password_containing_colon(self):
        self.assertEqual(
            extract_basic_auth(
                f"Authorization: Basic {b64('admin', 'p@ss:word')}"),
            ("admin", "p@ss:word"))


class TestMaskPassword(unittest.TestCase):

    def test_masks(self):
        self.assertEqual(mask_password("AiTest@2026"), "A***")
        self.assertEqual(mask_password("x"), "***")
        self.assertEqual(mask_password(""), "(空)")


class TestMaskAuthorizationHeader(unittest.TestCase):

    def test_masked_header_has_no_plaintext(self):
        masked = mask_authorization_header(REAL_REQUEST_HEADER)
        self.assertNotIn("AiTest@2026", masked)
        self.assertNotIn(b64("cfgdump", "AiTest@2026"), masked)
        # 打码对象是 base64 token 本身，保留其首字符
        self.assertIn("Authorization: Basic Y***", masked)


class TestBuildProbePath(unittest.TestCase):

    def test_unique_and_shape(self):
        p1, p2 = build_probe_path(), build_probe_path()
        self.assertTrue(p1.startswith("/_cluster/health/__cred_probe_"))
        self.assertTrue(p1.endswith("__"))
        self.assertNotEqual(p1, p2)  # 随机，避免命中真实索引


class TestCredentialExporter(unittest.TestCase):

    def _exporter(self):
        return CredentialExporter(MagicMock())

    def test_load_targets_skips_system(self):
        r = self._exporter()
        r.client.get_clusters.return_value = [
            {"id": "sys", "name": "INFINI_SYSTEM (X)"},
            {"id": "c1", "name": "es7102-test"},
        ]
        self.assertEqual([c["id"] for c in r.load_targets()], ["c1"])

        r2 = CredentialExporter(MagicMock(), include_system=True)
        r2.client.get_clusters.return_value = [
            {"id": "sys", "name": "INFINI_SYSTEM (X)"},
            {"id": "c1", "name": "es7102-test"},
        ]
        self.assertEqual(len(r2.load_targets()), 2)

    def test_extract_ok(self):
        r = self._exporter()
        r.client.proxy_raw.return_value = (
            404, {"request_header": REAL_REQUEST_HEADER,
                  "response_header": "HTTP/1.1 404", "response_body": "{}"})
        result = r.extract_one("cid")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["username"], "cfgdump")
        self.assertEqual(result["password"], "AiTest@2026")
        # 探测路径必须是 health 子 API
        args = r.client.proxy_raw.call_args[0]
        self.assertEqual(args[1], "GET")
        self.assertTrue(args[2].startswith("/_cluster/health/"))

    def test_extract_no_credential(self):
        r = self._exporter()
        r.client.proxy_raw.return_value = (
            404, {"request_header": "GET /x HTTP/1.1\r\nHost: y\r\n\r\n"})
        self.assertEqual(r.extract_one("cid")["status"], "no_credential")

    def test_extract_no_header(self):
        r = self._exporter()
        r.client.proxy_raw.return_value = (200, {"response_body": "ok"})
        self.assertEqual(r.extract_one("cid")["status"], "no_header")

    def test_extract_request_error(self):
        r = self._exporter()
        r.client.proxy_raw.side_effect = ConsoleAPIError("连接被重置")
        result = r.extract_one("cid")
        self.assertEqual(result["status"], "error")
        self.assertIn("连接被重置", result["detail"])


class TestStatusLabels(unittest.TestCase):

    def test_labels_cover_all_statuses(self):
        for s in ("ok", "no_credential", "no_header", "error"):
            self.assertIn(s, STATUS_LABELS)


if __name__ == "__main__":
    unittest.main()
