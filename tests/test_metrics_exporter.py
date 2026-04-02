#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 Metrics Exporter 模块
"""

import os
import sys
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.config import SamplingConfig

# 导入 metrics_exporter 模块
import importlib.util
spec = importlib.util.spec_from_file_location("metrics_exporter", str(Path(__file__).parent.parent / "metrics-exporter" / "metrics_exporter.py"))
metrics_exporter = importlib.util.module_from_spec(spec)
sys.modules["metrics_exporter"] = metrics_exporter
spec.loader.exec_module(metrics_exporter)

CompactJSONWriter = metrics_exporter.CompactJSONWriter
ExportResult = metrics_exporter.ExportResult
MetricsExporter = metrics_exporter.MetricsExporter
METRIC_TYPES = metrics_exporter.METRIC_TYPES
ALERT_TYPES = metrics_exporter.ALERT_TYPES


class TestCompactJSONWriter(unittest.TestCase):
    """测试紧凑 JSON 写入器"""

    def test_write_empty(self):
        """写入空数据"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name

        try:
            with CompactJSONWriter(temp_path):
                pass

            with open(temp_path, 'r') as f:
                content = f.read()
            self.assertEqual(content, "[\n]")
        finally:
            os.unlink(temp_path)

    def test_write_single_doc(self):
        """写入单个文档"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name

        try:
            with CompactJSONWriter(temp_path) as writer:
                writer.write_doc({"key": "value"})

            with open(temp_path, 'r') as f:
                content = f.read()
            self.assertIn('"key":"value"', content)
            self.assertNotIn('indent', content)  # 紧凑格式，无 indent
        finally:
            os.unlink(temp_path)

    def test_write_multiple_docs(self):
        """写入多个文档"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name

        try:
            with CompactJSONWriter(temp_path, buffer_size=2) as writer:
                writer.write_doc({"id": 1})
                writer.write_doc({"id": 2})
                writer.write_doc({"id": 3})

            with open(temp_path, 'r') as f:
                content = f.read()

            # 验证 JSON 有效
            data = json.loads(content)
            self.assertEqual(len(data), 3)
            self.assertEqual(data[0]["id"], 1)
            self.assertEqual(data[2]["id"], 3)
        finally:
            os.unlink(temp_path)

    def test_compact_format(self):
        """验证紧凑格式"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name

        try:
            with CompactJSONWriter(temp_path) as writer:
                writer.write_doc({"nested": {"key": "value"}, "array": [1, 2, 3]})

            with open(temp_path, 'r') as f:
                content = f.read()

            # 紧凑格式不应该有多余空格
            self.assertIn(':"value"', content)
            self.assertIn(':[1,2,3]', content.replace(" ", ""))
        finally:
            os.unlink(temp_path)

    def test_flush_makes_buffer_visible(self):
        """flush 应让当前批次内容立即写入文件"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name

        try:
            with CompactJSONWriter(temp_path, buffer_size=10) as writer:
                writer.write_doc({"id": 1})
                writer.flush()

                with open(temp_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                self.assertIn('"id":1', content)
                self.assertTrue(content.startswith("["))
        finally:
            os.unlink(temp_path)


class TestExportResult(unittest.TestCase):
    """测试导出结果"""

    def test_success_result(self):
        """成功的导出结果"""
        result = ExportResult("node_stats", "节点统计指标")
        result.count = 1000
        result.file_path = "node_stats.json"
        result.duration_ms = 5000

        d = result.to_dict()
        self.assertEqual(d["name"], "节点统计指标")
        self.assertEqual(d["count"], 1000)
        self.assertEqual(d["file"], "node_stats.json")
        self.assertIsNone(d["error"])
        self.assertEqual(d["duration_ms"], 5000)

    def test_error_result(self):
        """失败的导出结果"""
        result = ExportResult("node_stats", "节点统计指标")
        result.error = "Connection timeout"

        d = result.to_dict()
        self.assertEqual(d["error"], "Connection timeout")
        self.assertIsNone(d["file"])


class TestMetricTypes(unittest.TestCase):
    """测试指标类型定义"""

    def test_all_metric_types_have_required_fields(self):
        """所有指标类型都有必需字段"""
        required_fields = ["name", "description", "index_pattern", "filter_template", "default_batch_size"]

        for metric_type, config in METRIC_TYPES.items():
            for field in required_fields:
                self.assertIn(field, config, f"{metric_type} missing {field}")

    def test_all_alert_types_have_required_fields(self):
        """所有告警类型都有必需字段"""
        required_fields = ["name", "description", "index_pattern", "default_batch_size"]

        for alert_type, config in ALERT_TYPES.items():
            for field in required_fields:
                self.assertIn(field, config, f"{alert_type} missing {field}")

    def test_batch_size_varies_by_type(self):
        """不同类型有不同的批次大小"""
        batch_sizes = [config["default_batch_size"] for config in METRIC_TYPES.values()]
        # 批次大小应该有差异
        self.assertEqual(len(set(batch_sizes)), 3)  # 2000, 3000, 5000


class TestMetricsExporterInit(unittest.TestCase):
    """测试导出器初始化"""

    def test_default_parameters(self):
        """默认参数"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        self.assertEqual(exporter.scroll_keepalive, "5m")
        self.assertEqual(exporter.parallel_jobs, 2)

    def test_custom_parameters(self):
        """自定义参数"""
        mock_client = MagicMock()
        exporter = MetricsExporter(
            mock_client,
            "system-id",
            scroll_keepalive="30s",
            parallel_jobs=4
        )

        self.assertEqual(exporter.scroll_keepalive, "30s")
        self.assertEqual(exporter.parallel_jobs, 4)


class TestQueryBuilding(unittest.TestCase):
    """测试查询构建"""

    def test_basic_query(self):
        """基本查询构建"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        query = exporter.build_metrics_query(
            'metadata.name:"node_stats"',
            time_range_hours=24
        )

        self.assertIn("query", query)
        self.assertIn("bool", query["query"])
        self.assertIn("sort", query)
        self.assertTrue(query["_source"])

    def test_query_with_cluster_filter(self):
        """带集群过滤的查询"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        query = exporter.build_metrics_query(
            'metadata.name:"node_stats"',
            time_range_hours=24,
            cluster_ids=["cluster-1", "cluster-2"]
        )

        must_clauses = query["query"]["bool"]["must"]
        terms_clause = [c for c in must_clauses if "terms" in c]
        self.assertEqual(len(terms_clause), 1)
        self.assertEqual(terms_clause[0]["terms"]["metadata.labels.cluster_id"], ["cluster-1", "cluster-2"])

    def test_query_with_source_fields(self):
        """带字段筛选的查询"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        query = exporter.build_metrics_query(
            'metadata.name:"node_stats"',
            time_range_hours=24,
            source_fields=["timestamp", "metadata.labels.cluster_id"]
        )

        self.assertEqual(query["_source"], ["timestamp", "metadata.labels.cluster_id"])


class TestScrollPagination(unittest.TestCase):
    """测试 scroll 分页逻辑"""

    def test_scroll_next_returns_latest_scroll_id(self):
        """scroll_next 应返回 ES 响应中的最新 scroll_id"""
        mock_client = MagicMock()
        mock_client.proxy_request.return_value = {
            "_scroll_id": "scroll-2",
            "hits": {
                "hits": [
                    {
                        "_id": "doc-1",
                        "_source": {"value": 1},
                        "sort": [123, "doc-1"],
                    }
                ]
            },
        }

        exporter = MetricsExporter(mock_client, "system-id")
        docs, next_scroll_id = exporter.scroll_next("scroll-1")

        self.assertEqual(next_scroll_id, "scroll-2")
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["_id"], "doc-1")
        self.assertEqual(docs[0]["sort"], [123, "doc-1"])

    def test_scroll_next_returns_empty_batch_on_non_context_error(self):
        """非 scroll 过期错误应安全返回空批次而不是破坏调用方协议"""
        mock_client = MagicMock()
        mock_client.proxy_request.side_effect = metrics_exporter.ConsoleAPIError("HTTP 500: boom")

        exporter = MetricsExporter(mock_client, "system-id")
        result = exporter.scroll_next("scroll-1")

        self.assertEqual(result, ([], None))

    def test_export_with_scroll_uses_refreshed_scroll_id(self):
        """导出循环应使用每一页返回的新 scroll_id"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        calls = []

        def proxy_request(cluster_id, method, path, body=None):
            self.assertEqual(cluster_id, "system-id")
            calls.append((method, path, body))

            if method == "POST" and path == "/metrics/_search?scroll=5m":
                return {
                    "_scroll_id": "scroll-1",
                    "hits": {
                        "total": {"value": 3},
                        "hits": [
                            {
                                "_id": "doc-1",
                                "_source": {"value": 1},
                                "sort": [3, "doc-1"],
                            }
                        ],
                    },
                }

            if method == "POST" and path == "/_search/scroll":
                current_scroll_id = body["scroll_id"]
                if current_scroll_id == "scroll-1":
                    return {
                        "_scroll_id": "scroll-2",
                        "hits": {
                            "hits": [
                                {
                                    "_id": "doc-2",
                                    "_source": {"value": 2},
                                    "sort": [2, "doc-2"],
                                }
                            ]
                        },
                    }
                if current_scroll_id == "scroll-2":
                    return {
                        "_scroll_id": "scroll-3",
                        "hits": {
                            "hits": [
                                {
                                    "_id": "doc-3",
                                    "_source": {"value": 3},
                                    "sort": [1, "doc-3"],
                                }
                            ]
                        },
                    }
                if current_scroll_id == "scroll-3":
                    return {
                        "_scroll_id": "scroll-4",
                        "hits": {"hits": []},
                    }

            if method == "DELETE" and path == "/_search/scroll":
                self.assertEqual(body, {"scroll_id": "scroll-4"})
                return {"succeeded": True}

            self.fail(f"Unexpected proxy_request call: {(method, path, body)}")

        mock_client.proxy_request.side_effect = proxy_request

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name

        try:
            count = exporter.export_with_scroll(
                index_pattern="metrics",
                query={"query": {"match_all": {}}},
                output_file=temp_path,
                batch_size=1,
                max_docs=0,
            )

            self.assertEqual(count, 3)

            scroll_calls = [call for call in calls if call[0] == "POST" and call[1] == "/_search/scroll"]
            self.assertEqual([call[2]["scroll_id"] for call in scroll_calls], ["scroll-1", "scroll-2", "scroll-3"])

            with open(temp_path, 'r') as f:
                data = json.load(f)

            self.assertEqual([doc["_id"] for doc in data], ["doc-1", "doc-2", "doc-3"])
        finally:
            os.unlink(temp_path)


class TestStratifiedSampling(unittest.TestCase):
    """测试 ES 端分层抽样导出"""

    def test_cluster_stats_interval_sampling_uses_es_aggregations(self):
        """所有指标的 interval 抽样都应走 ES 聚合"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        calls = []

        def proxy_request(cluster_id, method, path, body=None):
            self.assertEqual(cluster_id, "system-id")
            calls.append((method, path, body))

            if method == "POST" and path == "/.infini_metrics/_search":
                return {
                    "aggregations": {
                        "sampled": {
                            "buckets": [
                                {
                                    "latest": {
                                        "hits": {
                                            "hits": [
                                                {
                                                    "_id": "doc-1",
                                                    "_source": {"timestamp": "2026-04-02T00:00:00Z", "v": 1},
                                                }
                                            ]
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }

            self.fail(f"Unexpected proxy_request call: {(method, path, body)}")

        mock_client.proxy_request.side_effect = proxy_request

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name

        try:
            result = exporter.export_metric_type(
                metric_type="cluster_stats",
                config=METRIC_TYPES["cluster_stats"],
                output_file=temp_path,
                time_range_hours=24,
                max_docs=0,
                cluster_ids=["cluster-1"],
                sampling=SamplingConfig(mode="sampling", interval="15m"),
            )

            self.assertEqual(result.count, 1)
            self.assertEqual(len([c for c in calls if c[1] == "/.infini_metrics/_search"]), 1)
        finally:
            os.unlink(temp_path)

    def test_node_stats_interval_sampling_uses_es_aggregations(self):
        """node_stats interval 抽样应走 ES 聚合而非 scroll 全量拉取"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        calls = []

        def proxy_request(cluster_id, method, path, body=None):
            self.assertEqual(cluster_id, "system-id")
            calls.append((method, path, body))

            if method == "POST" and path == "/.infini_metrics/_search":
                after = body.get("aggs", {}).get("sampled", {}).get("composite", {}).get("after")
                if not after:
                    return {
                        "aggregations": {
                            "sampled": {
                                "buckets": [
                                    {
                                        "latest": {
                                            "hits": {
                                                "hits": [
                                                    {
                                                        "_id": "doc-1",
                                                        "_source": {"timestamp": "2026-04-02T00:00:00Z", "v": 1},
                                                    }
                                                ]
                                            }
                                        }
                                    },
                                    {
                                        "latest": {
                                            "hits": {
                                                "hits": [
                                                    {
                                                        "_id": "doc-2",
                                                        "_source": {"timestamp": "2026-04-02T00:15:00Z", "v": 2},
                                                    }
                                                ]
                                            }
                                        }
                                    },
                                ],
                                "after_key": {"group_0": "c1", "group_1": "n2", "time_bucket": 1712016900000},
                            }
                        }
                    }

                return {
                    "aggregations": {
                        "sampled": {
                            "buckets": [
                                {
                                    "latest": {
                                        "hits": {
                                            "hits": [
                                                {
                                                    "_id": "doc-3",
                                                    "_source": {"timestamp": "2026-04-02T00:30:00Z", "v": 3},
                                                }
                                            ]
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }

            self.fail(f"Unexpected proxy_request call: {(method, path, body)}")

        mock_client.proxy_request.side_effect = proxy_request

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name

        try:
            result = exporter.export_metric_type(
                metric_type="node_stats",
                config=METRIC_TYPES["node_stats"],
                output_file=temp_path,
                time_range_hours=24,
                max_docs=0,
                cluster_ids=["cluster-1"],
                sampling=SamplingConfig(mode="sampling", interval="15m"),
            )

            self.assertEqual(result.count, 3)

            search_calls = [c for c in calls if c[0] == "POST" and c[1] == "/.infini_metrics/_search"]
            self.assertEqual(len(search_calls), 2)

            with open(temp_path, 'r') as f:
                data = json.load(f)

            self.assertEqual([d["_id"] for d in data], ["doc-1", "doc-2", "doc-3"])
        finally:
            os.unlink(temp_path)

    def test_export_with_scroll_recovers_with_search_after(self):
        """scroll context 过期后应使用 search_after 恢复导出"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        calls = []

        def proxy_request(cluster_id, method, path, body=None):
            self.assertEqual(cluster_id, "system-id")
            calls.append((method, path, body))

            if method == "POST" and path == "/metrics/_search?scroll=5m":
                if body.get("search_after") == [2, "doc-1"]:
                    return {
                        "_scroll_id": "scroll-2",
                        "hits": {
                            "total": {"value": 2},
                            "hits": [
                                {
                                    "_id": "doc-2",
                                    "_source": {"value": 2},
                                    "sort": [1, "doc-2"],
                                }
                            ],
                        },
                    }

                return {
                    "_scroll_id": "scroll-1",
                    "hits": {
                        "total": {"value": 2},
                        "hits": [
                            {
                                "_id": "doc-1",
                                "_source": {"value": 1},
                                "sort": [2, "doc-1"],
                            }
                        ],
                    },
                }

            if method == "POST" and path == "/_search/scroll":
                current_scroll_id = body["scroll_id"]
                if current_scroll_id == "scroll-1":
                    raise metrics_exporter.ConsoleAPIError(
                        'HTTP 404: {"error":{"root_cause":[{"type":"search_context_missing_exception","reason":"No search context found for id [1]"}]}}'
                    )
                if current_scroll_id == "scroll-2":
                    return {
                        "_scroll_id": "scroll-3",
                        "hits": {"hits": []},
                    }

            if method == "DELETE" and path == "/_search/scroll":
                return {"succeeded": True}

            self.fail(f"Unexpected proxy_request call: {(method, path, body)}")

        mock_client.proxy_request.side_effect = proxy_request

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name

        try:
            count = exporter.export_with_scroll(
                index_pattern="metrics",
                query={"query": {"match_all": {}}},
                output_file=temp_path,
                batch_size=1,
                max_docs=0,
            )

            self.assertEqual(count, 2)

            scroll_calls = [call for call in calls if call[0] == "POST" and call[1] == "/_search/scroll"]
            self.assertEqual([call[2]["scroll_id"] for call in scroll_calls], ["scroll-1", "scroll-2"])

            resume_calls = [
                call for call in calls
                if call[0] == "POST"
                and call[1] == "/metrics/_search?scroll=5m"
                and call[2].get("search_after") == [2, "doc-1"]
            ]
            self.assertEqual(len(resume_calls), 1)

            clear_calls = [call for call in calls if call[0] == "DELETE" and call[1] == "/_search/scroll"]
            self.assertEqual([call[2]["scroll_id"] for call in clear_calls], ["scroll-1", "scroll-3"])

            with open(temp_path, 'r') as f:
                data = json.load(f)

            self.assertEqual([doc["_id"] for doc in data], ["doc-1", "doc-2"])
        finally:
            os.unlink(temp_path)


if __name__ == "__main__":
    unittest.main()
