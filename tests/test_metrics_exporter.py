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

JSONLinesWriter = metrics_exporter.JSONLinesWriter
ShardedJSONLinesWriter = metrics_exporter.ShardedJSONLinesWriter
ExportResult = metrics_exporter.ExportResult
MetricsExporter = metrics_exporter.MetricsExporter
METRIC_TYPES = metrics_exporter.METRIC_TYPES
ALERT_TYPES = metrics_exporter.ALERT_TYPES


class TestJSONLinesWriter(unittest.TestCase):
    """测试 JSON Lines 写入器"""

    def test_write_empty(self):
        """写入空数据"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            temp_path = f.name

        try:
            with JSONLinesWriter(temp_path):
                pass

            with open(temp_path, 'r') as f:
                content = f.read()
            self.assertEqual(content, "")
        finally:
            os.unlink(temp_path)

    def test_write_single_doc(self):
        """写入单个文档"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            temp_path = f.name

        try:
            with JSONLinesWriter(temp_path) as writer:
                writer.write_doc({"key": "value"})

            with open(temp_path, 'r') as f:
                content = f.read()
            self.assertIn('"key":"value"', content)
            self.assertNotIn('indent', content)  # 紧凑格式，无 indent
            self.assertFalse(content.startswith("["))  # 不是 JSON 数组
        finally:
            os.unlink(temp_path)

    def test_write_multiple_docs(self):
        """写入多个文档"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            temp_path = f.name

        try:
            with JSONLinesWriter(temp_path, buffer_size=2) as writer:
                writer.write_doc({"id": 1})
                writer.write_doc({"id": 2})
                writer.write_doc({"id": 3})

            with open(temp_path, 'r') as f:
                lines = f.readlines()

            # 验证每行是一个独立的 JSON
            self.assertEqual(len(lines), 3)
            data = [json.loads(line) for line in lines]
            self.assertEqual(data[0]["id"], 1)
            self.assertEqual(data[2]["id"], 3)
        finally:
            os.unlink(temp_path)

    def test_compact_format(self):
        """验证紧凑格式"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            temp_path = f.name

        try:
            with JSONLinesWriter(temp_path) as writer:
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
        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
            temp_path = f.name

        try:
            with JSONLinesWriter(temp_path, buffer_size=10) as writer:
                writer.write_doc({"id": 1})
                writer.flush()

                with open(temp_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                self.assertIn('"id":1', content)
                self.assertFalse(content.startswith("["))  # JSON Lines 不是数组
        finally:
            os.unlink(temp_path)


class TestShardedJSONLinesWriter(unittest.TestCase):
    """测试分片 JSON Lines 写入器"""

    def test_single_shard(self):
        """单个分片"""
        with tempfile.TemporaryDirectory() as temp_dir:
            base_path = os.path.join(temp_dir, "test")

            with ShardedJSONLinesWriter(base_path, shard_size=3) as writer:
                writer.write_doc({"id": 1})
                writer.write_doc({"id": 2})

            # 只有一个文件
            self.assertEqual(len(writer.get_file_paths()), 1)
            self.assertEqual(writer.get_file_paths()[0], "test.jsonl")

            with open(f"{base_path}.jsonl", 'r') as f:
                lines = f.readlines()
            self.assertEqual(len(lines), 2)

    def test_multiple_shards(self):
        """多个分片"""
        with tempfile.TemporaryDirectory() as temp_dir:
            base_path = os.path.join(temp_dir, "test")

            with ShardedJSONLinesWriter(base_path, shard_size=2) as writer:
                writer.write_doc({"id": 1})
                writer.write_doc({"id": 2})
                writer.write_doc({"id": 3})
                writer.write_doc({"id": 4})
                writer.write_doc({"id": 5})

            # 应该有 3 个文件
            self.assertEqual(len(writer.get_file_paths()), 3)
            self.assertEqual(writer.total_count, 5)

            # 验证文件内容
            with open(f"{base_path}.jsonl", 'r') as f:
                self.assertEqual(len(f.readlines()), 2)
            with open(f"{base_path}_1.jsonl", 'r') as f:
                self.assertEqual(len(f.readlines()), 2)
            with open(f"{base_path}_2.jsonl", 'r') as f:
                self.assertEqual(len(f.readlines()), 1)


class TestExportResult(unittest.TestCase):
    """测试导出结果"""

    def test_success_result(self):
        """成功的导出结果"""
        result = ExportResult("node_stats", "节点统计指标")
        result.count = 1000
        result.file_paths = ["node_stats.jsonl"]
        result.duration_ms = 5000

        d = result.to_dict()
        self.assertEqual(d["name"], "节点统计指标")
        self.assertEqual(d["count"], 1000)
        self.assertEqual(d["file"], "node_stats.jsonl")
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

    def test_alert_history_query_uses_timestamp_time_range(self):
        """alert_history 应按 timestamp 应用时间范围"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        query = exporter.build_alert_query("alert_history", time_range_hours=24)

        must_clauses = query["query"]["bool"]["must"]
        self.assertEqual(len(must_clauses), 1)
        self.assertIn("range", must_clauses[0])
        self.assertIn("timestamp", must_clauses[0]["range"])

    def test_alert_messages_query_uses_created_time_range(self):
        """alert_messages 应按 created 应用时间范围"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        query = exporter.build_alert_query("alert_messages", time_range_hours=24)

        must_clauses = query["query"]["bool"]["must"]
        self.assertEqual(len(must_clauses), 1)
        self.assertIn("range", must_clauses[0])
        self.assertIn("created", must_clauses[0]["range"])

    def test_alert_rules_query_remains_match_all(self):
        """alert_rules 是配置数据，应保持全量导出"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        query = exporter.build_alert_query("alert_rules", time_range_hours=24)

        self.assertEqual(query["query"], {"match_all": {}})

    def test_query_with_explicit_time_range_datetime(self):
        """支持 YYYY-MM-DD HH:MM:SS 格式的起止时间"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        query = exporter.build_metrics_query(
            'metadata.name:"node_stats"',
            time_range_hours=24,
            start_time="2026-04-03 11:38:46",
            end_time="2026-04-03 12:38:46",
        )

        must_clauses = query["query"]["bool"]["must"]
        range_clause = [c for c in must_clauses if "range" in c][0]
        ts = range_clause["range"]["timestamp"]

        self.assertEqual(ts["gte"], "2026-04-03T11:38:46+00:00")
        self.assertEqual(ts["lte"], "2026-04-03T12:38:46+00:00")

    def test_query_with_explicit_time_range_date_only(self):
        """支持 YYYY-MM-DD 格式，结束时间自动补 23:59:59"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        query = exporter.build_metrics_query(
            'metadata.name:"node_stats"',
            time_range_hours=24,
            start_time="2026-04-03",
            end_time="2026-04-03",
        )

        must_clauses = query["query"]["bool"]["must"]
        range_clause = [c for c in must_clauses if "range" in c][0]
        ts = range_clause["range"]["timestamp"]

        self.assertEqual(ts["gte"], "2026-04-03T00:00:00+00:00")
        self.assertEqual(ts["lte"], "2026-04-03T23:59:59+00:00")

    def test_query_with_invalid_time_format_raises(self):
        """不支持的时间格式应抛出异常"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        with self.assertRaises(ValueError):
            exporter.build_metrics_query(
                'metadata.name:"node_stats"',
                time_range_hours=24,
                start_time="2026/04/03",
            )

    def test_sampling_group_fields_for_node_stats(self):
        """node_stats 抽样应固定按 cluster_id + node_id 分组"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        fields = exporter._get_sampling_group_fields("node_stats", METRIC_TYPES["node_stats"])
        self.assertEqual(fields, ["metadata.labels.cluster_id", "metadata.labels.node_id"])

    def test_sampling_group_fields_for_index_stats(self):
        """index_stats 抽样应固定按 cluster_id + index_name 分组"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        fields = exporter._get_sampling_group_fields("index_stats", METRIC_TYPES["index_stats"])
        self.assertEqual(fields, ["metadata.labels.cluster_id", "metadata.labels.index_name"])


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

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_base = os.path.join(temp_dir, "test")

            count, file_paths = exporter.export_with_scroll(
                index_pattern="metrics",
                query={"query": {"match_all": {}}},
                output_file=temp_base,
                batch_size=1,
            )

            self.assertEqual(count, 3)

            scroll_calls = [call for call in calls if call[0] == "POST" and call[1] == "/_search/scroll"]
            self.assertEqual([call[2]["scroll_id"] for call in scroll_calls], ["scroll-1", "scroll-2", "scroll-3"])

            # 读取生成的 jsonl 文件
            output_file = f"{temp_base}.jsonl"
            with open(output_file, 'r') as f:
                lines = f.readlines()
            data = [json.loads(line) for line in lines]
            self.assertEqual([doc["_id"] for doc in data], ["doc-1", "doc-2", "doc-3"])

    def test_export_with_scroll_supports_sliced_parallelism(self):
        """parallel_degree>1 时应按 slice 并行导出并汇总结果"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        def proxy_request(cluster_id, method, path, body=None):
            self.assertEqual(cluster_id, "system-id")

            if method == "POST" and path == "/metrics/_search?scroll=5m":
                slice_info = body.get("slice")
                self.assertIsNotNone(slice_info)

                if slice_info["id"] == 0:
                    return {
                        "_scroll_id": "slice-0-scroll",
                        "hits": {
                            "total": {"value": 1},
                            "hits": [
                                {
                                    "_id": "s0-doc-1",
                                    "_source": {"value": 1},
                                    "sort": [2, "s0-doc-1"],
                                }
                            ],
                        },
                    }

                if slice_info["id"] == 1:
                    return {
                        "_scroll_id": "slice-1-scroll",
                        "hits": {
                            "total": {"value": 1},
                            "hits": [
                                {
                                    "_id": "s1-doc-1",
                                    "_source": {"value": 2},
                                    "sort": [1, "s1-doc-1"],
                                }
                            ],
                        },
                    }

            if method == "POST" and path == "/_search/scroll":
                if body["scroll_id"] == "slice-0-scroll":
                    return {"_scroll_id": "slice-0-end", "hits": {"hits": []}}
                if body["scroll_id"] == "slice-1-scroll":
                    return {"_scroll_id": "slice-1-end", "hits": {"hits": []}}

            if method == "DELETE" and path == "/_search/scroll":
                return {"succeeded": True}

            self.fail(f"Unexpected proxy_request call: {(method, path, body)}")

        mock_client.proxy_request.side_effect = proxy_request

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_base = os.path.join(temp_dir, "test")

            count, file_paths = exporter.export_with_scroll(
                index_pattern="metrics",
                query={"query": {"match_all": {}}},
                output_file=temp_base,
                batch_size=1,
                parallel_degree=2,
            )

            self.assertEqual(count, 2)
            self.assertTrue(any(f.startswith("test_slice0") for f in file_paths))
            self.assertTrue(any(f.startswith("test_slice1") for f in file_paths))

            with open(f"{temp_base}_slice0.jsonl", 'r') as f:
                docs0 = [json.loads(line) for line in f.readlines()]
            with open(f"{temp_base}_slice1.jsonl", 'r') as f:
                docs1 = [json.loads(line) for line in f.readlines()]

            self.assertEqual([d["_id"] for d in docs0], ["s0-doc-1"])
            self.assertEqual([d["_id"] for d in docs1], ["s1-doc-1"])


class TestStratifiedSampling(unittest.TestCase):
    """测试 ES 端分层抽样导出"""

    def test_sampling_parallel_splits_on_interval_boundaries(self):
        """并行 sampling 的 worker 时间范围应对齐到 fixed_interval 桶边界"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        sampled_ranges = []

        def proxy_request(cluster_id, method, path, body=None):
            self.assertEqual(cluster_id, "system-id")

            if method == "POST" and path == "/.infini_metrics/_search":
                if body.get("size") == 1 and not body.get("aggs"):
                    return {
                        "hits": {
                            "hits": [
                                {
                                    "_source": {
                                        "metadata": {
                                            "labels": {
                                                "cluster_id": "c1",
                                                "node_id": "n1",
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }

                ts_range = None
                for clause in body.get("query", {}).get("bool", {}).get("must", []):
                    if "range" in clause and "timestamp" in clause["range"]:
                        ts_range = clause["range"]["timestamp"]
                        break

                self.assertIsNotNone(ts_range)
                sampled_ranges.append(ts_range)

                return {"aggregations": {"sampled": {"buckets": []}}}

            self.fail(f"Unexpected proxy_request call: {(method, path, body)}")

        mock_client.proxy_request.side_effect = proxy_request

        exporter.export_with_es_sampling(
            index_pattern=".infini_metrics",
            query={
                "query": {
                    "bool": {
                        "must": [
                            {"query_string": {"query": 'metadata.name:"node_stats"'}},
                            {
                                "range": {
                                    "timestamp": {
                                        "gte": "2026-04-02T00:07:30+00:00",
                                        "lte": "2026-04-02T00:52:30+00:00",
                                    }
                                }
                            },
                        ]
                    }
                },
                "sort": [{"timestamp": {"order": "desc"}}],
                "_source": True,
            },
            output_file="/tmp/unused-test-output",
            sampling=SamplingConfig(mode="sampling", interval="15m"),
            batch_size=3000,
            group_fields=["metadata.labels.cluster_id", "metadata.labels.node_id"],
            parallel_degree=2,
        )

        # 第一次是探测字段，后面两个是 worker 查询
        worker_ranges = sampled_ranges[-2:]
        self.assertEqual(len(worker_ranges), 2)

        actual_ranges = {
            tuple(sorted(r.items()))
            for r in worker_ranges
        }
        expected_ranges = {
            tuple(sorted({"gte": "2026-04-02T00:07:30+00:00", "lt": "2026-04-02T00:30:00+00:00"}.items())),
            tuple(sorted({"gte": "2026-04-02T00:30:00+00:00", "lte": "2026-04-02T00:52:30+00:00"}.items())),
        }
        self.assertEqual(actual_ranges, expected_ranges)

    def test_sampling_parallel_writes_per_worker_files(self):
        """sampling 并行应按 worker 分别写文件"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        def proxy_request(cluster_id, method, path, body=None):
            self.assertEqual(cluster_id, "system-id")

            if method == "POST" and path == "/.infini_metrics/_search":
                if body.get("size") == 1 and not body.get("aggs"):
                    return {
                        "hits": {
                            "hits": [
                                {
                                    "_source": {
                                        "metadata": {
                                            "labels": {
                                                "cluster_id": "c1",
                                                "node_id": "n1",
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }

                self.assertNotIn("slice", body)

                # 根据拆分后的 timestamp 范围区分 worker 请求
                ts_range = None
                for c in body.get("query", {}).get("bool", {}).get("must", []):
                    if "range" in c and "timestamp" in c["range"]:
                        ts_range = c["range"]["timestamp"]
                        break

                if ts_range and "lt" in ts_range:
                    return {
                        "aggregations": {
                            "sampled": {
                                "buckets": [
                                    {
                                        "key": {
                                            "group_0": "c1",
                                            "group_1": "n1",
                                            "time_bucket": 1712016000000,
                                        },
                                        "latest": {
                                            "hits": {
                                                "hits": [
                                                    {
                                                        "_id": "worker0-doc",
                                                        "_source": {"timestamp": "2026-04-02T00:00:00Z", "v": 1},
                                                        "sort": [1712016000000],
                                                    }
                                                ]
                                            }
                                        },
                                    }
                                ]
                            }
                        }
                    }

                if ts_range and "lte" in ts_range:
                    return {
                        "aggregations": {
                            "sampled": {
                                "buckets": [
                                    {
                                        "key": {
                                            "group_0": "c1",
                                            "group_1": "n1",
                                            "time_bucket": 1712016000000,
                                        },
                                        "latest": {
                                            "hits": {
                                                "hits": [
                                                    {
                                                        "_id": "worker1-doc",
                                                        "_source": {"timestamp": "2026-04-02T00:00:00Z", "v": 2},
                                                        "sort": [1712016000000],
                                                    }
                                                ]
                                            }
                                        },
                                    }
                                ]
                            }
                        }
                    }

            self.fail(f"Unexpected proxy_request call: {(method, path, body)}")

        mock_client.proxy_request.side_effect = proxy_request

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_base = os.path.join(temp_dir, "test")

            result = exporter.export_metric_type(
                metric_type="node_stats",
                config=METRIC_TYPES["node_stats"],
                output_file=temp_base,
                time_range_hours=24,
                cluster_ids=["cluster-1"],
                sampling=SamplingConfig(mode="sampling", interval="15m"),
                skip_estimation=True,
                parallel_degree=2,
            )

            self.assertEqual(result.count, 2)
            self.assertTrue(any(f.startswith("test_worker0") for f in result.file_paths))
            self.assertTrue(any(f.startswith("test_worker1") for f in result.file_paths))

            with open(f"{temp_base}_worker0.jsonl", 'r') as f:
                data0 = [json.loads(line) for line in f.readlines()]
            with open(f"{temp_base}_worker1.jsonl", 'r') as f:
                data1 = [json.loads(line) for line in f.readlines()]

            self.assertEqual([d["_id"] for d in data0], ["worker0-doc"])
            self.assertEqual([d["_id"] for d in data1], ["worker1-doc"])

    def test_sampling_parallel_same_bucket_keeps_worker_outputs(self):
        """并发 sampling 下即便 bucket 相同，也按 worker 独立写文件"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        def proxy_request(cluster_id, method, path, body=None):
            self.assertEqual(cluster_id, "system-id")

            if method == "POST" and path == "/.infini_metrics/_search":
                if body.get("size") == 1 and not body.get("aggs"):
                    return {
                        "hits": {
                            "hits": [
                                {
                                    "_source": {
                                        "metadata": {
                                            "labels": {
                                                "cluster_id": "c1",
                                                "node_id": "n1",
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }

                self.assertNotIn("slice", body)

                ts_range = None
                for c in body.get("query", {}).get("bool", {}).get("must", []):
                    if "range" in c and "timestamp" in c["range"]:
                        ts_range = c["range"]["timestamp"]
                        break

                same_bucket_key = {
                    "group_0": "c1",
                    "group_1": "n1",
                    "time_bucket": 1712016000000,
                }

                if ts_range and "lt" in ts_range:
                    return {
                        "aggregations": {
                            "sampled": {
                                "buckets": [
                                    {
                                        "key": same_bucket_key,
                                        "latest": {
                                            "hits": {
                                                "hits": [
                                                    {
                                                        "_id": "older-doc",
                                                        "_source": {"timestamp": "2026-04-02T00:00:00Z", "v": 1},
                                                        "sort": [1712016000000],
                                                    }
                                                ]
                                            }
                                        },
                                    }
                                ]
                            }
                        }
                    }

                if ts_range and "lte" in ts_range:
                    return {
                        "aggregations": {
                            "sampled": {
                                "buckets": [
                                    {
                                        "key": same_bucket_key,
                                        "latest": {
                                            "hits": {
                                                "hits": [
                                                    {
                                                        "_id": "newer-doc",
                                                        "_source": {"timestamp": "2026-04-02T00:05:00Z", "v": 2},
                                                        "sort": [1712016300000],
                                                    }
                                                ]
                                            }
                                        },
                                    }
                                ]
                            }
                        }
                    }

            self.fail(f"Unexpected proxy_request call: {(method, path, body)}")

        mock_client.proxy_request.side_effect = proxy_request

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_base = os.path.join(temp_dir, "test")

            result = exporter.export_metric_type(
                metric_type="node_stats",
                config=METRIC_TYPES["node_stats"],
                output_file=temp_base,
                time_range_hours=24,
                cluster_ids=["cluster-1"],
                sampling=SamplingConfig(mode="sampling", interval="15m"),
                skip_estimation=True,
                parallel_degree=2,
            )

            self.assertEqual(result.count, 2)

            with open(f"{temp_base}_worker0.jsonl", 'r') as f:
                data0 = [json.loads(line) for line in f.readlines()]
            with open(f"{temp_base}_worker1.jsonl", 'r') as f:
                data1 = [json.loads(line) for line in f.readlines()]

            self.assertEqual([d["_id"] for d in data0], ["older-doc"])
            self.assertEqual([d["_id"] for d in data1], ["newer-doc"])

    def test_sampling_path_supports_parallel_degree(self):
        """sampling 模式在 parallel_degree>1 时应走 sliced 查询并汇总"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        calls = []

        def proxy_request(cluster_id, method, path, body=None):
            self.assertEqual(cluster_id, "system-id")
            calls.append((method, path, body))

            if method == "POST" and path == "/.infini_metrics/_search":
                # _detect_valid_group_fields
                if body.get("size") == 1 and not body.get("aggs"):
                    return {
                        "hits": {
                            "hits": [
                                {
                                    "_source": {
                                        "metadata": {
                                            "labels": {
                                                "cluster_id": "c1",
                                                "node_id": "n1",
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }

                sampled_agg = body.get("aggs", {}).get("sampled", {})
                if not sampled_agg:
                    self.fail(f"Unexpected _search body: {body}")

                self.assertNotIn("slice", body)

                ts_range = None
                for c in body.get("query", {}).get("bool", {}).get("must", []):
                    if "range" in c and "timestamp" in c["range"]:
                        ts_range = c["range"]["timestamp"]
                        break

                # 每个 worker 返回一个桶
                if ts_range and "lt" in ts_range:
                    return {
                        "aggregations": {
                            "sampled": {
                                "buckets": [
                                    {
                                        "key": {
                                            "group_0": "c1",
                                            "group_1": "n1",
                                            "time_bucket": 1712016000000,
                                        },
                                        "latest": {
                                            "hits": {
                                                "hits": [
                                                    {
                                                        "_id": "doc-s0",
                                                        "_source": {"timestamp": "2026-04-02T00:00:00Z", "v": 1},
                                                        "sort": [1712016000000],
                                                    }
                                                ]
                                            }
                                        },
                                    }
                                ]
                            }
                        }
                    }

                if ts_range and "lte" in ts_range:
                    return {
                        "aggregations": {
                            "sampled": {
                                "buckets": [
                                    {
                                        "key": {
                                            "group_0": "c1",
                                            "group_1": "n2",
                                            "time_bucket": 1712016000000,
                                        },
                                        "latest": {
                                            "hits": {
                                                "hits": [
                                                    {
                                                        "_id": "doc-s1",
                                                        "_source": {"timestamp": "2026-04-02T00:00:00Z", "v": 2},
                                                        "sort": [1712016000000],
                                                    }
                                                ]
                                            }
                                        },
                                    }
                                ]
                            }
                        }
                    }

            self.fail(f"Unexpected proxy_request call: {(method, path, body)}")

        mock_client.proxy_request.side_effect = proxy_request

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_base = os.path.join(temp_dir, "test")

            result = exporter.export_metric_type(
                metric_type="node_stats",
                config=METRIC_TYPES["node_stats"],
                output_file=temp_base,
                time_range_hours=24,
                cluster_ids=["cluster-1"],
                sampling=SamplingConfig(mode="sampling", interval="15m"),
                skip_estimation=True,
                parallel_degree=2,
            )

            self.assertEqual(result.count, 2)

            search_calls = [c for c in calls if c[0] == "POST" and c[1] == "/.infini_metrics/_search"]
            sampled_calls = [c for c in search_calls if c[2].get("aggs")]
            self.assertEqual(len(sampled_calls), 2)
            self.assertTrue(all("slice" not in c[2] for c in sampled_calls))

            self.assertTrue(any(f.startswith("test_worker0") for f in result.file_paths))
            self.assertTrue(any(f.startswith("test_worker1") for f in result.file_paths))

            with open(f"{temp_base}_worker0.jsonl", 'r') as f:
                data0 = [json.loads(line) for line in f.readlines()]
            with open(f"{temp_base}_worker1.jsonl", 'r') as f:
                data1 = [json.loads(line) for line in f.readlines()]
            self.assertEqual([d["_id"] for d in data0], ["doc-s0"])
            self.assertEqual([d["_id"] for d in data1], ["doc-s1"])

    def test_cluster_stats_interval_sampling_uses_es_aggregations(self):
        """所有指标的 interval 抽样都应走 ES 聚合"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        calls = []

        def proxy_request(cluster_id, method, path, body=None):
            self.assertEqual(cluster_id, "system-id")
            calls.append((method, path, body))

            # estimate_export_count 先调用 _count
            if method == "POST" and path == "/.infini_metrics/_count":
                return {"count": 100}

            if method == "POST" and path == "/.infini_metrics/_search":
                # _detect_valid_group_fields 会调用 _search（size=1，无聚合）
                if body.get("size") == 1 and not body.get("aggs"):
                    return {
                        "hits": {
                            "hits": [
                                {
                                    "_source": {
                                        "metadata": {
                                            "labels": {
                                                "cluster_id": "c1"
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }

                # 检查是否是 estimate_export_count 的聚合查询（没有 latest 子聚合）
                sampled_agg = body.get("aggs", {}).get("sampled", {})
                if "aggs" not in sampled_agg:
                    # 预估阶段，返回一个 bucket 表示有一条数据
                    return {
                        "aggregations": {
                            "sampled": {
                                "buckets": [{"key": "bucket-1"}]
                            }
                        }
                    }

                # 实际导出的聚合查询（有 latest 子聚合）
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

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_base = os.path.join(temp_dir, "test")

            result = exporter.export_metric_type(
                metric_type="cluster_stats",
                config=METRIC_TYPES["cluster_stats"],
                output_file=temp_base,
                time_range_hours=24,
                cluster_ids=["cluster-1"],
                sampling=SamplingConfig(mode="sampling", interval="15m"),
            )

            self.assertEqual(result.count, 1)
            # 1次 _count + 1次 _detect_valid_group_fields(预估) + 1次 estimate 聚合 + 1次 _detect_valid_group_fields(导出) + 1次实际导出聚合
            self.assertEqual(len([c for c in calls if c[1] == "/.infini_metrics/_search"]), 4)
            self.assertEqual(len([c for c in calls if c[1] == "/.infini_metrics/_count"]), 1)

    def test_node_stats_interval_sampling_uses_es_aggregations(self):
        """node_stats interval 抽样应走 ES 聚合而非 scroll 全量拉取"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        calls = []

        def proxy_request(cluster_id, method, path, body=None):
            self.assertEqual(cluster_id, "system-id")
            calls.append((method, path, body))

            # estimate_export_count 先调用 _count
            if method == "POST" and path == "/.infini_metrics/_count":
                return {"count": 100}

            if method == "POST" and path == "/.infini_metrics/_search":
                # _detect_valid_group_fields 会调用 _search（size=1，无聚合）
                if body.get("size") == 1 and not body.get("aggs"):
                    return {
                        "hits": {
                            "hits": [
                                {
                                    "_source": {
                                        "metadata": {
                                            "labels": {
                                                "cluster_id": "c1",
                                                "node_id": "n1"
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }

                sampled_agg = body.get("aggs", {}).get("sampled", {})

                # 预估阶段只有 composite，没有 aggs 子聚合
                if "aggs" not in sampled_agg:
                    return {
                        "aggregations": {
                            "sampled": {
                                "buckets": []
                            }
                        }
                    }

                # 实际导出的聚合查询
                after = sampled_agg.get("composite", {}).get("after")
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

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_base = os.path.join(temp_dir, "test")

            result = exporter.export_metric_type(
                metric_type="node_stats",
                config=METRIC_TYPES["node_stats"],
                output_file=temp_base,
                time_range_hours=24,
                cluster_ids=["cluster-1"],
                sampling=SamplingConfig(mode="sampling", interval="15m"),
            )

            self.assertEqual(result.count, 3)

            # 1次 _count + 1次 _detect_valid_group_fields(预估) + 1次 estimate 聚合 + 1次 _detect_valid_group_fields(导出) + 2次实际导出聚合（有 after_key）
            search_calls = [c for c in calls if c[0] == "POST" and c[1] == "/.infini_metrics/_search"]
            self.assertEqual(len(search_calls), 5)
            self.assertEqual(len([c for c in calls if c[1] == "/.infini_metrics/_count"]), 1)

            # 读取生成的 jsonl 文件
            output_file = f"{temp_base}.jsonl"
            with open(output_file, 'r') as f:
                lines = f.readlines()
            data = [json.loads(line) for line in lines]
            self.assertEqual([d["_id"] for d in data], ["doc-1", "doc-2", "doc-3"])

    def test_sampling_bucket_uses_avg_value_as_sample_point(self):
        """sampling 应优先使用桶内 avg 值作为采样点 value"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        def proxy_request(cluster_id, method, path, body=None):
            self.assertEqual(cluster_id, "system-id")

            if method == "POST" and path == "/.infini_metrics/_search":
                if body.get("size") == 1 and not body.get("aggs"):
                    return {
                        "hits": {
                            "hits": [
                                {
                                    "_source": {
                                        "metadata": {
                                            "labels": {
                                                "cluster_id": "c1",
                                                "node_id": "n1",
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }

                return {
                    "aggregations": {
                        "sampled": {
                            "buckets": [
                                {
                                    "key": {
                                        "group_0": "c1",
                                        "group_1": "n1",
                                        "time_bucket": 1712016000000,
                                    },
                                    "latest": {
                                        "hits": {
                                            "hits": [
                                                {
                                                    "_id": "doc-1",
                                                    "_source": {
                                                        "timestamp": "2026-04-02T00:00:00Z",
                                                        "value": 10,
                                                    },
                                                }
                                            ]
                                        }
                                    },
                                    "avg_0": {"value": 25.5},
                                }
                            ]
                        }
                    }
                }

            self.fail(f"Unexpected proxy_request call: {(method, path, body)}")

        mock_client.proxy_request.side_effect = proxy_request

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_base = os.path.join(temp_dir, "test")

            result = exporter.export_metric_type(
                metric_type="node_stats",
                config=METRIC_TYPES["node_stats"],
                output_file=temp_base,
                time_range_hours=24,
                cluster_ids=["cluster-1"],
                sampling=SamplingConfig(mode="sampling", interval="15m"),
                skip_estimation=True,
            )

            self.assertEqual(result.count, 1)

            with open(f"{temp_base}.jsonl", 'r') as f:
                data = [json.loads(line) for line in f.readlines()]

            self.assertEqual(data[0]["_id"], "doc-1")
            self.assertEqual(data[0]["value"], 25.5)

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

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_base = os.path.join(temp_dir, "test")

            count, file_paths = exporter.export_with_scroll(
                index_pattern="metrics",
                query={"query": {"match_all": {}}},
                output_file=temp_base,
                batch_size=1,
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

            # 读取生成的 jsonl 文件
            output_file = f"{temp_base}.jsonl"
            with open(output_file, 'r') as f:
                lines = f.readlines()
            data = [json.loads(line) for line in lines]
            self.assertEqual([doc["_id"] for doc in data], ["doc-1", "doc-2"])


class TestSkipEstimation(unittest.TestCase):
    """测试跳过预估功能"""

    def test_skip_estimation_skips_count_query(self):
        """skip_estimation=True 时跳过 _count 查询"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        calls = []

        def proxy_request(cluster_id, method, path, body=None):
            calls.append((method, path))
            if method == "POST" and path == "/.infini_metrics/_search?scroll=5m":
                return {
                    "_scroll_id": "scroll-1",
                    "hits": {
                        "total": {"value": 1},
                        "hits": [
                            {
                                "_id": "doc-1",
                                "_source": {"timestamp": "2026-04-02T00:00:00Z", "v": 1},
                                "sort": [1, "doc-1"],
                            }
                        ],
                    },
                }
            if method == "POST" and path == "/_search/scroll":
                return {"_scroll_id": "scroll-2", "hits": {"hits": []}}
            if method == "DELETE":
                return {"succeeded": True}
            self.fail(f"Unexpected call: {method} {path}")

        mock_client.proxy_request.side_effect = proxy_request

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_base = os.path.join(temp_dir, "test")

            result = exporter.export_metric_type(
                metric_type="node_stats",
                config=METRIC_TYPES["node_stats"],
                output_file=temp_base,
                time_range_hours=24,
                skip_estimation=True,
            )

            self.assertEqual(result.count, 1)
            # 不应该有 _count 调用
            self.assertFalse(any(c[1] == "/.infini_metrics/_count" for c in calls))

    def test_skip_estimation_false_calls_count_query(self):
        """skip_estimation=False 时执行 _count 查询"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        calls = []

        def proxy_request(cluster_id, method, path, body=None):
            calls.append((method, path))
            if method == "POST" and path == "/.infini_metrics/_count":
                return {"count": 100}
            if method == "POST" and path == "/.infini_metrics/_search?scroll=5m":
                return {
                    "_scroll_id": "scroll-1",
                    "hits": {
                        "total": {"value": 1},
                        "hits": [
                            {
                                "_id": "doc-1",
                                "_source": {"timestamp": "2026-04-02T00:00:00Z", "v": 1},
                                "sort": [1, "doc-1"],
                            }
                        ],
                    },
                }
            if method == "POST" and path == "/_search/scroll":
                return {"_scroll_id": "scroll-2", "hits": {"hits": []}}
            if method == "DELETE":
                return {"succeeded": True}
            self.fail(f"Unexpected call: {method} {path}")

        mock_client.proxy_request.side_effect = proxy_request

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_base = os.path.join(temp_dir, "test")

            result = exporter.export_metric_type(
                metric_type="node_stats",
                config=METRIC_TYPES["node_stats"],
                output_file=temp_base,
                time_range_hours=24,
                skip_estimation=False,
            )

            self.assertEqual(result.count, 1)
            # 应该有 _count 调用
            self.assertTrue(any(c[1] == "/.infini_metrics/_count" for c in calls))

    def test_skip_estimation_with_sampling_skips_composite_aggregation(self):
        """skip_estimation=True 时跳过抽样预估的 composite aggregation"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-id")

        calls = []

        def proxy_request(cluster_id, method, path, body=None):
            calls.append((method, path, body))

            if method == "POST" and path == "/.infini_metrics/_search":
                sampled_agg = body.get("aggs", {}).get("sampled", {})

                # _detect_valid_group_fields 会调用 _search（size=1，无聚合）
                if body.get("size") == 1 and not body.get("aggs"):
                    return {
                        "hits": {
                            "hits": [
                                {
                                    "_source": {
                                        "metadata": {
                                            "labels": {
                                                "cluster_id": "c1",
                                                "node_id": "n1"
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }

                # 实际导出的聚合（有 latest 子聚合）
                if "aggs" in sampled_agg:
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
                                ],
                                "after_key": None,
                            }
                        }
                    }

                # 预估阶段的 composite aggregation（不应该被调用）
                self.fail("预估阶段的 composite aggregation 不应该被调用")

            if method == "POST" and path == "/.infini_metrics/_count":
                self.fail("_count 查询不应该被调用")

            self.fail(f"Unexpected call: {method} {path}")

        mock_client.proxy_request.side_effect = proxy_request

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_base = os.path.join(temp_dir, "test")

            result = exporter.export_metric_type(
                metric_type="node_stats",
                config=METRIC_TYPES["node_stats"],
                output_file=temp_base,
                time_range_hours=24,
                sampling=SamplingConfig(mode="sampling", interval="15m"),
                skip_estimation=True,
            )

            self.assertEqual(result.count, 1)
            # 不应该有 _count 调用
            self.assertFalse(any(c[1] == "/.infini_metrics/_count" for c in calls))
            # 应该有两次 _search：一次是 _detect_valid_group_fields，一次是实际导出
            search_calls = [c for c in calls if c[0] == "POST" and c[1] == "/.infini_metrics/_search"]
            self.assertEqual(len(search_calls), 2)


class TestIPMasking(unittest.TestCase):
    """测试IP地址脱敏功能"""

    def test_mask_simple_ipv4(self):
        """脱敏简单IPv4地址"""
        test_cases = [
            ("192.168.1.1", "*.*.1.1"),
            ("10.0.0.1", "*.*.0.1"),
            ("172.16.254.1", "*.*.254.1"),
            ("127.0.0.1", "*.*.0.1"),
            ("8.8.8.8", "*.*.8.8"),
            ("255.255.255.255", "*.*.255.255"),
        ]
        
        for ip_input, expected in test_cases:
            result = MetricsExporter._mask_doc(ip_input)
            self.assertEqual(result, expected, f"脱敏 {ip_input} 失败")

    def test_mask_multiple_ips_in_string(self):
        """脱敏字符串中的多个IP地址"""
        input_str = "IP is 192.168.1.1 and 10.0.0.1"
        expected = "IP is *.*.1.1 and *.*.0.1"
        result = MetricsExporter._mask_doc(input_str)
        self.assertEqual(result, expected)

    def test_mask_no_ip(self):
        """对无IP的字符串不做改动"""
        input_str = "No IP here, just text"
        result = MetricsExporter._mask_doc(input_str)
        self.assertEqual(result, input_str)

    def test_mask_doc_with_nested_ips(self):
        """脱敏嵌套文档中的IP地址"""
        doc = {
            "timestamp": "2026-04-02T10:00:00Z",
            "node": {
                "id": "node-1",
                "ip": "192.168.1.1",
                "name": "es-node-1"
            },
            "cluster": {
                "id": "cluster-1",
                "master": "10.0.0.1"
            }
        }
        
        result = MetricsExporter._mask_doc(doc)
        
        # 验证脱敏结果
        self.assertEqual(result["node"]["ip"], "*.*.1.1")
        self.assertEqual(result["cluster"]["master"], "*.*.0.1")
        # 非IP字段不变
        self.assertEqual(result["node"]["id"], "node-1")
        self.assertEqual(result["timestamp"], "2026-04-02T10:00:00Z")

    def test_mask_doc_with_array(self):
        """脱敏数组中的IP地址"""
        doc = {
            "peers": ["192.168.1.1", "10.0.0.1", "172.16.0.1"],
            "tags": ["ip:192.168.1.1", "host:es-node-1"]
        }
        
        result = MetricsExporter._mask_doc(doc)
        
        # 验证数组元素被脱敏
        self.assertEqual(result["peers"], ["*.*.1.1", "*.*.0.1", "*.*.0.1"])
        self.assertEqual(result["tags"], ["ip:*.*.1.1", "host:es-node-1"])

    def test_mask_doc_with_mixed_types(self):
        """脱敏混合类型的文档"""
        doc = {
            "string_ip": "192.168.1.1",
            "number": 42,
            "float": 3.14,
            "boolean": True,
            "null_value": None,
            "nested": {
                "ip": "10.0.0.1",
                "count": 100
            },
            "array": [1, 2, "192.168.100.200"]
        }
        
        result = MetricsExporter._mask_doc(doc)
        
        # 访问IP字段的脱敏
        self.assertEqual(result["string_ip"], "*.*.1.1")
        self.assertEqual(result["nested"]["ip"], "*.*.0.1")
        self.assertEqual(result["array"][2], "*.*.100.200")
        
        # 非string类型不变
        self.assertEqual(result["number"], 42)
        self.assertEqual(result["float"], 3.14)
        self.assertEqual(result["boolean"], True)
        self.assertIsNone(result["null_value"])
        self.assertEqual(result["nested"]["count"], 100)

    def test_mask_empty_string(self):
        """脱敏空字符串"""
        result = MetricsExporter._mask_doc("")
        self.assertEqual(result, "")

    def test_mask_empty_dict(self):
        """脱敏空字典"""
        result = MetricsExporter._mask_doc({})
        self.assertEqual(result, {})

    def test_mask_empty_list(self):
        """脱敏空列表"""
        result = MetricsExporter._mask_doc([])
        self.assertEqual(result, [])

    def test_mask_ip_in_json_string(self):
        """脱敏JSON字符串中的IP（以字符串形式）"""
        # 这个测试验证当JSON内容本身是字符串时的行为
        doc = {
            "config": '{"server": "192.168.1.1"}'
        }
        
        result = MetricsExporter._mask_doc(doc)
        
        # 验证JSON字符串中的IP被脱敏
        self.assertIn("*.*.1.1", result["config"])

    def test_deeply_nested_structure(self):
        """脱敏深层嵌套结构"""
        doc = {
            "level1": {
                "level2": {
                    "level3": {
                        "ip": "192.168.1.1",
                        "peers": ["10.0.0.1", "10.0.0.2"]
                    }
                }
            }
        }
        
        result = MetricsExporter._mask_doc(doc)
        
        self.assertEqual(result["level1"]["level2"]["level3"]["ip"], "*.*.1.1")
        self.assertEqual(result["level1"]["level2"]["level3"]["peers"], ["*.*.0.1", "*.*.0.2"])


if __name__ == "__main__":
    unittest.main()
