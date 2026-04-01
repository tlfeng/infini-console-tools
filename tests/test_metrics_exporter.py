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


class TestSampling(unittest.TestCase):
    """测试抽样逻辑"""

    def test_interval_sampling_hourly(self):
        """测试按小时间隔抽样"""
        # 创建模拟导出器
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-cluster-id")

        # 创建测试数据：每小时一条，共10条
        now = datetime.now(timezone.utc)
        docs = []
        for i in range(10):
            ts = now - timedelta(hours=i)
            docs.append({
                "timestamp": ts.isoformat(),
                "value": i
            })

        sampling = SamplingConfig(mode="sampling", interval="1h")
        sampled = exporter._interval_sampling(docs, "1h")

        # 每5条保留一条（因为间隔足够大）
        self.assertGreater(len(sampled), 0)
        self.assertLessEqual(len(sampled), len(docs))

    def test_interval_sampling_minutes(self):
        """测试按分钟间隔抽样"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-cluster-id")

        now = datetime.now(timezone.utc)
        docs = []
        for i in range(60):
            ts = now - timedelta(minutes=i)
            docs.append({
                "timestamp": ts.isoformat(),
                "value": i
            })

        sampling = SamplingConfig(mode="sampling", interval="5m")
        sampled = exporter._interval_sampling(docs, "5m")

        # 5分钟间隔应该保留约12条
        self.assertGreater(len(sampled), 8)
        self.assertLess(len(sampled), 20)

    def test_ratio_sampling(self):
        """测试比例抽样"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-cluster-id")

        docs = [{"id": i} for i in range(1000)]

        # 设置随机种子以保证可重复性
        import random
        random.seed(42)

        sampled = exporter._ratio_sampling(docs, 0.1)

        # 10% 比例应该接近100条
        self.assertGreater(len(sampled), 50)
        self.assertLess(len(sampled), 150)

    def test_full_mode_no_sampling(self):
        """全量模式不抽样"""
        mock_client = MagicMock()
        exporter = MetricsExporter(mock_client, "system-cluster-id")

        docs = [{"id": i} for i in range(100)]

        sampling = SamplingConfig(mode="full")
        result = exporter._apply_sampling(docs, sampling)

        self.assertEqual(len(result), 100)


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

        self.assertEqual(exporter.scroll_keepalive, "60s")
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


if __name__ == "__main__":
    unittest.main()
