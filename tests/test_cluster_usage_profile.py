#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 Cluster Usage Profile 模块
"""

import sys
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "cluster-usage-profile"))

from cluster_usage_profile import (
    parse_size,
    to_int,
    to_float,
    strip_date_suffix,
    summarize_node_role,
    shard_size_bucket,
    classify_activity,
    compute_index_rates,
    match_action_family,
    aggregate_rest_actions,
    infer_scenario_labels,
    UsageProfiler,
)


class TestParseSize(unittest.TestCase):
    """测试人类可读大小解析"""

    def test_common_units(self):
        self.assertEqual(parse_size("272b"), 272)
        self.assertEqual(parse_size("9.4kb"), int(9.4 * 1024))
        self.assertEqual(parse_size("204mb"), 204 * 1024 ** 2)
        self.assertEqual(parse_size("5.1gb"), int(5.1 * 1024 ** 3))
        self.assertEqual(parse_size("2.85tb"), int(2.85 * 1024 ** 4))

    def test_edge_cases(self):
        self.assertEqual(parse_size(None), 0)
        self.assertEqual(parse_size(""), 0)
        self.assertEqual(parse_size("unknown"), 0)
        self.assertEqual(parse_size(1024), 1024)


class TestToIntToFloat(unittest.TestCase):
    """测试安全数值转换"""

    def test_to_int(self):
        self.assertEqual(to_int("123"), 123)
        self.assertEqual(to_int(None), 0)
        self.assertEqual(to_int("abc"), 0)
        self.assertEqual(to_int("12", 5), 12)
        self.assertEqual(to_int("abc", 5), 5)

    def test_to_float(self):
        self.assertEqual(to_float("1.5"), 1.5)
        self.assertEqual(to_float(None), 0.0)
        self.assertEqual(to_float("x", 2.0), 2.0)


class TestStripDateSuffix(unittest.TestCase):
    """测试索引名日期后缀剥离"""

    def test_daily_pattern(self):
        self.assertEqual(strip_date_suffix("log-2026.09.21"), ("log", "2026.09.21"))
        self.assertEqual(strip_date_suffix("log_2026_09_21"), ("log", "2026_09_21"))
        self.assertEqual(strip_date_suffix("app20260921"), ("app", "20260921"))

    def test_monthly_yearly_pattern(self):
        self.assertEqual(strip_date_suffix("order-2026.09"), ("order", "2026.09"))
        self.assertEqual(strip_date_suffix("audit-2026"), ("audit", "2026"))

    def test_no_pattern(self):
        self.assertEqual(strip_date_suffix("business-index"), ("business-index", None))
        self.assertEqual(strip_date_suffix("es6-1"), ("es6-1", None))
        self.assertEqual(strip_date_suffix("picc-acrm"), ("picc-acrm", None))


class TestSummarizeNodeRole(unittest.TestCase):
    """测试节点角色解析"""

    def test_6x_roles(self):
        self.assertEqual(summarize_node_role("dim"),
                         {"data", "master_eligible", "ingest"})
        self.assertEqual(summarize_node_role("di"), {"data", "ingest"})

    def test_7x_roles(self):
        # 7.10 默认 data 节点角色串（不含 master）
        roles = summarize_node_role("cdhilstw")
        self.assertIn("data", roles)
        self.assertIn("cold_data", roles)
        self.assertIn("hot_data", roles)
        self.assertNotIn("master_eligible", roles)
        # 带 master 的 7.x 角色
        roles = summarize_node_role("cdhilmrstw")
        self.assertIn("master_eligible", roles)

    def test_coordinating_only(self):
        self.assertEqual(summarize_node_role("-"), {"coordinating"})
        self.assertEqual(summarize_node_role(""), {"coordinating"})
        self.assertEqual(summarize_node_role(None), {"coordinating"})


class TestShardSizeBucket(unittest.TestCase):
    """测试分片大小分桶"""

    MB = 1024 ** 2
    GB = 1024 ** 3

    def test_buckets(self):
        self.assertEqual(shard_size_bucket(50 * self.MB), "<100MB")
        self.assertEqual(shard_size_bucket(500 * self.MB), "100MB-1GB")
        self.assertEqual(shard_size_bucket(5 * self.GB), "1-10GB")
        self.assertEqual(shard_size_bucket(20 * self.GB), "10-30GB")
        self.assertEqual(shard_size_bucket(40 * self.GB), "30-50GB")
        self.assertEqual(shard_size_bucket(80 * self.GB), ">=50GB")


class TestClassifyActivity(unittest.TestCase):
    """测试活跃度分类"""

    def test_classification(self):
        self.assertEqual(classify_activity(1.0, 1.0), "读写均活跃")
        self.assertEqual(classify_activity(1.0, 0.0), "查询为主")
        self.assertEqual(classify_activity(0.0, 1.0), "写入为主")
        self.assertEqual(classify_activity(0.01, 0.0), "低频")
        self.assertEqual(classify_activity(0.0, 0.0), "闲置")


class TestComputeIndexRates(unittest.TestCase):
    """测试两轮采样速率计算"""

    def test_basic_rates(self):
        snap0 = {
            "a": {"query_total": 1000, "index_total": 100},
            "b": {"query_total": 0, "index_total": 500},
            "c": {"query_total": 10, "index_total": 10},  # 只在 t0 存在，不输出
        }
        snap1 = {
            "a": {"query_total": 1600, "index_total": 100},
            "b": {"query_total": 60, "index_total": 800},
        }
        rates = compute_index_rates(snap0, snap1, 60)
        self.assertAlmostEqual(rates["a"]["qps"], 10.0)
        self.assertAlmostEqual(rates["a"]["ips"], 0.0)
        self.assertAlmostEqual(rates["b"]["qps"], 1.0)
        self.assertAlmostEqual(rates["b"]["ips"], 5.0)
        self.assertNotIn("c", rates)

    def test_counter_rollback_clamped(self):
        # 索引重建导致计数回退时按 0 处理
        snap0 = {"a": {"query_total": 100, "index_total": 100}}
        snap1 = {"a": {"query_total": 50, "index_total": 80}}
        rates = compute_index_rates(snap0, snap1, 10)
        self.assertEqual(rates["a"]["qps"], 0.0)
        self.assertEqual(rates["a"]["ips"], 0.0)

    def test_zero_interval(self):
        self.assertEqual(compute_index_rates({}, {}, 0), {})


class TestActionFamilies(unittest.TestCase):
    """测试 REST action 家族归类与聚合"""

    def test_match_family(self):
        self.assertEqual(match_action_family(
            "org.elasticsearch.rest.action.document.RestBulkAction"), "Bulk批量写入")
        self.assertEqual(match_action_family(
            "org.elasticsearch.rest.action.search.RestSearchAction"), "Search检索")
        self.assertEqual(match_action_family(
            "org.elasticsearch.rest.action.document.RestGetAction"), "Get点查")
        self.assertEqual(match_action_family(
            "org.elasticsearch.rest.action.admin.cluster.RestNodesUsageAction"),
            "其他")

    def test_aggregate(self):
        payload = {
            "nodes": {
                "n1": {"rest_actions": {
                    "org.elasticsearch.rest.action.document.RestBulkAction": 100,
                    "org.elasticsearch.rest.action.search.RestSearchAction": 30,
                }, "since": "2026-09-21T00:00:00Z"},
                "n2": {"rest_actions": {
                    "org.elasticsearch.rest.action.document.RestBulkAction": 50,
                }},
            }
        }
        merged, node_count, sincers = aggregate_rest_actions(payload)
        self.assertEqual(node_count, 2)
        self.assertEqual(merged["org.elasticsearch.rest.action.document.RestBulkAction"], 150)
        self.assertEqual(len(sincers), 1)


class TestInferScenarioLabels(unittest.TestCase):
    """测试场景推断"""

    def test_write_heavy(self):
        labels = infer_scenario_labels(
            qps_total=10.0, ips_total=100.0, bulk_count=1000, search_count=100,
            get_count=50, ts_store_share=0.8, ts_index_share=0.7,
            active_write_count=30, total_open_count=100, sample_interval=60)
        text = "\n".join(labels)
        self.assertIn("数据接入", text)
        self.assertIn("时间序列", text)
        self.assertIn("Bulk", text)

    def test_read_heavy_without_sampling(self):
        labels = infer_scenario_labels(
            qps_total=0.0, ips_total=0.0, bulk_count=0, search_count=500,
            get_count=200, ts_store_share=0.1, ts_index_share=0.2,
            active_write_count=0, total_open_count=50, sample_interval=0)
        text = "\n".join(labels)
        self.assertIn("未启用速率采样", text)


class TestUsageProfiler(unittest.TestCase):
    """测试 UsageProfiler 分析与输出"""

    def _make_profiler(self):
        client = MagicMock()
        profiler = UsageProfiler(client, "test-cluster-id",
                                 display_name="test-cluster",
                                 include_system_indices=False,
                                 top_n=5,
                                 sample_interval=60)
        return profiler

    def test_analyze_nodes(self):
        profiler = self._make_profiler()
        profiler.report["nodes_raw"] = [
            {"name": "n1", "ip": "1.1.1.1", "heap.percent": "40", "cpu": "10",
             "disk.used_percent": "50.2", "disk.avail": "1tb", "master": "*",
             "node.role": "dim"},
            {"name": "n2", "ip": "1.1.1.2", "heap.percent": "60", "cpu": "20",
             "disk.used_percent": "91.5", "disk.avail": "100gb", "master": "-",
             "node.role": "di"},
        ]
        analysis = profiler.analyze_nodes()
        self.assertEqual(analysis["total_nodes"], 2)
        self.assertEqual(analysis["role_counts"]["data"], 2)
        self.assertEqual(analysis["role_counts"]["master_eligible"], 1)
        self.assertEqual(analysis["current_master"], "n1")
        self.assertEqual(analysis["heap_percent"]["max"], 60)
        self.assertEqual(len(analysis["disk_hot_nodes"]), 1)
        self.assertEqual(analysis["disk_hot_nodes"][0]["name"], "n2")

    def test_analyze_shards(self):
        profiler = self._make_profiler()
        profiler.report["shards_raw"] = [
            {"index": "big", "shard": "0", "prirep": "p", "state": "STARTED",
             "store": "60gb", "docs": "1000", "node": "n1"},
            {"index": "big", "shard": "0", "prirep": "r", "state": "STARTED",
             "store": "60gb", "docs": "1000", "node": "n2"},
            {"index": "tiny", "shard": "0", "prirep": "p", "state": "STARTED",
             "store": "50mb", "docs": "10", "node": "n1"},
            {"index": "lost", "shard": "1", "prirep": "p", "state": "UNASSIGNED",
             "store": None, "docs": None, "node": None},
        ]
        analysis = profiler.analyze_shards()
        self.assertEqual(analysis["total_shards"], 4)
        self.assertEqual(analysis["state_counts"]["UNASSIGNED"], 1)
        self.assertEqual(analysis["oversized_ge50gb_count"], 2)
        self.assertEqual(analysis["tiny_shards_lt100mb"], 1)
        self.assertEqual(analysis["largest_shards"][0]["index"], "big")
        self.assertEqual(analysis["unassigned_sample"][0]["index"], "lost")

    def test_build_index_records_and_groups(self):
        profiler = self._make_profiler()
        profiler.report["indices_raw"] = [
            {"index": "log-2026.09.20", "health": "green", "status": "open",
             "pri": "3", "rep": "1", "docs.count": "100",
             "docs.deleted": "0", "store.size": "3gb", "pri.store.size": "1.5gb",
             "creation.date": "1758300000000"},
            {"index": "log-2026.09.21", "health": "green", "status": "open",
             "pri": "3", "rep": "1", "docs.count": "200",
             "docs.deleted": "0", "store.size": "3gb", "pri.store.size": "1.5gb",
             "creation.date": "1758386400000"},
            {"index": ".infini_metrics", "health": "green", "status": "open",
             "pri": "1", "rep": "1", "docs.count": "10", "docs.deleted": "0",
             "store.size": "10mb", "pri.store.size": "5mb"},
            {"index": "archive-old", "health": "", "status": "close",
             "pri": "1", "rep": "1", "docs.count": "0", "docs.deleted": "0",
             "store.size": "0b", "pri.store.size": "0b"},
        ]
        profiler.report["aliases_map"] = {"log-2026.09.21": ["log-alias"]}
        records = profiler.build_index_records()
        # 系统索引被排除
        self.assertEqual(len(records), 3)
        self.assertEqual(profiler.report["indices_excluded_system"], 1)

        groups = profiler.build_groups()
        by_group = {g["group"]: g for g in groups}
        self.assertIn("log", by_group)
        self.assertEqual(by_group["log"]["index_count"], 2)
        self.assertTrue(by_group["log"]["date_pattern"])
        self.assertFalse(by_group["archive-old"]["date_pattern"])
        self.assertEqual(by_group["archive-old"]["closed_count"], 1)

    def test_compute_activity_no_sampling(self):
        profiler = self._make_profiler()
        profiler.sample_interval = 0
        profiler.report["index_records"] = [
            {"index": "a", "status": "open"},
            {"index": "b", "status": "close"},
        ]
        profiler.compute_activity()
        activity = profiler.report["activity"]
        self.assertFalse(activity["sampled"])
        self.assertEqual(profiler.report["index_records"][1]["activity"], "closed")

    def test_compute_activity_with_rates(self):
        profiler = self._make_profiler()
        profiler._stats_t0 = {
            "a": {"query_total": 0, "index_total": 0},
            "b": {"query_total": 0, "index_total": 0},
        }
        profiler._stats_t1 = {
            "a": {"query_total": 600, "index_total": 0},
            "b": {"query_total": 0, "index_total": 0},
        }
        profiler._t0_wall = 0.0
        profiler._t1_wall = 60.0
        profiler.report["index_records"] = [
            {"index": "a", "status": "open", "qps": None, "ips": None,
             "activity": "", "query_total": 0, "index_total": 0},
            {"index": "b", "status": "open", "qps": None, "ips": None,
             "activity": "", "query_total": 0, "index_total": 0},
        ]
        profiler.compute_activity()
        records = {r["index"]: r for r in profiler.report["index_records"]}
        self.assertEqual(records["a"]["activity"], "查询为主")
        self.assertAlmostEqual(records["a"]["qps"], 10.0)
        self.assertEqual(records["b"]["activity"], "闲置")
        self.assertEqual(profiler.report["activity"]["qps_total"], 10.0)

    def test_build_risks(self):
        profiler = self._make_profiler()
        profiler.report["nodes_analysis"] = {
            "disk_hot_nodes": [
                {"name": "n1", "disk_used_percent": 95.0, "disk_avail": "10gb"},
            ],
        }
        profiler.report["shards_analysis"] = {
            "state_counts": {"UNASSIGNED": 3},
            "oversized_ge50gb_count": 1,
            "tiny_share": 0.2,
            "shard_count": 200,
        }
        profiler.report["governance"] = {
            "blocked_indices": [{"index": "log-2026.09.01",
                                 "blocks": ["read_only_allow_delete"]}],
            "snapshot_repos": {},
        }
        profiler.report["overview"] = {"disk_watermarks": {}}
        profiler.report["governance_pending_tasks"] = []
        risks = profiler.build_risks()
        text = "\n".join(risks)
        self.assertIn("磁盘高危", text)
        self.assertIn("只读阻断", text)
        self.assertIn("未分配分片", text)
        self.assertIn("超大分片", text)
        self.assertIn("快照仓库", text)

    def test_generate_markdown_and_csv(self):
        profiler = self._make_profiler()
        profiler.report["cluster"] = {
            "cluster_name": "test", "version": "7.10.2", "cluster_uuid": "uuid"}
        profiler.report["overview"] = {
            "status": "green", "number_of_nodes": 3, "number_of_data_nodes": 3,
            "active_primary_shards": 10, "active_shards": 20,
            "unassigned_shards": 0, "relocating_shards": 0,
            "initializing_shards": 0, "pending_tasks_count": 0,
            "disk_watermarks": {
                "cluster.routing.allocation.disk.watermark.flood_stage": "95%"}}
        profiler.report["nodes_analysis"] = {
            "total_nodes": 3, "role_counts": {"data": 3},
            "role_raw_distribution": {}, "current_master": "n1",
            "heap_percent": {"min": 30, "avg": 40, "max": 50},
            "cpu_percent": {"min": 5, "avg": 10, "max": 20},
            "disk_used_percent": {"min": 40, "avg": 50, "max": 60},
            "disk_hot_nodes": []}
        profiler.report["shards_analysis"] = {
            "total_shards": 20, "state_counts": {"STARTED": 20},
            "size_buckets": {"1-10GB": 20}, "shard_count": 20,
            "tiny_shards_lt100mb": 0, "tiny_share": 0.0,
            "oversized_ge50gb_count": 0, "largest_shards": [],
            "shards_per_node_top": [], "pri_shards_per_index_top": [],
            "unassigned_sample": []}
        profiler.report["index_records"] = [{
            "index": "app", "group": "app", "date_pattern": False,
            "date_token": "", "health": "green", "status": "open",
            "pri": 1, "rep": 1, "docs": 100, "deleted_docs": 0,
            "store_bytes": 1024 ** 3, "pri_store_bytes": 512 * 1024 ** 2,
            "segments": 1, "query_total": 10, "index_total": 5,
            "age_days": 3.0, "aliases": [], "qps": 0.1, "ips": 0.05,
            "activity": "低频",
        }]
        profiler.report["indices_excluded_system"] = 0
        profiler.report["groups"] = [{
            "group": "app", "date_pattern": False, "index_count": 1,
            "closed_count": 0, "docs": 100, "store_bytes": 1024 ** 3,
            "pri_store_bytes": 512 * 1024 ** 2, "write_active_count": 0}]
        profiler.report["timeseries_share"] = {"store_share": 0.0, "index_share": 0.0}
        profiler.report["activity"] = {
            "sampled": True, "elapsed_seconds": 60, "qps_total": 0.1,
            "ips_total": 0.05, "open_indices": 1, "active_read_indices": 1,
            "active_write_indices": 0, "classification_counts": {"低频": 1},
            "top_read": [], "top_write": []}
        profiler.report["api_usage"] = {"available": False}
        profiler.report["governance"] = {
            "templates": [], "ilm_available": True, "ilm_policies": {},
            "snapshot_repos": {}, "slowlog_enabled_indices": [],
            "blocked_indices": []}
        profiler.report["governance_pending_tasks"] = []
        profiler.report["risks"] = ["未配置快照仓库（_snapshot 为空），数据备份能力存疑"]
        profiler.report["scenario"] = ["读写特征：未启用速率采样"]

        markdown = profiler.generate_markdown()
        for section in ["集群概览", "节点画像", "分片画像", "索引与业务域画像",
                        "读写热度", "API 使用画像", "治理与配置", "风险与建议",
                        "使用场景推断"]:
            self.assertIn(section, markdown)
        self.assertIn("test", markdown)

        with tempfile.TemporaryDirectory() as tmp:
            csv_file = str(Path(tmp) / "indices.csv")
            profiler.generate_csv(csv_file)
            content = Path(csv_file).read_text(encoding="utf-8-sig")
            self.assertIn("索引名", content)
            self.assertIn("app", content)

    def test_collect_overview_with_mock_client(self):
        profiler = self._make_profiler()

        def fake_proxy(cluster_id, method, path, body=None):
            if path == "/":
                return {"cluster_name": "test", "cluster_uuid": "u1",
                        "version": {"number": "7.10.2"}}
            if path == "/_cluster/health":
                return {"status": "green", "number_of_nodes": 3,
                        "number_of_data_nodes": 3, "active_primary_shards": 5,
                        "active_shards": 10, "unassigned_shards": 0}
            if "cluster/settings" in path:
                return {
                    "persistent": {},
                    "transient": {},
                    "defaults": {
                        "cluster.routing.allocation.disk.watermark.flood_stage": "95%",
                    },
                }
            if path == "/_cluster/pending_tasks":
                return {"tasks": [{"source": "create-index",
                                   "time_in_queue_millis": 100}]}
            return {}

        profiler.client.proxy_request = MagicMock(side_effect=fake_proxy)
        profiler.collect_overview()
        overview = profiler.report["overview"]
        self.assertEqual(overview["status"], "green")
        self.assertEqual(overview["disk_watermarks"][
            "cluster.routing.allocation.disk.watermark.flood_stage"], "95%")
        self.assertEqual(overview["pending_tasks_count"], 1)
        self.assertEqual(profiler.report["cluster"]["version"], "7.10.2")


if __name__ == "__main__":
    unittest.main()
