#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 Console 指标写入延迟排查工具（console_queue_delay）
"""

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "console-queue-delay"))

from console_queue_delay import (
    _parse_offset,
    _age_seconds,
    _fmt_delay,
    classify_absent_type,
    summarize_queues,
    verdict,
    fetch_collection_states,
    diagnose_absent_clusters,
    render_report,
    EXPECTED_TYPES,
    DELAY_WARN_SEC,
)
from common.console_client import ConsoleAPIError


class TestParseOffset(unittest.TestCase):
    """测试队列 offset 解析"""

    def test_valid(self):
        self.assertEqual(_parse_offset("4,62257043"), (4, 62257043))
        self.assertEqual(_parse_offset("0,0"), (0, 0))

    def test_invalid(self):
        self.assertIsNone(_parse_offset("not-an-offset"))
        self.assertIsNone(_parse_offset("4"))
        self.assertIsNone(_parse_offset(12345))
        self.assertIsNone(_parse_offset(None))
        self.assertIsNone(_parse_offset("a,b"))


class TestSummarizeQueues(unittest.TestCase):
    """测试 /queue/stats 解析与积压计算"""

    def test_lag_same_segment(self):
        raw = {"queue": {"disk": {
            "q1": {
                "metadata": {"name": "metrics"},
                "offset": "4,100",
                "earliest_consumer_offset": "4,40",
                "depth": 2,
                "messages": 1000,
                "consumers": [{"last_active": "2026-09-28T17:07:15+08:00"}],
                "storage": {"local_usage": "1.2GB"},
            },
        }}}
        queues = summarize_queues(raw)
        self.assertEqual(len(queues), 1)
        q = queues[0]
        self.assertEqual(q["name"], "metrics")
        self.assertEqual(q["lag"], 60)
        self.assertFalse(q["multi_segment"])
        self.assertEqual(q["consumers"], 1)

    def test_cross_segment_backlog(self):
        raw = {"queue": {"disk": {
            "q1": {
                "metadata": {"name": "metrics"},
                "offset": "5,10",
                "earliest_consumer_offset": "4,900000",
                "depth": 3,
                "consumers": [],
            },
        }}}
        q = summarize_queues(raw)[0]
        self.assertIsNone(q["lag"])
        self.assertTrue(q["multi_segment"])

    def test_metric_queues_sorted_first(self):
        raw = {"queue": {"disk": {
            "a": {"metadata": {"name": "alerting"}, "offset": "1,1",
                  "earliest_consumer_offset": "1,1", "depth": 0, "consumers": []},
            "m": {"metadata": {"name": "metrics"}, "offset": "1,1",
                  "earliest_consumer_offset": "1,1", "depth": 0, "consumers": []},
        }}}
        queues = summarize_queues(raw)
        self.assertEqual([q["name"] for q in queues], ["metrics", "alerting"])


class TestAgeSeconds(unittest.TestCase):
    def test_with_timezone(self):
        ts = datetime.now(timezone.utc).astimezone().isoformat()
        age = _age_seconds(ts)
        self.assertIsNotNone(age)
        self.assertLess(abs(age), 60)

    def test_stale(self):
        old = (datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S")
        age = _age_seconds(old)
        self.assertIsNotNone(age)
        self.assertGreater(age, 7000)

    def test_invalid(self):
        self.assertIsNone(_age_seconds("garbage"))
        self.assertIsNone(_age_seconds(""))


class TestFmtDelay(unittest.TestCase):
    def test_formats(self):
        self.assertEqual(_fmt_delay(None), "-")
        self.assertEqual(_fmt_delay(30), "30s")
        self.assertEqual(_fmt_delay(125), "2m05s")
        self.assertEqual(_fmt_delay(7265), "2h01m")


class TestVerdict(unittest.TestCase):
    """测试综合判定"""

    def _fresh(self, delay):
        return {"overall": delay, "error": None}

    def test_healthy(self):
        queues = [{"name": "metrics", "lag": 0, "depth": 0,
                   "multi_segment": False, "last_active": ""}]
        conclusion, reasons = verdict(queues, self._fresh(30))
        self.assertIn("健康", conclusion)
        self.assertEqual(reasons, [])

    def test_backlog(self):
        queues = [{"name": "metrics", "lag": 50000, "depth": 10,
                   "multi_segment": False, "last_active": ""}]
        conclusion, _ = verdict(queues, self._fresh(7200))
        self.assertIn("写入链路积压", conclusion)

    def test_collection_stopped(self):
        queues = [{"name": "metrics", "lag": 0, "depth": 0,
                   "multi_segment": False, "last_active": ""}]
        conclusion, _ = verdict(queues, self._fresh(7200))
        self.assertIn("采集任务停了", conclusion)

    def test_consumer_stalled(self):
        stale_ts = (datetime.now() - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S")
        # lag 小于积压阈值且 depth 不超限，才能走到"消费者卡死"分支
        queues = [{"name": "metrics", "lag": 5, "depth": 1,
                   "multi_segment": False, "last_active": stale_ts}]
        conclusion, _ = verdict(queues, self._fresh(1200))
        self.assertIn("卡死", conclusion)

    def test_freshness_missing(self):
        conclusion, _ = verdict([], {"overall": None, "error": "x"})
        self.assertIn("无法判定", conclusion)


class TestClassifyAbsentType(unittest.TestCase):
    """测试缺席二次诊断归类"""

    def setUp(self):
        self.now_ms = 1_800_000_000_000

    def test_no_data(self):
        result = classify_absent_type(None, self.now_ms)
        self.assertEqual(result["classification"], "no_data")

    def test_stale(self):
        # 1 小时前的时间戳，超过 15 分钟窗口
        result = classify_absent_type(self.now_ms - 3600 * 1000, self.now_ms)
        self.assertEqual(result["classification"], "stale")
        self.assertEqual(result["delay_sec"], 3600)

    def test_fresh_but_absent(self):
        # 2 分钟前，仍在 15 分钟窗口内
        result = classify_absent_type(self.now_ms - 120 * 1000, self.now_ms)
        self.assertEqual(result["classification"], "fresh_but_absent")


class TestFetchCollectionStates(unittest.TestCase):
    """测试 _collection_stats 解析与 absent 合成"""

    def _cluster(self, **kw):
        return {"id": "c1", "name": "prod-es", "mode": "agentless",
                "monitored": True, "enabled": True, **kw}

    def test_partial_types_missing_computed(self):
        client = MagicMock()
        client.request.return_value = {
            "cluster_id": "c1",
            "metric_collection_mode": "agentless",
            "cluster_health": {"last_active_at": 1, "status": "ok"},
        }
        states = fetch_collection_states(client, [self._cluster()])
        state = states[0]
        self.assertEqual(state["mode"], "agentless")
        self.assertEqual(state["stats"]["cluster_health"]["status"], "ok")
        # 响应里缺席的类型被补成 absent
        self.assertEqual(state["stats"]["index_stats"]["status"], "absent")
        # missing_types 驱动二次诊断，应包含所有缺席类型
        self.assertEqual(sorted(state["missing_types"]),
                         sorted(["cluster_stats", "node_stats", "index_stats", "index_health"]))

    def test_all_absent_synthesized(self):
        client = MagicMock()
        # Console 在 15 分钟窗口无数据时只返回 cluster_id/mode，无指标 key
        client.request.return_value = {
            "cluster_id": "c1", "metric_collection_mode": "agentless",
        }
        states = fetch_collection_states(client, [self._cluster()])
        state = states[0]
        self.assertEqual(len(state["stats"]), len(EXPECTED_TYPES["agentless"]))
        for t in EXPECTED_TYPES["agentless"]:
            self.assertEqual(state["stats"][t]["status"], "absent")
        self.assertEqual(sorted(state["missing_types"]),
                         sorted(EXPECTED_TYPES["agentless"]))

    def test_unmonitored_skipped(self):
        client = MagicMock()
        states = fetch_collection_states(client, [self._cluster(monitored=False)])
        self.assertIsNone(states[0]["stats"])
        self.assertIn("跳过", states[0]["note"])
        client.request.assert_not_called()

    def test_api_error_noted(self):
        client = MagicMock()
        client.request.side_effect = ConsoleAPIError("HTTP 500: boom")
        states = fetch_collection_states(client, [self._cluster()])
        self.assertIsNone(states[0]["stats"])
        self.assertIn("500", states[0]["note"])


class TestDiagnoseAbsentClusters(unittest.TestCase):
    """测试缺席二次诊断的查询与归类"""

    def test_diagnosis_mapping(self):
        now_ms = 1_800_000_000_000
        client = MagicMock()
        # 该集群有 cluster_stats（1 小时前）和 node_stats（2 分钟前），
        # index_stats / cluster_health / index_health 完全没有文档
        client.proxy_request.return_value = {
            "hits": {"total": {"value": 42}},
            "aggregations": {"by_name": {"buckets": [
                {"key": "cluster_stats", "max_ts": {"value": now_ms - 3600 * 1000}},
                {"key": "node_stats", "max_ts": {"value": now_ms - 120 * 1000}},
            ]}},
        }
        cluster_states = [{
            "id": "c1", "name": "prod-es", "mode": "agentless",
            "monitored": True, "enabled": True,
            "stats": {t: {"status": "absent", "delay_sec": None}
                      for t in EXPECTED_TYPES["agentless"]},
            "missing_types": list(EXPECTED_TYPES["agentless"]),
        }]
        with patch("console_queue_delay.time.time", return_value=now_ms / 1000):
            diag = diagnose_absent_clusters(client, "system", cluster_states)

        self.assertIn("c1", diag)
        self.assertEqual(diag["c1"]["total_docs"], 42)
        types = diag["c1"]["types"]
        self.assertEqual(types["index_stats"]["classification"], "no_data")
        self.assertEqual(types["cluster_health"]["classification"], "no_data")
        self.assertEqual(types["index_health"]["classification"], "no_data")
        self.assertEqual(types["cluster_stats"]["classification"], "stale")
        self.assertEqual(types["node_stats"]["classification"], "fresh_but_absent")

    def test_no_missing_types_no_query(self):
        client = MagicMock()
        diag = diagnose_absent_clusters(client, "system", [
            {"id": "c1", "name": "x", "missing_types": []}])
        self.assertEqual(diag, {})
        client.proxy_request.assert_not_called()


class TestRenderReport(unittest.TestCase):
    """测试报告渲染包含关键内容"""

    def test_report_contains_sections(self):
        report = render_report(
            queues=[], cluster_states=[], freshness=None, es_health={},
            conclusion="健康", reasons=[],
            console_url="http://x", absent_diagnosis={
                "c1": {"total_docs": 7, "types": {
                    "index_stats": {"classification": "fresh_but_absent",
                                    "max_timestamp": "2026-10-09 12:00:00",
                                    "delay_sec": 60}}}},
        )
        self.assertIn("Console 指标写入延迟排查报告", report)
        self.assertIn("缺席二次诊断", report)
        self.assertIn("fresh_but_absent", report or "疑似 Console bug")
        self.assertIn("疑似 Console bug", report)


if __name__ == "__main__":
    unittest.main()
