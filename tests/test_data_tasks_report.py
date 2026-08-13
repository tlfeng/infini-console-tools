#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 data-tasks 数据工具任务报表模块
"""

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "data-tasks"))

from data_tasks_report import (
    TASK_KINDS,
    apply_child_task_times_fallback,
    build_task_detail,
    compute_task_times,
    epoch_ms_to_iso,
    format_bytes,
    format_duration_ms,
    parse_iso_to_ms,
    resolve_child_task_times,
    safe_int,
    _tz_offset_minutes_from_iso,
)

# 用户例子 c86-arm-tier_2g 的时间
# 开始: 2026-08-06 16:38:28 +08:00 = 1786005508000 ms
# 结束: 2026-08-06 17:25:53 +08:00 = 1786008353000 ms
START_MS = 1786005508000
COMPLETED_MS = 1786008353000


def make_hit(start_ms, config=None, created="2026-08-06T16:30:00+08:00", **source_extra):
    """构造一个 search hit（主任务 _source）"""
    cfg = config or {
        "name": "测试任务",
        "cluster": {"source": {"id": "src1", "name": "源集群"},
                    "target": {"id": "tgt1", "name": "目标集群"}},
        "indices": [{"source": {"name": "test_index", "doc_type": "_doc", "docs": 100},
                     "target": {"name": "test_index_copy", "docs": 0}}],
    }
    source = {
        "id": "task_id",
        "status": "complete",
        "task_lifecycle": "complete",
        "created": created,
        "updated": "2026-08-06T17:25:53+08:00",
        "start_time_in_millis": start_ms,
        "completed_time": "2026-08-06T17:25:53+08:00",
        "cancellable": True,
        "runnable": False,
        "config_string": json.dumps(cfg),
        "metadata": {"labels": {"name": "测试任务"}},
    }
    source.update(source_extra)
    return {"_id": "task_id", "_source": source}


class FakeClient:
    """按路径返回 per-index 响应的 mock client"""

    def __init__(self, responses=None):
        self.responses = responses or []  # [{"path": "...", "data": {...}}]
        self.requests = []

    def _make_request(self, path, method="GET"):
        self.requests.append((method, path))
        for r in self.responses:
            if r["path"] in path:
                return r["data"]
        return {}


class TestFormatDurationMs(unittest.TestCase):
    """测试 format_duration_ms 各种格式"""

    def test_none_or_zero(self):
        self.assertEqual(format_duration_ms(None), "")
        self.assertEqual(format_duration_ms(0), "")
        self.assertEqual(format_duration_ms(-5), "")

    def test_under_1s(self):
        self.assertEqual(format_duration_ms(500), "500 ms")
        self.assertEqual(format_duration_ms(999), "999 ms")

    def test_under_1min(self):
        # <10s：2 位小数
        self.assertEqual(format_duration_ms(5432), "5.43 s")
        # ≥10s：1 位小数
        self.assertEqual(format_duration_ms(12345), "12.3 s")

    def test_hhmmss(self):
        self.assertEqual(format_duration_ms(90000), "00:01:30")
        # 用户例子：47 分 24 秒
        self.assertEqual(format_duration_ms(2844000), "00:47:24")

    def test_over_24h(self):
        self.assertEqual(format_duration_ms(90000000), "1d 01:00:00")
        self.assertEqual(format_duration_ms(2844000 + 86400000), "1d 00:47:24")


class TestTzOffset(unittest.TestCase):
    """测试时区偏移提取"""

    def test_positive_offset(self):
        self.assertEqual(_tz_offset_minutes_from_iso("2026-08-06T16:38:28+08:00"), 480)

    def test_negative_offset(self):
        self.assertEqual(_tz_offset_minutes_from_iso("2026-08-06T16:38:28-05:30"), -330)

    def test_no_offset(self):
        # Z 等价于 +00:00（UTC 偏移 0）
        self.assertEqual(_tz_offset_minutes_from_iso("2026-08-06T16:38:28Z"), 0)
        # 无偏移的 naive 串 → None
        self.assertIsNone(_tz_offset_minutes_from_iso("2026-08-06T16:38:28"))
        self.assertIsNone(_tz_offset_minutes_from_iso(""))

    def test_malformed(self):
        self.assertIsNone(_tz_offset_minutes_from_iso("+abc"))


class TestEpochMsToIso(unittest.TestCase):
    """测试 epoch ms → ISO"""

    def test_with_reference_tz(self):
        result = epoch_ms_to_iso(START_MS, "2026-08-06T16:30:00+08:00")
        self.assertEqual(result, "2026-08-06T16:38:28+08:00")

    def test_negative_offset(self):
        result = epoch_ms_to_iso(START_MS, "2026-08-06T03:30:00-05:00")
        self.assertIn("-05:00", result)

    def test_zero(self):
        self.assertEqual(epoch_ms_to_iso(0, "2026-08-06T16:30:00+08:00"), "")
        self.assertEqual(epoch_ms_to_iso(-1, "2026-08-06T16:30:00+08:00"), "")

    def test_no_reference_uses_local_tz(self):
        result = epoch_ms_to_iso(START_MS)
        self.assertTrue(result.startswith("2026-08-06"))


class TestComputeTaskTimes(unittest.TestCase):
    """测试耗时/开始时间计算"""

    def test_complete(self):
        iso, dur = compute_task_times(START_MS, COMPLETED_MS, "complete", "2026-08-06T16:30:00+08:00")
        self.assertEqual(iso, "2026-08-06T16:38:28+08:00")
        self.assertEqual(dur, COMPLETED_MS - START_MS)  # 47 分 25 秒

    def test_running(self):
        # 运行中：耗时 = now - start，应大于 0
        iso, dur = compute_task_times(START_MS, None, "running", "2026-08-06T16:30:00+08:00")
        self.assertEqual(iso, "2026-08-06T16:38:28+08:00")
        self.assertIsNotNone(dur)
        self.assertGreater(dur, 0)

    def test_no_start(self):
        # 无开始时间 → start_time 空、duration None（不再用 created 兜底）
        iso, dur = compute_task_times(0, COMPLETED_MS, "complete", "2026-08-06T16:30:00+08:00")
        self.assertEqual(iso, "")
        self.assertIsNone(dur)


class TestBuildTaskDetail(unittest.TestCase):
    """测试 build_task_detail 时间/耗时段"""

    def test_with_start_ms(self):
        r = build_task_detail(make_hit(START_MS), None, "migration", "数据迁移")
        self.assertEqual(r["start_time_in_millis"], START_MS)
        self.assertEqual(r["start_time"], "2026-08-06T16:38:28+08:00")
        self.assertEqual(r["completed_time"], "2026-08-06T17:25:53+08:00")
        self.assertEqual(r["duration_ms"], COMPLETED_MS - START_MS)
        self.assertEqual(r["duration"], "00:47:25")

    def test_zero_start_no_fallback(self):
        # start_time_in_millis == 0 且无子任务兜底 → start_time / duration 留空
        r = build_task_detail(make_hit(0), None, "migration", "数据迁移")
        self.assertEqual(r["start_time_in_millis"], 0)
        self.assertEqual(r["start_time"], "")
        self.assertIsNone(r["duration_ms"])
        self.assertEqual(r["duration"], "")

    def test_no_start_and_not_started(self):
        r = build_task_detail(make_hit(0, status="init"), None, "migration", "数据迁移")
        self.assertEqual(r["start_time"], "")
        self.assertIsNone(r["duration_ms"])


class TestResolveChildTaskTimes(unittest.TestCase):
    """测试子任务时间聚合（min(start) / max(completed)）"""

    RESPONSES = [
        {"path": "/migration/data/t/info/idx1:_doc",
         "data": {"start_time": START_MS, "completed_time": COMPLETED_MS}},
        {"path": "/migration/data/t/info/idx2:_doc",
         "data": {"start_time": START_MS + 12000, "completed_time": START_MS + 20000}},
    ]

    INDICES = [
        {"source_index": "idx1", "doc_type": "_doc"},
        {"source_index": "idx2", "doc_type": "_doc"},
    ]

    def test_aggregate_min_max(self):
        client = FakeClient(self.RESPONSES)
        start, completed = resolve_child_task_times(
            client, "/migration/data/{id}/info/{index}", "t", self.INDICES
        )
        self.assertEqual(start, START_MS)  # min
        self.assertEqual(completed, COMPLETED_MS)  # max
        # 应构造 unique_index_name = "{source}:{doc_type}"
        paths = [p[1] for p in client.requests]
        self.assertTrue(any("idx1:_doc" in p for p in paths))
        self.assertTrue(any("idx2:_doc" in p for p in paths))

    def test_empty_indices(self):
        client = FakeClient(self.RESPONSES)
        start, completed = resolve_child_task_times(client, "/migration/data/{id}/info/{index}", "t", [])
        self.assertIsNone(start)
        self.assertIsNone(completed)

    def test_no_start_data(self):
        client = FakeClient([
            {"path": "/migration/data/t/info/idx1:_doc", "data": {"start_time": 0, "completed_time": 0}},
        ])
        start, completed = resolve_child_task_times(
            client, "/migration/data/{id}/info/{index}", "t", self.INDICES[:1]
        )
        self.assertIsNone(start)
        self.assertIsNone(completed)

    def test_index_without_source_skipped(self):
        client = FakeClient(self.RESPONSES)
        start, completed = resolve_child_task_times(
            client, "/migration/data/{id}/info/{index}", "t",
            [{"source_index": "", "doc_type": "_doc"}]
        )
        self.assertIsNone(start)
        self.assertEqual(client.requests, [])  # 没发请求


class TestApplyChildTaskTimesFallback(unittest.TestCase):
    """测试 apply_child_task_times_fallback 就地回填"""

    def test_zero_start_with_child_aggregation(self):
        client = FakeClient([
            {"path": "/migration/data/task_id/info/test_index:_doc",
             "data": {"start_time": START_MS, "completed_time": COMPLETED_MS}},
        ])
        td = build_task_detail(make_hit(0), None, "migration", "数据迁移")
        apply_child_task_times_fallback(client, "/migration/data/{id}/info/{index}", "task_id", td)
        self.assertEqual(td["start_time_in_millis"], START_MS)
        self.assertEqual(td["start_time"], "2026-08-06T16:38:28+08:00")
        self.assertEqual(td["completed_time"], "2026-08-06T17:25:53+08:00")
        self.assertEqual(td["duration_ms"], COMPLETED_MS - START_MS)
        self.assertEqual(td["duration"], "00:47:25")

    def test_no_aggregation_when_start_not_zero(self):
        # start 已有值 → 不额外调用 per-index
        client = FakeClient([])
        td = build_task_detail(make_hit(START_MS), None, "migration", "数据迁移")
        apply_child_task_times_fallback(client, "/migration/data/{id}/info/{index}", "task_id", td)
        self.assertEqual(client.requests, [])
        self.assertEqual(td["start_time"], "2026-08-06T16:38:28+08:00")

    def test_no_aggregation_when_no_per_index_tpl(self):
        client = FakeClient([])
        td = build_task_detail(make_hit(0), None, "migration", "数据迁移")
        apply_child_task_times_fallback(client, None, "task_id", td)
        self.assertEqual(client.requests, [])
        self.assertEqual(td["duration"], "")

    def test_running_child_aggregation(self):
        # 子任务仍在跑：completed 为 0 → duration = now - start（非空）
        client = FakeClient([
            {"path": "/migration/data/task_id/info/test_index:_doc",
             "data": {"start_time": START_MS, "completed_time": 0}},
        ])
        td = build_task_detail(make_hit(0, status="running"), None, "migration", "数据迁移")
        apply_child_task_times_fallback(client, "/migration/data/{id}/info/{index}", "task_id", td)
        self.assertEqual(td["start_time_in_millis"], START_MS)
        self.assertIsNotNone(td["duration_ms"])
        self.assertGreater(td["duration_ms"], 0)


class TestTaskKinds(unittest.TestCase):
    """测试 TASK_KINDS 结构（含 per-index 端点模板）"""

    def test_kind_structure(self):
        self.assertEqual(len(TASK_KINDS), 2)
        for (list_ep, info_tpl, label, kind, per_index_tpl) in TASK_KINDS:
            self.assertTrue(list_ep.endswith("/_search"))
            self.assertIn("{id}", info_tpl)
            self.assertIn("{index}", per_index_tpl)
            self.assertIn(kind, per_index_tpl)

    def test_migration_per_index_template(self):
        _, _, _, kind, per_index_tpl = TASK_KINDS[0]
        self.assertEqual(kind, "migration")
        self.assertEqual(per_index_tpl, "/migration/data/{id}/info/{index}")


class TestParseIsoAndBytes(unittest.TestCase):
    """测试既有工具函数（回归）"""

    def test_parse_iso(self):
        from datetime import datetime, timezone

        self.assertEqual(parse_iso_to_ms("2026-08-06T16:38:28+08:00"), START_MS)
        # 带 Z 的写法按 UTC 解析
        utc_ms = int(datetime(2026, 8, 6, 16, 38, 28, tzinfo=timezone.utc).timestamp() * 1000)
        self.assertEqual(parse_iso_to_ms("2026-08-06T16:38:28Z"), utc_ms)
        self.assertIsNone(parse_iso_to_ms(None))
        self.assertIsNone(parse_iso_to_ms("not a date"))

    def test_safe_int(self):
        self.assertEqual(safe_int("123"), 123)
        self.assertIsNone(safe_int("abc"))
        self.assertIsNone(safe_int(None))

    def test_format_bytes(self):
        self.assertEqual(format_bytes(None), "")
        self.assertEqual(format_bytes(0), "0 B")
        self.assertEqual(format_bytes(2048), "2.00 KB")


if __name__ == "__main__":
    unittest.main()