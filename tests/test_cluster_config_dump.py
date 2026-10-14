#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 Cluster Config Dump 模块
"""

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "cluster-config-dump"))

from common.console_client import (
    ConsoleAPIError,
    ConsoleClient,
    DEFAULT_SYSTEM_CLUSTER_ID,
    ProxyResponseError,
)
from cluster_config_dump import (
    API_GROUPS,
    GROUP_KEYS,
    build_plan,
    parse_group_selection,
    slugify,
    print_catalog,
    evaluate_findings,
    ConfigDumper,
    dump_one_cluster,
    run_all_clusters,
)


class TestCatalog(unittest.TestCase):
    """测试 API 目录完整性"""

    def test_group_keys_unique_and_ordered(self):
        self.assertEqual(len(GROUP_KEYS), len(set(GROUP_KEYS)))
        self.assertEqual(GROUP_KEYS,
                         ["cluster", "cat", "index", "license", "diagnostic", "ml"])

    def test_api_names_unique(self):
        names = [a["name"] for g in API_GROUPS for a in g["apis"]]
        self.assertEqual(len(names), len(set(names)),
                         f"API 名称重复: {[n for n in names if names.count(n) > 1]}")

    def test_api_paths_valid(self):
        for g in API_GROUPS:
            for a in g["apis"]:
                self.assertTrue(a["path"].startswith("/"),
                                f"{a['name']} 路径非法: {a['path']}")
                if a["path"].startswith("/_cat"):
                    self.assertIn("format=json", a["path"],
                                  f"cat API 缺少 format=json: {a['path']}")

    def test_expected_coverage(self):
        """官方 Cluster APIs 目录的可执行 GET 必须全部在 cluster 组里"""
        cluster_paths = [a["path"] for a in API_GROUPS[0]["apis"]]
        for expected in ("/_cluster/health", "/_cluster/state", "/_cluster/stats",
                         "/_cluster/settings?include_defaults=true",
                         "/_cluster/pending_tasks", "/_nodes?flat_settings=true",
                         "/_nodes/stats", "/_nodes/usage", "/_nodes/hot_threads",
                         "/_remote/info", "/_tasks",
                         "/_cluster/allocation/explain"):
            self.assertIn(expected, cluster_paths, f"缺少 Cluster API: {expected}")

    def test_print_catalog_runs(self):
        import io
        import contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            print_catalog()
        output = buf.getvalue()
        self.assertIn("76 个 API", output)
        self.assertIn("[1] cluster", output)


class TestParseGroupSelection(unittest.TestCase):

    def test_empty_and_all(self):
        self.assertEqual(parse_group_selection(""), GROUP_KEYS)
        self.assertEqual(parse_group_selection("all"), GROUP_KEYS)
        self.assertEqual(parse_group_selection(" ALL "), GROUP_KEYS)

    def test_by_number(self):
        self.assertEqual(parse_group_selection("1,3"), ["cluster", "index"])

    def test_by_key(self):
        self.assertEqual(parse_group_selection("cat,ml"), ["cat", "ml"])

    def test_mixed_and_dedup(self):
        self.assertEqual(parse_group_selection("2,cat,1,cat"),
                         ["cat", "cluster"])

    def test_invalid_number(self):
        with self.assertRaises(ValueError):
            parse_group_selection("9")

    def test_invalid_key(self):
        with self.assertRaises(ValueError):
            parse_group_selection("nope")

    def test_empty_result(self):
        with self.assertRaises(ValueError):
            parse_group_selection("，，")


class TestBuildPlan(unittest.TestCase):

    def test_subset_plan(self):
        plan = build_plan(["cluster"], include_heavy=False)
        names = [p["name"] for p in plan]
        self.assertIn("cluster_health", names)
        self.assertNotIn("cluster_state", names)  # heavy 被排除
        self.assertEqual([p["seq"] for p in plan],
                         list(range(1, len(plan) + 1)))

    def test_all_plan_with_heavy(self):
        plan = build_plan(GROUP_KEYS, include_heavy=True)
        self.assertEqual(len(plan), 76)
        heavy_names = [p["name"] for p in plan if p["heavy"]]
        self.assertIn("cluster_state", heavy_names)
        self.assertIn("index_stats", heavy_names)

    def test_group_order_follows_catalog(self):
        plan = build_plan(["ml", "cluster"], include_heavy=True)
        groups = [p["group_key"] for p in plan]
        # cluster 在目录中排在 ml 前面，计划也应保持该顺序
        self.assertEqual(groups.index("cluster"), 0)
        self.assertEqual(groups[-1], "ml")


class TestSlugify(unittest.TestCase):

    def test_basic(self):
        self.assertEqual(slugify("cluster_health"), "cluster_health")

    def test_special_chars(self):
        self.assertEqual(slugify("cat health!"), "cat_health")
        self.assertNotEqual(slugify("///"), "")


def _results_with(payload_overrides: dict) -> dict:
    """构造 evaluate_findings 的输入：默认各 API 均为健康数据"""
    base = {
        "cluster_health": {"cluster_name": "es", "status": "green",
                           "number_of_nodes": 3, "number_of_data_nodes": 3,
                           "active_shards": 10, "active_primary_shards": 5,
                           "unassigned_shards": 0,
                           "task_max_waiting_in_queue_millis": 0,
                           "number_of_pending_tasks": 0},
        "cluster_pending_tasks": {"tasks": []},
        "nodes_stats": {"nodes": {}},
        "nodes_info": {"nodes": {}},
        "license": {},
        "ilm_status": {"operation_mode": "RUNNING"},
        "slm_status": {"operation_mode": "RUNNING"},
        "snapshot_repositories": {"repo1": {"type": "fs"}},
    }
    base.update(payload_overrides)
    return {name: {"payload": payload, "status": "ok"}
            for name, payload in base.items()}


class TestEvaluateFindings(unittest.TestCase):

    def test_healthy_cluster_no_warnings(self):
        findings = evaluate_findings(_results_with({}))
        levels = [f["level"] for f in findings]
        self.assertNotIn("critical", levels)
        self.assertNotIn("warn", levels)

    def test_red_status_critical(self):
        results = _results_with({
            "cluster_health": {"status": "red", "unassigned_shards": 5},
        })
        findings = evaluate_findings(results)
        self.assertTrue(any(f["level"] == "critical" and "red" in f["message"]
                            for f in findings))

    def test_yellow_status_warn(self):
        results = _results_with({
            "cluster_health": {"status": "yellow", "unassigned_shards": 3},
        })
        findings = evaluate_findings(results)
        self.assertTrue(any(f["level"] == "warn" and "yellow" in f["message"]
                            for f in findings))

    def test_pending_tasks_backlog(self):
        results = _results_with({
            "cluster_pending_tasks": {"tasks": [
                {"source": "put-mapping", "time_in_queue_millis": 120000},
            ]},
        })
        findings = evaluate_findings(results)
        self.assertTrue(any(f["level"] == "warn" and "积压 1 个" in f["message"]
                            for f in findings))

    def test_node_signals(self):
        results = _results_with({
            "nodes_stats": {"nodes": {
                "n1": {"name": "node-1",
                       "jvm": {"mem": {"heap_used_percent": 92}},
                       "fs": {"total": {"total_in_bytes": 1000,
                                        "available_in_bytes": 30}},
                       "thread_pool": {"write": {"rejected": 7}},
                       "breakers": {"parent": {
                           "limit_size_in_bytes": 100,
                           "estimated_size_in_bytes": 100}}},
            }},
        })
        findings = evaluate_findings(results)
        messages = " | ".join(f["message"] for f in findings)
        self.assertTrue(any(f["level"] == "critical" and "堆使用率峰值 92" in f["message"]
                            for f in findings))
        self.assertTrue(any(f["level"] == "critical" and "磁盘使用 97.0%" in f["message"]
                            for f in findings))
        self.assertTrue(any(f["level"] == "critical" and "circuit breaker" in f["message"]
                            for f in findings))
        self.assertTrue(any(f["level"] == "warn" and "拒绝 7 次" in f["message"]
                            for f in findings))
        self.assertIn("node-1", messages)

    def test_version_mismatch(self):
        results = _results_with({
            "nodes_info": {"nodes": {
                "a": {"version": "7.10.2"},
                "b": {"version": "7.9.0"},
            }},
        })
        findings = evaluate_findings(results)
        self.assertTrue(any(f["level"] == "warn" and "版本不一致" in f["message"]
                            for f in findings))

    def test_license_expiry_soon(self):
        soon = (time.time() + 10 * 86400) * 1000
        results = _results_with({
            "license": {"license": {"status": "active", "type": "platinum",
                                    "expiry_date_in_millis": int(soon)}},
        })
        findings = evaluate_findings(results)
        self.assertTrue(any(f["level"] == "warn" and "到期" in f["message"]
                            for f in findings))

    def test_ilm_stopped(self):
        results = _results_with({
            "ilm_status": {"operation_mode": "STOPPED"},
        })
        findings = evaluate_findings(results)
        self.assertTrue(any(f["level"] == "warn" and "ILM" in f["message"]
                            for f in findings))

    def test_no_snapshot_repo(self):
        results = _results_with({"snapshot_repositories": {}})
        findings = evaluate_findings(results)
        self.assertTrue(any(f["level"] == "warn" and "快照仓库" in f["message"]
                            for f in findings))

    def test_missing_apis_tolerated(self):
        """只采了部分类别时不报错、不产生虚假结论"""
        findings = evaluate_findings({
            "cluster_health": {"payload": {"status": "green"}, "status": "ok"},
        })
        self.assertEqual([f for f in findings if f["level"] != "info"], [])


def _fake_client(responses: dict):
    """构造按 path 返回预设响应的假 ConsoleClient"""
    client = MagicMock()
    client.proxy_request.side_effect = [
        responses[item_path] for item_path in responses
    ]

    def side_effect(cluster_id, method, path):
        if isinstance(responses, Exception):
            raise responses
        result = responses[path]
        if isinstance(result, Exception):
            raise result
        return result

    client.proxy_request.side_effect = side_effect
    return client


class TestConfigDumperRun(unittest.TestCase):
    """端到端：假客户端执行完整采集，校验落盘结构"""

    CLUSTER_ID = "test-cluster"

    def _run_dump(self, tmpdir: Path):
        responses = {
            "/": {"cluster_name": "es-test", "cluster_uuid": "uuid-1",
                  "version": {"number": "7.10.2"}},
            "/_cluster/health": {"status": "green", "unassigned_shards": 0,
                                 "number_of_nodes": 3},
            # ES 报错体（如 allocation explain 全分配场景）
            "/_cluster/allocation/explain": {
                "error": "unable to find any unassigned shards",
                "status": 400},
            "/_cluster/pending_tasks": {"tasks": []},
        }
        client = _fake_client(responses)

        plan = build_plan(["cluster"], include_heavy=False)
        # 只保留上面准备了响应的 4 条，其余替换为 ok 响应
        for p in plan:
            responses.setdefault(p["path"], {"mock": p["name"]})

        dumper = ConfigDumper(client, self.CLUSTER_ID, "测试集群", plan,
                              tmpdir, console_url="http://console:9000")
        counts = dumper.run()
        return dumper, counts

    def test_run_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "run"
            dumper, counts = self._run_dump(run_dir)

            # 计数正确
            self.assertEqual(counts["es_error"], 1)
            self.assertEqual(counts["failed"], 0)
            self.assertEqual(counts["ok"] + counts["es_error"], len(dumper.plan))

            # 目录结构: 01_cluster/NNN_name.json
            self.assertTrue((run_dir / "01_cluster" / "001_root.json").exists())
            self.assertTrue(
                (run_dir / "01_cluster" / "013_cluster_allocation_explain.json")
                .exists())

            # 内容是合法 JSON
            root_data = json.loads(
                (run_dir / "01_cluster" / "001_root.json").read_text())
            self.assertEqual(root_data["cluster_name"], "es-test")

            # ES 报错结果原样落盘（cluster 组不含重接口时共 13 条，
            # allocation/explain 是最后一条）
            alloc = json.loads(
                (run_dir / "01_cluster"
                 / "013_cluster_allocation_explain.json").read_text())
            self.assertEqual(alloc["status"], 400)

    def test_manifest_and_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "run"
            dumper, counts = self._run_dump(run_dir)

            manifest = dumper.build_manifest(["cluster"], True, counts)
            self.assertEqual(manifest["total"], len(dumper.plan))
            self.assertEqual(manifest["cluster"]["id"], self.CLUSTER_ID)
            by_name = {i["name"]: i for i in manifest["items"]}
            self.assertEqual(by_name["root"]["status"], "ok")
            self.assertEqual(by_name["cluster_allocation_explain"]["status"],
                             "es_error")
            self.assertEqual(by_name["cluster_allocation_explain"]["es_error_status"],
                             400)
            # manifest 可序列化
            json.dumps(manifest)

            summary = dumper.generate_summary_md(["cluster"], True, counts)
            self.assertIn("体检摘要", summary)
            self.assertIn("## 1. 集群概览", summary)
            self.assertIn("## 2. 需要关注的信号", summary)
            self.assertIn("## 4. 采集结果统计", summary)
            self.assertIn("es_error", summary.lower())

    def test_request_failure_recorded(self):
        """请求层面抛异常时记为 failed 且不中断后续采集"""
        responses = {
            "/": {"cluster_name": "es", "version": {"number": "7.10.2"}},
            "/_cluster/health": ConsoleAPIError("连接被重置"),
            "/_cluster/stats": {"nodes": {}},
        }
        client = _fake_client(responses)
        plan = [p for p in build_plan(["cluster"], include_heavy=False)
                if p["path"] in responses]
        dumper = ConfigDumper(client, "cid", "c", plan, Path(tempfile.mkdtemp()))
        counts = dumper.run()
        self.assertEqual(counts["failed"], 1)
        self.assertEqual(dumper.results["cluster_health"]["status"], "failed")
        self.assertIn("连接被重置", dumper.results["cluster_health"]["error"])

    def test_expected_status_marked_skipped(self):
        """目录标注的预期状态码（如无快照仓库 404）记为 skipped 而非 failed"""
        responses = {
            "/_cat/repositories": {},
            "/_cat/snapshots/_all?format=json": ConsoleAPIError(
                "请求 /_cat/snapshots/_all?format=json 失败: "
                'HTTP 404: {"error": "repository_missing"}'),
        }
        client = _fake_client(responses)
        plan = [p for p in build_plan(["cat"], include_heavy=True)
                if p["path"] in responses]
        self.assertEqual(plan[0]["expected_status"], [404])
        dumper = ConfigDumper(client, "cid", "c", plan, Path(tempfile.mkdtemp()))
        counts = dumper.run()
        self.assertEqual(counts["skipped"], 1)
        self.assertEqual(dumper.results["cat_snapshots"]["status"], "skipped")

    def test_unexpected_status_still_failed(self):
        """不在预期列表中的状态码仍然记为 failed"""
        responses = {
            "/_ccr/stats": ConsoleAPIError("请求 /_ccr/stats 失败: HTTP 500: boom"),
        }
        client = _fake_client(responses)
        plan = [p for p in build_plan(["diagnostic"], include_heavy=True)
                if p["path"] == "/_ccr/stats"]
        dumper = ConfigDumper(client, "cid", "c", plan, Path(tempfile.mkdtemp()))
        counts = dumper.run()
        self.assertEqual(counts["failed"], 1)
        self.assertEqual(counts["skipped"], 0)


class TestSelectTargetClusters(unittest.TestCase):
    """测试公共的目标集群筛选（common/console_client.py）"""

    def test_filters_system_clusters(self):
        clusters = [
            {"id": "c1", "name": "prod", "version": "7.10.2"},
            {"id": DEFAULT_SYSTEM_CLUSTER_ID, "name": "INFINI SYSTEM"},
            {"id": "c2", "name": "my slingshot cluster"},
            {"id": "", "name": "no-id"},
        ]
        targets = ConsoleClient.select_target_clusters(clusters)
        self.assertEqual([t["id"] for t in targets], ["c1"])
        self.assertEqual(targets[0]["display_name"], "prod")

    def test_include_system_keeps_all(self):
        clusters = [
            {"id": "c1", "name": "prod"},
            {"id": DEFAULT_SYSTEM_CLUSTER_ID, "name": "INFINI SYSTEM"},
        ]
        targets = ConsoleClient.select_target_clusters(clusters,
                                                       include_system=True)
        self.assertEqual(len(targets), 2)

    def test_display_name_fallback_and_keys_preserved(self):
        targets = ConsoleClient.select_target_clusters(
            [{"id": "c9", "version": "7.10.2"}])
        self.assertEqual(targets[0]["display_name"], "c9")
        self.assertEqual(targets[0]["version"], "7.10.2")

    def test_none_and_empty_input(self):
        self.assertEqual(ConsoleClient.select_target_clusters(None), [])
        self.assertEqual(ConsoleClient.select_target_clusters([]), [])


class TestRunAllClusters(unittest.TestCase):
    """全集群模式：过滤系统集群、跳过不可达集群、落盘批处理清单"""

    def _client(self):
        client = MagicMock()
        client.get_clusters.return_value = [
            {"id": "c1", "name": "es-prod", "version": "7.10.2"},
            {"id": "c2", "name": "es-dev", "version": "6.8.2"},
            {"id": DEFAULT_SYSTEM_CLUSTER_ID, "name": "INFINI SYSTEM"},
        ]
        client.proxy_request.side_effect = lambda cid, method, path: {
            "path": path, "cluster_name": f"cluster-{cid}"}
        return client

    def test_batch_dump(self):
        client = self._client()
        # c2 不可达 → 跳过；c1 正常采集
        with patch("cluster_config_dump.check_cluster_reachable",
                   side_effect=lambda c, cid:
                   (True, f"cluster-{cid} (v7.10.2)") if cid == "c1"
                   else (False, "HTTP 404: not found")):
            with tempfile.TemporaryDirectory() as tmp:
                results = run_all_clusters(
                    client, selected_keys=["license"], include_heavy=True,
                    output_root=Path(tmp), timestamp="20260101_000000",
                    console_url="http://console:9000",
                    include_console_cluster=False)

                # 系统集群被过滤，只剩 2 个目标
                self.assertEqual(len(results), 2)
                by_id = {r["cluster_id"]: r for r in results}
                # c1 采集成功，产物齐备
                self.assertTrue(by_id["c1"]["ok"])
                run_dir = by_id["c1"]["run_dir"]
                self.assertEqual(run_dir.name,
                                 "config_dump_20260101_000000_es-prod")
                self.assertTrue((run_dir / "manifest.json").exists())
                self.assertTrue((run_dir / "summary.md").exists())
                # c2 不可达，标记失败
                self.assertFalse(by_id["c2"]["ok"])
                self.assertIn("404", by_id["c2"]["error"])
                self.assertIsNone(by_id["c2"]["run_dir"])

                # 批处理清单
                manifest_file = Path(tmp) / "batch_manifest_20260101_000000.json"
                self.assertTrue(manifest_file.exists())
                manifest = json.loads(manifest_file.read_text())
                self.assertEqual(manifest["mode"], "all-clusters")
                self.assertEqual(len(manifest["clusters"]), 2)
                self.assertEqual(manifest["groups_selected"], ["license"])
                entry = {c["cluster_id"]: c for c in manifest["clusters"]}
                self.assertTrue(entry["c1"]["ok"])
                self.assertIn("config_dump_20260101_000000_es-prod",
                              entry["c1"]["run_dir"])
                self.assertFalse(entry["c2"]["ok"])
                self.assertEqual(entry["c2"]["counts"], None)

    def test_dump_one_cluster_outputs(self):
        client = self._client()
        with tempfile.TemporaryDirectory() as tmp:
            result = dump_one_cluster(
                client, cluster_id="c1", display_name="es/prod:1",
                selected_keys=["license"], include_heavy=True,
                output_root=Path(tmp), timestamp="20260101_000000",
                console_url="http://console:9000")
            self.assertTrue(result["ok"])
            run_dir = result["run_dir"]
            # 显示名中的特殊字符被替换为下划线
            self.assertEqual(run_dir.name, "config_dump_20260101_000000_es_prod_1")
            self.assertTrue((run_dir / "manifest.json").exists())
            self.assertTrue((run_dir / "summary.md").exists())
            self.assertTrue((run_dir / "04_license").exists())


class TestProxyErrorRecordedAsEsError(unittest.TestCase):
    """集群侧非 2xx（经 _proxy 透传）记 es_error 并把集群错误体落盘，不再算请求失败"""

    @staticmethod
    def _proxy_error(path: str, status: int, body):
        return ProxyResponseError(
            f"请求 /elasticsearch/c1/_proxy?method=GET&path={path} 失败: "
            f"HTTP {status}: wrapper", status, body)

    def _dump_one(self, group: str, path: str, exc):
        client = _fake_client({path: exc})
        plan = [p for p in build_plan([group], include_heavy=True) if p["path"] == path]
        dumper = ConfigDumper(client, "cid", "c", plan, Path(tempfile.mkdtemp()))
        return dumper, dumper.run()

    def _saved(self, dumper, name):
        rel = dumper.results[name]["file"]
        return json.loads((dumper.run_dir / rel).read_text(encoding="utf-8"))

    def test_unsupported_api_is_es_error(self):
        body = {"error": "Incorrect HTTP method for uri [/_license]", "status": 405}
        dumper, counts = self._dump_one("license", "/_license",
                                        self._proxy_error("/_license", 405, body))
        self.assertEqual(counts["es_error"], 1)
        self.assertEqual(counts["failed"], 0)
        rec = dumper.results["license"]
        self.assertEqual(rec["status"], "es_error")
        self.assertEqual(rec["es_error_status"], 405)
        self.assertEqual(rec["error"], "ES 返回错误 405")
        # 落盘的应是集群返回的错误体本身，而不是代理包装/请求头回显
        self.assertEqual(self._saved(dumper, "license"), body)

    def test_expected_status_stays_skipped_and_keeps_body(self):
        body = {"error": "repository_missing", "status": 404}
        dumper, counts = self._dump_one(
            "cat", "/_cat/snapshots/_all?format=json",
            self._proxy_error("/_cat/snapshots/_all?format=json", 404, body))
        self.assertEqual(counts["skipped"], 1)
        self.assertEqual(counts["failed"], 0)
        saved = self._saved(dumper, "cat_snapshots")
        self.assertEqual(saved["expected_status"], 404)
        self.assertEqual(saved["es_response"], body)

    def test_plain_console_error_still_failed(self):
        """非代理层的请求失败（网络/Console 拒绝）仍记 failed，不受本次改动影响"""
        dumper, counts = self._dump_one("cluster", "/_cluster/health",
                                        ConsoleAPIError("连接被重置"))
        self.assertEqual(counts["failed"], 1)
        self.assertEqual(dumper.results["cluster_health"]["status"], "failed")

    def test_manifest_reports_es_error(self):
        body = {"error": {"reason": "no unassigned shards"}, "status": 400}
        dumper, counts = self._dump_one(
            "cluster", "/_cluster/allocation/explain",
            self._proxy_error("/_cluster/allocation/explain", 400, body))
        manifest = dumper.build_manifest(["cluster"], True, counts)
        self.assertEqual(manifest["es_error"], 1)
        self.assertEqual(manifest["failed"], 0)
        item = [i for i in manifest["items"]
                if i["name"] == "cluster_allocation_explain"][0]
        self.assertEqual(item["status"], "es_error")
        self.assertEqual(item["es_error_status"], 400)
        summary = dumper.generate_summary_md(["cluster"], True, counts)
        self.assertIn("ES 400", summary)


if __name__ == "__main__":
    unittest.main()
