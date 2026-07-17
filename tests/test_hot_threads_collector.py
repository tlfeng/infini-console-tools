#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 Hot Threads Collector 模块
"""

import json
import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "hot-threads"))

from hot_threads_collector import (  # noqa: E402
    ConsoleAPIError,
    append_record,
    build_hot_threads_path,
    choose_cluster_id,
    main,
)


class TestBuildHotThreadsPath(unittest.TestCase):
    """测试 hot threads 路径构建"""

    def test_build_path_for_all_nodes_default(self):
        args = Namespace(
            node_id="",
            threads=3,
            snapshots=10,
            ignore_idle_threads=True,
            type="cpu",
            sample_interval="",
            api_timeout="",
        )

        path = build_hot_threads_path(args)

        self.assertEqual(
            path,
            "/_nodes/hot_threads?threads=3&snapshots=10&ignore_idle_threads=true&type=cpu",
        )

    def test_build_path_for_specific_nodes_with_optional_params(self):
        args = Namespace(
            node_id="node-1,node-2",
            threads=5,
            snapshots=20,
            ignore_idle_threads=False,
            type="wait",
            sample_interval="1s",
            api_timeout="30s",
        )

        path = build_hot_threads_path(args)

        self.assertEqual(
            path,
            "/_nodes/node-1,node-2/hot_threads?"
            "threads=5&snapshots=20&ignore_idle_threads=false&type=wait&interval=1s&timeout=30s",
        )


class TestChooseClusterId(unittest.TestCase):
    """测试集群选择逻辑"""

    def test_return_cluster_id_directly(self):
        mock_client = MagicMock()

        selected = choose_cluster_id(mock_client, "cluster-from-arg", "")

        self.assertEqual(selected, "cluster-from-arg")
        mock_client.get_clusters.assert_not_called()

    def test_choose_single_non_system_cluster(self):
        mock_client = MagicMock()
        mock_client.get_clusters.return_value = [
            {"id": "infini_default_system_cluster", "name": "INFINI_SYSTEM"},
            {"id": "cluster-1", "name": "prod-es"},
        ]

        selected = choose_cluster_id(mock_client, "", "")

        self.assertEqual(selected, "cluster-1")

    def test_choose_by_cluster_name(self):
        mock_client = MagicMock()
        mock_client.get_clusters.return_value = [
            {"id": "cluster-a", "name": "alpha"},
            {"id": "cluster-b", "name": "beta"},
        ]

        selected = choose_cluster_id(mock_client, "", "beta")

        self.assertEqual(selected, "cluster-b")

    def test_raise_when_multiple_available_clusters_without_target(self):
        mock_client = MagicMock()
        mock_client.get_clusters.return_value = [
            {"id": "cluster-a", "name": "alpha"},
            {"id": "cluster-b", "name": "beta"},
        ]

        with self.assertRaises(ConsoleAPIError) as ctx:
            choose_cluster_id(mock_client, "", "")

        self.assertIn("多个可用集群", str(ctx.exception))

    def test_raise_when_no_clusters(self):
        mock_client = MagicMock()
        mock_client.get_clusters.return_value = []

        with self.assertRaises(ConsoleAPIError) as ctx:
            choose_cluster_id(mock_client, "", "")

        self.assertIn("未获取到任何集群", str(ctx.exception))


class TestAppendRecord(unittest.TestCase):
    """测试 JSONL 落盘"""

    def test_append_record_writes_jsonl(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_file = Path(tmp_dir) / "nested" / "records.jsonl"

            append_record(output_file, {"sequence": 1, "status": "ok"})
            append_record(output_file, {"sequence": 2, "status": "failed"})

            lines = output_file.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 2)
            self.assertEqual(json.loads(lines[0])["sequence"], 1)
            self.assertEqual(json.loads(lines[1])["status"], "failed")


class TestMainValidation(unittest.TestCase):
    """测试 main 参数校验"""

    @patch("hot_threads_collector.load_and_merge_config")
    @patch("hot_threads_collector.parse_args")
    def test_main_returns_error_when_poll_interval_invalid(
        self,
        mock_parse_args,
        mock_load_and_merge_config,
    ):
        mock_parse_args.return_value = Namespace(
            console="http://localhost:9000",
            username="",
            password="",
            timeout=60,
            insecure=False,
            config="",
            output="",
            cluster_id="cluster-1",
            cluster_name="",
            node_id="",
            poll_interval=0,
            duration_minutes=0,
            count=1,
            retries=0,
            retry_delay=0,
            threads=3,
            snapshots=10,
            sample_interval="",
            ignore_idle_threads=True,
            type="cpu",
            api_timeout="",
        )
        mock_load_and_merge_config.return_value = ({}, mock_parse_args.return_value)

        exit_code = main()

        self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
