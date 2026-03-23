#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 Cluster Report 模块
"""

import sys
import json
import unittest
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "cluster-report"))

from cluster_report import (
    ClusterInfo,
    ClusterReporter,
)


class TestClusterInfo(unittest.TestCase):
    """测试 ClusterInfo 数据类"""

    def test_default_values(self):
        """测试默认值"""
        info = ClusterInfo()
        self.assertEqual(info.cluster_name, "")
        self.assertEqual(info.display_name, "")
        self.assertEqual(info.version, "")
        self.assertEqual(info.health_status, "")
        self.assertFalse(info.available)
        self.assertFalse(info.monitored)
        self.assertEqual(info.uptime, "Unknown")
        self.assertEqual(info.nodes_count, 0)
        self.assertEqual(info.indices_count, 0)
        self.assertEqual(info.documents_count, 0)

    def test_custom_values(self):
        """测试自定义值"""
        info = ClusterInfo(
            cluster_name="test-cluster",
            display_name="Test Cluster",
            version="8.0.0",
            health_status="green",
            available=True,
            monitored=True,
            nodes_count=3,
            indices_count=10,
            documents_count=1000,
        )
        self.assertEqual(info.cluster_name, "test-cluster")
        self.assertTrue(info.available)
        self.assertEqual(info.nodes_count, 3)


class TestClusterReporter(unittest.TestCase):
    """测试 ClusterReporter 类"""

    def setUp(self):
        self.mock_client = MagicMock()
        self.reporter = ClusterReporter(self.mock_client)

    def test_generate_summary_empty(self):
        """测试空数据汇总"""
        summary = self.reporter.generate_summary([])
        self.assertEqual(summary, {})

    def test_generate_summary_single_cluster(self):
        """测试单集群汇总"""
        data = [
            ClusterInfo(
                cluster_name="cluster1",
                available=True,
                monitored=True,
                health_status="green",
                nodes_count=3,
                indices_count=10,
                documents_count=1000,
                storage_used_bytes=1024 * 1024,
            )
        ]
        
        summary = self.reporter.generate_summary(data)
        
        self.assertEqual(summary["total_clusters"], 1)
        self.assertEqual(summary["available_clusters"], 1)
        self.assertEqual(summary["monitored_clusters"], 1)
        self.assertEqual(summary["total_nodes"], 3)
        self.assertEqual(summary["total_indices"], 10)
        self.assertEqual(summary["total_documents"], 1000)

    def test_generate_summary_multiple_clusters(self):
        """测试多集群汇总"""
        data = [
            ClusterInfo(
                cluster_name="cluster1",
                available=True,
                monitored=True,
                health_status="green",
                nodes_count=3,
                indices_count=10,
                documents_count=1000,
                storage_used_bytes=1024,
            ),
            ClusterInfo(
                cluster_name="cluster2",
                available=False,
                monitored=False,
                health_status="red",
                nodes_count=1,
                indices_count=5,
                documents_count=500,
                storage_used_bytes=2048,
            ),
        ]
        
        summary = self.reporter.generate_summary(data)
        
        self.assertEqual(summary["total_clusters"], 2)
        self.assertEqual(summary["available_clusters"], 1)
        self.assertEqual(summary["monitored_clusters"], 1)
        self.assertEqual(summary["total_nodes"], 4)
        self.assertEqual(summary["total_indices"], 15)
        self.assertEqual(summary["total_documents"], 1500)

    def test_generate_summary_health_distribution(self):
        """测试健康状态分布"""
        data = [
            ClusterInfo(health_status="green"),
            ClusterInfo(health_status="green"),
            ClusterInfo(health_status="yellow"),
            ClusterInfo(health_status="red"),
        ]
        
        summary = self.reporter.generate_summary(data)
        
        health_dist = summary["health_distribution"]
        self.assertEqual(health_dist["green"], 2)
        self.assertEqual(health_dist["yellow"], 1)
        self.assertEqual(health_dist["red"], 1)

    @patch("cluster_report.datetime")
    def test_collect_all_data_filters_system_clusters(self, mock_datetime):
        """测试过滤系统集群"""
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01 00:00:00"
        
        self.mock_client.get_clusters.return_value = [
            {"id": "cluster1", "name": "Normal Cluster", "version": "8.0.0", "monitored": True},
            {"id": "infini_default_system_cluster", "name": "INFINI_SYSTEM", "version": "8.0.0", "monitored": True},
        ]
        self.mock_client.get_clusters_status.return_value = {
            "cluster1": {"available": True, "health": {"status": "green", "number_of_nodes": 3}},
        }
        self.mock_client.get_cluster_metrics.return_value = {
            "summary": {
                "indices_count": 10,
                "document_count": 1000,
            }
        }

        result = self.reporter.collect_all_data(include_console_cluster=False)
        
        # 应该只返回非系统集群
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].cluster_name, "Normal Cluster")

    @patch("cluster_report.datetime")
    def test_collect_all_data_includes_system_clusters(self, mock_datetime):
        """测试包含系统集群"""
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01 00:00:00"
        
        self.mock_client.get_clusters.return_value = [
            {"id": "cluster1", "name": "Normal Cluster", "version": "8.0.0", "monitored": True},
            {"id": "sys_cluster", "name": "INFINI_SYSTEM", "version": "8.0.0", "monitored": True},
        ]
        self.mock_client.get_clusters_status.return_value = {
            "cluster1": {"available": True, "health": {"status": "green"}},
            "sys_cluster": {"available": True, "health": {"status": "green"}},
        }
        self.mock_client.get_cluster_metrics.return_value = {"summary": {}}

        result = self.reporter.collect_all_data(include_console_cluster=True)
        
        # 应该返回所有集群
        self.assertEqual(len(result), 2)


if __name__ == "__main__":
    unittest.main()
