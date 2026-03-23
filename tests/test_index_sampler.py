#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 Index Sampler 模块
"""

import sys
import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "index-sampler"))

from index_sampler import (
    IndexSample,
    ClusterResult,
    SamplingReport,
    sample_cluster,
    export_csv,
)


class TestIndexSample(unittest.TestCase):
    """测试 IndexSample 类"""

    def test_init(self):
        """测试初始化"""
        sample = IndexSample("cluster1", "Cluster One", "my_index")
        self.assertEqual(sample.cluster_id, "cluster1")
        self.assertEqual(sample.cluster_name, "Cluster One")
        self.assertEqual(sample.index_name, "my_index")
        self.assertIsNone(sample.mapping)
        self.assertEqual(sample.sample_docs, [])
        self.assertEqual(sample.doc_count, 0)

    def test_to_dict(self):
        """测试转换为字典"""
        sample = IndexSample("cluster1", "Cluster One", "my_index")
        sample.mapping = {"properties": {"name": {"type": "text"}}}
        sample.sample_docs = [{"_id": "1", "_source": {"name": "test"}}]
        sample.doc_count = 100

        result = sample.to_dict()
        self.assertEqual(result["cluster_id"], "cluster1")
        self.assertEqual(result["cluster_name"], "Cluster One")
        self.assertEqual(result["index_name"], "my_index")
        self.assertEqual(result["doc_count"], 100)
        self.assertIn("properties", result["mapping"])


class TestClusterResult(unittest.TestCase):
    """测试 ClusterResult 类"""

    def test_init(self):
        """测试初始化"""
        result = ClusterResult("cluster1", "Cluster One")
        self.assertEqual(result.cluster_id, "cluster1")
        self.assertEqual(result.cluster_name, "Cluster One")
        self.assertEqual(result.indices_count, 0)
        self.assertEqual(result.indices, [])

    def test_to_dict_empty(self):
        """测试空结果转字典"""
        result = ClusterResult("cluster1", "Cluster One")
        data = result.to_dict()
        self.assertEqual(data["cluster_id"], "cluster1")
        self.assertEqual(data["cluster_name"], "Cluster One")
        self.assertEqual(data["indices_count"], 0)
        self.assertEqual(data["indices"], [])

    def test_to_dict_with_indices(self):
        """测试带索引的结果转字典"""
        result = ClusterResult("cluster1", "Cluster One")
        sample = IndexSample("cluster1", "Cluster One", "idx1")
        result.indices.append(sample)
        result.indices_count = 1

        data = result.to_dict()
        self.assertEqual(data["indices_count"], 1)
        self.assertEqual(len(data["indices"]), 1)


class TestSamplingReport(unittest.TestCase):
    """测试 SamplingReport 类"""

    def test_init(self):
        """测试初始化"""
        report = SamplingReport()
        self.assertEqual(report.total_clusters, 0)
        self.assertEqual(report.results, [])

    def test_to_dict(self):
        """测试转字典"""
        report = SamplingReport()
        report.total_clusters = 2
        
        cluster_result = ClusterResult("c1", "Cluster 1")
        report.results.append(cluster_result)

        data = report.to_dict()
        self.assertEqual(data["total_clusters"], 2)
        self.assertEqual(len(data["results"]), 1)


class TestSampleCluster(unittest.TestCase):
    """测试 sample_cluster 函数"""

    def setUp(self):
        self.mock_client = MagicMock()
        self.cluster = {"id": "cluster1", "name": "Test Cluster"}
        self.clusters_status = {
            "cluster1": {"available": True, "health": {"status": "green"}}
        }

    def test_skip_unavailable_cluster(self):
        """测试跳过不可用的集群"""
        clusters_status = {"cluster1": {"available": False}}
        
        result = sample_cluster(
            self.mock_client,
            self.cluster,
            clusters_status,
            sample_size=2,
            max_indices=10,
            include_system_indices=False,
        )
        self.assertIsNone(result)

    def test_skip_no_status_cluster(self):
        """测试跳过无状态信息的集群"""
        clusters_status = {}
        
        result = sample_cluster(
            self.mock_client,
            self.cluster,
            clusters_status,
            sample_size=2,
            max_indices=10,
            include_system_indices=False,
        )
        self.assertIsNone(result)

    def test_no_indices(self):
        """测试无索引的情况"""
        self.mock_client.get_indices.return_value = {}
        
        result = sample_cluster(
            self.mock_client,
            self.cluster,
            self.clusters_status,
            sample_size=2,
            max_indices=10,
            include_system_indices=False,
        )
        self.assertIsNone(result)

    def test_skip_system_indices(self):
        """测试跳过系统索引"""
        self.mock_client.get_indices.return_value = {
            ".kibana": {"health": "green", "docs.count": "10"},
            "normal_index": {"health": "green", "docs.count": "100"},
        }
        self.mock_client.get_index_mapping.return_value = {}
        self.mock_client.search_index.return_value = []

        result = sample_cluster(
            self.mock_client,
            self.cluster,
            self.clusters_status,
            sample_size=2,
            max_indices=10,
            include_system_indices=False,  # 不包含系统索引
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result.indices), 1)
        self.assertEqual(result.indices[0].index_name, "normal_index")

    def test_include_system_indices(self):
        """测试包含系统索引"""
        self.mock_client.get_indices.return_value = {
            ".kibana": {"health": "green", "docs.count": "10"},
            "normal_index": {"health": "green", "docs.count": "100"},
        }
        self.mock_client.get_index_mapping.return_value = {}
        self.mock_client.search_index.return_value = []

        result = sample_cluster(
            self.mock_client,
            self.cluster,
            self.clusters_status,
            sample_size=2,
            max_indices=10,
            include_system_indices=True,  # 包含系统索引
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result.indices), 2)

    def test_skip_unavailable_index(self):
        """测试跳过 unavailable 状态的索引"""
        self.mock_client.get_indices.return_value = {
            "bad_index": {"health": "unavailable", "docs.count": "0"},
            "good_index": {"health": "green", "docs.count": "100"},
        }
        self.mock_client.get_index_mapping.return_value = {}
        self.mock_client.search_index.return_value = []

        result = sample_cluster(
            self.mock_client,
            self.cluster,
            self.clusters_status,
            sample_size=2,
            max_indices=10,
            include_system_indices=False,
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result.indices), 1)
        self.assertEqual(result.indices[0].index_name, "good_index")

    def test_max_indices_limit(self):
        """测试最大索引数限制"""
        self.mock_client.get_indices.return_value = {
            "index1": {"health": "green", "docs.count": "10"},
            "index2": {"health": "green", "docs.count": "20"},
            "index3": {"health": "green", "docs.count": "30"},
        }
        self.mock_client.get_index_mapping.return_value = {}
        self.mock_client.search_index.return_value = []

        result = sample_cluster(
            self.mock_client,
            self.cluster,
            self.clusters_status,
            sample_size=2,
            max_indices=2,  # 最多2个
            include_system_indices=False,
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result.indices), 2)

    def test_doc_count_parsing(self):
        """测试文档数量解析"""
        self.mock_client.get_indices.return_value = {
            "test_index": {"health": "green", "docs.count": "42"},
        }
        self.mock_client.get_index_mapping.return_value = {}
        self.mock_client.search_index.return_value = []

        result = sample_cluster(
            self.mock_client,
            self.cluster,
            self.clusters_status,
            sample_size=2,
            max_indices=10,
            include_system_indices=False,
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result.indices[0].doc_count, 42)

    def test_doc_count_invalid(self):
        """测试无效的文档数量"""
        self.mock_client.get_indices.return_value = {
            "test_index": {"health": "green", "docs.count": "invalid"},
        }
        self.mock_client.get_index_mapping.return_value = {}
        self.mock_client.search_index.return_value = []

        result = sample_cluster(
            self.mock_client,
            self.cluster,
            self.clusters_status,
            sample_size=2,
            max_indices=10,
            include_system_indices=False,
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result.indices[0].doc_count, 0)  # 无效值应转为 0


if __name__ == "__main__":
    unittest.main()
