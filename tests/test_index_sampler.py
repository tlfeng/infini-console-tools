#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 Index Sampler 模块
"""

import sys
import csv
import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

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
        self.assertEqual(sample.index_info, {})
        self.assertIsNone(sample.mapping)
        self.assertIsNone(sample.settings)
        self.assertEqual(sample.sample_docs, [])

    def test_to_dict(self):
        """测试转换为字典"""
        sample = IndexSample("cluster1", "Cluster One", "my_index")
        sample.index_info = {
            "health": "green",
            "status": "open",
            "shards": "5",
            "pri": "5",
            "rep": "1",
            "docs.count": "100",
            "docs.deleted": "0",
            "store.size": "10mb",
            "pri.store.size": "5mb",
        }
        sample.mapping = {"properties": {"name": {"type": "text"}}}
        sample.settings = {"index": {"number_of_replicas": "1"}}
        sample.sample_docs = [{"_id": "1", "_source": {"name": "test"}}]

        result = sample.to_dict()
        self.assertEqual(result["cluster_id"], "cluster1")
        self.assertEqual(result["cluster_name"], "Cluster One")
        self.assertEqual(result["index_name"], "my_index")
        self.assertEqual(result["index_info"]["health"], "green")
        self.assertEqual(result["index_info"]["docs.count"], "100")
        self.assertEqual(result["index_info"]["store.size"], "10mb")
        self.assertIn("properties", result["mapping"])
        self.assertIn("number_of_replicas", result["settings"]["index"])

    def test_to_dict_preserves_all_index_info_fields(self):
        """测试 index_info 保留 _cat/indices 返回的所有字段"""
        sample = IndexSample("c1", "C1", "idx1")
        sample.index_info = {
            "index": "idx1",
            "health": "green",
            "status": "open",
            "shards": "5",
            "pri": "5",
            "rep": "1",
            "docs.count": "12345",
            "docs.deleted": "678",
            "store.size": "1.2gb",
            "pri.store.size": "600mb",
        }

        result = sample.to_dict()
        self.assertEqual(result["index_info"]["index"], "idx1")
        self.assertEqual(result["index_info"]["docs.deleted"], "678")
        self.assertEqual(result["index_info"]["store.size"], "1.2gb")


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
        sample.index_info = {"health": "green", "docs.count": "50"}
        result.indices.append(sample)
        result.indices_count = 1

        data = result.to_dict()
        self.assertEqual(data["indices_count"], 1)
        self.assertEqual(data["indices"][0]["index_info"]["health"], "green")


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
        self.mock_client.get_index_settings.return_value = {}
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
        self.assertEqual(result.indices[0].index_name, "normal_index")

    def test_include_system_indices(self):
        """测试包含系统索引"""
        self.mock_client.get_indices.return_value = {
            ".kibana": {"health": "green", "docs.count": "10"},
            "normal_index": {"health": "green", "docs.count": "100"},
        }
        self.mock_client.get_index_mapping.return_value = {}
        self.mock_client.get_index_settings.return_value = {}
        self.mock_client.search_index.return_value = []

        result = sample_cluster(
            self.mock_client,
            self.cluster,
            self.clusters_status,
            sample_size=2,
            max_indices=10,
            include_system_indices=True,
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
        self.mock_client.get_index_settings.return_value = {}
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
        self.mock_client.get_index_settings.return_value = {}
        self.mock_client.search_index.return_value = []

        result = sample_cluster(
            self.mock_client,
            self.cluster,
            self.clusters_status,
            sample_size=2,
            max_indices=2,
            include_system_indices=False,
        )

        self.assertIsNotNone(result)
        self.assertEqual(len(result.indices), 2)

    def test_index_info_stored(self):
        """测试完整的 _cat/indices 信息被保存到 index_info"""
        index_info = {
            "health": "green",
            "status": "open",
            "shards": "5",
            "pri": "5",
            "rep": "1",
            "docs.count": "42",
            "docs.deleted": "3",
            "store.size": "1.2gb",
            "pri.store.size": "600mb",
        }
        self.mock_client.get_indices.return_value = {
            "test_index": index_info,
        }
        self.mock_client.get_index_mapping.return_value = {"properties": {}}
        self.mock_client.get_index_settings.return_value = {"number_of_replicas": "1"}
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
        # index_info 保存了完整字典
        self.assertEqual(result.indices[0].index_info, index_info)
        self.assertEqual(result.indices[0].index_info["store.size"], "1.2gb")
        self.assertEqual(result.indices[0].index_info["docs.deleted"], "3")
        self.assertEqual(result.indices[0].index_info["shards"], "5")

    def test_settings_stored(self):
        """测试 settings 被正确保存"""
        settings_data = {"number_of_replicas": "1", "number_of_shards": "5"}
        self.mock_client.get_indices.return_value = {
            "test_index": {"health": "green", "docs.count": "42"},
        }
        self.mock_client.get_index_mapping.return_value = {"properties": {}}
        self.mock_client.get_index_settings.return_value = settings_data
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
        self.assertEqual(result.indices[0].settings, settings_data)

    def test_settings_none_when_api_fails(self):
        """测试 settings API 失败时返回 None"""
        self.mock_client.get_indices.return_value = {
            "test_index": {"health": "green", "docs.count": "42"},
        }
        self.mock_client.get_index_mapping.return_value = {}
        self.mock_client.get_index_settings.return_value = None
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
        self.assertIsNone(result.indices[0].settings)

    def test_all_api_calls_per_index(self):
        """测试每个索引都会调用 mapping、settings、search 三个 API"""
        self.mock_client.get_indices.return_value = {
            "idx1": {"health": "green", "docs.count": "10"},
            "idx2": {"health": "yellow", "docs.count": "20"},
        }
        self.mock_client.get_index_mapping.return_value = {}
        self.mock_client.get_index_settings.return_value = {}
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
        # 每个 index 调用 get_index_mapping、get_index_settings、search_index
        self.assertEqual(self.mock_client.get_index_mapping.call_count, 2)
        self.assertEqual(self.mock_client.get_index_settings.call_count, 2)
        self.assertEqual(self.mock_client.search_index.call_count, 2)


class TestExportCsv(unittest.TestCase):
    """测试 export_csv 函数"""

    def test_csv_headers_and_rows(self):
        """测试 CSV 表头和数据行"""
        report = SamplingReport()
        cluster_result = ClusterResult("c1", "Cluster 1")

        sample = IndexSample("c1", "Cluster 1", "my_index")
        sample.index_info = {
            "health": "green",
            "status": "open",
            "shards": "5",
            "pri": "5",
            "rep": "1",
            "docs.count": "100",
            "docs.deleted": "0",
            "store.size": "10mb",
            "pri.store.size": "5mb",
        }
        sample.mapping = {"properties": {}}
        sample.settings = {"number_of_replicas": "1"}
        sample.sample_docs = [{"_id": "1"}]

        cluster_result.indices.append(sample)
        cluster_result.indices_count = 1
        report.results.append(cluster_result)

        # 导出到临时路径
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            tmp_path = Path(f.name)

        export_csv(report, tmp_path)

        with open(tmp_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)

        # 验证表头
        self.assertEqual(rows[0][0], "Cluster ID")
        self.assertEqual(rows[0][3], "Health")
        self.assertEqual(rows[0][11], "Primary Store Size")
        self.assertEqual(rows[0][13], "Has Mapping")
        self.assertEqual(rows[0][14], "Has Settings")

        # 验证数据行
        self.assertEqual(rows[1][0], "c1")
        self.assertEqual(rows[1][2], "my_index")
        self.assertEqual(rows[1][3], "green")
        self.assertEqual(rows[1][9], "0")
        self.assertEqual(rows[1][10], "10mb")
        self.assertEqual(rows[1][13], "Yes")
        self.assertEqual(rows[1][14], "Yes")

        tmp_path.unlink()

    def test_csv_missing_fields_default_empty(self):
        """测试 index_info 中缺失的字段在 CSV 中为空字符串"""
        report = SamplingReport()
        cluster_result = ClusterResult("c1", "Cluster 1")

        sample = IndexSample("c1", "Cluster 1", "sparse_index")
        sample.index_info = {"health": "green"}  # 只有 health
        sample.mapping = None
        sample.settings = None
        sample.sample_docs = []

        cluster_result.indices.append(sample)
        cluster_result.indices_count = 1
        report.results.append(cluster_result)

        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            tmp_path = Path(f.name)

        export_csv(report, tmp_path)

        with open(tmp_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)

        # 缺失字段应为空字符串
        self.assertEqual(rows[1][4], "")  # status
        self.assertEqual(rows[1][9], "")   # docs.deleted
        self.assertEqual(rows[1][10], "")  # store.size
        self.assertEqual(rows[1][13], "No")
        self.assertEqual(rows[1][14], "No")

        tmp_path.unlink()


if __name__ == "__main__":
    unittest.main()