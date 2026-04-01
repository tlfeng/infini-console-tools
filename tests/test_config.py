#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试配置管理模块
"""

import os
import sys
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.config import (
    load_config_file,
    get_config_value,
    BaseConfig,
    add_common_args,
    load_and_merge_config,
    # 新增
    TargetFilter,
    TargetsConfig,
    SamplingConfig,
    OutputConfig,
    ExecutionConfig,
    MetricsJobConfig,
    MetricsExporterConfig,
    GlobalConfig,
    AppConfig,
    ConfigValidationError,
    validate_config,
)


class TestLoadConfigFile(unittest.TestCase):
    """测试配置文件加载"""

    def test_load_valid_config(self):
        """测试加载有效的配置文件"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"consoleUrl": "http://test:9000", "timeout": 30}, f)
            temp_path = f.name

        try:
            config = load_config_file(temp_path)
            self.assertEqual(config["consoleUrl"], "http://test:9000")
            self.assertEqual(config["timeout"], 30)
        finally:
            os.unlink(temp_path)

    def test_load_nonexistent_file(self):
        """测试加载不存在的配置文件"""
        config = load_config_file("/nonexistent/config.json")
        self.assertEqual(config, {})

    def test_load_invalid_json(self):
        """测试加载无效的 JSON 文件"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("invalid json")
            temp_path = f.name

        try:
            with self.assertRaises(json.JSONDecodeError):
                load_config_file(temp_path)
        finally:
            os.unlink(temp_path)


class TestGetConfigValue(unittest.TestCase):
    """测试配置值获取"""

    def test_cli_value_priority(self):
        """测试命令行参数优先级最高"""
        os.environ["TEST_VAR"] = "env_value"
        result = get_config_value("cli_value", "config_value", "TEST_VAR", "default")
        self.assertEqual(result, "cli_value")
        del os.environ["TEST_VAR"]

    def test_config_value_priority(self):
        """测试配置文件值优先级次之"""
        os.environ["TEST_VAR"] = "env_value"
        result = get_config_value("", "config_value", "TEST_VAR", "default")
        self.assertEqual(result, "config_value")
        del os.environ["TEST_VAR"]

    def test_env_value_priority(self):
        """测试环境变量优先级再次之"""
        os.environ["TEST_VAR"] = "env_value"
        result = get_config_value("", "", "TEST_VAR", "default")
        self.assertEqual(result, "env_value")
        del os.environ["TEST_VAR"]

    def test_default_value(self):
        """测试默认值"""
        result = get_config_value("", "", "NONEXISTENT_VAR", "default")
        self.assertEqual(result, "default")

    def test_empty_string_not_used(self):
        """测试空字符串不被使用"""
        result = get_config_value("", "config", "VAR", "default")
        self.assertEqual(result, "config")


class TestBaseConfig(unittest.TestCase):
    """测试基础配置类"""

    def test_default_values(self):
        """测试默认值"""
        config = BaseConfig()
        self.assertEqual(config.console_url, "http://localhost:9000")
        self.assertEqual(config.username, "")
        self.assertEqual(config.password, "")
        self.assertEqual(config.timeout, 60)
        self.assertFalse(config.insecure)

    def test_custom_values(self):
        """测试自定义值"""
        config = BaseConfig(
            console_url="http://test:9000",
            username="admin",
            password="secret",
            timeout=120,
            insecure=True,
        )
        self.assertEqual(config.console_url, "http://test:9000")
        self.assertEqual(config.username, "admin")
        self.assertEqual(config.password, "secret")
        self.assertEqual(config.timeout, 120)
        self.assertTrue(config.insecure)


class TestAddCommonArgs(unittest.TestCase):
    """测试参数解析器"""

    def test_add_common_args(self):
        """测试添加通用参数"""
        import argparse
        parser = argparse.ArgumentParser()
        parser = add_common_args(parser)

        # 测试解析默认参数
        args = parser.parse_args([])
        self.assertEqual(args.console, "http://localhost:9000")
        self.assertEqual(args.timeout, 60)
        self.assertFalse(args.insecure)

    def test_custom_args(self):
        """测试自定义参数"""
        import argparse
        parser = argparse.ArgumentParser()
        parser = add_common_args(parser)

        args = parser.parse_args([
            "-c", "http://custom:9000",
            "-u", "testuser",
            "-p", "testpass",
            "--timeout", "120",
            "--insecure",
        ])
        self.assertEqual(args.console, "http://custom:9000")
        self.assertEqual(args.username, "testuser")
        self.assertEqual(args.password, "testpass")
        self.assertEqual(args.timeout, 120)
        self.assertTrue(args.insecure)


class TestLoadAndMergeConfig(unittest.TestCase):
    """测试配置合并"""

    def test_without_config_file(self):
        """测试没有配置文件的情况"""
        import argparse
        args = argparse.Namespace()
        args.config = ""

        config, merged_args = load_and_merge_config(args)
        self.assertEqual(config, {})

    def test_with_config_file(self):
        """测试有配置文件的情况"""
        import argparse

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"consoleUrl": "http://test:9000"}, f)
            temp_path = f.name

        try:
            args = argparse.Namespace()
            args.config = temp_path

            config, merged_args = load_and_merge_config(args)
            self.assertEqual(config["consoleUrl"], "http://test:9000")
        finally:
            os.unlink(temp_path)


class TestTargetFilter(unittest.TestCase):
    """测试目标筛选"""

    def test_empty_filter_matches_all(self):
        """空筛选器匹配所有"""
        f = TargetFilter()
        self.assertTrue(f.matches("anything"))
        self.assertTrue(f.matches("cluster-1"))

    def test_include_only(self):
        """仅 include 筛选"""
        f = TargetFilter(include=["cluster-1", "cluster-2"])
        self.assertTrue(f.matches("cluster-1"))
        self.assertTrue(f.matches("cluster-2"))
        self.assertFalse(f.matches("cluster-3"))

    def test_exclude_only(self):
        """仅 exclude 筛选"""
        f = TargetFilter(exclude=["test-*"])
        self.assertFalse(f.matches("test-1"))
        self.assertFalse(f.matches("test-abc"))
        self.assertTrue(f.matches("prod-1"))

    def test_include_and_exclude(self):
        """include 和 exclude 组合"""
        f = TargetFilter(include=["*-prod-*"], exclude=["*-prod-test"])
        self.assertTrue(f.matches("us-prod-1"))
        self.assertFalse(f.matches("us-prod-test"))
        self.assertFalse(f.matches("us-dev-1"))

    def test_wildcard_match(self):
        """通配符匹配"""
        f = TargetFilter(include=["*-cluster"])
        self.assertTrue(f.matches("test-cluster"))
        self.assertTrue(f.matches("prod-cluster"))
        self.assertFalse(f.matches("cluster-test"))


class TestSamplingConfig(unittest.TestCase):
    """测试抽样配置"""

    def test_full_mode(self):
        """全量模式"""
        s = SamplingConfig(mode="full")
        self.assertFalse(s.is_sampling())

    def test_sampling_with_interval(self):
        """时间间隔抽样"""
        s = SamplingConfig(mode="sampling", interval="1h")
        self.assertTrue(s.is_sampling())
        self.assertEqual(s.interval, "1h")

    def test_sampling_with_ratio(self):
        """比例抽样"""
        s = SamplingConfig(mode="sampling", ratio=0.1)
        self.assertTrue(s.is_sampling())
        self.assertEqual(s.ratio, 0.1)

    def test_invalid_mode(self):
        """无效模式"""
        with self.assertRaises(ConfigValidationError):
            SamplingConfig.from_dict({"mode": "invalid"})

    def test_sampling_without_interval_or_ratio(self):
        """抽样模式缺少参数"""
        with self.assertRaises(ConfigValidationError):
            SamplingConfig.from_dict({"mode": "sampling"})

    def test_invalid_ratio(self):
        """无效比例"""
        with self.assertRaises(ConfigValidationError):
            SamplingConfig.from_dict({"mode": "sampling", "ratio": 1.5})
        with self.assertRaises(ConfigValidationError):
            SamplingConfig.from_dict({"mode": "sampling", "ratio": 0})


class TestExecutionConfig(unittest.TestCase):
    """测试执行配置"""

    def test_default_values(self):
        """默认值"""
        e = ExecutionConfig()
        self.assertEqual(e.parallel_metrics, 2)
        self.assertEqual(e.scroll_keepalive, "60s")
        self.assertEqual(e.max_retries, 3)

    def test_from_dict(self):
        """从字典解析"""
        e = ExecutionConfig.from_dict({
            "parallelMetrics": 4,
            "batchSize": 5000,
            "scrollKeepalive": "30s"
        })
        self.assertEqual(e.parallel_metrics, 4)
        self.assertEqual(e.batch_size, 5000)
        self.assertEqual(e.scroll_keepalive, "30s")


class TestMetricsJobConfig(unittest.TestCase):
    """测试 Job 配置"""

    def test_valid_job(self):
        """有效的 Job 配置"""
        job = MetricsJobConfig.from_dict({
            "name": "test-job",
            "metrics": ["node_stats", "index_stats"],
            "timeRangeHours": 24
        })
        self.assertEqual(job.name, "test-job")
        self.assertEqual(job.metrics, ["node_stats", "index_stats"])
        self.assertEqual(job.time_range_hours, 24)

    def test_job_without_name(self):
        """缺少名称"""
        with self.assertRaises(ConfigValidationError):
            MetricsJobConfig.from_dict({"metrics": ["node_stats"]})

    def test_job_without_metrics(self):
        """缺少指标类型"""
        with self.assertRaises(ConfigValidationError):
            MetricsJobConfig.from_dict({"name": "test"})

    def test_job_with_invalid_metric_type(self):
        """无效的指标类型"""
        with self.assertRaises(ConfigValidationError):
            MetricsJobConfig.from_dict({
                "name": "test",
                "metrics": ["invalid_type"]
            })

    def test_job_with_sampling(self):
        """带抽样配置的 Job"""
        job = MetricsJobConfig.from_dict({
            "name": "sampled-job",
            "metrics": ["node_stats"],
            "sampling": {"mode": "sampling", "interval": "1h"}
        })
        self.assertTrue(job.sampling.is_sampling())
        self.assertEqual(job.sampling.interval, "1h")


class TestAppConfig(unittest.TestCase):
    """测试完整应用配置"""

    def test_load_valid_config(self):
        """加载有效配置"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({
                "consoleUrl": "http://test:9000",
                "auth": {"username": "admin", "password": "test"},
                "metricsExporter": {
                    "jobs": [
                        {
                            "name": "test-job",
                            "metrics": ["node_stats"],
                            "timeRangeHours": 24
                        }
                    ]
                }
            }, f)
            temp_path = f.name

        try:
            config = AppConfig.load(temp_path)
            self.assertEqual(config.global_config.console_url, "http://test:9000")
            self.assertEqual(config.global_config.username, "admin")
            self.assertIsNotNone(config.metrics_exporter)
            self.assertEqual(len(config.metrics_exporter.jobs), 1)
        finally:
            os.unlink(temp_path)

    def test_validate_config(self):
        """验证配置文件"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({
                "consoleUrl": "http://test:9000",
                "metricsExporter": {
                    "jobs": [
                        {
                            "name": "valid-job",
                            "metrics": ["cluster_health", "node_stats"]
                        }
                    ]
                }
            }, f)
            temp_path = f.name

        try:
            errors = validate_config(temp_path)
            self.assertEqual(len(errors), 0)
        finally:
            os.unlink(temp_path)

    def test_validate_invalid_config(self):
        """验证无效配置"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({
                "metricsExporter": {
                    "jobs": [
                        {
                            "name": "invalid-job",
                            "metrics": ["invalid_metric"]
                        }
                    ]
                }
            }, f)
            temp_path = f.name

        try:
            errors = validate_config(temp_path)
            self.assertGreater(len(errors), 0)
        finally:
            os.unlink(temp_path)


if __name__ == "__main__":
    unittest.main()
