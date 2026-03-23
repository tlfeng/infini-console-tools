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


if __name__ == "__main__":
    unittest.main()
