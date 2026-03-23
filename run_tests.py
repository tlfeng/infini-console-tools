#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试运行脚本

运行所有测试或指定测试

用法:
  python run_tests.py              # 运行所有测试
  python run_tests.py -v           # 运行所有测试（详细输出）
  python run_tests.py config       # 只运行 config 测试
  python run_tests.py console      # 只运行 console_client 测试
  python run_tests.py sampler      # 只运行 index_sampler 测试
  python run_tests.py cluster      # 只运行 cluster_report 测试
"""

import sys
import unittest
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / "tests"))

# 导入测试模块
from tests import (
    test_config,
    test_console_client,
    test_index_sampler,
    test_cluster_report,
)


def run_all_tests(verbosity=1):
    """运行所有测试"""
    # 创建测试加载器
    loader = unittest.TestLoader()
    
    # 创建测试套件
    suite = unittest.TestSuite()
    
    # 添加各个测试模块
    suite.addTests(loader.loadTestsFromModule(test_config))
    suite.addTests(loader.loadTestsFromModule(test_console_client))
    suite.addTests(loader.loadTestsFromModule(test_index_sampler))
    suite.addTests(loader.loadTestsFromModule(test_cluster_report))
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)
    
    return result.wasSuccessful()


def run_specific_test(test_name, verbosity=1):
    """运行特定测试"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    test_map = {
        "config": test_config,
        "console": test_console_client,
        "sampler": test_index_sampler,
        "cluster": test_cluster_report,
    }
    
    if test_name in test_map:
        suite.addTests(loader.loadTestsFromModule(test_map[test_name]))
    else:
        print(f"未知测试模块: {test_name}")
        print(f"可用选项: {', '.join(test_map.keys())}")
        return False
    
    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)
    
    return result.wasSuccessful()


def main():
    """主函数"""
    args = sys.argv[1:]
    
    verbosity = 1
    if "-v" in args or "--verbose" in args:
        verbosity = 2
        args = [a for a in args if a not in ("-v", "--verbose")]
    
    if not args:
        print("=" * 60)
        print("Running all tests")
        print("=" * 60)
        success = run_all_tests(verbosity)
    else:
        test_name = args[0].lower()
        print("=" * 60)
        print(f"Running {test_name} tests")
        print("=" * 60)
        success = run_specific_test(test_name, verbosity)
    
    # 返回退出码
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
