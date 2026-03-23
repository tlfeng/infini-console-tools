#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ES Query Test Runner - Python 版本
自动化 Elasticsearch 查询性能测试工具
支持 JSON 配置文件驱动，记录时延和 ES Took
支持 Kibana 格式的查询文件
支持性能对比测试和可视化报告
"""

import json
import requests
import sys
import argparse
import os
import time
import csv
import copy
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib
from matplotlib import rcParams
matplotlib.use('Agg')  # 使用非交互式后端

# 配置中文字体
try:
    rcParams['font.sans-serif'] = ['PingFang SC', 'STHeiti', 'Microsoft YaHei', 'SimHei', 'DejaVu Sans']
except:
    pass
rcParams['axes.unicode_minus'] = False  # 解决负号显示问题


class AuthManager:
    """认证管理器"""

    def __init__(self, base_url):
        self.base_url = base_url.rstrip('/')

    def login(self, username, password):
        """登录获取访问 token"""
        url = f"{self.base_url}/account/login"
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'username': username,
            'password': password
        }

        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()

            result = response.json()
            if result.get('status') != 'ok':
                raise Exception(f"登录失败: {result.get('error', '未知错误')}")

            return f"Bearer {result['access_token']}"
        except requests.exceptions.RequestException as e:
            print(f"登录请求失败: {e}")
            return None
        except ValueError as e:
            print(f"响应解析失败: {e}")
            return None


class QueryExecutor:
    """查询执行器"""

    def __init__(self, base_url, token, cluster_id):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.cluster_id = cluster_id
        self.headers = {
            'Authorization': token,
            'Content-Type': 'application/json',
        }

    def is_search_request(self, path):
        """判断是否为 _search 请求（可使用 search profile）"""
        normalized = (path or '').split('?', 1)[0].strip().lower()
        return normalized.endswith('/_search') or '/_search/' in normalized

    def is_msearch_request(self, path):
        """判断是否为 _msearch 请求"""
        normalized = (path or '').split('?', 1)[0].strip().lower()
        return normalized.endswith('/_msearch') or '/_msearch/' in normalized

    def extract_took(self, path, response_body):
        """提取 took。_msearch 按子查询 took 最大值；其他请求按顶层 took。"""
        if not isinstance(response_body, dict):
            return None

        if self.is_msearch_request(path):
            responses = response_body.get('responses')
            if isinstance(responses, list):
                sub_tooks = [
                    item.get('took')
                    for item in responses
                    if isinstance(item, dict) and isinstance(item.get('took'), (int, float))
                ]
                if sub_tooks:
                    return max(sub_tooks)

        took = response_body.get('took')
        return took if isinstance(took, (int, float)) else None

    def build_profile_body(self, body):
        """为查询 body 注入 profile: true（不修改原对象）"""
        if isinstance(body, dict):
            profile_body = copy.deepcopy(body)
        else:
            profile_body = {}
        profile_body['profile'] = True
        return profile_body

    def extract_profile_summary(self, profile_data):
        """提取对 ES 语句优化有参考价值的 profile 关键信息"""
        if not isinstance(profile_data, dict):
            return None

        shards = profile_data.get('shards') or []
        if not shards:
            return None

        total_query_nanos = 0
        total_fetch_nanos = 0
        total_agg_nanos = 0
        total_rewrite_nanos = 0
        breakdown_totals = {}
        query_components = []
        collector_components = []
        slowest_shard = None

        def nanos_to_ms(nanos):
            return round(nanos / 1_000_000, 3)

        for shard in shards:
            shard_query_nanos = 0
            shard_fetch_nanos = 0
            shard_agg_nanos = 0
            shard_rewrite_nanos = 0

            for search in shard.get('searches', []):
                rewrite_nanos = search.get('rewrite_time', 0) or 0
                shard_rewrite_nanos += rewrite_nanos

                for query in search.get('query', []):
                    query_nanos = query.get('time_in_nanos', 0) or 0
                    shard_query_nanos += query_nanos
                    query_components.append({
                        'type': query.get('type'),
                        'description': query.get('description'),
                        'time_ms': nanos_to_ms(query_nanos),
                        'shard': shard.get('id'),
                        'index': shard.get('index'),
                    })

                    breakdown = query.get('breakdown', {}) or {}
                    for key, value in breakdown.items():
                        if key.endswith('_count'):
                            continue
                        if isinstance(value, (int, float)):
                            breakdown_totals[key] = breakdown_totals.get(key, 0) + value

                for collector in search.get('collector', []):
                    collector_nanos = collector.get('time_in_nanos', 0) or 0
                    collector_components.append({
                        'name': collector.get('name'),
                        'reason': collector.get('reason'),
                        'time_ms': nanos_to_ms(collector_nanos),
                        'shard': shard.get('id'),
                        'index': shard.get('index'),
                    })

            fetch = shard.get('fetch', {}) or {}
            shard_fetch_nanos += fetch.get('time_in_nanos', 0) or 0

            for agg in shard.get('aggregations', []) or []:
                shard_agg_nanos += agg.get('time_in_nanos', 0) or 0

            shard_total = shard_query_nanos + shard_fetch_nanos + shard_agg_nanos
            shard_summary = {
                'id': shard.get('id'),
                'index': shard.get('index'),
                'node_id': shard.get('node_id'),
                'total_time_ms': nanos_to_ms(shard_total),
                'query_time_ms': nanos_to_ms(shard_query_nanos),
                'fetch_time_ms': nanos_to_ms(shard_fetch_nanos),
                'aggregation_time_ms': nanos_to_ms(shard_agg_nanos),
                'rewrite_time_ms': nanos_to_ms(shard_rewrite_nanos),
            }
            if slowest_shard is None or shard_total > (slowest_shard.get('_total_nanos') or 0):
                slowest_shard = {'_total_nanos': shard_total, **shard_summary}

            total_query_nanos += shard_query_nanos
            total_fetch_nanos += shard_fetch_nanos
            total_agg_nanos += shard_agg_nanos
            total_rewrite_nanos += shard_rewrite_nanos

        top_query_components = sorted(
            query_components,
            key=lambda item: item.get('time_ms', 0),
            reverse=True
        )[:3]
        top_collectors = sorted(
            collector_components,
            key=lambda item: item.get('time_ms', 0),
            reverse=True
        )[:3]
        top_breakdown = sorted(
            [
                {'phase': phase, 'time_ms': nanos_to_ms(nanos)}
                for phase, nanos in breakdown_totals.items()
            ],
            key=lambda item: item['time_ms'],
            reverse=True
        )[:3]

        if slowest_shard:
            slowest_shard.pop('_total_nanos', None)

        return {
            'shard_count': len(shards),
            'total_query_time_ms': nanos_to_ms(total_query_nanos),
            'total_fetch_time_ms': nanos_to_ms(total_fetch_nanos),
            'total_aggregation_time_ms': nanos_to_ms(total_agg_nanos),
            'total_rewrite_time_ms': nanos_to_ms(total_rewrite_nanos),
            'slowest_shard': slowest_shard,
            'top_query_components': top_query_components,
            'top_collectors': top_collectors,
            'top_breakdown_phases': top_breakdown,
        }

    def execute_query(self, path, method, body=None, enable_profile=False):
        """执行 Elasticsearch 查询"""
        start_time = datetime.now()

        # 构造代理请求 URL
        params = {
            'path': path,
            'method': method.upper(),
        }
        url = f"{self.base_url}/elasticsearch/{self.cluster_id}/_proxy"

        # 为非 HEAD/DELETE 请求添加请求体（GET 请求在 ES API 中可以包含 body）
        http_method = method.upper()
        request_body = None
        request_headers = dict(self.headers)
        if http_method not in ['HEAD', 'DELETE']:
            query_body = body
            if enable_profile and self.is_search_request(path):
                query_body = self.build_profile_body(body)
            if isinstance(query_body, str):
                request_body = query_body
            else:
                request_body = json.dumps(query_body)
            if self.is_msearch_request(path):
                request_headers['Content-Type'] = 'application/x-ndjson'

        try:
            if request_body:
                response = requests.post(url, headers=request_headers, params=params, data=request_body)
            else:
                response = requests.post(url, headers=request_headers, params=params)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() * 1000  # 转换为毫秒

            # 获取响应文本，先检查是否为 JSON
            response_text = response.text
            try:
                data = json.loads(response_text)
            except json.JSONDecodeError as e:
                print(f"响应解析失败 ({path}):")
                print(f"响应内容: {response_text}")
                print(f"请求URL: {url}")
                raise Exception("服务器返回非JSON响应")

            # 解析 response_body 获取 took 字段
            took = None
            response_body = None
            profile_summary = None
            if data.get('response_body'):
                try:
                    response_body = json.loads(data['response_body'])
                    took = self.extract_took(path, response_body)
                    if enable_profile:
                        profile_summary = self.extract_profile_summary(response_body.get('profile'))
                except json.JSONDecodeError:
                    # response_body 可能不是 JSON，忽略错误
                    pass

            return {
                'success': response.ok and not data.get('error'),
                'status_code': response.status_code,
                'status_text': response.reason,
                'duration': round(duration, 2),
                'took': took,
                'path': path,
                'method': method,
                'error': data.get('error'),
                'raw_response': data,
                'profile_summary': profile_summary,
            }

        except requests.exceptions.RequestException as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() * 1000
            return {
                'success': False,
                'status_code': 0,
                'status_text': str(e),
                'duration': round(duration, 2),
                'took': None,
                'path': path,
                'method': method,
                'error': str(e),
                'raw_response': None,
                'profile_summary': None,
            }


class TestReporter:
    """测试报告生成器"""

    def __init__(self):
        pass

    def generate_summary(self, results):
        """生成汇总统计信息"""
        total = len(results)
        passed = sum(1 for r in results if r['success'])
        failed = total - passed

        duration_stats = [r['duration'] for r in results if r['success']]
        took_stats = [r['took'] for r in results if r['took'] is not None]

        # 计算总耗时
        total_duration_sum = sum(duration_stats)
        total_took_sum = sum(took_stats)

        # 分别计算原始查询和优化查询的总耗时
        original_duration_sum = sum([r['duration'] for r in results if r['success'] and '原始查询' in r.get('name', '')])
        original_took_sum = sum([r['took'] for r in results if r['took'] is not None and '原始查询' in r.get('name', '')])
        optimized_duration_sum = sum([r['duration'] for r in results if r['success'] and '优化后查询' in r.get('name', '')])
        optimized_took_sum = sum([r['took'] for r in results if r['took'] is not None and '优化后查询' in r.get('name', '')])
        original_took_count = sum([1 for r in results if r['took'] is not None and '原始查询' in r.get('name', '')])
        optimized_took_count = sum([1 for r in results if r['took'] is not None and '优化后查询' in r.get('name', '')])
        original_avg_took = round(original_took_sum / original_took_count, 2) if original_took_count > 0 else 0
        optimized_avg_took = round(optimized_took_sum / optimized_took_count, 2) if optimized_took_count > 0 else 0

        # 按 query 维度统计（用于 Performance Summary 展示）
        query_groups = {}
        for r in results:
            query_name = r.get('name', 'Unknown Query')
            if query_name not in query_groups:
                query_groups[query_name] = {
                    'duration': [],
                    'took': [],
                    'query_type': r.get('query_type'),
                    'query_index': r.get('query_index'),
                    'base_name': r.get('base_name'),
                }
            if r.get('success'):
                query_groups[query_name]['duration'].append(r.get('duration'))
            if r.get('took') is not None:
                query_groups[query_name]['took'].append(r.get('took'))

        query_stats = []
        def query_sort_key(query_name):
            info = query_groups[query_name]
            query_index = info.get('query_index')
            query_type = info.get('query_type')
            base_name = info.get('base_name')

            # 性能模式：按 query_index 排序，并固定原始查询在前、优化后查询在后
            if isinstance(query_index, int):
                type_order = 0 if query_type == 'original' else 1
                return (0, query_index, type_order, query_name)

            # 回退：按名称推断原始/优化顺序
            inferred_base_name = base_name or query_name.replace(' (原始查询)', '').replace(' (优化后查询)', '')
            inferred_type_order = 0 if '原始查询' in query_name else 1 if '优化后查询' in query_name else 2
            return (1, inferred_base_name, inferred_type_order, query_name)

        for query_name in sorted(query_groups.keys(), key=query_sort_key):
            duration_values = [v for v in query_groups[query_name]['duration'] if v is not None]
            took_values = [v for v in query_groups[query_name]['took'] if v is not None]
            query_stats.append({
                'name': query_name,
                'duration': {
                    'count': len(duration_values),
                    'avg': round(sum(duration_values) / len(duration_values), 2) if duration_values else 0,
                    'min': min(duration_values) if duration_values else 0,
                    'max': max(duration_values) if duration_values else 0,
                },
                'took': {
                    'count': len(took_values),
                    'avg': round(sum(took_values) / len(took_values), 2) if took_values else 0,
                    'min': min(took_values) if took_values else 0,
                    'max': max(took_values) if took_values else 0,
                },
            })

        return {
            'total': total,
            'passed': passed,
            'failed': failed,
            'pass_rate': f"{passed/total*100:.2f}%" if total > 0 else "0.00%",
            'avg_duration': round(sum(duration_stats)/len(duration_stats), 2) if duration_stats else 0,
            'min_duration': min(duration_stats) if duration_stats else 0,
            'max_duration': max(duration_stats) if duration_stats else 0,
            'avg_took': round(sum(took_stats)/len(took_stats), 2) if took_stats else 0,
            'min_took': min(took_stats) if took_stats else 0,
            'max_took': max(took_stats) if took_stats else 0,
            'total_duration_sum': round(total_duration_sum, 2),
            'total_took_sum': round(total_took_sum, 2),
            'original_duration_sum': round(original_duration_sum, 2),
            'original_took_sum': round(original_took_sum, 2),
            'original_took_count': original_took_count,
            'original_avg_took': original_avg_took,
            'optimized_duration_sum': round(optimized_duration_sum, 2),
            'optimized_took_sum': round(optimized_took_sum, 2),
            'optimized_took_count': optimized_took_count,
            'optimized_avg_took': optimized_avg_took,
            'query_stats': query_stats,
        }

    def generate_report(self, results):
        """生成测试报告"""
        summary = self.generate_summary(results)

        return {
            'timestamp': datetime.now().isoformat(),
            'summary': summary,
            'field_descriptions': {
                'duration': "时延 - 从发送请求到收到响应的总耗时（毫秒）",
                'took': "ES查询执行时间 - Elasticsearch 内部查询执行时间（毫秒），仅搜索查询有效"
            },
            'results': results,
        }

    def save_report(self, file_path, report):
        """保存测试报告到文件"""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            print(f"\n详细报告已保存到: {file_path}")
        except Exception as e:
            print(f"保存报告失败: {e}")

    def print_summary(self, summary):
        """打印测试结果汇总"""
        print('=' * 60)
        print('Test Results Summary')
        print('=' * 60)

        print(f"Total: {summary['total']}")
        print(f"Passed: {summary['passed']}")
        print(f"Failed: {summary['failed']}")
        print(f"Pass Rate: {summary['pass_rate']}")

        if summary['passed'] > 0:
            print("\nPerformance Summary:")
            for item in summary.get('query_stats', []):
                print(f"  Query: {item['name']}")
                print(
                    f"    时延(Duration): Avg={item['duration']['avg']}ms, "
                    f"Min={item['duration']['min']}ms, Max={item['duration']['max']}ms, "
                    f"n={item['duration']['count']}"
                )
                print(
                    f"    ES Took: Avg={item['took']['avg']}ms, "
                    f"Min={item['took']['min']}ms, Max={item['took']['max']}ms, "
                    f"n={item['took']['count']}"
                )

        # 打印总耗时（按每次请求的 duration/took 累加，不包含 intervalSeconds 休息间隙）
        total_duration_sum = summary.get('total_duration_sum', 0)
        total_took_sum = summary.get('total_took_sum', 0)
        original_duration_sum = summary.get('original_duration_sum', 0)
        original_took_sum = summary.get('original_took_sum', 0)
        optimized_duration_sum = summary.get('optimized_duration_sum', 0)
        optimized_took_sum = summary.get('optimized_took_sum', 0)

        if total_duration_sum > 0 or total_took_sum > 0:
            print(f"\n总耗时统计:")
            print(f"  原始查询:")
            print(f"    总时延: {original_duration_sum}ms")
            print(f"    总Took: {original_took_sum}ms")
            print(f"  优化后查询:")
            print(f"    总时延: {optimized_duration_sum}ms")
            print(f"    总Took: {optimized_took_sum}ms")

    def print_test_result(self, result):
        """打印单个测试结果"""
        print(f"\n  Testing: {result['name']}")
        print(f"    Path: {result['path']}")
        print(f"    Method: {result['method']}")

        if result['success']:
            print("    ✓ Success")
            print(f"    时延 (Duration): {result['duration']}ms")
            if result['took'] is not None:
                print(f"    ES Took (ES执行时间): {result['took']}ms")
        else:
            print(f"    ✗ Failed ({result['status_code']} {result['status_text']})")
            print(f"    Error: {result['error']}")


class ESQueryTestRunner:
    """ES 查询测试运行器"""

    def __init__(self, config_path, enable_profile=False):
        self.config_path = config_path
        self.config = self.load_config(config_path)
        self.enable_profile = enable_profile

    def load_config(self, config_path):
        """加载配置文件"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            # 验证配置格式
            if not config.get('baseUrl') or not config.get('auth') or not config.get('testCases'):
                raise Exception("配置文件格式错误：需要包含 baseUrl、auth 和 testCases 字段")

            return config
        except FileNotFoundError:
            raise Exception(f"配置文件不存在: {config_path}")
        except json.JSONDecodeError as e:
            raise Exception(f"配置文件格式错误: {e}")

    def run_tests(self):
        """运行测试用例"""
        print('=' * 60)
        print('ES Query Test Runner (Python)')
        print('=' * 60)

        # 1. 加载配置
        print('\n[1/4] Loading configuration...')
        print(f"✓ Configuration loaded from: {self.config_path}")

        # 2. 登录获取 token
        print('\n[2/4] Login to Infini Console...')
        auth_manager = AuthManager(self.config['baseUrl'])
        token = auth_manager.login(
            self.config['auth']['username'],
            self.config['auth']['password']
        )

        if not token:
            raise Exception("登录失败")

        print("✓ Login successful!")

        # 3. 执行测试用例
        print(f"\n[3/4] Running {len(self.config['testCases'])} test cases...")

        results = []
        profile_records = []
        for test_case in self.config['testCases']:
            print(f"\n  Testing: {test_case['name']}")
            print(f"    Path: {test_case['path']}")
            print(f"    Method: {test_case['method']}")

            # 执行查询
            executor = QueryExecutor(
                self.config['baseUrl'],
                token,
                test_case['clusterId']
            )

            result = executor.execute_query(
                test_case['path'],
                test_case['method'],
                test_case.get('body'),
                enable_profile=self.enable_profile,
            )

            final_result = {
                'name': test_case['name'],
                **result
            }
            results.append(final_result)
            if final_result.get('profile_summary'):
                profile_record = self.build_profile_record(final_result)
                profile_records.append(profile_record)
                self.print_profile_key_info(profile_record)

        # 4. 生成和保存报告
        print('\n[4/4] Generating report...')
        reporter = TestReporter()
        report = reporter.generate_report(results)
        reporter.save_report('test-results.json', report)
        if self.enable_profile:
            self.save_profile_report('search-profile-summary.json', profile_records)

        # 显示汇总信息
        reporter.print_summary(report['summary'])

        # 显示失败的测试
        failed_results = [r for r in results if not r['success']]
        if failed_results:
            print('\nFailed Tests:')
            for result in failed_results:
                reporter.print_test_result(result)

        # 设置退出码
        exit_code = 0 if report['summary']['failed'] == 0 else 1
        return exit_code

    def build_profile_record(self, result):
        """构造 profile 结果记录"""
        return {
            'name': result.get('name'),
            'path': result.get('path'),
            'method': result.get('method'),
            'duration_ms': result.get('duration'),
            'took_ms': result.get('took'),
            'profile_summary': result.get('profile_summary'),
        }

    def print_profile_key_info(self, profile_record):
        """输出 profile 关键指标到控制台"""
        summary = profile_record.get('profile_summary') or {}
        slowest_shard = summary.get('slowest_shard') or {}
        top_component = (summary.get('top_query_components') or [{}])[0]
        top_breakdown = summary.get('top_breakdown_phases') or []
        print("    Profile关键指标:")
        print(
            f"      Query/Fetch/Rewrite: "
            f"{summary.get('total_query_time_ms', 0)}ms / "
            f"{summary.get('total_fetch_time_ms', 0)}ms / "
            f"{summary.get('total_rewrite_time_ms', 0)}ms"
        )
        if slowest_shard:
            print(
                f"      慢分片: index={slowest_shard.get('index')} id={slowest_shard.get('id')} "
                f"total={slowest_shard.get('total_time_ms')}ms"
            )
        if top_component.get('type'):
            print(
                f"      最慢Query组件: {top_component.get('type')} "
                f"{top_component.get('time_ms')}ms"
            )
        if top_breakdown:
            print("      Top3 Breakdown Phases:")
            for idx, item in enumerate(top_breakdown[:3], start=1):
                phase = item.get('phase', 'unknown')
                time_ms = item.get('time_ms', 0)
                print(f"        {idx}) {phase}: {time_ms}ms")

    def save_profile_report(self, file_path, profile_records):
        """保存 profile 关键信息报告"""
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'record_count': len(profile_records),
            'records': profile_records,
        }
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        print(f"\nProfile关键信息已保存到: {file_path}")


def parse_kibana_format_queries(file_path):
    """
    解析 Kibana 格式的查询文件

    文件格式示例:
    # 查询名称1
    GET products/_search
    {
      "query": {
        "match": {
          "name": "手机"
        }
      }
    }

    # 查询名称2
    GET orders/_search
    {"query": {"match_all": {}}}

    Args:
        file_path: 查询文件路径

    Returns:
        list: 查询列表，每个元素包含 name, method, path, body
    """
    queries = []
    current_query = None
    current_body_lines = []
    in_body = False

    def is_msearch_path(path):
        normalized = (path or '').split('?', 1)[0].strip().lower()
        return normalized.endswith('/_msearch') or '/_msearch/' in normalized

    def build_query_body(query, body_lines):
        if not body_lines:
            return {}

        body_text = '\n'.join(body_lines).strip()
        if not body_text:
            return {}

        # _msearch 使用 NDJSON，保留原文并确保以换行结尾
        if is_msearch_path(query.get('path')):
            return body_text if body_text.endswith('\n') else f"{body_text}\n"

        return json.loads(body_text)

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        i = 0
        while i < len(lines):
            line = lines[i].rstrip()

            # 检测 # 开头的行作为查询名称分隔符
            if line.strip().startswith('#'):
                # 保存上一个查询（如果有）
                if current_query:
                    if current_body_lines:
                        try:
                            current_query['body'] = build_query_body(current_query, current_body_lines)
                        except json.JSONDecodeError as e:
                            print(f"警告: 查询 '{current_query['name']}' 的 body JSON 解析失败: {e}")
                            current_query['body'] = {}
                    else:
                        current_query['body'] = {}
                    queries.append(current_query)

                # 开始新的查询 - 保留完整的注释格式
                name = line.strip()
                current_query = {
                    'name': name,
                    'method': 'GET',
                    'path': '',
                    'body': {}
                }
                current_body_lines = []
                in_body = False
                i += 1
                continue

            # 解析方法行（如 GET products/_search）
            if current_query and not in_body and not current_query['path']:
                parts = line.split(None, 1) if line.strip() else []
                if len(parts) >= 2:
                    method = parts[0].upper()
                    path = parts[1].strip()
                    current_query['method'] = method
                    current_query['path'] = path
                    i += 1
                    continue

            # 检测 JSON body 开始
            if line.strip().startswith('{'):
                in_body = True
                current_body_lines.append(line)
                # 继续读取直到找到匹配的 }
                # 使用当前行的括号平衡来初始化计数器
                brace_count = line.count('{') - line.count('}')
                i += 1
                while i < len(lines) and brace_count > 0:
                    next_line = lines[i].rstrip()
                    current_body_lines.append(next_line)
                    brace_count += next_line.count('{') - next_line.count('}')
                    i += 1
                continue

            i += 1

        # 保存最后一个查询（如果有）
        if current_query:
            if current_body_lines:
                try:
                    current_query['body'] = build_query_body(current_query, current_body_lines)
                except json.JSONDecodeError as e:
                    print(f"警告: 查询 '{current_query['name']}' 的 body JSON 解析失败: {e}")
                    current_query['body'] = {}
            else:
                current_query['body'] = {}
            queries.append(current_query)

        return queries

    except FileNotFoundError:
        raise Exception(f"查询文件不存在: {file_path}")
    except Exception as e:
        raise Exception(f"解析查询文件失败: {e}")


class PerformanceTestRunner:
    """ES 查询性能测试运行器"""

    def __init__(self, config_path, enable_profile=False):
        self.config_path = config_path
        self.config = self.load_config(config_path)
        self.enable_profile = enable_profile

    def load_config(self, config_path):
        """加载配置文件"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)

            # 验证配置格式
            required_fields = ['baseUrl', 'auth', 'clusterId']
            for field in required_fields:
                if not config.get(field):
                    raise Exception(f"配置文件格式错误：需要包含 {field} 字段")

            return config
        except FileNotFoundError:
            raise Exception(f"配置文件不存在: {config_path}")
        except json.JSONDecodeError as e:
            raise Exception(f"配置文件格式错误: {e}")

    def load_queries_from_files(self, original_queries_path, optimized_queries_path):
        """从 Kibana 格式文件加载查询"""
        original_queries = parse_kibana_format_queries(original_queries_path)
        optimized_queries = parse_kibana_format_queries(optimized_queries_path)

        return {
            'originalQueries': original_queries,
            'optimizedQueries': optimized_queries
        }

    def calculate_stats(self, values):
        """计算统计值"""
        if not values:
            return {'avg': 0, 'min': 0, 'max': 0}

        valid_values = [v for v in values if v is not None]
        if not valid_values:
            return {'avg': 0, 'min': 0, 'max': 0}

        return {
            'avg': round(sum(valid_values) / len(valid_values), 2),
            'min': min(valid_values),
            'max': max(valid_values)
        }

    def run_single_query_test(self, executor, query, label):
        """运行单个查询的一次测试"""
        result = executor.execute_query(
            query['path'],
            query['method'],
            query.get('body'),
            enable_profile=self.enable_profile,
        )

        return {
            'name': f"{query['name']} ({label})",
            **result
        }

    def run_performance_tests(self):
        """运行性能测试"""
        print('=' * 60)
        print('ES Query Performance Test Runner (Python)')
        print('=' * 60)

        # 创建结果文件夹
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        result_folder = f"test_results_{timestamp}"
        os.makedirs(result_folder, exist_ok=True)
        print(f"✓ 结果将保存到文件夹: {result_folder}")

        # 1. 加载配置
        print('\n[1/5] Loading configuration...')
        print(f"✓ Configuration loaded from: {self.config_path}")

        # 2. 登录获取 token
        print('\n[2/5] Login to Infini Console...')
        auth_manager = AuthManager(self.config['baseUrl'])
        token = auth_manager.login(
            self.config['auth']['username'],
            self.config['auth']['password']
        )

        if not token:
            raise Exception("登录失败")

        print("✓ Login successful!")

        # 3. 加载查询
        print('\n[3/5] Loading queries...')

        # 检查是使用内联查询还是从文件加载
        original_queries = self.config.get('originalQueries', [])
        optimized_queries = self.config.get('optimizedQueries', [])

        original_queries_path = self.config.get('originalQueriesPath')
        optimized_queries_path = self.config.get('optimizedQueriesPath')

        if original_queries_path and optimized_queries_path:
            print(f"✓ Loading queries from files...")
            print(f"  - Original queries: {original_queries_path}")
            print(f"  - Optimized queries: {optimized_queries_path}")
            queries = self.load_queries_from_files(original_queries_path, optimized_queries_path)
            original_queries = queries['originalQueries']
            optimized_queries = queries['optimizedQueries']
        elif original_queries and optimized_queries:
            print("✓ Loading queries from config file...")
        else:
            raise Exception("配置文件中需要包含 originalQueries/optimizedQueries 或 originalQueriesPath/optimizedQueriesPath")

        original_label = self.config.get('originalQueryLabel', '原始查询')
        optimized_label = self.config.get('optimizedQueryLabel', '优化后查询')
        iterations = self.config.get('iterations', 1)
        warmup_iterations = self.config.get('warmupIterations', 0)
        interval_seconds = self.config.get('intervalSeconds', 0)

        if not isinstance(warmup_iterations, int) or warmup_iterations < 0:
            raise Exception("配置文件格式错误：warmupIterations 必须是大于等于 0 的整数")
        if warmup_iterations >= iterations:
            raise Exception("配置文件格式错误：warmupIterations 必须小于 iterations")
        measured_iterations = iterations - warmup_iterations

        print(f"  - {len(original_queries)} original queries")
        print(f"  - {len(optimized_queries)} optimized queries")
        print(f"  - Iterations: {iterations}")
        print(f"  - Warmup Iterations: {warmup_iterations}")
        print(f"  - Measured Iterations: {measured_iterations}")
        print(f"  - Interval: {interval_seconds}s")

        # 4. 执行性能测试
        print(f'\n[4/5] Running performance tests...')
        all_results = []
        profile_records = []

        # 创建查询执行器
        executor = QueryExecutor(
            self.config['baseUrl'],
            token,
            self.config['clusterId']
        )

        # 用于生成图表的数据
        performance_data = {
            'iterations': measured_iterations,
            'original': {'took': [], 'duration': [], 'took_total': [], 'duration_total': [], 'label': original_label},
            'optimized': {'took': [], 'duration': [], 'took_total': [], 'duration_total': [], 'label': optimized_label},
            'query_wise_data': {}  # 为每个查询单独存储数据
        }

        for iteration in range(1, iterations + 1):
            is_warmup_round = iteration <= warmup_iterations
            print(f"\n  Iteration {iteration}/{iterations}")
            if is_warmup_round:
                print("  [WARMUP] 本轮仅预热，不计入最终统计")
            print(f"  开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            iteration_original_results = []
            iteration_optimized_results = []

            # 执行原始查询
            for query_index, query in enumerate(original_queries):
                result = self.run_single_query_test(executor, query, original_label)
                result['iteration'] = iteration
                result['is_warmup'] = is_warmup_round
                result['query_type'] = 'original'
                result['query_index'] = query_index
                result['base_name'] = query.get('name', f"Query {query_index + 1}")
                all_results.append(result)
                iteration_original_results.append(result)
                if result.get('profile_summary'):
                    profile_record = self.build_profile_record(result)
                    profile_records.append(profile_record)
                    self.print_profile_key_info(profile_record)

                # 等待间隔
                if interval_seconds > 0:
                    time.sleep(interval_seconds)

            # 执行优化查询
            for query_index, query in enumerate(optimized_queries):
                result = self.run_single_query_test(executor, query, optimized_label)
                result['iteration'] = iteration
                result['is_warmup'] = is_warmup_round
                result['query_type'] = 'optimized'
                result['query_index'] = query_index
                result['base_name'] = query.get('name', f"Query {query_index + 1}")
                all_results.append(result)
                iteration_optimized_results.append(result)
                if result.get('profile_summary'):
                    profile_record = self.build_profile_record(result)
                    profile_records.append(profile_record)
                    self.print_profile_key_info(profile_record)

                # 等待间隔
                if interval_seconds > 0:
                    time.sleep(interval_seconds)

            # 收集本轮的平均数据用于图表
            original_took = [r['took'] for r in iteration_original_results if r['took'] is not None]
            optimized_took = [r['took'] for r in iteration_optimized_results if r['took'] is not None]
            original_duration = [r['duration'] for r in iteration_original_results if r['success']]
            optimized_duration = [r['duration'] for r in iteration_optimized_results if r['success']]

            if not is_warmup_round:
                measured_iteration = len(performance_data['original']['took']) + 1
                for result in iteration_original_results + iteration_optimized_results:
                    result['measured_iteration'] = measured_iteration

                if original_took:
                    performance_data['original']['took'].append(sum(original_took) / len(original_took))
                    performance_data['original']['took_total'].append(sum(original_took))
                else:
                    performance_data['original']['took'].append(None)
                    performance_data['original']['took_total'].append(None)

                if optimized_took:
                    performance_data['optimized']['took'].append(sum(optimized_took) / len(optimized_took))
                    performance_data['optimized']['took_total'].append(sum(optimized_took))
                else:
                    performance_data['optimized']['took'].append(None)
                    performance_data['optimized']['took_total'].append(None)

                if original_duration:
                    performance_data['original']['duration'].append(sum(original_duration) / len(original_duration))
                    performance_data['original']['duration_total'].append(sum(original_duration))
                else:
                    performance_data['original']['duration'].append(None)
                    performance_data['original']['duration_total'].append(None)

                if optimized_duration:
                    performance_data['optimized']['duration'].append(sum(optimized_duration) / len(optimized_duration))
                    performance_data['optimized']['duration_total'].append(sum(optimized_duration))
                else:
                    performance_data['optimized']['duration'].append(None)
                    performance_data['optimized']['duration_total'].append(None)

                print(f"  本轮平均 Took:")
                print(f"    {original_label}: {performance_data['original']['took'][-1]}ms")
                print(f"    {optimized_label}: {performance_data['optimized']['took'][-1]}ms")

        # 5. 生成和保存报告
        print('\n[5/5] Generating report...')
        reporter = TestReporter()
        measured_results = [r for r in all_results if not r.get('is_warmup')]

        # 生成基本报告
        report = reporter.generate_report(measured_results)
        report['results'] = all_results
        report['summary']['warmup_iterations'] = warmup_iterations
        report['summary']['measured_iterations'] = measured_iterations
        report['summary']['total_iterations'] = iterations

        # 添加性能对比分析
        report['performance_comparison'] = self.generate_performance_comparison(
            measured_results, original_label, optimized_label
        )
        report['performance_series'] = {
            'description': '每轮总耗时序列（用于 performance_chart* 图表，不含热身轮次）',
            'original': {
                'label': original_label,
                'took_total': performance_data['original']['took_total'],
                'duration_total': performance_data['original']['duration_total'],
            },
            'optimized': {
                'label': optimized_label,
                'took_total': performance_data['optimized']['took_total'],
                'duration_total': performance_data['optimized']['duration_total'],
            },
        }

        # 收集每个查询的按迭代数据（用于生成折线图）
        self.collect_query_wise_data(
            measured_results,
            performance_data,
            original_label,
            optimized_label,
            measured_iterations,
            original_queries,
            optimized_queries,
        )

        # 保存 JSON 报告
        report_filename = os.path.join(result_folder, f"performance-test-results-{timestamp}.json")
        reporter.save_report(report_filename, report)
        if self.enable_profile:
            profile_filename = os.path.join(result_folder, f"search-profile-summary-{timestamp}.json")
            self.save_profile_report(profile_filename, profile_records)

        # 生成 CSV 报告
        self.generate_csv_report(all_results, performance_data, result_folder, timestamp)

        # 生成图表
        self.generate_chart(performance_data, result_folder, timestamp)

        # 为每个查询生成单独的折线图
        self.generate_query_wise_charts(performance_data, result_folder, timestamp)

        # 显示汇总信息
        reporter.print_summary(report['summary'])

        # 显示性能对比
        self.print_performance_comparison(report['performance_comparison'])

        # 设置退出码
        exit_code = 0 if report['summary']['failed'] == 0 else 1
        return exit_code

    def build_profile_record(self, result):
        """构造 profile 结果记录"""
        return {
            'name': result.get('name'),
            'path': result.get('path'),
            'method': result.get('method'),
            'iteration': result.get('iteration'),
            'query_type': result.get('query_type'),
            'duration_ms': result.get('duration'),
            'took_ms': result.get('took'),
            'profile_summary': result.get('profile_summary'),
        }

    def print_profile_key_info(self, profile_record):
        """输出 profile 关键指标到控制台"""
        summary = profile_record.get('profile_summary') or {}
        slowest_shard = summary.get('slowest_shard') or {}
        top_component = (summary.get('top_query_components') or [{}])[0]
        top_breakdown = summary.get('top_breakdown_phases') or []
        print("    Profile关键指标:")
        print(
            f"      Query/Fetch/Rewrite: "
            f"{summary.get('total_query_time_ms', 0)}ms / "
            f"{summary.get('total_fetch_time_ms', 0)}ms / "
            f"{summary.get('total_rewrite_time_ms', 0)}ms"
        )
        if slowest_shard:
            print(
                f"      慢分片: index={slowest_shard.get('index')} id={slowest_shard.get('id')} "
                f"total={slowest_shard.get('total_time_ms')}ms"
            )
        if top_component.get('type'):
            print(
                f"      最慢Query组件: {top_component.get('type')} "
                f"{top_component.get('time_ms')}ms"
            )
        if top_breakdown:
            print("      Top3 Breakdown Phases:")
            for idx, item in enumerate(top_breakdown[:3], start=1):
                phase = item.get('phase', 'unknown')
                time_ms = item.get('time_ms', 0)
                print(f"        {idx}) {phase}: {time_ms}ms")

    def save_profile_report(self, file_path, profile_records):
        """保存 profile 关键信息报告"""
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'record_count': len(profile_records),
            'records': profile_records,
        }
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)
        print(f"✓ Profile关键信息已保存: {file_path}")

    def generate_csv_report(self, all_results, performance_data, result_folder, timestamp):
        """生成CSV报告"""
        csv_file = os.path.join(result_folder, f"performance_report_{timestamp}.csv")

        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            # 写入表头
            writer.writerow(['轮次', '是否热身', '查询类型', '查询名称', 'Took (ms)', '端到端用时 (ms)', '状态'])

            # 写入详细数据
            for result in all_results:
                writer.writerow([
                    result.get('iteration', ''),
                    '是' if result.get('is_warmup') else '否',
                    result.get('query_type', ''),
                    result['name'],
                    result.get('took') if result.get('took') is not None else '',
                    result['duration'],
                    '成功' if result['success'] else '失败'
                ])

            # 写入统计信息
            writer.writerow([])
            writer.writerow(['统计摘要（不含热身轮次）'])

            # 原始查询 Took 总耗时统计（每轮总和）
            original_took_stats = self.calculate_stats(performance_data['original']['took_total'])
            writer.writerow([f"{performance_data['original']['label']} - Took总耗时（按轮次）"])
            writer.writerow(['', '平均值', '最小值', '最大值', '总和'])
            original_took_sum = sum([v for v in performance_data['original']['took_total'] if v is not None])
            writer.writerow(['', original_took_stats['avg'], original_took_stats['min'], original_took_stats['max'], original_took_sum])

            # 优化后查询 Took 总耗时统计（每轮总和）
            optimized_took_stats = self.calculate_stats(performance_data['optimized']['took_total'])
            writer.writerow([f"{performance_data['optimized']['label']} - Took总耗时（按轮次）"])
            writer.writerow(['', '平均值', '最小值', '最大值', '总和'])
            optimized_took_sum = sum([v for v in performance_data['optimized']['took_total'] if v is not None])
            writer.writerow(['', optimized_took_stats['avg'], optimized_took_stats['min'], optimized_took_stats['max'], optimized_took_sum])

            # 原始查询 Duration 总耗时统计（每轮总和）
            original_duration_stats = self.calculate_stats(performance_data['original']['duration_total'])
            writer.writerow([f"{performance_data['original']['label']} - 端到端时延总耗时（按轮次）"])
            writer.writerow(['', '平均值', '最小值', '最大值', '总和'])
            original_duration_sum = sum([v for v in performance_data['original']['duration_total'] if v is not None])
            writer.writerow(['', original_duration_stats['avg'], original_duration_stats['min'], original_duration_stats['max'], original_duration_sum])

            # 优化后查询 Duration 总耗时统计（每轮总和）
            optimized_duration_stats = self.calculate_stats(performance_data['optimized']['duration_total'])
            writer.writerow([f"{performance_data['optimized']['label']} - 端到端时延总耗时（按轮次）"])
            writer.writerow(['', '平均值', '最小值', '最大值', '总和'])
            optimized_duration_sum = sum([v for v in performance_data['optimized']['duration_total'] if v is not None])
            writer.writerow(['', optimized_duration_stats['avg'], optimized_duration_stats['min'], optimized_duration_stats['max'], optimized_duration_sum])

        print(f"✓ CSV报告已保存: {csv_file}")

    def generate_chart(self, performance_data, result_folder, timestamp):
        """生成性能对比折线图"""
        x = range(1, len(performance_data['original']['took_total']) + 1)

        # 生成 Took 总耗时对比图（每轮总和）
        plt.figure(figsize=(10, 6))
        plt.plot(x, performance_data['original']['took_total'], marker='o', label=performance_data['original']['label'], linewidth=2)
        plt.plot(x, performance_data['optimized']['took_total'], marker='s', label=performance_data['optimized']['label'], linewidth=2)
        plt.xlabel('运行轮次')
        plt.ylabel('Took 总耗时 (ms)')
        title = self.config.get('title', 'ES查询性能对比')
        plt.title(f'{title}\n(Took 总耗时对比)')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        took_chart_file = os.path.join(result_folder, f"performance_chart_took_{timestamp}.png")
        plt.savefig(took_chart_file, dpi=300)
        plt.close()
        print(f"✓ Took总耗时对比图已保存: {took_chart_file}")

        # 生成端到端时延总耗时对比图（每轮总和）
        plt.figure(figsize=(10, 6))
        plt.plot(x, performance_data['original']['duration_total'], marker='o', label=performance_data['original']['label'], linewidth=2)
        plt.plot(x, performance_data['optimized']['duration_total'], marker='s', label=performance_data['optimized']['label'], linewidth=2)
        plt.xlabel('运行轮次')
        plt.ylabel('端到端时延总耗时 (ms)')
        plt.title('端到端时延总耗时对比')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        duration_chart_file = os.path.join(result_folder, f"performance_chart_duration_{timestamp}.png")
        plt.savefig(duration_chart_file, dpi=300)
        plt.close()
        print(f"✓ 端到端时延总耗时对比图已保存: {duration_chart_file}")

    def collect_query_wise_data(
        self,
        all_results,
        performance_data,
        original_label,
        optimized_label,
        iterations,
        original_queries,
        optimized_queries,
    ):
        """收集每个查询对按迭代的数据（用于生成对比折线图）

        配对规则：按查询在文件中的顺序（索引）配对，第 i 个原始查询 ↔ 第 i 个优化查询。
        """
        max_pairs = max(len(original_queries), len(optimized_queries))
        query_pairs = []

        for i in range(max_pairs):
            original_name = original_queries[i].get('name') if i < len(original_queries) else None
            optimized_name = optimized_queries[i].get('name') if i < len(optimized_queries) else None

            if original_name and optimized_name and original_name != optimized_name:
                display_name = f"{original_name} vs {optimized_name}"
            else:
                display_name = original_name or optimized_name or f"Query {i + 1}"

            query_pairs.append({
                'pair_index': i,
                'display_name': display_name,
                'original_name': original_name,
                'optimized_name': optimized_name,
                'original': {'took': [None] * iterations, 'duration': [None] * iterations},
                'optimized': {'took': [None] * iterations, 'duration': [None] * iterations},
            })

        # 将执行结果按 (query_index, iteration) 写入对应的查询对
        for result in all_results:
            query_index = result.get('query_index')
            iteration = result.get('measured_iteration', result.get('iteration'))
            query_type = result.get('query_type')

            if query_index is None or iteration is None or query_type not in ('original', 'optimized'):
                continue
            if not (0 <= query_index < len(query_pairs)):
                continue
            if not (1 <= iteration <= iterations):
                continue

            pair = query_pairs[query_index]
            iteration_idx = iteration - 1

            pair[query_type]['took'][iteration_idx] = result.get('took')
            pair[query_type]['duration'][iteration_idx] = result['duration'] if result.get('success') else None

        performance_data['query_wise_data'] = query_pairs

    def generate_query_wise_charts(self, performance_data, result_folder, timestamp):
        """为每个查询对生成对比折线图"""
        query_pairs = performance_data.get('query_wise_data', [])

        if not query_pairs:
            return

        print(f"\n  生成 {len(query_pairs)} 个查询对的对比折线图...")

        # 创建查询图表文件夹
        charts_folder = os.path.join(result_folder, "query_charts")
        os.makedirs(charts_folder, exist_ok=True)

        for pair in query_pairs:
            # 为每个查询对生成 took 和 duration 两个对比图
            for metric in ['took', 'duration']:
                plt.figure(figsize=(10, 6))

                x = list(range(1, performance_data['iterations'] + 1))

                if any(val is not None for val in pair['original'][metric]):
                    original_y = [val if val is not None else float('nan') for val in pair['original'][metric]]
                    plt.plot(
                        x,
                        original_y,
                        marker='o',
                        label=performance_data['original']['label'],
                        linewidth=2,
                    )

                if any(val is not None for val in pair['optimized'][metric]):
                    optimized_y = [val if val is not None else float('nan') for val in pair['optimized'][metric]]
                    plt.plot(
                        x,
                        optimized_y,
                        marker='s',
                        label=performance_data['optimized']['label'],
                        linewidth=2,
                    )

                # 设置图表标题和标签
                plt.xlabel('运行轮次')
                if metric == 'took':
                    plt.ylabel('Took 时间 (ms)')
                    title_suffix = 'Took'
                else:
                    plt.ylabel('端到端时延 (ms)')
                    title_suffix = 'Duration'

                display_name = pair['display_name']
                safe_base = display_name.replace('/', '_').replace('\\', '_').replace(':', '_')
                safe_base = ''.join(ch if (ch.isalnum() or ch in '._- ') else '_' for ch in safe_base).strip()
                if not safe_base:
                    safe_base = f"query_{pair.get('pair_index', 0) + 1}"

                plt.title(f'{display_name}\n({title_suffix} 时间对比)')
                plt.legend()
                plt.grid(True, alpha=0.3)
                plt.xlim(1, performance_data['iterations'])
                plt.tight_layout()

                # 保存图表：加上 pair_index，避免同名覆盖
                pair_index = pair.get('pair_index', 0) + 1
                chart_file = os.path.join(
                    charts_folder,
                    f"query_{pair_index:02d}_{safe_base}_{metric}_{timestamp}.png",
                )
                plt.savefig(chart_file, dpi=300)
                plt.close()

        print(f"✓ 所有查询对的对比折线图已保存到: {charts_folder}")

    def generate_performance_comparison(self, results, original_label, optimized_label):
        """生成性能对比分析"""
        comparison = {}

        # 按查询名称分组
        queries_by_name = {}
        for result in results:
            # 提取查询名称（去掉标签后缀）
            name = result['name']
            if f'({original_label})' in name:
                base_name = name.replace(f' ({original_label})', '')
                if base_name not in queries_by_name:
                    queries_by_name[base_name] = {'original': [], 'optimized': []}
                queries_by_name[base_name]['original'].append(result)
            elif f'({optimized_label})' in name:
                base_name = name.replace(f' ({optimized_label})', '')
                if base_name not in queries_by_name:
                    queries_by_name[base_name] = {'original': [], 'optimized': []}
                queries_by_name[base_name]['optimized'].append(result)

        # 计算每个查询的平均查询时间（优先 took，无 took 时回退 duration）和对比
        for name, data in queries_by_name.items():
            if data['original'] and data['optimized']:
                original_took = [r['took'] for r in data['original'] if r.get('took') is not None]
                optimized_took = [r['took'] for r in data['optimized'] if r.get('took') is not None]
                original_duration = [r['duration'] for r in data['original'] if r.get('success')]
                optimized_duration = [r['duration'] for r in data['optimized'] if r.get('success')]

                use_took = bool(original_took and optimized_took)
                original_values = original_took if use_took else original_duration
                optimized_values = optimized_took if use_took else optimized_duration

                if original_values and optimized_values:
                    avg_original = sum(original_values) / len(original_values)
                    avg_optimized = sum(optimized_values) / len(optimized_values)
                    improvement = ((avg_original - avg_optimized) / avg_original) * 100

                    comparison[name] = {
                        'metric': 'took' if use_took else 'duration',
                        'avg_original': round(avg_original, 2),
                        'avg_optimized': round(avg_optimized, 2),
                        'improvement_percent': round(improvement, 2),
                        'is_faster': improvement > 0
                    }

        return comparison

    def print_performance_comparison(self, comparison):
        """打印性能对比结果"""
        if not comparison:
            return

        print('\n' + '=' * 60)
        print('Performance Comparison')
        print('=' * 60)

        for name, data in comparison.items():
            status = "✓" if data['is_faster'] else "✗"
            improvement_text = f"+{data['improvement_percent']}%" if data['is_faster'] else f"{data['improvement_percent']}%"
            metric_text = "平均Took" if data.get('metric') == 'took' else "平均时延"
            print(f"\n  {status} {name}")
            print(f"    指标: {metric_text}")
            print(f"    {self.config.get('originalQueryLabel', '原始查询')}: {data['avg_original']}ms")
            print(f"    {self.config.get('optimizedQueryLabel', '优化后查询')}: {data['avg_optimized']}ms")
            print(f"    Improvement: {improvement_text}")


def show_help():
    """显示使用帮助"""
    help_text = """
ES Query Test Runner - 自动化 ES 查询测试工具 (Python 版本)

用法:
  python es_test_runner.py [配置文件路径]
  python es_test_runner.py --performance [配置文件路径]
  python es_test_runner.py --profile [配置文件路径]
  python es_test_runner.py --performance --profile [配置文件路径]

运行模式:
  --performance     运行性能测试模式（对比原始查询和优化查询的性能）
                    默认配置文件: config/performance-test-config.json
  --profile         对 _search 请求开启 ES Search Profile（自动注入 profile: true）
                    输出关键 profile 指标，并保存 profile 关键信息文件
  无此参数         运行基础测试模式

示例:
  # 基础测试模式
  python es_test_runner.py config/test-cases.json

  # 性能测试模式
  python es_test_runner.py --performance
  python es_test_runner.py --performance config/performance-test-config.json

  # 开启 profile（基础/性能模式均可）
  python es_test_runner.py --profile
  python es_test_runner.py --performance --profile

性能测试配置:
  配置文件 (performance-test-config.json) 中需要包含:
  - baseUrl: ES 集群地址
  - auth: 登录信息 (username, password)
  - clusterId: 集群 ID
  - iterations: 测试次数
  - warmupIterations: 预热轮次（包含在 iterations 内，不计入统计）
  - intervalSeconds: 查询间隔秒数
  - originalQueriesPath: 原始查询文件路径（Kibana 格式）
  - optimizedQueriesPath: 优化查询文件路径（Kibana 格式）
  - 或 originalQueries/optimizedQueries: 内联查询数组

Kibana 格式查询文件示例:
  # 查询名称
  GET products/_search
  {
    "query": {
      "match": {
        "name": "手机"
      }
    }
  }
"""
    print(help_text)


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='ES Query Test Runner', add_help=False)
    parser.add_argument('config_file', nargs='?', default=None,
                       help='配置文件路径')
    parser.add_argument('--performance', action='store_true',
                       help='运行性能测试模式')
    parser.add_argument('--profile', action='store_true',
                       help='为 _search 请求开启 ES Search Profile，并输出关键信息')
    parser.add_argument('-h', '--help', action='store_true',
                       help='显示帮助信息')

    args = parser.parse_args()

    # 显示帮助
    if args.help:
        show_help()
        return 0

    # 根据模式选择配置文件
    if args.performance:
        if args.config_file is None:
            args.config_file = 'config/performance-test-config.json'
        mode = 'performance'
    else:
        if args.config_file is None:
            args.config_file = 'config/test-cases.json'
        mode = 'basic'

    try:
        # 根据模式运行测试
        if mode == 'performance':
            runner = PerformanceTestRunner(args.config_file, enable_profile=args.profile)
            exit_code = runner.run_performance_tests()
        else:
            runner = ESQueryTestRunner(args.config_file, enable_profile=args.profile)
            exit_code = runner.run_tests()
        return exit_code
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
