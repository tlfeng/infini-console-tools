#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cluster Report - Console 集群信息收集工具

收集 INFINI Console 中所有 Elasticsearch 集群的基本信息：
- 集群名称、版本、健康状态
- 在线时长、可用性、监控状态
- 节点数、索引数、分片数
- 文档总数、存储空间、JVM 内存

输出 CSV 格式的详细报告和汇总报告
"""

import argparse
import csv
import getpass
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent))
from common.console_client import ConsoleClient, ConsoleAuthError
from common.config import add_common_args, load_and_merge_config, get_config_value


@dataclass
class ClusterInfo:
    """集群信息数据类"""
    cluster_name: str = ""
    display_name: str = ""
    version: str = ""
    health_status: str = ""
    available: bool = False
    monitored: bool = False
    uptime: str = "Unknown"
    nodes_count: int = 0
    indices_count: int = 0
    primary_shards: int = 0
    total_shards: int = 0
    unassigned_shards: int = 0
    documents_count: int = 0
    storage_used_formatted: str = ""
    storage_total_formatted: str = ""
    storage_used_bytes: int = 0
    storage_total_bytes: int = 0
    jvm_used_formatted: str = ""
    jvm_total_formatted: str = ""
    jvm_used_bytes: int = 0
    jvm_total_bytes: int = 0
    jvm_used_percent: float = 0.0
    collection_time: str = ""


class ClusterReporter:
    """Console 集群信息收集器"""

    def __init__(self, client: ConsoleClient):
        self.client = client

    def collect_all_data(self, include_console_cluster: bool = False) -> List[ClusterInfo]:
        """收集所有集群的完整数据"""
        print("正在获取集群列表...")
        clusters = self.client.get_clusters()

        if not clusters:
            print("未找到任何集群")
            return []

        print(f"找到 {len(clusters)} 个集群")

        # 过滤系统集群
        if not include_console_cluster:
            clusters = [
                c for c in clusters
                if not ConsoleClient.is_system_cluster(c["id"], c["name"])
            ]
            print(f"过滤后处理 {len(clusters)} 个非系统集群")

        # 获取所有集群的状态
        print("正在获取集群状态...")
        all_status = self.client.get_clusters_status()

        results = []
        for cluster in clusters:
            cid = cluster["id"]
            status_info = all_status.get(cid, {}) or {}
            health = status_info.get("health") or {}

            # 获取 metrics
            try:
                metrics_data = self.client.get_cluster_metrics(cid)
                summary = metrics_data.get("summary", {})
            except Exception:
                summary = {}

            # 获取在线时长
            uptime_ms = summary.get("uptime", 0)
            uptime_str = ConsoleClient.format_duration(uptime_ms) if uptime_ms else "Unknown"

            # 获取版本
            version = summary.get("version")
            if isinstance(version, list) and len(version) > 0:
                version = version[0]
            elif not version:
                version = cluster["version"]

            # 存储空间
            used_store_bytes = summary.get("used_store_bytes", 0) or 0
            max_store_bytes = summary.get("max_store_bytes", 0) or 0

            # JVM内存
            used_jvm_bytes = summary.get("used_jvm_bytes", 0) or 0
            max_jvm_bytes = summary.get("max_jvm_bytes", 0) or 0

            info = ClusterInfo(
                cluster_name=summary.get("cluster_name") or cluster["name"],
                display_name=cluster["name"],
                version=version,
                health_status=summary.get("status") or health.get("status", "unknown"),
                available=status_info.get("available", False),
                monitored=cluster.get("monitored", False),
                uptime=uptime_str,
                nodes_count=summary.get("nodes_count") or health.get("number_of_nodes", 0),
                indices_count=summary.get("indices_count", 0),
                primary_shards=summary.get("primary_shards") or health.get("active_primary_shards", 0),
                total_shards=summary.get("total_shards") or health.get("active_shards", 0),
                unassigned_shards=summary.get("unassigned_shards") or health.get("unassigned_shards", 0),
                documents_count=summary.get("document_count", 0),
                storage_used_formatted=ConsoleClient.format_bytes(used_store_bytes),
                storage_total_formatted=ConsoleClient.format_bytes(max_store_bytes),
                storage_used_bytes=used_store_bytes,
                storage_total_bytes=max_store_bytes,
                jvm_used_formatted=ConsoleClient.format_bytes(used_jvm_bytes),
                jvm_total_formatted=ConsoleClient.format_bytes(max_jvm_bytes),
                jvm_used_bytes=used_jvm_bytes,
                jvm_total_bytes=max_jvm_bytes,
                jvm_used_percent=round(used_jvm_bytes / max_jvm_bytes * 100, 2) if max_jvm_bytes > 0 else 0.0,
                collection_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            results.append(info)

        return results

    def generate_csv_report(self, data: List[ClusterInfo], output_file: str = None) -> str:
        """生成CSV详细报告"""
        if not data:
            print("没有数据可导出")
            return ""

        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"cluster_report_{timestamp}.csv"

        headers = [
            "Console显示名称", "集群名称", "集群版本", "健康状态", "可用性",
            "在线时长", "节点数", "索引数", "主分片数", "总分片数",
            "未分配分片", "文档数", "存储已用", "存储总量",
            "JVM已用", "JVM总量", "JVM使用率%", "采集时间",
        ]

        with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(headers)

            for item in data:
                row = [
                    item.display_name, item.cluster_name, item.version,
                    item.health_status, "是" if item.available else "否",
                    item.uptime, item.nodes_count, item.indices_count,
                    item.primary_shards, item.total_shards, item.unassigned_shards,
                    item.documents_count, item.storage_used_formatted,
                    item.storage_total_formatted, item.jvm_used_formatted,
                    item.jvm_total_formatted, item.jvm_used_percent,
                    item.collection_time,
                ]
                writer.writerow(row)

        print(f"\n详细报告已保存: {output_file}")
        return output_file

    def generate_summary(self, data: List[ClusterInfo]) -> Dict[str, Any]:
        """生成汇总统计"""
        if not data:
            return {}

        total_clusters = len(data)
        available_clusters = sum(1 for d in data if d.available)
        monitored_clusters = sum(1 for d in data if d.monitored)

        health_counts = {}
        for d in data:
            status = d.health_status
            health_counts[status] = health_counts.get(status, 0) + 1

        return {
            "total_clusters": total_clusters,
            "available_clusters": available_clusters,
            "monitored_clusters": monitored_clusters,
            "health_distribution": health_counts,
            "total_nodes": sum(d.nodes_count for d in data),
            "total_indices": sum(d.indices_count for d in data),
            "total_documents": sum(d.documents_count for d in data),
            "total_storage_used": sum(d.storage_used_bytes for d in data),
            "total_storage_used_formatted": ConsoleClient.format_bytes(
                sum(d.storage_used_bytes for d in data)
            ),
        }

    def print_summary(self, summary: Dict[str, Any]):
        """打印汇总统计"""
        if not summary:
            return

        print("\n" + "=" * 60)
        print("集群汇总统计")
        print("=" * 60)
        print(f"集群总数: {summary['total_clusters']}")
        print(f"可用集群: {summary['available_clusters']}")
        print(f"监控中集群: {summary['monitored_clusters']}")
        print("\n健康状态分布:")
        for status, count in summary['health_distribution'].items():
            print(f"  {status}: {count}")
        print(f"\n总节点数: {summary['total_nodes']}")
        print(f"总索引数: {summary['total_indices']}")
        print(f"总文档数: {summary['total_documents']:,}")
        print(f"\n存储空间: {summary['total_storage_used_formatted']}")
        print("=" * 60)


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="Cluster Report - 收集 INFINI Console 中所有 Elasticsearch 集群信息",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 基础用法
  python cluster_report.py -c http://localhost:9000

  # 使用认证
  python cluster_report.py -c http://localhost:9000 -u admin -p password

  # 使用配置文件
  python cluster_report.py --config config.json

  # 仅显示汇总信息
  python cluster_report.py -c http://localhost:9000 --summary-only

  # 包含 Console 系统集群
  python cluster_report.py -c http://localhost:9000 --include-console-cluster

Environment Variables:
  CONSOLE_URL       Console URL (默认: http://localhost:9000)
  CONSOLE_USERNAME  用户名
  CONSOLE_PASSWORD  密码
  CONSOLE_TIMEOUT   超时时间(秒)
        """,
    )

    # 添加通用参数
    parser = add_common_args(parser)

    # 添加本工具特有参数
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="仅显示汇总统计，不生成 CSV 文件"
    )
    parser.add_argument(
        "--include-console-cluster",
        action="store_true",
        help="包含 INFINI Console 系统集群"
    )

    return parser.parse_args()


def main():
    args = parse_args()
    config, _ = load_and_merge_config(args)

    # 从配置文件、环境变量或命令行参数获取值
    console_url = get_config_value(args.console, config.get('consoleUrl'), 'CONSOLE_URL', 'http://localhost:9000')
    username = get_config_value(args.username, config.get('auth', {}).get('username'), 'CONSOLE_USERNAME', '')
    password = get_config_value(args.password, config.get('auth', {}).get('password'), 'CONSOLE_PASSWORD', '')
    timeout = int(get_config_value(str(args.timeout), str(config.get('timeout')), 'CONSOLE_TIMEOUT', '60'))
    insecure = args.insecure or config.get('insecure', False)
    
    output = args.output or config.get('output')
    summary_only = args.summary_only or config.get('summaryOnly', False)
    include_console_cluster = args.include_console_cluster or config.get('includeConsoleCluster', False)

    # 如果需要认证但未提供密码，提示输入
    if username and not password:
        password = getpass.getpass(f"请输入 {username} 的密码: ")

    print(f"连接到 Console: {console_url}")

    # 创建客户端
    client = ConsoleClient(console_url, username, password, timeout=timeout, verify_ssl=not insecure)

    # 登录
    if username and password:
        print("正在登录...")
        try:
            if not client.login():
                print("登录失败，请检查用户名和密码")
                sys.exit(1)
            print("登录成功")
        except ConsoleAuthError as e:
            print(f"登录失败: {e}")
            sys.exit(1)

    # 收集数据
    reporter = ClusterReporter(client)
    data = reporter.collect_all_data(include_console_cluster)

    if not data:
        print("未收集到任何数据")
        sys.exit(1)

    # 生成汇总
    summary = reporter.generate_summary(data)
    reporter.print_summary(summary)

    if not summary_only:
        # 生成详细报告
        detail_file = reporter.generate_csv_report(data, output)
        if detail_file:
            print(f"\n已生成报告: {detail_file}")


if __name__ == "__main__":
    main()
