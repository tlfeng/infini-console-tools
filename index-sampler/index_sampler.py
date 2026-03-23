#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Index Sampler - 索引采样工具

从 INFINI Console 管理的所有 Elasticsearch 集群中：
- 获取所有非系统索引
- 提取每个索引的 mapping
- 提取每个索引的样本文档
- 导出 JSON 和 CSV 格式的总结文档

依赖：Python 3.6+，仅需标准库
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

# 添加父目录到路径以导入 common 模块
sys.path.insert(0, str(Path(__file__).parent.parent))
from common.console_client import ConsoleClient, ConsoleAuthError, ConsoleAPIError
from common.config import add_common_args, load_and_merge_config, get_config_value


class IndexSample:
    """存储单个索引的采样信息"""

    def __init__(self, cluster_id: str, cluster_name: str, index_name: str):
        self.cluster_id = cluster_id
        self.cluster_name = cluster_name
        self.index_name = index_name
        self.mapping: Optional[Dict] = None
        self.sample_docs: List[Dict] = []
        self.doc_count: int = 0

    def to_dict(self) -> Dict:
        return {
            "cluster_id": self.cluster_id,
            "cluster_name": self.cluster_name,
            "index_name": self.index_name,
            "mapping": self.mapping,
            "sample_docs": self.sample_docs,
            "doc_count": self.doc_count,
        }


class ClusterResult:
    """存储单个集群的采样结果"""

    def __init__(self, cluster_id: str, cluster_name: str):
        self.cluster_id = cluster_id
        self.cluster_name = cluster_name
        self.indices_count: int = 0
        self.indices: List[IndexSample] = []

    def to_dict(self) -> Dict:
        return {
            "cluster_id": self.cluster_id,
            "cluster_name": self.cluster_name,
            "indices_count": self.indices_count,
            "indices": [idx.to_dict() for idx in self.indices],
        }


class SamplingReport:
    """完整的采样报告"""

    def __init__(self):
        self.total_clusters: int = 0
        self.results: List[ClusterResult] = []

    def to_dict(self) -> Dict:
        return {
            "total_clusters": self.total_clusters,
            "results": [r.to_dict() for r in self.results],
        }


def sample_cluster(
    client: ConsoleClient,
    cluster: Dict,
    clusters_status: Dict,
    sample_size: int,
    max_indices: int,
    include_system_indices: bool,
) -> Optional[ClusterResult]:
    """对单个集群进行采样"""
    cluster_id = cluster["id"]
    cluster_name = cluster.get("name", "")

    print(f"Processing cluster: {cluster_name} ({cluster_id})")

    # 检查集群健康状态
    cluster_status = clusters_status.get(cluster_id)
    if not cluster_status:
        print(f"  Skipping cluster: {cluster_name} (no status info)")
        return None
    
    if not cluster_status.get("available", False):
        print(f"  Skipping cluster: {cluster_name} (not available)")
        return None

    try:
        indices = client.get_indices(cluster_id)
    except Exception as e:
        print(f"  Error: Failed to get indices: {e}")
        return None

    if not indices:
        print(f"  No indices found or unable to fetch indices")
        return None

    result = ClusterResult(cluster_id, cluster_name)
    processed = 0

    for index_name, index_info in indices.items():
        # 跳过系统索引
        if not include_system_indices and index_name.startswith("."):
            continue

        if max_indices > 0 and processed >= max_indices:
            break

        # 检查索引健康状态
        health = index_info.get("health", "").lower()
        if health == "unavailable":
            print(f"  Skipping index: {index_name} (health: {health})")
            continue

        print(f"  Sampling index: {index_name}")

        sample = IndexSample(cluster_id, cluster_name, index_name)

        # 获取文档数量
        doc_count = index_info.get("docs.count", "0")
        try:
            sample.doc_count = int(doc_count)
        except (ValueError, TypeError):
            sample.doc_count = 0

        # 获取 mapping
        sample.mapping = client.get_index_mapping(cluster_id, index_name)

        # 获取样本文档
        sample.sample_docs = client.search_index(cluster_id, index_name, size=sample_size)

        result.indices.append(sample)
        processed += 1

    result.indices_count = len(result.indices)
    return result


def export_results(report: SamplingReport, output_dir: str):
    """导出采样结果"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # 导出JSON
    json_path = output_path / "index_sampling_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
    print(f"JSON report saved to: {json_path}")

    # 导出CSV
    csv_path = output_path / "index_sampling_report.csv"
    export_csv(report, csv_path)
    print(f"CSV report saved to: {csv_path}")

    # 导出每个索引的详细JSON文件
    details_dir = output_path / "details"
    details_dir.mkdir(exist_ok=True)

    for cluster_result in report.results:
        for idx_sample in cluster_result.indices:
            filename = f"{cluster_result.cluster_id}_{idx_sample.index_name}.json"
            filename = filename.replace("/", "_").replace("\\", "_")
            filepath = details_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(idx_sample.to_dict(), f, ensure_ascii=False, indent=2)

    print(f"Detail files saved to: {details_dir}")


def export_csv(report: SamplingReport, filepath: Path):
    """导出CSV格式的报告"""
    headers = [
        "Cluster ID",
        "Cluster Name",
        "Index Name",
        "Document Count",
        "Sample Docs Count",
        "Has Mapping",
    ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for cluster_result in report.results:
            for idx_sample in cluster_result.indices:
                has_mapping = "Yes" if idx_sample.mapping and len(idx_sample.mapping) > 0 else "No"

                row = [
                    cluster_result.cluster_id,
                    cluster_result.cluster_name,
                    idx_sample.index_name,
                    idx_sample.doc_count,
                    len(idx_sample.sample_docs),
                    has_mapping,
                ]
                writer.writerow(row)


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="Index Sampler - 从 INFINI Console 管理的集群中采样索引信息",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 基础用法
  python index_sampler.py -c http://localhost:9000

  # 使用认证
  python index_sampler.py -c http://localhost:9000 -u admin -p password

  # 使用配置文件
  python index_sampler.py --config config.json

  # 包含系统索引
  python index_sampler.py -c http://localhost:9000 --include-system-indices

  # 包含 Console 系统集群
  python index_sampler.py -c http://localhost:9000 --include-console-cluster

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
        "-s",
        "--sample-size",
        type=int,
        default=2,
        help="每个索引采样的文档数量 (默认: 2)",
    )
    parser.add_argument(
        "-m",
        "--max-indices",
        type=int,
        default=0,
        help="每个集群最大采样索引数 (0=无限制, 默认: 0)",
    )
    parser.add_argument(
        "--include-system-indices",
        action="store_true",
        help="包含系统索引(以 . 开头)",
    )
    parser.add_argument(
        "--include-console-cluster",
        action="store_true",
        help="包含 INFINI Console 系统集群 (默认: 排除)",
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
    
    output = args.output or config.get('output', './index_sampling_output')
    sample_size = args.sample_size or config.get('sampleSize', 2)
    max_indices = args.max_indices or config.get('maxIndices', 0)
    include_system_indices = args.include_system_indices or config.get('includeSystemIndices', False)
    include_console_cluster = args.include_console_cluster or config.get('includeConsoleCluster', False)

    # 创建客户端
    client = ConsoleClient(console_url, username, password, timeout=timeout, verify_ssl=not insecure)

    # 登录获取 token
    if username and password:
        print("Logging in...")
        try:
            if not client.login():
                print("Warning: Login failed, trying to proceed without authentication...")
        except ConsoleAuthError as e:
            print(f"Error: {e}")
            sys.exit(1)

    # 获取所有集群
    print("Fetching clusters...")
    try:
        clusters = client.get_clusters()
    except Exception as e:
        print(f"Error: Failed to get clusters: {e}")
        sys.exit(1)

    # 过滤系统集群（默认排除）
    filtered_clusters = []
    for cluster in clusters:
        cluster_id = cluster.get("id", "")
        cluster_name = cluster.get("name", "")

        if not include_console_cluster and ConsoleClient.is_system_cluster(
            cluster_id, cluster_name
        ):
            print(f"  Skipping Console system cluster: {cluster_name} ({cluster_id})")
            continue
        filtered_clusters.append(cluster)

    print(f"Found {len(filtered_clusters)} clusters (skipped {len(clusters) - len(filtered_clusters)} system clusters)")

    # 获取所有集群状态
    print("Fetching clusters status...")
    try:
        clusters_status = client.get_clusters_status()
    except Exception as e:
        print(f"Error: Failed to get clusters status: {e}")
        sys.exit(1)

    # 创建报告
    report = SamplingReport()
    report.total_clusters = len(filtered_clusters)

    # 遍历每个集群
    for cluster in filtered_clusters:
        result = sample_cluster(
            client,
            cluster,
            clusters_status,
            sample_size,
            max_indices,
            include_system_indices,
        )
        if result:
            report.results.append(result)

    # 导出结果
    print("\nExporting results...")
    export_results(report, output)

    print(f"\nSampling complete. Results exported to: {output}")


if __name__ == "__main__":
    main()
