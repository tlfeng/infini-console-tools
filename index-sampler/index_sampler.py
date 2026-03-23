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


def main():
    parser = argparse.ArgumentParser(
        description="Index Sampler - 从 INFINI Console 管理的集群中采样索引信息",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python index_sampler.py -c http://localhost:9000

  # With authentication
  python index_sampler.py -c http://localhost:9000 -u admin -p password

  # Include system indices
  python index_sampler.py -c http://localhost:9000 --include-system-indices

  # Include Console system cluster (usually not needed)
  python index_sampler.py -c http://localhost:9000 --include-console-cluster
        """,
    )

    parser.add_argument(
        "-c",
        "--console",
        default="http://localhost:9000",
        help="INFINI Console API URL (default: http://localhost:9000)",
    )
    parser.add_argument("-u", "--username", default="", help="Login username")
    parser.add_argument("-p", "--password", default="", help="Login password")
    parser.add_argument(
        "-o",
        "--output",
        default="./index_sampling_output",
        help="Output directory for results (default: ./index_sampling_output)",
    )
    parser.add_argument(
        "-s",
        "--sample-size",
        type=int,
        default=2,
        help="Number of sample documents per index (default: 2)",
    )
    parser.add_argument(
        "-m",
        "--max-indices",
        type=int,
        default=0,
        help="Maximum indices to sample per cluster (0 = unlimited, default: 0)",
    )
    parser.add_argument(
        "--include-system-indices",
        action="store_true",
        help="Include system indices (starting with .)",
    )
    parser.add_argument(
        "--include-console-cluster",
        action="store_true",
        help="Include INFINI Console system cluster (default: excluded)",
    )

    args = parser.parse_args()

    # 创建客户端
    client = ConsoleClient(args.console, args.username, args.password)

    # 登录获取 token
    if args.username and args.password:
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

        if not args.include_console_cluster and ConsoleClient.is_system_cluster(
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
            args.sample_size,
            args.max_indices,
            args.include_system_indices,
        )
        if result:
            report.results.append(result)

    # 导出结果
    print("\nExporting results...")
    export_results(report, args.output)

    print(f"\nSampling complete. Results exported to: {args.output}")


if __name__ == "__main__":
    main()
