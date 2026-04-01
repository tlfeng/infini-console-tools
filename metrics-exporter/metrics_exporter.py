#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Metrics Exporter - Console 监控数据导出工具

从 INFINI Console 系统集群导出 ES 集群监控指标数据，供离线分析使用。

特点：
- 使用 scroll API 分批读取，避免内存溢出
- 流式写入文件，每批数据直接落盘
- 支持大数据量导出

导出的数据类型：
- cluster_health: 集群健康指标
- cluster_stats: 集群统计指标
- node_stats: 节点统计指标
- index_stats: 索引统计指标
- shard_stats: 分片统计指标
"""

import argparse
import getpass
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))
from common.console_client import ConsoleClient, ConsoleAuthError, ConsoleAPIError
from common.config import add_common_args, load_and_merge_config, get_config_value


# 监控指标类型定义
METRIC_TYPES = {
    "cluster_health": {
        "name": "集群健康指标",
        "description": "集群级别的健康状态信息，包括节点数、分片状态等",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"cluster_health"',
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.cluster_name"],
    },
    "cluster_stats": {
        "name": "集群统计指标",
        "description": "集群级别的统计信息，包括索引数、文档数、存储大小、JVM内存等",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"cluster_stats"',
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.cluster_name"],
    },
    "node_stats": {
        "name": "节点统计指标",
        "description": "节点级别的详细统计，包括CPU、内存、JVM、磁盘IO、网络、索引操作等",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"node_stats"',
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.node_id", "metadata.labels.node_name"],
    },
    "index_stats": {
        "name": "索引统计指标",
        "description": "索引级别的统计信息，包括文档数、存储大小、查询/索引操作次数和耗时",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"index_stats"',
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.index_name"],
    },
    "shard_stats": {
        "name": "分片统计指标",
        "description": "分片级别的详细统计，包括文档数、存储大小、读写操作等",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"shard_stats"',
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.index_name", "metadata.labels.shard_id"],
    },
}

# 告警相关数据类型
ALERT_TYPES = {
    "alert_rules": {
        "name": "告警规则",
        "description": "配置的告警规则定义",
        "index_pattern": ".infini_alert-rule",
    },
    "alert_messages": {
        "name": "告警消息",
        "description": "告警触发产生的消息记录",
        "index_pattern": ".infini_alert-message",
    },
    "alert_history": {
        "name": "告警历史",
        "description": "告警状态变更的历史记录",
        "index_pattern": ".infini_alert-history",
    },
}

# 默认批次大小
DEFAULT_BATCH_SIZE = 1000


class StreamingJSONWriter:
    """流式 JSON 数组写入器"""

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.file = None
        self.count = 0
        self.first = True

    def __enter__(self):
        self.file = open(self.file_path, "w", encoding="utf-8")
        self.file.write("[\n")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.write("\n]")
            self.file.close()
        return False

    def write_doc(self, doc: Dict):
        """写入单个文档"""
        if not self.first:
            self.file.write(",\n")
        self.first = False
        json.dump(doc, self.file, ensure_ascii=False, indent=2)
        self.count += 1

    def write_batch(self, docs: List[Dict]):
        """写入一批文档"""
        for doc in docs:
            self.write_doc(doc)


class MetricsExporter:
    """监控数据导出器"""

    def __init__(self, client: ConsoleClient, system_cluster_id: str):
        self.client = client
        self.system_cluster_id = system_cluster_id

    def get_system_cluster_id(self) -> Optional[str]:
        """获取系统集群ID"""
        clusters = self.client.get_clusters()
        for cluster in clusters:
            if ConsoleClient.is_system_cluster(cluster["id"], cluster["name"]):
                return cluster["id"]
        return None

    def build_metrics_query(
        self,
        query_filter: str,
        time_range_hours: int,
        cluster_id_filter: str = None,
    ) -> Dict:
        """构建监控指标查询"""
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=time_range_hours)

        must_clauses = [
            {"query_string": {"query": query_filter}},
            {"range": {"timestamp": {"gte": start_time.isoformat(), "lte": now.isoformat()}}},
        ]

        if cluster_id_filter:
            must_clauses.append({"term": {"metadata.labels.cluster_id": cluster_id_filter}})

        return {
            "query": {"bool": {"must": must_clauses}},
            "sort": [{"timestamp": {"order": "desc"}}],
            "_source": True,
        }

    def build_all_docs_query(self) -> Dict:
        """构建查询所有文档的查询"""
        return {
            "query": {"match_all": {}},
            "sort": [
                {"created": {"order": "desc", "unmapped_type": "date"}},
                {"_id": {"order": "asc"}},
            ],
            "_source": True,
        }

    def search_with_scroll(
        self,
        index_pattern: str,
        query: Dict,
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_docs: int = 100000,
    ) -> tuple:
        """
        使用 scroll API 初始化搜索，返回第一批结果和 scroll_id

        Returns:
            (first_batch, scroll_id, total_count)
        """
        query["size"] = batch_size

        try:
            result = self.client.proxy_request(
                self.system_cluster_id,
                "POST",
                f"/{index_pattern}/_search?scroll=2m",
                query,
            )

            hits = result.get("hits", {}).get("hits", [])
            total = result.get("hits", {}).get("total", 0)
            if isinstance(total, dict):
                total = total.get("value", 0)

            scroll_id = result.get("_scroll_id")

            docs = [
                {
                    "_id": hit.get("_id"),
                    "_score": hit.get("_score"),
                    **hit.get("_source", {}),
                }
                for hit in hits
            ]

            return docs, scroll_id, total
        except Exception as e:
            print(f"    查询失败: {e}")
            return [], None, 0

    def scroll_next(self, scroll_id: str, batch_size: int = DEFAULT_BATCH_SIZE) -> List[Dict]:
        """获取下一批 scroll 结果"""
        try:
            result = self.client.proxy_request(
                self.system_cluster_id,
                "POST",
                "/_search/scroll",
                {
                    "scroll": "2m",
                    "scroll_id": scroll_id,
                },
            )

            hits = result.get("hits", {}).get("hits", [])

            docs = [
                {
                    "_id": hit.get("_id"),
                    "_score": hit.get("_score"),
                    **hit.get("_source", {}),
                }
                for hit in hits
            ]

            return docs
        except Exception as e:
            print(f"    Scroll 失败: {e}")
            return []

    def clear_scroll(self, scroll_id: str):
        """清除 scroll 上下文"""
        try:
            self.client.proxy_request(
                self.system_cluster_id,
                "DELETE",
                f"/_search/scroll",
                {"scroll_id": scroll_id},
            )
        except Exception:
            pass

    def export_with_scroll(
        self,
        index_pattern: str,
        query: Dict,
        output_file: str,
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_docs: int = 100000,
        progress_callback=None,
    ) -> int:
        """
        使用 scroll API 流式导出数据

        Returns:
            导出的文档总数
        """
        total_exported = 0

        # 初始化 scroll
        first_batch, scroll_id, total_count = self.search_with_scroll(
            index_pattern, query, batch_size, max_docs
        )

        if not first_batch:
            # 创建空文件
            with StreamingJSONWriter(output_file):
                pass
            return 0

        # 流式写入
        with StreamingJSONWriter(output_file) as writer:
            # 写入第一批
            docs_to_write = first_batch[:max_docs - total_exported] if max_docs else first_batch
            writer.write_batch(docs_to_write)
            total_exported += len(docs_to_write)

            if progress_callback:
                progress_callback(total_exported, total_count)

            # 继续获取后续批次
            while scroll_id and (max_docs == 0 or total_exported < max_docs):
                batch = self.scroll_next(scroll_id, batch_size)

                if not batch:
                    break

                # 限制最大文档数
                if max_docs:
                    remaining = max_docs - total_exported
                    if remaining <= 0:
                        break
                    batch = batch[:remaining]

                writer.write_batch(batch)
                total_exported += len(batch)

                if progress_callback:
                    progress_callback(total_exported, total_count)

                # 检查是否达到限制
                if max_docs and total_exported >= max_docs:
                    break

        # 清理 scroll
        if scroll_id:
            self.clear_scroll(scroll_id)

        return total_exported

    def export_metric_type(
        self,
        metric_type: str,
        config: Dict,
        output_file: str,
        time_range_hours: int,
        max_docs: int,
        cluster_id_filter: str = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> int:
        """导出指定类型的监控指标，流式写入文件"""
        print(f"  导出 {config['name']}...")

        query = self.build_metrics_query(
            config["filter_template"],
            time_range_hours,
            cluster_id_filter,
        )

        def progress_callback(current, total):
            if total > 0:
                percent = min(100, current * 100 // total)
                print(f"\r    已导出 {current:,} / {total:,} ({percent}%)", end="", flush=True)

        count = self.export_with_scroll(
            config["index_pattern"],
            query,
            output_file,
            batch_size,
            max_docs,
            progress_callback,
        )

        print(f"\r    已保存 {count:,} 条记录到 {output_file}    ")
        return count

    def export_alert_type(
        self,
        alert_type: str,
        config: Dict,
        output_file: str,
        max_docs: int,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> int:
        """导出告警相关数据，流式写入文件"""
        print(f"  导出 {config['name']}...")

        query = self.build_all_docs_query()

        def progress_callback(current, total):
            if total > 0:
                percent = min(100, current * 100 // total)
                print(f"\r    已导出 {current:,} / {total:,} ({percent}%)", end="", flush=True)

        count = self.export_with_scroll(
            config["index_pattern"],
            query,
            output_file,
            batch_size,
            max_docs,
            progress_callback,
        )

        print(f"\r    已保存 {count:,} 条记录到 {output_file}    ")
        return count

    def get_available_clusters(self) -> List[Dict]:
        """获取有监控数据的集群列表"""
        query = {
            "size": 0,
            "aggs": {
                "clusters": {
                    "terms": {
                        "field": "metadata.labels.cluster_id",
                        "size": 1000,
                    },
                    "aggs": {
                        "cluster_name": {
                            "terms": {
                                "field": "metadata.labels.cluster_name",
                                "size": 1,
                            }
                        }
                    }
                }
            },
            "query": {
                "bool": {
                    "must": [
                        {"query_string": {"query": 'metadata.name:"cluster_health"'}},
                        {"range": {"timestamp": {"gte": "now-24h"}}},
                    ]
                }
            },
        }

        try:
            result = self.client.proxy_request(
                self.system_cluster_id,
                "POST",
                "/.infini_metrics/_search",
                query,
            )

            buckets = result.get("aggregations", {}).get("clusters", {}).get("buckets", [])
            clusters = []
            for bucket in buckets:
                cluster_id = bucket.get("key")
                name_buckets = bucket.get("cluster_name", {}).get("buckets", [])
                cluster_name = name_buckets[0].get("key") if name_buckets else cluster_id
                clusters.append({
                    "cluster_id": cluster_id,
                    "cluster_name": cluster_name,
                    "doc_count": bucket.get("doc_count", 0),
                })
            return clusters
        except Exception as e:
            print(f"获取集群列表失败: {e}")
            return []

    def export_all(
        self,
        output_dir: str,
        metric_types: List[str] = None,
        alert_types: List[str] = None,
        time_range_hours: int = 24,
        max_docs: int = 100000,
        cluster_id_filter: str = None,
        include_alerts: bool = True,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Dict[str, Any]:
        """导出所有监控数据"""
        os.makedirs(output_dir, exist_ok=True)

        # 如果未指定，导出所有类型
        if metric_types is None:
            metric_types = list(METRIC_TYPES.keys())
        if alert_types is None:
            alert_types = list(ALERT_TYPES.keys())

        export_summary = {
            "export_time": datetime.now().isoformat(),
            "time_range_hours": time_range_hours,
            "max_docs_per_type": max_docs,
            "batch_size": batch_size,
            "cluster_filter": cluster_id_filter,
            "metric_types": {},
            "alert_types": {},
            "clusters_with_data": [],
        }

        # 获取有数据的集群列表
        print("\n正在获取有监控数据的集群列表...")
        clusters = self.get_available_clusters()
        export_summary["clusters_with_data"] = clusters
        print(f"找到 {len(clusters)} 个有监控数据的集群")

        # 导出监控指标
        print("\n正在导出监控指标数据...")
        for metric_type in metric_types:
            if metric_type not in METRIC_TYPES:
                print(f"  警告: 未知的指标类型 {metric_type}，跳过")
                continue

            config = METRIC_TYPES[metric_type]
            output_file = os.path.join(output_dir, f"{metric_type}.json")

            count = self.export_metric_type(
                metric_type, config, output_file,
                time_range_hours, max_docs, cluster_id_filter, batch_size
            )

            export_summary["metric_types"][metric_type] = {
                "name": config["name"],
                "count": count,
                "file": f"{metric_type}.json" if count > 0 else None,
            }

        # 导出告警数据
        if include_alerts:
            print("\n正在导出告警数据...")
            for alert_type in alert_types:
                if alert_type not in ALERT_TYPES:
                    print(f"  警告: 未知的告警类型 {alert_type}，跳过")
                    continue

                config = ALERT_TYPES[alert_type]
                output_file = os.path.join(output_dir, f"{alert_type}.json")

                count = self.export_alert_type(
                    alert_type, config, output_file, max_docs, batch_size
                )

                export_summary["alert_types"][alert_type] = {
                    "name": config["name"],
                    "count": count,
                    "file": f"{alert_type}.json" if count > 0 else None,
                }

        # 保存导出摘要
        summary_file = os.path.join(output_dir, "export_summary.json")
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(export_summary, f, ensure_ascii=False, indent=2)
        print(f"\n导出摘要已保存: {summary_file}")

        return export_summary

    def print_summary(self, summary: Dict[str, Any]):
        """打印导出摘要"""
        print("\n" + "=" * 60)
        print("导出摘要")
        print("=" * 60)
        print(f"导出时间: {summary['export_time']}")
        print(f"时间范围: 最近 {summary['time_range_hours']} 小时")
        print(f"每类型最大文档数: {summary['max_docs_per_type']:,}")
        print(f"批次大小: {summary['batch_size']}")

        if summary.get("cluster_filter"):
            print(f"集群过滤: {summary['cluster_filter']}")

        print(f"\n有监控数据的集群 ({len(summary['clusters_with_data'])} 个):")
        for cluster in summary["clusters_with_data"][:10]:
            print(f"  - {cluster['cluster_name']} ({cluster['cluster_id']}): {cluster['doc_count']:,} 条记录")
        if len(summary["clusters_with_data"]) > 10:
            print(f"  ... 还有 {len(summary['clusters_with_data']) - 10} 个集群")

        print("\n监控指标数据:")
        total_metric_docs = 0
        for metric_type, info in summary["metric_types"].items():
            print(f"  - {info['name']}: {info['count']:,} 条记录")
            total_metric_docs += info["count"]

        print("\n告警数据:")
        total_alert_docs = 0
        for alert_type, info in summary["alert_types"].items():
            print(f"  - {info['name']}: {info['count']:,} 条记录")
            total_alert_docs += info["count"]

        print(f"\n总计: {total_metric_docs + total_alert_docs:,} 条记录")
        print("=" * 60)


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="Metrics Exporter - 从 Console 系统集群导出监控数据（流式处理）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 导出最近24小时的监控数据
  python metrics_exporter.py -c http://localhost:9000 -u admin -p password

  # 导出最近7天的监控数据
  python metrics_exporter.py -c http://localhost:9000 -u admin -p password --time-range 168

  # 只导出特定集群的数据
  python metrics_exporter.py -c http://localhost:9000 -u admin -p password --cluster-id xxx

  # 指定批次大小（控制内存使用）
  python metrics_exporter.py -c http://localhost:9000 -u admin -p password --batch-size 500

  # 使用配置文件
  python metrics_exporter.py --config config.json

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
        "--time-range",
        type=int,
        default=24,
        help="导出时间范围(小时)，默认24小时",
    )
    parser.add_argument(
        "--max-docs",
        type=int,
        default=100000,
        help="每种类型最大导出文档数，默认100000",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="每批次读取的文档数，默认1000。减小此值可降低内存使用",
    )
    parser.add_argument(
        "--cluster-id",
        type=str,
        default=None,
        help="只导出指定集群ID的数据",
    )
    parser.add_argument(
        "--metric-types",
        type=str,
        default=None,
        help="要导出的指标类型，逗号分隔，如: cluster_health,node_stats",
    )
    parser.add_argument(
        "--no-alerts",
        action="store_true",
        help="不导出告警数据",
    )
    parser.add_argument(
        "--list-clusters",
        action="store_true",
        help="只列出有监控数据的集群，不导出数据",
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

    output = args.output or config.get('output') or f"metrics_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    time_range = args.time_range or config.get('timeRangeHours', 24)
    max_docs = args.max_docs or config.get('maxDocs', 100000)
    batch_size = args.batch_size or config.get('batchSize', 1000)
    cluster_id = args.cluster_id or config.get('clusterId')
    include_alerts = not args.no_alerts and not config.get('noAlerts', False)

    # 解析指标类型
    metric_types = None
    if args.metric_types:
        metric_types = [t.strip() for t in args.metric_types.split(",")]

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

    # 获取系统集群ID
    print("正在获取系统集群...")
    exporter = MetricsExporter(client, "")
    system_cluster_id = exporter.get_system_cluster_id()

    if not system_cluster_id:
        print("未找到系统集群，请确保 Console 系统集群已配置")
        sys.exit(1)

    print(f"系统集群ID: {system_cluster_id}")
    exporter.system_cluster_id = system_cluster_id

    # 只列出集群
    if args.list_clusters:
        clusters = exporter.get_available_clusters()
        print("\n有监控数据的集群:")
        for cluster in clusters:
            print(f"  {cluster['cluster_name']} ({cluster['cluster_id']}): {cluster['doc_count']:,} 条记录")
        return

    # 导出数据
    summary = exporter.export_all(
        output_dir=output,
        metric_types=metric_types,
        time_range_hours=time_range,
        max_docs=max_docs,
        cluster_id_filter=cluster_id,
        include_alerts=include_alerts,
        batch_size=batch_size,
    )

    exporter.print_summary(summary)
    print(f"\n数据已导出到: {output}")


if __name__ == "__main__":
    main()
