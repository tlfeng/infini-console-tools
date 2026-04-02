#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Metrics Exporter - Console 监控数据导出工具 (优化版)

从 INFINI Console 系统集群导出 ES 集群监控指标数据，供离线分析使用。

优化特性：
- 可配置的批次大小和 scroll keepalive
- 紧凑 JSON 输出，减少 IO 开销
- 并行导出多种指标类型
- 支持字段筛选，减少传输量
- 流式写入，内存占用低

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
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))
from common.console_client import ConsoleClient, ConsoleAuthError, ConsoleAPIError
from common.config import (
    add_common_args, load_and_merge_config, get_config_value,
    AppConfig, MetricsJobConfig, SamplingConfig, ConfigValidationError
)


# 监控指标类型定义
METRIC_TYPES = {
    "cluster_health": {
        "name": "集群健康指标",
        "description": "集群级别的健康状态信息，包括节点数、分片状态等",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"cluster_health"',
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.cluster_name"],
        "default_batch_size": 5000,  # 数据量小，可用大批次
    },
    "cluster_stats": {
        "name": "集群统计指标",
        "description": "集群级别的统计信息，包括索引数、文档数、存储大小、JVM内存等",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"cluster_stats"',
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.cluster_name"],
        "default_batch_size": 5000,
    },
    "node_stats": {
        "name": "节点统计指标",
        "description": "节点级别的详细统计，包括CPU、内存、JVM、磁盘IO、网络、索引操作等",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"node_stats"',
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.node_id", "metadata.labels.node_name"],
        "default_batch_size": 3000,  # 数据量大，中等批次
    },
    "index_stats": {
        "name": "索引统计指标",
        "description": "索引级别的统计信息，包括文档数、存储大小、查询/索引操作次数和耗时",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"index_stats"',
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.index_name"],
        "default_batch_size": 3000,
    },
    "shard_stats": {
        "name": "分片统计指标",
        "description": "分片级别的详细统计，包括文档数、存储大小、读写操作等",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"shard_stats"',
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.index_name", "metadata.labels.shard_id"],
        "default_batch_size": 2000,  # 数据量最大，较小批次
    },
}

# 告警相关数据类型
ALERT_TYPES = {
    "alert_rules": {
        "name": "告警规则",
        "description": "配置的告警规则定义",
        "index_pattern": ".infini_alert-rule",
        "default_batch_size": 5000,
    },
    "alert_messages": {
        "name": "告警消息",
        "description": "告警触发产生的消息记录",
        "index_pattern": ".infini_alert-message",
        "default_batch_size": 3000,
    },
    "alert_history": {
        "name": "告警历史",
        "description": "告警状态变更的历史记录",
        "index_pattern": ".infini_alert-history",
        "default_batch_size": 3000,
    },
}

# 默认配置
DEFAULT_BATCH_SIZE = 3000
DEFAULT_SCROLL_KEEPALIVE = "5m"  # 增加到 5 分钟，避免大数据量时 scroll context 过期
DEFAULT_PARALLEL_JOBS = 2  # 默认并行导出的指标类型数


class CompactJSONWriter:
    """紧凑 JSON 数组写入器 - 优化版，减少 CPU 和 IO 开销"""

    def __init__(self, file_path: str, buffer_size: int = 100):
        self.file_path = file_path
        self.file = None
        self.count = 0
        self.first = True
        self.buffer: List[str] = []
        self.buffer_size = buffer_size

    def __enter__(self):
        self.file = open(self.file_path, "w", encoding="utf-8")
        self.file.write("[")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 写入缓冲区剩余数据
        if self.buffer:
            self._flush_buffer()
        if self.file:
            self.file.write("\n]")
            self.file.close()
        return False

    def _flush_buffer(self):
        """刷新缓冲区到文件"""
        if not self.buffer:
            return
        content = ",\n".join(self.buffer)
        if not self.first:
            self.file.write(",\n")
        else:
            self.first = False
        self.file.write(content)
        self.buffer.clear()

    def flush(self):
        """将当前缓冲内容刷新到文件，确保批次写入及时可见"""
        if not self.file:
            return
        if self.buffer:
            self._flush_buffer()
        self.file.flush()

    def write_doc(self, doc: Dict):
        """写入单个文档（紧凑格式，无 indent）"""
        self.buffer.append(json.dumps(doc, ensure_ascii=False, separators=(",", ":")))
        self.count += 1
        if len(self.buffer) >= self.buffer_size:
            self._flush_buffer()


class ExportResult:
    """导出结果"""

    def __init__(self, metric_type: str, name: str):
        self.metric_type = metric_type
        self.name = name
        self.count = 0
        self.file_path = ""
        self.error: Optional[str] = None
        self.duration_ms = 0

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "count": self.count,
            "file": self.file_path if self.count > 0 else None,
            "error": self.error,
            "duration_ms": self.duration_ms,
        }


class MetricsExporter:
    """监控数据导出器 - 优化版"""

    def __init__(
        self,
        client: ConsoleClient,
        system_cluster_id: str,
        scroll_keepalive: str = DEFAULT_SCROLL_KEEPALIVE,
        parallel_jobs: int = DEFAULT_PARALLEL_JOBS,
    ):
        self.client = client
        self.system_cluster_id = system_cluster_id
        self.scroll_keepalive = scroll_keepalive
        self.parallel_jobs = parallel_jobs

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
        cluster_ids: List[str] = None,
        source_fields: List[str] = None,
        sampling: SamplingConfig = None,
    ) -> Dict:
        """构建监控指标查询"""
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(hours=time_range_hours)

        must_clauses = [
            {"query_string": {"query": query_filter}},
            {"range": {"timestamp": {"gte": start_time.isoformat(), "lte": now.isoformat()}}},
        ]

        # 单个集群过滤（向后兼容）
        if cluster_id_filter:
            must_clauses.append({"term": {"metadata.labels.cluster_id": cluster_id_filter}})

        # 多集群过滤
        if cluster_ids and len(cluster_ids) > 0:
            if len(cluster_ids) == 1:
                must_clauses.append({"term": {"metadata.labels.cluster_id": cluster_ids[0]}})
            else:
                must_clauses.append({"terms": {"metadata.labels.cluster_id": cluster_ids}})

        query = {
            "query": {"bool": {"must": must_clauses}},
            "sort": [{"timestamp": {"order": "desc"}}],
        }

        # 字段筛选
        if source_fields:
            query["_source"] = source_fields
        else:
            query["_source"] = True

        return query

    def build_all_docs_query(self, source_fields: List[str] = None) -> Dict:
        """构建查询所有文档的查询"""
        query = {
            "query": {"match_all": {}},
            "sort": [
                {"created": {"order": "desc", "unmapped_type": "date"}},
                {"_id": {"order": "asc"}},
            ],
        }
        if source_fields:
            query["_source"] = source_fields
        else:
            query["_source"] = True
        return query

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
                f"/{index_pattern}/_search?scroll={self.scroll_keepalive}",
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
                    **hit.get("_source", {}),
                    "sort": hit.get("sort"),  # 保留排序值用于恢复
                }
                for hit in hits
            ]

            return docs, scroll_id, total
        except Exception as e:
            print(f"    查询失败: {e}")
            return [], None, 0

    def _parse_scroll_response(self, result: Dict[str, Any]) -> Tuple[List[Dict], Optional[str]]:
        """解析 scroll 响应，提取文档和最新的 scroll_id"""
        hits = result.get("hits", {}).get("hits", [])
        next_scroll_id = result.get("_scroll_id")

        docs = [
            {
                "_id": hit.get("_id"),
                **hit.get("_source", {}),
                "sort": hit.get("sort"),
            }
            for hit in hits
        ]

        return docs, next_scroll_id

    def scroll_next(self, scroll_id: str) -> Optional[Tuple[List[Dict], Optional[str]]]:
        """
        获取下一批 scroll 结果和最新的 scroll_id

        Returns:
            (文档列表, 最新 scroll_id)，如果 scroll context 已过期则返回 None
        """
        try:
            result = self.client.proxy_request(
                self.system_cluster_id,
                "POST",
                "/_search/scroll",
                {
                    "scroll": self.scroll_keepalive,
                    "scroll_id": scroll_id,
                },
            )

            return self._parse_scroll_response(result)
        except ConsoleAPIError as e:
            error_msg = str(e)
            # 检查是否是 scroll context 过期
            if "search_context_missing_exception" in error_msg or "No search context found" in error_msg:
                print(f"\n    Scroll context 已过期，尝试重新初始化...")
                return None
            print(f"    Scroll 失败: {e}")
            return [], None
        except Exception as e:
            print(f"    Scroll 失败: {e}")
            return [], None

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
        sampling: SamplingConfig = None,
    ) -> int:
        """
        使用 scroll API 流式导出数据

        支持自动恢复：如果 scroll context 过期，会自动重新初始化查询继续导出。

        Returns:
            导出的文档总数
        """
        total_exported = 0
        scroll_id = None
        last_sort_values = None  # 用于恢复时继续查询

        # 初始化 scroll
        first_batch, scroll_id, total_count = self.search_with_scroll(
            index_pattern, query, batch_size, max_docs
        )

        if not first_batch:
            # 创建空文件
            with CompactJSONWriter(output_file):
                pass
            return 0

        # 应用抽样
        if sampling and sampling.is_sampling():
            first_batch = self._apply_sampling(first_batch, sampling)

        # 流式写入
        with CompactJSONWriter(output_file) as writer:
            # 写入第一批
            docs_to_write = first_batch[:max_docs - total_exported] if max_docs else first_batch
            for doc in docs_to_write:
                # 移除 sort 字段（仅用于恢复）
                doc_copy = {k: v for k, v in doc.items() if k != "sort"}
                writer.write_doc(doc_copy)
            writer.flush()
            total_exported += len(docs_to_write)

            # 记录最后一条记录的排序值（用于恢复）
            if docs_to_write:
                last_sort_values = docs_to_write[-1].get("sort")

            if progress_callback:
                progress_callback(total_exported, total_count)

            # 继续获取后续批次
            while scroll_id and (max_docs == 0 or total_exported < max_docs):
                scroll_result = self.scroll_next(scroll_id)

                # scroll context 过期，尝试恢复
                if scroll_result is None:
                    print(f"\n    Scroll context 过期，正在从位置 {total_exported:,} 恢复...")
                    # 清理旧的 scroll
                    self.clear_scroll(scroll_id)

                    # 使用 search_after 恢复查询
                    batch, scroll_id, _ = self._resume_with_search_after(
                        index_pattern, query, batch_size, last_sort_values
                    )

                    if batch is None:
                        print("    恢复失败，停止导出")
                        break

                    print(f"    恢复成功，继续导出...")

                else:
                    batch, next_scroll_id = scroll_result
                    scroll_id = next_scroll_id or scroll_id

                if not batch:
                    break

                # 应用抽样
                if sampling and sampling.is_sampling():
                    batch = self._apply_sampling(batch, sampling)

                # 限制最大文档数
                if max_docs:
                    remaining = max_docs - total_exported
                    if remaining <= 0:
                        break
                    batch = batch[:remaining]

                for doc in batch:
                    # 移除 sort 字段（仅用于恢复）
                    doc_copy = {k: v for k, v in doc.items() if k != "sort"}
                    writer.write_doc(doc_copy)
                writer.flush()
                total_exported += len(batch)

                # 更新最后排序值
                if batch:
                    last_sort_values = batch[-1].get("sort")

                if progress_callback:
                    progress_callback(total_exported, total_count)

                # 检查是否达到限制
                if max_docs and total_exported >= max_docs:
                    break

        # 清理 scroll
        if scroll_id:
            self.clear_scroll(scroll_id)

        return total_exported

    def _resume_with_search_after(
        self,
        index_pattern: str,
        query: Dict,
        batch_size: int,
        last_sort_values: List = None,
    ) -> tuple:
        """
        使用 search_after 恢复查询（当 scroll context 过期时）

        Returns:
            (first_batch, scroll_id, total_count)
        """
        try:
            # 复制查询并添加 search_after
            resume_query = dict(query)
            resume_query["size"] = batch_size

            if last_sort_values:
                resume_query["search_after"] = last_sort_values

            result = self.client.proxy_request(
                self.system_cluster_id,
                "POST",
                f"/{index_pattern}/_search?scroll={self.scroll_keepalive}",
                resume_query,
            )

            hits = result.get("hits", {}).get("hits", [])
            total = result.get("hits", {}).get("total", 0)
            if isinstance(total, dict):
                total = total.get("value", 0)

            scroll_id = result.get("_scroll_id")

            docs = [
                {
                    "_id": hit.get("_id"),
                    **hit.get("_source", {}),
                    "sort": hit.get("sort"),  # 保留排序值用于后续恢复
                }
                for hit in hits
            ]

            return docs, scroll_id, total
        except Exception as e:
            print(f"    恢复查询失败: {e}")
            return None, None, 0

    def _apply_sampling(self, docs: List[Dict], sampling: SamplingConfig) -> List[Dict]:
        """应用抽样策略"""
        if not docs or not sampling.is_sampling():
            return docs

        # 时间间隔抽样
        if sampling.interval:
            return self._interval_sampling(docs, sampling.interval)

        # 比例抽样
        if sampling.ratio:
            return self._ratio_sampling(docs, sampling.ratio)

        return docs

    def _interval_sampling(self, docs: List[Dict], interval: str) -> List[Dict]:
        """按时间间隔抽样（保留每个时间窗口的第一条记录）"""
        # 解析间隔（如 "1h", "5m", "30s"）
        match = re.match(r'(\d+)([hms])', interval)
        if not match:
            return docs

        value = int(match.group(1))
        unit = match.group(2)

        if unit == 'h':
            delta = timedelta(hours=value)
        elif unit == 'm':
            delta = timedelta(minutes=value)
        else:  # 's'
            delta = timedelta(seconds=value)

        if delta.total_seconds() == 0:
            return docs

        sampled = []
        last_time = None

        for doc in docs:
            ts_str = doc.get('timestamp')
            if not ts_str:
                sampled.append(doc)
                continue

            try:
                # 解析 ISO 格式时间
                if isinstance(ts_str, str):
                    ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                else:
                    sampled.append(doc)
                    continue

                if last_time is None or (last_time - ts) >= delta:
                    sampled.append(doc)
                    last_time = ts
            except Exception:
                sampled.append(doc)

        return sampled

    def _ratio_sampling(self, docs: List[Dict], ratio: float) -> List[Dict]:
        """按比例随机抽样"""
        import random
        return [doc for doc in docs if random.random() < ratio]

    def export_metric_type(
        self,
        metric_type: str,
        config: Dict,
        output_file: str,
        time_range_hours: int,
        max_docs: int,
        cluster_id_filter: str = None,
        cluster_ids: List[str] = None,
        batch_size: int = None,
        source_fields: List[str] = None,
        sampling: SamplingConfig = None,
    ) -> ExportResult:
        """导出指定类型的监控指标"""
        result = ExportResult(metric_type, config["name"])
        start_time = time.time()

        try:
            # 使用配置的批次大小或默认值
            effective_batch_size = batch_size or config.get("default_batch_size", DEFAULT_BATCH_SIZE)

            query = self.build_metrics_query(
                config["filter_template"],
                time_range_hours,
                cluster_id_filter,
                cluster_ids,
                source_fields,
                sampling,
            )

            def progress_callback(current, total):
                if total > 0:
                    percent = min(100, current * 100 // total)
                    print(f"\r    [{metric_type}] 已导出 {current:,} / {total:,} ({percent}%)", end="", flush=True)

            count = self.export_with_scroll(
                config["index_pattern"],
                query,
                output_file,
                effective_batch_size,
                max_docs,
                progress_callback,
                sampling,
            )

            result.count = count
            result.file_path = os.path.basename(output_file)

        except Exception as e:
            result.error = str(e)

        result.duration_ms = int((time.time() - start_time) * 1000)
        return result

    def export_alert_type(
        self,
        alert_type: str,
        config: Dict,
        output_file: str,
        max_docs: int,
        batch_size: int = None,
        source_fields: List[str] = None,
    ) -> ExportResult:
        """导出告警相关数据"""
        result = ExportResult(alert_type, config["name"])
        start_time = time.time()

        try:
            effective_batch_size = batch_size or config.get("default_batch_size", DEFAULT_BATCH_SIZE)
            query = self.build_all_docs_query(source_fields)

            def progress_callback(current, total):
                if total > 0:
                    percent = min(100, current * 100 // total)
                    print(f"\r    [{alert_type}] 已导出 {current:,} / {total:,} ({percent}%)", end="", flush=True)

            count = self.export_with_scroll(
                config["index_pattern"],
                query,
                output_file,
                effective_batch_size,
                max_docs,
                progress_callback,
            )

            result.count = count
            result.file_path = os.path.basename(output_file)

        except Exception as e:
            result.error = str(e)

        result.duration_ms = int((time.time() - start_time) * 1000)
        return result

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
        cluster_ids: List[str] = None,
        include_alerts: bool = True,
        batch_size: int = None,
        source_fields: List[str] = None,
        parallel_jobs: int = None,
        sampling: SamplingConfig = None,
    ) -> Dict[str, Any]:
        """导出所有监控数据（支持并行）"""
        os.makedirs(output_dir, exist_ok=True)

        # 如果未指定，导出所有类型
        if metric_types is None:
            metric_types = list(METRIC_TYPES.keys())
        if alert_types is None:
            alert_types = list(ALERT_TYPES.keys())

        effective_parallel = parallel_jobs or self.parallel_jobs

        export_summary = {
            "export_time": datetime.now().isoformat(),
            "time_range_hours": time_range_hours,
            "max_docs_per_type": max_docs,
            "batch_size": batch_size,
            "scroll_keepalive": self.scroll_keepalive,
            "parallel_jobs": effective_parallel,
            "cluster_filter": cluster_id_filter,
            "cluster_ids": cluster_ids,
            "source_fields": source_fields,
            "sampling": sampling.mode if sampling else "full",
            "metric_types": {},
            "alert_types": {},
            "clusters_with_data": [],
        }

        # 仅在未指定集群筛选时才提前获取集群列表（用于摘要展示）
        # 如果已指定 cluster_ids 或 cluster_id_filter，则跳过此步骤避免额外网络请求
        clusters = []
        if not cluster_ids and not cluster_id_filter:
            print("\n正在获取有监控数据的集群列表...")
            clusters = self.get_available_clusters()
            export_summary["clusters_with_data"] = clusters
            print(f"找到 {len(clusters)} 个有监控数据的集群")
            if not clusters:
                print("警告: 未找到有监控数据的集群，导出可能为空")

        # 导出监控指标（并行）
        print(f"\n正在导出监控指标数据 (并行度: {effective_parallel})...")

        metric_results: List[ExportResult] = []
        valid_metric_types = [t for t in metric_types if t in METRIC_TYPES]

        # 打印无效类型
        for mt in metric_types:
            if mt not in METRIC_TYPES:
                print(f"  警告: 未知的指标类型 {mt}，跳过")

        # 并行导出
        with ThreadPoolExecutor(max_workers=effective_parallel) as executor:
            futures = {}
            for metric_type in valid_metric_types:
                config = METRIC_TYPES[metric_type]
                output_file = os.path.join(output_dir, f"{metric_type}.json")
                future = executor.submit(
                    self.export_metric_type,
                    metric_type,
                    config,
                    output_file,
                    time_range_hours,
                    max_docs,
                    cluster_id_filter,
                    cluster_ids,
                    batch_size,
                    source_fields,
                    sampling,
                )
                futures[future] = metric_type

            for future in as_completed(futures):
                metric_type = futures[future]
                try:
                    result = future.result()
                    metric_results.append(result)
                    if result.error:
                        print(f"\n  [{metric_type}] 导出失败: {result.error}")
                    else:
                        print(f"\n  [{metric_type}] 已保存 {result.count:,} 条记录")
                except Exception as e:
                    print(f"\n  [{metric_type}] 导出异常: {e}")

        # 汇总 metric 结果
        for result in metric_results:
            export_summary["metric_types"][result.metric_type] = result.to_dict()

        # 导出告警数据（并行）
        if include_alerts:
            print(f"\n正在导出告警数据 (并行度: {effective_parallel})...")

            alert_results: List[ExportResult] = []
            valid_alert_types = [t for t in alert_types if t in ALERT_TYPES]

            for at in alert_types:
                if at not in ALERT_TYPES:
                    print(f"  警告: 未知的告警类型 {at}，跳过")

            with ThreadPoolExecutor(max_workers=effective_parallel) as executor:
                futures = {}
                for alert_type in valid_alert_types:
                    config = ALERT_TYPES[alert_type]
                    output_file = os.path.join(output_dir, f"{alert_type}.json")
                    future = executor.submit(
                        self.export_alert_type,
                        alert_type,
                        config,
                        output_file,
                        max_docs,
                        batch_size,
                        source_fields,
                    )
                    futures[future] = alert_type

                for future in as_completed(futures):
                    alert_type = futures[future]
                    try:
                        result = future.result()
                        alert_results.append(result)
                        if result.error:
                            print(f"\n  [{alert_type}] 导出失败: {result.error}")
                        else:
                            print(f"\n  [{alert_type}] 已保存 {result.count:,} 条记录")
                    except Exception as e:
                        print(f"\n  [{alert_type}] 导出异常: {e}")

            # 汇总 alert 结果
            for result in alert_results:
                export_summary["alert_types"][result.metric_type] = result.to_dict()

        # 保存导出摘要
        summary_file = os.path.join(output_dir, "export_summary.json")
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(export_summary, f, ensure_ascii=False, indent=2)
        print(f"\n导出摘要已保存: {summary_file}")

        return export_summary

    def execute_job(self, job: MetricsJobConfig) -> Dict[str, Any]:
        """执行单个导出任务"""
        print(f"\n{'='*60}")
        print(f"执行任务: {job.name}")
        print(f"{'='*60}")

        # 解析集群筛选
        cluster_ids = None
        if job.targets and job.targets.clusters:
            all_clusters = self.get_available_clusters()
            cluster_ids = [
                c['cluster_id'] for c in all_clusters
                if job.targets.clusters.matches(c['cluster_id']) or
                   job.targets.clusters.matches(c['cluster_name'])
            ]

        # 更新执行参数
        self.scroll_keepalive = job.execution.scroll_keepalive
        self.parallel_jobs = job.execution.parallel_metrics

        # 执行导出
        return self.export_all(
            output_dir=job.output.directory,
            metric_types=job.metrics,
            alert_types=job.alert_types if job.include_alerts else [],
            time_range_hours=job.time_range_hours,
            max_docs=job.max_docs,
            cluster_ids=cluster_ids,
            include_alerts=job.include_alerts,
            batch_size=job.execution.batch_size,
            source_fields=job.source_fields,
            parallel_jobs=job.execution.parallel_metrics,
            sampling=job.sampling,
        )

    def print_summary(self, summary: Dict[str, Any]):
        """打印导出摘要"""
        print("\n" + "=" * 60)
        print("导出摘要")
        print("=" * 60)
        print(f"导出时间: {summary['export_time']}")
        print(f"时间范围: 最近 {summary['time_range_hours']} 小时")
        print(f"每类型最大文档数: {summary['max_docs_per_type']:,}")
        print(f"批次大小: {summary.get('batch_size', '自适应')}")
        print(f"Scroll Keepalive: {summary.get('scroll_keepalive', DEFAULT_SCROLL_KEEPALIVE)}")
        print(f"并行度: {summary.get('parallel_jobs', 2)}")

        if summary.get("cluster_filter"):
            print(f"集群过滤: {summary['cluster_filter']}")

        if summary.get("source_fields"):
            print(f"字段筛选: {len(summary['source_fields'])} 个字段")

        print(f"\n有监控数据的集群 ({len(summary['clusters_with_data'])} 个):")
        for cluster in summary["clusters_with_data"][:10]:
            print(f"  - {cluster['cluster_name']} ({cluster['cluster_id']}): {cluster['doc_count']:,} 条记录")
        if len(summary["clusters_with_data"]) > 10:
            print(f"  ... 还有 {len(summary['clusters_with_data']) - 10} 个集群")

        print("\n监控指标数据:")
        total_metric_docs = 0
        total_metric_time = 0
        for metric_type, info in summary["metric_types"].items():
            duration_s = info.get("duration_ms", 0) / 1000
            status = "✓" if not info.get("error") else "✗"
            print(f"  - {status} {info['name']}: {info['count']:,} 条记录 ({duration_s:.1f}s)")
            total_metric_docs += info["count"]
            total_metric_time += info.get("duration_ms", 0)

        print("\n告警数据:")
        total_alert_docs = 0
        total_alert_time = 0
        for alert_type, info in summary["alert_types"].items():
            duration_s = info.get("duration_ms", 0) / 1000
            status = "✓" if not info.get("error") else "✗"
            print(f"  - {status} {info['name']}: {info['count']:,} 条记录 ({duration_s:.1f}s)")
            total_alert_docs += info["count"]
            total_alert_time += info.get("duration_ms", 0)

        total_time_s = (total_metric_time + total_alert_time) / 1000
        print(f"\n总计: {total_metric_docs + total_alert_docs:,} 条记录")
        print(f"总耗时: {total_time_s:.1f}s")
        print("=" * 60)


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="Metrics Exporter - 从 Console 系统集群导出监控数据（优化版）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 导出最近24小时的监控数据
  python metrics_exporter.py -c http://localhost:9000 -u admin -p password

  # 导出最近7天的监控数据，并行度4
  python metrics_exporter.py -c http://localhost:9000 -u admin -p password --time-range 168 --parallel 4

  # 只导出特定集群的数据，使用大批次
  python metrics_exporter.py -c http://localhost:9000 -u admin -p password --cluster-id xxx --batch-size 5000

  # 只导出特定字段，减少数据量
  python metrics_exporter.py -c http://localhost:9000 -u admin -p password --fields timestamp,metadata,.payload.elasticsearch.node_stats.os

  # 使用配置文件执行指定 job
  python metrics_exporter.py --config config.json --job "全量导出-一周"

  # 使用配置文件执行所有启用的 jobs
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
        default=None,
        help="每批次读取的文档数，默认根据指标类型自适应 (2000-5000)",
    )
    parser.add_argument(
        "--scroll-keepalive",
        type=str,
        default="5m",
        help="Scroll 上下文保持时间，默认5m（建议大数据量时使用更长时间）",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=2,
        help="并行导出的指标类型数，默认2",
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
        "--fields",
        type=str,
        default=None,
        help="只导出指定字段，逗号分隔，如: timestamp,metadata.labels.cluster_id",
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
    # 新增 job 参数
    parser.add_argument(
        "--job",
        type=str,
        default=None,
        help="指定要执行的 job 名称（配合 --config 使用）",
    )
    parser.add_argument(
        "--list-jobs",
        action="store_true",
        help="列出配置文件中的所有 jobs",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # 尝试加载新格式配置
    app_config = None
    if args.config and Path(args.config).exists():
        try:
            app_config = AppConfig.load(args.config)
        except ConfigValidationError as e:
            print(f"配置文件错误: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"加载配置文件失败: {e}")
            sys.exit(1)

    # 如果是列出 jobs
    if args.list_jobs:
        if not app_config or not app_config.metrics_exporter:
            print("配置文件中没有定义 metricsExporter.jobs")
            sys.exit(1)
        print("\n可用的导出任务:")
        for job in app_config.metrics_exporter.jobs:
            status = "✓" if job.enabled else "✗"
            sampling_str = f" (抽样: {job.sampling.mode}" + (
                f" {job.sampling.interval}" if job.sampling.interval else ""
            ) + ")" if job.sampling.is_sampling() else " (全量)"
            print(f"  {status} {job.name}{sampling_str}")
            print(f"      指标: {', '.join(job.metrics)}")
            print(f"      时间范围: {job.time_range_hours}h")
        return

    # 从配置或命令行获取连接参数
    if app_config:
        console_url = app_config.global_config.console_url
        username = app_config.global_config.username
        password = app_config.global_config.password
        timeout = app_config.global_config.timeout
        insecure = app_config.global_config.insecure
    else:
        config, _ = load_and_merge_config(args)
        console_url = get_config_value(args.console, config.get('consoleUrl'), 'CONSOLE_URL', 'http://localhost:9000')
        username = get_config_value(args.username, config.get('auth', {}).get('username'), 'CONSOLE_USERNAME', '')
        password = get_config_value(args.password, config.get('auth', {}).get('password'), 'CONSOLE_PASSWORD', '')
        timeout = int(get_config_value(str(args.timeout), str(config.get('timeout')), 'CONSOLE_TIMEOUT', '60'))
        insecure = args.insecure or config.get('insecure', False)

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

    # 如果有 jobs 配置，使用 job 模式
    if app_config and app_config.metrics_exporter:
        jobs_to_run = []

        if args.job:
            # 执行指定的 job
            for job in app_config.metrics_exporter.jobs:
                if job.name == args.job:
                    jobs_to_run.append(job)
                    break
            if not jobs_to_run:
                print(f"未找到名为 '{args.job}' 的 job")
                sys.exit(1)
        else:
            # 执行所有启用的 jobs
            jobs_to_run = [job for job in app_config.metrics_exporter.jobs if job.enabled]

        if not jobs_to_run:
            print("没有启用的导出任务")
            sys.exit(0)

        print(f"\n将执行 {len(jobs_to_run)} 个导出任务")

        for job in jobs_to_run:
            summary = exporter.execute_job(job)
            exporter.print_summary(summary)
            print(f"\n数据已导出到: {job.output.directory}")

        return

    # 回退到传统命令行模式
    config, _ = load_and_merge_config(args)

    output = args.output or config.get('output') or f"metrics_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    time_range = args.time_range or config.get('timeRangeHours', 24)
    max_docs = args.max_docs or config.get('maxDocs', 100000)
    batch_size = args.batch_size or config.get('batchSize')
    scroll_keepalive = args.scroll_keepalive or config.get('scrollKeepalive', DEFAULT_SCROLL_KEEPALIVE)
    parallel = args.parallel or config.get('parallelJobs', 2)
    cluster_id = args.cluster_id or config.get('clusterId')
    include_alerts = not args.no_alerts and not config.get('noAlerts', False)

    # 解析字段筛选
    source_fields = None
    if args.fields:
        source_fields = [f.strip() for f in args.fields.split(",")]

    # 解析指标类型
    metric_types = None
    if args.metric_types:
        metric_types = [t.strip() for t in args.metric_types.split(",")]

    exporter.scroll_keepalive = scroll_keepalive
    exporter.parallel_jobs = parallel

    # 导出数据
    summary = exporter.export_all(
        output_dir=output,
        metric_types=metric_types,
        time_range_hours=time_range,
        max_docs=max_docs,
        cluster_id_filter=cluster_id,
        include_alerts=include_alerts,
        batch_size=batch_size,
        source_fields=source_fields,
        parallel_jobs=parallel,
    )

    exporter.print_summary(summary)
    print(f"\n数据已导出到: {output}")


if __name__ == "__main__":
    main()
