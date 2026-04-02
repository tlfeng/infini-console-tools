#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Metrics Exporter - Console 监控数据导出工具 (优化版)

从 INFINI Console 系统集群导出 ES 集群监控指标数据，供离线分析使用。

优化特性：
- 可配置的批次大小和 scroll keepalive
- JSON Lines 输出格式，支持流式读取
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
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))
from common.console_client import ConsoleClient, ConsoleAuthError, ConsoleAPIError
from common.config import (
    add_common_args, load_and_merge_config, get_config_value,
    AppConfig, MetricsJobConfig, SamplingConfig, SlimConfig, ConfigValidationError
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


class JSONLinesWriter:
    """JSON Lines 写入器 - 每行一个 JSON 对象，支持流式读取"""

    def __init__(self, file_path: str, buffer_size: int = 100):
        self.file_path = file_path
        self.file = None
        self.count = 0
        self.buffer: List[str] = []
        self.buffer_size = buffer_size

    def __enter__(self):
        self.file = open(self.file_path, "w", encoding="utf-8")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 写入缓冲区剩余数据
        if self.buffer:
            self._flush_buffer()
        if self.file:
            self.file.close()
        return False

    def _flush_buffer(self):
        """刷新缓冲区到文件"""
        if not self.buffer:
            return
        # 每行一个 JSON 对象
        content = "\n".join(self.buffer) + "\n"
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
        """写入单个文档（JSON Lines 格式，每行一个紧凑 JSON）"""
        self.buffer.append(json.dumps(doc, ensure_ascii=False, separators=(",", ":")))
        self.count += 1
        if len(self.buffer) >= self.buffer_size:
            self._flush_buffer()


class ShardedJSONLinesWriter:
    """分片 JSON Lines 写入器 - 当文档数超过阈值时自动创建新文件"""

    def __init__(
        self,
        base_path: str,  # 基础路径，如 "output/node_stats_20260402_143052"（不含 .jsonl）
        shard_size: int = 100000,  # 每个分片的最大文档数
        buffer_size: int = 100,
    ):
        self.base_path = base_path
        self.shard_size = shard_size
        self.buffer_size = buffer_size
        self.current_writer: Optional[JSONLinesWriter] = None
        self.current_shard = 0
        self.total_count = 0
        self.shard_counts: List[int] = []  # 每个分片的文档数
        self.file_paths: List[str] = []  # 所有生成的文件路径（完整路径）

    def __enter__(self):
        self._start_new_shard()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_current_shard()
        return False

    def _start_new_shard(self):
        """开始新的分片文件"""
        self._close_current_shard()

        # 文件命名：第一个文件无后缀，后续文件带序号
        if self.current_shard == 0:
            file_path = f"{self.base_path}.jsonl"
        else:
            file_path = f"{self.base_path}_{self.current_shard}.jsonl"

        self.file_paths.append(file_path)
        self.current_writer = JSONLinesWriter(file_path, self.buffer_size)
        self.current_writer.__enter__()
        self.shard_counts.append(0)

    def _close_current_shard(self):
        """关闭当前分片"""
        if self.current_writer:
            self.current_writer.__exit__(None, None, None)
            self.current_writer = None

    def write_doc(self, doc: Dict):
        """写入文档，自动处理分片"""
        # 检查是否需要切换到新分片
        if self.shard_counts[self.current_shard] >= self.shard_size:
            self.current_shard += 1
            self._start_new_shard()

        self.current_writer.write_doc(doc)
        self.shard_counts[self.current_shard] += 1
        self.total_count += 1

    def flush(self):
        """刷新当前分片的缓冲区"""
        if self.current_writer:
            self.current_writer.flush()

    def get_file_paths(self) -> List[str]:
        """获取所有生成的文件路径（仅文件名）"""
        return [os.path.basename(p) for p in self.file_paths]

    def get_shard_info(self) -> List[Dict]:
        """获取分片详细信息"""
        return [
            {"file": os.path.basename(self.file_paths[i]), "count": self.shard_counts[i]}
            for i in range(len(self.file_paths))
        ]


class ExportResult:
    """导出结果"""

    def __init__(self, metric_type: str, name: str):
        self.metric_type = metric_type
        self.name = name
        self.count = 0
        self.file_path = ""  # 向后兼容：单个文件时的路径
        self.file_paths: List[str] = []  # 所有文件路径列表
        self.shard_info: List[Dict] = []  # 分片详情
        self.error: Optional[str] = None
        self.duration_ms = 0

    def to_dict(self) -> Dict:
        result = {
            "name": self.name,
            "count": self.count,
            "error": self.error,
            "duration_ms": self.duration_ms,
        }

        # 多文件情况
        if len(self.file_paths) > 1:
            result["files"] = self.shard_info
            result["sharded"] = True
        # 单文件情况（向后兼容）
        elif self.file_paths:
            result["file"] = self.file_paths[0]
            result["sharded"] = False
        elif self.count == 0:
            result["file"] = None

        return result


class MetricsExporter:
    """监控数据导出器 - 优化版"""

    # 精简模式下要删除的字段
    SLIM_META_FIELDS = {"_id", "agent"}
    SLIM_META_PREFIXES = ("category", "datatype", "name")  # metadata 下的字段
    SLIM_HUMAN_READABLE = {"store", "estimated_size", "limit_size"}

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

    @staticmethod
    def _slim_doc(doc: Dict, slim_config: SlimConfig) -> Dict:
        """
        精简文档，删除不必要的字段

        删除的字段包括：
        1. 和集群排障无关的字段：_id, agent, metadata.category/datatype/name
        2. 冗余的人类可读格式字段：store, estimated_size, limit_size（保留 *_in_bytes）
        """
        if not slim_config or not slim_config.enabled:
            return doc

        result = {}

        for key, value in doc.items():
            # 删除顶层元数据字段
            if slim_config.remove_meta and key in MetricsExporter.SLIM_META_FIELDS:
                continue

            # 处理 metadata 字段
            if key == "metadata" and isinstance(value, dict) and slim_config.remove_meta:
                slimmed_metadata = {}
                for mk, mv in value.items():
                    if mk not in MetricsExporter.SLIM_META_PREFIXES:
                        slimmed_metadata[mk] = mv
                result[key] = slimmed_metadata
            else:
                # 递归处理嵌套字典，删除人类可读格式字段
                result[key] = MetricsExporter._remove_human_readable(value, slim_config)

        return result

    @staticmethod
    def _remove_human_readable(value: Any, slim_config: SlimConfig) -> Any:
        """递归删除人类可读格式字段"""
        if not slim_config.remove_human_readable:
            return value

        if isinstance(value, dict):
            result = {}
            for k, v in value.items():
                # 删除人类可读格式字段（有对应的 *_in_bytes 字段）
                if k in MetricsExporter.SLIM_HUMAN_READABLE:
                    continue
                result[k] = MetricsExporter._remove_human_readable(v, slim_config)
            return result
        elif isinstance(value, list):
            return [MetricsExporter._remove_human_readable(item, slim_config) for item in value]
        else:
            return value

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

        query["_source"] = source_fields if source_fields else True
        return query

    def build_all_docs_query(self, source_fields: List[str] = None) -> Dict:
        """构建查询所有文档的查询"""
        return {
            "query": {"match_all": {}},
            "sort": [
                {"created": {"order": "desc", "unmapped_type": "date"}},
                {"_id": {"order": "asc"}},
            ],
            "_source": source_fields if source_fields else True,
        }
        return query

    def _parse_hits(self, hits: List[Dict]) -> List[Dict]:
        """解析 hits 为统一格式文档"""
        return [
            {"_id": hit.get("_id"), **hit.get("_source", {}), "sort": hit.get("sort")}
            for hit in hits
        ]

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
            return self._parse_hits(hits), scroll_id, total
        except Exception as e:
            print(f"    查询失败: {e}")
            return [], None, 0

    def _parse_scroll_response(self, result: Dict[str, Any]) -> tuple:
        """解析 scroll 响应，提取文档和最新的 scroll_id"""
        hits = result.get("hits", {}).get("hits", [])
        next_scroll_id = result.get("_scroll_id")
        return self._parse_hits(hits), next_scroll_id

    def scroll_next(self, scroll_id: str) -> Optional[tuple]:
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
                "/_search/scroll",
                {"scroll_id": scroll_id},
            )
        except Exception:
            pass

    def _write_docs(self, writer, docs: List[Dict], slim_config: SlimConfig = None) -> None:
        """写入文档列表，移除 sort 字段，可选精简数据"""
        for doc in docs:
            doc_copy = {k: v for k, v in doc.items() if k != "sort"}
            # 应用精简配置
            if slim_config and slim_config.enabled:
                doc_copy = self._slim_doc(doc_copy, slim_config)
            writer.write_doc(doc_copy)

    def export_with_scroll(
        self,
        index_pattern: str,
        query: Dict,
        output_file: str,
        batch_size: int = DEFAULT_BATCH_SIZE,
        shard_size: int = 100000,
        progress_callback=None,
        slim_config: SlimConfig = None,
    ) -> Tuple[int, List[str]]:
        """
        使用 scroll API 流式导出数据（支持自动分片）

        支持自动恢复：如果 scroll context 过期，会自动重新初始化查询继续导出。

        Args:
            output_file: 基础输出路径（不含 .json 后缀）
            shard_size: 每个分片文件的最大文档数，默认 100000
            slim_config: 精简数据配置

        Returns:
            (导出的文档总数, 文件路径列表)
        """
        total_exported = 0
        scroll_id = None
        last_sort_values = None  # 用于恢复时继续查询

        # 初始化 scroll（不限制文档数）
        first_batch, scroll_id, total_count = self.search_with_scroll(
            index_pattern, query, batch_size, 0
        )

        if not first_batch:
            # 创建空文件
            with JSONLinesWriter(f"{output_file}.jsonl"):
                pass
            return 0, [f"{os.path.basename(output_file)}.jsonl"]

        # 使用分片写入器
        with ShardedJSONLinesWriter(output_file, shard_size) as writer:
            # 写入第一批
            self._write_docs(writer, first_batch, slim_config)
            writer.flush()
            total_exported += len(first_batch)

            # 记录最后一条记录的排序值（用于恢复）
            if first_batch:
                last_sort_values = first_batch[-1].get("sort")

            if progress_callback:
                progress_callback(total_exported, total_count)

            # 继续获取后续批次
            while scroll_id:
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

                self._write_docs(writer, batch, slim_config)
                writer.flush()
                total_exported += len(batch)

                # 更新最后排序值
                if batch:
                    last_sort_values = batch[-1].get("sort")

                if progress_callback:
                    progress_callback(total_exported, total_count)

        # 清理 scroll
        if scroll_id:
            self.clear_scroll(scroll_id)

        return total_exported, writer.get_file_paths()

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
            return self._parse_hits(hits), scroll_id, total
        except Exception as e:
            print(f"    恢复查询失败: {e}")
            return None, None, 0

    def _should_use_es_sampling(self, sampling: SamplingConfig) -> bool:
        """sampling 模式统一使用 ES 端抽样"""
        return bool(sampling and sampling.is_sampling())

    def _get_sampling_group_fields(self, metric_type: str, config: Dict[str, Any]) -> List[str]:
        """分层抽样分组字段，优先使用指标定义的 key_fields"""
        return config.get("key_fields") or ["metadata.labels.cluster_id"]

    def export_with_es_sampling(
        self,
        index_pattern: str,
        query: Dict,
        output_file: str,
        sampling: SamplingConfig,
        batch_size: int,
        group_fields: List[str],
        shard_size: int = 100000,
        progress_callback=None,
        source_fields: List[str] = None,
        slim_config: SlimConfig = None,
    ) -> Tuple[int, List[str]]:
        """
        ES 端抽样：时间桶 + 维度分层（top_hits）
        每个 (维度组合, 时间桶) 只保留最新的一条记录

        Args:
            output_file: 基础输出路径（不含 .json 后缀）
            shard_size: 每个分片文件的最大文档数，默认 100000
            slim_config: 精简数据配置

        Returns:
            (导出的文档总数, 文件路径列表)
        """
        sampling_interval = sampling.interval
        if not sampling_interval:
            return self.export_with_scroll(
                index_pattern,
                query,
                output_file,
                batch_size,
                shard_size,
                progress_callback,
                slim_config,
            )

        total_exported = 0
        after_key = None
        composite_page_size = 1000

        sources = []
        for i, field in enumerate(group_fields):
            sources.append({f"group_{i}": {"terms": {"field": field}}})
        sources.append(
            {
                "time_bucket": {
                    "date_histogram": {
                        "field": "timestamp",
                        "fixed_interval": sampling_interval,
                    }
                }
            }
        )

        with ShardedJSONLinesWriter(output_file, shard_size) as writer:
            while True:
                body = {
                    "size": 0,
                    "track_total_hits": False,
                    "query": query.get("query", {"match_all": {}}),
                    "aggs": {
                        "sampled": {
                            "composite": {
                                "size": composite_page_size,
                                "sources": sources,
                            },
                            "aggs": {
                                "latest": {
                                    "top_hits": {
                                        "size": 1,
                                        "sort": [{"timestamp": {"order": "desc"}}],
                                        "_source": source_fields if source_fields else True,
                                    }
                                }
                            },
                        }
                    },
                }
                if after_key:
                    body["aggs"]["sampled"]["composite"]["after"] = after_key

                result = self.client.proxy_request(
                    self.system_cluster_id,
                    "POST",
                    f"/{index_pattern}/_search",
                    body,
                )

                sampled = result.get("aggregations", {}).get("sampled", {})
                buckets = sampled.get("buckets", [])
                if not buckets:
                    break

                for bucket in buckets:
                    hits = bucket.get("latest", {}).get("hits", {}).get("hits", [])
                    if not hits:
                        continue
                    hit = hits[0]
                    doc = {"_id": hit.get("_id"), **hit.get("_source", {})}
                    # 应用精简配置
                    if slim_config and slim_config.enabled:
                        doc = self._slim_doc(doc, slim_config)
                    writer.write_doc(doc)
                    total_exported += 1

                writer.flush()
                if progress_callback:
                    progress_callback(total_exported, 0)

                after_key = sampled.get("after_key")
                if not after_key:
                    break

        return total_exported, writer.get_file_paths()

    def estimate_export_count(
        self,
        metric_type: str,
        config: Dict,
        time_range_hours: int,
        cluster_id_filter: str = None,
        cluster_ids: List[str] = None,
        sampling: SamplingConfig = None,
    ) -> tuple:
        """
        估计要导出的数据条数

        Returns:
            (原始文档数, 抽样后数据量) 元组
            如果预估失败，返回 (-1, -1)
            如果无抽样，两个值相同
        """
        query = self.build_metrics_query(
            config["filter_template"],
            time_range_hours,
            cluster_id_filter,
            cluster_ids,
            None,
        )

        try:
            # 先获取原始文档数
            count_body = {"query": query.get("query", {"match_all": {}})}
            count_result = self.client.proxy_request(
                self.system_cluster_id,
                "POST",
                f"/{config['index_pattern']}/_count",
                count_body,
            )
            total_docs = count_result.get("count", 0)

            # interval 抽样时，使用聚合预估实际写入的数据量
            if sampling and sampling.interval:
                group_fields = self._get_sampling_group_fields(metric_type, config)
                sources = []
                for i, field in enumerate(group_fields):
                    sources.append({f"group_{i}": {"terms": {"field": field}}})
                sources.append(
                    {
                        "time_bucket": {
                            "date_histogram": {
                                "field": "timestamp",
                                "fixed_interval": sampling.interval,
                            }
                        }
                    }
                )

                # 使用 composite aggregation 遍历所有 bucket 来计算总数
                total_buckets = 0
                after_key = None
                composite_page_size = 1000

                while True:
                    body = {
                        "size": 0,
                        "track_total_hits": False,
                        "query": query.get("query", {"match_all": {}}),
                        "aggs": {
                            "sampled": {
                                "composite": {
                                    "size": composite_page_size,
                                    "sources": sources,
                                }
                            }
                        },
                    }
                    if after_key:
                        body["aggs"]["sampled"]["composite"]["after"] = after_key

                    result = self.client.proxy_request(
                        self.system_cluster_id,
                        "POST",
                        f"/{config['index_pattern']}/_search",
                        body,
                    )

                    buckets = result.get("aggregations", {}).get("sampled", {}).get("buckets", [])
                    total_buckets += len(buckets)

                    after_key = result.get("aggregations", {}).get("sampled", {}).get("after_key")
                    if not after_key or not buckets:
                        break

                return (total_docs, total_buckets)
            else:
                # 无抽样时，两个值相同
                return (total_docs, total_docs)
        except Exception as e:
            print(f"    预估数据量失败: {e}")
            return (-1, -1)

    def _make_progress_callback(self, metric_type: str):
        """创建进度回调函数"""
        def callback(current, total):
            if total > 0:
                percent = min(100, current * 100 // total)
                print(f"\r    [{metric_type}] 已导出 {current:,} / {total:,} ({percent}%)", end="", flush=True)
            else:
                print(f"\r    [{metric_type}] 已导出 {current:,}", end="", flush=True)
        return callback

    def export_metric_type(
        self,
        metric_type: str,
        config: Dict,
        output_file: str,
        time_range_hours: int,
        shard_size: int = 100000,
        cluster_id_filter: str = None,
        cluster_ids: List[str] = None,
        batch_size: int = None,
        source_fields: List[str] = None,
        sampling: SamplingConfig = None,
        slim_config: SlimConfig = None,
    ) -> ExportResult:
        """导出指定类型的监控指标（支持自动分片）

        Args:
            output_file: 基础输出路径（不含 .json 后缀）
            shard_size: 每个分片文件的最大文档数，默认 100000
            slim_config: 精简数据配置
        """
        result = ExportResult(metric_type, config["name"])
        start_time = time.time()

        try:
            # 使用配置的批次大小或默认值
            effective_batch_size = batch_size or config.get("default_batch_size", DEFAULT_BATCH_SIZE)

            # 首先预估数据量
            total_docs, sampled_docs = self.estimate_export_count(
                metric_type, config, time_range_hours, cluster_id_filter, cluster_ids, sampling
            )
            if total_docs >= 0:
                # 判断是否有抽样
                if sampling and sampling.interval and total_docs != sampled_docs:
                    print(f"    原始数据量: {total_docs:,} 条，抽样后: {sampled_docs:,} 条")
                else:
                    print(f"    预估需要导出约 {total_docs:,} 条记录")
                if sampled_docs > shard_size:
                    print(f"    将自动分文件存储 (每文件最多 {shard_size:,} 条)")

            query = self.build_metrics_query(
                config["filter_template"],
                time_range_hours,
                cluster_id_filter,
                cluster_ids,
                source_fields,
            )

            progress_callback = self._make_progress_callback(metric_type)

            if self._should_use_es_sampling(sampling):
                count, file_paths = self.export_with_es_sampling(
                    config["index_pattern"],
                    query,
                    output_file,
                    sampling,
                    effective_batch_size,
                    self._get_sampling_group_fields(metric_type, config),
                    shard_size,
                    progress_callback,
                    source_fields,
                    slim_config,
                )
            else:
                count, file_paths = self.export_with_scroll(
                    config["index_pattern"],
                    query,
                    output_file,
                    effective_batch_size,
                    shard_size,
                    progress_callback,
                    slim_config,
                )

            result.count = count
            result.file_paths = file_paths
            if file_paths:
                result.file_path = file_paths[0]  # 向后兼容
                if len(file_paths) > 1:
                    result.shard_info = [
                        {"file": f, "count": shard_size if i < len(file_paths) - 1 else count % shard_size or shard_size}
                        for i, f in enumerate(file_paths)
                    ]

        except Exception as e:
            result.error = str(e)

        result.duration_ms = int((time.time() - start_time) * 1000)
        return result

    def export_alert_type(
        self,
        alert_type: str,
        config: Dict,
        output_file: str,
        shard_size: int = 100000,
        batch_size: int = None,
        source_fields: List[str] = None,
        slim_config: SlimConfig = None,
    ) -> ExportResult:
        """导出告警相关数据（支持自动分片）

        Args:
            output_file: 基础输出路径（不含 .json 后缀）
            shard_size: 每个分片文件的最大文档数，默认 100000
            slim_config: 精简数据配置
        """
        result = ExportResult(alert_type, config["name"])
        start_time = time.time()

        try:
            effective_batch_size = batch_size or config.get("default_batch_size", DEFAULT_BATCH_SIZE)
            query = self.build_all_docs_query(source_fields)

            count, file_paths = self.export_with_scroll(
                config["index_pattern"],
                query,
                output_file,
                effective_batch_size,
                shard_size,
                self._make_progress_callback(alert_type),
                slim_config,
            )

            result.count = count
            result.file_paths = file_paths
            if file_paths:
                result.file_path = file_paths[0]  # 向后兼容
                if len(file_paths) > 1:
                    result.shard_info = [
                        {"file": f, "count": shard_size if i < len(file_paths) - 1 else count % shard_size or shard_size}
                        for i, f in enumerate(file_paths)
                    ]

        except Exception as e:
            result.error = str(e)

        result.duration_ms = int((time.time() - start_time) * 1000)
        return result

    def get_available_clusters(self, time_range_hours: int = 24) -> List[Dict]:
        """获取有监控数据的集群列表

        Args:
            time_range_hours: 查询时间范围（小时），默认 24 小时
        """
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
                        {"range": {"timestamp": {"gte": f"now-{time_range_hours}h"}}},
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
        shard_size: int = 100000,
        cluster_id_filter: str = None,
        cluster_ids: List[str] = None,
        include_alerts: bool = True,
        batch_size: int = None,
        source_fields: List[str] = None,
        parallel_jobs: int = None,
        sampling: SamplingConfig = None,
        slim_config: SlimConfig = None,
    ) -> Dict[str, Any]:
        """导出所有监控数据（支持并行和自动分片）

        Args:
            shard_size: 每个分片文件的最大文档数，默认 100000
            slim_config: 精简数据配置
        """
        # 如果 cluster_ids 是空列表（不是 None），说明指定了集群但匹配不到
        # 此时应该返回空结果，而不是导出所有集群的数据
        if cluster_ids is not None and len(cluster_ids) == 0:
            print("警告: 指定的集群过滤条件未匹配到任何集群，跳过导出")
            os.makedirs(output_dir, exist_ok=True)
            timestamp_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
            export_summary = {
                "export_time": datetime.now().isoformat(),
                "time_range_hours": time_range_hours,
                "cluster_ids": [],
                "metric_types": {},
                "alert_types": {},
                "clusters_with_data": [],
                "skipped": True,
                "skip_reason": "指定的集群过滤条件未匹配到任何集群",
            }
            summary_file = os.path.join(output_dir, f"export_summary_{timestamp_suffix}.json")
            with open(summary_file, "w", encoding="utf-8") as f:
                json.dump(export_summary, f, ensure_ascii=False, indent=2)
            return export_summary

        os.makedirs(output_dir, exist_ok=True)

        # 生成时间后缀，用于所有输出文件
        timestamp_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 如果未指定，导出所有类型
        if metric_types is None:
            metric_types = list(METRIC_TYPES.keys())
        if alert_types is None:
            alert_types = list(ALERT_TYPES.keys())

        effective_parallel = parallel_jobs or self.parallel_jobs

        export_summary = {
            "export_time": datetime.now().isoformat(),
            "time_range_hours": time_range_hours,
            "shard_size": shard_size,
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
            clusters = self.get_available_clusters(time_range_hours)
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
                # 基础路径，不含扩展名（由 ShardedJSONLinesWriter 添加）
                output_file_base = os.path.join(output_dir, f"{metric_type}_{timestamp_suffix}")
                future = executor.submit(
                    self.export_metric_type,
                    metric_type,
                    config,
                    output_file_base,
                    time_range_hours,
                    shard_size,
                    cluster_id_filter,
                    cluster_ids,
                    batch_size,
                    source_fields,
                    sampling,
                    slim_config,
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
                    # 基础路径，不含扩展名
                    output_file_base = os.path.join(output_dir, f"{alert_type}_{timestamp_suffix}")
                    future = executor.submit(
                        self.export_alert_type,
                        alert_type,
                        config,
                        output_file_base,
                        shard_size,
                        batch_size,
                        source_fields,
                        slim_config,
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
        summary_file = os.path.join(output_dir, f"export_summary_{timestamp_suffix}.json")
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
        cluster_filter_specified = False  # 标记用户是否指定了集群过滤
        if job.targets and job.targets.clusters:
            cluster_filter_specified = True
            all_clusters = self.get_available_clusters(job.time_range_hours)
            cluster_ids = [
                c['cluster_id'] for c in all_clusters
                if job.targets.clusters.matches(c['cluster_id']) or
                   job.targets.clusters.matches(c['cluster_name'])
            ]

            # 如果指定了集群但匹配不到，打印警告
            if not cluster_ids:
                included = job.targets.clusters.include
                print(f"警告: 在最近 {job.time_range_hours} 小时内未找到匹配的集群")
                if included:
                    print(f"  指定的集群: {included}")
                print(f"  可用的集群: {[c['cluster_id'] for c in all_clusters][:10]}")
                # 匹配不到时，设置为空列表以明确"无匹配"
                # 后续会跳过导出或返回 0 条数据

        # 更新执行参数
        self.scroll_keepalive = job.execution.scroll_keepalive
        self.parallel_jobs = job.execution.parallel_metrics

        # 执行导出
        return self.export_all(
            output_dir=job.output.directory,
            metric_types=job.metrics,
            alert_types=job.alert_types if job.include_alerts else [],
            time_range_hours=job.time_range_hours,
            shard_size=job.shard_size,
            cluster_ids=cluster_ids,
            include_alerts=job.include_alerts,
            batch_size=job.execution.batch_size,
            source_fields=job.source_fields,
            parallel_jobs=job.execution.parallel_metrics,
            sampling=job.sampling,
            slim_config=job.slim,
        )

    def _print_type_summary(self, title: str, types_data: Dict, totals: list) -> None:
        """打印类型摘要，更新 totals [doc_count, duration_ms]"""
        print(f"\n{title}:")
        total_docs, total_time = 0, 0
        for type_key, info in types_data.items():
            duration_s = info.get("duration_ms", 0) / 1000
            status = "✓" if not info.get("error") else "✗"
            count = info["count"]

            # 处理分片显示
            if info.get("sharded") and info.get("files"):
                print(f"  - {status} {info['name']}: {count:,} 条记录 ({duration_s:.1f}s)")
                files_info = ", ".join([f"{f['file']} ({f['count']:,})" for f in info["files"]])
                print(f"      文件: {files_info}")
            else:
                print(f"  - {status} {info['name']}: {count:,} 条记录 ({duration_s:.1f}s)")
                if info.get("file"):
                    print(f"      文件: {info['file']}")

            total_docs += count
            total_time += info.get("duration_ms", 0)
        totals[0] += total_docs
        totals[1] += total_time

    def print_summary(self, summary: Dict[str, Any]):
        """打印导出摘要"""
        print("\n" + "=" * 60)
        print("导出摘要")
        print("=" * 60)
        print(f"导出时间: {summary['export_time']}")
        print(f"时间范围: 最近 {summary['time_range_hours']} 小时")
        print(f"分片大小: {summary.get('shard_size', 100000):,} 条/文件")
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

        totals = [0, 0]  # [total_docs, total_time_ms]
        self._print_type_summary("监控指标数据", summary["metric_types"], totals)
        self._print_type_summary("告警数据", summary["alert_types"], totals)

        print(f"\n总计: {totals[0]:,} 条记录")
        print(f"总耗时: {totals[1] / 1000:.1f}s")
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
        "--shard-size",
        type=int,
        default=100000,
        help="每个分片文件的最大文档数，超过时自动分文件存储，默认100000",
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
    parser.add_argument(
        "--slim",
        action="store_true",
        help="精简数据：删除和排障无关的字段（_id, agent, metadata.category/datatype/name）和冗余的人类可读格式字段（store, estimated_size, limit_size）",
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
    shard_size = args.shard_size or config.get('shardSize', 100000)
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

    # 解析精简配置
    slim_config = None
    if args.slim or config.get('slim', False):
        slim_config = SlimConfig(enabled=True)

    # 导出数据
    summary = exporter.export_all(
        output_dir=output,
        metric_types=metric_types,
        time_range_hours=time_range,
        shard_size=shard_size,
        cluster_id_filter=cluster_id,
        include_alerts=include_alerts,
        batch_size=batch_size,
        source_fields=source_fields,
        parallel_jobs=parallel,
        slim_config=slim_config,
    )

    exporter.print_summary(summary)
    print(f"\n数据已导出到: {output}")


if __name__ == "__main__":
    main()
