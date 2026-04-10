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
import copy
import getpass
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))
from common.console_client import ConsoleClient, ConsoleAuthError, ConsoleAPIError
from common.config import (
    add_common_args, get_config_value,
    AppConfig, MetricsJobConfig, ConfigValidationError,
    SamplingConfig, SlimConfig,
    FieldAggStrategy, METRIC_FIELD_AGG_CONFIG,
)


# 监控指标类型定义
METRIC_TYPES = {
    "cluster_health": {
        "name": "集群健康指标",
        "description": "集群级别的健康状态信息，包括节点数、分片状态等",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"cluster_health"',
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.cluster_name"],
        "default_batch_size": 8000,  # 数据量小，可用大批次
    },
    "cluster_stats": {
        "name": "集群统计指标",
        "description": "集群级别的统计信息，包括索引数、文档数、存储大小、JVM内存等",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"cluster_stats"',
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.cluster_name"],
        "default_batch_size": 8000,
    },
    "node_stats": {
        "name": "节点统计指标",
        "description": "节点级别的详细统计，包括CPU、内存、JVM、磁盘IO、网络、索引操作等",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"node_stats"',
        # 抽样分层按 cluster + node 维度，避免 node_name 变更导致同节点被拆分
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.node_id"],
        "default_batch_size": 5000,  # 数据量大，从3000提升到5000
    },
    "index_stats": {
        "name": "索引统计指标",
        "description": "索引级别的统计信息，包括文档数、存储大小、查询/索引操作次数和耗时",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"index_stats"',
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.index_name"],
        "default_batch_size": 5000,
    },
    "shard_stats": {
        "name": "分片统计指标",
        "description": "分片级别的详细统计，包括文档数、存储大小、读写操作等",
        "index_pattern": ".infini_metrics",
        "filter_template": 'metadata.name:"shard_stats"',
        "key_fields": ["metadata.labels.cluster_id", "metadata.labels.index_name", "metadata.labels.shard_id"],
        "default_batch_size": 3000,  # 数据量最大，从2000提升到3000
    },
}

# 告警相关数据类型
ALERT_TYPES = {
    "alert_rules": {
        "name": "告警规则",
        "description": "配置的告警规则定义",
        "index_pattern": ".infini_alert-rule",
        "default_batch_size": 8000,
    },
    "alert_messages": {
        "name": "告警消息",
        "description": "告警触发产生的消息记录",
        "index_pattern": ".infini_alert-message",
        "default_batch_size": 5000,
    },
    "alert_history": {
        "name": "告警历史",
        "description": "告警状态变更的历史记录",
        "index_pattern": ".infini_alert-history",
        "default_batch_size": 5000,
    },
}

# 默认配置
DEFAULT_BATCH_SIZE = 3000
DEFAULT_SCROLL_KEEPALIVE = "5m"  # 增加到 5 分钟，避免大数据量时 scroll context 过期
DEFAULT_PARALLEL_JOBS = 2  # 默认并行导出的指标类型数
DEFAULT_PARALLEL_DEGREE = 1  # 单个指标内部并行度（基于 sliced scroll）
DEFAULT_COMPOSITE_PAGE_SIZE = 3000  # sampling/composite 每页桶数，减少请求往返


class FieldAggregator:
    """
    字段聚合器：根据字段聚合策略，从 ES 聚合桶中计算覆盖值。

    策略类型：
    - rate:     累积计数器的变化率，delta(value) / bucket_size
    - ratio:    两个导数字段的比率，delta(num) / delta(den)
    - latency:  平均延迟，delta(time) / delta(count)
    - max:      取最大值（瞬时值类字段）
    - latest:   取最新值（默认策略，未分类字段）
    """

    def __init__(self, metric_type: str, interval_ms: int):
        self.metric_type = metric_type
        self.interval_ms = interval_ms
        self.config = METRIC_FIELD_AGG_CONFIG.get(metric_type, {})
        self.rate_fields = set(self.config.get("rate_fields", {}).keys())
        self.latency_fields = self.config.get("latency_fields", {})
        self.max_fields = set(self.config.get("max_fields", {}).keys())
        # rate_state: 按 (group_key, field_path) 维护上一桶的值，用于跨页/跨 worker 导数计算
        self._rate_state: Dict[Tuple[str, str], float] = {}

    def _group_key_from_bucket(self, bucket_key: Dict) -> str:
        """从 composite bucket key 生成唯一分组标识"""
        parts = []
        for k in sorted(bucket_key.keys()):
            if k != "time_bucket":
                parts.append(str(bucket_key[k]))
        return "|".join(parts) if parts else "_default"

    def _set_nested_value(self, doc: Dict, field_path: str, value: Any) -> None:
        """设置嵌套字段值，自动创建中间字典"""
        parts = field_path.split(".")
        current = doc
        for part in parts[:-1]:
            if part not in current or not isinstance(current[part], dict):
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value

    def _get_nested_value(self, doc: Dict, field_path: str) -> Optional[Any]:
        """获取嵌套字段值"""
        parts = field_path.split(".")
        value = doc
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None
        return value

    def apply_field_overrides(
        self,
        doc: Dict,
        bucket_key: Dict,
        bucket_aggs: Optional[Dict] = None,
    ) -> Dict:
        """
        根据字段聚合策略覆盖 latest 快照中的字段值。

        步骤：
        1. 取 latest 快照作为底稿
        2. 对已分类字段，按策略计算覆盖值
        3. 未分类字段保持 latest 值（typed fallback 由快照保证）
        """
        if not self.config:
            return doc

        group_key = self._group_key_from_bucket(bucket_key)
        result = copy.deepcopy(doc)
        bucket_size_sec = self.interval_ms / 1000.0

        # 1. Latency 字段：delta(time) / delta(count)
        # 注意：latency 必须先于 rate 处理，因为它依赖原始累积值
        for field_path, spec in self.latency_fields.items():
            time_field = spec["time_field"]
            count_field = spec["count_field"]
            # 使用原始文档中的值，而不是 result（可能已被 rate 转换）
            time_val = self._get_nested_value(doc, time_field)
            count_val = self._get_nested_value(doc, count_field)
            if time_val is None or count_val is None:
                continue
            try:
                time_num = float(time_val)
                count_num = float(count_val)
            except (TypeError, ValueError):
                continue

            time_state_key = (group_key, time_field)
            count_state_key = (group_key, count_field)

            prev_time = self._rate_state.get(time_state_key)
            prev_count = self._rate_state.get(count_state_key)

            # 更新 rate_state（latency 的 time/count 也要参与 rate 状态追踪）
            self._rate_state[time_state_key] = time_num
            self._rate_state[count_state_key] = count_num

            if prev_time is not None and prev_count is not None:
                delta_time = time_num - prev_time
                delta_count = count_num - prev_count
                if delta_time < 0:
                    delta_time = time_num
                if delta_count < 0:
                    delta_count = count_num
                if delta_count == 0:
                    latency_val = 0.0
                else:
                    latency_val = delta_time / delta_count
                self._set_nested_value(result, field_path, latency_val)

        # 2. Rate 字段：delta / bucket_size
        for field_path in self.rate_fields:
            current_val = self._get_nested_value(doc, field_path)
            if current_val is None:
                continue
            try:
                current_num = float(current_val)
            except (TypeError, ValueError):
                continue

            state_key = (group_key, field_path)
            prev_val = self._rate_state.get(state_key)
            self._rate_state[state_key] = current_num

            if prev_val is not None:
                delta = current_num - prev_val
                # 处理计数器重置（如节点重启）
                if delta < 0:
                    delta = current_num
                rate_val = delta / bucket_size_sec
                self._set_nested_value(result, field_path, rate_val)
            # 首次见到该字段（无前值），保留原始值不变

        # 3. Max 字段：从 bucket 聚合中取 max（如果有的话），否则保留 latest
        if bucket_aggs:
            for field_path in self.max_fields:
                # bucket_aggs 的 key 是扁平的字符串（如 "payload.elasticsearch.node_stats.jvm.mem.heap_used_in_bytes"）
                agg_val = bucket_aggs.get(field_path)
                if agg_val is not None:
                    if isinstance(agg_val, dict) and "value" in agg_val:
                        max_val = agg_val["value"]
                        if max_val is not None:
                            self._set_nested_value(result, field_path, max_val)

        return result

    def build_max_aggs(self) -> Dict[str, Any]:
        """构建 max 聚合 DSL，用于需要取最大值的字段。
        直接用原始字段路径作为聚合名，和 apply_field_overrides 里的查找 key 保持一致。
        ES 聚合名可以是任意字符串，含点号亦合法。
        """
        aggs = {}
        for field_path in self.max_fields:
            aggs[field_path] = {"max": {"field": field_path}}
        return aggs


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


class ConsoleProgressReporter:
    """线程安全的控制台进度输出器，避免并行任务输出相互覆盖。"""

    def __init__(self, min_interval_sec: float = 0.8):
        self._lock = threading.Lock()
        self._min_interval_sec = min_interval_sec
        self._last_emit_ts: Dict[str, float] = {}

    def stage(self, message: str) -> None:
        with self._lock:
            print(message)

    def start(self, task_name: str) -> None:
        with self._lock:
            print(f"  [{task_name}] 已启动")

    def update(self, task_name: str, current: int, total: int = 0, force: bool = False) -> None:
        now = time.time()
        with self._lock:
            last_ts = self._last_emit_ts.get(task_name, 0)
            should_emit = force or (now - last_ts >= self._min_interval_sec)
            if not should_emit:
                return

            if total > 0:
                percent = min(100, current * 100 // total)
                print(f"    [{task_name}] 进度 {current:,}/{total:,} ({percent}%)")
            else:
                print(f"    [{task_name}] 进度 {current:,}")

            self._last_emit_ts[task_name] = now

    def finish(self, task_name: str, count: int, error: Optional[str] = None) -> None:
        with self._lock:
            if error:
                print(f"  [{task_name}] 失败: {error}")
            else:
                print(f"  [{task_name}] 完成，已保存 {count:,} 条记录")


class MetricsExporter:
    """监控数据导出器 - 优化版"""

    # 精简模式下要删除的字段
    SLIM_META_FIELDS = {"_id", "agent"}
    SLIM_META_PREFIXES = ("category", "datatype", "name")  # metadata 下的字段
    SLIM_HUMAN_READABLE = {"store", "estimated_size", "limit_size"}

    @staticmethod
    def _parse_time_input(raw: str, is_end: bool = False) -> datetime:
        """解析时间字符串，支持 YYYY-MM-DD HH:MM:SS 或 YYYY-MM-DD。"""
        if not raw:
            raise ValueError("时间字符串不能为空")

        raw = raw.strip()
        parsed: Optional[datetime] = None

        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(raw, fmt)
                # 日期格式默认解释为当天起止
                if fmt == "%Y-%m-%d" and is_end:
                    parsed = parsed.replace(hour=23, minute=59, second=59)
                break
            except ValueError:
                continue

        if parsed is None:
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError(
                    f"无效时间格式: {raw}，支持 'YYYY-MM-DD HH:MM:SS' 或 'YYYY-MM-DD'"
                ) from exc

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)

        return parsed

    def _resolve_time_window(
        self,
        time_range_hours: int,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> Tuple[datetime, datetime]:
        """解析最终时间窗口。未指定绝对时间时使用最近 N 小时。"""
        now = datetime.now(timezone.utc)
        start_dt = self._parse_time_input(start_time, is_end=False) if start_time else None
        end_dt = self._parse_time_input(end_time, is_end=True) if end_time else None

        if start_dt is None and end_dt is None:
            end_dt = now
            start_dt = now - timedelta(hours=time_range_hours)
        elif start_dt is None:
            start_dt = end_dt - timedelta(hours=time_range_hours)
        elif end_dt is None:
            end_dt = now

        if start_dt > end_dt:
            raise ValueError(
                f"开始时间不能晚于结束时间: start={start_dt.isoformat()}, end={end_dt.isoformat()}"
            )

        return start_dt, end_dt

    def __init__(
        self,
        client: ConsoleClient,
        system_cluster_id: str,
        scroll_keepalive: str = DEFAULT_SCROLL_KEEPALIVE,
        parallel_jobs: int = DEFAULT_PARALLEL_JOBS,
        parallel_degree: int = DEFAULT_PARALLEL_DEGREE,
    ):
        self.client = client
        self.system_cluster_id = system_cluster_id
        self.scroll_keepalive = scroll_keepalive
        self.parallel_jobs = parallel_jobs
        self.parallel_degree = max(1, parallel_degree)

    @staticmethod
    def _mask_doc(value: Any) -> Any:
        """递归脱敏文档中的IP地址，隐藏前两个octet (如 192.168.1.1 -> *.*.1.1)"""
        import re
        
        if isinstance(value, str):
            # 匹配并替换IPv4地址
            ipv4_pattern = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
            def replace_ipv4(match):
                parts = match.group(0).split('.')
                return f"*.*.{parts[2]}.{parts[3]}" if len(parts) == 4 else match.group(0)
            return re.sub(ipv4_pattern, replace_ipv4, value)
        elif isinstance(value, dict):
            return {k: MetricsExporter._mask_doc(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [MetricsExporter._mask_doc(item) for item in value]
        else:
            return value

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
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> Dict:
        """构建监控指标查询"""
        start_dt, end_dt = self._resolve_time_window(time_range_hours, start_time, end_time)

        must_clauses = [
            {"query_string": {"query": query_filter}},
            {"range": {"timestamp": {"gte": start_dt.isoformat(), "lte": end_dt.isoformat()}}},
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

    def build_alert_query(
        self,
        alert_type: str,
        time_range_hours: int,
        source_fields: List[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> Dict:
        """构建告警数据查询。

        alert_history 使用 timestamp，alert_messages 使用 created 作为时间范围过滤。
        alert_rules 不属于时序日志，保持全量导出。
        """
        # 告警规则是配置数据，不是时序事件，不做时间过滤
        if alert_type == "alert_rules":
            return self.build_all_docs_query(source_fields)

        start_dt, end_dt = self._resolve_time_window(time_range_hours, start_time, end_time)

        # 优先使用不同告警类型的主时间字段
        time_field = "timestamp" if alert_type == "alert_history" else "created"

        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                time_field: {
                                    "gte": start_dt.isoformat(),
                                    "lte": end_dt.isoformat(),
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [
                {time_field: {"order": "desc", "unmapped_type": "date"}},
                {"_id": {"order": "asc"}},
            ],
            "_source": source_fields if source_fields else True,
        }

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

    def _write_docs(self, writer, docs: List[Dict], slim_config: SlimConfig = None, mask_ip: bool = False) -> None:
        """写入文档列表，移除 sort 字段，可选精简数据和脱敏IP"""
        for doc in docs:
            doc_copy = {k: v for k, v in doc.items() if k != "sort"}
            # 应用IP脱敏
            if mask_ip:
                doc_copy = self._mask_doc(doc_copy)
            # 应用精简配置
            if slim_config and slim_config.enabled:
                doc_copy = self._slim_doc(doc_copy, slim_config)
            writer.write_doc(doc_copy)

    def _export_with_scroll_single(
        self,
        index_pattern: str,
        query: Dict,
        output_file: str,
        batch_size: int = DEFAULT_BATCH_SIZE,
        shard_size: int = 100000,
        progress_callback=None,
        slim_config: SlimConfig = None,
        mask_ip: bool = False,
    ) -> Tuple[int, List[str]]:
        """
        使用 scroll API 流式导出数据（支持自动分片）

        支持自动恢复：如果 scroll context 过期，会自动重新初始化查询继续导出。

        Args:
            output_file: 基础输出路径（不含 .json 后缀）
            shard_size: 每个分片文件的最大文档数，默认 100000
            slim_config: 精简数据配置
            mask_ip: 是否脱敏IP地址

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

        # 预取下一页，和当前批次写盘并行
        def _start_prefetch(
            executor: ThreadPoolExecutor,
            current_scroll_id: Optional[str],
        ) -> Tuple[Optional[Any], Optional[str]]:
            if not current_scroll_id:
                return None, None
            return executor.submit(self.scroll_next, current_scroll_id), current_scroll_id

        # 使用分片写入器
        with ShardedJSONLinesWriter(output_file, shard_size) as writer:
            current_batch = first_batch
            with ThreadPoolExecutor(max_workers=1) as prefetch_executor:
                next_future, requested_scroll_id = _start_prefetch(prefetch_executor, scroll_id)

                while current_batch:
                    self._write_docs(writer, current_batch, slim_config, mask_ip)
                    writer.flush()
                    total_exported += len(current_batch)

                    # 更新最后排序值（用于恢复）
                    last_sort_values = current_batch[-1].get("sort")

                    if progress_callback:
                        progress_callback(total_exported, total_count)

                    if not next_future:
                        break

                    scroll_result = next_future.result()

                    # scroll context 过期，尝试恢复
                    if scroll_result is None:
                        print(f"\n    Scroll context 过期，正在从位置 {total_exported:,} 恢复...")
                        if requested_scroll_id:
                            self.clear_scroll(requested_scroll_id)

                        # 使用 search_after 恢复查询
                        current_batch, scroll_id, _ = self._resume_with_search_after(
                            index_pattern, query, batch_size, last_sort_values
                        )

                        if current_batch is None:
                            print("    恢复失败，停止导出")
                            break

                        print(f"    恢复成功，继续导出...")
                        next_future, requested_scroll_id = _start_prefetch(prefetch_executor, scroll_id)
                        continue

                    current_batch, next_scroll_id = scroll_result
                    scroll_id = next_scroll_id or scroll_id

                    if not current_batch:
                        break

                    next_future, requested_scroll_id = _start_prefetch(prefetch_executor, scroll_id)

        # 清理 scroll
        if scroll_id:
            self.clear_scroll(scroll_id)

        return total_exported, writer.get_file_paths()

    def _export_with_sliced_scroll(
        self,
        index_pattern: str,
        query: Dict,
        output_file: str,
        batch_size: int,
        shard_size: int,
        parallel_degree: int,
        progress_callback=None,
        slim_config: SlimConfig = None,
        mask_ip: bool = False,
    ) -> Tuple[int, List[str]]:
        """使用 sliced scroll 在单个指标内并行导出。"""
        effective_slices = max(1, parallel_degree)
        progress_lock = threading.Lock()
        per_slice_exported = [0] * effective_slices
        total_exported = 0
        all_file_paths: List[str] = []

        def make_slice_progress(slice_id: int):
            def _callback(current: int, _total: int):
                nonlocal total_exported
                with progress_lock:
                    delta = current - per_slice_exported[slice_id]
                    if delta > 0:
                        per_slice_exported[slice_id] = current
                        total_exported += delta
                        if progress_callback:
                            progress_callback(total_exported, 0)

            return _callback

        def run_slice(slice_id: int) -> Tuple[int, List[str]]:
            slice_query = copy.deepcopy(query)
            slice_query["slice"] = {"id": slice_id, "max": effective_slices}
            slice_output_base = f"{output_file}_slice{slice_id}"

            return self._export_with_scroll_single(
                index_pattern=index_pattern,
                query=slice_query,
                output_file=slice_output_base,
                batch_size=batch_size,
                shard_size=shard_size,
                progress_callback=make_slice_progress(slice_id),
                slim_config=slim_config,
                mask_ip=mask_ip,
            )

        with ThreadPoolExecutor(max_workers=effective_slices) as executor:
            futures = {executor.submit(run_slice, slice_id): slice_id for slice_id in range(effective_slices)}

            for future in as_completed(futures):
                slice_id = futures[future]
                count, file_paths = future.result()
                # 某些分片可能无数据，保留空文件行为以便定位分片执行状态
                all_file_paths.extend(file_paths)
                with progress_lock:
                    per_slice_exported[slice_id] = count

        return sum(per_slice_exported), sorted(all_file_paths)

    def export_with_scroll(
        self,
        index_pattern: str,
        query: Dict,
        output_file: str,
        batch_size: int = DEFAULT_BATCH_SIZE,
        shard_size: int = 100000,
        progress_callback=None,
        slim_config: SlimConfig = None,
        mask_ip: bool = False,
        parallel_degree: int = 1,
    ) -> Tuple[int, List[str]]:
        """使用 scroll API 导出数据，可选在单个指标内启用 sliced scroll 并行。"""
        if parallel_degree and parallel_degree > 1:
            return self._export_with_sliced_scroll(
                index_pattern=index_pattern,
                query=query,
                output_file=output_file,
                batch_size=batch_size,
                shard_size=shard_size,
                parallel_degree=parallel_degree,
                progress_callback=progress_callback,
                slim_config=slim_config,
                mask_ip=mask_ip,
            )

        return self._export_with_scroll_single(
            index_pattern=index_pattern,
            query=query,
            output_file=output_file,
            batch_size=batch_size,
            shard_size=shard_size,
            progress_callback=progress_callback,
            slim_config=slim_config,
            mask_ip=mask_ip,
        )

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
        # 明确约束关键指标的分组维度，确保采样语义稳定
        if metric_type == "node_stats":
            return ["metadata.labels.cluster_id", "metadata.labels.node_id"]
        if metric_type == "index_stats":
            return ["metadata.labels.cluster_id", "metadata.labels.index_name"]

        return config.get("key_fields") or ["metadata.labels.cluster_id"]

    def _detect_valid_group_fields(
        self, index_pattern: str, query: Dict, group_fields: List[str]
    ) -> List[str]:
        """
        检测哪些分组字段实际存在（有非空值）

        通过查询一条记录来检查字段是否存在且有值
        """
        valid_fields = []

        # 查询一条记录来检查字段
        try:
            result = self.client.proxy_request(
                self.system_cluster_id,
                "POST",
                f"/{index_pattern}/_search",
                {"size": 1, "query": query, "_source": group_fields},
            )
            hits = result.get("hits", {}).get("hits", [])
            if hits:
                doc = hits[0].get("_source", {})
                for field in group_fields:
                    # 检查字段是否存在且有值
                    value = self._get_nested_value(doc, field)
                    if value is not None:
                        valid_fields.append(field)
        except Exception:
            pass

        return valid_fields

    def _get_nested_value(self, doc: Dict, field_path: str) -> Any:
        """获取嵌套字段的值"""
        parts = field_path.split(".")
        value = doc
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None
        return value

    def _parse_interval_to_ms(self, interval: str) -> int:
        """解析 interval 字符串为毫秒"""
        match = re.fullmatch(r"(\d+)(ms|s|m|h|d)", interval or "")
        if not match:
            raise ValueError(f"Invalid interval format: {interval}")

        value = int(match.group(1))
        unit = match.group(2)

        multipliers = {
            "ms": 1,
            "s": 1000,
            "m": 60 * 1000,
            "h": 60 * 60 * 1000,
            "d": 24 * 60 * 60 * 1000,
        }

        return value * multipliers[unit]

    def _resolve_effective_group_fields(
        self,
        metric_type: str,
        index_pattern: str,
        query: Dict[str, Any],
        group_fields: List[str],
    ) -> List[str]:
        """解析抽样实际分组字段，避免预估和导出重复探测。"""
        effective_group_fields = self._detect_valid_group_fields(
            index_pattern,
            query.get("query", {"match_all": {}}),
            group_fields,
        )

        if metric_type in {"node_stats", "index_stats"}:
            # 对 node/index 抽样强制保留完整分组维度，避免探测样本缺失导致降维
            effective_group_fields = group_fields

        if not effective_group_fields:
            effective_group_fields = group_fields[:1]

        return effective_group_fields

    def _build_sampling_point_from_bucket(
        self, bucket: Dict[str, Any],
        field_aggregator: Optional['FieldAggregator'] = None,
    ) -> Optional[Dict[str, Any]]:
        """从 sampling bucket 构建采样点（保留时间桶内最新真实快照），并应用字段聚合覆盖。"""
        hits = bucket.get("latest", {}).get("hits", {}).get("hits", [])
        if not hits:
            return None

        hit = hits[0]
        doc = {"_id": hit.get("_id"), **hit.get("_source", {})}

        # 应用字段聚合覆盖
        if field_aggregator:
            bucket_key = bucket.get("key", {})
            bucket_aggs = {k: v for k, v in bucket.items() if k not in ("key", "latest", "doc_count")}
            doc = field_aggregator.apply_field_overrides(doc, bucket_key, bucket_aggs)

        return doc

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
        mask_ip: bool = False,
        parallel_degree: int = 1,
        metric_type: str = None,
        effective_group_fields: List[str] = None,
    ) -> Tuple[int, List[str]]:
        """
        ES 端抽样：时间桶 + 维度分层（top_hits 或 字段聚合）
        每个 (维度组合, 时间桶) 只保留最新的一条记录，或使用字段聚合计算指标值

        Args:
            output_file: 基础输出路径（不含 .json 后缀）
            shard_size: 每个分片文件的最大文档数，默认 100000
            slim_config: 精简数据配置
            mask_ip: 是否脱敏IP地址
            metric_type: 指标类型，用于获取内置的字段聚合配置

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
                mask_ip,
                parallel_degree,
            )

        total_exported = 0
        after_key = None
        composite_page_size = DEFAULT_COMPOSITE_PAGE_SIZE

        # 创建字段聚合器（仅 node_stats / index_stats 启用字段聚合）
        interval_ms = self._parse_interval_to_ms(sampling_interval)
        field_aggregator: Optional[FieldAggregator] = None
        if metric_type and metric_type in METRIC_FIELD_AGG_CONFIG:
            field_aggregator = FieldAggregator(metric_type, interval_ms)

        if not effective_group_fields:
            effective_group_fields = self._resolve_effective_group_fields(
                metric_type or "",
                index_pattern,
                query,
                group_fields,
            )

        sources = []
        for i, field in enumerate(effective_group_fields):
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

        def build_sampling_body(search_query: Dict, after: Dict = None) -> Dict[str, Any]:
            sampling_aggs: Dict[str, Any] = {
                "latest": {
                    "top_hits": {
                        "size": 1,
                        "sort": [
                            {"timestamp": {"order": "desc"}},
                            {"_id": {"order": "desc"}},
                        ],
                        "_source": source_fields if source_fields else True,
                    }
                }
            }

            # 对 max 型字段追加 max 聚合
            if field_aggregator:
                max_aggs = field_aggregator.build_max_aggs()
                if max_aggs:
                    sampling_aggs.update(max_aggs)

            body = {
                "size": 0,
                "track_total_hits": False,
                "query": search_query.get("query", {"match_all": {}}),
                "aggs": {
                    "sampled": {
                        "composite": {
                            "size": composite_page_size,
                            "sources": sources,
                        },
                        "aggs": sampling_aggs,
                    }
                },
            }
            if after:
                body["aggs"]["sampled"]["composite"]["after"] = after
            return body

        def _split_sampling_queries(base_query: Dict, parts: int) -> List[Dict]:
            """按 fixed_interval 桶边界拆分时间范围，避免跨 worker 重复 time_bucket。"""
            if parts <= 1:
                return [base_query]

            must_clauses = base_query.get("query", {}).get("bool", {}).get("must", [])
            ts_range = None
            for clause in must_clauses:
                if isinstance(clause, dict) and "range" in clause and "timestamp" in clause["range"]:
                    ts_range = clause["range"]["timestamp"]
                    break

            if not ts_range:
                return [base_query]

            start_raw = ts_range.get("gte")
            end_raw = ts_range.get("lte")
            if not start_raw or not end_raw:
                return [base_query]

            try:
                start_dt = datetime.fromisoformat(str(start_raw).replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(str(end_raw).replace("Z", "+00:00"))
            except Exception:
                return [base_query]

            start_ms = int(start_dt.timestamp() * 1000)
            end_ms = int(end_dt.timestamp() * 1000)
            if end_ms < start_ms:
                return [base_query]

            try:
                interval_ms = self._parse_interval_to_ms(sampling_interval)
            except ValueError:
                return [base_query]

            first_bucket_ms = (start_ms // interval_ms) * interval_ms
            last_bucket_ms = (end_ms // interval_ms) * interval_ms
            bucket_count = ((last_bucket_ms - first_bucket_ms) // interval_ms) + 1
            if bucket_count <= 1:
                return [base_query]

            worker_count = min(parts, bucket_count)
            base_buckets_per_worker = bucket_count // worker_count
            remainder = bucket_count % worker_count

            queries: List[Dict] = []
            bucket_offset = 0
            for i in range(worker_count):
                worker_bucket_count = base_buckets_per_worker + (1 if i < remainder else 0)
                worker_bucket_start_ms = first_bucket_ms + bucket_offset * interval_ms
                worker_bucket_end_ms = worker_bucket_start_ms + worker_bucket_count * interval_ms
                bucket_offset += worker_bucket_count

                worker_start_ms = start_ms if i == 0 else worker_bucket_start_ms
                worker_end_exclusive_ms = worker_bucket_end_ms
                worker_end_inclusive_ms = end_ms if i == worker_count - 1 else None

                q = copy.deepcopy(base_query)
                q_must = q.get("query", {}).get("bool", {}).get("must", [])
                for clause in q_must:
                    if isinstance(clause, dict) and "range" in clause and "timestamp" in clause["range"]:
                        original = clause["range"]["timestamp"]
                        new_range = dict(original)
                        new_range["gte"] = datetime.fromtimestamp(worker_start_ms / 1000, timezone.utc).isoformat()
                        if i == worker_count - 1:
                            new_range["lte"] = datetime.fromtimestamp(worker_end_inclusive_ms / 1000, timezone.utc).isoformat()
                            new_range.pop("lt", None)
                        else:
                            new_range["lt"] = datetime.fromtimestamp(worker_end_exclusive_ms / 1000, timezone.utc).isoformat()
                            new_range.pop("lte", None)
                        clause["range"]["timestamp"] = new_range
                        break

                queries.append(q)

            return queries

        def _extract_docs_and_after_from_sampled(
            sampled: Dict[str, Any],
            aggregator: Optional["FieldAggregator"] = None,
        ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
            """从 sampled 聚合结果提取文档列表和 after_key。"""
            buckets = sampled.get("buckets", [])
            if not buckets:
                return [], None

            docs: List[Dict[str, Any]] = []
            for bucket in buckets:
                doc = self._build_sampling_point_from_bucket(bucket, aggregator)
                if not doc:
                    continue
                if mask_ip:
                    doc = self._mask_doc(doc)
                if slim_config and slim_config.enabled:
                    doc = self._slim_doc(doc, slim_config)
                docs.append(doc)

            return docs, sampled.get("after_key")

        def _fetch_sampling_page(
            search_query: Dict,
            after: Dict = None,
            aggregator: Optional["FieldAggregator"] = None,
        ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
            """请求单页 sampling/composite 数据并转换为文档。"""
            body = build_sampling_body(search_query, after)
            result = self.client.proxy_request(
                self.system_cluster_id,
                "POST",
                f"/{index_pattern}/_search",
                body,
            )
            sampled = result.get("aggregations", {}).get("sampled", {})
            return _extract_docs_and_after_from_sampled(sampled, aggregator)

        # 并行路径：每个 worker 独立写文件，避免全局去重 map 长时间占用内存
        if parallel_degree and parallel_degree > 1:
            effective_slices = max(1, parallel_degree)
            worker_queries = _split_sampling_queries(query, effective_slices)
            if len(worker_queries) == 1 and effective_slices > 1:
                print("    sampling 并行降级为单线程：未识别到可拆分的 timestamp 范围")

            progress_lock = threading.Lock()
            per_worker_exported = [0] * len(worker_queries)
            all_file_paths: List[str] = []

            def _report_worker_progress(worker_id: int, current_count: int) -> None:
                if not progress_callback:
                    return
                with progress_lock:
                    per_worker_exported[worker_id] = current_count
                    progress_callback(sum(per_worker_exported), 0)

            def run_sampling_slice(worker_id: int, worker_query: Dict) -> Tuple[int, List[str]]:
                # 每个 worker 独立的字段聚合器，避免多 worker 并发时 _rate_state 竞争
                worker_aggregator = (
                    FieldAggregator(metric_type, interval_ms)
                    if field_aggregator is not None
                    else None
                )
                local_count = 0
                worker_output = f"{output_file}_worker{worker_id}"

                with ShardedJSONLinesWriter(worker_output, shard_size) as writer:
                    current_docs, next_after = _fetch_sampling_page(worker_query, None, worker_aggregator)

                    with ThreadPoolExecutor(max_workers=1) as prefetch_executor:
                        next_future = (
                            prefetch_executor.submit(_fetch_sampling_page, worker_query, next_after, worker_aggregator)
                            if next_after
                            else None
                        )

                        while current_docs:
                            for doc in current_docs:
                                writer.write_doc(doc)
                                local_count += 1

                            writer.flush()
                            _report_worker_progress(worker_id, local_count)

                            if not next_future:
                                break

                            current_docs, next_after = next_future.result()
                            next_future = (
                                prefetch_executor.submit(_fetch_sampling_page, worker_query, next_after, worker_aggregator)
                                if next_after
                                else None
                            )

                return local_count, writer.get_file_paths()

            with ThreadPoolExecutor(max_workers=len(worker_queries)) as executor:
                futures = {
                    executor.submit(run_sampling_slice, worker_id, worker_query): worker_id
                    for worker_id, worker_query in enumerate(worker_queries)
                }

                for future in as_completed(futures):
                    worker_count, worker_files = future.result()
                    total_exported += worker_count
                    all_file_paths.extend(worker_files)

            if progress_callback:
                progress_callback(total_exported, 0)

            return total_exported, sorted(all_file_paths)

        with ShardedJSONLinesWriter(output_file, shard_size) as writer:
            current_docs, after_key = _fetch_sampling_page(query, None, field_aggregator)

            with ThreadPoolExecutor(max_workers=1) as prefetch_executor:
                next_future = (
                    prefetch_executor.submit(_fetch_sampling_page, query, after_key, field_aggregator)
                    if after_key
                    else None
                )

                while current_docs:
                    for doc in current_docs:
                        writer.write_doc(doc)
                        total_exported += 1

                    writer.flush()
                    if progress_callback:
                        progress_callback(total_exported, 0)

                    if not next_future:
                        break

                    current_docs, after_key = next_future.result()
                    next_future = (
                        prefetch_executor.submit(_fetch_sampling_page, query, after_key, field_aggregator)
                        if after_key
                        else None
                    )

        return total_exported, writer.get_file_paths()

    def estimate_export_count(
        self,
        metric_type: str,
        config: Dict,
        time_range_hours: int,
        cluster_id_filter: str = None,
        cluster_ids: List[str] = None,
        sampling: SamplingConfig = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        effective_group_fields: List[str] = None,
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
            start_time,
            end_time,
        )

        try:
            # 先获取原始文档数
            print(f"    正在查询原始数据量...")
            count_body = {"query": query.get("query", {"match_all": {}})}
            count_result = self.client.proxy_request(
                self.system_cluster_id,
                "POST",
                f"/{config['index_pattern']}/_count",
                count_body,
            )
            total_docs = count_result.get("count", 0)
            print(f"    原始数据量: {total_docs:,} 条")

            # interval 抽样时，使用轻量估算：分组基数 * 时间桶数
            if sampling and sampling.interval:
                print(f"    正在预估抽样数据量 (interval={sampling.interval})...")
                if not effective_group_fields:
                    group_fields = self._get_sampling_group_fields(metric_type, config)
                    effective_group_fields = self._resolve_effective_group_fields(
                        metric_type,
                        config["index_pattern"],
                        query,
                        group_fields,
                    )

                script_parts = [
                    f"(doc['{field}'].size()!=0 ? doc['{field}'].value.toString() : '')"
                    for field in effective_group_fields
                ]
                cardinality_body = {
                    "size": 0,
                    "track_total_hits": False,
                    "query": query.get("query", {"match_all": {}}),
                    "aggs": {
                        "group_cardinality": {
                            "cardinality": {
                                "script": {
                                    "source": " + '|' + ".join(script_parts)
                                },
                                "precision_threshold": 40000,
                            }
                        }
                    },
                }

                cardinality_result = self.client.proxy_request(
                    self.system_cluster_id,
                    "POST",
                    f"/{config['index_pattern']}/_search",
                    cardinality_body,
                )
                group_count = int(
                    cardinality_result.get("aggregations", {})
                    .get("group_cardinality", {})
                    .get("value", 0)
                )

                # ES fixed_interval 桶数量：floor(end/interval)-floor(start/interval)+1
                start_dt, end_dt = self._resolve_time_window(time_range_hours, start_time, end_time)
                interval_ms = self._parse_interval_to_ms(sampling.interval)
                start_ms = int(start_dt.timestamp() * 1000)
                end_ms = int(end_dt.timestamp() * 1000)
                bucket_count = max(0, (end_ms // interval_ms) - (start_ms // interval_ms) + 1)

                sampled_estimate = min(total_docs, group_count * bucket_count)
                print(f"    估算分组数: {group_count:,}，时间桶数: {bucket_count:,}")

                return (total_docs, sampled_estimate)
            else:
                # 无抽样时，两个值相同
                return (total_docs, total_docs)
        except Exception as e:
            print(f"    预估数据量失败: {e}")
            return (-1, -1)

    def _make_progress_callback(self, metric_type: str, reporter: Optional[ConsoleProgressReporter] = None):
        """创建进度回调函数"""
        def callback(current, total):
            if reporter:
                reporter.update(metric_type, current, total)
                return

            if total > 0:
                percent = min(100, current * 100 // total)
                print(f"\r    [{metric_type}] 已导出 {current:,} / {total:,} ({percent}%)", end="", flush=True)
            else:
                print(f"\r    [{metric_type}] 已导出 {current:,}", end="", flush=True)
        return callback

    def _build_shard_info_from_files(self, output_file: str, file_paths: List[str]) -> List[Dict[str, Any]]:
        """根据真实输出文件统计分片记录数，兼容 worker 文件与普通分片文件。"""
        output_dir = os.path.dirname(output_file) or "."
        shard_info: List[Dict[str, Any]] = []

        for file_name in file_paths:
            count = 0
            full_path = os.path.join(output_dir, file_name)
            try:
                with open(full_path, "r", encoding="utf-8") as handle:
                    for count, _ in enumerate(handle, start=1):
                        pass
            except OSError:
                count = 0

            shard_info.append({"file": file_name, "count": count})

        return shard_info

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
        mask_ip: bool = False,
        skip_estimation: bool = False,
        parallel_degree: int = 1,
        progress_reporter: Optional[ConsoleProgressReporter] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> ExportResult:
        """导出指定类型的监控指标（支持自动分片）

        Args:
            output_file: 基础输出路径（不含 .json 后缀）
            shard_size: 每个分片文件的最大文档数，默认 100000
            slim_config: 精简数据配置
            mask_ip: 是否脱敏IP地址
            skip_estimation: 跳过数据量预估，加速启动
        """
        result = ExportResult(metric_type, config["name"])
        start_ts = time.time()

        try:
            # 使用配置的批次大小或默认值
            effective_batch_size = batch_size or config.get("default_batch_size", DEFAULT_BATCH_SIZE)

            query = self.build_metrics_query(
                config["filter_template"],
                time_range_hours,
                cluster_id_filter,
                cluster_ids,
                source_fields,
                start_time,
                end_time,
            )

            effective_group_fields = None
            if self._should_use_es_sampling(sampling):
                effective_group_fields = self._resolve_effective_group_fields(
                    metric_type,
                    config["index_pattern"],
                    query,
                    self._get_sampling_group_fields(metric_type, config),
                )

            # 预估数据量（可选）
            total_docs, sampled_docs = -1, -1
            if not skip_estimation:
                total_docs, sampled_docs = self.estimate_export_count(
                    metric_type,
                    config,
                    time_range_hours,
                    cluster_id_filter,
                    cluster_ids,
                    sampling,
                    start_time,
                    end_time,
                    effective_group_fields,
                )
                if total_docs >= 0:
                    # 判断是否有抽样
                    if sampling and sampling.interval and total_docs != sampled_docs:
                        print(f"    原始数据量: {total_docs:,} 条，抽样后: {sampled_docs:,} 条")
                    else:
                        print(f"    预估需要导出约 {total_docs:,} 条记录")
                    if sampled_docs > shard_size:
                        print(f"    将自动分文件存储 (每文件最多 {shard_size:,} 条)")
            else:
                print(f"    已跳过数据量预估，直接开始导出...")

            progress_callback = self._make_progress_callback(metric_type, progress_reporter)
            metric_parallel_degree = (
                max(1, parallel_degree)
                if metric_type in {"node_stats", "index_stats", "shard_stats", "cluster_stats", "cluster_health"}
                else 1
            )

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
                    mask_ip,
                    metric_parallel_degree,
                    metric_type=metric_type,
                    effective_group_fields=effective_group_fields,
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
                    mask_ip,
                    metric_parallel_degree,
                )

            result.count = count
            result.file_paths = file_paths
            if file_paths:
                result.file_path = file_paths[0]  # 向后兼容
                if len(file_paths) > 1:
                    result.shard_info = self._build_shard_info_from_files(output_file, file_paths)

        except Exception as e:
            result.error = str(e)

        result.duration_ms = int((time.time() - start_ts) * 1000)
        return result

    def export_alert_type(
        self,
        alert_type: str,
        config: Dict,
        output_file: str,
        time_range_hours: int,
        shard_size: int = 100000,
        batch_size: int = None,
        source_fields: List[str] = None,
        slim_config: SlimConfig = None,
        mask_ip: bool = False,
        parallel_degree: int = 1,
        progress_reporter: Optional[ConsoleProgressReporter] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> ExportResult:
        """导出告警相关数据（支持自动分片）

        Args:
            output_file: 基础输出路径（不含 .json 后缀）
            shard_size: 每个分片文件的最大文档数，默认 100000
            slim_config: 精简数据配置
            mask_ip: 是否脱敏IP地址
        """
        result = ExportResult(alert_type, config["name"])
        start_ts = time.time()

        try:
            effective_batch_size = batch_size or config.get("default_batch_size", DEFAULT_BATCH_SIZE)
            query = self.build_alert_query(alert_type, time_range_hours, source_fields, start_time, end_time)

            count, file_paths = self.export_with_scroll(
                config["index_pattern"],
                query,
                output_file,
                effective_batch_size,
                shard_size,
                self._make_progress_callback(alert_type, progress_reporter),
                slim_config,
                mask_ip,
                parallel_degree,
            )

            result.count = count
            result.file_paths = file_paths
            if file_paths:
                result.file_path = file_paths[0]  # 向后兼容
                if len(file_paths) > 1:
                    result.shard_info = self._build_shard_info_from_files(output_file, file_paths)

        except Exception as e:
            result.error = str(e)

        result.duration_ms = int((time.time() - start_ts) * 1000)
        return result

    def get_available_clusters(
        self,
        time_range_hours: int = 24,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> List[Dict]:
        """获取有监控数据的集群列表

        Args:
            time_range_hours: 查询时间范围（小时），默认 24 小时
        """
        start_dt, end_dt = self._resolve_time_window(time_range_hours, start_time, end_time)

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
                        {
                            "range": {
                                "timestamp": {
                                    "gte": start_dt.isoformat(),
                                    "lte": end_dt.isoformat(),
                                }
                            }
                        },
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
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        shard_size: int = 100000,
        cluster_id_filter: str = None,
        cluster_ids: List[str] = None,
        include_alerts: bool = True,
        batch_size: int = None,
        source_fields: List[str] = None,
        parallel_jobs: int = None,
        parallel_degree: int = 1,
        sampling: SamplingConfig = None,
        slim_config: SlimConfig = None,
        mask_ip: bool = False,
        skip_estimation: bool = False,
    ) -> Dict[str, Any]:
        """导出所有监控数据（支持并行和自动分片）

        Args:
            shard_size: 每个分片文件的最大文档数，默认 100000
            slim_config: 精简数据配置
            mask_ip: 是否脱敏IP地址
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
                "start_time": start_time,
                "end_time": end_time,
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
        progress_reporter = ConsoleProgressReporter()

        export_summary = {
            "export_time": datetime.now().isoformat(),
            "time_range_hours": time_range_hours,
            "start_time": start_time,
            "end_time": end_time,
            "shard_size": shard_size,
            "batch_size": batch_size,
            "scroll_keepalive": self.scroll_keepalive,
            "parallel_jobs": effective_parallel,
            "parallel_degree": max(1, parallel_degree),
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
            progress_reporter.stage("\n正在获取有监控数据的集群列表...")
            clusters = self.get_available_clusters(time_range_hours, start_time, end_time)
            export_summary["clusters_with_data"] = clusters
            progress_reporter.stage(f"找到 {len(clusters)} 个有监控数据的集群")
            if not clusters:
                progress_reporter.stage("警告: 未找到有监控数据的集群，导出可能为空")

        # 导出监控指标（并行）
        progress_reporter.stage(f"\n正在导出监控指标数据 (并行度: {effective_parallel})...")

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
                progress_reporter.start(metric_type)
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
                    mask_ip,
                    skip_estimation,
                    parallel_degree,
                    progress_reporter,
                    start_time,
                    end_time,
                )
                futures[future] = metric_type

            for future in as_completed(futures):
                metric_type = futures[future]
                try:
                    result = future.result()
                    metric_results.append(result)
                    progress_reporter.update(metric_type, result.count, 0, force=True)
                    progress_reporter.finish(metric_type, result.count, result.error)
                except Exception as e:
                    progress_reporter.finish(metric_type, 0, str(e))

        # 汇总 metric 结果
        for result in metric_results:
            export_summary["metric_types"][result.metric_type] = result.to_dict()

        # 导出告警数据（并行）
        if include_alerts and alert_types:
            progress_reporter.stage(f"\n正在导出告警数据 (并行度: {effective_parallel})...")
            alert_results: List[ExportResult] = []
            valid_alert_types = [t for t in alert_types if t in ALERT_TYPES]

            for at in alert_types:
                if at not in ALERT_TYPES:
                    print(f"  警告: 未知的告警类型 {at}，跳过")

            with ThreadPoolExecutor(max_workers=effective_parallel) as executor:
                futures = {}
                for alert_type in valid_alert_types:
                    config = ALERT_TYPES[alert_type]
                    progress_reporter.start(alert_type)
                    # 基础路径，不含扩展名
                    output_file_base = os.path.join(output_dir, f"{alert_type}_{timestamp_suffix}")
                    future = executor.submit(
                        self.export_alert_type,
                        alert_type,
                        config,
                        output_file_base,
                        time_range_hours,
                        shard_size,
                        batch_size,
                        source_fields,
                        slim_config,
                        mask_ip,
                        parallel_degree,
                        progress_reporter,
                        start_time,
                        end_time,
                    )
                    futures[future] = alert_type

                for future in as_completed(futures):
                    alert_type = futures[future]
                    try:
                        result = future.result()
                        alert_results.append(result)
                        progress_reporter.update(alert_type, result.count, 0, force=True)
                        progress_reporter.finish(alert_type, result.count, result.error)
                    except Exception as e:
                        progress_reporter.finish(alert_type, 0, str(e))

            # 汇总 alert 结果
            for result in alert_results:
                export_summary["alert_types"][result.metric_type] = result.to_dict()

        # 保存导出摘要
        summary_file = os.path.join(output_dir, f"export_summary_{timestamp_suffix}.json")
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(export_summary, f, ensure_ascii=False, indent=2)
        progress_reporter.stage(f"\n导出摘要已保存: {summary_file}")

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
            all_clusters = self.get_available_clusters(job.time_range_hours, job.start_time, job.end_time)
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
        self.parallel_degree = max(1, job.execution.parallel_degree)

        # 执行导出
        return self.export_all(
            output_dir=job.output.directory,
            metric_types=job.metrics,
            alert_types=job.alert_types if job.include_alerts else [],
            time_range_hours=job.time_range_hours,
            start_time=job.start_time,
            end_time=job.end_time,
            shard_size=job.shard_size,
            cluster_ids=cluster_ids,
            include_alerts=job.include_alerts,
            batch_size=job.execution.batch_size,
            source_fields=job.source_fields,
            parallel_jobs=job.execution.parallel_metrics,
            parallel_degree=job.execution.parallel_degree,
            sampling=job.sampling,
            slim_config=job.slim,
            mask_ip=job.mask_ip,
            skip_estimation=job.execution.skip_estimation,
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
        if summary.get("start_time") or summary.get("end_time"):
            print(
                "时间范围: "
                f"{summary.get('start_time') or '自动计算'} ~ {summary.get('end_time') or '当前时间'}"
            )
        else:
            print(f"时间范围: 最近 {summary['time_range_hours']} 小时")
        print(f"分片大小: {summary.get('shard_size', 100000):,} 条/文件")
        print(f"批次大小: {summary.get('batch_size', '自适应')}")
        print(f"Scroll Keepalive: {summary.get('scroll_keepalive', DEFAULT_SCROLL_KEEPALIVE)}")
        print(f"并行度: {summary.get('parallel_jobs', 2)}")
        print(f"单指标并行度: {summary.get('parallel_degree', 1)}")

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
        description="Metrics Exporter - 从 Console 系统集群导出监控数据",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 使用命令行参数导出最近24小时的监控数据
  python metrics_exporter.py -c http://localhost:9000 -u admin -p password

  # 导出最近7天的数据，启用精简和IP脱敏
  python metrics_exporter.py -c http://localhost:9000 -u admin -p password --time-range 168 --slim --mask-ip

  # 只导出特定集群的数据
  python metrics_exporter.py -c http://localhost:9000 -u admin -p password --cluster-id xxx

  # 使用配置文件执行所有启用的 jobs
  python metrics_exporter.py --config config.json

  # 使用配置文件执行指定 job
  python metrics_exporter.py --config config.json --job "全量导出-一周"
        """,
    )

    # 添加通用参数
    parser = add_common_args(parser)

    # 命令行模式参数
    parser.add_argument(
        "--time-range",
        type=int,
        default=None,
        help="导出时间范围(小时)，默认24小时",
    )
    parser.add_argument(
        "--start-time",
        type=str,
        default=None,
        help="开始时间，格式: 'YYYY-MM-DD HH:MM:SS' 或 'YYYY-MM-DD'",
    )
    parser.add_argument(
        "--end-time",
        type=str,
        default=None,
        help="结束时间，格式: 'YYYY-MM-DD HH:MM:SS' 或 'YYYY-MM-DD'",
    )
    parser.add_argument(
        "--shard-size",
        type=int,
        default=100000,
        help="每个分片文件的最大文档数，默认100000",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="每批次读取的文档数",
    )
    parser.add_argument(
        "--scroll-keepalive",
        type=str,
        default="5m",
        help="Scroll 上下文保持时间，默认5m",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=2,
        help="并行导出的指标类型数，默认2",
    )
    parser.add_argument(
        "--parallel-degree",
        type=int,
        default=1,
        help="单个指标内并行度（sliced scroll），默认1",
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
        help="只导出指定字段，逗号分隔",
    )
    parser.add_argument(
        "--no-alerts",
        action="store_true",
        help="不导出告警数据",
    )
    parser.add_argument(
        "--slim",
        action="store_true",
        help="精简数据：删除和排障无关的字段",
    )
    parser.add_argument(
        "--mask-ip",
        action="store_true",
        help="脱敏IP地址：隐藏前两个octet，例如 192.168.1.1 -> *.*.1.1",
    )
    parser.add_argument(
        "--sampling-interval",
        type=str,
        default=None,
        help="抽样时间间隔，如 1h, 5m（不指定则为全量导出）",
    )

    # 配置文件模式参数
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
        "--list-clusters",
        action="store_true",
        help="只列出有监控数据的集群，不导出数据",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # 判断是否使用配置文件模式
    use_config_mode = args.config and Path(args.config).exists()

    # 配置文件模式：提前处理不需要连接的操作
    if use_config_mode:
        try:
            app_config = AppConfig.load(args.config)
        except ConfigValidationError as e:
            print(f"配置文件错误: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"加载配置文件失败: {e}")
            sys.exit(1)

        # --list-jobs 不需要连接 Console
        if args.list_jobs:
            _list_jobs(app_config)
            return

        # 获取连接参数
        console_url = app_config.global_config.console_url
        username = app_config.global_config.username
        password = app_config.global_config.password
        timeout = app_config.global_config.timeout
        insecure = app_config.global_config.insecure
    else:
        # 命令行模式：从环境变量或参数获取连接信息
        console_url = get_config_value(args.console, None, 'CONSOLE_URL', 'http://localhost:9000')
        username = get_config_value(args.username, None, 'CONSOLE_USERNAME', '')
        password = get_config_value(args.password, None, 'CONSOLE_PASSWORD', '')
        timeout = int(get_config_value(str(args.timeout), None, 'CONSOLE_TIMEOUT', '60'))
        insecure = args.insecure
        app_config = None

    # 连接 Console
    client = _connect_console(console_url, username, password, timeout, insecure)

    # 获取系统集群
    exporter = _get_exporter(client)

    # --list-clusters
    if args.list_clusters:
        _list_clusters(exporter)
        return

    # 执行导出
    if use_config_mode:
        _run_config_mode(exporter, app_config, args)
    else:
        _run_cli_mode(exporter, args)


def _list_jobs(app_config: AppConfig) -> None:
    """列出配置文件中的 jobs"""
    if not app_config.metrics_exporter:
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
        if job.start_time or job.end_time:
            print(f"      时间范围: {job.start_time or '自动计算'} ~ {job.end_time or '当前时间'}")
        else:
            print(f"      时间范围: {job.time_range_hours}h")


def _connect_console(console_url: str, username: str, password: str, timeout: int, insecure: bool) -> ConsoleClient:
    """连接并登录 Console"""
    if username and not password:
        password = getpass.getpass(f"请输入 {username} 的密码: ")

    print(f"连接到 Console: {console_url}")
    client = ConsoleClient(console_url, username, password, timeout=timeout, verify_ssl=not insecure)

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

    return client


def _get_exporter(client: ConsoleClient) -> 'MetricsExporter':
    """获取初始化的 MetricsExporter"""
    print("正在获取系统集群...")
    exporter = MetricsExporter(client, "")
    system_cluster_id = exporter.get_system_cluster_id()

    if not system_cluster_id:
        print("未找到系统集群，请确保 Console 系统集群已配置")
        sys.exit(1)

    print(f"系统集群ID: {system_cluster_id}")
    exporter.system_cluster_id = system_cluster_id
    return exporter


def _list_clusters(exporter: 'MetricsExporter') -> None:
    """列出有监控数据的集群"""
    clusters = exporter.get_available_clusters()
    print("\n有监控数据的集群:")
    for cluster in clusters:
        print(f"  {cluster['cluster_name']} ({cluster['cluster_id']}): {cluster['doc_count']:,} 条记录")


def _run_config_mode(exporter: 'MetricsExporter', app_config: AppConfig, args) -> None:
    """配置文件模式：执行 jobs"""
    if not app_config.metrics_exporter:
        print("配置文件中没有定义 metricsExporter.jobs")
        sys.exit(1)

    if args.job:
        jobs_to_run = [j for j in app_config.metrics_exporter.jobs if j.name == args.job]
        if not jobs_to_run:
            print(f"未找到名为 '{args.job}' 的 job")
            sys.exit(1)
    else:
        jobs_to_run = [j for j in app_config.metrics_exporter.jobs if j.enabled]

    if not jobs_to_run:
        print("没有启用的导出任务")
        sys.exit(0)

    print(f"\n将执行 {len(jobs_to_run)} 个导出任务")

    for job in jobs_to_run:
        summary = exporter.execute_job(job)
        exporter.print_summary(summary)
        print(f"\n数据已导出到: {job.output.directory}")


def _run_cli_mode(exporter: 'MetricsExporter', args) -> None:
    """命令行模式：构建 job 并执行"""
    output_dir = args.output or f"metrics_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    has_absolute_range = bool(args.start_time or args.end_time)
    if has_absolute_range and args.time_range is not None:
        print("时间参数错误: --time-range 与 --start-time/--end-time 只能二选一")
        sys.exit(1)

    if (args.start_time is None) != (args.end_time is None):
        print("时间参数错误: 使用绝对时间时必须同时提供 --start-time 和 --end-time")
        sys.exit(1)

    # 构建 job 配置
    job_config = {
        "name": "命令行导出",
        "enabled": True,
        "shardSize": args.shard_size,
        "includeAlerts": not args.no_alerts,
        "maskIp": args.mask_ip,
        "output": {"directory": output_dir},
        "execution": {
            "parallelMetrics": args.parallel,
            "parallelDegree": args.parallel_degree,
            "batchSize": args.batch_size,
            "scrollKeepalive": args.scroll_keepalive,
        },
    }

    if has_absolute_range:
        job_config["startTime"] = args.start_time
        job_config["endTime"] = args.end_time
    elif args.time_range is not None:
        job_config["timeRangeHours"] = args.time_range

    if args.metric_types:
        job_config["metrics"] = [t.strip() for t in args.metric_types.split(",")]

    if args.fields:
        job_config["sourceFields"] = [f.strip() for f in args.fields.split(",")]

    if args.slim:
        job_config["slim"] = True

    if args.sampling_interval:
        job_config["sampling"] = {"mode": "sampling", "interval": args.sampling_interval}

    if args.cluster_id:
        job_config["targets"] = {"clusters": {"include": [args.cluster_id]}}

    # 命令行提前校验时间参数，尽早反馈用户
    try:
        MetricsExporter._parse_time_input(args.start_time, is_end=False) if args.start_time else None
        MetricsExporter._parse_time_input(args.end_time, is_end=True) if args.end_time else None
    except ValueError as e:
        print(f"时间参数错误: {e}")
        sys.exit(1)

    job = MetricsJobConfig.from_dict(job_config)

    # 执行导出
    summary = exporter.execute_job(job)
    exporter.print_summary(summary)
    print(f"\n数据已导出到: {output_dir}")


if __name__ == "__main__":
    main()
