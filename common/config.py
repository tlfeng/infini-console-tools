#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置管理公共模块

统一的配置文件加载、环境变量支持和参数解析
支持新的 metricsExporter jobs 配置格式
"""

import argparse
import json
import os
import re
import fnmatch
from typing import Dict, Any, Optional, List, Set
from pathlib import Path
from collections import Counter
from dataclasses import dataclass, field


def load_config_file(config_path: str) -> Dict[str, Any]:
    """加载 JSON 配置文件"""
    if not config_path or not Path(config_path).exists():
        return {}

    with open(config_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    return data if isinstance(data, dict) else {}


def get_config_value(
    cli_value: Optional[str],
    config_value: Optional[str],
    env_var: str,
    default: Optional[str] = None
) -> Optional[str]:
    """获取配置值，优先级：CLI > 配置文件 > 环境变量 > 默认值"""
    if cli_value is not None and cli_value != '':
        return cli_value
    if config_value is not None and config_value != '':
        return config_value
    return os.getenv(env_var, default)


class ConfigValidationError(Exception):
    """配置校验错误"""
    pass


@dataclass
class TargetFilter:
    """目标筛选配置"""
    include: List[str] = field(default_factory=list)
    exclude: List[str] = field(default_factory=list)

    def matches(self, value: str) -> bool:
        """检查值是否匹配筛选条件"""
        # 如果没有 include，默认匹配所有
        if not self.include:
            included = True
        else:
            included = any(self._pattern_match(p, value) for p in self.include)

        # 检查 exclude
        if included and self.exclude:
            excluded = any(self._pattern_match(p, value) for p in self.exclude)
            return not excluded

        return included

    @staticmethod
    def _pattern_match(pattern: str, value: str) -> bool:
        return fnmatch.fnmatch(value, pattern)


@dataclass
class TargetsConfig:
    """目标配置"""
    clusters: Optional[TargetFilter] = None
    nodes: Optional[TargetFilter] = None
    indices: Optional[TargetFilter] = None

    @classmethod
    def from_dict(cls, data: Dict) -> 'TargetsConfig':
        if not data:
            return cls()

        def parse_filter(d: Optional[Dict]) -> Optional[TargetFilter]:
            if not d:
                return None
            return TargetFilter(
                include=d.get('include', []),
                exclude=d.get('exclude', [])
            )

        return cls(
            clusters=parse_filter(data.get('clusters')),
            nodes=parse_filter(data.get('nodes')),
            indices=parse_filter(data.get('indices'))
        )


@dataclass
class SlimConfig:
    """精简数据配置 - 删除不必要的字段"""
    enabled: bool = False
    # 删除和排障无关的字段
    remove_meta: bool = True  # _id, agent, metadata.category/datatype/name
    # 删除冗余的人类可读格式字段（保留 *_in_bytes）
    remove_human_readable: bool = True

    # 预定义的要删除的字段
    META_FIELDS = {"_id", "agent"}
    META_PREFIXES = ("metadata.category", "metadata.datatype", "metadata.name")
    HUMAN_READABLE_FIELDS = {"store", "estimated_size", "limit_size"}

    @classmethod
    def from_dict(cls, data: Optional[Dict]) -> 'SlimConfig':
        if not data:
            return cls()

        # 支持 enabled=True 或直接传 slim: true
        if isinstance(data, bool):
            return cls(enabled=data)

        return cls(
            enabled=data.get('enabled', False),
            remove_meta=data.get('removeMeta', True),
            remove_human_readable=data.get('removeHumanReadable', True)
        )

    def get_fields_to_remove(self) -> tuple:
        """返回要删除的字段集合"""
        meta_fields = set()
        human_readable_fields = set()

        if self.enabled:
            if self.remove_meta:
                meta_fields = self.META_FIELDS.copy()
                # metadata 下的字段需要在处理时特殊处理
            if self.remove_human_readable:
                human_readable_fields = self.HUMAN_READABLE_FIELDS.copy()

        return meta_fields, human_readable_fields, self.META_PREFIXES


@dataclass
class SamplingConfig:
    """抽样配置"""
    mode: str = "full"  # full 或 sampling
    interval: Optional[str] = None  # 如 "1h", "5m" 表示时间桶间隔

    @classmethod
    def from_dict(cls, data: Optional[Dict]) -> 'SamplingConfig':
        if not data:
            return cls()

        mode = data.get('mode', 'full')
        if mode not in ('full', 'sampling'):
            raise ConfigValidationError(f"无效的采样模式: {mode}，必须是 'full' 或 'sampling'")

        interval = data.get('interval')

        if mode == 'sampling':
            if interval is None:
                raise ConfigValidationError("sampling 模式需要指定 interval")

        return cls(mode=mode, interval=interval)

    def is_sampling(self) -> bool:
        return self.mode == 'sampling'


# 字段聚合策略类型
class FieldAggStrategy:
    """字段聚合策略常量"""
    RATE = "rate"  # 导数字段：delta / bucket_size
    RATIO = "ratio"  # 比率字段：delta(numerator) / delta(denominator)
    LATENCY = "latency"  # 延迟字段：delta(total_time) / delta(count)
    MAX = "max"  # 取最大值（如线程池、连接数等瞬时值）
    LATEST = "latest"  # 取最新值（默认策略）


# 指标类型字段聚合配置
# 定义哪些字段需要使用特定的聚合策略
METRIC_FIELD_AGG_CONFIG: Dict[str, Dict[str, Any]] = {
    "node_stats": {
        # Rate 型字段：累积计数器，计算 delta / bucket_size
        # 公式: (current_value - previous_value) / interval_seconds
        "rate_fields": {
            # 索引操作相关
            "payload.elasticsearch.node_stats.indices.indexing.index_total": {"unit": "ops/sec", "desc": "索引操作速率(ops/sec)"},
            "payload.elasticsearch.node_stats.indices.indexing.index_time_in_millis": {"unit": "ms/sec", "desc": "索引耗时速率"},
            "payload.elasticsearch.node_stats.indices.indexing.delete_total": {"unit": "ops/sec", "desc": "删除操作速率"},
            "payload.elasticsearch.node_stats.indices.indexing.delete_time_in_millis": {"unit": "ms/sec", "desc": "删除耗时速率"},
            "payload.elasticsearch.node_stats.indices.indexing.noop_update_total": {"unit": "ops/sec", "desc": "空更新速率"},
            "payload.elasticsearch.node_stats.indices.indexing.throttle_time_in_millis": {"unit": "ms/sec", "desc": "限流时间速率"},
            # 查询相关
            "payload.elasticsearch.node_stats.indices.search.query_total": {"unit": "queries/sec", "desc": "查询速率(QPS)"},
            "payload.elasticsearch.node_stats.indices.search.query_time_in_millis": {"unit": "ms/sec", "desc": "查询耗时速率"},
            "payload.elasticsearch.node_stats.indices.search.fetch_total": {"unit": "ops/sec", "desc": "Fetch操作速率"},
            "payload.elasticsearch.node_stats.indices.search.fetch_time_in_millis": {"unit": "ms/sec", "desc": "Fetch耗时速率"},
            "payload.elasticsearch.node_stats.indices.search.scroll_total": {"unit": "ops/sec", "desc": "滚动查询速率"},
            "payload.elasticsearch.node_stats.indices.search.scroll_time_in_millis": {"unit": "ms/sec", "desc": "滚动查询耗时速率"},
            "payload.elasticsearch.node_stats.indices.search.suggest_total": {"unit": "ops/sec", "desc": "Suggest操作速率"},
            "payload.elasticsearch.node_stats.indices.search.suggest_time_in_millis": {"unit": "ms/sec", "desc": "Suggest耗时速率"},
            # 段合并相关
            "payload.elasticsearch.node_stats.indices.merges.total": {"unit": "merges/sec", "desc": "合并操作速率"},
            "payload.elasticsearch.node_stats.indices.merges.total_time_in_millis": {"unit": "ms/sec", "desc": "合并耗时速率"},
            "payload.elasticsearch.node_stats.indices.merges.total_docs": {"unit": "docs/sec", "desc": "合并文档数速率"},
            "payload.elasticsearch.node_stats.indices.merges.total_size_in_bytes": {"unit": "bytes/sec", "desc": "合并数据量速率"},
            "payload.elasticsearch.node_stats.indices.merges.total_stopped_time_in_millis": {"unit": "ms/sec", "desc": "合并停止时间速率"},
            "payload.elasticsearch.node_stats.indices.merges.total_throttled_time_in_millis": {"unit": "ms/sec", "desc": "合并限流时间速率"},
            # 刷新相关
            "payload.elasticsearch.node_stats.indices.refresh.total": {"unit": "ops/sec", "desc": "刷新操作速率"},
            "payload.elasticsearch.node_stats.indices.refresh.total_time_in_millis": {"unit": "ms/sec", "desc": "刷新耗时速率"},
            # Flush 相关
            "payload.elasticsearch.node_stats.indices.flush.total": {"unit": "ops/sec", "desc": "Flush操作速率"},
            "payload.elasticsearch.node_stats.indices.flush.total_time_in_millis": {"unit": "ms/sec", "desc": "Flush耗时速率"},
            # GET 操作相关
            "payload.elasticsearch.node_stats.indices.get.total": {"unit": "ops/sec", "desc": "GET操作速率"},
            "payload.elasticsearch.node_stats.indices.get.time_in_millis": {"unit": "ms/sec", "desc": "GET耗时速率"},
            "payload.elasticsearch.node_stats.indices.get.exists_total": {"unit": "ops/sec", "desc": "EXISTS操作速率"},
            "payload.elasticsearch.node_stats.indices.get.exists_time_in_millis": {"unit": "ms/sec", "desc": "EXISTS耗时速率"},
            "payload.elasticsearch.node_stats.indices.get.missing_total": {"unit": "ops/sec", "desc": "GET失败速率"},
            "payload.elasticsearch.node_stats.indices.get.missing_time_in_millis": {"unit": "ms/sec", "desc": "GET失败耗时速率"},
            # 缓存相关
            "payload.elasticsearch.node_stats.indices.request_cache.hit_count": {"unit": "hits/sec", "desc": "请求缓存命中速率"},
            "payload.elasticsearch.node_stats.indices.request_cache.miss_count": {"unit": "misses/sec", "desc": "请求缓存失败速率"},
            "payload.elasticsearch.node_stats.indices.query_cache.hit_count": {"unit": "hits/sec", "desc": "查询缓存命中速率"},
            "payload.elasticsearch.node_stats.indices.query_cache.miss_count": {"unit": "misses/sec", "desc": "查询缓存失败速率"},
            "payload.elasticsearch.node_stats.indices.fielddata.evictions": {"unit": "ops/sec", "desc": "字段缓存驱逐速率"},
            "payload.elasticsearch.node_stats.indices.query_cache.evictions": {"unit": "ops/sec", "desc": "查询缓存驱逐速率"},
            # Warmer 相关
            "payload.elasticsearch.node_stats.indices.warmer.total": {"unit": "ops/sec", "desc": "Warmer操作速率"},
            "payload.elasticsearch.node_stats.indices.warmer.total_time_in_millis": {"unit": "ms/sec", "desc": "Warmer耗时速率"},
            # 恢复相关
            "payload.elasticsearch.node_stats.indices.recovery.throttle_time_in_millis": {"unit": "ms/sec", "desc": "恢复限流时间速率"},
            # 事务日志相关
            "payload.elasticsearch.node_stats.indices.translog.operations": {"unit": "ops", "desc": "事务日志操作数"},
            "payload.elasticsearch.node_stats.indices.translog.size_in_bytes": {"unit": "bytes", "desc": "事务日志大小"},
            "payload.elasticsearch.node_stats.indices.translog.uncommitted_operations": {"unit": "ops", "desc": "未提交操作数"},
            "payload.elasticsearch.node_stats.indices.translog.uncommitted_size_in_bytes": {"unit": "bytes", "desc": "未提交日志大小"},
            # 网络相关
            "payload.elasticsearch.node_stats.transport.rx_count": {"unit": "packets/sec", "desc": "接收包速率"},
            "payload.elasticsearch.node_stats.transport.rx_size_in_bytes": {"unit": "bytes/sec", "desc": "接收数据量(bytes/sec)"},
            "payload.elasticsearch.node_stats.transport.tx_count": {"unit": "packets/sec", "desc": "发送包速率"},
            "payload.elasticsearch.node_stats.transport.tx_size_in_bytes": {"unit": "bytes/sec", "desc": "发送数据量(bytes/sec)"},
            # HTTP 连接
            "payload.elasticsearch.node_stats.http.total_opened": {"unit": "connections/sec", "desc": "HTTP连接开启速率"},
            # 文件系统 IO
            "payload.elasticsearch.node_stats.fs.io_stats.total.read_operations": {"unit": "ops/sec", "desc": "磁盘读操作速率"},
            "payload.elasticsearch.node_stats.fs.io_stats.total.read_kilobytes": {"unit": "KB/sec", "desc": "磁盘读吞吐量"},
            "payload.elasticsearch.node_stats.fs.io_stats.total.write_operations": {"unit": "ops/sec", "desc": "磁盘写操作速率"},
            "payload.elasticsearch.node_stats.fs.io_stats.total.write_kilobytes": {"unit": "KB/sec", "desc": "磁盘写吞吐量"},
            # 进程 CPU
            "payload.elasticsearch.node_stats.process.cpu.total_in_millis": {"unit": "ms/sec", "desc": "进程CPU累计耗时"},
        },
        # Latency 型字段：计算 delta(time) / delta(count)
        "latency_fields": {
            "payload.elasticsearch.node_stats.indices.search.query_latency": {
                "time_field": "payload.elasticsearch.node_stats.indices.search.query_time_in_millis",
                "count_field": "payload.elasticsearch.node_stats.indices.search.query_total",
                "unit": "ms",
                "desc": "平均查询延迟(ms)"
            },
            "payload.elasticsearch.node_stats.indices.indexing.index_latency": {
                "time_field": "payload.elasticsearch.node_stats.indices.indexing.index_time_in_millis",
                "count_field": "payload.elasticsearch.node_stats.indices.indexing.index_total",
                "unit": "ms",
                "desc": "平均索引延迟(ms)"
            },
            "payload.elasticsearch.node_stats.indices.search.fetch_latency_ms": {
                "time_field": "payload.elasticsearch.node_stats.indices.search.fetch_time_in_millis",
                "count_field": "payload.elasticsearch.node_stats.indices.search.fetch_total",
                "unit": "ms",
                "desc": "平均Fetch延迟(ms)"
            },
            "payload.elasticsearch.node_stats.indices.indexing.delete_latency_ms": {
                "time_field": "payload.elasticsearch.node_stats.indices.indexing.delete_time_in_millis",
                "count_field": "payload.elasticsearch.node_stats.indices.indexing.delete_total",
                "unit": "ms",
                "desc": "平均删除延迟(ms)"
            },
            "payload.elasticsearch.node_stats.indices.get.latency_ms": {
                "time_field": "payload.elasticsearch.node_stats.indices.get.time_in_millis",
                "count_field": "payload.elasticsearch.node_stats.indices.get.total",
                "unit": "ms",
                "desc": "平均GET延迟(ms)"
            },
        },
        # Max 型字段：取最大值（瞬时值类）
        # 用于 JVM 内存、线程池大小等
        "max_fields": {
            # JVM 内存（瞬时值，取峰值）
            "payload.elasticsearch.node_stats.jvm.mem.heap_used_in_bytes": {"unit": "bytes", "desc": "堆内存使用峰值"},
            "payload.elasticsearch.node_stats.jvm.mem.heap_used_percent": {"unit": "%", "desc": "堆内存使用率峰值"},
            "payload.elasticsearch.node_stats.jvm.mem.heap_committed_in_bytes": {"unit": "bytes", "desc": "堆内存已提交"},
            "payload.elasticsearch.node_stats.jvm.mem.heap_max_in_bytes": {"unit": "bytes", "desc": "堆内存最大值"},
            "payload.elasticsearch.node_stats.jvm.mem.non_heap_used_in_bytes": {"unit": "bytes", "desc": "非堆内存使用"},
            "payload.elasticsearch.node_stats.jvm.mem.non_heap_committed_in_bytes": {"unit": "bytes", "desc": "非堆内存已提交"},
            # GC 统计
            "payload.elasticsearch.node_stats.jvm.gc.collectors.young.collection_count": {"unit": "count", "desc": "Young GC次数"},
            "payload.elasticsearch.node_stats.jvm.gc.collectors.young.collection_time_in_millis": {"unit": "ms", "desc": "Young GC总耗时"},
            "payload.elasticsearch.node_stats.jvm.gc.collectors.old.collection_count": {"unit": "count", "desc": "Full GC次数"},
            "payload.elasticsearch.node_stats.jvm.gc.collectors.old.collection_time_in_millis": {"unit": "ms", "desc": "Full GC总耗时"},
            # 线程池（瞬时值）
            "payload.elasticsearch.node_stats.thread_pool.bulk.queue": {"unit": "count", "desc": "Bulk线程池排队数"},
            "payload.elasticsearch.node_stats.thread_pool.bulk.active": {"unit": "count", "desc": "Bulk活跃线程数"},
            "payload.elasticsearch.node_stats.thread_pool.bulk.largest": {"unit": "count", "desc": "Bulk最大线程数"},
            "payload.elasticsearch.node_stats.thread_pool.write.queue": {"unit": "count", "desc": "Write线程池排队数"},
            "payload.elasticsearch.node_stats.thread_pool.write.active": {"unit": "count", "desc": "Write活跃线程数"},
            "payload.elasticsearch.node_stats.thread_pool.write.largest": {"unit": "count", "desc": "Write最大线程数"},
            "payload.elasticsearch.node_stats.thread_pool.search.queue": {"unit": "count", "desc": "Search线程池排队数"},
            "payload.elasticsearch.node_stats.thread_pool.search.active": {"unit": "count", "desc": "Search活跃线程数"},
            "payload.elasticsearch.node_stats.thread_pool.search.largest": {"unit": "count", "desc": "Search最大线程数"},
            "payload.elasticsearch.node_stats.thread_pool.get.queue": {"unit": "count", "desc": "Get线程池排队数"},
            "payload.elasticsearch.node_stats.thread_pool.get.active": {"unit": "count", "desc": "Get活跃线程数"},
            "payload.elasticsearch.node_stats.thread_pool.get.largest": {"unit": "count", "desc": "Get最大线程数"},
            # 系统负载
            "payload.elasticsearch.node_stats.os.cpu.percent": {"unit": "%", "desc": "CPU使用率"},
            "payload.elasticsearch.node_stats.os.cpu.load_average.1m": {"unit": "load", "desc": "1分钟负载"},
            "payload.elasticsearch.node_stats.os.cpu.load_average.5m": {"unit": "load", "desc": "5分钟负载"},
            "payload.elasticsearch.node_stats.os.cpu.load_average.15m": {"unit": "load", "desc": "15分钟负载"},
            # 文件系统
            "payload.elasticsearch.node_stats.fs.total.free_in_bytes": {"unit": "bytes", "desc": "可用磁盘空间"},
            "payload.elasticsearch.node_stats.fs.total.available_in_bytes": {"unit": "bytes", "desc": "磁盘可用空间"},
            "payload.elasticsearch.node_stats.fs.total.total_in_bytes": {"unit": "bytes", "desc": "磁盘总大小"},
            # 缓存大小
            "payload.elasticsearch.node_stats.indices.query_cache.memory_size_in_bytes": {"unit": "bytes", "desc": "查询缓存内存"},
            "payload.elasticsearch.node_stats.indices.fielddata.memory_size_in_bytes": {"unit": "bytes", "desc": "字段缓存内存"},
            "payload.elasticsearch.node_stats.indices.request_cache.memory_size_in_bytes": {"unit": "bytes", "desc": "请求缓存内存"},
            # 分片和索引
            "payload.elasticsearch.node_stats.indices.segments.count": {"unit": "count", "desc": "段数量"},
            "payload.elasticsearch.node_stats.indices.segments.memory_in_bytes": {"unit": "bytes", "desc": "段内存占用"},
            "payload.elasticsearch.node_stats.indices.store.size_in_bytes": {"unit": "bytes", "desc": "索引存储大小"},

            # cgroup 数值字段（瞬时值）
            "payload.elasticsearch.node_stats.os.cgroup.cpu.cfs_period_micros": {"unit": "micros", "desc": "cgroup cpu period"},
            "payload.elasticsearch.node_stats.os.cgroup.cpu.cfs_quota_micros": {"unit": "micros", "desc": "cgroup cpu quota"},
            "payload.elasticsearch.node_stats.os.cgroup.cpu.stat.number_of_elapsed_periods": {"unit": "count", "desc": "cgroup cpu elapsed periods"},
            "payload.elasticsearch.node_stats.os.cgroup.cpu.stat.number_of_times_throttled": {"unit": "count", "desc": "cgroup cpu throttled 次数"},
            "payload.elasticsearch.node_stats.os.cgroup.cpu.stat.time_throttled_nanos": {"unit": "nanos", "desc": "cgroup cpu throttled 时间"},
            "payload.elasticsearch.node_stats.os.cgroup.cpuacct.usage_nanos": {"unit": "nanos", "desc": "cgroup cpu 使用时间"},
            "payload.elasticsearch.node_stats.os.cgroup.memory.limit_in_bytes": {"unit": "bytes", "desc": "cgroup 内存限制"},
            "payload.elasticsearch.node_stats.os.cgroup.memory.usage_in_bytes": {"unit": "bytes", "desc": "cgroup 内存使用"},

            # 线程池补齐（瞬时值）
            "payload.elasticsearch.node_stats.thread_pool.ccr.active": {"unit": "count", "desc": "CCR活跃线程数"},
            "payload.elasticsearch.node_stats.thread_pool.ccr.queue": {"unit": "count", "desc": "CCR排队数"},
            "payload.elasticsearch.node_stats.thread_pool.ccr.largest": {"unit": "count", "desc": "CCR最大线程数"},
            "payload.elasticsearch.node_stats.thread_pool.ccr.threads": {"unit": "count", "desc": "CCR线程数"},
            "payload.elasticsearch.node_stats.thread_pool.index.active": {"unit": "count", "desc": "Index活跃线程数"},
            "payload.elasticsearch.node_stats.thread_pool.index.queue": {"unit": "count", "desc": "Index排队数"},
            "payload.elasticsearch.node_stats.thread_pool.index.largest": {"unit": "count", "desc": "Index最大线程数"},
            "payload.elasticsearch.node_stats.thread_pool.index.threads": {"unit": "count", "desc": "Index线程数"},
            "payload.elasticsearch.node_stats.thread_pool.ml_autodetect.active": {"unit": "count", "desc": "ML autodetect活跃线程数"},
            "payload.elasticsearch.node_stats.thread_pool.ml_autodetect.queue": {"unit": "count", "desc": "ML autodetect排队数"},
            "payload.elasticsearch.node_stats.thread_pool.ml_autodetect.largest": {"unit": "count", "desc": "ML autodetect最大线程数"},
            "payload.elasticsearch.node_stats.thread_pool.ml_autodetect.threads": {"unit": "count", "desc": "ML autodetect线程数"},
            "payload.elasticsearch.node_stats.thread_pool.ml_datafeed.active": {"unit": "count", "desc": "ML datafeed活跃线程数"},
            "payload.elasticsearch.node_stats.thread_pool.ml_datafeed.queue": {"unit": "count", "desc": "ML datafeed排队数"},
            "payload.elasticsearch.node_stats.thread_pool.ml_datafeed.largest": {"unit": "count", "desc": "ML datafeed最大线程数"},
            "payload.elasticsearch.node_stats.thread_pool.ml_datafeed.threads": {"unit": "count", "desc": "ML datafeed线程数"},
            "payload.elasticsearch.node_stats.thread_pool.ml_utility.active": {"unit": "count", "desc": "ML utility活跃线程数"},
            "payload.elasticsearch.node_stats.thread_pool.ml_utility.queue": {"unit": "count", "desc": "ML utility排队数"},
            "payload.elasticsearch.node_stats.thread_pool.ml_utility.largest": {"unit": "count", "desc": "ML utility最大线程数"},
            "payload.elasticsearch.node_stats.thread_pool.ml_utility.threads": {"unit": "count", "desc": "ML utility线程数"},
            "payload.elasticsearch.node_stats.thread_pool.rollup_indexing.active": {"unit": "count", "desc": "Rollup活跃线程数"},
            "payload.elasticsearch.node_stats.thread_pool.rollup_indexing.queue": {"unit": "count", "desc": "Rollup排队数"},
            "payload.elasticsearch.node_stats.thread_pool.rollup_indexing.largest": {"unit": "count", "desc": "Rollup最大线程数"},
            "payload.elasticsearch.node_stats.thread_pool.rollup_indexing.threads": {"unit": "count", "desc": "Rollup线程数"},
            "payload.elasticsearch.node_stats.thread_pool.security-token-key.active": {"unit": "count", "desc": "Security token活跃线程数"},
            "payload.elasticsearch.node_stats.thread_pool.security-token-key.queue": {"unit": "count", "desc": "Security token排队数"},
            "payload.elasticsearch.node_stats.thread_pool.security-token-key.largest": {"unit": "count", "desc": "Security token最大线程数"},
            "payload.elasticsearch.node_stats.thread_pool.security-token-key.threads": {"unit": "count", "desc": "Security token线程数"},
            "payload.elasticsearch.node_stats.thread_pool.watcher.active": {"unit": "count", "desc": "Watcher活跃线程数"},
            "payload.elasticsearch.node_stats.thread_pool.watcher.queue": {"unit": "count", "desc": "Watcher排队数"},
            "payload.elasticsearch.node_stats.thread_pool.watcher.largest": {"unit": "count", "desc": "Watcher最大线程数"},
            "payload.elasticsearch.node_stats.thread_pool.watcher.threads": {"unit": "count", "desc": "Watcher线程数"},
        },
        # 其余字段（字符串、数组、结构体等）自动 fallback 到 latest 快照值，无需显式列出
    },
    "index_stats": {
        # Rate 型字段：累积计数器，计算 delta / bucket_size  
        "rate_fields": {
            # 索引操作
            "payload.elasticsearch.index_stats.total.indexing.index_total": {"unit": "ops/sec", "desc": "每秒索引操作数"},
            "payload.elasticsearch.index_stats.total.indexing.index_time_in_millis": {"unit": "ms/sec", "desc": "索引累计耗时"},
            "payload.elasticsearch.index_stats.total.indexing.delete_total": {"unit": "ops/sec", "desc": "每秒删除操作数"},
            "payload.elasticsearch.index_stats.total.indexing.delete_time_in_millis": {"unit": "ms/sec", "desc": "删除累计耗时"},
            "payload.elasticsearch.index_stats.total.indexing.noop_update_total": {"unit": "ops/sec", "desc": "每秒空更新数"},
            "payload.elasticsearch.index_stats.total.indexing.throttle_time_in_millis": {"unit": "ms/sec", "desc": "索引限流时间"},
            # 查询操作
            "payload.elasticsearch.index_stats.total.search.query_total": {"unit": "queries/sec", "desc": "每秒查询数(QPS)"},
            "payload.elasticsearch.index_stats.total.search.query_time_in_millis": {"unit": "ms/sec", "desc": "查询累计耗时"},
            "payload.elasticsearch.index_stats.total.search.fetch_total": {"unit": "ops/sec", "desc": "每秒Fetch操作数"},
            "payload.elasticsearch.index_stats.total.search.fetch_time_in_millis": {"unit": "ms/sec", "desc": "Fetch累计耗时"},
            "payload.elasticsearch.index_stats.total.search.scroll_total": {"unit": "ops/sec", "desc": "每秒Scroll操作数"},
            "payload.elasticsearch.index_stats.total.search.scroll_time_in_millis": {"unit": "ms/sec", "desc": "Scroll累计耗时"},
            # GET操作
            "payload.elasticsearch.index_stats.total.get.total": {"unit": "ops/sec", "desc": "每秒GET操作数"},
            "payload.elasticsearch.index_stats.total.get.time_in_millis": {"unit": "ms/sec", "desc": "GET累计耗时"},
            "payload.elasticsearch.index_stats.total.get.exists_total": {"unit": "ops/sec", "desc": "每秒EXISTS操作数"},
            "payload.elasticsearch.index_stats.total.get.exists_time_in_millis": {"unit": "ms/sec", "desc": "EXISTS累计耗时"},
            "payload.elasticsearch.index_stats.total.get.missing_total": {"unit": "ops/sec", "desc": "每秒missing操作数"},
            "payload.elasticsearch.index_stats.total.get.missing_time_in_millis": {"unit": "ms/sec", "desc": "missing累计耗时"},
            # Refresh
            "payload.elasticsearch.index_stats.total.refresh.total": {"unit": "ops/sec", "desc": "每秒Refresh操作数"},
            "payload.elasticsearch.index_stats.total.refresh.total_time_in_millis": {"unit": "ms/sec", "desc": "Refresh累计耗时"},
            # Flush  
            "payload.elasticsearch.index_stats.total.flush.total": {"unit": "ops/sec", "desc": "每秒Flush操作数"},
            "payload.elasticsearch.index_stats.total.flush.total_time_in_millis": {"unit": "ms/sec", "desc": "Flush累计耗时"},
            # Merge
            "payload.elasticsearch.index_stats.total.merges.total": {"unit": "ops/sec", "desc": "每秒Merge操作数"},
            "payload.elasticsearch.index_stats.total.merges.total_time_in_millis": {"unit": "ms/sec", "desc": "Merge累计耗时"},
            "payload.elasticsearch.index_stats.total.merges.total_docs": {"unit": "docs/sec", "desc": "每秒Merge文档数"},
            "payload.elasticsearch.index_stats.total.merges.total_size_in_bytes": {"unit": "bytes/sec", "desc": "Merge数据量速率"},
        },
        # Latency 型字段：计算 delta(time) / delta(count)
        "latency_fields": {
            "payload.elasticsearch.index_stats.total.search.query_latency": {
                "time_field": "payload.elasticsearch.index_stats.total.search.query_time_in_millis",
                "count_field": "payload.elasticsearch.index_stats.total.search.query_total",
                "unit": "ms",
                "desc": "平均查询延迟(ms)"
            },
            "payload.elasticsearch.index_stats.total.indexing.index_latency": {
                "time_field": "payload.elasticsearch.index_stats.total.indexing.index_time_in_millis",
                "count_field": "payload.elasticsearch.index_stats.total.indexing.index_total",
                "unit": "ms",
                "desc": "平均索引延迟(ms)"
            },
            "payload.elasticsearch.index_stats.total.search.fetch_latency_ms": {
                "time_field": "payload.elasticsearch.index_stats.total.search.fetch_time_in_millis",
                "count_field": "payload.elasticsearch.index_stats.total.search.fetch_total",
                "unit": "ms",
                "desc": "平均Fetch延迟(ms)"
            },
            "payload.elasticsearch.index_stats.total.get.latency_ms": {
                "time_field": "payload.elasticsearch.index_stats.total.get.time_in_millis",
                "count_field": "payload.elasticsearch.index_stats.total.get.total",
                "unit": "ms",
                "desc": "平均GET延迟(ms)"
            },
        },
        # Max 型字段（瞬时值）
        "max_fields": {
            # 文档统计
            "payload.elasticsearch.index_stats.total.docs.count": {"unit": "docs", "desc": "文档总数"},
            "payload.elasticsearch.index_stats.total.docs.deleted": {"unit": "docs", "desc": "删除文档数"},
            # 存储大小
            "payload.elasticsearch.index_stats.total.store.size_in_bytes": {"unit": "bytes", "desc": "总存储大小(含副本)"},
            "payload.elasticsearch.index_stats.primaries.store.size_in_bytes": {"unit": "bytes", "desc": "主分片存储大小"},
            # 主分片文档
            "payload.elasticsearch.index_stats.primaries.docs.count": {"unit": "docs", "desc": "主分片文档数"},
            "payload.elasticsearch.index_stats.primaries.docs.deleted": {"unit": "docs", "desc": "主分片删除数"},
            # 段和缓存
            "payload.elasticsearch.index_stats.total.segments.count": {"unit": "count", "desc": "段数量"},
            "payload.elasticsearch.index_stats.total.segments.memory_in_bytes": {"unit": "bytes", "desc": "段内存占用"},
            "payload.elasticsearch.index_stats.primaries.segments.count": {"unit": "count", "desc": "主分片段数"},
            "payload.elasticsearch.index_stats.total.query_cache.memory_size_in_bytes": {"unit": "bytes", "desc": "查询缓存内存"},
            "payload.elasticsearch.index_stats.total.fielddata.memory_size_in_bytes": {"unit": "bytes", "desc": "字段缓存内存"},
            "payload.elasticsearch.index_stats.total.request_cache.memory_size_in_bytes": {"unit": "bytes", "desc": "请求缓存内存"},
        },
        # 其余字段（字符串、数组、结构体等）自动 fallback 到 latest 快照值，无需显式列出
    },
    "cluster_health": {
        # cluster_health 以瞬时值为主，数值取 max，字符串走 latest
        "rate_fields": {},
        "latency_fields": {},
        "max_fields": {
            "payload.elasticsearch.cluster_health.number_of_nodes": {"unit": "count", "desc": "节点数"},
            "payload.elasticsearch.cluster_health.number_of_data_nodes": {"unit": "count", "desc": "数据节点数"},
            "payload.elasticsearch.cluster_health.active_primary_shards": {"unit": "count", "desc": "主分片数"},
            "payload.elasticsearch.cluster_health.active_shards": {"unit": "count", "desc": "活跃分片数"},
            "payload.elasticsearch.cluster_health.relocating_shards": {"unit": "count", "desc": "迁移分片数"},
            "payload.elasticsearch.cluster_health.initializing_shards": {"unit": "count", "desc": "初始化分片数"},
            "payload.elasticsearch.cluster_health.unassigned_shards": {"unit": "count", "desc": "未分配分片数"},
            "payload.elasticsearch.cluster_health.delayed_unassigned_shards": {"unit": "count", "desc": "延迟未分配分片数"},
            "payload.elasticsearch.cluster_health.number_of_pending_tasks": {"unit": "count", "desc": "待处理任务数"},
            "payload.elasticsearch.cluster_health.number_of_in_flight_fetch": {"unit": "count", "desc": "in-flight fetch数"},
            "payload.elasticsearch.cluster_health.task_max_waiting_in_queue_millis": {"unit": "ms", "desc": "任务队列最长等待"},
            "payload.elasticsearch.cluster_health.active_shards_percent_as_number": {"unit": "%", "desc": "活跃分片占比"},
        },
        # 其余字段（字符串、数组、结构体等）自动 fallback 到 latest 快照值，无需显式列出
    },
    "cluster_stats": {
        # cluster_stats 主体是计数器快照，取 max 保留峰值
        "rate_fields": {},
        "latency_fields": {},
        "max_fields": {
            # indices
            "payload.elasticsearch.cluster_stats.indices.count": {"unit": "count", "desc": "索引数"},
            "payload.elasticsearch.cluster_stats.indices.docs.count": {"unit": "docs", "desc": "文档数"},
            "payload.elasticsearch.cluster_stats.indices.docs.deleted": {"unit": "docs", "desc": "删除文档数"},
            "payload.elasticsearch.cluster_stats.indices.store.size_in_bytes": {"unit": "bytes", "desc": "存储大小"},
            "payload.elasticsearch.cluster_stats.indices.fielddata.memory_size_in_bytes": {"unit": "bytes", "desc": "fielddata内存"},
            "payload.elasticsearch.cluster_stats.indices.fielddata.evictions": {"unit": "count", "desc": "fielddata驱逐"},
            "payload.elasticsearch.cluster_stats.indices.query_cache.memory_size_in_bytes": {"unit": "bytes", "desc": "query cache内存"},
            "payload.elasticsearch.cluster_stats.indices.query_cache.total_count": {"unit": "count", "desc": "query cache总请求"},
            "payload.elasticsearch.cluster_stats.indices.query_cache.hit_count": {"unit": "count", "desc": "query cache命中"},
            "payload.elasticsearch.cluster_stats.indices.query_cache.miss_count": {"unit": "count", "desc": "query cache未命中"},
            "payload.elasticsearch.cluster_stats.indices.query_cache.cache_count": {"unit": "count", "desc": "query cache写入"},
            "payload.elasticsearch.cluster_stats.indices.query_cache.cache_size": {"unit": "count", "desc": "query cache当前条目"},
            "payload.elasticsearch.cluster_stats.indices.query_cache.evictions": {"unit": "count", "desc": "query cache驱逐"},
            "payload.elasticsearch.cluster_stats.indices.completion.size_in_bytes": {"unit": "bytes", "desc": "completion内存"},
            "payload.elasticsearch.cluster_stats.indices.segments.count": {"unit": "count", "desc": "segments数量"},
            "payload.elasticsearch.cluster_stats.indices.segments.memory_in_bytes": {"unit": "bytes", "desc": "segments内存"},
            "payload.elasticsearch.cluster_stats.indices.segments.terms_memory_in_bytes": {"unit": "bytes", "desc": "terms内存"},
            "payload.elasticsearch.cluster_stats.indices.segments.stored_fields_memory_in_bytes": {"unit": "bytes", "desc": "stored_fields内存"},
            "payload.elasticsearch.cluster_stats.indices.segments.term_vectors_memory_in_bytes": {"unit": "bytes", "desc": "term_vectors内存"},
            "payload.elasticsearch.cluster_stats.indices.segments.norms_memory_in_bytes": {"unit": "bytes", "desc": "norms内存"},
            "payload.elasticsearch.cluster_stats.indices.segments.points_memory_in_bytes": {"unit": "bytes", "desc": "points内存"},
            "payload.elasticsearch.cluster_stats.indices.segments.doc_values_memory_in_bytes": {"unit": "bytes", "desc": "doc_values内存"},
            "payload.elasticsearch.cluster_stats.indices.segments.index_writer_memory_in_bytes": {"unit": "bytes", "desc": "index_writer内存"},
            "payload.elasticsearch.cluster_stats.indices.segments.version_map_memory_in_bytes": {"unit": "bytes", "desc": "version_map内存"},
            "payload.elasticsearch.cluster_stats.indices.segments.fixed_bit_set_memory_in_bytes": {"unit": "bytes", "desc": "fixed_bit_set内存"},

            # shards
            "payload.elasticsearch.cluster_stats.indices.shards.total": {"unit": "count", "desc": "总分片数"},
            "payload.elasticsearch.cluster_stats.indices.shards.primaries": {"unit": "count", "desc": "主分片数"},
            "payload.elasticsearch.cluster_stats.indices.shards.replication": {"unit": "ratio", "desc": "副本系数"},
            "payload.elasticsearch.cluster_stats.indices.shards.index.shards.min": {"unit": "count", "desc": "单索引最小分片"},
            "payload.elasticsearch.cluster_stats.indices.shards.index.shards.max": {"unit": "count", "desc": "单索引最大分片"},
            "payload.elasticsearch.cluster_stats.indices.shards.index.shards.avg": {"unit": "count", "desc": "单索引平均分片"},
            "payload.elasticsearch.cluster_stats.indices.shards.index.primaries.min": {"unit": "count", "desc": "单索引最小主分片"},
            "payload.elasticsearch.cluster_stats.indices.shards.index.primaries.max": {"unit": "count", "desc": "单索引最大主分片"},
            "payload.elasticsearch.cluster_stats.indices.shards.index.primaries.avg": {"unit": "count", "desc": "单索引平均主分片"},
            "payload.elasticsearch.cluster_stats.indices.shards.index.replication.min": {"unit": "ratio", "desc": "单索引最小副本系数"},
            "payload.elasticsearch.cluster_stats.indices.shards.index.replication.max": {"unit": "ratio", "desc": "单索引最大副本系数"},
            "payload.elasticsearch.cluster_stats.indices.shards.index.replication.avg": {"unit": "ratio", "desc": "单索引平均副本系数"},

            # nodes
            "payload.elasticsearch.cluster_stats.nodes.count.total": {"unit": "count", "desc": "节点总数"},
            "payload.elasticsearch.cluster_stats.nodes.count.data": {"unit": "count", "desc": "data节点数"},
            "payload.elasticsearch.cluster_stats.nodes.count.master": {"unit": "count", "desc": "master节点数"},
            "payload.elasticsearch.cluster_stats.nodes.count.ingest": {"unit": "count", "desc": "ingest节点数"},
            "payload.elasticsearch.cluster_stats.nodes.count.coordinating_only": {"unit": "count", "desc": "coordinating-only节点数"},
            "payload.elasticsearch.cluster_stats.nodes.os.available_processors": {"unit": "count", "desc": "可用CPU核数"},
            "payload.elasticsearch.cluster_stats.nodes.os.allocated_processors": {"unit": "count", "desc": "分配CPU核数"},
            "payload.elasticsearch.cluster_stats.nodes.os.mem.total_in_bytes": {"unit": "bytes", "desc": "总内存"},
            "payload.elasticsearch.cluster_stats.nodes.os.mem.free_in_bytes": {"unit": "bytes", "desc": "空闲内存"},
            "payload.elasticsearch.cluster_stats.nodes.os.mem.used_in_bytes": {"unit": "bytes", "desc": "已用内存"},
            "payload.elasticsearch.cluster_stats.nodes.os.mem.free_percent": {"unit": "%", "desc": "空闲内存占比"},
            "payload.elasticsearch.cluster_stats.nodes.os.mem.used_percent": {"unit": "%", "desc": "已用内存占比"},
            "payload.elasticsearch.cluster_stats.nodes.process.cpu.percent": {"unit": "%", "desc": "进程CPU使用率"},
            "payload.elasticsearch.cluster_stats.nodes.process.open_file_descriptors.min": {"unit": "count", "desc": "最小打开文件数"},
            "payload.elasticsearch.cluster_stats.nodes.process.open_file_descriptors.max": {"unit": "count", "desc": "最大打开文件数"},
            "payload.elasticsearch.cluster_stats.nodes.process.open_file_descriptors.avg": {"unit": "count", "desc": "平均打开文件数"},
            "payload.elasticsearch.cluster_stats.nodes.jvm.max_uptime_in_millis": {"unit": "ms", "desc": "最长JVM运行时长"},
            "payload.elasticsearch.cluster_stats.nodes.jvm.mem.heap_max_in_bytes": {"unit": "bytes", "desc": "JVM堆上限"},
            "payload.elasticsearch.cluster_stats.nodes.jvm.mem.heap_used_in_bytes": {"unit": "bytes", "desc": "JVM堆使用"},
            "payload.elasticsearch.cluster_stats.nodes.jvm.threads": {"unit": "count", "desc": "JVM线程数"},
            "payload.elasticsearch.cluster_stats.nodes.fs.total_in_bytes": {"unit": "bytes", "desc": "磁盘总空间"},
            "payload.elasticsearch.cluster_stats.nodes.fs.free_in_bytes": {"unit": "bytes", "desc": "磁盘空闲空间"},
            "payload.elasticsearch.cluster_stats.nodes.fs.available_in_bytes": {"unit": "bytes", "desc": "磁盘可用空间"},
        },
        # 其余字段（字符串、数组、结构体等）自动 fallback 到 latest 快照值，无需显式列出
    },
    "shard_stats": {
        # Shard 级别的统计 - 数据量最大需要采样
        "rate_fields": {
            "payload.elasticsearch.shard_stats.stats.indexing.index_total": {"unit": "ops/sec", "desc": "每秒索引数"},
            "payload.elasticsearch.shard_stats.stats.indexing.index_time_in_millis": {"unit": "ms/sec", "desc": "索引耗时"},
            "payload.elasticsearch.shard_stats.stats.indexing.delete_total": {"unit": "ops/sec", "desc": "每秒删除数"},
            "payload.elasticsearch.shard_stats.stats.search.query_total": {"unit": "queries/sec", "desc": "每秒查询数"},
            "payload.elasticsearch.shard_stats.stats.search.query_time_in_millis": {"unit": "ms/sec", "desc": "查询耗时"},
            "payload.elasticsearch.shard_stats.stats.search.fetch_total": {"unit": "ops/sec", "desc": "每秒Fetch数"},
        },
        "latency_fields": {
            "payload.elasticsearch.shard_stats.stats.search.query_latency": {
                "time_field": "payload.elasticsearch.shard_stats.stats.search.query_time_in_millis",
                "count_field": "payload.elasticsearch.shard_stats.stats.search.query_total",
                "unit": "ms",
                "desc": "平均查询延迟(ms)"
            },
            "payload.elasticsearch.shard_stats.stats.indexing.index_latency": {
                "time_field": "payload.elasticsearch.shard_stats.stats.indexing.index_time_in_millis",
                "count_field": "payload.elasticsearch.shard_stats.stats.indexing.index_total",
                "unit": "ms",
                "desc": "平均索引延迟(ms)"
            },
        },
        "max_fields": {
            "payload.elasticsearch.shard_stats.stats.docs.count": {"unit": "docs", "desc": "分片文档数"},
            "payload.elasticsearch.shard_stats.stats.store.size_in_bytes": {"unit": "bytes", "desc": "分片存储大小"},
        },
    },
}


@dataclass
class OutputConfig:
    """输出配置"""
    directory: str = "."
    split_by: str = "metric_type"  # cluster, metric_type, none
    filename_prefix: str = ""
    compress: bool = False

    @classmethod
    def from_dict(cls, data: Optional[Dict]) -> 'OutputConfig':
        if not data:
            return cls()

        split_by = data.get('splitBy', 'metric_type')
        if split_by not in ('cluster', 'metric_type', 'none'):
            raise ConfigValidationError(f"无效的 splitBy: {split_by}，必须是 'cluster', 'metric_type' 或 'none'")

        return cls(
            directory=data.get('directory', '.'),
            split_by=split_by,
            filename_prefix=data.get('filenamePrefix', ''),
            compress=data.get('compress', False)
        )


@dataclass
class ExecutionConfig:
    """执行配置"""
    parallel_metrics: int = 2  # 并行导出的指标类型数
    parallel_degree: int = 1   # 单个指标内的并行度
    batch_size: Optional[int] = None
    scroll_keepalive: str = "5m"
    max_retries: int = 3
    retry_delay: int = 5  # 秒
    skip_estimation: bool = False  # 跳过数据量预估（加速启动）

    @classmethod
    def from_dict(cls, data: Optional[Dict]) -> 'ExecutionConfig':
        if not data:
            return cls()

        return cls(
            parallel_metrics=data.get('parallelMetrics', 2),
            parallel_degree=data.get('parallelDegree', 1),
            batch_size=data.get('batchSize'),
            scroll_keepalive=data.get('scrollKeepalive', '5m'),
            max_retries=data.get('maxRetries', 3),
            retry_delay=data.get('retryDelay', 5),
            skip_estimation=data.get('skipEstimation', False)
        )


SUPPORTED_TYPES = {
    "metrics": {"cluster_health", "cluster_stats", "node_stats", "index_stats", "shard_stats"},
    "alerts": {"alert_rules", "alert_messages", "alert_history"}
}


def _validate_types(name: str, types: list, kind: str) -> None:
    """验证类型是否在支持列表中"""
    invalid = set(types) - SUPPORTED_TYPES[kind]
    if invalid:
        type_name = "指标" if kind == "metrics" else "告警"
        raise ConfigValidationError(f"job '{name}' 包含不支持的{type_name}类型: {invalid}")


@dataclass
class MetricsJobConfig:
    """单个导出任务配置"""
    name: str
    enabled: bool = True
    targets: Optional[TargetsConfig] = None
    metrics: List[str] = field(default_factory=list)  # 指标类型列表
    sampling: SamplingConfig = field(default_factory=SamplingConfig)
    slim: SlimConfig = field(default_factory=SlimConfig)  # 精简数据配置
    mask_ip: bool = False  # IP 地址脱敏
    output: OutputConfig = field(default_factory=OutputConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    time_range_hours: int = 24
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    shard_size: int = 100000  # 每个分片文件的最大文档数
    source_fields: Optional[List[str]] = None
    include_alerts: bool = True
    alert_types: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict) -> 'MetricsJobConfig':
        name = data.get('name')
        if not name:
            raise ConfigValidationError("job 必须有 name 字段")

        # 解析指标类型（允许为空，比如只导出告警数据的场景）
        metrics = data.get('metrics', [])
        _validate_types(name, metrics, "metrics")

        # 解析告警类型
        alert_types = data.get('alertTypes', [])
        _validate_types(name, alert_types, "alerts")

        has_time_range = 'timeRangeHours' in data and data.get('timeRangeHours') is not None
        has_start_time = data.get('startTime') is not None
        has_end_time = data.get('endTime') is not None

        # 时间配置模式互斥：只能选择 timeRangeHours 或 startTime+endTime
        if has_time_range and (has_start_time or has_end_time):
            raise ConfigValidationError(
                f"job '{name}' 的时间配置冲突：timeRangeHours 与 startTime/endTime 只能二选一"
            )

        if has_start_time != has_end_time:
            raise ConfigValidationError(
                f"job '{name}' 的时间配置不完整：使用绝对时间时必须同时提供 startTime 和 endTime"
            )

        time_range_hours = data.get('timeRangeHours', 24)
        if has_start_time and has_end_time:
            # 绝对时间模式下，保留一个默认值占位，不参与实际查询
            time_range_hours = 24

        return cls(
            name=name,
            enabled=data.get('enabled', True),
            targets=TargetsConfig.from_dict(data.get('targets', {})),
            metrics=metrics,
            sampling=SamplingConfig.from_dict(data.get('sampling')),
            slim=SlimConfig.from_dict(data.get('slim')),
            mask_ip=data.get('maskIp', False),
            output=OutputConfig.from_dict(data.get('output', {})),
            execution=ExecutionConfig.from_dict(data.get('execution', {})),
            time_range_hours=time_range_hours,
            start_time=data.get('startTime'),
            end_time=data.get('endTime'),
            shard_size=data.get('shardSize', 100000),
            source_fields=data.get('sourceFields'),
            include_alerts=data.get('includeAlerts', True),
            alert_types=alert_types
        )


@dataclass
class MetricsExporterConfig:
    """Metrics Exporter 完整配置"""
    jobs: List[MetricsJobConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict) -> 'MetricsExporterConfig':
        jobs_data = data.get('jobs', [])
        if not jobs_data:
            raise ConfigValidationError("metricsExporter 需要至少一个 job")

        jobs = [MetricsJobConfig.from_dict(job) for job in jobs_data]

        # 检查 job 名称唯一性
        names = [j.name for j in jobs]
        duplicates = [n for n, count in Counter(names).items() if count > 1]
        if duplicates:
            raise ConfigValidationError(f"job 名称重复: {set(duplicates)}")

        return cls(jobs=jobs)


@dataclass
class GlobalConfig:
    """全局配置"""
    console_url: str = "http://localhost:9000"
    username: str = ""
    password: str = ""
    timeout: int = 60
    insecure: bool = False

    @classmethod
    def from_dict(cls, data: Dict) -> 'GlobalConfig':
        auth = data.get('auth', {}) if isinstance(data.get('auth'), dict) else {}

        return cls(
            console_url=data.get('consoleUrl', 'http://localhost:9000'),
            username=auth.get('username', ''),
            password=auth.get('password', ''),
            timeout=data.get('timeout', 60),
            insecure=data.get('insecure', False)
        )


@dataclass
class AppConfig:
    """应用完整配置"""
    global_config: GlobalConfig = field(default_factory=GlobalConfig)
    metrics_exporter: Optional[MetricsExporterConfig] = None

    @classmethod
    def from_dict(cls, data: Dict) -> 'AppConfig':
        global_config = GlobalConfig.from_dict(data)

        metrics_exporter = None
        if 'metricsExporter' in data:
            metrics_exporter = MetricsExporterConfig.from_dict(data['metricsExporter'])

        return cls(global_config=global_config, metrics_exporter=metrics_exporter)

    @classmethod
    def load(cls, config_path: str) -> 'AppConfig':
        """从文件加载配置"""
        data = load_config_file(config_path)
        return cls.from_dict(data)


class BaseConfig:
    """基础配置类（向后兼容）"""

    def __init__(
        self,
        console_url: str = "http://localhost:9000",
        username: str = "",
        password: str = "",
        timeout: int = 60,
        insecure: bool = False,
    ):
        self.console_url = console_url
        self.username = username
        self.password = password
        self.timeout = timeout
        self.insecure = insecure

    @classmethod
    def from_args_and_config(cls, args: argparse.Namespace, config: Dict[str, Any]):
        """从命令行参数和配置文件创建配置"""
        auth_config = config.get('auth', {}) if isinstance(config.get('auth'), dict) else {}

        return cls(
            console_url=get_config_value(
                getattr(args, 'console', None) or getattr(args, 'host', None),
                config.get('consoleUrl') or config.get('baseUrl'),
                'CONSOLE_URL',
                'http://localhost:9000'
            ),
            username=get_config_value(
                getattr(args, 'username', None),
                auth_config.get('username'),
                'CONSOLE_USERNAME',
                ''
            ),
            password=get_config_value(
                getattr(args, 'password', None),
                auth_config.get('password'),
                'CONSOLE_PASSWORD',
                ''
            ),
            timeout=int(get_config_value(
                str(getattr(args, 'timeout', 60)),
                str(config.get('timeout', 60)),
                'CONSOLE_TIMEOUT',
                '60'
            )),
            insecure=getattr(args, 'insecure', False) or config.get('insecure', False),
        )


def add_common_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """添加通用参数到 ArgumentParser"""

    # Console 连接参数
    conn_group = parser.add_argument_group('Console 连接参数')
    conn_group.add_argument(
        '-c', '--console',
        default='http://localhost:9000',
        help='INFINI Console URL (默认: http://localhost:9000, 环境变量: CONSOLE_URL)'
    )
    conn_group.add_argument(
        '-u', '--username',
        default='',
        help='登录用户名 (环境变量: CONSOLE_USERNAME)'
    )
    conn_group.add_argument(
        '-p', '--password',
        default='',
        help='登录密码 (环境变量: CONSOLE_PASSWORD)'
    )
    conn_group.add_argument(
        '--timeout',
        type=int,
        default=60,
        help='请求超时时间(秒) (默认: 60, 环境变量: CONSOLE_TIMEOUT)'
    )
    conn_group.add_argument(
        '--insecure',
        action='store_true',
        help='忽略 SSL 证书验证'
    )

    # 配置文件参数
    config_group = parser.add_argument_group('配置参数')
    config_group.add_argument(
        '--config',
        default='',
        help='配置文件路径 (JSON 格式)'
    )

    # 输出参数
    output_group = parser.add_argument_group('输出参数')
    output_group.add_argument(
        '-o', '--output',
        default='',
        help='输出文件或目录路径'
    )

    return parser


def load_and_merge_config(args: argparse.Namespace) -> tuple:
    """加载配置文件并合并参数

    Returns:
        (merged_config_dict, effective_args)
    """
    config = {}
    if getattr(args, 'config', None) and Path(args.config).exists():
        config = load_config_file(args.config)

    return config, args


def validate_config(config_path: str) -> List[str]:
    """验证配置文件，返回错误列表"""
    errors = []
    try:
        AppConfig.load(config_path)
    except ConfigValidationError as e:
        errors.append(str(e))
    except json.JSONDecodeError as e:
        errors.append(f"JSON 解析错误: {e}")
    except Exception as e:
        errors.append(f"配置加载错误: {e}")
    return errors
