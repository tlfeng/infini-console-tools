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


@dataclass
class FieldAggregationConfig:
    """字段聚合配置 - 定义每个字段使用的聚合类型"""
    max_fields: List[str] = field(default_factory=list)  # 使用 max 聚合的字段
    derivative_fields: List[str] = field(default_factory=list)  # 使用 derivative 聚合的字段

    @classmethod
    def from_dict(cls, data: Optional[Dict]) -> 'FieldAggregationConfig':
        if not data:
            return cls()

        return cls(
            max_fields=data.get('max', []),
            derivative_fields=data.get('derivative', [])
        )

    def has_aggregations(self) -> bool:
        """是否配置了聚合字段"""
        return bool(self.max_fields or self.derivative_fields)

    def get_all_fields(self) -> List[str]:
        """获取所有需要聚合的字段"""
        return self.max_fields + self.derivative_fields


# 内置的 derivative 字段定义（从 Console 项目搬运）
# 这些字段需要计算相邻时间桶的差值，再除以 bucketSize 得到每秒速率
DERIVATIVE_FIELDS = {
    "node_stats": [
        # 索引相关
        "indices.indexing.index_total",
        "indices.indexing.index_time_in_millis",
        "indices.store.size_in_bytes",
        # 搜索相关
        "indices.search.query_total",
        "indices.search.query_time_in_millis",
        "indices.search.fetch_total",
        "indices.search.fetch_time_in_millis",
        "indices.search.scroll_total",
        "indices.search.scroll_time_in_millis",
        # 合并/刷新/刷盘
        "indices.merges.total",
        "indices.merges.total_time_in_millis",
        "indices.refresh.total",
        "indices.refresh.total_time_in_millis",
        "indices.flush.total",
        "indices.flush.total_time_in_millis",
        # 请求缓存
        "indices.request_cache.hit_count",
        "indices.request_cache.miss_count",
        "indices.query_cache.cache_count",
        "indices.query_cache.hit_count",
        "indices.query_cache.miss_count",
        # HTTP
        "http.total_opened",
        # GC
        "jvm.gc.collectors.young.collection_count",
        "jvm.gc.collectors.young.collection_time_in_millis",
        "jvm.gc.collectors.old.collection_count",
        "jvm.gc.collectors.old.collection_time_in_millis",
        # Transport
        "transport.tx_count",
        "transport.rx_count",
        "transport.tx_size_in_bytes",
        "transport.rx_size_in_bytes",
        # 磁盘 IO
        "fs.io_stats.total.operations",
        "fs.io_stats.total.read_operations",
        "fs.io_stats.total.write_operations",
        # Breaker
        "breakers.parent.tripped",
        "breakers.accounting.tripped",
        "breakers.fielddata.tripped",
        "breakers.request.tripped",
        "breakers.in_flight_requests.tripped",
    ],
    "index_stats": [
        # 索引相关
        "primaries.indexing.index_total",
        "primaries.indexing.index_time_in_millis",
        "total.indexing.index_total",
        "total.indexing.index_time_in_millis",
        # 搜索相关
        "primaries.search.query_total",
        "primaries.search.query_time_in_millis",
        "total.search.query_total",
        "total.search.query_time_in_millis",
        "total.search.fetch_total",
        "total.search.scroll_total",
        # 合并/刷新/刷盘
        "primaries.merges.total",
        "total.merges.total",
        "primaries.refresh.total",
        "total.refresh.total",
        "primaries.flush.total",
        "total.flush.total",
        # 请求缓存
        "primaries.request_cache.hit_count",
        "primaries.request_cache.miss_count",
        "total.request_cache.hit_count",
        "total.request_cache.miss_count",
        # 查询缓存
        "primaries.query_cache.cache_count",
        "primaries.query_cache.hit_count",
        "primaries.query_cache.miss_count",
    ],
    "shard_stats": [
        # 索引相关
        "indexing.index_total",
        # 搜索相关
        "search.query_total",
        "search.fetch_total",
        "search.scroll_total",
        # 合并/刷新/刷盘
        "merges.total",
        "refresh.total",
        "flush.total",
        # 请求缓存
        "request_cache.hit_count",
        "request_cache.miss_count",
        # 查询缓存
        "query_cache.cache_count",
        "query_cache.hit_count",
        "query_cache.miss_count",
    ],
    "cluster_health": [],  # cluster_health 通常不需要 derivative
    # cluster_stats 结构在不同 ES/Easysearch 版本差异较大，
    # 固定字段聚合容易得到大量 null，改为 top_hits 抽样保留真实快照。
    "cluster_stats": [],
}


def get_derivative_fields(metric_type: str) -> List[str]:
    """获取指定指标类型的 derivative 字段列表"""
    return DERIVATIVE_FIELDS.get(metric_type, [])


def get_builtin_field_aggregation(metric_type: str) -> FieldAggregationConfig:
    """获取内置的字段聚合配置"""
    derivative_fields = get_derivative_fields(metric_type)
    return FieldAggregationConfig(
        max_fields=[],  # max 字段不单独列出，由其他字段隐式确定
        derivative_fields=derivative_fields
    )


@dataclass
class FieldAggregationsConfig:
    """按指标类型配置的字段聚合"""
    aggregations: Dict[str, FieldAggregationConfig] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Optional[Dict]) -> 'FieldAggregationsConfig':
        if not data:
            return cls()

        aggregations = {}
        for metric_type, config in data.items():
            aggregations[metric_type] = FieldAggregationConfig.from_dict(config)

        return cls(aggregations=aggregations)

    def get_for_metric(self, metric_type: str) -> FieldAggregationConfig:
        """获取指定指标类型的聚合配置"""
        return self.aggregations.get(metric_type, FieldAggregationConfig())


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
    field_aggregations: FieldAggregationsConfig = field(default_factory=FieldAggregationsConfig)  # 字段聚合配置

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
