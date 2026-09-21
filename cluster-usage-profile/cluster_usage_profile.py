#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cluster Usage Profile - 集群使用画像工具

通过 INFINI Console 代理接口（_proxy）采集单个 Elasticsearch 集群的
使用情况与使用场景画像，适用于只能通过 Console 访问集群的场景：

- 集群概览：版本、健康、磁盘水位配置、pending tasks
- 节点画像：角色分布、heap/CPU/磁盘水位
- 分片画像：状态分布、大小分布、最大分片、碎片分片
- 索引画像：规模、命名模式分组（业务域/时间序列）、别名、closed 归档
- 读写热度：两轮采样计算每个索引的查询/写入速率
- API 画像：_nodes/usage REST 请求计数，区分 bulk 写入/检索/点查
- 治理画像：模板、ILM、快照仓库、slowlog、只读阻断

兼容 6.8.x / 7.10.x：列缺失时自动降级，ILM/usage 不可用时跳过。

输出 Markdown 报告 + JSON 明细 + 索引级 CSV。

用法:
  python cluster_usage_profile.py -c http://localhost:9000 -u admin -p password \\
      --cluster-name my-cluster

  python cluster_usage_profile.py --config config.json --cluster-id xxx \\
      --sample-interval 120 -o ./exports
"""

import argparse
import csv
import json
import re
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))
from common.console_client import ConsoleClient, ConsoleAPIError, ConsoleAuthError
from common.config import add_common_args, load_and_merge_config, get_config_value


# ---------------------------------------------------------------------------
# 常量与纯函数（可单测）
# ---------------------------------------------------------------------------

SIZE_UNITS = {
    "b": 1,
    "kb": 1024,
    "mb": 1024 ** 2,
    "gb": 1024 ** 3,
    "tb": 1024 ** 4,
    "pb": 1024 ** 5,
}

# 索引名尾部日期模式，如 log-2026.09.21 / order_2026_09 / app20260921 / audit-2026
DATE_SUFFIX_RE = re.compile(
    r"^(?P<base>.+?)[._-]?(?P<token>20\d{2}(?:[._-]?\d{1,2}){0,2})$"
)

SIZE_RE = re.compile(r"^\s*(?P<num>[\d.]+)\s*(?P<unit>[a-zA-Z]+)?\s*$")

# 活跃阈值：分片级 ops/s，>= 该值视为活跃（约 8640 次/天）
ACTIVE_THRESHOLD_OPS_PER_SEC = 0.1


def parse_size(text: Any) -> int:
    """解析 _cat 输出的人类可读大小，如 '5.1gb'/'204mb'/'272b'，返回字节数"""
    if text is None:
        return 0
    if isinstance(text, (int, float)):
        return int(text)
    match = SIZE_RE.match(str(text))
    if not match:
        return 0
    num = float(match.group("num"))
    unit = (match.group("unit") or "b").lower()
    return int(num * SIZE_UNITS.get(unit, 1))


def to_int(value: Any, default: int = 0) -> int:
    """安全转 int，_cat JSON 里的值都是字符串"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def to_float(value: Any, default: float = 0.0) -> float:
    """安全转 float"""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def strip_date_suffix(name: str) -> Tuple[str, Optional[str]]:
    """
    剥离索引名尾部的日期后缀，返回 (基础名, 日期token或None)。

    log-2026.09.21 -> ("log", "2026.09.21")
    order_2026_09  -> ("order", "2026_09")
    app20260921    -> ("app", "20260921")
    audit-2026     -> ("audit", "2026")
    business-index -> ("business-index", None)
    """
    match = DATE_SUFFIX_RE.match(name)
    if match:
        return match.group("base"), match.group("token")
    return name, None


def summarize_node_role(role_str: str) -> set:
    """
    将 _cat/nodes 的角色字符串解析为角色集合。

    6.x 常见 'dim'/'di'/'-'，7.x 常见 'cdhilstw'/'-'。
    'd'=data 'm'=master候选 'i'=ingest 'l'=ml 't'=transform 'v'=voting
    'r'=remote_client 'c'=cold 'h'=hot 'w'=warm，'-'/空=coordinating-only。
    """
    s = (role_str or "").strip().lower()
    roles = set()
    if not s or s == "-":
        roles.add("coordinating")
        return roles
    mapping = {
        "d": "data",
        "m": "master_eligible",
        "i": "ingest",
        "l": "ml",
        "t": "transform",
        "v": "voting_only",
        "r": "remote_client",
        "c": "cold_data",
        "h": "hot_data",
        "w": "warm_data",
    }
    for ch, role in mapping.items():
        if ch in s:
            roles.add(role)
    if not roles:
        roles.add("coordinating")
    return roles


def shard_size_bucket(size_bytes: int) -> str:
    """分片大小分桶"""
    mb = 1024 ** 2
    gb = 1024 ** 3
    if size_bytes < 100 * mb:
        return "<100MB"
    if size_bytes < gb:
        return "100MB-1GB"
    if size_bytes < 10 * gb:
        return "1-10GB"
    if size_bytes < 30 * gb:
        return "10-30GB"
    if size_bytes < 50 * gb:
        return "30-50GB"
    return ">=50GB"


def classify_activity(qps: float, ips: float,
                      threshold: float = ACTIVE_THRESHOLD_OPS_PER_SEC) -> str:
    """根据采样速率分类索引活跃度"""
    if qps >= threshold and ips >= threshold:
        return "读写均活跃"
    if qps >= threshold:
        return "查询为主"
    if ips >= threshold:
        return "写入为主"
    if qps > 0 or ips > 0:
        return "低频"
    return "闲置"


def compute_index_rates(snap0: Dict[str, Dict[str, int]],
                        snap1: Dict[str, Dict[str, int]],
                        interval_seconds: float) -> Dict[str, Dict[str, float]]:
    """
    由两轮 _stats 采样计算每个索引的速率。

    snap 结构: {index: {"query_total": n, "index_total": n}}
    返回: {index: {"qps": x, "ips": y}}（分片级，primaries 口径）
    """
    rates: Dict[str, Dict[str, float]] = {}
    if interval_seconds <= 0:
        return rates
    for index, cur in snap1.items():
        prev = snap0.get(index)
        if not prev:
            continue
        q0 = to_int(prev.get("query_total"))
        q1 = to_int(cur.get("query_total"))
        i0 = to_int(prev.get("index_total"))
        i1 = to_int(cur.get("index_total"))
        # 计数器可能因索引重建而回退，负增量按 0 处理
        qps = max(0.0, (q1 - q0) / interval_seconds)
        ips = max(0.0, (i1 - i0) / interval_seconds)
        rates[index] = {"qps": qps, "ips": ips}
    return rates


ACTION_FAMILY_RULES = [
    ("multisearch", "MultiSearch检索"),
    ("msearch", "MultiSearch检索"),
    ("bulk", "Bulk批量写入"),
    ("scroll", "Scroll导出"),
    ("search", "Search检索"),
    ("mget", "MultiGet点查"),
    ("get", "Get点查"),
    ("index", "Index单条写入"),
    ("update", "Update更新"),
    ("delete", "Delete删除"),
    ("count", "Count计数"),
    ("cat", "Cat运维查询"),
    ("cluster", "集群管理"),
]


def match_action_family(action_name: str) -> str:
    """
    将 REST action 类名归入使用家族。

    按最后一段类名匹配（完整路径含 org.elasticsearch，其内嵌 'search'
    会造成误判）；cat 动作按包名识别。
    """
    name = action_name.lower()
    short = name.rsplit(".", 1)[-1]
    if ".cat." in name or short.startswith("restcat"):
        return "Cat运维查询"
    for keyword, family in ACTION_FAMILY_RULES:
        if keyword in short:
            return family
    return "其他"


def aggregate_rest_actions(usage_payload: Dict[str, Any]) -> Tuple[Counter, int, List[str]]:
    """
    聚合 /_nodes/usage 各节点的 rest_actions 计数。

    返回 (合并后的 action->count, 上报节点数, since 时间列表)
    """
    merged: Counter = Counter()
    node_count = 0
    sincers: List[str] = []
    for node in (usage_payload or {}).get("nodes", {}).values():
        actions = node.get("rest_actions") or {}
        if not isinstance(actions, dict):
            continue
        node_count += 1
        since = node.get("since")
        if since:
            sincers.append(str(since))
        for action, count in actions.items():
            merged[action] += to_int(count)
    return merged, node_count, sincers


def infer_scenario_labels(qps_total: float, ips_total: float,
                          bulk_count: int, search_count: int, get_count: int,
                          ts_store_share: float, ts_index_share: float,
                          active_write_count: int, total_open_count: int,
                          sample_interval: int) -> List[str]:
    """根据各项画像指标推断使用场景，返回结论列表"""
    labels = []

    # 读写特征
    if sample_interval > 0 and (qps_total > 0 or ips_total > 0):
        if ips_total >= qps_total * 3:
            labels.append(
                f"读写特征：写入明显多于查询（写入 ~{ips_total:.1f} ops/s vs 查询 ~{qps_total:.1f} ops/s），"
                "偏向数据接入/存储型场景")
        elif qps_total >= ips_total * 3:
            labels.append(
                f"读写特征：查询明显多于写入（查询 ~{qps_total:.1f} ops/s vs 写入 ~{ips_total:.1f} ops/s），"
                "偏向在线检索服务型场景")
        else:
            labels.append(
                f"读写特征：读写相对均衡（查询 ~{qps_total:.1f} ops/s，写入 ~{ips_total:.1f} ops/s）")
    else:
        labels.append("读写特征：未启用速率采样（--sample-interval 0 可关闭），无法判断读写比例")

    # 数据形态
    if ts_index_share >= 0.5 or ts_store_share >= 0.5:
        labels.append(
            f"数据形态：时间序列命名索引占比高（按数量 {ts_index_share * 100:.0f}%、"
            f"按存储 {ts_store_share * 100:.0f}%），典型的按日期滚动接入")
    else:
        labels.append(
            f"数据形态：以固定业务索引为主（时间序列命名按数量 {ts_index_share * 100:.0f}%、"
            f"按存储 {ts_store_share * 100:.0f}%）")

    # 写入方式（REST 计数口径，受节点 uptime 影响，仅作参考）
    total_fam = bulk_count + search_count + get_count
    if total_fam > 0:
        if bulk_count >= search_count and bulk_count >= get_count:
            labels.append(
                f"访问方式：REST 计数中 Bulk 批量写入占比最高（{bulk_count:,} 次，"
                f"占三大类 {bulk_count / total_fam * 100:.0f}%），以批量写入通道为主")
        elif search_count >= get_count:
            labels.append(
                f"访问方式：REST 计数中 Search 检索占比最高（{search_count:,} 次，"
                f"占三大类 {search_count / total_fam * 100:.0f}%），以查询通道为主")
        else:
            labels.append(
                f"访问方式：REST 计数中 Get 点查占比最高（{get_count:,} 次，"
                f"占三大类 {get_count / total_fam * 100:.0f}%），以主键点查场景为主")

    # 活跃度
    if sample_interval > 0 and total_open_count > 0:
        labels.append(
            f"活跃度：采样窗口 {sample_interval}s 内 {active_write_count}/{total_open_count} "
            "个 open 索引有写入，其余大概率是只读历史/归档数据")

    return labels


# ---------------------------------------------------------------------------
# 采集与分析
# ---------------------------------------------------------------------------

class UsageProfiler:
    """集群使用画像采集器，全部请求走 Console _proxy"""

    def __init__(self, client: ConsoleClient, cluster_id: str,
                 display_name: str = "",
                 include_system_indices: bool = False,
                 top_n: int = 20,
                 sample_interval: int = 60):
        self.client = client
        self.cluster_id = cluster_id
        self.display_name = display_name or cluster_id
        self.include_system_indices = include_system_indices
        self.top_n = top_n
        self.sample_interval = sample_interval

        self.report: Dict[str, Any] = {
            "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "sample_interval_seconds": sample_interval,
            "include_system_indices": include_system_indices,
        }
        self._stats_t0: Dict[str, Dict[str, int]] = {}
        self._stats_t1: Dict[str, Dict[str, int]] = {}
        self._t0_wall: float = 0.0

    # ---------------- 基础请求 ----------------

    def proxy_get(self, path: str) -> Any:
        return self.client.proxy_request(self.cluster_id, "GET", path)

    def cat_json(self, base_path: str, columns: List[str],
                 fallbacks: Optional[List[List[str]]] = None) -> List[Dict]:
        """
        执行 _cat 请求，列集合不支持时（老版本集群）自动降级重试。
        """
        attempts = [columns] + (fallbacks or [])
        last_err: Optional[Exception] = None
        for cols in attempts:
            path = f"{base_path}?format=json&h={','.join(cols)}"
            try:
                result = self.proxy_get(path)
                return result if isinstance(result, list) else []
            except ConsoleAPIError as e:
                last_err = e
                continue
        print(f"  警告: {base_path} 采集失败: {last_err}", file=sys.stderr)
        return []

    # ---------------- 采集 ----------------

    def collect_overview(self):
        """集群版本、健康、磁盘水位配置、pending tasks"""
        print("采集集群概览...")
        root = self.proxy_get("/")
        version_info = root.get("version", {}) if isinstance(root, dict) else {}
        self.report["cluster"] = {
            "console_display_name": self.display_name,
            "cluster_name": root.get("cluster_name", ""),
            "cluster_uuid": root.get("cluster_uuid", ""),
            "version": version_info.get("number", ""),
        }

        health = self.proxy_get("/_cluster/health")
        self.report["overview"] = {
            "status": health.get("status", ""),
            "number_of_nodes": health.get("number_of_nodes", 0),
            "number_of_data_nodes": health.get("number_of_data_nodes", 0),
            "active_primary_shards": health.get("active_primary_shards", 0),
            "active_shards": health.get("active_shards", 0),
            "unassigned_shards": health.get("unassigned_shards", 0),
            "relocating_shards": health.get("relocating_shards", 0),
            "initializing_shards": health.get("initializing_shards", 0),
            "pending_tasks_count": len(health.get("pending_tasks", []) or []),
        }

        # 集群设置：只关注磁盘水位相关（flat_settings 方便取值）
        settings = {}
        try:
            settings = self.proxy_get(
                "/_cluster/settings?flat_settings=true&include_defaults=true")
        except ConsoleAPIError:
            pass
        defaults = settings.get("defaults", {}) if isinstance(settings, dict) else {}
        overrides = {}
        for scope in ("persistent", "transient"):
            for key, value in (settings.get(scope) or {}).items():
                overrides[key] = value
        watermark_keys = [
            "cluster.routing.allocation.disk.watermark.low",
            "cluster.routing.allocation.disk.watermark.high",
            "cluster.routing.allocation.disk.watermark.flood_stage",
        ]
        watermarks = {}
        for key in watermark_keys:
            if key in overrides:
                watermarks[key] = overrides[key]
            elif key in defaults:
                watermarks[key] = defaults[key]
        self.report["overview"]["disk_watermarks"] = watermarks
        self.report["overview"]["cluster_settings_overrides"] = overrides

        # pending tasks 明细（/health 只返回计数类字段，明细从这里取）
        try:
            pending = self.proxy_get("/_cluster/pending_tasks")
            tasks = pending.get("tasks", []) if isinstance(pending, dict) else []
            self.report["overview"]["pending_tasks_count"] = len(tasks)
            self.report["governance_pending_tasks"] = [
                {
                    "source": t.get("source", ""),
                    "time_in_queue_millis": t.get("time_in_queue_millis", 0),
                }
                for t in tasks[:20]
            ]
        except ConsoleAPIError:
            self.report["governance_pending_tasks"] = []

    def collect_nodes(self):
        """节点画像：_cat/nodes"""
        print("采集节点信息...")
        cols = ["name", "ip", "heap.percent", "ram.percent", "cpu",
                "disk.used_percent", "disk.total", "disk.used", "disk.avail",
                "master", "node.role"]
        fallbacks = [
            ["name", "ip", "heap.percent", "ram.percent", "cpu",
             "disk.used_percent", "disk.total", "disk.used", "disk.avail",
             "master", "role"],
            ["name", "ip", "heap.percent", "ram.percent", "cpu",
             "disk.used_percent", "master"],
        ]
        rows = self.cat_json("/_cat/nodes", cols, fallbacks)
        self.report["nodes_raw"] = rows

    def collect_shards(self):
        """分片画像：_cat/shards"""
        print("采集分片信息...")
        cols = ["index", "shard", "prirep", "state", "store", "docs", "node"]
        rows = self.cat_json("/_cat/shards", cols)
        self.report["shards_raw"] = rows

    def fetch_index_stats(self) -> Dict[str, Dict[str, int]]:
        """
        拉取 /_all/_stats 的索引级读写计数（primaries 口径）。

        返回 {index: {query_total, index_total}}
        """
        try:
            resp = self.proxy_get(
                "/_all/_stats/docs,store,search,indexing,segments")
        except ConsoleAPIError as e:
            print(f"  警告: _stats 采集失败: {e}", file=sys.stderr)
            return {}
        snapshots = {}
        for index, info in (resp.get("indices") or {}).items():
            primaries = info.get("primaries") or {}
            snapshots[index] = {
                "query_total": to_int((primaries.get("search") or {}).get("query_total")),
                "index_total": to_int((primaries.get("indexing") or {}).get("index_total")),
            }
        return snapshots

    def collect_indices_static(self):
        """索引静态信息 + 别名"""
        print("采集索引信息（_cat/indices）...")
        cols = ["index", "health", "status", "pri", "rep", "docs.count",
                "docs.deleted", "store.size", "pri.store.size", "creation.date"]
        fallbacks = [
            ["index", "health", "status", "pri", "rep", "docs.count",
             "docs.deleted", "store.size", "pri.store.size"],
        ]
        rows = self.cat_json("/_cat/indices", cols, fallbacks)
        self.report["indices_raw"] = rows

        print("采集别名信息（_cat/aliases）...")
        alias_cols = ["alias", "index"]
        alias_rows = self.cat_json("/_cat/aliases", alias_cols)
        alias_map: Dict[str, List[str]] = {}
        for row in alias_rows:
            index = row.get("index", "")
            alias = row.get("alias", "")
            if index and alias:
                alias_map.setdefault(index, []).append(alias)
        self.report["aliases_map"] = alias_map

    def collect_api_usage(self):
        """API 使用画像：_nodes/usage（老版本或未开放时跳过）"""
        print("采集 API 使用画像（_nodes/usage）...")
        try:
            payload = self.proxy_get("/_nodes/usage")
        except ConsoleAPIError as e:
            print(f"  提示: _nodes/usage 不可用（{e}），跳过 API 画像", file=sys.stderr)
            self.report["api_usage"] = {"available": False}
            return
        merged, node_count, sincers = aggregate_rest_actions(payload)
        self.report["api_usage"] = {
            "available": True,
            "nodes_reported": node_count,
            "since": sincers[:3],
            "rest_actions": dict(merged),
        }

    def collect_governance(self):
        """治理画像：模板、ILM、快照仓库、slowlog、只读阻断"""
        print("采集治理信息（模板/ILM/快照/slowlog/阻断）...")
        gov: Dict[str, Any] = {}

        # index templates
        tpl_cols = ["name", "index_patterns", "order", "version", "composed_of"]
        tpl_fallbacks = [
            ["name", "index_patterns", "order", "version"],
            ["name", "index_patterns"],
        ]
        templates = self.cat_json("/_cat/templates", tpl_cols, tpl_fallbacks)
        gov["templates"] = [
            {
                "name": t.get("name", ""),
                "index_patterns": t.get("index_patterns", ""),
                "order": t.get("order", ""),
            }
            for t in templates
        ]

        # ILM 策略
        try:
            policies = self.proxy_get("/_ilm/policy")
            if not isinstance(policies, dict):
                policies = {}
            gov["ilm_available"] = True
            gov["ilm_policies"] = {
                name: {
                    "version": p.get("version", 0),
                    "phases": sorted(list((p.get("policy") or {}).get("phases", {}).keys())),
                }
                for name, p in policies.items()
            }
        except ConsoleAPIError as e:
            gov["ilm_available"] = False
            gov["ilm_policies"] = {}
            print(f"  提示: ILM 不可用（可能是 6.x 未启用 x-pack），跳过", file=sys.stderr)

        # 快照仓库
        try:
            repos = self.proxy_get("/_snapshot")
            if not isinstance(repos, dict):
                repos = {}
            gov["snapshot_repos"] = {
                name: {"type": (info or {}).get("type", "")}
                for name, info in repos.items()
            }
        except ConsoleAPIError as e:
            gov["snapshot_repos"] = {}
            print(f"  提示: 快照仓库信息不可用: {e}", file=sys.stderr)

        # slowlog 配置（只返回配置了的索引）
        try:
            resp = self.proxy_get(
                "/_all/_settings?filter_path=*.settings.index.search.slowlog")
            slowlog_indices = []
            for index, info in (resp or {}).items():
                cfg = ((info or {}).get("settings") or {}).get("index") or {}
                search = (cfg.get("search") or {}) if isinstance(cfg, dict) else {}
                if search.get("slowlog"):
                    slowlog_indices.append(index)
            gov["slowlog_enabled_indices"] = sorted(slowlog_indices)
        except ConsoleAPIError:
            gov["slowlog_enabled_indices"] = []

        # 只读/写阻断索引
        try:
            resp = self.proxy_get(
                "/_all/_settings?filter_path=*.settings.index.blocks")
            blocked = []
            for index, info in (resp or {}).items():
                cfg = ((info or {}).get("settings") or {}).get("index") or {}
                blocks = (cfg.get("blocks") or {}) if isinstance(cfg, dict) else {}
                flags = [k for k, v in blocks.items()
                         if str(v).lower() == "true"
                         and k in ("read_only", "read_only_allow_delete",
                                   "write", "metadata_read_only")]
                if flags:
                    blocked.append({"index": index, "blocks": flags})
            gov["blocked_indices"] = blocked
        except ConsoleAPIError:
            gov["blocked_indices"] = []

        self.report["governance"] = gov

    # ---------------- 分析 ----------------

    def analyze_nodes(self) -> Dict[str, Any]:
        rows = self.report.get("nodes_raw", [])
        role_counter: Counter = Counter()
        role_set_counter: Counter = Counter()
        heap_vals, cpu_vals, disk_vals = [], [], []
        current_master = ""
        disk_hot_nodes = []
        for row in rows:
            role_raw = row.get("node.role", row.get("role", ""))
            roles = summarize_node_role(role_raw)
            for r in roles:
                role_counter[r] += 1
            role_set_counter[role_raw or "-"] += 1
            if row.get("master") == "*":
                current_master = row.get("name", "")
            heap_vals.append(to_int(row.get("heap.percent")))
            cpu_vals.append(to_int(row.get("cpu")))
            disk_pct = to_float(row.get("disk.used_percent"))
            if disk_pct > 0:
                disk_vals.append(disk_pct)
                if disk_pct >= 85:
                    disk_hot_nodes.append({
                        "name": row.get("name", ""),
                        "disk_used_percent": disk_pct,
                        "disk_avail": row.get("disk.avail", ""),
                    })

        disk_hot_nodes.sort(key=lambda x: -x["disk_used_percent"])
        analysis = {
            "total_nodes": len(rows),
            "role_counts": dict(role_counter),
            "role_raw_distribution": dict(role_set_counter),
            "current_master": current_master,
            "heap_percent": self._min_avg_max(heap_vals),
            "cpu_percent": self._min_avg_max(cpu_vals),
            "disk_used_percent": self._min_avg_max(disk_vals),
            "disk_hot_nodes": disk_hot_nodes[:self.top_n],
        }
        self.report["nodes_analysis"] = analysis
        return analysis

    @staticmethod
    def _min_avg_max(values: List[float]) -> Dict[str, float]:
        if not values:
            return {"min": 0, "avg": 0, "max": 0}
        return {
            "min": round(min(values), 1),
            "avg": round(sum(values) / len(values), 1),
            "max": round(max(values), 1),
        }

    def analyze_shards(self) -> Dict[str, Any]:
        rows = self.report.get("shards_raw", [])
        state_counter: Counter = Counter()
        bucket_counter: Counter = Counter()
        node_counter: Counter = Counter()
        sized: List[Dict[str, Any]] = []
        unassigned_sample: List[Dict[str, str]] = []
        index_shard_counts: Counter = Counter()

        for row in rows:
            state = row.get("state", "")
            state_counter[state] += 1
            if row.get("prirep") == "p":
                index_shard_counts[row.get("index", "")] += 1
            node = row.get("node", "")
            if node and state == "STARTED":
                node_counter[node] += 1
            size_bytes = parse_size(row.get("store"))
            if state == "STARTED" and size_bytes > 0:
                bucket_counter[shard_size_bucket(size_bytes)] += 1
                sized.append({
                    "index": row.get("index", ""),
                    "shard": row.get("shard", ""),
                    "prirep": row.get("prirep", ""),
                    "size_bytes": size_bytes,
                    "docs": to_int(row.get("docs")),
                    "node": node,
                })
            if state == "UNASSIGNED" and len(unassigned_sample) < self.top_n:
                unassigned_sample.append({
                    "index": row.get("index", ""),
                    "shard": row.get("shard", ""),
                    "prirep": row.get("prirep", ""),
                })

        sized.sort(key=lambda x: -x["size_bytes"])
        tiny = sum(1 for s in sized if s["size_bytes"] < 1024 ** 2 * 100)
        oversized = [s for s in sized if s["size_bytes"] >= 50 * 1024 ** 3]

        top_nodes = [
            {"node": n, "shards": c}
            for n, c in node_counter.most_common(self.top_n)
        ]
        top_shard_indices = [
            {"index": idx, "pri_shards": c}
            for idx, c in index_shard_counts.most_common(self.top_n)
            if c >= 1
        ][:self.top_n]

        analysis = {
            "total_shards": len(rows),
            "state_counts": dict(state_counter),
            "size_buckets": dict(bucket_counter),
            "shard_count": len(sized),
            "tiny_shards_lt100mb": tiny,
            "tiny_share": round(tiny / len(sized), 3) if sized else 0,
            "oversized_ge50gb_count": len(oversized),
            "largest_shards": sized[:self.top_n],
            "shards_per_node_top": top_nodes,
            "pri_shards_per_index_top": top_shard_indices,
            "unassigned_sample": unassigned_sample,
        }
        self.report["shards_analysis"] = analysis
        return analysis

    def build_index_records(self) -> List[Dict[str, Any]]:
        """合并 cat/indices + aliases + 分组信息，生成索引记录表"""
        cat_rows = self.report.get("indices_raw", [])
        alias_map = self.report.get("aliases_map", {})
        now_ms = time.time() * 1000

        records = []
        for row in cat_rows:
            name = row.get("index", "")
            if not name:
                continue
            if not self.include_system_indices and name.startswith("."):
                continue
            base, date_token = strip_date_suffix(name)
            creation = to_int(row.get("creation.date"))
            age_days = round((now_ms - creation) / 86400000, 1) if creation > 0 else None
            records.append({
                "index": name,
                "group": base,
                "date_pattern": date_token is not None,
                "date_token": date_token or "",
                "health": row.get("health", ""),
                "status": row.get("status", "open"),
                "pri": to_int(row.get("pri")),
                "rep": to_int(row.get("rep")),
                "docs": to_int(row.get("docs.count")),
                "deleted_docs": to_int(row.get("docs.deleted")),
                "store_bytes": parse_size(row.get("store.size")),
                "pri_store_bytes": parse_size(row.get("pri.store.size")),
                "segments": 0,
                "query_total": 0,
                "index_total": 0,
                "age_days": age_days,
                "aliases": alias_map.get(name, []),
                "qps": None,
                "ips": None,
                "activity": "",
            })

        self.report["indices_excluded_system"] = sum(
            1 for row in cat_rows if row.get("index", "").startswith(".")
        ) if not self.include_system_indices else 0
        self.report["index_records"] = records
        return records

    def merge_index_stats(self, stats_snapshot: Dict[str, Dict[str, int]]):
        """把 /_all/_stats 的 docs/store/segments/读写计数合并进索引记录"""
        records = self.report.get("index_records", [])
        by_name = {r["index"]: r for r in records}
        for index, snap in stats_snapshot.items():
            record = by_name.get(index)
            if not record:
                continue
            record["query_total"] = to_int(snap.get("query_total"))
            record["index_total"] = to_int(snap.get("index_total"))
        # segments/store 需要完整 stats 响应，重新取一次明细
        for index, detail in (getattr(self, "_stats_detail", {}) or {}).items():
            record = by_name.get(index)
            if not record:
                continue
            record["segments"] = to_int(detail.get("segments"))
            if record["store_bytes"] == 0:
                record["store_bytes"] = to_int(detail.get("total_store_bytes"))
                record["pri_store_bytes"] = to_int(detail.get("pri_store_bytes"))
            if record["docs"] == 0 and record["status"] != "close":
                record["docs"] = to_int(detail.get("docs"))

    def fetch_index_stats_detail(self) -> Dict[str, Dict[str, int]]:
        """拉取带明细的 _stats（docs/store/segments），同时填充读写计数快照"""
        try:
            resp = self.proxy_get(
                "/_all/_stats/docs,store,search,indexing,segments")
        except ConsoleAPIError as e:
            print(f"  警告: _stats 采集失败: {e}", file=sys.stderr)
            return {}
        detail: Dict[str, Dict[str, int]] = {}
        snapshot: Dict[str, Dict[str, int]] = {}
        for index, info in (resp.get("indices") or {}).items():
            primaries = info.get("primaries") or {}
            total = info.get("total") or {}
            docs = primaries.get("docs") or {}
            store = primaries.get("store") or {}
            total_store = total.get("store") or {}
            search = primaries.get("search") or {}
            indexing = primaries.get("indexing") or {}
            segments = primaries.get("segments") or {}
            detail[index] = {
                "docs": to_int(docs.get("count")),
                "pri_store_bytes": to_int(store.get("size_in_bytes")),
                "total_store_bytes": to_int(total_store.get("size_in_bytes")),
                "segments": to_int(segments.get("count")),
            }
            snapshot[index] = {
                "query_total": to_int(search.get("query_total")),
                "index_total": to_int(indexing.get("index_total")),
            }
        self._stats_detail = detail
        return snapshot

    def compute_activity(self):
        """计算两轮采样之间的每索引读写速率并分类"""
        interval = self.sample_interval
        if interval <= 0 or not self._stats_t0 or not self._stats_t1:
            self.report["activity"] = {"sampled": False}
            for record in self.report.get("index_records", []):
                if record["status"] == "close":
                    record["activity"] = "closed"
                else:
                    record["activity"] = "未采样"
            return

        elapsed = min(max(self._t1_wall - self._t0_wall, 1.0), interval * 3)
        rates = compute_index_rates(self._stats_t0, self._stats_t1, elapsed)

        records = self.report.get("index_records", [])
        qps_sum = 0.0
        ips_sum = 0.0
        active_read = 0
        active_write = 0
        open_count = 0
        classification: Counter = Counter()
        for record in records:
            if record["status"] == "close":
                record["activity"] = "closed"
                classification["closed"] += 1
                continue
            open_count += 1
            rate = rates.get(record["index"])
            if not rate:
                record["activity"] = "未采样"
                classification["未采样"] += 1
                continue
            qps = rate["qps"]
            ips = rate["ips"]
            record["qps"] = round(qps, 3)
            record["ips"] = round(ips, 3)
            record["activity"] = classify_activity(qps, ips)
            classification[record["activity"]] += 1
            qps_sum += qps
            ips_sum += ips
            if qps >= ACTIVE_THRESHOLD_OPS_PER_SEC:
                active_read += 1
            if ips >= ACTIVE_THRESHOLD_OPS_PER_SEC:
                active_write += 1

        read_rows = sorted(
            [r for r in records if r["qps"] is not None],
            key=lambda r: -r["qps"])
        write_rows = sorted(
            [r for r in records if r["ips"] is not None],
            key=lambda r: -r["ips"])

        self.report["activity"] = {
            "sampled": True,
            "elapsed_seconds": round(elapsed, 1),
            "qps_total": round(qps_sum, 2),
            "ips_total": round(ips_sum, 2),
            "open_indices": open_count,
            "active_read_indices": active_read,
            "active_write_indices": active_write,
            "classification_counts": dict(classification),
            "top_read": [
                {"index": r["index"], "qps": r["qps"],
                 "query_total": r["query_total"]}
                for r in read_rows[:self.top_n]
            ],
            "top_write": [
                {"index": r["index"], "ips": r["ips"],
                 "index_total": r["index_total"]}
                for r in write_rows[:self.top_n]
            ],
        }

    def build_groups(self) -> List[Dict[str, Any]]:
        """按命名分组聚合索引（业务域/时间序列画像）"""
        records = self.report.get("index_records", [])
        groups: Dict[str, Dict[str, Any]] = {}
        for r in records:
            g = groups.setdefault(r["group"], {
                "group": r["group"],
                "date_pattern": r["date_pattern"],
                "index_count": 0,
                "closed_count": 0,
                "docs": 0,
                "store_bytes": 0,
                "pri_store_bytes": 0,
                "write_active_count": 0,
            })
            g["index_count"] += 1
            if r["status"] == "close":
                g["closed_count"] += 1
            g["docs"] += r["docs"]
            g["store_bytes"] += r["store_bytes"]
            g["pri_store_bytes"] += r["pri_store_bytes"]
            if r["activity"] in ("写入为主", "读写均活跃"):
                g["write_active_count"] += 1

        group_list = sorted(groups.values(), key=lambda g: -g["store_bytes"])

        total_store = sum(g["store_bytes"] for g in group_list) or 1
        ts_store = sum(g["store_bytes"] for g in group_list if g["date_pattern"])
        ts_count = sum(g["index_count"] for g in group_list if g["date_pattern"])
        total_count = sum(g["index_count"] for g in group_list) or 1
        self.report["groups"] = group_list
        self.report["timeseries_share"] = {
            "store_share": round(ts_store / total_store, 3),
            "index_share": round(ts_count / total_count, 3),
        }
        return group_list

    def build_risks(self) -> List[str]:
        """风险检查"""
        risks: List[str] = []
        nodes = self.report.get("nodes_analysis", {})
        shards = self.report.get("shards_analysis", {})
        gov = self.report.get("governance", {})
        watermarks = self.report.get("overview", {}).get("disk_watermarks", {})
        flood = watermarks.get(
            "cluster.routing.allocation.disk.watermark.flood_stage", "95%")

        for node in nodes.get("disk_hot_nodes", []):
            if node["disk_used_percent"] >= 90:
                risks.append(
                    f"磁盘高危: 节点 {node['name']} 磁盘使用 {node['disk_used_percent']}%"
                    f"（剩余 {node['disk_avail']}），flood_stage 水位 {flood}，"
                    "超过后索引会被置为只读、写入失败")
        blocked = gov.get("blocked_indices") or []
        if blocked:
            names = ", ".join(b["index"] for b in blocked[:5])
            risks.append(
                f"只读阻断: {len(blocked)} 个索引存在 read_only/read_only_allow_delete "
                f"阻断（如 {names}），多为磁盘超水位所致")
        unassigned = shards.get("state_counts", {}).get("UNASSIGNED", 0)
        if unassigned:
            risks.append(f"未分配分片: {unassigned} 个分片未分配，需要排查")
        oversized = shards.get("oversized_ge50gb_count", 0)
        if oversized:
            risks.append(
                f"超大分片: {oversized} 个分片 >= 50GB，迁移与恢复耗时长，建议评估拆分")
        tiny_share = shards.get("tiny_share", 0)
        if tiny_share >= 0.5 and shards.get("shard_count", 0) > 100:
            risks.append(
                f"分片碎片化: {tiny_share * 100:.0f}% 分片小于 100MB，"
                "shard 开销大，建议合并索引或调整模板分片数")
        if not gov.get("snapshot_repos"):
            risks.append("未配置快照仓库（_snapshot 为空），数据备份能力存疑")
        pending = self.report.get("governance_pending_tasks") or []
        slow_pending = [t for t in pending
                        if to_int(t.get("time_in_queue_millis")) > 60000]
        if slow_pending:
            risks.append(
                f"集群 pending tasks 积压: {len(slow_pending)} 个任务排队超过 1 分钟")

        self.report["risks"] = risks
        return risks

    def build_scenario(self) -> List[str]:
        """使用场景推断"""
        activity = self.report.get("activity", {})
        api_usage = self.report.get("api_usage", {})
        ts = self.report.get("timeseries_share", {})

        bulk_count = search_count = get_count = 0
        if api_usage.get("available"):
            for action, count in (api_usage.get("rest_actions") or {}).items():
                family = match_action_family(action)
                if family == "Bulk批量写入":
                    bulk_count += to_int(count)
                elif family == "Search检索":
                    search_count += to_int(count)
                elif family == "Get点查":
                    get_count += to_int(count)

        labels = infer_scenario_labels(
            qps_total=to_float(activity.get("qps_total")),
            ips_total=to_float(activity.get("ips_total")),
            bulk_count=bulk_count,
            search_count=search_count,
            get_count=get_count,
            ts_store_share=to_float(ts.get("store_share")),
            ts_index_share=to_float(ts.get("index_share")),
            active_write_count=to_int(activity.get("active_write_indices")),
            total_open_count=to_int(activity.get("open_indices")),
            sample_interval=self.sample_interval,
        )
        self.report["scenario"] = labels
        return labels

    # ---------------- 编排 ----------------

    def collect(self):
        """完整采集流程：两轮 _stats 采样之间穿插其余采集"""
        self.collect_overview()
        self.collect_nodes()
        self.collect_shards()

        print("首轮索引读写计数采样（t0）...")
        self._stats_t0 = self.fetch_index_stats_detail()
        self._t0_wall = time.time()

        self.collect_indices_static()
        self.collect_api_usage()
        self.collect_governance()

        if self.sample_interval > 0:
            elapsed = time.time() - self._t0_wall
            wait = max(0, self.sample_interval - int(elapsed))
            if wait > 0:
                print(f"等待 {wait}s 后进行第二轮采样（--sample-interval 可调整）...")
                time.sleep(wait)
            print("第二轮索引读写计数采样（t1）...")
            self._stats_t1 = self.fetch_index_stats()
            self._t1_wall = time.time()
        else:
            self._stats_t1 = {}
            self._t1_wall = 0.0

        print("分析中...")
        self.build_index_records()
        self.merge_index_stats(self._stats_t0)
        self.analyze_nodes()
        self.analyze_shards()
        self.build_groups()
        self.compute_activity()
        self.build_risks()
        self.build_scenario()

    # ---------------- 输出 ----------------

    def generate_markdown(self) -> str:
        overview = self.report.get("overview", {})
        cluster = self.report.get("cluster", {})
        nodes = self.report.get("nodes_analysis", {})
        shards = self.report.get("shards_analysis", {})
        groups = self.report.get("groups", [])
        activity = self.report.get("activity", {})
        api_usage = self.report.get("api_usage", {})
        gov = self.report.get("governance", {})
        ts = self.report.get("timeseries_share", {})
        fmt = ConsoleClient.format_bytes

        lines: List[str] = []
        lines.append(f"# 集群使用画像: {cluster.get('cluster_name', self.display_name)}")
        lines.append("")
        lines.append(
            f"- 采集时间: {self.report.get('collected_at')}　|　"
            f"版本: {cluster.get('version', '')}　|　"
            f"健康: {overview.get('status', '')}")
        lines.append(
            f"- Console 集群 ID: `{self.cluster_id}`　|　"
            f"采样间隔: {self.sample_interval}s")
        lines.append("")

        # 概览
        lines.append("## 1. 集群概览")
        lines.append("")
        lines.append("| 指标 | 值 |")
        lines.append("|------|-----|")
        lines.append(f"| 节点数 | {overview.get('number_of_nodes', 0)} "
                     f"(数据节点 {overview.get('number_of_data_nodes', 0)}) |")
        lines.append(f"| 主分片 / 总分片 | {overview.get('active_primary_shards', 0)} / "
                     f"{overview.get('active_shards', 0)} |")
        lines.append(f"| 未分配分片 | {overview.get('unassigned_shards', 0)} |")
        lines.append(f"| Pending Tasks | {overview.get('pending_tasks_count', 0)} |")
        watermarks = overview.get("disk_watermarks", {})
        if watermarks:
            wm = watermarks.get(
                "cluster.routing.allocation.disk.watermark.flood_stage", "-")
            lines.append(f"| 磁盘 flood_stage 水位 | {wm} |")
        lines.append("")

        # 节点画像
        lines.append("## 2. 节点画像")
        lines.append("")
        lines.append(f"- 当前 master: **{nodes.get('current_master', '-')}**")
        role_counts = nodes.get("role_counts", {})
        lines.append(
            f"- 角色分布: data={role_counts.get('data', 0)}, "
            f"master候选={role_counts.get('master_eligible', 0)}, "
            f"ingest={role_counts.get('ingest', 0)}, "
            f"coordinating-only={role_counts.get('coordinating', 0)}")
        heap = nodes.get("heap_percent", {})
        cpu = nodes.get("cpu_percent", {})
        disk = nodes.get("disk_used_percent", {})
        lines.append(
            f"- Heap 使用率: min {heap.get('min', 0)}% / avg {heap.get('avg', 0)}% "
            f"/ max {heap.get('max', 0)}%")
        lines.append(
            f"- CPU: max {cpu.get('max', 0)}%　|　"
            f"磁盘使用率: min {disk.get('min', 0)}% / avg {disk.get('avg', 0)}% "
            f"/ max {disk.get('max', 0)}%")
        hot_nodes = nodes.get("disk_hot_nodes", [])
        if hot_nodes:
            lines.append("")
            lines.append("### 磁盘水位 TOP（>=85%）")
            lines.append("")
            lines.append("| 节点 | 磁盘使用率 | 剩余空间 |")
            lines.append("|------|-----------|---------|")
            for n in hot_nodes[:10]:
                lines.append(f"| {n['name']} | {n['disk_used_percent']}% "
                             f"| {n['disk_avail']} |")
        lines.append("")

        # 分片画像
        lines.append("## 3. 分片画像")
        lines.append("")
        states = shards.get("state_counts", {})
        lines.append(
            f"- 分片状态: STARTED={states.get('STARTED', 0)}, "
            f"UNASSIGNED={states.get('UNASSIGNED', 0)}, "
            f"RELOCATING={states.get('RELOCATING', 0)}, "
            f"INITIALIZING={states.get('INITIALIZING', 0)}")
        buckets = shards.get("size_buckets", {})
        if buckets:
            lines.append("")
            lines.append("### 分片大小分布")
            lines.append("")
            lines.append("| 大小区间 | 分片数 |")
            lines.append("|---------|-------|")
            for label in ["<100MB", "100MB-1GB", "1-10GB", "10-30GB",
                          "30-50GB", ">=50GB"]:
                if label in buckets:
                    lines.append(f"| {label} | {buckets[label]} |")
        largest = shards.get("largest_shards", [])
        if largest:
            lines.append("")
            lines.append(f"### 最大分片 TOP{self.top_n}")
            lines.append("")
            lines.append("| 索引 | 分片 | 大小 | 文档数 | 节点 |")
            lines.append("|------|------|------|--------|------|")
            for s in largest[:self.top_n]:
                lines.append(
                    f"| {s['index']} | {s['shard']}{s['prirep']} "
                    f"| {fmt(s['size_bytes'])} | {s['docs']:,} | {s['node']} |")
        top_nodes = shards.get("shards_per_node_top", [])
        if top_nodes:
            lines.append("")
            lines.append(f"### 节点分片数 TOP{self.top_n}（检查均衡度）")
            lines.append("")
            lines.append("| 节点 | 分片数 |")
            lines.append("|------|--------|")
            for n in top_nodes[:10]:
                lines.append(f"| {n['node']} | {n['shards']} |")
        lines.append("")

        # 索引分组
        lines.append("## 4. 索引与业务域画像")
        lines.append("")
        total_records = len(self.report.get("index_records", []))
        excluded = self.report.get("indices_excluded_system", 0)
        closed = sum(g["closed_count"] for g in groups)
        lines.append(
            f"- 共 {total_records} 个非系统索引"
            + (f"（另有 {excluded} 个系统索引未纳入）" if excluded else "")
            + (f"，其中 {closed} 个已 close" if closed else ""))
        lines.append(
            f"- 时间序列命名占比: 按数量 {ts.get('index_share', 0) * 100:.0f}%，"
            f"按存储 {ts.get('store_share', 0) * 100:.0f}%")
        lines.append("")
        lines.append(f"### 命名分组 TOP{self.top_n}（按存储）")
        lines.append("")
        lines.append("| 分组 | 时间序列 | 索引数 | 文档数 | 存储 | 主分片存储 | 近期有写入 |")
        lines.append("|------|---------|--------|--------|------|-----------|-----------|")
        for g in groups[:self.top_n]:
            lines.append(
                f"| {g['group']} | {'是' if g['date_pattern'] else '否'} "
                f"| {g['index_count']} | {g['docs']:,} "
                f"| {fmt(g['store_bytes'])} | {fmt(g['pri_store_bytes'])} "
                f"| {g['write_active_count']} |")
        lines.append("")

        # 读写热度
        lines.append("## 5. 读写热度（索引级）")
        lines.append("")
        if activity.get("sampled"):
            lines.append(
                f"- 采样窗口: {activity.get('elapsed_seconds', 0)}s　|　"
                f"分片级查询总量: ~{activity.get('qps_total', 0)} ops/s　|　"
                f"分片级写入总量: ~{activity.get('ips_total', 0)} ops/s")
            lines.append(
                f"- 活跃索引: 有查询 {activity.get('active_read_indices', 0)} 个，"
                f"有写入 {activity.get('active_write_indices', 0)} 个"
                f"（共 {activity.get('open_indices', 0)} 个 open 索引）")
            classification = activity.get("classification_counts", {})
            if classification:
                parts = [f"{k}={v}" for k, v in sorted(classification.items())]
                lines.append(f"- 活跃度分布: {', '.join(parts)}")
            top_read = activity.get("top_read", [])
            if top_read:
                lines.append("")
                lines.append(f"### 查询热度 TOP{self.top_n}")
                lines.append("")
                lines.append("| 索引 | 采样QPS(分片级) | 累计查询次数 |")
                lines.append("|------|----------------|--------------|")
                for r in top_read[:self.top_n]:
                    lines.append(f"| {r['index']} | {r['qps']} | {r['query_total']:,} |")
            top_write = activity.get("top_write", [])
            if top_write:
                lines.append("")
                lines.append(f"### 写入热度 TOP{self.top_n}")
                lines.append("")
                lines.append("| 索引 | 采样写入速率(ops/s) | 累计写入次数 |")
                lines.append("|------|--------------------|--------------|")
                for r in top_write[:self.top_n]:
                    lines.append(f"| {r['index']} | {r['ips']} | {r['index_total']:,} |")
        else:
            lines.append("> 未启用速率采样（sample-interval=0），仅静态采集。")
        lines.append("")

        # API 画像
        lines.append("## 6. API 使用画像（_nodes/usage）")
        lines.append("")
        if api_usage.get("available"):
            actions = api_usage.get("rest_actions") or {}
            lines.append(
                f"- 上报节点: {api_usage.get('nodes_reported', 0)} 个"
                "（计数自节点重启起累计，受 uptime 影响，仅作参考）")
            family_counter: Counter = Counter()
            for action, count in actions.items():
                family_counter[match_action_family(action)] += to_int(count)
            lines.append("")
            lines.append("| 使用家族 | 累计请求数 |")
            lines.append("|---------|-----------|")
            for family, count in family_counter.most_common(10):
                lines.append(f"| {family} | {count:,} |")
            top_actions = sorted(actions.items(), key=lambda kv: -kv[1])[:self.top_n]
            if top_actions:
                lines.append("")
                lines.append(f"### REST Action TOP{self.top_n}")
                lines.append("")
                lines.append("| Action | 累计次数 |")
                lines.append("|--------|---------|")
                for action, count in top_actions:
                    lines.append(f"| `{action}` | {count:,} |")
        else:
            lines.append("> 该集群 _nodes/usage 不可用，跳过。")
        lines.append("")

        # 治理
        lines.append("## 7. 治理与配置")
        lines.append("")
        templates = gov.get("templates", [])
        lines.append(f"- Index Templates: {len(templates)} 个")
        for t in templates[:10]:
            lines.append(f"  - `{t['name']}` → {t['index_patterns']}")
        ilm_policies = gov.get("ilm_policies", {})
        if gov.get("ilm_available"):
            lines.append(f"- ILM 策略: {len(ilm_policies)} 个")
            for name, info in list(ilm_policies.items())[:10]:
                lines.append(f"  - `{name}`: phases={','.join(info['phases'])}")
        else:
            lines.append("- ILM: 不可用（未启用 x-pack 或版本过老）")
        repos = gov.get("snapshot_repos", {})
        if repos:
            lines.append(f"- 快照仓库: " + ", ".join(
                f"{name}({info['type']})" for name, info in repos.items()))
        else:
            lines.append("- 快照仓库: 无")
        aliases_map = self.report.get("aliases_map", {})
        lines.append(
            f"- 别名: {len(set(a for lst in aliases_map.values() for a in lst))} 个，"
            f"覆盖 {len(aliases_map)} 个索引")
        slowlog = gov.get("slowlog_enabled_indices", [])
        lines.append(
            f"- Slowlog: {len(slowlog)} 个索引配置了慢日志"
            + (f"（如 {', '.join(slowlog[:3])}）" if slowlog else ""))
        blocked = gov.get("blocked_indices", [])
        if blocked:
            lines.append(f"- 只读阻断索引: {len(blocked)} 个: "
                         + ", ".join(b["index"] for b in blocked[:10]))
        pending = self.report.get("governance_pending_tasks", [])
        if pending:
            lines.append(f"- Pending Tasks: {len(pending)} 个排队中")
        lines.append("")

        # 风险
        risks = self.report.get("risks", [])
        lines.append("## 8. 风险与建议")
        lines.append("")
        if risks:
            for r in risks:
                lines.append(f"- ⚠ {r}")
        else:
            lines.append("- 未发现明显风险项。")
        lines.append("")

        # 场景推断
        lines.append("## 9. 使用场景推断（自动分析，供参考）")
        lines.append("")
        for label in self.report.get("scenario", []):
            lines.append(f"- {label}")
        lines.append("")

        return "\n".join(lines)

    CSV_HEADERS = [
        "索引名", "命名分组", "时间序列", "健康", "状态", "主分片", "副本",
        "文档数", "已删文档", "总存储MB", "主分片存储MB", "Segment数",
        "累计查询次数", "累计写入次数", "采样QPS", "采样写入ops/s",
        "活跃度", "索引年龄(天)", "别名",
    ]

    def generate_csv(self, output_file: str) -> str:
        records = self.report.get("index_records", [])
        with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(self.CSV_HEADERS)
            for r in records:
                writer.writerow([
                    r["index"], r["group"],
                    "是" if r["date_pattern"] else "否",
                    r["health"], r["status"], r["pri"], r["rep"],
                    r["docs"], r["deleted_docs"],
                    round(r["store_bytes"] / 1024 ** 2, 2),
                    round(r["pri_store_bytes"] / 1024 ** 2, 2),
                    r["segments"], r["query_total"], r["index_total"],
                    r["qps"] if r["qps"] is not None else "",
                    r["ips"] if r["ips"] is not None else "",
                    r["activity"],
                    r["age_days"] if r["age_days"] is not None else "",
                    ";".join(r["aliases"]),
                ])
        return output_file

    def print_summary(self):
        cluster = self.report.get("cluster", {})
        overview = self.report.get("overview", {})
        nodes = self.report.get("nodes_analysis", {})
        shards = self.report.get("shards_analysis", {})
        activity = self.report.get("activity", {})
        fmt = ConsoleClient.format_bytes

        print()
        print("=" * 60)
        print(f"集群使用画像: {cluster.get('cluster_name', self.display_name)} "
              f"(v{cluster.get('version', '')}, {overview.get('status', '')})")
        print("=" * 60)
        print(f"节点: {overview.get('number_of_nodes', 0)} "
              f"(data={nodes.get('role_counts', {}).get('data', 0)})　"
              f"分片: {shards.get('total_shards', 0)}　"
              f"索引: {len(self.report.get('index_records', []))}")
        disk = nodes.get("disk_used_percent", {})
        print(f"磁盘使用率: max {disk.get('max', 0)}%　"
              f"Heap: max {nodes.get('heap_percent', {}).get('max', 0)}%")
        if activity.get("sampled"):
            print(f"读写速率(分片级): 查询 ~{activity.get('qps_total', 0)} ops/s，"
                  f"写入 ~{activity.get('ips_total', 0)} ops/s")
            print(f"活跃索引: 有查询 {activity.get('active_read_indices', 0)}，"
                  f"有写入 {activity.get('active_write_indices', 0)}")
        ts = self.report.get("timeseries_share", {})
        print(f"时间序列索引占比: 按存储 {ts.get('store_share', 0) * 100:.0f}%")
        risks = self.report.get("risks", [])
        print(f"风险项: {len(risks)}")
        for r in risks[:5]:
            print(f"  - {r}")
        print("=" * 60)


# ---------------------------------------------------------------------------
# 命令行入口
# ---------------------------------------------------------------------------

def list_clusters_and_exit(client: ConsoleClient):
    """未指定目标集群时，列出可选集群"""
    clusters = client.get_clusters()
    print("\n未指定目标集群，可用的集群（--cluster-id 或 --cluster-name）:")
    print(f"{'ID':<28} {'名称':<36} 版本")
    for c in clusters:
        print(f"{str(c.get('id', '')):<28} {str(c.get('name', '')):<36} "
              f"{c.get('version', '')}")
    sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Cluster Usage Profile - 采集单个 ES 集群的使用情况与使用场景画像",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 按集群名采集（推荐，两轮采样默认间隔 60s）
  python cluster_usage_profile.py -c http://localhost:9000 -u admin -p password \\
      --cluster-name my-cluster

  # 按集群 ID 采集，采样 120s，输出到指定目录
  python cluster_usage_profile.py --config config.json --cluster-id xxx \\
      --sample-interval 120 -o ./exports

  # 不等待采样（只做静态画像）
  python cluster_usage_profile.py --config config.json --cluster-name my-cluster \\
      --sample-interval 0

Environment Variables:
  CONSOLE_URL           Console URL (默认: http://localhost:9000)
  CONSOLE_USERNAME      用户名
  CONSOLE_PASSWORD      密码
  CONSOLE_CLUSTER_ID    目标集群 ID
  CONSOLE_CLUSTER_NAME  目标集群名称
        """,
    )

    parser = add_common_args(parser)

    parser.add_argument(
        "--cluster-id",
        default="",
        help="目标集群 ID (环境变量: CONSOLE_CLUSTER_ID)")
    parser.add_argument(
        "--cluster-name",
        default="",
        help="目标集群名称（Console 显示名，支持部分匹配）(环境变量: CONSOLE_CLUSTER_NAME)")
    parser.add_argument(
        "--sample-interval",
        type=int,
        default=60,
        help="两轮读写采样的间隔秒数，0 表示关闭速率采样 (默认: 60)")
    parser.add_argument(
        "--include-system-indices",
        action="store_true",
        help="包含系统索引（. 开头）")
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="报告中 TOP 列表的条数 (默认: 20)")

    return parser.parse_args()


def main():
    args = parse_args()
    config, _ = load_and_merge_config(args)

    console_url = get_config_value(
        args.console, config.get('consoleUrl'), 'CONSOLE_URL', 'http://localhost:9000')
    username = get_config_value(
        args.username, config.get('auth', {}).get('username'), 'CONSOLE_USERNAME', '')
    password = get_config_value(
        args.password, config.get('auth', {}).get('password'), 'CONSOLE_PASSWORD', '')
    timeout = int(get_config_value(
        str(args.timeout), str(config.get('timeout')), 'CONSOLE_TIMEOUT', '60'))
    insecure = args.insecure or config.get('insecure', False)

    cluster_id = get_config_value(
        args.cluster_id, config.get('clusterId'), 'CONSOLE_CLUSTER_ID', '')
    cluster_name = get_config_value(
        args.cluster_name, config.get('clusterName'), 'CONSOLE_CLUSTER_NAME', '')
    sample_interval = int(get_config_value(
        str(args.sample_interval), str(config.get('sampleInterval')),
        'CONSOLE_SAMPLE_INTERVAL', '60'))
    include_system = args.include_system_indices or config.get(
        'includeSystemIndices', False)
    top_n = int(get_config_value(str(args.top), str(config.get('top')), '', '20'))

    output = args.output or config.get('output') or "exports"

    import getpass
    if username and not password:
        password = getpass.getpass(f"请输入 {username} 的密码: ")

    print(f"连接到 Console: {console_url}")
    client = ConsoleClient(console_url, username, password,
                           timeout=timeout, verify_ssl=not insecure)
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

    # 解析目标集群
    if not cluster_id and not cluster_name:
        list_clusters_and_exit(client)
    if not cluster_id and cluster_name:
        try:
            cluster_id = client.resolve_cluster_id_by_name(cluster_name)
            print(f"已根据 clusterName 解析 clusterId: {cluster_name} -> {cluster_id}")
        except ConsoleAPIError as e:
            print(f"解析集群失败: {e}")
            sys.exit(1)

    # 检查集群可用性
    try:
        status = client.get_cluster_status(cluster_id)
        if not status.get("available", False):
            print(f"集群不可用（available=false），请检查 Console 中该集群的连接状态")
            sys.exit(1)
    except ConsoleAPIError as e:
        print(f"获取集群状态失败: {e}")
        sys.exit(1)

    profiler = UsageProfiler(
        client, cluster_id,
        display_name=cluster_name or cluster_id,
        include_system_indices=include_system,
        top_n=top_n,
        sample_interval=sample_interval,
    )

    try:
        profiler.collect()
    except ConsoleAPIError as e:
        print(f"\n采集失败: {e}")
        sys.exit(1)

    profiler.print_summary()

    # 输出文件
    output_dir = Path(output)
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^\w.-]+", "_", profiler.display_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"usage_profile_{safe_name}_{timestamp}"

    md_file = output_dir / f"{base}.md"
    md_file.write_text(profiler.generate_markdown(), encoding="utf-8")
    csv_file = profiler.generate_csv(str(output_dir / f"{base}_indices.csv"))
    json_file = output_dir / f"{base}.json"
    json_file.write_text(
        json.dumps(profiler.report, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8")

    print(f"\nMarkdown 报告: {md_file}")
    print(f"索引明细 CSV: {csv_file}")
    print(f"JSON 明细:    {json_file}")


if __name__ == "__main__":
    main()
