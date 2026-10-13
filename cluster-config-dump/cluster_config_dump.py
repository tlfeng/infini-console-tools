#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cluster Config Dump - 集群配置全量采集工具

通过 INFINI Console 代理接口（_proxy）对 Elasticsearch 集群执行一批只读
GET API，把每个 API 的响应落盘为 JSON，用于离线判断集群能否正常运行：
配置快照、健康体检、变更前后 diff 都基于同一份产物目录。

API 目录按类别组织（参考 elasticsearch 7.10 官方文档 Cluster APIs 分类，
并结合 7.10.2 源码逐条核对过路由）：
- cluster      Cluster APIs 核心（health/state/stats/settings/nodes/tasks 等）
- cat          Cat APIs 人读汇总视图
- index        索引/模板/管道/ILM/SLM/快照配置
- license      License 与 xpack 功能开关
- diagnostic   诊断与功能模块健康（deprecations/watcher/ccr/transform 等）
- ml           机器学习（未启用时报错属预期）

支持交互式菜单选择集群与类别；也提供 --groups/--yes/--skip-heavy 非交互参数。

输出目录结构（清晰分组，manifest 可追溯每条 API 的状态与耗时）:
  <output>/config_dump_<时间戳>_<集群>/
  ├── manifest.json          每条 API 的状态/耗时/大小/错误
  ├── summary.md             自动体检摘要
  ├── 01_cluster/001_root.json ...
  ├── 02_cat/ ...
  └── ...

用法:
  python cluster_config_dump.py -c http://localhost:9000 -u admin -p password
      # 交互式：选择集群、选择类别

  python cluster_config_dump.py --config config.json --cluster-name prod \\
      --groups cluster,cat,index --skip-heavy
      # 非交互：只采指定类别，跳过重接口

  python cluster_config_dump.py --list
      # 只打印 API 目录
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))
from common.console_client import (
    ConsoleClient,
    ConsoleAPIError,
    ConsoleAuthError,
    create_authenticated_client,
)
from common.config import add_common_args, load_and_merge_config, get_config_value


# ---------------------------------------------------------------------------
# API 目录（依据 elasticsearch v7.10.2 源码 Rest*Action 路由逐条核对）
#
# 跳过的子 API（被通配父 API 完全包含，符合"父 API 覆盖即可"的约定）:
#   /_cluster/health/{index}、/_cluster/state/{metric}[/{indices}]、
#   /_cluster/stats/nodes/{nodeId}、/_nodes/{nodeId}/... 全系、
#   /_nodes/stats/{metric}...、/_nodes/usage/{metric}、/_tasks/{task_id}、
#   /_cat/xxx/{index|nodes|fields|alias|repository|name} 等子路径
# 跳过的写操作: reroute(POST)、update settings(PUT)、reload secure settings(POST)、
#   tasks cancel(POST)、voting config exclusions(POST/DELETE)
# 7.10.2 不存在: GET /_nodes/shutdown（8.x）、GET /_features（7.12+）
# ---------------------------------------------------------------------------

API_GROUPS: List[Dict[str, Any]] = [
    {
        "key": "cluster",
        "title": "Cluster APIs 核心",
        "description": "官方 Cluster APIs 目录中的全部可执行 GET",
        "apis": [
            {"name": "root", "path": "/", "note": "版本号与 cluster_uuid"},
            {"name": "cluster_health", "path": "/_cluster/health",
             "note": "red/yellow、未分配分片、task 队列"},
            {"name": "cluster_health_level_indices", "path": "/_cluster/health?level=indices",
             "note": "索引级健康，定位具体异常索引"},
            {"name": "cluster_stats", "path": "/_cluster/stats",
             "note": "节点/磁盘/JVM/内存汇总，体检核心"},
            {"name": "cluster_state", "path": "/_cluster/state", "heavy": True,
             "note": "全量集群状态，大集群响应可达数十 MB"},
            {"name": "cluster_settings", "path": "/_cluster/settings?include_defaults=true",
             "note": "persistent/transient 用户设置 + defaults 全量默认值"},
            {"name": "cluster_pending_tasks", "path": "/_cluster/pending_tasks",
             "note": "master 待处理队列积压"},
            {"name": "nodes_info", "path": "/_nodes?flat_settings=true",
             "note": "节点角色/版本/JVM/插件/全部节点设置"},
            {"name": "nodes_stats", "path": "/_nodes/stats",
             "note": "线程池拒绝、GC、磁盘、breaker 等运行指标"},
            {"name": "nodes_usage", "path": "/_nodes/usage",
             "note": "功能模块使用统计"},
            {"name": "nodes_hot_threads", "path": "/_nodes/hot_threads",
             "note": "CPU 热点线程快照"},
            {"name": "remote_info", "path": "/_remote/info",
             "note": "跨集群（CCS）配置与连通性"},
            {"name": "tasks", "path": "/_tasks",
             "note": "当前运行中的任务"},
            {"name": "cluster_allocation_explain", "path": "/_cluster/allocation/explain",
             "note": "分片未分配原因；全部分配正常时返回 400，属预期"},
        ],
    },
    {
        "key": "cat",
        "title": "Cat APIs 汇总",
        "description": "人读性最好的紧凑视图，全部 ?format=json",
        "apis": [
            {"name": "cat_health", "path": "/_cat/health?format=json"},
            {"name": "cat_nodes", "path": "/_cat/nodes?format=json"},
            {"name": "cat_indices", "path": "/_cat/indices?format=json"},
            {"name": "cat_shards", "path": "/_cat/shards?format=json"},
            {"name": "cat_allocation", "path": "/_cat/allocation?format=json"},
            {"name": "cat_pending_tasks", "path": "/_cat/pending_tasks?format=json"},
            {"name": "cat_recovery", "path": "/_cat/recovery?format=json"},
            {"name": "cat_segments", "path": "/_cat/segments?format=json"},
            {"name": "cat_thread_pool", "path": "/_cat/thread_pool?format=json"},
            {"name": "cat_repositories", "path": "/_cat/repositories?format=json"},
            {"name": "cat_snapshots", "path": "/_cat/snapshots/_all?format=json",
             "expected_status": [404],
             "note": "未配置仓库时 ES 返回 404，属预期，会记为跳过"},
            {"name": "cat_templates", "path": "/_cat/templates?format=json"},
            {"name": "cat_plugins", "path": "/_cat/plugins?format=json"},
            {"name": "cat_nodeattrs", "path": "/_cat/nodeattrs?format=json"},
            {"name": "cat_master", "path": "/_cat/master?format=json"},
            {"name": "cat_fielddata", "path": "/_cat/fielddata?format=json"},
            {"name": "cat_count", "path": "/_cat/count?format=json"},
            {"name": "cat_tasks", "path": "/_cat/tasks?format=json"},
            {"name": "cat_aliases", "path": "/_cat/aliases?format=json"},
        ],
    },
    {
        "key": "index",
        "title": "索引与治理配置",
        "description": "索引级聚合配置、模板、ILM/SLM、快照",
        "apis": [
            {"name": "index_stats", "path": "/_stats", "heavy": True,
             "note": "全部索引统计（docs/store/search/indexing/merges）"},
            {"name": "index_settings", "path": "/_settings",
             "note": "索引级设置（含只读阻断、slowlog）"},
            {"name": "index_mapping", "path": "/_mapping"},
            {"name": "index_alias", "path": "/_alias"},
            {"name": "index_segments", "path": "/_segments", "heavy": True},
            {"name": "index_recovery", "path": "/_recovery",
             "note": "进行中/已完成的分片恢复"},
            {"name": "index_shard_stores", "path": "/_shard_stores", "heavy": True,
             "note": "分片副本在各节点的存放情况"},
            {"name": "index_templates_legacy", "path": "/_template",
             "note": "6.x 遗留模板"},
            {"name": "index_templates_composable", "path": "/_index_template",
             "note": "7.8+ 可组合索引模板"},
            {"name": "component_templates", "path": "/_component_template"},
            {"name": "ingest_pipelines", "path": "/_ingest/pipeline"},
            {"name": "ilm_policies", "path": "/_ilm/policy"},
            {"name": "ilm_status", "path": "/_ilm/status",
             "note": "ILM 运行状态（STOPPED 说明生命周期管理停摆）"},
            {"name": "slm_policies", "path": "/_slm/policy"},
            {"name": "slm_stats", "path": "/_slm/stats"},
            {"name": "slm_status", "path": "/_slm/status"},
            {"name": "snapshot_repositories", "path": "/_snapshot",
             "note": "快照仓库列表，为空说明没有备份"},
            {"name": "snapshot_status", "path": "/_snapshot/_status",
             "note": "当前正在运行的快照"},
            {"name": "dangling_indices", "path": "/_dangling",
             "note": "游离索引（磁盘上存在但集群态缺失）"},
        ],
    },
    {
        "key": "license",
        "title": "License 与 xpack",
        "description": "授权状态与功能开关（过期会导致功能退化）",
        "apis": [
            {"name": "xpack_info", "path": "/_xpack"},
            {"name": "xpack_usage", "path": "/_xpack/usage",
             "note": "各功能 enabled/available 开关"},
            {"name": "license", "path": "/_license",
             "note": "授权类型/状态/到期时间"},
            {"name": "license_basic_status", "path": "/_license/basic_status"},
            {"name": "license_trial_status", "path": "/_license/trial_status"},
            {"name": "license_feature_usage", "path": "/_license/feature_usage"},
        ],
    },
    {
        "key": "diagnostic",
        "title": "诊断与模块健康",
        "description": "升级检查、账号身份、周边功能模块状态",
        "apis": [
            {"name": "migration_deprecations", "path": "/_migration/deprecations",
             "note": "升级阻塞项（配置/模板/mapping 弃用告警）"},
            {"name": "security_authenticate", "path": "/_security/_authenticate",
             "note": "Console 代理账号身份与角色"},
            {"name": "watcher_stats", "path": "/_watcher/stats"},
            {"name": "enrich_policies", "path": "/_enrich/policy"},
            {"name": "enrich_stats", "path": "/_enrich/_stats"},
            {"name": "ccr_stats", "path": "/_ccr/stats", "expected_status": [403],
             "note": "基础 license 不含 CCR，返回 403 属预期"},
            {"name": "rollup_jobs", "path": "/_rollup/job/_all"},
            {"name": "transform_jobs", "path": "/_transform"},
            {"name": "transform_stats", "path": "/_transform/_stats"},
            {"name": "script_contexts", "path": "/_script_context"},
            {"name": "script_languages", "path": "/_script_language"},
        ],
    },
    {
        "key": "ml",
        "title": "机器学习",
        "description": "未启用 ML 时这些 API 报错属预期",
        "apis": [
            {"name": "ml_info", "path": "/_ml/info",
             "note": "未启用 ML 时报错，属预期"},
            {"name": "ml_anomaly_detectors", "path": "/_ml/anomaly_detectors"},
            {"name": "ml_anomaly_detectors_stats", "path": "/_ml/anomaly_detectors/_stats"},
            {"name": "ml_datafeeds", "path": "/_ml/datafeeds"},
            {"name": "ml_datafeeds_stats", "path": "/_ml/datafeeds/_stats"},
            {"name": "ml_data_frame_analytics", "path": "/_ml/data_frame/analytics"},
            {"name": "ml_data_frame_analytics_stats", "path": "/_ml/data_frame/analytics/_stats"},
        ],
    },
]

GROUP_KEYS = [g["key"] for g in API_GROUPS]


def slugify(name: str) -> str:
    """API 名称转安全文件名片段"""
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", name).strip("_") or "unnamed"


def parse_group_selection(spec: str) -> List[str]:
    """
    解析类别选择参数。

    支持: "" / "all" → 全部；"1,3,5" 编号（对应 API_GROUPS 顺序）；
    "cluster,cat" 类别 key；编号与 key 可混用。返回去重后的 key 列表。
    """
    spec = (spec or "").strip()
    if not spec or spec.lower() == "all":
        return list(GROUP_KEYS)

    chosen: List[str] = []
    for token in re.split(r"[,，\s]+", spec):
        token = token.strip()
        if not token:
            continue
        if token.lower() == "all":
            for key in GROUP_KEYS:
                if key not in chosen:
                    chosen.append(key)
            continue
        if token.isdigit():
            idx = int(token)
            if not 1 <= idx <= len(API_GROUPS):
                raise ValueError(
                    f"类别编号 {idx} 超出范围 (1-{len(API_GROUPS)})")
            key = GROUP_KEYS[idx - 1]
        else:
            if token not in GROUP_KEYS:
                raise ValueError(
                    f"未知类别 '{token}'，可选: {', '.join(GROUP_KEYS)}")
            key = token
        if key not in chosen:
            chosen.append(key)
    if not chosen:
        raise ValueError("类别选择为空")
    return chosen


def build_plan(selected_keys: List[str],
               include_heavy: bool = True) -> List[Dict[str, Any]]:
    """
    构建执行计划：按选定类别顺序展开 API 条目。

    返回条目: {seq, group_key, name, path, note, heavy}
    """
    plan: List[Dict[str, Any]] = []
    seq = 0
    for group in API_GROUPS:
        if group["key"] not in selected_keys:
            continue
        for api in group["apis"]:
            if api.get("heavy") and not include_heavy:
                continue
            seq += 1
            plan.append({
                "seq": seq,
                "group_key": group["key"],
                "name": api["name"],
                "path": api["path"],
                "note": api.get("note", ""),
                "heavy": bool(api.get("heavy")),
                "expected_status": list(api.get("expected_status", [])),
            })
    return plan


def print_catalog(selected_keys: Optional[List[str]] = None) -> None:
    """打印完整 API 目录（--list 用）"""
    selected_keys = selected_keys or GROUP_KEYS
    total = 0
    for idx, group in enumerate(API_GROUPS, 1):
        mark = "*" if group["key"] in selected_keys else " "
        heavies = [a["name"] for a in group["apis"] if a.get("heavy")]
        extra = f"，重接口: {', '.join(heavies)}" if heavies else ""
        print(f"{mark} [{idx}] {group['key']:<10} {group['title']}"
              f"（{len(group['apis'])} 项{extra}）")
        for api in group["apis"]:
            heavy = " [重]" if api.get("heavy") else ""
            note = f"  — {api['note']}" if api.get("note") else ""
            print(f"        GET {api['path']}{heavy}{note}")
            total += 1
    print(f"\n共 {len(API_GROUPS)} 类 / {total} 个 API（* = 已选择）")


def choose_cluster_interactive(client: ConsoleClient) -> Tuple[str, str]:
    """交互式选择目标集群，返回 (cluster_id, display_name)"""
    clusters = [
        c for c in client.get_clusters()
        if not ConsoleClient.is_system_cluster(str(c.get("id", "")), str(c.get("name", "")))
    ]
    if not clusters:
        print("Console 中没有可采集的非系统集群")
        sys.exit(1)

    print("\n可采集的集群:")
    for i, c in enumerate(clusters, 1):
        print(f"  [{i}] {c.get('name', '')}  (v{c.get('version', '')}, "
              f"id={c.get('id', '')})")

    while True:
        try:
            choice = input(f"选择集群编号 (1-{len(clusters)}, q 退出): ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(1)
        if choice.lower() in ("q", "quit", "exit"):
            sys.exit(0)
        if choice.isdigit() and 1 <= int(choice) <= len(clusters):
            picked = clusters[int(choice) - 1]
            return str(picked["id"]), str(picked["name"])
        # 允许直接输入集群名或 ID
        for c in clusters:
            if choice in (str(c.get("id")), str(c.get("name"))):
                return str(c["id"]), str(c["name"])
        print(f"无效输入: {choice}，请输入列表中的编号")


def choose_groups_interactive() -> List[str]:
    """交互式选择要导出的 API 类别"""
    print("\n可导出的 API 类别:")
    for idx, group in enumerate(API_GROUPS, 1):
        heavies = sum(1 for a in group["apis"] if a.get("heavy"))
        extra = f"，含 {heavies} 个重接口" if heavies else ""
        print(f"  [{idx}] {group['key']:<10} {group['title']}  "
              f"（{len(group['apis'])} 项{extra}）")
        print(f"        {group['description']}")

    while True:
        try:
            choice = input(
                "选择要导出的类别（编号或 key，逗号分隔；回车=全部）: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(1)
        if choice == "":
            return list(GROUP_KEYS)
        try:
            return parse_group_selection(choice)
        except ValueError as e:
            print(f"输入无效: {e}")


def choose_heavy_interactive(plan_heavy_names: List[str]) -> bool:
    """交互式询问是否包含重接口"""
    print(f"\n选中的类别包含 {len(plan_heavy_names)} 个重接口"
          f"（响应大/耗时长）: {', '.join(plan_heavy_names)}")
    try:
        answer = input("包含这些重接口? [Y/n]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(1)
    return answer not in ("n", "no", "否")


# ---------------------------------------------------------------------------
# 体检结论提取（纯函数，可单测）
# ---------------------------------------------------------------------------

def _iter_nodes(payload: Any) -> List[Dict[str, Any]]:
    """_nodes*/stats 类响应中的 nodes 字典转列表"""
    if not isinstance(payload, dict):
        return []
    nodes = payload.get("nodes")
    if isinstance(nodes, dict):
        return [v for v in nodes.values() if isinstance(v, dict)]
    if isinstance(nodes, list):
        return [v for v in nodes if isinstance(v, dict)]
    return []


def _fmt_bytes(num: Any) -> str:
    try:
        return ConsoleClient.format_bytes(int(num))
    except (TypeError, ValueError):
        return "-"


def evaluate_findings(results: Dict[str, Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    从采集结果中提取需要关注的信号。

    results: {api_name: {"payload": Any, "status": "ok"|"es_error"|"failed"}}
    返回: [{level: critical|warn|info, message}]
    """
    findings: List[Dict[str, str]] = []

    def add(level: str, message: str):
        findings.append({"level": level, "message": message})

    def payload_of(name: str) -> Any:
        return (results.get(name) or {}).get("payload")

    # ---- 集群健康 ----
    health = payload_of("cluster_health")
    if isinstance(health, dict):
        status = str(health.get("status", "")).lower()
        if status == "red":
            add("critical", f"集群状态为 red，主分片有丢失"
                f"（unassigned_shards={health.get('unassigned_shards', 0)}）")
        elif status == "yellow":
            add("warn", f"集群状态为 yellow，副本分片未全部分配"
                f"（unassigned_shards={health.get('unassigned_shards', 0)}）")
        unassigned = health.get("unassigned_shards", 0) or 0
        if unassigned and status == "green":
            add("warn", f"状态 green 但仍有 {unassigned} 个未分配分片")
        max_wait = health.get("task_max_waiting_in_queue_millis", 0) or 0
        if max_wait > 60000:
            add("warn", f"健康接口 task 队列最长等待 {max_wait / 1000:.0f}s（>60s），"
                "master 可能处理不过来")
        relocating = health.get("relocating_shards", 0) or 0
        initializing = health.get("initializing_shards", 0) or 0
        if relocating or initializing:
            add("info", f"分片在迁移中: relocating={relocating}, "
                f"initializing={initializing}（若长期不变需排查）")

    # ---- pending tasks ----
    pending = payload_of("cluster_pending_tasks")
    tasks = pending.get("tasks", []) if isinstance(pending, dict) else []
    if tasks:
        slowest = max(
            (t.get("time_in_queue_millis", 0) or 0 for t in tasks), default=0)
        heads = "; ".join(
            f"{t.get('source', '?')}({t.get('time_in_queue_millis', 0)}ms)"
            for t in tasks[:5])
        level = "warn" if slowest > 60000 else "info"
        add(level, f"master pending tasks 积压 {len(tasks)} 个，"
            f"最长排队 {slowest / 1000:.1f}s: {heads}")

    # ---- allocation explain ----
    alloc = payload_of("cluster_allocation_explain")
    if isinstance(alloc, dict) and alloc.get("index"):
        add("warn", f"存在未分配分片解释: {alloc.get('index')} "
            f"[{alloc.get('shard')}/{alloc.get('primary', '?')}] "
            f"{alloc.get('unassigned_info', {}).get('reason', '')}，"
            "详情见 allocation_explain 文件")

    # ---- 节点级信号（heap/磁盘/breaker/线程池/版本） ----
    node_stats = payload_of("nodes_stats")
    if isinstance(node_stats, dict):
        max_heap, disk_worst = 0, {"used_percent": 0.0, "name": ""}
        rejected_total = 0
        breaker_tripped = []
        for node_id, node in (node_stats.get("nodes") or {}).items():
            if not isinstance(node, dict):
                continue
            name = node.get("name", node_id)
            heap = ((node.get("jvm") or {}).get("mem") or {}).get(
                "heap_used_percent") or 0
            max_heap = max(max_heap, heap)
            fs = ((node.get("fs") or {}).get("total") or {})
            total_b = fs.get("total_in_bytes") or 0
            avail_b = fs.get("available_in_bytes") or 0
            if total_b > 0:
                used_pct = round((total_b - avail_b) / total_b * 100, 1)
                if used_pct > disk_worst["used_percent"]:
                    disk_worst = {"used_percent": used_pct, "name": name,
                                  "avail": _fmt_bytes(avail_b)}
            for pool_name, pool in (node.get("thread_pool") or {}).items():
                if isinstance(pool, dict) and (pool.get("rejected") or 0) > 0:
                    rejected_total += pool["rejected"]
            for brk_name, brk in (node.get("breakers") or {}).items():
                if not isinstance(brk, dict):
                    continue
                limit = brk.get("limit_size_in_bytes") or 0
                estimated = brk.get("estimated_size_in_bytes") or 0
                if limit > 0 and estimated >= limit:
                    breaker_tripped.append(f"{name}/{brk_name}")
        if max_heap >= 90:
            add("critical", f"JVM 堆使用率峰值 {max_heap}%（>=90%），GC 压力大")
        elif max_heap >= 85:
            add("warn", f"JVM 堆使用率峰值 {max_heap}%（>=85%），关注 GC 与老年代")
        if disk_worst["used_percent"] >= 95:
            add("critical", f"节点 {disk_worst['name']} 磁盘使用 "
                f"{disk_worst['used_percent']}%（>=95%），索引即将被置为只读"
                f"，剩余 {disk_worst.get('avail', '-')}")
        elif disk_worst["used_percent"] >= 85:
            add("warn", f"节点 {disk_worst['name']} 磁盘使用 "
                f"{disk_worst['used_percent']}%（>=85% 水位），会停止分片分配"
                f"，剩余 {disk_worst.get('avail', '-')}")
        if breaker_tripped:
            add("critical", "circuit breaker 已触顶: " + ", ".join(breaker_tripped[:5]))
        if rejected_total > 0:
            add("warn", f"线程池累计拒绝 {rejected_total} 次（rejected>0），"
                "说明有过载，重点看 write/search/get 池")

    # ---- 节点版本一致性 ----
    versions = {
        n.get("version") for n in _iter_nodes(payload_of("nodes_info"))
        if isinstance(n, dict) and n.get("version")
    }
    if len(versions) > 1:
        add("warn", f"节点版本不一致: {', '.join(sorted(versions))}，"
            "滚动升级未完成或存在异构节点")

    # ---- license ----
    lic = payload_of("license")
    if isinstance(lic, dict):
        license_info = lic.get("license") or {}
        lic_status = license_info.get("status", "")
        if lic_status and lic_status != "active":
            add("warn", f"License 状态为 {lic_status}"
                f"（type={license_info.get('type', '-')}），相关功能可能已退化")
        expiry = license_info.get("expiry_date_in_millis")
        if isinstance(expiry, int) and expiry > 0:
            days = (expiry / 1000 - time.time()) / 86400
            if 0 < days < 30:
                add("warn", f"License 将于 {days:.0f} 天后到期"
                    f"（type={license_info.get('type', '-')}）")

    # ---- ILM / SLM ----
    for name, label in (("ilm_status", "ILM"), ("slm_status", "SLM")):
        payload = payload_of(name)
        mode = payload.get("operation_mode", "") if isinstance(payload, dict) else ""
        if mode and mode not in ("RUNNING", "STOPPING"):
            add("warn", f"{label} 运行状态为 {mode}，生命周期管理未正常运行")

    # ---- 快照仓库 ----
    repos = payload_of("snapshot_repositories")
    if isinstance(repos, dict) and not repos:
        add("warn", "未配置任何快照仓库（/_snapshot 为空），数据备份能力存疑")

    # ---- deprecations ----
    dep = payload_of("migration_deprecations")
    if isinstance(dep, dict):
        dep_count = (
            len(dep.get("cluster_settings") or {})
            + len(dep.get("index_settings") or {})
            + len(dep.get("node_settings") or [])
        )
        if dep_count:
            add("info", f"存在 {dep_count} 项弃用配置告警（_migration/deprecations），"
                "升级前需处理")

    return findings


LEVEL_ORDER = {"critical": 0, "warn": 1, "info": 2}
LEVEL_ICONS = {"critical": "❌", "warn": "⚠️ ", "info": "ℹ️ "}


# ---------------------------------------------------------------------------
# 采集器
# ---------------------------------------------------------------------------

class ConfigDumper:
    """集群配置采集器：按计划顺序执行 GET 并落盘"""

    def __init__(self, client: ConsoleClient, cluster_id: str,
                 display_name: str, plan: List[Dict[str, Any]],
                 run_dir: Path, console_url: str = ""):
        self.client = client
        self.cluster_id = cluster_id
        self.display_name = display_name or cluster_id
        self.plan = plan
        self.run_dir = run_dir
        self.console_url = console_url
        # name -> {"item", "status", "payload", "error", "es_error_status",
        #          "duration_ms", "size_bytes", "file"}
        self.results: Dict[str, Dict[str, Any]] = {}
        # 最近一次请求的耗时（毫秒），由 _fetch_one 维护
        self._last_duration_ms = 0

    # ---------------- 采集 ----------------

    def run(self) -> Dict[str, int]:
        """执行采集，返回 {ok, es_error, failed, skipped} 计数"""
        counts = {"ok": 0, "es_error": 0, "failed": 0, "skipped": 0}
        total = len(self.plan)
        t_start = time.time()
        print(f"\n开始采集 {total} 个 API（顺序执行）...")
        for item in self.plan:
            payload, status, err, es_err_status = self._fetch_one(item)
            size, rel_file = self._save_result(item, payload, status, err)
            record = {
                "item": item,
                "status": status,
                "payload": payload if status != "failed" else None,
                "error": err or "",
                "es_error_status": es_err_status,
                "size_bytes": size,
                "file": rel_file,
                "duration_ms": self._last_duration_ms,
            }
            self.results[item["name"]] = record
            counts[status] += 1

            icon = {"ok": "OK  ", "es_error": "ESERR", "failed": "FAIL ",
                    "skipped": "SKIP "}[status]
            print(f"  [{item['seq']:>3}/{total}] {icon} "
                  f"{record['duration_ms'] / 1000:>6.1f}s "
                  f"{ConsoleClient.format_bytes(size):>9} "
                  f"{item['group_key']}/{item['name']}")
            if status in ("es_error", "failed", "skipped") and err:
                print(f"          ↳ {err[:200]}")
        elapsed = time.time() - t_start
        print(f"\n采集完成: 成功 {counts['ok']}，ES 报错 {counts['es_error']}，"
              f"失败 {counts['failed']}，按预期跳过 {counts['skipped']}，"
              f"耗时 {elapsed:.1f}s")
        return counts

    def _fetch_one(self, item: Dict[str, Any]) -> Tuple[Any, str, str, Optional[int]]:
        """
        执行单条 API。

        返回 (payload, status, error, es_error_status)。
        status: ok=正常响应；es_error=ES 返回了错误体（仍是有效 JSON 诊断信息）；
        failed=请求层面失败（网络/Console 拒绝）。
        """
        t0 = time.time()
        try:
            payload = self.client.proxy_request(self.cluster_id, "GET", item["path"])
        except ConsoleAPIError as e:
            # 目录里标注为"预期"的 HTTP 状态码（如无快照仓库时的 404、
            # 基础 license 下 CCR 的 403）记为 skipped，不算失败
            m = re.search(r"HTTP (\d{3})", str(e))
            if m and int(m.group(1)) in item.get("expected_status", []):
                return ({"_skipped": True, "expected_status": int(m.group(1)),
                         "reason": str(e), "_requested_path": item["path"]},
                        "skipped", f"预期状态码 {m.group(1)}，按预期跳过", None)
            return ({"_dump_error": str(e), "_requested_path": item["path"]},
                    "failed", str(e), None)
        except Exception as e:  # 任何意外异常都记为失败，绝不让单条 API 中断整轮采集
            return ({"_dump_error": f"{type(e).__name__}: {e}",
                     "_requested_path": item["path"]},
                    "failed", f"{type(e).__name__}: {e}", None)
        finally:
            self._last_duration_ms = int((time.time() - t0) * 1000)

        # ES 错误响应体形如 {"error": {...}, "status": 400}
        if isinstance(payload, dict) and "error" in payload and "status" in payload:
            es_status = payload.get("status")
            try:
                es_status = int(es_status)
            except (TypeError, ValueError):
                es_status = None
            note = f"ES 返回错误 {es_status}" if es_status else "ES 返回错误"
            return payload, "es_error", note, es_status
        return payload, "ok", "", None

    # ---------------- 落盘 ----------------

    def _group_dir(self, group_key: str) -> Tuple[Path, str]:
        """类别子目录: 01_cluster/...，按 API_GROUPS 全局顺序编号，保持稳定"""
        idx = GROUP_KEYS.index(group_key) + 1
        dir_name = f"{idx:02d}_{group_key}"
        return self.run_dir / dir_name, dir_name

    def _save_result(self, item: Dict[str, Any], payload: Any, status: str,
                     err: str) -> Tuple[int, str]:
        """把单条结果写成 pretty JSON，返回 (字节数, 相对路径)"""
        group_dir, dir_name = self._group_dir(item["group_key"])
        group_dir.mkdir(parents=True, exist_ok=True)
        file_path = group_dir / f"{item['seq']:03d}_{slugify(item['name'])}.json"
        try:
            text = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
        except (TypeError, ValueError) as e:
            text = json.dumps({"_dump_error": f"结果不可序列化: {e}",
                               "_requested_path": item["path"]},
                              ensure_ascii=False, indent=2)
        file_path.write_text(text, encoding="utf-8")
        return len(text.encode("utf-8")), f"{dir_name}/{file_path.name}"

    # ---------------- manifest ----------------

    def build_manifest(self, selected_keys: List[str], include_heavy: bool,
                       counts: Dict[str, int]) -> Dict[str, Any]:
        items = []
        for item in self.plan:
            r = self.results.get(item["name"], {})
            items.append({
                "seq": item["seq"],
                "group": item["group_key"],
                "name": item["name"],
                "path": item["path"],
                "note": item["note"],
                "heavy": item["heavy"],
                "status": r.get("status", "failed"),
                "es_error_status": r.get("es_error_status"),
                "duration_ms": r.get("duration_ms", 0),
                "size_bytes": r.get("size_bytes", 0),
                "file": r.get("file", ""),
                "error": r.get("error", ""),
            })
        return {
            "tool": "cluster-config-dump",
            "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "console_url": self.console_url,
            "cluster": {
                "id": self.cluster_id,
                "display_name": self.display_name,
            },
            "groups_selected": selected_keys,
            "include_heavy": include_heavy,
            "total": len(items),
            "ok": counts["ok"],
            "es_error": counts["es_error"],
            "failed": counts["failed"],
            "items": items,
        }

    # ---------------- summary.md ----------------

    def generate_summary_md(self, selected_keys: List[str],
                            include_heavy: bool,
                            counts: Dict[str, int]) -> str:
        findings = evaluate_findings(self.results)
        health = self.results.get("cluster_health", {}).get("payload") or {}
        root = self.results.get("root", {}).get("payload") or {}
        version = (root.get("version") or {}).get("number", "-") \
            if isinstance(root, dict) else "-"

        lines: List[str] = []
        lines.append(f"# 集群配置采集体检摘要: {self.display_name}")
        lines.append("")
        lines.append(
            f"- 采集时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}　|　"
            f"ES 版本: {version}　|　"
            f"Console 集群 ID: `{self.cluster_id}`")
        if isinstance(root, dict):
            lines.append(
                f"- 集群名: {root.get('cluster_name', '-')}　|　"
                f"cluster_uuid: `{root.get('cluster_uuid', '-')}`")
        lines.append(
            f"- 采集范围: {len(selected_keys)} 类 {len(self.plan)} 个 API"
            f"（{'包含' if include_heavy else '不包含'}重接口）")
        lines.append("")

        # 1. 集群概览
        lines.append("## 1. 集群概览")
        lines.append("")
        if isinstance(health, dict) and health:
            status = health.get("status", "-")
            lines.append("| 指标 | 值 |")
            lines.append("|------|-----|")
            lines.append(f"| 健康状态 | **{status}** |")
            lines.append(f"| 节点数（数据节点） | {health.get('number_of_nodes', '-')} "
                         f"（{health.get('number_of_data_nodes', '-')}） |")
            lines.append(f"| 活跃分片 / 主分片 | {health.get('active_shards', '-')} / "
                         f"{health.get('active_primary_shards', '-')} |")
            lines.append(f"| 未分配分片 | {health.get('unassigned_shards', 0)} |")
            lines.append(f"| 活跃分片占比 | "
                         f"{health.get('active_shards_percent_as_number', '-')}% |")
            lines.append(f"| pending tasks | {health.get('number_of_pending_tasks', 0)} |")
            lines.append(f"| task 队列最长等待 | "
                         f"{health.get('task_max_waiting_in_queue_millis', 0)} ms |")
        else:
            lines.append("> cluster_health 未采集成功，请查看明细文件。")
        lines.append("")

        # 2. 需要关注的信号
        lines.append("## 2. 需要关注的信号（自动检查）")
        lines.append("")
        if findings:
            findings.sort(key=lambda f: LEVEL_ORDER.get(f["level"], 9))
            for f in findings:
                lines.append(f"- {LEVEL_ICONS[f['level']]} **[{f['level'].upper()}]** "
                             f"{f['message']}")
        else:
            lines.append("- 未发现自动检查的异常信号（不代表全部正常，请查看明细）。")
        lines.append("")

        # 3. 配置与功能模块
        module_lines_start = len(lines)
        lines.append("## 3. 配置与功能模块")
        lines.append("")

        def module_line(name: str, describe) -> None:
            payload = self.results.get(name, {}).get("payload")
            text = ""
            try:
                text = describe(payload) if payload is not None else ""
            except Exception:
                text = ""
            if text:
                lines.append(f"- {text}")

        def f_license(p):
            info = (p or {}).get("license") or {}
            if not info:
                return None
            expiry = info.get("expiry_date_in_millis")
            expiry_str = datetime.fromtimestamp(expiry / 1000).strftime(
                "%Y-%m-%d") if isinstance(expiry, int) and expiry > 0 else "永久"
            return (f"License: {info.get('type', '-')} "
                    f"（{info.get('status', '-')}，到期: {expiry_str}）")

        def f_xpack_usage(p):
            feats = (p or {}).get("features") or {}
            enabled = sorted(
                k for k, v in feats.items()
                if isinstance(v, dict) and v.get("enabled"))
            return (f"xpack 已启用功能: {', '.join(enabled) if enabled else '无'}")

        def f_ilm(p):
            mode = (p or {}).get("operation_mode")
            return f"ILM: {mode}" if mode else None

        def f_slm(p):
            mode = (p or {}).get("operation_mode")
            return f"SLM: {mode}" if mode else None

        def f_watcher(p):
            state = (p or {}).get("watcher_state")
            return f"Watcher: {state}" if state else None

        def f_repos(p):
            names = list((p or {}).keys()) if isinstance(p, dict) else []
            return (f"快照仓库: {len(names)} 个"
                    + (f"（{', '.join(names[:5])}）" if names else ""))

        def f_snapshot_status(p):
            snaps = (p or {}).get("snapshots") or []
            return f"进行中的快照: {len(snaps)} 个" if snaps else None

        def f_ml(p):
            return "ML 模块: 可用" if isinstance(p, dict) and p else None

        def f_ccr(p):
            follow = ((p or {}).get("follow_stats") or {}).get("indices") or []
            return f"CCR follower 索引: {len(follow)} 个" if follow else None

        def f_auth(p):
            user = (p or {}).get("username")
            if not user:
                return None
            roles = (p or {}).get("roles") or []
            return (f"代理账号: {user}（roles: {', '.join(roles[:8])}"
                    f"{'…' if len(roles) > 8 else ''}）")

        def f_transform(p):
            jobs = (p or {}).get("transforms") or (p or {}).get("jobs") or []
            return f"Transform 任务: {len(jobs)} 个" if jobs else None

        def f_rollup(p):
            jobs = (p or {}).get("jobs") or []
            return f"Rollup 任务: {len(jobs)} 个" if jobs else None

        def f_dep(p):
            if not isinstance(p, dict):
                return None
            n = (len(p.get("cluster_settings") or {})
                 + len(p.get("index_settings") or {})
                 + len(p.get("node_settings") or []))
            return f"升级弃用告警: {n} 项（详见 05_diagnostic/migration_deprecations）" if n else None

        for name, fn in (
            ("license", f_license), ("xpack_usage", f_xpack_usage),
            ("ilm_status", f_ilm), ("slm_status", f_slm),
            ("watcher_stats", f_watcher),
            ("snapshot_repositories", f_repos),
            ("snapshot_status", f_snapshot_status),
            ("transform_stats", f_transform), ("rollup_jobs", f_rollup),
            ("ccr_stats", f_ccr), ("ml_info", f_ml),
            ("security_authenticate", f_auth),
            ("migration_deprecations", f_dep),
        ):
            module_line(name, fn)
        if len(lines) == module_lines_start + 2:
            lines.append("> 未采集 license/ILM/SLM 等相关类别，无模块状态可显示"
                         "（用 --groups 补充类别后重跑）。")
        lines.append("")

        # 4. 采集结果统计
        lines.append("## 4. 采集结果统计")
        lines.append("")
        lines.append("| 类别 | API 数 | 成功 | ES 报错 | 请求失败 | 按预期跳过 |")
        lines.append("|------|--------|------|---------|----------|------------|")
        for key in selected_keys:
            group_items = [i for i in self.plan if i["group_key"] == key]
            ok = sum(1 for i in group_items
                     if self.results[i["name"]]["status"] == "ok")
            es_err = sum(1 for i in group_items
                         if self.results[i["name"]]["status"] == "es_error")
            failed = sum(1 for i in group_items
                         if self.results[i["name"]]["status"] == "failed")
            skipped = sum(1 for i in group_items
                          if self.results[i["name"]]["status"] == "skipped")
            idx = GROUP_KEYS.index(key) + 1
            lines.append(f"| {idx:02d}_{key} | {len(group_items)} | {ok} | "
                         f"{es_err} | {failed} | {skipped} |")
        lines.append("")

        problems = [
            (r["item"], r) for r in self.results.values()
            if r["status"] in ("es_error", "failed")
        ]
        if problems:
            lines.append("### 异常/报错明细")
            lines.append("")
            for item, r in problems:
                if r["status"] == "es_error":
                    lines.append(
                        f"- `[ES {r['es_error_status']}]` "
                        f"`{item['group_key']}/{item['name']}` {item['path']}"
                        + (f"（{item['note']}）" if item.get("note") else ""))
                else:
                    lines.append(
                        f"- `[FAIL]` `{item['group_key']}/{item['name']}` "
                        f"{item['path']} — {r['error'][:160]}")
            lines.append("")

        # 5. 文件说明
        lines.append("## 5. 文件说明")
        lines.append("")
        lines.append("- `manifest.json`: 全部 API 的采集状态/耗时/大小/错误，"
                     "两次运行可直接 diff 对比集群变化")
        lines.append("- 各类别子目录 `NN_<类别>/NNN_<api名>.json`: "
                     "每条 API 的完整响应")
        lines.append("- `es_error` 状态表示 ES 返回了错误响应体（部分属于预期，"
                     "如 allocation/explain 在无未分配分片时返回 400）")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# 命令行入口
# ---------------------------------------------------------------------------

def check_cluster_reachable(client: ConsoleClient,
                            cluster_id: str) -> Tuple[bool, str]:
    """通过 _proxy 访问集群根路径做可用性预检"""
    try:
        root = client.proxy_request(cluster_id, "GET", "/")
    except ConsoleAPIError as e:
        return False, str(e)
    cname = root.get("cluster_name", "") if isinstance(root, dict) else ""
    version = ((root.get("version") or {}).get("number", "")
               if isinstance(root, dict) else "")
    return True, f"{cname or cluster_id} (v{version})"


def list_clusters_and_exit(client: ConsoleClient) -> None:
    """非交互模式下未指定集群时，列出可选集群"""
    try:
        clusters = [
            c for c in client.get_clusters()
            if not ConsoleClient.is_system_cluster(
                str(c.get("id", "")), str(c.get("name", "")))
        ]
    except ConsoleAPIError as e:
        print(f"获取集群列表失败: {e}")
        sys.exit(1)
    print("\n未指定目标集群，可用的集群:")
    print(f"{'ID':<28} {'名称':<36} 版本")
    for c in clusters:
        print(f"{str(c.get('id', '')):<28} {str(c.get('name', '')):<36} "
              f"{c.get('version', '')}")
    print("\n用 --cluster-id / --cluster-name 指定目标集群")
    sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Cluster Config Dump - 采集 ES 集群配置全量 GET API 并落盘，"
                    "用于健康判断与变更 diff",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 交互式：选择集群与类别
  python cluster_config_dump.py -c http://localhost:9000 -u admin -p password

  # 非交互：指定集群与类别，跳过重接口
  python cluster_config_dump.py --config config.json --cluster-name prod \\
      --groups cluster,cat,index --skip-heavy

  # 全部类别（含重接口）
  python cluster_config_dump.py -c http://localhost:9000 -u admin -p 'pw' \\
      --cluster-id xxx --yes

  # 只查看 API 目录
  python cluster_config_dump.py --list

Environment Variables:
  CONSOLE_URL / CONSOLE_USERNAME / CONSOLE_PASSWORD
  CONSOLE_CLUSTER_ID / CONSOLE_CLUSTER_NAME / CONSOLE_DUMP_GROUPS
        """,
    )

    parser = add_common_args(parser)

    target_group = parser.add_argument_group("目标集群参数（二选一：--cluster-id / --cluster-name）")
    target_group.add_argument(
        "--cluster-id", default="", help="目标集群 ID (环境变量: CONSOLE_CLUSTER_ID)")
    target_group.add_argument(
        "--cluster-name", default="",
        help="目标集群名称（Console 显示名，支持部分匹配）"
             " (环境变量: CONSOLE_CLUSTER_NAME)")

    parser.add_argument(
        "--groups", "-g", default="",
        help="要导出的 API 类别，逗号分隔编号或 key，all=全部"
             " (环境变量: CONSOLE_DUMP_GROUPS)。给出此参数则跳过交互菜单")
    parser.add_argument(
        "--yes", "-y", action="store_true",
        help="非交互模式：全部类别、包含重接口")
    parser.add_argument(
        "--skip-heavy", action="store_true",
        help="跳过重接口（_cluster/state、_stats 等大响应 API）")
    parser.add_argument(
        "--list", action="store_true", help="只打印 API 目录后退出")

    # 该工具会采大响应 API，默认放宽连接超时
    parser.set_defaults(timeout=120)
    return parser.parse_args()


def main():
    args = parse_args()
    config, _ = load_and_merge_config(args)
    options = config.get("clusterConfigDumpOptions", {}) if isinstance(
        config.get("clusterConfigDumpOptions"), dict) else {}

    console_url = get_config_value(
        args.console, config.get("consoleUrl"), "CONSOLE_URL", "http://localhost:9000")
    username = get_config_value(
        args.username, config.get("auth", {}).get("username"), "CONSOLE_USERNAME", "")
    password = get_config_value(
        args.password, config.get("auth", {}).get("password"), "CONSOLE_PASSWORD", "")
    timeout = int(get_config_value(
        str(args.timeout), str(config.get("timeout")), "CONSOLE_TIMEOUT", "120"))
    insecure = args.insecure or config.get("insecure", False)

    cluster_id = get_config_value(
        args.cluster_id, config.get("clusterId"), "CONSOLE_CLUSTER_ID", "")
    cluster_name = get_config_value(
        args.cluster_name, config.get("clusterName"), "CONSOLE_CLUSTER_NAME", "")

    # 类别选择：CLI > 配置文件 > 环境变量；--yes 表示全选
    groups_spec = get_config_value(
        args.groups, options.get("groups"), "CONSOLE_DUMP_GROUPS", "")
    skip_heavy = args.skip_heavy or bool(options.get("skipHeavy", False))
    non_interactive = bool(groups_spec) or args.yes

    output = args.output or options.get("output") or "./cluster_config_dump"

    if args.list:
        print_catalog()
        return

    try:
        client = create_authenticated_client(
            console_url, username, password,
            timeout=timeout, verify_ssl=not insecure)
    except ConsoleAuthError as e:
        print(f"登录失败: {e}")
        sys.exit(1)

    # ---------------- 目标集群 ----------------
    try:
        clusters = client.get_clusters()
    except ConsoleAPIError:
        clusters = []

    if not cluster_id and not cluster_name:
        if non_interactive or not sys.stdin.isatty():
            list_clusters_and_exit(client)
        cluster_id, cluster_name = choose_cluster_interactive(client)
    elif not cluster_id and cluster_name:
        try:
            cluster_id = client.resolve_cluster_id_by_name(cluster_name)
            print(f"已根据 clusterName 解析 clusterId: {cluster_name} -> {cluster_id}")
        except ConsoleAPIError as e:
            print(f"解析集群失败: {e}")
            list_clusters_and_exit(client)
    elif cluster_id and clusters:
        # 校验 cluster-id；不匹配时尝试按名称纠正
        if not any(c.get("id") == cluster_id for c in clusters):
            print(f"警告: cluster-id '{cluster_id}' 不在 Console 集群列表中，尝试按名称解析...")
            try:
                resolved = client.resolve_cluster_id_by_name(cluster_id)
                print(f"已按名称解析: {cluster_id} -> {resolved}")
                cluster_id = resolved
            except ConsoleAPIError:
                list_clusters_and_exit(client)

    display_name = cluster_name or cluster_id
    for c in clusters:
        if c.get("id") == cluster_id:
            display_name = c.get("name") or cluster_id
            break

    reachable, info = check_cluster_reachable(client, cluster_id)
    if not reachable:
        print(f"通过 Console 代理访问集群失败: {info}")
        if "404" in info:
            print("提示: 集群 ID 不存在或账号无权访问该集群")
        elif "403" in info:
            print("提示: 当前账号没有该集群的 _proxy 访问权限")
        else:
            print("提示: 集群可能在 Console 中处于离线状态，或 Console 无法连通集群")
        sys.exit(1)
    print(f"集群可达: {info}")

    # ---------------- 类别选择 ----------------
    try:
        if groups_spec:
            selected_keys = parse_group_selection(groups_spec)
            include_heavy = not skip_heavy
        elif args.yes:
            selected_keys = list(GROUP_KEYS)
            include_heavy = not skip_heavy
        else:
            selected_keys = choose_groups_interactive()
            heavy_names = [a["name"] for key in selected_keys
                           for a in next(g for g in API_GROUPS
                                         if g["key"] == key)["apis"]
                           if a.get("heavy")]
            include_heavy = (not skip_heavy) if heavy_names else True
            if heavy_names:
                include_heavy = choose_heavy_interactive(heavy_names)
    except ValueError as e:
        print(f"参数错误: {e}")
        sys.exit(1)

    plan = build_plan(selected_keys, include_heavy)
    if not plan:
        print("执行计划为空（所选类别无 API 或全部被 --skip-heavy 排除）")
        sys.exit(1)

    print(f"\n执行计划: {len(selected_keys)} 类 {len(plan)} 个 API"
          f"（{'包含' if include_heavy else '不含'}重接口）")

    # ---------------- 采集 ----------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = re.sub(r"[^\w.-]+", "_", display_name)
    output_root = Path(output)
    output_root.mkdir(parents=True, exist_ok=True)
    run_dir = output_root / f"config_dump_{timestamp}_{safe_name}"
    run_dir.mkdir(parents=True, exist_ok=True)

    dumper = ConfigDumper(
        client, cluster_id, display_name, plan, run_dir, console_url=console_url)
    counts = dumper.run()

    # manifest 与 summary 无论成败都落盘
    manifest = dumper.build_manifest(selected_keys, include_heavy, counts)
    manifest_file = run_dir / "manifest.json"
    manifest_file.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_md = dumper.generate_summary_md(selected_keys, include_heavy, counts)
    summary_file = run_dir / "summary.md"
    summary_file.write_text(summary_md, encoding="utf-8")

    print()
    print("=" * 64)
    print(f"采集完成: 成功 {counts['ok']} / ES 报错 {counts['es_error']} / "
          f"失败 {counts['failed']} / 按预期跳过 {counts['skipped']}")
    print(f"输出目录:   {run_dir}")
    print(f"总清单:     {manifest_file}")
    print(f"体检摘要:   {summary_file}")
    print("=" * 64)

    sys.exit(0 if counts["ok"] > 0 else 1)


if __name__ == "__main__":
    main()
