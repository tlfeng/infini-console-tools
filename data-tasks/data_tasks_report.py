#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据工具任务报表 - 读取 INFINI Console 侧边栏“数据工具”下的
数据迁移 / 数据比对 任务，尽可能多地导出每个任务的详细信息。

对应 UI 侧边栏项:
  数据工具 → 数据迁移  (cluster_migration)
  数据工具 → 数据比对  (cluster_comparison)

调用的 Console API:
  GET /migration/data/_search             # 任务列表（自带汇总）
  GET /comparison/data/_search
  GET /migration/data/{id}/info           # 每个任务的详情（含每个索引的进度和状态）
  GET /comparison/data/{id}/info

信息来源：
  - search 响应本身已经被 populateMajorTaskInfo 填了 target_total_docs /
    source_total_docs / error_partitions / running_children / repeat 等
  - /info 响应里 config_string 的 indices[] 会被填上 percent / status /
    exported_percent (迁移) 或 scroll_percent / total_scroll_docs /
    total_diff_docs (比对)

输出：
  - JSON: 完整嵌套结构，字段最全
  - CSV : 每行一个 (任务, 索引)，方便放进电子表格
默认两种都写。也可以用 --format json / --format csv 只写一种。
"""

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).parent.parent))
from common.config import add_common_args, get_config_value, load_and_merge_config
from common.console_client import ConsoleAPIError, ConsoleAuthError, ConsoleClient, create_authenticated_client


TASK_KINDS = [
    # (list_endpoint, info_endpoint_template, 中文名, kind)
    ("/migration/data/_search", "/migration/data/{id}/info", "数据迁移", "migration"),
    ("/comparison/data/_search", "/comparison/data/{id}/info", "数据比对", "comparison"),
]


# ------------------------- 通用格式化 -------------------------


def format_bytes(n: Optional[int]) -> str:
    if n is None:
        return ""
    try:
        n = int(n)
    except (TypeError, ValueError):
        return ""
    if n == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    val = float(n)
    for unit in units:
        if abs(val) < 1024:
            return f"{val:.2f} {unit}"
        val /= 1024
    return f"{val:.2f} EB"


def format_duration_ms(ms: Optional[int]) -> str:
    """毫秒 → 可读时长（1d 2h 3m 4s）"""
    if ms is None or ms <= 0:
        return ""
    total_seconds = int(ms) // 1000
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")
    return " ".join(parts)


def parse_iso_to_ms(s: Optional[str]) -> Optional[int]:
    """尝试把 ISO 时间串转成毫秒时间戳，失败返回 None"""
    if not s or not isinstance(s, str):
        return None
    try:
        from datetime import datetime

        # Python 3.11+ 支持带时区偏移；这里兼容带 Z 的写法
        clean = s.replace("Z", "+00:00")
        return int(datetime.fromisoformat(clean).timestamp() * 1000)
    except Exception:
        return None


def safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def parse_task_config(hit_source: Dict[str, Any]) -> Dict[str, Any]:
    """等价于前端 parseTaskConfig：优先 config，其次解析 config_string。"""
    if isinstance(hit_source.get("config"), dict):
        return hit_source["config"]
    raw = hit_source.get("config_string")
    if raw:
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError) as e:
            print(f"warn: failed to parse config_string: {e}", file=sys.stderr)
    return {}


# ------------------------- 数据抓取 -------------------------


def fetch_tasks(
    client: ConsoleClient, endpoint: str, page_size: int = 200
) -> List[Dict[str, Any]]:
    """分页拉一种类型的全部父任务。"""
    all_hits: List[Dict[str, Any]] = []
    from_ = 0
    while True:
        query = urlencode({"from": from_, "size": page_size})
        resp = client._make_request(f"{endpoint}?{query}", "GET")
        hits_obj = resp.get("hits") or {}
        hits = hits_obj.get("hits") or []
        if not hits:
            break
        all_hits.extend(hits)

        total = hits_obj.get("total") or {}
        total_val = safe_int(total.get("value") if isinstance(total, dict) else total)

        from_ += len(hits)
        if total_val is not None and from_ >= total_val:
            break
        if len(hits) < page_size:
            break
    return all_hits


def fetch_task_info(
    client: ConsoleClient, endpoint_tpl: str, task_id: str
) -> Optional[Dict[str, Any]]:
    """获取单个任务的 /info 详情。找不到 → None，其它错误抛出。"""
    path = endpoint_tpl.format(id=task_id)
    try:
        info = client._make_request(path, "GET")
        if isinstance(info, dict) and info.get("found") is False:
            return None
        return info if isinstance(info, dict) else None
    except ConsoleAPIError as e:
        # 权限或不存在时静默降级到 search 的数据
        print(f"warn: fetch {path} failed: {e}", file=sys.stderr)
        return None


# ------------------------- 详情提取 -------------------------


def _extract_execution(config: Dict[str, Any]) -> Dict[str, Any]:
    """从 config.settings.execution 里挖出执行相关信息（节点、时间窗、并发、重试）。"""
    settings = config.get("settings") or {}
    execution = settings.get("execution") or {}

    nodes: List[Dict[str, str]] = []
    seen_node_ids = set()
    node_permit = (execution.get("nodes") or {}).get("permit") or []
    for n in node_permit:
        if not isinstance(n, dict):
            continue
        nid = n.get("id", "")
        # /info 会在 permit 基础上再 append 一次运行节点，这里去重
        if nid and nid in seen_node_ids:
            continue
        seen_node_ids.add(nid)
        nodes.append({"id": nid, "name": n.get("name", "")})

    return {
        "runtime_group": execution.get("runtime_group") or {},
        "nodes": nodes,
        "time_window": execution.get("time_window") or [],
        "max_tasks_per_instance": execution.get("max_tasks_per_instance"),
        "max_retry_times": execution.get("max_retry_times"),
        "repeat": execution.get("repeat") or {},
    }


def _extract_scroll_bulk(config: Dict[str, Any], kind: str) -> Dict[str, Any]:
    """迁移任务：scroll + bulk；比对任务：dump + diff。"""
    settings = config.get("settings") or {}
    if kind == "migration":
        return {
            "scroll": settings.get("scroll") or {},
            "bulk": settings.get("bulk") or {},
            "optimization": settings.get("optimization") or {},
            "auto_create_comparison": bool(settings.get("auto_create_comparison")),
            "skip_scroll_count_check": bool(settings.get("skip_scroll_count_check")),
            "skip_bulk_count_check": bool(settings.get("skip_bulk_count_check")),
        }
    return {
        "dump": settings.get("dump") or {},
        "diff": settings.get("diff") or {},
        "optimization": settings.get("optimization") or {},
    }


def _index_entry(idx: Dict[str, Any], kind: str) -> Dict[str, Any]:
    """把单个索引条目转成扁平/带进度信息的字典。"""
    src = idx.get("source") or {}
    tgt = idx.get("target") or {}
    incremental = idx.get("incremental") or None
    partition = idx.get("partition") or None
    raw_filter = idx.get("raw_filter")

    src_size = safe_int(src.get("store_size_in_bytes")) or 0
    src_docs = safe_int(src.get("docs")) or 0
    tgt_docs = safe_int(tgt.get("docs")) or 0

    entry: Dict[str, Any] = {
        "source_index": src.get("name") or "",
        "target_index": tgt.get("name") or "",
        "doc_type": src.get("doc_type") or "",
        "source_docs": src_docs,
        "target_docs": tgt_docs,
        "source_store_bytes": src_size,
        "source_store_size": format_bytes(src_size),
        "has_filter": bool(raw_filter),
        "raw_filter": raw_filter,
        "incremental": incremental,  # 完整对象，含 field_name / delay / full
        "partitioned": bool(partition),
        "partition": partition,
    }

    # 迁移: percent / status / exported_percent
    # 比对: scroll_percent / total_scroll_docs / total_diff_docs / status
    for extra in (
        "percent",
        "status",
        "exported_percent",
        "scroll_percent",
        "total_scroll_docs",
        "total_diff_docs",
        "error_partitions",
        "running_children",
    ):
        if extra in idx:
            entry[extra] = idx[extra]

    return entry


def build_task_detail(
    hit: Dict[str, Any], info: Optional[Dict[str, Any]], kind: str, kind_label: str
) -> Dict[str, Any]:
    """把一个 ES hit + /info 返回合成一个任务详情。"""
    # info 里 config_string 会带每个索引的进度信息；优先用 info
    if info is not None:
        source = info
        # info 顶层不带 _id，用 id / task_id / 或 search hit
        task_id = info.get("id") or hit.get("_id")
    else:
        source = hit.get("_source") or {}
        task_id = hit.get("_id") or source.get("id")

    config = parse_task_config(source)
    metadata = source.get("metadata") or {}
    labels = metadata.get("labels") or {}
    repeat = source.get("repeat") or labels.get("repeat") or {}

    # ---- 时间/时长 ----
    created = source.get("created") or ""
    updated = source.get("updated") or ""
    start_ms = safe_int(source.get("start_time_in_millis")) or 0
    completed_time = source.get("completed_time")  # ISO 或 None
    completed_ms = parse_iso_to_ms(completed_time) if completed_time else None
    created_ms = parse_iso_to_ms(created)
    updated_ms = parse_iso_to_ms(updated)

    duration_ms: Optional[int] = None
    if start_ms > 0 and completed_ms:
        duration_ms = max(0, completed_ms - start_ms)
    elif start_ms > 0 and updated_ms and source.get("status") in ("running", "ready"):
        duration_ms = max(0, updated_ms - start_ms)
    elif created_ms and completed_ms:
        # 兜底：没有 start，用 created → completed
        duration_ms = max(0, completed_ms - created_ms)

    # ---- 索引 ----
    indices_cfg = config.get("indices") if isinstance(config.get("indices"), list) else []
    indices = [_index_entry(i, kind) for i in indices_cfg]

    # ---- 汇总数据量 ----
    total_source_docs = sum(i["source_docs"] for i in indices)
    total_target_docs = sum(i["target_docs"] for i in indices)
    total_source_bytes = sum(i["source_store_bytes"] for i in indices)

    # 重复调度类：官方 UI 用 labels.source_total_docs / target_total_docs
    is_repeat = bool(repeat.get("is_repeat") or labels.get("repeat", {}).get("is_repeat"))
    labels_src_total = safe_int(labels.get("source_total_docs"))
    labels_tgt_total = safe_int(labels.get("target_total_docs"))
    if is_repeat:
        if labels_src_total is not None:
            total_source_docs = labels_src_total
        if labels_tgt_total is not None:
            total_target_docs = labels_tgt_total
    else:
        # 非 repeat 时，labels 里的数字通常是 populateMajorTaskInfo 填的最新值，
        # 比 config.indices[].source.docs（快照）更准
        if labels_tgt_total is not None:
            total_target_docs = labels_tgt_total
        if labels_src_total is not None and labels_src_total > total_source_docs:
            total_source_docs = labels_src_total

    # ---- 进度百分比 ----
    if kind == "migration":
        overall_percent = None
        if total_source_docs > 0:
            overall_percent = round(min(100.0, total_target_docs / total_source_docs * 100), 2)
        elif source.get("status") == "complete":
            overall_percent = 100.0
    else:  # comparison
        # 比对：以 total_scroll_docs / (source+target) 为准
        scroll_docs = safe_int(labels.get("source_scroll_docs") or 0) or 0
        scroll_docs += safe_int(labels.get("target_scroll_docs") or 0) or 0
        expected = total_source_docs + total_target_docs
        overall_percent = None
        if expected > 0:
            overall_percent = round(min(100.0, scroll_docs / expected * 100), 2)

    # ---- 集群 ----
    cluster = config.get("cluster") or {}
    src_cluster = cluster.get("source") or {}
    tgt_cluster = cluster.get("target") or {}

    # ---- 执行 / scroll / bulk ----
    execution = _extract_execution(config)
    tuning = _extract_scroll_bulk(config, kind)

    # ---- 完成的索引数（后端返回） ----
    completed_indices = safe_int(labels.get("completed_indices"))

    return {
        "kind": kind,
        "kind_label": kind_label,
        "task_id": task_id,
        "name": config.get("name") or labels.get("name") or "",
        "status": source.get("status") or "",
        "task_lifecycle": source.get("task_lifecycle") or "",
        "cancellable": bool(source.get("cancellable")),
        "runnable": bool(source.get("runnable")),
        # 时间
        "created": created,
        "updated": updated,
        "start_time_in_millis": start_ms,
        "completed_time": completed_time,
        "duration_ms": duration_ms,
        "duration": format_duration_ms(duration_ms),
        # 人员
        "creator": (config.get("creator") or {}).get("name") or labels.get("creator") or "",
        "creator_id": (config.get("creator") or {}).get("id") or "",
        "tags": config.get("tags") or [],
        # 集群
        "source_cluster": {
            "id": src_cluster.get("id") or labels.get("source_cluster_id") or "",
            "name": src_cluster.get("name") or "",
            "distribution": src_cluster.get("distribution") or "",
        },
        "target_cluster": {
            "id": tgt_cluster.get("id") or labels.get("target_cluster_id") or "",
            "name": tgt_cluster.get("name") or "",
            "distribution": tgt_cluster.get("distribution") or "",
        },
        # 索引汇总
        "indices_count": len(indices),
        "completed_indices": completed_indices,
        "total_source_docs": total_source_docs,
        "total_target_docs": total_target_docs,
        "total_source_store_bytes": total_source_bytes,
        "total_source_store_size": format_bytes(total_source_bytes),
        "overall_percent": overall_percent,
        # 状态位
        "is_incremental": any(i["incremental"] for i in indices),
        "is_partitioned": any(i["partitioned"] for i in indices),
        "is_repeat": is_repeat,
        "run_times": safe_int(labels.get("run_times")),
        "error_partitions": safe_int(labels.get("error_partitions")),
        "running_children": safe_int(source.get("running_children")),
        "manual_pause": bool(labels.get("manual_pause")),
        "next_run_time": labels.get("next_run_time"),
        "repeat_state": labels.get("repeat_state") or "",
        # 执行 / 调优参数
        "execution": execution,
        "tuning": tuning,
        # log 位置（用于查错）
        "log_info": labels.get("log_info") or {},
        # 每个索引的详情
        "indices": indices,
    }


# ------------------------- 输出 -------------------------


def write_json(tasks: List[Dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(tasks, f, ensure_ascii=False, indent=2)


CSV_FIELDS = [
    # 任务级
    "kind",
    "task_id",
    "task_name",
    "status",
    "task_lifecycle",
    "created",
    "start_time",
    "completed_time",
    "duration",
    "duration_ms",
    "creator",
    "source_cluster",
    "source_cluster_id",
    "source_distribution",
    "target_cluster",
    "target_cluster_id",
    "target_distribution",
    "task_indices_count",
    "task_completed_indices",
    "task_total_source_docs",
    "task_total_target_docs",
    "task_total_source_size",
    "task_overall_percent",
    "task_is_incremental",
    "task_is_partitioned",
    "task_is_repeat",
    "task_run_times",
    "task_error_partitions",
    "task_running_children",
    "runtime_group",
    "execution_nodes",
    "scroll_slice_size",
    "scroll_partition_size",
    "scroll_docs",
    "scroll_timeout",
    "bulk_docs",
    "bulk_store_size_mb",
    "bulk_max_worker_size",
    "bulk_slice_size",
    "max_tasks_per_instance",
    "max_retry_times",
    "tags",
    # 索引级
    "source_index",
    "target_index",
    "doc_type",
    "source_docs",
    "target_docs",
    "source_store_size",
    "source_store_bytes",
    "index_status",
    "index_percent",
    "index_exported_percent",
    "index_scroll_percent",
    "index_total_scroll_docs",
    "index_total_diff_docs",
    "index_error_partitions",
    "index_running_children",
    "has_filter",
    "incremental_field",
    "incremental_delay",
    "partitioned",
    "partition_field",
    "partition_step",
]


def write_csv(tasks: List[Dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for t in tasks:
            exec_ = t.get("execution") or {}
            tuning = t.get("tuning") or {}
            scroll = tuning.get("scroll") or tuning.get("dump") or {}
            bulk = tuning.get("bulk") or {}
            nodes = exec_.get("nodes") or []
            runtime_group = exec_.get("runtime_group") or {}
            start_ms = t.get("start_time_in_millis") or 0
            from datetime import datetime, timezone

            start_str = ""
            if start_ms > 0:
                try:
                    start_str = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat()
                except Exception:
                    start_str = str(start_ms)

            base = {
                "kind": t["kind_label"],
                "task_id": t["task_id"],
                "task_name": t["name"],
                "status": t["status"],
                "task_lifecycle": t.get("task_lifecycle") or "",
                "created": t.get("created") or "",
                "start_time": start_str,
                "completed_time": t.get("completed_time") or "",
                "duration": t.get("duration") or "",
                "duration_ms": t.get("duration_ms") if t.get("duration_ms") is not None else "",
                "creator": t.get("creator") or "",
                "source_cluster": t["source_cluster"]["name"] or t["source_cluster"]["id"],
                "source_cluster_id": t["source_cluster"]["id"],
                "source_distribution": t["source_cluster"]["distribution"],
                "target_cluster": t["target_cluster"]["name"] or t["target_cluster"]["id"],
                "target_cluster_id": t["target_cluster"]["id"],
                "target_distribution": t["target_cluster"]["distribution"],
                "task_indices_count": t.get("indices_count") or 0,
                "task_completed_indices": t.get("completed_indices") if t.get("completed_indices") is not None else "",
                "task_total_source_docs": t.get("total_source_docs") or 0,
                "task_total_target_docs": t.get("total_target_docs") or 0,
                "task_total_source_size": t.get("total_source_store_size") or "",
                "task_overall_percent": t.get("overall_percent") if t.get("overall_percent") is not None else "",
                "task_is_incremental": t.get("is_incremental", False),
                "task_is_partitioned": t.get("is_partitioned", False),
                "task_is_repeat": t.get("is_repeat", False),
                "task_run_times": t.get("run_times") if t.get("run_times") is not None else "",
                "task_error_partitions": t.get("error_partitions") if t.get("error_partitions") is not None else "",
                "task_running_children": t.get("running_children") if t.get("running_children") is not None else "",
                "runtime_group": runtime_group.get("name") or runtime_group.get("id") or "",
                "execution_nodes": ";".join(n.get("name") or n.get("id") or "" for n in nodes),
                "scroll_slice_size": scroll.get("slice_size", ""),
                "scroll_partition_size": scroll.get("partition_size", ""),
                "scroll_docs": scroll.get("docs", ""),
                "scroll_timeout": scroll.get("timeout") or scroll.get("scroll_time", ""),
                "bulk_docs": bulk.get("docs", ""),
                "bulk_store_size_mb": bulk.get("store_size_in_mb", ""),
                "bulk_max_worker_size": bulk.get("max_worker_size", ""),
                "bulk_slice_size": bulk.get("slice_size", ""),
                "max_tasks_per_instance": exec_.get("max_tasks_per_instance", "") or "",
                "max_retry_times": exec_.get("max_retry_times", "") or "",
                "tags": ";".join(t.get("tags") or []),
            }

            if not t["indices"]:
                writer.writerow(base)
                continue

            for idx in t["indices"]:
                inc = idx.get("incremental") or {}
                if not isinstance(inc, dict):
                    inc = {}
                part = idx.get("partition") or {}
                if not isinstance(part, dict):
                    part = {}
                row = dict(base)
                row.update(
                    {
                        "source_index": idx.get("source_index", ""),
                        "target_index": idx.get("target_index", ""),
                        "doc_type": idx.get("doc_type", ""),
                        "source_docs": idx.get("source_docs", ""),
                        "target_docs": idx.get("target_docs", ""),
                        "source_store_size": idx.get("source_store_size", ""),
                        "source_store_bytes": idx.get("source_store_bytes", ""),
                        "index_status": idx.get("status", ""),
                        "index_percent": idx.get("percent", ""),
                        "index_exported_percent": idx.get("exported_percent", ""),
                        "index_scroll_percent": idx.get("scroll_percent", ""),
                        "index_total_scroll_docs": idx.get("total_scroll_docs", ""),
                        "index_total_diff_docs": idx.get("total_diff_docs", ""),
                        "index_error_partitions": idx.get("error_partitions", ""),
                        "index_running_children": idx.get("running_children", ""),
                        "has_filter": idx.get("has_filter", False),
                        "incremental_field": inc.get("field_name", ""),
                        "incremental_delay": inc.get("delay", ""),
                        "partitioned": idx.get("partitioned", False),
                        "partition_field": part.get("field_name", ""),
                        "partition_step": part.get("step", ""),
                    }
                )
                writer.writerow(row)


def print_summary(tasks: List[Dict[str, Any]]) -> None:
    if not tasks:
        print("未查询到任何任务。", file=sys.stderr)
        return
    by_kind: Dict[str, int] = {}
    for t in tasks:
        by_kind[t["kind_label"]] = by_kind.get(t["kind_label"], 0) + 1
    for k, v in by_kind.items():
        print(f"  {k}: {v} 个任务", file=sys.stderr)

    print("", file=sys.stderr)
    # 更宽的表头
    print(
        f"{'类型':<6}{'状态':<10}{'任务名称':<30}{'索引数':>4} {'进度':>6}"
        f"  {'源文档':>10}  {'目标文档':>10}  {'用时':>10}",
        file=sys.stderr,
    )
    for t in tasks:
        name = (t["name"] or "(未命名)")[:28]
        percent = t.get("overall_percent")
        percent_s = f"{percent:.1f}%" if isinstance(percent, (int, float)) else "-"
        print(
            f"{t['kind_label']:<6}{(t['status'] or '-'):<10}{name:<30}"
            f"{t['indices_count']:>4} {percent_s:>6}"
            f"  {t['total_source_docs']:>10,}"
            f"  {t['total_target_docs']:>10,}"
            f"  {(t.get('duration') or '-'):>10}",
            file=sys.stderr,
        )


# ------------------------- 入口 -------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="导出 INFINI Console 数据工具（迁移/比对）任务的详细报告",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 使用配置文件（推荐）
  python data_tasks_report.py --config ../config.json

  # 直接指定 Console
  python data_tasks_report.py -c https://localhost:9000 -u admin -p password --insecure

  # 密码包含特殊字符（如 !）时，必须用单引号括起来:
  python data_tasks_report.py -c https://localhost:9000 -u admin -p 'your!password@123' --insecure

  # 不指定密码，交互式输入（避免命令行记录密码）
  python data_tasks_report.py -c https://localhost:9000 -u admin --insecure

  # 只导出数据迁移任务，输出 JSON
  python data_tasks_report.py --config ../config.json --kind migration --format json

  # 跳过 /info 详情调用（更快，但没有每个索引的进度）
  python data_tasks_report.py --config ../config.json --no-info

  # 指定输出目录
  python data_tasks_report.py --config ../config.json -o ./exports
""",
    )
    add_common_args(parser)

    parser.add_argument(
        "--kind",
        choices=["migration", "comparison", "all"],
        default="all",
        help="要导出的任务类型 (默认: all)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv", "both"],
        default="both",
        help="输出格式 (默认: both — 同时输出 JSON 和 CSV)",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=200,
        help="每次翻页拉取的任务数 (默认: 200)",
    )
    parser.add_argument(
        "--no-info",
        action="store_true",
        help="不调用 /info 接口获取每个索引的进度（速度更快）",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config, _ = load_and_merge_config(args)

    console_url = get_config_value(
        args.console, config.get("consoleUrl"), "CONSOLE_URL", "http://localhost:9000"
    )
    auth = config.get("auth", {}) if isinstance(config.get("auth"), dict) else {}
    username = get_config_value(args.username, auth.get("username"), "CONSOLE_USERNAME", "")
    password = get_config_value(args.password, auth.get("password"), "CONSOLE_PASSWORD", "")
    timeout = int(get_config_value(str(args.timeout), str(config.get("timeout", 60)), "CONSOLE_TIMEOUT", "60"))
    insecure = bool(args.insecure or config.get("insecure", False))

    try:
        client = create_authenticated_client(
            console_url=console_url,
            username=username,
            password=password,
            timeout=timeout,
            verify_ssl=not insecure,
            verbose=True,
        )
    except ConsoleAuthError as e:
        print(f"错误: {e}", file=sys.stderr)
        return 2

    kinds_to_fetch = TASK_KINDS
    if args.kind != "all":
        kinds_to_fetch = [k for k in TASK_KINDS if k[3] == args.kind]

    all_tasks: List[Dict[str, Any]] = []
    for list_endpoint, info_tpl, label, kind in kinds_to_fetch:
        print(f"正在获取 {label} 任务列表 ({list_endpoint}) ...", file=sys.stderr)
        try:
            hits = fetch_tasks(client, list_endpoint, page_size=args.page_size)
        except ConsoleAPIError as e:
            print(f"获取 {label} 任务失败: {e}", file=sys.stderr)
            continue

        print(f"  → {len(hits)} 条; 正在拉取详情 ...", file=sys.stderr)
        for i, hit in enumerate(hits, 1):
            task_id = hit.get("_id")
            info = None
            if not args.no_info and task_id:
                info = fetch_task_info(client, info_tpl, task_id)
            all_tasks.append(build_task_detail(hit, info, kind, label))
            if i % 20 == 0:
                print(f"    ... {i}/{len(hits)}", file=sys.stderr)

    print("", file=sys.stderr)
    print_summary(all_tasks)

    # 写文件
    out_dir = Path(args.output) if args.output else Path("./data_tasks_output")
    if out_dir.suffix:  # 用户直接给了文件名
        if args.format == "both":
            base = out_dir.with_suffix("")
            write_json(all_tasks, base.with_suffix(".json"))
            write_csv(all_tasks, base.with_suffix(".csv"))
            print(f"\n已写入: {base.with_suffix('.json')}", file=sys.stderr)
            print(f"已写入: {base.with_suffix('.csv')}", file=sys.stderr)
        elif args.format == "json":
            write_json(all_tasks, out_dir)
            print(f"\n已写入: {out_dir}", file=sys.stderr)
        else:
            write_csv(all_tasks, out_dir)
            print(f"\n已写入: {out_dir}", file=sys.stderr)
    else:
        out_dir.mkdir(parents=True, exist_ok=True)
        json_path = out_dir / "data_tasks.json"
        csv_path = out_dir / "data_tasks.csv"
        wrote = []
        if args.format in ("json", "both"):
            write_json(all_tasks, json_path)
            wrote.append(json_path)
        if args.format in ("csv", "both"):
            write_csv(all_tasks, csv_path)
            wrote.append(csv_path)
        print("", file=sys.stderr)
        for p in wrote:
            print(f"已写入: {p}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n用户中断。", file=sys.stderr)
        sys.exit(130)
