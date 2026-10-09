#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Console 指标写入延迟排查工具（仅 Console 权限，不需要 ES 集群密码）

背景：非探针（agentless）模式下 Console 自己定时采集集群指标（默认 10s/轮），
  采集时打上 timestamp -> 推入本地磁盘队列(metrics) -> 后台消费者异步 bulk 写入系统 ES。
  监控图表和"非探针"状态灯都读文档的 timestamp；写入链路积压时，文档带着旧时间戳
  晚 N 小时落库，表现为：图表永远只画到 now-N小时、状态灯红色。

所有检查都通过 Console 自身接口完成（Console 会自己去查系统 ES，因此不需要 ES 密码）：
  A. Console 本地队列        GET  /queue/stats
  B. 被监控集群列表          GET  /elasticsearch/_search（ConsoleClient 分页封装）
  C. 采集状态（红绿灯数据）  GET  /elasticsearch/<cluster_id>/_collection_stats
  D. 指标新鲜度明细          _proxy -> .infini_metrics-*/_search
  E. 系统 ES 负载（根因）    _proxy -> _cat/thread_pool/write 等
  F. 缺席二次诊断            _proxy -> 按 cluster_id 直查 .infini_metrics 实际最新时间戳

关于 absent（已对照 Console 源码 modules/elastic/api/cluster_overview.go:
getClusterMonitorState / getCollectionStats 确认）：
  - _collection_stats 只查 timestamp >= now-15m 窗口，窗口内没有某类型的文档，
    响应里就没有这个类型的 key；
  - Console 返回的 status 只有 ok（5 分钟内）/ warning（超过 5 分钟），不存在
    "absent" 值——本工具在响应完全没有指标 key 时把预期类型统一标记为 absent，
    语义是"该类型 >=15 分钟无数据"；
  - 因此"所有集群全部 absent"的直接含义是：系统 ES 最近 15 分钟没有落库任何指标
    文档。在写入积压环境下这是必然表现（文档带旧时间戳晚到），不是采集坏了；
    是否真是积压，看本报告的新鲜度明细与队列 lag 即可判别。

缺席二次诊断（本工具增强）：对报 absent 的集群直查系统 ES 里该 cluster_id 的
  实际最新指标时间戳，把每个缺席类型归类为：
  - no_data          系统 ES 里该集群从未有过该类型文档（采集从未成功/cluster_id 变更）
  - stale            有文档但最新时间戳已超过 15 分钟（采集停止或写入积压）
  - fresh_but_absent 最新时间戳在 15 分钟内，_collection_stats 却缺席
                     —— Console 状态接口与实际数据不一致，疑似 Console bug，建议上报

判定逻辑：
  - 队列有积压（lag/depth 超阈值）且指标延迟大  => 写入链路积压（采集正常，落库慢）
  - 队列无积压但指标延迟大                      => 采集停了/队列消费者卡死
  - 队列无积压且指标延迟 < 5 分钟               => 健康

用法：
  python console_queue_delay.py -c https://<console>:9000 -u admin -p '<密码>'
  python console_queue_delay.py --config config.json
  python console_queue_delay.py -c ... --watch --duration 600 --interval 10   # 趋势观察

输出（写入 --output 目录，默认 ./exports）：
  console_delay_report_<时间戳>.md    人读报告
  console_delay_raw_<时间戳>.json     原始数据
  console_delay_trend_<时间戳>.csv    watch 模式趋势采样

依赖：仅标准库 + 仓库 common 模块。
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))
from common.config import add_common_args, get_config_value, load_and_merge_config
from common.console_client import (
    ConsoleAPIError,
    ConsoleAuthError,
    ConsoleClient,
    DEFAULT_SYSTEM_CLUSTER_ID,
)

# 各采集模式下 Console 实际会采集的指标类型（对齐 Console 源码
# modules/elastic/api/cluster_overview.go getClusterMonitorState 的白名单：
# agentless 采 index_stats；agent 采 shard_stats；其余类型两种模式都采）
EXPECTED_TYPES: Dict[str, List[str]] = {
    "agentless": ["cluster_health", "cluster_stats", "node_stats", "index_stats", "index_health"],
    "agent": ["cluster_health", "cluster_stats", "node_stats", "shard_stats", "index_health"],
}
# 判定延迟时关注的核心类型（任一模式下都有意义）
CORE_METRIC_NAMES = ["cluster_health", "cluster_stats", "node_stats", "index_stats", "shard_stats"]

# 判定阈值
DELAY_WARN_SEC = 300        # 指标延迟超过 5 分钟即不健康（对齐 Console 状态灯判定）
DELAY_BAD_SEC = 1800        # 延迟超过 30 分钟判定为严重
QUEUE_LAG_WARN = 1000       # 消费 offset 落后写入 offset 超过该值视为积压
QUEUE_DEPTH_WARN = 1        # depth>0 即有未消费消息
COLLECTION_WINDOW_SEC = 900  # _collection_stats 的查询窗口（now-15m），缺席诊断用同一口径


# --------------------------------------------------------------------------
# 纯函数（可单测）
# --------------------------------------------------------------------------

def _parse_offset(v: Any) -> Optional[Tuple[int, int]]:
    """队列 offset 形如 '4,62257043'（段,位置），返回 (段号, 位置)。
    必须段号相同才能比较位置大小——写入已滚动到新段时跨段比较会得出荒谬的 lag。"""
    if not isinstance(v, str) or "," not in v:
        return None
    try:
        seg, pos = v.split(",", 1)
        return int(seg), int(pos)
    except ValueError:
        return None


def summarize_queues(raw: Any) -> List[Dict[str, Any]]:
    """把 /queue/stats 的 disk 队列转成扁平列表，附积压计算。"""
    out: List[Dict[str, Any]] = []
    disk = ((raw or {}).get("queue") or {}).get("disk") or {}
    for qid, q in disk.items():
        meta = q.get("metadata", {}) or {}
        consumers = q.get("consumers") or []
        write = _parse_offset(q.get("offset"))
        read = _parse_offset(q.get("earliest_consumer_offset"))
        lag, multi_segment = None, False
        if write and read:
            if write[0] == read[0]:
                lag = write[1] - read[1]
            else:
                # 写入已滚动到更新的段：有跨段积压，位置差无意义
                multi_segment = True
        last_active = max((c.get("last_active") or "" for c in consumers), default="")
        out.append({
            "name": meta.get("name", qid),
            "depth": q.get("depth"),
            "messages": q.get("messages"),
            "lag": lag,
            "multi_segment": multi_segment,
            "offset": q.get("offset"),
            "consumer_offset": q.get("earliest_consumer_offset"),
            "consumers": len(consumers),
            "last_active": last_active,
            "storage": (q.get("storage") or {}).get("local_usage", ""),
        })
    # 指标相关队列排最前，其余按名字排
    out.sort(key=lambda x: (0 if "metric" in x["name"].lower() else 1, x["name"]))
    return out


def _age_seconds(last_active: str) -> Optional[float]:
    """消费者 last_active（RFC3339，如 2026-09-28T17:07:15+08:00）距今秒数。
    用 timezone-aware 写法，兼容 3.12+（utcnow 已废弃）至未来版本。"""
    try:
        t = dt.datetime.fromisoformat(last_active.strip())
        if t.tzinfo is not None:
            now = dt.datetime.now(tz=t.tzinfo)
        else:
            now = dt.datetime.now()
        return (now - t).total_seconds()
    except Exception:
        pass
    return None


def _age_gt(last_active: str, sec_threshold: int) -> bool:
    sec = _age_seconds(last_active)
    return sec is not None and sec > sec_threshold


def verdict(queues: List[Dict[str, Any]],
            freshness: Optional[Dict[str, Any]]) -> Tuple[str, List[str]]:
    """综合队列积压与指标新鲜度给出结论"""
    reasons: List[str] = []
    metric_queues = [q for q in queues if "metric" in q["name"].lower()]
    has_backlog = any(
        (q["lag"] is not None and q["lag"] > QUEUE_LAG_WARN)
        or q.get("multi_segment")
        or (isinstance(q["depth"], int) and q["depth"] > QUEUE_DEPTH_WARN)
        for q in metric_queues)
    # 卡死 = 队列里有待消费消息(lag>0)且消费者 5 分钟没动；lag=0 时消费者空闲是正常的
    consumers_stalled = any(
        (q["lag"] or 0) > 0 and _age_gt(q["last_active"], 300)
        for q in metric_queues)
    delay = freshness.get("overall") if freshness else None

    if has_backlog:
        reasons.append("指标相关队列存在积压（lag/depth 超阈值）→ 采集正常、写入端（队列消费者/系统ES）跟不上")
    if consumers_stalled:
        reasons.append("队列消费者 last_active 停滞 → 消费者可能卡死")
    if delay is None:
        if not reasons:
            return "无法判定（指标新鲜度查询失败，仅队列数据可用）", reasons
        return "疑似队列侧问题（无指标新鲜度数据佐证）", reasons
    if delay <= DELAY_WARN_SEC:
        if not reasons:
            return "健康：指标延迟在 5 分钟内，队列无积压", reasons
        return "队列有积压但指标尚新鲜（刚发生/轻微）", reasons
    if has_backlog:
        tag = "严重" if delay > DELAY_BAD_SEC else "明显"
        return f"写入链路积压：指标延迟 {int(delay)}s（{tag}），文档带旧时间戳晚到 → 图表延迟、状态灯红", reasons
    if consumers_stalled:
        return f"队列消费者疑似卡死：指标延迟 {int(delay)}s 且消费者不推进", reasons
    return (f"队列无积压但指标延迟 {int(delay)}s → 采集任务停了或没写入"
            "（查 Console 日志 collect/persist 错误、集群监控是否被关闭）", reasons)


def _now_str() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _ms_str(val: Any) -> str:
    if not val:
        return "-"
    try:
        return dt.datetime.fromtimestamp(val / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(val)


def _fmt_delay(sec: Any) -> str:
    if sec is None:
        return "-"
    sec = int(sec)
    if sec < 90:
        return f"{sec}s"
    if sec < 7200:
        return f"{sec // 60}m{sec % 60:02d}s"
    return f"{sec // 3600}h{(sec % 3600) // 60:02d}m"


def classify_absent_type(max_ts_ms: Optional[int], now_ms: int) -> Dict[str, Any]:
    """缺席二次诊断：按该类型在系统 ES 里的实际最新时间戳归类。

    - no_data:          该集群没有该类型的任何文档（从未采集成功/cluster_id 变更）
    - stale:            有文档但最新时间戳已超出 _collection_stats 的 15 分钟窗口
    - fresh_but_absent: 最新时间戳仍在 15 分钟内，状态接口却缺席 —— 疑似 Console bug
    """
    if not max_ts_ms:
        return {"classification": "no_data", "max_timestamp": None,
                "delay_sec": None}
    delay = round((now_ms - max_ts_ms) / 1000)
    if delay <= COLLECTION_WINDOW_SEC:
        return {"classification": "fresh_but_absent",
                "max_timestamp": _ms_str(max_ts_ms), "delay_sec": delay}
    return {"classification": "stale",
            "max_timestamp": _ms_str(max_ts_ms), "delay_sec": delay}


# --------------------------------------------------------------------------
# Console 访问（基于 common.console_client.ConsoleClient）
# --------------------------------------------------------------------------

def connect_client(console_url: str, username: str, password: str,
                   timeout: int, insecure: bool) -> ConsoleClient:
    """创建客户端并登录；协议猜错（http/https）时自动切换重试一次"""
    def _attempt(base_url: str) -> Tuple[bool, str, ConsoleClient]:
        client = ConsoleClient(base_url, username, password,
                               timeout=timeout, verify_ssl=not insecure)
        try:
            return client.login(), "", client
        except ConsoleAPIError as e:
            return False, str(e), client
        except ConsoleAuthError as e:
            return False, str(e), client

    ok, err, client = _attempt(console_url)
    if not ok:
        parsed = urllib.parse.urlparse(console_url)
        flipped = ((f"http://{parsed.netloc}" if parsed.scheme == "https"
                    else f"https://{parsed.netloc}"))
        print(f"登录失败（{err or '凭据被拒绝'}），改试 {flipped} ...")
        ok, err, alt = _attempt(flipped)
        if ok:
            client = alt
    if not ok:
        raise SystemExit(
            f"Console 登录失败: {err or '用户名或密码错误'}\n"
            "   提示：检查地址/账号；Console 1.31+ 使用挑战-响应登录（已内置支持）")
    return client


def call_with_relogin(client: ConsoleClient, func, *args, **kwargs):
    """调用 Console API；token 过期（401）时重新登录一次再试"""
    try:
        return func(*args, **kwargs)
    except ConsoleAPIError as e:
        if "401" not in str(e):
            raise
        client.login()
        return func(*args, **kwargs)


def resolve_system_cluster_id(client: ConsoleClient,
                              preferred: str = "") -> str:
    """系统集群 ID：显式指定优先，否则从集群列表识别，最后兜底固定值"""
    if preferred:
        return preferred
    try:
        clusters = client.get_clusters()
    except ConsoleAPIError as e:
        print(f"警告: 获取集群列表失败({e})，降级使用默认系统集群ID {DEFAULT_SYSTEM_CLUSTER_ID}",
              file=sys.stderr)
        return DEFAULT_SYSTEM_CLUSTER_ID
    for c in clusters:
        if ConsoleClient.is_system_cluster(c.get("id", ""), c.get("name", "")):
            return c["id"]
    return DEFAULT_SYSTEM_CLUSTER_ID


# --------------------------------------------------------------------------
# 各数据源采集
# --------------------------------------------------------------------------

def fetch_queue_stats(client: ConsoleClient) -> Tuple[List[Dict[str, Any]], str]:
    """Console 本地磁盘队列统计；/queue/stats 部分版本无需登录"""
    try:
        raw = call_with_relogin(client, client.request, "queue/stats", "GET")
    except ConsoleAPIError as e:
        return [], f"HTTP错误: {e}"
    return summarize_queues(raw), ""


def fetch_collection_states(client: ConsoleClient,
                            clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """逐集群调 _collection_stats（界面红绿灯数据源）。
    Console 只返回窗口内有数据的类型；本工具把缺席的预期类型补成 absent
    （= 已 >=15 分钟无数据），并记录缺失清单供二次诊断。"""
    out: List[Dict[str, Any]] = []
    for c in clusters:
        if not (c.get("monitored") and c.get("enabled")):
            out.append({**c, "stats": None, "missing_types": [],
                        "note": "监控未启用，跳过"})
            continue
        try:
            b = call_with_relogin(
                client, client.request,
                f"elasticsearch/{c['id']}/_collection_stats", "GET")
        except ConsoleAPIError as e:
            out.append({**c, "stats": None, "missing_types": [],
                        "note": f"查询失败: {e}"})
            continue
        if not isinstance(b, dict):
            out.append({**c, "stats": None, "missing_types": [],
                        "note": "响应格式异常"})
            continue
        types: Dict[str, Any] = {}
        for k, v in b.items():
            if isinstance(v, dict) and "last_active_at" in v:
                ts = v.get("last_active_at")
                delay = (round((time.time() * 1000 - ts) / 1000)
                         if isinstance(ts, (int, float)) else None)
                types[k] = {"status": v.get("status"), "delay_sec": delay}
        mode = str(b.get("metric_collection_mode") or c.get("mode") or "").lower()
        expected = EXPECTED_TYPES.get(mode, EXPECTED_TYPES["agentless"])
        missing = [t for t in expected if t not in types]
        # Console 只返回窗口内有数据的类型；缺席的预期类型统一补成 absent 标记
        # （= 已 >=15 分钟无数据），并保留 missing 清单供二次诊断
        for t in missing:
            types[t] = {"status": "absent", "delay_sec": None}
        out.append({**c, "stats": types, "missing_types": missing, "mode": mode})
    return out


def metric_freshness(client: ConsoleClient, system_cluster_id: str) -> Dict[str, Any]:
    """经系统集群代理查 .infini_metrics-* 每类指标/每个集群的最新 timestamp，计算延迟。
    overall 只取核心指标类型的最大延迟——次要类型可能因历史集群残留而陈旧，
    不应主导判定。"""
    now_ms = int(time.time() * 1000)
    res: Dict[str, Any] = {"checked_at": _now_str(), "overall": None,
                           "by_name": [], "by_cluster": [],
                           "docs_last_5m": None, "docs_last_1h": None}
    try:
        b = client.proxy_request(system_cluster_id, "POST", ".infini_metrics-*/_search", {
            "size": 0,
            "aggs": {
                "by_name": {"terms": {"field": "metadata.name", "size": 50},
                            "aggs": {"max_ts": {"max": {"field": "timestamp"}}}},
                "by_cluster": {"terms": {"field": "metadata.labels.cluster_id", "size": 50},
                               "aggs": {"max_ts": {"max": {"field": "timestamp"}}}},
            },
        })
    except ConsoleAPIError as e:
        res["error"] = str(e)
        return res
    if not isinstance(b, dict) or "aggregations" not in b:
        # 代理返回 200 但没有 aggregations：通常是权限/代理层错误，把原始体带出来便于定位
        res["error"] = f"响应异常: {str(b)[:300]}"
        return res
    aggs = b["aggregations"]

    def fmt(bucket: dict) -> dict:
        val = ((bucket.get("max_ts") or {}).get("value"))
        delay = (now_ms - val) / 1000 if val else None
        return {"name_or_cluster": bucket.get("key"), "max_timestamp": _ms_str(val),
                "delay_sec": round(delay) if delay is not None else None}

    def sort_key(d: dict):
        return (d["delay_sec"] is None, -(d["delay_sec"] or 0))

    res["by_name"] = sorted((fmt(x) for x in aggs.get("by_name", {}).get("buckets", [])), key=sort_key)
    res["by_cluster"] = sorted((fmt(x) for x in aggs.get("by_cluster", {}).get("buckets", [])), key=sort_key)
    core_delays = [d["delay_sec"] for d in res["by_name"]
                   if d["name_or_cluster"] in CORE_METRIC_NAMES and d["delay_sec"] is not None]
    if core_delays:
        res["overall"] = max(core_delays)

    for key, gte in (("docs_last_5m", "now-5m"), ("docs_last_1h", "now-1h")):
        try:
            b = client.proxy_request(system_cluster_id, "POST", ".infini_metrics-*/_search",
                                     {"size": 0, "query": {"range": {"timestamp": {"gte": gte}}}})
        except ConsoleAPIError:
            continue
        if isinstance(b, dict):
            total = b.get("hits", {}).get("total", {})
            res[key] = total.get("value") if isinstance(total, dict) else total
    return res


def diagnose_absent_clusters(client: ConsoleClient, system_cluster_id: str,
                             cluster_states: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """缺席二次诊断：对报 absent 的集群直查系统 ES 里该 cluster_id 的实际最新时间戳。

    返回 {cluster_id: {"total_docs": n, "types": {类型: 诊断结果}}}，
    仅包含有缺席类型的集群。用于判别"absent 到底是积压/采集停止，还是
    Console 状态接口与数据不一致（bug）"。
    """
    targets = [c for c in cluster_states if c.get("missing_types")]
    if not targets:
        return {}
    now_ms = int(time.time() * 1000)
    diagnosis: Dict[str, Dict[str, Any]] = {}
    for c in targets:
        cluster_id = c["id"]
        entry: Dict[str, Any] = {"total_docs": None, "types": {}}
        try:
            b = client.proxy_request(system_cluster_id, "POST", ".infini_metrics-*/_search", {
                "size": 0,
                "query": {"bool": {"must": [
                    {"term": {"metadata.labels.cluster_id": cluster_id}},
                    {"term": {"metadata.category": "elasticsearch"}},
                ]}},
                "aggs": {"by_name": {"terms": {"field": "metadata.name", "size": 50},
                                     "aggs": {"max_ts": {"max": {"field": "timestamp"}}}}},
            })
        except ConsoleAPIError as e:
            entry["error"] = str(e)
            diagnosis[cluster_id] = entry
            continue
        if not isinstance(b, dict):
            entry["error"] = "响应格式异常"
            diagnosis[cluster_id] = entry
            continue
        hits_total = b.get("hits", {}).get("total", {})
        entry["total_docs"] = hits_total.get("value") if isinstance(hits_total, dict) else hits_total
        buckets = {bk.get("key"): ((bk.get("max_ts") or {}).get("value"))
                   for bk in b.get("aggregations", {}).get("by_name", {}).get("buckets", [])}
        for t in c["missing_types"]:
            entry["types"][t] = classify_absent_type(buckets.get(t), now_ms)
        diagnosis[cluster_id] = entry
    return diagnosis


def es_system_health(client: ConsoleClient, system_cluster_id: str) -> Dict[str, Any]:
    """系统集群自身负载（经代理）：write 线程池、节点负载、指标索引大小。"""
    res: Dict[str, Any] = {}
    for key, path in (("thread_pool_write", "/_cat/thread_pool/write?v&h=node_name,active,queue,rejected"),
                      ("nodes", "/_cat/nodes?v&h=name,cpu,heap.percent,disk.used_percent,disk.used"),
                      ("metrics_indices", "/_cat/indices/.infini_metrics*?v&h=index,docs.count,store.size")):
        try:
            res[key] = client.proxy_request(system_cluster_id, "GET", path.lstrip("/"))
        except ConsoleAPIError as e:
            res[key] = f"查询失败: {e}"
    return res


# --------------------------------------------------------------------------
# 报告渲染
# --------------------------------------------------------------------------

def render_report(queues, cluster_states, freshness, es_health, conclusion, reasons,
                  console_url, absent_diagnosis=None, samples=None) -> str:
    L: List[str] = []
    L.append("# Console 指标写入延迟排查报告")
    L.append("")
    L.append(f"- 时间: {_now_str()}")
    L.append(f"- Console: {console_url}（全部检查经 Console 接口，无需 ES 密码）")
    L.append("")
    L.append(f"## 结论：{conclusion}")
    for r in reasons:
        L.append(f"- {r}")
    L.append("")

    L.append("## Console 本地队列（/queue/stats）")
    L.append("")
    if queues:
        L.append("| 队列 | depth | messages | 积压(lag) | 写入offset | 消费offset | 消费者 | 最后活跃 | 磁盘占用 |")
        L.append("|---|---|---|---|---|---|---|---|---|")
        for q in queues:
            lag_disp = "跨段" if q.get("multi_segment") else (
                q["lag"] if q["lag"] is not None else "-")
            flag = " ⚠️" if (("metric" in q["name"].lower()) and
                            (q.get("multi_segment") or (q["lag"] or 0) > QUEUE_LAG_WARN or
                             (isinstance(q["depth"], int) and q["depth"] > 0))) else ""
            L.append(f"| {q['name']}{flag} | {q['depth'] if q['depth'] is not None else '-'} "
                     f"| {q['messages'] if q['messages'] is not None else '-'} "
                     f"| {lag_disp} "
                     f"| {q['offset'] or '-'} | {q['consumer_offset'] or '-'} "
                     f"| {q['consumers']} | {q['last_active'] or '-'} | {q['storage'] or '-'} |")
    else:
        L.append("（未获取到队列数据）")
    L.append("")

    if samples:
        L.append("## 趋势采样（watch）")
        L.append("")
        L.append("| 时间 | 指标延迟 | metrics队列lag | metrics队列depth |")
        L.append("|---|---|---|---|")
        for smp in samples:
            L.append(f"| {smp['time']} | {_fmt_delay(smp['delay_sec'])} | {smp['lag']} | {smp['depth']} |")
        if len(samples) >= 2:
            d0, d1 = samples[0]["delay_sec"], samples[-1]["delay_sec"]
            if d0 is not None and d1 is not None:
                delta = d1 - d0
                trend = ("延迟在增长（积压恶化中）" if delta > 60 else
                         "延迟在缩小（正在追平）" if delta < -60 else "延迟基本不变（稳态积压或健康）")
                L.append("")
                L.append(f"采样区间内指标延迟变化: {delta:+d}s → **{trend}**")
        L.append("")

    if cluster_states:
        L.append("## 各集群采集状态（_collection_stats，即界面红绿灯数据源）")
        L.append("")
        L.append("注：某类型缺席（absent，工具合成标记）= 该类型已 ≥15 分钟无数据"
                 "（Console 后端查询窗口 now-15m，源码 getClusterMonitorState）；"
                 "status=warning = 5~15 分钟无数据；ok = 5 分钟内。"
                 "写入积压时文档带旧时间戳晚落库，窗口内无数据，所有集群会同时全部 absent——"
                 "这是积压的连带表现，不代表采集停止，请结合下方新鲜度与缺席诊断判读。")
        L.append("")
        for c in cluster_states:
            note = c.get("note")
            L.append(f"### {c['name']} ({c['id']}) 模式={c.get('mode') or '?'}"
                     + (f" —— {note}" if note else ""))
            L.append("")
            if not c.get("stats"):
                L.append("（无数据）")
                L.append("")
                continue
            L.append("| 指标类型 | 状态 | 延迟 |")
            L.append("|---|---|---|")
            for t, v in c["stats"].items():
                mark = " ⚠️" if (t in CORE_METRIC_NAMES and (v["delay_sec"] or 0) > DELAY_WARN_SEC) else ""
                L.append(f"| {t}{mark} | {v['status']} | {_fmt_delay(v['delay_sec'])} |")
            L.append("")

    if absent_diagnosis:
        L.append("## 缺席二次诊断（直查系统 ES 实际数据，判别 absent 成因）")
        L.append("")
        L.append("对上方报 absent 的集群，按 cluster_id 直查 `.infini_metrics-*` 里该集群"
                 "各类型的实际最新时间戳：")
        L.append("")
        L.append("- **stale**：有数据但最新时间戳超过 15 分钟 → 采集停止或写入积压（结合队列判定）")
        L.append("- **no_data**：该集群从未有过该类型文档 → 采集从未成功或 cluster_id 已变更")
        L.append("- **fresh_but_absent**：数据其实新鲜（15 分钟内），状态接口却缺席 "
                 "→ **Console 状态接口与实际数据不一致，疑似 Console bug，建议上报 INFINI Labs**")
        L.append("")
        for cid, diag in absent_diagnosis.items():
            cname = next((c["name"] for c in (cluster_states or []) if c["id"] == cid), cid)
            L.append(f"### {cname} ({cid})")
            L.append("")
            if diag.get("error"):
                L.append(f"诊断查询失败: {diag['error']}")
                L.append("")
                continue
            L.append(f"- 该集群指标文档总数: {diag.get('total_docs')}")
            L.append("")
            L.append("| 缺席类型 | 实际最新时间戳 | 延迟 | 判定 |")
            L.append("|---|---|---|---|")
            label = {"stale": "数据陈旧（采集停/积压）",
                     "no_data": "从未有数据",
                     "fresh_but_absent": "⚠️ 数据新鲜但状态缺席（疑似 Console bug）"}
            for t, info in diag.get("types", {}).items():
                L.append(f"| {t} | {info.get('max_timestamp') or '-'} "
                         f"| {_fmt_delay(info.get('delay_sec'))} "
                         f"| {label.get(info['classification'], info['classification'])} |")
            L.append("")

    if freshness and not freshness.get("error"):
        L.append("## 指标新鲜度明细（经代理查系统 ES .infini_metrics-*）")
        L.append("")
        L.append("| 指标类型 | 最新时间戳 | 延迟 |")
        L.append("|---|---|---|")
        for item in freshness.get("by_name", []):
            mark = " ⚠️" if (item["name_or_cluster"] in CORE_METRIC_NAMES and
                             (item["delay_sec"] or 0) > DELAY_WARN_SEC) else ""
            L.append(f"| {item['name_or_cluster']}{mark} | {item['max_timestamp']} "
                     f"| {_fmt_delay(item['delay_sec'])} |")
        L.append("")
        L.append(f"- 最近 5 分钟新指标文档数: {freshness.get('docs_last_5m')}（健康应 >0）")
        L.append(f"- 最近 1 小时新指标文档数: {freshness.get('docs_last_1h')}")
        L.append("")
        L.append("### 按集群")
        L.append("")
        L.append("| cluster_id | 最新时间戳 | 延迟 |")
        L.append("|---|---|---|")
        for item in freshness.get("by_cluster", []):
            L.append(f"| {item['name_or_cluster']} | {item['max_timestamp']} | {_fmt_delay(item['delay_sec'])} |")
        L.append("")
    elif freshness:
        L.append(f"## 指标新鲜度：查询失败 — {freshness.get('error')}")
        L.append("")

    if es_health:
        L.append("## 系统集群负载（积压常见根因，经 Console 代理）")
        L.append("")
        for key in ("thread_pool_write", "nodes", "metrics_indices"):
            L.append(f"### {key}")
            L.append("```")
            L.append(str(es_health.get(key)))
            L.append("```")
            L.append("")
    L.append("## 判读参考")
    L.append("")
    L.append("- metrics 队列 lag>0 持续增长 + 指标延迟大 → 写入积压：查系统 ES write 线程池 rejected、磁盘/CPU；"
             "缓解：调大采集间隔（监控设置 10s→30/60s）、减少 index_stats 范围")
    L.append("- 队列无积压但延迟大 → 采集停了：查 Console 日志 `collect ... error` / "
             "`persist ... failed` / `NOT eligible for metrics collection`")
    L.append("- absent 但二次诊断 fresh_but_absent → Console `_collection_stats` 与实际数据不一致，属于 Console bug")
    L.append("- 健康：延迟秒级、depth=0、消费 offset==写入 offset")
    return "\n".join(L)


# --------------------------------------------------------------------------
# 采集主流程
# --------------------------------------------------------------------------

def collect_once(client: ConsoleClient, system_cluster_id: str) -> Tuple[
        List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    queues, queue_err = fetch_queue_stats(client)
    if queue_err:
        print(f"⚠️  /queue/stats {queue_err}")
    try:
        clusters = client.get_clusters()
    except ConsoleAPIError as e:
        print(f"⚠️  集群列表查询失败: {e}")
        clusters = []
    states = fetch_collection_states(client, clusters) if clusters else []
    freshness = metric_freshness(client, system_cluster_id)
    es_health = es_system_health(client, system_cluster_id) if not freshness.get("error") else {}
    return queues, states, freshness, es_health


def run(client: ConsoleClient, system_cluster_id: str, out_dir: str,
        watch: bool, interval: int, duration: int) -> None:
    print("Console 登录成功")
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

    if watch:
        print(f"== watch 模式：每 {interval}s 采样一次，共 {duration}s（Ctrl+C 提前结束）==")
        samples: List[Dict[str, Any]] = []
        deadline = time.time() + duration
        while time.time() < deadline:
            queues, _, freshness, _ = collect_once(client, system_cluster_id)
            mq = next((q for q in queues if q["name"] == "metrics"), None)
            mqr = next((q for q in queues if q["name"] == "metrics_requests"), None)
            delay = freshness.get("overall") if freshness else None
            smp = {"time": _now_str(), "delay_sec": delay,
                   "lag": (mq or mqr or {}).get("lag"),
                   "depth": (mq or {}).get("depth")}
            samples.append(smp)
            print(f"   [{smp['time']}] 指标延迟={_fmt_delay(delay)}  "
                  f"metrics队列lag={smp['lag']} depth={smp['depth']}")
            if freshness and freshness.get("error"):
                print(f"   指标新鲜度查询失败: {freshness['error']}")
            time.sleep(interval)
        queues, states, freshness, es_health = collect_once(client, system_cluster_id)
        conclusion, reasons = verdict(queues, freshness)
        absent_diagnosis = diagnose_absent_clusters(client, system_cluster_id, states)
        csv_path = os_path(out_dir, f"console_delay_trend_{ts}.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["time", "delay_sec", "lag", "depth"])
            w.writeheader()
            w.writerows(samples)
        print(f"📝 趋势采样已写入: {csv_path}")
    else:
        queues, states, freshness, es_health = collect_once(client, system_cluster_id)
        conclusion, reasons = verdict(queues, freshness)
        absent_diagnosis = diagnose_absent_clusters(client, system_cluster_id, states)
        samples = None

    report = render_report(queues, states, freshness, es_health, conclusion, reasons,
                           client.base_url, absent_diagnosis, samples)
    md_path = os_path(out_dir, f"console_delay_report_{ts}.md")
    Path(md_path).write_text(report, encoding="utf-8")

    raw_snapshot = {"checked_at": _now_str(), "console_url": client.base_url,
                    "queues": queues, "cluster_states": states,
                    "freshness": freshness, "es_health": es_health,
                    "absent_diagnosis": absent_diagnosis,
                    "verdict": {"conclusion": conclusion, "reasons": reasons}}
    json_path = os_path(out_dir, f"console_delay_raw_{ts}.json")
    Path(json_path).write_text(
        json.dumps(raw_snapshot, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8")

    print()
    print(report)
    print()
    print(f"📝 报告: {md_path}")
    print(f"📝 原始数据: {json_path}")


def os_path(out_dir: str, name: str) -> str:
    return str(Path(out_dir) / name)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Console 指标写入延迟排查工具（仅 Console 权限，不需要 ES 密码）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 基础排查，出报告
  python console_queue_delay.py -c https://console:9000 -u admin -p '<密码>'

  # 趋势观察：每 10s 采样，持续 10 分钟
  python console_queue_delay.py -c ... --watch --duration 600 --interval 10

Environment Variables:
  CONSOLE_URL           Console URL (默认: http://localhost:9000)
  CONSOLE_USERNAME      用户名
  CONSOLE_PASSWORD      密码
        """,
    )
    parser = add_common_args(parser)
    parser.add_argument(
        "--system-cluster-id",
        default="",
        help="系统集群 ID（默认自动从集群列表识别，兜底 infini_default_system_cluster）")
    parser.add_argument(
        "--watch", action="store_true",
        help="持续采样观察趋势")
    parser.add_argument(
        "--interval", type=int, default=10,
        help="watch 采样间隔秒 (默认: 10)")
    parser.add_argument(
        "--duration", type=int, default=300,
        help="watch 总时长秒 (默认: 300)")
    return parser.parse_args()


def main() -> None:
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
    system_cluster_id = get_config_value(
        args.system_cluster_id, config.get('systemClusterId'),
        'CONSOLE_SYSTEM_CLUSTER_ID', '')
    output = args.output or config.get('output') or "exports"

    import getpass
    if username and not password:
        password = getpass.getpass(f"请输入 {username} 的密码: ")

    print(f"Console: {console_url}")
    client = connect_client(console_url, username, password, timeout, insecure)
    system_id = resolve_system_cluster_id(client, system_cluster_id)
    if system_cluster_id:
        print(f"系统集群 ID: {system_id}（--system-cluster-id 指定）")
    else:
        print(f"系统集群 ID: {system_id}")

    run(client, system_id, output, args.watch, args.interval, args.duration)


if __name__ == "__main__":
    try:
        # 立即输出且不缓冲：避免在管道/后台/某些终端里看不到任何输出
        try:
            sys.stdout.reconfigure(line_buffering=True)
            sys.stderr.reconfigure(line_buffering=True)
        except Exception:
            pass
        print(f"[启动] python={sys.version.split()[0]}，正在连接 Console...", flush=True)
        main()
    except SystemExit as e:
        print(f"\n[退出] {e}", flush=True)
        raise
    except KeyboardInterrupt:
        print("\n中断", flush=True)
        sys.exit(130)
    except Exception:
        import traceback
        print("\n[出错] 运行异常：", flush=True)
        traceback.print_exc()
        sys.exit(1)
