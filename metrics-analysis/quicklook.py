#!/usr/bin/env python3
"""node_stats 快速分析：差分速率日表、重启检测、节点档案、健康/告警摘要。

所有速率均为"集群每秒"口径（已实盘对账验证）：
    每 ts: 集群速率 = Σ(各节点计数器增量) / median(各节点采样间隔)
    过滤: 采样间隔 50–200s，且计数器增量 ≥0（剔除回绕）

用法：
    python quicklook.py <analysis_dir>/node_stats.csv.gz            # 日表
    python quicklook.py node_stats.csv.gz --restarts                # 重启事件
    python quicklook.py node_stats.csv.gz --profile                 # 节点档案
    python quicklook.py node_stats.csv.gz --day MM-DD               # 某天分钟级曲线摘要
    python quicklook.py node_stats.csv.gz --health analysis/cluster_health.csv.gz
    python quicklook.py node_stats.csv.gz --alerts analysis/alert_messages.json
"""
import argparse
import datetime
import sys

import pandas as pd

CST = "Asia/Shanghai"
DOW = "一二三四五六日"
COUNTERS = ["q_total", "q_time", "s_completed", "s_rejected", "i_total", "d_total",
            "gc_o", "gc_o_time", "gc_y", "io_wb", "f_total"]


def load(path, usecols=None):
    df = pd.read_csv(path, usecols=usecols, dtype={"node": "category"})
    df["ts"] = pd.to_datetime(df.ts, unit="s", utc=True).dt.tz_convert(CST)
    return df


def add_diffs(df):
    """必须先 sort_values(["node","ts"])；【禁止】之后 reset_index，
    否则布尔掩码（node 前缀过滤）会与行错位。"""
    df = df.sort_values(["node", "ts"])
    g = df.groupby("node", observed=True)
    df["dt"] = g["ts"].diff().dt.total_seconds()
    for c in COUNTERS:
        if c in df.columns:
            df[f"d_{c}"] = g[c].diff()
    return df


def valid_rows(df, drop_negative=True):
    v = df[(df.dt >= 50) & (df.dt <= 200)]
    if drop_negative:
        for c in COUNTERS:
            if f"d_{c}" in v.columns:
                v = v[v[f"d_{c}"] >= 0]
    return v


def cluster_rates(v, node_filter=None):
    """按 ts 聚合为集群每秒速率：Σ增量 / median(dt)。"""
    sub = v if node_filter is None else v[v.node.str.match(node_filter)]
    r = sub.groupby("ts").agg(dt1=("dt", "median")).reset_index()
    for c in COUNTERS:
        col = f"d_{c}"
        if col in sub.columns:
            r[col] = sub.groupby("ts")[col].sum().values
    r["qps"] = r.d_q_total / r.dt1 if "d_q_total" in r.columns else None
    r["cps"] = r.d_s_completed / r.dt1 if "d_s_completed" in r.columns else None
    r["node"] = sub.groupby("ts").node.nunique().values
    return r


def daily_table(df):
    """09–12 窗口按天：查询/协调速率、分片均耗时、active/queue/CPU、扇出。"""
    v = valid_rows(add_diffs(df))
    v["day"] = v.ts.dt.strftime("%m-%d")
    v["hour"] = v.ts.dt.hour
    w = v[v.hour.isin([9, 10, 11])]

    qd = cluster_rates(w, r"^data-")
    cd = cluster_rates(w, r"^coordinate-")
    out = pd.DataFrame({
        "查询avg_k": qd.groupby(qd.ts.dt.strftime("%m-%d")).qps.mean() / 1e3,
        "查询max_k": qd.groupby(qd.ts.dt.strftime("%m-%d")).qps.max() / 1e3,
        "协调avg_k": cd.groupby(cd.ts.dt.strftime("%m-%d")).cps.mean() / 1e3,
        "协调max_k": cd.groupby(cd.ts.dt.strftime("%m-%d")).cps.max() / 1e3,
        "n_coord": cd.groupby(cd.ts.dt.strftime("%m-%d")).node.max(),
    })
    # 分片平均耗时 = ΣΔq_time / ΣΔq_total
    qq = w[w.node.str.match(r"^data-")].groupby("day").agg(dqt=("d_q_time", "sum"),
                                                           dqn=("d_q_total", "sum"))
    out["分片ms"] = qq.dqt / qq.dqn
    g = w[w.node.str.match(r"^data-")].groupby("day").agg(
        s_act_max=("s_active", "max"), q_max=("s_queue", "max"), cpu_max=("cpu", "max"),
        heap_max=("heap", "max"))
    out = out.join(g)
    out["扇出"] = out["查询avg_k"] / out["协调avg_k"]
    out.index.name = "day"
    return out.round(2)


def restarts(df, expect_drop=1e6):
    """真重启 = 计数器大幅回绕（回绕后计数 < expect_drop）。
    Console 采集毛刺只掉几千个计数，不算重启。"""
    v = add_diffs(df)[["ts", "node", "q_total", "d_q_total", "s_completed", "d_s_completed"]]
    bad = v[((v.d_q_total < 0) & (v.q_total < expect_drop)) |
            ((v.d_s_completed < 0) & (v.s_completed < expect_drop))]
    bad = bad.sort_values("ts")
    if not len(bad):
        print("无重启事件")
        return
    print(bad.to_string(index=False))


def profile(df):
    a = df.groupby("node", observed=True).agg(
        first=("ts", "min"), last=("ts", "max"), samples=("ts", "size"),
        docs_max=("docs", "max"), store_max=("store", "max"),
        s_threads_last=("s_threads", "last"), http_max=("http", "max"), heap_max=("heap", "max"))
    a["first"] = a.first.dt.strftime("%m-%d %H:%M")
    a["last"] = a.last.dt.strftime("%m-%d %H:%M")
    print(a.sort_index().to_string())


def day_series(df, day):
    v = valid_rows(add_diffs(df))
    v = v[v.ts.dt.strftime("%m-%d") == day]
    dat = v[v.node.str.match(r"^data-")]
    qd = cluster_rates(dat)
    cd = cluster_rates(v[v.node.str.match(r"^coordinate-")])
    m = pd.DataFrame({
        "q_k": qd.qps.values / 1e3,
        "c_k": cd.set_index("ts").cps.reindex(qd.ts).values / 1e3,
        "merge_gb": dat.groupby("ts").m_cur_size.sum().reindex(qd.ts).fillna(0).values / 1e9,
    }, index=qd.ts)
    m.index = m.index.strftime("%H:%M")
    print(f"=== {day} 分钟级（5min 聚合）===")
    print(m.resample("5min").agg(
        {"q_k": ["mean", "max"], "c_k": ["mean", "max"], "merge_gb": "max"}).round(2).to_string())


def health(path):
    ch = load(path)
    ch["day"] = ch.ts.dt.strftime("%m-%d")
    print("status 分布:", ch.status.value_counts().to_dict())
    ng = ch[ch.status != "green"]
    if len(ng):
        print("非 green 样本:")
        print(ng.to_string(index=False))
    base = ch.n_nodes.mode()[0]
    odd = ch[ch.n_nodes != base]
    print(f"\n节点数变化（基线 {base}）: {len(odd)} 个样本")
    if len(odd):
        print(odd.set_index("ts").resample("5min").last().dropna().head(30).to_string())


def alerts(path, cluster_marker=None):
    import json
    from collections import Counter
    msgs = json.load(open(path))
    days = Counter()
    for m in msgs:
        try:
            days[pd.to_datetime(m["created"]).tz_localize(None).strftime("%m-%d")] += 1
        except Exception:
            pass
    print(f"告警消息 {len(msgs)} 条；首条 {min(days)}，末条 {max(days)}")
    print("按天:", dict(sorted(days.items())))
    if cluster_marker:
        hit = [m for m in msgs if cluster_marker in json.dumps(m, ensure_ascii=False)]
        print(f"匹配 '{cluster_marker}' 的消息 {len(hit)} 条")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("node_stats", help="extract.py 产出的 node_stats.csv.gz")
    ap.add_argument("--restarts", action="store_true", help="重启检测（计数器大幅回绕）")
    ap.add_argument("--profile", action="store_true", help="节点档案")
    ap.add_argument("--day", help="某天分钟级曲线（如 10-08）")
    ap.add_argument("--health", help="同时分析 cluster_health.csv.gz")
    ap.add_argument("--alerts", help="同时分析 alert_messages.json（可配 --marker）")
    ap.add_argument("--marker", help="集群标识（cluster_id 或 IP 段）")
    a = ap.parse_args()

    use = ["ts", "node", "s_completed", "q_total", "q_time", "s_active", "s_queue",
           "s_threads", "cpu", "heap", "http", "docs", "store", "m_cur_size"]
    df = load(a.node_stats, usecols=use)
    if "m_cur_size" not in df.columns:
        df["m_cur_size"] = float("nan")

    if a.restarts:
        restarts(df)
    if a.profile:
        profile(df)
    if a.day:
        day_series(df, a.day)
    if a.health:
        health(a.health)
    if a.alerts:
        alerts(a.alerts, a.marker)
    if not (a.restarts or a.profile or a.day or a.health or a.alerts):
        print(daily_table(df).to_string())


if __name__ == "__main__":
    sys.exit(main())
