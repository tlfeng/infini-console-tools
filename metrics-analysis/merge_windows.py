#!/usr/bin/env python3
"""合并窗口与事件段分析：每天合并从几点到几点、大体积合并的完整起止。

口径：
    - 节点级 merges.current_size 集群合计，≥ --thr GB（默认 1）视为"在合并"
    - 段间隔 ≤ --gap 分钟（默认 10）并成一段
    - "大体积段"默认阈值 --big 5 GB

用法：
    python merge_windows.py node_stats.csv.gz                     # 日窗口 + 大段
    python merge_windows.py node_stats.csv.gz --big 10            # 只看 ≥10GB
    python merge_windows.py node_stats.csv.gz -o out/             # 同时落 CSV
"""
import argparse
import os
import sys

import pandas as pd

CST = "Asia/Shanghai"


def load_merge_series(path, cols=("ts", "node", "m_cur", "m_cur_size")):
    df = pd.read_csv(path, usecols=list(cols), dtype={"node": "category"})
    df["ts"] = pd.to_datetime(df.ts, unit="s", utc=True).dt.tz_convert(CST)
    dat = df[df.node.str.startswith("data-")]
    s = dat.groupby("ts").agg(gb=("m_cur_size", lambda x: x.sum() / 1e9),
                              tasks=("m_cur", "sum"))
    return s.sort_index()


def episodes(s, thr_gb, gap_min=10):
    """连续 ≥thr 的采样为一段；段间隔 ≤gap_min 合并。返回 [(start,end,peak,peak_ts,max_tasks)]。"""
    act = s[s.gb >= thr_gb]
    if not len(act):
        return []
    eps, start, prev = [], act.index[0], act.index[0]
    peak, pts, tasks = act.iloc[0].gb, act.index[0], act.iloc[0].tasks
    for ts, r in act.iloc[1:].iterrows():
        if (ts - prev).total_seconds() <= gap_min * 60:
            if r.gb > peak:
                peak, pts = r.gb, ts
            tasks = max(tasks, r.tasks)
        else:
            eps.append((start, prev, peak, pts, tasks))
            start, peak, pts, tasks = ts, r.gb, ts, r.tasks
        prev = ts
    eps.append((start, prev, peak, pts, tasks))
    return eps


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("node_stats", help="extract.py 产出的 node_stats.csv.gz")
    ap.add_argument("--thr", type=float, default=1.0, help="在合并阈值 GB（默认 1）")
    ap.add_argument("--big", type=float, default=5.0, help="大体积段阈值 GB（默认 5）")
    ap.add_argument("--gap", type=float, default=10, help="段合并间隔分钟（默认 10）")
    ap.add_argument("-o", "--out", help="输出目录（写 daily_merge_windows.csv / big_merge_episodes.csv）")
    a = ap.parse_args()

    s = load_merge_series(a.node_stats)
    s["day"] = s.index.strftime("%m-%d")

    rows, big = [], []
    for d, dd in s.groupby("day"):
        eps = episodes(dd, a.thr, a.gap)
        if not eps:
            rows.append((d, "-", "-", 0, 0, 0, ""))
            continue
        active_min = sum((e - st).total_seconds() / 60 for st, e, *_ in eps)
        rows.append((d, eps[0][0].strftime("%H:%M"), eps[-1][1].strftime("%H:%M"),
                     round(active_min), len(eps), sum(1 for e in eps if e[2] >= a.big),
                     "全天合并" if active_min > 1400 else ""))
        for st, en, pk, pt, tk in episodes(dd, a.big, a.gap):
            big.append((d, st, en, (en - st).total_seconds() / 60, round(pk, 1),
                        pt.strftime("%H:%M"), int(tk)))

    w = pd.DataFrame(rows, columns=["日期", "首段开始", "末段结束", "活跃分钟", "段数", f"其中≥{a.big:g}GB", "备注"])
    print(f"=== 每日合并窗口（合计 ≥{a.thr:g}GB）===")
    print(w.to_string(index=False))

    b = pd.DataFrame(big, columns=["日期", "开始", "结束", "时长min", "峰值GB", "峰值时刻", "并发max"])
    b = b[b["峰值GB"] >= a.big].sort_values(["日期", "开始"])
    print(f"\n=== 大体积合并段（峰值 ≥{a.big:g}GB，共 {len(b)} 段）===")
    print(b.to_string(index=False))

    if a.out:
        os.makedirs(a.out, exist_ok=True)
        w.to_csv(os.path.join(a.out, "daily_merge_windows.csv"), index=False)
        b.to_csv(os.path.join(a.out, "big_merge_episodes.csv"), index=False)
        print(f"\nsaved -> {a.out}/daily_merge_windows.csv, big_merge_episodes.csv")


if __name__ == "__main__":
    sys.exit(main())
