#!/usr/bin/env python3
"""INFINI Console 指标导出数据 → 分析用 CSV。

输入：metrics-exporter 导出并解压后的目录，形如
    <data_dir>/<batch_ts>/node_stats_*.jsonl     （每个批次一个目录）
    <data_dir>/alerts/alert_messages_*.jsonl
输出（-o 目录，默认 <data_dir>/analysis）：
    node_stats.csv.gz / index_stats.csv.gz / cluster_stats.csv.gz /
    cluster_health.csv.gz / alert_messages.json / alert_rules.json
    - ts 列为 epoch 秒（UTC），读取时 pd.to_datetime(ts, unit="s", utc=True)
    - 指标列说明见同目录 README.md

用法：
    python extract.py /path/to/export_dir
    python extract.py /path/to/归档 -o /tmp/out --types node_stats cluster_health
"""
import argparse
import csv
import glob
import gzip
import json
import os
import sys
from datetime import datetime
from multiprocessing import Pool

# ---------------- node_stats ----------------
NS_COLS = [
    "ts", "node",
    # counters（差分求速率）
    "s_completed", "s_rejected", "g_completed", "g_rejected",
    "w_completed", "w_rejected",
    "q_total", "q_time", "f_total", "f_time", "sc_total",
    "i_total", "i_time", "d_total",
    "r_total", "fl_total",
    "m_total", "m_time", "m_size",
    "qc_hit", "qc_miss", "qc_ev", "rc_hit", "rc_miss", "rc_ev",
    "gc_y", "gc_y_time", "gc_o", "gc_o_time",
    "io_r", "io_w", "io_rb", "io_wb", "t_tx", "t_rx",
    "br_parent", "br_fld", "br_req", "br_ifl",
    # gauges（直读）
    "cpu", "proc_cpu", "load1", "heap", "old_used", "old_max",
    "s_active", "s_queue", "s_threads",
    "g_active", "g_queue", "w_active", "w_queue",
    "http", "ctx", "seg_count", "seg_mem", "docs", "docs_del", "store",
    "m_cur", "m_cur_size", "m_cur_docs",
    "fd_mem", "qc_mem", "rc_mem",
    "fs_total", "fs_avail", "fdesc", "fdesc_max", "mem_pct",
]

def _to_epoch(ts):
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return int(datetime.fromisoformat(ts).timestamp())

def parse_node_line(line):
    try:
        d = json.loads(line)
        ns = d["payload"]["elasticsearch"]["node_stats"]
        node = d["metadata"]["labels"]["node_name"]
        t = _to_epoch(d["timestamp"])
    except Exception:
        return None
    idx = ns.get("indices") or {}
    search, indexing = idx.get("search") or {}, idx.get("indexing") or {}
    merges, docs, store = idx.get("merges") or {}, idx.get("docs") or {}, idx.get("store") or {}
    segs, qc, rc, fd = idx.get("segments") or {}, idx.get("query_cache") or {}, idx.get("request_cache") or {}, idx.get("fielddata") or {}
    tp = ns.get("thread_pool") or {}
    sp, gp, wp = tp.get("search") or {}, tp.get("get") or {}, tp.get("write") or {}
    jvm = ns.get("jvm") or {}
    jm, pools = jvm.get("mem") or {}, jvm.get("mem", {}).get("pools") or {}
    oldp = pools.get("old") or {}
    coll = (jvm.get("gc") or {}).get("collectors") or {}
    ygc, ogc = coll.get("young") or {}, coll.get("old") or {}
    osd = ns.get("os") or {}
    cpuo, mem = osd.get("cpu") or {}, osd.get("mem") or {}
    proc = ns.get("process") or {}
    fsd, fst = ns.get("fs") or {}, (ns.get("fs") or {}).get("total") or {}
    io = ((ns.get("fs") or {}).get("io_stats") or {}).get("total") or {}
    brk = ns.get("breakers") or {}
    pcpu = proc.get("cpu")
    ofd = proc.get("open_file_descriptors")
    mfd = proc.get("max_file_descriptors")  # 7.x 为 int，个别版本为 dict
    v = {
        "ts": t, "node": node,
        "s_completed": sp.get("completed"), "s_rejected": sp.get("rejected"),
        "g_completed": gp.get("completed"), "g_rejected": gp.get("rejected"),
        "w_completed": wp.get("completed"), "w_rejected": wp.get("rejected"),
        "q_total": search.get("query_total"), "q_time": search.get("query_time_in_millis"),
        "f_total": search.get("fetch_total"), "f_time": search.get("fetch_time_in_millis"),
        "sc_total": search.get("scroll_total"),
        "i_total": indexing.get("index_total"), "i_time": indexing.get("index_time_in_millis"),
        "d_total": indexing.get("delete_total"),
        "r_total": (idx.get("refresh") or {}).get("total"),
        "fl_total": (idx.get("flush") or {}).get("total"),
        "m_total": merges.get("total"), "m_time": merges.get("total_time_in_millis"),
        "m_size": merges.get("total_size_in_bytes"),
        "qc_hit": qc.get("hit_count"), "qc_miss": qc.get("miss_count"), "qc_ev": qc.get("evictions"),
        "rc_hit": rc.get("hit_count"), "rc_miss": rc.get("miss_count"), "rc_ev": rc.get("evictions"),
        "gc_y": ygc.get("collection_count"), "gc_y_time": ygc.get("collection_time_in_millis"),
        "gc_o": ogc.get("collection_count"), "gc_o_time": ogc.get("collection_time_in_millis"),
        "io_r": io.get("read_operations"), "io_w": io.get("write_operations"),
        "io_rb": io.get("read_bytes"), "io_wb": io.get("write_bytes"),
        "t_tx": (ns.get("transport") or {}).get("tx_size_in_bytes"),
        "t_rx": (ns.get("transport") or {}).get("rx_size_in_bytes"),
        "br_parent": (brk.get("parent") or {}).get("tripped"),
        "br_fld": (brk.get("fielddata") or {}).get("tripped"),
        "br_req": (brk.get("request") or {}).get("tripped"),
        "br_ifl": (brk.get("in_flight_requests") or {}).get("tripped"),
        "cpu": cpuo.get("percent"),
        "proc_cpu": pcpu.get("percent") if isinstance(pcpu, dict) else None,
        "load1": (cpuo.get("load_average") or {}).get("1m"),
        "heap": jm.get("heap_used_percent"),
        "old_used": oldp.get("used_in_bytes"), "old_max": oldp.get("max_in_bytes"),
        "s_active": sp.get("active"), "s_queue": sp.get("queue"), "s_threads": sp.get("threads"),
        "g_active": gp.get("active"), "g_queue": gp.get("queue"),
        "w_active": wp.get("active"), "w_queue": wp.get("queue"),
        "http": (ns.get("http") or {}).get("current_open"),
        "ctx": search.get("open_contexts"),
        "seg_count": segs.get("count"), "seg_mem": segs.get("memory_in_bytes"),
        "docs": docs.get("count"), "docs_del": docs.get("deleted"),
        "store": store.get("size_in_bytes"),
        "m_cur": merges.get("current"), "m_cur_size": merges.get("current_size_in_bytes"),
        "m_cur_docs": merges.get("current_docs"),
        "fd_mem": fd.get("memory_size_in_bytes"), "qc_mem": qc.get("memory_size_in_bytes"),
        "rc_mem": rc.get("memory_size_in_bytes"),
        "fs_total": fst.get("total_in_bytes"), "fs_avail": fst.get("available_in_bytes"),
        "fdesc": ofd.get("count") if isinstance(ofd, dict) else ofd,
        "fdesc_max": mfd.get("limit") if isinstance(mfd, dict) else mfd,
        "mem_pct": mem.get("used_percent"),
    }
    return [v.get(c) for c in NS_COLS]

def _parse_file(args):
    path, cols, row_fn = args
    rows, bad = [], 0
    op = gzip.open if path.endswith(".gz") else open
    with op(path, "rt") as fh:
        for line in fh:
            if not line.strip():
                continue
            r = row_fn(line)
            if r is None:
                bad += 1
                continue
            rows.append(r)
    return path, rows, bad

# ---------------- index_stats ----------------
IS_P = [("docs", "count", "p_docs"), ("docs", "deleted", "p_docs_del"),
        ("store", "size_in_bytes", "p_store"),
        ("search", "query_total", "p_q_total"), ("search", "query_time_in_millis", "p_q_time"),
        ("search", "open_contexts", "p_ctx"),
        ("indexing", "index_total", "p_i_total"), ("indexing", "index_time_in_millis", "p_i_time"),
        ("indexing", "delete_total", "p_d_total"),
        ("merges", "current", "p_m_cur"), ("merges", "current_size_in_bytes", "p_m_cur_size"),
        ("merges", "total", "p_m_total"), ("merges", "total_size_in_bytes", "p_m_size"),
        ("segments", "count", "p_seg_count"),
        ("refresh", "total", "p_r_total"), ("flush", "total", "p_fl_total"),
        ("query_cache", "evictions", "p_qc_ev"), ("fielddata", "memory_size_in_bytes", "p_fd_mem")]
IS_T = [("docs", "count", "t_docs"), ("search", "query_total", "t_q_total"),
        ("merges", "current", "t_m_cur"), ("merges", "current_size_in_bytes", "t_m_cur_size")]
IS_COLS = ["ts", "index"] + [c for _, _, c in IS_P] + [c for _, _, c in IS_T]

def parse_index_line(line):
    try:
        d = json.loads(line)
        ist = d["payload"]["elasticsearch"]["index_stats"]
        idx = d["metadata"]["labels"]["index_name"]
        t = _to_epoch(d["timestamp"])
    except Exception:
        return None
    p, tt = ist.get("primaries") or {}, ist.get("total") or {}
    v = {"ts": t, "index": idx}
    for sec, fld, col in IS_P:
        v[col] = (p.get(sec) or {}).get(fld)
    for sec, fld, col in IS_T:
        v[col] = (tt.get(sec) or {}).get(fld)
    return [v.get(c) for c in IS_COLS]

# ---------------- cluster_health / cluster_stats / alerts ----------------
CH_COLS = ["ts", "status", "n_nodes", "n_data", "active_pri", "active", "reloc",
           "init", "unassigned", "delayed_unassigned", "pending_tasks", "active_pct"]
CS_COLS = ["ts", "status", "idx_count", "docs", "deleted", "shards_total", "store_bytes",
           "n_total", "n_data", "n_master", "fs_avail", "heap_used", "heap_max", "max_uptime_ms"]

def parse_ch_line(line):
    try:
        d = json.loads(line)
        p = d["payload"]["elasticsearch"]["cluster_health"]
        return [_to_epoch(d["timestamp"]), p.get("status"), p.get("number_of_nodes"),
                p.get("number_of_data_nodes"), p.get("active_primary_shards"),
                p.get("active_shards"), p.get("relocating_shards"), p.get("initializing_shards"),
                p.get("unassigned_shards"), p.get("delayed_unassigned_shards"),
                p.get("number_of_pending_tasks"), p.get("active_shards_percent_as_number")]
    except Exception:
        return None

def parse_cs_line(line):
    try:
        d = json.loads(line)
        p = d["payload"]["elasticsearch"]["cluster_stats"]
        idx = p.get("indices") or {}
        nodes = p.get("nodes") or {}
        jvm = nodes.get("jvm") or {}
        return [_to_epoch(d["timestamp"]), p.get("status"), idx.get("count"),
                (idx.get("docs") or {}).get("count"), (idx.get("docs") or {}).get("deleted"),
                (idx.get("shards") or {}).get("total"), (idx.get("store") or {}).get("size_in_bytes"),
                (nodes.get("count") or {}).get("total"), (nodes.get("count") or {}).get("data"),
                (nodes.get("count") or {}).get("master"), (nodes.get("fs") or {}).get("available_in_bytes"),
                (jvm.get("mem") or {}).get("heap_used_in_bytes"),
                (jvm.get("mem") or {}).get("heap_max_in_bytes"), jvm.get("max_uptime_in_millis")]
    except Exception:
        return None

# ---------------- main ----------------
TYPES = {
    "node_stats":     ("node_stats_*.jsonl", NS_COLS, parse_node_line),
    "index_stats":    ("index_stats_*.jsonl", IS_COLS, parse_index_line),
    "cluster_health": ("cluster_health_*.jsonl", CH_COLS, parse_ch_line),
    "cluster_stats":  ("cluster_stats_*.jsonl", CS_COLS, parse_cs_line),
}
ALERT_TYPES = {"alert_messages", "alert_rules", "alert_history"}

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("data_dir", help="导出数据根目录（含批次子目录与 alerts/）")
    ap.add_argument("-o", "--out", default=None, help="输出目录（默认 <data_dir>/analysis）")
    ap.add_argument("--types", nargs="+", default=list(TYPES) + ["alerts"],
                    help=f"要提取的类型: {list(TYPES) + ['alerts']}")
    ap.add_argument("-j", "--jobs", type=int, default=8, help="并行进程数")
    a = ap.parse_args()
    outdir = a.out or os.path.join(a.data_dir, "analysis")
    os.makedirs(outdir, exist_ok=True)

    for typ in a.types:
        if typ == "alerts":
            for name in sorted(ALERT_TYPES):
                rows = []
                for f in sorted(glob.glob(os.path.join(a.data_dir, "alerts", f"{name}_*.jsonl"))):
                    with open(f) as fh:
                        rows += [json.loads(x) for x in fh if x.strip()]
                with open(os.path.join(outdir, f"{name}.json"), "w") as fh:
                    json.dump(rows, fh, ensure_ascii=False, indent=1)
                print(f"alerts/{name}: {len(rows)}", flush=True)
            continue
        pattern, cols, row_fn = TYPES[typ]
        files = sorted(glob.glob(os.path.join(a.data_dir, "*", pattern)))
        if not files:
            print(f"{typ}: 无文件，跳过", flush=True)
            continue
        ext = os.path.join(outdir, f"{typ}.csv.gz")
        total = bad = 0
        with Pool(a.jobs) as pool, gzip.open(ext, "wt", compresslevel=1) as out:
            w = csv.writer(out)
            w.writerow(cols)
            for path, rows, nbad in pool.imap_unordered(_parse_file,
                                                        [(f, cols, row_fn) for f in files]):
                rows.sort(key=lambda r: (r[0], r[1] if len(r) > 1 else ""))
                w.writerows(rows)
                total += len(rows)
                bad += nbad
                print(f"  {os.path.basename(path)}: {len(rows)} rows ({nbad} bad)", flush=True)
        print(f"{typ}: DONE rows={total} bad={bad} -> {ext}", flush=True)

if __name__ == "__main__":
    sys.exit(main())
