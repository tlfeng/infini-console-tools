#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hot Threads Collector - 定时抓取 Elasticsearch hot threads

通过 INFINI Console 的 _proxy 接口调用 ES:
- GET /_nodes/hot_threads
- 或 GET /_nodes/{node_id}/hot_threads

每次抓取会写入一行 JSONL，包含：
- 抓取时间
- 请求耗时
- 请求参数
- hot threads 原始结果
"""

import argparse
import getpass
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).parent.parent))
from common.config import add_common_args, get_config_value, load_and_merge_config
from common.console_client import ConsoleAPIError, ConsoleAuthError, ConsoleClient


def build_hot_threads_path(args: argparse.Namespace) -> str:
    """构建 hot threads API 路径"""
    if args.node_id:
        base_path = f"/_nodes/{args.node_id}/hot_threads"
    else:
        base_path = "/_nodes/hot_threads"

    params: Dict[str, Any] = {
        "threads": args.threads,
        "snapshots": args.snapshots,
        "ignore_idle_threads": str(args.ignore_idle_threads).lower(),
        "type": args.type,
    }

    if args.sample_interval:
        params["interval"] = args.sample_interval
    if args.api_timeout:
        params["timeout"] = args.api_timeout

    return f"{base_path}?{urlencode(params)}"


def choose_cluster_id(client: ConsoleClient, cluster_id: str, cluster_name: str) -> str:
    """根据参数选择目标集群"""
    if cluster_id:
        return cluster_id

    clusters = client.get_clusters()
    if not clusters:
        raise ConsoleAPIError("未获取到任何集群")

    available = [
        c for c in clusters
        if not ConsoleClient.is_system_cluster(c.get("id", ""), c.get("name", ""))
    ]

    if cluster_name:
        matched = [c for c in available if c.get("name") == cluster_name]
        if len(matched) == 1:
            return matched[0].get("id", "")
        if len(matched) > 1:
            raise ConsoleAPIError(f"匹配到多个同名集群: {cluster_name}，请改用 --cluster-id")
        raise ConsoleAPIError(f"未找到指定集群名: {cluster_name}")

    if len(available) == 1:
        return available[0].get("id", "")

    choices = [f"{c.get('name', 'Unknown')} ({c.get('id', '')})" for c in available]
    raise ConsoleAPIError(
        "存在多个可用集群，请指定 --cluster-id 或 --cluster-name。可选集群:\n"
        + "\n".join(choices)
    )


def append_record(output_file: Path, record: Dict[str, Any]) -> None:
    """追加写入 JSONL 记录"""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="定时调用 ES hot threads API 并写入 JSONL 文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 每 10 秒抓取一次，持续抓取
  python hot_threads_collector.py -c http://localhost:9000 -u admin -p password \
    --cluster-id CLUSTER_ID --poll-interval 10

  # 仅抓取 30 次，并指定输出文件
  python hot_threads_collector.py -c http://localhost:9000 -u admin -p password \
    --cluster-id CLUSTER_ID --poll-interval 5 --count 30 -o ./exports/hot_threads.jsonl

  # 指定节点与 hot threads 参数
  python hot_threads_collector.py -c http://localhost:9000 -u admin -p password \
    --cluster-id CLUSTER_ID --node-id node-1 --threads 10 --snapshots 20 --type cpu
""",
    )

    add_common_args(parser)

    target_group = parser.add_argument_group("目标集群参数")
    target_group.add_argument("--cluster-id", default="", help="目标集群 ID")
    target_group.add_argument("--cluster-name", default="", help="目标集群名称（可选）")
    target_group.add_argument("--node-id", default="", help="节点 ID/名称，逗号分隔；为空表示全节点")

    collect_group = parser.add_argument_group("采集参数")
    collect_group.add_argument(
        "--poll-interval",
        type=float,
        default=10.0,
        help="抓取间隔(秒)，默认 10",
    )
    collect_group.add_argument(
        "--duration-minutes",
        type=float,
        default=0,
        help="运行时长(分钟)，0 表示不限制",
    )
    collect_group.add_argument(
        "--count",
        type=int,
        default=0,
        help="抓取次数，0 表示持续抓取直到手动停止",
    )
    collect_group.add_argument(
        "--retries",
        type=int,
        default=2,
        help="单次失败后的重试次数，默认 2",
    )
    collect_group.add_argument(
        "--retry-delay",
        type=float,
        default=1.0,
        help="重试间隔(秒)，默认 1",
    )

    api_group = parser.add_argument_group("hot threads API 参数")
    api_group.add_argument("--threads", type=int, default=3, help="返回线程数，默认 3")
    api_group.add_argument("--snapshots", type=int, default=10, help="采样次数，默认 10")
    api_group.add_argument(
        "--sample-interval",
        default="",
        help="hot threads 采样间隔，例如 500ms, 1s",
    )
    api_group.add_argument(
        "--ignore-idle-threads",
        dest="ignore_idle_threads",
        action="store_true",
        default=True,
        help="忽略空闲线程（默认开启）",
    )
    api_group.add_argument(
        "--include-idle-threads",
        dest="ignore_idle_threads",
        action="store_false",
        help="包含空闲线程",
    )
    api_group.add_argument(
        "--type",
        choices=["cpu", "wait", "block"],
        default="cpu",
        help="采样类型，默认 cpu",
    )
    api_group.add_argument(
        "--api-timeout",
        default="",
        help="hot threads API 超时，例如 30s",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config, _ = load_and_merge_config(args)

    auth_config = config.get("auth", {}) if isinstance(config, dict) else {}

    console_url = get_config_value(
        args.console,
        config.get("consoleUrl") if isinstance(config, dict) else None,
        "CONSOLE_URL",
        "http://localhost:9000",
    )
    username = get_config_value(
        args.username,
        auth_config.get("username") if isinstance(auth_config, dict) else None,
        "CONSOLE_USERNAME",
        "",
    )
    password = get_config_value(
        args.password,
        auth_config.get("password") if isinstance(auth_config, dict) else None,
        "CONSOLE_PASSWORD",
        "",
    )
    timeout = int(
        get_config_value(
            str(args.timeout),
            str(config.get("timeout", args.timeout)) if isinstance(config, dict) else str(args.timeout),
            "CONSOLE_TIMEOUT",
            str(args.timeout),
        )
    )
    insecure = args.insecure or (config.get("insecure", False) if isinstance(config, dict) else False)

    if username and not password:
        password = getpass.getpass("Password: ")

    if args.poll_interval <= 0:
        print("错误: --poll-interval 必须大于 0")
        return 1
    if args.duration_minutes < 0:
        print("错误: --duration-minutes 不能小于 0")
        return 1
    if args.count < 0:
        print("错误: --count 不能小于 0")
        return 1
    if args.retries < 0:
        print("错误: --retries 不能小于 0")
        return 1
    if args.retry_delay < 0:
        print("错误: --retry-delay 不能小于 0")
        return 1

    output_arg = args.output or ""
    if not output_arg:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_arg = os.path.join("exports", f"hot_threads_{timestamp}.jsonl")
    output_file = Path(output_arg)

    client = ConsoleClient(
        base_url=console_url,
        username=username,
        password=password,
        timeout=timeout,
        verify_ssl=not insecure,
    )

    try:
        if username and password:
            if not client.login():
                print("认证失败：请检查用户名或密码")
                return 1

        target_cluster_id = choose_cluster_id(client, args.cluster_id, args.cluster_name)
        api_path = build_hot_threads_path(args)

        print(f"目标集群: {target_cluster_id}")
        print(f"请求路径: {api_path}")
        print(f"输出文件: {output_file}")
        if args.count > 0:
            print(f"抓取次数: {args.count} 次")
        else:
            print("抓取次数: 持续抓取（按 Ctrl+C 停止）")
        if args.duration_minutes > 0:
            print(f"运行时长: {args.duration_minutes} 分钟")
        else:
            print("运行时长: 不限制")
        print(f"抓取间隔: {args.poll_interval} 秒")
        print(f"失败重试: {args.retries} 次，重试间隔 {args.retry_delay} 秒")
        print("开始抓取...\n")

        iteration = 0
        success_count = 0
        failure_count = 0
        start_monotonic = time.monotonic()
        deadline = (
            start_monotonic + args.duration_minutes * 60
            if args.duration_minutes > 0
            else None
        )
        next_tick = start_monotonic

        while True:
            now_monotonic = time.monotonic()
            if deadline is not None and now_monotonic >= deadline:
                break
            if args.count > 0 and iteration >= args.count:
                break

            iteration += 1
            attempt = 0
            done = False
            while attempt <= args.retries and not done:
                attempt += 1
                start = time.time()
                fetch_time = datetime.now(timezone.utc).isoformat()

                try:
                    result = client.proxy_request(target_cluster_id, "GET", api_path)
                    elapsed_ms = round((time.time() - start) * 1000, 2)

                    record = {
                        "sequence": iteration,
                        "attempt": attempt,
                        "fetch_time": fetch_time,
                        "elapsed_ms": elapsed_ms,
                        "cluster_id": target_cluster_id,
                        "api_path": api_path,
                        "response": result,
                    }
                    append_record(output_file, record)
                    success_count += 1
                    done = True

                    print(
                        f"[{iteration}] 抓取成功 "
                        f"attempt={attempt} time={fetch_time} elapsed={elapsed_ms}ms"
                    )
                except Exception as exc:
                    elapsed_ms = round((time.time() - start) * 1000, 2)
                    record = {
                        "sequence": iteration,
                        "attempt": attempt,
                        "fetch_time": fetch_time,
                        "elapsed_ms": elapsed_ms,
                        "cluster_id": target_cluster_id,
                        "api_path": api_path,
                        "error": str(exc),
                    }
                    append_record(output_file, record)

                    if attempt <= args.retries:
                        print(
                            f"[{iteration}] 抓取失败，准备重试 "
                            f"attempt={attempt}/{args.retries + 1} elapsed={elapsed_ms}ms error={exc}"
                        )
                        if args.retry_delay > 0:
                            time.sleep(args.retry_delay)
                    else:
                        failure_count += 1
                        done = True
                        print(
                            f"[{iteration}] 抓取失败，达到最大重试次数 "
                            f"attempt={attempt}/{args.retries + 1} elapsed={elapsed_ms}ms error={exc}"
                        )

            if args.count > 0 and iteration >= args.count:
                break
            if deadline is not None and time.monotonic() >= deadline:
                break

            # 使用固定节拍，避免循环耗时导致采集间隔漂移
            next_tick += args.poll_interval
            sleep_seconds = next_tick - time.monotonic()
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
            else:
                # 如果当前任务执行超时，重置节拍到当前时间
                next_tick = time.monotonic()

        print("\n抓取结束")
        print(f"结果文件: {output_file}")
        print(
            f"汇总: total={iteration}, success={success_count}, failed={failure_count}, "
            f"retries={args.retries}"
        )
        return 0

    except KeyboardInterrupt:
        print("\n收到中断信号，已停止抓取")
        print(f"结果文件: {output_file}")
        return 0
    except ConsoleAuthError as exc:
        print(f"认证异常: {exc}")
        return 1
    except ConsoleAPIError as exc:
        print(f"API 异常: {exc}")
        return 1
    except Exception as exc:
        print(f"运行异常: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
