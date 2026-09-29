#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Console 连接诊断脚本（自包含，无第三方依赖）

用途：定位 WinError 10054（远程主机强迫关闭了一个现有的连接）这类
连接被重置（RST）问题，并判断责任方：
  - 服务端/网关短时抖动导致的间歇性重置
  - 按接口/方法/内容被中间设备（WAF/IPS）确定性拦截
  - 接口自身的问题（HTTP 4xx/5xx，与链路无关）
  - 只有「每请求新建连接」才失败（accept 队列/并发连接限制/新连接策略）

关键设计：同一个请求重复执行多次（默认 5 次），并把「每次新建连接」与
「同一条 keep-alive 连接上连打」两种形态分开统计。只有这样才能区分
「这个接口必然失败」和「偶发失败」——单次探测很容易得出错误结论。

用法：
  python console_diag.py -c metrics_export_4windows.json
  python console_diag.py -u https://10.139.130.34:7474 -U admin -P '密码'
  python console_diag.py -u https://10.139.130.34:7474 -U admin --repeat 10
  python console_diag.py -u https://10.139.130.34:7474 -U admin --body-test 3  # 追加 body 敏感性测试

输出说明：
  [HTTP xxx]   服务端正常返回了 HTTP 状态码（2xx/4xx/5xx 都说明链路通）
  [RST]        连接被重置（无 HTTP 响应）——就是导出工具报的 WinError 10054
  [TIMEOUT]    连接或响应超时
  [TLS中断]    连接在 HTTP 响应前被掐断，TLS 层先报错（同样是链路被中断）
"""

import argparse
import getpass
import hashlib
import hmac
import http.client
import json
import ssl
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def build_ssl_context(insecure: bool = True) -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    if insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def classify(exc: BaseException) -> str:
    """把请求异常归类为简短标记"""
    if isinstance(exc, urllib.error.HTTPError):
        return f"HTTP {exc.code}"
    s = str(exc)
    if isinstance(exc, (ConnectionResetError, ConnectionAbortedError)):
        return "RST(连接被重置)"
    if "10054" in s or "10053" in s or "forcibly closed" in s or "远程主机强迫关闭" in s:
        return "RST(连接被重置)"
    if isinstance(exc, TimeoutError) or "timed out" in s or "timeout" in s.lower():
        return "TIMEOUT"
    if isinstance(exc, ssl.SSLError):
        return "TLS中断(响应前连接被掐断)"
    if "EOF" in s or "RemoteDisconnected" in type(exc).__name__ or "Connection closed" in s:
        return "EOF(服务端未返回响应即断开)"
    return f"{type(exc).__name__}: {s}"


def is_http_ok(tag: str) -> bool:
    return tag.startswith("HTTP ")


def http_status(tag: str):
    """从 'HTTP 503' 里取出状态码"""
    if not tag.startswith("HTTP "):
        return None
    try:
        return int(tag.split()[1])
    except (IndexError, ValueError):
        return None


class Diag:
    def __init__(self, base_url, username, password, timeout=30, insecure=True, repeat=5):
        self.base = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout = timeout
        self.repeat = max(1, repeat)
        self.ctx = build_ssl_context(insecure)
        self.token = None
        self.results = []             # [(步骤名, 标记, 备注)]
        self.search_new_conn = []     # 集群搜索：每次新建连接的结果
        self.search_keepalive = []    # 集群搜索：同一条连接上连打的结果

    # ---------- 基础请求 ----------

    def request(self, method, path, body=None, auth=True, headers=None):
        """返回 (标记, 详情dict)。标记为 'HTTP xxx' 或错误分类。"""
        if body is not None and not isinstance(body, (bytes, str)):
            body = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(self.base + path, data=body, method=method)
        req.add_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        if auth and self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        t0 = time.time()
        try:
            with urllib.request.urlopen(req, context=self.ctx, timeout=self.timeout) as resp:
                raw = resp.read()
                dt = time.time() - t0
                try:
                    parsed = json.loads(raw.decode("utf-8", "replace")) if raw else {}
                except json.JSONDecodeError:
                    parsed = {"_raw": raw[:120].decode("utf-8", "replace")}
                return f"HTTP {resp.status}", {"body": parsed, "sec": dt, "len": len(raw)}
        except urllib.error.HTTPError as e:
            body_txt = e.read().decode("utf-8", "replace")
            try:
                parsed = json.loads(body_txt)
            except json.JSONDecodeError:
                parsed = {"_raw": body_txt[:200]}
            return f"HTTP {e.code}", {"body": parsed, "sec": time.time() - t0}
        except Exception as e:
            return classify(e), {"err": str(e), "sec": time.time() - t0}

    def step(self, name, method, path, body=None, auth=True, note=""):
        tag, info = self.request(method, path, body, auth)
        self.print_step(f"{method} {path}", tag, info, note)
        self.results.append((name, tag, note))
        return tag, info

    @staticmethod
    def print_step(summary, tag, info, note=""):
        print(f"  [{tag}] {summary}  ({info.get('sec', 0):.2f}s)")
        if note:
            print(f"         {note}")
        if is_http_ok(tag):
            preview = json.dumps(info.get("body", {}), ensure_ascii=False)[:160]
            print(f"         响应: {preview}")
        else:
            print(f"         !!! 链路异常: {info.get('err', '')}")

    def body_sensitivity_test(self, rounds: int):
        """交替发送 空{} 与 导出工具的body，统计两边命中率，判断 RST 是否由 body 决定"""
        print(f"\n== 附加: body 敏感性测试（各 {rounds} 轮交替）")
        exporter_body = json.dumps({"size": 1000, "query": {"match_all": {}}}).encode("utf-8")
        tally = {"空body{}": {"HTTP": 0, "异常": 0}, "导出body": {"HTTP": 0, "异常": 0}}
        for i in range(rounds):
            for label, body in (("空body{}", b"{}"), ("导出body", exporter_body)):
                tag, info = self.request("POST", "/elasticsearch/_search", body)
                bucket = "HTTP" if is_http_ok(tag) else "异常"
                tally[label][bucket] += 1
                mark = "" if bucket == "HTTP" else f"  <-- {tag}"
                print(f"  第{i + 1}轮 [{label}] -> {tag}{mark}")
                time.sleep(0.3)
        print("\n  统计:")
        for label, t in tally.items():
            print(f"    {label}: HTTP响应 {t['HTTP']} 次, 连接异常 {t['异常']} 次")
        empty_all_bad = tally["空body{}"]["异常"] == rounds and tally["导出body"]["异常"] == 0
        both_flaky = tally["空body{}"]["异常"] > 0 and tally["导出body"]["异常"] > 0
        if empty_all_bad:
            print("  >>> 空 body {} 稳定触发 RST、带 body 稳定正常：确认为 body 触发条件，")
            print("      客户端规避空 body 即可绕过（新版客户端已不再向该接口发 body）。")
        elif both_flaky:
            print("  >>> 两种 body 都出现异常：RST 与 body 内容无关，是环境间歇性问题，")
            print("      依赖重试 + 降级兜底，并按汇总建议查服务端 panic 日志 / 中间设备。")
        else:
            print("  >>> 差异不明显或样本不足，可加大 --body-test 轮数再试。")

    # ---------- 同一条 keep-alive 连接上连打 ----------

    def _new_raw_connection(self):
        parts = urllib.parse.urlsplit(self.base)
        host = parts.hostname
        port = parts.port or (443 if parts.scheme == "https" else 80)
        if parts.scheme == "https":
            return http.client.HTTPSConnection(host, port, timeout=self.timeout, context=self.ctx)
        return http.client.HTTPConnection(host, port, timeout=self.timeout)

    def repeat_on_one_connection(self, method, path, body=None, count=None):
        """在一条复用连接上连续发 count 次，返回 ([(tag, 耗时)], 重连次数)"""
        count = count or self.repeat
        headers = {"Content-Type": "application/json", "Accept-Encoding": "identity"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        results = []
        conn = self._new_raw_connection()
        reconnects = 0
        for _ in range(count):
            t0 = time.time()
            try:
                conn.request(method, path, body=body, headers=headers)
                resp = conn.getresponse()
                resp.read()
                results.append((f"HTTP {resp.status}", time.time() - t0))
                if resp.will_close:
                    conn.close()
                    conn = self._new_raw_connection()
                    reconnects += 1
            except Exception as e:
                results.append((classify(e), time.time() - t0))
                try:
                    conn.close()
                except Exception:
                    pass
                conn = self._new_raw_connection()
                reconnects += 1
        try:
            conn.close()
        except Exception:
            pass
        return results, reconnects

    # ---------- 登录 ----------

    def login(self):
        print("== 步骤 1: 登录（挑战-响应优先，失败回退明文）")
        # ---- 挑战-响应（Console 1.31+）----
        tag, info = self.request("POST", "/account/login/challenge", {"username": self.username}, auth=False)
        ch = info.get("body", {})
        if tag == "HTTP 200" and isinstance(ch, dict) and ch.get("method") == "challenge":
            verifier = hashlib.pbkdf2_hmac(
                "sha256", self.password.encode("utf-8"),
                ch["salt"].encode("utf-8"), ch["iterations"], dklen=32)
            msg = f"{self.username}:{ch['challenge_id']}:{ch['nonce']}".encode("utf-8")
            proof = hmac.new(verifier, msg, hashlib.sha256).hexdigest()
            _, rn = self.request("POST", "/account/replay_nonce",
                                 {"method": "POST", "path": "/account/login"}, auth=False)
            nonce = rn.get("body", {}).get("nonce") if isinstance(rn.get("body"), dict) else None
            headers = {"X-Request-Nonce": nonce} if nonce else None
            tag, info = self.request("POST", "/account/login", {
                "userName": self.username, "type": "account",
                "challenge_id": ch["challenge_id"], "proof": proof,
            }, auth=False, headers=headers)
            token = self._extract_token(info.get("body"))
            if token:
                self.token = token
                print("  [HTTP 200] 挑战-响应登录成功 (method=challenge)")
                return "challenge"
            print(f"  [挑战登录未取到 token: {tag}]，回退明文登录")
        else:
            print(f"  [挑战接口不可用: {tag}]，回退明文登录（旧版本行为）")

        # ---- 明文登录（旧版本）----
        tag, info = self.request("POST", "/account/login",
                                 {"username": self.username, "password": self.password}, auth=False)
        token = self._extract_token(info.get("body"))
        if token:
            self.token = token
            print(f"  [{tag}] 明文登录成功 (method=plaintext)")
            return "plaintext"
        print(f"  [{tag}] 登录失败，响应: {json.dumps(info.get('body', {}), ensure_ascii=False)[:200]}")
        return None

    @staticmethod
    def _extract_token(result):
        if not isinstance(result, dict):
            return None
        token = result.get("token") or result.get("access_token")
        if not token and isinstance(result.get("data"), dict):
            token = result["data"].get("token") or result["data"].get("access_token")
        return token

    # ---------- 主流程 ----------

    def run(self, body_test_rounds: int = 0):
        method = self.login()
        if not self.token:
            print("\n登录失败，后续步骤需要 token，终止。请检查账号密码。")
            return
        print(f"\n  登录方式: {method}")
        print(f"  Token（可用于手工 curl 复测）:\n  {self.token}\n")

        print("== 步骤 2: Console 版本信息（与本地 console 源码 git log 对比 build_hash）")
        tag, info = self.step("_info", "GET", "/_info",
                              note="部署版本的 build_hash 应能对应到 console 仓库的某个提交")
        if tag == "HTTP 200":
            b = info.get("body", {})
            app = b.get("application", b)
            ver = app.get("version", {})
            flat = {k: ver.get(k) for k in ("number", "build_hash", "framework_hash") if ver.get(k)}
            print(f"  >>> Console 版本: {json.dumps(flat or ver, ensure_ascii=False)}")

        print("\n== 步骤 3: 基准接口（不涉及集群搜索）")
        self.step("集群状态", "GET", "/elasticsearch/status",
                  note="若此步也失败：偏向网络路径或服务端整体异常，而不是某个接口被拦")
        self.step("账号信息", "GET", "/account/profile")

        print(f"\n== 步骤 4: 集群搜索（重复 {self.repeat} 次，每次新建连接，与导出工具一致）")
        # 注意：Console 只认 URL 查询参数，POST body 里的 size/from 会被忽略，
        # 所以这里用查询串（新客户端就是这么发的）
        path = "/elasticsearch/_search?size=1000&from=0"
        for index in range(self.repeat):
            tag, info = self.request("GET", path)
            self.search_new_conn.append((tag, info.get("sec", 0)))
            print(f"  [{tag}] 第 {index + 1}/{self.repeat} 次  ({info.get('sec', 0):.2f}s)")
            if not is_http_ok(tag):
                print(f"         !!! {info.get('err', '')}")
        ok_count = sum(1 for tag, _ in self.search_new_conn if is_http_ok(tag))
        print(f"  >>> 每请求新建连接: {ok_count}/{self.repeat} 成功")

        print(f"\n== 步骤 4b: 同一条 keep-alive 连接上连打 {self.repeat} 次")
        ka_results, reconnects = self.repeat_on_one_connection("GET", path)
        self.search_keepalive = ka_results
        for index, (tag, sec) in enumerate(ka_results):
            print(f"  [{tag}] 第 {index + 1}/{len(ka_results)} 次  ({sec:.2f}s)")
        ka_ok = sum(1 for tag, _ in ka_results if is_http_ok(tag))
        print(f"  >>> 单连接复用: {ka_ok}/{len(ka_results)} 成功，重连 {reconnects} 次")

        print("\n== 步骤 4c: POST 方法对照（同一查询串，body 会被 Console 忽略）")
        self.step("集群搜索-POST", "POST", path, b"{}",
                  note="GET 与 POST 表现不一致才可能是按方法拦截")

        print("\n== 步骤 5: _proxy 通道（导出工具后续依赖它查指标）")
        self.step("_proxy 系统集群", "POST",
                  "/elasticsearch/infini_default_system_cluster/_proxy?method=GET&path=/",
                  note="若此步失败：即使集群搜索修好，导出也会卡在这里")

        if body_test_rounds > 0:
            self.body_sensitivity_test(body_test_rounds)

        self.summarize(method)

    # ---------- 结论 ----------

    def summarize(self, login_method):
        print("\n" + "=" * 62)
        print("结果汇总与建议")
        print("=" * 62)
        print(f"登录方式: {login_method}")

        all_tags = [tag for _, tag, _ in self.results]
        failed_steps = [(name, tag) for name, tag, _ in self.results if not is_http_ok(tag)]
        new_conn_ok = sum(1 for tag, _ in self.search_new_conn if is_http_ok(tag))
        new_conn_total = len(self.search_new_conn)
        ka_ok = sum(1 for tag, _ in self.search_keepalive if is_http_ok(tag))
        ka_total = len(self.search_keepalive)

        self._print_timing("集群搜索（每次新建连接）", self.search_new_conn)
        self._print_timing("集群搜索（单连接复用）", self.search_keepalive)

        clean = (not failed_steps and new_conn_total and ka_total
                 and new_conn_ok == new_conn_total and ka_ok == ka_total)
        if clean:
            print("所有步骤均收到 HTTP 响应，本次未复现链路异常。")
            print("若导出工具仍报 WinError 10054：属于间歇性故障，用 --repeat 加大次数复现，")
            print("并比较「工具失败时刻」与「本次成功时刻」是否只差几分钟（抖动窗口）。")
            print("  1) 把本脚本放到 Console 宿主机上跑（-u https://127.0.0.1:<端口>）：")
            print("     本机也偶发 → Console/宿主机侧；本机一直正常 → 网络路径或客户端侧。")
            print("  2) 抓包看 RST 的来源（Wireshark 过滤 tcp.flags.reset == 1）：")
            print("     RST 报文的 TTL 与正常报文不同 → 中间设备伪造的 RST。")
            print("  3) 提高客户端重试预算（导出工具已支持）: set CONSOLE_MAX_ATTEMPTS=8")
            return

        if failed_steps or new_conn_total - new_conn_ok or ka_total - ka_ok:
            print("\n异常步骤:")
            for name, tag in failed_steps:
                print(f"  - {name}: {tag}")
            if new_conn_total - new_conn_ok:
                print(f"  - 集群搜索(每请求新建连接): {new_conn_total - new_conn_ok}/{new_conn_total} 次失败")
            if ka_total - ka_ok:
                print(f"  - 集群搜索(单连接复用): {ka_total - ka_ok}/{ka_total} 次失败")

        # 只要所有失败都带着 HTTP 状态码，就说明链路是通的
        link_errors = [tag for tag in all_tags if not is_http_ok(tag)]
        error_codes = [http_status(tag) for tag in all_tags if http_status(tag)]
        if not link_errors and any(code >= 400 for code in error_codes):
            print()
            print("- 服务端都返回了 HTTP 状态码，说明链路是通的，属于接口/权限/版本问题：")
            print("  1) 401/403：账号权限被 Console 权限模型拒绝；")
            print("  2) 404：该接口在此 Console 版本上不存在（升级/降级引起）；")
            print("  3) 5xx：看 Console 日志 grep -iE 'panic|error' <console>/log/console/nodes/*/console.log")
            return

        if new_conn_total and ka_total:
            new_conn_rate = new_conn_ok / new_conn_total
            ka_rate = ka_ok / ka_total
            print()
            print(f"- 每请求新建连接成功率 {new_conn_ok}/{new_conn_total}，"
                  f"单连接复用成功率 {ka_ok}/{ka_total}")
            if new_conn_rate < 1 and ka_rate == 1:
                print("  → 只有「新建连接」会失败：偏向 accept 队列/并发连接限制/")
                print("    IPS 新连接策略，以及客户端每请求新建 TLS 的连接开销。")
                print("    导出工具已改为 keep-alive 复用连接，这类失败应显著减少。")
            elif new_conn_rate == 0 and ka_rate == 0:
                print("  → 该接口稳定失败而其它接口正常：可能是按路径/参数拦截，")
                print("    也可能服务端在这个路径上异常，需要服务端日志佐证。")
            else:
                print("  → 间歇性连接重置：不是这个接口被确定性拦截，而是短时抖动窗口。")
                print("    失败若成簇出现（连着几次失败后恢复）基本可以确认。")

        print()
        print("- 责任方定位（按顺序做）:")
        print("  1) Console 服务端日志: grep -iE 'panic|i/o timeout' <console>/log/console/nodes/*/console.log")
        print("     有 panic 堆栈 → 服务端 bug；完全无日志 → 请求没到达 Console（中间设备拦截）。")
        print("  2) 把本脚本放到 Console 宿主机上跑（-u https://127.0.0.1:<端口>）：")
        print("     本机也不定时失败 → Console/宿主机；本机一直正常 → 网络路径或客户端侧。")
        print("  3) 换一台同网段机器跑本脚本，排除本机安全软件/代理因素。")
        print("  4) 确认 7474 是 Console 本体还是前置网关/负载均衡，查其拦截与健康检查日志。")
        print()
        print("- 客户端侧已做的加固（infini-console-tools 最新代码）:")
        print("  * keep-alive 连接复用，不再每个请求新建 TLS 连接")
        print("  * 指数退避 + 抖动重试（1/2/4/8s + 0-0.5s），可用 CONSOLE_MAX_ATTEMPTS 调整")
        print("  * 集群列表查询改用 URL 查询参数并自动翻页（旧实现 body 被 Console 忽略，最多 20 条）")
        print("  * 系统集群ID 可显式指定（--system-cluster-id / systemClusterId），跳过易抖动的预检")

    @staticmethod
    def _print_timing(label, results):
        if not results:
            return
        seconds = [sec for _, sec in results]
        if len(seconds) > 1:
            print(f"  耗时统计[{label}]: 最小 {min(seconds):.2f}s / "
                  f"中位 {statistics.median(seconds):.2f}s / 最大 {max(seconds):.2f}s")
        else:
            print(f"  耗时统计[{label}]: {seconds[0]:.2f}s")


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    auth = cfg.get("auth", {})
    return cfg.get("consoleUrl"), auth.get("username"), auth.get("password")


def main():
    ap = argparse.ArgumentParser(description="Console 连接诊断")
    ap.add_argument("-c", "--config", help="导出工具的 JSON 配置文件路径（含 consoleUrl/auth）")
    ap.add_argument("-u", "--url", help="Console 地址，如 https://10.139.130.34:7474")
    ap.add_argument("-U", "--username", default="admin")
    ap.add_argument("-P", "--password", help="密码（不传则交互输入）")
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--repeat", type=int, default=5,
                    help="集群搜索的重复次数（默认 5），用于把间歇性故障复现成失败率")
    ap.add_argument("--body-test", dest="body_test", type=int, default=0, metavar="N",
                    help="追加 N 轮 body 敏感性测试：{} 与带 body 的 POST /elasticsearch/_search 交替各打 N 次")
    ap.add_argument("--verify-ssl", action="store_true", help="校验 SSL 证书（默认跳过校验）")
    args = ap.parse_args()

    url = user = pwd = None
    if args.config:
        url, user, pwd = load_config(args.config)
    url = args.url or url
    user = args.username or user or "admin"
    pwd = args.password or pwd
    if not url:
        ap.error("请通过 -c 配置文件或 -u 指定 Console 地址")
    if not pwd:
        pwd = getpass.getpass(f"请输入 {user} 的密码: ")

    print(f"目标: {url}")
    Diag(url, user, pwd, timeout=args.timeout,
         insecure=not args.verify_ssl, repeat=args.repeat).run(
        body_test_rounds=max(0, args.body_test))


if __name__ == "__main__":
    main()
