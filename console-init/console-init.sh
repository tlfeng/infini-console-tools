#!/usr/bin/env bash
# =============================================================================
# INFINI Console 通用初始化 / 重置脚本
#
# 功能：
#   setup  - （默认）自动分流：若已初始化则先重置再初始化，未初始化则直接初始化。
#            无论之前在 Web UI 上操作了什么，最终都回到"初始化后"的干净状态。
#   init   - 仅初始化（要求全新环境，若已初始化会报错）。
#   reset  - 重置为"初始化后"状态并重新初始化。
#
# 用法：
#   交互式（推荐）：
#     ./console-init.sh setup
#     ./console-init.sh init
#     ./console-init.sh reset
#
#   非交互式（通过环境变量传入所有参数）：
#     CONSOLE_HOST=127.0.0.1 CONSOLE_PORT=9000 \
#     CLUSTER_HOST=127.0.0.1 CLUSTER_PORT=9200 CLUSTER_SCHEME=https \
#     CLUSTER_USER=admin CLUSTER_PASS='mypass' \
#     CONSOLE_USER=admin CONSOLE_PASS='Qwer@12345' \
#     CONSOLE_DIR=/path/to/console \
#     ./console-init.sh setup
#
# 必需参数：
#   CONSOLE_HOST / CONSOLE_PORT      Console 的 host 与 port（默认 127.0.0.1:9000）
#   CONSOLE_SCHEME                   Console 协议：http 或 https（默认 https）
#   CLUSTER_HOST / CLUSTER_PORT     系统集群（es/easysearch）的 host 与 port
#   CLUSTER_SCHEME                   系统集群协议：http 或 https
#   CLUSTER_USER / CLUSTER_PASS      系统集群认证（可选，无认证可留空）
#   CONSOLE_USER / CONSOLE_PASS      Console 登录用户名 / 密码（默认 admin / Qwer@12345）
#
# setup / reset 需要 CONSOLE_DIR（Console 安装目录，用于可能的重置）：
#   CONSOLE_DIR    Console 安装目录（含 console-linux-* 二进制与 console.yml）
#                  reset 会停止该目录下的 console 进程、清理数据并后台重启。
# =============================================================================
set -uo pipefail

# ---------- 默认值 ----------
CONSOLE_HOST="${CONSOLE_HOST:-127.0.0.1}"
CONSOLE_PORT="${CONSOLE_PORT:-9000}"
CONSOLE_SCHEME="${CONSOLE_SCHEME:-https}"
CONSOLE_USER="${CONSOLE_USER:-admin}"
CONSOLE_PASS="${CONSOLE_PASS:-Qwer@12345}"
CLUSTER_USER="${CLUSTER_USER:-}"
CLUSTER_PASS="${CLUSTER_PASS:-}"
CONSOLE_DIR="${CONSOLE_DIR:-}"

# ---------- 工具函数 ----------
log()  { echo -e "\033[0;32m[INFO ]\033[0m $*"; }
warn() { echo -e "\033[0;33m[WARN ]\033[0m $*"; }
err()  { echo -e "\033[0;31m[ERROR]\033[0m $*"; }

die() { err "$*"; exit 1; }

# 交互式输入
prompt() {
  local var="$1" label="$2" def="${3:-}" val
  if [ -n "$def" ]; then
    read -r -p "  $label [$def]: " val
    val="${val:-$def}"
  else
    read -r -p "  $label: " val
  fi
  eval "$var=\"$val\""
}

read_inputs() {
  echo ""
  echo "===== Console 连接信息 ====="
  prompt CONSOLE_HOST "Console host" "$CONSOLE_HOST"
  prompt CONSOLE_PORT "Console port" "$CONSOLE_PORT"
  echo ""
  echo "===== 系统集群（es/easysearch）连接信息 ====="
  prompt CLUSTER_HOST "集群 host" "${CLUSTER_HOST:-}"
  prompt CLUSTER_PORT "集群 port" "${CLUSTER_PORT:-}"
  prompt CLUSTER_SCHEME "集群协议 (http/https)" "${CLUSTER_SCHEME:-https}"
  echo -n "  集群认证（可选，直接回车跳过）用户名: "
  read -r CLUSTER_USER
  if [ -n "$CLUSTER_USER" ]; then
    read -r -s -p "  集群认证密码: " CLUSTER_PASS
    echo ""
  fi
  echo ""
  echo "===== Console 登录账号 ====="
  prompt CONSOLE_USER "Console 用户名" "$CONSOLE_USER"
  read -s -r -p "  Console 密码: " CONSOLE_PASS
  echo ""
}

CONSOLE_BASE_URL="${CONSOLE_SCHEME:-https}://${CONSOLE_HOST}:${CONSOLE_PORT}"

# 获取 replay nonce
get_nonce() {
  local method="$1" path="$2"
  python3 - "$CONSOLE_BASE_URL" "$method" "$path" <<'PYEOF'
import sys, json, urllib.request, ssl
base, method, path = sys.argv[1], sys.argv[2], sys.argv[3]
ctx = ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
req = urllib.request.Request(base + "/account/replay_nonce",
      data=json.dumps({"method": method, "path": path}).encode(),
      headers={"Content-Type":"application/json"}, method="POST")
try:
    r = urllib.request.urlopen(req, context=ctx, timeout=15)
    print(json.loads(r.read().decode())["nonce"])
except Exception as e:
    sys.exit(1)
PYEOF
}

# 调用 setup API：$1=path $2=json_body，返回响应体（stdout）
# 兼容两种 Console 版本：老版本要求 /account/replay_nonce 的 X-Request-Nonce 头；
# 新版本（如 1.30.x）无该端点（404），此时不带 nonce 头直接调用。
call_setup() {
  local path="$1" body="$2"
  local nonce=""
  nonce=$(get_nonce "POST" "$path" 2>/dev/null) || nonce=""
  python3 - "$CONSOLE_BASE_URL" "$path" "$nonce" "$body" <<'PYEOF'
import sys, json, urllib.request, ssl
base, path, nonce, body = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
ctx = ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
headers = {"Content-Type":"application/json"}
if nonce:
    headers["X-Request-Nonce"] = nonce
req = urllib.request.Request(base + path, data=body.encode(),
      headers=headers, method="POST")
try:
    r = urllib.request.urlopen(req, context=ctx, timeout=120)
    print(r.read().decode())
except urllib.error.HTTPError as e:
    print(e.read().decode())
    sys.exit(0)  # 返回 body，由调用者判断 success
except Exception as e:
    print(json.dumps({"success":False,"error":{"reason":str(e)}}))
    sys.exit(0)
PYEOF
}

# 构造 cluster 请求体片段
build_cluster_json() {
  local hostport="${CLUSTER_HOST}:${CLUSTER_PORT}"
  if [ -n "$CLUSTER_USER" ]; then
    python3 - "$CLUSTER_SCHEME" "$hostport" "$CLUSTER_USER" "$CLUSTER_PASS" <<'PYEOF'
import sys, json
scheme, hostport, u, p = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
print(json.dumps({"endpoint": f"{scheme}://{hostport}", "hosts": [hostport],
                  "schema": scheme, "username": u, "password": p}))
PYEOF
  else
    python3 - "$CLUSTER_SCHEME" "$hostport" <<'PYEOF'
import sys, json
scheme, hostport = sys.argv[1], sys.argv[2]
print(json.dumps({"endpoint": f"{scheme}://{hostport}", "hosts": [hostport], "schema": scheme}))
PYEOF
  fi
}

# 生成 16 位随机 credential secret
gen_secret() {
  python3 - <<'PYEOF'
import random, string
print("".join(random.SystemRandom().choice(string.ascii_letters + string.digits) for _ in range(16)))
PYEOF
}

# ---------- 操作：init ----------
do_init() {
  log "开始初始化 Console @ ${CONSOLE_BASE_URL}"
  local cluster_json
  cluster_json=$(build_cluster_json)

  # 1. validate
  log "校验系统集群连接..."
  local vbody errinfo
  vbody=$(call_setup "/setup/_validate" "{\"cluster\":$cluster_json}")
  if ! echo "$vbody" | python3 -c 'import sys,json;sys.exit(0 if json.load(sys.stdin).get("success") else 1)' 2>/dev/null; then
    errinfo=$(echo "$vbody" | python3 -c 'import sys,json
try:
    d=json.load(sys.stdin)
    r=d.get("error",{}).get("reason","") or d.get("reason","")
    print(r)
except Exception:
    print("")' 2>/dev/null)
    if echo "$vbody" | grep -q "setup not permitted"; then
      die "Console 已完成初始化（setup 已完成），不能重复 init。如需重新初始化请使用 reset 命令。"
    elif echo "$errinfo" | grep -qiE "indices exists|template already exists"; then
      die "系统集群已存在 .infini_* 索引或 .infini 模板，无法初始化。请使用 reset 命令清理后重试。\n  详情: $errinfo"
    else
      die "集群校验失败: $errinfo"
    fi
  fi
  log "集群校验通过"

  # 2. initialize_template（必须在 initialize 之前执行）
  local primary_shards=1 auto_expand="0-1"
  local templates=("template_ilm" "insight" "alerting" "agent" "view")
  # easysearch >=1.12.1 支持 rollup
  local dist
  dist=$(echo "$vbody" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("distribution",""))' 2>/dev/null)
  if [ "$dist" == "easysearch" ]; then
    templates=("template_ilm" "rollup" "insight" "alerting" "agent" "view")
  fi
  for tpl in "${templates[@]}"; do
    log "初始化模板: $tpl ..."
    local tbody
    tbody=$(call_setup "/setup/_initialize_template" \
      "{\"cluster\":$cluster_json,\"initialize_template\":\"$tpl\",\"primary_shards\":$primary_shards,\"auto_expand_replicas\":\"$auto_expand\",\"enable_rollup\":true}")
    if ! echo "$tbody" | python3 -c 'import sys,json;sys.exit(0 if json.load(sys.stdin).get("success") else 1)' 2>/dev/null; then
      warn "模板 $tpl 初始化失败（可忽略，主流程继续）: $tbody"
    fi
  done

  # 3. initialize（创建系统集群配置 + admin 用户 + 系统索引）
  log "初始化系统集群与 admin 用户..."
  local secret
  secret=$(gen_secret)
  # 用 python 构造请求体：密码等字段含引号/反斜杠等特殊字符时仍能生成合法 JSON
  local ibody
  ibody=$(python3 - "$cluster_json" "$CONSOLE_USER" "$CONSOLE_PASS" "$secret" <<'PYEOF'
import sys, json
cluster, user, password, secret = sys.argv[1:5]
print(json.dumps({"cluster": json.loads(cluster), "skip": False,
                  "reset_user": True, "bootstrap_username": user,
                  "bootstrap_password": password, "credential_secret": secret}))
PYEOF
)
  local rbody
  rbody=$(call_setup "/setup/_initialize" "$ibody")
  if ! echo "$rbody" | python3 -c 'import sys,json;sys.exit(0 if json.load(sys.stdin).get("success") else 1)' 2>/dev/null; then
    die "初始化失败: $rbody"
  fi
  log "初始化成功"

  verify_ok
}

# ---------- 状态检测：判断当前是否已初始化 ----------
# 返回：new=全新可直接初始化 / setup_done=console已完成setup / indices_exists=集群已有.infini数据
detect_state() {
  local cluster_json
  cluster_json=$(build_cluster_json)
  local vbody
  vbody=$(call_setup "/setup/_validate" "{\"cluster\":$cluster_json}")
  if echo "$vbody" | grep -q "setup not permitted"; then
    echo "setup_done"
  elif echo "$vbody" | grep -qiE "indices exists|template already exists"; then
    echo "indices_exists"
  elif echo "$vbody" | python3 -c 'import sys,json;sys.exit(0 if json.load(sys.stdin).get("success") else 1)' 2>/dev/null; then
    echo "new"
  else
    # 把 _validate 的真实响应打到 stderr，方便排查连接问题（stdout 只保留状态字）
    echo "$vbody" | python3 -c 'import sys,json
try:
    d=json.load(sys.stdin)
    r=d.get("error",{}).get("reason","") or d.get("reason","") or json.dumps(d,ensure_ascii=False)
    print(r)
except Exception:
    print(sys.stdin.read() or "(non-json response)")' 2>/dev/null | sed 's/^/[validate] /' >&2
    echo "unknown"
  fi
}

# ---------- 合并命令：自动判断，已初始化则重置再初始化，否则直接初始化 ----------
do_setup() {
  log "检测 Console 初始化状态..."
  # 兼容 macOS bash 3.2：`state="$(detect_state)"` + `case "$state"` 在 set -u 下
  # 会误报 unbound（detect_state 内部含 local 变量触发 bug）。故在 case 判断时
  # 临时关闭 set -u，选分支后再恢复，让 do_init/do_reset 在 set -u 下执行。
  local _action=""
  set +u
  _setup_state="$(detect_state)"
  case "$_setup_state" in
    new)              _action="init" ;;
    setup_done|indices_exists) _action="reset" ;;
    *)                _action="unknown" ;;
  esac
  set -u
  case "$_action" in
    init)
      log "检测到全新环境（未初始化），直接初始化"
      do_init
      ;;
    reset)
      log "检测到已初始化，先重置再初始化"
      do_reset
      ;;
    *)
      die "无法判断初始化状态，请检查 Console 与系统集群连接"
      ;;
  esac
}

# ---------- 验证：登录 + 集群列表（带重试，等待模块就绪） ----------
verify_ok() {
  log "验证 admin 登录与系统集群..."
  local token=""
  # initialize 后各模块（登录/鉴权）异步启动，需要等待就绪
  for i in $(seq 1 30); do
    token=$(python3 - "$CONSOLE_BASE_URL" "$CONSOLE_USER" "$CONSOLE_PASS" <<'PYEOF'
import sys, json, urllib.request, ssl
base, u, p = sys.argv[1], sys.argv[2], sys.argv[3]
ctx = ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
req = urllib.request.Request(base + "/account/login",
      data=json.dumps({"username":u,"password":p}).encode(),
      headers={"Content-Type":"application/json"}, method="POST")
try:
    r = urllib.request.urlopen(req, context=ctx, timeout=10)
    print(json.loads(r.read().decode())["access_token"])
    sys.exit(0)
except Exception:
    sys.exit(1)
PYEOF
)
    [ -n "$token" ] && break
    sleep 2
  done
  [ -z "$token" ] && die "admin 登录失败，请检查用户名/密码（console 是否已就绪）"

  local clusters=""
  for i in $(seq 1 10); do
    clusters=$(python3 - "$CONSOLE_BASE_URL" "$token" <<'PYEOF'
import sys, json, urllib.request, ssl
base, tok = sys.argv[1], sys.argv[2]
ctx = ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
req = urllib.request.Request(base + "/elasticsearch/_search",
      data=b'{"from":0,"size":20}', headers={"Content-Type":"application/json","Authorization":"Bearer "+tok}, method="POST")
try:
    r = urllib.request.urlopen(req, context=ctx, timeout=10)
    d = json.loads(r.read().decode())
    tot = d.get("hits",{}).get("total",{}).get("value",0)
    print(f"total={tot}")
except Exception:
    print("err")
PYEOF
)
    [[ "$clusters" == total=* ]] && break
    sleep 2
  done
  if [[ "$clusters" == total=* ]]; then
    log "验证通过：系统集群已注册，Web UI 集群管理页面可见（total=${clusters#total=}）"
  else
    warn "验证异常：$clusters"
  fi
}

# ---------- 操作：reset ----------
do_reset() {
  [ -z "$CONSOLE_DIR" ] && die "reset 需要 CONSOLE_DIR（Console 安装目录）"
  [ -d "$CONSOLE_DIR" ] || die "CONSOLE_DIR 不存在: $CONSOLE_DIR"

  local bin
  bin=$(ls "$CONSOLE_DIR"/console-linux-* "$CONSOLE_DIR"/console-mac-* 2>/dev/null | head -1)
  [ -z "$bin" ] && die "在 $CONSOLE_DIR 下找不到 console 二进制"
  bin=$(basename "$bin")
  log "Console 二进制: $bin"

  # 1. 停止 console 进程
  log "停止 console 进程..."
  pkill -f "$bin" 2>/dev/null
  sleep 3
  if pgrep -f "$bin" >/dev/null 2>&1; then
    warn "console 未正常退出，强制终止..."
    pkill -9 -f "$bin" 2>/dev/null
    sleep 2
  fi
  pgrep -f "$bin" >/dev/null 2>&1 && die "无法停止 console 进程"

  # 2. 清理本地数据（badger/keystore/.setup_lock/setup编排文件）
  log "清理 console 本地数据..."
  rm -rf "$CONSOLE_DIR"/data/console/nodes/*
  rm -f "$CONSOLE_DIR/data"/*.lock
  rm -f "$CONSOLE_DIR"/config/system_config.yml
  rm -f "$CONSOLE_DIR"/config/system_config.tpl.bak

  # 3. 清理系统集群中的 .infini 索引 / 模板 / ILM
  log "清理系统集群中的 .infini 数据..."
  local escli
  escli="curl -sk"
  local auth=""
  [ -n "$CLUSTER_USER" ] && auth="-u $CLUSTER_USER:$CLUSTER_PASS"
  local base="${CLUSTER_SCHEME}://${CLUSTER_HOST}:${CLUSTER_PORT}"
  # shellcheck disable=SC2086
  $escli $auth -X DELETE "$base/.infini_*"   >/dev/null 2>&1
  # shellcheck disable=SC2086
  $escli $auth -X DELETE "$base/_template/.infini" >/dev/null 2>&1
  # shellcheck disable=SC2086
  $escli $auth -X DELETE "$base/_index_template/.infini" >/dev/null 2>&1
  # 删除可能的 ILM 策略
  # shellcheck disable=SC2086
  $escli $auth -X DELETE "$base/_ilm/policy/ilm_.infini_*" >/dev/null 2>&1
  log "系统集群 .infini 数据已清理"

  # 4. 后台重启 console
  log "后台重启 console..."
  mkdir -p "$CONSOLE_DIR/log"
  cd "$CONSOLE_DIR" || die "无法进入 $CONSOLE_DIR"
  nohup "./$bin" > "$CONSOLE_DIR/log/console-init.out" 2>&1 &
  disown

  # 5. 等待 console 就绪
  log "等待 console 就绪..."
  local ok=0
  for i in $(seq 1 60); do
    if curl -sk -o /dev/null -w "%{http_code}" "$CONSOLE_BASE_URL/" 2>/dev/null | grep -qE "200|302"; then
      ok=1; break
    fi
    sleep 2
  done
  [ "$ok" -eq 1 ] || die "console 未在 120 秒内就绪，请检查 $CONSOLE_DIR/log/console-init.out"

  log "console 已就绪，开始重新初始化"
  do_init
}

# ---------- 主入口 ----------
# 测试模式：仅加载函数定义，不执行主入口（供测试脚本 source）
if [ "${CONSOLE_INIT_TEST:-0}" = "1" ]; then
  return 0 2>/dev/null || exit 0
fi

cmd="${1:-setup}"
case "$cmd" in
  setup)
    # 自动分流：已初始化则重置再初始化，未初始化则直接初始化（需要 CONSOLE_DIR 以便可能的重置）
    if [ -z "${NONINTERACTIVE:-}" ] && { [ -z "${CLUSTER_HOST:-}" ] || [ -z "${CLUSTER_PORT:-}" ] || [ -z "${CONSOLE_DIR:-}" ]; }; then
      read_inputs
      [ -z "$CONSOLE_DIR" ] && prompt CONSOLE_DIR "Console 安装目录"
    fi
    [ -z "${CLUSTER_HOST:-}" ] && die "缺少 CLUSTER_HOST"
    [ -z "${CLUSTER_PORT:-}" ] && die "缺少 CLUSTER_PORT"
    case "${CLUSTER_SCHEME:-}" in
      http|https) ;;
      *) die "缺少或非法的 CLUSTER_SCHEME（须为 http/https）" ;;
    esac
    [ -z "$CONSOLE_DIR" ] && die "缺少 CONSOLE_DIR（setup 可能需要重置，需知道 console 安装目录）"
    CONSOLE_BASE_URL="${CONSOLE_SCHEME:-https}://${CONSOLE_HOST}:${CONSOLE_PORT}"
    do_setup
    ;;
  init)
    if [ -z "${NONINTERACTIVE:-}" ] && { [ -z "${CLUSTER_HOST:-}" ] || [ -z "${CLUSTER_PORT:-}" ]; }; then
      read_inputs
    fi
    [ -z "${CLUSTER_HOST:-}" ] && die "缺少 CLUSTER_HOST"
    [ -z "${CLUSTER_PORT:-}" ] && die "缺少 CLUSTER_PORT"
    case "${CLUSTER_SCHEME:-}" in
      http|https) ;;
      *) die "缺少或非法的 CLUSTER_SCHEME（须为 http/https）" ;;
    esac
    CONSOLE_BASE_URL="${CONSOLE_SCHEME:-https}://${CONSOLE_HOST}:${CONSOLE_PORT}"
    do_init
    ;;
  reset)
    if [ -z "${NONINTERACTIVE:-}" ] && { [ -z "${CLUSTER_HOST:-}" ] || [ -z "${CLUSTER_PORT:-}" ] || [ -z "${CONSOLE_DIR:-}" ]; }; then
      read_inputs
      [ -z "$CONSOLE_DIR" ] && prompt CONSOLE_DIR "Console 安装目录"
    fi
    [ -z "${CLUSTER_HOST:-}" ] && die "缺少 CLUSTER_HOST"
    [ -z "${CLUSTER_PORT:-}" ] && die "缺少 CLUSTER_PORT"
    case "${CLUSTER_SCHEME:-}" in
      http|https) ;;
      *) die "缺少或非法的 CLUSTER_SCHEME（须为 http/https）" ;;
    esac
    [ -z "$CONSOLE_DIR" ] && die "缺少 CONSOLE_DIR"
    CONSOLE_BASE_URL="${CONSOLE_SCHEME:-https}://${CONSOLE_HOST}:${CONSOLE_PORT}"
    do_reset
    ;;
  *)
    err "未知命令: $cmd (可选: setup / init / reset)"
    exit 1
    ;;
esac
exit 0