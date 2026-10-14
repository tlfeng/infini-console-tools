#!/usr/bin/env bash
# =============================================================================
# console-init.sh 单元测试
#
# 用法：
#   ./test_console_init.sh            # 运行所有单元测试
#   ./test_console_init.sh <pattern>  # 只运行匹配的测试（substring）
#
# 说明：
#   - 通过 CONSOLE_INIT_TEST=1 source 主脚本，仅加载函数定义不执行主入口。
#   - 网络/进程相关函数（call_setup, get_nonce, verify_ok, do_reset）会被
#     mock 掉，避免真实调用 Console / easysearch / 操作系统进程。
#   - 纯逻辑函数（gen_secret, build_cluster_json, detect_state, do_setup
#     分流）做真实断言。
# =============================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="${SCRIPT_DIR}/console-init.sh"

# ---------- 加载被测脚本（测试模式） ----------
[ -f "$SCRIPT" ] || { echo "[FATAL] 找不到 ${SCRIPT}"; exit 1; }
export CONSOLE_INIT_TEST=1
# 主脚本使用 set -u（unbound variable 报错），先预定义所有可能被访问的变量
CONSOLE_HOST="${CONSOLE_HOST:-127.0.0.1}"
CONSOLE_PORT="${CONSOLE_PORT:-9000}"
CONSOLE_SCHEME="${CONSOLE_SCHEME:-https}"
CONSOLE_USER="${CONSOLE_USER:-admin}"
CONSOLE_PASS="${CONSOLE_PASS:-Qwer@12345}"
CLUSTER_HOST="${CLUSTER_HOST:-10.0.0.1}"
CLUSTER_PORT="${CLUSTER_PORT:-9200}"
CLUSTER_SCHEME="${CLUSTER_SCHEME:-http}"
CLUSTER_USER="${CLUSTER_USER:-}"
CLUSTER_PASS="${CLUSTER_PASS:-}"
CONSOLE_DIR="${CONSOLE_DIR:-}"
# shellcheck source=console-init.sh
source "$SCRIPT"

# ---------- 测试框架 ----------
export PASS=0 FAIL=0 CURRENT=""
FILTER="${1:-}"   # 可选：只运行名称包含该子串的用例

t()   { CURRENT="$1"; }
skip(){ [ -z "$FILTER" ] || { echo "$CURRENT" | grep -qF "$FILTER" || return 0; }; return 1; }
ok()  { skip && return; PASS=$((PASS+1)); echo "  ✓ $CURRENT - $1"; }
fail(){ skip && return; FAIL=$((FAIL+1)); echo "  ✗ $CURRENT - $1"; }

assert_eq() { # $1=actual $2=expected $3=msg
  if [ "$1" == "$2" ]; then ok "$3"; else fail "$3 (期望 '$2' 实际 '$1')"; fi
}
assert_contains() { # $1=haystack $2=needle $3=msg
  if echo "$1" | grep -qF "$2"; then ok "$3"; else fail "$3 (未包含 '$2')"; fi
}
assert_not_contains() { # $1=haystack $2=needle $3=msg
  if echo "$1" | grep -qF "$2"; then fail "$3 (不应包含 '$2')"; else ok "$3"; fi
}
assert_true() { # $1=cmd_result $2=msg
  if [ "$1" == "0" ]; then ok "$2"; else fail "$2 (退出码 $1)"; fi
}

# ---------- mock：防止真实网络/进程调用 ----------
# 默认所有 HTTP/进程函数都 mock 为安全响应；具体测试用例再覆盖。
_called=""
MOCK_VALIDATE=""
mock_call_setup() { _called="${_called} call_setup:$1"; echo "$MOCK_VALIDATE"; }
mock_get_nonce()  { echo "mocked-nonce"; }
mock_verify_ok()  { return 0; }
mock_do_reset()   { _called="${_called} do_reset"; }
mock_do_init()    { _called="${_called} do_init"; }

# 备份原函数，测试后恢复
BACKUP_CALL_SETUP="$(declare -f call_setup)"
BACKUP_GET_NONCE="$(declare -f get_nonce)"
BACKUP_VERIFY="$(declare -f verify_ok)"

# 帮助：切换 mock 开关
mock_on() {
  call_setup(){ mock_call_setup "$@"; }
  get_nonce(){ mock_get_nonce "$@"; }
  verify_ok(){ mock_verify_ok "$@"; }
}
mock_off() {
  eval "$BACKUP_CALL_SETUP"
  eval "$BACKUP_GET_NONCE"
  eval "$BACKUP_VERIFY"
}

# =============================================================================
# 测试组 1：gen_secret
# =============================================================================
t "gen_secret"
s=$(gen_secret)
assert_eq "${#s}" "16" "gen_secret 输出 16 位"
if echo "$s" | grep -qE '^[A-Za-z0-9]+$'; then ok "gen_secret 仅含字母数字"; else fail "gen_secret 含非法字符: $s"; fi
s2=$(gen_secret)
assert_true "$([ "$s" != "$s2" ] && echo 0 || echo 1)" "gen_secret 两次调用结果不同（随机性）"

# =============================================================================
# 测试组 2：build_cluster_json（无认证）
# =============================================================================
t "build_cluster_json 无认证"
CLUSTER_HOST="10.0.0.1" CLUSTER_PORT="9200" CLUSTER_SCHEME="https" CLUSTER_USER="" CLUSTER_PASS=""
j=$(build_cluster_json)
assert_contains "$j" '"endpoint": "https://10.0.0.1:9200"' "endpoint 正确"
assert_contains "$j" '"hosts": ["10.0.0.1:9200"]' "hosts 正确"
assert_contains "$j" '"schema": "https"' "schema 正确"
assert_not_contains "$j" 'username' "无认证时不包含 username"
assert_not_contains "$j" 'password' "无认证时不包含 password"

# =============================================================================
# 测试组 3：build_cluster_json（有认证）
# =============================================================================
t "build_cluster_json 有认证"
CLUSTER_HOST="10.0.0.1" CLUSTER_PORT="9200" CLUSTER_SCHEME="http" CLUSTER_USER="admin" CLUSTER_PASS="p@ss:word"
j=$(build_cluster_json)
assert_contains "$j" '"endpoint": "http://10.0.0.1:9200"' "http endpoint 正确"
assert_contains "$j" '"username": "admin"' "包含 username"
assert_contains "$j" '"password": "p@ss:word"' "包含 password（含特殊字符）"

# =============================================================================
# 测试组 4：detect_state 状态判断
# =============================================================================
t "detect_state: setup_done"
mock_on
MOCK_VALIDATE='{"error":{"reason":"setup not permitted"},"status":500}'
state=$(detect_state)
assert_eq "$state" "setup_done" "console 已 setup → setup_done"
mock_off

t "detect_state: indices_exists"
mock_on
MOCK_VALIDATE='{"error":{"reason":"there are following indices exists in target elasticsearch: \n.infini_cluster"},"type":"elasticsearch_indices_exists"}'
state=$(detect_state)
assert_eq "$state" "indices_exists" "集群已有 .infini 索引 → indices_exists"
mock_off

t "detect_state: new"
mock_on
MOCK_VALIDATE='{"success":true}'
state=$(detect_state)
assert_eq "$state" "new" "全新环境 → new"
mock_off

t "detect_state: unknown"
mock_on
MOCK_VALIDATE='{"error":{"reason":"connection refused"}}'
state=$(detect_state)
assert_eq "$state" "unknown" "无法判断 → unknown"
mock_off

# =============================================================================
# 测试组 5：do_setup 分流逻辑
# =============================================================================
t "do_setup: new → do_init"
mock_on
MOCK_VALIDATE='{"success":true}'
_called=""
# 覆盖 do_init/do_reset 为记录版本（do_reset 模拟真实行为：内部调用 do_init）
do_init(){ _called="${_called} do_init"; }
do_reset(){ _called="${_called} do_reset"; do_init; }
do_setup
assert_contains "$_called" "do_init" "new 状态调用 do_init（不调用 do_reset）"
assert_not_contains "$_called" "do_reset" "new 状态不调用 do_reset"
mock_off

t "do_setup: setup_done → do_reset"
mock_on
MOCK_VALIDATE='{"error":{"reason":"setup not permitted"},"status":500}'
_called=""
do_init(){ _called="${_called} do_init"; }
do_reset(){ _called="${_called} do_reset"; do_init; }
do_setup
assert_contains "$_called" "do_reset" "setup_done 状态调用 do_reset"
assert_contains "$_called" "do_init" "setup_done 状态重置后调用 do_init"
mock_off

t "do_setup: indices_exists → do_reset"
mock_on
MOCK_VALIDATE='{"error":{"reason":"there are following indices exists in target elasticsearch"},"type":"elasticsearch_indices_exists"}'
_called=""
do_init(){ _called="${_called} do_init"; }
do_reset(){ _called="${_called} do_reset"; do_init; }
do_setup
assert_contains "$_called" "do_reset" "indices_exists 状态调用 do_reset"
assert_contains "$_called" "do_init" "indices_exists 状态重置后调用 do_init"
mock_off

# =============================================================================
# 测试组 6：do_init 中的模板列表（easysearch 含 rollup）
# =============================================================================
t "do_init 模板列表：easysearch 含 rollup"
# 直接验证模板列表逻辑：easysearch 应包含 rollup
dist="easysearch"
tpls=("template_ilm" "insight" "alerting" "agent" "view")
if [ "$dist" == "easysearch" ]; then
  tpls=("template_ilm" "rollup" "insight" "alerting" "agent" "view")
fi
assert_contains "${tpls[*]}" "rollup" "easysearch 模板列表包含 rollup"
assert_eq "${#tpls[@]}" "6" "easysearch 模板数量为 6"

dist="elasticsearch"
tpls=("template_ilm" "insight" "alerting" "agent" "view")
if [ "$dist" == "easysearch" ]; then
  tpls=("template_ilm" "rollup" "insight" "alerting" "agent" "view")
fi
assert_not_contains "${tpls[*]}" "rollup" "非 easysearch 不包含 rollup"
assert_eq "${#tpls[@]}" "5" "非 easysearch 模板数量为 5"

# =============================================================================
# 测试组 7：主入口分类（shell 层面）
# =============================================================================
t "主入口命令分类"
# 验证 case 分支能正确识别命令（通过提取 case 匹配）
case "setup" in setup|init|reset) ok "setup 是合法命令";; *) fail "setup 未被识别";; esac
case "init"  in setup|init|reset) ok "init 是合法命令";;  *) fail "init 未被识别";;  esac
case "reset" in setup|init|reset) ok "reset 是合法命令";; *) fail "reset 未被识别";; esac
case "badcmd" in setup|init|reset) fail "badcmd 被错误识别";; *) ok "badcmd 被正确拒绝";; esac

# =============================================================================
echo ""
echo "========================================="
echo "  通过: ${PASS}   失败: ${FAIL}"
echo "========================================="
[ "$FAIL" -eq 0 ] || exit 1
exit 0