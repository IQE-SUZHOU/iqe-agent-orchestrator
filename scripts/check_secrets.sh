#!/usr/bin/env bash
# 提交前拦截：别把明文口令写进代码、清单或文档。
#
# 允许的形式：${PG_PASSWORD} 环境变量引用、{a.app_pass} 这类 f-string 变量、CHANGE_ME 占位、
#             文档里的省略号示意，以及 localhost 上无口令的默认 DSN。
#
# 规则分两部分：
#   1) 通用形态（本脚本内置，可公开）——URL 里带字面量口令、k8s dockerconfigjson 等；
#   2) 已知口令黑名单（**不入库**）——放 deploy/secret_denylist.txt（已 .gitignore），
#      每行一条固定字符串。这样脚本本身可以安全地推到远端仓库。
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DENYLIST="$ROOT/deploy/secret_denylist.txt"

PATTERNS=(
  'amqp://[A-Za-z0-9_]+:[^$"{<>[:space:]]{6,}@'
  'postgresql://[A-Za-z0-9_]+:[^$"{<>[:space:]]{6,}@'
  'mysql://[A-Za-z0-9_]+:[^$"{<>[:space:]]{6,}@'
  '\.dockerconfigjson:'
  '(password|passwd|secret)["'\'':= ]+["'\''][^"$]{8,}["'\'']'
)

if [ -f "$DENYLIST" ]; then
  while IFS= read -r line; do
    line="${line%$'\r'}"
    [ -z "$line" ] && continue
    case "$line" in \#*) continue ;; esac
    PATTERNS+=("$line")
  done < "$DENYLIST"
else
  echo "  （提示：$DENYLIST 不存在，只跑通用形态检查；本地建议放一份已知口令黑名单）" >&2
fi

bad=0
# 不带参数时必须自己扫全仓，否则「直接跑一下」永远绿灯（这个洞曾经让口令写进文档也没报）。
if [ "$#" -eq 0 ]; then
  if git -C "$ROOT" rev-parse --git-dir >/dev/null 2>&1; then
    mapfile -t _files < <(git -C "$ROOT" ls-files)
  else
    mapfile -t _files < <(cd "$ROOT" && find . -type f -not -path './.git/*' | sed 's|^\./||')
  fi
  set -- "${_files[@]}"
fi
for f in "$@"; do
  case "$f" in /*) abs="$f" ;; *) abs="$ROOT/$f" ;; esac
  [ -f "$abs" ] || continue
  rel="${abs#"$ROOT"/}"
  case "$rel" in
    deploy/secrets.env|deploy/secrets.env.example|deploy/secret_denylist.txt|scripts/check_secrets.sh) continue ;;
  esac
  for p in "${PATTERNS[@]}"; do
    if grep -nE "$p" "$abs" >/dev/null 2>&1; then
      echo "  ✗ $rel 命中可疑凭据模式：$p" >&2
      grep -nE "$p" "$abs" | head -3 | sed 's/^/      /' >&2
      bad=1
    fi
  done
done
if [ $bad -ne 0 ]; then
  echo "口令请放 deploy/secrets.env（已 .gitignore）或环境变量；代码/清单/文档里只留变量引用。" >&2
fi
exit $bad
