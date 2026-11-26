#!/usr/bin/env bash
# 提交前拦截：别把明文口令写进代码、清单或文档。
# 允许的形式：${PG_PASSWORD} 环境变量引用、{a.app_pass} 这类 f-string 变量、CHANGE_ME 占位、
#             README 里的省略号示意，以及 localhost 上无口令的默认 DSN。
set -uo pipefail
PATTERNS=(
  'postgres-super-secret-password'
  'rabbitmq-super-secret-password'
  'ci-triage-demo-pass'
  'Harbor12345'
  'amqp://[A-Za-z0-9_]+:[^$"{[:space:]]{6,}@'
  'postgresql://[A-Za-z0-9_]+:[^$"{[:space:]]{6,}@'
  '\.dockerconfigjson:'
)
bad=0
for f in "$@"; do
  [ -f "$f" ] || continue
  case "$f" in deploy/secrets.env|deploy/secrets.env.example) continue ;; esac
  for p in "${PATTERNS[@]}"; do
    if grep -nE "$p" "$f" >/dev/null 2>&1; then
      echo "  ✗ $f 命中可疑凭据模式：$p" >&2
      grep -nE "$p" "$f" | head -3 | sed 's/^/      /' >&2
      bad=1
    fi
  done
done
if [ $bad -ne 0 ]; then
  echo "口令请放 deploy/secrets.env（已 .gitignore）或环境变量；代码/清单/文档里只留变量引用。" >&2
fi
exit $bad
