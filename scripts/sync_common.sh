#!/usr/bin/env bash
# 共享层是「一份源码、两处副本」：以 orchestrator 的 common 为唯一真源，
# 复制到 agents。改完共享代码后执行一次；CI 的 contract 测试会校验两份字节一致。
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$ROOT/orchestrator/src/iqe_orchestrator/common"
DST="$ROOT/agents/src/iqe_agents/common"

[ -d "$SRC" ] || { echo "缺少源目录: $SRC" >&2; exit 1; }
mkdir -p "$DST"
if command -v rsync >/dev/null; then
  rsync -a --delete --exclude '__pycache__' "$SRC"/ "$DST"/
else
  rm -rf "$DST"
  mkdir -p "$DST"
  (cd "$SRC" && tar -cf - --exclude='__pycache__' .) | (cd "$DST" && tar -xf -)
fi
echo "[sync_common] $SRC -> $DST 完成"
