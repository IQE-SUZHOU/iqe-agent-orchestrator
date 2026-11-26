#!/usr/bin/env bash
# 本机全链路起进程：1 个编排层 + 6 个 Agent，连集群里现有的 RabbitMQ / Postgres。
# 用途：在打镜像上 k8s 之前，先在本机跑通端到端（测试直接打 http://127.0.0.1:8080）。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE"

VENV="${VENV:-$HERE/../.venv/bin/python}"

# 凭据来自 deploy/secrets.env（.gitignore 排除），不在脚本里留明文
SECRETS_ENV="${SECRETS_ENV:-$HERE/deploy/secrets.env}"
if [ -f "$SECRETS_ENV" ]; then
  set -a; . "$SECRETS_ENV"; set +a
fi
: "${CI_APP_PASS:?缺少 CI_APP_PASS：cp deploy/secrets.env.example deploy/secrets.env 后填真实值}"
: "${PG_PASSWORD:?缺少 PG_PASSWORD（同上）}"
AMQP_HOST="${AMQP_HOST_LOCAL:-10.100.200.12:5672}"
AMQP_PASS="${AMQP_PASS:-$CI_APP_PASS}"
PG_DSN="${PG_DSN:-postgresql://postgres:${PG_PASSWORD}@10.96.75.0:5432/ci_triage}"
LOG_DIR="${LOG_DIR:-$HERE/.run}"
ORCH_PORT="${ORCH_PORT:-8080}"
mkdir -p "$LOG_DIR"

amqp() { echo "amqp://$1:$AMQP_PASS@$AMQP_HOST/ci"; }

cleanup() {
  echo "[run_local] 停止所有进程"
  for f in "$LOG_DIR"/*.pid; do
    [ -e "$f" ] || continue
    pid=$(cat "$f")
    kill "$pid" 2>/dev/null || true
    rm -f "$f"
  done
}
trap cleanup EXIT INT TERM

export PYTHONPATH="$HERE" PG_DSN LOG_JSON=false MOCK_SCENARIO=default

echo "[run_local] 1) 编排层 :$ORCH_PORT"
AMQP_URL="$(amqp ci_orch)" HTTP_PORT="$ORCH_PORT" PLANS_DIR="$HERE/.plans" \
  nohup "$VENV" -m orchestrator.main >"$LOG_DIR/orchestrator.log" 2>&1 &
echo $! >"$LOG_DIR/orchestrator.pid"

for i in $(seq 1 60); do
  if curl -fsS -m 2 "http://127.0.0.1:$ORCH_PORT/readyz" >/dev/null 2>&1; then
    echo "[run_local] 编排层 ready（队列已声明）"; break
  fi
  sleep 1
done

ports=(monitor:8101 triage:8102 reproduction:8103 ticket:8104 review:8105 reporting:8106)
for entry in "${ports[@]}"; do
  stage="${entry%%:*}"; port="${entry##*:}"
  echo "[run_local] 2) agent $stage :$port"
  AMQP_URL="$(amqp "ci_agent_$stage")" SVC="agent-$stage" HTTP_PORT="$port" \
    nohup "$VENV" -m "agents.$stage.main" >"$LOG_DIR/$stage.log" 2>&1 &
  echo $! >"$LOG_DIR/$stage.pid"
done

sleep 4
echo "[run_local] 全部进程已启动，日志在 $LOG_DIR/"
echo "[run_local] UI: http://127.0.0.1:$ORCH_PORT/   API base: http://127.0.0.1:$ORCH_PORT"
echo "[run_local] 跑测试：ORCH_BASE_URL=http://127.0.0.1:$ORCH_PORT PG_DSN=$PG_DSN $VENV -m pytest tests -q"

while true; do
  for entry in orchestrator monitor triage reproduction ticket review reporting; do
    pid_file="$LOG_DIR/$entry.pid"
    [ -e "$pid_file" ] || continue
    if ! kill -0 "$(cat "$pid_file")" 2>/dev/null; then
      echo "[run_local] 进程 $entry 退出！最后 20 行日志："
      tail -20 "$LOG_DIR/$entry.log" || true
      exit 1
    fi
  done
  sleep 5
done
