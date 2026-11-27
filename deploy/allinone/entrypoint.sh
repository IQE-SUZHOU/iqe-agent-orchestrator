#!/usr/bin/env bash
# 全自包含演示镜像的入口：Postgres → RabbitMQ（vhost/账号）→ 编排层 → 8 个 Agent。
#
# 设计取舍：
# - PG / RabbitMQ 用系统包自带的守护方式起（pg_ctlcluster / rabbitmq-server -detached），
#   不引入 supervisord；进程少、日志直接落文件，排障时 docker exec 一看就懂。
# - 编排层必须先于 Agent 就绪：拓扑（4 exchange + 13 队列）由编排层声明，Agent 只做 passive 校验。
# - 9 个 python 进程统一用 appuser(10001) 跑，PG/MQ 用各自的系统用户。
set -euo pipefail

CI_DEMO_PASS="${CI_DEMO_PASS:-ci-demo-pass}"
APP_USER=appuser
PYTHON=/usr/local/bin/python
LOG_DIR=/var/log/ci-triage
PLANS_DIR="${PLANS_DIR:-/var/lib/ci-triage/plans}"
HTTP_PORT="${HTTP_PORT:-8080}"
STAGES=(monitor triage reviewer remediation reproduction regression ticketing reporting)

log() { printf '[entrypoint] %s\n' "$*"; }
die() {
  log "FATAL: $*"
  for f in "$LOG_DIR"/*.log; do
    [ -e "$f" ] || continue
    printf '\n===== %s =====\n' "$f"
    tail -n 40 "$f" || true
  done
  exit 1
}

mkdir -p "$LOG_DIR" "$PLANS_DIR"
chown -R "$APP_USER:$APP_USER" "$LOG_DIR" "$PLANS_DIR"

# ------------------------------------------------------------------ 1) Postgres
PGVER="$(ls /usr/lib/postgresql | sort -V | tail -1)"
if ! pg_lsclusters -h 2>/dev/null | grep -qE "^${PGVER}[[:space:]]+main"; then
  log "创建 postgres 集群 ${PGVER}/main"
  pg_createcluster "$PGVER" main >/dev/null
fi
log "启动 postgres ${PGVER}/main"
pg_ctlcluster "$PGVER" main start || true
for _ in $(seq 1 30); do pg_isready -q -h 127.0.0.1 -p 5432 && break; sleep 1; done
pg_isready -q -h 127.0.0.1 -p 5432 || die "postgres 未就绪"

# 演示口令：容器里 PG 只监听回环，口令由 CI_DEMO_PASS 统一派生
runuser -u postgres -- psql -q -v ON_ERROR_STOP=1 \
  -c "alter user postgres password '${CI_DEMO_PASS}'" >/dev/null
if ! runuser -u postgres -- psql -tAc "select 1 from pg_database where datname='ci_triage'" | grep -q 1; then
  log "创建数据库 ci_triage"
  runuser -u postgres -- createdb ci_triage
fi
export PG_DSN="${PG_DSN:-postgresql://postgres:${CI_DEMO_PASS}@127.0.0.1:5432/ci_triage}"

# ------------------------------------------------------------------ 2) RabbitMQ
log "启动 rabbitmq"
runuser -u rabbitmq -- /usr/sbin/rabbitmq-server -detached
for _ in $(seq 1 90); do
  runuser -u rabbitmq -- rabbitmq-diagnostics -q ping >/dev/null 2>&1 && break
  sleep 1
done
runuser -u rabbitmq -- rabbitmq-diagnostics -q ping >/dev/null 2>&1 || die "rabbitmq 未就绪"

log "创建 vhost /ci 与 9 个账号（星型权限：只有 ci_orch 能写 ci.cmd）"
runuser -u "$APP_USER" -- "$PYTHON" -m iqe_orchestrator.tools.setup_broker \
  --mgmt http://127.0.0.1:15672 --admin-user guest --admin-pass guest \
  --vhost ci --app-pass "$CI_DEMO_PASS" >"$LOG_DIR/setup_broker.log" 2>&1 \
  || die "setup_broker 失败（见 $LOG_DIR/setup_broker.log）"

amqp() { printf 'amqp://%s:%s@127.0.0.1:5672/ci' "$1" "$CI_DEMO_PASS"; }

PIDS=()
start_proc() { # name module amqp_url port svc
  local name="$1" module="$2" amqp_url="$3" port="$4" svc="$5"
  # HTTP_PORT 必须逐进程给：Dockerfile 里的全局 8080 只属于编排层，
  # 8 个 Agent 共用 8080 会 EADDRINUSE，uvicorn 启动失败后进程直接退出。
  runuser -u "$APP_USER" -- env AMQP_URL="$amqp_url" SVC="$svc" HTTP_PORT="$port" \
    "$PYTHON" -m "$module" >>"$LOG_DIR/$name.log" 2>&1 &
  PIDS+=("$!")
  log "启动 $name（pid ${PIDS[-1]}，HTTP_PORT=$port）"
}

# ------------------------------------------------------------------ 3) 编排层
start_proc orchestrator iqe_orchestrator.main "$(amqp ci_orch)" "$HTTP_PORT" orchestrator
for _ in $(seq 1 120); do
  curl -fsS -m 2 "http://127.0.0.1:${HTTP_PORT}/readyz" >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS -m 2 "http://127.0.0.1:${HTTP_PORT}/readyz" >/dev/null 2>&1 \
  || die "编排层 /readyz 未就绪（拓扑没声明成功）"

# ------------------------------------------------------------------ 4) 8 个 Agent（端口与 scripts/run_local.sh 的 8101..8108 一致）
agent_port() {
  local i=8100 s
  for s in "${STAGES[@]}"; do
    i=$((i + 1))
    [ "$s" = "$1" ] && { printf '%s' "$i"; return; }
  done
  return 1
}
for s in "${STAGES[@]}"; do
  start_proc "agent-$s" "iqe_agents.$s.main" "$(amqp "ci_agent_$s")" "$(agent_port "$s")" "agent-$s"
done

for s in "${STAGES[@]}"; do
  port="$(agent_port "$s")"
  for _ in $(seq 1 60); do
    curl -fsS -m 2 "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1 && break
    sleep 1
  done
  # 端口没起来说明该 Agent 已经死了（常见原因：口令不对 / 队列不存在），必须 fail-fast
  curl -fsS -m 2 "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1 \
    || die "agent-$s /healthz(:$port) 未就绪"
done

log "全部就绪：UI http://127.0.0.1:${HTTP_PORT}/ · MQ 管理台 :15672（guest/guest）· PG :5432"
log "跑一次演示扫描：curl -s -XPOST localhost:${HTTP_PORT}/api/scan -H 'content-type: application/json' -d '{\"scenario\":\"new_failure\",\"wait\":true}'"

# ------------------------------------------------------------------ 5) 前台守候
tail -n +1 -F "$LOG_DIR"/*.log &
TAIL_PID=$!
trap 'kill "$TAIL_PID" "${PIDS[@]}" 2>/dev/null || true' TERM INT

set +e
wait -n "${PIDS[@]}"
rc=$?
log "有进程退出（rc=${rc}），容器停止（日志见 $LOG_DIR/）"
exit "$rc"