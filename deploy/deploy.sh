#!/usr/bin/env bash
# 构建 → 推 Harbor → 部署到 ci-triage（复用 infra-database 的 RabbitMQ / Postgres）
# 用法：bash deploy/deploy.sh        TAG=xxx bash deploy/deploy.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PY="${PY:-../.venv/bin/python}"
NS=ci-triage

# ── 凭据只从 deploy/secrets.env 读（该文件已 .gitignore，仓库里不留明文）──
SECRETS_ENV="${SECRETS_ENV:-deploy/secrets.env}"
if [ -f "$SECRETS_ENV" ]; then
  set -a; . "$SECRETS_ENV"; set +a
  echo "== 0) 已载入 $SECRETS_ENV"
fi
: "${PG_PASSWORD:?缺少 PG_PASSWORD：cp deploy/secrets.env.example deploy/secrets.env 后填真实值}"
: "${CI_APP_PASS:?缺少 CI_APP_PASS（本系统 7 个 MQ 账号共用的口令）}"
: "${RABBITMQ_ADMIN_PASS:?缺少 RABBITMQ_ADMIN_PASS}"
: "${HARBOR_USER:?缺少 HARBOR_USER}"
: "${HARBOR_PASS:?缺少 HARBOR_PASS}"
REGISTRY="${HARBOR_REGISTRY:-172.31.0.3:7777}"
REPO="${REPO:-$REGISTRY/library/ci-triage}"
PG_CLUSTER_HOST="${PG_CLUSTER_HOST:-postgres.infra-database.svc.cluster.local}"
PG_PORT="${PG_PORT:-5432}"
AMQP_HOST="${AMQP_HOST:-rabbitmq.infra-database.svc.cluster.local}"
AMQP_PORT="${AMQP_PORT:-5672}"
# 本机直连 Postgres 只能走 ClusterIP 10.96.75.0（172.31.0.3:5432 被 pg_hba 拒绝）
PG_ADMIN="${PG_ADMIN:-postgresql://postgres:${PG_PASSWORD}@10.96.75.0:5432/ci_triage}"
# 镜像 tag = 时间 + git 短 sha + 构建内容哈希。
# 内容哈希是必需的：同 tag 重推时节点上的旧镜像缓存不会失效（Harbor 同 tag 覆盖不通知 kubelet）。
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)$(git diff --quiet 2>/dev/null || echo -dirty)"
CONTENT_HASH="${CONTENT_HASH:-$(git ls-files -z 2>/dev/null | sort -z | xargs -0 -r sha256sum 2>/dev/null | sha256sum | cut -c1-12)}"
TAG="${TAG:-$(date +%Y%m%d-%H%M)-${GIT_SHA}-${CONTENT_HASH}}"

echo "== 1) 离线自检（不需要 broker / DB）"
$PY -m iqe_agents.tools.selfcheck
$PY -c "import iqe_orchestrator.app, iqe_orchestrator.graph, iqe_agents.runtime; print('import OK')"

echo "== 2) 构建并推送镜像"
docker build -t "$REPO:$TAG" .
docker push "$REPO:$TAG"
echo "digest: $(docker inspect --format='{{index .RepoDigests 0}}' "$REPO:$TAG")"

echo "== 3) 前置：vhost /ci + 7 个账号 + 目标数据库"
kubectl -n infra-database port-forward svc/rabbitmq 15690:15672 >/dev/null 2>&1 &
PF=$!; trap 'kill $PF 2>/dev/null || true' EXIT; sleep 2
$PY -m iqe_orchestrator.tools.setup_broker --mgmt http://127.0.0.1:15690
PG_DSN="$PG_ADMIN" $PY -c "import asyncio; from iqe_orchestrator.migrate import ensure_database; asyncio.run(ensure_database())"

echo "== 4) 生成 Secret（凭证来源：deploy/secrets.env）"
kubectl -n "$NS" create secret docker-registry harbor-pull \
  --docker-server="$REGISTRY" --docker-username="$HARBOR_USER" --docker-password="$HARBOR_PASS" \
  --dry-run=client -o yaml | kubectl apply -f -
CREDS=$(mktemp)
{
  echo "PG_DSN=postgresql://postgres:${PG_PASSWORD}@${PG_CLUSTER_HOST}:${PG_PORT}/ci_triage"
  echo "AMQP_ORCH=amqp://ci_orch:${CI_APP_PASS}@${AMQP_HOST}:${AMQP_PORT}/ci"
  for s in monitor triage reproduction ticket review reporting; do
    echo "AMQP_${s^^}=amqp://ci_agent_${s}:${CI_APP_PASS}@${AMQP_HOST}:${AMQP_PORT}/ci"
  done
} > "$CREDS"
kubectl -n "$NS" create secret generic ci-triage-credentials --from-env-file="$CREDS" \
  --dry-run=client -o yaml | kubectl apply -f -
rm -f "$CREDS"

echo "== 5) 应用清单（替换 IMAGE_TAG）"
TMP=$(mktemp -d)
for f in deploy/k8s/*.yaml; do sed "s|IMAGE_TAG|$TAG|g" "$f" > "$TMP/$(basename "$f")"; done
kubectl apply -f "$TMP/00-infra.yaml"
kubectl apply -f "$TMP/20-orchestrator.yaml"
kubectl apply -f "$TMP/21-agents.yaml"
kubectl -n "$NS" rollout status deployment/orchestrator --timeout=240s
for s in monitor triage reproduction ticket review reporting; do
  kubectl -n "$NS" rollout status deployment/agent-"$s" --timeout=240s
done

echo "== 6) 当前镜像（回滚锚点，请记下 digest）"
kubectl -n "$NS" get deploy -o custom-columns=NAME:.metadata.name,IMAGE:.spec.template.spec.containers[0].image
echo "UI  http://172.31.0.3:30099/"
echo "API http://172.31.0.3:30099/api/cases"
echo "跑端到端：ORCH_BASE_URL=http://172.31.0.3:30099 PG_DSN=$PG_ADMIN $PY -m pytest tests -q"
