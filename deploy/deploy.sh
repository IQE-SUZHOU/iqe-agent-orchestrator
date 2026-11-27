#!/usr/bin/env bash
# 构建 → 推 Harbor → 部署到 ci-triage（复用 infra-database 的 RabbitMQ / Postgres）
# 用法：bash deploy/deploy.sh        TAG=xxx bash deploy/deploy.sh
set -euo pipefail
cd "$(dirname "$0")/.."

PY="${PY:-../.venv/bin/python}"
NS=ci-triage

# 阶段名单从清单里反解，避免脚本与 Deployment 漂移（曾经就漂过一次：review → reviewer）。
STAGES="${STAGES:-$(grep -hoE 'command: \["ci-agent-[a-z]+"\]' deploy/k8s/21-agents.yaml |
                    sed -E 's/.*ci-agent-([a-z]+)".*/\1/' | sort -u | tr '\n' ' ')}"
[ -n "${STAGES// /}" ] || { echo "无法从 deploy/k8s/21-agents.yaml 解析阶段名单"; exit 1; }

# ── 凭据只从 deploy/secrets.env 读（该文件已 .gitignore，仓库里不留明文）──
SECRETS_ENV="${SECRETS_ENV:-deploy/secrets.env}"
if [ -f "$SECRETS_ENV" ]; then
  set -a; . "$SECRETS_ENV"; set +a
  echo "== 0) 已载入 $SECRETS_ENV"
fi
: "${PG_APP_USER:?缺少 PG_APP_USER：cp deploy/secrets.env.example deploy/secrets.env 后填真实值}"
: "${PG_APP_PASS:?缺少 PG_APP_PASS（本系统专属库角色口令）}"
: "${CI_APP_PASS:?缺少 CI_APP_PASS（编排层 + 8 个 Agent 共 9 个 MQ 账号共用的口令）}"
: "${RABBITMQ_ADMIN_PASS:?缺少 RABBITMQ_ADMIN_PASS}"
: "${HARBOR_USER:?缺少 HARBOR_USER}"
: "${HARBOR_PASS:?缺少 HARBOR_PASS}"
REGISTRY="${HARBOR_REGISTRY:-172.31.0.3:7777}"
# 镜像放在独立项目 ci-triage 下（不用 library：那是共享项目，且推送凭证会波及别人的镜像）
REPO="${REPO:-$REGISTRY/iqe-agent-orchestrator/iqe-agent-orchestrator}"
PG_CLUSTER_HOST="${PG_CLUSTER_HOST:-postgres.infra-database.svc.cluster.local}"
PG_PORT="${PG_PORT:-5432}"
AMQP_HOST="${AMQP_HOST:-rabbitmq.infra-database.svc.cluster.local}"
AMQP_PORT="${AMQP_PORT:-5672}"
# 本机直连 Postgres 只能走 ClusterIP 10.96.75.0（172.31.0.3:5432 被 pg_hba 拒绝）
# 业务一律用专属角色 PG_APP_USER；PG_ADMIN（超户）只在 ci_triage 库尚不存在时用来建库，可选。
PG_ADMIN="${PG_ADMIN:-postgresql://postgres:${PG_PASSWORD:-}@10.96.75.0:5432/ci_triage}"
# 写进 k8s Secret 的连接串（集群内 Service 域名）
PG_APP_DSN="${PG_APP_DSN:-postgresql://${PG_APP_USER}:${PG_APP_PASS}@${PG_CLUSTER_HOST}:${PG_PORT}/ci_triage}"
# 本机建库建表用的连接串（同一个库，走 ClusterIP）
PG_APP_DSN_LOCAL="${PG_APP_DSN_LOCAL:-postgresql://${PG_APP_USER}:${PG_APP_PASS}@10.96.75.0:5432/ci_triage}"
# 镜像 tag = 时间 + git 短 sha + 构建内容哈希。
# 内容哈希是必需的：同 tag 重推时节点上的旧镜像缓存不会失效（Harbor 同 tag 覆盖不通知 kubelet）。
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)$(git diff --quiet 2>/dev/null || echo -dirty)"
CONTENT_HASH="${CONTENT_HASH:-$(git ls-files -z 2>/dev/null | sort -z | xargs -0 -r sha256sum 2>/dev/null | sha256sum | cut -c1-12)}"
TAG="${TAG:-$(date +%Y%m%d-%H%M)-${GIT_SHA}-${CONTENT_HASH}}"

echo "== 1) 离线自检（不需要 broker / DB）"
$PY -m iqe_agents.tools.selfcheck
$PY -c "import iqe_orchestrator.app, iqe_orchestrator.graph, iqe_agents.runtime; print('import OK')"

# 防漂移：ConfigMap 里的计划步数上限必须 ≥ 最长内置 workflow。
# 曾经钉在 8 而 s2_full 有 11 步：线上每个长计划都被校验器 V1 判非法、Case 变 blocked，
# 本机却不读 ConfigMap（代码默认 24）测试全绿——只有部署前自检能挡住这类"线上配置滞后代码"。
WF_DIR="orchestrator/src/iqe_orchestrator/common/data/workflows"
longest=$(grep -c '^  - id:' "$WF_DIR"/*.yaml 2>/dev/null | awk -F: '{if ($2 > m) m = $2} END {print m + 0}')
cm_steps=$(sed -n 's/^[[:space:]]*MAX_PLAN_STEPS:[[:space:]]*"\([0-9]*\)".*/\1/p' deploy/k8s/00-infra.yaml)
if [ -n "$cm_steps" ] && [ "$longest" -gt 0 ] && [ "$longest" -gt "$cm_steps" ]; then
  echo "   ✗ ConfigMap MAX_PLAN_STEPS=$cm_steps < 最长内置 workflow $longest 步 → 长计划会被判非法，拒绝部署"
  exit 1
fi
echo "   计划步数上限 ${cm_steps:-（未设，用代码默认）} ≥ 最长内置 workflow $longest 步"

echo "== 2) 构建并推送镜像（$REPO，推送账号 $HARBOR_USER）"
printf '%s' "$HARBOR_PASS" | docker login "$REGISTRY" -u "$HARBOR_USER" --password-stdin >/dev/null
docker build -t "$REPO:$TAG" .
docker push "$REPO:$TAG"
echo "digest: $(docker inspect --format='{{index .RepoDigests 0}}' "$REPO:$TAG")"

echo "== 3) 前置：vhost /ci + 1 编排账号 + $(echo $STAGES | wc -w) 个 Agent 账号 + 目标数据库"
kubectl -n infra-database port-forward svc/rabbitmq 15690:15672 >/dev/null 2>&1 &
PF=$!; trap 'kill $PF 2>/dev/null || true' EXIT; sleep 2
$PY -m iqe_orchestrator.tools.setup_broker --mgmt http://127.0.0.1:15690
# 建库需要超户（库已存在时这步只是确认）；建表用专属角色，跑在本机 ClusterIP 连接上。
PG_DSN="$PG_ADMIN" $PY -c "import asyncio; from iqe_orchestrator.migrate import ensure_database; asyncio.run(ensure_database())"
PG_DSN="$PG_APP_DSN_LOCAL" $PY -m iqe_orchestrator.migrate

echo "== 4) 生成 Secret（凭证来源：deploy/secrets.env）"
kubectl -n "$NS" create secret docker-registry harbor-pull \
  --docker-server="$REGISTRY" --docker-username="$HARBOR_USER" --docker-password="$HARBOR_PASS" \
  --dry-run=client -o yaml | kubectl apply -f -
CREDS=$(mktemp)
{
  echo "PG_DSN=$PG_APP_DSN"
  echo "AMQP_ORCH=amqp://ci_orch:${CI_APP_PASS}@${AMQP_HOST}:${AMQP_PORT}/ci"
  for s in $STAGES; do
    echo "AMQP_${s^^}=amqp://ci_agent_${s}:${CI_APP_PASS}@${AMQP_HOST}:${AMQP_PORT}/ci"
  done
} > "$CREDS"
kubectl -n "$NS" create secret generic ci-triage-credentials --from-env-file="$CREDS" \
  --dry-run=client -o yaml | kubectl apply -f -
rm -f "$CREDS"

echo "== 5) 应用清单（替换 IMAGE_TAG）"
CMV=$(kubectl -n "$NS" get cm ci-triage-config -o jsonpath='{.metadata.resourceVersion}' 2>/dev/null || echo none)
SECV=$(kubectl -n "$NS" get secret ci-triage-credentials -o jsonpath='{.metadata.resourceVersion}' 2>/dev/null || echo none)
TMP=$(mktemp -d)
for f in deploy/k8s/*.yaml; do sed "s|IMAGE_TAG|$TAG|g" "$f" > "$TMP/$(basename "$f")"; done
kubectl apply -f "$TMP/00-infra.yaml"
kubectl apply -f "$TMP/20-orchestrator.yaml"
kubectl apply -f "$TMP/21-agents.yaml"
# 先清掉上一代遗留的 Agent Deployment：阶段改名（review → reviewer、ticket → ticketing）后，
# 旧 Deployment 会拿新镜像跑已经不存在的模块，CrashLoop 之外还会抢旧队列。
for d in $(kubectl -n "$NS" get deploy -l app=ci-triage-agent -o jsonpath='{.items[*].metadata.name}'); do
  stage="${d#agent-}"
  case " $STAGES " in
    *" $stage "*) ;;
    *) echo "  删除遗留 Deployment $d"; kubectl -n "$NS" delete deploy "$d" --wait=false ;;
  esac
done

# ConfigMap / Secret 变了不会自动重启 Pod：改了 MAX_PLAN_STEPS 这类上限或轮换口令后，
# 线上进程还带着旧环境变量继续跑。这里比对 apply 前后的 resourceVersion，变了就滚动一次。
CMV2=$(kubectl -n "$NS" get cm ci-triage-config -o jsonpath='{.metadata.resourceVersion}' 2>/dev/null || echo none)
SECV2=$(kubectl -n "$NS" get secret ci-triage-credentials -o jsonpath='{.metadata.resourceVersion}' 2>/dev/null || echo none)
if [ "$CMV" != "$CMV2" ] || [ "$SECV" != "$SECV2" ]; then
  echo "  ConfigMap/Secret 有变更（$CMV→$CMV2 / $SECV→$SECV2）→ 滚动重启，让新环境变量生效"
  kubectl -n "$NS" rollout restart deployment
fi

kubectl -n "$NS" rollout status deployment/orchestrator --timeout=240s
for s in $STAGES; do
  kubectl -n "$NS" rollout status deployment/agent-"$s" --timeout=240s
done

echo "== 6) 当前镜像（回滚锚点，请记下 digest）"
kubectl -n "$NS" get deploy -o custom-columns=NAME:.metadata.name,IMAGE:.spec.template.spec.containers[0].image
echo "UI  http://172.31.0.3:30099/"
echo "API http://172.31.0.3:30099/api/cases"
echo "跑端到端：set -a; . deploy/secrets.env; set +a; ORCH_BASE_URL=http://172.31.0.3:30099 \$PY -m pytest -m integration -q"
echo "（PG_DSN 由 secrets.env 提供，已指向专属角色 $PG_APP_USER，不再用超户）"
