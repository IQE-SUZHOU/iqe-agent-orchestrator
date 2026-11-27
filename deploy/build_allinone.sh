#!/usr/bin/env bash
# 构建并推送「全自包含演示镜像」（Postgres + RabbitMQ + 编排层 + 8 Agent）。
#
# 用法：
#   bash deploy/build_allinone.sh              # 构建 + 推送
#   TAG=dev-001 bash deploy/build_allinone.sh  # 指定 tag
#   PUSH=0 bash deploy/build_allinone.sh       # 只构建不推送
#
# 与 deploy/deploy.sh 的区别：deploy.sh 部署的是「无状态进程 + 外部 PG/MQ」的生产口径；
# 这个脚本只产出演示镜像，不改动集群里的任何工作负载。
set -euo pipefail
cd "$(dirname "$0")/.."

SECRETS_ENV="${SECRETS_ENV:-deploy/secrets.env}"
if [ -f "$SECRETS_ENV" ]; then
  set -a; . "$SECRETS_ENV"; set +a
fi
REGISTRY="${HARBOR_REGISTRY:-172.31.0.3:7777}"
REGISTRY="${REGISTRY#http://}"; REGISTRY="${REGISTRY#https://}"
REPO="${ALLINONE_REPO:-$REGISTRY/iqe-agent-orchestrator/iqe-agent-orchestrator-allinone}"
# 防呆：本脚本产出的 all-in-one 镜像（内含 Postgres + RabbitMQ，约 1GB）绝不能进生产主镜像仓库。
# 曾经 secrets.env 里一个共用的 REPO 变量让下面的默认值失效，把演示镜像推成了生产 latest。
case "$REPO" in
  *-allinone) ;;
  *)
    echo "✗ ALLINONE_REPO 指向 $REPO，不是 *-allinone 仓库 → 拒绝推送（会覆盖生产主镜像）"
    exit 1 ;;
esac
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)$(git diff --quiet 2>/dev/null || echo -dirty)"
TAG="${TAG:-$(date +%Y%m%d-%H%M)-${GIT_SHA}}"
PUSH="${PUSH:-1}"

echo "== 镜像：$REPO:$TAG"
echo "== 1) 构建（约 3-6 分钟：apt 装 postgresql + rabbitmq-server 是主要耗时）"
docker build -f deploy/allinone/Dockerfile -t "$REPO:$TAG" .

if [ "$PUSH" = "0" ]; then
  echo "== 跳过推送（PUSH=0）"
  exit 0
fi

: "${HARBOR_USER:?缺少 HARBOR_USER（deploy/secrets.env）}"
: "${HARBOR_PASS:?缺少 HARBOR_PASS（deploy/secrets.env）}"
echo "== 2) 推送 Harbor（$REGISTRY，HTTP + insecure registry）"
printf '%s' "$HARBOR_PASS" | docker login "$REGISTRY" -u "$HARBOR_USER" --password-stdin
docker push "$REPO:$TAG"
docker tag "$REPO:$TAG" "$REPO:latest"
docker push "$REPO:latest"

echo "== 3) 完成"
docker inspect --format='{{index .RepoDigests 0}}' "$REPO:$TAG" || true
echo "拉取：docker pull $REPO:$TAG"
echo "运行：docker run -d --name ci-allinone -p 18080:8080 -p 15672:15672 $REPO:$TAG"