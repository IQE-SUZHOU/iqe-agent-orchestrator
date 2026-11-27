# 全自包含演示镜像：Postgres + RabbitMQ + 编排层 + 8 个 Agent 装在同一镜像里。
#
# 用途：给实现人员/评审「docker run 一条命令就能跑完整 12 阶段流水线」的环境，
# 不需要连集群里的 infra-database（PG / MQ 都在容器内）。
#
#   docker build -f deploy/allinone/Dockerfile -t ci-triage-allinone:dev .
#   docker run -d -p 18080:8080 -p 15672:15672 --name ci-allinone ci-triage-allinone:dev
#
# 注意：镜像内跑的是 mock 剧本 + 演示口令（默认 ci-demo-pass），仅供演示/参考，
# 不要当作生产镜像使用（生产请用根目录 Dockerfile：无状态进程 + 外部 PG/MQ）。
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8

# postgresql：内置库（默认 15）；rabbitmq-server：内置 broker；curl 用于就绪探针
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql rabbitmq-server curl procps tini \
    && rm -rf /var/lib/apt/lists/* \
    && rabbitmq-plugins enable --offline rabbitmq_management

WORKDIR /build

# 先只拷打包所需文件：改代码不会让依赖安装层失效
COPY orchestrator/pyproject.toml orchestrator/MANIFEST.in orchestrator/README.md ./orchestrator/
COPY orchestrator/src ./orchestrator/src
COPY agents/pyproject.toml agents/MANIFEST.in agents/README.md ./agents/
COPY agents/src ./agents/src
RUN python -m pip install --no-cache-dir ./orchestrator ./agents && rm -rf /build

# 运行用户：9 个 python 进程用 10001（与根 Dockerfile 的 UID 保持一致）；
# postgres / rabbitmq 由各自的系统用户跑（entrypoint 里用 runuser 切）
RUN useradd -r -u 10001 appuser \
    && mkdir -p /var/lib/ci-triage/plans /var/log/ci-triage \
    && chown -R appuser:appuser /var/lib/ci-triage /var/log/ci-triage

COPY deploy/allinone/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# 演示口令：可用 -e CI_DEMO_PASS=xxx 覆盖；PG 口令同源（容器内只监听 127.0.0.1）
ENV CI_DEMO_PASS=ci-demo-pass \
    HTTP_PORT=8080 \
    PLANS_DIR=/var/lib/ci-triage/plans \
    MOCK_SCENARIO=default \
    MOCK_LLM_ENABLED=true \
    LOG_JSON=false \
    LOG_LEVEL=info \
    REPORT_TZ=UTC \
    SCHEDULER_ENABLED=true \
    MONITOR_SCAN_INTERVAL_S=3600 \
    SCAN_STARTUP_DELAY_S=3600 \
    STAGE_TIMEOUT_S=30 \
    STAGE_MAX_ATTEMPTS=2 \
    MAX_REVIEW_LOOPS=2

EXPOSE 8080 5432 5672 15672 15692

# tini 做 PID 1：容器里的 postgres/rabbitmq 都是后台子进程，需要有人收尸
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]