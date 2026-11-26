FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY agentkit /app/agentkit
COPY orchestrator /app/orchestrator
COPY agents /app/agents
COPY schemas /app/schemas
COPY workflows /app/workflows
COPY fixtures /app/fixtures
COPY tools /app/tools

RUN useradd -r -u 10001 appuser && mkdir -p /var/lib/ci-triage && chown -R appuser:appuser /app /var/lib/ci-triage
# runAsNonRoot: true 时 kubelet 必须能验证「运行用户不是 root」：
# USER 写用户名（appuser）会被拒绝——"image has non-numeric user (appuser)"。写 UID。
USER 10001

EXPOSE 8080
# 同一个镜像、不同 command = 7 个独立部署单元（编排层 / 6 个 Agent）
CMD ["python", "-m", "orchestrator.main"]
