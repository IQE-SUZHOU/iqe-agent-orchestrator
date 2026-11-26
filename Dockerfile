# 一份镜像装两个子项目：orchestrator + agents，靠不同 console script 起 7 个进程。
# 需要真正隔离时，可分别用 orchestrator/ 或 agents/ 单独构建（各自都带一份 common 层）。
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# 先只拷打包所需文件：改代码不会让依赖安装层失效
COPY orchestrator/pyproject.toml orchestrator/MANIFEST.in orchestrator/README.md ./orchestrator/
COPY orchestrator/src ./orchestrator/src
COPY agents/pyproject.toml agents/MANIFEST.in agents/README.md ./agents/
COPY agents/src ./agents/src
RUN python -m pip install --no-cache-dir ./orchestrator ./agents

RUN useradd -r -u 10001 appuser \
    && mkdir -p /var/lib/ci-triage \
    && chown -R appuser:appuser /var/lib/ci-triage
# runAsNonRoot: true 时 kubelet 必须能验证「运行用户不是 root」：
# USER 写用户名（appuser）会被拒绝——"image has non-numeric user (appuser)"。写 UID。
USER 10001

EXPOSE 8080
# 默认是编排层；6 个 Agent 由 Deployment 覆盖 command（ci-agent-monitor / ci-agent-triage / …）
CMD ["ci-orchestrator"]
