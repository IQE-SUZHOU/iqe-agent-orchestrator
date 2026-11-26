"""triage Agent 入口（Mock）。运行方式：python -m agents.triage.main"""
from __future__ import annotations

import asyncio
import logging

from agentkit.agent_runtime import AgentRuntime
from agentkit.config import cfg

from .playbook import handler

log = logging.getLogger("agent.triage")

if __name__ == "__main__":
    cfg.svc = cfg.svc if cfg.svc != "unknown" else "agent-triage"
    log.info("agent_main stage=triage svc=" + cfg.svc)
    asyncio.run(AgentRuntime("triage", handler).run())
