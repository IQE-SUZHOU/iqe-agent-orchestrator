"""monitor Agent 入口（Mock）。运行方式：python -m agents.monitor.main"""
from __future__ import annotations

import asyncio
import logging

from agentkit.agent_runtime import AgentRuntime
from agentkit.config import cfg

from .playbook import handler

log = logging.getLogger("agent.monitor")

if __name__ == "__main__":
    cfg.svc = cfg.svc if cfg.svc != "unknown" else "agent-monitor"
    log.info("agent_main stage=monitor svc=" + cfg.svc)
    asyncio.run(AgentRuntime("monitor", handler).run())
