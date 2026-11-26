"""review Agent 入口（Mock）。运行方式：python -m agents.review.main"""
from __future__ import annotations

import asyncio
import logging

from agentkit.agent_runtime import AgentRuntime
from agentkit.config import cfg

from .playbook import handler

log = logging.getLogger("agent.review")

if __name__ == "__main__":
    cfg.svc = cfg.svc if cfg.svc != "unknown" else "agent-review"
    log.info("agent_main stage=review svc=" + cfg.svc)
    asyncio.run(AgentRuntime("review", handler).run())
