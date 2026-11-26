"""ticket Agent 入口（Mock）。运行方式：ci-agent-ticket（console script）或 python -m iqe_agents.ticket.main"""

from __future__ import annotations

import asyncio
import logging

from iqe_agents.common.config import cfg
from iqe_agents.runtime import AgentRuntime

from .playbook import handler

log = logging.getLogger("agent.ticket")


def main() -> int:
    cfg.svc = cfg.svc if cfg.svc != "unknown" else "agent-ticket"
    log.info("agent_main stage=ticket svc=" + cfg.svc)
    asyncio.run(AgentRuntime("ticket", handler).run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
