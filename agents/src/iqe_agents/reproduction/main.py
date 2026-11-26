"""reproduction Agent 入口（Mock）。运行方式：ci-agent-reproduction（console script）或 python -m iqe_agents.reproduction.main"""

from __future__ import annotations

import asyncio
import logging

from iqe_agents.common.config import cfg
from iqe_agents.runtime import AgentRuntime

from .playbook import handler

log = logging.getLogger("agent.reproduction")


def main() -> int:
    cfg.svc = cfg.svc if cfg.svc != "unknown" else "agent-reproduction"
    log.info("agent_main stage=reproduction svc=" + cfg.svc)
    asyncio.run(AgentRuntime("reproduction", handler).run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
