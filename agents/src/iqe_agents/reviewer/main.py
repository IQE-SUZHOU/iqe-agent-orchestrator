"""reviewer Agent 入口（Mock）。运行方式：ci-agent-reviewer（console script）或 python -m iqe_agents.reviewer.main"""

from __future__ import annotations

import asyncio
import logging

from iqe_agents.common.config import cfg
from iqe_agents.runtime import AgentRuntime

from .playbook import handler

log = logging.getLogger("agent.reviewer")


def main() -> int:
    cfg.svc = cfg.svc if cfg.svc != "unknown" else "agent-reviewer"
    log.info("agent_main stage=reviewer svc=" + cfg.svc)
    asyncio.run(AgentRuntime("reviewer", handler).run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
