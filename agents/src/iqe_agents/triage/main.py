"""triage Agent 入口（Mock）。运行方式：ci-agent-triage（console script）或 python -m iqe_agents.triage.main"""

from __future__ import annotations

import asyncio
import logging

from iqe_agents.common.config import cfg
from iqe_agents.runtime import AgentRuntime

from .playbook import handler

log = logging.getLogger("agent.triage")


def main() -> int:
    cfg.svc = cfg.svc if cfg.svc != "unknown" else "agent-triage"
    log.info("agent_main stage=triage svc=" + cfg.svc)
    asyncio.run(AgentRuntime("triage", handler).run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
