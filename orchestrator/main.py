"""编排层进程入口：uvicorn 承载 API + UI，后台跑 LangGraph 引擎 / 网关 / 调度器。"""
from __future__ import annotations

import asyncio
import logging
import signal

import uvicorn

from agentkit.config import cfg

from .app import app
from .migrate import ensure_database

log = logging.getLogger("orchestrator.main")


async def bootstrap() -> None:
    await ensure_database()


def main() -> None:
    cfg.svc = "orchestrator"
    asyncio.run(bootstrap())
    uvicorn.run(app, host="0.0.0.0", port=cfg.http_port, log_level="warning", access_log=False)


if __name__ == "__main__":
    main()
