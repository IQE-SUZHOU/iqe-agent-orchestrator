"""健康与指标端点：/healthz /readyz /metrics（每个部署单元都起一个）。"""

from __future__ import annotations

import asyncio
import logging

import uvicorn
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from .config import cfg
from .logs import counters

log = logging.getLogger("ci_triage.common.health")


def build_app(*, probes: dict | None = None, title: str = "") -> FastAPI:
    app = FastAPI(title=title or cfg.svc, docs_url="/docs")
    state = {"ready": False, "probes": probes or {}}

    @app.get("/healthz")
    async def healthz():
        return {
            "status": "ok",
            "svc": cfg.svc,
            "scenario": cfg.mock_scenario,
            "ready": state["ready"],
        }

    @app.get("/readyz")
    async def readyz():
        if not state["ready"]:
            return PlainTextResponse("not ready", status_code=503)
        return {"status": "ready", **state["probes"]}

    @app.get("/metrics")
    async def metrics():
        return PlainTextResponse(counters.render(), media_type="text/plain")

    @app.on_event("startup")
    async def _mark():
        state["ready"] = True

    return app


async def serve(app: FastAPI) -> asyncio.Task:
    config = uvicorn.Config(
        app, host="0.0.0.0", port=cfg.http_port, log_level="warning", access_log=False
    )
    server = uvicorn.Server(config)
    loop = asyncio.get_event_loop()
    task = loop.create_task(server.serve())

    def _stop(*_):
        server.should_exit = True

    serve._server = server  # 供关停复用
    return task
