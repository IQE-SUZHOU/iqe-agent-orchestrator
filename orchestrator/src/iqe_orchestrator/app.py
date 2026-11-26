"""编排层服务入口：LangGraph 引擎 + 总线网关 + Admin API + Web UI。"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel

from iqe_orchestrator.common import bus as bus_mod
from iqe_orchestrator.common.config import cfg
from iqe_orchestrator.common.db import DB
from iqe_orchestrator.common.envelope import make_command, now_iso, ulid
from iqe_orchestrator.common.logs import counters, log_with, setup_logging
from iqe_orchestrator.common.mock import Fixtures

from .dispatch import BusGateway
from .graph import CaseEngine
from .plan import LLMPlanner, PlanResolver, PlanStore
from .reporting import trigger_daily
from .scan import ScanRunner
from .scheduler import Scheduler

log = logging.getLogger("orchestrator.app")
UI_DIR = Path(__file__).resolve().parent / "ui"


class ScanRequest(BaseModel):
    scenario: str | None = None
    lookback: str = "24h"
    jobs_filter: list[str] | None = None
    wait: bool = True


class DailyRequest(BaseModel):
    day: str | None = None


class InjectRequest(BaseModel):
    kind: str  # bad_command | duplicate_command
    stage: str = "review"
    mode: str = "run"
    case_id: str | None = None
    step_id: str = "x1"
    attempt: int = 1
    publish_times: int = 2


class Runtime:
    def __init__(self) -> None:
        self.db: DB | None = None
        self.bus: bus_mod.Bus | None = None
        self.gw: BusGateway | None = None
        self.resolver: PlanResolver | None = None
        self.planner: LLMPlanner | None = None
        self.store: PlanStore | None = None
        self.engine: CaseEngine | None = None
        self.runner: ScanRunner | None = None
        self.scheduler: Scheduler | None = None
        self.saver: AsyncPostgresSaver | None = None
        self.ckpt_pool: AsyncConnectionPool | None = None
        self.started_at = now_iso()


RT = Runtime()


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    from .migrate import ensure_ready

    RT.db = await DB.create()
    await RT.db.init_schema()
    await ensure_ready(RT.db)
    await bus_mod.wait_ready(timeout=90)
    RT.bus = await bus_mod.connect(declare=True, prefetch=cfg.orch_prefetch)
    RT.gw = BusGateway(RT.bus, RT.db)
    await RT.gw.start()
    RT.resolver = PlanResolver()
    n = RT.resolver.load_registry()
    for wf in RT.resolver.workflows.values():
        await RT.db.upsert_workflow(
            key=wf.key,
            title=wf.title,
            enabled=wf.enabled,
            priority=wf.priority,
            match=wf.match,
            steps=wf.steps,
            source="file",
            version=wf.version,
        )
    RT.planner = LLMPlanner()
    RT.store = PlanStore()
    RT.ckpt_pool = AsyncConnectionPool(
        conninfo=cfg.pg_dsn,
        min_size=1,
        max_size=6,
        open=False,
        name="langgraph-checkpoint",
        kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row},
    )
    await RT.ckpt_pool.open(wait=True, timeout=30)
    RT.saver = AsyncPostgresSaver(RT.ckpt_pool)
    await RT.saver.setup()
    RT.engine = CaseEngine(RT.db, RT.gw, RT.resolver, RT.planner, RT.store, RT.saver)
    RT.runner = ScanRunner(RT.db, RT.gw, RT.engine)
    RT.scheduler = Scheduler(RT.runner, RT.db)
    await RT.scheduler.start()
    RT.ready = True  # /readyz 的门闩：k8s readinessProbe 打的就是 /readyz
    log_with(
        log,
        "info",
        "orchestrator_ready",
        workflows=n,
        plan_mode=cfg.plan_mode,
        pg=cfg.pg_dsn.split("@")[-1],
        amqp=cfg.amqp_url.split("@")[-1],
    )
    yield
    await RT.scheduler.stop_all()
    await RT.engine.drain()
    await RT.gw.stop()
    await RT.bus.close()
    await RT.ckpt_pool.close()
    await RT.db.close()


app = FastAPI(title="CI Triage Orchestrator", lifespan=lifespan)
templates = Jinja2Templates(directory=str(UI_DIR / "templates"))


# --------------------------------------------------------------------------- 健康
@app.get("/healthz")
async def healthz():
    return {
        "status": "ok",
        "svc": "orchestrator",
        "started_at": RT.started_at,
        "plan_mode": cfg.plan_mode,
        "scheduler_enabled": cfg.scheduler_enabled,
    }


@app.get("/readyz")
async def readyz():
    """readiness：队列声明完成、DB 迁移完成、网关与计划层就位才算 ready。

    k8s 的 readinessProbe 与 tools/run_local.sh 都依赖这个端点；缺它会让
    Deployment 永远不 Ready（历史坑：app.py 早先只有 /healthz）。
    """
    wired = (
        bool(getattr(RT, "ready", False))
        and bool(getattr(RT, "engine", None))
        and bool(getattr(RT, "gw", None))
        and bool(getattr(RT, "bus", None))
    )
    if not wired:
        return JSONResponse(status_code=503, content={"status": "starting", "svc": "orchestrator"})
    return {
        "status": "ready",
        "svc": "orchestrator",
        "workflows": await RT.db.count_workflows(),
        "plan_mode": cfg.plan_mode,
    }


@app.get("/metrics")
async def metrics():
    return PlainTextResponse(counters.render(), media_type="text/plain")


# --------------------------------------------------------------------------- 扫描 / 日报
@app.post("/api/scan")
async def api_scan(req: ScanRequest):
    res = await RT.runner.run_scan(
        scenario=req.scenario,
        lookback=req.lookback,
        jobs_filter=req.jobs_filter,
        wait_cases=req.wait,
    )
    return res


@app.get("/api/scans")
async def api_scans(limit: int = 30):
    return {"items": await RT.db.list_scans(limit)}


@app.get("/api/scenarios")
async def api_scenarios():
    fx = Fixtures()
    names = (
        sorted(p.stem for p in fx.path("ci_feed").glob("*.json")) if fx.exists("ci_feed") else []
    )
    return {
        "feed_scenarios": names,
        "agent_scenarios": {
            "triage": ["default", "unknown_low_confidence"],
            "reproduction": [
                "default",
                "flaky",
                "not_reproducible",
                "blocked_no_machine",
                "timeout",
            ],
            "ticket": ["default", "duplicate"],
            "review": ["default", "needs_info", "reject", "insufficient"],
        },
        "note": "source_unavailable 场景故意没有夹具文件，用于验证 Monitor 的 blocked 路径",
    }


@app.post("/api/report/daily")
async def api_daily(req: DailyRequest):
    return await trigger_daily(RT.gw, RT.db, day=req.day)


@app.get("/api/daily-report")
async def api_daily_get(day: str | None = None):
    from iqe_orchestrator.common.dates import today_str

    day = day or today_str()  # 缺省取 REPORT_TZ 下的今天
    row = await RT.db.get_daily_report(day)
    if not row:
        raise HTTPException(404, f"没有 {day} 的日报")
    return row


# --------------------------------------------------------------------------- Case 查询
@app.get("/api/cases")
async def api_cases(
    status: str | None = None,
    signal: str | None = None,
    plan_source: str | None = None,
    limit: int = 100,
):
    items = await RT.db.list_cases(
        status=status, signal=signal, plan_source=plan_source, limit=limit
    )
    for c in items:
        c["progress"] = c.get("progress") or {}
    return {"items": items, "count": len(items)}


@app.get("/api/cases/{case_id}")
async def api_case(case_id: str):
    case = await RT.db.get_case(case_id)
    if not case:
        raise HTTPException(404, "case_not_found")
    plan = await RT.db.get_plan(case_id)
    trace = await RT.db.trace(case_id)
    ev = await RT.db.evidence(case_id)
    return {"case": case, "plan": plan, "trace": trace, "evidence": ev}


@app.get("/api/cases/{case_id}/trace")
async def api_case_trace(case_id: str):
    if not await RT.db.get_case(case_id):
        raise HTTPException(404, "case_not_found")
    return {"items": await RT.db.trace(case_id)}


@app.get("/api/cases/{case_id}/plan")
async def api_case_plan(case_id: str):
    row = await RT.db.get_plan(case_id)
    if not row:
        raise HTTPException(404, "plan_not_found")
    return row


@app.get("/api/cases/{case_id}/plan.yaml", response_class=PlainTextResponse)
async def api_case_plan_yaml(case_id: str):
    row = await RT.db.get_plan(case_id)
    if not row:
        raise HTTPException(404, "plan_not_found")
    text = RT.store.read(case_id, row["plan_id"])
    if text is None:
        raise HTTPException(404, "plan_file_missing")
    return PlainTextResponse(text, media_type="application/yaml")


@app.post("/api/cases/{case_id}/replay")
async def api_case_replay(case_id: str):
    res = await RT.engine.replay_case(case_id)
    if not res.get("ok"):
        raise HTTPException(404, res.get("reason", "replay_failed"))
    await asyncio.sleep(0.2)
    await RT.engine.drain()
    return res


@app.get("/api/cases/{case_id}/evidence")
async def api_case_evidence(case_id: str):
    return {"items": await RT.db.evidence(case_id)}


# --------------------------------------------------------------------------- 计划 / workflow / DLQ
@app.get("/api/plans")
async def api_plans(limit: int = 50):
    return {"items": await RT.db.list_plans(limit)}


@app.get("/api/workflows")
async def api_workflows():
    return {
        "items": await RT.db.list_workflows(),
        "registry_dir": cfg.workflows_dir,
        "mode_rules": {
            "triage": ["full", "classify_only"],
            "reproduction": ["run"],
            "ticket": ["run"],
            "review": ["run"],
            "reporting": ["case", "daily"],
        },
    }


@app.get("/api/dlq")
async def api_dlq(limit: int = 20):
    async with RT.db.pool.acquire() as conn:
        rows = await conn.fetch(
            "select id, stage, idempotency_key, case_id, reason, left(body,600) as body,"
            " created_at from dlq order by id desc limit $1",
            limit,
        )
    return {"items": [dict(r) for r in rows]}


@app.get("/api/stats")
async def api_stats():
    async with RT.db.pool.acquire() as conn:
        cases = await conn.fetch(
            "select status, signal, plan_source, count(*) n from cases group by 1,2,3"
        )
        stages = await conn.fetch(
            "select stage, status, count(*) n, avg(latency_ms) avg_ms from stage_runs"
            " group by 1,2 order by 1"
        )
        tickets = await conn.fetch("select ticket_status, count(*) n from tickets group by 1")
        reviews = await conn.fetch("select verdict, count(*) n from reviews group by 1")
        plans = await conn.fetch("select source, degraded, count(*) n from plans group by 1,2")
    return {
        "cases": [dict(r) for r in cases],
        "stages": [dict(r) for r in stages],
        "tickets": [dict(r) for r in tickets],
        "reviews": [dict(r) for r in reviews],
        "plans": [dict(r) for r in plans],
        "counters": counters.render(),
    }


# --------------------------------------------------------------------------- 故障注入（演示幂等与 DLQ）
@app.post("/api/ops/inject")
async def api_inject(req: InjectRequest):
    case_id = req.case_id or f"inject-{ulid()[:12]}"
    payload = {
        "mode": req.mode if req.kind == "duplicate_command" else "bogus_mode",
        "input": {
            "case_id": case_id,
            "source": "mock",
            "job": "inject-job",
            "signal": "error",
            "rca": "test",
            "confidence": 0.7,
            "key_frames": ["frame-1"],
            "repro_attempted": False,
            "evidence_count": 1,
            "loop_seq": 1,
        },
    }
    env = make_command(
        stage=req.stage,
        action="run",
        case_id=case_id,
        run_id=f"inject-{ulid()[:8]}",
        step_id=req.step_id,
        attempt=req.attempt,
        payload=payload,
        mode=payload["mode"],
        scenario_hint="default",
    )
    env["idempotency_key"] = f"{case_id}:{req.step_id}:{req.attempt}"
    await RT.gw.publish_command_only(env)
    times = req.publish_times if req.kind == "duplicate_command" else 1
    for _ in range(times - 1):
        await RT.gw.publish_command_only(env)
    await asyncio.sleep(2.5)
    async with RT.db.pool.acquire() as conn:
        ev = await conn.fetchval("select count(*) from evidence where case_id=$1", case_id)
        dlq = await conn.fetchval(
            "select count(*) from dlq where idempotency_key=$1", env["idempotency_key"]
        )
    return {
        "idempotency_key": env["idempotency_key"],
        "case_id": case_id,
        "published": times,
        "evidence_rows": ev,
        "dlq_rows": dlq,
    }


# --------------------------------------------------------------------------- Web UI
@app.get("/", response_class=HTMLResponse)
async def ui_index(request: Request):
    cases = await RT.db.list_cases(limit=120)
    scans = await RT.db.list_scans(8)
    workflows = await RT.db.list_workflows()
    async with RT.db.pool.acquire() as conn:
        plans_n = await conn.fetchval("select count(*) from plans")
        dlq_n = await conn.fetchval("select count(*) from dlq")
        ev_n = await conn.fetchval("select count(*) from evidence")
        runs_n = await conn.fetchval("select count(*) from stage_runs")
    scenarios = sorted(p.stem for p in Fixtures().path("ci_feed").glob("*.json"))
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "request": request,
            "cases": cases,
            "scans": scans,
            "workflows": workflows,
            "stats": {
                "plans": plans_n,
                "dlq": dlq_n,
                "evidence": ev_n,
                "stage_runs": runs_n,
                "cases": len(cases),
            },
            "scenarios": scenarios,
            "cfg": {
                "plan_mode": cfg.plan_mode,
                "scan_interval": cfg.monitor_scan_interval_s,
                "repro_enabled": cfg.repro_enabled,
                "ticket_autocreate": cfg.ticket_autocreate,
            },
        },
    )


@app.get("/ui/cases/{case_id}", response_class=HTMLResponse)
async def ui_case(request: Request, case_id: str):
    case = await RT.db.get_case(case_id)
    if not case:
        raise HTTPException(404, "case_not_found")
    plan = await RT.db.get_plan(case_id)
    trace = await RT.db.trace(case_id)
    ev = await RT.db.evidence(case_id)
    async with RT.db.pool.acquire() as conn:
        tickets = [
            dict(r) for r in await conn.fetch("select * from tickets where case_id=$1", case_id)
        ]
        reviews = [
            dict(r)
            for r in await conn.fetch(
                "select * from reviews where case_id=$1 order by loop_seq", case_id
            )
        ]
    plan_yaml = RT.store.read(case_id, plan["plan_id"]) if plan else None
    steps = (plan or {}).get("steps") or []
    step_status = (case.get("progress") or {}).get("step_status") or {}
    attempts = (case.get("progress") or {}).get("attempts") or {}
    return templates.TemplateResponse(
        request,
        "case.html",
        {
            "request": request,
            "case": case,
            "plan": plan,
            "plan_yaml": plan_yaml,
            "trace": trace,
            "evidence": ev,
            "tickets": tickets,
            "reviews": reviews,
            "steps": steps,
            "step_status": step_status,
            "attempts": attempts,
            "json": json.dumps,
        },
    )


@app.get("/ui/plans", response_class=HTMLResponse)
async def ui_plans(request: Request):
    return templates.TemplateResponse(
        request,
        "plans.html",
        {
            "request": request,
            "plans": await RT.db.list_plans(60),
            "workflows": await RT.db.list_workflows(),
        },
    )


@app.get("/ui/health", response_class=HTMLResponse)
async def ui_health(request: Request):
    async with RT.db.pool.acquire() as conn:
        rows = [
            dict(r)
            for r in await conn.fetch(
                "select stage, status, count(*) n, round(avg(latency_ms)) avg_ms, max(latency_ms) max_ms"
                " from stage_runs group by 1,2 order by 1,2"
            )
        ]
    return templates.TemplateResponse(
        request,
        "health.html",
        {"request": request, "rows": rows, "counters": counters.render(), "dlq": await api_dlq(10)},
    )
