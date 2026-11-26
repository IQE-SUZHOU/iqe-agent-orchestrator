"""Agent 运行时骨架：订阅命令 → 校验 → 幂等 → 执行剧本 → 发布 result → ack。

顺序遵循 design/02 §10：先发布 result（等 publisher confirm）再 ack；
发布失败则不 ack，由 broker redeliver，幂等层吸收重复。
毒消息（schema 不合法 / ttl 过期且无法处理）nack(requeue=False) 进 DLQ。
"""
from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass, field

import aio_pika

from . import bus as bus_mod
from .config import cfg
from .db import DB
from .envelope import age_ms, load, make_result, validate
from .logs import counters, log_with, setup_logging
from .mock import Fixtures, jitter, pick_scenario, should_fail

log = logging.getLogger("agentkit.runtime")


class MockRetry(Exception):
    """剧本主动返回 retryable。"""

    def __init__(self, code: str = "mock_fault", detail: str = ""):
        super().__init__(detail or code)
        self.code, self.detail = code, detail


class Blocked(Exception):
    def __init__(self, code: str, detail: str = ""):
        super().__init__(detail or code)
        self.code, self.detail = code, detail


@dataclass
class Ctx:
    stage: str
    env: dict
    scenario: str
    db: DB
    fixtures: Fixtures
    refs: list[dict] = field(default_factory=list)
    extra: dict = field(default_factory=dict)

    async def evidence(self, kind: str, body: str, meta: dict | None = None) -> str:
        uri = await self.db.save_evidence(case_id=self.env.get("case_id"), stage=self.stage,
                                          kind=kind, meta=meta or {}, body=body,
                                          scan_id=((self.env.get("payload") or {}).get("input") or {}).get("scan_id"))
        self.refs.append({"kind": kind, "uri": uri, "note": (meta or {}).get("note", "")[:200]})
        return uri

    async def sleep(self) -> None:
        await asyncio.sleep(jitter())

    def fail_if_injected(self) -> None:
        if should_fail():
            raise MockRetry("mock_fault", f"injected failure (rate={cfg.mock_failure_rate})")


class AgentRuntime:
    def __init__(self, stage: str, handler, *, modes: list[str] | None = None):
        self.stage = stage
        self.handler = handler
        self.modes = modes
        self.bus: bus_mod.Bus | None = None
        self.db: DB | None = None
        self.fixtures = Fixtures()
        self.stopped = asyncio.Event()

    # ---------------- 入口 ----------------
    async def run(self) -> None:
        setup_logging()
        await bus_mod.wait_ready(timeout=60)
        self.db = await DB.create()
        self.bus = await bus_mod.connect(declare=False,
                                         prefetch=bus_mod.AGENT_QUEUES[self.stage][2])
        qname, bind, _pf = bus_mod.AGENT_QUEUES[self.stage]
        queue = await self.bus.channel.declare_queue(qname, durable=True, passive=True)
        from .health import build_app, serve
        await serve(build_app(title=f"ci-agent-{self.stage}",
                              probes={"stage": self.stage, "queue": qname, "binding": bind}))
        log_with(log, "info", "agent_ready", stage=self.stage, queue=qname, binding=bind,
                 amqp_host=cfg.amqp_url.split("@")[-1])
        counters.inc("ci_agent_ready_total", stage=self.stage)
        await bus_mod.consume_agent(self.bus, self.stage, self._handle)
        await self.stopped.wait()

    async def stop(self) -> None:
        self.stopped.set()
        if self.bus:
            await self.bus.close()
        if self.db:
            await self.db.close()

    # ---------------- 消息处理 ----------------
    async def _handle(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        t0 = asyncio.get_event_loop().time()
        try:
            env = load(message.body)
        except Exception as exc:
            counters.inc("ci_stage_results_total", stage=self.stage, status="rejected")
            log_with(log, "error", "envelope_invalid", stage=self.stage, err=str(exc)[:300])
            await bus_mod.nack_to_dlq(message)
            return

        if env["msg_type"] != "command" or env["stage"] != self.stage:
            await message.ack()
            return

        case_id, key = env.get("case_id"), env["idempotency_key"]
        attempt = env.get("attempt", 1)
        scenario = pick_scenario(env)
        log_with(log, "info", "cmd_consumed", stage=self.stage, case_id=case_id, attempt=attempt,
                 key=key, scenario=scenario, age_ms=age_ms(env))

        # ttl 过期：直接回 expired（不重跑）
        if age_ms(env) > env.get("ttl_ms", 30000):
            await self._publish(env, make_result(command=env, status="expired",
                                                 error={"code": "timeout", "detail": "ttl expired"}))
            await message.ack()
            return

        # 幂等层 2：该 key 已有 done 结果
        if await self.db.has_result(key):
            counters.inc("ci_dedup_skip_total", stage=self.stage, layer="result")
            log_with(log, "info", "dedup_skip", stage=self.stage, key=key, layer="result")
            await message.ack()
            return
        # 幂等层 1：抢锁
        if not await self.db.claim(key, self.stage):
            counters.inc("ci_dedup_skip_total", stage=self.stage, layer="claim")
            log_with(log, "info", "dedup_skip", stage=self.stage, key=key, layer="claim")
            await message.ack()
            return

        # 入参契约校验（stage-contracts 为权威）
        try:
            self._validate_input(env)
        except ValueError as exc:
            counters.inc("ci_stage_results_total", stage=self.stage, status="rejected")
            log_with(log, "error", "input_contract_violation", stage=self.stage, key=key,
                     err=str(exc)[:300])
            await bus_mod.nack_to_dlq(message)
            return

        ctx = Ctx(stage=self.stage, env=env, scenario=scenario, db=self.db, fixtures=self.fixtures)
        try:
            await ctx.sleep()
            ctx.fail_if_injected()
            output = await self.handler(ctx)
            if output is None:
                output = {}
            self._check_output(output)
            result = make_result(command=env, status="ok", output=output,
                                 evidence_refs=ctx.refs, produced_by=self.stage)
        except Blocked as exc:
            counters.inc("ci_stage_results_total", stage=self.stage, status="blocked")
            result = make_result(command=env, status="blocked",
                                 error={"code": exc.code, "detail": exc.detail},
                                 evidence_refs=ctx.refs, produced_by=self.stage)
        except MockRetry as exc:
            counters.inc("ci_stage_results_total", stage=self.stage, status="retryable")
            result = make_result(command=env, status="retryable",
                                 error={"code": exc.code, "detail": exc.detail},
                                 evidence_refs=ctx.refs, produced_by=self.stage)
        except Exception as exc:  # 剧本异常 → retryable，交给编排层重试
            counters.inc("ci_stage_results_total", stage=self.stage, status="retryable")
            log_with(log, "error", "handler_crash", stage=self.stage, key=key, err=repr(exc)[:400])
            result = make_result(command=env, status="retryable",
                                 error={"code": "mock_fault", "detail": repr(exc)[:300]},
                                 evidence_refs=ctx.refs, produced_by=self.stage)

        dur = (asyncio.get_event_loop().time() - t0) * 1000
        counters.observe("ci_stage_duration_seconds", dur / 1000)
        counters.inc("ci_stage_results_total", stage=self.stage, status=result["payload"]["status"])
        await self._publish(env, result)
        await message.ack()
        log_with(log, "info", "result_published", stage=self.stage, case_id=case_id, key=key,
                 status=result["payload"]["status"], dur_ms=int(dur))

    async def _publish(self, command: dict, result: dict) -> None:
        await self.bus.publish_result(command, result)

    def _check_output(self, output: dict) -> None:
        """出参契约软校验：违反只告警（便于观察 Mock 与契约漂移），不阻断链路。"""
        import jsonschema
        try:
            jsonschema.validate(instance=output, schema=validate_stage_contract(self.stage, "output"))
        except jsonschema.ValidationError as exc:
            counters.inc("ci_output_contract_violations_total", stage=self.stage)
            log_with(log, "warn", "output_contract_violation", stage=self.stage,
                     case_id=self.env.get("case_id"), err=str(exc.message)[:300])

    def _validate_input(self, env: dict) -> None:
        contracts = {
            "monitor": ("monitor", env["payload"].get("mode", "scan")),
            "triage": ("triage", env["payload"].get("mode", "full")),
            "reproduction": ("reproduction", env["payload"].get("mode", "run")),
            "ticket": ("ticket", env["payload"].get("mode", "run")),
            "review": ("review", env["payload"].get("mode", "run")),
            "reporting": ("reporting", env["payload"].get("mode", "case")),
        }
        stage_key, mode = contracts[self.stage]
        schema = validate_stage_contract(stage_key, "input")
        payload = dict(env["payload"])
        try:
            import jsonschema
            jsonschema.validate(instance=payload, schema=schema)
        except jsonschema.ValidationError as exc:
            raise ValueError(f"stage={stage_key} mode={mode}: {exc.message}") from exc

    def __repr__(self) -> str:  # pragma: no cover
        return f"<AgentRuntime stage={self.stage}>"


def validate_stage_contract(stage: str, which: str) -> dict:
    """从 schemas/stage-contracts.json 取出某 stage 的 input/output JSON Schema。"""
    import json
    from pathlib import Path
    doc = json.loads((Path(__file__).resolve().parent.parent / "schemas" / "stage-contracts.json")
                     .read_text(encoding="utf-8"))
    stage_doc = doc["stages"][stage]
    base = {"$defs": doc["$defs"], "type": "object"}
    schema = dict(base)
    schema.update(stage_doc[which])
    schema["required"] = stage_doc[which].get("required", [])
    return schema
