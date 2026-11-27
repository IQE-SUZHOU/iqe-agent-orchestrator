"""Agent 运行时骨架：订阅命令 → 校验 → 幂等 → 执行剧本 → 发布 result → ack。

顺序遵循 design/02 §10：先发布 result（等 publisher confirm）再 ack；
发布失败则不 ack，由 broker redeliver，幂等层吸收重复。
毒消息（schema 不合法 / mode 不属于本 stage）nack(requeue=False) 进 DLQ。

mode 与 IO 契约的唯一权威是 ``common/data/schemas/stage-contracts.json``（见 common.contracts），
本文件不再维护任何 stage/mode 的白名单副本。
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

import aio_pika

from .common import bus as bus_mod
from .common.config import cfg
from .common.contracts import is_valid_mode, stage_modes, validate_stage_contract
from .common.db import DB
from .common.envelope import age_ms, load, make_result
from .common.logs import counters, log_with, setup_logging
from .common.mock import Fixtures, jitter, pick_scenario, should_fail

log = logging.getLogger("iqe_agents.runtime")

MODE_MISMATCH = "mode_not_owned"


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

    @property
    def mode(self) -> str:
        return str((self.env.get("payload") or {}).get("mode") or "")

    async def evidence(self, kind: str, body: str, meta: dict | None = None) -> str:
        uri = await self.db.save_evidence(
            case_id=self.env.get("case_id"),
            stage=self.stage,
            kind=kind,
            meta=meta or {},
            body=body,
            scan_id=((self.env.get("payload") or {}).get("input") or {}).get("scan_id"),
        )
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
        self.modes = tuple(modes or stage_modes(stage))
        self.bus: bus_mod.Bus | None = None
        self.db: DB | None = None
        self.fixtures = Fixtures()
        self.stopped = asyncio.Event()

    # ---------------- 入口 ----------------
    async def run(self) -> None:
        setup_logging()
        await bus_mod.wait_ready(timeout=60)
        self.db = await DB.create()
        self.bus = await bus_mod.connect(
            declare=False, prefetch=bus_mod.AGENT_QUEUES[self.stage][2]
        )
        qname, bind, _pf = bus_mod.AGENT_QUEUES[self.stage]
        # passive 声明只为在启动时校验队列已存在（不存在会立刻抛错）
        await self.bus.channel.declare_queue(qname, durable=True, passive=True)
        from .common.health import build_app, serve

        await serve(
            build_app(
                title=f"ci-agent-{self.stage}",
                probes={"stage": self.stage, "queue": qname, "binding": bind, "modes": self.modes},
            )
        )
        log_with(
            log,
            "info",
            "agent_ready",
            stage=self.stage,
            queue=qname,
            binding=bind,
            modes=list(self.modes),
            amqp_host=cfg.amqp_url.split("@")[-1],
        )
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
        mode = str((env.get("payload") or {}).get("mode") or "")
        scenario = pick_scenario(env)
        log_with(
            log,
            "info",
            "cmd_consumed",
            stage=self.stage,
            mode=mode,
            case_id=case_id,
            attempt=attempt,
            key=key,
            scenario=scenario,
            age_ms=age_ms(env),
        )

        # ttl 过期：直接回 expired（不重跑）
        if age_ms(env) > env.get("ttl_ms", 30000):
            await self._publish(
                env,
                make_result(
                    command=env,
                    status="expired",
                    error={"code": "timeout", "detail": "ttl expired"},
                ),
            )
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

        # 入参契约校验（stage-contracts 为权威：mode 归属 + payload 形状）
        try:
            self._validate_input(env, mode)
        except ValueError as exc:
            counters.inc("ci_stage_results_total", stage=self.stage, status="rejected")
            log_with(
                log,
                "error",
                "input_contract_violation",
                stage=self.stage,
                mode=mode,
                key=key,
                err=str(exc)[:300],
            )
            await bus_mod.nack_to_dlq(message)
            return

        ctx = Ctx(stage=self.stage, env=env, scenario=scenario, db=self.db, fixtures=self.fixtures)
        try:
            await ctx.sleep()
            ctx.fail_if_injected()
            output = await self.handler(ctx)
            if output is None:
                output = {}
            self._check_output(output, case_id)
            result = make_result(
                command=env,
                status="ok",
                output=output,
                evidence_refs=ctx.refs,
                produced_by=self.stage,
            )
        except Blocked as exc:
            counters.inc("ci_stage_results_total", stage=self.stage, status="blocked")
            result = make_result(
                command=env,
                status="blocked",
                error={"code": exc.code, "detail": exc.detail},
                evidence_refs=ctx.refs,
                produced_by=self.stage,
            )
        except MockRetry as exc:
            counters.inc("ci_stage_results_total", stage=self.stage, status="retryable")
            result = make_result(
                command=env,
                status="retryable",
                error={"code": exc.code, "detail": exc.detail},
                evidence_refs=ctx.refs,
                produced_by=self.stage,
            )
        except Exception as exc:  # 剧本异常 → retryable，交给编排层重试
            counters.inc("ci_stage_results_total", stage=self.stage, status="retryable")
            log_with(log, "error", "handler_crash", stage=self.stage, key=key, err=repr(exc)[:400])
            result = make_result(
                command=env,
                status="retryable",
                error={"code": "mock_fault", "detail": repr(exc)[:300]},
                evidence_refs=ctx.refs,
                produced_by=self.stage,
            )

        dur = (asyncio.get_event_loop().time() - t0) * 1000
        counters.observe("ci_stage_duration_seconds", dur / 1000)
        counters.inc("ci_stage_results_total", stage=self.stage, status=result["payload"]["status"])
        await self._publish(env, result)
        await message.ack()
        log_with(
            log,
            "info",
            "result_published",
            stage=self.stage,
            mode=mode,
            case_id=case_id,
            key=key,
            status=result["payload"]["status"],
            dur_ms=int(dur),
        )

    async def _publish(self, command: dict, result: dict) -> None:
        await self.bus.publish_result(command, result)

    def _check_output(self, output: dict, case_id: str | None) -> None:
        """出参契约软校验：违反只告警（便于观察 Mock 与契约漂移），不阻断链路。"""
        import jsonschema

        try:
            jsonschema.validate(
                instance=output, schema=validate_stage_contract(self.stage, "output")
            )
        except jsonschema.ValidationError as exc:
            counters.inc("ci_output_contract_violations_total", stage=self.stage)
            log_with(
                log,
                "warn",
                "output_contract_violation",
                stage=self.stage,
                case_id=case_id,
                err=str(exc.message)[:300],
            )

    def _validate_input(self, env: dict, mode: str) -> None:
        if not is_valid_mode(self.stage, mode):
            raise ValueError(
                f"stage={self.stage} 不接受 mode={mode!r}（允许 {list(stage_modes(self.stage))}）"
            )
        import jsonschema

        try:
            jsonschema.validate(
                instance=dict(env["payload"]), schema=validate_stage_contract(self.stage, "input")
            )
        except jsonschema.ValidationError as exc:
            raise ValueError(f"stage={self.stage} mode={mode}: {exc.message}") from exc

    def __repr__(self) -> str:  # pragma: no cover
        return f"<AgentRuntime stage={self.stage} modes={self.modes}>"


__all__ = [
    "AgentRuntime",
    "Blocked",
    "Ctx",
    "MockRetry",
    "is_valid_mode",
    "stage_modes",
    "validate_stage_contract",
]
