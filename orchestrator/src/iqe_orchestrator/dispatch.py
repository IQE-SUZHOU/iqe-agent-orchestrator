"""Dispatch–Await：编排层唯一的总线写入者。

编排层发命令 → 就地等 in-process Future（按 idempotency_key 配对）→ 超时判 retryable。
所有单元只部署一个编排实例（V1），因此 Future 不需要跨进程路由。
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import aio_pika

from iqe_orchestrator.common import bus as bus_mod
from iqe_orchestrator.common.config import cfg
from iqe_orchestrator.common.db import DB
from iqe_orchestrator.common.envelope import age_ms, load, make_command, now_iso
from iqe_orchestrator.common.logs import counters, log_with

log = logging.getLogger("orchestrator.dispatch")

RESULT_STATUS_TO_RUN = {
    "ok": "done",
    "blocked": "blocked",
    "retryable": "failed",
    "expired": "expired",
}

ACTION_OF = {
    "monitor": "scan",
    "triage": "run",
    "reproduction": "run",
    "ticket": "run",
    "review": "run",
    "reporting": "case",
}


@dataclass
class StepResult:
    stage: str
    step_id: str
    attempt: int
    key: str
    status: str  # ok | retryable | blocked | expired | timeout
    output: dict
    error: dict | None
    latency_ms: int


def _dlq_reason(env: dict | None, headers: dict, original_rk: str) -> str:
    """DLQ 归档时判定「为什么进死信」：编排层自己再判一次（不依赖 Agent 权限，也不依赖
    basic.nack 里并不存在的 reason 字段）。"""
    from iqe_orchestrator.common.envelope import age_ms

    if env is None:
        return "envelope_invalid"
    if age_ms(env) > cfg.ttl_ms:
        return "expired"
    try:
        import jsonschema

        from iqe_orchestrator.common.agent_runtime import validate_stage_contract

        jsonschema.validate(
            instance=env.get("payload") or {},
            schema=validate_stage_contract(env.get("stage") or "", "input"),
        )
    except Exception as exc:
        return f"contract_violation: {str(exc)[:160]}"
    xdeath = headers.get("x-death")
    base = ""
    if isinstance(xdeath, list) and xdeath:
        base = str(xdeath[0].get("reason") or "")
    return base or "consumer_rejected"


class BusGateway:
    def __init__(self, bus: bus_mod.Bus, db: DB):
        self.bus = bus
        self.db = db
        self.pending: dict[str, asyncio.Future] = {}
        self.consumers: list = []

    async def start(self) -> None:
        self.consumers.append(await bus_mod.consume_results(self.bus, self._on_result))
        queue = await self.bus.channel.declare_queue(
            bus_mod.DLQ_QUEUE, durable=True, arguments={"x-message-ttl": 3_600_000}, passive=True
        )
        self.consumers.append(await queue.consume(self._on_dlq, no_ack=False))
        queue = await self.bus.channel.declare_queue(
            bus_mod.ORCH_EVENT_QUEUE,
            durable=True,
            arguments={"x-message-ttl": 3_600_000},
            passive=True,
        )
        self.consumers.append(await queue.consume(self._on_event, no_ack=False))
        log_with(
            log,
            "info",
            "gateway_listening",
            results_queue=bus_mod.ORCH_RESULT_QUEUE,
            dlq=bus_mod.DLQ_QUEUE,
        )

    # ---------------- 发命令并等结果 ----------------
    async def dispatch(
        self,
        *,
        stage: str,
        mode: str,
        payload: dict,
        run_id: str,
        step_id: str,
        attempt: int,
        case_id: str | None = None,
        scan_id: str | None = None,
        scenario_hint: str | None = None,
        timeout_s: float | None = None,
    ) -> StepResult:
        action = ACTION_OF[stage] if stage != "reporting" else mode
        # 契约形状（schemas/stage-contracts.json 为权威）：payload = {mode, input:{...}}
        envelope_payload = {"mode": mode, "input": payload}
        env = make_command(
            stage=stage,
            action=action,
            case_id=case_id,
            run_id=run_id,
            step_id=step_id,
            attempt=attempt,
            payload=envelope_payload,
            mode=mode,
            scenario_hint=scenario_hint,
        )
        key = env["idempotency_key"]
        timeout = timeout_s or cfg.stage_timeout_s
        closed = False
        counters.inc(
            "ci_dispatch_total", stage=stage, mode=mode
        )  # 超时/异常分支已经落库，成功分支再补一次
        await self.db.stage_run_open(
            key=key,
            case_id=case_id,
            run_id=run_id,
            scan_id=scan_id,
            stage=stage,
            step_id=step_id,
            mode=mode,
            attempt=attempt,
        )
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self.pending[key] = fut
        t0 = loop.time()
        try:
            await self.bus.publish(
                bus_mod.CMD_EXCHANGE,
                f"ci.cmd.{stage}.{action}",
                env,
                message_id=env["msg_id"],
                correlation_id=env["correlation_id"],
                headers={"idempotency_key": key, "case_id": case_id or "", "attempt": attempt},
                priority=env["priority"],
            )
            log_with(
                log,
                "info",
                "cmd_published",
                stage=stage,
                case_id=case_id,
                key=key,
                subject=f"ci.cmd.{stage}.{action}",
                mode=mode,
            )
            result = await asyncio.wait_for(fut, timeout=timeout)
        except TimeoutError:
            counters.inc("ci_dispatch_timeouts_total", stage=stage)
            result = {
                "payload": {
                    "status": "retryable",
                    "error": {"code": "timeout", "detail": f"等待 {stage} 结果超过 {timeout}s"},
                }
            }
            await self.db.stage_run_close(
                key=key, status="timeout", output=None, error=result["payload"]["error"]
            )
            closed = True
        except Exception as exc:
            counters.inc("ci_dispatch_errors_total", stage=stage)
            result = {
                "payload": {
                    "status": "retryable",
                    "error": {"code": "mock_fault", "detail": f"dispatch 失败: {exc}"[:300]},
                }
            }
            await self.db.stage_run_close(
                key=key, status="failed", output=None, error=result["payload"]["error"]
            )
            closed = True
        finally:
            self.pending.pop(key, None)

        payload_res = result.get("payload", {})
        status = payload_res.get("status", "retryable")
        dur = int((loop.time() - t0) * 1000)
        if not closed:
            # stage_runs 的终态：ok→done / blocked→blocked / retryable→failed / expired→expired
            await self.db.stage_run_close(
                key=key,
                status=RESULT_STATUS_TO_RUN.get(status, "failed"),
                output=payload_res.get("output"),
                error=payload_res.get("error"),
            )
        counters.observe("ci_dispatch_wait_seconds", dur / 1000)
        log_with(
            log,
            "info",
            "step_result",
            stage=stage,
            case_id=case_id,
            key=key,
            status=status,
            dur_ms=dur,
            err=(payload_res.get("error") or {}).get("code"),
        )
        return StepResult(
            stage=stage,
            step_id=step_id,
            attempt=attempt,
            key=key,
            status=status,
            output=payload_res.get("output") or {},
            error=payload_res.get("error"),
            latency_ms=dur,
        )

    async def publish_command_only(self, env: dict) -> None:
        """故障注入用：直接往命令 exchange 投一条消息，不等结果。"""
        await self.bus.publish(
            bus_mod.CMD_EXCHANGE,
            f"ci.cmd.{env['stage']}.{ACTION_OF[env['stage']]}",
            env,
            message_id=env["msg_id"],
            correlation_id=env["correlation_id"],
            headers={"idempotency_key": env["idempotency_key"]},
        )

    # ---------------- 入站 ----------------
    async def _on_result(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        # 任何异常都必须 ack：编排层的 prefetch 有限，未 ack 的消息会把后续投递全部卡住。
        try:
            await self._handle_result(message)
        except Exception as exc:
            log_with(log, "error", "result_handler_failed", err=str(exc)[:400])
        finally:
            await message.ack()

    async def _handle_result(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        try:
            env = load(message.body)
        except Exception as exc:
            log_with(log, "error", "result_envelope_invalid", err=str(exc)[:300])
            return
        key = env["idempotency_key"]
        status = env["payload"]["status"]
        counters.inc("ci_results_inbound_total", stage=env["stage"], status=status)
        log_with(
            log,
            "info",
            "result_consumed",
            stage=env["stage"],
            case_id=env.get("case_id"),
            key=key,
            status=status,
            age_ms=age_ms(env),
            redelivered=bool(message.redelivered),
        )
        fut = self.pending.get(key)
        if fut is None:
            counters.inc("ci_late_or_duplicate_results_total", stage=env["stage"], status=status)
            written = await self.db.stage_run_mark_late(
                key, status, env["payload"].get("output"), env["payload"].get("error")
            )
            log_with(
                log,
                "warn",
                "result_without_pending",
                stage=env["stage"],
                key=key,
                status=status,
                stage_run_updated=written,
            )
            return
        if not fut.done():
            fut.set_result(env)

    async def _on_dlq(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        raw = message.body.decode("utf-8", "replace")
        headers = dict(message.headers or {})
        try:
            env = load(message.body)
            key, case_id = env["idempotency_key"], env.get("case_id")
        except Exception:
            env, key, case_id = None, headers.get("idempotency_key") or "unknown", None
        # 死信消息的 routing_key 已经是 ci.dlq.<stage>；命令原本的 routing key 在 x-death 头里。
        # 实测（RabbitMQ 4.3.6）：x-death 条目用的是 **routing-keys**（复数、数组），没有 routing-key。
        xdeath = headers.get("x-death")
        original_rk = message.routing_key or ""
        if isinstance(xdeath, list) and xdeath:
            entry = xdeath[0]
            keys = entry.get("routing-keys") or entry.get("routing-key")
            if isinstance(keys, (list, tuple)) and keys:
                original_rk = str(keys[0])
            elif keys:
                original_rk = str(keys)
        parts = original_rk.split(".")
        stage = (
            parts[2]
            if len(parts) >= 3 and parts[0] == "ci" and parts[1] == "cmd"
            else (message.routing_key or "").split(".")[-1]
        )
        reason = _dlq_reason(env, headers, original_rk)
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "insert into dlq(stage, idempotency_key, case_id, reason, original_routing_key,"
                " redelivered, body) values($1,$2,$3,$4,$5,$6,$7)",
                stage,
                key,
                case_id,
                str(reason)[:200],
                str(original_rk)[:200],
                bool(message.redelivered),
                raw[:4000],
            )
        counters.inc("ci_dlq_total", stage=stage)
        log_with(
            log,
            "warn",
            "dlq_message_archived",
            stage=stage,
            key=key,
            rk=original_rk,
            dlq_rk=message.routing_key,
            reason=str(reason)[:120],
        )
        await message.ack()

    async def _on_event(self, message: aio_pika.abc.AbstractIncomingMessage) -> None:
        try:
            env = load(message.body)
            counters.inc("ci_events_inbound_total", event=env["payload"].get("event", "unknown"))
            log_with(
                log,
                "info",
                "event_consumed",
                event_name=env["payload"].get("event"),
                case_id=env.get("case_id"),
                at=now_iso(),
            )
        except Exception as exc:
            log_with(log, "warn", "event_invalid", err=str(exc)[:200])
        await message.ack()

    async def publish_event(self, *, rk: str, correlation_id: str | None, data: dict) -> None:
        from iqe_orchestrator.common.envelope import make_event

        env = make_event(
            rk=rk, produced_by="orchestrator", correlation_id=correlation_id, data=data
        )
        await self.bus.publish(
            bus_mod.EVENT_EXCHANGE,
            rk,
            env,
            message_id=env["msg_id"],
            correlation_id=env["correlation_id"],
        )

    async def stop(self) -> None:
        for c in self.consumers:
            try:
                await c.cancel()
            except Exception:
                pass
        for fut in self.pending.values():
            if not fut.done():
                fut.cancel()
        self.pending.clear()
