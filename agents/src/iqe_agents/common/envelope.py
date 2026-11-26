"""消息信封：所有跨单元通信的唯一载体（契约见 schemas/envelope.schema.json）。

约定：
- msg_type: command（编排层→Agent） | result（Agent→编排层） | event（可观测事件）
- idempotency_key = `<case_id 或 run_id>:<step_id>:<attempt>`，全链路唯一
- correlation_id = case_id（Case 类消息）或 run_id（扫描/日报类消息）
"""

from __future__ import annotations

import json
import random
import re
import time
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

import jsonschema
from ulid import ULID

from .config import cfg

_SCHEMA_RE = re.compile(r"^[A-Za-z0-9_.-]+\.json$")


def ulid() -> str:
    return str(ULID())


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


# --------------------------------------------------------------------------- schema
@lru_cache(maxsize=8)
def load_schema(filename: str) -> dict:
    if not _SCHEMA_RE.match(filename):
        raise ValueError(f"非法 schema 文件名: {filename}")
    path = Path(cfg.schemas_dir) / filename
    if not path.exists():
        raise FileNotFoundError(f"schema 不存在: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def validate(instance: dict, filename: str = "envelope.schema.json") -> None:
    """校验失败抛 ValueError（调用方决定是否 nack）。"""
    try:
        jsonschema.validate(instance=instance, schema=load_schema(filename))
    except jsonschema.ValidationError as exc:
        raise ValueError(f"{filename}: {exc.message} @ {'/'.join(map(str, exc.path))}") from exc


def dump(env: dict) -> bytes:
    return json.dumps(env, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def load(body: bytes | str) -> dict:
    raw = json.loads(body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else body)
    validate(raw, "envelope.schema.json")
    return raw


# --------------------------------------------------------------------------- 构造
def _base(
    msg_type: str,
    *,
    produced_by: str,
    stage: str,
    correlation_id: str,
    payload: dict,
    run_id: str | None,
    case_id: str | None,
    scenario_hint: str | None,
) -> dict:
    return {
        "msg_id": ulid(),
        "msg_type": msg_type,
        "schema": "v1",
        "produced_at": now_iso(),
        "produced_by": produced_by,
        "correlation_id": correlation_id,
        "run_id": run_id,
        "case_id": case_id,
        "stage": stage,
        "scenario_hint": scenario_hint,
        "payload": payload,
    }


def make_command(
    *,
    stage: str,
    action: str,
    payload: dict,
    run_id: str,
    step_id: str,
    attempt: int,
    case_id: str | None = None,
    mode: str | None = None,
    priority: int = 5,
    ttl_ms: int | None = None,
    scenario_hint: str | None = None,
) -> dict:
    env = _base(
        "command",
        produced_by="orchestrator",
        stage=stage,
        correlation_id=case_id or run_id,
        run_id=run_id,
        case_id=case_id,
        scenario_hint=scenario_hint,
        payload=dict(payload),
    )
    env.update(
        {
            "step_id": step_id,
            "attempt": attempt,
            "idempotency_key": f"{case_id or run_id}:{step_id}:{attempt}",
            "ttl_ms": ttl_ms or cfg.ttl_ms,
            "priority": max(0, min(9, priority)),
        }
    )
    env["payload"].setdefault("mode", mode or action)
    validate(env, "envelope.schema.json")
    return env


def make_result(
    *,
    command: dict,
    status: str,
    output: dict | None = None,
    error: dict | None = None,
    evidence_refs: list | None = None,
    produced_by: str | None = None,
) -> dict:
    env = _base(
        "result",
        produced_by=produced_by or command["stage"],
        stage=command["stage"],
        correlation_id=command["correlation_id"],
        run_id=command.get("run_id"),
        case_id=command.get("case_id"),
        scenario_hint=command.get("scenario_hint"),
        payload={
            "stage": command["stage"],
            "attempt": command.get("attempt", 1),
            "status": status,
            "duration_ms": age_ms(command),
        },
    )
    if output is not None:
        env["payload"]["output"] = output
    if error is not None:
        env["payload"]["error"] = error
    if evidence_refs:
        env["payload"]["evidence_refs"] = evidence_refs
        env["evidence_refs"] = evidence_refs
    env.update(
        {
            "step_id": command.get("step_id"),
            "attempt": command.get("attempt", 1),
            "idempotency_key": command["idempotency_key"],
        }
    )
    validate(env, "envelope.schema.json")
    return env


def make_event(
    *,
    rk: str,
    data: dict,
    correlation_id: str,
    produced_by: str = "orchestrator",
    case_id: str | None = None,
    run_id: str | None = None,
) -> dict:
    payload = {"event": rk, "subject": rk}
    payload.update(data or {})
    env = _base(
        "event",
        produced_by=produced_by,
        stage="none",
        correlation_id=correlation_id,
        run_id=run_id,
        case_id=case_id,
        scenario_hint=None,
        payload=payload,
    )
    validate(env, "envelope.schema.json")
    return env


# --------------------------------------------------------------------------- 工具
def age_ms(env: dict) -> int:
    try:
        return max(
            0,
            int((datetime.now(UTC) - _parse_iso(env["produced_at"])).total_seconds() * 1000),
        )
    except Exception:
        return 0


def subject_of(env: dict) -> str:
    """路由键的可读形式，例如 ci.cmd.triage.classify / ci.result.review。"""
    kind = env["msg_type"]
    exchange = {"command": "ci.cmd", "result": "ci.result", "event": "ci.event"}.get(
        kind, "ci.event"
    )
    if kind == "command":
        return f"{exchange}.{env['stage']}.{env['payload'].get('mode', 'run')}"
    if kind == "result":
        return f"{exchange}.{env['stage']}.{env['payload'].get('status', 'ok')}"
    return f"{exchange}.{env['payload'].get('event', 'unknown')}"


def sleep_ms_range(spec: str) -> tuple[int, int]:
    """'150:500' → (150, 500)；单值 '300' → (300, 300)。"""
    parts = str(spec).split(":")
    try:
        lo = int(parts[0])
        hi = int(parts[1]) if len(parts) > 1 and parts[1] else lo
    except ValueError:
        lo, hi = 150, 500
    return (min(lo, hi), max(lo, hi))


def rand_delay_ms(spec: str | None = None) -> int:
    lo, hi = sleep_ms_range(spec or cfg.mock_delay_ms)
    return random.randint(lo, hi)


def monotonic_ms() -> int:
    return int(time.monotonic() * 1000)
