"""Reproduction 剧本：一个 Agent 两个 mode，对应图中 4b 与 5a。

- mode=evidence（阶段 4b）：外部路径的受控复现取证 —— 锁机 → 同参重建 → 复现矩阵 → 结论；
- mode=verify（阶段 5a）：修复后的验证 —— 应用 4a 的改动 → 重跑 → 判定 fix_verified。

替换点（真实实现）：Jenkins Lockable Resources 锁机 + 触发重跑 + 抓 console + 指纹比对。

剧本规则：
1. 夹具 ``fixtures/repro/<scenario>.json`` 分 ``evidence`` / ``verify`` 两段（缺失回落 default）；
2. 段内 ``raise_blocked`` → Blocked(code, detail)（机位不可用等）；
3. 段内 ``sleep_s`` → 先 sleep（演示编排层超时 → retryable → 重试耗尽 → DLQ）；
4. 每次 matrix 重跑写一条 repro_log 证据，UI 可逐次 attempt 追溯；
5. evidence 模式下 rca=infrastructure 直接 Blocked(disabled)：基础设施类不占重跑机位。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from iqe_agents.common.config import cfg
from iqe_agents.common.tools.fingerprint import signature_of
from iqe_agents.runtime import Blocked, Ctx

log = logging.getLogger("agent.reproduction.playbook")

DEFAULT_SPEC: dict[str, Any] = {
    "reproducible": "no",
    "attempts": 0,
    "matrix": [],
    "steps_summary": [],
    "log_excerpt": "",
    "blocked_reason": None,
}
NO_REPRO_RCA = ("infrastructure",)


def _scenario_names(scenario: str) -> list[str]:
    out: list[str] = []
    for name in (scenario, str(scenario).replace("-", "_"), str(scenario).replace("_", "-")):
        if name and name not in out:
            out.append(name)
    return out


def _pick_json(ctx: Ctx) -> dict[str, Any]:
    """夹具名兼容连字符/下划线；都缺则回落 default。"""
    for name in _scenario_names(ctx.scenario or ""):
        if ctx.fixtures.exists("repro", f"{name}.json"):
            return ctx.fixtures.json("repro", f"{name}.json")
    return ctx.fixtures.pick_json("repro", "default", fallback="default")[1]


async def _block_if_unavailable(ctx: Ctx, spec: dict, case_id: str, mode: str) -> None:
    blocked = spec.get("raise_blocked")
    if isinstance(blocked, dict):
        await ctx.evidence(
            "repro_log",
            f"锁机失败：{blocked.get('detail', '')}",
            {"case_id": case_id, "mode": mode, "note": f"机位不可用（{blocked.get('code')}）"},
        )
        raise Blocked(
            str(blocked.get("code", "source_unavailable")),
            str(blocked.get("detail", "重跑机位不可用（Mock）")),
        )
    if spec.get("sleep_s"):
        # 故意慢：让编排层按 step timeout 得到 retryable，重试耗尽后进 DLQ
        log.info(
            "repro_sleep",
            extra={"ctx": {"case_id": case_id, "mode": mode, "sleep_s": spec["sleep_s"]}},
        )
        await asyncio.sleep(float(spec["sleep_s"]))


async def handler(ctx: Ctx) -> dict[str, Any]:
    payload = ctx.env.get("payload", {}) or {}
    mode = str(payload.get("mode") or "evidence")
    src = payload.get("input", {}) or {}
    case_id = src.get("case_id") or ctx.env.get("case_id")
    job = str(src.get("job") or "unknown-job")
    build = src.get("build") if isinstance(src.get("build"), int) else 0
    rca = str(src.get("rca") or "unknown")
    remediation = dict(src.get("remediation") or {})
    signature = src.get("signature") or signature_of(" ".join(src.get("key_frames") or []))
    if not case_id:
        raise Blocked("schema_invalid", "reproduction 输入缺少 case_id")
    if mode not in ("evidence", "verify"):
        raise Blocked("schema_invalid", f"reproduction 不支持的 mode={mode}")

    # 护栏：基础设施类失败不进重跑队列（真实实现同样受 REPRO_ENABLED 控制）
    if mode == "evidence" and rca in NO_REPRO_RCA:
        await ctx.evidence(
            "repro_log",
            "未执行受控重跑：rca=infrastructure（不占用重跑机位）。",
            {"case_id": case_id, "mode": mode, "note": "复现阶段被跳过（disabled）"},
        )
        raise Blocked("disabled", "infrastructure 类不做受控重跑")
    if not bool(getattr(cfg, "repro_enabled", True)):
        raise Blocked("disabled", "REPRO_ENABLED=false：受控重跑能力被关闭")

    doc = dict(_pick_json(ctx) or {})
    spec = {**DEFAULT_SPEC, **(doc.get(mode) or {})}
    await _block_if_unavailable(ctx, spec, case_id, mode)
    await ctx.sleep()
    ctx.fail_if_injected()

    matrix = list(spec.get("matrix") or [])
    max_runs = int(getattr(cfg, "repro_max_runs", 0) or 0)
    if max_runs > 0 and len(matrix) > max_runs:
        matrix = matrix[:max_runs]  # 受控重跑机位配额（REPRO_MAX_RUNS）
    steps = list(spec.get("steps_summary") or [])
    new_refs: list[dict[str, Any]] = []
    for i, row in enumerate(matrix, start=1):
        outcome = str(row.get("outcome") or "unknown")
        uri = await ctx.evidence(
            "repro_log",
            f"attempt {i}: {outcome}\nos={row.get('os')}\nconfig={row.get('config')}\n"
            f"signature={signature}",
            {
                "case_id": case_id,
                "mode": mode,
                "attempt": i,
                "outcome": outcome,
                "os": row.get("os"),
                "config": row.get("config"),
                "job": job,
                "build": build,
                "note": f"[{mode}] 重跑第 {i} 次：{row.get('os')} / {row.get('config')} → {outcome}",
            },
        )
        new_refs.append(
            {"kind": "repro_log", "uri": uri, "note": f"[{mode}] attempt {i} → {outcome}"}
        )

    reproducible = str(spec.get("reproducible") or DEFAULT_SPEC["reproducible"])
    attempts = int(spec.get("attempts") if isinstance(spec.get("attempts"), int) else len(matrix))
    if matrix:
        attempts = max(1, min(attempts, len(matrix)))

    fix_verified: bool | None = None
    if mode == "verify":
        applied = bool(remediation.get("fixed"))
        fix_verified = bool(spec.get("fix_verified")) and applied
        if not applied:
            steps = [*steps, "4a 未产出可用修复：验证只记录基线结论，供闸门 2 打回"]

    notes = steps or [f"在 {job}#{build} 同参重建 {attempts} 次", "结果与原始失败签名比对"]
    await ctx.evidence(
        "repro_plan",
        "\n".join(
            [
                f"case={case_id}",
                f"mode={mode}",
                f"rca={rca}",
                f"signature={signature}",
                f"attempts={attempts}",
                f"reproducible={reproducible}",
                f"fix_verified={fix_verified}",
                f"scenario={ctx.scenario}",
                f"note={spec.get('note', '')}",
            ]
        ),
        {
            "case_id": case_id,
            "mode": mode,
            "note": (
                f"{mode} 结论：reproducible={reproducible}"
                + (f"，fix_verified={fix_verified}" if mode == "verify" else "")
            ),
        },
    )
    log.info(
        "reproduction_done",
        extra={
            "ctx": {
                "case_id": case_id,
                "mode": mode,
                "reproducible": reproducible,
                "fix_verified": fix_verified,
            }
        },
    )

    return {
        "case_id": case_id,
        "mode": mode,
        "reproducible": reproducible,
        "attempts": attempts,
        "matrix": matrix[:10],
        "steps_summary": notes[:12],
        "failing_tests": list(spec.get("failing_tests") or [])[:20],
        "passing_tests": list(spec.get("passing_tests") or [])[:20],
        "fix_verified": fix_verified,
        "new_evidence": new_refs,
        "blocked_reason": spec.get("blocked_reason"),
        "log_excerpt": str(spec.get("log_excerpt") or "")[:4000],
    }
