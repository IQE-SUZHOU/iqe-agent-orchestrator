"""Regression 剧本（阶段 5b / 6c）：回归判定与 guilty patch 定位。

5b mode=check：拿同配置历史基线比对 → is_regression；
   - 是回归 → 编排层路由到 6c（bisect）；
   - 不是回归 → 跳过 6c，直接建单（工单里写明「长期环境问题」）。
6c mode=bisect：在变更区间内二分 → guilty_patch{commit,pr,author,confidence,rationale}；
   - 未收敛时 guilty_patch=null，并把知识库里的历史同类补丁作为参考写进 notes。

替换点（真实实现）：基线数据库查询 + 逐提交触发流水线二分（Jenkins API）+ 提交元数据关联。
"""

from __future__ import annotations

import logging
from typing import Any

from iqe_agents.common.kb import KnowledgeBase
from iqe_agents.runtime import Blocked, Ctx

log = logging.getLogger("agent.regression.playbook")

DEFAULT_SPEC: dict[str, Any] = {
    "check": {
        "is_regression": None,
        "baseline": None,
        "change_window": None,
        "candidates": [],
        "notes": ["缺少回归夹具，按未知处理"],
    },
    "bisect": {"bisected": False, "guilty_patch": None, "notes": ["未执行二分"]},
}


def _scenario_names(scenario: str) -> list[str]:
    out: list[str] = []
    for name in (scenario, str(scenario).replace("-", "_"), str(scenario).replace("_", "-")):
        if name and name not in out:
            out.append(name)
    return out


def _pick_json(ctx: Ctx) -> dict[str, Any]:
    for name in _scenario_names(ctx.scenario or ""):
        if ctx.fixtures.exists("regression", f"{name}.json"):
            return ctx.fixtures.json("regression", f"{name}.json")
    return ctx.fixtures.pick_json("regression", "default", fallback="default")[1]


async def _check(ctx: Ctx, src: dict, spec: dict) -> dict[str, Any]:
    case_id, job, build, rca = _common(src, ctx)
    sec = dict(spec.get("check") or DEFAULT_SPEC["check"])
    repro = dict(src.get("reproduction") or {})
    baseline = str(src.get("baseline") or sec.get("baseline") or "unknown")
    window = str(src.get("change_window") or sec.get("change_window") or "unknown")
    candidates = list(sec.get("candidates") or [])[:20]
    notes = [str(n) for n in (sec.get("notes") or [])]
    is_regression = sec.get("is_regression")
    if is_regression is None:
        # 没有基线可比：按「非回归」保守处理，交由闸门 3 判断工单是否成立
        is_regression = False
        notes.append("基线缺失：保守判定为非回归，需人工确认")

    await ctx.evidence(
        "regression_check",
        "\n".join(
            [
                f"case={case_id} job={job}#{build} rca={rca}",
                f"复现结论={repro.get('reproducible')}（attempts={repro.get('attempts')}）",
                f"基线={baseline} 变更区间={window}",
                f"is_regression={is_regression} 候选提交 {len(candidates)} 个",
                *[f"- {c.get('commit')} {c.get('title')}" for c in candidates[:8]],
                *[f"note: {n}" for n in notes],
            ]
        ),
        {
            "case_id": case_id,
            "is_regression": bool(is_regression),
            "note": f"5b 回归判定：{'是回归' if is_regression else '非回归'}（基线 {baseline}）",
        },
    )
    return {
        "case_id": case_id,
        "mode": "check",
        "is_regression": bool(is_regression),
        "baseline": baseline,
        "change_window": window,
        "bisected": False,
        "guilty_patch": None,
        "candidates": candidates,
        "evidence": list(ctx.refs),
        "notes": notes[:10],
    }


async def _bisect(ctx: Ctx, src: dict, spec: dict) -> dict[str, Any]:
    case_id, job, build, rca = _common(src, ctx)
    sec = dict(spec.get("bisect") or DEFAULT_SPEC["bisect"])
    notes = [str(n) for n in (sec.get("notes") or [])]
    kb = KnowledgeBase(ctx.fixtures)
    patch = sec.get("guilty_patch")
    if isinstance(patch, dict):
        patch = {
            "commit": str(patch.get("commit") or ""),
            "pr": patch.get("pr"),
            "author": patch.get("author"),
            "confidence": float(patch.get("confidence") or 0.5),
            "rationale": str(patch.get("rationale") or "")[:500],
        }
    else:
        patch = None
    if patch is None:
        hint = kb.guilty_patch_for(str(src.get("component") or ""), src.get("signature"))
        if hint:
            notes.append(
                f"知识库参考（不作为结论）：{hint.get('id')} {hint.get('summary')}，"
                f"置信度 {hint.get('confidence')}"
            )

    await ctx.evidence(
        "regression_bisect",
        "\n".join(
            [
                f"case={case_id} job={job}#{build} rca={rca}",
                f"bisected={bool(patch)}",
                (
                    f"guilty_patch={patch.get('commit')}（{patch.get('pr')} / "
                    f"{patch.get('author')}，confidence={patch.get('confidence')}）\n"
                    f"rationale: {patch.get('rationale')}"
                    if patch
                    else "guilty_patch=未收敛"
                ),
                *[f"note: {n}" for n in notes],
            ]
        ),
        {
            "case_id": case_id,
            "bisected": bool(patch),
            "note": f"6c 二分{'定位到 ' + patch['commit'] if patch else '未收敛'}",
        },
    )
    return {
        "case_id": case_id,
        "mode": "bisect",
        "is_regression": True if patch else None,
        "baseline": src.get("baseline"),
        "change_window": src.get("change_window"),
        "bisected": bool(patch),
        "guilty_patch": patch,
        "candidates": [],
        "evidence": list(ctx.refs),
        "notes": notes[:10],
    }


def _common(src: dict, ctx: Ctx) -> tuple[str, str, Any, str]:
    case_id = src.get("case_id") or ctx.env.get("case_id")
    if not case_id:
        raise Blocked("schema_invalid", "regression 输入缺少 case_id")
    job = str(src.get("job") or "unknown-job")
    build = src.get("build") if isinstance(src.get("build"), int) else 0
    return case_id, job, build, str(src.get("rca") or "unknown")


async def handler(ctx: Ctx) -> dict[str, Any]:
    payload = ctx.env.get("payload", {}) or {}
    mode = str(payload.get("mode") or "check")
    src = payload.get("input", {}) or {}
    spec = {**DEFAULT_SPEC, **(_pick_json(ctx) or {})}
    if mode == "check":
        return await _check(ctx, src, spec)
    if mode == "bisect":
        return await _bisect(ctx, src, spec)
    raise Blocked("schema_invalid", f"regression 不支持的 mode={mode}")
