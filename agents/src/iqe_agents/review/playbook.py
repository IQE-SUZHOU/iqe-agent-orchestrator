"""Review 剧本：8 条机械预检 → verdict / reason_code / requested_info（Mock Reviewer）。

预检顺序固定（编排层与自测都按 checks[i].id 取值，编号即契约编号）：
 #1 evidence_present            Case 已有证据引用                     insufficient_evidence
 #2 evidence_count_ok           证据条数 >= MIN_EVIDENCE              insufficient_evidence
 #3 triage_confidence_ok        rca 非 unknown 且 confidence >= 0.70  missing_triage
 #4 repro_conclusion            复现段结论存在（看 repro_attempted）  missing_repro
 #5 ticket_sections_nonempty    工单关键段落非空（无 ticket 段则跳过）missing_ticket_sections
 #6 no_duplicate_ticket         不得既有重复工单又继续开单            duplicate_ticket
 #7 signal_repro_consistent     signal 与复现结论自洽                 inconsistent_evidence
 #8 loop_not_exhausted          回炉轮次 <= loop_max                  loop_exhausted

verdict：全部通过 → approve；有失败 → needs_info（#6 失败且已是第 2 轮 → reject）；
夹具 fixtures/review/<scenario>.json 的 override 可直接给定复核结论（演示 reject / 永远 needs_info）。
"""

from __future__ import annotations

import logging
from typing import Any

from iqe_agents.common.config import cfg
from iqe_agents.runtime import Blocked, Ctx

log = logging.getLogger("agent.review.playbook")

REQUIRED_SECTIONS = ("rca", "repro", "evidence")
CONFIDENCE_MIN = 0.70
MIN_EVIDENCE = 2
DEFAULT_SPEC: dict[str, Any] = {
    "quality_weights": {"evidence": 0.4, "repro": 0.3, "consistency": 0.3},
    "notes": "Mock 复核说明",
}
_REQUEST_INFO = {
    "evidence_present": "补充 Case 证据：失败段 console 与 dmesg 尾部",
    "evidence_count_ok": "至少提供 2 条可追溯证据（日志摘录 + 蒸馏日志）",
    "triage_confidence_ok": "重新分诊：给出明确 rca 与更高置信度（含关键帧）",
    "repro_conclusion": "补充受控重跑记录与 reproducible 结论",
    "ticket_sections_nonempty": "工单补齐 rca / repro / evidence 三段内容",
    "no_duplicate_ticket": "确认与已有工单的关系，避免重复开单",
    "signal_repro_consistent": "核对 signal 与复现结论，消除矛盾",
    "loop_not_exhausted": "回炉已达上限，请人工介入收尾",
}


def _scenario_names(scenario: str) -> list[str]:
    out: list[str] = []
    for name in (scenario, str(scenario).replace("-", "_"), str(scenario).replace("_", "-")):
        if name and name not in out:
            out.append(name)
    return out


def _pick_json(ctx: Ctx, subdir: str) -> dict[str, Any]:
    """夹具名兼容连字符/下划线；都缺则回落 default。"""
    for name in _scenario_names(ctx.scenario or ""):
        if ctx.fixtures.exists(subdir, f"{name}.json"):
            return ctx.fixtures.json(subdir, f"{name}.json")
    return ctx.fixtures.pick_json(subdir, "default", fallback="default")[1]


def _spec(ctx: Ctx) -> dict[str, Any]:
    return {**DEFAULT_SPEC, **(_pick_json(ctx, "review") or {})}


def _check(
    cid: str, name: str, passed: bool, detail: str, reason_code: str | None = None
) -> dict[str, Any]:
    return {
        "id": cid,
        "name": name,
        "pass": bool(passed),
        "detail": detail[:220],
        "reason_code": None if passed else reason_code,
    }


async def handler(ctx: Ctx) -> dict[str, Any]:
    payload = ctx.env.get("payload", {}) or {}
    src = payload.get("input", {}) or {}
    case_id = src.get("case_id") or ctx.env.get("case_id")
    if not case_id:
        raise Blocked("schema_invalid", "review 输入缺少 case_id")

    signal = src.get("signal") or "error"
    rca = str(src.get("rca") or "unknown")
    confidence = src.get("confidence")
    key_frames = list(src.get("key_frames") or [])
    reproducible = src.get("reproducible")
    repro_attempted = bool(src.get("repro_attempted"))
    ticket_status = src.get("ticket_status")
    ticket_sections = list(src.get("ticket_sections") or [])
    duplicate_of = src.get("duplicate_of")
    evidence_count = int(src.get("evidence_count") or 0)
    loop_seq = int(src.get("loop_seq") or 1)
    requested_focus = str(src.get("requested_focus") or "")
    loop_max = int(
        getattr(cfg, "max_review_loops", None) or getattr(cfg, "review_loop_max", 2) or 2
    )

    checks: list[dict[str, Any]] = []

    # #1 证据引用存在
    ok = evidence_count > 0 or bool(key_frames)
    checks.append(
        _check(
            "evidence_present",
            "Case 已有证据引用",
            ok,
            f"证据 {evidence_count} 条，关键帧 {len(key_frames)} 条"
            + ("" if ok else "：Case 没有任何证据引用"),
            "insufficient_evidence",
        )
    )
    # #2 证据条数达标
    ok = evidence_count >= MIN_EVIDENCE
    checks.append(
        _check(
            "evidence_count_ok",
            f"证据条数 >= {MIN_EVIDENCE}",
            ok,
            f"证据 {evidence_count} 条（要求 >= {MIN_EVIDENCE}）",
            "insufficient_evidence",
        )
    )
    # #3 分诊结论可用（rca 明确 + 置信度达标）
    ok = (
        rca != "unknown"
        and isinstance(confidence, (int, float))
        and float(confidence) >= CONFIDENCE_MIN
    )
    detail = f"rca={rca}，置信度 {confidence if confidence is not None else '未提供'}"
    if not ok:
        detail += f"：rca 不明确或置信度低于 {CONFIDENCE_MIN:.2f}"
    checks.append(
        _check("triage_confidence_ok", "分诊结论明确且置信度达标", ok, detail, "missing_triage")
    )
    # #4 复现段结论（关键：计划里没有 reproduction 段时不算失败）
    if signal == "error" and repro_attempted and reproducible is None:
        checks.append(
            _check(
                "repro_conclusion",
                "复现段已给出 reproducible 结论",
                False,
                "reproduction 已执行但输出里没有 reproducible 结论",
                "missing_repro",
            )
        )
    elif not repro_attempted:
        checks.append(
            _check(
                "repro_conclusion", "复现段已给出 reproducible 结论", True, "计划未包含复现段，跳过"
            )
        )
    else:
        checks.append(
            _check(
                "repro_conclusion",
                "复现段已给出 reproducible 结论",
                True,
                f"复现结论：{reproducible}",
            )
        )
    # #5 工单段落非空（无 ticket 段则跳过）
    if ticket_status is None:
        checks.append(
            _check("ticket_sections_nonempty", "工单关键段落非空", True, "计划无 ticket，跳过")
        )
    else:
        keys = {s.get("key") for s in ticket_sections}
        empty = [
            k
            for k in REQUIRED_SECTIONS
            if k in keys
            and not str(
                next((s.get("content") for s in ticket_sections if s.get("key") == k), "") or ""
            ).strip()
        ]
        missing = [k for k in REQUIRED_SECTIONS if k not in keys] + empty
        checks.append(
            _check(
                "ticket_sections_nonempty",
                "工单关键段落非空",
                not missing,
                f"ticket_status={ticket_status}，缺失/空段落：{missing or '无'}",
                "missing_ticket_sections",
            )
        )
    # #6 无重复工单
    dup = bool(duplicate_of) or ticket_status == "linked_duplicate"
    checks.append(
        _check(
            "no_duplicate_ticket",
            "不存在重复工单",
            not dup,
            (
                f"已存在重复工单 {duplicate_of}（ticket_status={ticket_status}）"
                if dup
                else "未检出重复工单"
            ),
            "duplicate_ticket",
        )
    )
    # #7 signal 与复现结论自洽
    bad = (signal == "none" and reproducible in ("yes", "flaky")) or (
        signal == "error" and reproducible == "blocked"
    )
    checks.append(
        _check(
            "signal_repro_consistent",
            "signal 与复现结论自洽",
            not bad,
            f"signal={signal} / reproducible={reproducible}"
            + ("" if not bad else "：结论互相矛盾"),
            "inconsistent_evidence",
        )
    )
    # #8 回炉轮次未耗尽
    ok = loop_seq <= loop_max
    checks.append(
        _check(
            "loop_not_exhausted",
            f"回炉轮次 <= {loop_max}",
            ok,
            f"当前第 {loop_seq} 轮（上限 {loop_max} 轮）",
            "loop_exhausted",
        )
    )

    failed = [c for c in checks if not c["pass"]]
    requested = [_REQUEST_INFO.get(c["id"], c["detail"]) for c in failed]

    if failed and any(c["id"] == "no_duplicate_ticket" for c in failed) and loop_seq >= 2:
        verdict, reason_code = "reject", "duplicate_ticket"  # 重复工单第二轮仍未澄清 → 拒绝
    elif failed:
        verdict, reason_code = "needs_info", failed[0].get("reason_code") or "insufficient_evidence"
    else:
        verdict, reason_code = "approve", "ok"

    # 夹具剧本可直接指定复核结论（needs_info / insufficient / reject 场景）
    override = dict(_spec(ctx).get("override") or {})
    first_only = bool(override.pop("first_loop_only", False)) and loop_seq > 1
    if override and not first_only:
        verdict = str(override.get("verdict") or verdict)
        reason_code = str(override.get("reason_code") or reason_code)
        if override.get("requested_info"):
            requested = list(override["requested_info"])
    if not next((c for c in checks if c["id"] == "loop_not_exhausted"), {"pass": True})["pass"]:
        # 回炉预算耗尽优先于剧本：明确给出 loop_exhausted，编排层据此把 Case 收尾为 blocked
        verdict, reason_code = "needs_info", "loop_exhausted"

    quality = _quality(ctx, checks, evidence_count, reproducible)
    notes = _notes(ctx, checks, verdict, reason_code, loop_seq, requested_focus)
    if verdict == "needs_info" and not requested:
        requested = ["补充失败段完整 console 与 dmesg 尾部"]

    await ctx.evidence(
        "review_record",
        "\n".join(
            [f"第 {loop_seq} 轮复核：{verdict}（{reason_code}）"]
            + [
                f"{'PASS' if c['pass'] else 'FAIL'} #{i+1} {c['id']}：{c['detail']}"
                for i, c in enumerate(checks)
            ]
            + [f"requested_info: {r}" for r in requested]
        ),
        {
            "case_id": case_id,
            "verdict": verdict,
            "reason_code": reason_code,
            "loop_seq": loop_seq,
            "checks_failed": [c["id"] for c in failed],
            "note": f"复核 {verdict}/{reason_code}（第 {loop_seq} 轮，失败 {len(failed)}/{len(checks)} 项）",
        },
    )

    return {
        "case_id": case_id,
        "verdict": verdict,
        "reason_code": reason_code,
        "requested_info": requested[:10],
        "checks": checks[:20],
        "quality_scores": quality,
        "notes": notes[:1000],
    }


def _quality(
    ctx: Ctx, checks: list[dict[str, Any]], evidence_count: int, reproducible: str | None
) -> dict[str, Any]:
    weights = dict(_spec(ctx).get("quality_weights") or DEFAULT_SPEC["quality_weights"])
    passed = sum(1 for c in checks if c["pass"])
    consistency = passed / max(1, len(checks))
    out: dict[str, Any] = {
        "evidence": round(min(1.0, evidence_count / 4.0) if evidence_count else 0.35, 2),
        "repro": round(
            {"yes": 1.0, "flaky": 0.8, "no": 0.6, "blocked": 0.2}.get(str(reproducible), 0.5), 2
        ),
        "consistency": round(consistency, 2),
        "checks_passed": passed,
        "checks_total": len(checks),
    }
    out["overall"] = round(
        sum(float(w) * float(out.get(k, consistency)) for k, w in weights.items()), 2
    )
    return out


def _notes(
    ctx: Ctx,
    checks: list[dict[str, Any]],
    verdict: str,
    reason_code: str,
    loop_seq: int,
    requested_focus: str,
) -> str:
    spec = _spec(ctx)
    failed = [c["id"] for c in checks if not c["pass"]]
    parts = [
        str(spec.get("notes") or "Mock 复核说明"),
        f"第 {loop_seq} 轮机械预检 {sum(1 for c in checks if c['pass'])}/{len(checks)} 通过",
        f"结论 {verdict}（{reason_code}）",
    ]
    if failed:
        parts.append("未通过项：" + "、".join(failed))
    if requested_focus:
        parts.append(f"上一轮要求补充：{requested_focus[:160]}")
    return "；".join(parts)
