"""Reviewer 剧本（阶段 3 / 6a / 8b）：三道闸门的机械预检 + 结论（Mock）。

一个 Agent 承载三道闸门，靠 payload.mode 区分；每道闸门只回答自己那一个问题：

gate1（阶段 3）  分诊结论能不能用来分流？        approve + decision=internal|external
gate2（阶段 6a） 修复是否真的被验证通过？        approve / rework（回炉 4a）/ reject
gate3（阶段 8b） 工单是否具备可交付质量？        approve / needs_info（回炉 7b）/ reject

预检结果写进 output.checks，编排层与自测都按 checks[i].id 取值（id 即契约编号）。
夹具 ``fixtures/reviewer/<scenario>.json`` 的 ``gate1|gate2|gate3`` 段可整体覆盖结论
（演示 reject / rework / needs_info / 路径反转），``first_loop_only=true`` 时只覆盖第一轮。
"""

from __future__ import annotations

import logging
from typing import Any

from iqe_agents.common.config import cfg
from iqe_agents.runtime import Blocked, Ctx

log = logging.getLogger("agent.reviewer.playbook")

CONFIDENCE_MIN = 0.70
MIN_EVIDENCE = 2
REQUIRED_TICKET_SECTIONS = ("rca", "repro", "evidence")
DEFAULT_SPEC: dict[str, Any] = {
    "quality_weights": {"evidence": 0.4, "consistency": 0.3, "path": 0.3},
    "gate1": {"verdict": "approve", "reason_code": "ok", "notes": "分诊结论可用于分流"},
    "gate2": {"verdict": "approve", "reason_code": "ok", "notes": "修复与验证证据一致"},
    "gate3": {"verdict": "approve", "reason_code": "ok", "notes": "工单内容齐备"},
}
REQUEST_INFO = {
    "evidence_present": "补充 Case 证据：失败段 console 与 dmesg 尾部",
    "triage_confident": "重新分诊：给出明确 rca 与更高置信度（附关键帧与知识库依据）",
    "path_known": "明确失败归属：内部自动化（internal）还是产品侧（external）",
    "fix_applied": "4a 未给出可用的修复改动，请补 diff 与目标文件",
    "change_target": "补上修复目标（文件/配置路径）",
    "verification_present": "补一次带修复的完整重跑（verify 模式）",
    "fix_verified": "验证未通过：附修复前后 dmesg 对比或回退修复",
    "ticket_status": "工单未创建成功，请重试或改走草稿",
    "ticket_sections": "工单补齐 rca / repro / evidence 段落",
    "owner_assigned": "确认责任团队（assignee_team）",
    "loop_not_exhausted": "回炉已达上限，请人工介入收尾",
}


def _scenario_names(scenario: str) -> list[str]:
    out: list[str] = []
    for name in (scenario, str(scenario).replace("-", "_"), str(scenario).replace("_", "-")):
        if name and name not in out:
            out.append(name)
    return out


def _pick_json(ctx: Ctx) -> dict[str, Any]:
    """夹具名兼容连字符/下划线；都缺则回落 default。"""
    for name in _scenario_names(ctx.scenario or ""):
        if ctx.fixtures.exists("reviewer", f"{name}.json"):
            return ctx.fixtures.json("reviewer", f"{name}.json")
    return ctx.fixtures.pick_json("reviewer", "default", fallback="default")[1]


def _spec(ctx: Ctx) -> dict[str, Any]:
    """只取夹具里显式写出的段：缺省段不覆盖机械预检算出来的结论。"""
    return dict(_pick_json(ctx) or {})


def _check(cid: str, name: str, passed: bool, detail: str, reason: str | None = None) -> dict:
    return {
        "id": cid,
        "name": name,
        "pass": bool(passed),
        "detail": detail[:220],
        "reason_code": None if passed else reason,
    }


def _loop_check(loop_seq: int, loop_max: int) -> dict:
    return _check(
        "loop_not_exhausted",
        f"回炉轮次 <= {loop_max}",
        loop_seq <= loop_max,
        f"当前第 {loop_seq} 轮（上限 {loop_max} 轮）",
        "loop_exhausted",
    )


# --------------------------------------------------------------------------- gate1
def _gate1(src: dict, loop_seq: int, loop_max: int) -> tuple[list[dict], str, str]:
    rca = str(src.get("rca") or "unknown")
    confidence = src.get("confidence")
    path = str(src.get("path") or "")
    key_frames = list(src.get("key_frames") or [])
    evidence_count = int(src.get("evidence_count") or 0)

    checks = [
        _check(
            "evidence_present",
            "Case 已有证据引用",
            evidence_count > 0 or bool(key_frames),
            f"证据 {evidence_count} 条，关键帧 {len(key_frames)} 条",
            "insufficient_evidence",
        ),
        _check(
            "triage_confident",
            f"分诊结论明确且置信度 >= {CONFIDENCE_MIN:.2f}",
            rca != "unknown"
            and isinstance(confidence, (int, float))
            and confidence >= CONFIDENCE_MIN,
            f"rca={rca}，置信度 {confidence if confidence is not None else '未提供'}",
            "insufficient_evidence",
        ),
        _check(
            "path_known",
            "失败归属可判定（internal / external）",
            path in ("internal", "external"),
            f"triage 预判路径={path or '未提供'}，知识库 {len(src.get('kb_refs') or [])} 条依据",
            "insufficient_evidence",
        ),
        _loop_check(loop_seq, loop_max),
    ]
    failed = [c for c in checks if not c["pass"]]
    if failed:
        return checks, "needs_info", failed[0].get("reason_code") or "insufficient_evidence"
    return checks, "approve", "ok"


# --------------------------------------------------------------------------- gate2
def _gate2(src: dict, loop_seq: int, loop_max: int) -> tuple[list[dict], str, str]:
    fix = dict(src.get("remediation") or {})
    repro = dict(src.get("reproduction") or {})
    evidence_count = int(src.get("evidence_count") or 0)
    fix_verified = repro.get("fix_verified")

    checks = [
        _check(
            "fix_applied",
            "4a 已给出可用的修复改动",
            bool(fix.get("fixed")),
            f"fixed={fix.get('fixed')}，change_type={fix.get('change_type')}",
            "missing_repro",
        ),
        _check(
            "change_target",
            "修复目标明确",
            bool(str(fix.get("target") or "").strip()),
            f"target={fix.get('target') or '未提供'}，risk={fix.get('risk')}",
            "insufficient_evidence",
        ),
        _check(
            "verification_present",
            "5a 验证阶段已执行",
            bool(repro) and repro.get("mode") == "verify",
            f"reproduction.mode={repro.get('mode') or '未执行'}，attempts={repro.get('attempts')}",
            "missing_repro",
        ),
        _check(
            "fix_verified",
            "验证结论为「修复有效」",
            fix_verified is True,
            f"fix_verified={fix_verified}，reproducible={repro.get('reproducible')}",
            "fix_not_verified",
        ),
        _check(
            "evidence_present",
            f"证据条数 >= {MIN_EVIDENCE}",
            evidence_count >= MIN_EVIDENCE,
            f"证据 {evidence_count} 条（要求 >= {MIN_EVIDENCE}）",
            "insufficient_evidence",
        ),
        _loop_check(loop_seq, loop_max),
    ]
    failed = [c for c in checks if not c["pass"]]
    if not failed:
        return checks, "approve", "ok"
    if any(c["id"] == "fix_verified" for c in failed):
        return checks, "rework", "fix_not_verified"
    return checks, "rework", failed[0].get("reason_code") or "insufficient_evidence"


# --------------------------------------------------------------------------- gate3
def _gate3(src: dict, loop_seq: int, loop_max: int) -> tuple[list[dict], str, str]:
    ticket = dict(src.get("ticketing") or {})
    sections = list(ticket.get("sections") or [])
    keys = {str(s.get("key")) for s in sections}
    empty = [
        s.get("key")
        for s in sections
        if str(s.get("key")) in REQUIRED_TICKET_SECTIONS and not str(s.get("content") or "").strip()
    ]
    missing = [k for k in REQUIRED_TICKET_SECTIONS if k not in keys] + empty
    status = ticket.get("ticket_status")
    dup = (status == "linked_duplicate" and bool(ticket.get("duplicate_of"))) or (
        status == "created" and bool(ticket.get("duplicate_of"))
    )

    checks = [
        _check(
            "ticket_status",
            "工单已创建或已关联",
            status in ("created", "drafted", "linked_duplicate"),
            f"ticket_status={status or '未创建'}，external_key={ticket.get('external_key')}",
            "insufficient_evidence",
        ),
        _check(
            "ticket_sections",
            "工单包含 rca / repro / evidence 且非空",
            not missing,
            f"缺失或为空的段落：{missing or '无'}",
            "missing_ticket_sections",
        ),
        _check(
            "owner_assigned",
            "责任团队已指派",
            bool(str(ticket.get("assignee_team") or "").strip()),
            f"assignee_team={ticket.get('assignee_team') or '未指派'}，"
            f"priority={ticket.get('priority') or ticket.get('ticket_priority')}",
            "wrong_owner",
        ),
        _check(
            "duplicate_consistent",
            "重复工单关系自洽",
            not dup or status == "linked_duplicate",
            f"ticket_status={status}，duplicate_of={ticket.get('duplicate_of')}",
            "duplicate_ticket",
        ),
        _loop_check(loop_seq, loop_max),
    ]
    failed = [c for c in checks if not c["pass"]]
    if not failed:
        return checks, "approve", "ok"
    return checks, "needs_info", failed[0].get("reason_code") or "missing_ticket_sections"


# --------------------------------------------------------------------------- 主流程
async def handler(ctx: Ctx) -> dict[str, Any]:
    payload = ctx.env.get("payload", {}) or {}
    gate = str(payload.get("mode") or "gate1")
    src = payload.get("input", {}) or {}
    case_id = src.get("case_id") or ctx.env.get("case_id")
    if not case_id:
        raise Blocked("schema_invalid", f"reviewer 输入缺少 case_id（gate={gate}）")
    if gate not in ("gate1", "gate2", "gate3"):
        raise Blocked("schema_invalid", f"reviewer 不支持的 gate={gate}")

    requested_focus = str(src.get("requested_focus") or "")
    loop_seq = int(src.get("loop_seq") or 1)
    loop_max = int(getattr(cfg, "max_review_loops", 2) or 2)
    evidence_count = int(src.get("evidence_count") or 0)
    decision = "none"

    if gate == "gate1":
        checks, verdict, reason_code = _gate1(
            {**src, "evidence_count": evidence_count}, loop_seq, loop_max
        )
        decision = str(src.get("path") or "external")
    elif gate == "gate2":
        checks, verdict, reason_code = _gate2(
            {**src, "evidence_count": evidence_count}, loop_seq, loop_max
        )
    else:
        checks, verdict, reason_code = _gate3(src, loop_seq, loop_max)

    # 夹具覆盖（演示 reject / rework / needs_info / 路径反转）：
    # 只允许把 approve 改成别的结论（或用 "force": true 强制），
    # 不会把机械预检已判定不通过的闸门改回 approve。
    spec = dict(_spec(ctx).get(gate) or {})
    first_only = bool(spec.get("first_loop_only", False)) and loop_seq > 1
    requested: list[str] = []
    if spec and not first_only:
        want = str(spec.get("verdict") or "")
        if want and (verdict == "approve" or bool(spec.get("force"))):
            verdict, reason_code = want, str(spec.get("reason_code") or reason_code)
        if gate == "gate1" and verdict == "approve":
            decision = str(spec.get("decision") or decision)
        if verdict != "approve":
            requested = [str(x) for x in (spec.get("requested_info") or [])]

    if not _loop_check(loop_seq, loop_max)["pass"]:
        verdict, reason_code = "reject", "loop_exhausted"
    if gate == "gate1" and verdict != "approve":
        decision = "none"
    if verdict in ("rework", "needs_info", "reject") and not requested:
        failed = [c for c in checks if not c["pass"]]
        requested = [REQUEST_INFO.get(c["id"], c["detail"]) for c in failed[:3]] or [
            REQUEST_INFO["loop_not_exhausted"]
        ]

    quality = _quality(checks, evidence_count, verdict)
    notes = _notes(ctx, gate, checks, verdict, reason_code, loop_seq, requested_focus)

    await ctx.evidence(
        "review_record",
        "\n".join(
            [
                f"闸门 {gate}（第 {loop_seq} 轮）：{verdict}（{reason_code}）"
                + (f"，decision={decision}" if gate == "gate1" else ""),
            ]
            + [
                f"{'PASS' if c['pass'] else 'FAIL'} #{i + 1} {c['id']}：{c['detail']}"
                for i, c in enumerate(checks)
            ]
            + [f"requested_info: {r}" for r in requested]
        ),
        {
            "case_id": case_id,
            "gate": gate,
            "verdict": verdict,
            "decision": decision,
            "reason_code": reason_code,
            "loop_seq": loop_seq,
            "checks_failed": [c["id"] for c in checks if not c["pass"]],
            "note": f"{gate} {verdict}/{reason_code}（第 {loop_seq} 轮，"
            f"失败 {sum(1 for c in checks if not c['pass'])}/{len(checks)} 项）",
        },
    )
    log.info(
        "reviewer_gate",
        extra={
            "ctx": {
                "case_id": case_id,
                "gate": gate,
                "verdict": verdict,
                "decision": decision,
                "loop_seq": loop_seq,
            }
        },
    )

    return {
        "case_id": case_id,
        "gate": gate,
        "verdict": verdict,
        "decision": decision,
        "reason_code": reason_code,
        "requested_info": requested[:10],
        "checks": checks[:20],
        "quality_scores": quality,
        "notes": notes[:1000],
    }


def _quality(checks: list[dict], evidence_count: int, verdict: str) -> dict[str, Any]:
    passed = sum(1 for c in checks if c["pass"])
    consistency = passed / max(1, len(checks))
    out: dict[str, Any] = {
        "evidence": round(min(1.0, evidence_count / 4.0) if evidence_count else 0.35, 2),
        "consistency": round(consistency, 2),
        "path": 1.0 if verdict == "approve" else 0.4,
        "checks_passed": passed,
        "checks_total": len(checks),
    }
    out["overall"] = round(
        (out["evidence"] * 0.4 + out["consistency"] * 0.3 + out["path"] * 0.3), 2
    )
    return out


def _notes(
    ctx: Ctx,
    gate: str,
    checks: list[dict],
    verdict: str,
    reason_code: str,
    loop_seq: int,
    requested_focus: str,
) -> str:
    spec = dict(_spec(ctx).get(gate) or {})
    failed = [c["id"] for c in checks if not c["pass"]]
    # 夹具说明只在它与最终结论一致时引用，避免「rework 却写着修复证据一致」
    spec_notes = (
        str(spec.get("notes") or "") if str(spec.get("verdict") or verdict) == verdict else ""
    )
    parts = [
        spec_notes or f"{gate} Mock 复核说明",
        f"第 {loop_seq} 轮机械预检 {sum(1 for c in checks if c['pass'])}/{len(checks)} 通过",
        f"结论 {verdict}（{reason_code}）",
    ]
    if failed:
        parts.append("未通过项：" + "、".join(failed))
    if requested_focus:
        parts.append(f"上一轮要求补充：{requested_focus[:160]}")
    return "；".join(parts)
