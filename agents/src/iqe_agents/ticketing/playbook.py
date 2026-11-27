"""Ticketing 剧本（阶段 7b）：查重 → 路由（rca × 复现 × 回归）→ 工单正文（Mock Jira）。

替换点（真实实现）：Jira REST（POST /issue、issue links）。Mock 只产生 external_key / url / 正文，
并把完整 payload 落到 evidence 表，便于审计与闸门 3 复核（TICKET_AUTOCREATE=false 时只 drafted）。

剧本规则：
1. cfg.ticket_autocreate == False → ticket_status="drafted"，external_key="DRAFT-<case前6位>"；
2. 夹具 duplicate_of（或上游 triage 的 duplicate_of）非空 → linked_duplicate，不另建单；
3. 优先级：回归且有 guilty patch → P1；driver+可复现 → P1；test/environment+可复现 → P2；
   不可复现或 flaky → P3；其余 → P2；
4. sections 由夹具定义，必须含 rca / repro / evidence 三段且非空（闸门 3 会校验）。
"""

from __future__ import annotations

import json
import logging
import zlib
from typing import Any

from iqe_agents.common.config import cfg
from iqe_agents.runtime import Blocked, Ctx

log = logging.getLogger("agent.ticketing.playbook")

DEFAULT_SPEC: dict[str, Any] = {
    "backend": "jira",
    "project_key": "OST",
    "sections": [
        {"key": "rca", "title": "根因结论"},
        {"key": "repro", "title": "复现结论与矩阵"},
        {"key": "regression", "title": "回归判定与 guilty patch"},
        {"key": "evidence", "title": "证据清单"},
        {"key": "impact", "title": "影响面"},
        {"key": "next_steps", "title": "建议下一步"},
    ],
    "assignee_map": {
        "driver": "OS-Driver",
        "test": "OS-QA",
        "environment": "OS-Infra",
        "infrastructure": "OS-Infra",
        "unknown": "OS-Triage",
    },
}
REQUIRED_SECTION_KEYS = ("rca", "repro", "evidence")


def _scenario_names(scenario: str) -> list[str]:
    out: list[str] = []
    for name in (scenario, str(scenario).replace("-", "_"), str(scenario).replace("_", "-")):
        if name and name not in out:
            out.append(name)
    return out


def _pick_json(ctx: Ctx) -> dict[str, Any]:
    for name in _scenario_names(ctx.scenario or ""):
        if ctx.fixtures.exists("ticketing", f"{name}.json"):
            return ctx.fixtures.json("ticketing", f"{name}.json")
    return ctx.fixtures.pick_json("ticketing", "default", fallback="default")[1]


def _spec(ctx: Ctx) -> dict[str, Any]:
    merged = {**DEFAULT_SPEC, **(_pick_json(ctx) or {})}
    if not merged.get("sections"):
        merged["sections"] = DEFAULT_SPEC["sections"]
    return merged


def _priority(rca: str, reproducible: str | None, regression: dict) -> str:
    patch = regression.get("guilty_patch") or {}
    if regression.get("is_regression") and patch.get("commit"):
        return "P1"
    if reproducible in ("no", "flaky"):
        return "P3"
    if rca == "driver" and reproducible == "yes":
        return "P1"
    if rca in ("test", "environment"):
        return "P2"
    return "P2"


async def handler(ctx: Ctx) -> dict[str, Any]:
    payload = ctx.env.get("payload", {}) or {}
    src = payload.get("input", {}) or {}
    case_id = src.get("case_id") or ctx.env.get("case_id")
    job = str(src.get("job") or "unknown-job")
    build = src.get("build") if isinstance(src.get("build"), int) else 0
    rca = str(src.get("rca") or "unknown")
    signature = src.get("signature")
    requested_focus = str(src.get("requested_focus") or "")
    repro = dict(src.get("reproduction") or {})
    regression = dict(src.get("regression") or {})
    if not case_id:
        raise Blocked("schema_invalid", "ticketing 输入缺少 case_id")

    spec = _spec(ctx)
    backend = str(getattr(cfg, "ticket_backend", "") or spec.get("backend") or "jira")
    autocreate = bool(getattr(cfg, "ticket_autocreate", True))
    assignee_map = dict(spec.get("assignee_map") or DEFAULT_SPEC["assignee_map"])
    project_key = str(spec.get("project_key") or "OST")

    dup_from_fixture = spec.get("duplicate_of_key") or spec.get("duplicate_of")
    dup_from_input = src.get("duplicate_of")
    duplicate_of = dup_from_fixture or dup_from_input

    if duplicate_of:
        ticket_status = "linked_duplicate"
        external_key = None
        url = None
    elif autocreate:
        ticket_status = "created"
        external_key = f"{project_key}-{1000 + (zlib.crc32(str(case_id).encode('utf-8')) % 800)}"
        url = f"http://jira.mock/browse/{external_key}"
    else:
        ticket_status = "drafted"
        external_key = f"DRAFT-{str(case_id)[:6]}"
        url = None

    priority = _priority(rca, repro.get("reproducible"), regression)
    assignee = assignee_map.get(rca, "OS-Triage")
    patch = regression.get("guilty_patch") or {}
    refs = list(src.get("evidence_refs") or []) + list(repro.get("new_evidence") or [])

    def _text(sec_key: str) -> str:
        if sec_key == "rca":
            body = [
                f"根因判定：{rca}（组件 {src.get('component') or '—'}，签名 {signature or '未提供'}）。"
            ]
            if requested_focus:
                body.append(f"- 闸门补充要求：{requested_focus[:150]}")
            return "\n".join(body)
        if sec_key == "repro":
            head = f"复现结论：{repro.get('reproducible') or '未做复现'}"
            if repro.get("attempts"):
                head += f"（{repro.get('attempts')} 次）"
            rows = [
                f"- {m.get('os')} / {m.get('config')} → {m.get('outcome')}"
                for m in (repro.get("matrix") or [])[:8]
            ]
            return "\n".join([head] + (rows or ["- 无重跑矩阵（测试类失败不占机位）"]))
        if sec_key == "regression":
            if not regression:
                return "未执行回归判定（计划未含 regression 段）"
            if regression.get("is_regression") is False:
                return "判定为非回归：同配置历史基线同样失败，属长期环境问题"
            if patch:
                return (
                    f"判定为回归，guilty patch：{patch.get('commit')}"
                    f"（{patch.get('pr')} / {patch.get('author')}，"
                    f"置信度 {patch.get('confidence')}）\n{patch.get('rationale') or ''}"
                ).strip()
            return "判定为回归，但二分未收敛：需人工在候选区间内继续定位"
        if sec_key == "evidence":
            rows = [
                f"- {r.get('kind')} {r.get('uri')} {r.get('note', '')}".rstrip() for r in refs[:12]
            ]
            return "\n".join(rows or ["- 见证据表 evidence 记录"])
        if sec_key == "impact":
            return (
                f"影响 job：{job}#{build or '-'}（{project_key} 流水线）。"
                f"同签名族在本次扫描内已合并处理。"
            )
        if sec_key == "next_steps":
            if ticket_status == "linked_duplicate":
                return f"建议：关联到已有工单 {duplicate_of}，本 Case 不另建单，仅补充新证据。"
            if regression.get("is_regression") is False:
                return "建议：按环境问题跟踪，补齐基线与机位健康检查，不阻塞发布。"
            if patch:
                return f"建议：{assignee} 复核 {patch.get('commit')} 并决定回滚或补丁修复。"
            return f"建议：{assignee} 在候选区间内继续二分定位。"
        return f"（{sec_key} 段说明）"

    sections = [
        {"key": s["key"], "title": s.get("title") or s["key"], "content": _text(s["key"])}
        for s in spec["sections"]
    ]
    missing = [k for k in REQUIRED_SECTION_KEYS if k not in {s["key"] for s in sections}]
    if missing:
        raise Blocked("schema_invalid", f"工单缺少必需段落: {missing}")

    audit = {
        "backend": backend,
        "autocreate": autocreate,
        "project_key": project_key,
        "case_id": case_id,
        "signature": signature,
        "scenario": ctx.scenario,
        "dedup_source": "fixture" if dup_from_fixture else ("triage" if dup_from_input else "none"),
        "routing_rule": "rca×reproducible×regression",
        "priority": priority,
        "assignee_team": assignee,
    }

    await ctx.evidence(
        "ticket_payload",
        json.dumps(
            {
                "external_key": external_key,
                "url": url,
                "status": ticket_status,
                "priority": priority,
                "assignee_team": assignee,
                "sections": sections,
            },
            ensure_ascii=False,
            indent=2,
        ),
        {
            "case_id": case_id,
            "backend": backend,
            "ticket_status": ticket_status,
            "note": f"{ticket_status}：{external_key or duplicate_of}（{priority}/{assignee}）",
        },
    )
    log.info(
        "ticketing_done",
        extra={
            "ctx": {
                "case_id": case_id,
                "status": ticket_status,
                "priority": priority,
                "assignee": assignee,
            }
        },
    )

    return {
        "case_id": case_id,
        "backend": backend,
        "external_key": external_key,
        "url": url,
        "ticket_status": ticket_status,
        "duplicate_of": duplicate_of,
        "priority": priority,
        "assignee_team": assignee,
        "linked_patches": [patch.get("commit")] if patch.get("commit") else [],
        "sections": sections,
        "audit": audit,
    }
