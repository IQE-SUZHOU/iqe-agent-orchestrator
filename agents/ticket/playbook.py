"""Ticket 剧本：查重 → 路由（rca × reproducible）→ 工单正文（Mock Jira）。

替换点（真实实现）：Jira REST（POST /issue、issue links）。Mock 只产生 external_key / url / 正文，
并把完整 payload 落到 evidence 表，便于审计与人工批准（TICKET_AUTOCREATE=false 时只 drafted）。

剧本规则：
1. cfg.ticket_autocreate == False → ticket_status="drafted"，external_key="DRAFT-<case前6位>"。
2. 夹具 fixtures/ticket/duplicate.json（或输入里 duplicate_of 非空）→ linked_duplicate。
3. 路由表：reproducible ∈ {no, flaky} → P3；driver+yes → P1；test+yes → P2；environment+yes → P2；
   infrastructure → P3；unknown → P3。assignee_team 取 fixtures/ticket/default.json 的 assignee_map。
4. sections 必须来自夹具定义（Review 预检要求含 rca / repro / evidence 三段且非空）。
"""
from __future__ import annotations

import json
import logging
import zlib
from typing import Any

from agentkit.agent_runtime import Blocked, Ctx
from agentkit.config import cfg

log = logging.getLogger("agent.ticket.playbook")

DEFAULT_SPEC: dict[str, Any] = {
    "backend": "jira", "project_key": "OST",
    "sections": [{"key": "rca", "title": "根因结论"}, {"key": "repro", "title": "复现结论与矩阵"},
                 {"key": "evidence", "title": "证据清单"}, {"key": "impact", "title": "影响面"},
                 {"key": "next_steps", "title": "建议下一步"}],
    "assignee_map": {"driver": "OS-Driver", "test": "OS-QA", "environment": "OS-Infra",
                     "infrastructure": "OS-Infra", "unknown": "OS-Triage"},
}
REQUIRED_SECTION_KEYS = ("rca", "repro", "evidence")


def _spec(ctx: Ctx) -> dict[str, Any]:
    doc = dict(_pick_json(ctx, "ticket") or {})
    merged = {**DEFAULT_SPEC, **doc}
    if not merged.get("sections"):
        merged["sections"] = DEFAULT_SPEC["sections"]
    return merged


def _priority(rca: str, reproducible: str | None) -> str:
    if reproducible in ("no", "flaky"):
        return "P3"
    if rca == "driver" and reproducible == "yes":
        return "P1"
    if rca in ("test", "environment") and reproducible == "yes":
        return "P2"
    if rca in ("test", "environment"):
        return "P2"
    return "P3"


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


async def handler(ctx: Ctx) -> dict[str, Any]:
    payload = ctx.env.get("payload", {}) or {}
    src = payload.get("input", {}) or {}
    case_id = src.get("case_id") or ctx.env.get("case_id")
    job = str(src.get("job") or "unknown-job")
    build = src.get("build") if isinstance(src.get("build"), int) else 0
    rca = str(src.get("rca") or "unknown")
    reproducible = src.get("reproducible")
    signature = src.get("signature")
    requested_focus = src.get("requested_focus") or ""
    if not case_id:
        raise Blocked("schema_invalid", "ticket 输入缺少 case_id")

    spec = _spec(ctx)
    backend = str(getattr(cfg, "ticket_backend", "") or spec.get("backend") or "jira")
    autocreate = bool(getattr(cfg, "ticket_autocreate", True))
    assignee_map = dict(spec.get("assignee_map") or DEFAULT_SPEC["assignee_map"])
    project_key = str(spec.get("project_key") or "OST")

    # 查重：夹具剧本（duplicate.json）或上游 triage 的 duplicate_of
    dup_from_fixture = spec.get("duplicate_of_key")
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

    priority = _priority(rca, reproducible)
    assignee = assignee_map.get(rca, "OS-Triage")

    triage_notes = src.get("triage_notes") or []
    key_frames = src.get("key_frames") or []
    repro_matrix = src.get("matrix") or []
    refs = src.get("evidence_refs") or src.get("new_evidence") or []

    def _text(sec_key: str) -> str:
        if sec_key == "rca":
            body = [f"根因判定：{rca}（签名 {signature or '未提供'}）。"]
            body += [f"- {n}" for n in triage_notes[:4]]
            if requested_focus:
                body.append(f"- 复核补充要求：{requested_focus[:150]}")
            return "\n".join(body)
        if sec_key == "repro":
            head = f"受控重跑结论：{reproducible if reproducible else '未做复现（计划未含复现段）'}"
            rows = [f"- {m.get('os')} / {m.get('config')} → {m.get('outcome')}" for m in repro_matrix[:8]]
            return "\n".join([head] + (rows or ["- 无重跑矩阵"]))
        if sec_key == "evidence":
            rows = [f"- {r.get('kind')} {r.get('uri')} {r.get('note', '')}".rstrip() for r in refs[:12]]
            frames = [f"- 关键帧：{k[:160]}" for k in key_frames[:5]]
            return "\n".join(rows + frames or ["- 见证据表 evidence 记录"])
        if sec_key == "impact":
            return (f"影响 job：{job}#{build or '-'}（{project_key} 流水线）。"
                    f"同签名族在本次扫描内的其他构建已合并处理，未合并的重复告警为 0。")
        if sec_key == "next_steps":
            if ticket_status == "linked_duplicate":
                return f"建议：关联到已有工单 {duplicate_of}，本 Case 不另建单，仅补充新证据。"
            return (f"建议：{'驱动组' if rca == 'driver' else '对应负责组'}按复现矩阵定位变更点；"
                    f"若复现结论为 flaky，先做稳定性看板跟踪再决定修复优先级。")
        return f"（{sec_key} 段说明）"

    sections = [{"key": s["key"], "title": s.get("title") or s["key"], "content": _text(s["key"])}
                for s in spec["sections"]]
    missing = [k for k in REQUIRED_SECTION_KEYS if k not in {s["key"] for s in sections}]
    if missing:
        raise Blocked("schema_invalid", f"工单缺少必需段落: {missing}")

    audit = {"backend": backend, "autocreate": autocreate, "project_key": project_key,
             "case_id": case_id, "signature": signature, "scenario": ctx.scenario,
             "dedup_source": "fixture" if dup_from_fixture else ("triage" if dup_from_input else "none"),
             "routing_rule": "rca×reproducible", "priority": priority, "assignee_team": assignee}

    await ctx.evidence("ticket_payload", json.dumps(
        {"external_key": external_key, "url": url, "status": ticket_status,
         "priority": priority, "assignee_team": assignee, "sections": sections},
        ensure_ascii=False, indent=2),
        {"case_id": case_id, "backend": backend, "ticket_status": ticket_status,
         "note": f"{ticket_status}：{external_key or duplicate_of}（{priority}/{assignee}）"})

    return {
        "case_id": case_id,
        "backend": backend,
        "external_key": external_key,
        "url": url,
        "ticket_status": ticket_status,
        "duplicate_of": duplicate_of,
        "ticket_priority": priority,
        "assignee_team": assignee,
        "linked_prs": [],
        "sections": sections,
        "audit": audit,
    }
