"""Reporting 剧本（阶段 9）：结案报告 / 日报 + 知识库回写清单。

数据来源全部是 Postgres（cases / stage_runs / evidence / scans / daily_reports），
剧本本身不做推理：
- case：get_case + stage_outputs + evidence → Markdown 结案报告 + kb_writes；
- daily：daily_metrics(day) + list_cases(limit) + fixtures/reporting/template.md → 日报正文。

知识库回写遵循写权分工：Agent 只把条目放进 result 的 ``kb_writes``，
由编排层落到 kb_entries 表（Agent 不直写知识库）。
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any

from iqe_agents.runtime import Blocked, Ctx

log = logging.getLogger("agent.reporting.playbook")

STAGE_ORDER = (
    "triage",
    "reviewer",
    "remediation",
    "reproduction",
    "regression",
    "ticketing",
    "reporting",
)
STAGE_LABEL = {
    "monitor": "1 Monitor 失败发现",
    "triage": "2 Triage 分诊",
    "reviewer": "3/6a/8b Reviewer 闸门",
    "remediation": "4a Remediation 修复",
    "reproduction": "4b/5a 复现与验证",
    "regression": "5b/6c 回归判定与定位",
    "ticketing": "7b Ticketing 建单",
    "reporting": "9 Reporting 报告",
}
OPEN_STATUSES = (
    "created",
    "triaged",
    "routed_internal",
    "routed_external",
    "remediated",
    "fix_verified",
    "reproduced",
    "regression_checked",
    "bisected",
    "ticketed",
)
GATE_LABEL = {"gate1": "闸门 1（分流）", "gate2": "闸门 2（修复）", "gate3": "闸门 3（工单）"}


def _today() -> str:
    from iqe_agents.common.dates import today_str

    return today_str()


def _date_of(value: Any) -> str:
    from iqe_agents.common.dates import date_of

    return date_of(value)


def _ms(row: dict) -> int:
    if isinstance(row.get("latency_ms"), int):
        return row["latency_ms"]
    start, end = row.get("started_at"), row.get("finished_at")
    if isinstance(start, dt.datetime) and isinstance(end, dt.datetime):
        return int((end - start).total_seconds() * 1000)
    return 0


def _render(template: str, mapping: dict[str, str]) -> str:
    out = template
    for key, val in mapping.items():
        out = out.replace("{{" + key + "}}", val)
    return out


def _metrics_jsonable(metrics: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in metrics.items():
        try:
            json.dumps(v, ensure_ascii=False)
            out[k] = v
        except TypeError:
            out[k] = str(v)
    return out


def _by_mode(outputs: dict[str, list[dict]], stage: str) -> dict[str, dict]:
    """stage → {mode: output}（同一 stage 多次执行时按 mode 取最新一次）。"""
    out: dict[str, dict] = {}
    for row in outputs.get(stage) or []:
        out[str(row.get("mode") or "")] = row.get("output") or {}
    return out


def _kb_writes(case: dict, outputs: dict[str, list[dict]], reports: dict[str, dict]) -> list[dict]:
    """把本 Case 的可复用结论整理成知识条目（由编排层落库）。"""
    triage = outputs.get("triage") or []
    triage_out = (triage[-1].get("output") if triage else {}) or {}
    component = str(triage_out.get("component") or "unknown")
    signature = triage_out.get("signature") or case.get("signature")
    case_id = case.get("case_id")
    entries: list[dict] = []

    if triage_out.get("rca") and triage_out.get("rca") != "unknown":
        entries.append(
            {
                "kind": "failure",
                "component": component,
                "signature": signature,
                "summary": (
                    f"rca={triage_out.get('rca')}，路径={triage_out.get('path')}，"
                    f"置信度={triage_out.get('confidence')}；"
                    f"{(triage_out.get('triage_notes') or [''])[0][:160]}"
                ),
                "case_id": case_id,
            }
        )

    fix = reports.get("fix") or {}
    if fix.get("fixed"):
        entries.append(
            {
                "kind": "fix",
                "component": component,
                "signature": signature,
                "summary": f"{fix.get('change_type')} → {fix.get('target')}：{fix.get('diff_summary')}",
                "case_id": case_id,
            }
        )

    patch = reports.get("guilty_patch") or {}
    if patch.get("commit"):
        entries.append(
            {
                "kind": "guilty_patch",
                "component": component,
                "signature": signature,
                "summary": (
                    f"{patch.get('commit')}（{patch.get('pr')} / {patch.get('author')}）："
                    f"{patch.get('rationale') or ''}"
                ),
                "case_id": case_id,
            }
        )

    verdicts = reports.get("verdicts") or {}
    if verdicts.get("gate3") == "approve" or verdicts.get("gate2") == "approve":
        entries.append(
            {
                "kind": "best_practice",
                "component": component,
                "signature": signature,
                "summary": (
                    f"该路径（{reports.get('path') or 'unknown'}）通过闸门复核："
                    f"gate2={verdicts.get('gate2', '—')}，gate3={verdicts.get('gate3', '—')}"
                ),
                "case_id": case_id,
            }
        )
    return entries[:10]


# --------------------------------------------------------------------------- mode=case
async def _case_report(ctx: Ctx, case_id: str) -> dict[str, Any]:
    case = await ctx.db.get_case(case_id)
    if case is None:
        raise Blocked("schema_invalid", f"cases 表里没有 {case_id}，无法出结案报告")
    trace = list(await ctx.db.trace(case_id) or [])
    evidence = list(await ctx.db.evidence(case_id) or [])
    outputs = await ctx.db.stage_outputs(case_id)

    triage_out = (outputs.get("triage") or [{}])[-1].get("output") or {}
    gates = _by_mode(outputs, "reviewer")
    repro = _by_mode(outputs, "reproduction")
    reg = _by_mode(outputs, "regression")
    fix = (outputs.get("remediation") or [{}])[-1].get("output") or {}
    ticket = (outputs.get("ticketing") or [{}])[-1].get("output") or {}

    verdicts = {gate: str((gates.get(gate) or {}).get("verdict")) for gate in GATE_LABEL}
    decisions = {gate: (gates.get(gate) or {}).get("decision") for gate in GATE_LABEL}
    path = decisions.get("gate1") if decisions.get("gate1") in ("internal", "external") else None
    path = path or triage_out.get("path") or "unknown"
    verify = repro.get("verify") or {}
    evidence_mode = repro.get("evidence") or {}
    check_out = reg.get("check") or {}
    bisect_out = reg.get("bisect") or {}
    patch = bisect_out.get("guilty_patch") or {}

    stage_rows = [r for r in trace if r.get("stage") in STAGE_LABEL]
    tat_ms = sum(_ms(r) for r in stage_rows)
    stage_status = {r.get("stage"): r.get("status") for r in stage_rows}

    md: list[str] = [f"# Case 结案报告 {case_id}", ""]
    md += [
        "## 结论摘要",
        f"- 状态：**{case.get('status') or 'unknown'}**（signal={case.get('signal')}，"
        f"job={case.get('job')}#{case.get('build') or '-'}）",
        f"- 签名：`{case.get('signature') or triage_out.get('signature') or '无'}`",
        f"- 根因：**{triage_out.get('rca') or '未分诊'}**（置信度 {triage_out.get('confidence', '—')}，"
        f"组件 {triage_out.get('component') or '—'}，OS 阶段 {triage_out.get('os_stage') or '—'}）",
        f"- 分流：**{path}**（闸门 1 {verdicts.get('gate1') or '未执行'}，"
        f"分诊预判 {triage_out.get('path') or '—'}）",
        f"- 修复：{('已提交改动 ' + str(fix.get('target'))) if fix.get('fixed') else '未走修复路径或修复未成形'}"
        f"（闸门 2 {verdicts.get('gate2') or '未执行'}）",
        f"- 复现取证：{evidence_mode.get('reproducible') or '本计划未做取证'}"
        f"（{evidence_mode.get('attempts') or 0} 次）；验证：fix_verified={verify.get('fix_verified')}",
        f"- 回归：is_regression={check_out.get('is_regression')}"
        f"，guilty patch={patch.get('commit') or '未定位'}",
        f"- 工单：{ticket.get('ticket_status') or '未成单'} "
        f"{ticket.get('external_key') or ticket.get('duplicate_of') or ''}"
        f"（{ticket.get('priority') or '—'} / {ticket.get('assignee_team') or '—'}，"
        f"闸门 3 {verdicts.get('gate3') or '未执行'}）",
        f"- 计划来源：{case.get('plan_source') or '—'} / {case.get('workflow_key') or '—'}",
        "",
    ]

    md += ["## 阶段执行", "| 阶段 | 状态 | 用时 | 尝试 |", "| --- | --- | --- | --- |"]
    for stage in STAGE_ORDER:
        rows = [r for r in stage_rows if r.get("stage") == stage]
        if not rows:
            continue
        attempt = max(int(r.get("attempt") or 1) for r in rows)
        modes = ", ".join(sorted({str(r.get("mode") or "—") for r in rows}))
        md.append(
            f"| {STAGE_LABEL[stage]}（{modes}） | {stage_status.get(stage)} | "
            f"{sum(_ms(r) for r in rows)} ms | {attempt} |"
        )
    md.append(f"\n端到端用时（各阶段累计）：{tat_ms} ms\n")

    md += ["## 证据链"]
    if evidence:
        for row in evidence[:20]:
            excerpt = str(row.get("excerpt") or "").replace("\n", " ⏎ ")[:150]
            md.append(
                f"- `{row.get('kind')}` → {row.get('uri') or 'db://evidence'}（{row.get('stage')}）："
                f"{excerpt}"
            )
    else:
        md.append("- 无证据记录")
    md.append("")

    unresolved: list[str] = [
        f"Case 终态 {case.get('status')}："
        f"{'需要人工收尾' if case.get('status') in OPEN_STATUSES else '已闭环'}"
    ]
    if verify and verify.get("fix_verified") is False:
        unresolved.append("验证未通过：修复无效，闸门 2 已打回或需人工介入")
    if check_out.get("is_regression") and not patch.get("commit"):
        unresolved.append("判定为回归但二分未收敛：需人工继续定位 guilty patch")
    if ticket.get("ticket_status") == "linked_duplicate":
        unresolved.append(f"已关联重复工单 {ticket.get('duplicate_of')}，未另建单")
    for gate, verdict in verdicts.items():
        if verdict in ("needs_info", "rework", "reject"):
            requested = list((gates.get(gate) or {}).get("requested_info") or [])[:2]
            unresolved.append(
                f"{GATE_LABEL[gate]} 结论 {verdict}：{'；'.join(requested) or '见复核记录'}"
            )
    md += ["## 未解决项"] + [f"- {u}" for u in unresolved]
    notes = list(triage_out.get("triage_notes") or [])[:4]
    if notes:
        md += ["", "## 分诊要点"] + [f"- {n}" for n in notes]

    metrics = _metrics_jsonable(
        {
            "case_id": case_id,
            "status": case.get("status"),
            "signal": case.get("signal"),
            "job": case.get("job"),
            "build": case.get("build"),
            "signature": case.get("signature"),
            "plan_source": case.get("plan_source"),
            "workflow_key": case.get("workflow_key"),
            "path": path,
            "rca": triage_out.get("rca"),
            "confidence": triage_out.get("confidence"),
            "verdicts": verdicts,
            "fix_verified": verify.get("fix_verified"),
            "reproducible": evidence_mode.get("reproducible"),
            "is_regression": check_out.get("is_regression"),
            "guilty_patch": patch.get("commit"),
            "ticket_status": ticket.get("ticket_status"),
            "ticket_key": ticket.get("external_key"),
            "ticket_priority": ticket.get("priority"),
            "stages": [
                {
                    "stage": r.get("stage"),
                    "mode": r.get("mode"),
                    "status": r.get("status"),
                    "ms": _ms(r),
                    "attempt": int(r.get("attempt") or 1),
                }
                for r in stage_rows
            ],
            "tat_ms": tat_ms,
            "evidence_count": len(evidence),
            "unresolved_count": len(unresolved),
        }
    )
    reports = {
        "path": path,
        "verdicts": verdicts,
        "fix": fix,
        "guilty_patch": patch,
    }
    kb_writes = _kb_writes(case, outputs, reports)
    if kb_writes:
        md += ["", "## 知识库回写"] + [
            f"- [{e['kind']}] {e['component']}：{e['summary'][:160]}" for e in kb_writes
        ]

    report_md = "\n".join(md)[:20000]
    await ctx.evidence(
        "report_md",
        report_md,
        {"case_id": case_id, "mode": "case", "note": f"Case 结案报告（{len(report_md)} 字）"},
    )
    log.info("report_case", extra={"ctx": {"case_id": case_id, "tat_ms": tat_ms}})
    return {
        "case_id": case_id,
        "day": _date_of(case.get("created_at")) or _today(),
        "report_md": report_md,
        "metrics": metrics,
        "kb_writes": kb_writes,
    }


# --------------------------------------------------------------------------- mode=daily
async def _daily_report(ctx: Ctx, day: str) -> dict[str, Any]:
    metrics = dict(await ctx.db.daily_metrics(day) or {})
    cases_all = list(await ctx.db.list_cases(limit=200) or [])
    cases = [c for c in cases_all if _date_of(c.get("created_at")) == day] or cases_all

    rca_dist = dict(metrics.get("rca_distribution") or {})
    rca_table = (
        (
            "| 根因 | 案例数 |\n| --- | --- |"
            + "\n".join(
                f"| {k} | {v} |" for k, v in sorted(rca_dist.items(), key=lambda kv: -kv[1])
            )
        )
        if rca_dist
        else "| 根因 | 案例数 |\n| --- | --- |\n| （当日无分诊记录） | 0 |"
    )

    status_dist = dict(metrics.get("status_distribution") or {})
    status_table = (
        (
            "| 终态 | 数量 |\n| --- | --- |"
            + "\n".join(
                f"| {k} | {v} |" for k, v in sorted(status_dist.items(), key=lambda kv: -kv[1])
            )
        )
        if status_dist
        else "| 终态 | 数量 |\n| --- | --- |\n| （无） | 0 |"
    )

    sig_counts: dict[str, int] = {}
    for c in cases:
        sig = c.get("signature")
        if sig:
            sig_counts[sig] = sig_counts.get(sig, 0) + 1
    top_signatures = sorted(sig_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:8]
    sig_table = (
        (
            "| 签名 | 出现次数 |\n| --- | --- |"
            + "\n".join(f"| `{s}` | {n} |" for s, n in top_signatures)
        )
        if top_signatures
        else "| 签名 | 出现次数 |\n| --- | --- |\n| （无） | 0 |"
    )

    open_cases = [c for c in cases if (c.get("status") or "") in OPEN_STATUSES]
    if open_cases:
        open_md = "\n".join(
            f"- `{c.get('case_id')}` {c.get('job')}#{c.get('build')} "
            f"状态 {c.get('status')}（签名 {c.get('signature') or '—'}）"
            for c in open_cases[:20]
        )
    else:
        open_md = "无未闭环案例。"

    overview = (
        "| 指标 | 数值 |\n| --- | --- |\n"
        f"| 案例总数 | {metrics.get('cases_total', 0)} |\n"
        f"| error 信号 | {metrics.get('errors', 0)} |\n"
        f"| 仅观测（无新失败） | {metrics.get('observed_only', 0)} |\n"
        f"| 被抑制的已知噪声 | {metrics.get('suppressed', 0)} |\n"
        f"| 扫描次数 | {metrics.get('scans', 0)} |\n"
        f"| 平均端到端用时 | {metrics.get('tat_ms_avg', 0)} ms |\n"
        f"| 计划来源 | {json.dumps(metrics.get('plan_source_distribution') or {}, ensure_ascii=False)} |"
    )

    template = ""
    if ctx.fixtures.exists("reporting", "template.md"):
        template = ctx.fixtures.text("reporting", "template.md")
    mapping = {
        "day": day,
        "metrics": overview,
        "rca_table": rca_table,
        "open_cases": open_md,
        "status_table": status_table,
        "top_signatures": sig_table,
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
    }
    if template:
        report_md = _render(template, mapping)
        for key in ("status_table", "top_signatures"):
            if "{{" + key + "}}" not in template:
                report_md += f"\n## {('终态分布' if key == 'status_table' else '高频签名')}\n{mapping[key]}\n"
    else:
        report_md = (
            f"# Staging-CI 日报 {day}\n\n## 总览\n{overview}\n\n## 根因分布\n{rca_table}\n\n"
            f"## 未闭环案例\n{open_md}\n"
        )

    await ctx.evidence(
        "report_md",
        report_md[:20000],
        {"day": day, "mode": "daily", "note": f"日报（{len(report_md)} 字）"},
    )
    log.info("report_daily", extra={"ctx": {"day": day, "cases": len(cases)}})
    return {
        "case_id": None,
        "day": day,
        "report_md": report_md[:20000],
        "metrics": _metrics_jsonable({**metrics, "cases": [c.get("case_id") for c in cases]}),
        "kb_writes": [],
    }


async def handler(ctx: Ctx) -> dict[str, Any]:
    payload = ctx.env.get("payload", {}) or {}
    mode = str(payload.get("mode") or "case")
    src = payload.get("input", {}) or {}
    if mode == "daily":
        day = str(src.get("day") or _today())
        return await _daily_report(ctx, day)
    if mode != "case":
        raise Blocked("schema_invalid", f"reporting 不支持的 mode={mode}")
    case_id = src.get("case_id") or ctx.env.get("case_id")
    if not case_id:
        raise Blocked("schema_invalid", "reporting(case) 输入缺少 case_id")
    return await _case_report(ctx, case_id)
