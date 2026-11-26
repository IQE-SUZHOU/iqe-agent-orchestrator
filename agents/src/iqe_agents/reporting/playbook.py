"""Reporting 剧本：mode=case 出单案例结案报告；mode=daily 出每日台账日报（Mock）。

数据来源全部是 Postgres（cases / stage_runs / evidence / scans / daily_reports），
剧本本身不做推理：
- case：get_case + trace + get_stage_output(各 stage) + evidence(case_id) → Markdown 结案报告；
- daily：daily_metrics(day) + list_cases(limit) + fixtures/reporting/template.md → 日报正文。
输出契约：{case_id, day, report_md, metrics}（正文走 report_md，指标走 metrics）。
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any

from iqe_agents.runtime import Blocked, Ctx

log = logging.getLogger("agent.reporting.playbook")

STAGE_ORDER = ("monitor", "triage", "reproduction", "ticket", "review", "reporting")
STAGE_LABEL = {
    "monitor": "Monitor 扫描",
    "triage": "Triage 分诊",
    "reproduction": "Reproduction 受控重跑",
    "ticket": "Ticket 成单",
    "review": "Review 复核",
    "reporting": "Reporting 报告",
}
OPEN_STATUSES = (
    "created",
    "triaged",
    "repro_running",
    "ticketed",
    "review_running",
    "review_needs_info",
)


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


# --------------------------------------------------------------------------- mode=case
async def _case_report(ctx: Ctx, case_id: str) -> dict[str, Any]:
    case = await ctx.db.get_case(case_id)
    if case is None:
        raise Blocked("schema_invalid", f"cases 表里没有 {case_id}，无法出结案报告")
    trace = list(await ctx.db.trace(case_id) or [])
    evidence = list(await ctx.db.evidence(case_id) or [])
    outputs = {
        stage: await ctx.db.get_stage_output(case_id, stage)
        for stage in ("triage", "reproduction", "ticket", "review")
    }
    triage = outputs.get("triage") or {}
    repro = outputs.get("reproduction") or {}
    ticket = outputs.get("ticket") or {}
    review = outputs.get("review") or {}

    stage_rows = [r for r in trace if r.get("stage") in STAGE_ORDER]
    tat_ms = sum(_ms(r) for r in stage_rows)
    stage_status = {r.get("stage"): r.get("status") for r in stage_rows}

    md: list[str] = [f"# Case 结案报告 {case_id}", ""]
    md += [
        "## 结论摘要",
        f"- 状态：**{case.get('status') or 'unknown'}**（signal={case.get('signal')}，"
        f"job={case.get('job')}#{case.get('build') or '-'}）",
        f"- 签名：`{case.get('signature') or triage.get('signature') or '无'}`",
        f"- 根因：**{triage.get('rca') or '未分诊'}**（置信度 {triage.get('confidence', '—')}，"
        f"组件 {triage.get('component') or '—'}，OS 阶段 {triage.get('os_stage') or '—'}）",
        f"- 复现结论：{repro.get('reproducible') or '本计划未做复现'}"
        + (f"（{repro.get('attempts')} 次）" if repro.get("attempts") else ""),
        f"- 工单：{ticket.get('ticket_status') or '未成单'} "
        f"{ticket.get('external_key') or ticket.get('duplicate_of') or ''}"
        f"（{ticket.get('ticket_priority') or '—'} / {ticket.get('assignee_team') or '—'}）",
        f"- 复核：**{review.get('verdict') or '未复核'}**（{review.get('reason_code') or '—'}）",
        f"- 计划来源：{case.get('plan_source') or '—'} / {case.get('workflow_key') or '—'}",
        "",
    ]

    md += ["## 阶段执行", "| 阶段 | 状态 | 用时 | 尝试 |", "| --- | --- | --- | --- |"]
    for stage in STAGE_ORDER:
        rows = [r for r in stage_rows if r.get("stage") == stage]
        if not rows:
            continue
        attempt = max(int(r.get("attempt") or 1) for r in rows)
        md.append(
            f"| {STAGE_LABEL[stage]} | {stage_status.get(stage)} | "
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

    notes = list(triage.get("triage_notes") or [])[:4]
    unresolved: list[str] = [
        f"Case 终态 {case.get('status')}：{'需要人工收尾' if case.get('status') in OPEN_STATUSES else '已闭环'}"
    ]
    if not repro:
        unresolved.append("本计划未包含受控重跑，复现结论待夜间批次补齐")
    if review.get("verdict") == "needs_info":
        unresolved.extend([f"复核要求补充：{r}" for r in (review.get("requested_info") or [])[:3]])
    if review.get("verdict") == "reject":
        unresolved.append(f"复核拒绝（{review.get('reason_code')}），未生成案例报告段")
    if triage.get("duplicate_of"):
        unresolved.append(f"疑似与 {triage['duplicate_of']} 重复，需人工确认是否合并")
    md += ["## 未解决项"] + [f"- {u}" for u in unresolved]
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
            "rca": triage.get("rca"),
            "confidence": triage.get("confidence"),
            "reproducible": repro.get("reproducible"),
            "repro_attempts": repro.get("attempts"),
            "ticket_status": ticket.get("ticket_status"),
            "ticket_key": ticket.get("external_key"),
            "ticket_priority": ticket.get("ticket_priority"),
            "verdict": review.get("verdict"),
            "reason_code": review.get("reason_code"),
            "quality_overall": (review.get("quality_scores") or {}).get("overall"),
            "stages": [
                {
                    "stage": r.get("stage"),
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
            f"# Staging-CI 日报 {day}\n\n## 概览\n{overview}\n\n## 根因分布\n{rca_table}\n\n"
            f"## 终态分布\n{status_table}\n\n## 高频签名\n{sig_table}\n\n## 未解决项\n{open_md}\n"
        )

    metrics = _metrics_jsonable(
        {
            **metrics,
            "day": day,
            "top_signatures": top_signatures,
            "open_cases": len(open_cases),
            "report_chars": len(report_md),
        }
    )
    slim_cases = [
        {
            "case_id": c.get("case_id"),
            "job": c.get("job"),
            "build": c.get("build"),
            "signal": c.get("signal"),
            "signature": c.get("signature"),
            "status": c.get("status"),
            "plan_source": c.get("plan_source"),
        }
        for c in cases[:50]
    ]
    await ctx.db.save_daily_report(
        day=day, report_md=report_md[:20000], metrics=metrics, cases=slim_cases
    )
    await ctx.evidence(
        "report_md",
        report_md[:6000],
        {
            "mode": "daily",
            "day": day,
            "note": f"日报 {day}（{len(report_md)} 字，案例 {len(cases)} 条）",
        },
    )
    log.info("report_daily", extra={"ctx": {"day": day, "cases": len(cases)}})
    return {"case_id": None, "day": day, "report_md": report_md[:20000], "metrics": metrics}


async def handler(ctx: Ctx) -> dict[str, Any]:
    payload = ctx.env.get("payload", {}) or {}
    mode = payload.get("mode") or "daily"
    src = payload.get("input", {}) or {}
    case_id = src.get("case_id") or ctx.env.get("case_id")
    day = src.get("day") or _today()

    if mode == "case":
        if not case_id:
            raise Blocked("schema_invalid", "reporting:case 缺少 case_id")
        return await _case_report(ctx, str(case_id))
    return await _daily_report(ctx, str(day))
