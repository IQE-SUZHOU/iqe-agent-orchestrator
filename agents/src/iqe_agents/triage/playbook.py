"""Triage 剧本：LogDistiller 蒸馏 → 组件/根因判定 → SKILL 卡片 → 复现资格。

mode=full：蒸馏 + 分类 + SKILL 卡片 + 复现资格 + 历史查重。
mode=classify_only：轻量档（只蒸馏+分类），key_frames 2 条、triage_notes 1 条、
                    repro_eligible 恒为 false（计划里没有 reproduction 段）、llm_calls=0。

Mock 边界：根因与置信度来自 fixtures/triage/rules.json 规则表（剧本），
真实实现里这里是模型判定 + 工具取证（Jarvis device_status / log_fetch）。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from iqe_agents.common.config import cfg
from iqe_agents.common.tools.fingerprint import component_of, signature_family, signature_of
from iqe_agents.common.tools.logdistiller import build_job
from iqe_agents.runtime import Blocked, Ctx

log = logging.getLogger("agent.triage.playbook")

SKILL_DIR = Path(__file__).resolve().parent / "skills"
SKILL_BY_RCA = {
    "driver": "driver-failure-triage",
    "test": "test-failure-triage",
    "environment": "environment-failure-triage",
    "infrastructure": "environment-failure-triage",
    "unknown": "environment-failure-triage",
}
KEY_MARKERS = ("error", "fail", "bug:", "assert", "panic", "oops", "timeout", "404")
DEFAULT_RULES = {
    "component_to_rca": {"unknown": "unknown"},
    "os_stage_map": {},
    "confidence_by_component": {"unknown": 0.30},
    "no_repro_rca": ["infrastructure"],
}


def _rules(ctx: Ctx) -> dict[str, Any]:
    if ctx.fixtures.exists("triage", "rules.json"):
        return ctx.fixtures.json("triage", "rules.json")
    log.warning("triage_rules_missing_using_default")
    return DEFAULT_RULES


def _scenario_names(scenario: str) -> list[str]:
    """剧本名兼容连字符/下划线（编排层 scenario_hint 命名不完全统一）。"""
    out: list[str] = []
    for name in (scenario, str(scenario).replace("-", "_"), str(scenario).replace("_", "-")):
        if name and name not in out:
            out.append(name)
    return out


def _scenario_spec(ctx: Ctx) -> dict[str, Any]:
    if not ctx.fixtures.exists("triage", "scenarios.json"):
        return {}
    doc = ctx.fixtures.json("triage", "scenarios.json") or {}
    for name in _scenario_names(ctx.scenario or ""):
        if doc.get(name):
            return dict(doc[name])
    return {}


def _key_frames(text: str, limit: int) -> list[str]:
    picked = [ln.strip() for ln in text.splitlines() if any(m in ln.lower() for m in KEY_MARKERS)]
    if len(picked) < limit:
        picked += [ln.strip() for ln in text.splitlines() if ln.strip()][:limit]
    return picked[:limit]


def _read_skill(ctx: Ctx, name: str) -> tuple[str, str]:
    path = SKILL_DIR / f"{name}.md"
    if not path.exists():
        log.warning("skill_card_missing", extra={"ctx": {"skill": name, "path": str(path)}})
        return name, ""
    return name, path.read_text(encoding="utf-8")


async def handler(ctx: Ctx) -> dict[str, Any]:
    payload = ctx.env.get("payload", {}) or {}
    mode = payload.get("mode") or "full"
    src = payload.get("input", {}) or {}
    case_id = src.get("case_id") or ctx.env.get("case_id")
    job = str(src.get("job") or "unknown-job")
    build = src.get("build") if isinstance(src.get("build"), int) else 0
    signal = src.get("signal") or "error"
    context = dict(src.get("context") or {})
    requested_focus = src.get("requested_focus") or ""
    if not case_id:
        raise Blocked("schema_invalid", "triage 输入缺少 case_id")

    rules = _rules(ctx)
    spec = _scenario_spec(ctx)

    # 1) 日志：优先用 Monitor 传来的失败段；缺失时退回夹具/内置样本（真实实现是 Jenkins console）
    raw = src.get("log_excerpt") or ""
    if not raw:
        candidates = [f"triage/{name}.log" for name in _scenario_names(ctx.scenario or "")] + [
            "triage/sample_failure.log"
        ]
        for candidate in candidates:
            if ctx.fixtures.exists(*candidate.split("/")):
                raw = ctx.fixtures.text(*candidate.split("/"))
                break
    max_chars = int(getattr(cfg, "distill_chars", 6000) or 6000)
    log_text, dist = build_job(job, build, raw, max_chars=max_chars)

    await ctx.evidence(
        "distilled_log",
        dist.text,
        {
            "fingerprint": dist.fingerprint,
            "original_bytes": dist.original_bytes,
            "distilled_bytes": dist.distilled_bytes,
            "distiller_version": dist.distiller_version,
            "case_id": case_id,
            "note": f"LogDistiller {dist.distiller_version}："
            f"{dist.original_bytes}B → {dist.distilled_bytes}B（保留 {dist.lines_kept} 行）",
        },
    )

    # 2) 组件 → 根因 → 置信度（剧本规则表，非模型推理）
    signature = src.get("signature") or signature_of(log_text)
    family = signature_family(signature)
    component = str(context.get("component") or component_of(dist.text))
    rca = str(rules.get("component_to_rca", {}).get(component, "unknown"))
    confidence = float(rules.get("confidence_by_component", {}).get(rca, 0.30))
    if signal == "none":
        # 构建成功：没有失败簇可归因，按台账 unknown 走轻量分诊（置信度取规则表值）
        component, rca = "none", "unknown"
        confidence = float(rules.get("confidence_by_component", {}).get("unknown", 0.30))
    if spec.get("force_rca"):
        rca = spec["force_rca"]
        confidence = float(spec.get("confidence", confidence))

    os_stage = rules.get("os_stage_map", {}).get(job)
    no_repro = list(rules.get("no_repro_rca", []))
    classify_only = mode == "classify_only"

    # 3) SKILL 卡片（演示阶段只读取并回显）
    skill_name = SKILL_BY_RCA.get(rca, "environment-failure-triage")
    skill_name, skill_body = _read_skill(ctx, skill_name)
    if skill_body:
        await ctx.evidence(
            "skill_card",
            skill_body[:800],
            {"skill": skill_name, "rca": rca, "note": f"SKILL 卡片 {skill_name}（前 800 字）"},
        )

    # 4) 复现资格
    repro_eligible = rca not in no_repro and not classify_only and signal != "none"
    repro_block_reason = None
    if not repro_eligible:
        if classify_only:
            repro_block_reason = "classify_only 计划不做复现评估"
        elif signal == "none":
            repro_block_reason = "signal=none：构建成功，无失败可复现"
        else:
            repro_block_reason = f"{rca} 类失败不占用重跑机位"

    # 5) 历史查重（Mock：签名族命中 known_issues 即视为已有历史条目）
    duplicate_of = None
    if ctx.fixtures.exists("known_issues.json"):
        fams = [
            f.get("family")
            for f in (ctx.fixtures.json("known_issues.json") or {}).get("families", [])
        ]
        if family in fams:
            fp = signature.split(":")[2] if len(signature.split(":")) > 2 else family
            duplicate_of = f"case:{fp[:8]}"

    # 6) 结论要点
    key_frames = _key_frames(dist.text, 2 if classify_only else 5)
    excluded = [c for c in ("driver", "test", "environment", "infrastructure") if c != rca][:2]
    notes: list[str] = []
    if classify_only:
        notes.append(
            f"轻量分诊（classify_only）：签名族 {family}，组件 {component}，"
            f"根因 {rca}，置信度 {confidence:.2f}，不做复现评估与工单级推理。"
        )
    else:
        notes.append(
            f"签名族 {family}（签名 {signature}）：错误簇集中在 {component} 侧，"
            f"根因判定 {rca}，置信度 {confidence:.2f}。"
        )
        notes.append(
            f"排除 {' / '.join(excluded)} 类：蒸馏段内未见对应特征行"
            f"（参考 SKILL 卡片 {skill_name}）。"
        )
        if os_stage:
            notes.append(f"归属 OS 阶段 {os_stage}（job {job}），影响 staging 流水线该环节。")
        notes.append(
            f"复现资格：{'允许受控重跑' if repro_eligible else '不重跑（' + (repro_block_reason or '') + '）'}。"
        )
        if duplicate_of:
            notes.append(
                f"签名族命中已知问题台账，疑似历史复发条目 {duplicate_of}，查重时优先关联。"
            )
        if spec.get("notes"):
            notes.extend(str(n) for n in spec["notes"])
        if requested_focus:
            notes.append(f"按复核要求补充：{requested_focus[:180]}")
    await ctx.evidence(
        "triage_note",
        "\n".join(notes),
        {
            "case_id": case_id,
            "rca": rca,
            "confidence": confidence,
            "note": "分诊结论要点（供复核与工单引用）",
        },
    )

    return {
        "case_id": case_id,
        "rca": rca,
        "component": component,
        "os_stage": os_stage,
        "signature": signature,
        "duplicate_of": duplicate_of,
        "confidence": round(confidence, 2),
        "key_frames": key_frames,
        "repro_eligible": repro_eligible,
        "repro_block_reason": repro_block_reason,
        "triage_notes": notes[:10],
        "evidence": list(ctx.refs),
        "skill_refs": [skill_name] if skill_name else [],
        "llm_calls": 0,
    }
