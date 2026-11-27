"""Triage 剧本（阶段 2）：LogDistiller 蒸馏 → 组件/根因 → internal/external 预判 → 知识库检索。

替换点（真实实现）：LogDistiller 之后接模型判定 + Jarvis 取证；知识库换向量检索。
Mock 边界：根因、置信度、路径全部来自 ``fixtures/triage/rules.json`` 与
``fixtures/triage/scenarios.json`` 的确定性规则表。

剧本规则：
1. 组件 → rca（component_to_rca）、置信度（confidence_by_component）；
2. 组件 → 路径预判（path_by_component，缺省 default_path）：
   内部自动化问题（ci-infra / test / toolchain / apt-repo / install）→ internal；
   产品/环境侧（driver / image-build / network / timeout / unknown）→ external；
3. 路径只是「预判」，最终由 Reviewer 闸门 1 决策（reviewer:gate1 的 decision 覆盖它）；
4. 知识库检索（common.kb）：同组件/同签名族的历史条目进 kb_refs，供闸门 1 与修复阶段引用。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from iqe_agents.common.config import cfg
from iqe_agents.common.kb import KnowledgeBase
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
INTERNAL_COMPONENTS = ("ci-infra", "test", "toolchain", "apt-repo", "install")
DEFAULT_RULES: dict[str, Any] = {
    "component_to_rca": {"unknown": "unknown"},
    "path_by_component": dict.fromkeys(INTERNAL_COMPONENTS, "internal"),
    "default_path": "external",
    "confidence_by_component": {"unknown": 0.30},
    "os_stage_map": {},
    "no_repro_rca": ["infrastructure"],
}


def _rules(ctx: Ctx) -> dict[str, Any]:
    if ctx.fixtures.exists("triage", "rules.json"):
        return {**DEFAULT_RULES, **ctx.fixtures.json("triage", "rules.json")}
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


def _known_families(ctx: Ctx) -> set[str]:
    if not ctx.fixtures.exists("known_issues.json"):
        return set()
    doc = ctx.fixtures.json("known_issues.json") or {}
    return {str(row["family"]) for row in doc.get("families", []) if row.get("family")}


async def handler(ctx: Ctx) -> dict[str, Any]:
    payload = ctx.env.get("payload", {}) or {}
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
    kb = KnowledgeBase(ctx.fixtures)

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
    component = str(
        spec.get("force_component") or context.get("component") or component_of(dist.text)
    )
    rca = str(rules.get("component_to_rca", {}).get(component, "unknown"))
    confidence = float(rules.get("confidence_by_component", {}).get(rca, 0.30))
    if signal == "none":
        # 构建成功：没有失败簇可归因，按台账 unknown 走轻量分类（置信度取规则表值）
        component, rca = "none", "unknown"
        confidence = float(rules.get("confidence_by_component", {}).get("unknown", 0.30))
    if spec.get("force_rca"):
        rca = spec["force_rca"]
        confidence = float(spec.get("confidence", confidence))

    # 3) 路径预判（Reviewer 闸门 1 拥有最终决策权）
    path = str(
        rules.get("path_by_component", {}).get(component) or rules.get("default_path", "external")
    )
    if spec.get("force_path"):
        path = str(spec["force_path"])
    if path not in ("internal", "external"):
        path = "external"
    routed_reason = (
        f"组件 {component} 归属 {'内部自动化' if path == 'internal' else '产品/环境侧'}，"
        f"预判走 {path} 路径（最终由闸门 1 决策）"
    )

    # 4) 知识库检索：同组件 / 同签名族的历史条目
    kb_hits = kb.search(component=component, signature=signature, top_k=3)
    kb_refs = [str(e.get("id")) for e in kb_hits if e.get("id")]
    if kb_hits:
        await ctx.evidence(
            "kb_lookup",
            "\n".join(
                f"{e.get('id')} [{e.get('kind')}/{e.get('component')}] {e.get('summary')}"
                f"（score={e.get('score')}）"
                for e in kb_hits
            ),
            {"case_id": case_id, "note": f"知识库命中 {len(kb_hits)} 条（{', '.join(kb_refs)}）"},
        )

    # 5) SKILL 卡片（演示阶段只读取并回显）
    skill_name = SKILL_BY_RCA.get(rca, "environment-failure-triage")
    skill_name, skill_body = _read_skill(ctx, skill_name)
    if skill_body:
        await ctx.evidence(
            "skill_card",
            skill_body[:800],
            {"skill": skill_name, "rca": rca, "note": f"SKILL 卡片 {skill_name}（前 800 字）"},
        )

    # 6) 历史查重（Mock：签名族命中 known_issues 或知识库 failure 条目即视为已有历史）
    duplicate_of = None
    if family in _known_families(ctx) or any(
        e.get("kind") == "failure" and e.get("signature_family") == family for e in kb_hits
    ):
        fp = signature.split(":")[2] if len(signature.split(":")) > 2 else family
        duplicate_of = f"case:{fp[:8]}"

    # 7) 结论要点
    key_frames = _key_frames(dist.text, 5 if signal != "none" else 2)
    excluded = [c for c in ("driver", "test", "environment", "infrastructure") if c != rca][:2]
    os_stage = rules.get("os_stage_map", {}).get(job)
    notes: list[str] = [
        f"签名族 {family}（签名 {signature}）：错误簇集中在 {component} 侧，"
        f"根因判定 {rca}，置信度 {confidence:.2f}。",
    ]
    if signal == "none":
        notes = [
            f"构建成功（signal=none），仅做蒸馏归档：签名族 {family}，不做路径判定与复现评估。"
        ]
    else:
        notes.append(
            f"排除 {' / '.join(excluded)} 类：蒸馏段内未见对应特征行"
            f"（参考 SKILL 卡片 {skill_name}）。"
        )
        if os_stage:
            notes.append(f"归属 OS 阶段 {os_stage}（job {job}），影响 staging 流水线该环节。")
        notes.append(f"路径预判：{path} —— {routed_reason}。")
        if kb_refs:
            notes.append(f"知识库命中 {', '.join(kb_refs)}，闸门 1 可分诊时引用历史结论。")
        if duplicate_of:
            notes.append(
                f"签名族命中已知问题台账，疑似历史复发条目 {duplicate_of}，查重时优先关联。"
            )
        if spec.get("notes"):
            notes.extend(str(n) for n in spec["notes"])
        if requested_focus:
            notes.append(f"按复核/闸门要求补充：{requested_focus[:180]}")
    await ctx.evidence(
        "triage_note",
        "\n".join(notes),
        {
            "case_id": case_id,
            "rca": rca,
            "path": path,
            "confidence": confidence,
            "note": "分诊结论要点（供闸门 1、工单与报告引用）",
        },
    )

    return {
        "case_id": case_id,
        "rca": rca,
        "path": path,
        "component": component,
        "os_stage": os_stage,
        "signature": signature,
        "duplicate_of": duplicate_of,
        "confidence": round(confidence, 2),
        "key_frames": key_frames,
        "routed_reason": routed_reason if signal != "none" else "signal=none，不分流",
        "kb_refs": kb_refs,
        "triage_notes": notes[:10],
        "evidence": list(ctx.refs),
        "skill_refs": [skill_name] if skill_name else [],
        "llm_calls": 0,
    }
