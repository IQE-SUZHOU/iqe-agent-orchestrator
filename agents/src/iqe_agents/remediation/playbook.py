"""Remediation 剧本（阶段 4a）：按知识库的修复方案生成改动 → 提 ADO 工作项 → 交 5a 验证。

替换点（真实实现）：git 工作区改动 + ADO REST（POST workitem）+ 变更审批流。
Mock 只产出 diff 摘要、目标文件、工作项键与风险等级，并由编排层落 remediations 表。

剧本规则：
1. rca=driver（产品代码缺陷）不在自动化修复范围 → fixed=false，转外部路径由人跟进；
2. 修复形态由组件决定（change_type）：ci-infra/apt-repo/install → script；
   toolchain/network → config；test → test；其余 → data；
3. 优先采纳知识库 fix 条目（common.kb.fixes_for）：命中则 adopted_from_kb=条目 id；
4. 闸门 2 打回（requested_focus 非空）时重做修复：notes 里记录本轮要求与重做说明。
"""

from __future__ import annotations

import json
import logging
from typing import Any

from iqe_agents.common.kb import KnowledgeBase
from iqe_agents.runtime import Blocked, Ctx

log = logging.getLogger("agent.remediation.playbook")

CHANGE_TYPE_BY_COMPONENT = {
    "ci-infra": "script",
    "apt-repo": "script",
    "install": "script",
    "toolchain": "config",
    "network": "config",
    "test": "test",
    "image-build": "config",
    "driver": "data",
    "unknown": "data",
}
TARGET_BY_COMPONENT = {
    "ci-infra": "ci/scripts/staging_install.sh",
    "apt-repo": "ci/scripts/apt_setup.sh",
    "install": "ci/scripts/os_staging_test.sh",
    "toolchain": "ci/config/toolchain.yaml",
    "network": "ci/config/jenkins-agent.yaml",
    "test": "tests/kernel-mode/queue_reinit.cfg",
    "image-build": "ci/config/image_build.yaml",
    "driver": "kernel/drivers/amdgpu",
    "unknown": "ci/pipeline/unknown",
}
DEFAULT_SPEC: dict[str, Any] = {
    "fixed": False,
    "change_type": "script",
    "target": "",
    "diff_summary": "未找到可复用的修复方案",
    "work_item_key": None,
    "risk": "high",
    "notes": [],
}


def _scenario_names(scenario: str) -> list[str]:
    out: list[str] = []
    for name in (scenario, str(scenario).replace("-", "_"), str(scenario).replace("_", "-")):
        if name and name not in out:
            out.append(name)
    return out


def _pick_json(ctx: Ctx) -> dict[str, Any]:
    for name in _scenario_names(ctx.scenario or ""):
        if ctx.fixtures.exists("remediation", f"{name}.json"):
            return ctx.fixtures.json("remediation", f"{name}.json")
    return ctx.fixtures.pick_json("remediation", "default", fallback="default")[1]


async def handler(ctx: Ctx) -> dict[str, Any]:
    payload = ctx.env.get("payload", {}) or {}
    src = payload.get("input", {}) or {}
    case_id = src.get("case_id") or ctx.env.get("case_id")
    job = str(src.get("job") or "unknown-job")
    build = src.get("build") if isinstance(src.get("build"), int) else 0
    rca = str(src.get("rca") or "unknown")
    component = str(src.get("component") or "unknown")
    signature = src.get("signature")
    requested_focus = str(src.get("requested_focus") or "")
    if not case_id:
        raise Blocked("schema_invalid", "remediation 输入缺少 case_id")

    spec = {**DEFAULT_SPEC, **(_pick_json(ctx) or {})}
    kb = KnowledgeBase(ctx.fixtures)
    kb_fixes = kb.fixes_for(component, signature)
    notes: list[str] = [str(n) for n in (spec.get("notes") or [])]

    if rca == "driver":
        # 产品代码缺陷：自动化修复越界，交回外部路径
        out = {
            "case_id": case_id,
            "fixed": False,
            "change_type": "data",
            "target": TARGET_BY_COMPONENT["driver"],
            "diff_summary": "驱动代码缺陷不在自动化修复范围：交产品组按 guilty patch 分支修复",
            "work_item_key": None,
            "risk": "high",
            "adopted_from_kb": None,
            "notes": [*notes, "rca=driver：4a 只做转派，不改产品代码"],
        }
        await ctx.evidence(
            "remediation_plan",
            json.dumps(out, ensure_ascii=False, indent=2),
            {"case_id": case_id, "note": "4a 判定不可自动修复（产品代码缺陷）"},
        )
        return {**out, "evidence": list(ctx.refs)}

    fixed = bool(spec.get("fixed"))
    change_type = str(spec.get("change_type") or CHANGE_TYPE_BY_COMPONENT.get(component, "script"))
    target = str(spec.get("target") or TARGET_BY_COMPONENT.get(component, "ci/pipeline/unknown"))
    diff_summary = str(spec.get("diff_summary") or DEFAULT_SPEC["diff_summary"])
    risk = str(spec.get("risk") or ("low" if fixed else "high"))
    adopted = str(kb_fixes[0]["id"]) if (fixed and kb_fixes) else None
    work_item_key = spec.get("work_item_key") if fixed else None

    if fixed and kb_fixes:
        notes.append(
            f"采纳知识库修复方案 {kb_fixes[0]['id']}（{kb_fixes[0].get('summary')}），"
            f"改动落到 {target}"
        )
    if not fixed:
        notes.append("没有可安全复用的修复：保留证据并交人工确认预期行为")
    if requested_focus:
        notes.append(f"闸门 2 打回后重做：{requested_focus[:180]}")

    out = {
        "case_id": case_id,
        "fixed": fixed,
        "change_type": change_type,
        "target": target,
        "diff_summary": diff_summary,
        "work_item_key": work_item_key,
        "work_item_url": (
            f"http://ado.mock/_workitems/edit/{work_item_key.split('-')[-1]}"
            if work_item_key
            else None
        ),
        "risk": risk,
        "adopted_from_kb": adopted,
        "notes": notes[:10],
    }
    await ctx.evidence(
        "remediation_plan",
        "\n".join(
            [
                f"case={case_id} job={job}#{build} rca={rca} component={component}",
                f"fixed={fixed} change_type={change_type} risk={risk}",
                f"target={target}",
                f"diff: {diff_summary}",
                f"work_item={work_item_key or '未创建'}",
                f"adopted_from_kb={adopted or '无'}",
                *[f"- {n}" for n in notes],
            ]
        ),
        {
            "case_id": case_id,
            "target": target,
            "fixed": fixed,
            "note": f"4a 修复{'已生成' if fixed else '未成形'}：{target}（{change_type}/{risk}）",
        },
    )
    log.info(
        "remediation_fix",
        extra={"ctx": {"case_id": case_id, "fixed": fixed, "target": target, "risk": risk}},
    )
    return {**out, "evidence": list(ctx.refs)}
