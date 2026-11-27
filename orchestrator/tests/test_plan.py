"""计划层单测：注册表匹配、校验器 V0–V9、LLM 兜底与 YAML 往返（全部离线）。

口径对齐 12 阶段 / 两条分支 / 三道闸门：
- 闸门 1（reviewer:gate1）必须同时给出 internal / external 路由；
- 内部路径 remediation:fix → reproduction:verify → 闸门 2；外部路径
  reproduction:evidence → regression:check（回归则 bisect）→ ticketing:create → 闸门 3；
- reporting:case 永远是最后一步；回炉只能用 jump 且只能指向更早的 step。
"""

from __future__ import annotations

import yaml

from iqe_orchestrator.common.config import cfg
import pytest

from iqe_orchestrator.plan import (
    LLMPlanner,
    PlanResolver,
    PlanStore,
    builtin_key,
    builtin_plan,
    plan_from_dict,
    validate_plan,
)

pytestmark = pytest.mark.unit

BUILTIN_KEYS = {"s1_observe_only", "s2_full", "s3_test_no_repro"}


def _resolver() -> PlanResolver:
    r = PlanResolver(cfg.workflows_dir)
    assert r.load_registry() >= len(BUILTIN_KEYS), "三份内置 workflow 应被加载"
    return r


def _steps(plan) -> list[tuple[str, str]]:
    return [(s.stage, s.mode) for s in plan.steps]


# --------------------------------------------------------------------- 注册表匹配
def test_registry_has_builtin_workflows():
    r = _resolver()
    assert BUILTIN_KEYS <= set(r.workflows)


def test_error_signal_matches_full_workflow():
    plan = _resolver().plan_for(signal="error", job="nightly/driver", signature="driver:crash")
    assert plan is not None
    assert plan.source == "workflow" and plan.workflow_key == "s2_full"
    steps = _steps(plan)
    # 两道分支与三道闸门都在同一张计划里
    for step in (
        ("reviewer", "gate1"),
        ("remediation", "fix"),
        ("reproduction", "verify"),
        ("reviewer", "gate2"),
        ("reproduction", "evidence"),
        ("regression", "check"),
        ("regression", "bisect"),
        ("ticketing", "create"),
        ("reviewer", "gate3"),
    ):
        assert step in steps, f"完整计划缺少 {step}"
    assert steps[-1] == ("reporting", "case")
    assert validate_plan(plan.to_dict()) == []


def test_test_signature_matches_no_repro_workflow():
    plan = _resolver().plan_for(signal="error", job="nightly/os-test", signature="test:assert-fail")
    assert plan is not None
    assert plan.workflow_key == "s3_test_no_repro"
    steps = _steps(plan)
    # 测试类失败不占重跑机位：外部路径直接进 5b
    assert ("reproduction", "evidence") not in steps
    assert ("regression", "check") in steps
    assert validate_plan(plan.to_dict()) == []


def test_none_signal_matches_observe_only():
    plan = _resolver().plan_for(signal="none", job="nightly/bench", signature=None)
    assert plan is not None
    assert plan.workflow_key == "s1_observe_only"
    assert _steps(plan) == [("triage", "classify"), ("reporting", "case")]
    assert validate_plan(plan.to_dict()) == []


def test_undefined_workflow_falls_through_to_none():
    # llm-plan-demo 被 s2_full 的 job_not_pattern 排除 → 交给 LLMPlanner 兜底
    plan = _resolver().plan_for(signal="error", job="llm-plan-demo/warming", signature=None)
    assert plan is None


# --------------------------------------------------------------------- 校验器闸门
def test_validator_rejects_monitor_stage():
    raw = builtin_plan("s2_full", "error").to_dict()
    raw["steps"] = [{"id": "m1", "stage": "monitor", "mode": "scan"}] + raw["steps"]
    violations = validate_plan(raw)
    # schema 的 stage 枚举先拦（V0）；语义层 V2 同样禁止 monitor 进计划
    assert violations and any("monitor" in v for v in violations), violations


def test_validator_rejects_unknown_mode():
    raw = builtin_plan("s2_full", "error").to_dict()
    raw["steps"][2]["mode"] = "full"  # remediation 只接受 fix
    violations = validate_plan(raw)
    assert violations and any("full" in v for v in violations), violations


def test_validator_rejects_step_budget_overflow():
    raw = builtin_plan("s2_full", "error").to_dict()
    assert any(v.startswith("V1") for v in validate_plan(raw, max_steps=3))


def test_validator_rejects_reporting_before_review():
    raw = builtin_plan("s2_full", "error").to_dict()
    last = raw["steps"].pop()
    raw["steps"].insert(0, last)
    assert any(v.startswith("V4") for v in validate_plan(raw))


def test_validator_requires_gate1_both_branches():
    raw = builtin_plan("s2_full", "error").to_dict()
    gate1 = next(s for s in raw["steps"] if s.get("mode") == "gate1")
    gate1["on"].pop("external")
    assert any(v.startswith("V9") for v in validate_plan(raw))


def test_validator_rejects_forward_jump():
    raw = builtin_plan("s2_full", "error").to_dict()
    gate1 = next(s for s in raw["steps"] if s.get("mode") == "gate1")
    gate1["on"]["needs_info"]["jump"] = "p1"  # 回炉只能指向更早的 step
    assert any(v.startswith("V5") for v in validate_plan(raw))


def test_validator_rejects_duplicate_step_id():
    raw = builtin_plan("s2_full", "error").to_dict()
    raw["steps"].append({"id": "t1", "stage": "triage", "mode": "classify"})
    assert any(v.startswith("V7") for v in validate_plan(raw))


def test_validator_rejects_loop_over_limit():
    raw = builtin_plan("s2_full", "error").to_dict()
    gate2 = next(s for s in raw["steps"] if s.get("mode") == "gate2")
    gate2["on"]["rework"]["max_loops"] = cfg.max_review_loops + 1
    assert any(v.startswith("V6") for v in validate_plan(raw))


def test_validator_rejects_log_injection():
    raw = builtin_plan("s2_full", "error").to_dict()
    raw["rationale"] = "see ci.cmd.triage.classify and http://evil.example/x"
    assert any(v.startswith("V8") for v in validate_plan(raw))


# --------------------------------------------------------------------- 内置安全网
def test_builtin_plan_is_valid_and_marked_degraded():
    assert builtin_key("error") == "s2_full"
    assert builtin_key("none") == "s1_observe_only"
    plan = builtin_plan("s3_test_no_repro", "error")
    assert plan.degraded is True and plan.workflow_key == "s3_test_no_repro"
    assert validate_plan(plan.to_dict()) == []


def test_yaml_roundtrip_is_stable():
    plan = builtin_plan("s2_full", "error")
    text = plan.yaml_text()
    again = plan_from_dict(yaml.safe_load(text))
    assert again.to_dict()["steps"] == plan.to_dict()["steps"]
    assert validate_plan(yaml.safe_load(text)) == []


def test_plan_store_seal_and_read(tmp_path):
    plan = builtin_plan("s2_full", "error")
    store = PlanStore(str(tmp_path))
    sealed = store.seal(plan, "C-plan-1")
    assert sealed.sha256 and sealed.file_path
    assert store.read("C-plan-1", plan.plan_id) == plan.yaml_text()


# --------------------------------------------------------------------- LLM 兜底
def test_llm_planner_accepts_valid_fixtures():
    planner = LLMPlanner(cfg.fixtures_dir)
    for hint in ("internal_ok", "external_ok"):
        plan, violations = planner.build(
            signal="error", job="llm-plan-demo/x", signature="driver:crash", hint=hint
        )
        assert plan is not None and not violations, f"{hint}: {violations}"
        assert plan.source == "llm" and plan.model
        assert validate_plan(plan.to_dict()) == []


def test_llm_planner_rejects_invalid_fixtures():
    planner = LLMPlanner(cfg.fixtures_dir)
    for hint in ("invalid_stage", "invalid_order"):
        plan, violations = planner.build(
            signal="error", job="llm-plan-demo/x", signature="driver:crash", hint=hint
        )
        assert plan is None, f"{hint} 不应通过校验"
        assert violations
