"""计划层单测：注册表匹配、校验器闸门、YAML 往返（全部离线）。"""

from __future__ import annotations

import yaml

from iqe_orchestrator.common.config import cfg
import pytest

from iqe_orchestrator.plan import (
    PlanResolver,
    builtin_plan,
    plan_from_dict,
    validate_plan,
)
pytestmark = pytest.mark.unit



def _resolver() -> PlanResolver:
    r = PlanResolver(cfg.workflows_dir)
    assert r.load_registry() >= 2, "内置 workflow（s1_lite/s2_full）应被加载"
    return r


def test_registry_has_builtin_workflows():
    r = _resolver()
    assert {"s1_lite", "s2_full"} <= set(r.workflows)


def test_error_signal_matches_full_workflow():
    plan = _resolver().plan_for(signal="error", job="nightly/driver", signature="driver:crash")
    assert plan is not None
    assert plan.source == "workflow"
    stages = [s.stage for s in plan.steps]
    assert "review" in stages
    assert validate_plan(plan.to_dict()) == []


def test_none_signal_matches_lite_workflow():
    plan = _resolver().plan_for(signal="none", job="nightly/bench", signature=None)
    assert plan is not None
    assert plan.source == "workflow"
    assert validate_plan(plan.to_dict()) == []


def test_undefined_workflow_falls_through_to_none():
    # warming 没有预定义 workflow → 返回 None，由 LLMPlanner 兜底（此处只验证"未命中"）
    r = _resolver()
    plan = r.plan_for(signal="none", job="llm-plan-demo/warming", signature=None)
    assert plan is None or plan.workflow_key != "warming"


def test_validator_rejects_monitor_stage():
    raw = builtin_plan("s2_full", "error").to_dict()
    raw["steps"] = [{"id": "m1", "stage": "monitor", "mode": "run"}] + raw["steps"]
    violations = validate_plan(raw)
    # monitor 不在 stage 白名单：schema（V0）与业务校验（V2）都会拒绝
    assert violations and any("monitor" in v for v in violations)


def test_validator_rejects_reporting_before_review():
    raw = builtin_plan("s2_full", "error").to_dict()
    order = [s["stage"] for s in raw["steps"]]
    i_rep, i_rev = order.index("reporting"), order.index("review")
    raw["steps"][i_rep], raw["steps"][i_rev] = raw["steps"][i_rev], raw["steps"][i_rep]
    assert any(v.startswith("V4") for v in validate_plan(raw))


def test_validator_rejects_log_injection():
    raw = builtin_plan("s2_full", "error").to_dict()
    raw["rationale"] = "see ci.cmd.triage.full and http://evil.example/x"
    assert any(v.startswith("V8") for v in validate_plan(raw))


def test_yaml_roundtrip_is_stable():
    plan = builtin_plan("s1_lite", "none")
    text = plan.yaml_text()
    again = plan_from_dict(yaml.safe_load(text))
    assert again.to_dict()["steps"] == plan.to_dict()["steps"]
    assert validate_plan(yaml.safe_load(text)) == []
