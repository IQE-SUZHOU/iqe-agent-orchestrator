"""Agent 运行时与契约单测：stage 契约加载、mode 归属、fixtures 可读。"""

from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest

from iqe_agents.common.config import cfg
from iqe_agents.common.contracts import MODE_AUTHORITY, STAGES, is_valid_mode
from iqe_agents.common.mock import Fixtures
from iqe_agents.runtime import validate_stage_contract

pytestmark = pytest.mark.unit

#: 8 个部署单元（monitor 只由 Scheduler 触发，但契约同样要能编译）。
ALL_STAGES = list(STAGES)


def test_stage_contract_is_loaded_from_common_data():
    schema = validate_stage_contract("triage", "output")
    assert schema["type"] == "object"
    assert "rca" in schema.get("properties", {})


@pytest.mark.parametrize("stage", ALL_STAGES)
def test_every_stage_contract_compiles(stage):
    for which in ("input", "output"):
        jsonschema.Draft202012Validator.check_schema(validate_stage_contract(stage, which))


@pytest.mark.parametrize("stage", ALL_STAGES)
def test_stage_declares_its_modes(stage):
    """8 个 Agent × mode：每个 stage 至少一个 mode，且都能在 x-mode-authority 里查到。"""
    assert MODE_AUTHORITY[stage], f"{stage} 没有声明 mode"
    schema = validate_stage_contract(stage, "input")
    modes = schema["properties"]["mode"]
    if "const" in modes:
        assert MODE_AUTHORITY[stage] == (modes["const"],)
    else:
        assert set(modes["enum"]) == set(MODE_AUTHORITY[stage])


def test_mode_authority_is_consistent_with_is_valid_mode():
    for stage, modes in MODE_AUTHORITY.items():
        for mode in modes:
            assert is_valid_mode(stage, mode)
        assert not is_valid_mode(stage, "nope")


def test_fixtures_are_readable():
    fx = Fixtures(cfg.fixtures_dir)
    assert fx.json("ci_feed", "default.json")


def test_all_ci_feed_fixtures_are_valid_json():
    root = Path(cfg.fixtures_dir) / "ci_feed"
    files = sorted(root.glob("*.json"))
    assert len(files) >= 19
    for f in files:
        json.loads(f.read_text(encoding="utf-8"))


def test_mode_contract_rejects_unknown_mode():
    schema = validate_stage_contract("triage", "input")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"mode": "nope"}, schema=schema)


@pytest.mark.parametrize(
    ("stage", "mode"),
    [("reviewer", "gate2"), ("reproduction", "evidence"), ("regression", "bisect")],
)
def test_multi_mode_stage_rejects_a_mode_from_another_stage(stage, mode):
    """reviewer/reproduction/regression 是多 mode 单元：不属于自己的 mode 必须被拒。"""
    schema = validate_stage_contract(stage, "input")
    foreign = {"reviewer": "fix", "reproduction": "check", "regression": "verify"}[stage]
    assert is_valid_mode(stage, mode)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"mode": foreign, "input": {}}, schema=schema)
