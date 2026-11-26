"""Agent 运行时与契约单测：stage 契约加载、mode 校验、fixtures 可读。"""

from __future__ import annotations

import json

import jsonschema
import pytest

import pytest

from iqe_agents.common.config import cfg
from iqe_agents.common.mock import Fixtures
from iqe_agents.runtime import validate_stage_contract
pytestmark = pytest.mark.unit



def test_stage_contract_is_loaded_from_common_data():
    schema = validate_stage_contract("triage", "output")
    assert schema["type"] == "object"
    assert "rca" in schema.get("properties", {})


@pytest.mark.parametrize("stage", ["monitor", "triage", "reproduction", "ticket", "review", "reporting"])
def test_every_stage_contract_compiles(stage):
    for which in ("input", "output"):
        jsonschema.Draft202012Validator.check_schema(validate_stage_contract(stage, which))


def test_fixtures_are_readable():
    fx = Fixtures(cfg.fixtures_dir)
    assert fx.json("ci_feed", "default.json")


def test_all_ci_feed_fixtures_are_valid_json():
    root = __import__("pathlib").Path(cfg.fixtures_dir) / "ci_feed"
    files = sorted(root.glob("*.json"))
    assert len(files) >= 19
    for f in files:
        json.loads(f.read_text(encoding="utf-8"))


def test_mode_contract_rejects_unknown_mode():
    schema = validate_stage_contract("triage", "input")
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(instance={"mode": "nope"}, schema=schema)
