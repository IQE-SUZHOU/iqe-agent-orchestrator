"""共享层（common）的离线单测：幂等键、subject、信封 schema。"""

from __future__ import annotations

import pytest

from iqe_orchestrator.common import envelope as env

pytestmark = pytest.mark.unit


def test_idempotency_key_shape():
    cmd = env.make_command(
        stage="triage",
        action="full",
        payload={"mode": "full"},
        run_id="run-1",
        step_id="s1",
        attempt=1,
        case_id="case-1",
    )
    assert cmd["idempotency_key"] == "case-1:s1:1"
    assert env.subject_of(cmd) == "ci.cmd.triage.full"


def test_scan_command_uses_run_id_when_no_case():
    cmd = env.make_command(
        stage="monitor",
        action="scan",
        payload={"mode": "run"},
        run_id="run-2",
        step_id="scan",
        attempt=1,
        case_id=None,
    )
    assert cmd["idempotency_key"] == "run-2:scan:1"
    assert env.subject_of(cmd) == "ci.cmd.monitor.run"


def test_command_must_pass_schema():
    # 缺字段的 ValidationError 被包成 ValueError（调用方据此决定 nack）
    with pytest.raises(ValueError, match="required"):
        env.validate({"kind": "command"}, "envelope.schema.json")
