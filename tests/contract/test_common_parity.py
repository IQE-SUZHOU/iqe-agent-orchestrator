"""契约测试：两个子项目各自携带的 common 层必须字节一致。

共享层是「一份源码、两处副本」（orchestrator 为真源，agents 由 scripts/sync_common.sh 同步）。
任何只改一边的提交都会在这里被拦下。
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "orchestrator/src/iqe_orchestrator/common"
DST = ROOT / "agents/src/iqe_agents/common"


def _digest(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for f in sorted(root.rglob("*")):
        if not f.is_file() or "__pycache__" in f.parts or f.suffix == ".pyc":
            continue
        out[f.relative_to(root).as_posix()] = hashlib.sha256(f.read_bytes()).hexdigest()
    return out


@pytest.mark.contract
def test_common_copies_are_byte_identical():
    a, b = _digest(SRC), _digest(DST)
    assert a, "orchestrator 的 common 层不应为空"
    missing = sorted(set(a) - set(b))
    extra = sorted(set(b) - set(a))
    assert not missing and not extra, f"文件集合不一致 missing={missing} extra={extra}"
    changed = sorted(k for k in a if a[k] != b[k])
    assert not changed, f"以下共享文件内容不一致：{changed}（先跑 make sync-common）"


@pytest.mark.contract
def test_shared_schemas_are_present_in_both():
    for rel in (
        "data/schemas/envelope.schema.json",
        "data/schemas/stage-contracts.json",
        "data/schemas/plan.schema.json",
        "data/workflows/s1_lite.yaml",
        "data/workflows/s2_full.yaml",
        "data/fixtures/plans/warming_ok.yaml",
    ):
        assert (SRC / rel).is_file(), rel
        assert (DST / rel).is_file(), rel
