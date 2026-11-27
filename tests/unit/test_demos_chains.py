"""demos/ 的两条分支必须能离线跑通。

改剧本 / 改契约后如果 demo 挂了，这里会先红——避免把「给实现人员看的参考样例」
留成过期文档。
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


@pytest.mark.unit
def test_demos_two_branches() -> None:
    proc = subprocess.run(
        [sys.executable, "demos/harness.py", "--all"],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "分支 internal" in proc.stdout
    assert "分支 external" in proc.stdout
    # 12 个阶段目录都要出现在输出里（两条分支合起来正好覆盖 12 个）
    for folder in (
        "01-monitor",
        "02-triage",
        "03-gate1",
        "04a-remediation",
        "05a-verify",
        "06a-gate2",
        "04b-evidence",
        "05b-regression-check",
        "06c-bisect",
        "07b-ticketing",
        "08b-gate3",
        "09-reporting",
    ):
        assert folder in proc.stdout, f"{folder} 没跑起来"
