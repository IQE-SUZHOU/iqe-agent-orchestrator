"""demos/ 的 12 个阶段目录必须自洽：一篇说明 + 一份输入 + 一份输出。"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


@pytest.mark.unit
def test_demos_every_folder_has_readme_and_samples() -> None:
    dirs = sorted(p for p in (REPO / "demos").iterdir() if p.is_dir())
    assert len(dirs) == 12, f"应当有 12 个阶段目录，实际 {len(dirs)}"
    for d in dirs:
        assert (d / "README.md").is_file(), f"{d.name} 缺 README.md"
        assert (d / "input.json").is_file(), f"{d.name} 缺 input.json"
        assert (d / "output.json").is_file(), f"{d.name} 缺 output.json"
