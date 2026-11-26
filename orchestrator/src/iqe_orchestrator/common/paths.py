"""运行时路径解析：包内 ``common/data/`` 为默认资源目录，环境变量可整体或逐项覆盖。

覆盖优先级（每项独立）：

1. 显式环境变量 ``SCHEMAS_DIR`` / ``FIXTURES_DIR`` / ``WORKFLOWS_DIR`` /
   ``PLANS_DIR`` / ``DATA_DIR``
2. 包内资源：``<this package>/common/data``（pip 安装、editable、镜像里都成立）
3. 兼容仓库根布局：``<cwd>/data``

可写目录（计划封存区）不放在包内：默认 ``<cwd>/.plans``，容器里由 ``PLANS_DIR``
指向 ``/var/lib/ci-triage/plans``。
"""

from __future__ import annotations

import os
from pathlib import Path

PKG_DIR = Path(__file__).resolve().parent
# src/<package>/common/paths.py → src/<package>/common
COMMON_DIR = PKG_DIR


def _env_dir(name: str) -> Path | None:
    raw = os.environ.get(name, "").strip()
    return Path(raw).expanduser().resolve() if raw else None


def _data_root() -> Path:
    for candidate in (_env_dir("DATA_DIR"), COMMON_DIR / "data", Path.cwd() / "data"):
        if candidate is not None and candidate.is_dir():
            return candidate
    return COMMON_DIR / "data"


def data_dir() -> Path:
    return _data_root()


def resource_dir(env_name: str, subdir: str) -> Path:
    return _env_dir(env_name) or (_data_root() / subdir)


def plans_dir() -> Path:
    """可写的封存计划目录（绝不落在只读的包目录里）。"""
    explicit = _env_dir("PLANS_DIR") or _env_dir("STATE_DIR")
    if explicit is not None:
        return explicit
    return Path.cwd() / ".plans"


def migrations_dir() -> Path:
    """orchestrator 的 SQL 迁移目录（agents 侧没有 migration，调用即报错）。"""
    candidate = COMMON_DIR.parent / "migrations"
    if candidate.is_dir():
        return candidate
    raise FileNotFoundError(f"找不到 migrations 目录：{candidate}")
