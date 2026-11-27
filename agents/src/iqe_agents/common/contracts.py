"""Stage 契约读取：``data/schemas/stage-contracts.json`` 是 mode 与输入/输出的唯一权威。

编排层（校验出参落库）与 Agent（校验入参/出参）共用同一份实现，
因此本文件属于共享层：改完必须 ``make sync-common`` 同步到 agents。
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from .config import cfg

#: 8 个部署单元（一个镜像 + 一个 command），与 bus.AGENT_QUEUES 一一对应。
STAGES: tuple[str, ...] = (
    "monitor",
    "triage",
    "reviewer",
    "remediation",
    "reproduction",
    "regression",
    "ticketing",
    "reporting",
)

#: 每个 stage 允许的 mode（与 x-mode-authority 保持一致，便于不做 IO 的快速校验）。
MODE_AUTHORITY: dict[str, tuple[str, ...]] = {
    "monitor": ("scan",),
    "triage": ("classify",),
    "reviewer": ("gate1", "gate2", "gate3"),
    "remediation": ("fix",),
    "reproduction": ("evidence", "verify"),
    "regression": ("check", "bisect"),
    "ticketing": ("create",),
    "reporting": ("case", "daily"),
}


def _contracts_path() -> Path:
    return Path(cfg.schemas_dir) / "stage-contracts.json"


@lru_cache(maxsize=8)
def _load(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def contracts_doc() -> dict:
    """整份契约文档（含 ``x-path-matrix``，供 UI/文档对账）。"""
    return _load(str(_contracts_path()))


def stage_modes(stage: str) -> tuple[str, ...]:
    doc = contracts_doc()["x-mode-authority"]
    return tuple(doc[stage])


def is_valid_mode(stage: str, mode: str | None) -> bool:
    return mode in (MODE_AUTHORITY.get(stage) or stage_modes(stage))


def validate_stage_contract(stage: str, which: str) -> dict:
    """取出某 stage 的 input/output JSON Schema（``which`` ∈ {input, output}）。

    顶层 ``$defs`` 会被拼进来，保证 ``$ref: "#/$defs/..."`` 能解析；
    子 schema 里的 ``required`` 会被提升到顶层（JSON Schema 的 required 只对自己那一层生效）。
    """
    doc = contracts_doc()
    stage_doc = doc["stages"][stage]
    schema: dict = {"$defs": doc["$defs"], "type": "object"}
    schema.update(stage_doc[which])
    schema["required"] = stage_doc[which].get("required", [])
    return schema


def path_matrix() -> dict:
    return dict(contracts_doc().get("x-path-matrix") or {})
