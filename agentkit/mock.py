"""Mock 数据层：夹具读取 + 剧本选择 + 行为注入（延迟 / 失败率）。"""
from __future__ import annotations

import json
import random
import time
from pathlib import Path

import yaml

from .config import cfg
from .envelope import sleep_ms_range


class Fixtures:
    def __init__(self, base: str | None = None):
        self.base = Path(base or cfg.fixtures_dir)

    def path(self, *parts: str) -> Path:
        return self.base.joinpath(*parts)

    def exists(self, *parts: str) -> bool:
        return self.path(*parts).exists()

    def json(self, *parts: str):
        return json.loads(self.path(*parts).read_text(encoding="utf-8"))

    def yaml(self, *parts: str):
        return yaml.safe_load(self.path(*parts).read_text(encoding="utf-8"))

    def text(self, *parts: str) -> str:
        return self.path(*parts).read_text(encoding="utf-8")

    def pick_json(self, subdir: str, scenario: str, fallback: str = "default"):
        """按剧本取 JSON；不存在则回落 default。"""
        for name in (scenario, fallback):
            p = self.path(subdir, f"{name}.json")
            if p.exists():
                return p, json.loads(p.read_text(encoding="utf-8"))
        raise FileNotFoundError(f"no fixture under {subdir} for scenario={scenario}")


def pick_scenario(env: dict, default: str = "default") -> str:
    """剧本优先级：命令 scenario_hint > env MOCK_SCENARIO > default。"""
    hint = env.get("scenario_hint")
    if hint:
        return hint
    if cfg.mock_scenario and cfg.mock_scenario != "default":
        return cfg.mock_scenario
    return default


def jitter() -> float:
    lo, hi = sleep_ms_range(cfg.mock_delay_ms)
    return random.uniform(lo, hi) / 1000.0


def should_fail(rate: float | None = None) -> bool:
    r = cfg.mock_failure_rate if rate is None else rate
    return r > 0 and random.random() < r


class Timer:
    def __init__(self):
        self.t0 = time.monotonic()

    def ms(self) -> int:
        return int((time.monotonic() - self.t0) * 1000)
