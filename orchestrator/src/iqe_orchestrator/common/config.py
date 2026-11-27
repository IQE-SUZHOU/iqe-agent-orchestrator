"""全局配置：全部来自环境变量，便于 7 个部署单元用同一镜像不同 env 运行。

路径类默认值走 :mod:`common.paths`（包内 ``common/data/``），不再依赖“源码目录就是仓库根”。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from . import paths


def _s(name: str, default: str) -> str:
    v = os.environ.get(name)
    return default if v is None or v == "" else v


def _i(name: str, default: int) -> int:
    try:
        return int(_s(name, str(default)))
    except ValueError:
        return default


def _f(name: str, default: float) -> float:
    try:
        return float(_s(name, str(default)))
    except ValueError:
        return default


def _b(name: str, default: bool) -> bool:
    return _s(name, "true" if default else "false").strip().lower() in ("1", "true", "yes", "on")


@dataclass
class Config:
    """演示用配置（设计文档 design/05 §4 的旋钮）。"""

    svc: str = field(default_factory=lambda: _s("SVC", "unknown"))
    app_env: str = field(default_factory=lambda: _s("APP_ENV", "demo"))

    # --- 基础设施（复用集群现有服务）---
    amqp_url: str = field(
        default_factory=lambda: _s("AMQP_URL", "amqp://guest:guest@127.0.0.1:5672/ci")
    )
    pg_dsn: str = field(
        default_factory=lambda: _s("PG_DSN", "postgresql://postgres@127.0.0.1:5432/ci_triage")
    )

    # --- 路径（默认在包内 common/data/，可用 DATA_DIR 或逐项 env 覆盖）---
    schemas_dir: str = field(
        default_factory=lambda: str(paths.resource_dir("SCHEMAS_DIR", "schemas"))
    )
    fixtures_dir: str = field(
        default_factory=lambda: str(paths.resource_dir("FIXTURES_DIR", "fixtures"))
    )
    workflows_dir: str = field(
        default_factory=lambda: str(paths.resource_dir("WORKFLOWS_DIR", "workflows"))
    )
    plans_dir: str = field(default_factory=lambda: str(paths.plans_dir()))

    # --- 编排上限 ---
    stage_timeout_s: float = field(default_factory=lambda: _f("STAGE_TIMEOUT_S", 30.0))
    stage_max_attempts: int = field(default_factory=lambda: _i("STAGE_MAX_ATTEMPTS", 2))
    max_review_loops: int = field(default_factory=lambda: _i("MAX_REVIEW_LOOPS", 2))
    max_plan_steps: int = field(default_factory=lambda: _i("MAX_PLAN_STEPS", 24))
    orch_prefetch: int = field(default_factory=lambda: _i("ORCH_PREFETCH", 64))
    max_inflight_cases: int = field(default_factory=lambda: _i("MAX_INFLIGHT_CASES", 4))
    recursion_limit: int = field(default_factory=lambda: _i("RECURSION_LIMIT", 40))
    ttl_ms: int = field(default_factory=lambda: _i("TTL_MS", 900_000))

    # --- 计划层 ---
    plan_mode: str = field(default_factory=lambda: _s("PLAN_MODE", "auto"))  # auto|strict|llm_only
    plan_llm_endpoint: str = field(default_factory=lambda: _s("PLAN_LLM_ENDPOINT", ""))
    plan_model: str = field(default_factory=lambda: _s("PLAN_MODEL", "mock-planner/v1"))
    plan_mock_enabled: bool = field(default_factory=lambda: _b("MOCK_LLM_ENABLED", True))
    plan_mock_scenario: str = field(default_factory=lambda: _s("PLAN_MOCK_SCENARIO", "internal_ok"))
    plan_ctx_chars: int = field(default_factory=lambda: _i("PLAN_CTX_CHARS", 1200))

    # --- 调度 ---
    # 编排层启动后到第一次定时扫描之间的宽限期：Agent Pod / 进程通常比编排层晚几十秒就绪，
    # 宽限期内下命令会让 Monitor 命令在队列里等着，直接把第一次扫描判成 timeout。
    scan_startup_delay_s: int = field(default_factory=lambda: _i("SCAN_STARTUP_DELAY_S", 30))
    monitor_scan_interval_s: int = field(
        default_factory=lambda: _i("MONITOR_SCAN_INTERVAL_S", 3600)
    )
    scheduler_enabled: bool = field(default_factory=lambda: _b("SCHEDULER_ENABLED", True))
    report_at: str = field(default_factory=lambda: _s("REPORT_AT", "18:00"))
    # 日报的「一天」按这个时区划分（统计窗口与 day 计算都走 common.dates）。
    # Postgres now() 是 UTC、k8s Pod 默认也是 UTC，因此默认 UTC；本机演示可设 Asia/Shanghai。
    report_tz: str = field(default_factory=lambda: _s("REPORT_TZ", "UTC"))

    # --- Mock 行为 ---
    mock_scenario: str = field(default_factory=lambda: _s("MOCK_SCENARIO", "default"))
    mock_delay_ms: str = field(default_factory=lambda: _s("MOCK_DELAY_MS", "150:500"))
    mock_failure_rate: float = field(default_factory=lambda: _f("MOCK_FAILURE_RATE", 0.0))
    noise_threshold: float = field(default_factory=lambda: _f("NOISE_THRESHOLD", 0.85))
    repro_enabled: bool = field(default_factory=lambda: _b("REPRO_ENABLED", True))
    repro_max_runs: int = field(default_factory=lambda: _i("REPRO_MAX_RUNS", 3))
    ticket_autocreate: bool = field(default_factory=lambda: _b("TICKET_AUTOCREATE", True))
    ticket_backend: str = field(default_factory=lambda: _s("TICKET_BACKEND", "jira-mock"))

    # --- 服务 ---
    http_port: int = field(default_factory=lambda: _i("HTTP_PORT", 8080))
    log_level: str = field(default_factory=lambda: _s("LOG_LEVEL", "info"))
    log_json: bool = field(default_factory=lambda: _b("LOG_JSON", True))

    def redacted(self) -> dict:
        return {
            k: (v if k not in ("amqp_url", "pg_dsn") else v.split("@")[-1])
            for k, v in self.__dict__.items()
        }


cfg = Config()
