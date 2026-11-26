"""计划层：Workflow Registry（预定义）→ 未命中则 LLMPlanner 兜底；两者同契约、同一校验器。

安全网：LLM 失败/非法 → degraded 内置计划（signal=error → s2_full；signal=none → s1_lite）；
PLAN_MODE=strict → 不调用 LLM，未命中直接 degraded。
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from iqe_orchestrator.common.config import cfg
from iqe_orchestrator.common.envelope import now_iso, ulid, validate

log = logging.getLogger("orchestrator.plan")

STAGE_MODES = {
    "triage": {"full", "classify_only"},
    "reproduction": {"run"},
    "ticket": {"run"},
    "review": {"run"},
    "reporting": {"case", "daily"},
}
ALLOWED_STAGES = set(STAGE_MODES)
STATUS_ENUM = {
    "created",
    "triaged",
    "reproduced",
    "ticketed",
    "reviewed",
    "reported",
    "closed",
    "closed_rejected",
    "blocked",
}
BANNED = re.compile(
    r"(ci\.cmd\.|ci\.result\.|ci\.event\.|q\.agent\.|q\.orch\.|exchange\b|amqp://|http://|https://|localhost|127\.0\.0\.1)"
)


@dataclass
class Step:
    id: str
    stage: str
    mode: str
    max_attempts: int = 2
    timeout_s: float = 30.0
    on: dict = field(default_factory=dict)

    @staticmethod
    def parse(raw: dict) -> Step:
        return Step(
            id=raw["id"],
            stage=raw["stage"],
            mode=raw["mode"],
            max_attempts=int(raw.get("max_attempts", cfg.stage_max_attempts)),
            timeout_s=float(raw.get("timeout_s", cfg.stage_timeout_s)),
            on=raw.get("on") or {},
        )


@dataclass
class Plan:
    plan_id: str
    source: str  # workflow | llm
    steps: list[Step]
    workflow_key: str | None = None
    degraded: bool = False
    signal: str = "unknown"
    rationale: str | None = None
    model: str | None = None
    confidence: float | None = None
    created_at: str = field(default_factory=now_iso)
    file_path: str | None = None
    sha256: str | None = None

    def step_at(self, cursor: int) -> Step | None:
        return self.steps[cursor] if 0 <= cursor < len(self.steps) else None

    def index_of(self, step_id: str) -> int:
        for i, s in enumerate(self.steps):
            if s.id == step_id:
                return i
        return -1

    def to_dict(self) -> dict:
        return {
            "plan_id": self.plan_id,
            "version": 1,
            "source": self.source,
            "workflow_key": self.workflow_key,
            "degraded": self.degraded,
            "created_at": self.created_at,
            "scope": "case",
            "signal": self.signal,
            "rationale": self.rationale,
            "model": self.model,
            "confidence": self.confidence,
            "steps": [
                {
                    "id": s.id,
                    "stage": s.stage,
                    "mode": s.mode,
                    "max_attempts": s.max_attempts,
                    "timeout_s": s.timeout_s,
                    **({"on": s.on} if s.on else {}),
                }
                for s in self.steps
            ],
            "validator": "plan-validator/v1",
        }

    def yaml_text(self) -> str:
        return yaml.safe_dump(self.to_dict(), sort_keys=False, allow_unicode=True)


# --------------------------------------------------------------------------- 校验器 V1–V8
def validate_plan(plan_dict: dict, *, max_steps: int | None = None) -> list[str]:
    """返回违规列表；空列表 = 合法。JSON Schema 打底 + V1–V8 语义规则。"""
    v: list[str] = []
    if any(True in s for s in plan_dict.get("steps") or []):
        plan_dict = {**plan_dict, "steps": normalize_steps(plan_dict.get("steps"))}
    try:
        validate(plan_dict, "plan.schema.json")
    except ValueError as exc:
        v.append(f"V0 schema: {exc}")
        return v

    steps = plan_dict.get("steps") or []
    limit = max_steps or cfg.max_plan_steps
    if not (1 <= len(steps) <= limit):  # V1
        v.append(f"V1 steps 数量 {len(steps)} 不在 [1,{limit}]")
    for s in steps:  # V2 / V3
        if s["stage"] not in ALLOWED_STAGES:
            v.append(
                f"V2 禁止的 stage: {s['stage']}（monitor/reporting 之外的来源不允许）"
                if s["stage"] != "monitor"
                else "V2 monitor 只能由 Scheduler 触发，不能进计划"
            )
        elif s["mode"] not in STAGE_MODES[s["stage"]]:
            v.append(
                f"V3 stage={s['stage']} 的 mode 只能是 {sorted(STAGE_MODES[s['stage']])}，实际 {s['mode']}"
            )
    ids = [s["id"] for s in steps]
    if len(set(ids)) != len(ids):  # V7
        v.append("V7 step id 重复")
    stages_seq = [s["stage"] for s in steps]
    signal = plan_dict.get("signal", "unknown")
    if signal == "error" and "review" not in stages_seq:  # V4
        v.append("V4 signal=error 的计划必须包含 review")
    if (
        "reporting" in stages_seq
        and "review" in stages_seq
        and stages_seq.index("reporting") < stages_seq.index("review")
    ):
        v.append("V4 reporting 必须在 review 之后")
    for s in steps:  # V5 / V6
        for route in (s.get("on") or {}).values():
            if "jump" in route:
                tgt = route["jump"]
                if tgt not in ids:
                    v.append(f"V5 jump 目标不存在: {tgt}")
                elif ids.index(tgt) >= ids.index(s["id"]):
                    v.append(f"V5 jump 必须回到更早的 step: {s['id']} -> {tgt}")
                loops = int(route.get("max_loops", cfg.max_review_loops))
                if loops > cfg.max_review_loops:
                    v.append(f"V6 max_loops={loops} 超过上限 {cfg.max_review_loops}")
            elif "next" in route and route["next"] not in ids:
                v.append(f"V5 next 目标不存在: {route['next']}")
            if route.get("set_status") and route["set_status"] not in STATUS_ENUM:
                v.append(f"V5 非法 set_status: {route['set_status']}")
    for text in (plan_dict.get("rationale") or "", plan_dict.get("model") or ""):  # V8
        if BANNED.search(text or ""):
            v.append(
                f"V8 计划文本含被禁止的内容（总线 subject/队列名/URL/命令字面量）: {text[:80]}"
            )
    for s in steps:
        for val in [s["stage"], s["mode"], s["id"]]:
            if not isinstance(val, str) or BANNED.search(val):
                v.append(f"V8 step 字段非法: {val!r}")
    return v


def normalize_steps(steps: list[dict]) -> list[dict]:
    """YAML 1.1 会把裸 on: 解析成布尔 True —— 归一化回字符串 'on'。"""
    out = []
    for s in steps or []:
        s = dict(s)
        if True in s:
            s["on"] = s.pop(True)
        out.append(s)
    return out


def plan_from_dict(raw: dict) -> Plan:
    """把已校验过的计划字典还原成 Plan 对象（保留 workflow 来源与 workflow_key）。"""
    return Plan(
        plan_id=raw["plan_id"],
        source=raw.get("source") or "workflow",
        workflow_key=raw.get("workflow_key"),
        steps=[Step.parse(s) for s in normalize_steps(raw.get("steps"))],
        signal=raw.get("signal", "unknown"),
        degraded=bool(raw.get("degraded", False)),
        rationale=raw.get("rationale"),
        model=raw.get("model"),
        confidence=raw.get("confidence"),
        created_at=raw.get("created_at"),
    )


# --------------------------------------------------------------------------- Workflow Registry
@dataclass
class Workflow:
    key: str
    title: str
    enabled: bool
    priority: int
    match: dict
    steps: list[dict]
    version: int = 1

    @staticmethod
    def load(path: Path) -> Workflow:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            raise ValueError(f"{path}: workflow YAML 必须是对象")
        return Workflow(
            key=doc["key"],
            title=doc.get("title", doc["key"]),
            enabled=bool(doc.get("enabled", True)),
            priority=int(doc.get("priority", 0)),
            match=doc.get("match") or {},
            steps=normalize_steps(doc["steps"]),
            version=int(doc.get("version", 1)),
        )


class PlanResolver:
    def __init__(self, workflows_dir: str | None = None):
        self.workflows_dir = Path(workflows_dir or cfg.workflows_dir)
        self.workflows: dict[str, Workflow] = {}

    def load_registry(self) -> int:
        self.workflows = {}
        if self.workflows_dir.exists():
            for p in sorted(self.workflows_dir.glob("*.yaml")):
                try:
                    wf = Workflow.load(p)
                except Exception as exc:  # 注册表解析失败只跳过该条
                    log.warning("workflow_load_failed file=%s err=%s", p.name, str(exc)[:200])
                    continue
                self.workflows[wf.key] = wf
        return len(self.workflows)

    def match(self, *, signal: str, job: str, signature: str | None) -> Workflow | None:
        cands = []
        for wf in self.workflows.values():
            if not wf.enabled:
                continue
            m = wf.match
            if m.get("signal") and m["signal"] != signal:
                continue
            if m.get("job_pattern") and not re.search(m["job_pattern"], job or ""):
                continue
            if m.get("job_not_pattern") and re.search(m["job_not_pattern"], job or ""):
                continue
            if m.get("signature_family_prefix") and not (signature or "").startswith(
                m["signature_family_prefix"]
            ):
                continue
            cands.append(wf)
        if not cands:
            return None
        cands.sort(key=lambda w: (-w.priority, w.key))
        return cands[0]

    def plan_for(self, *, signal: str, job: str, signature: str | None) -> Plan | None:
        wf = self.match(signal=signal, job=job, signature=signature)
        if not wf:
            return None
        return Plan(
            plan_id=ulid(),
            source="workflow",
            workflow_key=wf.key,
            steps=[Step.parse(s) for s in wf.steps],
            signal=signal,
            rationale=f"registry workflow {wf.key} (priority={wf.priority})",
            created_at=now_iso(),
        )


# --------------------------------------------------------------------------- LLM 兜底
PLAN_PROMPT = """你是 CI 故障编排规划器。为下面这个 Case 选择执行计划，只输出 YAML（符合 plan 契约）。
可选 stage：triage(full|classify_only) / reproduction(run) / ticket(run) / review(run) / reporting(case)。
硬性规则：signal=error 必须包含 review；reporting 只能在 review 之后；step id 用小写字母数字；
不允许出现队列名、exchange 名、URL、命令字面量。最多 {max_steps} 步。rationale 用一句中文说明取舍。
Case 摘要：{ctx}
"""


class LLMPlanner:
    """PLAN_LLM_ENDPOINT 为空或 MOCK_LLM_ENABLED=true 时用夹具 Mock 模型（演示零模型调用）。"""

    def __init__(self, fixtures_dir: str | None = None):
        self.fixtures = Path(fixtures_dir or cfg.fixtures_dir) / "plans"

    def _raw(self, scenario: str | None = None) -> dict:
        scenario = scenario or cfg.plan_mock_scenario or "warming_ok"
        path = self.fixtures / f"{scenario}.yaml"
        if not path.exists():
            path = self.fixtures / "warming_ok.yaml"
        return yaml.safe_load(path.read_text(encoding="utf-8"))

    def build(
        self,
        *,
        signal: str,
        job: str,
        signature: str | None,
        hint: str | None,
        ctx_chars: int | None = None,
    ) -> tuple[Plan | None, list[str]]:
        ctx = json.dumps(
            {
                "signal": signal,
                "job": job,
                "signature_family": (signature or "").split(":")[:2],
                "hint": hint,
            },
            ensure_ascii=False,
        )
        prompt = PLAN_PROMPT.format(
            max_steps=cfg.max_plan_steps, ctx=ctx[: ctx_chars or cfg.plan_ctx_chars]
        )
        log.info(
            "llm_plan_prompt_chars=%d endpoint=%s mock=%s",
            len(prompt),
            cfg.plan_llm_endpoint or "<mock>",
            cfg.plan_mock_enabled,
        )
        for attempt in (1, 2):
            raw = self._raw(hint if hint and (self.fixtures / f"{hint}.yaml").exists() else None)
            raw = dict(raw)
            raw["steps"] = normalize_steps(raw.get("steps"))
            raw.update(
                {
                    "plan_id": ulid(),
                    "source": "llm",
                    "workflow_key": None,
                    "degraded": False,
                    "created_at": now_iso(),
                    "signal": signal,
                    "scope": "case",
                    "model": cfg.plan_model,
                    "prompt_hash": hashlib.sha256(prompt.encode()).hexdigest()[:16],
                }
            )
            raw.setdefault("rationale", "Mock 规划器：按信号与签名族选择最短可信路径")
            violations = validate_plan(raw)
            if not violations:
                plan = self._to_plan(raw)
                log.info("llm_plan_ok attempt=%d steps=%s", attempt, [s.stage for s in plan.steps])
                return plan, []
            log.warning(
                "llm_plan_invalid attempt=%d violations=%s", attempt, "; ".join(violations)[:400]
            )
        return None, ["llm 计划两次校验失败"]

    @staticmethod
    def _to_plan(raw: dict) -> Plan:
        return Plan(
            plan_id=raw["plan_id"],
            source="llm",
            steps=[Step.parse(s) for s in raw["steps"]],
            signal=raw.get("signal", "unknown"),
            degraded=bool(raw.get("degraded", False)),
            rationale=raw.get("rationale"),
            model=raw.get("model"),
            confidence=raw.get("confidence"),
            created_at=raw.get("created_at") or now_iso(),
        )


# --------------------------------------------------------------------------- 内置安全网
BUILTIN_PLANS: dict[str, dict] = {
    "s1_lite": {"steps": [{"id": "t1", "stage": "triage", "mode": "classify_only"}]},
    "s2_full": {
        "steps": [
            {"id": "t1", "stage": "triage", "mode": "full"},
            {"id": "r1", "stage": "reproduction", "mode": "run"},
            {"id": "k1", "stage": "ticket", "mode": "run"},
            {
                "id": "v1",
                "stage": "review",
                "mode": "run",
                "on": {
                    "approve": {"next": "p1"},
                    "needs_info": {"jump": "t1", "max_loops": 2, "set_status": "triaged"},
                    "reject": {"end": True, "set_status": "closed_rejected"},
                },
            },
            {"id": "p1", "stage": "reporting", "mode": "case"},
        ]
    },
}


def builtin_plan(key: str, signal: str) -> Plan:
    raw = {
        "plan_id": ulid(),
        "version": 1,
        "source": "workflow",
        "workflow_key": key,
        "degraded": True,
        "created_at": now_iso(),
        "scope": "case",
        "signal": signal,
        "rationale": f"degraded 安全网：使用内置 {key}",
        "steps": BUILTIN_PLANS[key]["steps"],
        "validator": "plan-validator/v1",
    }
    violations = validate_plan(raw)
    if violations:
        raise RuntimeError(f"内置计划非法 {key}: {violations}")
    plan = LLMPlanner._to_plan(raw)
    plan.degraded = True
    plan.workflow_key = key
    return plan


# --------------------------------------------------------------------------- 计划落盘
class PlanStore:
    def __init__(self, plans_dir: str | None = None):
        self.plans_dir = Path(plans_dir or cfg.plans_dir)

    def seal(self, plan: Plan, case_id: str) -> Plan:
        d = self.plans_dir / case_id
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"{plan.plan_id}.yaml"
        text = plan.yaml_text()
        path.write_text(text, encoding="utf-8")
        plan.file_path = str(path)
        plan.sha256 = hashlib.sha256(text.encode("utf-8")).hexdigest()
        return plan

    def read(self, case_id: str, plan_id: str) -> str | None:
        p = self.plans_dir / case_id / f"{plan_id}.yaml"
        return p.read_text(encoding="utf-8") if p.exists() else None
