"""计划层：Workflow Registry（预定义）→ 未命中则 LLMPlanner 兜底；两者同契约、同一校验器。

图中的 12 个阶段由 8 个 Agent 的 mode 承载（见 common/contracts.py），计划只编排 step：
- ``next``：前向跳转，用来表达闸门 1 的 internal / external 双分支、非回归跳过 6c；
- ``jump``：后向回炉，只能指回更早的 step（gate2 rework→4a、gate3 needs_info→7b）。
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from iqe_orchestrator.common.config import cfg
from iqe_orchestrator.common.contracts import MODE_AUTHORITY
from iqe_orchestrator.common.envelope import now_iso, ulid, validate

log = logging.getLogger("orchestrator.plan")

#: 计划里允许出现的 stage：monitor 只由 Scheduler 触发，不进计划。
STAGE_MODES: dict[str, set[str]] = {
    stage: set(modes) for stage, modes in MODE_AUTHORITY.items() if stage != "monitor"
}
ALLOWED_STAGES = set(STAGE_MODES)
STATUS_ENUM = {
    "created",
    "triaged",
    "routed_internal",
    "routed_external",
    "remediated",
    "fix_verified",
    "fix_approved",
    "reproduced",
    "regression_checked",
    "bisected",
    "ticketed",
    "ticket_approved",
    "reported",
    "closed",
    "closed_rejected",
    "blocked",
}
ROUTE_VERBS = {
    "internal",
    "external",
    "approve",
    "rework",
    "needs_info",
    "reject",
    "regression",
    "not_regression",
    "done",
    "blocked",
}
JUMP_VERBS = {"rework", "needs_info"}
BANNED = re.compile(
    r"(ci\.cmd\.|ci\.result\.|ci\.event\.|q\.agent\.|q\.orch\.|exchange\b|amqp://|http://|https://|localhost|127\.0\.0\.1)"
)
STEP_ID = re.compile(r"^[a-z][a-z0-9_]{0,15}$")


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
            # 路由表可能与注册表/内置计划共享同一份对象，解析时立刻深拷贝
            on=copy.deepcopy(raw.get("on") or {}),
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
                    **({"on": copy.deepcopy(s.on)} if s.on else {}),
                }
                for s in self.steps
            ],
            "validator": "plan-validator/v1",
        }

    def yaml_text(self) -> str:
        return yaml.safe_dump(self.to_dict(), sort_keys=False, allow_unicode=True)


# --------------------------------------------------------------------------- 校验器 V0–V9
def validate_plan(plan_dict: dict, *, max_steps: int | None = None) -> list[str]:
    """返回违规列表；空列表 = 合法。JSON Schema 打底 + V1–V9 语义规则。"""
    v: list[str] = []
    if any(True in s for s in plan_dict.get("steps") or []):
        plan_dict = {**plan_dict, "steps": normalize_steps(plan_dict.get("steps"))}
    try:
        validate(plan_dict, "plan.schema.json")
    except ValueError as exc:
        v.append(f"V0 schema: {exc}")
        return v

    steps = plan_dict.get("steps") or []
    ids = [s["id"] for s in steps]
    pos = {sid: i for i, sid in enumerate(ids)}
    limit = max_steps or cfg.max_plan_steps

    if not (1 <= len(steps) <= limit):  # V1
        v.append(f"V1 steps 数量 {len(steps)} 不在 [1,{limit}]")
    for s in steps:  # V2 / V3
        stage, mode = s["stage"], s["mode"]
        if stage not in ALLOWED_STAGES:
            v.append(
                f"V2 monitor 只能由 Scheduler 触发，不能进计划（step={s['id']}）"
                if stage == "monitor"
                else f"V2 禁止的 stage: {stage}（step={s['id']}）"
            )
        elif mode not in STAGE_MODES[stage]:
            v.append(
                f"V3 stage={stage} 的 mode 只能是 {sorted(STAGE_MODES[stage])}，"
                f"实际 {mode}（step={s['id']}）"
            )
    if len(set(ids)) != len(ids):  # V7
        v.append("V7 step id 重复")
    for sid in ids:
        if not STEP_ID.match(sid):
            v.append(f"V7 非法 step id: {sid!r}")

    # ---------------- V4 主线形状：分流闸门必须在修/建单之前，报告必须最后 ----------------
    signal = plan_dict.get("signal", "unknown")
    has = {(s["stage"], s["mode"]) for s in steps}
    if signal == "error":
        if not steps or steps[0]["stage"] != "triage":
            v.append("V4 signal=error 的计划必须以 triage 开头")
        if ("reviewer", "gate1") not in has:
            v.append(
                "V4 signal=error 的计划必须包含 reviewer:gate1（闸门 1 决定 internal/external）"
            )
        ordered = (
            (("reviewer", "gate2"), ("reviewer", "gate1"), "V4 闸门 2 必须在闸门 1 之后"),
            (("reviewer", "gate3"), ("reviewer", "gate1"), "V4 闸门 3 必须在闸门 1 之后"),
            (
                ("reproduction", "verify"),
                ("remediation", "fix"),
                "V4 修复验证（5a）必须在 4a 修复之后",
            ),
            (
                ("regression", "bisect"),
                ("regression", "check"),
                "V4 二分定位（6c）必须在回归判定（5b）之后",
            ),
            (
                ("ticketing", "create"),
                ("regression", "check"),
                "V4 建单（7b）必须在回归判定（5b）之后",
            ),
        )
        for later, earlier, msg in ordered:
            if later in has and earlier in has and _first(steps, *later) < _first(steps, *earlier):
                v.append(msg)
    if ("reporting", "case") in has:
        last = max(pos.values()) if pos else 0
        if pos.get(_last_id(steps, "reporting"), -1) != last:
            v.append("V4 reporting 必须是计划的最后一步（且只能出现一次）")
        if sum(1 for s in steps if s["stage"] == "reporting" and s["mode"] == "case") > 1:
            v.append("V4 reporting:case 在一个计划里只能出现一次")

    # ---------------- V5 / V6 路由与回炉 ----------------
    for s in steps:
        on = s.get("on") or {}
        for verb, route in on.items():
            if verb not in ROUTE_VERBS:
                v.append(f"V5 step={s['id']} 非法路由动词 {verb!r}")
                continue
            if ("jump" in route) != (verb in JUMP_VERBS):
                v.append(
                    f"V5 step={s['id']} 的 {verb} 路由必须"
                    f"{'是 jump（回炉）' if verb in JUMP_VERBS else '是 next/end（前向）'}"
                )
            if "jump" in route:
                if "next" in route or "end" in route:
                    v.append(f"V5 step={s['id']} 的 {verb} 是回炉路由，不能再带 next/end")
            elif ("next" in route) == ("end" in route):
                v.append(f"V5 step={s['id']} 的 {verb} 路由必须且只能有 next 或 end 之一")
            if "jump" in route:
                tgt = route["jump"]
                if tgt not in pos:
                    v.append(f"V5 jump 目标不存在: {tgt}")
                elif pos[tgt] >= pos[s["id"]]:
                    v.append(f"V5 jump 必须回到更早的 step: {s['id']} -> {tgt}")
                loops = int(route.get("max_loops", cfg.max_review_loops))
                if loops > cfg.max_review_loops:
                    v.append(f"V6 max_loops={loops} 超过上限 {cfg.max_review_loops}")
            elif "next" in route:
                tgt = route["next"]
                if tgt not in pos:
                    v.append(f"V5 next 目标不存在: {tgt}")
                elif pos[tgt] <= pos[s["id"]]:
                    v.append(f"V5 next 只能前向: {s['id']} -> {tgt}（回炉请用 jump）")
            if route.get("set_status") and route["set_status"] not in STATUS_ENUM:
                v.append(f"V5 非法 set_status: {route['set_status']}")

    # ---------------- V8 防注入 ----------------
    for text in (plan_dict.get("rationale") or "", plan_dict.get("model") or ""):  # V8
        if BANNED.search(text or ""):
            v.append(
                f"V8 计划文本含被禁止的内容（总线 subject/队列名/URL/命令字面量）: {text[:80]}"
            )
    for s in steps:
        for val in [s["stage"], s["mode"], s["id"]]:
            if not isinstance(val, str) or BANNED.search(val):
                v.append(f"V8 step 字段非法: {val!r}")

    # ---------------- V9 分支闭合 ----------------
    for s in steps:
        on = s.get("on") or {}
        if (
            (s["stage"], s["mode"]) == ("reviewer", "gate1")
            and signal == "error"
            and ("internal" not in on or "external" not in on)
        ):
            v.append(
                f"V9 step={s['id']}（gate1）必须同时给出 internal 与 external 路由，否则双分支无法闭合"
            )
        if (s["stage"], s["mode"]) == ("reviewer", "gate2") and "approve" not in on:
            v.append(f"V9 step={s['id']}（gate2）必须给出 approve 路由")
        if (s["stage"], s["mode"]) == ("reviewer", "gate3") and "approve" not in on:
            v.append(f"V9 step={s['id']}（gate3）必须给出 approve 路由")
        if (s["stage"], s["mode"]) == ("regression", "check") and (
            "regression" not in on or "not_regression" not in on
        ):
            v.append(f"V9 step={s['id']}（5b）必须同时给出 regression 与 not_regression 路由")
    return v


def _first(steps: list[dict], stage: str, mode: str) -> int:
    for i, s in enumerate(steps):
        if s["stage"] == stage and s["mode"] == mode:
            return i
    return len(steps) + 1


def _last_id(steps: list[dict], stage: str) -> str | None:
    for s in reversed(steps):
        if s["stage"] == stage:
            return s["id"]
    return None


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
8 个 Agent 的 mode：
  triage(classify) / reviewer(gate1|gate2|gate3) / remediation(fix) /
  reproduction(evidence|verify) / regression(check|bisect) / ticketing(create) / reporting(case)
硬性规则：signal=error 必须先 triage 再 reviewer:gate1；gate1 必须给出 internal/external 双路由；
内部路径 = remediation:fix → reproduction:verify → reviewer:gate2；
外部路径 = reproduction:evidence → regression:check →（回归则 regression:bisect）→ ticketing:create → reviewer:gate3；
reporting:case 必须是最后一步；回炉只能用 jump 且只能指向更早的 step；最多 {max_steps} 步。
不允许出现队列名、exchange 名、URL、命令字面量。rationale 用一句中文说明取舍。
Case 摘要：{ctx}
"""


class LLMPlanner:
    """PLAN_LLM_ENDPOINT 为空或 MOCK_LLM_ENABLED=true 时用夹具 Mock 模型（演示零模型调用）。"""

    def __init__(self, fixtures_dir: str | None = None):
        self.fixtures = Path(fixtures_dir or cfg.fixtures_dir) / "plans"

    def _raw(self, scenario: str | None = None) -> dict:
        scenario = scenario or cfg.plan_mock_scenario or "internal_ok"
        path = self.fixtures / f"{scenario}.yaml"
        if not path.exists():
            path = self.fixtures / "internal_ok.yaml"
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
    "s1_observe_only": {
        "steps": [
            {"id": "t1", "stage": "triage", "mode": "classify"},
            {"id": "p1", "stage": "reporting", "mode": "case"},
        ]
    },
    "s2_full": {
        "steps": [
            {"id": "t1", "stage": "triage", "mode": "classify"},
            {
                "id": "g1",
                "stage": "reviewer",
                "mode": "gate1",
                "on": {
                    "internal": {"next": "r1", "set_status": "routed_internal"},
                    "external": {"next": "e1", "set_status": "routed_external"},
                    "needs_info": {"jump": "t1", "max_loops": 2, "set_status": "triaged"},
                    "reject": {"end": True, "set_status": "closed_rejected"},
                },
            },
            {"id": "r1", "stage": "remediation", "mode": "fix"},
            {"id": "v1", "stage": "reproduction", "mode": "verify"},
            {
                "id": "g2",
                "stage": "reviewer",
                "mode": "gate2",
                "on": {
                    "approve": {"next": "p1", "set_status": "fix_approved"},
                    "rework": {"jump": "r1", "max_loops": 2, "set_status": "remediated"},
                    "reject": {"end": True, "set_status": "closed_rejected"},
                },
            },
            {"id": "e1", "stage": "reproduction", "mode": "evidence"},
            {
                "id": "c1",
                "stage": "regression",
                "mode": "check",
                "on": {
                    "regression": {"next": "b1", "set_status": "regression_checked"},
                    "not_regression": {"next": "k1", "set_status": "reproduced"},
                },
            },
            {"id": "b1", "stage": "regression", "mode": "bisect"},
            {"id": "k1", "stage": "ticketing", "mode": "create"},
            {
                "id": "g3",
                "stage": "reviewer",
                "mode": "gate3",
                "on": {
                    "approve": {"next": "p1", "set_status": "ticket_approved"},
                    "needs_info": {"jump": "k1", "max_loops": 2, "set_status": "ticketed"},
                    "reject": {"end": True, "set_status": "closed_rejected"},
                },
            },
            {"id": "p1", "stage": "reporting", "mode": "case"},
        ]
    },
    "s3_test_no_repro": {
        "steps": [
            {"id": "t1", "stage": "triage", "mode": "classify"},
            {
                "id": "g1",
                "stage": "reviewer",
                "mode": "gate1",
                "on": {
                    "internal": {"next": "r1", "set_status": "routed_internal"},
                    "external": {"next": "c1", "set_status": "routed_external"},
                    "needs_info": {"jump": "t1", "max_loops": 2, "set_status": "triaged"},
                    "reject": {"end": True, "set_status": "closed_rejected"},
                },
            },
            {"id": "r1", "stage": "remediation", "mode": "fix"},
            {"id": "v1", "stage": "reproduction", "mode": "verify"},
            {
                "id": "g2",
                "stage": "reviewer",
                "mode": "gate2",
                "on": {
                    "approve": {"next": "p1", "set_status": "fix_approved"},
                    "rework": {"jump": "r1", "max_loops": 2, "set_status": "remediated"},
                    "reject": {"end": True, "set_status": "closed_rejected"},
                },
            },
            {
                "id": "c1",
                "stage": "regression",
                "mode": "check",
                "on": {
                    "regression": {"next": "b1", "set_status": "regression_checked"},
                    "not_regression": {"next": "k1", "set_status": "reproduced"},
                },
            },
            {"id": "b1", "stage": "regression", "mode": "bisect"},
            {"id": "k1", "stage": "ticketing", "mode": "create"},
            {
                "id": "g3",
                "stage": "reviewer",
                "mode": "gate3",
                "on": {
                    "approve": {"next": "p1", "set_status": "ticket_approved"},
                    "needs_info": {"jump": "k1", "max_loops": 2, "set_status": "ticketed"},
                    "reject": {"end": True, "set_status": "closed_rejected"},
                },
            },
            {"id": "p1", "stage": "reporting", "mode": "case"},
        ]
    },
}


def builtin_key(signal: str) -> str:
    return "s2_full" if signal == "error" else "s1_observe_only"


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
        "steps": copy.deepcopy(BUILTIN_PLANS[key]["steps"]),
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


__all__ = [
    "ALLOWED_STAGES",
    "BUILTIN_PLANS",
    "STAGE_MODES",
    "STATUS_ENUM",
    "LLMPlanner",
    "Plan",
    "PlanResolver",
    "PlanStore",
    "Step",
    "builtin_key",
    "builtin_plan",
    "normalize_steps",
    "plan_from_dict",
    "validate_plan",
]
