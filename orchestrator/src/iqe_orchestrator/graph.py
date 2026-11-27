"""LangGraph 编排图：resolve_plan → validate_plan → step → advance → finalize。

流程图上的 12 个阶段收敛成 8 个 Agent 的 mode（见 ``common/contracts.py``），
计划层只描述 step，执行期不调用模型：

- ``next``：前向跳转 —— 闸门 1 的 internal / external 双分支、非回归跳过 6c；
- ``jump``：后向回炉 —— 闸门 1/3 的 needs_info 回到上游、闸门 2 的 rework 回到 4a；

step 输出按 **step_id** 存放：reviewer 在一个计划里会跑三道闸门，按 stage 存会互相覆盖。
Dispatch–Await 在 step 节点内完成（发命令 → 等 Future → 超时/重试/blocked）。
"""

from __future__ import annotations

import asyncio
import logging
from typing import TypedDict

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import END, START, StateGraph

from iqe_orchestrator.common.config import cfg
from iqe_orchestrator.common.contracts import validate_stage_contract
from iqe_orchestrator.common.db import DB
from iqe_orchestrator.common.envelope import now_iso, ulid
from iqe_orchestrator.common.kb import normalize_write
from iqe_orchestrator.common.logs import counters, log_with

from .dispatch import BusGateway
from .plan import (
    LLMPlanner,
    PlanResolver,
    PlanStore,
    builtin_key,
    builtin_plan,
    plan_from_dict,
    validate_plan,
)

log = logging.getLogger("orchestrator.graph")

#: 某一步成功后 Case 应该落在哪个状态（路由里的 set_status 优先）。
STATUS_AFTER: dict[tuple[str, str], str] = {
    ("triage", "classify"): "triaged",
    ("reviewer", "gate1"): "triaged",
    ("remediation", "fix"): "remediated",
    ("reproduction", "verify"): "fix_verified",
    ("reviewer", "gate2"): "fix_approved",
    ("reproduction", "evidence"): "reproduced",
    ("regression", "check"): "regression_checked",
    ("regression", "bisect"): "bisected",
    ("ticketing", "create"): "ticketed",
    ("reviewer", "gate3"): "ticket_approved",
    ("reporting", "case"): "reported",
    ("reporting", "daily"): "reported",
}


class CaseState(TypedDict, total=False):
    case_id: str
    run_id: str
    scan_id: str | None
    job: str
    build: int | None
    signal: str
    signature: str | None
    scenario_hint: str | None
    payload: dict
    plan: dict
    plan_id: str
    plan_source: str
    workflow_key: str | None
    degraded: bool
    plan_violations: list[str]
    cursor: int
    step_status: dict[str, str]
    attempts: dict[str, int]
    outputs: dict[str, dict]
    completed: list[str]
    loops: dict[str, int]
    pending_verb: str | None
    pending_route: dict | None
    loop_count: int
    review_loop_seq: int
    jump_count: int
    requested_focus: str | None
    evidence_count: int
    status: str
    terminated: bool
    blocked_reason: str | None
    trace: list[dict]
    gen: int


class CaseEngine:
    def __init__(
        self,
        db: DB,
        gateway: BusGateway,
        resolver: PlanResolver,
        planner: LLMPlanner,
        store: PlanStore,
        saver: AsyncPostgresSaver,
    ):
        self.db = db
        self.gw = gateway
        self.resolver = resolver
        self.planner = planner
        self.store = store
        self.saver = saver
        self.sem = asyncio.Semaphore(cfg.max_inflight_cases)
        self.graph = self._build_graph().compile(checkpointer=saver)
        self.tasks: set[asyncio.Task] = set()

    # ------------------------------------------------------------- 对外入口
    async def start_case(self, *, obs: dict, run_id: str, scan_id: str | None) -> str:
        case_id = ulid()
        signal = obs["signal"]
        await self.db.create_case(
            case_id=case_id,
            run_id=run_id,
            scan_id=scan_id,
            source=obs.get("source", "mock"),
            job=obs["job"],
            build=obs.get("build"),
            signal=signal,
            signature=obs.get("signature"),
            status="created",
            priority=1 if signal == "error" else 3,
            scenario_hint=(obs.get("context") or {}).get("mock_scenario"),
            payload={"observation": obs},
        )
        state: CaseState = {
            "case_id": case_id,
            "run_id": run_id,
            "scan_id": scan_id,
            "job": obs["job"],
            "build": obs.get("build"),
            "signal": signal,
            "signature": obs.get("signature"),
            "scenario_hint": (obs.get("context") or {}).get("mock_scenario"),
            "payload": obs,
            "cursor": 0,
            "step_status": {},
            "attempts": {},
            "outputs": {},
            "completed": [],
            "loops": {},
            "loop_count": 0,
            "review_loop_seq": 0,
            "jump_count": 0,
            "evidence_count": 0,
            "status": "created",
            "terminated": False,
            "trace": [],
            "gen": 1,
            "plan_violations": [],
        }
        task = asyncio.create_task(self._run(case_id, state, resume=False))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return case_id

    async def replay_case(self, case_id: str) -> dict:
        case = await self.db.get_case(case_id)
        if not case:
            return {"ok": False, "reason": "case_not_found"}
        progress = case["progress"] or {}
        gen = int(progress.get("gen", 1)) + 1
        payload = case["payload"] or {}
        obs = payload.get("observation") or {
            "job": case["job"],
            "build": case["build"],
            "signal": case["signal"],
            "signature": case["signature"],
            "source": case["source"],
            "context": {},
        }
        seeded_attempts = await self.db.max_attempt_by_step(case_id)
        state: CaseState = {
            "case_id": case_id,
            "run_id": case["run_id"],
            "scan_id": case["scan_id"],
            "job": case["job"],
            "build": case["build"],
            "signal": case["signal"],
            "signature": case["signature"],
            "scenario_hint": case["scenario_hint"],
            "payload": obs,
            "cursor": 0,
            "step_status": {},
            "attempts": dict(seeded_attempts),
            "outputs": {},
            "completed": [],
            "loops": {},
            "loop_count": 0,
            "review_loop_seq": 0,
            "jump_count": 0,
            "evidence_count": 0,
            "status": "created",
            "terminated": False,
            "trace": [],
            "gen": gen,
            "plan_violations": [],
        }
        await self.db.set_case_status(
            case_id, "created", progress={"gen": gen, "replayed_at": now_iso()}, blocked_reason=None
        )
        task = asyncio.create_task(self._run(case_id, state, resume=False))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return {"ok": True, "case_id": case_id, "gen": gen}

    async def _run(self, case_id: str, state: CaseState, *, resume: bool) -> dict:
        async with self.sem:
            thread = f"{case_id}#{state.get('gen', 1)}"
            log_with(
                log,
                "info",
                "case_graph_start",
                case_id=case_id,
                signal=state["signal"],
                thread=thread,
            )
            try:
                final = await self.graph.ainvoke(
                    dict(state),
                    config={
                        "configurable": {"thread_id": thread},
                        "recursion_limit": cfg.recursion_limit,
                    },
                )
            except Exception as exc:
                counters.inc("ci_case_graph_crashes_total")
                log_with(log, "error", "case_graph_crash", case_id=case_id, err=repr(exc)[:400])
                await self.db.set_case_status(
                    case_id, "blocked", blocked_reason=f"graph_crash: {exc}"[:300]
                )
                return {"status": "blocked"}
            plan_dict = final.get("plan") or {}
            progress = {
                "cursor": final.get("cursor", 0),
                "plan_steps": [
                    {
                        "id": st["id"],
                        "stage": st["stage"],
                        "mode": st["mode"],
                        "timeout_s": st.get("timeout_s"),
                    }
                    for st in plan_dict.get("steps", [])
                ],
                "step_status": final.get("step_status", {}),
                "attempts": final.get("attempts", {}),
                "completed": final.get("completed", []),
                "loops": final.get("loops", {}),
                "review_loop_seq": final.get("review_loop_seq", 0),
                "jump_count": final.get("jump_count", 0),
                "gen": final.get("gen", 1),
                "plan_id": final.get("plan_id"),
                "plan_source": final.get("plan_source"),
                "workflow_key": final.get("workflow_key"),
                "degraded": final.get("degraded", False),
                "terminal_from": final.get("terminal_from"),
                "finished_at": now_iso(),
            }
            await self.db.set_case_status(
                case_id,
                final.get("status", "closed"),
                progress=progress,
                blocked_reason=final.get("blocked_reason"),
            )
            log_with(
                log,
                "info",
                "case_graph_done",
                case_id=case_id,
                status=final.get("status"),
                steps=len(final.get("trace", [])),
            )
            counters.inc("ci_cases_finished_total", status=final.get("status", "unknown"))
            return final

    # ------------------------------------------------------------- 图
    def _build_graph(self) -> StateGraph:
        b = StateGraph(CaseState)
        b.add_node("resolve_plan", self._resolve_plan)
        b.add_node("validate_plan", self._validate_plan)
        b.add_node("step", self._step)
        b.add_node("advance", self._advance)
        b.add_node("finalize", self._finalize)
        b.add_edge(START, "resolve_plan")
        b.add_conditional_edges(
            "resolve_plan",
            lambda s: "finalize" if s.get("plan_violations") else "validate_plan",
            {"validate_plan": "validate_plan", "finalize": "finalize"},
        )
        b.add_conditional_edges(
            "validate_plan",
            lambda s: "finalize" if s.get("plan_violations") else "step",
            {"step": "step", "finalize": "finalize"},
        )
        b.add_edge("step", "advance")
        b.add_conditional_edges(
            "advance",
            lambda s: "finalize" if s.get("terminated") else "step",
            {"step": "step", "finalize": "finalize"},
        )
        b.add_edge("finalize", END)
        return b

    # ------------------------------------------------------------- 节点
    async def _resolve_plan(self, s: CaseState) -> dict:
        signal, job, sig = s["signal"], s["job"], s.get("signature")
        wf = self.resolver.match(signal=signal, job=job, signature=sig)
        if wf and cfg.plan_mode != "llm_only":
            plan = self.resolver.plan_for(signal=signal, job=job, signature=sig)
            log_with(
                log,
                "info",
                "plan_resolved",
                case_id=s["case_id"],
                source="workflow",
                workflow_key=wf.key,
                steps=[x["id"] for x in wf.steps],
            )
            return {
                "plan": plan.to_dict(),
                "plan_id": plan.plan_id,
                "plan_source": "workflow",
                "workflow_key": wf.key,
                "degraded": False,
            }
        if cfg.plan_mode == "strict":
            key = builtin_key(signal)
            plan = builtin_plan(key, signal)
            log_with(log, "warn", "plan_degraded_strict", case_id=s["case_id"], builtin=key)
            return {
                "plan": plan.to_dict(),
                "plan_id": plan.plan_id,
                "plan_source": "workflow",
                "workflow_key": key,
                "degraded": True,
            }
        plan, violations = self.planner.build(
            signal=signal, job=job, signature=sig, hint=s.get("scenario_hint")
        )
        if plan is None:
            key = builtin_key(signal)
            plan = builtin_plan(key, signal)
            log_with(
                log,
                "warn",
                "plan_degraded_llm",
                case_id=s["case_id"],
                builtin=key,
                violations="; ".join(violations)[:200],
            )
            return {
                "plan": plan.to_dict(),
                "plan_id": plan.plan_id,
                "plan_source": "workflow",
                "workflow_key": key,
                "degraded": True,
                "trace": [
                    *s.get("trace", []),
                    {"at": now_iso(), "event": f"llm_plan_degraded:{key}"},
                ],
            }
        log_with(
            log,
            "info",
            "plan_llm_generated",
            case_id=s["case_id"],
            steps=[x.id for x in plan.steps],
            model=plan.model,
            rationale=(plan.rationale or "")[:120],
        )
        return {
            "plan": plan.to_dict(),
            "plan_id": plan.plan_id,
            "plan_source": "llm",
            "workflow_key": None,
            "degraded": False,
        }

    async def _validate_plan(self, s: CaseState) -> dict:
        plan_dict = s["plan"]
        violations = validate_plan(plan_dict)
        if violations:
            counters.inc("ci_plan_rejected_total")
            log_with(
                log,
                "error",
                "plan_validation_failed",
                case_id=s["case_id"],
                violations="; ".join(violations)[:400],
            )
            return {"plan_violations": violations}
        plan = plan_from_dict(plan_dict)
        self.store.seal(plan, s["case_id"])
        await self.db.upsert_plan(
            plan_id=plan.plan_id,
            case_id=s["case_id"],
            source=plan.source,
            workflow_key=plan.workflow_key,
            degraded=plan.degraded,
            path=plan.file_path or "",
            sha256=plan.sha256 or "",
            validator="plan-validator/v1",
            steps=plan_dict["steps"],
            rationale=plan.rationale,
            model=plan.model,
        )
        await self.db.set_case_status(
            s["case_id"],
            s["status"],
            workflow_key=plan.workflow_key,
            plan_id=plan.plan_id,
            plan_source=plan.source,
            priority=1 if s["signal"] == "error" else 3,
        )
        await self._persist(
            {
                **dict(s),
                "plan": plan_dict,
                "plan_id": plan.plan_id,
                "plan_source": plan.source,
                "workflow_key": plan.workflow_key,
                "degraded": plan.degraded,
            }
        )
        log_with(
            log,
            "info",
            "plan_sealed",
            case_id=s["case_id"],
            plan_id=plan.plan_id,
            source=plan.source,
            file=plan.file_path,
            steps=len(plan.steps),
        )
        return {
            "plan_violations": [],
            "plan": plan_dict,
            "plan_id": plan.plan_id,
            "plan_source": plan.source,
            "workflow_key": plan.workflow_key,
            "degraded": plan.degraded,
            "trace": [
                *s.get("trace", []),
                {"at": now_iso(), "event": f"plan_sealed:{plan.source}"},
            ],
        }

    async def _step(self, s: CaseState) -> dict:
        plan = s["plan"]
        idx = s["cursor"]
        if idx >= len(plan["steps"]):
            return {"terminated": True}
        raw = plan["steps"][idx]
        step_id, stage, mode = raw["id"], raw["stage"], raw["mode"]
        # attempt 在 Case 生命周期内单调递增（保证幂等键唯一）；
        # max_attempts 只限制“本次访问”内的重试次数（回炉后会再次进入同一步）。
        attempt_prev = int(s.get("attempts", {}).get(step_id, 0))
        max_attempts = int(raw.get("max_attempts", cfg.stage_max_attempts))
        outputs = dict(s.get("outputs", {}))
        attempts = dict(s.get("attempts", {}))
        trace = list(s.get("trace", []))
        step_status = dict(s.get("step_status", {}))
        verb: str | None = None
        status = s["status"]

        for attempt in range(attempt_prev + 1, attempt_prev + max_attempts + 1):
            attempts[step_id] = attempt
            payload = await self._build_input(s, raw)
            res = await self.gw.dispatch(
                stage=stage,
                mode=mode,
                payload=payload,
                run_id=s["run_id"],
                step_id=step_id,
                attempt=attempt,
                case_id=s["case_id"],
                scan_id=s.get("scan_id"),
                scenario_hint=s.get("scenario_hint"),
                timeout_s=float(raw.get("timeout_s", cfg.stage_timeout_s)),
            )
            trace.append(
                {
                    "at": now_iso(),
                    "event": f"{stage}:{mode}:{step_id}:attempt{attempt}:{res.status}",
                }
            )
            if res.status == "ok":
                outputs[step_id] = res.output
                self._check_output(stage, mode, res.output, s["case_id"])
                verb = self._verb_for(stage, mode, res.output)
                step_status[step_id] = "done"
                route = self._route_for(raw, verb)
                status = (
                    (route or {}).get("set_status")
                    or STATUS_AFTER.get((stage, mode))
                    or s["status"]
                )
                break
            if res.status in ("retryable", "timeout") and attempt < max_attempts:
                log_with(
                    log,
                    "warn",
                    "step_retry",
                    case_id=s["case_id"],
                    stage=stage,
                    mode=mode,
                    step_id=step_id,
                    attempt=attempt,
                    reason=(res.error or {}).get("code"),
                )
                continue
            step_status[step_id] = "blocked"
            counters.inc("ci_steps_blocked_total", stage=stage, mode=mode)
            merged = {
                **self._progress(s, outputs, attempts, step_status, trace),
                "terminated": True,
                "status": "blocked",
                "cursor": idx,
                "blocked_reason": f"{stage}:{mode} 第 {attempt} 次仍失败: "
                f"{(res.error or {}).get('code')} {(res.error or {}).get('detail', '')}"[:300],
                "pending_verb": None,
                "pending_route": None,
            }
            await self._persist({**dict(s), **merged})
            return merged

        evidence_count = len(await self.db.evidence(s["case_id"]))
        await self._store_artifacts(s, step_id, stage, mode, outputs.get(step_id) or {}, status)
        completed = [sid for sid in s.get("completed", []) if sid != step_id] + [step_id]
        await self.gw.publish_event(
            rk=f"ci.event.case.{status}",
            correlation_id=s["case_id"],
            data={"stage": stage, "mode": mode, "step_id": step_id, "verb": verb},
        )
        route = self._route_for(raw, verb or "")
        log_with(
            log,
            "info",
            "step_ok",
            case_id=s["case_id"],
            stage=stage,
            mode=mode,
            step_id=step_id,
            verb=verb,
            evidence_count=evidence_count,
        )
        merged = {
            **self._progress(s, outputs, attempts, step_status, trace),
            "completed": completed,
            "status": status,
            "evidence_count": evidence_count,
            "pending_verb": verb,
            "pending_route": route,
        }
        if route is None:
            merged.update(
                {
                    "terminated": True,
                    "status": "blocked",
                    "blocked_reason": f"{stage}:{mode}（step={step_id}）返回了 {verb!r}，"
                    "但计划没有对应的 on 路由"[:300],
                    "cursor": idx,
                }
            )
        await self._persist({**dict(s), **merged})
        return merged

    async def _advance(self, s: CaseState) -> dict:
        if s.get("terminated"):
            # _step 已经把本步判为 blocked（或已终止）：保持终态，不再前进
            return {
                "terminated": True,
                "status": s.get("status") or "blocked",
                "blocked_reason": s.get("blocked_reason"),
                "pending_route": None,
                "pending_verb": None,
                "trace": list(s.get("trace", [])),
            }
        plan = s["plan"]
        route = s.get("pending_route") or {}
        cursor = s["cursor"]
        step_status = dict(s.get("step_status", {}))
        outputs = dict(s.get("outputs", {}))
        completed = list(s.get("completed", []))
        loops = dict(s.get("loops", {}))
        trace = list(s.get("trace", []))
        status = s["status"]
        loop_count = int(s.get("review_loop_seq", 0))
        requested_focus = s.get("requested_focus")

        if "jump" in route:
            target = route["jump"]
            loops_limit = int(route.get("max_loops", cfg.max_review_loops))
            key = f"{plan['steps'][cursor]['id']}:{s.get('pending_verb') or 'jump'}"
            used = int(loops.get(key, 0))
            if used + 1 > loops_limit:  # 预算已用尽：这次跳转不会发生，计数也不加
                counters.inc("ci_review_loop_exhausted_total", step=key)
                return {
                    "terminated": True,
                    "status": "blocked",
                    "review_loop_seq": loop_count,
                    "jump_count": loop_count,
                    "loops": loops,
                    "blocked_reason": f"{key} 回炉超过 {loops_limit} 轮仍未通过",
                    "trace": [*trace, {"at": now_iso(), "event": f"loop_exhausted:{key}"}],
                }
            loops[key] = used + 1  # 只有真正发生的跳转才计数
            loop_count += 1
            tgt_idx = next((i for i, st in enumerate(plan["steps"]) if st["id"] == target), None)
            if tgt_idx is None:
                return {
                    "terminated": True,
                    "status": "blocked",
                    "blocked_reason": f"jump 目标 {target} 不存在",
                }
            trigger = outputs.get(plan["steps"][cursor]["id"]) or {}
            requested_focus = (
                "; ".join(str(x) for x in (trigger.get("requested_info") or []))[:400]
                or requested_focus
            )
            # 回炉：target 及其之后的输出/状态全部作废（reviewer 三道闸门按 step_id 存，逐个清）
            future_ids = {st["id"] for st in plan["steps"][tgt_idx:]}
            for sid in future_ids:
                outputs.pop(sid, None)
                step_status.pop(sid, None)
            completed = [sid for sid in completed if sid not in future_ids]
            status = route.get("set_status") or "triaged"
            log_with(
                log,
                "info",
                "plan_jump",
                case_id=s["case_id"],
                target=target,
                loop_key=key,
                loop_seq=loops[key],
                requested_focus=(requested_focus or "")[:120],
            )
            return {
                "cursor": tgt_idx,
                "outputs": outputs,
                "step_status": step_status,
                "completed": completed,
                "status": status,
                "loops": loops,
                "review_loop_seq": loop_count,
                "jump_count": loop_count,
                "requested_focus": requested_focus,
                "terminated": False,
                "pending_route": None,
                "pending_verb": None,
                "trace": [*trace, {"at": now_iso(), "event": f"jump:{target}:loop{loops[key]}"}],
            }

        if route.get("end"):
            return {
                "terminated": True,
                "status": route.get("set_status") or status,
                "loops": loops,
                "pending_route": None,
                "trace": [*trace, {"at": now_iso(), "event": "plan_end"}],
            }
        if "next" in route:
            nxt = next((i for i, st in enumerate(plan["steps"]) if st["id"] == route["next"]), None)
            if nxt is None:
                return {
                    "terminated": True,
                    "status": "blocked",
                    "blocked_reason": f"next 目标不存在: {route['next']}",
                }
            cursor = nxt
        else:
            cursor += 1
        if cursor >= len(plan["steps"]):
            return {
                "cursor": cursor,
                "terminated": True,
                "status": route.get("set_status") or "closed",
                "loops": loops,
                "pending_route": None,
                "trace": [*trace, {"at": now_iso(), "event": "plan_exhausted"}],
            }
        merged = {
            "cursor": cursor,
            "terminated": False,
            "pending_route": None,
            "pending_verb": None,
            "loops": loops,
            "trace": trace,
        }
        await self._persist({**dict(s), **merged})
        return merged

    async def _persist(self, s: CaseState, **extra) -> None:
        plan = s.get("plan") or {}
        progress = {
            "cursor": s.get("cursor", 0),
            "plan_steps": [
                {"id": st["id"], "stage": st["stage"], "mode": st["mode"]}
                for st in plan.get("steps", [])
            ],
            "step_status": s.get("step_status", {}),
            "attempts": s.get("attempts", {}),
            "completed": s.get("completed", []),
            "loops": s.get("loops", {}),
            "review_loop_seq": s.get("review_loop_seq", 0),
            "jump_count": s.get("jump_count", 0),
            "requested_focus": s.get("requested_focus"),
            "gen": s.get("gen", 1),
            "plan_id": s.get("plan_id"),
            "plan_source": s.get("plan_source"),
            "workflow_key": s.get("workflow_key"),
            "degraded": s.get("degraded", False),
            "updated_at": now_iso(),
        }
        progress.update(extra)
        await self.db.set_case_status(s["case_id"], s.get("status", "created"), progress=progress)

    async def _finalize(self, s: CaseState) -> dict:
        live = s.get("status") or "closed"
        reason = s.get("blocked_reason")
        violations = s.get("plan_violations") or []
        if violations:
            live, reason = "blocked", "计划校验失败: " + "; ".join(violations)[:250]
        # 终态词表收敛：closed（正常走完，closed_reason 记录最后活状态）/ closed_rejected / blocked
        status = live if live in ("blocked", "closed", "closed_rejected") else "closed"
        trace = [*list(s.get("trace", [])), {"at": now_iso(), "event": f"finalize:{status}:{live}"}]
        await self.gw.publish_event(
            rk=f"ci.event.case.{status}",
            correlation_id=s["case_id"],
            data={"status": status, "terminal_from": live, "blocked_reason": reason},
        )
        log_with(
            log,
            "info",
            "case_finalize",
            case_id=s["case_id"],
            status=status,
            terminal_from=live,
            blocked_reason=(reason or "")[:200],
        )
        return {
            "terminated": True,
            "status": status,
            "blocked_reason": reason,
            "terminal_from": live,
            "trace": trace,
        }

    # ------------------------------------------------------------- 落库
    async def _store_artifacts(
        self, s: CaseState, step_id: str, stage: str, mode: str, output: dict, status: str
    ) -> None:
        """把 Agent 的输出写进它对应的表：Agent 只写 evidence，业务表归编排层。"""
        case_id = s["case_id"]
        if stage == "reviewer":
            await self.db.save_review(
                case_id=case_id,
                output=output,
                loop_seq=int(s.get("review_loop_seq", 0)) + 1,
                gate=mode,
            )
        elif stage == "remediation":
            await self.db.save_remediation(case_id=case_id, output=output)
        elif stage == "reproduction":
            await self.db.save_verification(case_id=case_id, output=output)
        elif stage == "regression":
            await self.db.save_regression(case_id=case_id, output=output)
        elif stage == "ticketing":
            if output.get("ticket_status"):
                await self.db.save_ticket(
                    case_id=case_id,
                    backend=str(output.get("backend") or cfg.ticket_backend),
                    output=output,
                )
        elif stage == "reporting":
            if mode == "daily":
                day = output.get("day") or _today()
                metrics = output.get("metrics") or {}
                await self.db.save_daily_report(
                    day=day,
                    report_md=output.get("report_md") or "",
                    metrics=metrics,
                    cases=metrics.get("cases") or [],
                )
            else:
                # 阶段 9 把结论回写共享知识库（写权仍在编排层）
                for raw in output.get("kb_writes") or []:
                    await self.db.kb_write(
                        normalize_write({**raw, "case_id": raw.get("case_id") or case_id})
                    )
                await self.db.set_case_status(
                    case_id,
                    status,
                    case_report={
                        "report_md": (output.get("report_md") or "")[:4000],
                        "written_at": now_iso(),
                    },
                )

    @staticmethod
    def _check_output(stage: str, mode: str, output: dict, case_id: str) -> None:
        """出参软校验：不合契约只告警（Agent 侧已经校验过，这里防的是版本漂移）。"""
        try:
            import jsonschema

            jsonschema.validate(
                instance=dict(output), schema=validate_stage_contract(stage, "output")
            )
        except Exception as exc:
            counters.inc("ci_stage_output_violations_total", stage=stage, mode=mode)
            log_with(
                log,
                "warn",
                "stage_output_contract_violation",
                case_id=case_id,
                stage=stage,
                mode=mode,
                err=str(exc)[:300],
            )

    # ------------------------------------------------------------- 辅助
    @staticmethod
    def _progress(s: CaseState, outputs, attempts, step_status, trace) -> dict:
        return {
            "outputs": outputs,
            "attempts": attempts,
            "step_status": step_status,
            "trace": trace,
            "loops": s.get("loops", {}),
            "review_loop_seq": int(s.get("review_loop_seq", 0)),
            "jump_count": int(s.get("jump_count", 0)),
        }

    @staticmethod
    def _verb_for(stage: str, mode: str, output: dict) -> str:
        """把 Agent 输出翻成计划里的路由动词（计划只认这几个词）。"""
        if stage == "reviewer":
            verdict = str(output.get("verdict") or "approve")
            if mode == "gate1" and verdict == "approve":
                return str(output.get("decision") or "external")
            return verdict
        if stage == "regression" and mode == "check":
            return "regression" if output.get("is_regression") else "not_regression"
        return "done"

    @staticmethod
    def _route_for(raw: dict, verb: str) -> dict | None:
        """取这一步的路由；返回 None 表示「计划没写这个动词的路由」（视为计划缺陷）。"""
        on = raw.get("on") or {}
        if not on:  # 没写 on：顺序执行下一步
            return {}
        if verb in on:
            return on[verb] or {}
        if "done" in on:
            return on["done"] or {}
        return None

    @staticmethod
    def _latest(s: CaseState, stage: str, mode: str | None = None) -> dict:
        """按执行顺序取某个 stage（可指定 mode）最近一次成功的输出。"""
        steps = {st["id"]: st for st in (s.get("plan") or {}).get("steps", [])}
        outputs = s.get("outputs") or {}
        for sid in reversed(s.get("completed") or []):
            st = steps.get(sid)
            if not st or st["stage"] != stage:
                continue
            if mode and st["mode"] != mode:
                continue
            out = outputs.get(sid)
            if out is not None:
                return dict(out)
        return {}

    async def _build_input(self, s: CaseState, raw: dict) -> dict:
        stage, mode = raw["stage"], raw["mode"]
        obs = s.get("payload") or {}
        base = {
            "case_id": s["case_id"],
            "build": s.get("build"),
            "source": obs.get("source", "mock"),
            "job": s["job"],
        }
        if stage == "reporting":
            if mode == "case":
                return {"case_id": s["case_id"], "day": None}
            return {"case_id": None, "day": _today()}

        triage = self._latest(s, "triage")
        if stage == "triage":
            return {
                **base,
                "signal": s["signal"],
                "signature": s.get("signature"),
                "log_excerpt": obs.get("log_excerpt"),
                "context": obs.get("context") or {},
                "evidence_refs": obs.get("evidence_refs") or [],
                "requested_focus": s.get("requested_focus"),
            }

        # 其余阶段都以分诊结论为前提（2 号阶段）
        common = {
            **base,
            "rca": triage.get("rca") or "unknown",
            "component": triage.get("component"),
            "signature": triage.get("signature") or s.get("signature"),
            "key_frames": triage.get("key_frames") or [],
        }
        if stage == "reviewer":
            payload = {
                **common,
                "signal": s["signal"],
                "path": triage.get("path"),
                "confidence": triage.get("confidence"),
                "evidence_count": len(await self.db.evidence(s["case_id"])),
                "loop_seq": min(int(s.get("review_loop_seq", 0)) + 1, 5),
                "requested_focus": s.get("requested_focus"),
            }
            if mode == "gate2":
                payload["remediation"] = self._latest(s, "remediation")
                payload["reproduction"] = self._latest(s, "reproduction", "verify")
            elif mode == "gate3":
                payload["reproduction"] = self._latest(s, "reproduction", "evidence")
                payload["regression"] = self._latest(s, "regression")
                payload["ticketing"] = self._latest(s, "ticketing")
            return payload
        if stage == "remediation":
            return {
                **common,
                "kb_fix_refs": list(triage.get("kb_refs") or []),
                "requested_focus": s.get("requested_focus"),
            }
        if stage == "reproduction":
            payload = {**common, "requested_focus": s.get("requested_focus")}
            if mode == "verify":
                payload["remediation"] = self._latest(s, "remediation")
            return payload
        if stage == "regression":
            check = self._latest(s, "regression", "check")
            payload = {
                **common,
                "reproduction": self._latest(s, "reproduction", "evidence"),
                "requested_focus": s.get("requested_focus"),
            }
            if mode == "bisect":
                payload["baseline"] = check.get("baseline")
                payload["change_window"] = check.get("change_window")
            return payload
        if stage == "ticketing":
            return {
                **common,
                "signal": s["signal"],
                "path": triage.get("path"),
                "reproduction": self._latest(s, "reproduction", "evidence"),
                "regression": self._latest(s, "regression"),
                "duplicate_of": triage.get("duplicate_of"),
                "requested_focus": s.get("requested_focus"),
            }
        raise ValueError(f"未知 stage: {stage}")

    async def drain(self) -> None:
        if self.tasks:
            await asyncio.gather(*list(self.tasks), return_exceptions=True)


def _today() -> str:
    from iqe_orchestrator.common.dates import today_str

    return today_str()
