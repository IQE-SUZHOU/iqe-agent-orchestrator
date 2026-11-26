"""LangGraph 编排图：resolve_plan → validate_plan → step → advance → finalize。

关键点：图结构固定，具体走哪些段完全由 plan.steps + on 规则决定；执行期不调用模型。
Dispatch–Await 在 step 节点内完成（发命令 → 等 Future → 超时/重试/blocked）。
"""

from __future__ import annotations

import asyncio
import logging
from typing import TypedDict

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import END, START, StateGraph

from iqe_orchestrator.common.config import cfg
from iqe_orchestrator.common.db import DB
from iqe_orchestrator.common.envelope import now_iso, ulid
from iqe_orchestrator.common.logs import counters, log_with

from .dispatch import BusGateway
from .plan import LLMPlanner, Plan, PlanResolver, PlanStore, plan_from_dict, validate_plan

log = logging.getLogger("orchestrator.graph")

STAGE_STATUS = {
    "triage": "triaged",
    "reproduction": "reproduced",
    "ticket": "ticketed",
    "review": "reviewed",
    "reporting": "reported",
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
    pending_verb: str | None
    pending_route: dict | None
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
            "review_loop_seq": int(progress.get("review_loop_seq", 0) or 0),
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
            key = "s2_full" if signal == "error" else "s1_lite"
            plan = _builtin(key, signal)
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
            key = "s2_full" if signal == "error" else "s1_lite"
            plan = _builtin(key, signal)
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
            payload = await self._build_input(s, stage, mode)
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
                {"at": now_iso(), "event": f"{stage}:{step_id}:attempt{attempt}:{res.status}"}
            )
            if res.status == "ok":
                outputs[stage] = res.output
                verb = self._verb_for(stage, res.output)
                step_status[step_id] = "done"
                status = (raw.get("on", {}).get(verb, {}) or {}).get("set_status") or STAGE_STATUS[
                    stage
                ]
                break
            if res.status in ("retryable", "timeout") and attempt < max_attempts:
                log_with(
                    log,
                    "warn",
                    "step_retry",
                    case_id=s["case_id"],
                    stage=stage,
                    step_id=step_id,
                    attempt=attempt,
                    reason=(res.error or {}).get("code"),
                )
                continue
            step_status[step_id] = "blocked"
            counters.inc("ci_steps_blocked_total", stage=stage)
            merged = {
                **self._progress(s, outputs, attempts, step_status, trace),
                "terminated": True,
                "status": "blocked",
                "cursor": idx,
                "blocked_reason": f"{stage} 第 {attempt} 次仍失败: {(res.error or {}).get('code')} "
                f"{(res.error or {}).get('detail', '')}"[:300],
                "pending_verb": None,
                "pending_route": None,
            }
            await self._persist({**dict(s), **merged})
            return merged

        evidence_count = len(await self.db.evidence(s["case_id"]))
        if stage == "review" and outputs.get("review", {}).get("verdict"):
            await self.db.save_review(
                case_id=s["case_id"],
                output=outputs["review"],
                loop_seq=int(s.get("review_loop_seq", 0) or 0) + 1,
            )
        if stage == "ticket" and outputs.get("ticket", {}).get("ticket_status"):
            await self.db.save_ticket(
                case_id=s["case_id"], backend=cfg.ticket_backend, output=outputs["ticket"]
            )
        if stage == "reporting":
            rep = outputs.get("reporting") or {}
            if mode == "daily":
                day = rep.get("day") or _today()
                await self.db.save_daily_report(
                    day=day,
                    report_md=rep.get("report_md", ""),
                    metrics=rep.get("metrics", {}),
                    cases=rep.get("metrics", {}).get("cases", []),
                )
            else:
                await self.db.set_case_status(
                    s["case_id"],
                    s["status"],
                    case_report={
                        "report_md": (rep.get("report_md") or "")[:4000],
                        "written_at": now_iso(),
                    },
                )
        await self.gw.publish_event(
            rk=f"ci.event.case.{status}",
            correlation_id=s["case_id"],
            data={"stage": stage, "step_id": step_id, "verb": verb},
        )
        route = (raw.get("on") or {}).get(verb or "", {}) or (raw.get("on") or {}).get("done", {})
        log_with(
            log,
            "info",
            "step_ok",
            case_id=s["case_id"],
            stage=stage,
            step_id=step_id,
            verb=verb,
            evidence_count=evidence_count,
        )
        merged = {
            **self._progress(s, outputs, attempts, step_status, trace),
            "status": status,
            "evidence_count": evidence_count,
            "pending_verb": verb,
            "pending_route": route,
        }
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
        trace = list(s.get("trace", []))
        status = s["status"]
        review_loop = int(s.get("review_loop_seq", 0))
        jump_count = int(s.get("jump_count", 0))
        requested_focus = s.get("requested_focus")

        if "jump" in route:
            target = route["jump"]
            loops = int(route.get("max_loops", cfg.max_review_loops))
            if review_loop + 1 > loops:  # 预算已用尽：这次跳转不会发生，计数也不加
                counters.inc("ci_review_loop_exhausted_total")
                return {
                    "terminated": True,
                    "status": "blocked",
                    "review_loop_seq": review_loop,
                    "jump_count": jump_count,
                    "blocked_reason": f"复核回炉超过 {loops} 轮仍未通过",
                    "trace": [*trace, {"at": now_iso(), "event": "review_loop_exhausted"}],
                }
            review_loop += 1  # 只有真正发生的跳转才计数
            jump_count += 1
            tgt_idx = next((i for i, st in enumerate(plan["steps"]) if st["id"] == target), None)
            if tgt_idx is None:
                return {
                    "terminated": True,
                    "status": "blocked",
                    "blocked_reason": f"jump 目标 {target} 不存在",
                }
            for st in plan["steps"][tgt_idx + 1 :]:
                outputs.pop(st["stage"], None)
                step_status.pop(st["id"], None)
            review = outputs.get("review") or {}
            requested_focus = "; ".join(review.get("requested_info", []))[:400] or requested_focus
            status = route.get("set_status") or "triaged"
            log_with(
                log,
                "info",
                "plan_jump",
                case_id=s["case_id"],
                target=target,
                review_loop=review_loop,
                requested_focus=(requested_focus or "")[:120],
            )
            return {
                "cursor": tgt_idx,
                "outputs": outputs,
                "step_status": step_status,
                "status": status,
                "review_loop_seq": review_loop,
                "jump_count": jump_count,
                "requested_focus": requested_focus,
                "terminated": False,
                "pending_route": None,
                "pending_verb": None,
                "trace": [*trace, {"at": now_iso(), "event": f"jump:{target}:loop{review_loop}"}],
            }

        if route.get("end"):
            return {
                "terminated": True,
                "status": route.get("set_status") or status,
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
                "pending_route": None,
                "trace": [*trace, {"at": now_iso(), "event": "plan_exhausted"}],
            }
        merged = {
            "cursor": cursor,
            "terminated": False,
            "pending_route": None,
            "pending_verb": None,
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
            "review_loop_seq": s.get("review_loop_seq", 0),
            "jump_count": s.get("jump_count", 0),
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

    # ------------------------------------------------------------- 辅助
    @staticmethod
    def _progress(s: CaseState, outputs, attempts, step_status, trace) -> dict:
        return {
            "outputs": outputs,
            "attempts": attempts,
            "step_status": step_status,
            "trace": trace,
            "review_loop_seq": int(s.get("review_loop_seq", 0)),
            "jump_count": int(s.get("jump_count", 0)),
        }

    @staticmethod
    def _verb_for(stage: str, output: dict) -> str:
        if stage == "triage":
            return "classified_error" if output.get("repro_eligible") else "classified_none"
        if stage == "review":
            return output.get("verdict") or "approve"
        return "done"

    async def _build_input(self, s: CaseState, stage: str, mode: str) -> dict:
        outputs = s.get("outputs", {})
        obs = s.get("payload") or {}
        triage = outputs.get("triage") or {}
        repro = outputs.get("reproduction") or {}
        ticket = outputs.get("ticket") or {}
        base = {
            "case_id": s["case_id"],
            "build": s.get("build"),
            "source": obs.get("source", "mock"),
            "job": s["job"],
        }
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
        if stage == "reproduction":
            return {
                **base,
                "rca": triage.get("rca", "unknown"),
                "signature": triage.get("signature") or s.get("signature"),
                "key_frames": triage.get("key_frames") or [],
                "requested_focus": s.get("requested_focus"),
            }
        if stage == "ticket":
            return {
                **base,
                "signal": s["signal"],
                "signature": triage.get("signature") or s.get("signature"),
                "rca": triage.get("rca", "unknown"),
                "reproducible": repro.get("reproducible"),
                "duplicate_of": triage.get("duplicate_of"),
                "requested_focus": s.get("requested_focus"),
            }
        if stage == "review":
            return {
                **base,
                "signal": s["signal"],
                "signature": triage.get("signature"),
                "rca": triage.get("rca", "unknown"),
                "confidence": triage.get("confidence"),
                "key_frames": triage.get("key_frames") or [],
                "reproducible": repro.get("reproducible"),
                "ticket_status": ticket.get("ticket_status"),
                "ticket_sections": ticket.get("sections") or [],
                "duplicate_of": ticket.get("duplicate_of") or triage.get("duplicate_of"),
                "evidence_count": int(s.get("evidence_count", 0)),
                "repro_attempted": "reproduction" in outputs,
                "loop_seq": int(s.get("review_loop_seq", 0)) + 1,
                "requested_focus": s.get("requested_focus"),
            }
        if stage == "reporting":
            return {
                "case_id": s["case_id"] if mode == "case" else None,
                "day": None if mode == "case" else _today(),
            }
        raise ValueError(f"未知 stage: {stage}")

    async def drain(self) -> None:
        if self.tasks:
            await asyncio.gather(*list(self.tasks), return_exceptions=True)


def _builtin(key: str, signal: str) -> Plan:
    from .plan import builtin_plan

    return builtin_plan(key, signal)


def _today() -> str:
    from iqe_orchestrator.common.dates import today_str

    return today_str()
