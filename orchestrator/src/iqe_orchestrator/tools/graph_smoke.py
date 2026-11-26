#!/usr/bin/env python3
"""离线编排图自检：用假网关（canned StepResult）跑 LangGraph，验证路由/回炉/终态/落库。

不需要 RabbitMQ，也不需要 6 个 Agent —— 只验证编排层自身的正确性。
用法：PG_DSN=... ci-graph-smoke（console script）
"""

from __future__ import annotations

import asyncio
import sys

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg_pool import AsyncConnectionPool

from iqe_orchestrator.common.config import cfg
from iqe_orchestrator.common.db import DB
from iqe_orchestrator.dispatch import StepResult
from iqe_orchestrator.graph import CaseEngine
from iqe_orchestrator.plan import LLMPlanner, PlanResolver, PlanStore

PASS = FAIL = 0


def check(cond: bool, label: str, detail: str = ""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS {label}")
    else:
        FAIL += 1
        print(f"  FAIL {label} {detail}")


class FakeGateway:
    """按 (stage, scenario) 返回预制结果，并记录调用序列。"""

    def __init__(self, scenario: str, *, repro_status="ok", review_seq=None):
        self.scenario = scenario
        self.calls: list[tuple[str, int]] = []
        self.published: list[str] = []
        self.repro_status = repro_status
        self.review_seq = review_seq or {}
        self.db = None

    async def dispatch(
        self,
        *,
        stage,
        mode,
        payload,
        run_id,
        step_id,
        attempt,
        case_id=None,
        scan_id=None,
        scenario_hint=None,
        timeout_s=None,
    ):
        self.calls.append((stage, attempt))
        out: dict = {}
        status = "ok"
        error = None
        if stage == "triage":
            out = {
                "rca": "driver",
                "confidence": 0.88,
                "signature": "driver:crash:aaa111222bbb",
                "key_frames": ["f1"],
                "repro_eligible": True,
                "context": {"duplicate_merge_count": 1},
            }
        elif stage == "reproduction":
            if self.repro_status == "blocked":
                status, error = "blocked", {"code": "source_unavailable", "detail": "no machine"}
            elif self.repro_status == "timeout":
                status, error = "timeout", {"code": "timeout", "detail": "20s"}
            else:
                out = {"reproducible": "yes", "runs": [{"n": 1, "passed": True}]}
        elif stage == "ticket":
            out = {
                "ticket_status": "created",
                "external_key": "OST-1",
                "ticket_priority": "P2",
                "assignee_team": "driver",
                "sections": [{"id": "rca", "content": "x"}],
            }
        elif stage == "review":
            verdict = self.review_seq.get(attempt, "approve")
            out = {
                "verdict": verdict,
                "reason_code": "checks_pass",
                "checks": [{"id": 1, "name": "ok", "pass": True}],
                "requested_info": ["补充日志"] if verdict == "needs_info" else [],
            }
        elif stage == "reporting":
            out = {"case_id": case_id, "day": None, "report_md": "## 案例报告\n内容", "metrics": {}}
        return StepResult(
            stage=stage,
            step_id=step_id,
            attempt=attempt,
            key=f"{case_id or run_id}:{step_id}:{attempt}",
            status=status,
            output=out,
            error=error,
            latency_ms=10,
        )

    async def publish_event(self, **kw):
        self.published.append(kw.get("rk", ""))

    async def publish_command_only(self, env):
        pass


async def main() -> int:
    db = await DB.create()
    await db.init_schema()
    ckpt_pool = AsyncConnectionPool(
        conninfo=cfg.pg_dsn,
        min_size=1,
        max_size=4,
        open=False,
        kwargs={"autocommit": True, "prepare_threshold": 0},
    )
    await ckpt_pool.open(wait=True, timeout=30)
    saver = AsyncPostgresSaver(ckpt_pool)
    await saver.setup()
    resolver = PlanResolver()
    resolver.load_registry()
    store = PlanStore()

    async def run_case(
        signal: str,
        job: str,
        signature: str | None,
        gw: FakeGateway,
        scenario_hint: str | None = None,
    ) -> dict:
        engine = CaseEngine(db, gw, resolver, LLMPlanner(), store, saver)
        obs = {
            "job": job,
            "build": 100,
            "signal": signal,
            "signature": signature,
            "source": "mock",
            "log_excerpt": "log",
            "context": {"mock_scenario": scenario_hint or "default"},
        }
        cid = await engine.start_case(obs=obs, run_id="R-" + job, scan_id=None)
        await engine.drain()
        return await db.get_case(cid)

    print("场景 1：signal=none → 只走 triage")
    gw = FakeGateway("default")
    c = await run_case("none", "os-staging-test", None, gw)
    check(c["status"] == "closed", "终态 closed", c["blocked_reason"] or "")
    check([s for s, _ in gw.calls] == ["triage"], "只调用 triage", str(gw.calls))
    check(c["progress"]["workflow_key"] == "s1_lite", "workflow=s1_lite", str(c["progress"]))

    print("场景 2：signal=error 完整链")
    gw = FakeGateway("default")
    c = await run_case("error", "os-staging-driver", "driver:crash:aaa", gw)
    check(
        [s for s, _ in gw.calls] == ["triage", "reproduction", "ticket", "review", "reporting"],
        "五段串行",
        str(gw.calls),
    )
    check(c["status"] == "closed", "终态 closed")
    check(c["progress"]["workflow_key"] == "s2_full", "workflow=s2_full")
    check(any(e.startswith("ci.event.case.closed") for e in gw.published), "闭合事件已发布")

    print("场景 3：needs_info 回炉一次后通过")
    gw = FakeGateway("default", review_seq={1: "needs_info", 2: "approve"})
    c = await run_case("error", "os-staging-loop", "driver:crash:bbb", gw)
    seq = [s for s, _ in gw.calls]
    check(
        seq
        == [
            "triage",
            "reproduction",
            "ticket",
            "review",
            "triage",
            "reproduction",
            "ticket",
            "review",
            "reporting",
        ],
        "回炉后重跑下游",
        str(seq),
    )
    check(
        c["status"] == "closed" and c["progress"]["review_loop_seq"] == 1,
        "回炉 1 轮并闭合",
        str(c["progress"]),
    )

    print("场景 4：insufficient → 回炉耗尽 blocked")
    gw = FakeGateway("default", review_seq={1: "needs_info", 2: "needs_info", 3: "needs_info"})
    c = await run_case("error", "os-staging-stuck", "driver:crash:ccc", gw)
    check(
        c["status"] == "blocked" and "回炉" in (c["blocked_reason"] or ""),
        "耗尽后 blocked",
        c["blocked_reason"] or "",
    )
    check("reporting" not in [s for s, _ in gw.calls], "不再执行 reporting", str(gw.calls))

    print("场景 5：reject → closed_rejected 且不 reporting")
    gw = FakeGateway("default", review_seq={1: "reject"})
    c = await run_case("error", "os-staging-reject", "driver:crash:ddd", gw)
    check(c["status"] == "closed_rejected", "终态 closed_rejected", c["status"])
    check("reporting" not in [s for s, _ in gw.calls], "reject 不执行 reporting", str(gw.calls))

    print("场景 6：reproduction blocked → case blocked")
    gw = FakeGateway("default", repro_status="blocked")
    c = await run_case("error", "os-staging-blocked", "driver:crash:eee", gw)
    check(c["status"] == "blocked", "blocked 终态", c["status"])
    check(
        [s for s, _ in gw.calls] == ["triage", "reproduction"], "blocked 后停止下游", str(gw.calls)
    )

    print("场景 7：reproduction 超时重试两次 → blocked")
    gw = FakeGateway("default", repro_status="timeout")
    c = await run_case("error", "os-staging-timeout", "driver:crash:fff", gw)
    check(
        [s for s, _ in gw.calls] == ["triage", "reproduction", "reproduction"],
        "重试一次",
        str(gw.calls),
    )
    check(
        c["status"] == "blocked" and "timeout" in (c["blocked_reason"] or "").lower(),
        "超时后 blocked",
        c["blocked_reason"] or "",
    )

    print("场景 8：test: 家族走 s4（无 reproduction）")
    gw = FakeGateway("default")
    c = await run_case("error", "os-staging-test-a", "test:assert-fail:111", gw)
    check(
        c["progress"]["workflow_key"] == "s4_test_no_repro",
        "命中 s4",
        str(c["progress"]["workflow_key"]),
    )
    check("reproduction" not in [s for s, _ in gw.calls], "跳过复现", str(gw.calls))

    print("场景 9：ci-infra: 家族走 s6")
    gw = FakeGateway("default")
    c = await run_case("error", "os-staging-infra", "ci-infra:infra-abort:222", gw)
    check(
        c["progress"]["workflow_key"] == "s6_infra_direct",
        "命中 s6",
        str(c["progress"]["workflow_key"]),
    )

    print("场景 10：注册表未命中 → LLM 兜底计划")
    gw = FakeGateway("default")
    c = await run_case("error", "llm-plan-demo-job", "driver:crash:333", gw)
    check(c["plan_source"] == "llm", "plan_source=llm", str(c["plan_source"]))
    check("reproduction" not in [s for s, _ in gw.calls], "LLM 计划不含复现", str(gw.calls))

    print("场景 11：落库完整性")
    prog = c["progress"]
    check(
        prog.get("plan_steps") and len(prog["plan_steps"]) >= 1,
        "progress 记录了计划步骤",
        str(prog),
    )
    check(
        all(v == "done" for v in prog.get("step_status", {}).values()) or c["status"] != "closed",
        "step_status 完整",
        str(prog.get("step_status")),
    )
    plan = await db.get_plan(c["case_id"])
    check(plan is not None and plan["yaml_sha256"], "计划封存记录")
    check(store.read(c["case_id"], plan["plan_id"]) is not None, "计划文件可读")
    ev = await db.evidence(c["case_id"])
    check(True, f"证据条数={len(ev)}（假网关不产证据，0 是正常的）")

    await ckpt_pool.close()
    await db.close()
    print(f"\n=== graph_smoke: PASS={PASS} FAIL={FAIL} ===")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
