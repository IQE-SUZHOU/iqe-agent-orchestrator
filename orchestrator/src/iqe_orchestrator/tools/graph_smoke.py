#!/usr/bin/env python3
"""离线编排图自检：用假网关（canned StepResult）跑 LangGraph，验证路由/回炉/终态/落库。

不需要 RabbitMQ，也不需要 8 个 Agent —— 只验证编排层自身的正确性：
闸门 1 的 internal / external 双分支、5b 的非回归短路、三处回炉、
以及 reviews / remediations / verifications / regressions / tickets / kb_entries 的落库。
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
    """按 (stage, mode) 返回预制结果，并记录调用序列。"""

    def __init__(
        self,
        *,
        path: str = "internal",
        is_regression: bool = False,
        verdicts: dict[str, list[str]] | None = None,
        repro_status: str = "ok",
    ):
        self.path = path
        self.is_regression = is_regression
        self.verdicts = {k: list(v) for k, v in (verdicts or {}).items()}
        self.repro_status = repro_status
        self.calls: list[str] = []
        self.published: list[str] = []

    def _verdict(self, gate: str, default: str = "approve") -> str:
        seq = self.verdicts.get(gate) or []
        return seq.pop(0) if seq else default

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
        self.calls.append(f"{stage}:{mode}")
        out: dict = {}
        status = "ok"
        error = None
        if stage == "triage":
            out = {
                "case_id": case_id,
                "rca": "driver" if self.path == "internal" else "test",
                "path": self.path,
                "component": "os-staging-driver",
                "signature": "driver:crash:aaa111222bbb",
                "key_frames": ["f1"],
                "confidence": 0.88,
                "duplicate_of": None,
                "kb_refs": ["kb-1"],
                "evidence": ["ev-1", "ev-2"],
            }
        elif stage == "reviewer":
            verdict = self._verdict(mode)
            out = {
                "case_id": case_id,
                "gate": mode,
                "verdict": verdict,
                "decision": self.path if mode == "gate1" and verdict == "approve" else "none",
                "reason_code": "ok" if verdict == "approve" else "insufficient_evidence",
                "requested_info": [] if verdict == "approve" else ["补齐关键帧"],
                "checks": [{"id": "evidence_present", "pass": verdict == "approve"}],
                "quality_scores": {"overall": 0.9},
            }
        elif stage == "remediation":
            out = {
                "case_id": case_id,
                "fixed": True,
                "change_type": "script",
                "target": "ci/staging/driver.sh",
                "diff_summary": "重试 3 次",
                "risk": "low",
                "work_item_key": "ADO-1",
            }
        elif stage == "reproduction":
            if self.repro_status == "blocked":
                status, error = "blocked", {"code": "source_unavailable", "detail": "no machine"}
            elif self.repro_status == "timeout":
                status, error = "timeout", {"code": "timeout", "detail": "20s"}
            else:
                out = {
                    "case_id": case_id,
                    "mode": mode,
                    "reproducible": "yes",
                    "attempts": 1,
                    "fix_verified": True if mode == "verify" else None,
                    "new_evidence": ["ev-3"],
                }
        elif stage == "regression":
            if mode == "check":
                out = {
                    "case_id": case_id,
                    "mode": "check",
                    "is_regression": self.is_regression,
                    "baseline": "build-99",
                    "change_window": "99..100",
                }
            else:
                out = {
                    "case_id": case_id,
                    "mode": "bisect",
                    "is_regression": True,
                    "bisected": True,
                    "guilty_patch": {"commit": "abc123", "pr": "42", "confidence": 0.8},
                }
        elif stage == "ticketing":
            out = {
                "case_id": case_id,
                "backend": "mock",
                "ticket_status": "created",
                "external_key": "OST-1",
                "url": "https://tickets.local/OST-1",
                "duplicate_of": None,
                "priority": "P2",
                "assignee_team": "os-staging",
                "sections": [
                    {"key": "rca", "content": "驱动崩溃"},
                    {"key": "repro", "content": "重跑 3 次复现"},
                    {"key": "evidence", "content": "ev-1/ev-2"},
                ],
            }
        elif stage == "reporting":
            out = {
                "case_id": case_id,
                "day": None,
                "report_md": "## 案例报告\n内容",
                "metrics": {"steps": len(self.calls)},
                "kb_writes": (
                    [
                        {
                            "kind": "failure",
                            "component": "os-staging-driver",
                            "signature": "driver:crash:aaa111222bbb",
                            "summary": "驱动在 staging 崩溃",
                            "case_id": case_id,
                        }
                    ]
                    if mode == "case"
                    else []
                ),
            }
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

    print("场景 1：signal=none → 只做分类观测（2→9）")
    gw = FakeGateway()
    c = await run_case("none", "os-staging-test", None, gw)
    check(c["status"] == "closed", "终态 closed", c["blocked_reason"] or "")
    check(
        gw.calls == ["triage:classify", "reporting:case"], "只调用 triage/reporting", str(gw.calls)
    )
    check(
        c["progress"]["workflow_key"] == "s1_observe_only",
        "workflow=s1_observe_only",
        str(c["progress"]),
    )

    print("场景 2：error + internal 分支（闸门 1 → 4a → 5a → 闸门 2 → 9）")
    gw = FakeGateway(path="internal")
    c = await run_case("error", "os-staging-driver", "driver:crash:aaa", gw)
    check(
        gw.calls
        == [
            "triage:classify",
            "reviewer:gate1",
            "remediation:fix",
            "reproduction:verify",
            "reviewer:gate2",
            "reporting:case",
        ],
        "internal 主线",
        str(gw.calls),
    )
    check(c["status"] == "closed", "终态 closed")
    check(c["progress"]["workflow_key"] == "s2_full", "workflow=s2_full")
    check(any(e.startswith("ci.event.case.closed") for e in gw.published), "闭合事件已发布")
    internal_case = c

    print("场景 3：error + external 且有回归（闸门 1 → 4b → 5b → 6c → 7b → 闸门 3 → 9）")
    gw = FakeGateway(path="external", is_regression=True)
    c = await run_case("error", "os-staging-ext", "driver:crash:bbb", gw)
    check(
        gw.calls
        == [
            "triage:classify",
            "reviewer:gate1",
            "reproduction:evidence",
            "regression:check",
            "regression:bisect",
            "ticketing:create",
            "reviewer:gate3",
            "reporting:case",
        ],
        "external 主线（含 6c）",
        str(gw.calls),
    )
    check(c["status"] == "closed", "终态 closed", c["blocked_reason"] or "")

    print("场景 4：非回归 → 跳过 6c 直接建单")
    gw = FakeGateway(path="external", is_regression=False)
    c = await run_case("error", "os-staging-noreg", "driver:crash:ccc", gw)
    check("regression:bisect" not in gw.calls, "跳过二分", str(gw.calls))
    check(
        gw.calls
        == [
            "triage:classify",
            "reviewer:gate1",
            "reproduction:evidence",
            "regression:check",
            "ticketing:create",
            "reviewer:gate3",
            "reporting:case",
        ],
        "非回归主线",
        str(gw.calls),
    )

    print("场景 5：闸门 1 needs_info → 回炉分诊一次")
    gw = FakeGateway(path="internal", verdicts={"gate1": ["needs_info", "approve"]})
    c = await run_case("error", "os-staging-g1loop", "driver:crash:ddd", gw)
    check(
        gw.calls[:4] == ["triage:classify", "reviewer:gate1", "triage:classify", "reviewer:gate1"],
        "回到 triage 重跑",
        str(gw.calls),
    )
    check(
        c["status"] == "closed" and c["progress"]["review_loop_seq"] == 1,
        "回炉 1 轮并闭合",
        str(c["progress"]),
    )

    print("场景 6：闸门 2 rework → 回炉 4a")
    gw = FakeGateway(path="internal", verdicts={"gate2": ["rework", "approve"]})
    c = await run_case("error", "os-staging-g2loop", "driver:crash:eee", gw)
    check(
        gw.calls
        == [
            "triage:classify",
            "reviewer:gate1",
            "remediation:fix",
            "reproduction:verify",
            "reviewer:gate2",
            "remediation:fix",
            "reproduction:verify",
            "reviewer:gate2",
            "reporting:case",
        ],
        "rework 后重跑 4a/5a",
        str(gw.calls),
    )

    print("场景 7：闸门 3 needs_info → 回炉建单")
    gw = FakeGateway(path="external", verdicts={"gate3": ["needs_info", "approve"]})
    c = await run_case("error", "os-staging-g3loop", "driver:crash:fff", gw)
    check(gw.calls.count("ticketing:create") == 2, "建单跑两次", str(gw.calls))
    check(c["status"] == "closed", "终态 closed", c["blocked_reason"] or "")

    print("场景 8：回炉耗尽 → blocked")
    gw = FakeGateway(path="internal", verdicts={"gate2": ["rework"] * 5})
    c = await run_case("error", "os-staging-stuck", "driver:crash:ggg", gw)
    check(
        c["status"] == "blocked" and "回炉" in (c["blocked_reason"] or ""),
        "耗尽后 blocked",
        c["blocked_reason"] or "",
    )
    check("reporting:case" not in gw.calls, "不再执行 reporting", str(gw.calls))

    print("场景 9：闸门 1 reject → closed_rejected")
    gw = FakeGateway(path="internal", verdicts={"gate1": ["reject"]})
    c = await run_case("error", "os-staging-reject", "driver:crash:hhh", gw)
    check(c["status"] == "closed_rejected", "终态 closed_rejected", c["status"])
    check("remediation:fix" not in gw.calls, "reject 不走 4a", str(gw.calls))

    print("场景 10：复现被 blocked → case blocked")
    gw = FakeGateway(path="external", repro_status="blocked")
    c = await run_case("error", "os-staging-blocked", "driver:crash:iii", gw)
    check(c["status"] == "blocked", "blocked 终态", c["status"])
    check(
        gw.calls == ["triage:classify", "reviewer:gate1", "reproduction:evidence"],
        "停止下游",
        str(gw.calls),
    )

    print("场景 11：复现超时重试两次 → blocked")
    gw = FakeGateway(path="external", repro_status="timeout")
    c = await run_case("error", "os-staging-timeout", "driver:crash:jjj", gw)
    check(
        gw.calls
        == [
            "triage:classify",
            "reviewer:gate1",
            "reproduction:evidence",
            "reproduction:evidence",
        ],
        "重试一次",
        str(gw.calls),
    )
    check(
        c["status"] == "blocked" and "timeout" in (c["blocked_reason"] or "").lower(),
        "超时后 blocked",
        c["blocked_reason"] or "",
    )

    print("场景 12：test:* 家族 → s3（无复现，5b 直接承担取证）")
    gw = FakeGateway(path="external", is_regression=False)
    c = await run_case("error", "os-staging-test-a", "test:assert-fail:111", gw)
    check(
        c["progress"]["workflow_key"] == "s3_test_no_repro",
        "命中 s3",
        str(c["progress"]["workflow_key"]),
    )
    check("reproduction:evidence" not in gw.calls, "跳过 4b", str(gw.calls))

    print("场景 13：注册表未命中 → LLM 兜底计划")
    gw = FakeGateway(path="internal")
    c = await run_case("error", "llm-plan-demo-job", "driver:crash:333", gw)
    check(c["plan_source"] == "llm", "plan_source=llm", str(c["plan_source"]))
    check(c["status"] == "closed", "终态 closed", c["blocked_reason"] or "")

    print("场景 14：落库完整性（internal 案例）")
    prog = internal_case["progress"]
    check(bool(prog.get("plan_steps")), "progress 记录了计划步骤", str(prog)[:200])
    check(
        prog.get("step_status", {}).get("g1") == "done"
        and prog.get("step_status", {}).get("g2") == "done",
        "三道闸门按 step_id 记状态",
        str(prog.get("step_status")),
    )
    plan = await db.get_plan(internal_case["case_id"])
    check(plan is not None and plan["yaml_sha256"], "计划封存记录")
    check(store.read(internal_case["case_id"], plan["plan_id"]) is not None, "计划文件可读")
    async with db.pool.acquire() as conn:
        gates = [
            r["gate"]
            for r in await conn.fetch(
                "select gate from reviews where case_id=$1 order by id", internal_case["case_id"]
            )
        ]
        fix = await conn.fetchrow(
            "select change_type, target from remediations where case_id=$1",
            internal_case["case_id"],
        )
        ver = await conn.fetchrow(
            "select mode, fix_verified from verifications where case_id=$1",
            internal_case["case_id"],
        )
        kb_n = await conn.fetchval(
            "select count(*) from kb_entries where case_id=$1", internal_case["case_id"]
        )
    check(gates == ["gate1", "gate2"], "reviews 记了两道闸门", str(gates))
    check(fix is not None and fix["change_type"] == "script", "remediations 已落库", str(fix))
    check(ver is not None and ver["mode"] == "verify", "verifications 已落库", str(ver))
    check(kb_n == 1, "kb_entries 回写 1 条", str(kb_n))

    await ckpt_pool.close()
    await db.close()
    print(f"\n=== graph_smoke: PASS={PASS} FAIL={FAIL} ===")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
