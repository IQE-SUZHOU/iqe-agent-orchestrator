#!/usr/bin/env python3
"""离线渲染 4 个 Web UI 页面（不需要 broker，只需要 Postgres）。

为什么要这个工具：UI 页面出过两次 500，纯 JSON API 测试都发现不了——
  1) starlette 1.x 删掉了 TemplateResponse(name, context) 旧签名 → 全站 500；
  2) Jinja 里 `dlq.items` 会先命中 dict.items **方法**（不是键 "items"）→ /ui/health 500。
所以每次改模板 / 换 fastapi 版本，先跑这个。

用法：
  PG_DSN=... python tools/ui_render_check.py            # 用当前库里的真实数据
  PG_DSN=... python tools/ui_render_check.py --empty    # 再用空库渲染一遍（边界：空表 / NULL 聚合）
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from starlette.requests import Request  # noqa: E402

from agentkit.db import DB  # noqa: E402
from orchestrator import app as A  # noqa: E402
from orchestrator.plan import PlanStore  # noqa: E402

FAILS: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'} {name}" + (f"  <- {detail[:200]}" if not ok and detail else ""))
    if not ok:
        FAILS.append(name)


def make_request(path: str) -> Request:
    return Request({"type": "http", "http_version": "1.1", "method": "GET", "path": path,
                    "raw_path": path.encode(), "query_string": b"", "headers": [],
                    "scheme": "http", "server": ("test", 80), "client": ("127.0.0.1", 1),
                    "root_path": ""})


def _dt(y:int=2026, mo:int=11, d:int=25, h:int=10, mi:int=0) -> datetime:
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


def fake_dlq(with_rows: bool) -> dict:
    return {"items": [{"id": 1, "stage": "review", "idempotency_key": "case:x1:1",
                       "case_id": "case", "reason": "contract_violation: mode", "original_routing_key": "ci.cmd.review.run",
                       "redelivered": False, "body": '{"mode":"bogus"}', "created_at": _dt()}],
            "count": 1} if with_rows else {"items": [], "count": 0}


async def main() -> None:
    empty = "--empty" in sys.argv
    db = await DB.create()
    A.RT.db = db
    A.RT.store = PlanStore()

    # 页面清单：(名字, 路由函数, 路径, 必须出现在 HTML 里的片段)
    pages = [
        ("index", A.ui_index, "/", ["Case 列表", "计划进度", "Workflow"]),
        ("plans", A.ui_plans, "/ui/plans", ["Workflow 注册表", "已封存计划"]),
        ("health", A.ui_health, "/ui/health", ["Stage 执行情况", "DLQ 归档"]),
    ]

    original_api_dlq = A.api_dlq
    for label in (("有数据", False), ("空数据", True)) if not empty else (("空数据", True),):
        async def _dlq(limit: int, _rows: bool = (not label[1])) -> dict:   # 必须是协程：ui_health 会 await
            return fake_dlq(_rows)
        A.api_dlq = _dlq
        for name, fn, path, markers in pages:
            try:
                resp = await fn(request=make_request(path))
                html = resp.body.decode("utf-8", "replace")
                ok = resp.status_code == 200 and all(m in html for m in markers)
                check(f"UI {name}（{label[0]}）渲染 200 且含 {markers}", ok, f"status={resp.status_code}")
            except Exception as exc:  # noqa: BLE001
                check(f"UI {name}（{label[0]}）渲染 200 且含 {markers}", False, f"{type(exc).__name__}: {exc}")
        A.api_dlq = original_api_dlq

    # 详情页：库里没有 case 时用合成数据（覆盖 plan/trace/review/ticket/evidence 全部区块）
    cases = await db.list_cases(limit=1)
    if cases:
        case_id = cases[0]["case_id"]
    else:
        case_id = "SYNTHETIC-CASE"
        A.RT.db = _SynthDB(db)
    try:
        resp = await A.ui_case(request=make_request(f"/ui/cases/{case_id}"), case_id=case_id)
        html = resp.body.decode("utf-8", "replace")
        markers = ["编排计划", "执行轨迹", "证据", "复核"]
        check(f"UI case（{'真实 case' if cases else '合成 case'}）渲染 200 且含 {markers}",
              resp.status_code == 200 and all(m in html for m in markers), f"status={resp.status_code}")
    except Exception as exc:  # noqa: BLE001
        check("UI case 渲染 200", False, f"{type(exc).__name__}: {exc}")

    await db.pool.close()
    print("\n" + ("结果：全部通过" if not FAILS else f"结果：FAIL {len(FAILS)} 项\n  " + "\n  ".join(FAILS)))
    return 0 if not FAILS else 1


class _SynthDB:
    """库里没有数据时给详情页喂一份形状正确的假数据（列名与 001_init.sql 一致）。"""

    def __init__(self, real: DB):
        self.pool = real.pool

    async def get_case(self, case_id: str) -> dict:
        return {"case_id": case_id, "scan_id": "scan-1", "run_id": "run-1", "signal": "error",
                "job": "os-staging-test", "build": 1200, "source": "staging", "status": "closed",
                "signature": "driver:crash", "scenario_hint": "new_failure", "plan_source": "workflow",
                "payload": {"observation": {"job": "os-staging-test", "build": 1200, "result": "FAIL",
                                            "signature": "driver:crash", "signal": "error",
                                            "promoted_reason": "new_failure_signature",
                                            "context": {"family": "driver:crash", "branch": "master"}},
                            "log_excerpt": "kernel BUG at mm.o:1234", "evidence_count": 1},
                "blocked_reason": None, "created_at": _dt(),
                "progress": {"workflow_key": "s2_full", "cursor": 5, "step_status": {"t1": "done", "r1": "done"},
                             "attempts": {"t1": 1, "r1": 2}, "review_loop_seq": 1, "jump_count": 1}}

    async def get_plan(self, case_id: str) -> dict:
        return {"case_id": case_id, "plan_id": "plan-1", "workflow_key": "s2_full", "source": "workflow",
                "degraded": False, "rationale": "注册表命中 s2_full", "yaml_sha256": "abc",
                "plan": {"key": "s2_full", "steps": [{"id": "t1", "stage": "triage", "mode": "full"},
                                                      {"id": "r1", "stage": "reproduction", "mode": "run",
                                                       "timeout_s": 20}]},
                "created_at": _dt(mi=1)}

    async def trace(self, case_id: str) -> list[dict]:
        return [{"id": 1, "case_id": case_id, "scan_id": "scan-1", "run_id": "run-1", "step_id": "t1",
                 "stage": "triage", "mode": "full", "attempt": 1, "status": "done", "idempotency_key": "k",
                 "latency_ms": 812, "error": None, "output": {"rca": "driver", "confidence": 0.82},
                 "started_at": _dt(mi=2), "finished_at": _dt(mi=3)}]

    async def evidence(self, case_id: str) -> list[dict]:
        return [{"id": 1, "case_id": case_id, "scan_id": "scan-1", "stage": "monitor", "kind": "log_excerpt",
                 "uri": "db://evidence/1", "meta": {"note": "首个失败"}, "excerpt": "AssertionError",
                 "created_at": _dt()}]

    # tickets / reviews 走真实 pool（这两张表为空时形状也要能渲染）


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
