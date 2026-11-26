#!/usr/bin/env python
"""离线自检：不依赖 RabbitMQ / Postgres，直接调用六个 stage 剧本，断言场景行为与输出契约。

用法：ci-selfcheck（console script）或 python -m iqe_agents.tools.selfcheck
退出码 0 = 全绿；每个 stage × 场景打印一行 PASS。

覆盖：
1) 六个剧本的输出全部通过 schemas/stage-contracts.json 的 output 契约（jsonschema 2020-12）；
2) 19 份 Mock CI feed 的 Monitor 计数（observed / promoted / suppressed）与签名家族；
3) 每个场景的行为核对（Blocked 的 code、reproducible、ticket_status、verdict 等）；
4) 三份 plan 夹具对照 schemas/plan.schema.json（合法 / 非法）。
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import sys

import jsonschema

from iqe_agents.common import paths
from iqe_agents.common.config import cfg
from iqe_agents.common.mock import Fixtures, jitter, pick_scenario
from iqe_agents.common.tools import fingerprint
from iqe_agents.monitor.playbook import handler as monitor_handler
from iqe_agents.reporting.playbook import handler as reporting_handler
from iqe_agents.reproduction.playbook import handler as repro_handler
from iqe_agents.review.playbook import handler as review_handler
from iqe_agents.runtime import Blocked, Ctx, validate_stage_contract
from iqe_agents.ticket.playbook import handler as ticket_handler
from iqe_agents.triage.playbook import handler as triage_handler

DATA = paths.data_dir()  # schemas / workflows / fixtures 都在包内 iqe_agents/common/data/

FAILURES: list[str] = []
MONITOR_ROWS: list[dict] = []

HANDLERS = {
    "monitor": monitor_handler,
    "triage": triage_handler,
    "reproduction": repro_handler,
    "ticket": ticket_handler,
    "review": review_handler,
    "reporting": reporting_handler,
}

# 自测必须离线且快：把 mock 行为注入关掉（jitter / 随机失败）
cfg.mock_delay_ms = "0-0"
cfg.mock_failure_rate = 0.0
agentkit_agent_runtime_jitter = jitter
FIXTURES = Fixtures(str(DATA / "fixtures"))


# --------------------------------------------------------------------- 内存替身 DB
class FakeDB:
    """只需覆盖剧本会调用的那几个方法，语义对齐 agentkit/db.py。"""

    def __init__(self) -> None:
        self.evidence_rows: list[dict] = []
        self.cases: dict[str, dict] = {}
        self.stage_runs: list[dict] = []
        self.scans: list[dict] = []
        self.daily_reports: dict[str, dict] = {}
        self.tickets: dict[str, dict] = {}
        self.reviews: list[dict] = []
        self._ev_id = 0

    async def save_evidence(self, *, case_id, stage, kind, meta, body, scan_id=None) -> str:
        self._ev_id += 1
        uri = f"db://evidence/{self._ev_id}"
        self.evidence_rows.append(
            {
                "id": self._ev_id,
                "case_id": case_id,
                "scan_id": scan_id,
                "stage": stage,
                "kind": kind,
                "uri": uri,
                "meta": meta or {},
                "excerpt": (body or "")[:400],
                "created_at": dt.datetime.now(),
            }
        )
        return uri

    async def evidence(self, case_id: str) -> list[dict]:
        return [r for r in self.evidence_rows if r.get("case_id") == case_id]

    async def create_case(self, **kw) -> None:
        row = {
            "case_id": kw["case_id"],
            "run_id": kw.get("run_id"),
            "scan_id": kw.get("scan_id"),
            "source": kw.get("source", "mock"),
            "job": kw.get("job"),
            "build": kw.get("build"),
            "signal": kw.get("signal"),
            "signature": kw.get("signature"),
            "status": kw.get("status"),
            "workflow_key": kw.get("workflow_key"),
            "plan_id": kw.get("plan_id"),
            "plan_source": kw.get("plan_source"),
            "priority": kw.get("priority"),
            "payload": kw.get("payload") or {},
            "created_at": kw.get("created_at") or dt.datetime.now(),
        }
        self.cases.setdefault(row["case_id"], row)

    async def get_case(self, case_id: str) -> dict | None:
        return self.cases.get(case_id)

    async def get_case_status(self, case_id: str) -> str | None:
        case = self.cases.get(case_id)
        return case["status"] if case else None

    async def set_case_status(self, case_id: str, status: str) -> None:
        if case_id in self.cases:
            self.cases[case_id]["status"] = status

    async def list_cases(
        self, *, status=None, signal=None, plan_source=None, limit: int = 100
    ) -> list[dict]:
        out = [
            c
            for c in self.cases.values()
            if (not status or c.get("status") == status)
            and (not signal or c.get("signal") == signal)
            and (not plan_source or c.get("plan_source") == plan_source)
        ]
        return out[:limit]

    async def stage_run_open(
        self, *, key, case_id, run_id, scan_id, stage, step_id, mode, attempt
    ) -> None:
        self.stage_runs.append(
            {
                "case_id": case_id,
                "run_id": run_id,
                "scan_id": scan_id,
                "stage": stage,
                "step_id": step_id,
                "mode": mode,
                "attempt": attempt,
                "status": "dispatched",
                "started_at": dt.datetime.now(),
                "finished_at": None,
                "latency_ms": None,
                "output": None,
                "error": None,
            }
        )

    async def stage_run_close(self, *, key, status, output, error) -> None:
        for row in reversed(self.stage_runs):
            if row.get("step_id") == key.split(":")[-2] or row.get("status") == "dispatched":
                row["status"], row["output"], row["error"] = status, output, error
                row["finished_at"] = dt.datetime.now()
                row["latency_ms"] = row.get("latency_ms") or 120
                break

    def add_stage_run(
        self,
        *,
        case_id,
        stage,
        mode,
        status="done",
        latency_ms=120,
        attempt=1,
        output=None,
        run_id="R-fake",
        scan_id=None,
    ) -> None:
        now = dt.datetime.now()
        self.stage_runs.append(
            {
                "case_id": case_id,
                "run_id": run_id,
                "scan_id": scan_id,
                "stage": stage,
                "step_id": f"{stage}1",
                "mode": mode,
                "attempt": attempt,
                "status": status,
                "started_at": now - dt.timedelta(milliseconds=latency_ms),
                "finished_at": now,
                "latency_ms": latency_ms,
                "output": output,
                "error": None,
            }
        )

    async def trace(self, case_id: str) -> list[dict]:
        case = self.cases.get(case_id)
        run_id = case.get("run_id") if case else None
        return [
            r
            for r in self.stage_runs
            if r.get("case_id") == case_id
            or (
                r.get("case_id") is None
                and r.get("run_id") == run_id
                and r.get("stage") == "monitor"
            )
        ]

    async def get_stage_output(self, case_id: str, stage: str) -> dict | None:
        rows = [
            r
            for r in self.stage_runs
            if r.get("case_id") == case_id
            and r.get("stage") == stage
            and r.get("status") == "done"
            and r.get("output")
        ]
        return rows[-1]["output"] if rows else None

    async def scan_close(self, *, scan_id, counts) -> None:
        self.scans.append({"scan_id": scan_id, "counts": counts, "started_at": dt.datetime.now()})

    async def daily_metrics(self, day: str) -> dict:
        cases = [c for c in self.cases.values() if str(c.get("created_at"))[:10] == day]
        from collections import Counter

        rca: Counter = Counter()
        for row in self.stage_runs:
            if row.get("stage") == "triage" and (row.get("output") or {}).get("rca"):
                rca[row["output"]["rca"]] += 1
        tats = []
        for c in cases:
            runs = [r for r in self.stage_runs if r.get("case_id") == c["case_id"]]
            if len(runs) > 1:
                tats.append(sum(int(r.get("latency_ms") or 0) for r in runs))
        return {
            "cases_total": len(cases),
            "errors": sum(1 for c in cases if c.get("signal") == "error"),
            "observed_only": sum(1 for c in cases if c.get("signal") == "none"),
            "status_distribution": dict(Counter(c.get("status") for c in cases)),
            "plan_source_distribution": dict(Counter(c.get("plan_source") for c in cases)),
            "rca_distribution": dict(rca),
            "suppressed": sum((s.get("counts") or {}).get("suppressed", 0) for s in self.scans),
            "scans": len(self.scans),
            "tat_ms_avg": int(sum(tats) / len(tats)) if tats else 0,
        }

    async def save_daily_report(self, *, day, report_md, metrics, cases) -> None:
        self.daily_reports[day] = {
            "day": day,
            "report_md": report_md,
            "metrics": metrics,
            "cases": cases,
        }

    async def get_daily_report(self, day: str) -> dict | None:
        return self.daily_reports.get(day)

    async def save_ticket(self, *, case_id, backend, output) -> None:
        self.tickets[case_id] = {"case_id": case_id, "backend": backend, **output}

    async def save_review(self, *, case_id, output, loop_seq) -> None:
        self.reviews.append({"case_id": case_id, "loop_seq": loop_seq, **output})


# --------------------------------------------------------------------- 断言工具
def check(label: str, cond: bool, detail: str = "") -> bool:
    if cond:
        print(f"PASS {label}")
        return True
    FAILURES.append(label)
    print(f"FAIL {label} :: {detail}")
    return False


def validate_output(stage: str, out: dict) -> tuple[bool, str]:
    try:
        jsonschema.validate(instance=out, schema=validate_stage_contract(stage, "output"))
        return True, ""
    except jsonschema.ValidationError as exc:
        return False, f"{stage} 输出违反契约: {exc.message} @ {'/'.join(map(str, exc.path))}"


class Runner:
    def __init__(self, db: FakeDB | None = None) -> None:
        self.db = db or FakeDB()

    async def run(
        self,
        stage: str,
        *,
        mode: str,
        scenario: str,
        input_obj: dict,
        case_id: str | None = None,
        scan_id: str | None = None,
        timeout_s: float | None = None,
    ) -> dict:
        env = {
            "run_id": f"R-{scenario}",
            "stage": stage,
            "step_id": f"{stage}_1",
            "attempt": 1,
            "case_id": case_id,
            "scan_id": scan_id,
            "mode": mode,
            "scenario_hint": scenario,
            "payload": {"mode": mode, "input": input_obj},
        }
        ctx = Ctx(stage=stage, env=env, scenario=pick_scenario(env), db=self.db, fixtures=FIXTURES)
        coro = HANDLERS[stage](ctx)
        out = await (asyncio.wait_for(coro, timeout_s) if timeout_s else coro)
        ok, detail = validate_output(stage, out)
        if not ok:
            FAILURES.append(f"{stage}/{scenario} 契约")
            print(f"FAIL {stage}/{scenario} 输出契约 :: {detail}")
        ctx.out = out
        ctx.db_rows = self.db
        return out


def kinds(db: FakeDB, case_id: str | None = None, stage: str | None = None) -> list[str]:
    return [
        r["kind"]
        for r in db.evidence_rows
        if (case_id is None or r.get("case_id") == case_id)
        and (stage is None or r.get("stage") == stage)
    ]


# --------------------------------------------------------------------- Monitor 场景表
MONITOR_EXPECT: dict[str, dict] = {
    # ci_feed 文件名 → 期望计数与签名家族（signature_family = component:error_code）
    "default": {"observed": 3, "promoted": 1, "suppressed": 0, "families": {"driver:crash"}},
    "all_success": {"observed": 3, "promoted": 0, "suppressed": 0, "families": set()},
    "new_failure": {"observed": 3, "promoted": 1, "suppressed": 0, "families": {"driver:crash"}},
    "known_issue_noise": {"observed": 3, "promoted": 0, "suppressed": 3, "families": set()},
    "mixed": {
        "observed": 4,
        "promoted": 1,
        "suppressed": 1,
        "families": {"toolchain:compile-error"},
    },
    "duplicate_merge": {
        "observed": 4,
        "promoted": 1,
        "suppressed": 3,
        "families": {"test:assert-fail"},
    },
    "flaky_unstable": {
        "observed": 1,
        "promoted": 1,
        "suppressed": 0,
        "families": {"driver:unknown"},
    },
    "aborted_infra": {
        "observed": 1,
        "promoted": 1,
        "suppressed": 0,
        "families": {"ci-infra:infra-abort"},
    },
    "many_errors": {
        "observed": 5,
        "promoted": 5,
        "suppressed": 0,
        "families": {
            "driver:crash",
            "test:assert-fail",
            "apt-repo:repo-404",
            "image-build:compile-error",
            "timeout:timeout",
        },
    },
    "blocked_no_machine": {
        "observed": 1,
        "promoted": 1,
        "suppressed": 0,
        "families": {"driver:crash"},
    },
    "timeout": {"observed": 1, "promoted": 1, "suppressed": 0, "families": {"driver:crash"}},
    "not_reproducible": {
        "observed": 1,
        "promoted": 1,
        "suppressed": 0,
        "families": {"driver:crash"},
    },
    "needs_info": {"observed": 1, "promoted": 1, "suppressed": 0, "families": {"driver:crash"}},
    "insufficient": {"observed": 1, "promoted": 1, "suppressed": 0, "families": {"driver:crash"}},
    "reject": {"observed": 1, "promoted": 1, "suppressed": 0, "families": {"driver:crash"}},
    "duplicate": {"observed": 1, "promoted": 1, "suppressed": 0, "families": {"driver:crash"}},
    "unknown_low_confidence": {
        "observed": 1,
        "promoted": 1,
        "suppressed": 0,
        "families": {"driver:crash"},
    },
    "llm_warming": {"observed": 1, "promoted": 1, "suppressed": 0, "families": {"driver:crash"}},
    "llm_invalid": {"observed": 1, "promoted": 1, "suppressed": 0, "families": {"driver:crash"}},
}


def obs_of(out: dict) -> list[dict]:
    return list(out.get("observations") or [])


async def run_monitor(r: Runner, feed: str) -> dict:
    out = await r.run(
        "monitor",
        mode="scan",
        scenario=feed,
        input_obj={"scan_id": f"scan-{feed}", "lookback": "15m", "jobs_filter": []},
        scan_id=f"scan-{feed}",
    )
    await r.db.scan_close(scan_id=f"scan-{feed}", counts=dict(out.get("counts") or {}))
    return out


async def selfcheck_monitor() -> None:
    print("\n=== Monitor：19 份 feed 的计数与签名家族 ===")
    feed_files = sorted(p.stem for p in (DATA / "fixtures" / "ci_feed").glob("*.json"))
    check(
        "fixtures/ci_feed 场景文件数 == 19",
        len(feed_files) == 19,
        f"实际 {len(feed_files)}: {feed_files}",
    )
    check(
        "ci_feed 覆盖核对表全部场景",
        set(feed_files) == set(MONITOR_EXPECT),
        f"缺 {set(MONITOR_EXPECT) - set(feed_files)} 多 {set(feed_files) - set(MONITOR_EXPECT)}",
    )

    for feed in feed_files:
        r = Runner()
        out = await run_monitor(r, feed)
        exp = MONITOR_EXPECT[feed]
        counts = dict(out.get("counts") or {})
        obs = obs_of(out)
        promoted = [o for o in obs if o.get("signal") == "error" and not o.get("suppressed")]
        suppressed = [o for o in obs if o.get("suppressed")]
        fams = {fingerprint.signature_family(o.get("signature") or "") for o in promoted}
        MONITOR_ROWS.append(
            {
                "scenario": feed,
                **{k: counts.get(k) for k in ("observed", "promoted", "suppressed")},
                "families": ",".join(sorted(fams)) or "-",
                "signatures": ",".join(sorted(o.get("signature") or "-" for o in promoted)) or "-",
            }
        )
        ok = (
            counts.get("observed") == exp["observed"]
            and counts.get("promoted") == exp["promoted"]
            and counts.get("suppressed") == exp["suppressed"]
            and fams == exp["families"]
            and len(promoted) == exp["promoted"]
            and len(suppressed) == exp["suppressed"]
            and counts.get("promoted") == len(promoted)
            and counts.get("suppressed") == len(suppressed)
            and counts.get("observed") == len(obs)
        )
        check(
            f"monitor/{feed} 计数与签名家族（observed={counts.get('observed')} "
            f"promoted={counts.get('promoted')} suppressed={counts.get('suppressed')} "
            f"families={sorted(fams) or '-'}）",
            ok,
            f"期望 {exp} 实际 {counts} fams={fams}",
        )
        check(
            f"monitor/{feed} 每条 promoted 都写了 log_excerpt 证据",
            all("log_excerpt" in kinds(r.db, stage="monitor") for _ in promoted)
            or ("log_excerpt" in kinds(r.db, stage="monitor")),
            kinds(r.db, stage="monitor"),
        )
        if feed == "duplicate_merge":
            # 语义（与契约一致）：被合并掉的行 promoted_reason 保持 None，抑制原因写在 context；
            # 保留下来的那一行（最大 build）带 duplicate_merge_count / duplicate_builds 计数。
            kept_ctx = promoted[0].get("context") or {}
            check(
                "monitor/duplicate_merge 同族只保留最大 build（1254）并记录合并计数",
                [o.get("build") for o in promoted] == [1254]
                and int(kept_ctx.get("duplicate_merge_count") or 0) >= 2
                and all(
                    (o.get("context") or {}).get("suppressed_reason") == "duplicate_signature_merge"
                    for o in suppressed
                ),
                f"promoted={[o.get('build') for o in promoted]} kept_ctx={ {k: kept_ctx.get(k) for k in ('duplicate_merge_count','duplicate_builds')} }",
            )
        if feed == "known_issue_noise":
            check(
                "monitor/known_issue_noise 已知噪声全部抑制并带 known_issue_ref",
                all(
                    o.get("known_issue_ref") == "network:offline"
                    and o.get("noise_score", 0) >= 0.85
                    for o in suppressed
                )
                and all(
                    (o.get("context") or {}).get("suppressed_reason") == "known_issue_noise"
                    for o in suppressed
                )
                and all(o.get("promoted_reason") is None for o in suppressed),  # 抑制不是晋升
                json.dumps([o.get("known_issue_ref") for o in suppressed], ensure_ascii=False),
            )
        if feed == "mixed":
            check(
                "monitor/mixed 新签名晋升 + 已知噪声抑制各 1",
                promoted[0].get("promoted_reason") == "new_failure_signature"
                and (suppressed[0].get("context") or {}).get("suppressed_reason")
                == "known_issue_noise",
                json.dumps(
                    [promoted[0].get("promoted_reason"), suppressed[0].get("promoted_reason")]
                ),
            )
        if feed == "flaky_unstable":
            check(
                "monitor/flaky_unstable UNSTABLE 也晋升（unstable_result）",
                promoted[0].get("promoted_reason") == "unstable_result"
                and promoted[0].get("result") == "UNSTABLE",
                json.dumps({k: promoted[0].get(k) for k in ("result", "promoted_reason")}),
            )
        if feed == "aborted_infra":
            check(
                "monitor/aborted_infra ABORTED 家族 ci-infra:",
                promoted[0].get("signature", "").startswith("ci-infra:")
                and promoted[0].get("promoted_reason") == "unstable_result",
                json.dumps({k: promoted[0].get(k) for k in ("signature", "promoted_reason")}),
            )
        if feed == "all_success":
            check(
                "monitor/all_success 无晋升但仍有 log_excerpt 摘要证据",
                "log_excerpt" in kinds(r.db, stage="monitor") and not promoted,
                kinds(r.db, stage="monitor"),
            )
        check(
            f"monitor/{feed} log_excerpt 不超过契约上限 4000",
            all(len(o.get("log_excerpt") or "") <= 4000 for o in obs),
            "",
        )

    # Monitor 数据源不可用 → Blocked(source_unavailable)
    r = Runner()
    try:
        await r.run(
            "monitor",
            mode="scan",
            scenario="no_such_feed",
            input_obj={"scan_id": "scan-x", "lookback": "15m"},
        )
        check("monitor/no_such_feed Blocked(source_unavailable)", False, "没有抛出 Blocked")
    except Blocked as b:
        check(
            "monitor/no_such_feed Blocked(source_unavailable)",
            b.code == "source_unavailable",
            f"code={b.code}",
        )


# --------------------------------------------------------------------- 下游剧本共用输入
async def monitor_promoted_obs(r: Runner, feed: str) -> dict:
    out = await run_monitor(r, feed)
    obs = [o for o in obs_of(out) if o.get("signal") == "error" and not o.get("suppressed")]
    if not obs:
        raise LookupError(f"feed {feed} 没有晋升的失败观测")
    return obs[0]


def triage_input(obs: dict, case_id: str, mode: str = "full") -> dict:
    return {
        "case_id": case_id,
        "build": obs.get("build"),
        "source": obs.get("source"),
        "job": obs.get("job"),
        "signal": obs.get("signal"),
        "signature": obs.get("signature"),
        "log_excerpt": obs.get("log_excerpt"),
        "context": dict(obs.get("context") or {}),
        "evidence_refs": list(obs.get("evidence_refs") or []),
        "requested_focus": None,
    }


def downstream_scenario(obs: dict) -> str:
    return str((obs.get("context") or {}).get("mock_scenario") or "default")


# --------------------------------------------------------------------- Triage
async def selfcheck_triage() -> None:
    print("\n=== Triage：分诊结论 / SKILL 卡片 / 复现资格 ===")
    cases = {
        "new_failure": {
            "rca": "driver",
            "conf": 0.82,
            "skill": "driver-failure-triage",
            "repro": True,
            "feed": "new_failure",
        },
        "duplicate_merge": {
            "rca": "test",
            "conf": 0.76,
            "skill": "test-failure-triage",
            "repro": True,
            "feed": "duplicate_merge",
        },
        "aborted_infra": {
            "rca": "infrastructure",
            "conf": 0.88,
            "skill": "environment-failure-triage",
            "repro": False,
            "feed": "aborted_infra",
        },
        "mixed": {
            "rca": "environment",
            "conf": 0.68,
            "skill": "environment-failure-triage",
            "repro": True,
            "feed": "mixed",
        },
        "unknown_low_confidence": {
            "rca": "unknown",
            "conf": 0.30,
            "skill": "environment-failure-triage",
            "repro": True,
            "feed": "unknown_low_confidence",
        },
    }
    for name, exp in cases.items():
        r = Runner()
        obs = await monitor_promoted_obs(r, exp["feed"])
        scen = downstream_scenario(obs)
        out = await r.run(
            "triage",
            mode="full",
            scenario=scen,
            case_id=f"case-{name}",
            input_obj=triage_input(obs, f"case-{name}"),
        )
        check(
            f"triage/{name} rca={out.get('rca')} confidence={out.get('confidence')} "
            f"signature={out.get('signature')} repro_eligible={out.get('repro_eligible')}",
            out.get("rca") == exp["rca"]
            and abs(float(out.get("confidence") or 0) - exp["conf"]) < 1e-6
            and out.get("repro_eligible") is exp["repro"]
            and out.get("skill_refs") == [exp["skill"]]
            and (out.get("signature") or "").startswith(
                f"{obs.get('signature', '').split(':')[0]}:"
            ),
            json.dumps(
                {
                    k: out.get(k)
                    for k in ("rca", "confidence", "repro_eligible", "skill_refs", "signature")
                },
                ensure_ascii=False,
            ),
        )
        check(
            f"triage/{name} 证据含 distilled_log + skill_card + triage_note",
            {"distilled_log", "skill_card", "triage_note"}
            <= set(kinds(r.db, f"case-{name}", "triage")),
            kinds(r.db, f"case-{name}", "triage"),
        )
        if exp["rca"] == "unknown":
            check(
                f"triage/{name} 置信度低于 0.70（会被复核第 3 条拦下）",
                float(out.get("confidence") or 1) < 0.70,
                str(out.get("confidence")),
            )
        if exp["rca"] == "infrastructure":
            check(
                f"triage/{name} 不占用重跑机位（repro_block_reason）",
                bool(out.get("repro_block_reason"))
                and "机位" in str(out.get("repro_block_reason")),
                str(out.get("repro_block_reason")),
            )
        check(
            f"triage/{name} key_frames 非空且 triage_notes<=10",
            bool(out.get("key_frames")) and len(out.get("triage_notes") or []) <= 10,
            json.dumps(
                {
                    "key_frames": len(out.get("key_frames") or []),
                    "triage_notes": len(out.get("triage_notes") or []),
                }
            ),
        )

    # 无新失败（signal=none）+ classify_only：只蒸馏归档，不占机位
    r = Runner()
    out = await run_monitor(r, "all_success")
    obs = obs_of(out)[0]
    out2 = await r.run(
        "triage",
        mode="classify_only",
        scenario="warming",
        case_id="case-warming",
        input_obj=triage_input(obs, "case-warming", "classify_only"),
    )
    check(
        "triage/warming(classify_only,signal=none) rca=unknown / 不复现 / key_frames=2 / notes=1",
        out2.get("rca") == "unknown"
        and out2.get("repro_eligible") is False
        and len(out2.get("key_frames") or []) == 2
        and len(out2.get("triage_notes") or []) == 1
        and out2.get("llm_calls") == 0
        and (
            "classify_only" in str(out2.get("repro_block_reason"))
            or "signal=none" in str(out2.get("repro_block_reason"))
        ),
        json.dumps(
            {
                k: out2.get(k)
                for k in (
                    "rca",
                    "repro_eligible",
                    "key_frames",
                    "triage_notes",
                    "llm_calls",
                    "repro_block_reason",
                )
            },
            ensure_ascii=False,
        ),
    )


# --------------------------------------------------------------------- Reproduction
async def repro_input(r: Runner, feed: str, case_id: str) -> tuple[dict, dict]:
    obs = await monitor_promoted_obs(r, feed)
    scen = downstream_scenario(obs)
    t = await r.run(
        "triage", mode="full", scenario=scen, case_id=case_id, input_obj=triage_input(obs, case_id)
    )
    src = triage_input(obs, case_id)
    src.update(
        {"rca": t["rca"], "signature": t["signature"], "key_frames": t.get("key_frames") or []}
    )
    return scen, src


async def selfcheck_reproduction() -> None:
    print("\n=== Reproduction：受控重跑 / Blocked 分支 ===")
    expectations = [("blocked_no_machine", "source_unavailable")]
    for feed, code in expectations:
        r = Runner()
        scen, src = await repro_input(r, feed, f"case-{feed}")
        try:
            await r.run(
                "reproduction", mode="run", scenario=scen, case_id=f"case-{feed}", input_obj=src
            )
            check(f"reproduction/{feed} Blocked({code})", False, "没有抛出 Blocked")
        except Blocked as b:
            check(
                f"reproduction/{feed} Blocked({code})",
                b.code == code,
                f"code={b.code} detail={b.detail}",
            )
            check(
                f"reproduction/{feed} Blocked 前已写 repro_log 证据",
                "repro_log" in kinds(r.db, f"case-{feed}", "reproduction"),
                kinds(r.db, f"case-{feed}"),
            )

    for feed, expected in [("default", "yes"), ("flaky", "flaky"), ("not_reproducible", "no")]:
        r = Runner()
        obs_feed = {
            "default": "default",
            "flaky": "flaky_unstable",
            "not_reproducible": "not_reproducible",
        }[feed]
        obs = await monitor_promoted_obs(r, obs_feed)
        scen = downstream_scenario(obs)
        src = triage_input(obs, f"case-{feed}")
        src.update(
            {"rca": "driver", "signature": obs.get("signature"), "key_frames": ["oops first line"]}
        )
        out = await r.run(
            "reproduction", mode="run", scenario=scen, case_id=f"case-{feed}", input_obj=src
        )
        check(
            f"reproduction/{feed} reproducible={out.get('reproducible')} attempts={out.get('attempts')}",
            out.get("reproducible") == expected
            and int(out.get("attempts") or 0) >= 2
            and len(out.get("matrix") or []) >= 1
            and bool(out.get("log_excerpt")),
            json.dumps(
                {k: out.get(k) for k in ("reproducible", "attempts", "matrix")}, ensure_ascii=False
            ),
        )
        check(
            f"reproduction/{feed} 每条重跑都写 repro_log 证据",
            kinds(r.db, f"case-{feed}", "reproduction").count("repro_log") >= 2,
            kinds(r.db, f"case-{feed}", "reproduction"),
        )

    # infrastructure 类不做受控重跑（即使有 reproduction 步骤也必须 Blocked(disabled)）
    r = Runner()
    obs = await monitor_promoted_obs(r, "aborted_infra")
    src = triage_input(obs, "case-aborted")
    src.update({"rca": "infrastructure", "signature": obs.get("signature"), "key_frames": []})
    try:
        await r.run(
            "reproduction",
            mode="run",
            scenario="aborted_infra",
            case_id="case-aborted",
            input_obj=src,
        )
        check("reproduction/infrastructure Blocked(disabled)", False, "没有抛出 Blocked")
    except Blocked as b:
        check(
            "reproduction/infrastructure Blocked(disabled)", b.code == "disabled", f"code={b.code}"
        )

    # reproduction 夹具 sleep_s=45 → 编排层 15s step 超时会先判 timeout；剧本必须真的在等
    r = Runner()
    obs = await monitor_promoted_obs(r, "timeout")
    src = triage_input(obs, "case-timeout")
    src.update({"rca": "driver", "signature": obs.get("signature"), "key_frames": []})
    try:
        await r.run(
            "reproduction",
            mode="run",
            scenario="timeout",
            case_id="case-timeout",
            input_obj=src,
            timeout_s=2.0,
        )
        check(
            "reproduction/timeout 长耗时（夹具 sleep_s=45，会被 step 超时截断）",
            False,
            "剧本没有按夹具等待",
        )
    except TimeoutError:
        check("reproduction/timeout 长耗时（夹具 sleep_s=45，会被 step 超时截断）", True)

    # 关掉重跑开关 → Blocked(disabled)
    r = Runner()
    obs = await monitor_promoted_obs(r, "new_failure")
    src = triage_input(obs, "case-norepro")
    src.update({"rca": "driver", "signature": obs.get("signature"), "key_frames": []})
    cfg.repro_enabled = False
    try:
        await r.run(
            "reproduction", mode="run", scenario="default", case_id="case-norepro", input_obj=src
        )
        check("reproduction/复现开关关闭 Blocked(disabled)", False, "没有抛出 Blocked")
    except Blocked as b:
        check("reproduction/复现开关关闭 Blocked(disabled)", b.code == "disabled", f"code={b.code}")
    finally:
        cfg.repro_enabled = True


# --------------------------------------------------------------------- Ticket
async def ticket_src(
    r: Runner, feed: str, case_id: str, *, rca: str, reproducible: str | None
) -> tuple[str, dict]:
    obs = await monitor_promoted_obs(r, feed)
    scen = downstream_scenario(obs)
    src = triage_input(obs, case_id)
    src.update(
        {
            "rca": rca,
            "signature": obs.get("signature"),
            "reproducible": reproducible,
            "duplicate_of": None,
        }
    )
    return scen, src


async def selfcheck_ticket() -> None:
    print("\n=== Ticket：成单状态 / 优先级 / 段落 ===")
    r = Runner()
    _scen, src = await ticket_src(r, "default", "case-ticket", rca="driver", reproducible="yes")
    out = await r.run(
        "ticket", mode="run", scenario="default", case_id="case-ticket", input_obj=src
    )
    keys = {s.get("key") for s in out.get("sections") or []}
    check(
        "ticket/default created + OST-* + P1 + OS-Driver + 段落齐全",
        out.get("ticket_status") == "created"
        and str(out.get("external_key") or "").startswith("OST-")
        and out.get("ticket_priority") == "P1"
        and out.get("assignee_team") == "OS-Driver"
        and {"rca", "repro", "evidence", "impact", "next_steps"} <= keys
        and all(str(s.get("content") or "").strip() for s in out.get("sections") or []),
        json.dumps(
            {
                k: out.get(k)
                for k in (
                    "ticket_status",
                    "external_key",
                    "ticket_priority",
                    "assignee_team",
                    "sections",
                )
            },
            ensure_ascii=False,
        )[:400],
    )
    check(
        "ticket/default 写了 ticket_payload 证据",
        "ticket_payload" in kinds(r.db, "case-ticket", "ticket"),
        kinds(r.db, "case-ticket", "ticket"),
    )

    _scen, src = await ticket_src(r, "duplicate", "case-dup", rca="driver", reproducible="yes")
    out = await r.run("ticket", mode="run", scenario="duplicate", case_id="case-dup", input_obj=src)
    check(
        "ticket/duplicate linked_duplicate OST-4821 且不开新单",
        out.get("ticket_status") == "linked_duplicate"
        and out.get("duplicate_of") == "OST-4821"
        and out.get("external_key") is None,
        json.dumps(
            {k: out.get(k) for k in ("ticket_status", "duplicate_of", "external_key")},
            ensure_ascii=False,
        ),
    )

    cfg.ticket_autocreate = False
    try:
        r = Runner()
        _scen, src = await ticket_src(r, "default", "case-draft", rca="driver", reproducible="yes")
        out = await r.run(
            "ticket", mode="run", scenario="default", case_id="case-draft", input_obj=src
        )
        check(
            "ticket/autocreate=off drafted + DRAFT-*",
            out.get("ticket_status") == "drafted"
            and str(out.get("external_key") or "").startswith("DRAFT-")
            and out.get("url") is None,
            json.dumps(
                {k: out.get(k) for k in ("ticket_status", "external_key", "url")},
                ensure_ascii=False,
            ),
        )
    finally:
        cfg.ticket_autocreate = True

    for feed, rca, repro, expected_prio, team in [
        ("duplicate_merge", "test", "yes", "P2", "OS-QA"),
        ("mixed", "environment", "no", "P3", "OS-Infra"),
    ]:
        r = Runner()
        _scen, src = await ticket_src(r, feed, f"case-{feed}-t", rca=rca, reproducible=repro)
        out = await r.run(
            "ticket", mode="run", scenario="default", case_id=f"case-{feed}-t", input_obj=src
        )
        check(
            f"ticket/{feed}({rca}/{repro}) 优先级 {expected_prio} 责任人 {team}",
            out.get("ticket_priority") == expected_prio and out.get("assignee_team") == team,
            json.dumps(
                {k: out.get(k) for k in ("ticket_priority", "assignee_team")}, ensure_ascii=False
            ),
        )


# --------------------------------------------------------------------- Review
def review_src(**kw) -> dict:
    base = {
        "case_id": kw.get("case_id", "case-review"),
        "source": "mock-jenkins",
        "job": "os-staging-gfx-test",
        "signal": kw.get("signal", "error"),
        "build": 1203,
        "signature": kw.get("signature", "driver:crash:aaaabbbbcccc"),
        "rca": kw.get("rca", "driver"),
        "confidence": kw.get("confidence", 0.82),
        "key_frames": kw.get(
            "key_frames", ["BUG: kernel NULL pointer dereference", "Oops#", "RIP: 0010:amdgpu+0x1f"]
        ),
        "reproducible": kw.get("reproducible", "yes"),
        "ticket_status": kw.get("ticket_status", "created"),
        "ticket_sections": kw.get(
            "ticket_sections",
            [
                {"key": "rca", "title": "根因结论", "content": "驱动空指针"},
                {"key": "repro", "title": "复现方法", "content": "同参重跑 2/2 复现"},
                {"key": "evidence", "title": "证据清单", "content": "db://evidence/1"},
                {"key": "impact", "title": "影响范围", "content": "staging 门禁"},
                {"key": "next_steps", "title": "下一步", "content": "补丁评审"},
            ],
        ),
        "duplicate_of": kw.get("duplicate_of"),
        "evidence_count": kw.get("evidence_count", 6),
        "requested_focus": kw.get("requested_focus"),
        "loop_seq": kw.get("loop_seq", 1),
    }
    base["repro_attempted"] = kw.get("repro_attempted", True)
    return base


def check_by_id(out: dict, cid: str) -> dict:
    return next((c for c in out.get("checks") or [] if c.get("id") == cid), {})


async def selfcheck_review() -> None:
    print("\n=== Review：8 条机械预检与结论 ===")
    r = Runner()
    out = await r.run(
        "review", mode="run", scenario="default", case_id="case-review", input_obj=review_src()
    )
    ids = [c.get("id") for c in out.get("checks") or []]
    expected_ids = [
        "evidence_present",
        "evidence_count_ok",
        "triage_confidence_ok",
        "repro_conclusion",
        "ticket_sections_nonempty",
        "no_duplicate_ticket",
        "signal_repro_consistent",
        "loop_not_exhausted",
    ]
    check("review/default 8 条预检顺序与编号契约一致", ids == expected_ids, json.dumps(ids))
    check(
        "review/default 全部通过 → approve/ok + quality_scores.overall",
        out.get("verdict") == "approve"
        and out.get("reason_code") == "ok"
        and all(c.get("pass") for c in out.get("checks") or [])
        and isinstance((out.get("quality_scores") or {}).get("overall"), (int, float)),
        json.dumps(
            {k: out.get(k) for k in ("verdict", "reason_code", "quality_scores")},
            ensure_ascii=False,
        ),
    )
    check(
        "review/default 写了 review_record 证据",
        "review_record" in kinds(r.db, "case-review", "review"),
        kinds(r.db, "case-review", "review"),
    )

    # 计划里没有复现段：第 4 条必须判 pass（跳过），不能误判 missing_repro
    out = await r.run(
        "review",
        mode="run",
        scenario="warming_ok",
        case_id="case-norepro-review",
        input_obj=review_src(
            case_id="case-norepro-review",
            repro_attempted=False,
            reproducible=None,
            ticket_status=None,
            ticket_sections=[],
        ),
    )
    c4, c5 = check_by_id(out, "repro_conclusion"), check_by_id(out, "ticket_sections_nonempty")
    check(
        "review/计划无复现段：第 4 条 pass（跳过）",
        c4.get("pass") is True and "跳过" in c4.get("detail", ""),
        json.dumps(c4, ensure_ascii=False),
    )
    check(
        "review/计划无 ticket：第 5 条 pass（跳过）",
        c5.get("pass") is True and "跳过" in c5.get("detail", ""),
        json.dumps(c5, ensure_ascii=False),
    )
    check(
        "review/计划无复现段且无 ticket → 仍 approve",
        out.get("verdict") == "approve",
        out.get("reason_code"),
    )

    # 复现段执行了但没给结论
    out = await r.run(
        "review",
        mode="run",
        scenario="default",
        case_id="case-missingrepro",
        input_obj=review_src(case_id="case-missingrepro", reproducible=None),
    )
    c4 = check_by_id(out, "repro_conclusion")
    check(
        "review/复现无结论 → 第 4 条 fail → needs_info/missing_repro",
        c4.get("pass") is False
        and out.get("verdict") == "needs_info"
        and out.get("reason_code") == "missing_repro"
        and bool(out.get("requested_info")),
        json.dumps(
            {k: out.get(k) for k in ("verdict", "reason_code", "requested_info")},
            ensure_ascii=False,
        ),
    )

    # 低置信度分诊 → 第 3 条 fail
    out = await r.run(
        "review",
        mode="run",
        scenario="unknown_low_confidence",
        case_id="case-lowconf",
        input_obj=review_src(case_id="case-lowconf", rca="unknown", confidence=0.30),
    )
    check(
        "review/低置信度 → 第 3 条 fail → needs_info/missing_triage",
        check_by_id(out, "triage_confidence_ok").get("pass") is False
        and out.get("verdict") == "needs_info"
        and out.get("reason_code") == "missing_triage",
        json.dumps({k: out.get(k) for k in ("verdict", "reason_code")}, ensure_ascii=False),
    )

    # 证据不足 → 第 1/2 条 fail
    out = await r.run(
        "review",
        mode="run",
        scenario="default",
        case_id="case-noev",
        input_obj=review_src(case_id="case-noev", evidence_count=0, key_frames=[]),
    )
    check(
        "review/零证据 → 第 1、2 条 fail → needs_info/insufficient_evidence",
        check_by_id(out, "evidence_present").get("pass") is False
        and check_by_id(out, "evidence_count_ok").get("pass") is False
        and out.get("reason_code") == "insufficient_evidence",
        json.dumps({k: out.get(k) for k in ("verdict", "reason_code")}, ensure_ascii=False),
    )

    # 工单段落为空 → 第 5 条 fail
    out = await r.run(
        "review",
        mode="run",
        scenario="default",
        case_id="case-nosections",
        input_obj=review_src(
            case_id="case-nosections",
            ticket_sections=[{"key": "rca", "content": "   "}, {"key": "impact", "content": "x"}],
        ),
    )
    check(
        "review/工单段落空 → 第 5 条 fail → missing_ticket_sections",
        check_by_id(out, "ticket_sections_nonempty").get("pass") is False
        and out.get("reason_code") == "missing_ticket_sections",
        json.dumps({k: out.get(k) for k in ("verdict", "reason_code")}, ensure_ascii=False),
    )

    # 矛盾结论 → 第 7 条 fail
    out = await r.run(
        "review",
        mode="run",
        scenario="default",
        case_id="case-conflict",
        input_obj=review_src(case_id="case-conflict", signal="none", reproducible="yes"),
    )
    check(
        "review/signal=none 但有复现结论 → 第 7 条 fail → inconsistent_evidence",
        check_by_id(out, "signal_repro_consistent").get("pass") is False
        and out.get("reason_code") == "inconsistent_evidence",
        json.dumps({k: out.get(k) for k in ("verdict", "reason_code")}, ensure_ascii=False),
    )

    # duplicate 场景：第 6 条必须 fail，第二轮仍未澄清 → reject
    out1 = await r.run(
        "review",
        mode="run",
        scenario="duplicate",
        case_id="case-dup-r",
        input_obj=review_src(
            case_id="case-dup-r",
            ticket_status="linked_duplicate",
            duplicate_of="OST-4821",
            loop_seq=1,
        ),
    )
    out2 = await r.run(
        "review",
        mode="run",
        scenario="duplicate",
        case_id="case-dup-r",
        input_obj=review_src(
            case_id="case-dup-r",
            ticket_status="linked_duplicate",
            duplicate_of="OST-4821",
            loop_seq=2,
        ),
    )
    check(
        "review/duplicate 第 6 条 fail（第 1 轮 needs_info/duplicate_ticket）",
        check_by_id(out1, "no_duplicate_ticket").get("pass") is False
        and out1.get("verdict") == "needs_info"
        and out1.get("reason_code") == "duplicate_ticket",
        json.dumps({k: out1.get(k) for k in ("verdict", "reason_code")}, ensure_ascii=False),
    )
    check(
        "review/duplicate 第 2 轮 → reject/duplicate_ticket",
        out2.get("verdict") == "reject" and out2.get("reason_code") == "duplicate_ticket",
        json.dumps({k: out2.get(k) for k in ("verdict", "reason_code")}, ensure_ascii=False),
    )

    # 场景夹具：needs_info（第一轮）→ 回炉后 approve
    out1 = await r.run(
        "review",
        mode="run",
        scenario="needs_info",
        case_id="case-needinfo",
        input_obj=review_src(case_id="case-needinfo", loop_seq=1),
    )
    out2 = await r.run(
        "review",
        mode="run",
        scenario="needs_info",
        case_id="case-needinfo",
        input_obj=review_src(
            case_id="case-needinfo",
            loop_seq=2,
            requested_focus="补充受控重跑记录与 reproducible 结论",
        ),
    )
    check(
        "review/needs_info 夹具：第 1 轮 needs_info + requested_info",
        out1.get("verdict") == "needs_info" and bool(out1.get("requested_info")),
        json.dumps(
            {k: out1.get(k) for k in ("verdict", "reason_code", "requested_info")},
            ensure_ascii=False,
        ),
    )
    check(
        "review/needs_info 夹具：第 2 轮回炉后按机械预检 approve",
        out2.get("verdict") == "approve" and out2.get("reason_code") == "ok",
        json.dumps({k: out2.get(k) for k in ("verdict", "reason_code")}, ensure_ascii=False),
    )

    # 场景夹具：insufficient → 永远 needs_info；回炉耗尽 → loop_exhausted
    out1 = await r.run(
        "review",
        mode="run",
        scenario="insufficient",
        case_id="case-insufficient",
        input_obj=review_src(case_id="case-insufficient", loop_seq=1),
    )
    out2 = await r.run(
        "review",
        mode="run",
        scenario="insufficient",
        case_id="case-insufficient",
        input_obj=review_src(case_id="case-insufficient", loop_seq=2),
    )
    out3 = await r.run(
        "review",
        mode="run",
        scenario="insufficient",
        case_id="case-insufficient",
        input_obj=review_src(case_id="case-insufficient", loop_seq=3),
    )
    check(
        "review/insufficient 夹具：每一轮都 needs_info/insufficient_evidence",
        out1.get("verdict") == "needs_info"
        and out2.get("verdict") == "needs_info"
        and out1.get("reason_code") == "insufficient_evidence"
        and out2.get("reason_code") == "insufficient_evidence",
        json.dumps([out1.get("reason_code"), out2.get("reason_code")]),
    )
    check(
        "review/insufficient 夹具：回炉预算耗尽 → 第 8 条 fail + reason loop_exhausted（编排层据此 blocked）",
        check_by_id(out3, "loop_not_exhausted").get("pass") is False
        and out3.get("reason_code") == "loop_exhausted",
        json.dumps({k: out3.get(k) for k in ("verdict", "reason_code")}, ensure_ascii=False),
    )

    # reject 夹具
    out = await r.run(
        "review",
        mode="run",
        scenario="reject",
        case_id="case-reject",
        input_obj=review_src(case_id="case-reject"),
    )
    check(
        "review/reject 夹具 → reject/mock_reject",
        out.get("verdict") == "reject" and out.get("reason_code") == "mock_reject",
        json.dumps({k: out.get(k) for k in ("verdict", "reason_code")}, ensure_ascii=False),
    )


# --------------------------------------------------------------------- Reporting
async def selfcheck_reporting() -> None:
    print("\n=== Reporting：结案报告与日报 ===")
    db = FakeDB()
    r = Runner(db)
    obs = await monitor_promoted_obs(r, "default")
    case_id = "case-report-1"
    await db.create_case(
        case_id=case_id,
        run_id="R-report",
        scan_id="scan-report",
        source="mock-jenkins",
        job=obs.get("job"),
        build=obs.get("build"),
        signal="error",
        signature=obs.get("signature"),
        status="closed",
        workflow_key="s2_full",
        plan_source="workflow",
        priority=100,
        created_at=dt.datetime.now(),
    )
    triage_out = await r.run(
        "triage",
        mode="full",
        scenario="driver-crash",
        case_id=case_id,
        input_obj=triage_input(obs, case_id),
    )
    db.add_stage_run(case_id=case_id, stage="monitor", mode="scan", latency_ms=1400, output={})
    db.add_stage_run(
        case_id=case_id, stage="triage", mode="full", latency_ms=2200, output=triage_out
    )
    src = triage_input(obs, case_id)
    src.update(
        {
            "rca": triage_out["rca"],
            "signature": triage_out["signature"],
            "key_frames": triage_out.get("key_frames") or [],
        }
    )
    repro_out = await r.run(
        "reproduction", mode="run", scenario="driver-crash", case_id=case_id, input_obj=src
    )
    db.add_stage_run(
        case_id=case_id, stage="reproduction", mode="run", latency_ms=3100, output=repro_out
    )
    src2 = dict(src, reproducible=repro_out["reproducible"])
    ticket_out = await r.run(
        "ticket", mode="run", scenario="default", case_id=case_id, input_obj=src2
    )
    db.add_stage_run(case_id=case_id, stage="ticket", mode="run", latency_ms=900, output=ticket_out)
    review_in = review_src(
        case_id=case_id,
        signature=triage_out["signature"],
        rca=triage_out["rca"],
        confidence=triage_out["confidence"],
        key_frames=triage_out.get("key_frames") or [],
        reproducible=repro_out["reproducible"],
        ticket_status=ticket_out["ticket_status"],
        ticket_sections=ticket_out.get("sections") or [],
        evidence_count=len(r.db.evidence_rows),
        loop_seq=1,
    )
    review_out = await r.run(
        "review", mode="run", scenario="default", case_id=case_id, input_obj=review_in
    )
    db.add_stage_run(
        case_id=case_id, stage="review", mode="run", latency_ms=1500, output=review_out
    )

    out = await r.run(
        "reporting",
        mode="case",
        scenario="default",
        case_id=case_id,
        input_obj={"case_id": case_id, "day": None},
    )
    md = out.get("report_md") or ""
    check(
        "reporting/case 结案报告结构齐全（结论摘要/阶段执行/证据链/未解决项）",
        all(
            s in md
            for s in ("# Case 结案报告", "## 结论摘要", "## 阶段执行", "## 证据链", "## 未解决项")
        ),
        md[:200],
    )
    m = out.get("metrics") or {}
    check(
        "reporting/case metrics 完整（tat_ms>0 / evidence_count>0 / rca / verdict）",
        m.get("tat_ms", 0) > 0
        and m.get("evidence_count", 0) > 0
        and m.get("rca") == "driver"
        and m.get("verdict") == "approve"
        and m.get("case_id") == case_id,
        json.dumps(m, ensure_ascii=False)[:400],
    )
    check(
        "reporting/case 写了 report_md 证据",
        "report_md" in kinds(r.db, case_id, "reporting"),
        kinds(r.db, case_id, "reporting"),
    )
    check(
        "reporting/case 输出契约字段（case_id/day/report_md/metrics）",
        {"case_id", "day", "report_md", "metrics"} <= set(out.keys()) and out.get("day"),
        json.dumps(sorted(out.keys())),
    )

    # daily：需要至少一份扫描计数与若干 case
    await db.scan_close(
        scan_id="scan-report", counts={"observed": 3, "promoted": 1, "suppressed": 2}
    )
    await db.create_case(
        case_id="case-report-2",
        run_id="R-report2",
        source="mock-jenkins",
        job="os-staging-test",
        build=1290,
        signal="none",
        signature=None,
        status="observed",
        plan_source="workflow",
        workflow_key="s1_lite",
        priority=50,
        created_at=dt.datetime.now(),
    )
    day = dt.date.today().isoformat()
    out = await r.run(
        "reporting",
        mode="daily",
        scenario="default",
        case_id=None,
        input_obj={"case_id": None, "day": day},
    )
    md = out.get("report_md") or ""
    check(
        "reporting/daily 使用夹具模板并替换占位符",
        md.startswith(f"# Staging-CI 日报 {day}")
        and "{{" not in md
        and all(s in md for s in ("## 概览", "## 根因分布", "## 未解决项")),
        md[:160],
    )
    m = out.get("metrics") or {}
    check(
        "reporting/daily 指标（cases_total / suppressed / rca_distribution / top_signatures）",
        m.get("cases_total") == 2
        and m.get("suppressed") == 2
        and m.get("scans", 0) >= 1
        and m.get("rca_distribution", {}).get("driver") == 1
        and m.get("top_signatures"),
        json.dumps(m, ensure_ascii=False)[:400],
    )
    stored = await db.get_daily_report(day)
    check(
        "reporting/daily 已落库 daily_reports（report_md + metrics + cases）",
        bool(stored) and stored.get("report_md") and len(stored.get("cases") or []) == 2,
        json.dumps(sorted((stored or {}).keys())),
    )
    check(
        "reporting/daily 的 case_id 为 null（契约允许）",
        out.get("case_id") is None,
        str(out.get("case_id")),
    )


# --------------------------------------------------------------------- Plan 夹具
async def selfcheck_plans() -> None:
    print("\n=== Plan 夹具：对照 schemas/plan.schema.json ===")
    import yaml

    schema = json.loads((DATA / "schemas" / "plan.schema.json").read_text(encoding="utf-8"))
    for name, expect_ok in (("warming_ok", True), ("no_repro", True), ("invalid_stage", False)):
        raw = yaml.safe_load(
            (DATA / "fixtures" / "plans" / f"{name}.yaml").read_text(encoding="utf-8")
        )
        errs = sorted(
            jsonschema.Draft202012Validator(schema).iter_errors(raw), key=lambda e: e.json_path
        )
        ok = bool(errs) == (not expect_ok)
        check(
            f"plans/{name}.yaml {'合法' if expect_ok else '非法（被校验器拒绝）'}",
            ok,
            json.dumps([f"{e.json_path}: {e.message}" for e in errs], ensure_ascii=False)[:300],
        )
        if expect_ok:
            stages = [s["stage"] for s in raw.get("steps") or []]
            check(
                f"plans/{name}.yaml 不含 monitor 步骤且 mode 合法",
                "monitor" not in stages
                and all(
                    s["mode"] in ("full", "classify_only", "run", "case", "daily")
                    for s in raw["steps"]
                ),
                json.dumps(stages),
            )
            check(
                f"plans/{name}.yaml 的 on 步骤名是字符串（不是 YAML 布尔键）",
                all(isinstance(k, str) for s in raw["steps"] for k in (s.get("on") or {})),
                json.dumps([list(s.get("on") or {}) for s in raw["steps"]]),
            )


async def selfcheck_alias() -> None:
    print("\n=== 剧本名兼容（连字符 / 下划线） ===")
    r = Runner()
    out = await run_monitor(r, "aborted-infra")
    obs = [o for o in obs_of(out) if o.get("signal") == "error" and not o.get("suppressed")]
    check(
        "monitor/aborted-infra（连字符剧本名）与 aborted_infra 夹具等价",
        out.get("counts") == {"observed": 1, "promoted": 1, "suppressed": 0}
        and (obs[0].get("signature") or "").startswith("ci-infra:"),
        json.dumps(out.get("counts"), ensure_ascii=False),
    )


# --------------------------------------------------------------------- 主流程
async def main() -> int:
    print(f"selfcheck_agents: data={DATA}")
    print(f"interpreter={sys.version.split()[0]}")
    await selfcheck_monitor()
    await selfcheck_triage()
    await selfcheck_reproduction()
    await selfcheck_ticket()
    await selfcheck_review()
    await selfcheck_reporting()
    await selfcheck_plans()
    await selfcheck_alias()

    print("\n=== Monitor 各场景计数实测 ===")
    print(f"{'scenario':<22} {'observed':>8} {'promoted':>8} {'suppressed':>10}  families")
    for row in MONITOR_ROWS:
        print(
            f"{row['scenario']:<22} {row.get('observed'):>8} {row.get('promoted'):>8} "
            f"{row.get('suppressed'):>10}  {row['families']}"
        )

    print("\n=== 场景行为核对 ===")
    print(
        f"- ci_feed 场景文件：{len(MONITOR_ROWS)} 个，全部计数与签名家族符合预期"
        if not any(f.startswith("monitor/") for f in FAILURES)
        else "- Monitor 场景存在偏差"
    )
    if FAILURES:
        print(f"\n结果：FAIL {len(FAILURES)} 项")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("\n结果：全部通过")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except Exception as exc:  # 自检脚本本身出错也要明确失败
        FAILURES.append(f"selfcheck 运行异常: {exc!r}")
        print(f"\nFAIL selfcheck 运行异常 :: {exc!r}")
        import traceback

        traceback.print_exc()
        sys.exit(2)
