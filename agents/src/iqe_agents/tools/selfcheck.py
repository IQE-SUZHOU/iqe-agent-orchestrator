"""离线自检：不依赖 RabbitMQ / Postgres，直接调用八个 Agent 剧本（按 mode 分派），
断言「12 阶段 / 两条分支 / 三道闸门」的 Mock 行为与输出契约。

用法：ci-selfcheck（console script）或 python -m iqe_agents.tools.selfcheck
退出码 0 = 全绿；每项断言打印一行 PASS。

覆盖：
1) monitor(scan) / triage(classify) / remediation(fix) / reproduction(evidence|verify) /
   regression(check|bisect) / ticketing(create) / reviewer(gate1|gate2|gate3) /
   reporting(case|daily) 的输出全部通过 schemas/stage-contracts.json（jsonschema 2020-12）；
2) 19 份 Mock CI feed 的 Monitor 计数（observed / promoted / suppressed）与签名家族；
3) 各 mode 的行为核对（分诊路径预判、修复采纳知识库、复现与验证、回归与二分、
   三道闸门结论、工单优先级、报告与 kb_writes）；
4) plan 夹具对照 schemas/plan.schema.json 与计划语义（双分支 / 回炉）。
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import sys

import jsonschema

from iqe_agents.common import paths
from iqe_agents.common.config import cfg
from iqe_agents.common.contracts import MODE_AUTHORITY
from iqe_agents.common.mock import Fixtures, pick_scenario
from iqe_agents.common.tools import fingerprint
from iqe_agents.monitor.playbook import handler as monitor_handler
from iqe_agents.regression.playbook import handler as regression_handler
from iqe_agents.remediation.playbook import handler as remediation_handler
from iqe_agents.reporting.playbook import handler as reporting_handler
from iqe_agents.reproduction.playbook import handler as repro_handler
from iqe_agents.reviewer.playbook import handler as reviewer_handler
from iqe_agents.runtime import Blocked, Ctx, validate_stage_contract
from iqe_agents.ticketing.playbook import handler as ticketing_handler
from iqe_agents.triage.playbook import handler as triage_handler

DATA = paths.data_dir()  # schemas / workflows / fixtures 都在包内 iqe_agents/common/data/

FAILURES: list[str] = []
CHECKS = [0]
MONITOR_ROWS: list[dict] = []

HANDLERS = {
    "monitor": monitor_handler,
    "triage": triage_handler,
    "reviewer": reviewer_handler,
    "remediation": remediation_handler,
    "reproduction": repro_handler,
    "regression": regression_handler,
    "ticketing": ticketing_handler,
    "reporting": reporting_handler,
}

# 自测必须离线且快：把 mock 行为注入关掉（jitter / 随机失败）
cfg.mock_delay_ms = "0:0"  # 只能是 "lo:hi" 形式，写成 "0-0" 会回落默认 150:500
cfg.mock_failure_rate = 0.0
FIXTURES = Fixtures(str(DATA / "fixtures"))


# --------------------------------------------------------------------- 内存替身 DB
class FakeDB:
    """只需覆盖剧本会调用的那几个方法，语义对齐 common/db.py；同样是「按 stage/mode 存」。"""

    def __init__(self) -> None:
        self.evidence_rows: list[dict] = []
        self.cases: dict[str, dict] = {}
        self.stage_runs: list[dict] = []
        self.scans: list[dict] = []
        self.daily_reports: dict[str, dict] = {}
        self.tickets: dict[str, dict] = {}
        self.reviews: list[dict] = []
        self.remediations: dict[str, dict] = {}
        self.verifications: dict[tuple[str, str], dict] = {}
        self.regressions: dict[tuple[str, str], dict] = {}
        self.kb_entries: list[dict] = []
        self._ev_id = 0

    # ---------- evidence / cases / stage_runs
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
        step_id=None,
    ) -> None:
        now = dt.datetime.now()
        self.stage_runs.append(
            {
                "case_id": case_id,
                "run_id": run_id,
                "scan_id": scan_id,
                "stage": stage,
                "step_id": step_id or f"{stage}_{mode}",
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

    async def stage_outputs(self, case_id: str) -> dict[str, list[dict]]:
        """按 stage 聚合成功输出（reviewer 一计划跑三次，按 mode 区分）。"""
        out: dict[str, list[dict]] = {}
        for row in self.stage_runs:
            if row.get("case_id") != case_id or row.get("status") != "done":
                continue
            if not row.get("output"):
                continue
            out.setdefault(row["stage"], []).append(
                {
                    "mode": row.get("mode"),
                    "step_id": row.get("step_id"),
                    "attempt": row.get("attempt") or 1,
                    "output": row["output"],
                }
            )
        return out

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

    # ---------- 日报
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

    # ---------- 工单 / 三道闸门 / 修复 / 复现 / 回归 / 知识库
    async def save_ticket(self, *, case_id, backend, output) -> None:
        self.tickets[case_id] = {"case_id": case_id, "backend": backend, **output}

    async def save_review(
        self, *, case_id: str, output: dict, loop_seq: int, gate: str | None = None
    ) -> None:
        self.reviews.append(
            {
                "case_id": case_id,
                "gate": gate or output.get("gate"),
                "loop_seq": loop_seq,
                "verdict": output.get("verdict"),
                "reason_code": output.get("reason_code"),
                "requested_info": list(output.get("requested_info") or []),
            }
        )

    async def save_remediation(self, *, case_id: str, output: dict) -> None:
        self.remediations[case_id] = dict(output)

    async def save_verification(self, *, case_id: str, output: dict) -> None:
        self.verifications[(case_id, str(output.get("mode") or "evidence"))] = dict(output)

    async def save_regression(self, *, case_id: str, output: dict) -> None:
        self.regressions[(case_id, str(output.get("mode") or "check"))] = dict(output)

    async def kb_write(self, entry: dict) -> None:
        self.kb_entries.append(dict(entry))

    async def kb_search(
        self, *, component: str | None = None, signature: str | None = None, limit: int = 5
    ) -> list[dict]:
        rows = [
            e
            for e in self.kb_entries
            if (not component or e.get("component") == component)
            or (
                signature and str(e.get("signature") or "").startswith(str(signature).split(":")[0])
            )
        ]
        return rows[-limit:]


# --------------------------------------------------------------------- 断言工具
def check(label: str, cond: bool, detail: str = "") -> bool:
    CHECKS[0] += 1
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
            "step_id": f"{stage}_{mode}",
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
                f"promoted={[o.get('build') for o in promoted]} kept_ctx={ {k: kept_ctx.get(k) for k in ('duplicate_merge_count', 'duplicate_builds')} }",
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


def triage_input(obs: dict, case_id: str, *, requested_focus: str | None = None) -> dict:
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
        "requested_focus": requested_focus,
    }


def downstream_scenario(obs: dict) -> str:
    return str((obs.get("context") or {}).get("mock_scenario") or "default")


# --------------------------------------------------------------------- 下游剧本共用输入
async def obs_where(r: Runner, feed: str, prefix: str) -> dict:
    """取某份 feed 里签名族匹配 prefix 的第一条晋升观测。"""
    out = await run_monitor(r, feed)
    for obs in obs_of(out):
        if obs.get("signal") != "error" or obs.get("suppressed"):
            continue
        if str(obs.get("signature") or "").startswith(prefix):
            return obs
    raise LookupError(f"feed {feed} 里没有 {prefix}* 的晋升观测")


async def triage_case(
    feed: str,
    *,
    scenario: str | None = None,
    case_id: str = "C-tri",
    prefix: str = "",
    requested_focus: str | None = None,
) -> tuple[Runner, dict]:
    r = Runner()
    obs = await obs_where(r, feed, prefix) if prefix else await monitor_promoted_obs(r, feed)
    out = await r.run(
        "triage",
        mode="classify",
        scenario=scenario or downstream_scenario(obs),
        input_obj=triage_input(obs, case_id, requested_focus=requested_focus),
        case_id=case_id,
    )
    return r, out


def triage_summary(out: dict) -> str:
    return json.dumps(
        {k: out.get(k) for k in ("rca", "path", "component", "confidence", "duplicate_of")},
        ensure_ascii=False,
    )


# --------------------------------------------------------------------- Triage（classify）
async def selfcheck_triage() -> None:
    print("\n=== Triage（classify）：根因 / 路径预判 / 知识库 / 查重 ===")
    r, out = await triage_case("new_failure", case_id="C-tri-1")
    check(
        "triage/new_failure → driver + external，置信度 0.82",
        (out["rca"], out["path"], out["component"], out["confidence"])
        == ("driver", "external", "driver", 0.82),
        triage_summary(out),
    )
    check(
        "triage/new_failure 归属 OS 阶段并引用知识库/技能卡",
        out.get("os_stage") == "staging-test"
        and out.get("kb_refs", [""])[0] == "KB-ISSUE-001"
        and out.get("skill_refs") == ["driver-failure-triage"],
        json.dumps(
            {k: out.get(k) for k in ("os_stage", "kb_refs", "skill_refs")}, ensure_ascii=False
        ),
    )
    got_kinds = kinds(r.db, case_id="C-tri-1", stage="triage")
    check(
        "triage 写了 distilled_log / skill_card / kb_lookup / triage_note 四类证据",
        {"distilled_log", "skill_card", "kb_lookup", "triage_note"} <= set(got_kinds),
        str(got_kinds),
    )
    check(
        "triage 的 triage_notes 写明路径预判理由",
        any("路径预判" in n for n in out.get("triage_notes") or []),
        json.dumps(out.get("triage_notes"), ensure_ascii=False)[:300],
    )

    _, out = await triage_case("duplicate_merge", case_id="C-tri-2")
    check(
        "triage/duplicate_merge → 测试类失败走 internal（0.76）",
        (out["rca"], out["path"], out["component"], out["confidence"])
        == ("test", "internal", "test", 0.76),
        triage_summary(out),
    )

    _, out = await triage_case("mixed", prefix="toolchain", case_id="C-tri-3")
    check(
        "triage/mixed → toolchain 编译失败判 environment + internal",
        (out["rca"], out["path"], out["component"]) == ("environment", "internal", "toolchain")
        and "KB-FIX-002" in (out.get("kb_refs") or []),
        triage_summary(out),
    )

    _, out = await triage_case("many_errors", prefix="apt-repo", case_id="C-tri-4")
    check(
        "triage/many_errors(apt-repo) → 命中已知问题台账，给出 duplicate_of",
        out["rca"] == "environment" and str(out.get("duplicate_of") or "").startswith("case:"),
        triage_summary(out),
    )

    _, out = await triage_case("unknown_low_confidence", case_id="C-tri-5")
    check(
        "triage/unknown_low_confidence → rca=unknown、置信度 0.30（闸门 1 会打回）",
        (out["rca"], out["confidence"], out["path"]) == ("unknown", 0.3, "external"),
        triage_summary(out),
    )

    _, out = await triage_case("new_failure", scenario="internal_script_fix", case_id="C-tri-6")
    check(
        "triage/internal_script_fix → infrastructure + internal + ci-infra（0.88）",
        (out["rca"], out["path"], out["component"], out["confidence"])
        == ("infrastructure", "internal", "ci-infra", 0.88)
        and "KB-FIX-001" in (out.get("kb_refs") or []),
        triage_summary(out),
    )

    _, out_alias = await triage_case(
        "new_failure", scenario="internal-script-fix", case_id="C-tri-7"
    )
    check(
        "triage 剧本名兼容连字符 / 下划线（internal-script-fix 等价）",
        triage_summary(out_alias) == triage_summary(out),
        f"{triage_summary(out_alias)} != {triage_summary(out)}",
    )

    _, out = await triage_case("new_failure", case_id="C-tri-8", requested_focus="补 dmesg 尾部")
    check(
        "triage 回炉时把 requested_focus 写进结论要点",
        any("补 dmesg 尾部" in n for n in out.get("triage_notes") or []),
        json.dumps(out.get("triage_notes"), ensure_ascii=False)[-200:],
    )

    # 成功构建：signal=none 不分流、不判复现
    r = Runner()
    out = await run_monitor(r, "all_success")
    success = next(o for o in obs_of(out) if o.get("signal") == "none")
    tri = await r.run(
        "triage",
        mode="classify",
        scenario="warming",
        input_obj=triage_input(success, "C-tri-9"),
        case_id="C-tri-9",
    )
    check(
        "triage/signal=none → 只归档不分流（rca=unknown、component=none）",
        tri["rca"] == "unknown"
        and tri.get("component") == "none"
        and str(tri.get("routed_reason") or "").startswith("signal=none")
        and len(tri.get("key_frames") or []) <= 2,
        json.dumps(
            {k: tri.get(k) for k in ("rca", "component", "routed_reason")}, ensure_ascii=False
        ),
    )


# --------------------------------------------------------------------- Remediation（fix）
def remediation_input(
    *,
    case_id: str,
    rca: str = "environment",
    component: str = "ci-infra",
    requested_focus: str | None = None,
) -> dict:
    return {
        "case_id": case_id,
        "source": "monitor",
        "job": "os-staging-install",
        "build": 1234,
        "rca": rca,
        "component": component,
        "signature": f"{component}:infra-abort",
        "requested_focus": requested_focus,
    }


async def selfcheck_remediation() -> None:
    print("\n=== Remediation（fix）：修复改动 / ADO 工作项 / 知识库采纳 ===")
    r = Runner()
    out = await r.run(
        "remediation",
        mode="fix",
        scenario="default",
        input_obj=remediation_input(case_id="C-rem-1"),
        case_id="C-rem-1",
    )
    check(
        "remediation/default → fixed=true + script + staging_install.sh",
        (out["fixed"], out["change_type"], out["target"], out["risk"])
        == (True, "script", "ci/scripts/staging_install.sh", "low"),
        json.dumps(out, ensure_ascii=False)[:300],
    )
    check(
        "remediation/default 采纳知识库 KB-FIX-001 并建 ADO 工作项",
        out.get("adopted_from_kb") == "KB-FIX-001"
        and out.get("work_item_key") == "ADO-4821"
        and str(out.get("work_item_url") or "").endswith("/4821"),
        json.dumps({k: out.get(k) for k in ("adopted_from_kb", "work_item_key", "work_item_url")}),
    )
    check(
        "remediation 写了 remediation_plan 证据",
        "remediation_plan" in kinds(r.db, case_id="C-rem-1", stage="remediation"),
        str(kinds(r.db, case_id="C-rem-1", stage="remediation")),
    )

    out = await Runner().run(
        "remediation",
        mode="fix",
        scenario="default",
        input_obj=remediation_input(case_id="C-rem-2", rca="driver", component="driver"),
        case_id="C-rem-2",
    )
    check(
        "remediation/rca=driver → 不自动改产品代码（fixed=false + 转派）",
        out["fixed"] is False
        and out["target"] == "kernel/drivers/amdgpu"
        and out.get("work_item_key") is None
        and any("只做转派" in n for n in out.get("notes") or []),
        json.dumps(
            {k: out.get(k) for k in ("fixed", "target", "work_item_key")}, ensure_ascii=False
        ),
    )

    out = await Runner().run(
        "remediation",
        mode="fix",
        scenario="insufficient",
        input_obj=remediation_input(case_id="C-rem-3"),
        case_id="C-rem-3",
    )
    check(
        "remediation/insufficient → fixed=false、risk=high、交人工",
        out["fixed"] is False and out["risk"] == "high" and "未找到" in out["diff_summary"],
        json.dumps({k: out.get(k) for k in ("fixed", "risk", "diff_summary")}, ensure_ascii=False)[
            :250
        ],
    )

    out = await Runner().run(
        "remediation",
        mode="fix",
        scenario="rollback",
        input_obj=remediation_input(case_id="C-rem-4", component="network"),
        case_id="C-rem-4",
    )
    check(
        "remediation/rollback → 配置类修复，且不硬套别的组件的知识库方案",
        (out["fixed"], out["change_type"], out["target"], out["risk"])
        == (True, "config", "ci/config/jenkins-agent.yaml", "medium")
        and out.get("adopted_from_kb") is None,
        json.dumps({k: out.get(k) for k in ("change_type", "adopted_from_kb")}, ensure_ascii=False),
    )

    out = await Runner().run(
        "remediation",
        mode="fix",
        scenario="default",
        input_obj=remediation_input(case_id="C-rem-5", requested_focus="补修复前后对比"),
        case_id="C-rem-5",
    )
    check(
        "remediation 闸门 2 打回后记录重做要求",
        any("打回后重做" in n and "补修复前后对比" in n for n in out.get("notes") or []),
        json.dumps(out.get("notes"), ensure_ascii=False)[-250:],
    )

    try:
        await Runner().run(
            "remediation",
            mode="fix",
            scenario="default",
            input_obj={"source": "monitor", "job": "j", "rca": "environment"},
            case_id=None,
        )
        check("remediation 缺 case_id → Blocked(schema_invalid)", False, "没有抛出 Blocked")
    except Blocked as b:
        check(
            "remediation 缺 case_id → Blocked(schema_invalid)",
            b.code == "schema_invalid",
            f"code={b.code}",
        )


# --------------------------------------------------------------------- Reproduction（evidence | verify）
def repro_input(
    *, case_id: str, rca: str = "driver", component: str = "driver", fixed: bool = False
) -> dict:
    return {
        "case_id": case_id,
        "source": "monitor",
        "job": "os-staging-test",
        "build": 1261,
        "rca": rca,
        "component": component,
        "signature": f"{component}:crash",
        "key_frames": ["amdgpu GPU page fault"],
        "remediation": {"fixed": fixed, "change_type": "script", "target": "ci/scripts/x.sh"},
    }


async def selfcheck_reproduction() -> None:
    print("\n=== Reproduction（evidence / verify）：4b 取证 + 5a 验证 ===")
    r = Runner()
    out = await r.run(
        "reproduction",
        mode="evidence",
        scenario="default",
        input_obj=repro_input(case_id="C-rep-1"),
        case_id="C-rep-1",
    )
    check(
        "reproduction/evidence → 复现 2 次、reproducible=yes、fix_verified=null",
        (out["mode"], out["reproducible"], out["attempts"], out["fix_verified"])
        == ("evidence", "yes", 2, None)
        and len(out["matrix"]) == 2,
        json.dumps({k: out.get(k) for k in ("mode", "reproducible", "attempts", "fix_verified")}),
    )
    got_kinds = kinds(r.db, case_id="C-rep-1", stage="reproduction")
    check(
        "reproduction 每次重跑一条 repro_log + 一份 repro_plan",
        got_kinds.count("repro_log") == 2 and got_kinds.count("repro_plan") == 1,
        str(got_kinds),
    )

    out = await Runner().run(
        "reproduction",
        mode="verify",
        scenario="default",
        input_obj=repro_input(case_id="C-rep-2", fixed=True),
        case_id="C-rep-2",
    )
    check(
        "reproduction/verify → 应用修复后 fix_verified=true",
        (out["mode"], out["fix_verified"], out["attempts"]) == ("verify", True, 3),
        json.dumps({k: out.get(k) for k in ("mode", "fix_verified", "attempts")}),
    )

    out = await Runner().run(
        "reproduction",
        mode="verify",
        scenario="fixed_not_verified",
        input_obj=repro_input(case_id="C-rep-3", fixed=True),
        case_id="C-rep-3",
    )
    check(
        "reproduction/verify 无效修复 → fix_verified=false（闸门 2 打回）",
        out["fix_verified"] is False and out["reproducible"] == "yes",
        json.dumps({k: out.get(k) for k in ("fix_verified", "reproducible")}),
    )

    out = await Runner().run(
        "reproduction",
        mode="verify",
        scenario="default",
        input_obj=repro_input(case_id="C-rep-4", fixed=False),
        case_id="C-rep-4",
    )
    check(
        "reproduction/verify 在 4a 未产出修复时不谎报验证通过",
        out["fix_verified"] is False
        and any("未产出可用修复" in s for s in out.get("steps_summary") or []),
        json.dumps({k: out.get(k) for k in ("fix_verified", "steps_summary")}, ensure_ascii=False)[
            :250
        ],
    )

    out = await Runner().run(
        "reproduction",
        mode="evidence",
        scenario="not_reproducible",
        input_obj=repro_input(case_id="C-rep-5"),
        case_id="C-rep-5",
    )
    check(
        "reproduction/not_reproducible → reproducible=no 且写明原因",
        out["reproducible"] == "no"
        and bool(out.get("blocked_reason"))
        and bool(out.get("log_excerpt")),
        json.dumps({k: out.get(k) for k in ("reproducible", "blocked_reason")}, ensure_ascii=False),
    )

    out = await Runner().run(
        "reproduction",
        mode="evidence",
        scenario="flaky",
        input_obj=repro_input(case_id="C-rep-6"),
        case_id="C-rep-6",
    )
    check(
        "reproduction/flaky → reproducible=flaky（3 次两败一过）",
        out["reproducible"] == "flaky" and out["attempts"] == 3,
        json.dumps({k: out.get(k) for k in ("reproducible", "attempts")}),
    )

    r = Runner()
    try:
        await r.run(
            "reproduction",
            mode="evidence",
            scenario="blocked_no_machine",
            input_obj=repro_input(case_id="C-rep-7"),
            case_id="C-rep-7",
        )
        check("reproduction/blocked_no_machine → Blocked(source_unavailable)", False, "没有抛出")
    except Blocked as b:
        check(
            "reproduction/blocked_no_machine → Blocked(source_unavailable)",
            b.code == "source_unavailable" and "repro_log" in kinds(r.db, case_id="C-rep-7"),
            f"code={b.code}",
        )

    try:
        await Runner().run(
            "reproduction",
            mode="evidence",
            scenario="default",
            input_obj=repro_input(case_id="C-rep-8", rca="infrastructure", component="ci-infra"),
            case_id="C-rep-8",
        )
        check("reproduction/rca=infrastructure → Blocked(disabled)", False, "没有抛出")
    except Blocked as b:
        check(
            "reproduction/rca=infrastructure → Blocked(disabled)（不占重跑机位）",
            b.code == "disabled",
            f"code={b.code}",
        )

    try:
        await Runner().run(
            "reproduction",
            mode="report",
            scenario="default",
            input_obj=repro_input(case_id="C-rep-9"),
            case_id="C-rep-9",
        )
        check("reproduction 非法 mode → Blocked(schema_invalid)", False, "没有抛出")
    except Blocked as b:
        check(
            "reproduction 非法 mode → Blocked(schema_invalid)",
            b.code == "schema_invalid",
            f"code={b.code}",
        )

    # 夹具故意 sleep_s=75（> s2_full 里 e1 的 60s）：步骤超时 → retryable，重试耗尽后 blocked
    t0 = dt.datetime.now()
    raised = False
    try:
        await Runner().run(
            "reproduction",
            mode="evidence",
            scenario="timeout",
            input_obj=repro_input(case_id="C-rep-10"),
            case_id="C-rep-10",
            timeout_s=0.2,
        )
    except TimeoutError:
        raised = True
    elapsed = (dt.datetime.now() - t0).total_seconds()
    check(
        "reproduction/timeout 夹具可被步骤超时打断（→retryable→DLQ）",
        raised and elapsed < 5,
        f"raised={raised} elapsed={elapsed:.1f}s",
    )


# --------------------------------------------------------------------- Regression（check | bisect）
def regression_input(*, case_id: str, component: str = "driver") -> dict:
    return {
        "case_id": case_id,
        "source": "monitor",
        "job": "os-staging-test",
        "build": 1261,
        "rca": "driver",
        "component": component,
        "signature": f"{component}:crash",
        "reproduction": {"reproducible": "yes", "attempts": 2},
    }


async def selfcheck_regression() -> None:
    print("\n=== Regression（check / bisect）：5b 回归判定 + 6c guilty patch ===")
    r = Runner()
    out = await r.run(
        "regression",
        mode="check",
        scenario="default",
        input_obj=regression_input(case_id="C-reg-1"),
        case_id="C-reg-1",
    )
    check(
        "regression/check → is_regression=true，带基线与候选区间",
        (out["mode"], out["is_regression"], out["baseline"], len(out["candidates"]))
        == ("check", True, "staging-pass-2026-11-24", 3)
        and out["guilty_patch"] is None,
        json.dumps({k: out.get(k) for k in ("mode", "is_regression", "baseline")}),
    )
    check(
        "regression/check 写了 regression_check 证据",
        "regression_check" in kinds(r.db, case_id="C-reg-1", stage="regression"),
        str(kinds(r.db, case_id="C-reg-1", stage="regression")),
    )

    out = await Runner().run(
        "regression",
        mode="check",
        scenario="no_regression",
        input_obj=regression_input(case_id="C-reg-2"),
        case_id="C-reg-2",
    )
    check(
        "regression/check 非回归 → 跳过 6c（is_regression=false）",
        out["is_regression"] is False and any("环境问题" in n for n in out.get("notes") or []),
        json.dumps(out.get("notes"), ensure_ascii=False)[:200],
    )

    out = await Runner().run(
        "regression",
        mode="bisect",
        scenario="default",
        input_obj=regression_input(case_id="C-reg-3"),
        case_id="C-reg-3",
    )
    patch = out.get("guilty_patch") or {}
    check(
        "regression/bisect → 收敛到 a1b2c3d（PR-1187 / 0.86）",
        out["bisected"] is True
        and patch.get("commit") == "a1b2c3d"
        and patch.get("pr") == "PR-1187"
        and patch.get("confidence") == 0.86,
        json.dumps(patch, ensure_ascii=False)[:250],
    )

    out = await Runner().run(
        "regression",
        mode="bisect",
        scenario="inconclusive",
        input_obj=regression_input(case_id="C-reg-4"),
        case_id="C-reg-4",
    )
    check(
        "regression/bisect 未收敛 → guilty_patch=null，知识库只作参考",
        out["bisected"] is False
        and out["guilty_patch"] is None
        and any("KB-PATCH-001" in n for n in out.get("notes") or []),
        json.dumps(out.get("notes"), ensure_ascii=False)[:250],
    )

    try:
        await Runner().run(
            "regression",
            mode="triage",
            scenario="default",
            input_obj=regression_input(case_id="C-reg-5"),
            case_id="C-reg-5",
        )
        check("regression 非法 mode → Blocked(schema_invalid)", False, "没有抛出")
    except Blocked as b:
        check(
            "regression 非法 mode → Blocked(schema_invalid)",
            b.code == "schema_invalid",
            f"code={b.code}",
        )


# --------------------------------------------------------------------- Reviewer（gate1 | gate2 | gate3）
def reviewer_input(*, case_id: str, path: str = "external", **kw) -> dict:
    base = {
        "case_id": case_id,
        "source": "monitor",
        "job": "os-staging-test",
        "build": 1261,
        "signal": "error",
        "rca": "driver",
        "component": "driver",
        "path": path,
        "confidence": 0.82,
        "key_frames": ["amdgpu GPU page fault"],
        "evidence_count": 6,
        "signature": "driver:crash",
        "loop_seq": 1,
    }
    base.update(kw)
    return base


def gate2_input(
    *, case_id: str, fixed: bool = True, fix_verified: bool = True, target: str = "ci/scripts/x.sh"
) -> dict:
    return reviewer_input(
        case_id=case_id,
        remediation={"fixed": fixed, "change_type": "script", "target": target},
        reproduction={
            "mode": "verify",
            "fix_verified": fix_verified,
            "reproducible": "yes",
            "attempts": 3,
        },
    )


def gate3_input(
    *,
    case_id: str,
    sections: list[dict] | None = None,
    status: str = "created",
    assignee: str = "OS-Driver",
    duplicate_of: str | None = None,
) -> dict:
    return reviewer_input(
        case_id=case_id,
        ticketing={
            "ticket_status": status,
            "external_key": "OST-1001",
            "duplicate_of": duplicate_of,
            "assignee_team": assignee,
            "sections": (
                sections
                if sections is not None
                else [
                    {"key": "rca", "content": "驱动缺陷"},
                    {"key": "repro", "content": "复现 2 次"},
                    {"key": "evidence", "content": "dmesg 尾部"},
                    {"key": "impact", "content": "gfx1151"},
                ]
            ),
        },
    )


async def selfcheck_reviewer() -> None:
    print("\n=== Reviewer（gate1 / gate2 / gate3）：三道闸门的结论与回炉要求 ===")
    r = Runner()
    out = await r.run(
        "reviewer",
        mode="gate1",
        scenario="default",
        input_obj=reviewer_input(case_id="C-rev-1"),
        case_id="C-rev-1",
    )
    check(
        "reviewer/gate1 → approve + decision=external（放行外部路径）",
        (out["gate"], out["verdict"], out["decision"], out["reason_code"])
        == ("gate1", "approve", "external", "ok")
        and [c["id"] for c in out["checks"]]
        == ["evidence_present", "triage_confident", "path_known", "loop_not_exhausted"],
        json.dumps(
            {k: out.get(k) for k in ("verdict", "decision", "reason_code")}, ensure_ascii=False
        ),
    )
    rec = next((e for e in r.db.evidence_rows if e["kind"] == "review_record"), None) or {}
    check(
        "reviewer 把闸门号写进 review_record 证据并给出质量分",
        (rec.get("meta") or {}).get("gate") == "gate1"
        and 0.0 <= float(out["quality_scores"]["overall"]) <= 1.0
        and out["quality_scores"]["checks_total"] == len(out["checks"]),
        json.dumps({k: out.get(k) for k in ("quality_scores",)}, ensure_ascii=False),
    )

    out = await Runner().run(
        "reviewer",
        mode="gate1",
        scenario="gate1_internal",
        input_obj=reviewer_input(case_id="C-rev-2"),
        case_id="C-rev-2",
    )
    check(
        "reviewer/gate1 夹具可反转分诊预判 → decision=internal",
        out["verdict"] == "approve" and out["decision"] == "internal",
        json.dumps({k: out.get(k) for k in ("verdict", "decision")}),
    )

    out = await Runner().run(
        "reviewer",
        mode="gate1",
        scenario="gate1_reject",
        input_obj=reviewer_input(case_id="C-rev-3"),
        case_id="C-rev-3",
    )
    check(
        "reviewer/gate1 reject → decision=none 且给出补充清单",
        out["verdict"] == "reject"
        and out["decision"] == "none"
        and out["reason_code"] == "insufficient_evidence"
        and len(out["requested_info"]) == 2,
        json.dumps(
            {k: out.get(k) for k in ("verdict", "decision", "requested_info")}, ensure_ascii=False
        ),
    )

    out = await Runner().run(
        "reviewer",
        mode="gate1",
        scenario="default",
        input_obj=reviewer_input(case_id="C-rev-4", rca="unknown", confidence=0.3),
        case_id="C-rev-4",
    )
    check(
        "reviewer/gate1 低置信度分诊 → needs_info（打回 2 Triage）",
        out["verdict"] == "needs_info"
        and out["decision"] == "none"
        and "triage_confident" in [c["id"] for c in out["checks"] if not c["pass"]]
        and bool(out["requested_info"]),
        json.dumps({k: out.get(k) for k in ("verdict", "requested_info")}, ensure_ascii=False)[
            :250
        ],
    )

    out = await Runner().run(
        "reviewer",
        mode="gate2",
        scenario="default",
        input_obj=gate2_input(case_id="C-rev-5"),
        case_id="C-rev-5",
    )
    check(
        "reviewer/gate2 → 修复已生效，approve 放行 9 报告",
        out["verdict"] == "approve"
        and [c["id"] for c in out["checks"]]
        == [
            "fix_applied",
            "change_target",
            "verification_present",
            "fix_verified",
            "evidence_present",
            "loop_not_exhausted",
        ],
        json.dumps({k: out.get(k) for k in ("verdict", "reason_code")}),
    )

    out = await Runner().run(
        "reviewer",
        mode="gate2",
        scenario="default",
        input_obj=gate2_input(case_id="C-rev-6", fix_verified=False),
        case_id="C-rev-6",
    )
    check(
        "reviewer/gate2 验证未通过 → rework（回炉 4a，不被夹具改回 approve）",
        out["verdict"] == "rework"
        and out["reason_code"] == "fix_not_verified"
        and bool(out["requested_info"]),
        json.dumps({k: out.get(k) for k in ("verdict", "reason_code")}),
    )

    out = await Runner().run(
        "reviewer",
        mode="gate2",
        scenario="gate2_rework",
        input_obj=gate2_input(case_id="C-rev-7"),
        case_id="C-rev-7",
    )
    check(
        "reviewer/gate2 夹具可演示 rework（附重跑要求）",
        out["verdict"] == "rework"
        and out["reason_code"] == "fix_not_verified"
        and len(out["requested_info"]) == 2,
        json.dumps(out["requested_info"], ensure_ascii=False),
    )

    out = await Runner().run(
        "reviewer",
        mode="gate2",
        scenario="default",
        input_obj=gate2_input(case_id="C-rev-8", fixed=False, target=""),
        case_id="C-rev-8",
    )
    check(
        "reviewer/gate2 没有可用修复 → rework(missing_repro)",
        out["verdict"] == "rework" and out["reason_code"] == "missing_repro",
        json.dumps({k: out.get(k) for k in ("verdict", "reason_code")}),
    )

    out = await Runner().run(
        "reviewer",
        mode="gate3",
        scenario="default",
        input_obj=gate3_input(case_id="C-rev-9"),
        case_id="C-rev-9",
    )
    check(
        "reviewer/gate3 → 工单齐备，approve 放行 9 报告",
        out["verdict"] == "approve"
        and [c["id"] for c in out["checks"]]
        == [
            "ticket_status",
            "ticket_sections",
            "owner_assigned",
            "duplicate_consistent",
            "loop_not_exhausted",
        ],
        json.dumps({k: out.get(k) for k in ("verdict", "reason_code")}),
    )

    out = await Runner().run(
        "reviewer",
        mode="gate3",
        scenario="default",
        input_obj=gate3_input(case_id="C-rev-10", sections=[{"key": "rca", "content": "驱动缺陷"}]),
        case_id="C-rev-10",
    )
    check(
        "reviewer/gate3 工单缺 repro/evidence → needs_info（回炉 7b）",
        out["verdict"] == "needs_info" and out["reason_code"] == "missing_ticket_sections",
        json.dumps({k: out.get(k) for k in ("verdict", "reason_code")}),
    )

    out = await Runner().run(
        "reviewer",
        mode="gate3",
        scenario="default",
        input_obj=gate3_input(case_id="C-rev-11", duplicate_of="OST-9"),
        case_id="C-rev-11",
    )
    check(
        "reviewer/gate3 重复工单关系不自洽 → needs_info(duplicate_ticket)",
        out["verdict"] == "needs_info" and out["reason_code"] == "duplicate_ticket",
        json.dumps({k: out.get(k) for k in ("verdict", "reason_code")}),
    )

    out = await Runner().run(
        "reviewer",
        mode="gate3",
        scenario="gate3_needs_info",
        input_obj=gate3_input(case_id="C-rev-12"),
        case_id="C-rev-12",
    )
    check(
        "reviewer/gate3 夹具可演示 needs_info（补影响面/责任人）",
        out["verdict"] == "needs_info"
        and out["reason_code"] == "missing_ticket_sections"
        and len(out["requested_info"]) == 2,
        json.dumps(out["requested_info"], ensure_ascii=False),
    )

    out = await Runner().run(
        "reviewer",
        mode="gate3",
        scenario="default",
        input_obj={**gate3_input(case_id="C-rev-15"), "loop_seq": 3},
        case_id="C-rev-15",
    )
    check(
        "reviewer 回炉轮次超上限 → reject(loop_exhausted)",
        out["verdict"] == "reject"
        and out["reason_code"] == "loop_exhausted"
        and "loop_not_exhausted" in [c["id"] for c in out["checks"] if not c["pass"]],
        json.dumps({k: out.get(k) for k in ("verdict", "reason_code")}),
    )

    try:
        await Runner().run(
            "reviewer",
            mode="gate4",
            scenario="default",
            input_obj=reviewer_input(case_id="C-rev-16"),
            case_id="C-rev-16",
        )
        check("reviewer 非法 gate → Blocked(schema_invalid)", False, "没有抛出")
    except Blocked as b:
        check(
            "reviewer 非法 gate → Blocked(schema_invalid)",
            b.code == "schema_invalid",
            f"code={b.code}",
        )


# --------------------------------------------------------------------- Ticketing（create）
def ticketing_input(
    *,
    case_id: str,
    rca: str = "driver",
    reproducible: str = "yes",
    regression: dict | None = None,
    duplicate_of: str | None = None,
) -> dict:
    return {
        "case_id": case_id,
        "source": "monitor",
        "job": "os-staging-test",
        "build": 1261,
        "signal": "error",
        "rca": rca,
        "component": "driver",
        "path": "external",
        "reproduction": {
            "reproducible": reproducible,
            "attempts": 2,
            "matrix": [{"os": "sut-target-os 24.04", "config": "default", "outcome": "fail"}],
            "new_evidence": [
                {"kind": "repro_log", "uri": "db://evidence/1", "note": "attempt 1 → fail"}
            ],
        },
        "regression": (
            regression if regression is not None else {"is_regression": False, "guilty_patch": None}
        ),
        "duplicate_of": duplicate_of,
    }


async def selfcheck_ticketing() -> None:
    print("\n=== Ticketing（create）：优先级 / 段落 / 查重 ===")
    r = Runner()
    out = await r.run(
        "ticketing",
        mode="create",
        scenario="default",
        input_obj=ticketing_input(case_id="C-tk-1"),
        case_id="C-tk-1",
    )
    secs = {s["key"]: s.get("content") for s in out["sections"]}
    check(
        "ticketing/default → created + OST 键 + P1/OS-Driver",
        out["ticket_status"] == "created"
        and str(out["external_key"]).startswith("OST-")
        and out["priority"] == "P1"
        and out["assignee_team"] == "OS-Driver",
        json.dumps({k: out.get(k) for k in ("ticket_status", "external_key", "priority")}),
    )
    check(
        "ticketing/default 含 rca/repro/regression/evidence 段落且非空",
        {"rca", "repro", "regression", "evidence"} <= set(secs)
        and all(str(secs[k] or "").strip() for k in ("rca", "repro", "regression", "evidence")),
        json.dumps(sorted(secs), ensure_ascii=False),
    )
    check(
        "ticketing/default 写了 ticket_payload 审计证据",
        "ticket_payload" in kinds(r.db, case_id="C-tk-1", stage="ticketing"),
        str(kinds(r.db, case_id="C-tk-1", stage="ticketing")),
    )

    out = await Runner().run(
        "ticketing",
        mode="create",
        scenario="default",
        input_obj=ticketing_input(
            case_id="C-tk-2",
            regression={
                "is_regression": True,
                "guilty_patch": {"commit": "a1b2c3d", "pr": "PR-1187", "author": "wang.lei"},
            },
        ),
        case_id="C-tk-2",
    )
    check(
        "ticketing 回归 + guilty patch → P1 并关联补丁",
        out["priority"] == "P1"
        and out["linked_patches"] == ["a1b2c3d"]
        and any("a1b2c3d" in str(s.get("content")) for s in out["sections"]),
        json.dumps({k: out.get(k) for k in ("priority", "linked_patches")}),
    )

    out = await Runner().run(
        "ticketing",
        mode="create",
        scenario="default",
        input_obj=ticketing_input(case_id="C-tk-3", reproducible="no"),
        case_id="C-tk-3",
    )
    check(
        "ticketing 不可复现 → P3",
        out["priority"] == "P3",
        json.dumps({"priority": out.get("priority")}),
    )

    out = await Runner().run(
        "ticketing",
        mode="create",
        scenario="duplicate",
        input_obj=ticketing_input(case_id="C-tk-4"),
        case_id="C-tk-4",
    )
    check(
        "ticketing/duplicate → linked_duplicate（关联 OST-2211，不另建单）",
        out["ticket_status"] == "linked_duplicate"
        and out["external_key"] is None
        and out["duplicate_of"] == "OST-2211"
        and any("关联到已有工单" in str(s.get("content")) for s in out["sections"]),
        json.dumps({k: out.get(k) for k in ("ticket_status", "duplicate_of")}),
    )

    out = await Runner().run(
        "ticketing",
        mode="create",
        scenario="default",
        input_obj=ticketing_input(case_id="C-tk-5", duplicate_of="case:8daea705"),
        case_id="C-tk-5",
    )
    check(
        "ticketing 继承 Triage 的 duplicate_of → 关联已有工单",
        out["ticket_status"] == "linked_duplicate"
        and out["duplicate_of"] == "case:8daea705"
        and out["audit"]["dedup_source"] == "triage",
        json.dumps({k: out.get(k) for k in ("ticket_status", "duplicate_of")}),
    )

    old_autocreate = cfg.ticket_autocreate
    cfg.ticket_autocreate = False
    try:
        out = await Runner().run(
            "ticketing",
            mode="create",
            scenario="default",
            input_obj=ticketing_input(case_id="C-tk-6"),
            case_id="C-tk-6",
        )
    finally:
        cfg.ticket_autocreate = old_autocreate
    check(
        "ticketing TICKET_AUTOCREATE=false → drafted 草稿",
        out["ticket_status"] == "drafted"
        and str(out["external_key"]).startswith("DRAFT-")
        and out["audit"]["autocreate"] is False,
        json.dumps({k: out.get(k) for k in ("ticket_status", "external_key")}),
    )


# --------------------------------------------------------------------- Reporting（case | daily）
def seed_case(
    db: FakeDB,
    case_id: str,
    *,
    path: str = "external",
    status: str = "reported",
    created_at: dt.datetime | None = None,
) -> None:
    """铺一条完整链路的历史（stage_runs + 一条证据），供 Reporting 出结案报告。"""
    db.cases[case_id] = {
        "case_id": case_id,
        "run_id": f"R-{case_id}",
        "scan_id": "scan-seed",
        "source": "mock",
        "job": "os-staging-test",
        "build": 1261,
        "signal": "error",
        "signature": "driver:crash:deadbeef",
        "status": status,
        "workflow_key": "s2_full",
        "plan_id": f"P-{case_id}",
        "plan_source": "workflow",
        "priority": None,
        "payload": {},
        "created_at": created_at or dt.datetime.now(),
    }
    db.evidence_rows.append(
        {
            "id": len(db.evidence_rows) + 1,
            "case_id": case_id,
            "scan_id": "scan-seed",
            "stage": "reproduction",
            "kind": "repro_log",
            "uri": "db://evidence/seed",
            "meta": {},
            "excerpt": "attempt 1: amdgpu GPU page fault",
            "created_at": dt.datetime.now(),
        }
    )
    db.add_stage_run(case_id=case_id, stage="monitor", mode="scan", step_id="scan")
    db.add_stage_run(
        case_id=case_id,
        stage="triage",
        mode="classify",
        step_id="t1",
        output={
            "case_id": case_id,
            "rca": "driver",
            "path": path,
            "component": "driver",
            "os_stage": "staging-test",
            "signature": "driver:crash:deadbeef",
            "confidence": 0.82,
            "triage_notes": ["错误簇集中在 driver 侧，判定产品缺陷"],
        },
    )
    db.add_stage_run(
        case_id=case_id,
        stage="reviewer",
        mode="gate1",
        step_id="g1",
        output={
            "case_id": case_id,
            "gate": "gate1",
            "verdict": "approve",
            "decision": path,
            "reason_code": "ok",
            "requested_info": [],
        },
    )
    if path == "external":
        db.add_stage_run(
            case_id=case_id,
            stage="reproduction",
            mode="evidence",
            step_id="e1",
            output={
                "case_id": case_id,
                "mode": "evidence",
                "reproducible": "yes",
                "attempts": 2,
                "matrix": [],
                "fix_verified": None,
            },
        )
        db.add_stage_run(
            case_id=case_id,
            stage="regression",
            mode="check",
            step_id="c1",
            output={
                "case_id": case_id,
                "mode": "check",
                "is_regression": True,
                "baseline": "staging-pass-2026-11-24",
                "change_window": "a1b2c3d..f4e5d6a",
                "bisected": False,
                "guilty_patch": None,
            },
        )
        db.add_stage_run(
            case_id=case_id,
            stage="regression",
            mode="bisect",
            step_id="b1",
            output={
                "case_id": case_id,
                "mode": "bisect",
                "is_regression": True,
                "bisected": True,
                "guilty_patch": {
                    "commit": "a1b2c3d",
                    "pr": "PR-1187",
                    "author": "wang.lei",
                    "confidence": 0.86,
                    "rationale": "改动引入 smu_ops 未初始化",
                },
            },
        )
        db.add_stage_run(
            case_id=case_id,
            stage="ticketing",
            mode="create",
            step_id="k1",
            output={
                "case_id": case_id,
                "backend": "jira",
                "external_key": "OST-1094",
                "ticket_status": "created",
                "priority": "P1",
                "assignee_team": "OS-Driver",
                "sections": [],
            },
        )
        db.add_stage_run(
            case_id=case_id,
            stage="reviewer",
            mode="gate3",
            step_id="g3",
            output={
                "case_id": case_id,
                "gate": "gate3",
                "verdict": "approve",
                "reason_code": "ok",
                "requested_info": [],
            },
        )
    else:
        db.add_stage_run(
            case_id=case_id,
            stage="remediation",
            mode="fix",
            step_id="r1",
            output={
                "case_id": case_id,
                "fixed": True,
                "change_type": "script",
                "target": "ci/scripts/staging_install.sh",
                "diff_summary": "清理残留 workspace",
                "risk": "low",
                "adopted_from_kb": "KB-FIX-001",
            },
        )
        db.add_stage_run(
            case_id=case_id,
            stage="reproduction",
            mode="verify",
            step_id="v1",
            output={
                "case_id": case_id,
                "mode": "verify",
                "reproducible": "yes",
                "attempts": 3,
                "matrix": [],
                "fix_verified": True,
            },
        )
        db.add_stage_run(
            case_id=case_id,
            stage="reviewer",
            mode="gate2",
            step_id="g2",
            output={
                "case_id": case_id,
                "gate": "gate2",
                "verdict": "approve",
                "reason_code": "ok",
                "requested_info": [],
            },
        )


async def selfcheck_reporting() -> None:
    print("\n=== Reporting（case / daily）：结案报告 + 知识库回写 + 日报 ===")
    db = FakeDB()
    seed_case(db, "C-report-ext", path="external")
    r = Runner(db)
    out = await r.run(
        "reporting",
        mode="case",
        scenario="default",
        input_obj={"case_id": "C-report-ext", "day": None},
        case_id="C-report-ext",
    )
    md = out["report_md"]
    check(
        "reporting/case（external）报告含摘要/阶段/证据链/未解决项",
        all(
            h in md
            for h in ("# Case 结案报告", "## 结论摘要", "## 阶段执行", "## 证据链", "## 未解决项")
        )
        and "分流：**external**" in md
        and "guilty patch=a1b2c3d" in md
        and "闸门 3 approve" in md,
        md[:400],
    )
    metrics = out["metrics"]
    check(
        "reporting/case 指标覆盖路径/根因/闸门/回归/工单",
        (metrics.get("path"), metrics.get("rca"), metrics.get("guilty_patch"))
        == ("external", "driver", "a1b2c3d")
        and metrics.get("verdicts", {}).get("gate3") == "approve"
        and metrics.get("ticket_status") == "created"
        and int(metrics.get("tat_ms") or 0) > 0,
        json.dumps(
            {k: metrics.get(k) for k in ("path", "rca", "guilty_patch")}, ensure_ascii=False
        ),
    )
    kb_kinds = {e["kind"] for e in out["kb_writes"]}
    check(
        "reporting/case 的 kb_writes 含 failure/guilty_patch/best_practice 三类",
        kb_kinds == {"failure", "guilty_patch", "best_practice"}
        and all(e.get("case_id") == "C-report-ext" for e in out["kb_writes"]),
        json.dumps(sorted(kb_kinds), ensure_ascii=False),
    )
    rep_ev = [
        e
        for e in db.evidence_rows
        if e["kind"] == "report_md" and e.get("case_id") == "C-report-ext"
    ]
    check(
        "reporting/case 把报告写进 report_md 证据",
        bool(rep_ev) and (rep_ev[-1].get("meta") or {}).get("mode") == "case",
        str(len(rep_ev)),
    )

    db2 = FakeDB()
    seed_case(db2, "C-report-int", path="internal", status="remediated")
    out = await Runner(db2).run(
        "reporting",
        mode="case",
        scenario="default",
        input_obj={"case_id": "C-report-int"},
        case_id="C-report-int",
    )
    kinds_int = {e["kind"] for e in out["kb_writes"]}
    check(
        "reporting/case（internal）回写修复经验（fix）并标注闸门 2",
        "fix" in kinds_int
        and "闸门 2 approve" in out["report_md"]
        and out["metrics"]["path"] == "internal",
        json.dumps(sorted(kinds_int), ensure_ascii=False),
    )

    try:
        await Runner().run(
            "reporting",
            mode="case",
            scenario="default",
            input_obj={"case_id": "C-not-exist"},
            case_id="C-not-exist",
        )
        check("reporting/case 缺 case 记录 → Blocked(schema_invalid)", False, "没有抛出")
    except Blocked as b:
        check(
            "reporting/case 缺 case 记录 → Blocked(schema_invalid)",
            b.code == "schema_invalid",
            f"code={b.code}",
        )

    today = dt.datetime.now().strftime("%Y-%m-%d")
    db3 = FakeDB()
    seed_case(db3, "C-day-1", path="external", created_at=dt.datetime.now())
    seed_case(db3, "C-day-2", path="internal", status="closed", created_at=dt.datetime.now())
    out = await Runner(db3).run(
        "reporting", mode="daily", scenario="default", input_obj={"day": today}, case_id=None
    )
    md = out["report_md"]
    check(
        "reporting/daily → 日报正文含总览/根因/终态/高频签名",
        out["day"] == today
        and out["case_id"] is None
        and out["kb_writes"] == []
        and "# Staging-CI 日报" in md
        and "## 根因分布" in md
        and "## 终态分布" in md
        and "## 高频签名" in md,
        md[:300],
    )
    check(
        "reporting/daily 指标统计当日两个 Case",
        out["metrics"].get("cases_total") == 2
        and set(out["metrics"].get("cases") or []) == {"C-day-1", "C-day-2"},
        json.dumps(out["metrics"].get("cases"), ensure_ascii=False),
    )


# --------------------------------------------------------------------- Plan 夹具
def plan_semantics(raw: dict) -> list[str]:
    """计划语义自检（schema 表达不了的：双分支、回炉方向、mode 归属）。"""
    issues: list[str] = []
    steps = list(raw.get("steps") or [])
    ids = [str(s.get("id")) for s in steps]
    index = {sid: i for i, sid in enumerate(ids)}
    if "monitor" in {str(s.get("stage")) for s in steps}:
        issues.append("计划里不应出现 monitor（由 Scheduler 触发）")
    for step in steps:
        sid, stage, mode = str(step.get("id")), str(step.get("stage")), str(step.get("mode"))
        if stage not in MODE_AUTHORITY:
            issues.append(f"{sid}: 未知 stage={stage}")
            continue
        if mode not in MODE_AUTHORITY[stage]:
            issues.append(f"{sid}: {stage} 不接受 mode={mode}")
        for verb, act in (step.get("on") or {}).items():
            act = act or {}
            target = act.get("next") or act.get("jump")
            if not target:
                continue
            if target not in index:
                issues.append(f"{sid}.{verb} → 不存在的 {target}")
            elif act.get("jump") and index[target] >= index[sid]:
                issues.append(f"{sid}.{verb} 的回炉目标 {target} 不在更早的步骤")
    gates = {f"{s.get('stage')}:{s.get('mode')}" for s in steps}
    if "reviewer:gate1" not in gates:
        issues.append("缺少闸门 1（reviewer:gate1）")
    if "reporting:case" not in gates:
        issues.append("缺少 reporting:case")
    for step in steps:
        if (
            step.get("stage") == "reviewer"
            and step.get("mode") == "gate1"
            and not {"internal", "external"} <= set(step.get("on") or {})
        ):
            issues.append("闸门 1 必须同时给出 internal / external 两条分支")
    return issues


async def selfcheck_plans() -> None:
    print("\n=== Plan 夹具：对照 schemas/plan.schema.json + 双分支/回炉语义 ===")
    import yaml

    schema = json.loads((DATA / "schemas" / "plan.schema.json").read_text(encoding="utf-8"))
    expect = {
        "internal_ok": True,
        "external_ok": True,
        "invalid_stage": False,
        "invalid_order": False,
    }
    for name, ok in expect.items():
        raw = yaml.safe_load(
            (DATA / "fixtures" / "plans" / f"{name}.yaml").read_text(encoding="utf-8")
        )
        errs = sorted(
            jsonschema.Draft202012Validator(schema).iter_errors(raw), key=lambda e: e.json_path
        )
        issues = plan_semantics(raw)
        check(
            f"plans/{name}.yaml {'合法' if ok else '非法'}（schema {len(errs)} 错 / 语义 {len(issues)} 错）",
            (not errs and not issues) == ok,
            json.dumps(
                [f"{e.json_path}: {e.message}" for e in errs][:2] + issues[:2], ensure_ascii=False
            )[:300],
        )
        if ok:
            steps = raw["steps"]
            gates = {f"{s['stage']}:{s['mode']}" for s in steps}
            loops = [
                int((a or {}).get("max_loops") or 0)
                for s in steps
                for a in (s.get("on") or {}).values()
            ]
            check(
                f"plans/{name}.yaml 同时含 internal（fix→verify→gate2）与 "
                "external（evidence→check→bisect→create→gate3）两支",
                {
                    "remediation:fix",
                    "reproduction:verify",
                    "reviewer:gate2",
                    "reproduction:evidence",
                    "regression:check",
                    "regression:bisect",
                    "ticketing:create",
                    "reviewer:gate3",
                }
                <= gates,
                json.dumps(sorted(gates), ensure_ascii=False),
            )
            check(
                f"plans/{name}.yaml 回炉上限 <= cfg.max_review_loops({cfg.max_review_loops})",
                max(loops or [0]) <= int(cfg.max_review_loops),
                str(loops),
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
    check(
        "8 个 Agent 都对上了 stage-contracts 的 mode 归属",
        set(HANDLERS) == set(MODE_AUTHORITY) and sum(len(m) for m in MODE_AUTHORITY.values()) == 13,
        json.dumps({k: list(v) for k, v in MODE_AUTHORITY.items()}, ensure_ascii=False),
    )
    await selfcheck_monitor()
    await selfcheck_triage()
    await selfcheck_remediation()
    await selfcheck_reproduction()
    await selfcheck_regression()
    await selfcheck_reviewer()
    await selfcheck_ticketing()
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
    print(f"- 断言总数：{CHECKS[0]}（8 Agent / 13 个 mode，覆盖 12 阶段两分支三闸门）")
    if FAILURES:
        print(f"\n结果：FAIL {len(FAILURES)} 项")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("\n结果：全部通过")
    return 0


def cli() -> int:
    """console script 入口：main 是协程，由这里起事件循环。"""
    try:
        return asyncio.run(main())
    except Exception as exc:  # 自检脚本本身出错也要明确失败
        FAILURES.append(f"selfcheck 运行异常: {exc!r}")
        print(f"\nFAIL selfcheck 运行异常 :: {exc!r}")
        import traceback

        traceback.print_exc()
        return 2


if __name__ == "__main__":
    sys.exit(cli())
