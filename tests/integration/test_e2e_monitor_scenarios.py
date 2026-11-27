"""端到端（集成）测试：覆盖 8 Agent × 13 mode 的 12 阶段两分支三闸门。

用例分四层：

1. **Monitor 场景矩阵**（19 个 ci_feed 夹具）：计数、建 Case 数、终态；
2. **链路形态**：观察链（s1）/ 外部分支（s2 全链）/ 内部分支 / 测试免复现（s3）；
3. **三道闸门**：gate1 回炉与驳回、gate3 工单质量、timeout 与 blocked；
4. **计划层 / 幂等 / DLQ / 重放 / API / Web UI**。

前置：编排层与 8 个 Agent 已在跑（本机 scripts/run_local.sh 或 k8s 部署），
且环境变量 ORCH_BASE_URL / PG_DSN 指向它。凭据从 deploy/secrets.env 来，不在代码里写死。

    # 本机栈
    PG_DSN="postgresql://postgres:…@10.96.75.0:5432/ci_triage" pytest -m integration -q
    # 集群（NodePort）
    ORCH_BASE_URL=http://172.31.0.3:30099 PG_DSN="postgresql://postgres:…@10.96.75.0:5432/ci_triage" \\
        pytest -m integration -q
"""

from __future__ import annotations

import os
import time
from typing import Any

import asyncpg
import pytest
import requests

pytestmark = pytest.mark.integration

BASE = os.environ.get("ORCH_BASE_URL", "http://127.0.0.1:8080")
PG = os.environ.get("PG_DSN", "postgresql://postgres@127.0.0.1:5432/ci_triage")
TERMINAL = {"closed", "closed_rejected", "blocked"}


def api(path: str, **kw) -> Any:
    # 扫描类接口 wait=true 会一直等到 Case 终态（timeout 场景要 2 分钟以上），允许单点放宽超时
    timeout = kw.pop("timeout", 120)
    r = requests.request(kw.pop("method", "GET"), f"{BASE}{path}", timeout=timeout, **kw)
    if r.status_code >= 400:
        raise AssertionError(f"{path} -> {r.status_code}: {r.text[:400]}")
    return r.json() if "json" in r.headers.get("content-type", "") else r.text


async def _fetch(sql: str, *args) -> list[dict]:
    conn = await asyncpg.connect(PG, timeout=20)
    for typ in ("jsonb", "json"):
        await conn.set_type_codec(
            typ,
            schema="pg_catalog",
            format="text",
            encoder=lambda v: (
                v
                if v is None or isinstance(v, (str, bytes))
                else __import__("json").dumps(v, ensure_ascii=False)
            ),
            decoder=__import__("json").loads,
        )
    try:
        return [dict(r) for r in await conn.fetch(sql, *args)]
    finally:
        await conn.close()


def q(sql: str, *args) -> list[dict]:
    import asyncio

    return asyncio.run(_fetch(sql, *args))


def q1(sql: str, *args) -> dict | None:
    rows = q(sql, *args)
    return rows[0] if rows else None


def trigger_scan(scenario: str) -> dict:
    res = api(
        "/api/scan",
        method="POST",
        json={"scenario": scenario, "lookback": "24h", "wait": True},
        timeout=300,
    )
    assert res["status"] in ("done", "blocked"), res
    return res


def wait_cases(scan_id: str, expect: int, timeout_s: int = 240) -> list[dict]:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        rows = q("select * from cases where scan_id=$1 order by created_at", scan_id)
        if len(rows) >= expect and all(r["status"] in TERMINAL for r in rows):
            return rows
        time.sleep(2)
    rows = q("select * from cases where scan_id=$1", scan_id)
    raise AssertionError(
        f"扫描 {scan_id} 的 Case 未全部终态：{[(r['case_id'][:8], r['status']) for r in rows]}"
    )


def runs_of(case_id: str) -> list[dict]:
    return q(
        "select stage, mode, step_id, attempt, status, error, output"
        " from stage_runs where case_id=$1 order by id",
        case_id,
    )


def stages_of(case_id: str) -> list[str]:
    return [r["stage"] for r in runs_of(case_id)]


def path_of(case_id: str) -> list[tuple[str, str]]:
    """实际执行过的 (stage, mode) 序列——分支是否正确，看这个。"""
    return [(r["stage"], r["mode"]) for r in runs_of(case_id)]


def output_of(case_id: str, stage: str, mode: str | None = None) -> dict:
    rows = [r for r in runs_of(case_id) if r["stage"] == stage]
    if mode:
        rows = [r for r in rows if r["mode"] == mode]
    assert rows, f"{case_id} 没有 {stage}:{mode} 的执行记录"
    return rows[-1]["output"] or {}


def reviews_of(case_id: str, gate: str | None = None) -> list[dict]:
    # reviews 表只有 verdict/reason_code/checks/quality/loop_seq；decision 在 reviewer 的阶段输出里
    sql = "select gate, verdict, reason_code, loop_seq, checks from reviews where case_id=$1"
    rows = q(sql + " order by id", case_id)
    return [r for r in rows if r["gate"] == gate] if gate else rows


def gate_outputs(case_id: str, gate: str) -> list[dict]:
    """reviewer 每次过闸门的完整输出（decision/quality_scores 只在这里）。"""
    return [
        r["output"] or {}
        for r in runs_of(case_id)
        if r["stage"] == "reviewer" and r["mode"] == gate
    ]


def trace_rows(case_id: str) -> list[dict]:
    return api(f"/api/cases/{case_id}/trace")["items"]


# --------------------------------------------------------------------------- Monitor 场景矩阵
SCENARIOS: dict[str, dict[str, Any]] = {
    "all_success": {"observed": 3, "promoted": 0, "suppressed": 0, "cases": 3, "signals": {"none"}},
    "new_failure": {
        "observed": 3,
        "promoted": 1,
        "suppressed": 0,
        "cases": 3,
        "signals": {"error", "none"},
    },
    "known_issue_noise": {
        "observed": 3,
        "promoted": 0,
        "suppressed": 3,
        "cases": 0,
        "signals": set(),
    },
    "mixed": {
        "observed": 4,
        "promoted": 1,
        "suppressed": 1,
        "cases": 3,
        "signals": {"error", "none"},
    },
    "duplicate_merge": {
        "observed": 4,
        "promoted": 1,
        "suppressed": 3,
        "cases": 1,
        "signals": {"error"},
    },
    "flaky_unstable": {
        "observed": 1,
        "promoted": 1,
        "suppressed": 0,
        "cases": 1,
        "signals": {"error"},
    },
    "aborted_infra": {
        "observed": 1,
        "promoted": 1,
        "suppressed": 0,
        "cases": 1,
        "signals": {"error"},
    },
    "many_errors": {
        "observed": 5,
        "promoted": 5,
        "suppressed": 0,
        "cases": 5,
        "signals": {"error"},
    },
    "blocked_no_machine": {
        "observed": 1,
        "promoted": 1,
        "suppressed": 0,
        "cases": 1,
        "signals": {"error"},
    },
    "timeout": {"observed": 1, "promoted": 1, "suppressed": 0, "cases": 1, "signals": {"error"}},
    "not_reproducible": {
        "observed": 1,
        "promoted": 1,
        "suppressed": 0,
        "cases": 1,
        "signals": {"error"},
    },
    "needs_info": {"observed": 1, "promoted": 1, "suppressed": 0, "cases": 1, "signals": {"error"}},
    "insufficient": {
        "observed": 1,
        "promoted": 1,
        "suppressed": 0,
        "cases": 1,
        "signals": {"error"},
    },
    "reject": {"observed": 1, "promoted": 1, "suppressed": 0, "cases": 1, "signals": {"error"}},
    "duplicate": {"observed": 1, "promoted": 1, "suppressed": 0, "cases": 1, "signals": {"error"}},
    "unknown_low_confidence": {
        "observed": 1,
        "promoted": 1,
        "suppressed": 0,
        "cases": 1,
        "signals": {"error"},
    },
    "llm_warming": {
        "observed": 1,
        "promoted": 1,
        "suppressed": 0,
        "cases": 1,
        "signals": {"error"},
    },
    "llm_invalid": {
        "observed": 1,
        "promoted": 1,
        "suppressed": 0,
        "cases": 1,
        "signals": {"error"},
    },
    "source_unavailable": {
        "observed": 0,
        "promoted": 0,
        "suppressed": 0,
        "cases": 0,
        "signals": set(),
        "scan_status": "blocked",
    },
}


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
def test_monitor_scenario_end_to_end(scenario: str):
    exp = SCENARIOS[scenario]
    res = trigger_scan(scenario)
    counts = res["counts"]
    scan_id = res["scan_id"]

    assert res["status"] == exp.get("scan_status", "done")
    assert counts["observed"] == exp["observed"], f"{scenario}: observed {counts}"
    assert counts["suppressed"] == exp["suppressed"], f"{scenario}: suppressed {counts}"
    if exp.get("scan_status") != "blocked":
        assert (
            counts.get("errors_promoted", counts.get("promoted", 0)) == exp["promoted"]
        ), f"{scenario}: promoted {counts}"
    assert len(res["cases"]) == exp["cases"], f"{scenario}: 建 Case 数 {res['cases']}"

    rows = wait_cases(scan_id, exp["cases"])
    assert len(rows) == exp["cases"]
    if exp["signals"]:
        assert {r["signal"] for r in rows} == exp["signals"]

    # Monitor 自身的 stage_run 记录（case_id 为空，靠 run_id 串联）
    scan_runs = q("select stage, status, error from stage_runs where scan_id=$1", scan_id)
    mon = [r for r in scan_runs if r["stage"] == "monitor"]
    assert len(mon) == 1
    if exp.get("scan_status") == "blocked":
        assert mon[0]["status"] == "blocked"
        assert (mon[0]["error"] or {}).get("code") == "source_unavailable"
        assert not rows, "blocked 扫描不应建 Case"
        assert q("select 1 from scans where scan_id=$1 and status='blocked'", scan_id)
        return

    assert mon[0]["status"] == "done"
    # 每个 Case 的计划来源与终态
    for r in rows:
        assert r["plan_id"], f"Case {r['case_id']} 没有封存计划"
        assert r["plan_source"] in ("workflow", "llm")
        assert (r["progress"] or {}).get("step_status"), "progress.step_status 为空"
        assert stages_of(r["case_id"]), "无 stage_run 记录"
    if exp["cases"]:
        assert any(r["status"] in TERMINAL for r in rows)
    # 同一扫描的 Case 共享 run_id（同一 correlation 链）
    assert {r["run_id"] for r in rows} == {res["run_id"]} if exp["cases"] else True


# --------------------------------------------------------------------------- 链路形态（关键设计断言）
def test_s1_observe_only_stops_after_triage_and_reporting():
    """无报错（signal=none）：只走 Triage → Reporting，绝不出现下游命令。"""
    res = trigger_scan("all_success")
    rows = wait_cases(res["scan_id"], 3)
    for c in rows:
        assert c["status"] == "closed", c["blocked_reason"]
        assert c["progress"]["workflow_key"] == "s1_observe_only", c["progress"]
        assert path_of(c["case_id"]) == [("triage", "classify"), ("reporting", "case")]
        out = output_of(c["case_id"], "triage", "classify")
        assert (
            out["rca"] == "unknown"
        ), out  # 契约枚举：driver/test/environment/infrastructure/unknown
        # 无报错 ⇒ 计划只有 2 步，不占用复现机位
        assert [x["mode"] for x in c["progress"]["plan_steps"]] == ["classify", "case"]
        assert out["confidence"] < 0.7, out


def test_s2_full_external_branch_runs_all_stages_in_order():
    """含报错 + 产品侧缺陷：闸门 1 判 external → 取证 → 回归 → 建单 → 闸门 3 → 报告。"""
    res = trigger_scan("new_failure")
    err = next(r for r in wait_cases(res["scan_id"], 3) if r["signal"] == "error")
    assert err["progress"]["workflow_key"] == "s2_full"
    assert err["status"] == "closed", err["blocked_reason"]
    assert path_of(err["case_id"]) == [
        ("triage", "classify"),
        ("reviewer", "gate1"),
        ("reproduction", "evidence"),
        ("regression", "check"),
        ("regression", "bisect"),
        ("ticketing", "create"),
        ("reviewer", "gate3"),
        ("reporting", "case"),
    ], path_of(err["case_id"])

    gate1 = reviews_of(err["case_id"], "gate1")[-1]
    assert gate1["verdict"] == "approve", gate1
    assert gate_outputs(err["case_id"], "gate1")[-1]["decision"] == "external", gate1
    gate3 = reviews_of(err["case_id"], "gate3")[-1]
    assert gate3["verdict"] == "approve", gate3

    tk = q1("select * from tickets where case_id=$1", err["case_id"])
    assert tk and tk["ticket_status"] in ("created", "linked_duplicate"), tk
    assert q(
        "select 1 from stage_runs where case_id=$1 and stage='reporting' and status='done'",
        err["case_id"],
    )


def test_known_issue_noise_is_suppressed_with_evidence():
    res = trigger_scan("known_issue_noise")
    assert res["counts"]["suppressed"] == 3
    ev = q("select stage, kind from evidence where scan_id=$1", res["scan_id"])
    assert any(e["stage"] == "monitor" for e in ev)
    reasons = [o.get("promoted_reason") for o in res["observations"]]
    assert all(r is None for r in reasons)
    sup = [o for o in res["observations"] if o.get("suppressed")]
    assert len(sup) == 3 and all(o.get("known_issue_ref") for o in sup), sup
    assert all(
        (o.get("context") or {}).get("suppressed_reason") == "known_issue_noise" for o in sup
    ), sup
    assert all(o.get("promoted_reason") is None for o in sup), "被抑制的观测不应有 promoted_reason"


def test_duplicate_merge_creates_one_case_and_skips_controlled_repro():
    res = trigger_scan("duplicate_merge")
    row = wait_cases(res["scan_id"], 1)[0]
    assert res["counts"]["suppressed"] == 3
    assert row["signature"].startswith("test:"), row["signature"]
    obs = (row["payload"] or {})["observation"]
    assert obs["context"].get("duplicate_merge_count", 0) >= 2, obs["context"]
    assert obs["context"].get("duplicate_builds"), obs["context"]
    # 注册表匹配 test:* 家族 → 跳过 4b 受控复现，只走内部修复链的 5a 验证
    assert row["progress"]["workflow_key"] == "s3_test_no_repro", row["progress"]
    assert path_of(row["case_id"]) == [
        ("triage", "classify"),
        ("reviewer", "gate1"),
        ("remediation", "fix"),
        ("reproduction", "verify"),
        ("reviewer", "gate2"),
        ("reporting", "case"),
    ], path_of(row["case_id"])
    assert row["status"] == "closed", row["blocked_reason"]


def test_aborted_infra_takes_internal_branch_without_evidence_run():
    """infra 中断判 internal：走 4a 修复 → 5a verify（不是 4b evidence）。"""
    res = trigger_scan("aborted_infra")
    row = wait_cases(res["scan_id"], 1)[0]
    triage_out = output_of(row["case_id"], "triage", "classify")
    assert triage_out["rca"] == "infrastructure", triage_out
    assert row["progress"]["workflow_key"] == "s2_full", row["progress"]
    modes = path_of(row["case_id"])
    assert ("remediation", "fix") in modes, modes
    assert ("reproduction", "verify") in modes, modes
    assert ("reproduction", "evidence") not in modes, modes
    assert ("ticketing", "create") not in modes, modes
    assert row["status"] == "closed", row["blocked_reason"]


def test_flaky_unstable_marks_reproduction_flaky():
    res = trigger_scan("flaky_unstable")
    row = wait_cases(res["scan_id"], 1)[0]
    obs = res["observations"][0]
    assert obs.get("promoted_reason") == "unstable_result", obs
    out = output_of(row["case_id"], "reproduction", "evidence")
    assert out["reproducible"] == "flaky", out
    # 复现矩阵：同参数三次重建，中间一次通过 → 不稳定度（契约字段是 matrix[].outcome）
    outcomes = [m["outcome"] for m in out["matrix"]]
    assert outcomes == ["fail", "pass", "fail"], outcomes
    assert out["attempts"] == 3, out


def test_many_errors_all_terminal_and_signatures_distinct():
    res = trigger_scan("many_errors")
    rows = wait_cases(res["scan_id"], 5, timeout_s=300)
    assert len(rows) == 5
    assert all(r["status"] in TERMINAL for r in rows)
    # 五个签名互不相同（不同家族），说明指纹去重没有误并
    assert len({r["signature"] for r in rows}) == 5, [r["signature"] for r in rows]
    # 内部/外部两条分支都出现过：environment 类（0.72）也能过闸门 1
    modes = {m for r in rows for m in path_of(r["case_id"])}
    assert ("remediation", "fix") in modes and ("reproduction", "evidence") in modes, modes


def test_reproduction_blocked_case_ends_blocked():
    res = trigger_scan("blocked_no_machine")
    row = wait_cases(res["scan_id"], 1)[0]
    assert row["status"] == "blocked", row["blocked_reason"]
    runs = runs_of(row["case_id"])
    repro = [r for r in runs if r["stage"] == "reproduction"]
    assert repro and repro[0]["status"] == "blocked", repro
    assert (repro[0]["error"] or {}).get("code") == "source_unavailable", repro
    assert "ticketing" not in [r["stage"] for r in runs], "blocked 后不应继续下游"


def test_source_unavailable_scan_is_blocked_and_recoverable():
    bad = trigger_scan("source_unavailable")
    assert bad["status"] == "blocked"
    assert bad["counts"]["cases_created"] == 0
    # 恢复：再来一次正常扫描必须成功
    good = trigger_scan("all_success")
    assert good["status"] == "done"
    wait_cases(good["scan_id"], 3)


# --------------------------------------------------------------------------- 三道闸门
def test_gate1_needs_info_loops_back_to_triage_then_approves():
    """needs_info 夹具只在第 1 轮生效：回炉重分诊后放行，走完整外部链。"""
    res = trigger_scan("needs_info")
    row = wait_cases(res["scan_id"], 1)[0]
    assert row["status"] == "closed", row["blocked_reason"]
    stages = stages_of(row["case_id"])
    assert stages.count("triage") == 2, f"应回炉一次重跑 triage：{stages}"
    assert stages[-1] == "reporting", stages
    assert row["progress"]["review_loop_seq"] == 1, row["progress"]

    gate1 = reviews_of(row["case_id"], "gate1")
    assert [r["verdict"] for r in gate1] == ["needs_info", "approve"], gate1
    assert gate1[0]["reason_code"] == "insufficient_evidence", gate1[0]
    # 机械预检其实是全通过的，是夹具把结论改严为 needs_info
    assert all(c["pass"] for c in gate1[0]["checks"]), gate1[0]["checks"]
    assert gate_outputs(row["case_id"], "gate1")[-1]["decision"] == "external"
    # 回炉后下游确实重新跑过（不是复用旧结论）
    assert ("ticketing", "create") in path_of(row["case_id"])


def test_gate1_insufficient_exhausts_loops_and_rejects():
    """每轮都判证据不足 → 回炉预算耗尽 → reject(loop_exhausted)。"""
    res = trigger_scan("insufficient")
    row = wait_cases(res["scan_id"], 1)[0]
    assert row["status"] == "closed_rejected", row["blocked_reason"]
    assert row["progress"]["review_loop_seq"] == 2, row["progress"]
    gate1 = reviews_of(row["case_id"], "gate1")
    assert [r["verdict"] for r in gate1] == ["needs_info", "needs_info", "reject"], gate1
    assert gate1[-1]["reason_code"] == "loop_exhausted", gate1[-1]
    assert "reporting" not in stages_of(row["case_id"])


def test_gate1_reject_closes_without_reporting():
    res = trigger_scan("reject")
    row = wait_cases(res["scan_id"], 1)[0]
    assert row["status"] == "closed_rejected", row["blocked_reason"]
    assert path_of(row["case_id"]) == [("triage", "classify"), ("reviewer", "gate1")]
    assert reviews_of(row["case_id"], "gate1")[-1]["verdict"] == "reject"


def test_gate3_accepts_linked_duplicate_ticket():
    """ticketing 查重命中 → linked_duplicate；闸门 3 的 duplicate_consistent 必须放行。"""
    res = trigger_scan("duplicate")
    row = wait_cases(res["scan_id"], 1)[0]
    tk = q1("select * from tickets where case_id=$1", row["case_id"])
    assert tk["ticket_status"] == "linked_duplicate" and tk["duplicate_of"], tk
    gate3 = reviews_of(row["case_id"], "gate3")[-1]
    assert gate3["verdict"] == "approve", gate3
    check = next(c for c in gate3["checks"] if c["id"] == "duplicate_consistent")
    assert check["pass"] is True, gate3["checks"]
    assert row["status"] == "closed", row["blocked_reason"]


def test_reproduction_not_reproducible_and_timeout_paths():
    res = trigger_scan("not_reproducible")
    row = wait_cases(res["scan_id"], 1)[0]
    out = output_of(row["case_id"], "reproduction", "evidence")
    assert out["reproducible"] == "no", out
    assert row["status"] in ("closed", "blocked")

    # 夹具 sleep 75s > s2_full 的 e1 timeout_s 60 → 两次都超时 → blocked
    res2 = trigger_scan("timeout")
    row2 = wait_cases(res2["scan_id"], 1, timeout_s=300)[0]
    runs = [r for r in runs_of(row2["case_id"]) if r["stage"] == "reproduction"]
    assert len(runs) == 2 and all(r["status"] in ("timeout", "late") for r in runs), runs
    assert row2["status"] == "blocked", row2["blocked_reason"]
    assert "timeout" in (row2["blocked_reason"] or "").lower(), row2["blocked_reason"]


def test_triage_low_confidence_gates_gate1():
    res = trigger_scan("unknown_low_confidence")
    row = wait_cases(res["scan_id"], 1)[0]
    out = output_of(row["case_id"], "triage", "classify")
    assert out["rca"] == "unknown" and out["confidence"] < 0.7, out
    gate1 = reviews_of(row["case_id"], "gate1")[-1]
    conf_check = next(c for c in gate1["checks"] if c["id"] == "triage_confident")
    assert conf_check["pass"] is False, gate1["checks"]
    assert row["status"] == "closed_rejected", row["blocked_reason"]


# --------------------------------------------------------------------------- 计划层（workflow / LLM / 校验器）
def test_registry_match_wins_and_plan_is_sealed():
    res = trigger_scan("new_failure")
    row = wait_cases(res["scan_id"], 3)[0]
    plan = api(f"/api/cases/{row['case_id']}/plan")
    assert plan["source"] == "workflow" and plan["workflow_key"]
    yaml_text = api(f"/api/cases/{row['case_id']}/plan.yaml")
    assert "steps:" in yaml_text and plan["yaml_sha256"], yaml_text[:120]
    import hashlib

    assert hashlib.sha256(yaml_text.encode("utf-8")).hexdigest() == plan["yaml_sha256"]


def test_llm_fallback_used_when_registry_has_no_match():
    res = trigger_scan("llm_warming")
    row = wait_cases(res["scan_id"], 1)[0]
    plan = api(f"/api/cases/{row['case_id']}/plan")
    assert plan["source"] == "llm", plan
    assert plan["rationale"], "LLM 计划必须有 rationale"
    assert plan["degraded"] is False
    modes = path_of(row["case_id"])
    assert ("triage", "classify") in modes and ("reviewer", "gate3") in modes, modes
    assert row["status"] == "closed", row["blocked_reason"]
    assert row["plan_source"] == "llm"


def test_invalid_llm_plan_is_rejected_and_degrades_to_builtin():
    res = trigger_scan("llm_invalid")
    row = wait_cases(res["scan_id"], 1)[0]
    plan = api(f"/api/cases/{row['case_id']}/plan")
    assert plan["degraded"] is True, plan
    assert plan["workflow_key"] == "s2_full", plan
    assert "degraded" in (plan["rationale"] or "").lower(), plan["rationale"]
    assert ("reproduction", "evidence") in path_of(row["case_id"])
    assert row["status"] == "closed", row["blocked_reason"]


def test_case_and_daily_reports():
    res = trigger_scan("new_failure")
    wait_cases(res["scan_id"], 3)
    # 报表日由服务端按 REPORT_TZ 决定（不要在前端用本机时钟造 day：跨 UTC 日界会统计到 0）
    r = api("/api/report/daily", method="POST", json={})
    assert r["status"] == "ok", r
    day = r["day"]
    rep = api(f"/api/daily-report?day={day}")
    metrics = rep["metrics"]
    assert metrics.get("cases_total", 0) > 0, metrics
    assert metrics.get("scans", 0) > 0, metrics
    assert "report_md" in rep and len(rep["report_md"]) > 200, rep["report_md"][:120]
    assert "##" in rep["report_md"]


# --------------------------------------------------------------------------- 幂等 / DLQ / 重放
def test_duplicate_command_is_absorbed_by_idempotency():
    res = api(
        "/api/ops/inject",
        method="POST",
        json={
            "kind": "duplicate_command",
            "stage": "reviewer",
            "mode": "gate1",
            "publish_times": 3,
        },
    )
    assert res["dlq_rows"] == 0, res
    assert (
        res["evidence_rows"] == 1
    ), f"重复命令应被幂等吸收，实际产生 {res['evidence_rows']} 条证据"
    keys = q("select count(*) n from msg_dedup where dedup_key=$1", res["idempotency_key"])
    assert keys[0]["n"] == 1, keys


def test_bad_command_goes_to_dlq_with_context():
    res = api("/api/ops/inject", method="POST", json={"kind": "bad_command", "stage": "reviewer"})
    assert res["dlq_rows"] == 1, res
    assert res["evidence_rows"] == 0, "毒消息不能被执行"
    row = q1(
        "select stage, idempotency_key, reason, original_routing_key, redelivered, body from dlq"
        " where idempotency_key=$1",
        res["idempotency_key"],
    )
    # 第三段是注入的非法 mode：ci.cmd.reviewer.bogus_mode
    assert row["stage"] == "reviewer", row
    assert row["original_routing_key"] == "ci.cmd.reviewer.bogus_mode", row
    assert "contract_violation" in row["reason"] or "expired" in row["reason"], row["reason"]
    assert '"payload"' in row["body"] and '"mode"' in row["body"], row["body"][:200]


def test_replay_reuses_plan_and_is_idempotent_per_attempt():
    res = trigger_scan("new_failure")
    rows = wait_cases(res["scan_id"], 3)
    err = next(r for r in rows if r["signal"] == "error")
    before = q("select idempotency_key from stage_runs where case_id=$1", err["case_id"])
    api(f"/api/cases/{err['case_id']}/replay", method="POST")
    after = q("select idempotency_key, status from stage_runs where case_id=$1", err["case_id"])
    new_keys = {r["idempotency_key"] for r in after} - {r["idempotency_key"] for r in before}
    assert new_keys, "重放应产生新 attempt 的执行记录"
    assert all(k.split(":")[-1] == "2" for k in new_keys), new_keys
    assert all(r["status"] in ("done", "blocked") for r in after), after
    row = q1("select status, progress from cases where case_id=$1", err["case_id"])
    assert row["progress"]["gen"] == 2, row["progress"]


def test_api_surface_health_and_stats():
    h = api("/healthz")
    assert h["status"] == "ok" and h["svc"] == "orchestrator"
    s = api("/api/stats")
    assert s["stages"] and s["cases"] and s["remediations"] and s["regressions"]
    w = api("/api/workflows")
    keys = {x["key"] for x in w["items"]}
    assert keys == {"s1_observe_only", "s2_full", "s3_test_no_repro"}, keys
    sc = api("/api/scenarios")
    assert "source_unavailable" not in sc["feed_scenarios"], "source_unavailable 必须故意没有夹具"
    metrics = api("/metrics")
    assert "ci_dispatch_total" in metrics and "ci_cases_finished_total" in metrics, metrics[:300]


# --------------------------------------------------------------------------- Web UI 页面
def test_ui_pages_render_with_real_data():
    """Web UI 是交付项：任务列表 / 编排计划 / 实时进度三个页面必须真的渲染出数据。

    历史坑：starlette 1.x 删掉了 TemplateResponse(name, context) 旧签名，UI 全部 500，
    而纯 JSON API 测试发现不了 —— 这条用例就是把它钉住。
    """
    res = trigger_scan("new_failure")
    rows = wait_cases(res["scan_id"], 3)
    err_case = next(r for r in rows if r["signal"] == "error")
    case_id = err_case["case_id"]

    pages = {
        "/": ["Case 列表", case_id],
        "/ui/plans": ["Workflow 注册表", "已封存计划"],
        "/ui/health": ["Stage 执行情况"],
        f"/ui/cases/{case_id}": ["编排计划", "执行轨迹", "reproduction"],
    }
    for path, markers in pages.items():
        r = requests.get(f"{BASE}{path}", timeout=60)
        assert r.status_code == 200, (path, r.status_code, r.text[:300])
        assert "text/html" in r.headers.get("content-type", ""), (path, r.headers)
        for m in markers:
            assert m in r.text, (path, m, r.text[:400])

    # 详情页里的 plan.yaml 原文与 UI 上显示的计划同源（sha256 可核对）
    plan = api(f"/api/cases/{case_id}/plan")
    raw = api(f"/api/cases/{case_id}/plan.yaml")
    assert plan["plan_id"] in raw, raw[:300]
    assert plan["steps"], plan
