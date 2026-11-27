#!/usr/bin/env python3
"""12 阶段离线 demo 脚手架：不连 RabbitMQ / Postgres，直接把某阶段的真实输入喂给 Agent 剧本。

用法（在仓库根目录执行）：

    python demos/harness.py --list          # 列 12 个阶段与两条分支
    python demos/harness.py --all           # 顺序跑完两条分支（推荐先跑这个）
    python demos/harness.py 03-gate1        # 只跑某个阶段（自动回放它之前的所有阶段）
    python demos/harness.py 03-gate1 --chain internal
    python demos/harness.py --write         # 重算并回写各目录的 input.json / output.json

设计约束（改代码时请保持）：

1. 输入构造逐字段对齐 ``orchestrator/src/iqe_orchestrator/graph.py::_build_input``，
   所以 ``input.json`` 就是编排层真实投递的 ``payload.input``。上游缺字段这里也会缺，
   不会「补一份好看的数据」骗过下游。
2. Agent 侧完全是生产代码：``iqe_agents.<stage>.playbook.handler`` +
   ``iqe_agents.runtime.Ctx``；DB 用 ``iqe_agents.tools.selfcheck.FakeDB``（内存替身，
   语义对齐 ``common/db.py``）。所以这里跑通 = 真 Agent 跑通，除了消息总线。
3. 每个输出都用 ``schemas/stage-contracts.json`` 校验，字段名写错立刻报错。
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import sys
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
for p in (str(REPO), str(REPO / "agents" / "src"), str(REPO / "orchestrator" / "src")):
    if p not in sys.path:
        sys.path.insert(0, p)

# 剧本里有 mock_delay_ms / mock_failure_rate，selfcheck 在 import 时就置零了；
# 这里再写一遍是为了不依赖 import 顺序（selfcheck 以后重构也不会让 demo 变慢）。
from iqe_agents.common.config import cfg  # noqa: E402
from iqe_agents.common.mock import pick_scenario  # noqa: E402
from iqe_agents.runtime import Ctx  # noqa: E402
from iqe_agents.tools.selfcheck import FIXTURES, FakeDB, validate_output  # noqa: E402

cfg.mock_delay_ms = "0:0"
cfg.mock_failure_rate = 0.0

HANDLERS = {}


def _load_handlers() -> dict:
    """八个 Agent 的 playbook handler；按需 import，避免 demo 依赖编排层。"""
    from iqe_agents.monitor.playbook import handler as monitor
    from iqe_agents.regression.playbook import handler as regression
    from iqe_agents.remediation.playbook import handler as remediation
    from iqe_agents.reporting.playbook import handler as reporting
    from iqe_agents.reproduction.playbook import handler as reproduction
    from iqe_agents.reviewer.playbook import handler as reviewer
    from iqe_agents.ticketing.playbook import handler as ticketing
    from iqe_agents.triage.playbook import handler as triage

    return {
        "monitor": monitor,
        "triage": triage,
        "reviewer": reviewer,
        "remediation": remediation,
        "reproduction": reproduction,
        "regression": regression,
        "ticketing": ticketing,
        "reporting": reporting,
    }


@dataclass(frozen=True)
class Step:
    folder: str  # demos/ 下的目录名
    stage: str
    mode: str
    step_id: str  # 与 workflows/s2_full.yaml 的 step id 一致
    title: str
    canonical: str = "external"  # 该阶段主要出现在哪条分支
    mirror: bool = False  # 是否额外保留另一条分支的样例
    scenario: str | None = None  # None = 跟随 case 的 scenario_hint


STEPS: list[Step] = [
    Step("01-monitor", "monitor", "scan", "scan", "① Monitor 巡检", scenario="new_failure"),
    Step("02-triage", "triage", "classify", "t1", "② Triage 分诊", mirror=True),
    Step("03-gate1", "reviewer", "gate1", "g1", "③ 闸门 1（分支决策）", mirror=True),
    Step("04a-remediation", "remediation", "fix", "r1", "④a 内部修复", canonical="internal"),
    Step("05a-verify", "reproduction", "verify", "v1", "⑤a 修复验证", canonical="internal"),
    Step("06a-gate2", "reviewer", "gate2", "g2", "⑥a 闸门 2（修复验收）", canonical="internal"),
    Step("04b-evidence", "reproduction", "evidence", "e1", "④b 受控复现"),
    Step("05b-regression-check", "regression", "check", "c1", "⑤b 回归判定"),
    Step("06c-bisect", "regression", "bisect", "b1", "⑥c guilty patch 定位"),
    Step("07b-ticketing", "ticketing", "create", "k1", "⑦b 建单"),
    Step("08b-gate3", "reviewer", "gate3", "g3", "⑧b 闸门 3（建单验收）"),
    Step("09-reporting", "reporting", "case", "p1", "⑨ 结案报告"),
]
STEP_BY_FOLDER = {s.folder: s for s in STEPS}

HEAD = ["01-monitor", "02-triage", "03-gate1"]
BRANCH = {
    "internal": ["04a-remediation", "05a-verify", "06a-gate2"],
    "external": [
        "04b-evidence",
        "05b-regression-check",
        "06c-bisect",
        "07b-ticketing",
        "08b-gate3",
    ],
}
TAIL = ["09-reporting"]
CASE_IDS = {"internal": "demo-case-internal", "external": "demo-case-external"}
# Monitor 用哪份 CI feed：external 拿 driver:crash，internal 拿 toolchain:compile-error
MONITOR_FEED = {"internal": "mixed", "external": "new_failure"}


# ---------------------------------------------------------------- 输入构造（对齐 _build_input）
def build_input(step: Step, *, case_id: str, obs: dict, state: dict, evidence_count: int) -> dict:
    stage, mode = step.stage, step.mode
    base = {
        "case_id": case_id,
        "build": obs.get("build"),
        "source": obs.get("source", "mock"),
        "job": obs.get("job"),
    }
    if stage == "monitor":
        return {"scan_id": f"scan-demo-{case_id}", "lookback": "15m", "jobs_filter": []}
    if stage == "reporting":
        if mode == "case":
            return {"case_id": case_id, "day": None}
        return {"case_id": None, "day": dt.date.today().isoformat()}

    triage = latest(state, "triage") or {}
    if stage == "triage":
        return {
            **base,
            "signal": obs.get("signal"),
            "signature": obs.get("signature"),
            "log_excerpt": obs.get("log_excerpt"),
            "context": obs.get("context") or {},
            "evidence_refs": obs.get("evidence_refs") or [],
            "requested_focus": None,
        }

    # 除 2 号阶段外都先看分诊结论；分诊没跑过就退化成 unknown（编排层同样兜底）
    common = {
        **base,
        "rca": triage.get("rca") or "unknown",
        "component": triage.get("component"),
        "signature": triage.get("signature") or obs.get("signature"),
        "key_frames": triage.get("key_frames") or [],
    }
    if stage == "reviewer":
        payload = {
            **common,
            "signal": obs.get("signal"),
            "path": triage.get("path"),
            "confidence": triage.get("confidence"),
            "evidence_count": evidence_count,
            "loop_seq": 1,
            "requested_focus": None,
        }
        if mode == "gate2":
            payload["remediation"] = latest(state, "remediation")
            payload["reproduction"] = latest(state, "reproduction", "verify")
        elif mode == "gate3":
            payload["reproduction"] = latest(state, "reproduction", "evidence")
            payload["regression"] = latest(state, "regression")
            payload["ticketing"] = latest(state, "ticketing")
        return payload
    if stage == "remediation":
        return {**common, "kb_fix_refs": list(triage.get("kb_refs") or []), "requested_focus": None}
    if stage == "reproduction":
        payload = {**common, "requested_focus": None}
        if mode == "verify":
            payload["remediation"] = latest(state, "remediation")
        return payload
    if stage == "regression":
        check = latest(state, "regression", "check") or {}
        payload = {
            **common,
            "reproduction": latest(state, "reproduction", "evidence"),
            "requested_focus": None,
        }
        if mode == "bisect":
            payload["baseline"] = check.get("baseline")
            payload["change_window"] = check.get("change_window")
        return payload
    if stage == "ticketing":
        return {
            **common,
            "signal": obs.get("signal"),
            "path": triage.get("path"),
            "reproduction": latest(state, "reproduction", "evidence"),
            "regression": latest(state, "regression"),
            "duplicate_of": triage.get("duplicate_of"),
            "requested_focus": None,
        }
    raise SystemExit(f"未知 stage: {stage}")


def latest(state: dict, stage: str, mode: str | None = None) -> dict | None:
    """取某 stage 最近一次输出；不指定 mode 时取最后跑的那个（regression 会拿到 bisect）。"""
    rows = state.get(stage) or {}
    if mode is not None:
        return rows.get(mode)
    if not rows:
        return None
    return rows[list(rows)[-1]]


# ---------------------------------------------------------------- 跑一个 stage
async def call(step: Step, *, case_id: str, input_obj: dict, scenario: str, db) -> dict:
    env = {
        "run_id": f"R-{case_id}",
        "stage": step.stage,
        "step_id": step.step_id,
        "attempt": 1,
        "case_id": case_id,
        "scan_id": None,
        "mode": step.mode,
        "scenario_hint": scenario,
        "payload": {"mode": step.mode, "input": input_obj},
    }
    ctx = Ctx(stage=step.stage, env=env, scenario=pick_scenario(env), db=db, fixtures=FIXTURES)
    out = await HANDLERS[step.stage](ctx)
    ok, detail = validate_output(step.stage, out)
    if not ok:
        raise SystemExit(f"[{step.folder}] 输出契约不通过：{detail}")
    db.add_stage_run(
        case_id=case_id, stage=step.stage, mode=step.mode, step_id=step.step_id, output=out
    )
    return out


def promoted_obs(out: dict) -> dict:
    for obs in out.get("observations") or []:
        if obs.get("signal") == "error" and not obs.get("suppressed"):
            return obs
    raise SystemExit("monitor 没有晋升任何失败观测，demo 无从继续")


async def seed_case(db, case_id: str, obs: dict, chain: str) -> None:
    await db.create_case(
        case_id=case_id,
        run_id=f"R-{case_id}",
        scan_id=f"scan-demo-{case_id}",
        source=obs.get("source", "mock"),
        job=obs.get("job"),
        build=obs.get("build"),
        signal=obs.get("signal"),
        signature=obs.get("signature"),
        status="reported",
        workflow_key=f"s2_full_{chain}",
        plan_source="workflow",
        payload=obs,
    )


# ---------------------------------------------------------------- 一条分支
async def run_chain(
    chain: str, *, only: str | None = None, override_input: bool = True
) -> dict[str, dict]:
    global HANDLERS
    if not HANDLERS:
        HANDLERS = _load_handlers()

    steps = [STEP_BY_FOLDER[f] for f in HEAD + BRANCH[chain] + TAIL]
    case_id = CASE_IDS[chain]
    db = FakeDB()
    state: dict[str, dict] = {}
    results: dict[str, dict] = {}
    obs: dict = {}

    for step in steps:
        if step.stage == "monitor":
            feed = MONITOR_FEED[chain] if step.folder == "01-monitor" else "new_failure"
            inp = build_input(step, case_id=case_id, obs={}, state=state, evidence_count=0)
            if step.folder == "01-monitor":
                out = await call(step, case_id=case_id, input_obj=inp, scenario=feed, db=db)
                obs = promoted_obs(out)
                await seed_case(db, case_id, obs, chain)
            else:  # pragma: no cover - 一条分支只有一次 scan
                out = await call(step, case_id=case_id, input_obj=inp, scenario=feed, db=db)
        else:
            evidence_count = len([r for r in db.evidence_rows if r.get("case_id") == case_id])
            inp = build_input(
                step, case_id=case_id, obs=obs, state=state, evidence_count=evidence_count
            )
            scenario = (obs.get("context") or {}).get("mock_scenario") or "default"
            if override_input and only == step.folder:
                # 单步模式：以目录里的 input.json 为准（实现人员改了字段就能立刻验证）
                cached = files_for(step, chain)[0]
                if cached.exists():
                    inp = json.loads(cached.read_text(encoding="utf-8"))
                    case_id = inp.get("case_id") or case_id
            out = await call(step, case_id=case_id, input_obj=inp, scenario=scenario, db=db)

        state.setdefault(step.stage, {})[step.mode] = out
        results[step.folder] = {"input": inp, "output": out, "scenario": feed_or(step, obs, chain)}
        if only == step.folder:
            return results
    return results


def feed_or(step: Step, obs: dict, chain: str) -> str:
    if step.stage == "monitor":
        return MONITOR_FEED[chain]
    return (obs.get("context") or {}).get("mock_scenario") or "default"


# ---------------------------------------------------------------- 展示 / 落盘
def summarize(step: Step, out: dict) -> str:
    if step.stage == "monitor":
        return json.dumps(out.get("counts"), ensure_ascii=False)
    if step.stage == "triage":
        keys = ("rca", "path", "component", "confidence")
    elif step.stage == "reviewer":
        keys = ("gate", "verdict", "decision", "reason_code")
    elif step.stage == "remediation":
        keys = ("fixed", "change_type", "target")
    elif step.stage == "reproduction":
        keys = ("mode", "reproducible", "fix_verified")
    elif step.stage == "regression":
        keys = ("mode", "is_regression", "bisected")
    elif step.stage == "ticketing":
        keys = ("ticket_status", "external_key", "priority")
    else:
        m = out.get("metrics") or {}
        keys = ("path", "rca", "ticket_status")
        return json.dumps({k: m.get(k) for k in keys}, ensure_ascii=False)
    return json.dumps({k: out.get(k) for k in keys}, ensure_ascii=False)


def files_for(step: Step, chain: str) -> tuple[Path, Path]:
    d = HERE / step.folder
    if chain == step.canonical:
        return d / "input.json", d / "output.json"
    return d / f"input.{chain}.json", d / f"output.{chain}.json"


def dump(path: Path, obj: dict) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


async def main_async(args) -> int:
    if args.list:
        print(f"{'目录':<22}{'stage':<13}{'mode':<10}{'step':<6}分支  说明")
        for s in STEPS:
            print(
                f"{s.folder:<22}{s.stage:<13}{s.mode:<10}{s.step_id:<6}"
                f"{s.canonical:<8}{s.title}"
            )
        return 0

    if args.write:
        for chain in ("internal", "external"):
            results = await run_chain(chain, override_input=False)
            for folder, row in results.items():
                step = STEP_BY_FOLDER[folder]
                if chain != step.canonical and not step.mirror:
                    continue
                inp_path, out_path = files_for(step, chain)
                inp_path.parent.mkdir(parents=True, exist_ok=True)
                dump(inp_path, row["input"])
                dump(out_path, row["output"])
                print(f"写 {inp_path.relative_to(REPO)} / {out_path.name}")
        return 0

    chains = ["internal", "external"] if args.all or not args.folder else [args.chain]
    if args.folder and args.folder not in STEP_BY_FOLDER:
        print(f"未知阶段目录：{args.folder}（用 --list 看可选项）", file=sys.stderr)
        return 2

    for chain in chains:
        results = await run_chain(chain, only=args.folder, override_input=True)
        print(f"\n=== 分支 {chain}（case={CASE_IDS[chain]}）")
        for folder, row in results.items():
            step = STEP_BY_FOLDER[folder]
            print(f"  {step.folder:<22}{step.title:<20}{summarize(step, row['output'])}")
        if args.json:
            print(
                json.dumps(
                    {f: r["output"] for f, r in results.items()}, ensure_ascii=False, indent=2
                )
            )
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="12 阶段离线 demo")
    ap.add_argument("folder", nargs="?", help="只跑这个阶段目录，例如 03-gate1")
    ap.add_argument("--chain", choices=["internal", "external"], default="external")
    ap.add_argument("--all", action="store_true", help="跑完整两条分支（默认行为）")
    ap.add_argument("--write", action="store_true", help="重算并回写 input.json / output.json")
    ap.add_argument("--list", action="store_true", help="列出 12 个阶段")
    ap.add_argument("--json", action="store_true", help="额外打印完整输出 JSON")
    return asyncio.run(main_async(ap.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
