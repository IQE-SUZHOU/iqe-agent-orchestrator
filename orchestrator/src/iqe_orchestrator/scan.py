"""扫描执行：编排层触发 Monitor，把观测转成 Case 并启动各自的 plan 图。

抑制规则（suppressed 的观测不建 Case，只留 Monitor 证据与计数）。
"""

from __future__ import annotations

import logging

from iqe_orchestrator.common.envelope import ulid
from iqe_orchestrator.common.logs import counters, log_with

from .dispatch import BusGateway
from .graph import CaseEngine

log = logging.getLogger("orchestrator.scan")


class ScanRunner:
    def __init__(self, db, gateway: BusGateway, engine: CaseEngine):
        self.db = db
        self.gw = gateway
        self.engine = engine

    async def run_scan(
        self,
        *,
        scenario: str | None = None,
        lookback: str = "24h",
        jobs_filter: list[str] | None = None,
        wait_cases: bool = False,
    ) -> dict:
        scan_id, run_id = ulid(), ulid()
        await self.db.scan_open(scan_id, run_id, scenario or "default")
        res = await self.gw.dispatch(
            stage="monitor",
            mode="scan",
            payload={"scan_id": scan_id, "lookback": lookback, "jobs_filter": jobs_filter or []},
            run_id=run_id,
            step_id="scan",
            attempt=1,
            case_id=None,
            scan_id=scan_id,
            scenario_hint=scenario,
        )
        if res.status != "ok":
            counts = {"observed": 0, "promoted": 0, "suppressed": 0, "cases_created": 0}
            await self.db.scan_close(scan_id, "blocked", counts, res.error)
            counters.inc("ci_scans_total", status="blocked")
            log_with(
                log,
                "warn",
                "scan_blocked",
                scan_id=scan_id,
                scenario=scenario,
                err=(res.error or {}).get("code"),
            )
            return {
                "scan_id": scan_id,
                "status": "blocked",
                "counts": counts,
                "cases": [],
                "error": res.error,
            }

        out = res.output
        observations = out.get("observations") or []
        counts = dict(out.get("counts") or {})
        errors = [o for o in observations if o.get("signal") == "error" and not o.get("suppressed")]
        nones = [o for o in observations if o.get("signal") == "none"]
        cases: list[str] = []
        for obs in errors + nones:
            cid = await self.engine.start_case(obs=obs, run_id=run_id, scan_id=scan_id)
            cases.append(cid)
        counts["cases_created"] = len(cases)
        counts["errors_promoted"] = len(errors)
        counts["observed_only"] = len(nones)
        await self.db.scan_close(scan_id, "done", counts, None)
        await self.gw.publish_event(
            rk="ci.event.case.created",
            correlation_id=scan_id,
            data={"scan_id": scan_id, "cases": len(cases), "counts": counts},
        )
        counters.inc("ci_scans_total", status="done")
        log_with(log, "info", "scan_done", scan_id=scan_id, scenario=scenario, **counts)
        if wait_cases:
            await self.engine.drain()
        return {
            "scan_id": scan_id,
            "status": "done",
            "counts": counts,
            "cases": cases,
            "observations": observations,
            "run_id": run_id,
        }
