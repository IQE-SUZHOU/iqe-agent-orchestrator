"""日报触发（编排层 → Reporting Agent，走同一条命令通道）。"""

from __future__ import annotations

import logging

from iqe_orchestrator.common.envelope import ulid
from iqe_orchestrator.common.logs import log_with

from .dispatch import BusGateway

log = logging.getLogger("orchestrator.reporting")


async def trigger_daily(gw: BusGateway, db, *, day: str | None = None) -> dict:
    from iqe_orchestrator.common.dates import today_str

    day = day or today_str()
    run_id = ulid()
    res = await gw.dispatch(
        stage="reporting",
        mode="daily",
        payload={"case_id": None, "day": day},
        run_id=run_id,
        step_id="daily1",
        attempt=1,
        case_id=None,
        timeout_s=60,
    )
    log_with(log, "info", "daily_report_dispatched", day=day, status=res.status, key=res.key)
    return {
        "day": day,
        "status": res.status,
        "output": res.output,
        "error": res.error,
        "key": res.key,
    }
