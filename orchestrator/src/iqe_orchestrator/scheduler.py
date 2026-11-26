"""定时触发：Monitor 扫描周期 + 每日日报。只发命令，不越权替 Agent 决策。"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

from iqe_orchestrator.common.config import cfg
from iqe_orchestrator.common.logs import log_with

from .scan import ScanRunner

log = logging.getLogger("orchestrator.scheduler")


class Scheduler:
    def __init__(self, runner: ScanRunner, db):
        self.runner = runner
        self.db = db
        self.tasks: list[asyncio.Task] = []
        self.stop = asyncio.Event()

    async def start(self) -> None:
        if cfg.scheduler_enabled:
            self.tasks.append(asyncio.create_task(self._scan_loop()))
        self.tasks.append(asyncio.create_task(self._daily_loop()))
        log_with(
            log,
            "info",
            "scheduler_started",
            scan_interval_s=cfg.monitor_scan_interval_s,
            scheduler_enabled=cfg.scheduler_enabled,
            report_at=cfg.report_at,
        )

    async def _scan_loop(self) -> None:
        # 启动后先跑一轮，便于演示与冒烟（等 Agent 侧的消费者都挂上队列）
        await asyncio.sleep(cfg.scan_startup_delay_s)
        while not self.stop.is_set():
            try:
                await self.runner.run_scan(scenario=cfg.mock_scenario or "default")
            except Exception as exc:
                log_with(log, "error", "scheduled_scan_failed", err=repr(exc)[:300])
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=cfg.monitor_scan_interval_s)
            except TimeoutError:
                pass

    async def _daily_loop(self) -> None:
        while not self.stop.is_set():
            from iqe_orchestrator.common.dates import tz

            now = datetime.now(tz())  # 日报触发时刻按 REPORT_TZ 判定
            hh, mm = (int(x) for x in cfg.report_at.split(":"))
            nxt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if nxt <= now:
                nxt += timedelta(days=1)
            delay = (nxt - now).total_seconds()
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=delay)
                break
            except TimeoutError:
                pass
            try:
                from .reporting import trigger_daily

                await trigger_daily(self.db, self.runner.gw, day=now.strftime("%Y-%m-%d"))
            except Exception as exc:
                log_with(log, "error", "daily_report_failed", err=repr(exc)[:300])

    async def stop_all(self) -> None:
        self.stop.set()
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
