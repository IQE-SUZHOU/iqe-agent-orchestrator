"""「一天」的定义必须全局一致：日报按 REPORT_TZ 划分日界。

Postgres 的 `now()` 是 UTC，进程本地时区可能是 UTC（k8s Pod 默认）也可能是
Asia/Shanghai（本机开发）。如果日报的 day 用本地日期、而统计用 `created_at::date`
（服务器 UTC），跨 UTC 日界的那几个小时就会被算到另一天去，报表全是 0。
所以：所有 day 计算与统计窗口都走本模块。
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from .config import cfg


def tz() -> ZoneInfo:
    return ZoneInfo(cfg.report_tz)


def now() -> datetime:
    return datetime.now(tz())


def today_str() -> str:
    """当前报表日（YYYY-MM-DD，按 REPORT_TZ）。"""
    return now().date().isoformat()


def as_date(day: str | date) -> date:
    if isinstance(day, date):
        return day
    return date.fromisoformat(str(day)[:10])


def day_range(day: str | date) -> tuple[datetime, datetime]:
    """给定报表日，返回 [起始, 结束) 的 timestamptz 区间（按 REPORT_TZ）。"""
    d = as_date(day)
    start = datetime.combine(d, datetime.min.time(), tzinfo=tz())
    return start, start + timedelta(days=1)


def date_of(value: object) -> str:
    """把带时区的时间戳折算成 REPORT_TZ 下的日期字符串。"""
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(tz())
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "")[:10]
