"""为什么这组测试存在：日报的「一天」必须只有一个口径，否则报表整段变 0。

Postgres 的 now() 是 UTC、k8s Pod 的本地时区也是 UTC，而本机开发常常是 Asia/Shanghai。
如果报表日用本地日期、统计窗口用 `created_at::date`（服务器 UTC 日界），跨 UTC 日界的那几个小时
就会被算到另一天去。实现给出的口径是（common/db.py: daily_metrics）：

    start, end = day_range(day)          # REPORT_TZ 下的 [00:00, 次日 00:00)
    ... where created_at >= start and created_at < end

也就是**半开区间**。这里钉死三件事：
1) 边界：`2026-11-25T23:59:59Z` 与 `2026-11-26T00:00:01Z` 必须落在不同的报表日；
2) 形状：day_range 首尾相接（相邻天上界 == 下界，不重叠也不留缝），且上界那一瞬间属于下一天；
3) 口径可换：REPORT_TZ 由环境变量驱动（common/config.py 在构造 Config 时读取），换时区后
   同一个 UTC 瞬间必须落到不同的报表日 —— 证明日期是运行时配置驱动、不是 import 时固化。

离线测试：不连库，只把 daily_metrics 的 SQL 谓词在内存里复刻一遍。
"""

from __future__ import annotations

import importlib
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from iqe_orchestrator.common import config as config_mod
from iqe_orchestrator.common import dates as dates_mod
from iqe_orchestrator.common.config import Config, cfg

pytestmark = pytest.mark.unit

DAY = "2026-11-25"
NEXT_DAY = "2026-11-26"
UTC_DAY_START = datetime(2026, 11, 25, 0, 0, 0, tzinfo=UTC)
UTC_DAY_END = datetime(2026, 11, 26, 0, 0, 0, tzinfo=UTC)
LAST_SECOND_OF_DAY = datetime(2026, 11, 25, 23, 59, 59, tzinfo=UTC)
FIRST_SECOND_OF_NEXT_DAY = datetime(2026, 11, 26, 0, 0, 1, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _pin_report_tz_utc(monkeypatch):
    """边界断言全部建立在部署默认值 REPORT_TZ=UTC 上：显式钉住，避免受宿主环境变量影响。"""
    monkeypatch.setattr(cfg, "report_tz", "UTC")


@pytest.fixture
def report_tz_from_env(monkeypatch):
    """改环境变量 + reload 模块：证明 day 边界跟着 REPORT_TZ 走（config.cfg 是 import 期构造的单例）。"""

    def _apply(name: str):
        monkeypatch.setenv("REPORT_TZ", name)
        importlib.reload(config_mod)
        # dates 用 `from .config import cfg` 拿的是对象引用，必须一起 reload 才能换掉 cfg
        return importlib.reload(dates_mod)

    yield _apply

    monkeypatch.delenv("REPORT_TZ", raising=False)
    importlib.reload(config_mod)
    importlib.reload(dates_mod)


def _in_day(day: str, ts: datetime) -> bool:
    """复刻 db.daily_metrics 的谓词：created_at >= start AND created_at < end。"""
    start, end = dates_mod.day_range(day)
    return start <= ts < end


# --------------------------------------------------------------------------- REPORT_TZ 接线
def test_report_tz_defaults_to_utc_and_follows_the_env_var(monkeypatch):
    monkeypatch.delenv("REPORT_TZ", raising=False)
    assert Config().report_tz == "UTC"
    monkeypatch.setenv("REPORT_TZ", "Asia/Shanghai")
    assert Config().report_tz == "Asia/Shanghai"
    # 空串按"未设置"处理（_s 把 "" 当缺省），避免 CI 里 `REPORT_TZ=` 造成非法时区
    monkeypatch.setenv("REPORT_TZ", "")
    assert Config().report_tz == "UTC"


# --------------------------------------------------------------------------- 半开区间语义
def test_day_range_is_a_half_open_utc_day_window():
    start, end = dates_mod.day_range(DAY)
    assert start == UTC_DAY_START
    assert end == UTC_DAY_END
    assert start.utcoffset() == timedelta(0)
    assert end.utcoffset() == timedelta(0)
    assert end - start == timedelta(days=1)


def test_adjacent_days_touch_without_overlap_or_gap():
    _start, end = dates_mod.day_range(DAY)
    next_start, next_end = dates_mod.day_range(NEXT_DAY)
    assert next_start == end  # 上界即下一天的下界：不留缝
    assert next_end - next_start == timedelta(days=1)
    # 上界那一瞬间属于下一天（半开区间的可观测后果）
    assert dates_mod.date_of(end) == NEXT_DAY


def test_timestamps_across_utc_midnight_belong_to_different_days():
    assert dates_mod.date_of(LAST_SECOND_OF_DAY) == DAY
    assert dates_mod.date_of(FIRST_SECOND_OF_NEXT_DAY) == NEXT_DAY
    assert dates_mod.date_of(LAST_SECOND_OF_DAY) != dates_mod.date_of(FIRST_SECOND_OF_NEXT_DAY)


def test_daily_metrics_window_includes_lower_bound_excludes_upper_bound():
    """不连库，把 daily_metrics 的 SQL 谓词在内存里复刻：含下界、不含上界。"""
    rows = [
        ("before", datetime(2026, 11, 24, 23, 59, 59, tzinfo=UTC)),
        ("day-start", UTC_DAY_START),
        ("day-last-second", LAST_SECOND_OF_DAY),
        ("midnight-boundary", UTC_DAY_END),
        ("next-day-second", FIRST_SECOND_OF_NEXT_DAY),
    ]
    assert [name for name, ts in rows if _in_day(DAY, ts)] == ["day-start", "day-last-second"]
    assert [name for name, ts in rows if _in_day(NEXT_DAY, ts)] == [
        "midnight-boundary",
        "next-day-second",
    ]


def test_every_timestamp_is_counted_by_exactly_one_report_day():
    days = ("2026-11-24", DAY, NEXT_DAY)
    stamps = [
        datetime(2026, 11, 24, 23, 59, 59, tzinfo=UTC),
        UTC_DAY_START,
        LAST_SECOND_OF_DAY,
        UTC_DAY_END,
        FIRST_SECOND_OF_NEXT_DAY,
    ]
    for ts in stamps:
        hits = [d for d in days if _in_day(d, ts)]
        assert hits == [dates_mod.date_of(ts)], f"{ts.isoformat()} 落到 {hits}"


def test_today_str_and_day_range_describe_the_same_day():
    day = dates_mod.today_str()
    assert date.fromisoformat(day)  # YYYY-MM-DD
    start, end = dates_mod.day_range(day)
    assert start.date().isoformat() == day
    assert end.date().isoformat() == (start + timedelta(days=1)).date().isoformat()
    assert _in_day(day, dates_mod.now())
    assert dates_mod.date_of(dates_mod.now()) == day


# --------------------------------------------------------------------------- 输入形态
def test_as_date_accepts_iso_string_with_time_and_date_objects():
    assert dates_mod.as_date(DAY) == date(2026, 11, 25)
    assert dates_mod.as_date(f"{DAY}T23:59:59+00:00") == date(2026, 11, 25)
    assert dates_mod.as_date(date(2026, 11, 25)) == date(2026, 11, 25)


def test_day_range_of_a_datetime_uses_only_its_date_part():
    # datetime 是 date 的子类：as_date 原样返回，day_range 只取日期部分（时间部分被丢弃）
    start, end = dates_mod.day_range(datetime(2026, 11, 25, 23, 59, 59, tzinfo=UTC))
    assert (start, end) == (UTC_DAY_START, UTC_DAY_END)


def test_date_of_converts_foreign_offsets_into_the_report_day():
    shanghai_morning = datetime(
        2026, 11, 25, 7, 30, tzinfo=ZoneInfo("Asia/Shanghai")
    )  # = 11-24T23:30Z
    assert dates_mod.date_of(shanghai_morning) == "2026-11-24"
    tokyo_evening = datetime(2026, 11, 26, 8, 0, tzinfo=ZoneInfo("Asia/Tokyo"))  # = 11-25T23:00Z
    assert dates_mod.date_of(tokyo_evening) == DAY


def test_date_of_takes_naive_timestamps_at_face_value():
    # 无 tzinfo 的时间戳不做换算，只取日期部分：调用方必须自己保证它已经是 UTC
    assert dates_mod.date_of(datetime(2026, 11, 25, 23, 59, 59)) == DAY
    assert dates_mod.date_of(None) == ""
    assert dates_mod.date_of("") == ""
    # 字符串走的是截断而不是换算：带 +08:00 的 ISO 串按面值取前 10 位（DB 侧返回的是 datetime）
    assert dates_mod.date_of("2026-11-26T07:00:00+08:00") == NEXT_DAY


# --------------------------------------------------------------------------- 换时区
def test_report_tz_env_var_moves_the_day_boundary(report_tz_from_env):
    dates = report_tz_from_env("Asia/Shanghai")
    assert config_mod.cfg.report_tz == "Asia/Shanghai"
    assert dates.tz().key == "Asia/Shanghai"

    start, end = dates.day_range(DAY)
    assert start == datetime(2026, 11, 25, 0, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert start.astimezone(UTC) == datetime(2026, 11, 24, 16, 0, 0, tzinfo=UTC)
    assert end - start == timedelta(days=1)

    # 同一个 UTC 瞬间：UTC 口径属于 11-24，上海口径属于 11-25 —— 这就是 REPORT_TZ 存在的全部理由
    instant = datetime(2026, 11, 24, 16, 0, 0, tzinfo=UTC)
    assert dates.date_of(instant) == DAY
    assert dates.date_of(datetime(2026, 11, 24, 15, 59, 59, tzinfo=UTC)) == "2026-11-24"
    assert dates.today_str() == dates.date_of(dates.now())
