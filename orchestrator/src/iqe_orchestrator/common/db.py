"""Postgres 访问层（复用集群 infra-database/postgres）。

写权分工（实现期收敛，避免双写竞态）：
- 编排层拥有 cases / scans / stage_runs / plans / workflows / daily_reports / reviews / tickets；
- Agent 只写 evidence 与 msg_dedup（幂等层 1）。
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

import asyncpg

from .config import cfg

log = logging.getLogger("ci_triage.common.db")

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "migrations" / "001_init.sql"

STAGES = ("monitor", "triage", "reproduction", "ticket", "review", "reporting")


def _as_date(day: str | date) -> date:
    """asyncpg 的 date 编码器只接受 date 对象；调用方（含 Agent）传的是 'YYYY-MM-DD' 字符串。"""
    if isinstance(day, date):
        return day
    return date.fromisoformat(str(day)[:10])


class DB:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @staticmethod
    async def _init_conn(conn: asyncpg.Connection) -> None:
        """jsonb/json 读出来就是 dict/list（写入侧允许直接传已序列化的字符串）。"""

        def _enc(v):
            if v is None or isinstance(v, (str, bytes)):
                return v
            return json.dumps(v, ensure_ascii=False)

        for typ in ("jsonb", "json"):
            await conn.set_type_codec(
                typ, encoder=_enc, decoder=json.loads, schema="pg_catalog", format="text"
            )

    @classmethod
    async def create(cls, dsn: str | None = None, min_size: int = 1, max_size: int = 8) -> DB:
        pool = await asyncpg.create_pool(
            dsn or cfg.pg_dsn,
            min_size=min_size,
            max_size=max_size,
            timeout=15,
            command_timeout=30,
            init=cls._init_conn,
        )
        return cls(pool)

    async def close(self) -> None:
        await self.pool.close()

    async def init_schema(self) -> None:
        sql = SCHEMA_FILE.read_text(encoding="utf-8")
        async with self.pool.acquire() as conn:
            await conn.execute(sql)
        log.info("schema ready")

    # ---------- 幂等层 1 ----------
    async def claim(self, key: str, stage: str) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "insert into msg_dedup(dedup_key, stage) values($1,$2) "
                "on conflict (dedup_key) do nothing returning dedup_key",
                key,
                stage,
            )
            return row is not None

    async def has_result(self, key: str) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "select 1 from stage_runs where idempotency_key=$1 and status='done'", key
            )
            return row is not None

    # ---------- 证据 ----------
    async def save_evidence(
        self,
        *,
        case_id: str | None,
        stage: str,
        kind: str,
        meta: dict,
        body: str,
        scan_id: str | None = None,
    ) -> str:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "insert into evidence(case_id, scan_id, stage, kind, meta, body) "
                "values($1,$2,$3,$4,$5::jsonb,$6) returning id",
                case_id,
                scan_id,
                stage,
                kind,
                json.dumps(meta, ensure_ascii=False),
                body[:20000],
            )
        return f"db://evidence/{row['id']}"

    async def evidence(self, case_id: str) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "select id, case_id, scan_id, stage, kind, uri, meta, left(body,400) as excerpt, created_at "
                "from evidence where case_id=$1 order by id",
                case_id,
            )
        return [dict(r) for r in rows]

    # ---------- Case ----------
    async def create_case(self, **kw) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """insert into cases(case_id, run_id, scan_id, source, job, build, signal, signature,
                                     status, workflow_key, plan_id, plan_source, priority, payload,
                                     scenario_hint)
                   values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,$15)
                   on conflict (case_id) do nothing""",
                kw["case_id"],
                kw["run_id"],
                kw.get("scan_id"),
                kw.get("source", "mock"),
                kw["job"],
                kw.get("build"),
                kw["signal"],
                kw.get("signature"),
                kw["status"],
                kw.get("workflow_key"),
                kw.get("plan_id"),
                kw.get("plan_source"),
                kw.get("priority", 1),
                json.dumps(kw.get("payload", {}), ensure_ascii=False),
                kw.get("scenario_hint"),
            )

    async def set_case_status(self, case_id: str, status: str, **extra) -> None:
        sets, args = ["status=$2", "updated_at=now()"], [case_id, status]
        for k, v in extra.items():
            sets.append(f"{k}=${len(args) + 1}")
            args.append(json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v)
        async with self.pool.acquire() as conn:
            await conn.execute(f"update cases set {', '.join(sets)} where case_id=$1", *args)

    async def get_case(self, case_id: str) -> dict | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("select * from cases where case_id=$1", case_id)
        return dict(row) if row else None

    async def list_cases(
        self,
        *,
        status: str | None = None,
        signal: str | None = None,
        plan_source: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        where, args = "where true", []
        for col, val in (("status", status), ("signal", signal), ("plan_source", plan_source)):
            if val:
                args.append(val)
                where += f" and {col}=${len(args)}"
        args.append(limit)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                f"select c.*, (select count(*) from stage_runs s where s.case_id=c.case_id) as stage_count,"
                f" (select max(started_at) from stage_runs s where s.case_id=c.case_id) as last_at "
                f"from cases c {where} order by c.created_at desc limit ${len(args)}",
                *args,
            )
        return [dict(r) for r in rows]

    # ---------- stage_runs（编排层唯一写者） ----------
    async def stage_run_open(
        self,
        *,
        key: str,
        case_id: str | None,
        run_id: str,
        scan_id: str | None,
        stage: str,
        step_id: str,
        mode: str | None,
        attempt: int,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """insert into stage_runs(case_id, run_id, scan_id, stage, step_id, mode, attempt,
                                          status, idempotency_key)
                   values($1,$2,$3,$4,$5,$6,$7,'dispatched',$8)
                   on conflict (idempotency_key) do update set status='dispatched', started_at=now()""",
                case_id,
                run_id,
                scan_id,
                stage,
                step_id,
                mode,
                attempt,
                key,
            )

    async def stage_run_close(
        self, *, key: str, status: str, output: dict | None, error: dict | None
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """update stage_runs set status=$2, finished_at=now(),
                          latency_ms=cast(extract(epoch from (now()-started_at))*1000 as int),
                          output=$3::jsonb, error=$4::jsonb
                   where idempotency_key=$1""",
                key,
                status,
                json.dumps(output, ensure_ascii=False) if output is not None else None,
                json.dumps(error, ensure_ascii=False) if error is not None else None,
            )

    async def stage_run_mark_late(
        self, key: str, status: str, output: dict | None, error: dict | None
    ) -> int:
        """迟到的 result（编排层已判超时）：只允许把 dispatched/timeout/failed 改写为 late，
        不能让迟到的成功结果复活已判超时的 attempt。"""
        async with self.pool.acquire() as conn:
            res = await conn.execute(
                """update stage_runs set status='late', finished_at=now(),
                          latency_ms=cast(extract(epoch from (now()-started_at))*1000 as int),
                          output=coalesce($2::jsonb, output), error=coalesce($3::jsonb, error)
                   where idempotency_key=$1 and status in ('dispatched','timeout','failed')""",
                key,
                json.dumps(output, ensure_ascii=False) if output is not None else None,
                json.dumps(error, ensure_ascii=False) if error is not None else None,
            )
        try:
            return int(res.split()[-1])
        except Exception:
            return 0

    async def count_workflows(self) -> int:
        async with self.pool.acquire() as conn:
            return int(await conn.fetchval("select count(*) from workflows"))

    async def max_attempt_by_step(self, case_id: str) -> dict[str, int]:
        """回放（replay）时用它给 attempt 播种：新 attempt 必须比历史上限更大，
        这样 idempotency_key 全新，Agent 不会把重放当成重复消息丢弃。"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "select step_id, max(attempt) a from stage_runs where case_id=$1 group by step_id",
                case_id,
            )
        return {r["step_id"]: int(r["a"]) for r in rows}

    async def trace(self, case_id: str) -> list[dict]:
        async with self.pool.acquire() as conn:
            case = await conn.fetchrow("select run_id from cases where case_id=$1", case_id)
            if not case:
                return []
            rows = await conn.fetch(
                """select s.* from stage_runs s
                   where s.case_id=$1 or (s.case_id is null and s.run_id=$2 and s.stage='monitor')
                   order by s.started_at, s.id""",
                case_id,
                case["run_id"],
            )
        return [dict(r) for r in rows]

    async def get_stage_output(self, case_id: str, stage: str) -> dict | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "select output from stage_runs where case_id=$1 and stage=$2 and status='done' "
                "order by attempt desc limit 1",
                case_id,
                stage,
            )
        return row["output"] if row else None

    # ---------- 计划 / workflow ----------
    async def upsert_plan(
        self,
        *,
        plan_id: str,
        case_id: str | None,
        source: str,
        workflow_key: str | None,
        degraded: bool,
        path: str,
        sha256: str,
        validator: str,
        steps: list[dict],
        rationale: str | None,
        model: str | None,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """insert into plans(plan_id, case_id, source, workflow_key, degraded, file_path,
                                     yaml_sha256, validator, steps, rationale, model)
                   values($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11)
                   on conflict (plan_id) do nothing""",
                plan_id,
                case_id,
                source,
                workflow_key,
                degraded,
                path,
                sha256,
                validator,
                json.dumps(steps, ensure_ascii=False),
                rationale,
                model,
            )

    async def get_plan(self, case_id: str) -> dict | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "select p.* from plans p join cases c on c.case_id=$1 where p.case_id=c.case_id "
                "order by p.created_at desc limit 1",
                case_id,
            )
        return dict(row) if row else None

    async def list_plans(self, limit: int = 50) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("select * from plans order by created_at desc limit $1", limit)
        return [dict(r) for r in rows]

    async def upsert_workflow(
        self,
        *,
        key: str,
        title: str,
        enabled: bool,
        priority: int,
        match: dict,
        steps: list[dict],
        source: str,
        version: int,
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """insert into workflows(key, title, enabled, priority, match, steps, source, version)
                   values($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8)
                   on conflict (key) do update set title=$2, enabled=$3, priority=$4, match=$5::jsonb,
                     steps=$6::jsonb, source=$7, version=$8, updated_at=now()""",
                key,
                title,
                enabled,
                priority,
                json.dumps(match, ensure_ascii=False),
                json.dumps(steps, ensure_ascii=False),
                source,
                version,
            )

    async def list_workflows(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("select * from workflows order by key")
        return [dict(r) for r in rows]

    # ---------- scans ----------
    async def scan_open(self, scan_id: str, run_id: str, scenario: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "insert into scans(scan_id, run_id, scenario, started_at) values($1,$2,$3,now()) "
                "on conflict (scan_id) do nothing",
                scan_id,
                run_id,
                scenario,
            )

    async def scan_close(self, scan_id: str, status: str, counts: dict, error: dict | None) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "update scans set finished_at=now(), status=$2, counts=$3::jsonb, error=$4::jsonb "
                "where scan_id=$1",
                scan_id,
                status,
                json.dumps(counts, ensure_ascii=False),
                json.dumps(error, ensure_ascii=False) if error else None,
            )

    async def list_scans(self, limit: int = 30) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("select * from scans order by started_at desc limit $1", limit)
        return [dict(r) for r in rows]

    # ---------- 工单 / 复核 / 日报 ----------
    async def save_ticket(self, *, case_id: str, backend: str, output: dict) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """insert into tickets(case_id, backend, external_key, url, ticket_status, duplicate_of,
                                       ticket_priority, assignee_team, payload)
                   values($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
                   on conflict (case_id, backend) do update set external_key=$3, url=$4,
                     ticket_status=$5, duplicate_of=$6, ticket_priority=$7, assignee_team=$8,
                     payload=$9::jsonb, created_at=now()""",
                case_id,
                backend,
                output.get("external_key"),
                output.get("url"),
                output.get("ticket_status"),
                output.get("duplicate_of"),
                output.get("ticket_priority"),
                output.get("assignee_team"),
                json.dumps(output, ensure_ascii=False),
            )

    async def save_review(self, *, case_id: str, output: dict, loop_seq: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """insert into reviews(case_id, verdict, reason_code, requested_info, checks, quality, loop_seq)
                   values($1,$2,$3,$4::jsonb,$5::jsonb,$6::jsonb,$7)""",
                case_id,
                output.get("verdict"),
                output.get("reason_code"),
                json.dumps(output.get("requested_info", []), ensure_ascii=False),
                json.dumps(output.get("checks", []), ensure_ascii=False),
                json.dumps(output.get("quality_scores", {}), ensure_ascii=False),
                loop_seq,
            )

    async def save_daily_report(
        self, *, day: str, report_md: str, metrics: dict, cases: list[dict]
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """insert into daily_reports(day, report_md, metrics, cases, published_at)
                   values($1::date,$2,$3::jsonb,$4::jsonb,now())
                   on conflict (day) do update set report_md=$2, metrics=$3::jsonb,
                     cases=$4::jsonb, published_at=now()""",
                _as_date(day),
                report_md,
                json.dumps(metrics, ensure_ascii=False),
                json.dumps(cases, ensure_ascii=False),
            )

    async def get_daily_report(self, day: str) -> dict | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "select * from daily_reports where day=$1::date", _as_date(day)
            )
        return dict(row) if row else None

    async def daily_metrics(self, day: str) -> dict[str, Any]:
        # 统计窗口 = [REPORT_TZ 下的 day 00:00, 次日 00:00)：不能用 created_at::date，
        # 那是服务器（UTC）的日界，与报表日的时区不一致时会把整段数据算漏。
        from .dates import day_range

        start, end = day_range(day)
        async with self.pool.acquire() as conn:
            cases = await conn.fetch(
                "select signal, status, plan_source, signature from cases "
                "where created_at >= $1 and created_at < $2",
                start,
                end,
            )
            rcas = await conn.fetch(
                """select output->>'rca' as rca from stage_runs
                   where stage='triage' and status='done'
                     and started_at >= $1 and started_at < $2""",
                start,
                end,
            )
            tats = await conn.fetch(
                """select extract(epoch from (max(finished_at)-min(started_at)))*1000 as ms
                   from stage_runs where case_id in
                     (select case_id from cases where created_at >= $1 and created_at < $2)
                   group by case_id having count(*)>1""",
                start,
                end,
            )
            scans = await conn.fetch(
                "select counts from scans where started_at >= $1 and started_at < $2", start, end
            )
        from collections import Counter

        return {
            "cases_total": len(cases),
            "errors": sum(1 for c in cases if c["signal"] == "error"),
            "observed_only": sum(1 for c in cases if c["signal"] == "none"),
            "status_distribution": dict(Counter(c["status"] for c in cases)),
            "plan_source_distribution": dict(Counter(c["plan_source"] for c in cases)),
            "rca_distribution": dict(Counter(r["rca"] for r in rcas if r["rca"])),
            "suppressed": sum(
                (json.loads(s["counts"]) if isinstance(s["counts"], str) else s["counts"]).get(
                    "suppressed", 0
                )
                for s in scans
            ),
            "scans": len(scans),
            "tat_ms_avg": int(sum(r["ms"] for r in tats) / len(tats)) if tats else 0,
        }
