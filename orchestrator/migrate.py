"""启动前置：确保目标数据库存在（复用集群 postgres，不新建实例）。"""
from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

import asyncpg

from agentkit.config import cfg

log = logging.getLogger("orchestrator.migrate")


def _parts(dsn: str) -> tuple[str, int, str, str, str]:
    u = urlparse(dsn)
    return (u.hostname or "localhost", u.port or 5432,
            (u.username or "postgres"), (u.password or ""), (u.path or "/postgres").lstrip("/"))


async def ensure_database(dsn: str | None = None) -> str:
    host, port, user, pw, dbname = _parts(dsn or cfg.pg_dsn)
    if not dbname or dbname == "postgres":
        return "postgres"
    conn = await asyncpg.connect(host=host, port=port, user=user, password=pw, database="postgres",
                                 timeout=15)
    try:
        exists = await conn.fetchval("select 1 from pg_database where datname=$1", dbname)
        if not exists:
            safe = re.fullmatch(r"[A-Za-z0-9_]+", dbname)
            if not safe:
                raise ValueError(f"数据库名不合法，拒绝创建: {dbname}")
            await conn.execute(f'create database "{dbname}"')
            log.info("created database %s", dbname)
        else:
            log.info("database %s exists", dbname)
    finally:
        await conn.close()
    return dbname


async def ensure_ready(db) -> None:
    async with db.pool.acquire() as conn:
        await conn.execute("select 1")
        tables = await conn.fetch("select tablename from pg_tables where schemaname='public'")
    log.info("db ready, tables=%d", len(tables))
