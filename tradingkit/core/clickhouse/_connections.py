"""tradingkit.core.clickhouse._connections — dynamic source connections (replaces connections.yaml)."""
from __future__ import annotations

import json
import logging

from tradingkit.core.clickhouse._sql import _basic_auth_header

logger = logging.getLogger(__name__)


class _ConnectionsMixin:
    """CRUD for the `connections` table — trading pairs collected via DataCollector."""

    async def ensure_connections_table(self) -> None:
        """Create the connections table via HTTP (DDL safe path)."""
        import aiohttp as _aiohttp
        create_sql = (
            "CREATE TABLE IF NOT EXISTS connections "
            "(name String, project LowCardinality(String) DEFAULT 'default', "
            " source String, symbol String, timeframe LowCardinality(String), "
            " start_date String, enabled UInt8 DEFAULT 1, "
            " aggregation String DEFAULT '', "
            " config String DEFAULT '{}', updated_at DateTime DEFAULT now()) "
            "ENGINE = ReplacingMergeTree(updated_at) ORDER BY name"
        )
        alter_sql = (
            "ALTER TABLE connections ADD COLUMN IF NOT EXISTS aggregation String DEFAULT ''"
        )
        headers = {"Authorization": _basic_auth_header(self.user, self.password)}
        try:
            async with _aiohttp.ClientSession() as sess:
                async with sess.post(f"http://{self.host}:{self.http_port}/", data=create_sql, headers=headers) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning(f"connections table HTTP {resp.status}: {body[:200]}")
                async with sess.post(f"http://{self.host}:{self.http_port}/", data=alter_sql, headers=headers) as resp:
                    pass  # ignore if column already exists
        except Exception as e:
            logger.warning(f"connections table creation skipped: {e}")

    async def list_connections(self) -> list[dict]:
        if not self._conn:
            return []
        rows = await self._execute(
            "SELECT name, project, source, symbol, timeframe, start_date, enabled, aggregation, config"
            " FROM connections FINAL ORDER BY name"
        )
        return [
            {
                "name":        r[0], "project":     r[1], "source":   r[2],
                "symbol":      r[3], "timeframe":   r[4], "start_date": r[5],
                "enabled":     bool(r[6]),
                "aggregation": r[7],
                "config":      r[8],
            }
            for r in rows
        ]

    async def upsert_connection(self, conn: dict) -> None:
        if not self._conn:
            return
        await self._execute(
            "INSERT INTO connections (name, project, source, symbol, timeframe,"
            " start_date, enabled, aggregation, config) VALUES"
            " (%(n)s, %(p)s, %(src)s, %(sym)s, %(tf)s, %(sd)s, %(en)s, %(agg)s, %(cfg)s)",
            {
                "n":   conn["name"],
                "p":   conn.get("project", "default"),
                "src": conn["source"],
                "sym": conn["symbol"],
                "tf":  conn["timeframe"],
                "sd":  conn.get("start_date", "2020-01-01T00:00:00Z"),
                "en":  1 if conn.get("enabled", True) else 0,
                "agg": conn.get("aggregation", ""),
                "cfg": json.dumps(conn.get("config", {}))
                       if isinstance(conn.get("config"), dict) else conn.get("config", "{}"),
            },
        )

    async def delete_connection(self, name: str) -> None:
        if not self._conn:
            return
        await self._execute(
            "ALTER TABLE connections DELETE WHERE name = %(n)s", {"n": name}
        )