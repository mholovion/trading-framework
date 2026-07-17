"""tradingkit.core.clickhouse._candles — raw OHLCV candle read/write on the `candles` table."""
from __future__ import annotations

from typing import Any

import polars as pl

from tradingkit.core.clickhouse._sql import _validates_identifiers

_OHLCV_SCHEMA = {
    "timestamp": pl.Int64,
    "open":      pl.Float64,
    "high":      pl.Float64,
    "low":       pl.Float64,
    "close":     pl.Float64,
    "volume":    pl.Float64,
}


def _to_ch_interval(seconds: int) -> str:
    """Convert arbitrary seconds to a ClickHouse INTERVAL expression."""
    for divisor, unit in [(604800, "WEEK"), (86400, "DAY"), (3600, "HOUR"), (60, "MINUTE")]:
        if seconds % divisor == 0:
            return f"{seconds // divisor} {unit}"
    return f"{seconds} SECOND"


class _CandlesMixin:
    """Raw candle storage/read helpers — the `candles` table and its aggregated views."""

    @_validates_identifiers("table_name")
    async def fetch_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_ts: int | None = None,
        end_ts:   int | None = None,
        limit:    int | None = None,
        order:    str = "ASC",
        table_name: str = "candles",
    ) -> list[dict]:
        if not self._conn:
            return []
        where = "exchange = %(ex)s AND symbol = %(sym)s AND timeframe = %(tf)s"
        params: dict[str, Any] = {"ex": exchange, "sym": symbol, "tf": timeframe}
        if start_ts is not None:
            where += " AND timestamp >= %(st)s"; params["st"] = start_ts
        if end_ts is not None:
            where += " AND timestamp <= %(et)s"; params["et"] = end_ts
        order_dir = "DESC" if order.upper() == "DESC" else "ASC"
        lim_sql = f"LIMIT {int(limit)}" if limit else ""
        sql = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM {table_name}
            WHERE {where}
            ORDER BY timestamp {order_dir} {lim_sql}
        """
        rows = await self._execute(sql, params)
        return [
            {"timestamp": r[0], "open": r[1], "high": r[2],
             "low": r[3], "close": r[4], "volume": r[5]}
            for r in rows
        ]

    @_validates_identifiers("table_name")
    async def fetch_ohlcv_aggregated(
        self,
        exchange: str,
        symbol: str,
        source_seconds: int,
        target_seconds: int,
        start_ts: int,
        end_ts: int,
        limit: int = 10000,
        table_name: str = "candles",
    ) -> pl.DataFrame:
        """
        Fetch OHLCV data from ClickHouse, aggregating from source_seconds to target_seconds.
        Supports any number of seconds for both source and target timeframes.
        Returns a Polars DataFrame with OHLCV columns.

        Example:
            fetch_ohlcv_aggregated("whitebit", "SOL_USDT", 60, 5400, ...)
            → aggregates 1m rows into 90m (5400 seconds) bars
        """
        if not self._conn:
            return pl.DataFrame(schema=_OHLCV_SCHEMA)

        interval = _to_ch_interval(target_seconds)
        source_tf_str = str(source_seconds)

        sql = f"""
            SELECT
                toUnixTimestamp(
                    toStartOfInterval(toDateTime(timestamp), INTERVAL {interval})
                ) AS timestamp,
                argMin(open,  timestamp) AS open,
                max(high)                AS high,
                min(low)                 AS low,
                argMax(close, timestamp) AS close,
                sum(volume)              AS volume
            FROM {table_name}
            WHERE exchange  = %(ex)s
              AND symbol    = %(sym)s
              AND timeframe = %(tf)s
              AND timestamp >= %(st)s
              AND timestamp <  %(et)s
            GROUP BY timestamp
            ORDER BY timestamp ASC
            LIMIT %(lim)s
        """
        rows = await self._execute(sql, {
            "ex": exchange, "sym": symbol, "tf": source_tf_str,
            "st": start_ts, "et": end_ts, "lim": limit,
        })

        if not rows:
            return pl.DataFrame(schema=_OHLCV_SCHEMA)

        return pl.DataFrame(
            {
                "timestamp": [int(r[0]) for r in rows],
                "open":      [float(r[1]) for r in rows],
                "high":      [float(r[2]) for r in rows],
                "low":       [float(r[3]) for r in rows],
                "close":     [float(r[4]) for r in rows],
                "volume":    [float(r[5]) for r in rows],
            },
            schema=_OHLCV_SCHEMA,
        )

    async def fetch_data_df(
        self,
        exchange: str,
        symbol: str,
        timeframe_seconds: int,
        start_ts: int,
        end_ts: int,
        limit: int = 10000,
        table_name: str = "candles",
    ) -> pl.DataFrame:
        """Fetch raw stored rows (no aggregation) as Polars DataFrame."""
        return await self.fetch_ohlcv_aggregated(
            exchange, symbol,
            source_seconds=timeframe_seconds,
            target_seconds=timeframe_seconds,
            start_ts=start_ts,
            end_ts=end_ts,
            limit=limit,
            table_name=table_name,
        )

    @_validates_identifiers("table_name")
    async def count_rows(
        self, exchange: str, symbol: str, timeframe: str,
        start_ts: int | None = None, end_ts: int | None = None,
        table_name: str = "candles",
    ) -> int:
        if not self._conn:
            return 0
        where = "exchange=%(ex)s AND symbol=%(sym)s AND timeframe=%(tf)s"
        params: dict = {"ex": exchange, "sym": symbol, "tf": timeframe}
        if start_ts is not None:
            where += " AND timestamp >= %(st)s"; params["st"] = start_ts
        if end_ts is not None:
            where += " AND timestamp <= %(et)s"; params["et"] = end_ts
        rows = await self._execute(
            f"SELECT count() FROM {table_name} FINAL WHERE {where}", params
        )
        return int(rows[0][0]) if rows else 0

    @_validates_identifiers("table_name")
    async def get_data_range(
        self, exchange: str, symbol: str, timeframe: str,
        start_ts: int | None = None, end_ts: int | None = None,
        table_name: str = "candles",
    ) -> tuple[int | None, int | None]:
        if not self._conn:
            return None, None
        where = "exchange=%(ex)s AND symbol=%(sym)s AND timeframe=%(tf)s"
        kw: dict[str, Any] = {"ex": exchange, "sym": symbol, "tf": timeframe}
        if start_ts is not None:
            where += " AND timestamp >= %(st)s"; kw["st"] = start_ts
        if end_ts is not None:
            where += " AND timestamp <= %(et)s"; kw["et"] = end_ts
        rows = await self._execute(
            f"SELECT min(timestamp), max(timestamp) FROM {table_name} FINAL WHERE {where}", kw
        )
        if rows and rows[0][0]:
            return int(rows[0][0]), int(rows[0][1])
        return None, None

    @_validates_identifiers("table_name")
    async def find_gaps(
        self, exchange: str, symbol: str, timeframe: str,
        start_ts: int, end_ts: int, tf_seconds: int,
        table_name: str = "candles",
    ) -> list[dict]:
        if not self._conn:
            return []
        sql = f"""
            SELECT
                prev_ts + %(tf)s  AS gap_start,
                cur_ts            AS gap_end,
                toUInt64((cur_ts - prev_ts) / %(tf)s - 1) AS missing_rows
            FROM (
                SELECT
                    timestamp AS cur_ts,
                    lagInFrame(timestamp) OVER (ORDER BY timestamp) AS prev_ts
                FROM {table_name} FINAL
                WHERE exchange=%(ex)s AND symbol=%(sym)s AND timeframe=%(tf_str)s
                  AND timestamp BETWEEN %(st)s AND %(et)s
            )
            WHERE prev_ts > 0 AND (cur_ts - prev_ts) > %(tf)s
        """
        rows = await self._execute(sql, {
            "ex": exchange, "sym": symbol, "tf_str": timeframe,
            "st": start_ts, "et": end_ts, "tf": tf_seconds,
        })
        return [
            {"start_timestamp": int(r[0]), "end_timestamp": int(r[1]) - tf_seconds,
             "missing_rows": int(r[2])}
            for r in rows if r[2] > 0
        ]

    @_validates_identifiers("table_name")
    async def get_distinct_symbols(self, table_name: str = "candles") -> list[dict]:
        if not self._conn:
            return []
        rows = await self._execute(
            f"SELECT DISTINCT exchange, symbol, timeframe FROM {table_name} ORDER BY exchange, symbol, timeframe"
        )
        return [{"exchange": r[0], "symbol": r[1], "timeframe": r[2]} for r in rows]