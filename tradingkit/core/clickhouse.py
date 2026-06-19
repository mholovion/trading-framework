from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Any, Optional, Union

import polars as pl

from tradingkit.core.params import IndicatorParams, StrategyParams

logger = logging.getLogger(__name__)

TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "12h": 43200,
    "1d": 86400, "1w": 604800,
}

_FLUSH_INTERVAL = 0.5
_IND_BATCH      = 1000

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


class ClickHouseManager:
    """
    Async ClickHouse client with write-side batch buffering.
    Uses HTTP interface (port 8123) for queries; native protocol (port 9000) for bulk inserts.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9000,
        database: str = "default",
        user: str = "default",
        password: str = "",
    ) -> None:
        self.host     = host
        self.port     = port
        self.database = database
        self.user     = user
        self.password = password

        self._conn: Any = None
        self._lock = asyncio.Lock()
        self._indicator_buffer: list[tuple] = []
        self._signal_buffer:    list[tuple] = []
        self._flush_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def initialize(self) -> None:
        try:
            import asynch
            self._conn = asynch.Connection(
                host=self.host, port=self.port,
                database=self.database, user=self.user, password=self.password,
            )
            await self._conn.connect()
            logger.info(f"ClickHouse connected: {self.host}:{self.port}/{self.database}")
        except ImportError:
            logger.warning("asynch not installed — ClickHouse unavailable")
            return
        except Exception as e:
            logger.error(f"ClickHouse connection failed: {e}")
            raise
        self._flush_task = asyncio.create_task(self._auto_flush_loop())

    async def close(self) -> None:
        if self._flush_task:
            self._flush_task.cancel()
        await self.flush()
        if self._conn:
            await self._conn.close()

    # ------------------------------------------------------------------ #
    # Write helpers                                                        #
    # ------------------------------------------------------------------ #

    async def store_indicator(
        self,
        params: IndicatorParams,
        exchange: str,
        symbol: str,
        values: list[tuple[int, float]],
    ) -> None:
        await self._ensure_params_registered(params, "indicator")
        h = params.to_hash()
        for ts, val in values:
            self._indicator_buffer.append((params.type, h, exchange, symbol, ts, float(val)))
            if len(self._indicator_buffer) >= _IND_BATCH:
                await self._flush_indicators()

    async def store_signal(
        self,
        params: StrategyParams,
        exchange: str,
        symbol: str,
        timestamp: int,
        signal_type: str,
        confidence: float,
        price: float,
        metadata: dict | None = None,
    ) -> None:
        await self._ensure_params_registered(params, "strategy")
        self._signal_buffer.append((
            params.type, params.to_hash(), exchange, symbol,
            timestamp, signal_type.upper(), float(confidence), float(price),
            json.dumps(metadata or {}),
        ))
        if len(self._signal_buffer) >= 200:
            await self._flush_signals()

    # ------------------------------------------------------------------ #
    # Read helpers — dict-based (backward compat)                          #
    # ------------------------------------------------------------------ #

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

    # ------------------------------------------------------------------ #
    # NEW: Polars-based aggregated fetch — arbitrary seconds               #
    # ------------------------------------------------------------------ #

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

    # ------------------------------------------------------------------ #
    # Indicator / signal read helpers                                       #
    # ------------------------------------------------------------------ #

    async def fetch_indicator(
        self,
        params: IndicatorParams,
        exchange: str,
        symbol: str,
        start_ts: int | None = None,
        end_ts:   int | None = None,
    ) -> list[tuple[int, float]]:
        if not self._conn:
            return []
        where = ("indicator_type = %(t)s AND params_hash = %(h)s"
                 " AND exchange = %(ex)s AND symbol = %(sym)s")
        kw: dict[str, Any] = {
            "t": params.type, "h": params.to_hash(),
            "ex": exchange, "sym": symbol,
        }
        if start_ts is not None:
            where += " AND timestamp >= %(st)s"; kw["st"] = start_ts
        if end_ts is not None:
            where += " AND timestamp <= %(et)s"; kw["et"] = end_ts
        sql = f"SELECT timestamp, value FROM indicators FINAL WHERE {where} ORDER BY timestamp ASC"
        rows = await self._execute(sql, kw)
        return [(r[0], r[1]) for r in rows]

    async def get_indicator_coverage(
        self, params: IndicatorParams, exchange: str, symbol: str,
    ) -> tuple[int | None, int | None, int]:
        if not self._conn:
            return None, None, 0
        sql = """
            SELECT min(timestamp), max(timestamp), count()
            FROM indicators
            WHERE indicator_type = %(t)s AND params_hash = %(h)s
              AND exchange = %(ex)s AND symbol = %(sym)s
        """
        rows = await self._execute(sql, {
            "t": params.type, "h": params.to_hash(), "ex": exchange, "sym": symbol,
        })
        if rows:
            return rows[0][0] or None, rows[0][1] or None, int(rows[0][2])
        return None, None, 0

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

    async def get_distinct_symbols(self, table_name: str = "candles") -> list[dict]:
        if not self._conn:
            return []
        rows = await self._execute(
            f"SELECT DISTINCT exchange, symbol, timeframe FROM {table_name} ORDER BY exchange, symbol, timeframe"
        )
        return [{"exchange": r[0], "symbol": r[1], "timeframe": r[2]} for r in rows]

    # ------------------------------------------------------------------ #
    # Connections (dynamic source connections, replaces connections.yaml)  #
    # ------------------------------------------------------------------ #

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
        try:
            async with _aiohttp.ClientSession() as sess:
                async with sess.post(f"http://{self.host}:8123/", data=create_sql) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning(f"connections table HTTP {resp.status}: {body[:200]}")
                async with sess.post(f"http://{self.host}:8123/", data=alter_sql) as resp:
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

    # ------------------------------------------------------------------ #
    # Plugin library — code access                                         #
    # ------------------------------------------------------------------ #

    async def get_plugin_code(self, namespace: str, type_: str, name: str) -> str | None:
        if not self._conn:
            return None
        rows = await self._execute(
            "SELECT code FROM plugin_library FINAL"
            " WHERE namespace=%(ns)s AND type=%(t)s AND name=%(n)s LIMIT 1",
            {"ns": namespace, "t": type_, "n": name},
        )
        return rows[0][0] if rows else None

    async def list_projects(self) -> list[str]:
        """Return distinct project namespaces from plugin_library."""
        if not self._conn:
            return []
        rows = await self._execute(
            "SELECT DISTINCT namespace FROM plugin_library FINAL"
            " WHERE namespace != 'shared' ORDER BY namespace"
        )
        return [r[0] for r in rows]

    async def fetch_signals(
        self,
        params: StrategyParams,
        exchange: str,
        symbol: str,
        start_ts: int | None = None,
        end_ts:   int | None = None,
    ) -> list[dict]:
        if not self._conn:
            return []
        where = ("strategy_type=%(t)s AND params_hash=%(h)s"
                 " AND exchange=%(ex)s AND symbol=%(sym)s")
        kw: dict[str, Any] = {
            "t": params.type, "h": params.to_hash(),
            "ex": exchange, "sym": symbol,
        }
        if start_ts is not None:
            where += " AND timestamp>=%(st)s"; kw["st"] = start_ts
        if end_ts is not None:
            where += " AND timestamp<=%(et)s"; kw["et"] = end_ts
        sql = (f"SELECT timestamp, signal_type, confidence, price, metadata"
               f" FROM strategy_signals WHERE {where} ORDER BY timestamp ASC")
        rows = await self._execute(sql, kw)
        return [
            {"timestamp": r[0], "signal_type": r[1], "confidence": r[2],
             "price": r[3], "metadata": json.loads(r[4])}
            for r in rows
        ]

    # ------------------------------------------------------------------ #
    # Pipeline library (save/load pipelines)                               #
    # ------------------------------------------------------------------ #

    async def save_pipeline(self, pipeline_dict: dict) -> None:
        name = pipeline_dict.get("name", "unnamed")
        sql = """
            INSERT INTO plugin_library (name, type, code, params_json)
            VALUES (%(n)s, 'pipeline', '', %(j)s)
        """
        await self._execute(sql, {"n": name, "j": json.dumps(pipeline_dict)})

    async def load_pipeline(self, name: str) -> dict | None:
        rows = await self._execute(
            "SELECT params_json FROM plugin_library WHERE name=%(n)s AND type='pipeline' LIMIT 1",
            {"n": name},
        )
        return json.loads(rows[0][0]) if rows else None

    # ------------------------------------------------------------------ #
    # Params registry                                                       #
    # ------------------------------------------------------------------ #

    async def _ensure_params_registered(
        self, params: IndicatorParams | StrategyParams, kind: str,
    ) -> None:
        await self._execute(
            "INSERT INTO params_meta (params_hash, params_type, params_json) VALUES (%(h)s, %(k)s, %(j)s)",
            {"h": params.to_hash(), "k": kind, "j": params.to_json()},
        )

    async def lookup_params(self, hash_: str) -> dict | None:
        rows = await self._execute(
            "SELECT params_json FROM params_meta WHERE params_hash = %(h)s LIMIT 1",
            {"h": hash_},
        )
        return json.loads(rows[0][0]) if rows else None

    # ------------------------------------------------------------------ #
    # Flush                                                                #
    # ------------------------------------------------------------------ #

    async def flush(self) -> None:
        await self._flush_indicators()
        await self._flush_signals()

    async def _flush_indicators(self) -> None:
        if not self._indicator_buffer or not self._conn:
            return
        batch, self._indicator_buffer = self._indicator_buffer, []
        await self._bulk_insert(
            "indicators",
            ["indicator_type", "params_hash", "exchange", "symbol", "timestamp", "value"],
            batch,
        )

    async def _flush_signals(self) -> None:
        if not self._signal_buffer or not self._conn:
            return
        batch, self._signal_buffer = self._signal_buffer, []
        await self._bulk_insert(
            "strategy_signals",
            ["strategy_type", "params_hash", "exchange", "symbol",
             "timestamp", "signal_type", "confidence", "price", "metadata"],
            batch,
        )

    # ------------------------------------------------------------------ #
    # Unit data — generic DataUnit tables + MV aggregation               #
    # ------------------------------------------------------------------ #

    async def ensure_raw_table(self, table_name: str, schema: dict) -> None:
        """CREATE TABLE IF NOT EXISTS {table_name} (ReplacingMergeTree) from Polars schema."""
        from tradingkit.schema import POLARS_TO_CH
        cols = ["exchange String", "symbol String"]
        cols += [f"{col} {POLARS_TO_CH.get(dtype, 'String')}" for col, dtype in schema.items()]
        await self._execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols)})"
            " ENGINE = ReplacingMergeTree ORDER BY (exchange, symbol, timestamp)"
        )

    async def ensure_agg_table(self, raw_table: str, bucket_s: int, folds: list) -> None:
        """CREATE TABLE IF NOT EXISTS {raw_table}_{bucket_s}s (AggregatingMergeTree)."""
        from tradingkit.schema import Fold
        agg_table = f"{raw_table}_{bucket_s}s"
        col_ddl   = ["exchange String", "symbol String", "bucket UInt64"]
        for f in folds:
            if isinstance(f, Fold):
                col_ddl.append(f"{f._alias or f.field} {f.ch_agg_type()}")
        await self._execute(
            f"CREATE TABLE IF NOT EXISTS {agg_table} ({', '.join(col_ddl)})"
            " ENGINE = AggregatingMergeTree ORDER BY (exchange, symbol, bucket)"
        )

    async def ensure_mv(self, raw_table: str, bucket_s: int, folds: list) -> None:
        """CREATE MATERIALIZED VIEW IF NOT EXISTS mv_{raw_table}_to_{bucket_s}s."""
        from tradingkit.schema import Fold
        agg_table = f"{raw_table}_{bucket_s}s"
        mv_name   = f"mv_{raw_table}_to_{bucket_s}s"
        selects   = [
            "exchange",
            "symbol",
            f"intDiv(timestamp, {bucket_s}) * {bucket_s} AS bucket",
        ]
        for f in folds:
            if isinstance(f, Fold):
                alias = f._alias or f.field
                selects.append(f.ch_state_expr(alias))
        await self._execute(
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name} TO {agg_table}"
            f" AS SELECT {', '.join(selects)} FROM {raw_table}"
            f" GROUP BY exchange, symbol, bucket"
        )

    async def backfill_agg(self, raw_table: str, bucket_s: int, folds: list) -> None:
        """INSERT INTO agg_table SELECT *State(...) FROM raw_table GROUP BY bucket."""
        from tradingkit.schema import Fold
        agg_table = f"{raw_table}_{bucket_s}s"
        selects   = [
            "exchange",
            "symbol",
            f"intDiv(timestamp, {bucket_s}) * {bucket_s} AS bucket",
        ]
        for f in folds:
            if isinstance(f, Fold):
                alias = f._alias or f.field
                selects.append(f.ch_state_expr(alias))
        await self._execute(
            f"INSERT INTO {agg_table}"
            f" SELECT {', '.join(selects)} FROM {raw_table}"
            f" GROUP BY exchange, symbol, bucket"
        )

    async def insert_unit_batch(
        self,
        table_name: str,
        df: "pl.DataFrame",
        exchange: str,
        symbol: str,
    ) -> None:
        """Bulk-insert a Polars DataFrame into a raw unit table."""
        if df.is_empty():
            return
        rows   = df.to_dicts()
        tuples = tuple(
            (exchange, symbol, *[r[c] for c in df.columns]) for r in rows
        )
        await self._bulk_insert(table_name, ["exchange", "symbol"] + df.columns, tuples)

    async def get_unit_range(
        self,
        table_name: str,
        exchange: str,
        symbol: str,
    ) -> tuple[int | None, int | None]:
        """min/max timestamp in a raw unit table."""
        rows = await self._execute(
            f"SELECT min(timestamp), max(timestamp) FROM {table_name}"
            " WHERE exchange = %(ex)s AND symbol = %(sym)s",
            {"ex": exchange, "sym": symbol},
        )
        if rows and rows[0][0] is not None:
            return int(rows[0][0]), int(rows[0][1])
        return None, None

    async def find_unit_gaps(
        self,
        table_name: str,
        exchange: str,
        symbol: str,
        start_ts: int,
        end_ts: int,
        unit_interval_s: int = 60,
    ) -> list[dict]:
        """Generic gap detection via lagInFrame on any raw unit table."""
        sql = f"""
            SELECT
                prev_ts + {unit_interval_s}   AS gap_start,
                cur_ts                         AS gap_end,
                toUInt64((cur_ts - prev_ts) / {unit_interval_s} - 1) AS missing
            FROM (
                SELECT
                    timestamp AS cur_ts,
                    lagInFrame(timestamp) OVER (ORDER BY timestamp) AS prev_ts
                FROM {table_name} FINAL
                WHERE exchange = %(ex)s AND symbol = %(sym)s
                  AND timestamp BETWEEN %(st)s AND %(et)s
            )
            WHERE prev_ts > 0 AND (cur_ts - prev_ts) > {unit_interval_s}
        """
        rows = await self._execute(sql, {
            "ex": exchange, "sym": symbol, "st": start_ts, "et": end_ts,
        })
        return [
            {
                "start_timestamp": int(r[0]),
                "end_timestamp":   int(r[1]) - unit_interval_s,
                "missing_rows":    int(r[2]),
            }
            for r in rows if r[2] > 0
        ]

    async def get_table_schema(self, table_name: str) -> dict:
        """
        Return {column_name: polars_dtype} by parsing DESCRIBE TABLE output.
        Used by AggregationScript to build a DataUnit for aggregation(unit) calls.
        """
        _CH_TO_PL: dict[str, Any] = {
            "Int64":   pl.Int64,   "UInt64":  pl.UInt64,
            "Int32":   pl.Int32,   "UInt32":  pl.UInt32,
            "Float64": pl.Float64, "Float32": pl.Float32,
            "String":  pl.String,  "UInt8":   pl.UInt8,
        }
        rows = await self._execute(f"DESCRIBE TABLE {table_name}")
        schema: dict[str, Any] = {}
        for row in rows:
            col_name = row[0]
            ch_type  = row[1].split("(")[0]  # strip LowCardinality(…) etc.
            schema[col_name] = _CH_TO_PL.get(ch_type, pl.String)
        return schema

    async def fetch_from_unit_table(
        self,
        table_name: str,
        exchange: str,
        symbol: str | None = None,
        start_ts: int | None = None,
        end_ts:   int | None = None,
    ) -> "pl.DataFrame":
        """
        Generic SELECT * FROM any raw unit table with exchange/symbol/time filters.
        Returns a Polars DataFrame. Used by AggregationContext.query().
        """
        where_parts = ["exchange = %(ex)s"]
        params: dict[str, Any] = {"ex": exchange}
        if symbol is not None:
            where_parts.append("symbol = %(sym)s")
            params["sym"] = symbol
        if start_ts is not None:
            where_parts.append("timestamp >= %(st)s")
            params["st"] = start_ts
        if end_ts is not None:
            where_parts.append("timestamp <= %(et)s")
            params["et"] = end_ts
        sql = (
            f"SELECT * FROM {table_name}"
            f" WHERE {' AND '.join(where_parts)}"
            f" ORDER BY timestamp ASC"
        )
        rows = await self._execute(sql, params)
        if not rows:
            return pl.DataFrame()
        schema = await self.get_table_schema(table_name)
        col_names = list(schema.keys())
        return pl.DataFrame(
            [dict(zip(col_names, r)) for r in rows],
            schema=schema,
        )

    async def query_agg(
        self,
        raw_table: str,
        bucket_s: int,
        folds: list,
        exchange: str,
        symbol: str,
        start_ts: int | None = None,
        end_ts:   int | None = None,
    ) -> list[dict]:
        """SELECT *Merge() FROM {raw_table}_{bucket_s}s GROUP BY bucket."""
        from tradingkit.schema import Fold
        agg_table = f"{raw_table}_{bucket_s}s"
        ch_folds  = [f for f in folds if isinstance(f, Fold)]
        selects   = ["bucket"] + [f.ch_merge_expr(f._alias or f.field) for f in ch_folds]
        where     = "exchange = %(ex)s AND symbol = %(sym)s"
        kw: dict  = {"ex": exchange, "sym": symbol}
        if start_ts is not None:
            where += " AND bucket >= %(st)s"; kw["st"] = start_ts
        if end_ts is not None:
            where += " AND bucket < %(et)s";  kw["et"] = end_ts
        rows = await self._execute(
            f"SELECT {', '.join(selects)} FROM {agg_table}"
            f" WHERE {where} GROUP BY bucket ORDER BY bucket",
            kw,
        )
        cols = ["bucket"] + [f._alias or f.field for f in ch_folds]
        return [dict(zip(cols, r)) for r in rows]

    async def _auto_flush_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(_FLUSH_INTERVAL)
                await self.flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Auto-flush error: {e}")

    # ------------------------------------------------------------------ #
    # Low-level execute / insert                                           #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _fmt(val: Any) -> str:
        if val is None:
            return "NULL"
        if isinstance(val, bool):
            return "1" if val else "0"
        if isinstance(val, int):
            return str(val)
        if isinstance(val, float):
            return repr(val)
        escaped = str(val).replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"

    @staticmethod
    def _interpolate(sql: str, params: dict | None) -> str:
        if not params:
            return sql
        def _sub(m: re.Match) -> str:
            return ClickHouseManager._fmt(params[m.group(1)])
        return re.sub(r"%\((\w+)\)s", _sub, sql)

    async def _execute(self, sql: str, params: dict | None = None) -> list:
        import aiohttp as _aiohttp
        rendered   = self._interpolate(sql, params)
        first_word = rendered.lstrip().split()[0].upper() if rendered.strip() else ""
        is_select  = first_word in ("SELECT", "WITH", "SHOW")
        post_sql   = (rendered + " FORMAT JSONCompact") if is_select else rendered
        url = f"http://{self.host}:8123/?database={self.database}&output_format_json_quote_64bit_integers=0"
        try:
            async with _aiohttp.ClientSession() as sess:
                async with sess.post(url, data=post_sql.encode()) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error(f"ClickHouse error HTTP {resp.status}: {body[:300]}")
                        return []
                    if is_select:
                        result = await resp.json(content_type=None)
                        return result.get("data", [])
                    return []
        except Exception as e:
            logger.error(f"ClickHouse query error: {e}")
            return []

    async def _bulk_insert(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        if not rows or not self._conn:
            return
        async with self._lock:
            try:
                async with self._conn.cursor() as cur:
                    await cur.execute(
                        f"INSERT INTO {table} ({', '.join(columns)}) VALUES",
                        rows,
                    )
                logger.debug(f"Inserted {len(rows)} rows into {table}")
            except Exception as e:
                logger.error(f"Bulk insert into {table} failed: {e}")


def create_clickhouse_manager(config: dict | None = None) -> ClickHouseManager:
    cfg = config or {}
    return ClickHouseManager(
        host     = os.getenv("CLICKHOUSE_HOST")     or cfg.get("host",     "localhost"),
        port     = int(os.getenv("CLICKHOUSE_PORT") or cfg.get("port",     9000)),
        database = os.getenv("CLICKHOUSE_DATABASE") or cfg.get("database", "default"),
        user     = os.getenv("CLICKHOUSE_USER")     or cfg.get("user",     "default"),
        password = os.getenv("CLICKHOUSE_PASSWORD", cfg.get("password",   "")),
    )
