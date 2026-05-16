from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Any, Optional

from core.params import IndicatorParams, StrategyParams

logger = logging.getLogger(__name__)

# Seconds between timeframe start boundaries (used for candle counting estimates)
TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "12h": 43200,
    "1d": 86400, "1w": 604800,
}

# ClickHouse GROUP BY interval syntax for each timeframe
_TF_INTERVAL: dict[str, str] = {
    "1m":  "1 MINUTE",  "3m":  "3 MINUTE",  "5m":  "5 MINUTE",
    "15m": "15 MINUTE", "30m": "30 MINUTE",
    "1h":  "1 HOUR",    "2h":  "2 HOUR",    "4h":  "4 HOUR",
    "6h":  "6 HOUR",    "12h": "12 HOUR",
    "1d":  "1 DAY",     "1w":  "1 WEEK",
}

_FLUSH_INTERVAL = 2.0   # seconds between auto-flushes
_CANDLE_BATCH   = 500
_IND_BATCH      = 1000


class ClickHouseManager:
    """
    Async ClickHouse client with write-side batch buffering.

    Uses the `asynch` library (native binary protocol, port 9000).
    Falls back gracefully if asynch is unavailable.
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
        self._candle_buffer:    list[tuple] = []
        self._indicator_buffer: list[tuple] = []
        self._signal_buffer:    list[tuple] = []
        self._flush_task:       Optional[asyncio.Task] = None

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def initialize(self) -> None:
        try:
            import asynch
            self._conn = asynch.Connection(
                host=self.host, port=self.port,
                database=self.database,
                user=self.user, password=self.password,
            )
            await self._conn.connect()
            logger.info(f"ClickHouse connected: {self.host}:{self.port}/{self.database}")
        except ImportError:
            logger.warning("asynch not installed — ClickHouse unavailable (pip install asynch)")
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

    async def store_candle(self, candle: dict) -> None:
        self._candle_buffer.append((
            candle["timestamp"],
            candle.get("exchange", ""),
            candle.get("symbol", ""),
            candle.get("timeframe", "1m"),
            float(candle.get("open",   candle.get("open_price",   0))),
            float(candle.get("high",   candle.get("high_price",   0))),
            float(candle.get("low",    candle.get("low_price",    0))),
            float(candle.get("close",  candle.get("close_price",  0))),
            float(candle.get("volume", 0)),
        ))
        if len(self._candle_buffer) >= _CANDLE_BATCH:
            await self._flush_candles()

    async def store_indicator(
        self,
        params: IndicatorParams,
        exchange: str,
        symbol: str,
        values: list[tuple[int, float]],
    ) -> None:
        """Bulk-store a list of (timestamp, value) pairs for one indicator config."""
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
    # Read helpers                                                         #
    # ------------------------------------------------------------------ #

    async def fetch_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_ts: int | None = None,
        end_ts:   int | None = None,
        limit:    int | None = None,
        order:    str = 'ASC',
    ) -> list[dict]:
        if not self._conn:
            return []

        where = "exchange = %(ex)s AND symbol = %(sym)s AND timeframe = %(tf)s"
        params: dict[str, Any] = {"ex": exchange, "sym": symbol, "tf": timeframe}

        if start_ts is not None:
            where += " AND timestamp >= %(st)s"
            params["st"] = start_ts
        if end_ts is not None:
            where += " AND timestamp <= %(et)s"
            params["et"] = end_ts

        order_dir = "DESC" if order.upper() == "DESC" else "ASC"
        order   = f"ORDER BY timestamp {order_dir}"
        lim_sql = f"LIMIT {int(limit)}" if limit else ""

        sql = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM candles
            WHERE {where}
            {order} {lim_sql}
        """
        rows = await self._execute(sql, params)
        return [
            {"timestamp": r[0], "open": r[1], "high": r[2],
             "low": r[3], "close": r[4], "volume": r[5]}
            for r in rows
        ]

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

        # FINAL deduplicates ReplacingMergeTree rows that haven't been merged yet
        sql = f"SELECT timestamp, value FROM indicators FINAL WHERE {where} ORDER BY timestamp ASC"
        rows = await self._execute(sql, kw)
        return [(r[0], r[1]) for r in rows]

    async def get_indicator_coverage(
        self,
        params: IndicatorParams,
        exchange: str,
        symbol: str,
    ) -> tuple[int | None, int | None, int]:
        """Return (min_ts, max_ts, count) for cached indicator data."""
        if not self._conn:
            return None, None, 0

        sql = """
            SELECT min(timestamp), max(timestamp), count()
            FROM indicators
            WHERE indicator_type = %(t)s AND params_hash = %(h)s
              AND exchange = %(ex)s AND symbol = %(sym)s
        """
        rows = await self._execute(sql, {
            "t": params.type, "h": params.to_hash(),
            "ex": exchange, "sym": symbol,
        })
        if rows:
            return rows[0][0] or None, rows[0][1] or None, int(rows[0][2])
        return None, None, 0

    async def count_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_ts: int | None = None,
        end_ts:   int | None = None,
    ) -> int:
        if not self._conn:
            return 0
        where = "exchange=%(ex)s AND symbol=%(sym)s AND timeframe=%(tf)s"
        params: dict = {"ex": exchange, "sym": symbol, "tf": timeframe}
        if start_ts is not None:
            where += " AND timestamp >= %(st)s"
            params["st"] = start_ts
        if end_ts is not None:
            where += " AND timestamp <= %(et)s"
            params["et"] = end_ts
        rows = await self._execute(
            f"SELECT count() FROM candles FINAL WHERE {where}", params
        )
        return int(rows[0][0]) if rows else 0

    async def get_candle_range(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_ts: int | None = None,
        end_ts:   int | None = None,
    ) -> tuple[int | None, int | None]:
        """Return (min_ts, max_ts) for the given filter range."""
        if not self._conn:
            return None, None
        where = "exchange=%(ex)s AND symbol=%(sym)s AND timeframe=%(tf)s"
        kw: dict[str, Any] = {"ex": exchange, "sym": symbol, "tf": timeframe}
        if start_ts is not None:
            where += " AND timestamp >= %(st)s"; kw["st"] = start_ts
        if end_ts is not None:
            where += " AND timestamp <= %(et)s"; kw["et"] = end_ts
        rows = await self._execute(
            f"SELECT min(timestamp), max(timestamp) FROM candles FINAL WHERE {where}", kw
        )
        if rows and rows[0][0]:
            return int(rows[0][0]), int(rows[0][1])
        return None, None

    async def find_candle_gaps(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int,
        tf_seconds: int,
    ) -> list[dict]:
        """
        Use ClickHouse neighbor() to find gaps between consecutive candles.
        Returns list of {'start_timestamp', 'end_timestamp', 'missing_candles'}.
        """
        if not self._conn:
            return []
        sql = """
            SELECT
                prev_ts + %(tf)s  AS gap_start,
                cur_ts            AS gap_end,
                toUInt64((cur_ts - prev_ts) / %(tf)s - 1) AS missing_candles
            FROM (
                SELECT
                    timestamp AS cur_ts,
                    lagInFrame(timestamp) OVER (ORDER BY timestamp) AS prev_ts
                FROM candles FINAL
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
             "missing_candles": int(r[2])}
            for r in rows if r[2] > 0
        ]

    async def get_distinct_symbols(self) -> list[dict]:
        """Return distinct (exchange, symbol, timeframe) combinations."""
        if not self._conn:
            return []
        rows = await self._execute(
            "SELECT DISTINCT exchange, symbol, timeframe FROM candles ORDER BY exchange, symbol, timeframe"
        )
        return [{"exchange": r[0], "symbol": r[1], "timeframe": r[2]} for r in rows]

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
    # Candle aggregation (on-demand resample)                             #
    # ------------------------------------------------------------------ #

    async def aggregate_candles(
        self,
        exchange: str,
        symbol: str,
        src: str,
        dst: str,
    ) -> int:
        """
        Resample candles from `src` timeframe to `dst` and INSERT into candles table.
        Only inserts periods not yet present in the cache.
        Returns number of rows inserted.
        """
        if not self._conn:
            return 0

        interval = _TF_INTERVAL.get(dst)
        if not interval:
            raise ValueError(f"Unknown timeframe '{dst}'")

        sql = f"""
            INSERT INTO candles
            SELECT
                toUnixTimestamp(toStartOfInterval(
                    toDateTime(c.timestamp), INTERVAL {interval}
                ))                             AS timestamp,
                c.exchange,
                c.symbol,
                %(dst)s                        AS timeframe,
                argMin(c.open,  c.timestamp)   AS open,
                max(c.high)                    AS high,
                min(c.low)                     AS low,
                argMax(c.close, c.timestamp)   AS close,
                sum(c.volume)                  AS volume
            FROM candles AS c
            WHERE c.exchange  = %(ex)s
              AND c.symbol    = %(sym)s
              AND c.timeframe = %(src)s
              AND toStartOfInterval(toDateTime(c.timestamp), INTERVAL {interval})
                  NOT IN (
                      SELECT toDateTime(timestamp)
                      FROM candles
                      WHERE exchange  = %(ex)s
                        AND symbol    = %(sym)s
                        AND timeframe = %(dst)s
                  )
            GROUP BY
                toStartOfInterval(toDateTime(c.timestamp), INTERVAL {interval}),
                c.exchange, c.symbol
        """
        await self._execute(sql, {"ex": exchange, "sym": symbol,
                                  "src": src, "dst": dst})

        inserted = await self.count_candles(exchange, symbol, dst)
        logger.info(f"Aggregated {src}→{dst} for {exchange} {symbol}: "
                    f"{inserted} total cached periods")
        return inserted

    # ------------------------------------------------------------------ #
    # Params registry (bidirectional hash ↔ JSON)                         #
    # ------------------------------------------------------------------ #

    async def _ensure_params_registered(
        self,
        params: IndicatorParams | StrategyParams,
        kind: str,
    ) -> None:
        sql = """
            INSERT INTO params_meta (params_hash, params_type, params_json)
            VALUES (%(h)s, %(k)s, %(j)s)
        """
        await self._execute(sql, {
            "h": params.to_hash(), "k": kind, "j": params.to_json(),
        })

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
        await self._flush_candles()
        await self._flush_indicators()
        await self._flush_signals()

    async def _flush_candles(self) -> None:
        if not self._candle_buffer or not self._conn:
            return
        batch, self._candle_buffer = self._candle_buffer, []
        await self._bulk_insert(
            "candles",
            ["timestamp","exchange","symbol","timeframe",
             "open","high","low","close","volume"],
            batch,
        )

    async def _flush_indicators(self) -> None:
        if not self._indicator_buffer or not self._conn:
            return
        batch, self._indicator_buffer = self._indicator_buffer, []
        await self._bulk_insert(
            "indicators",
            ["indicator_type","params_hash","exchange","symbol","timestamp","value"],
            batch,
        )

    async def _flush_signals(self) -> None:
        if not self._signal_buffer or not self._conn:
            return
        batch, self._signal_buffer = self._signal_buffer, []
        await self._bulk_insert(
            "strategy_signals",
            ["strategy_type","params_hash","exchange","symbol",
             "timestamp","signal_type","confidence","price","metadata"],
            batch,
        )

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
        """Format a Python value for safe inline ClickHouse SQL interpolation."""
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
        """Replace %(name)s placeholders with safely formatted values."""
        if not params:
            return sql
        def _sub(m: re.Match) -> str:
            return ClickHouseManager._fmt(params[m.group(1)])
        return re.sub(r"%\((\w+)\)s", _sub, sql)

    async def _execute(self, sql: str, params: dict | None = None) -> list:
        if not self._conn:
            return []
        rendered = self._interpolate(sql, params)
        async with self._lock:
            try:
                async with self._conn.cursor() as cur:
                    await cur.execute(rendered)
                    try:
                        return await cur.fetchall()
                    except Exception:
                        return []
            except Exception as e:
                logger.error(f"ClickHouse query error: {e}\nSQL: {rendered[:300]}")
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


# ------------------------------------------------------------------ #
# Factory — reads connection params from env / config               #
# ------------------------------------------------------------------ #

def create_clickhouse_manager(config: dict | None = None) -> ClickHouseManager:
    cfg = config or {}
    return ClickHouseManager(
        host     = os.getenv("CLICKHOUSE_HOST")     or cfg.get("host",     "localhost"),
        port     = int(os.getenv("CLICKHOUSE_PORT") or cfg.get("port",     9000)),
        database = os.getenv("CLICKHOUSE_DATABASE") or cfg.get("database", "default"),
        user     = os.getenv("CLICKHOUSE_USER")     or cfg.get("user",     "default"),
        password = os.getenv("CLICKHOUSE_PASSWORD", cfg.get("password",   "")),
    )
