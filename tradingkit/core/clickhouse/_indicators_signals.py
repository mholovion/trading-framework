"""tradingkit.core.clickhouse._indicators_signals — indicator/signal storage, fetch, and flush."""
from __future__ import annotations

import json
import logging
from typing import Any

from tradingkit.core.params import IndicatorParams, StrategyParams

logger = logging.getLogger(__name__)

_IND_BATCH = 1000


class _IndicatorsSignalsMixin:
    """Write-buffered indicator values and strategy signals, plus their read paths."""

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