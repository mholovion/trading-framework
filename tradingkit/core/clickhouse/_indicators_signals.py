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

    #: Columns strategy_signals has of its own. Anything else a strategy emits is kept in
    #: the metadata JSON rather than dropped -- see store_signal().
    _SIGNAL_COLUMNS = ("signal_type", "confidence", "price", "metadata")

    async def store_signal(
        self,
        params: StrategyParams,
        exchange: str,
        symbol: str,
        timestamp: int,
        record: dict | None = None,
        **fields: Any,
    ) -> None:
        """
        Store one signal. A signal is a free record (see Signal): the strategy author owns
        its schema, so this takes whatever it was given.

        `strategy_signals` still has fixed columns, so fields matching them fill those and
        **everything else goes into the metadata JSON** — recovered on the way out by
        fetch_signals(), which merges it back. That round-trip has to stay lossless: the
        bug this replaces put a field somewhere the serializer didn't know about, so it
        silently disappeared. Storing signals under the strategy's own schema, the way
        DataCollector already stores arbitrary source schemas, is the cleaner end state
        and a separate migration — this keeps the data intact until then.

        Accepts either a record dict or keyword fields, so callers can pass a signal
        straight through.
        """
        await self._ensure_params_registered(params, "strategy")
        fields = {**(record or {}), **fields}

        metadata = dict(fields.get("metadata") or {})
        extra = {
            k: v for k, v in fields.items()
            if k not in self._SIGNAL_COLUMNS and k != "timestamp"
        }
        metadata.update(extra)

        signal_type = fields.get("signal_type") or ""
        confidence  = fields.get("confidence")
        price       = fields.get("price")

        self._signal_buffer.append((
            params.type, params.to_hash(), exchange, symbol,
            timestamp, str(signal_type).upper(),
            float(confidence) if confidence is not None else 0.0,
            float(price) if price is not None else 0.0,
            json.dumps(metadata, default=str),
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
        out = []
        for r in rows:
            metadata = json.loads(r[4])
            # Fields the strategy emitted that this table has no column for were parked in
            # metadata by store_signal(); lift them back so the record the caller gets is
            # the record the strategy produced. metadata itself stays, so a reader that
            # only knows the old shape is unaffected.
            out.append({
                "timestamp": r[0], "signal_type": r[1], "confidence": r[2],
                "price": r[3], "metadata": metadata, **metadata,
            })
        return out

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