from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Callable, Optional

from core.clickhouse import ClickHouseManager, TIMEFRAME_SECONDS
from core.params import IndicatorParams, StrategyParams

logger = logging.getLogger(__name__)

# Minimum fraction of expected candles that must be cached before we
# consider a timeframe "fresh" (avoids re-aggregating on every request).
_CACHE_FRESH_RATIO = 0.98

ProgressCb = Optional[Callable[[dict], Any]]


class DependencyResolver:
    """
    Recursive on-demand resolver.

    Resolution order:
        strategy
          └── for each required IndicatorParams:
                └── resolve_indicator
                      └── resolve_candles (aggregate if missing)
                            └── raw 1m candles (always present)

    Every intermediate result is cached in ClickHouse.  Subsequent calls
    for the same params_hash return immediately from the cache.
    """

    def __init__(self, db: ClickHouseManager) -> None:
        self.db = db

    # ------------------------------------------------------------------ #
    # Public entry points                                                  #
    # ------------------------------------------------------------------ #

    async def resolve_indicator(
        self,
        params: IndicatorParams,
        exchange: str,
        symbol: str,
        progress_cb: ProgressCb = None,
    ) -> list[tuple[int, float]]:
        """
        Ensure indicator is fully cached, then return (timestamp, value) list.
        Triggers candle aggregation if needed.
        """
        await self._ensure_indicator(params, exchange, symbol, progress_cb)
        return await self.db.fetch_indicator(params, exchange, symbol)

    async def resolve_strategy(
        self,
        params: StrategyParams,
        exchange: str,
        symbol: str,
        progress_cb: ProgressCb = None,
    ) -> list[dict]:
        """
        Ensure all dependent indicators are cached, run strategy, cache + return signals.
        """
        from plugins.strategies.loader import load_strategy_plugin  # local import avoids circularity

        plugin = load_strategy_plugin(params.type, params.to_dict())
        required: list[IndicatorParams] = plugin.get_required_indicators(params)

        total = len(required)
        for i, ind_params in enumerate(required):
            if progress_cb:
                await progress_cb({
                    "stage": "indicator",
                    "name": ind_params.display_name(),
                    "step": i + 1,
                    "total": total,
                })
            await self._ensure_indicator(ind_params, exchange, symbol, progress_cb)

        if progress_cb:
            await progress_cb({"stage": "strategy", "name": params.display_name()})

        return await self._compute_strategy(params, plugin, exchange, symbol)

    # ------------------------------------------------------------------ #
    # Internal: indicator                                                  #
    # ------------------------------------------------------------------ #

    async def _ensure_indicator(
        self,
        params: IndicatorParams,
        exchange: str,
        symbol: str,
        progress_cb: ProgressCb,
    ) -> None:
        # Check cache coverage: count + timestamp freshness
        _, max_ind_ts, cached_count = await self.db.get_indicator_coverage(params, exchange, symbol)
        candle_count = await self.db.count_candles(exchange, symbol, params.timeframe)

        logger.info(
            f"Cache check {params.display_name()} hash={params.to_hash()} "
            f"cached={cached_count} candles={candle_count} "
            f"need={int(candle_count * _CACHE_FRESH_RATIO)} max_ind_ts={max_ind_ts}"
        )

        incremental = False
        if candle_count > 0 and cached_count >= int(candle_count * _CACHE_FRESH_RATIO):
            # Count ratio looks good — but also verify the cache covers the *latest* candles.
            # New candles arriving in real-time can push candle_count up while the ratio
            # stays ≥ 0.98, meaning recently arrived candles never get computed.
            _, max_candle_ts = await self.db.get_candle_range(exchange, symbol, params.timeframe)
            tf_sec = TIMEFRAME_SECONDS.get(params.timeframe, 60)
            if max_ind_ts and max_candle_ts and max_ind_ts >= max_candle_ts - tf_sec:
                logger.info(f"Indicator cache HIT: {params.display_name()} {exchange} {symbol}")
                return
            if max_ind_ts and max_candle_ts and max_ind_ts < max_candle_ts:
                incremental = True
                logger.info(
                    f"Cache stale (new candles): max_ind_ts={max_ind_ts} "
                    f"max_candle_ts={max_candle_ts} — incremental update"
                )

        # Ensure candles for the required timeframe exist first
        await self._ensure_candles(params.timeframe, exchange, symbol, progress_cb)

        from plugins.indicators.loader import load_indicator_plugin
        plugin = load_indicator_plugin(params.type, params.to_dict())
        warmup = plugin.get_required_periods()

        tf_sec = TIMEFRAME_SECONDS.get(params.timeframe, 60)

        if incremental and max_ind_ts:
            # Load only the context window + new candles — warmup*3 periods back is enough
            # for any indicator to converge from a known-good state.
            context_start = max_ind_ts - warmup * tf_sec * 3
            candles = await self.db.fetch_candles(
                exchange, symbol, params.timeframe, start_ts=context_start
            )
            logger.info(
                f"Incremental load: {len(candles)} candles from ts={context_start} "
                f"for {params.display_name()}"
            )
        else:
            total_candles = await self.db.count_candles(exchange, symbol, params.timeframe)
            if progress_cb:
                await progress_cb({"stage": "loading",
                                    "message": f"Loading {total_candles:,} {params.timeframe} candles…"})
            candles = await self.db.fetch_candles(exchange, symbol, params.timeframe)

        if not candles:
            logger.warning(f"No candles for {params.timeframe} {exchange} {symbol}")
            return

        if len(candles) < warmup:
            logger.warning(f"Not enough candles for {params.display_name()}: "
                           f"{len(candles)} < {warmup}")
            return

        if progress_cb:
            await progress_cb({"stage": "computing",
                                "message": f"Computing {params.display_name()}…"})

        results = await plugin.calculate_stream(candles, warmup)

        values: list[tuple[int, float]] = []
        for i, r in enumerate(results):
            if r is not None and r.get("value") is not None:
                ts = candles[warmup + i]["timestamp"]
                # Incremental: skip values already in cache
                if max_ind_ts is None or ts > max_ind_ts:
                    values.append((ts, float(r["value"])))

        if values:
            if progress_cb:
                await progress_cb({"stage": "storing",
                                    "message": f"Storing {len(values):,} values…"})
            await self.db.store_indicator(params, exchange, symbol, values)
            await self.db.flush()  # commit buffer so next cache check sees the data
            logger.info(
                f"{'Incremental' if incremental else 'Full'} cache: stored {len(values)} values "
                f"for {params.display_name()} {exchange} {symbol}"
            )

    # ------------------------------------------------------------------ #
    # Internal: candle aggregation                                         #
    # ------------------------------------------------------------------ #

    async def _ensure_candles(
        self,
        timeframe: str,
        exchange: str,
        symbol: str,
        progress_cb: ProgressCb = None,
    ) -> None:
        if timeframe == "1m":
            return  # raw data — always present

        raw_count  = await self.db.count_candles(exchange, symbol, "1m")
        tf_seconds = TIMEFRAME_SECONDS.get(timeframe, 60)
        expected   = max(1, raw_count * 60 // tf_seconds)  # rough estimate
        existing   = await self.db.count_candles(exchange, symbol, timeframe)

        if existing >= int(expected * _CACHE_FRESH_RATIO):
            logger.debug(f"Candle cache hit: {timeframe} {exchange} {symbol}")
            return

        if progress_cb:
            await progress_cb({"stage": "aggregating", "timeframe": timeframe})

        logger.info(f"Aggregating 1m→{timeframe} for {exchange} {symbol} ...")
        await self.db.aggregate_candles(exchange, symbol, src="1m", dst=timeframe)

    # ------------------------------------------------------------------ #
    # Internal: strategy computation                                       #
    # ------------------------------------------------------------------ #

    async def _compute_strategy(
        self,
        params: StrategyParams,
        plugin: Any,
        exchange: str,
        symbol: str,
    ) -> list[dict]:
        """
        Fetch all required indicator data, run strategy.process() for each
        candle timestamp, store and return signals.
        """
        required: list[IndicatorParams] = plugin.get_required_indicators(params)

        # Gather indicator series keyed by params hash (or type) for lookup
        ind_series: dict[str, list[tuple[int, float]]] = {}
        for ind_p in required:
            ind_series[ind_p.to_hash()] = await self.db.fetch_indicator(
                ind_p, exchange, symbol
            )

        # Build a sorted list of timestamps where ALL indicators have values
        from bisect import bisect_right

        # Index each series by sorted timestamps for bisect lookup
        ts_lists: dict[str, list[int]]   = {h: [r[0] for r in s] for h, s in ind_series.items()}
        val_lists: dict[str, list[float]] = {h: [r[1] for r in s] for h, s in ind_series.items()}

        # Use the hash of the first required indicator as the "base" timeline
        if not required:
            return []
        base_hash = required[0].to_hash()
        base_ts   = ts_lists.get(base_hash, [])

        signals: list[dict] = []

        for ts in base_ts:
            indicators_data: dict[str, dict] = {}
            for ind_p in required:
                h = ind_p.to_hash()
                ts_list  = ts_lists.get(h, [])
                val_list = val_lists.get(h, [])
                # Find the latest value at or before this timestamp
                idx = bisect_right(ts_list, ts) - 1
                if idx < 0:
                    break
                indicators_data[ind_p.to_hash()] = {"value": val_list[idx]}
            else:
                # All indicators present — run strategy
                price_data = await self.db.fetch_candles(
                    exchange, symbol,
                    required[0].timeframe,
                    start_ts=ts, end_ts=ts, limit=1,
                )
                price = float(price_data[0]["close"]) if price_data else 0.0

                try:
                    signal = await plugin.process(
                        indicators_data=indicators_data,
                        current_price=price,
                        signal_timestamp=ts,
                    )
                except Exception as e:
                    logger.debug(f"Strategy process error at {ts}: {e}")
                    signal = None

                if signal is not None:
                    sig_dict = signal.to_dict()
                    sig_dict["timestamp"] = ts
                    signals.append(sig_dict)
                    await self.db.store_signal(
                        params, exchange, symbol,
                        timestamp=ts,
                        signal_type=signal.signal_type.value,
                        confidence=signal.confidence,
                        price=price,
                        metadata=signal.metadata,
                    )

        await self.db.flush()
        logger.info(f"Strategy {params.display_name()}: {len(signals)} signals "
                    f"for {exchange} {symbol}")
        return signals
