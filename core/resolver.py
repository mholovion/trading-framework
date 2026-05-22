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

        # get_required_indicators() may return IndicatorParams objects or hash strings.
        # Hash strings: look up full params from params_meta table so we can ensure
        # the indicator is computed and properly keyed.
        raw_required = plugin.get_required_indicators(params)
        required: list[IndicatorParams] = []
        for item in raw_required:
            if isinstance(item, str):
                meta = await self.db.lookup_params(item)
                if meta:
                    required.append(IndicatorParams.from_dict(meta))
                else:
                    logger.warning(f"Unknown indicator hash '{item}' for strategy {params.type}")
            else:
                required.append(item)

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
        from plugin_runner.client import RemoteIndicatorPlugin
        plugin = load_indicator_plugin(params.type, params.to_dict())
        warmup = plugin.get_required_periods()

        tf_sec = TIMEFRAME_SECONDS.get(params.timeframe, 60)

        if progress_cb:
            await progress_cb({"stage": "computing",
                                "message": f"Computing {params.display_name()}…"})

        if isinstance(plugin, RemoteIndicatorPlugin):
            # Plugin-runner fetches candles from ClickHouse itself — no large payload
            start_ts_arg = (max_ind_ts - warmup * tf_sec * 3) if (incremental and max_ind_ts) else None
            raw_data = await plugin.compute_for(
                exchange, symbol, params.timeframe, start_ts=start_ts_arg, limit=0
            )
            values: list[tuple[int, float]] = [
                (int(d["timestamp"]), float(d["value"]))
                for d in raw_data
                if max_ind_ts is None or int(d["timestamp"]) > max_ind_ts
            ]
        else:
            # Local plugin (dev without Docker): fetch candles here
            if incremental and max_ind_ts:
                context_start = max_ind_ts - warmup * tf_sec * 3
                candles = await self.db.fetch_candles(
                    exchange, symbol, params.timeframe, start_ts=context_start
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
                logger.warning(f"Not enough candles for {params.display_name()}: {len(candles)} < {warmup}")
                return

            results = await plugin.calculate_stream(candles, warmup)

            values: list[tuple[int, float]] = []
            for i, r in enumerate(results):
                if r is not None and r.get("value") is not None:
                    ts = candles[warmup + i]["timestamp"]
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
        import time as _time
        if timeframe == "1m":
            return  # raw data — always present

        tf_seconds = TIMEFRAME_SECONDS.get(timeframe, 60)

        # Compute the start of the current (open) bar — we must NOT cache this
        now_ts = int(_time.time())
        current_bar_start = (now_ts // tf_seconds) * tf_seconds

        raw_count  = await self.db.count_candles(exchange, symbol, "1m")
        existing   = await self.db.count_candles(exchange, symbol, timeframe)
        # expected excludes the currently-open bar (it won't be cached)
        expected   = max(1, (raw_count * 60 - tf_seconds) // tf_seconds)

        if existing >= int(expected * _CACHE_FRESH_RATIO):
            # Count looks fresh — also verify the last cached bar is the most recent closed bar
            _, max_cached_ts = await self.db.get_candle_range(exchange, symbol, timeframe)
            last_closed_bar  = current_bar_start - tf_seconds
            if max_cached_ts and max_cached_ts >= last_closed_bar:
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
        Fetch all required indicator data, run strategy for each candle,
        store and return signals.

        When the plugin is a RemoteStrategyPlugin all candles are sent in one
        batch request; local plugins are called per-candle as before.
        """
        from bisect import bisect_right
        from plugin_runner.client import RemoteStrategyPlugin

        raw_required = plugin.get_required_indicators(params)

        # Normalise: may return strings (indicator hashes) or IndicatorParams
        required: list[IndicatorParams] = []
        for item in raw_required:
            if isinstance(item, str):
                meta = await self.db.lookup_params(item)
                if meta:
                    required.append(IndicatorParams.from_dict(meta))
                else:
                    logger.warning(f"Unknown indicator hash '{item}' in _compute_strategy")
            else:
                required.append(item)

        if not required:
            return []

        # Gather indicator series keyed by params hash
        ind_series: dict[str, list[tuple[int, float]]] = {}
        for ind_p in required:
            ind_series[ind_p.to_hash()] = await self.db.fetch_indicator(
                ind_p, exchange, symbol
            )

        ts_lists: dict[str, list[int]]    = {h: [r[0] for r in s] for h, s in ind_series.items()}
        val_lists: dict[str, list[float]] = {h: [r[1] for r in s] for h, s in ind_series.items()}

        base_hash = required[0].to_hash()
        base_ts   = ts_lists.get(base_hash, [])

        # Pre-fetch ALL prices in one query instead of one per candle
        all_prices = await self.db.fetch_candles(exchange, symbol, required[0].timeframe)
        price_map: dict[int, float] = {c["timestamp"]: float(c["close"]) for c in all_prices}

        # Build per-candle inputs
        candle_inputs: list[dict] = []
        for ts in base_ts:
            indicators_data: dict[str, dict] = {}
            skip = False
            for ind_p in required:
                h        = ind_p.to_hash()
                ts_list  = ts_lists.get(h, [])
                val_list = val_lists.get(h, [])
                idx = bisect_right(ts_list, ts) - 1
                if idx < 0:
                    skip = True
                    break
                indicators_data[h] = {"value": val_list[idx]}
            if skip:
                continue
            candle_inputs.append({
                "timestamp":       ts,
                "indicators_data": indicators_data,
                "current_price":   price_map.get(ts, 0.0),
            })

        if not candle_inputs:
            return []

        signals: list[dict] = []

        if isinstance(plugin, RemoteStrategyPlugin):
            # Single batch HTTP call — plugin-runner executes all candles
            raw_signals = await plugin.process_batch(candle_inputs)
            for inp, sig_dict in zip(candle_inputs, raw_signals):
                if sig_dict is None:
                    continue
                ts    = inp["timestamp"]
                price = inp["current_price"]
                sig_dict["timestamp"] = ts
                signals.append(sig_dict)
                await self.db.store_signal(
                    params, exchange, symbol,
                    timestamp=ts,
                    signal_type=sig_dict["signal_type"],
                    confidence=sig_dict["confidence"],
                    price=price,
                    metadata=sig_dict.get("metadata", {}),
                )
        else:
            # Local plugin — per-candle loop (existing behaviour)
            for inp in candle_inputs:
                ts    = inp["timestamp"]
                price = inp["current_price"]
                try:
                    signal = await plugin.process(
                        indicators_data=inp["indicators_data"],
                        current_price=price,
                        signal_timestamp=ts,
                    )
                except Exception as exc:
                    logger.debug(f"Strategy process error at {ts}: {exc}")
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
        logger.info(
            f"Strategy {params.display_name()}: {len(signals)} signals for {exchange} {symbol}"
        )
        return signals
