from __future__ import annotations

import logging
from bisect import bisect_right
from typing import Any, Callable, Optional, TYPE_CHECKING

from tradingkit.core.clickhouse import ClickHouseManager, TIMEFRAME_SECONDS
from tradingkit.core.params import IndicatorParams, StrategyParams

if TYPE_CHECKING:
    from tradingkit.executor.base import PluginExecutor

logger = logging.getLogger(__name__)

_CACHE_FRESH_RATIO = 0.98
ProgressCb = Optional[Callable[[dict], Any]]


class DependencyResolver:
    """
    Recursive on-demand resolver for indicators and strategies.

    Resolution order:
        strategy
          └── for each required IndicatorParams:
                └── resolve_indicator
                      └── _ensure_ohlcv_agg (aggregate if missing)
                            └── raw 1m rows (always present)

    Every intermediate result is cached in ClickHouse. Subsequent calls
    for the same params_hash return immediately from the cache.

    executor: PluginExecutor — if None, falls back to PLUGIN_RUNNER_URL env var
              (backward-compat with existing Docker plugin-runner deployments).
    """

    def __init__(
        self,
        db: ClickHouseManager,
        executor: Optional["PluginExecutor"] = None,
    ) -> None:
        self.db = db
        self.executor = executor

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
        await self._ensure_indicator(params, exchange, symbol, progress_cb)
        return await self.db.fetch_indicator(params, exchange, symbol)

    async def resolve_strategy(
        self,
        params: StrategyParams,
        exchange: str,
        symbol: str,
        progress_cb: ProgressCb = None,
    ) -> list[dict]:
        plugin = self._load_strategy_plugin(params)

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
        _, max_ind_ts, cached_count = await self.db.get_indicator_coverage(params, exchange, symbol)
        row_count = await self.db.count_rows(exchange, symbol, params.timeframe)

        logger.info(
            f"Cache check {params.display_name()} hash={params.to_hash()} "
            f"cached={cached_count} rows={row_count}"
        )

        incremental = False
        if row_count > 0 and cached_count >= int(row_count * _CACHE_FRESH_RATIO):
            _, max_candle_ts = await self.db.get_data_range(exchange, symbol, params.timeframe)
            tf_sec = TIMEFRAME_SECONDS.get(params.timeframe, 60)
            if max_ind_ts and max_candle_ts and max_ind_ts >= max_candle_ts - tf_sec:
                logger.info(f"Indicator cache HIT: {params.display_name()} {exchange} {symbol}")
                return
            if max_ind_ts and max_candle_ts and max_ind_ts < max_candle_ts:
                incremental = True

        await self._ensure_ohlcv_agg(params.timeframe, exchange, symbol, progress_cb)

        plugin = self._load_indicator_plugin(params)
        warmup = plugin.get_required_periods()
        tf_sec = TIMEFRAME_SECONDS.get(params.timeframe, 60)

        if progress_cb:
            await progress_cb({"stage": "computing", "message": f"Computing {params.display_name()}…"})

        values: list[tuple[int, float]] = []

        if self.executor is None:
            raise RuntimeError(
                "No executor configured. Pass executor= to DependencyResolver."
            )

        from tradingkit.indicator import IndicatorContext

        if incremental and max_ind_ts:
            context_start = max_ind_ts - warmup * tf_sec * 3
        else:
            context_start = None

        tf_seconds = TIMEFRAME_SECONDS.get(params.timeframe, 60)
        end_ts     = int(__import__("time").time())
        start_ts   = context_start or 0

        df = await self.db.fetch_data_df(
            exchange, symbol, tf_seconds, start_ts, end_ts
        )
        if df.height == 0:
            logger.warning(f"No data for {params.timeframe} {exchange} {symbol}")
            return

        ctx           = IndicatorContext(df)
        result_series = await self.executor.compute_indicator(plugin, ctx)
        ts_list       = df["timestamp"].to_list()
        val_list      = result_series.to_list()

        for ts, val in zip(ts_list[warmup:], val_list[warmup:]):
            if val is not None and val == val:  # not NaN
                if max_ind_ts is None or ts > max_ind_ts:
                    values.append((ts, float(val)))

        if values:
            if progress_cb:
                await progress_cb({"stage": "storing", "message": f"Storing {len(values):,} values…"})
            await self.db.store_indicator(params, exchange, symbol, values)
            await self.db.flush()

    # ------------------------------------------------------------------ #
    # Internal: candle aggregation                                         #
    # ------------------------------------------------------------------ #

    async def _ensure_ohlcv_agg(
        self,
        timeframe: str,
        exchange: str,
        symbol: str,
        progress_cb: ProgressCb = None,
    ) -> None:
        import time as _time
        if timeframe == "1m":
            return

        tf_seconds = TIMEFRAME_SECONDS.get(timeframe, 60)
        now_ts = int(_time.time())
        current_bar_start = (now_ts // tf_seconds) * tf_seconds

        raw_count = await self.db.count_rows(exchange, symbol, "1m")
        existing  = await self.db.count_rows(exchange, symbol, timeframe)
        expected  = max(1, (raw_count * 60 - tf_seconds) // tf_seconds)

        if existing >= int(expected * _CACHE_FRESH_RATIO):
            _, max_cached_ts = await self.db.get_data_range(exchange, symbol, timeframe)
            last_closed_bar  = current_bar_start - tf_seconds
            if max_cached_ts and max_cached_ts >= last_closed_bar:
                return

        if progress_cb:
            await progress_cb({"stage": "aggregating", "timeframe": timeframe})

        await self._do_ohlcv_aggregation(exchange, symbol, "1m", timeframe)

    async def _do_ohlcv_aggregation(
        self, exchange: str, symbol: str, src: str, dst: str
    ) -> None:
        """Aggregate OHLCV rows via ClickHouse INSERT...SELECT."""
        from tradingkit.core.timeframe import parse_timeframe
        from tradingkit.core.clickhouse import _to_ch_interval

        parse_timeframe(src)  # validates src is a well-formed timeframe string; raises if not
        dst_seconds = parse_timeframe(dst)
        interval = _to_ch_interval(dst_seconds)

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
                  < toStartOfInterval(now(), INTERVAL {interval})
              AND toStartOfInterval(toDateTime(c.timestamp), INTERVAL {interval})
                  NOT IN (
                      SELECT toDateTime(timestamp)
                      FROM candles
                      WHERE exchange = %(ex)s AND symbol = %(sym)s AND timeframe = %(dst)s
                  )
            GROUP BY
                toStartOfInterval(toDateTime(c.timestamp), INTERVAL {interval}),
                c.exchange, c.symbol
        """
        await self.db._execute(sql, {"ex": exchange, "sym": symbol, "src": src, "dst": dst})

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
        raw_required = plugin.get_required_indicators(params)
        required: list[IndicatorParams] = []
        for item in raw_required:
            if isinstance(item, str):
                meta = await self.db.lookup_params(item)
                if meta:
                    required.append(IndicatorParams.from_dict(meta))
            else:
                required.append(item)

        if not required:
            return []

        ind_series: dict[str, list[tuple[int, float]]] = {}
        for ind_p in required:
            ind_series[ind_p.to_hash()] = await self.db.fetch_indicator(ind_p, exchange, symbol)

        ts_lists  = {h: [r[0] for r in s] for h, s in ind_series.items()}
        val_lists = {h: [r[1] for r in s] for h, s in ind_series.items()}

        base_hash = required[0].to_hash()
        base_ts   = ts_lists.get(base_hash, [])

        all_rows = await self.db.fetch_data(exchange, symbol, required[0].timeframe)
        row_map: dict[int, dict] = {r["timestamp"]: r for r in all_rows}

        inputs: list[dict] = []
        for ts in base_ts:
            indicators_data: dict[str, dict] = {}
            skip = False
            for ind_p in required:
                h       = ind_p.to_hash()
                ts_list = ts_lists.get(h, [])
                v_list  = val_lists.get(h, [])
                idx = bisect_right(ts_list, ts) - 1
                if idx < 0:
                    skip = True
                    break
                indicators_data[h] = {"value": v_list[idx]}
            if skip:
                continue
            inputs.append({
                "timestamp":       ts,
                "indicators_data": indicators_data,
                "row":             row_map.get(ts, {"timestamp": ts}),
            })

        if not inputs:
            return []

        signals: list[dict] = []

        for inp in inputs:
            ts  = inp["timestamp"]
            row = inp["row"]
            try:
                sig_dict = await plugin.process(
                    indicators_data=inp["indicators_data"],
                    row=row,
                    signal_timestamp=ts,
                )
            except Exception as exc:
                logger.debug(f"Strategy process error at {ts}: {exc}")
                sig_dict = None

            if sig_dict is not None:
                sig_dict["timestamp"] = ts
                signals.append(sig_dict)
                await self.db.store_signal(
                    params, exchange, symbol,
                    timestamp=ts,
                    signal_type=sig_dict["signal_type"],
                    confidence=sig_dict["confidence"],
                    price=row.get("close"),
                    metadata=sig_dict.get("metadata", {}),
                )

        await self.db.flush()
        return signals

    # ------------------------------------------------------------------ #
    # Plugin loading helpers                                               #
    # ------------------------------------------------------------------ #

    def _load_indicator_plugin(self, params: IndicatorParams) -> Any:
        try:
            from tradingkit.indicator import load_indicator_plugin
            return load_indicator_plugin(params.type, params.to_dict())
        except Exception as e:
            logger.error(f"Failed to load indicator plugin {params.type}: {e}")
            raise

    def _load_strategy_plugin(self, params: StrategyParams) -> Any:
        try:
            from tradingkit.strategy import load_strategy_plugin
            return load_strategy_plugin(params.type, params.to_dict())
        except Exception as e:
            logger.error(f"Failed to load strategy plugin {params.type}: {e}")
            raise
