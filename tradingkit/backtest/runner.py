"""
tradingkit.backtest.runner — BacktestRunner.

Runs a strategy against historical data and produces BacktestResult.

Usage:
    runner = BacktestRunner(executor=LocalExecutor())
    result = await runner.run(
        data=df,
        indicators={"rsi": RSI(14), "ema": EMA(50)},
        strategy=MyStrategy(),
    )
    print(result.summary())
"""
from __future__ import annotations

import logging
from typing import Any

import polars as pl

from tradingkit.backtest.result import BacktestResult
from tradingkit.indicator import Indicator, IndicatorContext
from tradingkit.strategy import BarContext, Signal, Strategy

logger = logging.getLogger(__name__)


def _as_signal_dict(result: Signal | dict, row: dict) -> dict:
    """Normalise whatever on_bar() returned into a plain record, stamping the bar's
    timestamp when the strategy didn't set one. Only `timestamp` is filled in -- see
    Pipeline._as_signal() for why nothing else is."""
    fields = result.to_dict() if isinstance(result, Signal) else dict(result)
    if fields.get("timestamp") is None:
        fields["timestamp"] = row.get("timestamp")
    return fields


class BacktestRunner:
    """
    Runs a strategy bar-by-bar on historical data.

    Accepts a pre-loaded pl.DataFrame so it works independently
    of any data source. Pair with Pipeline for a full pipeline run.
    """

    def __init__(self, executor: Any = None) -> None:
        if executor is None:
            from tradingkit.executor.local import LocalExecutor
            executor = LocalExecutor()
        self._executor = executor

    async def run(
        self,
        data: pl.DataFrame,
        indicators: dict[str, Indicator],
        strategy: Strategy,
    ) -> BacktestResult:
        """
        Run strategy over data bar-by-bar.

        Args:
            data:       pl.DataFrame with OHLCV columns
            indicators: dict of {name: Indicator} — computed once over all rows
            strategy:   Strategy instance with on_bar()

        Returns:
            BacktestResult with signals, data, and indicator series
        """
        if data.height == 0:
            return BacktestResult(signals=[], data=data, indicators={})

        ctx = IndicatorContext(data)

        ind_series: dict[str, pl.Series] = {}
        for name, ind in indicators.items():
            ind_series[name] = await self._executor.compute_indicator(ind, ctx)

        signals: list[dict] = []
        rows = data.to_dicts()

        for i, row in enumerate(rows):
            indicators_at_bar = {
                name: (series[i] if series[i] is not None else float("nan"))
                for name, series in ind_series.items()
            }
            series_map = {
                name: series[:i + 1] for name, series in ind_series.items()
            }
            bar = BarContext(row, indicators_at_bar, series_map)
            try:
                signal: Signal | None = await self._executor.process_strategy_bar(strategy, bar)
            except Exception as exc:
                logger.debug(f"Strategy error at bar {i}: {exc}")
                signal = None

            if signal is not None:
                signals.append(_as_signal_dict(signal, row))

        return BacktestResult(signals=signals, data=data, indicators=ind_series)

    # _pair_signals() lived here: it string-matched "buy"/"sell", hardcoded side="buy",
    # and so reported a short strategy's +25 as a fabricated long trade of +5. Pairing
    # needs to know what the strategy's own field values mean, which only the author does,
    # so it now belongs to a configurable Metric over signals_df rather than here.
