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

from tradingkit.indicator import Indicator, IndicatorContext
from tradingkit.strategy import Strategy, BarContext, Signal
from tradingkit.backtest.result import BacktestResult, Trade

logger = logging.getLogger(__name__)


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
            BacktestResult with trades, signals, data, and indicator series
        """
        if data.height == 0:
            return BacktestResult(trades=[], signals=[], data=data, indicators={})

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
                if signal.timestamp is None:
                    signal.timestamp = row.get("timestamp")
                if signal.price is None:
                    signal.price = row.get("close")
                d = signal.to_dict()
                d["timestamp"] = signal.timestamp
                d["price"] = signal.price
                signals.append(d)

        trades = self._pair_signals(signals)
        return BacktestResult(
            trades=trades,
            signals=signals,
            data=data,
            indicators=ind_series,
        )

    @staticmethod
    def _pair_signals(signals: list[dict]) -> list[Trade]:
        trades: list[Trade] = []
        open_trade = None
        for sig in sorted(signals, key=lambda s: s.get("timestamp", 0)):
            st = sig.get("signal_type")
            price = sig.get("price") or 0.0
            ts = sig.get("timestamp") or 0
            conf = sig.get("confidence", 0.0)

            if st == "buy" and open_trade is None:
                open_trade = {"ts": ts, "price": price, "conf": conf}
            elif st == "sell" and open_trade is not None:
                ep = open_trade["price"]
                pnl = price - ep
                pnl_pct = (pnl / ep * 100) if ep else 0.0
                trades.append(Trade(
                    entry_ts=open_trade["ts"],
                    exit_ts=ts,
                    side="buy",
                    entry_price=ep,
                    exit_price=price,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    entry_signal_confidence=open_trade["conf"],
                    exit_signal_confidence=conf,
                ))
                open_trade = None
        return trades
