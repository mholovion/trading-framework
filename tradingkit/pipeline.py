"""
tradingkit.pipeline — Pipeline: source + indicators + strategy → PipelineResult.

A Pipeline is the serializable "project" concept.
It can be saved to ClickHouse (plugin_library) and re-run later.

Usage:
    pipeline = Pipeline(
        name="my_rsi_strategy",
        source=ScriptSource(code="..."),
        indicators={"rsi": WeightedRSI(14), "ema": EMA(50)},
        strategy=MyStrategy(),
    )
    result = await pipeline.run("SOL_USDT", "4h", start_ts=..., end_ts=...)
    print(result.summary())
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Optional

import numpy as np
import polars as pl

from tradingkit.indicator import Indicator, IndicatorContext
from tradingkit.strategy import Strategy, Signal, BarContext
from tradingkit.source import DataSource
from tradingkit.core.timeframe import parse_timeframe

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# PipelineResult                                                       #
# ------------------------------------------------------------------ #

@dataclass
class Trade:
    entry_ts:   int
    exit_ts:    int
    side:       str   # "buy" or "sell"
    entry_price: float
    exit_price:  float
    pnl:         float
    pnl_pct:     float


@dataclass
class PipelineResult:
    signals:    list[Signal]
    data:       pl.DataFrame
    indicators: dict[str, pl.Series]

    @property
    def trades(self) -> list[Trade]:
        """Pair BUY/SELL signals into trades."""
        trades: list[Trade] = []
        open_trade: Optional[dict] = None
        for sig in sorted(self.signals, key=lambda s: s.timestamp or 0):
            if sig.is_buy and open_trade is None:
                open_trade = {"ts": sig.timestamp, "price": sig.price or 0.0}
            elif sig.is_sell and open_trade is not None:
                ep = open_trade["price"]
                xp = sig.price or 0.0
                pnl = xp - ep
                pnl_pct = (pnl / ep * 100) if ep else 0.0
                trades.append(Trade(
                    entry_ts=open_trade["ts"],
                    exit_ts=sig.timestamp or 0,
                    side="buy",
                    entry_price=ep,
                    exit_price=xp,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                ))
                open_trade = None
        return trades

    def summary(self) -> dict:
        trades = self.trades
        if not trades:
            return {"trades": 0, "win_rate": 0.0, "total_pnl": 0.0, "signals": len(self.signals)}
        wins = [t for t in trades if t.pnl > 0]
        total_pnl = sum(t.pnl for t in trades)
        return {
            "trades":    len(trades),
            "wins":      len(wins),
            "win_rate":  len(wins) / len(trades),
            "total_pnl": total_pnl,
            "avg_pnl":   total_pnl / len(trades),
            "signals":   len(self.signals),
        }


# ------------------------------------------------------------------ #
# Pipeline                                                             #
# ------------------------------------------------------------------ #

@dataclass
class Pipeline:
    """
    Serializable project: source + indicators + strategy.

    Can be saved to ClickHouse via db.save_pipeline() / db.load_pipeline().
    Run via pipeline.run() for historical backtesting.
    """
    name:       str
    source:     DataSource
    indicators: dict[str, Indicator]
    strategy:   Strategy
    executor:   Any = None   # PluginExecutor | None → defaults to LocalExecutor

    def _get_executor(self) -> Any:
        if self.executor is not None:
            return self.executor
        from tradingkit.executor.local import LocalExecutor
        return LocalExecutor()

    async def run(
        self,
        symbol: str,
        timeframe: str | int,
        start_ts: int,
        end_ts: int,
    ) -> PipelineResult:
        """
        Run pipeline over a historical date range.
        1. Fetch data via source
        2. Compute all indicators
        3. Call strategy.on_bar() for each bar
        Returns PipelineResult with signals, data, and indicator series.
        """
        executor = self._get_executor()
        tf_seconds = parse_timeframe(timeframe)

        df: pl.DataFrame = await executor.fetch_source_data(
            self.source, symbol, tf_seconds, start_ts, end_ts
        )
        if df.height == 0:
            return PipelineResult(signals=[], data=df, indicators={})

        ctx = IndicatorContext(df)

        ind_series: dict[str, pl.Series] = {}
        for name, indicator in self.indicators.items():
            ind_series[name] = await executor.compute_indicator(indicator, ctx)

        signals: list[Signal] = []
        rows = df.to_dicts()

        for i, row in enumerate(rows):
            indicators_at_bar: dict[str, float] = {
                name: (series[i] if series[i] is not None else float("nan"))
                for name, series in ind_series.items()
            }
            series_map: dict[str, pl.Series] = {
                name: series[:i + 1] for name, series in ind_series.items()
            }
            bar = BarContext(row, indicators_at_bar, series_map)
            try:
                signal = await executor.process_strategy_bar(self.strategy, bar)
            except Exception as exc:
                logger.debug(f"Strategy error at bar {i}: {exc}")
                signal = None

            if signal is not None:
                if signal.timestamp is None:
                    signal.timestamp = row.get("timestamp")
                if getattr(signal, "price", None) is None:
                    signal.price = row.get("close")
                signals.append(signal)

        return PipelineResult(signals=signals, data=df, indicators=ind_series)

    async def run_live(
        self,
        symbol: str,
        timeframe: str | int,
    ) -> AsyncIterator[Signal]:
        """
        Stream live signals as new rows arrive from source.stream().
        Yields Signal objects. Requires source to support streaming.
        """
        executor = self._get_executor()
        tf_seconds = parse_timeframe(timeframe)

        ind_series: dict[str, pl.Series] = {}
        rows_acc: list[dict] = []

        async for row in self.source.stream(symbol, tf_seconds):
            rows_acc.append(row)
            df = pl.DataFrame(rows_acc)
            ctx = IndicatorContext(df)

            for name, indicator in self.indicators.items():
                ind_series[name] = await executor.compute_indicator(indicator, ctx)

            i = len(rows_acc) - 1
            indicators_at_bar = {
                name: (series[i] if series[i] is not None else float("nan"))
                for name, series in ind_series.items()
            }
            bar = BarContext(row, indicators_at_bar, {
                name: series[:i + 1] for name, series in ind_series.items()
            })
            signal = await executor.process_strategy_bar(self.strategy, bar)
            if signal is not None:
                if signal.timestamp is None:
                    signal.timestamp = row.get("timestamp")
                if getattr(signal, "price", None) is None:
                    signal.price = row.get("close")
                yield signal

    def to_dict(self) -> dict:
        """Serialize to JSON-compatible dict for ClickHouse storage."""
        import pickle, base64
        return {
            "name":       self.name,
            "source":     base64.b64encode(pickle.dumps(self.source)).decode(),
            "indicators": {
                k: base64.b64encode(pickle.dumps(v)).decode()
                for k, v in self.indicators.items()
            },
            "strategy":   base64.b64encode(pickle.dumps(self.strategy)).decode(),
        }

    @classmethod
    def from_dict(cls, data: dict, executor: Any = None) -> "Pipeline":
        """Deserialize from dict (loaded from ClickHouse)."""
        import pickle, base64
        return cls(
            name=data["name"],
            source=pickle.loads(base64.b64decode(data["source"])),
            indicators={
                k: pickle.loads(base64.b64decode(v))
                for k, v in data.get("indicators", {}).items()
            },
            strategy=pickle.loads(base64.b64decode(data["strategy"])),
            executor=executor,
        )


__all__ = ["Pipeline", "PipelineResult", "Trade"]
