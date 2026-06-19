"""
tradingkit.executor.local — LocalExecutor: runs plugins in-process.

No isolation. Use for trusted/internal plugins or development.
"""
from __future__ import annotations

import polars as pl
from typing import Optional, TYPE_CHECKING

from tradingkit.executor.base import PluginExecutor

if TYPE_CHECKING:
    from tradingkit.indicator import Indicator, IndicatorContext
    from tradingkit.strategy import Strategy, BarContext, Signal
    from tradingkit.source import DataSource


class LocalExecutor(PluginExecutor):
    """
    Runs indicator/strategy/source code directly in the current process.

    Pros: zero overhead, full Python access
    Cons: no memory/CPU limits, no import restrictions
    """

    def __init__(self, cpp_pool=None) -> None:
        self.cpp_pool = cpp_pool

    async def compute_indicator(
        self,
        indicator: "Indicator",
        ctx: "IndicatorContext",
    ) -> pl.Series:
        from tradingkit.indicator import CppIndicator
        if isinstance(indicator, CppIndicator):
            if self.cpp_pool is None:
                raise RuntimeError(
                    "CppIndicator requires CppRunnerPool — pass cpp_pool= to LocalExecutor"
                )
            arr = await self.cpp_pool.run(indicator._so_bytes, ctx.df, indicator.params)
            return pl.Series("value", arr, dtype=pl.Float64)
        return indicator(ctx)

    async def process_strategy_bar(
        self,
        strategy: "Strategy",
        bar: "BarContext",
    ) -> Optional["Signal"]:
        return await strategy.on_bar(bar)

    async def fetch_source_data(
        self,
        source: "DataSource",
        symbol: str,
        timeframe_seconds: int,
        start_ts: int,
        end_ts: int,
        limit: int = 5000,
    ) -> pl.DataFrame:
        return await source.get_historical_data(
            symbol, timeframe_seconds, start_ts, end_ts, limit
        )
