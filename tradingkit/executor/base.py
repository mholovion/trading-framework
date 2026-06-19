"""
tradingkit.executor.base — PluginExecutor ABC.
"""
from __future__ import annotations

import polars as pl
from abc import ABC, abstractmethod
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from tradingkit.indicator import Indicator, IndicatorContext
    from tradingkit.strategy import Strategy, BarContext, Signal
    from tradingkit.source import DataSource
    from tradingkit.executor.cpp_pool import CppRunnerPool


class PluginExecutor(ABC):
    """
    Backend for running indicators, strategies, and data sources.

    Implementations:
        LocalExecutor      — in-process, no isolation
        SubprocessExecutor — Arrow IPC sandbox
        RemoteExecutor     — generic HTTP (tradingkit-runner server)

    Optionally attach a CppRunnerPool to enable CppIndicator execution:
        executor = SubprocessExecutor(cpp_pool=CppRunnerPool())
    """

    #: Optional C++ runner pool — set to enable CppIndicator execution.
    cpp_pool: Optional["CppRunnerPool"] = None

    @abstractmethod
    async def compute_indicator(
        self,
        indicator: "Indicator",
        ctx: "IndicatorContext",
    ) -> pl.Series:
        """
        Run indicator.compute(ctx) and return pl.Series of Float64.
        Length == len(ctx). First required_periods() values will be NaN.
        """
        ...

    @abstractmethod
    async def process_strategy_bar(
        self,
        strategy: "Strategy",
        bar: "BarContext",
    ) -> Optional["Signal"]:
        """
        Call strategy.on_bar(bar) and return Signal or None.
        """
        ...

    @abstractmethod
    async def fetch_source_data(
        self,
        source: "DataSource",
        symbol: str,
        timeframe_seconds: int,
        start_ts: int,
        end_ts: int,
        limit: int = 5000,
    ) -> pl.DataFrame:
        """
        Call source.get_historical_data() and return pl.DataFrame.
        """
        ...

    async def start(self) -> None:
        """Optional: initialize executor resources (subprocesses, connections...)."""

    async def stop(self) -> None:
        """Optional: release executor resources."""

    async def __aenter__(self) -> "PluginExecutor":
        await self.start()
        return self

    async def __aexit__(self, *_) -> None:
        await self.stop()
