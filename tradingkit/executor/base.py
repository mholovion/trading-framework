"""
tradingkit.executor.base — PluginExecutor ABC.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Self

import polars as pl

if TYPE_CHECKING:
    from tradingkit.executor.cpp_pool import CppRunnerPool
    from tradingkit.indicator import Indicator, IndicatorContext
    from tradingkit.source import DataSource
    from tradingkit.strategy import BarContext, Signal, Strategy


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
    cpp_pool: CppRunnerPool | None = None

    @abstractmethod
    async def compute_indicator(
        self,
        indicator: Indicator,
        ctx: IndicatorContext,
    ) -> pl.Series:
        """
        Run indicator.compute(ctx) and return pl.Series of Float64.
        Length == len(ctx). First required_periods() values will be NaN.
        """
        ...

    @abstractmethod
    async def process_strategy_bar(
        self,
        strategy: Strategy,
        bar: BarContext,
    ) -> Signal | None:
        """
        Call strategy.on_bar(bar) and return Signal or None.
        """
        ...

    @abstractmethod
    async def fetch_source_data(
        self,
        source: DataSource,
        symbol: str,
        timeframe: int,
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

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_) -> None:
        await self.stop()
