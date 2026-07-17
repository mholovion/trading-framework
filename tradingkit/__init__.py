"""
tradingkit — pip-installable trading strategy framework.

Quick start:
    from tradingkit import Indicator, Strategy, Signal, BarContext, ta_indicator

Writing an indicator:
    from tradingkit import Indicator, IndicatorContext
    import numpy as np

    class WeightedRSI(Indicator):
        def compute(self, ctx: IndicatorContext) -> np.ndarray:
            rsi = ctx.ta.rsi(ctx.np.close, self.period)
            return rsi * (ctx.np.volume / ctx.np.volume.mean())
        def required_periods(self) -> int:
            return self.period + 1

Writing a strategy:
    from tradingkit import Strategy, Signal, BarContext, ta_indicator

    class MyStrategy(Strategy):
        rsi = ta_indicator.rsi(period=14)
        ema = ta_indicator.ema(period=50)

        async def on_bar(self, bar: BarContext) -> Signal | None:
            if bar.rsi < 30 and bar.close < bar.ema:
                return Signal("entry", 0.8, metadata={"direction": "long"})
            if bar.rsi > 70:
                return Signal("exit", 0.7)
"""

# ── Indicator ────────────────────────────────────────────────────────
from tradingkit.indicator import (
    Indicator,
    IndicatorContext,
    IndicatorDeclaration,
    ScriptIndicator,
    JitIndicator,
    CppIndicator,
    ta_indicator,
    load_indicator_plugin,
)

# ── Strategy ─────────────────────────────────────────────────────────
from tradingkit.strategy import (
    Strategy,
    Signal,
    BarContext,
    ScriptStrategy,
    CppStrategyPlugin,
    load_strategy_plugin,
    StrategyPlugin,
)

# ── Source ───────────────────────────────────────────────────────────
from tradingkit.source import (
    DataSource,
    ScriptSource,
)

# ── Executor ─────────────────────────────────────────────────────────
from tradingkit.executor import (
    PluginExecutor,
    LocalExecutor,
    SubprocessExecutor,
    RemoteExecutor,
    CppRunnerPool,
)

# ── Pipeline ─────────────────────────────────────────────────────────
from tradingkit.pipeline import (
    Pipeline,
    PipelineResult,
)

# ── Backtest ─────────────────────────────────────────────────────────
from tradingkit.backtest import (
    BacktestRunner,
    BacktestResult,
)

# ── Collector ────────────────────────────────────────────────────────
from tradingkit.collector import DataCollector

# ── Aggregation ──────────────────────────────────────────────────────
from tradingkit.aggregation import (
    AggregationContext,
    AggregationWorker,
    AggregationScript,
    load_aggregation_script,
)

# ── Application layer ────────────────────────────────────────────────
from tradingkit.context import TradingContext
from tradingkit.core.clickhouse import ClickHouseManager
from tradingkit.core.resolver import DependencyResolver
from tradingkit.core.timeframe import parse_timeframe


__version__ = "0.1.0"

__all__ = [
    # Indicator
    "Indicator",
    "IndicatorContext",
    "IndicatorDeclaration",
    "ScriptIndicator",
    "JitIndicator",
    "CppIndicator",
    "ta_indicator",
    "load_indicator_plugin",
    # Strategy
    "Strategy",
    "Signal",
    "BarContext",
    "ScriptStrategy",
    "CppStrategyPlugin",
    "load_strategy_plugin",
    "StrategyPlugin",
    # Source
    "DataSource",
    "ScriptSource",
    # Executor
    "PluginExecutor",
    "LocalExecutor",
    "SubprocessExecutor",
    "RemoteExecutor",
    "CppRunnerPool",
    # Pipeline
    "Pipeline",
    "PipelineResult",
    # Backtest
    "BacktestRunner",
    "BacktestResult",
    # Collector
    "DataCollector",
    # Aggregation
    "AggregationContext",
    "AggregationWorker",
    "AggregationScript",
    "load_aggregation_script",
    # Application
    "TradingContext",
    "ClickHouseManager",
    "DependencyResolver",
    "parse_timeframe",
]
