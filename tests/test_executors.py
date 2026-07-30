"""tradingkit.executor.local / subprocess_ — LocalExecutor, SubprocessExecutor (real local process)."""
from __future__ import annotations

import polars as pl
import pytest

from tradingkit.executor.local import LocalExecutor
from tradingkit.executor.subprocess_ import SubprocessExecutor
from tradingkit.indicator import CppIndicator, IndicatorContext, ScriptIndicator
from tradingkit.source import ScriptSource
from tradingkit.strategy import BarContext, ScriptStrategy


@pytest.fixture
def ctx() -> IndicatorContext:
    df = pl.DataFrame({"timestamp": [1, 2, 3], "close": [1.0, 2.0, 3.0]})
    return IndicatorContext(df)


# ------------------------------------------------------------------ #
# LocalExecutor                                                        #
# ------------------------------------------------------------------ #

async def test_local_executor_compute_indicator(ctx):
    executor = LocalExecutor()
    ind = ScriptIndicator(code="result = close * 2", period=1)
    series = await executor.compute_indicator(ind, ctx)
    assert list(series) == [2.0, 4.0, 6.0]


async def test_local_executor_process_strategy_bar():
    executor = LocalExecutor()
    strategy = ScriptStrategy(code='signal = Signal(action="buy", confidence=0.9) if bar.rsi < 30 else None')
    bar = BarContext(row={"timestamp": 1}, indicators={"rsi": 10.0})
    signal = await executor.process_strategy_bar(strategy, bar)
    assert signal is not None
    assert signal.action == "buy"


async def test_local_executor_process_strategy_bar_no_signal():
    executor = LocalExecutor()
    strategy = ScriptStrategy(code="pass")
    bar = BarContext(row={"timestamp": 1}, indicators={})
    assert await executor.process_strategy_bar(strategy, bar) is None


async def test_local_executor_fetch_source_data():
    executor = LocalExecutor()
    source = ScriptSource(code="result = pl.DataFrame({'timestamp':[1],'close':[1.0]})")
    df = await executor.fetch_source_data(source, "BTC_USDT", 60, 0, 100)
    assert df["close"].to_list() == [1.0]


async def test_local_executor_cpp_indicator_without_pool_raises(ctx):
    executor = LocalExecutor()
    ind = CppIndicator(cpp_code="", so_bytes=b"", period=1)
    with pytest.raises(RuntimeError, match="CppRunnerPool"):
        await executor.compute_indicator(ind, ctx)


async def test_local_executor_cpp_indicator_routes_through_pool(ctx):
    class FakePool:
        async def run(self, so_bytes, df, params):
            return [9.0] * len(df)

    executor = LocalExecutor(cpp_pool=FakePool())
    ind = CppIndicator(cpp_code="", so_bytes=b"fake", period=1)
    series = await executor.compute_indicator(ind, ctx)
    assert list(series) == [9.0, 9.0, 9.0]


# ------------------------------------------------------------------ #
# SubprocessExecutor — real local subprocess, no Docker/network needed  #
# ------------------------------------------------------------------ #

async def test_subprocess_executor_compute_indicator(ctx):
    async with SubprocessExecutor(timeout_s=15) as executor:
        ind = ScriptIndicator(code="result = close * 2", period=1)
        series = await executor.compute_indicator(ind, ctx)
        assert list(series) == [2.0, 4.0, 6.0]


async def test_subprocess_executor_process_strategy_bar():
    async with SubprocessExecutor(timeout_s=15) as executor:
        strategy = ScriptStrategy(code='signal = Signal(action="buy", confidence=0.9) if bar.rsi < 30 else None')
        bar = BarContext(row={"timestamp": 1}, indicators={"rsi": 10.0})
        signal = await executor.process_strategy_bar(strategy, bar)
        assert signal.action == "buy"


async def test_subprocess_executor_fetch_source_data():
    async with SubprocessExecutor(timeout_s=15) as executor:
        source = ScriptSource(code="result = pl.DataFrame({'timestamp':[1],'close':[1.0]})")
        df = await executor.fetch_source_data(source, "BTC_USDT", 60, 0, 100)
        assert df["close"].to_list() == [1.0]


async def test_subprocess_executor_propagates_indicator_errors(ctx):
    async with SubprocessExecutor(timeout_s=15) as executor:
        ind = ScriptIndicator(code="result = undefined_name", period=1)
        with pytest.raises(RuntimeError, match="Subprocess indicator error"):
            await executor.compute_indicator(ind, ctx)


async def test_subprocess_executor_cpp_indicator_without_pool_raises(ctx):
    executor = SubprocessExecutor()
    ind = CppIndicator(cpp_code="", so_bytes=b"", period=1)
    with pytest.raises(RuntimeError, match="CppRunnerPool"):
        await executor.compute_indicator(ind, ctx)