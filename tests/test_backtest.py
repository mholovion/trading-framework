"""tradingkit.backtest — BacktestRunner and BacktestResult."""
from __future__ import annotations

import polars as pl
import pytest

from tradingkit.backtest import BacktestResult, BacktestRunner
from tradingkit.indicator import Indicator, IndicatorContext
from tradingkit.strategy import BarContext, Signal, Strategy


class ConstantRSI(Indicator):
    """Feeds a scripted sequence of RSI values so signal emission is deterministic."""
    def __init__(self, values, **params):
        super().__init__(**params)
        self._values = values

    def compute(self, ctx: IndicatorContext):
        import numpy as np
        return np.array(self._values[: len(ctx)])

    def required_periods(self) -> int:
        return 1


class ThresholdStrategy(Strategy):
    """Emits its own vocabulary — the framework privileges none of these field names."""
    async def on_bar(self, bar: BarContext):
        if bar.rsi < 30:
            return Signal(action="open_long", price=bar.close)
        if bar.rsi > 70:
            return Signal(action="close", price=bar.close)
        return None


@pytest.fixture
def data() -> pl.DataFrame:
    return pl.DataFrame({
        "timestamp": [0, 60, 120, 180, 240],
        "open":  [10.0, 10.0, 10.0, 10.0, 10.0],
        "high":  [10.0, 10.0, 10.0, 10.0, 10.0],
        "low":   [10.0, 10.0, 10.0, 10.0, 10.0],
        "close": [10.0, 11.0, 12.0, 13.0, 14.0],
        "volume": [1.0, 1.0, 1.0, 1.0, 1.0],
    })


async def test_backtest_collects_signals_with_the_strategys_own_fields(data):
    runner = BacktestRunner()
    result = await runner.run(
        data=data,
        indicators={"rsi": ConstantRSI([20, 50, 50, 50, 80], period=1)},
        strategy=ThresholdStrategy(),
    )
    assert [s["action"] for s in result.signals] == ["open_long", "close"]
    assert [s["price"] for s in result.signals] == [10.0, 14.0]
    assert [s["timestamp"] for s in result.signals] == [0, 240]


async def test_backtest_no_signals(data):
    runner = BacktestRunner()
    result = await runner.run(
        data=data,
        indicators={"rsi": ConstantRSI([50, 50, 50, 50, 50], period=1)},
        strategy=ThresholdStrategy(),
    )
    assert result.signals == []


async def test_backtest_empty_data_returns_empty_result():
    runner = BacktestRunner()
    empty = pl.DataFrame(schema={"timestamp": pl.Int64, "close": pl.Float64})
    result = await runner.run(data=empty, indicators={}, strategy=ThresholdStrategy())
    assert result.signals == []
    assert result.signals_df.height == 0


async def test_backtest_stamps_timestamp_but_nothing_else(data):
    """The framework fills in `timestamp` only — auto-filling a `price` from `close`
    would privilege one field name and assume the source is OHLCV."""
    class Minimal(Strategy):
        async def on_bar(self, bar: BarContext):
            return Signal(note="hi")

    result = await BacktestRunner().run(data=data, indicators={}, strategy=Minimal())
    assert result.signals[0] == {"note": "hi", "timestamp": 0}


async def test_backtest_accepts_a_plain_dict_from_on_bar(data):
    class DictStrategy(Strategy):
        async def on_bar(self, bar: BarContext):
            return {"action": "buy", "px": bar.close}

    result = await BacktestRunner().run(data=data, indicators={}, strategy=DictStrategy())
    assert result.signals[0] == {"action": "buy", "px": 10.0, "timestamp": 0}


def test_signals_df_unions_columns_across_signals():
    """A strategy may emit different fields on different bars; the table unions them."""
    result = BacktestResult(
        signals=[{"timestamp": 1, "action": "buy"}, {"timestamp": 2, "zscore": 4.2}],
        data=pl.DataFrame(), indicators={},
    )
    df = result.signals_df
    assert set(df.columns) == {"timestamp", "action", "zscore"}
    assert df["zscore"].to_list() == [None, 4.2]


def test_signals_df_is_indicator_context_ready():
    """This is the seam a Metric will use: analytics over signals is the same primitive
    as indicators over prices, so signals_df must always carry `timestamp`."""
    result = BacktestResult(
        signals=[{"timestamp": 1, "px": 10.0}], data=pl.DataFrame(), indicators={},
    )
    ctx = IndicatorContext(result.signals_df)
    assert ctx.px.to_list() == [10.0]


def test_empty_signals_df_still_has_timestamp():
    result = BacktestResult(signals=[], data=pl.DataFrame(), indicators={})
    assert result.signals_df.columns == ["timestamp"]
    IndicatorContext(result.signals_df)  # must not raise
