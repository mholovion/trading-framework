"""tradingkit.strategy — Signal, BarContext, Strategy, ScriptStrategy."""
from __future__ import annotations

import pickle

import polars as pl
import pytest

from tradingkit.strategy import BarContext, ScriptStrategy, Signal, Strategy


def test_signal_to_dict_and_from_dict_roundtrip():
    sig = Signal("buy", 0.8, timestamp=100, metadata={"direction": "long"})
    d = sig.to_dict()
    restored = Signal.from_dict(d)
    assert restored.type == "buy"
    assert restored.confidence == 0.8
    assert restored.timestamp == 100
    assert restored.direction == "long"


def test_signal_getattr_reads_from_metadata():
    sig = Signal("buy", 0.8, metadata={"zscore": 4.2})
    assert sig.zscore == 4.2
    with pytest.raises(AttributeError):
        sig.nonexistent


def test_signal_pickle_roundtrip():
    sig = Signal("sell", 0.5, metadata={"reason": "overbought"})
    restored = pickle.loads(pickle.dumps(sig))
    assert restored.type == "sell"
    assert restored.reason == "overbought"


def test_bar_context_row_and_indicator_access():
    bar = BarContext(row={"timestamp": 1, "close": 42.0}, indicators={"rsi": 55.0})
    assert bar.close == 42.0
    assert bar.rsi == 55.0
    assert bar.timestamp == 1


def test_bar_context_indicators_take_precedence_over_row():
    bar = BarContext(row={"timestamp": 1, "x": "from_row"}, indicators={"x": "from_indicator"})
    assert bar.x == "from_indicator"


def test_bar_context_missing_attribute_raises_with_helpful_message():
    bar = BarContext(row={"timestamp": 1}, indicators={"rsi": 1.0})
    with pytest.raises(AttributeError, match="no attribute 'nope'"):
        bar.nope


def test_bar_context_series_lookback():
    series = {"rsi": pl.Series("rsi", [1.0, 2.0, 3.0])}
    bar = BarContext(row={"timestamp": 1}, indicators={"rsi": 3.0}, series=series)
    assert list(bar.series("rsi")) == [1.0, 2.0, 3.0]
    with pytest.raises(KeyError):
        bar.series("missing")


def test_bar_context_pickle_roundtrip_with_polars_series():
    """Regression: unpickling used to recurse infinitely via __getattr__('__setstate__')."""
    series = {"rsi": pl.Series("rsi", [1.0, 2.0])}
    bar = BarContext(row={"timestamp": 1, "close": 1.0}, indicators={"rsi": 5.0}, series=series)
    restored = pickle.loads(pickle.dumps(bar))
    assert restored.rsi == 5.0
    assert list(restored.series("rsi")) == [1.0, 2.0]


def test_strategy_required_indicators_collects_class_attrs():
    from tradingkit.indicator import Indicator

    class RSI(Indicator):
        def compute(self, ctx): return ctx.np.close
        def required_periods(self): return 14

    class MyStrategy(Strategy):
        rsi = RSI(period=14)
        async def on_bar(self, bar): return None

    reqs = MyStrategy.required_indicators()
    assert "rsi" in reqs
    assert isinstance(reqs["rsi"], RSI)


SCRIPT_STRATEGY_CODE = '''
def get_required_indicators(config):
    return ["rsi"]

async def process(indicators_data, row, signal_timestamp=None):
    return None
'''


def test_script_strategy_get_required_indicators():
    s = ScriptStrategy(code=SCRIPT_STRATEGY_CODE)
    assert s.get_required_indicators() == ["rsi"]


async def test_script_strategy_on_bar_returns_signal():
    s = ScriptStrategy(code="""
if bar.rsi < 30:
    signal = Signal("buy", 0.8)
""")
    bar = BarContext(row={"timestamp": 1}, indicators={"rsi": 20.0})
    sig = await s.on_bar(bar)
    assert isinstance(sig, Signal)
    assert sig.type == "buy"


async def test_script_strategy_on_bar_returns_none_when_no_signal():
    s = ScriptStrategy(code="pass")
    bar = BarContext(row={"timestamp": 1}, indicators={})
    assert await s.on_bar(bar) is None


async def test_script_strategy_on_bar_wrong_type_raises():
    s = ScriptStrategy(code="signal = 42")
    bar = BarContext(row={"timestamp": 1}, indicators={})
    with pytest.raises(TypeError):
        await s.on_bar(bar)


def test_script_strategy_pickle_roundtrip_rebuilds_ns():
    """Regression: exec()'d _ns pulls in every builtin, which isn't safely picklable."""
    s = ScriptStrategy(code=SCRIPT_STRATEGY_CODE)
    restored = pickle.loads(pickle.dumps(s))
    assert restored.get_required_indicators() == ["rsi"]
    assert "get_required_indicators" in restored._ns


def test_script_strategy_getstate_excludes_ns():
    s = ScriptStrategy(code=SCRIPT_STRATEGY_CODE)
    state = s.__getstate__()
    assert "_ns" not in state