"""tradingkit.strategy — Signal, BarContext, Strategy, ScriptStrategy."""
from __future__ import annotations

import pickle

import polars as pl
import pytest

from tradingkit.strategy import BarContext, ScriptStrategy, Signal, Strategy


def test_signal_takes_any_fields():
    """The framework mandates no fields -- the strategy author owns the schema."""
    sig = Signal(action="short", px=42.0, zscore=4.2)
    assert sig.action == "short"
    assert sig.px == 42.0
    assert sig.to_dict() == {"action": "short", "px": 42.0, "zscore": 4.2}


def test_signal_needs_no_fields_at_all():
    assert Signal().to_dict() == {}


def test_signal_to_dict_cannot_lose_a_field():
    """Regression: `signal.price = x` used to write to __dict__ while to_dict() only
    serialised declared fields + metadata, so the price silently vanished on the way
    out through the API."""
    sig = Signal(action="buy")
    sig.price = 42.0                      # set after construction, as the framework does
    sig.timestamp = 100
    assert sig.to_dict() == {"action": "buy", "price": 42.0, "timestamp": 100}
    assert Signal.from_dict(sig.to_dict()) == sig


def test_signal_missing_field_raises_attribute_error():
    sig = Signal(action="buy")
    with pytest.raises(AttributeError, match="nonexistent"):
        sig.nonexistent
    # getattr-with-default must keep working for optional fields
    assert getattr(sig, "nonexistent", None) is None


def test_signal_pickle_roundtrip():
    """__slots__ means unpickling restores state without __init__ -- it must not route
    through __setattr__ before _fields exists."""
    sig = Signal(action="sell", reason="overbought")
    restored = pickle.loads(pickle.dumps(sig))
    assert restored == sig
    assert restored.reason == "overbought"


def test_signal_contains_and_repr():
    sig = Signal(action="buy")
    assert "action" in sig
    assert "price" not in sig
    assert repr(sig) == "Signal(action='buy')"


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
    signal = Signal(action="buy", confidence=0.8)
""")
    bar = BarContext(row={"timestamp": 1}, indicators={"rsi": 20.0})
    sig = await s.on_bar(bar)
    assert isinstance(sig, Signal)
    assert sig.action == "buy"


async def test_script_strategy_accepts_a_plain_dict():
    """A dict is the same record without the import, so scripts needn't reach for
    Signal at all."""
    s = ScriptStrategy(code='signal = {"action": "buy", "px": 42.0}')
    sig = await s.on_bar(BarContext(row={"timestamp": 1}, indicators={}))
    assert isinstance(sig, Signal)
    assert sig.to_dict() == {"action": "buy", "px": 42.0}


async def test_script_strategy_rejects_a_non_record():
    s = ScriptStrategy(code="signal = 42")
    with pytest.raises(TypeError, match="Signal or dict"):
        await s.on_bar(BarContext(row={"timestamp": 1}, indicators={}))


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

def test_strategy_config_survives_a_subclass_that_skips_super_init():
    """Regression: a strategy whose own __init__ doesn't call super() had no .config at
    all, so get_required_indicators() raised AttributeError. Writing such an __init__ is
    the common case -- parameters are usually plain arguments."""
    class NoSuper(Strategy):
        def __init__(self):
            self.rsi_oversold = 30
        async def on_bar(self, bar: BarContext):
            return None

    s = NoSuper()
    assert s.config == {}
    assert s.get_required_indicators() == []


def test_strategy_config_default_is_not_shared_between_instances():
    class NoSuper(Strategy):
        def __init__(self):
            pass
        async def on_bar(self, bar: BarContext):
            return None

    a, b = NoSuper(), NoSuper()
    a.config["k"] = "v"
    assert b.config == {}


def test_strategy_dead_parameters_attribute_is_gone():
    """self.parameters was assigned and then read by nothing in the entire framework."""
    class S(Strategy):
        async def on_bar(self, bar: BarContext):
            return None

    assert not hasattr(S(), "parameters")
