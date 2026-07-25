"""tradingkit.pipeline — Pipeline serialization + end-to-end run()."""
from __future__ import annotations

import base64
import os
import pickle

import polars as pl
import pytest

from tradingkit.indicator import ScriptIndicator
from tradingkit.pipeline import Pipeline, PipelineResult
from tradingkit.runner._safe_pickle import UnsafeUnpicklingError
from tradingkit.source import ScriptSource
from tradingkit.strategy import ScriptStrategy, Signal


def _make_pipeline() -> Pipeline:
    return Pipeline(
        name="test_pipeline",
        source=ScriptSource(code="result = pl.DataFrame({'timestamp':[1],'close':[1.0]})"),
        indicators={"rsi": ScriptIndicator(code="result = close", period=14)},
        strategy=ScriptStrategy(code="def get_required_indicators(config): return ['rsi']"),
    )


def test_to_dict_from_dict_roundtrip():
    p = _make_pipeline()
    restored = Pipeline.from_dict(p.to_dict())
    assert restored.name == "test_pipeline"
    assert isinstance(restored.source, ScriptSource)
    assert isinstance(restored.indicators["rsi"], ScriptIndicator)
    assert isinstance(restored.strategy, ScriptStrategy)
    assert restored.strategy.get_required_indicators() == ["rsi"]


def test_from_dict_uses_restricted_unpickler_for_source():
    """Regression: from_dict() used to call raw pickle.loads() on ClickHouse-sourced data."""
    class Evil:
        def __reduce__(self):
            return (os.system, ("echo pwned",))

    evil_dict = {
        "name": "evil",
        "source": base64.b64encode(pickle.dumps(Evil())).decode(),
        "indicators": {},
        "strategy": base64.b64encode(pickle.dumps(ScriptStrategy(code="pass"))).decode(),
    }
    with pytest.raises(UnsafeUnpicklingError):
        Pipeline.from_dict(evil_dict)


def test_from_dict_uses_restricted_unpickler_for_indicators_and_strategy():
    class Evil:
        def __reduce__(self):
            return (os.system, ("echo pwned",))

    good_source = base64.b64encode(pickle.dumps(ScriptSource(code="result = None"))).decode()
    for field in ("indicators", "strategy"):
        d = {
            "name": "evil",
            "source": good_source,
            "indicators": {},
            "strategy": base64.b64encode(pickle.dumps(ScriptStrategy(code="pass"))).decode(),
        }
        if field == "indicators":
            d["indicators"] = {"bad": base64.b64encode(pickle.dumps(Evil())).decode()}
        else:
            d["strategy"] = base64.b64encode(pickle.dumps(Evil())).decode()
        with pytest.raises(UnsafeUnpicklingError):
            Pipeline.from_dict(d)


async def test_pipeline_run_end_to_end():
    p = Pipeline(
        name="rsi_pipeline",
        source=ScriptSource(code="""
import numpy as np
n = 60
close = 100 + np.cumsum(np.random.default_rng(0).normal(0, 1, n))
result = pl.DataFrame({"timestamp": np.arange(n) * 60, "open": close, "high": close + 1,
                        "low": close - 1, "close": close, "volume": np.ones(n)})
"""),
        indicators={"rsi": ScriptIndicator(code="result = ta.rsi(close, 14)", period=14)},
        strategy=ScriptStrategy(code="""
if bar.rsi < 30:
    signal = Signal("buy", 0.8)
elif bar.rsi > 70:
    signal = Signal("sell", 0.7)
"""),
    )
    result = await p.run("BTC_USDT", "1m", start_ts=0, end_ts=3600)
    assert result.data.height == 60
    assert "rsi" in result.indicators


def _signal_with_price(type_: str, timestamp: int, price: float) -> Signal:
    sig = Signal(type_, 0.8, timestamp=timestamp)
    sig.price = price
    return sig


def test_pipeline_result_trades_pairs_buy_sell_signals():
    """Regression: .trades used sig.is_buy/is_sell, which didn't exist on Signal."""
    signals = [
        _signal_with_price("buy", 100, 10.0),
        _signal_with_price("sell", 200, 12.0),
    ]
    result = PipelineResult(signals=signals, data=pl.DataFrame(), indicators={})

    trades = result.trades
    assert len(trades) == 1
    trade = trades[0]
    assert trade.side == "buy"
    assert trade.entry_ts == 100
    assert trade.exit_ts == 200
    assert trade.entry_price == 10.0
    assert trade.exit_price == 12.0
    assert trade.pnl == pytest.approx(2.0)
    assert trade.pnl_pct == pytest.approx(20.0)


def test_pipeline_result_trades_ignores_unmatched_signals():
    signals = [
        _signal_with_price("sell", 50, 9.0),   # sell with no open trade -> ignored
        _signal_with_price("buy", 100, 10.0),  # never closed
    ]
    result = PipelineResult(signals=signals, data=pl.DataFrame(), indicators={})
    assert result.trades == []