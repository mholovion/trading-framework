"""tradingkit.pipeline — Pipeline serialization + end-to-end run()."""
from __future__ import annotations

import base64
import math
import os
import pickle

import polars as pl
import pytest

from tradingkit.indicator import ScriptIndicator
from tradingkit.pipeline import Pipeline, PipelineResult, _gap_rows, _merge_rows
from tradingkit.runner._safe_pickle import UnsafeUnpicklingError
from tradingkit.source import DataSource, ScriptSource
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
    result = await p.run("BTC_USDT", 60, start_ts=0, end_ts=3600)
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

# ------------------------------------------------------------------ #
# Gap-aware indicator computation (TASK-014)                           #
# ------------------------------------------------------------------ #

class GappySource(DataSource):
    """Streams/returns bars with a deliberate hole, and can refill it on request —
    stands in for an exchange whose WebSocket dropped bars over a reconnect."""

    def __init__(self, rows: list[dict], missing: list[dict] | None = None):
        super().__init__()
        self.rows = rows
        self.missing = missing or []
        self.fetch_gap_calls: list[tuple[int, int]] = []

    async def get_historical_data(self, symbol, timeframe, start_ts, end_ts, limit=5000):
        return pl.DataFrame(self.rows)

    async def fetch_gap(self, symbol, timeframe, start_ts, end_ts):
        self.fetch_gap_calls.append((start_ts, end_ts))
        return [r for r in self.missing if start_ts <= r["timestamp"] <= end_ts]

    async def stream(self, symbol, timeframe):
        for row in self.rows:
            yield row


def _bar(ts: int, close: float) -> dict:
    return {"timestamp": ts, "open": close, "high": close + 1,
            "low": close - 1, "close": close, "volume": 1.0}


async def test_gap_rows_returns_nothing_without_a_gap():
    rows = [_bar(0, 1.0), _bar(60, 2.0), _bar(120, 3.0)]
    source = GappySource(rows)
    assert await _gap_rows(rows, source, "BTC", 60) == []
    assert source.fetch_gap_calls == []


async def test_gap_rows_refetches_the_missing_window():
    rows = [_bar(0, 1.0), _bar(240, 5.0)]           # 60, 120, 180 missing
    missing = [_bar(60, 2.0), _bar(120, 3.0), _bar(180, 4.0)]
    source = GappySource(rows, missing)

    filler = await _gap_rows(rows, source, "BTC", 60)

    assert source.fetch_gap_calls == [(60, 180)]
    assert [r["timestamp"] for r in filler] == [60, 120, 180]


async def test_gap_rows_ignores_duplicate_timestamps():
    """A source re-pushing the same still-forming bar must not look like a gap."""
    rows = [_bar(60, 1.0), _bar(60, 1.1), _bar(120, 2.0)]
    source = GappySource(rows)
    assert await _gap_rows(rows, source, "BTC", 60) == []


async def test_gap_rows_survives_an_unreachable_source():
    """A failed re-fetch leaves the gap open, exactly as before backfill existed —
    it must not take the run down."""
    class Broken(GappySource):
        async def fetch_gap(self, *a, **kw):
            raise ConnectionError("exchange down")

    rows = [_bar(0, 1.0), _bar(240, 5.0)]
    assert await _gap_rows(rows, Broken(rows), "BTC", 60) == []


def test_merge_rows_keeps_existing_rows_on_overlap():
    rows = [_bar(0, 1.0), _bar(60, 2.0)]
    filler = [{**_bar(60, 99.0)}]           # overlaps a row we already have
    merged = _merge_rows(rows, filler)
    assert [r["close"] for r in merged] == [1.0, 2.0]


async def test_run_backfills_gaps_before_computing_indicators():
    rows = [_bar(0, 1.0), _bar(240, 5.0)]
    missing = [_bar(60, 2.0), _bar(120, 3.0), _bar(180, 4.0)]
    source = GappySource(rows, missing)
    p = Pipeline(name="p", source=source, indicators={}, strategy=ScriptStrategy(code="pass"))

    result = await p.run("BTC", 60, start_ts=0, end_ts=240)

    assert result.data["timestamp"].to_list() == [0, 60, 120, 180, 240]


async def test_run_rsi_is_not_permanently_nan_after_a_gap():
    """The regression this whole feature exists for: verified against real TA-Lib, one
    NaN poisons every later value, so an unfilled gap would leave RSI NaN forever."""
    closes = [100.0 + i for i in range(40)]
    rows    = [_bar(i * 60, c) for i, c in enumerate(closes)]
    kept    = rows[:20] + rows[25:]          # drop five bars in the middle
    missing = rows[20:25]

    source = GappySource(kept, missing)
    p = Pipeline(
        name="rsi",
        source=source,
        indicators={"rsi": ScriptIndicator(code="result = ta.rsi(close, 14)", period=14)},
        strategy=ScriptStrategy(code="pass"),
    )
    result = await p.run("BTC", 60, start_ts=0, end_ts=40 * 60)

    assert source.fetch_gap_calls == [(20 * 60, 24 * 60)]
    assert result.data.height == 40
    tail = result.indicators["rsi"].to_list()[-5:]
    assert all(v is not None and not math.isnan(v) for v in tail), \
        f"RSI went NaN after the gap: {tail}"


async def test_run_live_marks_backfilled_signals():
    """Backfilled bars run fully through the strategy and may produce signals of their
    own — flagged so a consumer can tell them from genuinely live ones."""
    streamed = [_bar(0, 1.0), _bar(180, 4.0)]     # 60 and 120 never arrive
    missing  = [_bar(60, 2.0), _bar(120, 3.0)]
    p = Pipeline(
        name="p",
        source=GappySource(streamed, missing),
        indicators={},
        strategy=ScriptStrategy(code='signal = Signal("buy", 1.0)'),
    )

    signals = [s async for s in p.run_live("BTC", 60)]

    assert [s.timestamp for s in signals] == [0, 60, 120, 180]
    assert [s.gap_recovered for s in signals] == [False, True, True, False]


async def test_run_live_deduplicates_a_backfilled_bar_that_also_arrives_live():
    """A bar re-fetched to close a gap can be delayed rather than lost, and turn up on
    the stream moments later — it must not be processed twice."""
    streamed = [_bar(0, 1.0), _bar(120, 3.0), _bar(120, 3.5)]
    missing  = [_bar(60, 2.0)]
    p = Pipeline(
        name="p",
        source=GappySource(streamed, missing),
        indicators={},
        strategy=ScriptStrategy(code='signal = Signal("buy", 1.0)'),
    )

    signals = [s async for s in p.run_live("BTC", 60)]

    assert [s.timestamp for s in signals] == [0, 60, 120, 120]
    assert [s.price for s in signals] == [1.0, 2.0, 3.0, 3.5]


async def test_run_live_survives_a_failing_strategy():
    """run() already tolerated a bad bar; run_live() used to die on the first one."""
    p = Pipeline(
        name="p",
        source=GappySource([_bar(0, 1.0), _bar(60, 2.0)]),
        indicators={},
        # References `bar`, so it raises per-bar rather than at construction time
        # (ScriptStrategy execs the script once up front, ignoring NameError).
        strategy=ScriptStrategy(code="if bar is not None:\n    raise RuntimeError('boom')"),
    )
    assert [s async for s in p.run_live("BTC", 60)] == []


async def test_run_live_survives_a_failing_indicator():
    p = Pipeline(
        name="p",
        source=GappySource([_bar(0, 1.0), _bar(60, 2.0)]),
        indicators={"bad": ScriptIndicator(code="raise RuntimeError('boom')", period=1)},
        strategy=ScriptStrategy(code='signal = Signal("buy", 1.0)'),
    )
    signals = [s async for s in p.run_live("BTC", 60)]
    assert [s.timestamp for s in signals] == [0, 60]
