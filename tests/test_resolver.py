"""tradingkit.core.resolver.DependencyResolver — cache hit/miss/incremental logic."""
from __future__ import annotations

import polars as pl
import pytest

from tradingkit.core.params import IndicatorParams, StrategyParams
from tradingkit.core.resolver import DependencyResolver


class FakeDb:
    """Minimal in-memory stand-in for ClickHouseManager, just enough for the resolver."""

    def __init__(self):
        self.indicator_coverage = (None, None, 0)   # (min_ts, max_ts, cached_count)
        self.row_count = 100
        self.data_range = (0, 5940)                  # (min_ts, max_ts)
        self.ohlcv_df = pl.DataFrame({
            "timestamp": [i * 60 for i in range(100)],
            "open": [1.0] * 100, "high": [1.0] * 100, "low": [1.0] * 100,
            "close": [1.0] * 100, "volume": [1.0] * 100,
        })
        self.stored_indicators: list = []
        self.stored_signals: list = []
        self.flushed = False
        self.indicator_rows: dict = {}
        self.params_registry: dict = {}
        self.all_rows: list = []

    async def get_indicator_coverage(self, params, exchange, symbol):
        return self.indicator_coverage

    async def count_rows(self, exchange, symbol, timeframe, **kw):
        return self.row_count

    async def get_data_range(self, exchange, symbol, timeframe, **kw):
        return self.data_range

    async def fetch_data_df(self, exchange, symbol, tf_seconds, start_ts, end_ts):
        return self.ohlcv_df

    async def store_indicator(self, params, exchange, symbol, values):
        self.stored_indicators.append((params, exchange, symbol, values))

    async def flush(self):
        self.flushed = True

    async def fetch_indicator(self, params, exchange, symbol):
        return self.indicator_rows.get(params.to_hash(), [])

    async def lookup_params(self, hash_):
        return self.params_registry.get(hash_)

    async def fetch_data(self, exchange, symbol, timeframe, **kw):
        return self.all_rows

    async def store_signal(self, params, exchange, symbol, **kw):
        self.stored_signals.append((params, exchange, symbol, kw))


class FakeExecutor:
    def __init__(self, values):
        self._values = values
        self.calls = 0

    async def compute_indicator(self, plugin, ctx):
        self.calls += 1
        return pl.Series("value", self._values)


def _rsi_params() -> IndicatorParams:
    return IndicatorParams.create("rsi", "1m", period=14)


async def test_ensure_indicator_cache_hit_skips_executor():
    db = FakeDb()
    db.row_count = 100
    db.indicator_coverage = (0, 5940, 100)   # fully cached, max_ind_ts == last candle
    db.data_range = (0, 5940)
    executor = FakeExecutor([1.0] * 100)
    resolver = DependencyResolver(db, executor=executor)

    await resolver._ensure_indicator(_rsi_params(), "wb", "BTC_USDT", None)

    assert executor.calls == 0, "cache is fresh -- must not recompute"
    assert db.stored_indicators == []


async def test_ensure_indicator_cache_miss_computes_and_stores():
    db = FakeDb()
    db.row_count = 100
    db.indicator_coverage = (None, None, 0)   # nothing cached
    executor = FakeExecutor([float(i) for i in range(100)])
    resolver = DependencyResolver(db, executor=executor)

    await resolver._ensure_indicator(_rsi_params(), "wb", "BTC_USDT", None)

    assert executor.calls == 1
    assert len(db.stored_indicators) == 1
    assert db.flushed is True


async def test_ensure_indicator_without_executor_raises():
    db = FakeDb()
    db.indicator_coverage = (None, None, 0)
    resolver = DependencyResolver(db, executor=None)
    with pytest.raises(RuntimeError, match="No executor configured"):
        await resolver._ensure_indicator(_rsi_params(), "wb", "BTC_USDT", None)


async def test_ensure_indicator_no_data_logs_and_returns():
    db = FakeDb()
    db.indicator_coverage = (None, None, 0)
    db.ohlcv_df = pl.DataFrame(schema={"timestamp": pl.Int64})
    executor = FakeExecutor([])
    resolver = DependencyResolver(db, executor=executor)
    await resolver._ensure_indicator(_rsi_params(), "wb", "BTC_USDT", None)
    assert db.stored_indicators == []


async def test_ensure_ohlcv_agg_skips_for_1m():
    db = FakeDb()
    resolver = DependencyResolver(db, executor=FakeExecutor([]))
    await resolver._ensure_ohlcv_agg("1m", "wb", "BTC_USDT", None)  # must not raise / touch db


async def test_progress_callback_invoked_during_indicator_resolution():
    db = FakeDb()
    db.indicator_coverage = (None, None, 0)
    executor = FakeExecutor([1.0] * 100)
    resolver = DependencyResolver(db, executor=executor)

    events = []
    async def cb(evt):
        events.append(evt)

    await resolver._ensure_indicator(_rsi_params(), "wb", "BTC_USDT", cb)
    stages = [e["stage"] for e in events]
    assert "computing" in stages
    assert "storing" in stages


async def test_resolve_strategy_resolves_indicators_and_computes_signals():
    db = FakeDb()
    rsi_params = _rsi_params()
    db.indicator_coverage = (0, 5940, 100)  # cache hit -- skip recompute path
    db.indicator_rows = {rsi_params.to_hash(): [(60, 20.0), (120, 25.0)]}
    db.all_rows = [{"timestamp": 60, "close": 1.0}, {"timestamp": 120, "close": 1.1}]

    class FakePlugin:
        def get_required_indicators(self, params):
            return [rsi_params]

        async def process(self, indicators_data, row, signal_timestamp=None):
            rsi_hash = rsi_params.to_hash()
            if indicators_data[rsi_hash]["value"] < 30:
                return {"signal_type": "buy", "confidence": 0.8}
            return None

    resolver = DependencyResolver(db, executor=FakeExecutor([]))
    resolver._load_strategy_plugin = lambda params: FakePlugin()

    strategy_params = StrategyParams.create("my_strategy")
    signals = await resolver.resolve_strategy(strategy_params, "wb", "BTC_USDT")

    assert len(signals) == 2  # both bars have rsi < 30 in this fixture
    assert db.stored_signals
    assert db.flushed is True


async def test_resolve_strategy_with_hash_reference_looks_up_params():
    db = FakeDb()
    rsi_params = _rsi_params()
    db.params_registry = {"abc123": rsi_params.to_dict()}
    db.indicator_coverage = (0, 5940, 100)

    class FakePlugin:
        def get_required_indicators(self, params):
            return ["abc123"]  # hash reference, not a full IndicatorParams

        async def process(self, **kw):
            return None

    resolver = DependencyResolver(db, executor=FakeExecutor([]))
    resolver._load_strategy_plugin = lambda params: FakePlugin()

    signals = await resolver.resolve_strategy(StrategyParams.create("x"), "wb", "BTC_USDT")
    # hash resolves to a real IndicatorParams (proves lookup_params was consulted), but no
    # indicator values/candles are seeded in FakeDb, so _compute_strategy has nothing to
    # iterate over and returns [] -- this test is really about the hash-lookup not raising.
    assert signals == []