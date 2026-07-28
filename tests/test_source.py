"""tradingkit.source — DataSource/ScriptSource/ConnectionScriptSource."""
from __future__ import annotations

import polars as pl
import pytest

from tradingkit.source import ConnectionScriptSource, DataSource, ScriptSource


async def test_script_source_returns_dataframe():
    src = ScriptSource(code="result = pl.DataFrame({'timestamp': [1, 2], 'close': [1.0, 2.0]})")
    df = await src.get_historical_data("BTC_USDT", 60, 0, 100)
    assert isinstance(df, pl.DataFrame)
    assert df["close"].to_list() == [1.0, 2.0]


async def test_script_source_receives_call_params_in_namespace():
    src = ScriptSource(code="result = pl.DataFrame({'timestamp': [start_ts, end_ts], 'v': [symbol, str(limit)]})")
    df = await src.get_historical_data("ETH_USDT", 300, 10, 20, limit=5)
    assert df["timestamp"].to_list() == [10, 20]
    assert df["v"].to_list() == ["ETH_USDT", "5"]


async def test_script_source_missing_result_raises():
    src = ScriptSource(code="x = 1")
    with pytest.raises(ValueError):
        await src.get_historical_data("BTC_USDT", 60, 0, 100)


async def test_script_source_wrong_result_type_raises():
    src = ScriptSource(code="result = [1, 2, 3]")
    with pytest.raises(TypeError):
        await src.get_historical_data("BTC_USDT", 60, 0, 100)


async def test_data_source_default_stream_raises_not_implemented():
    class Dummy(DataSource):
        async def get_historical_data(self, *a, **kw):
            return pl.DataFrame()

    d = Dummy()
    with pytest.raises(NotImplementedError):
        await d.stream("BTC_USDT", 60)


FULL_SOURCE_SCRIPT = '''
__params__ = {"batch_size": {"type": "int", "default": 1440, "label": "Batch size"}}
TABLE_NAME = "funding_rates"

async def historical(symbol, timeframe, start_ts, end_ts, config):
    return [{"symbol": symbol, "batch_size": config["batch_size"]}]

async def realtime(symbol, timeframe, config):
    yield {"symbol": symbol}
'''


def test_connection_script_source_extract_params():
    src = ConnectionScriptSource(FULL_SOURCE_SCRIPT)
    assert src.extract_params() == {
        "batch_size": {"type": "int", "default": 1440, "label": "Batch size"},
    }


def test_connection_script_source_merged_config_applies_overrides():
    src = ConnectionScriptSource(FULL_SOURCE_SCRIPT, config={"batch_size": 500})
    assert src.merged_config() == {"batch_size": 500}


def test_connection_script_source_merged_config_uses_defaults():
    src = ConnectionScriptSource(FULL_SOURCE_SCRIPT)
    assert src.merged_config() == {"batch_size": 1440}


def test_connection_script_source_has_historical_and_realtime():
    src = ConnectionScriptSource(FULL_SOURCE_SCRIPT)
    assert src.has_historical() is True
    assert src.has_realtime() is True


def test_connection_script_source_get_table_name():
    src = ConnectionScriptSource(FULL_SOURCE_SCRIPT)
    assert src.get_table_name() == "funding_rates"


def test_connection_script_source_empty_script_degrades_gracefully():
    src = ConnectionScriptSource("")
    assert src.has_historical() is False
    assert src.has_realtime() is False
    assert src.get_table_name() is None
    assert src.extract_params() == {}


async def test_connection_script_source_runs_historical():
    src = ConnectionScriptSource(FULL_SOURCE_SCRIPT)
    df = await src.get_historical_data("BTC_USDT", 3600, 0, 100)
    assert df.to_dicts() == [{"symbol": "BTC_USDT", "batch_size": 1440}]


async def test_connection_script_source_deprecated_historical_shim():
    """historical() still works for old callers, but now normalizes to a DataFrame."""
    src = ConnectionScriptSource(FULL_SOURCE_SCRIPT)
    df = await src.historical("BTC_USDT", "1h", 0, 100)
    assert df.to_dicts() == [{"symbol": "BTC_USDT", "batch_size": 1440}]


async def test_script_with_no_recognized_convention_raises():
    """A script matching none of the three conventions names all of them in the error."""
    src = ConnectionScriptSource("TABLE_NAME = 'x'")
    with pytest.raises(ValueError, match="historical"):
        await src.get_historical_data("BTC_USDT", 3600, 0, 100)


async def test_connection_script_source_streams_realtime():
    src = ConnectionScriptSource(FULL_SOURCE_SCRIPT)
    rows = [row async for row in src.stream("BTC_USDT", 3600)]
    assert rows == [{"symbol": "BTC_USDT"}]


# ------------------------------------------------------------------ #
# Class convention — script subclasses DataSource directly             #
# ------------------------------------------------------------------ #

CLASS_SOURCE_SCRIPT = '''
class MySource(DataSource):
    timestamp_unit = "ms"
    TABLE_NAME = "unit_custom"

    async def get_historical_data(self, symbol, timeframe, start_ts, end_ts, limit=5000):
        return pl.DataFrame({"timestamp": [start_ts], "symbol": [symbol]})

    async def detect_gaps(self, rows, timeframe):
        return [{"start_timestamp": 1, "end_timestamp": 2, "missing_rows": 1}]

    async def stream(self, symbol, timeframe):
        yield {"timestamp": 1, "symbol": symbol}
'''


async def test_class_convention_delegates_get_historical_data():
    src = ScriptSource(CLASS_SOURCE_SCRIPT)
    df = await src.get_historical_data("BTC_USDT", 100, 42, 99)
    assert df.to_dicts() == [{"timestamp": 42, "symbol": "BTC_USDT"}]


async def test_class_convention_overrides_detect_gaps():
    """The whole point of the class convention: hooks beyond fetching are overridable."""
    src = ScriptSource(CLASS_SOURCE_SCRIPT)
    assert await src.detect_gaps([], 100) == [
        {"start_timestamp": 1, "end_timestamp": 2, "missing_rows": 1}
    ]


async def test_class_convention_streams():
    src = ScriptSource(CLASS_SOURCE_SCRIPT)
    assert [r async for r in src.stream("ETH", 100)] == [{"timestamp": 1, "symbol": "ETH"}]


def test_class_convention_exposes_timestamp_unit_and_table_name():
    src = ScriptSource(CLASS_SOURCE_SCRIPT)
    assert src.timestamp_unit == "ms"
    assert src.get_table_name() == "unit_custom"
    assert src.has_historical() is True
    assert src.has_realtime() is True


def test_class_convention_picks_most_derived_class():
    """A script may define a shared base plus a concrete subclass; dict order must not
    decide which one gets instantiated."""
    src = ScriptSource('''
class Base(DataSource):
    async def get_historical_data(self, *a, **kw):
        return pl.DataFrame({"which": ["base"]})

class Concrete(Base):
    async def get_historical_data(self, *a, **kw):
        return pl.DataFrame({"which": ["concrete"]})
''')
    assert src._impl_or_none().__class__.__name__ == "Concrete"


async def test_class_convention_init_without_config_arg():
    """A user __init__ that takes no config must still instantiate."""
    src = ScriptSource('''
class NoConfig(DataSource):
    def __init__(self):
        super().__init__()
        self.marker = "built"
    async def get_historical_data(self, *a, **kw):
        return pl.DataFrame()
''')
    assert src._impl_or_none().marker == "built"


def test_class_convention_abstract_class_raises_clearly():
    """Forgetting get_historical_data must surface Python's own error, not be swallowed."""
    src = ScriptSource("class Broken(DataSource):\n    pass\n")
    with pytest.raises(TypeError, match="abstract"):
        src._impl_or_none()


# ------------------------------------------------------------------ #
# Preparation / caching                                                #
# ------------------------------------------------------------------ #

COUNTING_SCRIPT = '''
try:
    _prepared_count += 1
except NameError:
    _prepared_count = 1

async def historical(symbol, timeframe, start_ts, end_ts, config):
    return [{"timestamp": start_ts, "n": _prepared_count}]
'''


async def test_prepare_compiles_script_only_once():
    """Every historical() call used to recompile+exec the whole script."""
    src = ScriptSource(COUNTING_SCRIPT)
    await src.get_historical_data("BTC", 60, 1, 2)
    await src.get_historical_data("BTC", 60, 3, 4)
    df = await src.get_historical_data("BTC", 60, 5, 6)
    assert df["n"].to_list() == [1]


async def test_prepare_recompiles_when_code_changes():
    """Cache is keyed on the code itself, so swapping it invalidates."""
    src = ScriptSource(COUNTING_SCRIPT)
    await src.get_historical_data("BTC", 60, 1, 2)
    ns_before = src._ns
    src._code = COUNTING_SCRIPT + "\n# edited\n"
    await src.get_historical_data("BTC", 60, 1, 2)
    assert src._ns is not ns_before


def test_source_is_picklable_after_preparation():
    """SubprocessExecutor pickles the source on every call; the prepared namespace can
    hold unpicklable objects, so it must not travel."""
    import pickle
    src = ScriptSource(CLASS_SOURCE_SCRIPT)
    src._impl_or_none()                       # warm the cache
    restored = pickle.loads(pickle.dumps(src))
    assert restored._ns is None
    assert restored._impl is None
    assert restored.timestamp_unit == "ms"    # rebuilt on demand


def test_expression_convention_is_not_prepared_eagerly():
    """Expression scripts read injected variables at module level -- exec'ing them at
    preparation time would raise NameError before symbol/start_ts exist."""
    src = ScriptSource(code="result = pl.DataFrame({'timestamp': [start_ts]})")
    assert src._impl_or_none() is None
    assert src._ns is None


# ------------------------------------------------------------------ #
# Gap recovery defaults / unit-agnosticism                             #
# ------------------------------------------------------------------ #

class _Dummy(DataSource):
    async def get_historical_data(self, symbol, timeframe, start_ts, end_ts, limit=5000):
        return pl.DataFrame({"timestamp": [start_ts], "close": [1.0]})


async def test_detect_gaps_finds_missing_bars():
    rows = [{"timestamp": 0}, {"timestamp": 60}, {"timestamp": 300}]
    assert await _Dummy().detect_gaps(rows, 60) == [
        {"start_timestamp": 120, "end_timestamp": 240, "missing_rows": 3},
    ]


async def test_detect_gaps_tolerates_duplicate_and_backwards_timestamps():
    """A source re-pushing the same still-forming bar must not look like a gap."""
    rows = [{"timestamp": 60}, {"timestamp": 60}, {"timestamp": 55}, {"timestamp": 120}]
    assert await _Dummy().detect_gaps(rows, 60) == []


async def test_detect_gaps_is_unit_agnostic():
    """Same shape of data in milliseconds yields the same gaps -- the arithmetic never
    assumes seconds."""
    sec = [{"timestamp": 0}, {"timestamp": 60}, {"timestamp": 300}]
    ms  = [{"timestamp": t["timestamp"] * 1000} for t in sec]
    gaps_s  = await _Dummy().detect_gaps(sec, 60)
    gaps_ms = await _Dummy().detect_gaps(ms, 60_000)
    assert gaps_ms == [{k: v * 1000 if k != "missing_rows" else v for k, v in g.items()}
                       for g in gaps_s]


def test_batch_gaps_merges_close_gaps():
    source = _Dummy({"gap_max_batch": 1000, "gap_max_time_gap": 3600})
    batches = source.batch_gaps([
        {"start_timestamp": 0, "end_timestamp": 60, "missing_rows": 10},
        {"start_timestamp": 120, "end_timestamp": 180, "missing_rows": 10},
    ])
    assert batches == [{"start_timestamp": 0, "end_timestamp": 180, "total_rows": 20}]


def test_batch_gaps_splits_when_over_max_batch():
    source = _Dummy({"gap_max_batch": 15, "gap_max_time_gap": 3600})
    assert len(source.batch_gaps([
        {"start_timestamp": 0, "end_timestamp": 60, "missing_rows": 10},
        {"start_timestamp": 120, "end_timestamp": 180, "missing_rows": 10},
    ])) == 2


def test_batch_gaps_splits_when_too_far_apart():
    source = _Dummy({"gap_max_batch": 1000, "gap_max_time_gap": 100})
    assert len(source.batch_gaps([
        {"start_timestamp": 0, "end_timestamp": 60, "missing_rows": 1},
        {"start_timestamp": 10_000, "end_timestamp": 10_060, "missing_rows": 1},
    ])) == 2


def test_batch_gaps_sorts_unordered_input():
    source = _Dummy({"gap_max_batch": 1000, "gap_max_time_gap": 3600})
    batches = source.batch_gaps([
        {"start_timestamp": 120, "end_timestamp": 180, "missing_rows": 1},
        {"start_timestamp": 0, "end_timestamp": 60, "missing_rows": 1},
    ])
    assert batches[0]["start_timestamp"] == 0


def test_batch_gaps_empty_input():
    assert _Dummy().batch_gaps([]) == []


async def test_fetch_gap_returns_real_rows():
    rows = await _Dummy().fetch_gap("BTC", 60, 120, 240)
    assert rows == [{"timestamp": 120, "close": 1.0}]


async def test_rate_limit_throttles_by_min_delay():
    import time as _time
    src = _Dummy({"rate_limit_ms": 60})
    await src.rate_limit()
    started = _time.monotonic()
    await src.rate_limit()
    assert _time.monotonic() - started >= 0.03


def test_params_cache_memoizes():
    src = ConnectionScriptSource(FULL_SOURCE_SCRIPT)
    first = src.extract_params()
    second = src.extract_params()
    assert first is second