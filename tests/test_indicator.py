"""tradingkit.indicator — Indicator/ScriptIndicator/JitIndicator/CppIndicator."""
from __future__ import annotations

import pickle

import numpy as np
import polars as pl
import pytest

from tradingkit.indicator import CppIndicator, Indicator, IndicatorContext, JitIndicator, ScriptIndicator


@pytest.fixture
def df() -> pl.DataFrame:
    return pl.DataFrame({
        "timestamp": [1, 2, 3, 4, 5],
        "close": [1.0, 2.0, 3.0, 4.0, 5.0],
    })


def test_indicator_context_requires_timestamp():
    with pytest.raises(ValueError):
        IndicatorContext(pl.DataFrame({"close": [1.0]}))


def test_indicator_context_np_and_column_access(df):
    ctx = IndicatorContext(df)
    assert list(ctx.close) == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert isinstance(ctx.np.close, np.ndarray)
    assert list(ctx.ts) == [1, 2, 3, 4, 5]
    assert len(ctx) == 5
    with pytest.raises(AttributeError):
        ctx.nonexistent_column


class DoubleClose(Indicator):
    def compute(self, ctx: IndicatorContext) -> np.ndarray:
        return ctx.np.close * 2

    def required_periods(self) -> int:
        return 1


def test_custom_indicator_params_and_call(df):
    ind = DoubleClose(period=7)
    assert ind.params == {"period": 7}
    assert ind.period == 7
    series = ind(IndicatorContext(df))
    assert isinstance(series, pl.Series)
    assert list(series) == [2.0, 4.0, 6.0, 8.0, 10.0]


def test_script_indicator_computes(df):
    ind = ScriptIndicator(code="result = close * 2", period=1)
    series = ind(IndicatorContext(df))
    assert list(series) == [2.0, 4.0, 6.0, 8.0, 10.0]


def test_script_indicator_missing_result_raises(df):
    ind = ScriptIndicator(code="x = 1", period=1)
    with pytest.raises(ValueError):
        ind.compute(IndicatorContext(df))


def test_script_indicator_pickle_roundtrip():
    ind = ScriptIndicator(code="result = close * 2", period=14)
    restored = pickle.loads(pickle.dumps(ind))
    assert restored._code == ind._code
    assert restored.period == 14


def test_jit_indicator_computes_correctly(df):
    ind = JitIndicator(code="result = close * 2.0", period=1)
    series = ind(IndicatorContext(df))
    assert list(series) == [2.0, 4.0, 6.0, 8.0, 10.0]


def test_jit_indicator_caches_compiled_function_per_columns(df):
    ind = JitIndicator(code="result = close * 2.0", period=1)
    ind(IndicatorContext(df))
    assert ("timestamp", "close") in ind._jit_cache
    ind(IndicatorContext(df))  # second call reuses the cache, doesn't recompile
    assert len(ind._jit_cache) == 1


def test_jit_indicator_getstate_excludes_compiled_cache(df):
    ind = JitIndicator(code="result = close * 2.0", period=1)
    ind(IndicatorContext(df))
    assert ind._jit_cache  # populated
    state = ind.__getstate__()
    assert state["_jit_cache"] == {}


def test_jit_indicator_pickle_roundtrip_recompiles(df):
    ind = JitIndicator(code="result = close * 2.0", period=1)
    ind(IndicatorContext(df))
    restored = pickle.loads(pickle.dumps(ind))
    assert restored._jit_cache == {}
    series = restored(IndicatorContext(df))
    assert list(series) == [2.0, 4.0, 6.0, 8.0, 10.0]


def test_cpp_indicator_compute_raises_without_runner_pool(df):
    ind = CppIndicator(cpp_code="// ...", so_bytes=b"\x7fELF", period=14)
    with pytest.raises(RuntimeError):
        ind.compute(IndicatorContext(df))


def test_cpp_indicator_required_periods_from_params():
    ind = CppIndicator(cpp_code="", so_bytes=b"", period=21)
    assert ind.required_periods() == 21


def test_cpp_indicator_pickle_roundtrip():
    ind = CppIndicator(cpp_code="// src", so_bytes=b"\x00\x01\x02", period=14)
    restored = pickle.loads(pickle.dumps(ind))
    assert restored._cpp_code == "// src"
    assert restored._so_bytes == b"\x00\x01\x02"


def test_ta_namespace_rsi(df):
    big_df = pl.DataFrame({
        "timestamp": list(range(30)),
        "close": [float(100 + i) for i in range(30)],
    })
    ctx = IndicatorContext(big_df)
    rsi = ctx.ta.rsi(ctx.np.close, 14)
    assert isinstance(rsi, np.ndarray)
    assert len(rsi) == 30