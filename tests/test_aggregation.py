"""
tradingkit.aggregation — Aggregation/ScriptAggregation/load_aggregation_plugin (cross-source,
TASK-005), AggregationScript (within-table aggregation(unit) half only), AggregationContext.
"""
from __future__ import annotations

import pickle

import pytest

from tradingkit.aggregation import (
    Aggregation,
    AggregationContext,
    AggregationScript,
    ScriptAggregation,
    SourceRef,
    load_aggregation_plugin,
)

CH_MV_SCRIPT = '''
SOURCE_TABLE = "candles"

def aggregation(unit):
    return None
'''

SCRIPT_AGG_SQL = '''
OUTPUT_TABLE = "spread_btc_eth"
SOURCES = {
    "btc": SourceRef("candles_btc_usdt", field="close"),
    "eth": SourceRef("candles_eth_usdt", field="close"),
}
COMBINE_SQL = "btc - eth"
'''

SCRIPT_AGG_PYTHON = '''
OUTPUT_TABLE = "spread_btc_eth_py"
OUTPUT_SCHEMA = {"timestamp": "Int64", "value": "Float64"}

async def combine(ctx, start_ts, end_ts):
    return [{"timestamp": start_ts, "value": 1.0}]
'''


# ---------------------------------------------------------------------------
# AggregationScript — within-table (aggregation(unit)) half only; the cross-table
# aggregate(ctx, ...) half this class used to also wrap has been replaced by
# Aggregation/ScriptAggregation below, see tradingkit/aggregation.py's module docstring.
# ---------------------------------------------------------------------------

def test_ch_mv_script_detection():
    s = AggregationScript(CH_MV_SCRIPT)
    assert s.is_ch_mv() is True
    assert s.get_source_table() == "candles"


def test_get_agg_spec_for_unit_calls_aggregation_fn():
    s = AggregationScript(CH_MV_SCRIPT)
    result = s.get_agg_spec_for_unit(unit=object())
    assert result is None  # the fixture script's aggregation() returns None


def test_malformed_script_is_not_ch_mv():
    s = AggregationScript("this is ( not valid python !!")
    assert s.is_ch_mv() is False


def test_cross_source_script_is_not_ch_mv():
    # SOURCES/COMBINE_SQL-shaped scripts (the new Aggregation format) don't define
    # aggregation(unit) -- confirms the two mechanisms don't collide on detection, since
    # both are stored under the same plugin_library type='aggregation' rows.
    s = AggregationScript(SCRIPT_AGG_SQL)
    assert s.is_ch_mv() is False


# ---------------------------------------------------------------------------
# SourceRef + Aggregation
# ---------------------------------------------------------------------------

def test_source_ref_defaults():
    ref = SourceRef("candles_btc_usdt")
    assert ref.table == "candles_btc_usdt"
    assert ref.field == "close"
    assert ref.ch_type == "Float64"


def test_aggregation_required_sources_collects_class_attrs():
    # Same __mro__/vars() scan as Strategy.required_indicators() -- mirrors
    # test_strategy_required_indicators_collects_class_attrs in test_strategy.py.
    class BtcEthSpread(Aggregation):
        OUTPUT_TABLE = "btc_eth_spread"
        btc = SourceRef("candles_btc_usdt")
        eth = SourceRef("candles_eth_usdt")

        def combine_sql(self) -> str:
            return "btc - eth"

    reqs = BtcEthSpread.required_sources()
    assert set(reqs) == {"btc", "eth"}
    assert reqs["btc"].table == "candles_btc_usdt"
    assert BtcEthSpread().combine_sql() == "btc - eth"


def test_aggregation_defaults_are_none():
    class Bare(Aggregation):
        OUTPUT_TABLE = "x"

    agg = Bare()
    assert agg.combine_sql() is None
    assert agg.output_schema() is None
    assert agg.required_sources() == {}


async def test_aggregation_default_combine_returns_none():
    class Bare(Aggregation):
        OUTPUT_TABLE = "x"

    assert await Bare().combine(ctx=None, start_ts=0, end_ts=1) is None


# ---------------------------------------------------------------------------
# ScriptAggregation
# ---------------------------------------------------------------------------

def test_script_aggregation_sql_path():
    s = ScriptAggregation(SCRIPT_AGG_SQL)
    assert s.OUTPUT_TABLE == "spread_btc_eth"
    assert s.combine_sql() == "btc - eth"
    reqs = s.required_sources()
    assert set(reqs) == {"btc", "eth"}
    assert reqs["btc"].table == "candles_btc_usdt"


async def test_script_aggregation_python_path():
    s = ScriptAggregation(SCRIPT_AGG_PYTHON)
    assert s.OUTPUT_TABLE == "spread_btc_eth_py"
    assert s.combine_sql() is None
    assert s.output_schema() == {"timestamp": "Int64", "value": "Float64"}
    rows = await s.combine(ctx=None, start_ts=100, end_ts=200)
    assert rows == [{"timestamp": 100, "value": 1.0}]


async def test_script_aggregation_combine_returns_none_when_undefined():
    s = ScriptAggregation(SCRIPT_AGG_SQL)  # defines COMBINE_SQL, not combine()
    assert await s.combine(ctx=None, start_ts=0, end_ts=1) is None


def test_script_aggregation_pickle_roundtrip():
    # Same __builtins__-pickling fix as ScriptStrategy.__getstate__/__setstate__.
    s = ScriptAggregation(SCRIPT_AGG_SQL)
    assert "_ns" not in s.__getstate__()
    restored = pickle.loads(pickle.dumps(s))
    assert restored.OUTPUT_TABLE == "spread_btc_eth"
    assert restored.combine_sql() == "btc - eth"


# ---------------------------------------------------------------------------
# load_aggregation_plugin
# ---------------------------------------------------------------------------

def test_load_aggregation_plugin_script_type():
    agg = load_aggregation_plugin("__script__", {"_code": SCRIPT_AGG_SQL})
    assert isinstance(agg, ScriptAggregation)
    assert agg.OUTPUT_TABLE == "spread_btc_eth"


def test_load_aggregation_plugin_script_type_requires_code():
    with pytest.raises(ValueError):
        load_aggregation_plugin("__script__", {})


def test_load_aggregation_plugin_unknown_type_raises():
    with pytest.raises(ValueError):
        load_aggregation_plugin("does-not-exist", {})


def test_load_aggregation_plugin_builtin_registry(monkeypatch):
    monkeypatch.setenv("TRADINGKIT_AGGREGATIONS_MODULE", "tests._fixtures.fake_registry")
    agg = load_aggregation_plugin("my_agg", {})
    assert isinstance(agg, ScriptAggregation)
    assert agg.OUTPUT_TABLE == "spread_btc_eth"


# ---------------------------------------------------------------------------
# AggregationContext
# ---------------------------------------------------------------------------

async def test_aggregation_context_query_delegates_to_db():
    calls = []

    class FakeDb:
        async def fetch_from_unit_table(self, table, exchange, symbol, start_ts, end_ts):
            calls.append((table, exchange, symbol, start_ts, end_ts))
            return "fake-dataframe"

    ctx = AggregationContext(FakeDb(), exchange="whitebit")
    result = await ctx.query("candles", symbol="BTC_USDT", start_ts=1, end_ts=2)
    assert result == "fake-dataframe"
    assert calls == [("candles", "whitebit", "BTC_USDT", 1, 2)]
