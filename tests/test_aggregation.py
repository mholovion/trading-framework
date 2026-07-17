"""tradingkit.aggregation — AggregationScript AST introspection, AggregationContext.query()."""
from __future__ import annotations

import pytest

from tradingkit.aggregation import AggregationContext, AggregationScript, load_aggregation_script


CH_MV_SCRIPT = '''
SOURCE_TABLE = "candles"

def aggregation(unit):
    return None
'''

PYTHON_AGG_SCRIPT = '''
OUTPUT_TABLE  = "spread_btc_eth"
OUTPUT_SCHEMA = {"timestamp": "Int64", "spread": "Float64"}
INTERVAL_S    = 30

async def aggregate(ctx, start_ts, end_ts):
    return []
'''


def test_ch_mv_script_detection():
    s = AggregationScript(CH_MV_SCRIPT)
    assert s.is_ch_mv() is True
    assert s.is_python() is False
    assert s.get_source_table() == "candles"


def test_python_script_detection():
    s = AggregationScript(PYTHON_AGG_SCRIPT)
    assert s.is_python() is True
    assert s.is_ch_mv() is False
    assert s.get_output_table() == "spread_btc_eth"
    assert s.get_output_schema() == {"timestamp": "Int64", "spread": "Float64"}
    assert s.get_interval_s() == 30


def test_interval_s_defaults_to_60_when_absent():
    s = AggregationScript("def aggregate(ctx, a, b): return []")
    assert s.get_interval_s() == 60


def test_malformed_script_degrades_gracefully():
    s = AggregationScript("this is ( not valid python !!")
    assert s.is_ch_mv() is False
    assert s.is_python() is False
    assert s.get_output_table() is None


async def test_run_aggregate_executes_and_awaits_coroutine():
    s = AggregationScript(PYTHON_AGG_SCRIPT)
    rows = await s.run_aggregate(ctx=None, start_ts=0, end_ts=100)
    assert rows == []


async def test_run_aggregate_returns_empty_when_no_aggregate_fn():
    s = AggregationScript(CH_MV_SCRIPT)
    rows = await s.run_aggregate(ctx=None, start_ts=0, end_ts=100)
    assert rows == []


def test_get_agg_spec_for_unit_calls_aggregation_fn():
    s = AggregationScript(CH_MV_SCRIPT)
    result = s.get_agg_spec_for_unit(unit=object())
    assert result is None  # the fixture script's aggregation() returns None


def test_load_aggregation_script_with_explicit_code():
    s = load_aggregation_script("anything", code=PYTHON_AGG_SCRIPT)
    assert s.is_python() is True


def test_load_aggregation_script_unknown_name_raises():
    with pytest.raises(KeyError):
        load_aggregation_script("does-not-exist")


async def test_aggregation_context_query_delegates_to_db(monkeypatch):
    calls = []

    class FakeDb:
        async def fetch_from_unit_table(self, table, exchange, symbol, start_ts, end_ts):
            calls.append((table, exchange, symbol, start_ts, end_ts))
            return "fake-dataframe"

    ctx = AggregationContext(FakeDb(), exchange="whitebit")
    result = await ctx.query("candles", symbol="BTC_USDT", start_ts=1, end_ts=2)
    assert result == "fake-dataframe"
    assert calls == [("candles", "whitebit", "BTC_USDT", 1, 2)]