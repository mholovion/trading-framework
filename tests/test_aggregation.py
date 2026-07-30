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
from tradingkit.core.clickhouse._sql import _SqlMixin
from tradingkit.core.clickhouse._unit_tables import (
    _render_equality_filters,
    _UnitTablesMixin,
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

class _RecordingDb:
    """Captures what AggregationContext.query() forwarded, so the filters it applies are
    asserted rather than inferred."""

    def __init__(self):
        self.calls: list[dict] = []

    async def fetch_from_unit_table(self, table, exchange=None, symbol=None,
                                    start_ts=None, end_ts=None, filters=None):
        self.calls.append({"table": table, "exchange": exchange, "symbol": symbol,
                           "start_ts": start_ts, "end_ts": end_ts,
                           "filters": dict(filters or {})})
        return "fake-dataframe"


async def test_aggregation_context_query_delegates_to_db():
    db = _RecordingDb()
    ctx = AggregationContext(db, {"exchange": "whitebit"})
    result = await ctx.query("candles", symbol="BTC_USDT", start_ts=1, end_ts=2)
    assert result == "fake-dataframe"
    assert db.calls == [{
        "table": "candles", "exchange": None, "symbol": "BTC_USDT",
        "start_ts": 1, "end_ts": 2, "filters": {"exchange": "whitebit"},
    }]


async def test_aggregation_context_applies_no_filter_when_none_declared():
    """Regression on TASK-023: the worker used to pass the *project namespace* as an
    exchange, so a project named "my_bot" filtered source rows by exchange="my_bot" and
    matched nothing, silently. No declaration must now mean no filter, not a wrong one."""
    db = _RecordingDb()
    await AggregationContext(db).query("candles")
    assert db.calls[0]["filters"] == {}
    assert db.calls[0]["exchange"] is None


async def test_aggregation_context_per_query_filter_overrides_declared():
    db = _RecordingDb()
    ctx = AggregationContext(db, {"exchange": "whitebit"})
    await ctx.query("candles", exchange="binance", timeframe="1m")
    assert db.calls[0]["filters"] == {"exchange": "binance", "timeframe": "1m"}


# ---------------------------------------------------------------------------
# SourceRef filters + generated WHERE (TASK-022)
# ---------------------------------------------------------------------------

def test_source_ref_defaults_to_no_filters():
    ref = SourceRef("candles_btc_usdt", field="close")
    assert ref.filters == {}


def test_source_ref_copies_filters():
    """A shared dict would let one SourceRef's filters mutate another's."""
    shared = {"symbol": "BTC_USDT"}
    ref = SourceRef("candles", filters=shared)
    shared["symbol"] = "ETH_USDT"
    assert ref.filters == {"symbol": "BTC_USDT"}


def test_render_equality_filters_empty_keeps_sql_unchanged():
    """Backwards compatibility: a one-series table with no filters must produce exactly
    the SQL it produced before filters existed."""
    assert _render_equality_filters(None, _SqlMixin._fmt) == ""
    assert _render_equality_filters({}, _SqlMixin._fmt) == ""


def test_render_equality_filters_sorts_for_determinism():
    where = _render_equality_filters(
        {"symbol": "BTC_USDT", "exchange": "whitebit"}, _SqlMixin._fmt
    )
    assert where == " WHERE exchange = 'whitebit' AND symbol = 'BTC_USDT'"


def test_render_equality_filters_rejects_a_non_identifier_column():
    with pytest.raises(ValueError, match="filter column"):
        _render_equality_filters({"symbol; DROP TABLE x": "BTC"}, _SqlMixin._fmt)


@pytest.mark.parametrize("value,expected", [
    ("x' OR 1=1 --",        "'x\\' OR 1=1 --'"),
    ("back\\slash",         "'back\\\\slash'"),
    ("plain",               "'plain'"),
])
def test_render_equality_filters_escapes_values(value, expected):
    """A Materialized View body is DDL and cannot be parameterized, so the value has to be
    rendered safely. This is the injection path that matters: the aggregation comes from
    plugin_library, i.e. from user input."""
    assert _render_equality_filters({"symbol": value}, _SqlMixin._fmt) == \
        f" WHERE symbol = {expected}"


class _SqlCapturingDb(_UnitTablesMixin, _SqlMixin):
    """Captures generated SQL without a ClickHouse connection, so the MV/backfill DDL can
    be asserted directly."""

    def __init__(self, schema=None, distinct=None):
        self.sql: list[str] = []
        self._schema = schema or {}
        self._distinct = distinct or {}

    async def _execute(self, sql, params=None, settings=None):
        self.sql.append(sql)
        for col, values in self._distinct.items():
            if f"SELECT DISTINCT {col} " in sql:
                return [(v,) for v in values]
        return []

    async def get_table_schema(self, table_name):
        return dict(self._schema)


async def test_mv_and_backfill_receive_identical_where():
    """The MV covers rows from now on and the backfill covers rows already there; if their
    WHERE clauses differ, clean live data ends up sitting on mixed history."""
    db = _SqlCapturingDb()
    filters = {"exchange": "whitebit", "symbol": "BTC_USDT"}
    await db.ensure_cross_source_mv("out", "candles", "btc", "close", "Float64",
                                    filters=filters)
    await db.backfill_cross_source("out", "candles", "btc", "close", "Float64",
                                   filters=filters)

    mv_sql, backfill_sql = db.sql
    where = " WHERE exchange = 'whitebit' AND symbol = 'BTC_USDT' "
    assert where in mv_sql
    assert where in backfill_sql


async def test_mv_without_filters_matches_the_previous_sql_shape():
    db = _SqlCapturingDb()
    await db.ensure_cross_source_mv("out", "candles_btc_usdt", "btc", "close", "Float64")
    assert "FROM candles_btc_usdt GROUP BY timestamp" in db.sql[0]
    assert "WHERE" not in db.sql[0]


# ---------------------------------------------------------------------------
# Ambiguity detection (TASK-022) — fail loudly instead of mixing series
# ---------------------------------------------------------------------------

async def test_ambiguous_source_raises_naming_the_column():
    db = _SqlCapturingDb(
        schema={"timestamp": None, "exchange": None, "symbol": None, "close": None},
        distinct={"symbol": ["BTC_USDT", "ETH_USDT", "SOL_USDT"], "exchange": ["whitebit"]},
    )
    with pytest.raises(ValueError, match="symbol"):
        await db.assert_source_unambiguous("candles")


async def test_pinned_column_is_not_ambiguous():
    db = _SqlCapturingDb(
        schema={"timestamp": None, "symbol": None, "close": None},
        distinct={"symbol": ["BTC_USDT", "ETH_USDT"]},
    )
    await db.assert_source_unambiguous("candles", {"symbol": "BTC_USDT"})


async def test_single_series_table_passes_without_filters():
    """The one-table-per-source style from the docstrings keeps working untouched."""
    db = _SqlCapturingDb(
        schema={"timestamp": None, "symbol": None, "close": None},
        distinct={"symbol": ["BTC_USDT"]},
    )
    await db.assert_source_unambiguous("candles_btc_usdt")


async def test_table_without_identity_columns_passes():
    db = _SqlCapturingDb(schema={"timestamp": None, "value": None})
    await db.assert_source_unambiguous("some_derived_table")


async def test_missing_table_is_not_treated_as_ambiguous():
    class Exploding(_SqlCapturingDb):
        async def get_table_schema(self, table_name):
            raise RuntimeError("table does not exist")

    await Exploding().assert_source_unambiguous("not_created_yet")
