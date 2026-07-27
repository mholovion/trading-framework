"""
ClickHouseManager against a real ClickHouse server.

Unlike test_clickhouse_manager.py (mocked HTTP transport — proves the Python code builds
the right SQL string), these tests run the SQL for real and check the results, and are
the only place the native protocol (asynch.Connection, used by _bulk_insert/initialize)
gets exercised at all.

Requires a reachable ClickHouse: set CLICKHOUSE_HOST (defaults to "localhost"). Skips the
whole module if the server isn't reachable, so `pytest` stays green with no ClickHouse
running (the common case for local dev) and only activates where one is actually up
(CI's `services:` container, or a manually started `docker run clickhouse/clickhouse-server`).
"""
from __future__ import annotations

import os
import socket
import uuid

import pytest

from tradingkit.core.clickhouse import ClickHouseManager
from tradingkit.schema import Fold


def _clickhouse_reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
_HTTP_PORT = int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"))
_NATIVE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))

pytestmark = pytest.mark.skipif(
    not _clickhouse_reachable(_HOST, _HTTP_PORT),
    reason=f"no ClickHouse reachable at {_HOST}:{_HTTP_PORT} (set CLICKHOUSE_HOST to enable)",
)


@pytest.fixture
async def db():
    manager = ClickHouseManager(host=_HOST, port=_NATIVE_PORT, http_port=_HTTP_PORT)
    await manager.initialize()
    assert manager._conn is not None, "native protocol connection failed — is ClickHouse actually up?"
    try:
        yield manager
    finally:
        await manager.close()


@pytest.fixture
def table_name() -> str:
    # unique per test run so parallel/repeat runs don't collide on leftover tables
    return f"test_unit_{uuid.uuid4().hex[:12]}"


async def test_native_protocol_connects(db):
    """initialize() must establish a real asynch connection -- this is the one path
    none of the mocked tests touch at all."""
    assert db._conn is not None


async def test_raw_table_roundtrip(db, table_name):
    import polars as pl

    await db.ensure_raw_table(table_name, {"timestamp": pl.Int64, "value": pl.Float64})
    df = pl.DataFrame({"timestamp": [1, 2, 3], "value": [10.0, 20.0, 30.0]})
    await db.insert_unit_batch(table_name, df, exchange="test_ex", symbol="TEST")
    await db.flush()

    result = await db.fetch_from_unit_table(table_name, exchange="test_ex", symbol="TEST")
    assert sorted(result["timestamp"].to_list()) == [1, 2, 3]
    assert sorted(result["value"].to_list()) == [10.0, 20.0, 30.0]


async def test_insert_unit_batch_stores_timeframe(db, table_name):
    """Regression: insert_unit_batch() had no timeframe parameter at all, so every row
    landed with ClickHouse's zero-default (empty string) regardless of what the caller's
    actual timeframe was -- found via a real deployment where this broke timeframe-filtered
    chart queries for a symbol whose collector was still actively writing."""
    import polars as pl

    await db.ensure_raw_table(table_name, {"timestamp": pl.Int64, "close": pl.Float64})
    df = pl.DataFrame({"timestamp": [1000], "close": [100.0]})
    await db.insert_unit_batch(table_name, df, exchange="whitebit", symbol="SOL_USDT", timeframe="1m")
    await db.flush()

    rows = await db._execute(f"SELECT timeframe FROM {table_name}")
    assert [r[0] for r in rows] == ["1m"]


async def test_ensure_raw_table_migrates_existing_table_missing_timeframe_column(db, table_name):
    """Regression: ensure_raw_table() only ran CREATE TABLE IF NOT EXISTS, so a table
    created before timeframe existed in the DDL would never gain the column -- any insert
    passing a real timeframe against it would fail with an unknown-column error."""
    import polars as pl

    # Simulates a table that predates this fix -- exact old-style DDL, no timeframe column.
    await db._execute(
        f"CREATE TABLE {table_name} (exchange String, symbol String, timestamp Int64, "
        f"close Float64) ENGINE = ReplacingMergeTree ORDER BY (exchange, symbol, timestamp)"
    )

    await db.ensure_raw_table(table_name, {"timestamp": pl.Int64, "close": pl.Float64})
    df = pl.DataFrame({"timestamp": [1000], "close": [100.0]})
    await db.insert_unit_batch(table_name, df, exchange="whitebit", symbol="SOL_USDT", timeframe="5m")
    await db.flush()

    rows = await db._execute(f"SELECT timeframe FROM {table_name}")
    assert [r[0] for r in rows] == ["5m"]


async def test_agg_table_and_materialized_view(db, table_name):
    """Proves ensure_agg_table/ensure_mv/backfill_agg produce SQL ClickHouse actually
    accepts and that the MV really aggregates -- the mocked tests can't tell the
    difference between correct and subtly-wrong ClickHouse syntax."""
    import polars as pl

    await db.ensure_raw_table(table_name, {"timestamp": pl.Int64, "price": pl.Float64})
    df = pl.DataFrame({
        "timestamp": [0, 30, 60, 90],
        "price":     [1.0, 2.0, 3.0, 4.0],
    })
    await db.insert_unit_batch(table_name, df, exchange="test_ex", symbol="TEST")
    await db.flush()

    folds = [Fold("sum", "price", "Float64").alias("total_price")]
    await db.ensure_agg_table(table_name, 60, folds)
    await db.ensure_mv(table_name, 60, folds)
    await db.backfill_agg(table_name, 60, folds)

    rows = await db.query_agg(table_name, 60, folds, exchange="test_ex", symbol="TEST")
    by_bucket = {r["bucket"]: r["total_price"] for r in rows}
    assert by_bucket.get(0) == pytest.approx(3.0)   # rows at t=0,30 -> bucket 0
    assert by_bucket.get(60) == pytest.approx(7.0)  # rows at t=60,90 -> bucket 60


async def test_agg_table_with_leading_param_function(db, table_name):
    """quantile-family functions use a different ClickHouse calling convention than
    sum/count/etc: the parameter isn't part of the serialized state, so it has to be
    supplied again at Merge time too (quantileMerge(0.95)(state), not
    quantileMerge(state) -- the latter silently defaults to the median instead of
    erroring, which is exactly the kind of wrong-not-failing bug mocks can't catch)."""
    import polars as pl

    await db.ensure_raw_table(table_name, {"timestamp": pl.Int64, "price": pl.Float64})
    df = pl.DataFrame({
        "timestamp": [0, 1, 2, 3, 4],
        "price":     [1.0, 2.0, 3.0, 4.0, 5.0],
    })
    await db.insert_unit_batch(table_name, df, exchange="test_ex", symbol="TEST")
    await db.flush()

    folds = [Fold("quantileExact", "price", "Float64", args=[0.95]).alias("q95")]
    await db.ensure_agg_table(table_name, 60, folds)
    await db.ensure_mv(table_name, 60, folds)
    await db.backfill_agg(table_name, 60, folds)

    rows = await db.query_agg(table_name, 60, folds, exchange="test_ex", symbol="TEST")
    by_bucket = {r["bucket"]: r["q95"] for r in rows}
    # quantileExact(0.95) of [1,2,3,4,5] -- if Merge silently dropped to the default
    # level (0.5, the median) this would read 3.0 instead.
    assert by_bucket.get(0) == pytest.approx(5.0)


async def test_agg_table_with_combinator_function(db, table_name):
    """sumIf/countIf-style combinator functions use yet another calling convention:
    the extra arg (the condition) sits alongside the column in one paren list, and --
    unlike quantile -- Merge needs no arg at all, since the condition was already
    applied when the state was built."""
    import polars as pl

    await db.ensure_raw_table(table_name, {"timestamp": pl.Int64, "qty": pl.Float64, "side": pl.Int64})
    df = pl.DataFrame({
        "timestamp": [0, 1, 2, 3, 4],
        "qty":       [10.0, 5.0, 20.0, 0.0, 0.0],
        "side":      [1, 0, 1, 0, 0],
    })
    await db.insert_unit_batch(table_name, df, exchange="test_ex", symbol="TEST")
    await db.flush()

    folds = [Fold("sumIf", "qty", "Float64", args=["side = 1"]).alias("sum_side1")]
    await db.ensure_agg_table(table_name, 60, folds)
    await db.ensure_mv(table_name, 60, folds)
    await db.backfill_agg(table_name, 60, folds)

    rows = await db.query_agg(table_name, 60, folds, exchange="test_ex", symbol="TEST")
    by_bucket = {r["bucket"]: r["sum_side1"] for r in rows}
    assert by_bucket.get(0) == pytest.approx(30.0)  # rows where side=1: 10 + 20


async def test_cross_source_aggregation_out_of_order_arrival(db):
    """The core TASK-005 mechanism: two sources arriving in different order still produce
    a correct combined value once both are present, and never a misleading partial result
    before that -- this is the exact scenario verified by hand while designing the gap-
    closing two-MV approach, now a permanent regression test."""
    import uuid

    from tradingkit.aggregation import Aggregation, SourceRef, query_aggregation, setup_aggregation

    suffix = uuid.uuid4().hex[:8]
    btc_table = f"test_btc_{suffix}"
    eth_table = f"test_eth_{suffix}"
    output_table = f"test_spread_{suffix}"

    await db._execute(f"CREATE TABLE {btc_table} (timestamp UInt64, close Float64) ENGINE=MergeTree ORDER BY timestamp")
    await db._execute(f"CREATE TABLE {eth_table} (timestamp UInt64, close Float64) ENGINE=MergeTree ORDER BY timestamp")

    class BtcEthSpread(Aggregation):
        OUTPUT_TABLE = output_table
        btc = SourceRef(btc_table, field="close")
        eth = SourceRef(eth_table, field="close")

        def combine_sql(self) -> str:
            return "btc - eth"

    agg = BtcEthSpread()
    await setup_aggregation(db, agg)

    # BTC arrives first -- must not surface as a (wrong) complete row.
    await db._execute(f"INSERT INTO {btc_table} VALUES (1000, 50000.0)")
    rows = await query_aggregation(db, agg, 0, 9999)
    assert rows == []

    # ETH arrives late (simulates a backfilled gap) -- now it must complete correctly.
    await db._execute(f"INSERT INTO {eth_table} VALUES (1000, 3000.0)")
    rows = await query_aggregation(db, agg, 0, 9999)
    assert rows == [{"timestamp": 1000, "value": 47000.0}]


async def test_cross_source_aggregation_with_int64_timestamp_source(db):
    """Regression: ensure_cross_source_mv/backfill_cross_source hardcoded UInt64 for the
    timestamp baked into the AggregateFunction(argMax, ..., UInt64) state, but a real
    DataSource's schema (e.g. WhiteBitDataSource, and ensure_raw_table's own POLARS_TO_CH
    mapping for pl.Int64) produces an Int64 timestamp column, not UInt64 -- found via real
    dogfooding (the example repo's aggregation demo), not anticipated up front. ClickHouse
    refuses to write an Int64-derived state into a UInt64-declared state column at all
    (CANNOT_CONVERT_TYPE) rather than silently coercing it, so this was a hard failure, not
    a subtly wrong result -- but only for source tables shaped like real ones, which every
    earlier test in this file (deliberately UInt64) didn't exercise."""
    import uuid

    from tradingkit.aggregation import Aggregation, SourceRef, query_aggregation, setup_aggregation

    suffix = uuid.uuid4().hex[:8]
    btc_table = f"test_btc_i64_{suffix}"
    eth_table = f"test_eth_i64_{suffix}"
    output_table = f"test_spread_i64_{suffix}"

    await db._execute(f"CREATE TABLE {btc_table} (timestamp Int64, close Float64) ENGINE=MergeTree ORDER BY timestamp")
    await db._execute(f"CREATE TABLE {eth_table} (timestamp Int64, close Float64) ENGINE=MergeTree ORDER BY timestamp")
    await db._execute(f"INSERT INTO {btc_table} VALUES (1000, 50000.0)")
    await db._execute(f"INSERT INTO {eth_table} VALUES (1000, 3000.0)")

    class BtcEthSpread(Aggregation):
        OUTPUT_TABLE = output_table
        btc = SourceRef(btc_table, field="close")
        eth = SourceRef(eth_table, field="close")

        def combine_sql(self) -> str:
            return "btc - eth"

    agg = BtcEthSpread()
    await setup_aggregation(db, agg)  # must not raise CANNOT_CONVERT_TYPE

    rows = await query_aggregation(db, agg, 0, 9999)
    assert rows == [{"timestamp": 1000, "value": 47000.0}]


async def test_cross_source_aggregation_combine_python_fallback(db):
    """combine() (Python fallback) driven end-to-end through AggregationWorker._run_one(),
    the same code path a real background worker uses -- not just calling combine() directly."""
    import uuid

    from tradingkit.aggregation import Aggregation, AggregationWorker

    suffix = uuid.uuid4().hex[:8]
    output_table = f"test_py_agg_{suffix}"

    class FixedValue(Aggregation):
        OUTPUT_TABLE = output_table

        async def combine(self, ctx, start_ts, end_ts):
            return [{"timestamp": start_ts, "value": 42.0}]

    worker = AggregationWorker(db)
    agg = FixedValue()
    await worker._run_one({"namespace": "default", "name": "fixed", "exchange": "test_ex", "agg": agg})

    result = await db.fetch_from_unit_table(output_table, exchange="test_ex")
    assert result["value"].to_list() == [42.0]


async def test_script_aggregation_equivalent_to_class(db):
    """The dynamic path (ScriptAggregation, what a SaaS UI editor would create) must behave
    identically to hand-writing the same thing as an Aggregation subclass -- not just in
    theory (same combine_sql() interface) but against a real server end to end. Data is
    inserted BEFORE setup_aggregation() runs, deliberately -- this doubles as the
    regression test for backfill_cross_source(): Materialized Views aren't retroactive,
    so without a backfill step this would return [] despite both sources having data."""
    import uuid

    from tradingkit.aggregation import load_aggregation_plugin, query_aggregation, setup_aggregation

    suffix = uuid.uuid4().hex[:8]
    btc_table = f"test_btc_{suffix}"
    eth_table = f"test_eth_{suffix}"
    output_table = f"test_spread_{suffix}"

    await db._execute(f"CREATE TABLE {btc_table} (timestamp UInt64, close Float64) ENGINE=MergeTree ORDER BY timestamp")
    await db._execute(f"CREATE TABLE {eth_table} (timestamp UInt64, close Float64) ENGINE=MergeTree ORDER BY timestamp")
    await db._execute(f"INSERT INTO {btc_table} VALUES (1000, 50000.0)")
    await db._execute(f"INSERT INTO {eth_table} VALUES (1000, 3000.0)")

    code = f'''
OUTPUT_TABLE = "{output_table}"
SOURCES = {{
    "btc": SourceRef("{btc_table}", field="close"),
    "eth": SourceRef("{eth_table}", field="close"),
}}
COMBINE_SQL = "btc - eth"
'''
    agg = load_aggregation_plugin("__script__", {"_code": code})
    await setup_aggregation(db, agg)

    rows = await query_aggregation(db, agg, 0, 9999)
    assert rows == [{"timestamp": 1000, "value": 47000.0}]


async def test_connections_crud_over_authenticated_http(db):
    """Exercises ensure_connections_table/upsert/list/delete over the HTTP path with the
    Basic Auth header this session added -- proves it actually authenticates against a
    real server, not just that some header gets attached to the request."""
    await db.ensure_connections_table()
    name = f"test_conn_{uuid.uuid4().hex[:8]}"
    await db.upsert_connection({
        "name": name, "source": "test.py", "symbol": "BTC_USDT", "timeframe": "1m",
    })
    try:
        conns = await db.list_connections()
        assert any(c["name"] == name for c in conns)
    finally:
        await db.delete_connection(name)
        conns_after = await db.list_connections()
        assert not any(c["name"] == name for c in conns_after)


async def test_identifier_validation_blocks_injection_against_real_server(db):
    """The Python-level ValueError must fire before any SQL reaches the server -- confirm
    the malicious identifier never even gets a chance to be (mis)interpreted by ClickHouse."""
    with pytest.raises(ValueError):
        await db.ensure_raw_table("t); DROP TABLE connections; --", {"timestamp": "Int64"})
    # connections table (created by other tests in this session) must be unaffected
    conns = await db.list_connections()
    assert conns is not None  # didn't raise -- table still exists