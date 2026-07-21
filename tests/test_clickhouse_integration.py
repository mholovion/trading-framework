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