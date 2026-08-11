"""tradingkit.core.clickhouse.ClickHouseManager — SQL construction, no real ClickHouse needed."""
from __future__ import annotations

import json

import polars as pl

from tests.conftest import FakeChResponse
from tradingkit.core.clickhouse import (
    ClickHouseManager,
    _basic_auth_header,
    create_clickhouse_manager,
)
from tradingkit.core.clickhouse._indicators_signals import _IndicatorsSignalsMixin


def _sql(fake_ch_http, index: int = -1) -> str:
    return fake_ch_http.calls[index]["body"]


async def test_fetch_data_builds_select_with_filters(fake_ch_http):
    fake_ch_http._responses.append(FakeChResponse(json_data={"data": []}))
    db = ClickHouseManager()
    db._conn = object()  # truthy sentinel -- fetch_data only checks `if not self._conn`

    await db.fetch_data("whitebit", "BTC_USDT", "1h", start_ts=100, end_ts=200, table_name="candles")
    sql = _sql(fake_ch_http)
    assert "FROM candles" in sql
    assert "exchange = 'whitebit'" in sql
    assert "symbol = 'BTC_USDT'" in sql
    assert "timestamp >= 100" in sql
    assert "timestamp <= 200" in sql


async def test_fetch_data_parses_rows_into_dicts(fake_ch_http):
    fake_ch_http._responses.append(FakeChResponse(json_data={
        "data": [[1000, 1.0, 2.0, 0.5, 1.5, 10.0]],
    }))
    db = ClickHouseManager()
    db._conn = object()

    rows = await db.fetch_data("whitebit", "BTC_USDT", "1h")
    assert rows == [{
        "timestamp": 1000, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0,
    }]


async def test_fetch_data_short_circuits_without_connection(fake_ch_http):
    db = ClickHouseManager()  # self._conn is None
    rows = await db.fetch_data("whitebit", "BTC_USDT", "1h")
    assert rows == []
    assert fake_ch_http.calls == [], "should never hit the network when disconnected"


async def test_fmt_escapes_string_values():
    assert ClickHouseManager._fmt("O'Brien") == "'O\\'Brien'"
    assert ClickHouseManager._fmt(None) == "NULL"
    assert ClickHouseManager._fmt(True) == "1"
    assert ClickHouseManager._fmt(False) == "0"
    assert ClickHouseManager._fmt(42) == "42"
    assert ClickHouseManager._fmt(3.14) == "3.14"


async def test_interpolate_substitutes_named_params():
    sql = ClickHouseManager._interpolate(
        "SELECT * FROM t WHERE a = %(a)s AND b = %(b)s",
        {"a": "it's", "b": 5},
    )
    assert sql == "SELECT * FROM t WHERE a = 'it\\'s' AND b = 5"


async def test_execute_appends_format_only_for_select(fake_ch_http):
    fake_ch_http._responses.extend([
        FakeChResponse(json_data={"data": []}),
        FakeChResponse(json_data={"data": []}),
    ])
    db = ClickHouseManager()
    await db._execute("SELECT 1")
    await db._execute("INSERT INTO t VALUES (1)")
    assert "FORMAT JSONCompact" in fake_ch_http.calls[0]["body"]
    assert "FORMAT JSONCompact" not in fake_ch_http.calls[1]["body"]


async def test_execute_appends_format_for_describe_and_show(fake_ch_http):
    """Regression: DESCRIBE/SHOW are read queries too, just like SELECT -- they used to
    fall through to the non-SELECT branch, so FORMAT JSONCompact was never appended and
    the response body was never parsed, silently returning [] every time. That made
    get_table_schema() always return {} against a real server (caught only by the real
    ClickHouse integration tests, never by mocks -- see test_get_table_schema_below)."""
    fake_ch_http._responses.extend([
        FakeChResponse(json_data={"data": []}),
        FakeChResponse(json_data={"data": []}),
    ])
    db = ClickHouseManager()
    await db._execute("DESCRIBE TABLE t")
    await db._execute("SHOW TABLES")
    assert "FORMAT JSONCompact" in fake_ch_http.calls[0]["body"]
    assert "FORMAT JSONCompact" in fake_ch_http.calls[1]["body"]


async def test_get_table_schema_parses_describe_output(fake_ch_http):
    fake_ch_http._responses.append(FakeChResponse(json_data={
        "data": [["timestamp", "Int64"], ["value", "Float64"]],
    }))
    db = ClickHouseManager()
    schema = await db.get_table_schema("t")
    assert schema == {"timestamp": pl.Int64, "value": pl.Float64}


async def test_execute_returns_empty_list_on_http_error(fake_ch_http):
    fake_ch_http._responses.append(FakeChResponse(status=500, text_data="boom"))
    db = ClickHouseManager()
    rows = await db._execute("SELECT 1")
    assert rows == []


async def test_execute_sends_basic_auth_header(fake_ch_http):
    """Regression: HTTP requests used to carry no credentials at all, even though
    user/password are accepted by __init__ and used on the native (bulk-insert) path."""
    fake_ch_http._responses.append(FakeChResponse(json_data={"data": []}))
    db = ClickHouseManager(user="bot", password="s3cret")
    await db._execute("SELECT 1")
    auth_header = fake_ch_http.calls[0]["headers"]["Authorization"]
    assert auth_header == _basic_auth_header("bot", "s3cret")


async def test_execute_uses_configurable_http_port(fake_ch_http):
    fake_ch_http._responses.append(FakeChResponse(json_data={"data": []}))
    db = ClickHouseManager(host="ch.internal", http_port=8443)
    await db._execute("SELECT 1")
    assert fake_ch_http.calls[0]["url"].startswith("http://ch.internal:8443/")


async def test_ensure_connections_table_sends_auth_and_configured_port(fake_ch_http):
    fake_ch_http._responses.extend([FakeChResponse(), FakeChResponse()])
    db = ClickHouseManager(user="bot", password="s3cret", http_port=8443)
    await db.ensure_connections_table()
    for call in fake_ch_http.calls:
        assert call["url"].startswith("http://localhost:8443/")
        assert call["headers"]["Authorization"] == _basic_auth_header("bot", "s3cret")


async def test_delete_connection_forces_synchronous_mutation(fake_ch_http):
    """Regression: ALTER ... DELETE is an async mutation by default -- a list_connections()
    call right after delete_connection() could still see the "deleted" row until the
    mutation gets applied to the underlying parts, unless mutations_sync is forced."""
    fake_ch_http._responses.append(FakeChResponse())
    db = ClickHouseManager()
    db._conn = object()
    await db.delete_connection("foo")
    assert "mutations_sync=1" in fake_ch_http.calls[0]["url"]


def test_basic_auth_header_matches_http_spec():
    import base64
    header = _basic_auth_header("bot", "s3cret")
    assert header.startswith("Basic ")
    decoded = base64.b64decode(header.removeprefix("Basic ")).decode()
    assert decoded == "bot:s3cret"


def test_create_clickhouse_manager_http_port(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_HTTP_PORT", "8443")
    db = create_clickhouse_manager()
    assert db.http_port == 8443


def test_create_clickhouse_manager_http_port_defaults(monkeypatch):
    monkeypatch.delenv("CLICKHOUSE_HTTP_PORT", raising=False)
    db = create_clickhouse_manager()
    assert db.http_port == 8123


def test_create_clickhouse_manager_reads_env(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_HOST", "ch.internal")
    monkeypatch.setenv("CLICKHOUSE_PORT", "9440")
    monkeypatch.setenv("CLICKHOUSE_DATABASE", "trading")
    monkeypatch.setenv("CLICKHOUSE_USER", "bot")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "secret")
    db = create_clickhouse_manager()
    assert db.host == "ch.internal"
    assert db.port == 9440
    assert db.database == "trading"
    assert db.user == "bot"
    assert db.password == "secret"


def test_create_clickhouse_manager_defaults(monkeypatch):
    for var in ("CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_DATABASE",
                "CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    db = create_clickhouse_manager()
    assert db.host == "localhost"
    assert db.port == 9000
    assert db.database == "default"


# ---------------------------------------------------------------------------
# store_signal / fetch_signals with free-record signals (TASK-024)
# ---------------------------------------------------------------------------

class _SignalDb(_IndicatorsSignalsMixin):
    """Exercises the buffer/serialisation without a ClickHouse connection."""

    def __init__(self):
        self._signal_buffer: list = []
        self._conn = True
        self.rows: list = []

    async def _ensure_params_registered(self, *a, **kw):
        pass

    async def _execute(self, sql, params=None, settings=None):
        return self.rows


class _Params:
    type = "s"

    def to_hash(self):
        return "h"


async def test_store_signal_keeps_unknown_fields_in_metadata():
    """The table has fixed columns, but a signal has no mandatory fields -- anything
    without a column of its own is parked in metadata rather than dropped."""
    db = _SignalDb()
    await db.store_signal(_Params(), "whitebit", "BTC", timestamp=1,
                          record={"action": "short", "price": 74.5, "zscore": 4.2})

    _, _, _, _, ts, signal_type, confidence, price, metadata = db._signal_buffer[0]
    assert (ts, price) == (1, 74.5)
    assert signal_type == "" and confidence == 0.0     # absent, defaulted, not invented
    assert json.loads(metadata) == {"action": "short", "zscore": 4.2}


async def test_store_signal_does_not_crash_without_signal_type():
    """The exact shape that raised KeyError on installed 0.4.0."""
    db = _SignalDb()
    await db.store_signal(_Params(), "wb", "BTC", timestamp=1,
                          record={"action": "short", "price": 74.5})
    assert len(db._signal_buffer) == 1


async def test_signal_roundtrip_is_lossless():
    """The whole reason metadata is used as the overflow: what goes in must come back.
    A field that survives the write but not the read is the bug this replaces."""
    db = _SignalDb()
    await db.store_signal(_Params(), "wb", "BTC", timestamp=1,
                          record={"action": "short", "price": 74.5, "zscore": 4.2})
    row = db._signal_buffer[0]
    db.rows = [(row[4], row[5], row[6], row[7], row[8])]

    got = (await db.fetch_signals(_Params(), "wb", "BTC"))[0]
    assert got["action"] == "short"
    assert got["zscore"] == 4.2
    assert got["price"] == 74.5


async def test_store_signal_still_accepts_the_old_keyword_form():
    db = _SignalDb()
    await db.store_signal(_Params(), "wb", "BTC", timestamp=1,
                          signal_type="buy", confidence=0.8, price=10.0,
                          metadata={"rsi": 20})
    _, _, _, _, _, signal_type, confidence, price, metadata = db._signal_buffer[0]
    assert (signal_type, confidence, price) == ("BUY", 0.8, 10.0)
    assert json.loads(metadata) == {"rsi": 20}
