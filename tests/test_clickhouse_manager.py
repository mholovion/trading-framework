"""tradingkit.core.clickhouse.ClickHouseManager — SQL construction, no real ClickHouse needed."""
from __future__ import annotations

from tradingkit.core.clickhouse import ClickHouseManager, _basic_auth_header, create_clickhouse_manager
from tests.conftest import FakeChResponse


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
