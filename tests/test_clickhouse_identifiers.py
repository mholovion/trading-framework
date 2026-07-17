"""tradingkit.core.clickhouse — identifier validation (SQL injection guard)."""
from __future__ import annotations

import pytest

from tradingkit.core.clickhouse import ClickHouseManager, _validate_identifier
from tradingkit.schema import Fold


@pytest.mark.parametrize("name", ["candles", "candles_3600s", "_private", "spread_btc_eth"])
def test_valid_identifiers_pass(name):
    assert _validate_identifier(name) == name


@pytest.mark.parametrize("name", [
    "candles; DROP TABLE users; --",
    "candles) UNION SELECT password FROM users --",
    "candles/*comment*/",
    "123leading_digit",
    "has space",
    "",
    None,
    123,
])
def test_invalid_identifiers_rejected(name):
    with pytest.raises(ValueError):
        _validate_identifier(name)


async def test_decorator_validates_positional_keyword_and_default_args():
    db = ClickHouseManager()  # self._conn stays None -- methods short-circuit before any I/O

    await db.fetch_data("wb", "BTC_USDT", "1m")  # default table_name
    await db.fetch_data("wb", "BTC_USDT", "1m", table_name="candles")  # keyword
    await db.get_distinct_symbols("candles")  # positional

    with pytest.raises(ValueError):
        await db.fetch_data("wb", "BTC_USDT", "1m", table_name="candles; DROP TABLE x; --")
    with pytest.raises(ValueError):
        await db.get_distinct_symbols("candles) UNION SELECT 1 --")


async def test_ensure_raw_table_validates_table_and_column_names(fake_ch_http):
    db = ClickHouseManager()
    await db.ensure_raw_table("spread_btc_eth", {"timestamp": None, "spread": None})

    with pytest.raises(ValueError):
        await db.ensure_raw_table("candles); DROP TABLE x; --", {"timestamp": None})
    with pytest.raises(ValueError):
        await db.ensure_raw_table("candles", {"value); DROP TABLE x; --": None})


async def test_fold_based_methods_validate_alias_and_bucket_s(fake_ch_http):
    db = ClickHouseManager()
    good_fold = Fold("sum", "volume", "Float64").alias("total_volume")

    # these two call _execute() unconditionally (no `if not self._conn` guard), so they
    # need the fake HTTP transport even though we're only asserting on validation here
    await db.ensure_agg_table("candles", 3600, [good_fold])
    await db.query_agg("candles", 3600, [good_fold], "wb", "BTC_USDT")

    with pytest.raises(ValueError):
        await db.ensure_agg_table("candles); DROP TABLE x; --", 3600, [good_fold])
    with pytest.raises((ValueError, TypeError)):
        await db.ensure_agg_table("candles", "3600); DROP TABLE x; --", [good_fold])


async def test_bulk_insert_validates_table_and_columns():
    db = ClickHouseManager()
    # self._conn is None -> _bulk_insert no-ops after validation, must not raise for good input
    await db._bulk_insert("indicators", ["indicator_type", "value"], [("rsi", 1.0)])

    with pytest.raises(ValueError):
        await db._bulk_insert("indicators; DROP TABLE x; --", ["value"], [(1.0,)])
    with pytest.raises(ValueError):
        await db._bulk_insert("indicators", ["value); DROP TABLE x; --"], [(1.0,)])