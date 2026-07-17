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
    result = await src.historical("BTC_USDT", "1h", 0, 100)
    assert result == [{"symbol": "BTC_USDT", "batch_size": 1440}]


async def test_connection_script_source_missing_historical_raises():
    src = ConnectionScriptSource("TABLE_NAME = 'x'")
    with pytest.raises(NotImplementedError):
        await src.historical("BTC_USDT", "1h", 0, 100)


async def test_connection_script_source_streams_realtime():
    src = ConnectionScriptSource(FULL_SOURCE_SCRIPT)
    rows = [row async for row in src.stream("BTC_USDT", "1h")]
    assert rows == [{"symbol": "BTC_USDT"}]


def test_params_cache_memoizes():
    src = ConnectionScriptSource(FULL_SOURCE_SCRIPT)
    first = src.extract_params()
    second = src.extract_params()
    assert first is second