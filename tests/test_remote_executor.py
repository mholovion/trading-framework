"""tradingkit.executor.remote — RemoteExecutor token handling and session lifecycle."""
from __future__ import annotations

import polars as pl
import pytest
from aiohttp.test_utils import TestServer

from tradingkit.executor.remote import RemoteExecutor
from tradingkit.indicator import IndicatorContext, ScriptIndicator
from tradingkit.runner.server import create_app

TOKEN = "remote-exec-token"


@pytest.fixture
async def server():
    app = create_app(token=TOKEN)
    srv = TestServer(app)
    await srv.start_server()
    yield str(srv.make_url(""))
    await srv.close()


async def test_missing_token_raises_permission_error(server):
    executor = RemoteExecutor(server, timeout=10)
    df = pl.DataFrame({"timestamp": [1], "close": [1.0]})
    ind = ScriptIndicator(code="result = close", period=1)
    try:
        with pytest.raises(PermissionError):
            await executor.compute_indicator(ind, IndicatorContext(df))
    finally:
        await executor.stop()


async def test_correct_token_computes_successfully(server):
    executor = RemoteExecutor(server, timeout=10, token=TOKEN)
    df = pl.DataFrame({"timestamp": [1, 2], "close": [1.0, 2.0]})
    ind = ScriptIndicator(code="result = close * 5", period=1)
    try:
        series = await executor.compute_indicator(ind, IndicatorContext(df))
        assert list(series) == [5.0, 10.0]
    finally:
        await executor.stop()


async def test_post_reuses_and_closes_session(server):
    """_post()'s auto-start path must store the session so stop() can actually close it."""
    executor = RemoteExecutor(server, timeout=10, token=TOKEN)
    df = pl.DataFrame({"timestamp": [1], "close": [1.0]})
    ind = ScriptIndicator(code="result = close", period=1)
    assert executor._session is None
    await executor.compute_indicator(ind, IndicatorContext(df))
    session_after_first_call = executor._session
    assert session_after_first_call is not None
    await executor.compute_indicator(ind, IndicatorContext(df))
    assert executor._session is session_after_first_call, "should reuse the same session, not open a new one"
    await executor.stop()
    assert executor._session is None
    assert session_after_first_call.closed