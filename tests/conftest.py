from __future__ import annotations

from unittest.mock import patch

import numpy as np
import polars as pl
import pytest


class FakeChResponse:
    """Stands in for the aiohttp response object _execute() reads from."""

    def __init__(self, status: int = 200, json_data: dict | None = None, text_data: str = ""):
        self.status = status
        self._json_data = json_data if json_data is not None else {"data": []}
        self._text_data = text_data

    async def json(self, content_type=None):
        return self._json_data

    async def text(self):
        return self._text_data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeChSession:
    """Stands in for aiohttp.ClientSession — records every POST, replays canned responses."""

    def __init__(self, responses: list[FakeChResponse] | None = None):
        self.calls: list[dict] = []
        self._responses = list(responses or [])

    def post(self, url, data=None, headers=None, **kwargs):
        self.calls.append({
            "url": url,
            "body": data.decode() if isinstance(data, bytes) else data,
            "headers": headers or {},
        })
        return self._responses.pop(0) if self._responses else FakeChResponse()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@pytest.fixture
def fake_ch_http():
    """
    Patches aiohttp.ClientSession so ClickHouseManager._execute() never touches the
    network. Yields the FakeChSession instance so tests can inspect .calls (recorded
    SQL bodies) and pre-seed .​_responses with FakeChResponse for SELECT queries.
    """
    session = FakeChSession()
    with patch("aiohttp.ClientSession", return_value=session):
        yield session


@pytest.fixture
def ohlcv_df() -> pl.DataFrame:
    """Small deterministic OHLCV frame, enough bars for a 14-period RSI to warm up."""
    n = 60
    rng = np.random.default_rng(42)
    close = 100 + np.cumsum(rng.normal(0, 1, n))
    return pl.DataFrame({
        "timestamp": np.arange(n) * 60,
        "open":      close,
        "high":      close + 1,
        "low":       close - 1,
        "close":     close,
        "volume":    np.ones(n),
    })