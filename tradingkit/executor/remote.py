"""
tradingkit.executor.remote — RemoteExecutor.

Delegates all computation to a tradingkit-runner HTTP server.
Run the server anywhere: locally, in Docker, Kubernetes, or a private network reachable
by the runner's --token holder — see the top-level README's "Security model" section
before binding the runner to anything other than 127.0.0.1.

    tradingkit-runner --token "$(openssl rand -hex 32)"

    executor = RemoteExecutor("http://localhost:8082", token="...")

Responses come from a tradingkit-runner process you configured and authenticated to, so
they're unpickled with plain pickle.loads() here — the restricted unpickler lives on the
server side, where *inbound* (potentially attacker-reachable) payloads are decoded.
"""
from __future__ import annotations

import logging
import pickle
from typing import TYPE_CHECKING, Any

import polars as pl
import pyarrow as pa

from tradingkit.executor.base import PluginExecutor

if TYPE_CHECKING:
    from tradingkit.indicator import Indicator, IndicatorContext
    from tradingkit.source import DataSource
    from tradingkit.strategy import BarContext, Signal, Strategy

logger = logging.getLogger(__name__)


def _df_to_arrow_bytes(df: pl.DataFrame) -> bytes:
    arrow_table = df.to_arrow()
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, arrow_table.schema)
    writer.write_table(arrow_table)
    writer.close()
    return sink.getvalue().to_pybytes()


def _arrow_bytes_to_df(data: bytes) -> pl.DataFrame:
    buf = pa.py_buffer(data)
    reader = pa.ipc.open_stream(buf)
    return pl.from_arrow(reader.read_all())


class RemoteExecutor(PluginExecutor):
    """
    Proxies plugin execution to a remote tradingkit-runner HTTP server.

    Indicators, strategies, and sources are serialized (pickle) and sent
    together with Arrow-encoded data. The runner executes them and returns
    Arrow-encoded results.

    Usage:
        executor = RemoteExecutor("http://tradingkit-runner:8082", token="...")
        result = await executor.compute_indicator(indicator, ctx)

    token must match the runner's --token / TRADINGKIT_RUNNER_TOKEN. Omit it only when
    the runner is bound to 127.0.0.1 without a token configured.
    """

    def __init__(self, url: str, timeout: int = 120, token: str | None = None) -> None:
        self._url = url.rstrip("/")
        self._timeout = timeout
        self._token = token
        self._session: Any = None

    async def start(self) -> None:
        import aiohttp
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self._timeout)
        )

    async def stop(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def _post(self, path: str, data: bytes) -> bytes:
        import aiohttp
        if self._session is None:
            # Auto-start for convenience (no explicit start() call) — kept on
            # self._session so it's reused across calls and closed by stop().
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._timeout)
            )
        session = self._session
        headers = {"Content-Type": "application/octet-stream"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        async with session.post(
            f"{self._url}{path}",
            data=data,
            headers=headers,
        ) as resp:
            if resp.status == 401:
                raise PermissionError(
                    f"RemoteExecutor: {self._url}{path} rejected the request (401) — "
                    "token missing or doesn't match the runner's --token/"
                    "TRADINGKIT_RUNNER_TOKEN."
                )
            resp.raise_for_status()
            return await resp.read()

    async def compute_indicator(
        self,
        indicator: Indicator,
        ctx: IndicatorContext,
    ) -> pl.Series:
        import numpy as np
        payload = {
            "indicator": pickle.dumps(indicator),
            "data":      _df_to_arrow_bytes(ctx.df),
        }
        raw = await self._post("/compute/indicator", pickle.dumps(payload))
        result = pickle.loads(raw)
        if "error" in result:
            raise RuntimeError(f"RemoteExecutor indicator error: {result['error']}")
        arr = np.frombuffer(result["values"], dtype=np.float64)
        return pl.Series("value", arr, dtype=pl.Float64)

    async def process_strategy_bar(
        self,
        strategy: Strategy,
        bar: BarContext,
    ) -> Signal | None:
        payload = {
            "strategy": pickle.dumps(strategy),
            "bar":      pickle.dumps(bar),
        }
        raw = await self._post("/compute/strategy", pickle.dumps(payload))
        result = pickle.loads(raw)
        if "error" in result:
            raise RuntimeError(f"RemoteExecutor strategy error: {result['error']}")
        if result.get("signal") is None:
            return None
        from tradingkit.strategy import Signal
        return Signal.from_dict(result["signal"])

    async def fetch_source_data(
        self,
        source: DataSource,
        symbol: str,
        timeframe_seconds: int,
        start_ts: int,
        end_ts: int,
        limit: int = 5000,
    ) -> pl.DataFrame:
        payload = {
            "source":            pickle.dumps(source),
            "symbol":            symbol,
            "timeframe_seconds": timeframe_seconds,
            "start_ts":          start_ts,
            "end_ts":            end_ts,
            "limit":             limit,
        }
        raw = await self._post("/compute/source", pickle.dumps(payload))
        result = pickle.loads(raw)
        if "error" in result:
            raise RuntimeError(f"RemoteExecutor source error: {result['error']}")
        return _arrow_bytes_to_df(result["data"])
