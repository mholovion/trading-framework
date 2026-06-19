"""
tradingkit.executor.remote — RemoteExecutor.

Delegates all computation to a tradingkit-runner HTTP server.
Run the server anywhere: locally, in Docker, Kubernetes, or the cloud.

    tradingkit-runner --host 0.0.0.0 --port 8082

    executor = RemoteExecutor("http://localhost:8082")
"""
from __future__ import annotations

import logging
import pickle
from typing import Any, Optional, TYPE_CHECKING

import polars as pl
import pyarrow as pa

from tradingkit.executor.base import PluginExecutor

if TYPE_CHECKING:
    from tradingkit.indicator import Indicator, IndicatorContext
    from tradingkit.strategy import Strategy, BarContext, Signal
    from tradingkit.source import DataSource

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
        executor = RemoteExecutor("http://tradingkit-runner:8082")
        result = await executor.compute_indicator(indicator, ctx)
    """

    def __init__(self, url: str, timeout: int = 120) -> None:
        self._url = url.rstrip("/")
        self._timeout = timeout
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
        session = self._session
        if session is None:
            # Auto-start for convenience (no explicit start() call)
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._timeout)
            )
        async with session.post(
            f"{self._url}{path}",
            data=data,
            headers={"Content-Type": "application/octet-stream"},
        ) as resp:
            resp.raise_for_status()
            return await resp.read()

    async def compute_indicator(
        self,
        indicator: "Indicator",
        ctx: "IndicatorContext",
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
        strategy: "Strategy",
        bar: "BarContext",
    ) -> Optional["Signal"]:
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
        source: "DataSource",
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
