"""
tradingkit.executor.subprocess_ — SubprocessExecutor.

Runs plugins in isolated subprocesses:
  - Data transfer via Apache Arrow IPC (zero-copy shared memory where possible)
  - Memory limit enforced via resource module
  - Import restrictions via RestrictedPython (for ScriptIndicator/ScriptStrategy)
  - Timeout per computation
"""
from __future__ import annotations

import asyncio
import logging
import os
import pickle
import struct
import sys
from typing import Optional, TYPE_CHECKING

import numpy as np
import polars as pl
import pyarrow as pa

from tradingkit.executor.base import PluginExecutor

if TYPE_CHECKING:
    from tradingkit.indicator import Indicator, IndicatorContext
    from tradingkit.strategy import Strategy, BarContext, Signal
    from tradingkit.source import DataSource

logger = logging.getLogger(__name__)

_WORKER_SCRIPT = os.path.join(os.path.dirname(__file__), "_worker.py")


def _df_to_arrow_bytes(df: pl.DataFrame) -> bytes:
    """Serialize Polars DataFrame → Arrow IPC bytes."""
    arrow_table = df.to_arrow()
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, arrow_table.schema)
    writer.write_table(arrow_table)
    writer.close()
    return sink.getvalue().to_pybytes()


def _arrow_bytes_to_df(data: bytes) -> pl.DataFrame:
    """Deserialize Arrow IPC bytes → Polars DataFrame."""
    buf = pa.py_buffer(data)
    reader = pa.ipc.open_stream(buf)
    return pl.from_arrow(reader.read_all())


class SubprocessExecutor(PluginExecutor):
    """
    Executes plugins in isolated subprocesses with Arrow IPC data transfer.

    Suitable for user-uploaded plugins and UI editor code (ScriptIndicator,
    ScriptStrategy, ScriptSource) where sandboxing is required.

    Usage:
        executor = SubprocessExecutor(max_memory_mb=512, timeout_s=60)
        await executor.start()
        result = await executor.compute_indicator(indicator, ctx)
        await executor.stop()

    Or as async context manager:
        async with SubprocessExecutor() as executor:
            result = await executor.compute_indicator(indicator, ctx)
    """

    def __init__(
        self,
        max_memory_mb: int = 512,
        timeout_s: int = 60,
        max_workers: int = 4,
        cpp_pool=None,
    ) -> None:
        self._max_memory_mb = max_memory_mb
        self._timeout_s = timeout_s
        self._max_workers = max_workers
        self._semaphore: asyncio.Semaphore | None = None
        self.cpp_pool = cpp_pool

    async def start(self) -> None:
        self._semaphore = asyncio.Semaphore(self._max_workers)
        if self.cpp_pool is not None:
            await self.cpp_pool.start()

    async def stop(self) -> None:
        if self.cpp_pool is not None:
            await self.cpp_pool.stop()

    async def compute_indicator(
        self,
        indicator: "Indicator",
        ctx: "IndicatorContext",
    ) -> pl.Series:
        from tradingkit.indicator import CppIndicator
        if isinstance(indicator, CppIndicator):
            if self.cpp_pool is None:
                raise RuntimeError(
                    "CppIndicator requires CppRunnerPool — pass cpp_pool= to SubprocessExecutor"
                )
            arr = await self.cpp_pool.run(indicator._so_bytes, ctx.df, indicator.params)
            return pl.Series("value", arr, dtype=pl.Float64)

        payload = {
            "task": "indicator",
            "indicator": pickle.dumps(indicator),
            "data": _df_to_arrow_bytes(ctx.df),
        }
        result = await self._run_in_subprocess(payload)
        if "error" in result:
            raise RuntimeError(f"Subprocess indicator error: {result['error']}")
        arr = np.frombuffer(result["values"], dtype=np.float64)
        return pl.Series("value", arr, dtype=pl.Float64)

    async def process_strategy_bar(
        self,
        strategy: "Strategy",
        bar: "BarContext",
    ) -> Optional["Signal"]:
        payload = {
            "task": "strategy",
            "strategy": pickle.dumps(strategy),
            "bar": pickle.dumps(bar),
        }
        result = await self._run_in_subprocess(payload)
        if "error" in result:
            raise RuntimeError(f"Subprocess strategy error: {result['error']}")
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
            "task": "source",
            "source": pickle.dumps(source),
            "symbol": symbol,
            "timeframe_seconds": timeframe_seconds,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": limit,
        }
        result = await self._run_in_subprocess(payload)
        if "error" in result:
            raise RuntimeError(f"Subprocess source error: {result['error']}")
        return _arrow_bytes_to_df(result["data"])

    async def _run_in_subprocess(self, payload: dict) -> dict:
        sem = self._semaphore or asyncio.Semaphore(self._max_workers)
        async with sem:
            return await asyncio.wait_for(
                self._spawn(payload),
                timeout=self._timeout_s,
            )

    async def _spawn(self, payload: dict) -> dict:
        raw = pickle.dumps(payload)
        header = struct.pack(">I", len(raw))

        proc = await asyncio.create_subprocess_exec(
            sys.executable, _WORKER_SCRIPT,
            f"--memory-mb={self._max_memory_mb}",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate(header + raw)
        if proc.returncode != 0:
            err = stderr.decode(errors="replace")
            raise RuntimeError(f"Subprocess worker failed (exit {proc.returncode}): {err}")

        if len(stdout) < 4:
            raise RuntimeError("Subprocess returned no data")
        resp_len = struct.unpack(">I", stdout[:4])[0]
        return pickle.loads(stdout[4:4 + resp_len])
