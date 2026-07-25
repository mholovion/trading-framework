"""
tradingkit.executor.cpp_pool — CppRunnerPool.

Manages a pool of persistent C++ runner workers and routes CppIndicator
execution through them.  The actual worker lifecycle (how runners are started
and stopped) is delegated to a RunnerLauncher.

Quick start (auto-detect Docker or fall back to subprocess):
    pool = CppRunnerPool()
    await pool.start()

Explicit launcher:
    from tradingkit.executor.launchers import DockerRunnerLauncher
    pool = CppRunnerPool(launcher=DockerRunnerLauncher())

    from tradingkit.executor.launchers import SubprocessRunnerLauncher
    pool = CppRunnerPool(launcher=SubprocessRunnerLauncher())  # dev

Protocol (same for both launchers, over Unix socket):
    Request:  length-prefixed pickle {"so": bytes, "data": arrow_ipc, "params": json}
    Response: length-prefixed pickle {"values": float64_bytes} | {"error": str}
"""
from __future__ import annotations

import asyncio
import json
import logging
import pickle
import struct
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import polars as pl

    from tradingkit.executor.launchers.base import RunnerLauncher

logger = logging.getLogger(__name__)


class CppRunnerPool:
    """
    Pool of persistent C++ runner workers, connected via Unix sockets.

    Args:
        launcher:  RunnerLauncher instance, or None to auto-detect
                   (DockerRunnerLauncher if Docker available, else SubprocessRunnerLauncher).
        pool_size: Number of concurrent runner workers.
    """

    def __init__(
        self,
        launcher: RunnerLauncher | None = None,
        pool_size: int = 2,
    ) -> None:
        self._launcher_arg = launcher
        self._pool_size    = pool_size
        self._launcher: RunnerLauncher | None = None
        self._pool: asyncio.Queue = asyncio.Queue()

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        from tradingkit.executor.launchers import auto_launcher

        self._launcher = self._launcher_arg or auto_launcher()
        logger.info("CppRunnerPool: using %s", type(self._launcher).__name__)

        await self._launcher.start(self._pool_size)

        socket_dir = self._launcher.socket_dir
        for i in range(self._pool_size):
            sock = str(Path(socket_dir) / f"runner-{i}.sock")
            # The launcher only waits for the socket *file* to exist, not for the
            # runner's listen() to be fully active -- across a bind-mounted volume in
            # particular there can be a brief window where the path is visible before
            # the listener is actually accepting, so a first connect attempt can see
            # ECONNREFUSED on an otherwise-healthy runner. Short bounded retry instead
            # of failing the whole pool on that one race.
            last_exc: Exception | None = None
            for _ in range(20):
                try:
                    reader, writer = await asyncio.open_unix_connection(sock)
                    await self._pool.put((reader, writer))
                    logger.info("CppRunnerPool: connected runner %d", i)
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
                    await asyncio.sleep(0.1)
            if last_exc is not None:
                logger.error("CppRunnerPool: cannot connect runner %d: %s", i, last_exc)

        if self._pool.empty():
            raise RuntimeError(
                "CppRunnerPool: no runners connected. "
                f"Check launcher: {type(self._launcher).__name__}"
            )

    async def stop(self) -> None:
        while not self._pool.empty():
            try:
                _, writer = self._pool.get_nowait()
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        if self._launcher is not None:
            await self._launcher.stop()

    # ------------------------------------------------------------------ #
    # Execution                                                            #
    # ------------------------------------------------------------------ #

    async def run(
        self,
        so_bytes: bytes,
        df: pl.DataFrame,
        params: dict,
    ) -> np.ndarray:
        """Send compiled .so + OHLCV data to a runner, return float64 array."""
        if self._pool.empty():
            raise RuntimeError("CppRunnerPool: no runners available")

        from tradingkit.executor.subprocess_ import _df_to_arrow_bytes

        reader, writer = await self._pool.get()
        try:
            payload = pickle.dumps({
                "so":     so_bytes,
                "data":   _df_to_arrow_bytes(df),
                "params": json.dumps(params),
            })
            writer.write(struct.pack(">I", len(payload)) + payload)
            await writer.drain()

            size_bytes   = await reader.readexactly(4)
            result_bytes = await reader.readexactly(struct.unpack(">I", size_bytes)[0])
            result = pickle.loads(result_bytes)

            if "error" in result:
                raise RuntimeError(f"CppRunner error: {result['error']}")
            return np.frombuffer(result["values"], dtype=np.float64).copy()
        finally:
            await self._pool.put((reader, writer))

    # ------------------------------------------------------------------ #
    # Compiler image helper (used by api/compile.py)                      #
    # ------------------------------------------------------------------ #

    async def ensure_compiler_image(self) -> str | None:
        """
        Return Docker image tag for compilation, or None to use local g++.
        Delegates to launcher.ensure_compiler_image().
        """
        if self._launcher is None:
            return None
        return await self._launcher.ensure_compiler_image()
