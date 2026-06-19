"""
tradingkit.executor.launchers.subprocess_ — SubprocessRunnerLauncher.

Spawns runner_worker.py as plain Python subprocesses.
No Docker required. Isolation via resource limits only.

Use for: development, trusted environments, or when Docker is unavailable.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

from tradingkit.executor.launchers.base import RunnerLauncher

logger = logging.getLogger(__name__)

_WORKER_SCRIPT = Path(__file__).parent.parent.parent / "_docker" / "runner" / "runner_worker.py"
_DEFAULT_SOCKET_BASE = "/tmp/tradingkit-cpp"


class SubprocessRunnerLauncher(RunnerLauncher):
    """
    Launches runner_worker.py as subprocesses in-process.

    No Docker needed. Works anywhere Python runs.
    Resource limits enforced via the OS (resource.setrlimit in the worker).
    """

    def __init__(self) -> None:
        self._socket_dir = f"{_DEFAULT_SOCKET_BASE}-{os.getpid()}"
        self._procs: list[asyncio.subprocess.Process] = []

    @property
    def socket_dir(self) -> str:
        return self._socket_dir

    async def start(self, pool_size: int) -> None:
        os.makedirs(self._socket_dir, exist_ok=True)

        for i in range(pool_size):
            sock = Path(self._socket_dir) / f"runner-{i}.sock"
            if sock.exists():
                sock.unlink()

            proc = await asyncio.create_subprocess_exec(
                sys.executable, str(_WORKER_SCRIPT),
                env={
                    **os.environ,
                    "RUNNER_ID":         str(i),
                    "RUNNER_SOCKET_DIR": self._socket_dir,
                },
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            self._procs.append(proc)
            logger.info("SubprocessRunnerLauncher: started runner %d (pid=%d)", i, proc.pid)

        await self._wait_for_sockets(pool_size)

    async def _wait_for_sockets(self, pool_size: int, timeout: float = 10.0) -> None:
        deadline = asyncio.get_event_loop().time() + timeout
        for i in range(pool_size):
            sock = Path(self._socket_dir) / f"runner-{i}.sock"
            while not sock.exists():
                if asyncio.get_event_loop().time() > deadline:
                    raise RuntimeError(
                        f"Runner {i} socket never appeared in {self._socket_dir} "
                        f"after {timeout}s"
                    )
                await asyncio.sleep(0.05)

    async def stop(self) -> None:
        for proc in self._procs:
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=3.0)
            except Exception:
                proc.kill()
        self._procs.clear()
        # Clean up sockets
        import shutil
        try:
            shutil.rmtree(self._socket_dir, ignore_errors=True)
        except Exception:
            pass
