"""
tradingkit.executor.launchers.base — RunnerLauncher ABC.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class RunnerLauncher(ABC):
    """
    Abstraction over "how to start/stop C++ runner workers".

    Each runner worker listens on a Unix socket at:
        {socket_dir}/runner-{i}.sock

    CppRunnerPool calls start() then connects to the sockets.
    Users can subclass to support Kubernetes, Firecracker, etc.
    """

    @abstractmethod
    async def start(self, pool_size: int) -> None:
        """
        Start `pool_size` runner workers.
        After this returns, sockets must be present at socket_dir.
        """

    @abstractmethod
    async def stop(self) -> None:
        """Stop all runner workers and release resources."""

    @property
    @abstractmethod
    def socket_dir(self) -> str:
        """Directory where runner-{i}.sock files are created."""

    async def ensure_compiler_image(self) -> str | None:
        """
        Return a Docker image tag to use for compilation, or None to compile
        with local g++ (direct subprocess, no Docker).

        DockerRunnerLauncher builds the image automatically on first call.
        SubprocessRunnerLauncher returns None → local g++ used.
        """
        return None
