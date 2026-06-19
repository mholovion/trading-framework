"""
tradingkit.executor.launchers.docker_ — DockerRunnerLauncher.

Manages runner and compiler Docker containers via docker-py SDK.

On first use:
  - Builds tradingkit_cpp_runner image from bundled _docker/runner/
  - Builds tradingkit_cpp_compiler image from bundled _docker/compiler/

Runner containers:
  --network=none  --security-opt seccomp=<bundled>
  --memory=256m   --read-only  --tmpfs /tmp
  Shared volume: tradingkit_runner_sockets → /run/cpp-runner

Requires:
  - Docker daemon accessible (docker.sock mounted or native)
  - pip install docker  (tradingkit[cpp] optional dep)
"""
from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from tradingkit.executor.launchers.base import RunnerLauncher

logger = logging.getLogger(__name__)

_DOCKER_DIR     = Path(__file__).parent.parent.parent / "_docker"
_RUNNER_IMAGE   = "tradingkit_cpp_runner"
_COMPILER_IMAGE = "tradingkit_cpp_compiler"
_SOCKET_DIR     = "/run/cpp-runner"   # path inside app container (volume mount)
_VOLUME_NAME    = "tradingkit_runner_sockets"
_SOCKET_TIMEOUT = 30.0


class DockerRunnerLauncher(RunnerLauncher):
    """
    Starts runner workers as isolated Docker containers.

    Max-security profile:
      network_mode=none  →  no outbound connections
      seccomp profile    →  minimal syscall whitelist
      read-only rootfs   →  no filesystem writes (except /tmp tmpfs)
      memory=256m        →  OOM killed on excess
      cpus=1             →  bounded CPU

    Images are built automatically on first use from the bundled
    framework/_docker/ directory — no manual `docker compose build` needed.
    """

    def __init__(
        self,
        socket_volume: str = _VOLUME_NAME,
        runner_image:  str = _RUNNER_IMAGE,
        compiler_image: str = _COMPILER_IMAGE,
    ) -> None:
        self._socket_volume  = socket_volume
        self._runner_image   = runner_image
        self._compiler_image = compiler_image
        self._containers: list = []
        self._client = None

    @property
    def socket_dir(self) -> str:
        return _SOCKET_DIR

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def start(self, pool_size: int) -> None:
        client = self._get_client()

        await asyncio.get_event_loop().run_in_executor(
            None, self._ensure_image, client, self._runner_image, "runner"
        )
        await asyncio.get_event_loop().run_in_executor(
            None, self._ensure_volume, client
        )

        for i in range(pool_size):
            c = await asyncio.get_event_loop().run_in_executor(
                None, self._start_container, client, i
            )
            self._containers.append(c)
            logger.info("DockerRunnerLauncher: started container %s (runner %d)", c.short_id, i)

        await self._wait_for_sockets(pool_size)

    async def stop(self) -> None:
        for c in self._containers:
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: c.stop(timeout=5)
                )
            except Exception as exc:
                logger.warning("Failed to stop container %s: %s", c.short_id, exc)
        self._containers.clear()

    # ------------------------------------------------------------------ #
    # Compiler image                                                       #
    # ------------------------------------------------------------------ #

    async def ensure_compiler_image(self) -> str | None:
        client = self._get_client()
        await asyncio.get_event_loop().run_in_executor(
            None, self._ensure_image, client, self._compiler_image, "compiler"
        )
        return self._compiler_image

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _get_client(self):
        if self._client is None:
            import docker
            self._client = docker.from_env()
        return self._client

    def _ensure_image(self, client, tag: str, subdir: str) -> None:
        import docker
        try:
            client.images.get(tag)
            logger.debug("Image %s already exists", tag)
        except docker.errors.ImageNotFound:
            logger.info("Building %s image from %s (first use)…", tag, subdir)
            client.images.build(
                path=str(_DOCKER_DIR / subdir),
                tag=tag,
                rm=True,
            )
            logger.info("Image %s built", tag)

    def _ensure_volume(self, client) -> None:
        import docker
        try:
            client.volumes.get(self._socket_volume)
        except docker.errors.NotFound:
            client.volumes.create(self._socket_volume)
            logger.info("Created volume %s", self._socket_volume)

    def _start_container(self, client, runner_id: int):
        seccomp = str(_DOCKER_DIR / "runner" / "seccomp.json")
        return client.containers.run(
            self._runner_image,
            detach=True,
            remove=True,
            network_mode="none",
            security_opt=[f"seccomp={seccomp}"],
            mem_limit="256m",
            nano_cpus=1_000_000_000,
            read_only=True,
            tmpfs={"/tmp": "size=64m,exec"},
            volumes={
                self._socket_volume: {"bind": _SOCKET_DIR, "mode": "rw"},
            },
            environment={
                "RUNNER_ID":         str(runner_id),
                "RUNNER_SOCKET_DIR": _SOCKET_DIR,
            },
        )

    async def _wait_for_sockets(self, pool_size: int) -> None:
        """
        Poll the shared volume via app-side mount point.
        The volume must be mounted in the app container at _SOCKET_DIR.
        """
        deadline = asyncio.get_event_loop().time() + _SOCKET_TIMEOUT
        for i in range(pool_size):
            sock = Path(_SOCKET_DIR) / f"runner-{i}.sock"
            while not sock.exists():
                if asyncio.get_event_loop().time() > deadline:
                    raise RuntimeError(
                        f"Runner {i} socket never appeared at {sock} "
                        f"after {_SOCKET_TIMEOUT}s. "
                        "Is the runner_sockets volume mounted in the app container?"
                    )
                await asyncio.sleep(0.1)
