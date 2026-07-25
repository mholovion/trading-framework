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
from functools import partial
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
        """
        socket_volume: a Docker named volume (default) shared with the app container in
        production -- see docker-compose.yml, where both `app` and each runner container
        mount it at _SOCKET_DIR, so a named volume works because the launcher's own
        process already lives inside a container.

        Pass an absolute host path instead (e.g. "/tmp/tradingkit_sockets") to bind-mount
        a real directory that a *bare* launcher process can also see directly -- needed
        wherever the launcher itself doesn't run inside a container with the volume
        mounted (tests, or a non-containerized dev setup).
        """
        self._socket_volume  = socket_volume
        self._runner_image   = runner_image
        self._compiler_image = compiler_image
        self._containers: list = []
        self._client = None

    @property
    def _is_bind_mount(self) -> bool:
        return self._socket_volume.startswith("/")

    @property
    def socket_dir(self) -> str:
        # Host-visible path when bind-mounted; the fixed in-container path otherwise
        # (relies on the launcher's own process sharing the named volume -- true in
        # production, where DockerRunnerLauncher always runs inside the app container).
        return self._socket_volume if self._is_bind_mount else _SOCKET_DIR

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
                    None, partial(c.stop, timeout=5)
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
            # docker-py's default is 60s, which a loaded host (many cached images/
            # volumes, or a container start under a seccomp/read-only/no-network
            # config) can genuinely exceed -- a slow container is not the same as a
            # broken one, so give it real headroom instead of failing the request.
            self._client = docker.from_env(timeout=180)
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
        if self._is_bind_mount:
            Path(self._socket_volume).mkdir(parents=True, exist_ok=True)
            return
        import docker
        try:
            client.volumes.get(self._socket_volume)
        except docker.errors.NotFound:
            client.volumes.create(self._socket_volume)
            logger.info("Created volume %s", self._socket_volume)

    def _start_container(self, client, runner_id: int):
        # docker-py talks to the Engine API directly, unlike the `docker` CLI it does
        # not resolve `seccomp=<path>` to file contents itself -- the profile JSON has
        # to be read and inlined here, or the daemon tries to parse the path string
        # itself as JSON and rejects it immediately.
        seccomp_json = (_DOCKER_DIR / "runner" / "seccomp.json").read_text()
        return client.containers.run(
            self._runner_image,
            detach=True,
            remove=True,
            network_mode="none",
            security_opt=[f"seccomp={seccomp_json}"],
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
        Poll the shared volume via this process's own view of it: the in-container
        mount point for the (default) named-volume mode, or the bind-mount host path
        when constructed with an absolute socket_volume. See socket_dir/__init__.
        """
        deadline = asyncio.get_event_loop().time() + _SOCKET_TIMEOUT
        for i in range(pool_size):
            sock = Path(self.socket_dir) / f"runner-{i}.sock"
            while not sock.exists():
                if asyncio.get_event_loop().time() > deadline:
                    raise RuntimeError(
                        f"Runner {i} socket never appeared at {sock} "
                        f"after {_SOCKET_TIMEOUT}s. Does this process share "
                        f"{self._socket_volume!r} with the runner containers -- "
                        "mounted at the same path if it's a named volume, or "
                        "constructed with that absolute path if it's a bind mount?"
                    )
                await asyncio.sleep(0.1)
