"""
CppRunnerPool — real end-to-end execution over the Unix-socket protocol.

Tier A (SubprocessRunnerLauncher) needs no Docker and runs everywhere this test suite
runs: local g++ compiles a real .so, a real OS subprocess loads and executes it via
ctypes, exactly like tradingkit/_docker/runner/runner_worker.py does in production.

Tier B (DockerRunnerLauncher) exercises the same protocol through a real container —
network=none, seccomp, memory-limited — and additionally asserts the isolation flags
actually made it into the container's HostConfig. Skipped wherever Docker isn't
reachable (same detection tradingkit.executor.launchers.auto_launcher() uses), so it's
inert in environments without Docker and active in CI.
"""
from __future__ import annotations

import shutil
import subprocess

import polars as pl
import pytest

from tradingkit.executor.cpp_pool import CppRunnerPool
from tradingkit.executor.launchers import DockerRunnerLauncher, SubprocessRunnerLauncher

# result[i] = close[i] * 2 + volume[i] -- exercises both a plain arg and length looping,
# distinct enough from any single input column to catch a wrong-pointer-order bug.
_INDICATOR_SRC = """
extern "C" void indicator_compute(
    const double* close, const double* high, const double* low,
    const double* open,  const double* volume,
    int length, double* result, const char* params_json)
{
    for (int i = 0; i < length; i++) {
        result[i] = close[i] * 2.0 + volume[i];
    }
}
"""


def _compile_test_indicator(tmp_path) -> bytes:
    """Mirrors _docker/compiler/compile.sh's exact g++ invocation."""
    src = tmp_path / "indicator.cpp"
    so = tmp_path / "indicator.so"
    src.write_text(_INDICATOR_SRC)
    subprocess.run(
        ["g++", "-shared", "-fPIC", "-std=c++20", "-O2", "-o", str(so), str(src)],
        check=True, capture_output=True, text=True,
    )
    return so.read_bytes()


@pytest.fixture(scope="module")
def so_bytes(tmp_path_factory) -> bytes:
    if shutil.which("g++") is None and shutil.which("gcc") is None:
        pytest.skip("no C++ compiler available")
    tmp_path = tmp_path_factory.mktemp("cpp_pool")
    return _compile_test_indicator(tmp_path)


@pytest.fixture
def ohlcv_df() -> pl.DataFrame:
    return pl.DataFrame({
        "timestamp": [1, 2, 3],
        "open":   [1.0, 2.0, 3.0],
        "high":   [1.0, 2.0, 3.0],
        "low":    [1.0, 2.0, 3.0],
        "close":  [10.0, 20.0, 30.0],
        "volume": [1.0, 1.0, 1.0],
    })


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


def _docker_is_desktop_vm() -> bool:
    """
    True under Docker Desktop (Mac/Windows), where the daemon runs inside a Linux VM --
    as opposed to a native Linux daemon (GitHub Actions' ubuntu-latest, most self-hosted
    setups). `docker info` reports this directly, no host-OS guessing needed.
    """
    try:
        import docker
        return docker.from_env().info().get("OperatingSystem") == "Docker Desktop"
    except Exception:
        return False


# ------------------------------------------------------------------ #
# Tier A — SubprocessRunnerLauncher, no Docker required                #
# ------------------------------------------------------------------ #

async def test_subprocess_launcher_runs_real_compiled_indicator(so_bytes, ohlcv_df):
    pool = CppRunnerPool(launcher=SubprocessRunnerLauncher(), pool_size=1)
    await pool.start()
    try:
        result = await pool.run(so_bytes, ohlcv_df, params={})
        assert list(result) == [21.0, 41.0, 61.0]  # close*2 + volume
    finally:
        await pool.stop()


async def test_subprocess_launcher_reports_compile_errors_as_runtime_errors(ohlcv_df):
    """A malformed .so (garbage bytes) should surface as a RuntimeError from the pool,
    not crash the runner process or hang the caller."""
    pool = CppRunnerPool(launcher=SubprocessRunnerLauncher(), pool_size=1)
    await pool.start()
    try:
        with pytest.raises(RuntimeError):
            await pool.run(b"not a real shared object", ohlcv_df, params={})
    finally:
        await pool.stop()


async def test_subprocess_launcher_pool_reuses_workers(so_bytes, ohlcv_df):
    """Two sequential runs against a pool_size=1 pool must both succeed -- proves the
    (reader, writer) pair is correctly returned to the queue after each run()."""
    pool = CppRunnerPool(launcher=SubprocessRunnerLauncher(), pool_size=1)
    await pool.start()
    try:
        r1 = await pool.run(so_bytes, ohlcv_df, params={})
        r2 = await pool.run(so_bytes, ohlcv_df, params={})
        assert list(r1) == list(r2) == [21.0, 41.0, 61.0]
    finally:
        await pool.stop()


async def test_subprocess_launcher_caches_loaded_so_by_content_hash(so_bytes, ohlcv_df):
    """runner_worker.py's _load_so caches by sha256(so_bytes), not a caller-supplied
    name -- two calls with identical bytes must dlopen/tempfile-write only once. This
    doesn't just re-check correctness (test_subprocess_launcher_pool_reuses_workers
    already does that) -- it inspects the runner's own log to prove the *caching*
    actually engaged: call 1 -> miss, call 2 -> hit, not two misses that happen to both
    produce the right answer."""
    launcher = SubprocessRunnerLauncher()
    pool = CppRunnerPool(launcher=launcher, pool_size=1)
    await pool.start()
    try:
        r1 = await pool.run(so_bytes, ohlcv_df, params={})
        r2 = await pool.run(so_bytes, ohlcv_df, params={})
        assert list(r1) == list(r2) == [21.0, 41.0, 61.0]
    finally:
        procs = list(launcher._procs)  # stop() clears launcher._procs -- grab refs first
        await pool.stop()

    stderr = (await procs[0].stderr.read()).decode(errors="replace")
    assert stderr.count("so cache miss") == 1, stderr
    assert stderr.count("so cache hit") == 1, stderr


async def test_auto_launcher_falls_back_to_subprocess_without_docker(monkeypatch):
    """auto_launcher() must not raise even when the `docker` package is missing/unreachable."""
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "docker":
            raise ImportError("simulated: docker not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    from tradingkit.executor.launchers import auto_launcher
    launcher = auto_launcher()
    assert isinstance(launcher, SubprocessRunnerLauncher)


# ------------------------------------------------------------------ #
# Tier B — DockerRunnerLauncher, real container, Docker-only            #
# ------------------------------------------------------------------ #

pytestmark_docker = pytest.mark.skipif(
    not _docker_available(), reason="Docker daemon not reachable in this environment"
)


@pytest.fixture
def socket_dir():
    """
    A bind-mounted host directory instead of DockerRunnerLauncher's default named
    volume. In production, DockerRunnerLauncher runs *inside* the app container, which
    shares the named volume with each runner container -- see docker-compose.yml. pytest
    here runs directly on the bare host (locally and in CI), which can't see into a named
    volume at all, so the launcher needs a real host path both sides can open directly.

    Deliberately not pytest's tmp_path: AF_UNIX socket paths are capped at ~104-108
    bytes by the kernel (sockaddr_un.sun_path), and tmp_path's
    /private/var/.../pytest-of-<user>/pytest-NN/<test-name>/ nesting blows past that on
    its own, before the socket filename is even appended.
    """
    import shutil
    import tempfile
    # dir="/tmp" (not the default $TMPDIR) so the path stays short on macOS, where
    # $TMPDIR is a long per-user /var/folders/... path that alone can exceed the limit.
    path = tempfile.mkdtemp(prefix="tk_ipc_", dir="/tmp")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytestmark_docker
@pytest.mark.skipif(
    _docker_is_desktop_vm(),
    reason=(
        "Docker Desktop runs the daemon inside a Linux VM: a bind-mounted Unix socket "
        "is metadata-visible from the host (its path exists) but not connectable from "
        "the host (needs the same kernel on both ends). test_docker_launcher_containers_"
        "are_actually_isolated below already proves the image/container/isolation-flags "
        "path works under Docker Desktop -- this one additionally needs a real "
        "host-to-container connection, which only a native Linux daemon provides "
        "(GitHub Actions' ubuntu-latest, most self-hosted runners)."
    ),
)
async def test_docker_launcher_runs_real_compiled_indicator(so_bytes, ohlcv_df, socket_dir):
    pool = CppRunnerPool(launcher=DockerRunnerLauncher(socket_volume=socket_dir), pool_size=1)
    await pool.start()
    try:
        result = await pool.run(so_bytes, ohlcv_df, params={})
        assert list(result) == [21.0, 41.0, 61.0]
    finally:
        await pool.stop()


@pytestmark_docker
async def test_docker_launcher_containers_are_actually_isolated(socket_dir):
    """The isolation flags DockerRunnerLauncher claims (network=none, 256m, read-only)
    must actually be present on the running container's HostConfig -- not just passed
    to the SDK call and silently ignored."""
    launcher = DockerRunnerLauncher(socket_volume=socket_dir)
    await launcher.start(1)
    try:
        assert len(launcher._containers) == 1
        container = launcher._containers[0]
        container.reload()
        host_config = container.attrs["HostConfig"]
        assert host_config["NetworkMode"] == "none"
        assert host_config["Memory"] == 256 * 1024 * 1024
        assert host_config["ReadonlyRootfs"] is True
    finally:
        await launcher.stop()