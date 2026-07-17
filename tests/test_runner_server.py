"""tradingkit.runner.server — auth middleware, restricted unpickling, fail-closed CLI."""
from __future__ import annotations

import os
import pickle
import subprocess
import sys

import polars as pl
import pytest
from aiohttp.test_utils import TestServer

from tradingkit.indicator import ScriptIndicator
from tradingkit.runner.server import _is_loopback, create_app

TOKEN = "test-token-abc123"


@pytest.fixture
async def server_with_token():
    app = create_app(token=TOKEN)
    server = TestServer(app)
    await server.start_server()
    yield server, str(server.make_url("/"))
    await server.close()


@pytest.fixture
async def server_without_token():
    app = create_app(token=None)
    server = TestServer(app)
    await server.start_server()
    yield server, str(server.make_url("/"))
    await server.close()


async def test_health_requires_no_token(server_with_token):
    import aiohttp
    _, base_url = server_with_token
    async with aiohttp.ClientSession() as sess:
        async with sess.get(f"{base_url}health") as resp:
            assert resp.status == 200


async def test_compute_without_token_is_rejected(server_with_token):
    import aiohttp
    _, base_url = server_with_token
    async with aiohttp.ClientSession() as sess:
        async with sess.post(f"{base_url}compute/indicator", data=b"whatever") as resp:
            assert resp.status == 401


async def test_compute_with_wrong_token_is_rejected(server_with_token):
    import aiohttp
    _, base_url = server_with_token
    async with aiohttp.ClientSession() as sess:
        async with sess.post(
            f"{base_url}compute/indicator", data=b"whatever",
            headers={"Authorization": "Bearer wrong-token"},
        ) as resp:
            assert resp.status == 401


async def test_compute_indicator_with_correct_token_computes(server_with_token):
    import aiohttp
    _, base_url = server_with_token
    df = pl.DataFrame({"timestamp": [1, 2, 3], "close": [1.0, 2.0, 3.0]})
    ind = ScriptIndicator(code="result = close * 3", period=1)
    payload = pickle.dumps({
        "indicator": pickle.dumps(ind),
        "data": _arrow_bytes(df),
    })
    async with aiohttp.ClientSession() as sess:
        async with sess.post(
            f"{base_url}compute/indicator", data=payload,
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as resp:
            assert resp.status == 200
            result = pickle.loads(await resp.read())
    import numpy as np
    values = np.frombuffer(result["values"], dtype=np.float64)
    assert list(values) == [3.0, 6.0, 9.0]


async def test_malicious_payload_rejected_even_with_valid_token(server_with_token, tmp_path):
    import aiohttp
    _, base_url = server_with_token
    marker = tmp_path / "pwned"

    class Evil:
        def __reduce__(self):
            return (os.system, (f"echo pwned > {marker}",))

    payload = pickle.dumps({"indicator": pickle.dumps(Evil()), "data": b""})
    async with aiohttp.ClientSession() as sess:
        async with sess.post(
            f"{base_url}compute/indicator", data=payload,
            headers={"Authorization": f"Bearer {TOKEN}"},
        ) as resp:
            assert resp.status == 400
    assert not marker.exists()


async def test_no_token_configured_allows_request(server_without_token):
    """Loopback-only, no-token dev mode: requests are accepted (see server.py fail-closed startup gate)."""
    import aiohttp
    _, base_url = server_without_token
    async with aiohttp.ClientSession() as sess:
        async with sess.get(f"{base_url}health") as resp:
            assert resp.status == 200


def _arrow_bytes(df: pl.DataFrame) -> bytes:
    import pyarrow as pa
    table = df.to_arrow()
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return sink.getvalue().to_pybytes()


# ------------------------------------------------------------------ #
# _is_loopback + fail-closed CLI validation                            #
# ------------------------------------------------------------------ #

@pytest.mark.parametrize("host,expected", [
    ("127.0.0.1", True),
    ("localhost", True),
    ("::1", True),
    ("0.0.0.0", False),
    ("192.168.1.5", False),
    ("example.com", False),
])
def test_is_loopback(host, expected):
    assert _is_loopback(host) is expected


def test_cli_refuses_nonloopback_without_allow_remote():
    proc = subprocess.run(
        [sys.executable, "-m", "tradingkit.runner.server", "--host", "0.0.0.0"],
        capture_output=True, text=True, timeout=10,
    )
    assert proc.returncode != 0
    assert "--allow-remote" in proc.stderr


def test_cli_refuses_allow_remote_without_token():
    proc = subprocess.run(
        [sys.executable, "-m", "tradingkit.runner.server", "--host", "0.0.0.0", "--allow-remote"],
        capture_output=True, text=True, timeout=10,
    )
    assert proc.returncode != 0
    assert "token" in proc.stderr.lower()