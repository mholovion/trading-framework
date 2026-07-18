#!/usr/bin/env python3
"""
C++ indicator runner — Unix socket server.

Listens on /run/cpp-runner/runner-{RUNNER_ID}.sock.
Accepts length-prefixed pickle requests, executes compiled .so, returns results.

Protocol (same as CppRunnerPool):
  Request:  {"so": bytes, "candles": arrow_ipc_bytes, "params": json_str}
  Response: {"values": float64_bytes} | {"error": str}

ABI expected from the .so:
  extern "C" void indicator_compute(
      const double* close, const double* high, const double* low,
      const double* open,  const double* volume,
      int length, double* result, const char* params_json);
"""
from __future__ import annotations

import asyncio
import ctypes
import logging
import os
import pickle
import struct
import tempfile

import numpy as np
import pyarrow as pa

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

SOCKET_DIR = os.environ.get("RUNNER_SOCKET_DIR", "/run/cpp-runner")
RUNNER_ID  = os.environ.get("RUNNER_ID", "0")
SOCK_PATH  = f"{SOCKET_DIR}/runner-{RUNNER_ID}.sock"


def _load_so(so_bytes: bytes) -> ctypes.CDLL:
    with tempfile.NamedTemporaryFile(suffix=".so", delete=False) as f:
        f.write(so_bytes)
        f.flush()
        return ctypes.CDLL(f.name)


def _run_indicator(so_bytes: bytes, candles_ipc: bytes, params_json: str) -> bytes:
    buf = pa.py_buffer(candles_ipc)
    reader = pa.ipc.open_stream(buf)
    table = reader.read_all()

    def _col(name: str) -> np.ndarray:
        return np.asarray(table.column(name), dtype=np.float64)

    close  = _col("close")
    high   = _col("high")
    low    = _col("low")
    open_  = _col("open")
    volume = _col("volume")
    length = len(close)
    result = np.zeros(length, dtype=np.float64)

    lib = _load_so(so_bytes)
    fn = lib.indicator_compute
    fn.restype = None
    fn.argtypes = [
        ctypes.POINTER(ctypes.c_double),  # close
        ctypes.POINTER(ctypes.c_double),  # high
        ctypes.POINTER(ctypes.c_double),  # low
        ctypes.POINTER(ctypes.c_double),  # open
        ctypes.POINTER(ctypes.c_double),  # volume
        ctypes.c_int,                      # length
        ctypes.POINTER(ctypes.c_double),  # result
        ctypes.c_char_p,                   # params_json
    ]

    fn(
        close.ctypes.data_as(ctypes.POINTER(ctypes.c_double)),
        high.ctypes.data_as(ctypes.POINTER(ctypes.c_double)),
        low.ctypes.data_as(ctypes.POINTER(ctypes.c_double)),
        open_.ctypes.data_as(ctypes.POINTER(ctypes.c_double)),
        volume.ctypes.data_as(ctypes.POINTER(ctypes.c_double)),
        ctypes.c_int(length),
        result.ctypes.data_as(ctypes.POINTER(ctypes.c_double)),
        params_json.encode(),
    )
    return result.tobytes()


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while True:
            header = await reader.readexactly(4)
            payload_len = struct.unpack(">I", header)[0]
            raw = await reader.readexactly(payload_len)
            req = pickle.loads(raw)

            try:
                values_bytes = _run_indicator(
                    req["so"],
                    req["data"],
                    req.get("params", "{}"),
                )
                response = {"values": values_bytes}
            except Exception as exc:
                logger.exception("indicator_compute failed")
                response = {"error": str(exc)}

            resp_bytes = pickle.dumps(response)
            writer.write(struct.pack(">I", len(resp_bytes)) + resp_bytes)
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionResetError):
        pass
    finally:
        writer.close()


async def main() -> None:
    os.makedirs(SOCKET_DIR, exist_ok=True)
    if os.path.exists(SOCK_PATH):
        os.unlink(SOCK_PATH)

    server = await asyncio.start_unix_server(handle_client, path=SOCK_PATH)
    try:
        os.chmod(SOCK_PATH, 0o660)
    except OSError as exc:
        # Defense-in-depth hardening, not the primary boundary (that's network=none +
        # seccomp + which processes can even see this path) -- some bind-mount backends
        # (observed: Docker Desktop's virtiofs) reject chmod on socket special files
        # with EINVAL. Not worth crashing the runner over.
        logger.warning("Could not chmod %s to 0o660: %s", SOCK_PATH, exc)
    logger.info("Runner %s listening on %s", RUNNER_ID, SOCK_PATH)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
