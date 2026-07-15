"""
tradingkit-runner — standalone HTTP server for remote plugin execution.

Start with:
    tradingkit-runner --token "$(openssl rand -hex 32)"

Then connect via:
    RemoteExecutor("http://localhost:8082", token="...")

Endpoints:
    POST /compute/indicator  — compute an Indicator on a data DataFrame
    POST /compute/strategy   — process a BarContext through a Strategy
    POST /compute/source     — fetch data from a DataSource
    GET  /health             — health check (no auth required)

Security model (see the "Security model" section of the top-level README for the full
picture):
  - Every /compute/* request must carry `Authorization: Bearer <token>`, checked with a
    constant-time comparison before the request body is even read. There is no
    unauthenticated fallback: binding to anything other than 127.0.0.1 requires a token.
  - Request bodies are deserialized with tradingkit.runner._safe_pickle, an allowlisting
    unpickler — not the raw stdlib pickle.loads(). This blocks the classic os/subprocess/
    eval pickle RCE gadgets even from an authenticated-but-malicious or replayed payload.
  - The token is a shared secret between trusted peers, not a full authz/encryption
    scheme. Plain HTTP sends it in cleartext. Do not expose this server to the public
    internet, token or not — put it behind a TLS-terminating reverse proxy or keep it on
    a private network (VPN/VPC), and pass --allow-remote only once that's in place.
"""
from __future__ import annotations

import argparse
import hmac
import ipaddress
import logging
import os

import numpy as np
import polars as pl
import pyarrow as pa
from aiohttp import web

from tradingkit.runner import _safe_pickle
from tradingkit.runner._safe_pickle import UnsafeUnpicklingError

logger = logging.getLogger(__name__)

_TOKEN_ENV_VAR = "TRADINGKIT_RUNNER_TOKEN"
_TRUST_MODULE_ENV_VAR = "TRADINGKIT_RUNNER_TRUST_MODULES"  # comma-separated


def _arrow_bytes_to_df(data: bytes) -> pl.DataFrame:
    buf = pa.py_buffer(data)
    reader = pa.ipc.open_stream(buf)
    return pl.from_arrow(reader.read_all())


def _df_to_arrow_bytes(df: pl.DataFrame) -> bytes:
    arrow_table = df.to_arrow()
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, arrow_table.schema)
    writer.write_table(arrow_table)
    writer.close()
    return sink.getvalue().to_pybytes()


def _is_loopback(host: str) -> bool:
    if host in ("localhost",):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False  # hostnames other than "localhost" are treated as non-loopback


# ------------------------------------------------------------------ #
# Auth middleware                                                       #
# ------------------------------------------------------------------ #

@web.middleware
async def _auth_middleware(request: web.Request, handler):
    token = request.app.get("token")
    if token is None or request.path == "/health":
        return await handler(request)

    auth_header = request.headers.get("Authorization", "")
    supplied = auth_header[7:] if auth_header.startswith("Bearer ") else ""
    if not hmac.compare_digest(supplied, token):
        logger.warning(
            "tradingkit-runner: rejected unauthenticated request from %s to %s",
            request.remote, request.path,
        )
        return web.json_response({"error": "unauthorized"}, status=401)
    return await handler(request)


# ------------------------------------------------------------------ #
# Compute endpoints                                                     #
# ------------------------------------------------------------------ #

async def compute_indicator(request: web.Request) -> web.Response:
    try:
        raw = await request.read()
        payload = _safe_pickle.loads(raw, trusted_modules=request.app["trusted_modules"])

        indicator = _safe_pickle.loads(
            payload["indicator"], trusted_modules=request.app["trusted_modules"]
        )
        df = _arrow_bytes_to_df(payload["data"])

        from tradingkit.indicator import IndicatorContext
        ctx = IndicatorContext(df)
        series: pl.Series = indicator(ctx)

        result = {"values": series.to_numpy().astype(np.float64).tobytes()}
        return web.Response(body=_safe_pickle_dumps(result), content_type="application/octet-stream")
    except UnsafeUnpicklingError as exc:
        logger.warning("compute_indicator: rejected payload: %s", exc)
        return web.Response(
            body=_safe_pickle_dumps({"error": "invalid payload"}),
            content_type="application/octet-stream",
            status=400,
        )
    except Exception as exc:
        logger.exception("compute_indicator failed")
        return web.Response(
            body=_safe_pickle_dumps({"error": str(exc)}),
            content_type="application/octet-stream",
            status=500,
        )


async def compute_strategy(request: web.Request) -> web.Response:
    try:
        raw = await request.read()
        payload = _safe_pickle.loads(raw, trusted_modules=request.app["trusted_modules"])

        strategy = _safe_pickle.loads(
            payload["strategy"], trusted_modules=request.app["trusted_modules"]
        )
        bar = _safe_pickle.loads(payload["bar"], trusted_modules=request.app["trusted_modules"])

        signal = await strategy.on_bar(bar)
        result = {"signal": signal.to_dict() if signal is not None else None}
        return web.Response(body=_safe_pickle_dumps(result), content_type="application/octet-stream")
    except UnsafeUnpicklingError as exc:
        logger.warning("compute_strategy: rejected payload: %s", exc)
        return web.Response(
            body=_safe_pickle_dumps({"error": "invalid payload"}),
            content_type="application/octet-stream",
            status=400,
        )
    except Exception as exc:
        logger.exception("compute_strategy failed")
        return web.Response(
            body=_safe_pickle_dumps({"error": str(exc)}),
            content_type="application/octet-stream",
            status=500,
        )


async def compute_source(request: web.Request) -> web.Response:
    try:
        raw = await request.read()
        payload = _safe_pickle.loads(raw, trusted_modules=request.app["trusted_modules"])

        source = _safe_pickle.loads(
            payload["source"], trusted_modules=request.app["trusted_modules"]
        )
        df: pl.DataFrame = await source.get_historical_data(
            symbol=payload["symbol"],
            timeframe_seconds=payload["timeframe_seconds"],
            start_ts=payload["start_ts"],
            end_ts=payload["end_ts"],
            limit=payload["limit"],
        )
        result = {"data": _df_to_arrow_bytes(df)}
        return web.Response(body=_safe_pickle_dumps(result), content_type="application/octet-stream")
    except UnsafeUnpicklingError as exc:
        logger.warning("compute_source: rejected payload: %s", exc)
        return web.Response(
            body=_safe_pickle_dumps({"error": "invalid payload"}),
            content_type="application/octet-stream",
            status=400,
        )
    except Exception as exc:
        logger.exception("compute_source failed")
        return web.Response(
            body=_safe_pickle_dumps({"error": str(exc)}),
            content_type="application/octet-stream",
            status=500,
        )


async def health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "service": "tradingkit-runner"})


def _safe_pickle_dumps(obj) -> bytes:
    # Responses are plain dicts of primitives/bytes that *this* process produced —
    # standard pickle is fine for output; only inbound network payloads go through
    # the restricted unpickler.
    import pickle
    return pickle.dumps(obj)


# ------------------------------------------------------------------ #
# App factory                                                          #
# ------------------------------------------------------------------ #

def create_app(token: str | None = None, trusted_modules: list[str] | None = None) -> web.Application:
    app = web.Application(client_max_size=256 * 1024 * 1024, middlewares=[_auth_middleware])
    app["token"] = token
    app["trusted_modules"] = trusted_modules or []
    app.router.add_post("/compute/indicator", compute_indicator)
    app.router.add_post("/compute/strategy", compute_strategy)
    app.router.add_post("/compute/source", compute_source)
    app.router.add_get("/health", health)
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="tradingkit-runner: remote plugin execution server")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8082)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--token", default=None,
        help=f"Shared-secret bearer token (or set {_TOKEN_ENV_VAR}). "
             "Required unless --host is loopback.",
    )
    parser.add_argument(
        "--allow-remote", action="store_true",
        help="Confirm binding to a non-loopback --host. Requires --token.",
    )
    parser.add_argument(
        "--trust-module", action="append", default=[], dest="trust_modules", metavar="MODULE",
        help="Additional module whose classes the runner may unpickle (repeatable). "
             f"Only pass modules you control. Also settable via {_TRUST_MODULE_ENV_VAR} "
             "(comma-separated).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    token = args.token or os.environ.get(_TOKEN_ENV_VAR)
    loopback = _is_loopback(args.host)

    if not loopback and not args.allow_remote:
        parser.error(
            f"--host {args.host!r} is not loopback. Pass --allow-remote to confirm "
            "you intend to bind non-locally (see the README's Security model section "
            "before doing this on a public network)."
        )
    if not loopback and not token:
        parser.error(
            "Binding to a non-loopback host requires a token: pass --token or set "
            f"{_TOKEN_ENV_VAR}."
        )
    if loopback and not token:
        logger.warning(
            "No auth token configured — /compute/* is reachable by any local process "
            "on this machine (bind is loopback-only, so remote hosts can't reach it)."
        )

    trusted_modules = list(args.trust_modules)
    env_modules = os.environ.get(_TRUST_MODULE_ENV_VAR)
    if env_modules:
        trusted_modules.extend(m.strip() for m in env_modules.split(",") if m.strip())
    if trusted_modules:
        logger.warning(
            "tradingkit-runner: trusting additional modules for unpickling: %s — "
            "only do this for modules you personally control.", trusted_modules,
        )

    app = create_app(token=token, trusted_modules=trusted_modules)
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()