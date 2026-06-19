"""
tradingkit-runner — standalone HTTP server for remote plugin execution.

Start with:
    tradingkit-runner --host 0.0.0.0 --port 8082

Then connect via:
    RemoteExecutor("http://localhost:8082")

Endpoints:
    POST /compute/indicator  — compute an Indicator on a data DataFrame
    POST /compute/strategy   — process a BarContext through a Strategy
    POST /compute/source     — fetch data from a DataSource
    GET  /health             — health check
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import pickle
import struct

import numpy as np
import polars as pl
import pyarrow as pa
from aiohttp import web

logger = logging.getLogger(__name__)


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


async def compute_indicator(request: web.Request) -> web.Response:
    try:
        raw = await request.read()
        payload = pickle.loads(raw)

        indicator = pickle.loads(payload["indicator"])
        df = _arrow_bytes_to_df(payload["data"])

        from tradingkit.indicator import IndicatorContext
        ctx = IndicatorContext(df)
        series: pl.Series = indicator(ctx)

        result = {"values": series.to_numpy().astype(np.float64).tobytes()}
        return web.Response(body=pickle.dumps(result), content_type="application/octet-stream")
    except Exception as exc:
        logger.exception("compute_indicator failed")
        return web.Response(
            body=pickle.dumps({"error": str(exc)}),
            content_type="application/octet-stream",
            status=500,
        )


async def compute_strategy(request: web.Request) -> web.Response:
    try:
        raw = await request.read()
        payload = pickle.loads(raw)

        strategy = pickle.loads(payload["strategy"])
        bar = pickle.loads(payload["bar"])

        signal = await strategy.on_bar(bar)
        result = {"signal": signal.to_dict() if signal is not None else None}
        return web.Response(body=pickle.dumps(result), content_type="application/octet-stream")
    except Exception as exc:
        logger.exception("compute_strategy failed")
        return web.Response(
            body=pickle.dumps({"error": str(exc)}),
            content_type="application/octet-stream",
            status=500,
        )


async def compute_source(request: web.Request) -> web.Response:
    try:
        raw = await request.read()
        payload = pickle.loads(raw)

        source = pickle.loads(payload["source"])
        df: pl.DataFrame = await source.get_historical_data(
            symbol=payload["symbol"],
            timeframe_seconds=payload["timeframe_seconds"],
            start_ts=payload["start_ts"],
            end_ts=payload["end_ts"],
            limit=payload["limit"],
        )
        result = {"data": _df_to_arrow_bytes(df)}
        return web.Response(body=pickle.dumps(result), content_type="application/octet-stream")
    except Exception as exc:
        logger.exception("compute_source failed")
        return web.Response(
            body=pickle.dumps({"error": str(exc)}),
            content_type="application/octet-stream",
            status=500,
        )


async def health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "service": "tradingkit-runner"})


def create_app() -> web.Application:
    app = web.Application(client_max_size=256 * 1024 * 1024)
    app.router.add_post("/compute/indicator", compute_indicator)
    app.router.add_post("/compute/strategy", compute_strategy)
    app.router.add_post("/compute/source", compute_source)
    app.router.add_get("/health", health)
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="tradingkit-runner: remote plugin execution server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8082)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app = create_app()
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
