#!/usr/bin/env python3
"""Plugin runner — sandboxed execution server for indicator and strategy plugins.

Runs as a standalone aiohttp service.
Has read-only ClickHouse access (no exchange API keys from .env).
"""

from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path

# Ensure project roots are on sys.path regardless of working directory
_FRAMEWORK_DIR = Path(__file__).parents[1]   # framework/
_PROJECT_ROOT  = Path(__file__).parents[2]   # project root
for _p in (_FRAMEWORK_DIR, _PROJECT_ROOT):
    _s = str(_p)
    if _s not in sys.path:
        sys.path.insert(0, _s)

from aiohttp import web

from plugins.indicators.loader import _load_locally as _load_indicator
from plugins.indicators.loader import load_all_primitives

logger = logging.getLogger(__name__)

_db = None  # ClickHouseManager, initialised on startup


# --------------------------------------------------------------------------- #
# Strategy local loading                                                       #
# --------------------------------------------------------------------------- #

def _load_strategy(name: str, config: dict):
    importlib.invalidate_caches()
    mod = importlib.import_module(f"plugins.strategies.{name}_plugin")
    cls_name = "".join(p.capitalize() for p in name.split("_")) + "Plugin"
    return getattr(mod, cls_name)(config)


# --------------------------------------------------------------------------- #
# Indicator endpoints                                                          #
# --------------------------------------------------------------------------- #

async def indicator_info(request: web.Request) -> web.Response:
    body = await request.json()
    plugin = _load_indicator(body["name"], body["config"])
    return web.json_response({"required_periods": plugin.get_required_periods()})


# Max candles per test run keyed by timeframe — keeps memory bounded
_TEST_LIMITS: dict[str, int] = {
    "1m": 50_000,   # ~35 days
    "5m": 50_000,   # ~174 days
    "15m": 50_000,  # ~520 days
    "1h": 100_000,  # ~11 years
    "4h": 100_000,
    "1d": 100_000,
}
_TEST_LIMIT_DEFAULT = 50_000


async def indicator_compute(request: web.Request) -> web.Response:
    """
    Plugin-runner fetches candles from ClickHouse itself, computes, returns data.

    body: {name, config, exchange, symbol, timeframe, start_ts?, end_ts?, limit?}
    Returns: {data: [{timestamp, value}, ...]}
    """
    body     = await request.json()
    name     = body["name"]
    config   = body["config"]
    exchange = body["exchange"]
    symbol   = body["symbol"]
    tf       = body["timeframe"]
    start_ts = body.get("start_ts")
    end_ts   = body.get("end_ts")
    # Caller can opt out of limit (e.g. resolver full-compute) by passing limit=0
    req_limit = body.get("limit", -1)
    if req_limit == 0:
        limit = None  # no limit — full history
    elif req_limit > 0:
        limit = req_limit
    else:
        limit = _TEST_LIMITS.get(tf, _TEST_LIMIT_DEFAULT)

    plugin = _load_indicator(name, config)
    warmup = plugin.get_required_periods()

    candles = await _db.fetch_candles(
        exchange, symbol, tf, start_ts=start_ts, end_ts=end_ts, limit=limit
    )
    if not candles:
        return web.json_response({"data": []})

    results = await plugin.calculate_stream(candles, warmup)

    data = []
    for i, r in enumerate(results):
        if r is not None and r.get("value") is not None:
            data.append({
                "timestamp": candles[warmup + i]["timestamp"],
                "value":     float(r["value"]),
            })

    return web.json_response({"data": data, "candles_used": len(candles), "limited": limit is not None})


async def indicator_calculate_stream(request: web.Request) -> web.Response:
    """Legacy: caller passes candles explicitly. Kept for compatibility."""
    body = await request.json()
    plugin = _load_indicator(body["name"], body["config"])
    results = await plugin.calculate_stream(body["candles"], body["warmup"])
    return web.json_response({"results": results})


# --------------------------------------------------------------------------- #
# Strategy endpoints                                                           #
# --------------------------------------------------------------------------- #

async def strategy_get_required(request: web.Request) -> web.Response:
    body = await request.json()
    plugin = _load_strategy(body["name"], body["config"])
    required = plugin.get_required_indicators()
    return web.json_response({"required": required})


async def strategy_process_batch(request: web.Request) -> web.Response:
    body         = await request.json()
    plugin       = _load_strategy(body["name"], body["config"])
    candle_inputs = body["candle_inputs"]

    signals = []
    for inp in candle_inputs:
        try:
            signal = await plugin.process(
                indicators_data=inp["indicators_data"],
                current_price=inp["current_price"],
                signal_timestamp=inp.get("timestamp"),
            )
        except Exception as exc:
            logger.debug("Strategy process error at ts=%s: %s", inp.get("timestamp"), exc)
            signal = None
        signals.append(signal.to_dict() if signal is not None else None)

    return web.json_response({"signals": signals})


# --------------------------------------------------------------------------- #
# App factory                                                                  #
# --------------------------------------------------------------------------- #

_MAX_BODY = 64 * 1024 * 1024  # 64 MB fallback for legacy calculate_stream


async def health(_request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


def create_app() -> web.Application:
    app = web.Application(client_max_size=_MAX_BODY)
    app.router.add_get("/health",                      health)
    app.router.add_post("/indicator/info",             indicator_info)
    app.router.add_post("/indicator/compute",          indicator_compute)
    app.router.add_post("/indicator/calculate_stream", indicator_calculate_stream)
    app.router.add_post("/strategy/get_required",      strategy_get_required)
    app.router.add_post("/strategy/process_batch",     strategy_process_batch)
    return app


async def on_startup(app: web.Application) -> None:
    global _db
    import asyncio
    from core.clickhouse import create_clickhouse_manager
    _db = create_clickhouse_manager()
    for attempt in range(10):
        try:
            await _db.initialize()
            break
        except Exception as exc:
            if attempt == 9:
                raise
            logger.warning("ClickHouse not ready (attempt %d/10): %s — retrying in 3s", attempt + 1, exc)
            await asyncio.sleep(3)
    load_all_primitives()
    logger.info("plugin-runner ready (ClickHouse connected)")


if __name__ == "__main__":
    from core.logging_config import setup_service_logging
    setup_service_logging("plugin-runner")

    app = create_app()
    app.on_startup.append(on_startup)
    web.run_app(app, host="0.0.0.0", port=8082)
