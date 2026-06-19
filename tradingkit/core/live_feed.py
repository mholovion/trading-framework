"""
tradingkit.core.live_feed — pub-sub broker for real-time candle WebSocket clients.

DataCollector workers call publish() when they receive a live row.
WS handlers subscribe() to receive an asyncio.Queue of candle dicts.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any


class LiveFeed:

    def __init__(self, db: Any = None, logger: logging.Logger | None = None):
        self._db  = db
        self._log = logger or logging.getLogger("LiveFeed")
        # key: (symbol, timeframe) → list[asyncio.Queue]
        self._subs: dict[tuple[str, str], list[asyncio.Queue]] = {}

    async def initialize(self) -> None:
        self._log.info("LiveFeed ready (pub-sub mode)")

    async def cleanup(self) -> None:
        self._subs.clear()

    # ------------------------------------------------------------------ #
    # Called by DataCollector workers                                      #
    # ------------------------------------------------------------------ #

    def publish(self, symbol: str, timeframe: str, candle: dict) -> None:
        """Broadcast a candle dict to all WS subscribers for this stream."""
        key    = (symbol.upper(), timeframe)
        queues = self._subs.get(key, [])
        if not queues:
            return
        payload = {
            "time":   int(candle["timestamp"]),
            "open":   float(candle.get("open",   0)),
            "high":   float(candle.get("high",   0)),
            "low":    float(candle.get("low",    0)),
            "close":  float(candle.get("close",  0)),
            "volume": float(candle.get("volume", 0)),
        }
        for q in queues:
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                try:
                    q.get_nowait()
                    q.put_nowait(payload)
                except Exception:
                    pass

    # ------------------------------------------------------------------ #
    # Called by WS handlers                                                #
    # ------------------------------------------------------------------ #

    async def subscribe(
        self, exchange: str, symbol: str, timeframe: str
    ) -> asyncio.Queue:
        """Return a Queue that will receive live candle dicts."""
        key: tuple[str, str] = (symbol.upper(), timeframe)
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._subs.setdefault(key, []).append(q)
        self._log.debug(f"LiveFeed: subscribed {exchange}/{symbol}/{timeframe}")
        return q

    def unsubscribe(
        self, exchange: str, symbol: str, timeframe: str, q: asyncio.Queue
    ) -> None:
        key = (symbol.upper(), timeframe)
        lst = self._subs.get(key, [])
        try:
            lst.remove(q)
        except ValueError:
            pass
