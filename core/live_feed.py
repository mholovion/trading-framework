"""
LiveFeed — universal realtime candle broadcaster.

Loads exchange plugins (same mechanism as orchestrator) and uses the plugin's
start_realtime_stream() API to receive live candles.  All exchange-specific
logic stays in the plugins; LiveFeed only knows the abstract ExchangePlugin
interface.

Browser WS clients subscribe to a (exchange, symbol, timeframe) feed and
receive candle dicts via asyncio.Queue.
"""

import asyncio
import importlib
import logging
from typing import Any, Optional


class LiveFeed:

    def __init__(self, config_manager, logger: Optional[logging.Logger] = None):
        self._cfg     = config_manager
        self._log     = logger or logging.getLogger("LiveFeed")
        # conn_name → ExchangePlugin
        self._plugins: dict[str, Any]             = {}
        # conn_name → list of subscriber Queues
        self._subs:    dict[str, list[asyncio.Queue]] = {}
        # conn_name → asyncio.Task (stream keeper)
        self._tasks:   dict[str, asyncio.Task]    = {}

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def initialize(self) -> None:
        """Load all enabled realtime exchange plugins from config."""
        connections = self._cfg.get_config("connections").get("connections", {})
        exchanges   = self._cfg.get_config("main").get("exchanges", {})

        for conn_name, conn_cfg in connections.items():
            if not conn_cfg.get("enabled"):
                continue
            if not conn_cfg.get("realtime", {}).get("enabled"):
                continue
            exch_name = conn_cfg.get("exchange", "")
            exch_cfg  = exchanges.get(exch_name, {})
            if not exch_cfg.get("enabled"):
                continue

            merged      = {**exch_cfg, **conn_cfg}
            plugin_name = merged.get("plugin", "")
            try:
                mod  = importlib.import_module(f"plugins.exchanges.{plugin_name}_plugin")
                cls  = getattr(mod, f"{plugin_name.title()}Plugin")
                plug = cls(merged)
                # initialize() is now network-resilient (lazy connectivity)
                await plug.initialize()
                self._plugins[conn_name] = plug
                self._log.info(f"LiveFeed loaded plugin: {conn_name} ({plugin_name})")
            except Exception as exc:
                self._log.error(f"LiveFeed failed to load {conn_name}: {exc}")

    async def cleanup(self) -> None:
        for task in self._tasks.values():
            task.cancel()
        for plug in self._plugins.values():
            try:
                await plug.cleanup()
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    # Public subscribe/unsubscribe                                         #
    # ------------------------------------------------------------------ #

    async def subscribe(
        self, exchange: str, symbol: str, timeframe: str
    ) -> asyncio.Queue | None:
        """Return a Queue that receives candle dicts, or None if no plugin found."""
        conn_name = self._find_conn(exchange, symbol, timeframe)
        if conn_name is None:
            self._log.warning(
                f"LiveFeed: no plugin for {exchange}/{symbol}/{timeframe}"
            )
            return None

        q: asyncio.Queue = asyncio.Queue(maxsize=50)
        self._subs.setdefault(conn_name, []).append(q)

        # Start stream task on first subscriber
        if conn_name not in self._tasks or self._tasks[conn_name].done():
            self._tasks[conn_name] = asyncio.create_task(
                self._keep_stream(conn_name),
                name=f"live_feed_{conn_name}",
            )

        return q

    def unsubscribe(
        self, exchange: str, symbol: str, timeframe: str, q: asyncio.Queue
    ) -> None:
        conn_name = self._find_conn(exchange, symbol, timeframe)
        if conn_name is None:
            return
        lst = self._subs.get(conn_name, [])
        try:
            lst.remove(q)
        except ValueError:
            pass
        # Stop stream when no subscribers remain
        if not lst and conn_name in self._tasks:
            self._tasks.pop(conn_name).cancel()

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _find_conn(
        self, exchange: str, symbol: str, timeframe: str
    ) -> str | None:
        connections = self._cfg.get_config("connections").get("connections", {})
        for conn_name, conn_cfg in connections.items():
            if conn_name not in self._plugins:
                continue
            if (
                conn_cfg.get("exchange", "").lower() == exchange.lower()
                and conn_cfg.get("symbol", "").upper() == symbol.upper()
                and conn_cfg.get("source_timeframe", "") == timeframe
            ):
                return conn_name
        return None

    def _broadcast(self, conn_name: str, payload: dict) -> None:
        for q in self._subs.get(conn_name, []):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                # Drop oldest to make room for newest tick
                try:
                    q.get_nowait()
                    q.put_nowait(payload)
                except Exception:
                    pass

    async def _keep_stream(self, conn_name: str) -> None:
        """Start plugin stream and keep it alive; plugin handles reconnection."""
        plugin = self._plugins[conn_name]
        connections = self._cfg.get_config("connections").get("connections", {})
        conn_cfg = connections[conn_name]
        symbol    = conn_cfg["symbol"]
        timeframe = conn_cfg["source_timeframe"]

        async def on_candle(candle: dict) -> None:
            payload = {
                "time":   int(candle["timestamp"]),
                "open":   float(candle["open"]),
                "high":   float(candle["high"]),
                "low":    float(candle["low"]),
                "close":  float(candle["close"]),
                "volume": float(candle.get("volume", 0)),
            }
            self._broadcast(conn_name, payload)

        try:
            await plugin.start_realtime_stream(symbol, timeframe, on_candle)
            # Plugin started its own background task for the WS.
            # We just need to keep this coroutine alive so unsubscribe can
            # cancel us cleanly.
            while conn_name in self._subs and self._subs[conn_name]:
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._log.error(f"LiveFeed stream {conn_name} error: {exc}")
