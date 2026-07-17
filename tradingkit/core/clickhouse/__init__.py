"""
tradingkit.core.clickhouse — async ClickHouse client.

Split into focused mixins by responsibility (candles, indicators/signals, connections,
plugin_library, generic unit tables, low-level SQL) instead of one large class body —
see the individual _*.py modules. ClickHouseManager composes all of them; its public
(and the handful of underscore-prefixed but externally-relied-on) methods keep their
exact names and signatures, so nothing importing `tradingkit.core.clickhouse` needs to
change.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from tradingkit.core.clickhouse._candles import _CandlesMixin, _to_ch_interval
from tradingkit.core.clickhouse._connections import _ConnectionsMixin
from tradingkit.core.clickhouse._indicators_signals import _IndicatorsSignalsMixin
from tradingkit.core.clickhouse._plugin_library import _PluginLibraryMixin
from tradingkit.core.clickhouse._sql import (
    _SqlMixin,
    _basic_auth_header,
    _validate_identifier,
    _validates_identifiers,
)
from tradingkit.core.clickhouse._unit_tables import _UnitTablesMixin

logger = logging.getLogger(__name__)

TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "12h": 43200,
    "1d": 86400, "1w": 604800,
}

_FLUSH_INTERVAL = 0.5


class ClickHouseManager(
    _CandlesMixin,
    _IndicatorsSignalsMixin,
    _ConnectionsMixin,
    _PluginLibraryMixin,
    _UnitTablesMixin,
    _SqlMixin,
):
    """
    Async ClickHouse client with write-side batch buffering.
    Uses HTTP interface (default port 8123, see http_port=) for queries, authenticated
    with user/password; native protocol (port 9000) for bulk inserts.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9000,
        database: str = "default",
        user: str = "default",
        password: str = "",
        http_port: int = 8123,
    ) -> None:
        self.host      = host
        self.port      = port
        self.database  = database
        self.user      = user
        self.password  = password
        self.http_port = http_port

        self._conn: Any = None
        self._lock = asyncio.Lock()
        self._indicator_buffer: list[tuple] = []
        self._signal_buffer:    list[tuple] = []
        self._flush_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def initialize(self) -> None:
        try:
            import asynch
            self._conn = asynch.Connection(
                host=self.host, port=self.port,
                database=self.database, user=self.user, password=self.password,
            )
            await self._conn.connect()
            logger.info(f"ClickHouse connected: {self.host}:{self.port}/{self.database}")
        except ImportError:
            logger.warning("asynch not installed — ClickHouse unavailable")
            return
        except Exception as e:
            logger.error(f"ClickHouse connection failed: {e}")
            raise
        self._flush_task = asyncio.create_task(self._auto_flush_loop())

    async def close(self) -> None:
        if self._flush_task:
            self._flush_task.cancel()
        await self.flush()
        if self._conn:
            await self._conn.close()

    async def _auto_flush_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(_FLUSH_INTERVAL)
                await self.flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Auto-flush error: {e}")


def create_clickhouse_manager(config: dict | None = None) -> ClickHouseManager:
    import os
    cfg = config or {}
    return ClickHouseManager(
        host      = os.getenv("CLICKHOUSE_HOST")      or cfg.get("host",      "localhost"),
        port      = int(os.getenv("CLICKHOUSE_PORT")  or cfg.get("port",      9000)),
        database  = os.getenv("CLICKHOUSE_DATABASE")  or cfg.get("database",  "default"),
        user      = os.getenv("CLICKHOUSE_USER")      or cfg.get("user",      "default"),
        password  = os.getenv("CLICKHOUSE_PASSWORD", cfg.get("password",   "")),
        http_port = int(os.getenv("CLICKHOUSE_HTTP_PORT") or cfg.get("http_port", 8123)),
    )


__all__ = [
    "ClickHouseManager",
    "TIMEFRAME_SECONDS",
    "create_clickhouse_manager",
    "_to_ch_interval",
    "_validate_identifier",
    "_validates_identifiers",
    "_basic_auth_header",
]