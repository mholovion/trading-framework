"""
tradingkit.context — TradingContext dependency injection container.

Usage in app.py:
    tk = TradingContext(db=db, resolver=resolver, executor=executor)
    app["tk"] = tk

Usage in routes:
    tk: TradingContext = request.app["tk"]
    candles = await tk.db.fetch_ohlcv_aggregated(...)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from tradingkit.core.clickhouse import ClickHouseManager
    from tradingkit.core.resolver import DependencyResolver
    from tradingkit.executor.base import PluginExecutor


@dataclass
class TradingContext:
    """
    Dependency injection container for the trading framework.

    Created once in app.py and stored on the aiohttp Application:
        app["tk"] = TradingContext(db=db, resolver=resolver, executor=executor)

    Accessed in every route via:
        tk: TradingContext = request.app["tk"]
    """
    db:       "ClickHouseManager"
    resolver: "DependencyResolver"
    executor: "PluginExecutor"
    config:   dict = field(default_factory=dict)
    extra:    dict = field(default_factory=dict)
