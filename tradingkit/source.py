"""
tradingkit.source — DataSource ABC and ScriptSource.

Usage:
    from tradingkit import DataSource
    from tradingkit.source import ScriptSource
"""
from __future__ import annotations

import numpy as np
import polars as pl
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator


# ------------------------------------------------------------------ #
# DataSource ABC                                                       #
# ------------------------------------------------------------------ #

class DataSource(ABC):
    """
    Base class for all data sources (exchanges, CSV files, APIs, custom feeds...).

    Implement get_historical_data() to return a pl.DataFrame.
    Optionally implement stream() for real-time streaming.

    The framework calls get_historical_data() via executor.fetch_source_data().
    ScriptSource wraps user-provided code for the UI editor.
    """

    def __init__(self, config: dict | None = None) -> None:
        self.config = config or {}

    @abstractmethod
    async def get_historical_data(
        self,
        symbol: str,
        timeframe_seconds: int,
        start_ts: int,
        end_ts: int,
        limit: int = 5000,
    ) -> pl.DataFrame:
        """
        Fetch historical data for the given symbol and time range.

        Returns pl.DataFrame. The schema is defined by the source implementation.
        Pipeline consumers (IndicatorContext, BarContext) expect OHLCV columns
        (timestamp, open, high, low, close, volume) — sources used in a Pipeline
        must satisfy this contract.
        """
        ...

    async def stream(
        self,
        symbol: str,
        timeframe_seconds: int,
    ) -> AsyncIterator[dict]:
        """
        Real-time data stream. Yields row dicts.
        Override for live data sources. Default raises NotImplementedError.
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support streaming")

    async def initialize(self) -> bool:
        """Optional: initialize connection. Called before first use."""
        return True

    async def cleanup(self) -> None:
        """Optional: release resources."""

    async def health_check(self) -> dict:
        """Optional: connection health check."""
        return {"status": "ok"}

    def validate(self) -> bool:
        return True


# ------------------------------------------------------------------ #
# ScriptSource — for UI editor                                         #
# ------------------------------------------------------------------ #

class ScriptSource(DataSource):
    """
    Wraps user-provided source code for sandboxed execution via SubprocessExecutor.

    The code receives:
        symbol              — str
        timeframe_seconds   — int
        start_ts            — int  (unix seconds)
        end_ts              — int  (unix seconds)
        limit               — int
        pl                  — polars module
        parse_timeframe     — callable
        **params            — any extra parameters

    The code MUST assign to 'result' (pl.DataFrame):
        import requests
        resp = requests.get(...)
        result = pl.DataFrame(resp.json(), schema={...})

    Network access is whitelisted via allowed_hosts when run in SubprocessExecutor.

    Usage:
        source = ScriptSource(
            code="result = pl.read_csv('data.csv')",
            allowed_hosts=["api.example.com"],
        )
        df = await executor.fetch_source_data(source, "BTC/USDT", 3600, ...)
    """

    def __init__(
        self,
        code: str,
        allowed_hosts: list[str] | None = None,
        **params: Any,
    ) -> None:
        super().__init__()
        self._code = code
        self.allowed_hosts = allowed_hosts or []
        self.params = params

    async def get_historical_data(
        self,
        symbol: str,
        timeframe_seconds: int,
        start_ts: int,
        end_ts: int,
        limit: int = 5000,
    ) -> pl.DataFrame:
        from tradingkit.core.timeframe import parse_timeframe

        namespace: dict[str, Any] = {
            "symbol":            symbol,
            "timeframe_seconds": timeframe_seconds,
            "start_ts":          start_ts,
            "end_ts":            end_ts,
            "limit":             limit,
            "pl":                pl,
            "np":                np,
            "parse_timeframe":   parse_timeframe,
            "result":            None,
            **self.params,
        }
        exec(compile(self._code, "<ui_source>", "exec"), namespace)  # noqa: S102
        df = namespace.get("result")
        if df is None:
            raise ValueError("Source code must assign a pl.DataFrame to 'result'")
        if not isinstance(df, pl.DataFrame):
            raise TypeError(f"Source code 'result' must be pl.DataFrame, got {type(df)}")
        return df


# ------------------------------------------------------------------ #
# ConnectionScriptSource — runs user source scripts                    #
# ------------------------------------------------------------------ #

class ConnectionScriptSource:
    """
    Executes a user-written source script in a subprocess for safe isolation.

    The script must define one or both of:

        async def historical(symbol, timeframe, start_ts, end_ts, config) -> list[dict]:
            ...

        async def realtime(symbol, timeframe, config):  # AsyncGenerator
            yield {...}

    Optional top-level dict:

        __params__ = {
            "batch_size": {"type": "int", "default": 1440, "label": "Batch size"},
        }

    `config` is the merged dict of __params__ defaults + user-overridden values.
    """

    def __init__(self, code: str, config: dict | None = None) -> None:
        from tradingkit.core.script_ast import ScriptAST
        self._code   = code
        self._config = config or {}
        self._ast = ScriptAST(code)
        self._params_cache: dict | None = None

    # ------------------------------------------------------------------
    # __params__ extraction (parsed without execution — safe AST walk)
    # ------------------------------------------------------------------

    def extract_params(self) -> dict:
        """Return the __params__ dict declared in the script, or {}."""
        if self._params_cache is not None:
            return self._params_cache
        self._params_cache = self._ast.get_literal("__params__") or {}
        return self._params_cache

    def merged_config(self) -> dict:
        """__params__ defaults merged with user-supplied config values."""
        defaults = {k: v["default"] for k, v in self.extract_params().items() if "default" in v}
        return {**defaults, **self._config}

    # ------------------------------------------------------------------
    # historical — runs script's historical() in subprocess
    # ------------------------------------------------------------------

    async def historical(
        self,
        symbol: str,
        timeframe: str,
        start_ts: int,
        end_ts: int,
    ) -> "pl.DataFrame | list[dict]":
        """Execute script's historical() and return pl.DataFrame or list[dict]."""
        cfg = self.merged_config()
        namespace: dict = {}
        exec(compile(self._code, "<source_script>", "exec"), namespace)  # noqa: S102
        fn = namespace.get("historical")
        if fn is None:
            raise NotImplementedError("Source script has no historical() function")
        result = await fn(symbol, timeframe, start_ts, end_ts, cfg)
        if result is None:
            return []
        return result

    # ------------------------------------------------------------------
    # stream — runs script's realtime() as an async generator
    # ------------------------------------------------------------------

    async def stream(self, symbol: str, timeframe: str):
        """Yield row dicts from script's realtime() async generator."""
        cfg = self.merged_config()
        namespace: dict = {}
        exec(compile(self._code, "<source_script>", "exec"), namespace)  # noqa: S102
        fn = namespace.get("realtime")
        if fn is None:
            raise NotImplementedError("Source script has no realtime() function")
        async for row in fn(symbol, timeframe, cfg):
            yield row

    def has_historical(self) -> bool:
        return self._ast.has_function("historical")

    def has_realtime(self) -> bool:
        return self._ast.has_function("realtime")

    # ------------------------------------------------------------------
    # DataUnit / AggSpec — new-style source plugin declarations
    # ------------------------------------------------------------------

    def get_table_name(self) -> str | None:
        """Return TABLE_NAME declared at module level in the script (AST only, no exec)."""
        return self._ast.get_literal("TABLE_NAME")


__all__ = [
    "DataSource",
    "ScriptSource",
    "ConnectionScriptSource",
]
