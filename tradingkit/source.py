"""
tradingkit.source — DataSource ABC and ScriptSource.

Usage:
    from tradingkit import DataSource
    from tradingkit.source import ScriptSource
"""
from __future__ import annotations

import asyncio
import inspect
import itertools
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any

import numpy as np
import polars as pl

from tradingkit.core.timeframe import UNIT_SCALE

# ------------------------------------------------------------------ #
# DataSource ABC                                                       #
# ------------------------------------------------------------------ #

class DataSource(ABC):
    """
    Base class for all data sources (exchanges, CSV files, APIs, custom feeds...).

    Implement get_historical_data() to return a pl.DataFrame.
    Optionally implement stream() for real-time streaming.

    The framework calls get_historical_data() via executor.fetch_source_data().
    ScriptSource wraps user-provided code — a script may subclass DataSource directly
    to override any of the hooks below.

    `timeframe` throughout is an integer *step* in this source's own timestamp unit —
    not necessarily seconds (see timestamp_unit).
    """

    #: Unit of this source's `timestamp` column. The framework never uses it in
    #: arithmetic — gap/batch math is unit-relative, correct in any unit as long as
    #: timestamps and `timeframe` share it. It is consulted only where wall-clock time
    #: must be converted into data timestamps, and for display.
    timestamp_unit: str = "s"

    def __init__(self, config: dict | None = None) -> None:
        self.config = config or {}
        self._request_times: list[float] = []
        self._last_request: float = 0.0

    @abstractmethod
    async def get_historical_data(
        self,
        symbol: str,
        timeframe: int,
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
        timeframe: int,
    ) -> AsyncIterator[dict]:
        """
        Real-time data stream. Yields row dicts.
        Override for live data sources. Default raises NotImplementedError.
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support streaming")

    # ------------------------------------------------------------------ #
    # Gap recovery                                                         #
    # ------------------------------------------------------------------ #

    async def detect_gaps(self, rows: list[dict], timeframe: int) -> list[dict]:
        """
        Find missing bars in a timestamp-ordered row sequence at a fixed timeframe.

        Default: flag any gap between consecutive rows. Override for source-specific
        gap semantics (e.g. skip known exchange maintenance windows), or return []
        to disable gap detection for this source entirely.

        Returns a list of {"start_timestamp", "end_timestamp", "missing_rows"} dicts.
        """
        gaps: list[dict] = []
        for prev_row, row in itertools.pairwise(rows):
            # Integer division tolerates small jitter and delta<=0 (e.g. a source
            # re-pushing the same still-forming bar) without a separate tolerance param.
            missing = int(row["timestamp"] - prev_row["timestamp"]) // timeframe - 1
            if missing > 0:
                gaps.append({
                    "start_timestamp": prev_row["timestamp"] + timeframe,
                    "end_timestamp":   row["timestamp"] - timeframe,
                    "missing_rows":    missing,
                })
        return gaps

    def batch_gaps(self, gaps: list[dict]) -> list[dict]:
        """
        Merge nearby gaps into one fetch, split apart gaps too big or too far apart to
        fetch in one request.

        Default reads gap_max_batch/gap_max_time_gap from self.config. Override for
        source-specific batching, e.g. an exchange with a tight rate limit wanting
        smaller batches.
        """
        if not gaps:
            return []
        max_batch    = self.config.get("gap_max_batch", 1440)
        max_time_gap = self.config.get("gap_max_time_gap", 3600)
        sorted_gaps = sorted(gaps, key=lambda g: g["start_timestamp"])
        batches: list[dict] = []
        cur = {
            "start_timestamp": sorted_gaps[0]["start_timestamp"],
            "end_timestamp":   sorted_gaps[0]["end_timestamp"],
            "total_rows":      sorted_gaps[0]["missing_rows"],
        }
        for gap in sorted_gaps[1:]:
            time_gap = gap["start_timestamp"] - cur["end_timestamp"]
            if (cur["total_rows"] + gap["missing_rows"] <= max_batch
                    and time_gap <= max_time_gap):
                cur["end_timestamp"] = gap["end_timestamp"]
                cur["total_rows"]   += gap["missing_rows"]
            else:
                batches.append(cur)
                cur = {
                    "start_timestamp": gap["start_timestamp"],
                    "end_timestamp":   gap["end_timestamp"],
                    "total_rows":      gap["missing_rows"],
                }
        batches.append(cur)
        return batches

    async def fetch_gap(
        self, symbol: str, timeframe: int, start_ts: int, end_ts: int,
    ) -> list[dict]:
        """
        Backfill one already-batched gap range with real data.

        Default: rate-limit, then re-fetch via get_historical_data() — not synthetic
        data. NaN-fill and forward-fill were both ruled out: NaN permanently poisons
        every subsequent indicator output, and forward-fill invents prices that never
        happened. Override to backfill from a different source, or return [] to disable
        backfill for this source (the gap is then left unfilled — no worse than before
        gap detection existed, just not fixed).
        """
        await self.rate_limit()
        df = await self.get_historical_data(symbol, timeframe, start_ts, end_ts)
        return df.to_dicts()

    async def rate_limit(self) -> None:
        """
        Throttle outbound requests. Rate limits are a property of the source (the
        exchange), not of whoever calls it — so DataCollector and Pipeline both get
        them from here instead of each re-implementing throttling.

        Reads max_requests / per_seconds / rate_limit_ms from self.config. Override for
        a different policy (e.g. the weight-based limits some exchanges use).
        """
        max_requests  = int(self.config.get("max_requests", 10000))
        per_seconds   = int(self.config.get("per_seconds", 10))
        rate_limit_ms = float(self.config.get("rate_limit_ms", 50))

        now = time.time()
        self._request_times = [t for t in self._request_times if now - t <= per_seconds]
        if len(self._request_times) >= max_requests:
            wait = per_seconds - (now - min(self._request_times))
            if wait > 0:
                await asyncio.sleep(wait)
        min_delay = rate_limit_ms / 1000.0
        elapsed = now - self._last_request
        if elapsed < min_delay:
            await asyncio.sleep(min_delay - elapsed)
        now = time.time()
        self._request_times.append(now)
        self._last_request = now

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

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
# ScriptSource — wraps user-written source code                        #
# ------------------------------------------------------------------ #

class ScriptSource(DataSource):
    """
    Wraps user-provided source code, run via an executor (SubprocessExecutor for
    sandboxing, LocalExecutor in-process).

    Three script conventions, checked in this order:

    1. Class convention — full control. The script subclasses DataSource, so it can
       override *any* hook (detect_gaps, fetch_gap, rate_limit, batch_gaps,
       timestamp_unit), not just fetching:

        class MySource(DataSource):
            timestamp_unit = "ms"
            async def get_historical_data(self, symbol, timeframe, start_ts, end_ts, limit=5000): ...
            async def detect_gaps(self, rows, timeframe): ...      # own gap rules

       ScriptSource instantiates it once and delegates to it — inheritance on the
       outside so Pipeline/DataCollector accept it anywhere, composition on the inside
       so the user's own class does the work.

    2. Function convention — historical/realtime at module level:

        async def historical(symbol, timeframe, start_ts, end_ts, config) -> list[dict]: ...
        async def realtime(symbol, timeframe, config):   # AsyncGenerator
            yield {...}

    3. Expression convention — quick UI-editor sources. The code MUST assign 'result',
       and receives symbol / timeframe / start_ts / end_ts / limit / pl / np /
       parse_timeframe / **params in its namespace:

        result = pl.DataFrame(requests.get(...).json(), schema={...})

    Optional module-level declarations, read by AST without executing the script:

        TABLE_NAME = "unit_whitebit"
        __params__ = {"batch_size": {"type": "int", "default": 1440, "label": "Batch size"}}

    `config` passed to conventions 1 and 2 is __params__ defaults merged with
    user-supplied config. Network access is whitelisted via allowed_hosts under
    SubprocessExecutor.
    """

    def __init__(
        self,
        code: str,
        config: dict | None = None,
        allowed_hosts: list[str] | None = None,
        **params: Any,
    ) -> None:
        super().__init__(config)
        self._code = code
        self._params_cache: dict | None = None
        self._ast_cache: Any = None
        self._ns: dict | None = None
        self._ns_code: str | None = None
        self._impl: DataSource | None = None
        self.allowed_hosts = allowed_hosts or []
        self.params = params

    @property
    def _ast(self) -> Any:
        """Parsed AST of the script, built on demand. Derived state rather than a
        constructor field so it never travels through pickle — tradingkit-runner's
        unpickling allowlist is a security boundary, and widening it for a cache that
        rebuilds from _code in microseconds would be the wrong trade."""
        from tradingkit.core.script_ast import ScriptAST
        if self._ast_cache is None or self._ast_cache._code != self._code:
            self._ast_cache = ScriptAST(self._code)
        return self._ast_cache

    # ------------------------------------------------------------------ #
    # Preparation — compile+exec once, not per call                        #
    # ------------------------------------------------------------------ #

    def _is_module_style(self) -> bool:
        """Whether the script is safe to exec at preparation time. Conventions 1 and 2
        define classes/functions and are; convention 3 reads injected variables
        (symbol, start_ts, ...) at module level, so its exec must stay deferred until
        those exist."""
        return (
            self._ast.has_class(base="DataSource")
            or self._ast.has_function("historical")
            or self._ast.has_function("realtime")
        )

    def _build(self) -> None:
        """One-time preparation: compile, exec, and instantiate the script's DataSource
        subclass if it defines one — previously every historical()/stream() call
        recompiled the script from scratch. This is also where a compiled-language
        source would do its compilation.

        Isolation comes from the executor, not from here: under SubprocessExecutor the
        source is pickled and __getstate__ drops this cache, so both the exec and the
        instantiation happen inside the subprocess; under LocalExecutor they happen
        in-process, exactly like the plain exec() this replaces.
        """
        from tradingkit.core.timeframe import parse_timeframe

        ns: dict[str, Any] = {
            "DataSource":      DataSource,
            "pl":              pl,
            "np":              np,
            "parse_timeframe": parse_timeframe,
            **self.params,
        }
        exec(compile(self._code, "<source_script>", "exec"), ns)  # noqa: S102
        self._ns, self._ns_code = ns, self._code
        self._impl = self._instantiate(ns)

    def _instantiate(self, ns: dict) -> DataSource | None:
        """Build the script's own DataSource, if it declares one. An abstract class (a
        missing get_historical_data) raises Python's own clear TypeError — deliberately
        not swallowed, since a silent fallback would hide the mistake."""
        classes = [
            v for v in ns.values()
            if isinstance(v, type) and issubclass(v, DataSource) and v is not DataSource
        ]
        if not classes:
            return None
        # A script may define a shared base plus a concrete subclass; dict order would
        # otherwise decide which one wins, so take the most-derived explicitly.
        cls = max(classes, key=lambda c: len(c.__mro__))
        takes_config = len(inspect.signature(cls.__init__).parameters) > 1
        return cls(self.merged_config()) if takes_config else cls()

    def _namespace(self) -> dict:
        """Prepared module namespace (function/expression conventions)."""
        if self._ns is None or self._ns_code != self._code:
            self._build()
        assert self._ns is not None
        return self._ns

    def _impl_or_none(self) -> DataSource | None:
        """The script's own DataSource instance, or None if it isn't class-style."""
        if not self._is_module_style():
            return None
        if self._ns is None or self._ns_code != self._code:
            self._build()
        return self._impl

    def __getstate__(self) -> dict:
        # The namespace and the instantiated impl can hold unpicklable objects (HTTP
        # sessions, sockets), and SubprocessExecutor pickles the source on every call —
        # neither may travel; both are rebuilt from _code on the other side.
        state = self.__dict__.copy()
        state["_ns"] = state["_ns_code"] = state["_impl"] = None
        state["_ast_cache"] = None
        return state

    # ------------------------------------------------------------------ #
    # __params__ / TABLE_NAME — parsed without execution (safe AST walk)   #
    # ------------------------------------------------------------------ #

    def extract_params(self) -> dict:
        """Return the __params__ dict declared in the script, or {}."""
        if self._params_cache is not None:
            return self._params_cache
        self._params_cache = self._ast.get_literal("__params__") or {}
        return self._params_cache

    def merged_config(self) -> dict:
        """__params__ defaults merged with user-supplied config values."""
        defaults = {k: v["default"] for k, v in self.extract_params().items() if "default" in v}
        return {**defaults, **self.config}

    def get_table_name(self) -> str | None:
        """TABLE_NAME declared at module level, or as an attribute on a class-style source."""
        name = self._ast.get_literal("TABLE_NAME")
        if name is None and (impl := self._impl_or_none()) is not None:
            name = getattr(impl, "TABLE_NAME", None)
        return name

    def has_historical(self) -> bool:
        if self._impl_or_none() is not None:
            return True          # get_historical_data is abstract — always implemented
        return self._ast.has_function("historical")

    def has_realtime(self) -> bool:
        if (impl := self._impl_or_none()) is not None:
            return type(impl).stream is not DataSource.stream
        return self._ast.has_function("realtime")

    # ------------------------------------------------------------------ #
    # Data access                                                          #
    # ------------------------------------------------------------------ #

    @property
    def timestamp_unit(self) -> str:  # type: ignore[override]
        impl = self._impl_or_none()
        return impl.timestamp_unit if impl is not None else "s"

    def _tf_arg(self, timeframe: int) -> str | int:
        """Scripts written against the original signature expect a string ("1m"), so
        convert for timestamp_unit="s" where seconds_to_tf_string() is defined; other
        units pass the integer step through unchanged."""
        from tradingkit.core.timeframe import seconds_to_tf_string
        return seconds_to_tf_string(timeframe) if self.timestamp_unit == "s" else timeframe

    async def get_historical_data(
        self,
        symbol: str,
        timeframe: int,
        start_ts: int,
        end_ts: int,
        limit: int = 5000,
    ) -> pl.DataFrame:
        if (impl := self._impl_or_none()) is not None:
            return await impl.get_historical_data(symbol, timeframe, start_ts, end_ts, limit)
        if self._ast.has_function("historical"):
            fn = self._namespace()["historical"]
            result = await fn(
                symbol, self._tf_arg(timeframe), start_ts, end_ts, self.merged_config()
            )
            return result if isinstance(result, pl.DataFrame) else pl.DataFrame(result or [])
        return self._eval_result(symbol, timeframe, start_ts, end_ts, limit)

    def _eval_result(
        self, symbol: str, timeframe: int, start_ts: int, end_ts: int, limit: int,
    ) -> pl.DataFrame:
        """Expression convention: exec with a fresh namespace, since the script reads
        symbol/start_ts/... as injected variables — a per-call namespace is inherent
        here, so this path deliberately doesn't use the prepared cache."""
        from tradingkit.core.timeframe import parse_timeframe

        namespace: dict[str, Any] = {
            "symbol":          symbol,
            "timeframe":       timeframe,
            "start_ts":        start_ts,
            "end_ts":          end_ts,
            "limit":           limit,
            "pl":              pl,
            "np":              np,
            "parse_timeframe": parse_timeframe,
            "result":          None,
            **self.params,
        }
        exec(compile(self._code, "<ui_source>", "exec"), namespace)  # noqa: S102
        df = namespace.get("result")
        if df is None:
            # Reached when the script matches none of the three conventions, so name all
            # of them -- "must assign result" alone is misleading for someone who meant
            # to write a connection script and mistyped historical().
            raise ValueError(
                "Source script provides no data: assign a pl.DataFrame to 'result', "
                "define an async historical() function, or subclass DataSource"
            )
        if not isinstance(df, pl.DataFrame):
            raise TypeError(f"Source code 'result' must be pl.DataFrame, got {type(df)}")
        return df

    async def stream(self, symbol: str, timeframe: int):
        """Yield row dicts from the script's own stream()/realtime()."""
        if (impl := self._impl_or_none()) is not None:
            async for row in impl.stream(symbol, timeframe):
                yield row
            return
        fn = self._namespace().get("realtime")
        if fn is None:
            raise NotImplementedError("Source script has no realtime() function")
        async for row in fn(symbol, self._tf_arg(timeframe), self.merged_config()):
            yield row

    # ------------------------------------------------------------------ #
    # Gap recovery / throttling — delegate to the user's class if present  #
    # ------------------------------------------------------------------ #

    async def detect_gaps(self, rows: list[dict], timeframe: int) -> list[dict]:
        if (impl := self._impl_or_none()) is not None:
            return await impl.detect_gaps(rows, timeframe)
        return await super().detect_gaps(rows, timeframe)

    def batch_gaps(self, gaps: list[dict]) -> list[dict]:
        if (impl := self._impl_or_none()) is not None:
            return impl.batch_gaps(gaps)
        return super().batch_gaps(gaps)

    async def fetch_gap(
        self, symbol: str, timeframe: int, start_ts: int, end_ts: int,
    ) -> list[dict]:
        if (impl := self._impl_or_none()) is not None:
            return await impl.fetch_gap(symbol, timeframe, start_ts, end_ts)
        await self.rate_limit()
        df = await self.get_historical_data(symbol, timeframe, start_ts, end_ts)
        return df.to_dicts()

    async def rate_limit(self) -> None:
        if (impl := self._impl_or_none()) is not None:
            await impl.rate_limit()
            return
        await super().rate_limit()

    # ------------------------------------------------------------------ #
    # Deprecated                                                           #
    # ------------------------------------------------------------------ #

    async def historical(
        self,
        symbol: str,
        timeframe: str | int,
        start_ts: int,
        end_ts: int,
    ) -> pl.DataFrame | list[dict]:
        """Deprecated: use get_historical_data(). Kept so callers written against the
        old ConnectionScriptSource API keep working."""
        from tradingkit.core.timeframe import parse_timeframe
        return await self.get_historical_data(
            symbol, parse_timeframe(timeframe), start_ts, end_ts
        )


#: Deprecated alias — ScriptSource now covers the connection-script convention too.
ConnectionScriptSource = ScriptSource


__all__ = [
    "ConnectionScriptSource",
    "DataSource",
    "ScriptSource",
    "UNIT_SCALE",
]
