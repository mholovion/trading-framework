"""
tradingkit.aggregation — AggregationContext, Aggregation, ScriptAggregation, AggregationWorker.

AggregationContext  — project-level object passed to Aggregation.combine(ctx, start_ts, end_ts).
                      Provides query() access to ANY table in ClickHouse.

Aggregation         — declarative cross-source aggregation (like Strategy declares
                      Indicators via class attributes). Declare SourceRef class attributes,
                      then either:
                        combine_sql() -> str   — SQL expression over the aliases, computed
                                                  entirely in ClickHouse via a gap-closing
                                                  Materialized View (see setup_aggregation) —
                                                  no polling, no Python roundtrip.
                        combine(ctx, start_ts, end_ts) -> list[dict] — Python fallback for
                                                  logic SQL can't express (windows, joins
                                                  across more than the declared sources,
                                                  external state). Same ctx.query() access as
                                                  the old aggregate(ctx, ...) free-function
                                                  convention this replaces — a strict
                                                  superset, not a narrower callback.
                      A class defining combine_sql() needs no worker at all: setup_aggregation()
                      installs the MV once and ClickHouse keeps it live forever. A class using
                      only combine() is driven by AggregationWorker on a timer, same as before.

ScriptAggregation   — wraps a code string (from plugin_library, SaaS UI editor) the same way
                      ScriptStrategy/ScriptIndicator wrap user code — dynamic equivalent of a
                      hand-written Aggregation subclass, loaded via load_aggregation_plugin().

AggregationWorker   — background task that periodically drives Aggregation/ScriptAggregation
                      instances whose combine_sql() is None (the ones combine_sql() covers
                      need it exactly zero times — ClickHouse's own MV mechanism keeps them
                      current on every insert).

AggregationScript   — NOT the same thing as Aggregation above, despite the name: this is the
                      *within-table* bucket-aggregation script wrapper (`aggregation(unit) ->
                      AggSpec`, consumed by DataCollector to auto-create per-connection
                      candles_60s/_3600s Materialized Views — tradingkit/schema.py's Fold
                      machinery). Cross-table aggregate(ctx, start_ts, end_ts) support has
                      been removed from this class in favor of Aggregation/ScriptAggregation
                      above; the within-table half is unrelated and unchanged.
"""
from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC
from typing import Any

import polars as pl

from tradingkit.core.clickhouse import ClickHouseManager

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 60  # seconds between AggregationWorker runs


# ---------------------------------------------------------------------------
# AggregationContext
# ---------------------------------------------------------------------------

class AggregationContext:
    """
    Project-level query context passed to Aggregation.combine(ctx, start_ts, end_ts).

    Provides access to ALL tables in ClickHouse via query().
    Scripts should use this to fetch any raw or pre-aggregated table.

    Example:
        btc = await ctx.query("candles", symbol="BTC_USDT", start_ts=start_ts, end_ts=end_ts)
        eth = await ctx.query("candles", symbol="ETH_USDT", start_ts=start_ts, end_ts=end_ts)
        joined = btc.join(eth, on="timestamp", suffix="_eth")
    """

    def __init__(self, db: ClickHouseManager, exchange: str) -> None:
        self._db       = db
        self._exchange = exchange

    async def query(
        self,
        table: str,
        symbol: str | None = None,
        start_ts: int | None = None,
        end_ts:   int | None = None,
    ) -> pl.DataFrame:
        """
        Fetch from any ClickHouse table with optional symbol and time filters.

        For raw unit tables (ReplacingMergeTree): plain SELECT *.
        For aggregating tables, the script is responsible for using the right table name
        (e.g. 'candles_3600s' for pre-computed 1h buckets).
        """
        return await self._db.fetch_from_unit_table(
            table, self._exchange, symbol, start_ts, end_ts
        )


# ---------------------------------------------------------------------------
# SourceRef + Aggregation — declarative cross-source aggregation
# ---------------------------------------------------------------------------

class SourceRef:
    """
    Declares one source a cross-source Aggregation reads from — a ClickHouse table plus
    which column to combine (usually a raw candles-shaped table, but any table with a
    timestamp column works).

        btc = SourceRef("candles_btc_usdt", field="close")
    """

    def __init__(self, table: str, field: str = "close", ch_type: str = "Float64") -> None:
        self.table   = table
        self.field   = field
        self.ch_type = ch_type


class Aggregation(ABC):
    """
    Declarative cross-source aggregation. Subclass and declare SourceRef class attributes
    (the same pattern Strategy uses for Indicator declarations), then implement either
    combine_sql() or combine() — see the module docstring for the tradeoff between them.

        class BtcEthSpread(Aggregation):
            OUTPUT_TABLE = "btc_eth_spread"
            btc = SourceRef("candles_btc_usdt", field="close")
            eth = SourceRef("candles_eth_usdt", field="close")

            def combine_sql(self) -> str:
                return "btc - eth"
    """

    OUTPUT_TABLE: str

    @classmethod
    def required_sources(cls) -> dict[str, SourceRef]:
        """Auto-collect all SourceRef class attributes — same __mro__/vars() scan as
        Strategy.required_indicators()."""
        result: dict[str, SourceRef] = {}
        for klass in reversed(cls.__mro__):
            for k, v in vars(klass).items():
                if isinstance(v, SourceRef):
                    result[k] = v
        return result

    def combine_sql(self) -> str | None:
        """
        SQL expression over the declared aliases (e.g. "btc - eth"), computed entirely in
        ClickHouse — no Python roundtrip. None (default) means there's no SQL-native path;
        use combine() instead.
        """
        return None

    def output_schema(self) -> dict[str, str] | None:
        """Optional explicit {column: ClickHouse type} for combine()'s output rows, cast
        before insertion. None (default) lets Polars infer types from the returned dicts."""
        return None

    async def combine(self, ctx: AggregationContext, start_ts: int, end_ts: int) -> list[dict] | None:
        """
        Python fallback for logic combine_sql() can't express — deliberately the same
        signature as the aggregate(ctx, start_ts, end_ts) free-function convention this
        replaces (full ctx.query() access, not a single merged row), so nothing that used
        to be possible stops being possible. Only called when combine_sql() returns None;
        driven by AggregationWorker on a timer rather than reactively.
        """
        return None


# ---------------------------------------------------------------------------
# ScriptAggregation — dynamic plugin, mirrors ScriptStrategy/ScriptIndicator
# ---------------------------------------------------------------------------

class ScriptAggregation(Aggregation):
    """
    Wraps a code string (from plugin_library / a UI editor) the same way ScriptStrategy
    wraps strategy code — the dynamic equivalent of hand-writing an Aggregation subclass,
    with no framework release needed to create a new one.

        OUTPUT_TABLE = "btc_eth_spread"
        SOURCES = {
            "btc": SourceRef("candles_btc_usdt", field="close"),
            "eth": SourceRef("candles_eth_usdt", field="close"),
        }
        COMBINE_SQL = "btc - eth"
    """

    def __init__(self, code: str, config: dict | None = None) -> None:
        self._code = code
        self.config = config or {}
        self._exec_ns()

    def _exec_ns(self) -> None:
        # Same pattern as ScriptStrategy._exec_ns() — execute once, read from self._ns
        # afterward. Unlike ScriptStrategy, no NameError/AttributeError swallowing here:
        # a valid aggregation script has no forward references to values injected later
        # (SourceRef is injected up front, not deferred), so a failure here means the
        # script itself is broken and should fail loudly.
        self._ns: dict[str, Any] = {"SourceRef": SourceRef}
        exec(compile(self._code, "<aggregation_script>", "exec"), self._ns)  # noqa: S102

    def __getstate__(self) -> dict:
        # Same __builtins__-pickling fix as ScriptStrategy.__getstate__ — _ns is a derived
        # cache of self._code, rebuilt in __setstate__ instead of pickled.
        state = self.__dict__.copy()
        state.pop("_ns", None)
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._exec_ns()

    @property
    def OUTPUT_TABLE(self) -> str:  # matches the base class's class-attribute name
        return self._ns["OUTPUT_TABLE"]

    def required_sources(self) -> dict[str, SourceRef]:
        return self._ns.get("SOURCES", {})

    def combine_sql(self) -> str | None:
        return self._ns.get("COMBINE_SQL")

    def output_schema(self) -> dict[str, str] | None:
        return self._ns.get("OUTPUT_SCHEMA")

    async def combine(self, ctx: AggregationContext, start_ts: int, end_ts: int) -> list[dict] | None:
        fn = self._ns.get("combine")
        if not callable(fn):
            return None
        result = fn(ctx, start_ts, end_ts)
        if asyncio.iscoroutine(result):
            result = await result
        return result


def load_aggregation_plugin(type_: str, params_dict: dict) -> Aggregation:
    """
    Create an Aggregation plugin from a type string and params dict — same dispatch shape
    as load_strategy_plugin/load_indicator_plugin.

    '__script__'  — ScriptAggregation wrapping user code (from '_code' param).
    named types   — looks up in a host-registered builtin registry, see
                    tradingkit.core.plugin_registry.
    """
    if type_ == "__script__":
        code = params_dict.get("_code", "")
        if not code:
            raise ValueError("Aggregation type '__script__' requires '_code' in params")
        return ScriptAggregation(code=code, config=params_dict)

    from tradingkit.core.plugin_registry import get_builtin_registry
    builtins_ = get_builtin_registry("TRADINGKIT_AGGREGATIONS_MODULE", "BUILTIN_AGGREGATIONS")
    code = builtins_.get(type_)

    if not code:
        raise ValueError(
            f"Unknown aggregation type {type_!r}. "
            f"Use '__script__' with '_code', or a registered builtin name."
        )
    return ScriptAggregation(code=code, config=params_dict)


# ---------------------------------------------------------------------------
# setup_aggregation / query_aggregation — orchestration for Aggregation instances
# ---------------------------------------------------------------------------

async def setup_aggregation(db: ClickHouseManager, agg: Aggregation) -> None:
    """
    One-time idempotent setup for an Aggregation instance.

    combine_sql() defined  — installs the gap-closing alignment table + one Materialized
                             View per declared source (ensure_cross_source_table/
                             ensure_cross_source_mv), then backfills each source's existing
                             rows (backfill_cross_source) — MVs are not retroactive, they
                             only fire on rows inserted after the MV itself exists, so
                             setting this up on top of already-populated source tables
                             would otherwise silently have no historical data. ClickHouse
                             keeps the result live from here on; call this once (e.g. at
                             app startup, or when a new aggregation is created via the SaaS
                             UI), never poll again.
    combine_sql() is None  — just ensures OUTPUT_TABLE exists for AggregationWorker to
                             write into; the actual periodic computation happens there.
    """
    sources = agg.required_sources()
    sql = agg.combine_sql()

    if sql is not None:
        await db.ensure_cross_source_table(
            agg.OUTPUT_TABLE, {alias: ref.ch_type for alias, ref in sources.items()}
        )
        for alias, ref in sources.items():
            await db.ensure_cross_source_mv(
                agg.OUTPUT_TABLE, ref.table, alias, ref.field, ref.ch_type
            )
            await db.backfill_cross_source(
                agg.OUTPUT_TABLE, ref.table, alias, ref.field, ref.ch_type
            )
        return

    schema = agg.output_schema()
    if schema:
        _PL_MAP = {
            "Int64":   pl.Int64,   "UInt64":  pl.UInt64,
            "Float64": pl.Float64, "Float32": pl.Float32,
            "String":  pl.String,
        }
        pl_schema = {k: _PL_MAP.get(v, pl.String) for k, v in schema.items()}
        await db.ensure_raw_table(agg.OUTPUT_TABLE, pl_schema)
    # Without an explicit output_schema(), OUTPUT_TABLE gets created lazily on the first
    # real combine() result — same as AggregationWorker._run_one() does today.


async def query_aggregation(
    db: ClickHouseManager,
    agg: Aggregation,
    start_ts: int,
    end_ts: int,
    exchange: str = "",
) -> list[dict]:
    """
    Read an Aggregation's result — the single entry point regardless of which path
    computed it.

    combine_sql() defined — query_cross_source() against the alignment table, computed at
                            read time, always fresh, only_complete=True (drops rows where a
                            source hasn't arrived yet rather than surfacing a misleading
                            NULL/0 unannounced).
    combine_sql() is None — plain AggregationContext.query() against OUTPUT_TABLE, i.e.
                            whatever AggregationWorker last stored (fresh to within
                            _POLL_INTERVAL, same staleness bound as before).
    """
    sql = agg.combine_sql()
    if sql is not None:
        aliases = list(agg.required_sources())
        return await db.query_cross_source(agg.OUTPUT_TABLE, aliases, sql, start_ts, end_ts)

    df = await AggregationContext(db, exchange).query(agg.OUTPUT_TABLE, start_ts=start_ts, end_ts=end_ts)
    return df.to_dicts()


# ---------------------------------------------------------------------------
# AggregationWorker
# ---------------------------------------------------------------------------

class AggregationWorker:
    """
    Background worker driving Aggregation/ScriptAggregation instances whose combine_sql()
    is None — the ones combine_sql() covers need this worker exactly zero times, since
    ClickHouse's own Materialized View mechanism keeps them current on every insert. As
    more aggregations move to combine_sql(), this worker naturally has less to do each
    cycle — that's an emergent property of the skip in _run_one(), not special-cased.

    Loads all scripts from plugin_library where type='aggregation' (skipping the
    unrelated within-table aggregation(unit) scripts DataCollector already handles — see
    AggregationScript.is_ch_mv()). Runs each combine_sql()-less one periodically, stores
    results into OUTPUT_TABLE.

    Usage (app.py):
        worker = AggregationWorker(db)
        await worker.start()
        ...
        await worker.stop()
    """

    def __init__(self, db: ClickHouseManager) -> None:
        self._db    = db
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run_loop(), name="aggregation-worker")
        logger.info("AggregationWorker started")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("AggregationWorker stopped")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _run_loop(self) -> None:
        while True:
            try:
                await self._run_all_scripts()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"AggregationWorker error: {e}")
            try:
                await asyncio.sleep(_POLL_INTERVAL)
            except asyncio.CancelledError:
                break

    async def _run_all_scripts(self) -> None:
        """Load all cross-source Aggregation scripts from plugin_library and run each."""
        scripts = await self._load_python_scripts()
        for script_info in scripts:
            try:
                await self._run_one(script_info)
            except Exception as e:
                logger.error(
                    f"Aggregation script {script_info.get('name')!r} failed: {e}"
                )

    # ------------------------------------------------------------------
    # Per-script execution
    # ------------------------------------------------------------------

    async def _run_one(self, script_info: dict) -> None:
        namespace = script_info.get("namespace", "default")
        name      = script_info.get("name", "unknown")
        exchange  = script_info.get("exchange", namespace)
        agg: Aggregation = script_info["agg"]

        if agg.combine_sql() is not None:
            return  # MV-driven — setup_aggregation() already installed it, nothing to poll

        output_table = agg.OUTPUT_TABLE
        now_ts  = int(time.time())
        last_ts = await self._get_last_ts(output_table, exchange)

        if last_ts is None:
            last_ts = 0  # first run: aggregate from the beginning

        if now_ts <= last_ts:
            return  # nothing new

        ctx = AggregationContext(self._db, exchange)
        rows = await agg.combine(ctx, last_ts, now_ts)
        if not rows:
            return

        df = pl.DataFrame(rows)

        schema = agg.output_schema()
        if schema:
            _PL_MAP = {
                "Int64":   pl.Int64,   "UInt64":  pl.UInt64,
                "Float64": pl.Float64, "Float32": pl.Float32,
                "String":  pl.String,
            }
            pl_schema = {k: _PL_MAP.get(v, pl.String) for k, v in schema.items()}
            df = df.cast(pl_schema)

        table_schema = dict(zip(df.columns, df.dtypes))
        await self._db.ensure_raw_table(output_table, table_schema)
        await self._db.insert_unit_batch(output_table, df, exchange, "", "")
        await self._db.flush()
        logger.info(
            f"AggregationWorker: {name!r} → {output_table} stored {len(rows)} rows"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _load_python_scripts(self) -> list[dict]:
        """
        Return all Aggregation instances from plugin_library that AREN'T within-table
        aggregation(unit) scripts (AggregationScript.is_ch_mv() — a different mechanism
        entirely, consumed by DataCollector, sharing the same type='aggregation' rows).
        Falls back to app-level BUILTIN_AGGREGATIONS for development/testing.
        """
        from tradingkit.core.plugin_registry import get_builtin_registry
        _APP_BUILTINS = get_builtin_registry("TRADINGKIT_AGGREGATIONS_MODULE", "BUILTIN_AGGREGATIONS")

        results: list[dict] = []

        if self._db._conn:
            rows = await self._db._execute(
                "SELECT namespace, name, code FROM plugin_library FINAL"
                " WHERE type = 'aggregation'"
            )
            for row in rows:
                namespace, name, code = row[0], row[1], row[2]
                if AggregationScript(code).is_ch_mv():
                    continue
                results.append({
                    "namespace": namespace, "name": name,
                    "agg": ScriptAggregation(code=code),
                })

        existing_names = {r["name"] for r in results}
        for name, code in _APP_BUILTINS.items():
            if name not in existing_names:
                if AggregationScript(code).is_ch_mv():
                    continue
                results.append({
                    "namespace": "shared", "name": name,
                    "agg": ScriptAggregation(code=code),
                })

        return results

    async def _get_last_ts(self, table: str, exchange: str) -> int | None:
        """Return max(timestamp) from output table, or None if table is empty/missing."""
        try:
            rows = await self._db._execute(
                f"SELECT max(timestamp) FROM {table}"
                " WHERE exchange = %(ex)s",
                {"ex": exchange},
            )
            val = rows[0][0] if rows else None
            return int(val) if val else None
        except Exception:
            return None


# ---------------------------------------------------------------------------
# AggregationScript — within-table bucket-aggregation script wrapper (unrelated to
# Aggregation/ScriptAggregation above — see module docstring)
# ---------------------------------------------------------------------------

class AggregationScript:
    """
    Wrapper around a within-table bucket-aggregation script (`aggregation(unit) ->
    AggSpec`, tradingkit/schema.py's Fold machinery) — consumed by DataCollector to
    auto-create per-connection candles_60s/_3600s tables + Materialized Views.

    NOT the cross-table mechanism — that's Aggregation/ScriptAggregation above. This class
    used to also wrap the cross-table aggregate(ctx, start_ts, end_ts) convention; that
    half has been removed in favor of Aggregation/ScriptAggregation, which cover the same
    ground plus combine_sql()'s MV-native fast path.

    Type is detected via AST — no exec needed for introspection:
      is_ch_mv()   → True when aggregation(unit) is defined
    """

    def __init__(self, code: str) -> None:
        from tradingkit.core.script_ast import ScriptAST
        self._code = code
        self._ast = ScriptAST(code)

    def is_ch_mv(self) -> bool:
        return self._ast.has_function("aggregation")

    def get_source_table(self) -> str | None:
        return self._ast.get_literal("SOURCE_TABLE")

    def get_agg_spec_for_unit(self, unit: Any) -> Any | None:
        ns: dict = {}
        exec(compile(self._code, "<aggregation_script>", "exec"), ns)  # noqa: S102
        fn = ns.get("aggregation")
        if not callable(fn):
            return None
        try:
            return fn(unit)
        except Exception:
            return None


# Empty by default -- a host app can register its own via get_builtin_registry()
# (tradingkit.core.plugin_registry), see AggregationWorker._load_python_scripts() and
# load_aggregation_plugin() above.
BUILTIN_AGGREGATIONS: dict[str, str] = {}


__all__ = [
    "BUILTIN_AGGREGATIONS",
    "Aggregation",
    "AggregationContext",
    "AggregationScript",
    "AggregationWorker",
    "ScriptAggregation",
    "SourceRef",
    "load_aggregation_plugin",
    "query_aggregation",
    "setup_aggregation",
]
