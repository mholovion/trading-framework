"""
tradingkit.aggregation — AggregationContext + AggregationWorker.

AggregationContext  — project-level object passed to aggregate(ctx, start_ts, end_ts).
                      Provides query() access to ANY table in ClickHouse.

AggregationWorker   — background task that periodically runs Python aggregation scripts
                      (type="aggregation" with aggregate() function) stored in plugin_library.
                      Stores results via insert_unit_batch() into OUTPUT_TABLE.

Data flow:
    plugin_library (type="aggregation", is_python())
        → AggregationWorker._run_one()
        → aggregate(ctx, last_ts, now_ts) → list[dict]
        → db.ensure_raw_table(OUTPUT_TABLE, output_schema)
        → db.insert_unit_batch(OUTPUT_TABLE, ...)
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import polars as pl

from tradingkit.core.clickhouse import ClickHouseManager

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 60  # seconds between aggregation runs


# ---------------------------------------------------------------------------
# AggregationContext
# ---------------------------------------------------------------------------

class AggregationContext:
    """
    Project-level query context passed to aggregate(ctx, start_ts, end_ts).

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
# AggregationWorker
# ---------------------------------------------------------------------------

class AggregationWorker:
    """
    Background worker for Python aggregation scripts (TASK-003).

    Loads all scripts from plugin_library where type='aggregation' and the script
    defines aggregate(ctx, start_ts, end_ts). Runs each script periodically,
    stores results into OUTPUT_TABLE.

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
        """Load all Python aggregation scripts from plugin_library and run each."""
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

        code      = script_info["code"]
        namespace = script_info.get("namespace", "default")
        name      = script_info.get("name", "unknown")
        exchange  = script_info.get("exchange", namespace)

        agg = AggregationScript(code)
        if not agg.is_python():
            return

        output_table  = agg.get_output_table()
        output_schema = agg.get_output_schema()
        if not output_table:
            logger.warning(f"Aggregation script {name!r}: OUTPUT_TABLE not defined, skipping")
            return

        now_ts  = int(time.time())
        last_ts = await self._get_last_ts(output_table, exchange)

        if last_ts is None:
            last_ts = 0  # first run: aggregate from the beginning

        if now_ts <= last_ts:
            return  # nothing new

        ctx = AggregationContext(self._db, exchange)
        rows = await agg.run_aggregate(ctx, last_ts, now_ts)
        if not rows:
            return

        df = pl.DataFrame(rows)

        if output_schema:
            _PL_MAP = {
                "Int64":   pl.Int64,   "UInt64":  pl.UInt64,
                "Float64": pl.Float64, "Float32": pl.Float32,
                "String":  pl.String,
            }
            pl_schema = {k: _PL_MAP.get(v, pl.String) for k, v in output_schema.items()}
            df = df.cast(pl_schema)

        schema = dict(zip(df.columns, df.dtypes))
        await self._db.ensure_raw_table(output_table, schema)
        await self._db.insert_unit_batch(output_table, df, exchange, "")
        await self._db.flush()
        logger.info(
            f"AggregationWorker: {name!r} → {output_table} stored {len(rows)} rows"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _load_python_scripts(self) -> list[dict]:
        """
        Return all aggregation scripts from plugin_library that define aggregate().
        Falls back to app-level BUILTIN_AGGREGATIONS for development/testing.
        """
        try:
            from plugins.aggregations.loader import BUILTIN_AGGREGATIONS as _APP_BUILTINS
        except ImportError:
            _APP_BUILTINS = {}

        results: list[dict] = []

        # Load from plugin_library
        if self._db._conn:
            rows = await self._db._execute(
                "SELECT namespace, name, code FROM plugin_library FINAL"
                " WHERE type = 'aggregation'"
            )
            for row in rows:
                namespace, name, code = row[0], row[1], row[2]
                agg = AggregationScript(code)
                if agg.is_python():
                    results.append({"namespace": namespace, "name": name, "code": code})

        # Supplement with builtins not already in library
        existing_names = {r["name"] for r in results}
        for name, code in _APP_BUILTINS.items():
            if name not in existing_names:
                agg = AggregationScript(code)
                if agg.is_python():
                    results.append({"namespace": "shared", "name": name, "code": code})

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
# AggregationScript — wraps a user or built-in aggregation script
# ---------------------------------------------------------------------------

class AggregationScript:
    """
    Wrapper around an aggregation script string.

    Type is detected via AST — no exec needed for introspection:
      is_ch_mv()   → True when aggregation(unit) is defined
      is_python()  → True when aggregate(ctx, ...) is defined
    """

    def __init__(self, code: str) -> None:
        import ast as _ast
        self._code = code
        self._tree: _ast.Module | None = None

    def _get_tree(self):
        import ast as _ast
        if self._tree is None:
            self._tree = _ast.parse(self._code)
        return self._tree

    def is_ch_mv(self) -> bool:
        return self._has_fn("aggregation")

    def is_python(self) -> bool:
        return self._has_fn("aggregate")

    def _has_fn(self, name: str) -> bool:
        import ast as _ast
        try:
            return any(
                isinstance(n, (_ast.AsyncFunctionDef, _ast.FunctionDef)) and n.name == name
                for n in _ast.walk(self._get_tree())
            )
        except Exception:
            return False

    def get_source_table(self) -> str | None:
        return self._ast_str("SOURCE_TABLE")

    def get_output_table(self) -> str | None:
        return self._ast_str("OUTPUT_TABLE")

    def get_output_schema(self) -> dict | None:
        return self._ast_literal("OUTPUT_SCHEMA")

    def get_interval_s(self) -> int:
        v = self._ast_literal("INTERVAL_S")
        return int(v) if isinstance(v, (int, float)) else 60

    def _ast_str(self, var: str) -> str | None:
        import ast as _ast
        try:
            for node in _ast.walk(self._get_tree()):
                if (isinstance(node, _ast.Assign)
                        and len(node.targets) == 1
                        and isinstance(node.targets[0], _ast.Name)
                        and node.targets[0].id == var):
                    return _ast.literal_eval(node.value)
        except Exception:
            pass
        return None

    def _ast_literal(self, var: str) -> Any:
        import ast as _ast
        try:
            for node in _ast.walk(self._get_tree()):
                if (isinstance(node, _ast.Assign)
                        and len(node.targets) == 1
                        and isinstance(node.targets[0], _ast.Name)
                        and node.targets[0].id == var):
                    return _ast.literal_eval(node.value)
        except Exception:
            pass
        return None

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

    async def run_aggregate(self, ctx: Any, start_ts: int, end_ts: int) -> list[dict]:
        import asyncio as _asyncio
        ns: dict = {}
        exec(compile(self._code, "<aggregation_script>", "exec"), ns)  # noqa: S102
        fn = ns.get("aggregate")
        if not callable(fn):
            return []
        result = fn(ctx, start_ts, end_ts)
        if _asyncio.iscoroutine(result):
            result = await result
        return result or []


# Empty registry — app-specific builtins live in plugins/aggregations/
BUILTIN_AGGREGATIONS: dict[str, str] = {}


def load_aggregation_script(name: str, code: str | None = None) -> AggregationScript:
    """
    Return AggregationScript for the given name.

    If code is provided, wraps it directly (user-supplied script).
    Otherwise looks up BUILTIN_AGGREGATIONS[name].
    Raises KeyError if not found.
    """
    if code is not None:
        return AggregationScript(code)
    if name in BUILTIN_AGGREGATIONS:
        return AggregationScript(BUILTIN_AGGREGATIONS[name])
    raise KeyError(f"Unknown aggregation script: {name!r}")


__all__ = [
    "AggregationContext",
    "AggregationWorker",
    "AggregationScript",
    "BUILTIN_AGGREGATIONS",
    "load_aggregation_script",
]
