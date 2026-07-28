"""
tradingkit.collector — DataCollector

Manages data collection for all enabled connections stored in ClickHouse.
Replaces: launcher.py, orchestrator.py, and all services/*.

One asyncio Task per connection handles:
  - Historical backfill   (batched, concurrent, rate-limited)
  - Realtime streaming    (script realtime(), auto-reconnect)
  - Gap recovery          (ClickHouse lagInFrame SQL, every 60s)
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable, Iterable
from datetime import datetime
from typing import Any, Self

from tradingkit.core.clickhouse import ClickHouseManager
from tradingkit.core.timeframe import UNIT_SCALE
from tradingkit.source import DataSource, ScriptSource

logger = logging.getLogger(__name__)

_CONCURRENCY      = 5
_GAP_INTERVAL     = 60
_HEALTH_INTERVAL  = 30
_RECONNECT_CD     = 120
_INFLIGHT_TTL     = 180
_MAX_BATCH        = 1440
_MAX_TIME_GAP     = 3600
_MAX_GAP_INTERVAL = 3600  # backoff ceiling: retry at most once an hour once fully backed off
_STALLED_THRESHOLD = 5    # consecutive no-progress cycles before status flips to "stalled"


async def recover_gaps(
    detect: Callable[[], Awaitable[list[dict]]],
    batch:  Callable[[list[dict]], Iterable[dict]],
    fetch:  Callable[[int, int], Awaitable[Any]],
) -> list[Any]:
    """Detect gaps, batch nearby ones together, and fetch each batch. detect(), batch(),
    and fetch() are all supplied by the caller -- this function only owns the
    detect -> batch -> fetch-per-batch orchestration, nothing about what any of the three
    steps actually do. Used here by _ConnectionWorker (ClickHouse-backed) and by
    tradingkit.pipeline.Pipeline (in-memory) -- the two have no source interface or
    storage target in common, so they each supply their own detect/batch/fetch.

    Returns one fetch() result per batch; [] if detect() found no gaps.
    """
    gaps = await detect()
    if not gaps:
        return []
    results = []
    for b in batch(gaps):
        results.append(await fetch(b["start_timestamp"], b["end_timestamp"]))
    return results


# ---------------------------------------------------------------------------
# Per-connection worker
# ---------------------------------------------------------------------------

class _ConnectionWorker:
    """
    One asyncio Task per connection (source_script + symbol + timeframe).

    Source scripts declare TABLE_NAME. CH MV aggregation is handled by a separate
    AggregationScript (agg_script), loaded via the 'aggregation' field on the connection.
    Data flows: historical/realtime → insert_unit_batch → raw table → MV → agg tables.

    Progress dict is publicly readable for /api/connections/status.
    """

    def __init__(
        self,
        conn: dict,
        db:   ClickHouseManager,
        source: DataSource,
        live_feed: Any = None,
        agg_script: Any = None,
        *,
        executor:           Any = None,
        concurrency:        int = _CONCURRENCY,
        gap_interval:       int = _GAP_INTERVAL,
        health_interval:    int = _HEALTH_INTERVAL,
        reconnect_cooldown: int = _RECONNECT_CD,
        inflight_ttl:       int = _INFLIGHT_TTL,
        max_gap_interval:   int = _MAX_GAP_INTERVAL,
        stalled_threshold:  int = _STALLED_THRESHOLD,
    ) -> None:
        self._conn       = conn
        self._db         = db
        self._source     = source
        self._live_feed  = live_feed
        self._agg_script = agg_script
        self._log        = logging.getLogger(f"collector.{conn['name']}")

        if executor is None:
            from tradingkit.executor.local import LocalExecutor
            executor = LocalExecutor()
        self._executor = executor

        self._exchange  = (conn.get("source") or "").replace(".py", "")
        self._symbol    = conn["symbol"]
        self._timeframe = conn.get("timeframe", "1m")

        # Source always writes one unit per interval; framework aggregates upward via MV.
        # Expressed in the source's own timestamp unit (see DataSource.timestamp_unit),
        # so a millisecond source gets 60_000 rather than 60.
        self._unit_interval: int = 60 * UNIT_SCALE.get(source.timestamp_unit, 1)

        hist_cfg = json.loads(conn.get("config", "{}")) if isinstance(conn.get("config"), str) \
                   else conn.get("config", {})
        self._start_date    = conn.get("start_date", "2020-01-01T00:00:00Z")
        self._batch_size    = int(hist_cfg.get("batch_size",    1440))

        # Per-connection timing overrides from config JSON. Rate limiting and gap
        # batching are NOT here -- they live on the source (DataSource.rate_limit /
        # batch_gaps), which reads the same config dict, since both are properties of
        # the exchange rather than of this worker.
        self._concurrency        = int(hist_cfg.get("concurrency",          concurrency))
        self._gap_interval       = int(hist_cfg.get("gap_interval_s",       gap_interval))
        self._health_interval    = int(hist_cfg.get("health_interval_s",    health_interval))
        self._reconnect_cooldown = int(hist_cfg.get("reconnect_cooldown_s", reconnect_cooldown))
        self._inflight_ttl       = int(hist_cfg.get("inflight_ttl_s",       inflight_ttl))
        self._max_gap_interval   = int(hist_cfg.get("max_gap_interval_s",   max_gap_interval))
        self._stalled_threshold  = int(hist_cfg.get("stalled_threshold",    stalled_threshold))

        self._last_row_ts: float | None = None
        self._last_reconnect: float = 0.0
        self._inflight: dict[tuple, float] = {}
        self._gap_failures: int = 0
        self._status_before_gap_recovery: str = "starting"
        self._tasks: list[asyncio.Task] = []

        # DataUnit table state (initialized on first insert)
        self._table: str        = source.get_table_name() or f"unit_{self._exchange}"
        self._folds: list       = []
        self._tables_ready: bool = False

        # Public progress — read by DataCollector.get_status()
        self.progress: dict = {
            "status":        "starting",
            "total_batches": 0,
            "done_batches":  0,
            "total_stored":  0,
            "error":         None,
        }

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def run(self) -> None:
        if self._source.has_historical():
            try:
                await self._backfill()
            except Exception as e:
                self._log.error(f"Backfill failed: {e}")
                self.progress["error"] = str(e)

        if self._source.has_realtime():
            self._tasks.append(
                asyncio.create_task(self._stream_realtime(), name=f"rt-{self._conn['name']}")
            )

        self._tasks.append(
            asyncio.create_task(self._gap_loop(), name=f"gap-{self._conn['name']}")
        )

        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def stop(self) -> None:
        self.progress["status"] = "stopped"
        for t in self._tasks:
            if not t.done():
                t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    # ------------------------------------------------------------------ #
    # Table setup                                                          #
    # ------------------------------------------------------------------ #

    async def _ensure_tables(self, df: Any) -> None:
        """Idempotent DDL: create raw table + agg tables + MVs on first insert."""
        if self._tables_ready:
            return
        schema = dict(zip(df.columns, df.dtypes))
        await self._db.ensure_raw_table(self._table, schema)
        if self._agg_script is not None and self._agg_script.is_ch_mv():
            from tradingkit.schema import DataUnit
            unit = DataUnit(schema)
            spec = self._agg_script.get_agg_spec_for_unit(unit)
            if spec:
                for bucket_s in spec.buckets:
                    await self._db.ensure_agg_table(self._table, bucket_s, spec.folds)
                    await self._db.ensure_mv(self._table, bucket_s, spec.folds)
                    await self._db.backfill_agg(self._table, bucket_s, spec.folds)
                self._folds = spec.folds
        self._tables_ready = True

    # ------------------------------------------------------------------ #
    # Historical backfill                                                  #
    # ------------------------------------------------------------------ #

    async def _backfill(self) -> None:
        self.progress["status"] = "backfilling"
        start_ts = self._parse_start_ts()
        end_ts   = self._safe_end()

        batch_dur = self._batch_size * self._unit_interval
        batches: list[tuple[int, int, int]] = []
        cur, bn = start_ts, 1
        while cur <= end_ts:
            batch_end = min(cur + batch_dur - self._unit_interval, end_ts)
            batches.append((bn, cur, batch_end))
            cur = batch_end + self._unit_interval
            bn += 1

        total = len(batches)
        self.progress["total_batches"] = total
        self._log.info(f"Backfill: {total} batches for {self._symbol}/{self._timeframe}")

        sem  = asyncio.Semaphore(self._concurrency)
        lock = asyncio.Lock()

        async def _run(bn, bs, be):
            async with sem:
                n = await self._fetch_and_store(bs, be, bn, total)
                async with lock:
                    self.progress["done_batches"] += 1
                    self.progress["total_stored"] += n

        await asyncio.gather(*[_run(*b) for b in batches])
        self.progress["status"] = "streaming" if self._source.has_realtime() else "complete"
        self._log.info(f"Backfill done: {self.progress['total_stored']} rows stored")

    # ------------------------------------------------------------------ #
    # Realtime stream                                                      #
    # ------------------------------------------------------------------ #

    async def _stream_realtime(self) -> None:
        import polars as pl
        self.progress["status"] = "streaming"
        self._log.info(f"Starting realtime stream for {self._symbol}/{self._timeframe}")
        try:
            # NOTE: unlike the historical path below, this does NOT go through the
            # executor -- PluginExecutor has no streaming method, so user script code
            # runs unsandboxed here. Tracked separately; see framework.todo.
            async for row in self._source.stream(self._symbol, self._unit_interval):
                self._last_row_ts = time.time()
                df = pl.DataFrame([row])
                await self._ensure_tables(df)
                await self._db.insert_unit_batch(
                    self._table, df, self._exchange, self._symbol, self._timeframe
                )
                if self._live_feed is not None:
                    self._live_feed.publish(self._exchange, self._symbol, self._timeframe, row)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._log.error(f"Realtime stream error: {e}")
            self.progress["status"] = "error"
            self.progress["error"]  = str(e)

    # ------------------------------------------------------------------ #
    # Gap recovery                                                         #
    # ------------------------------------------------------------------ #

    async def _gap_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._current_gap_interval())
                await self._gap_cycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._log.error(f"Gap recovery error: {e}")
                self._gap_failures += 1

    async def _gap_cycle(self) -> None:
        """One gap-recovery attempt: overlay status, run it, then either clear the overlay
        (progress made) or escalate it to "stalled" once failures cross the threshold --
        never silently restore an overlay status (gap_recovery/stalled) as if it were the
        connection's real prior state."""
        if self.progress["status"] not in ("gap_recovery", "stalled"):
            self._status_before_gap_recovery = self.progress["status"]
        self.progress["status"] = "gap_recovery"

        made_progress = await self._recover_gaps()
        self._gap_failures = 0 if made_progress else self._gap_failures + 1

        if self._gap_failures >= self._stalled_threshold:
            self.progress["status"] = "stalled"
        else:
            self.progress["status"] = self._status_before_gap_recovery

    def _current_gap_interval(self) -> int:
        """Exponential backoff after consecutive no-progress cycles, capped at
        _max_gap_interval -- found via a real deployment where a connection stuck offline
        for hours retried at the same fixed interval forever, generating constant load with
        no signal that anything was actually wrong beyond a slowly growing gap count."""
        if self._gap_failures == 0:
            return self._gap_interval
        return min(self._gap_interval * (2 ** self._gap_failures), self._max_gap_interval)

    async def _recover_gaps(self) -> bool:
        """Returns whether this cycle made progress: True if there were no gaps to begin
        with, or at least one attempted batch actually stored rows; False if gaps were
        found, batches were attempted, and none of them stored anything (the source is
        likely unreachable)."""
        start_ts = self._parse_start_ts()
        end_ts   = self._safe_end()
        now = time.time()
        self._inflight = {k: v for k, v in self._inflight.items() if now - v < self._inflight_ttl}

        async def _detect() -> list[dict]:
            gaps = await self._detect_gaps(start_ts, end_ts)
            if gaps:
                self._log.info(f"Found {len(gaps)} gaps in {self._symbol}/{self._timeframe}")
            return gaps

        async def _fetch_batch(bstart: int, bend: int) -> int | None:
            key = (self._symbol, self._timeframe, bstart, bend)
            if key in self._inflight:
                return None
            self._inflight[key] = now
            n = await self._fetch_and_store(bstart, bend, 1, 1)
            self.progress["total_stored"] += n
            return n

        # detect comes from the worker (ClickHouse knows what's missing), batch from the
        # source (the exchange's own limits decide what fits in one request).
        results = await recover_gaps(
            detect=_detect, batch=self._source.batch_gaps, fetch=_fetch_batch,
        )
        if not results:
            return True

        attempted = [n for n in results if n is not None]
        return any(n > 0 for n in attempted) or not attempted

    async def _detect_gaps(self, start_ts: int, end_ts: int) -> list[dict]:
        gaps: list[dict] = []
        try:
            min_ts, max_ts = await self._db.get_unit_range(
                self._table, self._exchange, self._symbol,
            )
            iv = self._unit_interval
            if min_ts is None:
                missing = (end_ts - start_ts) // iv
                if missing > 0:
                    gaps.append({"start_timestamp": start_ts, "end_timestamp": end_ts,
                                 "missing_rows": missing})
                return gaps

            if min_ts > start_ts:
                missing = (min_ts - start_ts) // iv
                if missing > 0:
                    gaps.append({"start_timestamp": start_ts,
                                 "end_timestamp": min_ts - iv,
                                 "missing_rows": missing})

            middle = await self._db.find_unit_gaps(
                self._table, self._exchange, self._symbol,
                start_ts, end_ts, unit_interval_s=iv,
            )
            gaps.extend(middle)

            expected_next = max_ts + iv
            if expected_next <= end_ts:
                missing = (end_ts - max_ts) // iv
                if missing > 0:
                    gaps.append({"start_timestamp": int(expected_next),
                                 "end_timestamp": int(end_ts),
                                 "missing_rows": missing})
        except Exception as e:
            self._log.error(f"Gap detection error: {e}")
        return gaps

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    async def _fetch_and_store(
        self, start_ts: int, end_ts: int, batch_num: int, total: int
    ) -> int:
        if await self._batch_complete(start_ts, end_ts):
            return 0
        try:
            # Through the executor rather than calling the script directly, so a
            # SubprocessExecutor actually isolates user code here (LocalExecutor keeps
            # the previous in-process behaviour). Throttling lives on the source now --
            # rate limits belong to the exchange, not to this worker.
            await self._source.rate_limit()
            df = await self._executor.fetch_source_data(
                self._source, self._symbol, self._unit_interval, start_ts, end_ts,
            )
            if df is None or df.is_empty():
                return 0
            row_count = len(df)
            await self._ensure_tables(df)
            await self._db.insert_unit_batch(
                self._table, df, self._exchange, self._symbol, self._timeframe
            )
            await self._db.flush()
            if total > 1:
                self._log.debug(
                    f"Batch {batch_num}/{total}: {row_count} rows "
                    f"({batch_num/total*100:.0f}%)"
                )
            return row_count
        except Exception as e:
            self._log.error(f"Batch {batch_num}/{total} failed: {e}")
            return 0

    async def _batch_complete(self, start_ts: int, end_ts: int) -> bool:
        try:
            min_ts, max_ts = await self._db.get_unit_range(
                self._table, self._exchange, self._symbol,
            )
            if min_ts is None:
                return False
            # Rough check: if range covers the window, assume complete
            return min_ts <= start_ts and max_ts >= end_ts - self._unit_interval * 2
        except Exception:
            return False

    def _scale(self) -> int:
        """Wall-clock seconds -> this source's timestamp unit. The only place the
        framework needs to know the unit at all: gap and batching arithmetic is
        unit-relative, but comparing `now` against stored timestamps is not."""
        return UNIT_SCALE.get(self._source.timestamp_unit, 1)

    def _parse_start_ts(self) -> int:
        raw = int(datetime.fromisoformat(self._start_date).timestamp() * self._scale())
        iv = self._unit_interval
        period_start = (raw // iv) * iv
        return period_start + iv if period_start < raw else period_start

    def _safe_end(self) -> int:
        now = int(time.time() * self._scale())
        iv = self._unit_interval
        return (now // iv) * iv - iv


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class DataCollector:
    """
    Framework-level data collection manager.

    Reads enabled connections from ClickHouse (connections table).
    Each connection references a source script stored in plugin_library.

    Usage:
        collector = DataCollector(db=db)
        async with collector:
            await serve(web_app)

    Hot-add a new pair at runtime:
        await collector.add_connection({
            "name": "btc_usdt_1m", "project": "my_bot", "source": "whitebit.py",
            "symbol": "BTC_USDT", "timeframe": "1m", "start_date": "2020-01-01T00:00:00Z",
        })
    """

    def __init__(
        self,
        db:        ClickHouseManager,
        live_feed: Any | None = None,
        *,
        executor:           Any = None,
        concurrency:        int = _CONCURRENCY,
        gap_interval:       int = _GAP_INTERVAL,
        health_interval:    int = _HEALTH_INTERVAL,
        reconnect_cooldown: int = _RECONNECT_CD,
        inflight_ttl:       int = _INFLIGHT_TTL,
        max_gap_interval:   int = _MAX_GAP_INTERVAL,
        stalled_threshold:  int = _STALLED_THRESHOLD,
    ) -> None:
        self._db        = db
        self._live_feed = live_feed
        #: Executor used to run source scripts. Defaults to in-process (LocalExecutor);
        #: pass SubprocessExecutor() to isolate user-submitted connection scripts.
        self._executor  = executor
        self._timing = {
            "executor": executor,
            "concurrency": concurrency,
            "gap_interval": gap_interval,
            "health_interval": health_interval,
            "reconnect_cooldown": reconnect_cooldown,
            "inflight_ttl": inflight_ttl,
            "max_gap_interval": max_gap_interval,
            "stalled_threshold": stalled_threshold,
        }
        self._workers:      dict[str, _ConnectionWorker] = {}
        self._worker_tasks: dict[str, asyncio.Task]      = {}

    async def start(self) -> None:
        await self._db.ensure_connections_table()
        connections = await self._db.list_connections()
        for conn in connections:
            if conn.get("enabled"):
                await self._start_connection(conn)

    async def stop(self) -> None:
        for worker in self._workers.values():
            await worker.stop()
        for task in self._worker_tasks.values():
            if not task.done():
                task.cancel()
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks.values(), return_exceptions=True)
        self._workers.clear()
        self._worker_tasks.clear()
        logger.info("DataCollector stopped")

    async def add_connection(self, conn: dict) -> None:
        """Persist to ClickHouse and hot-start a worker."""
        await self._db.upsert_connection(conn)
        if conn.get("enabled", True):
            await self._start_connection(conn)

    async def update_connection(self, name: str, updates: dict) -> None:
        """Update config in ClickHouse and restart the worker."""
        if name in self._workers:
            await self._workers[name].stop()
            del self._workers[name]
        if name in self._worker_tasks:
            del self._worker_tasks[name]
        connections = await self._db.list_connections()
        conn = next((c for c in connections if c["name"] == name), None)
        if conn:
            conn.update(updates)
            await self._db.upsert_connection(conn)
            if conn.get("enabled"):
                await self._start_connection(conn)

    async def remove_connection(self, name: str) -> None:
        """Stop worker and delete from ClickHouse."""
        if name in self._workers:
            await self._workers[name].stop()
            del self._workers[name]
        if name in self._worker_tasks:
            task = self._worker_tasks.pop(name)
            if not task.done():
                task.cancel()
        await self._db.delete_connection(name)

    def get_status(self) -> dict[str, dict]:
        """Return progress dict for all workers."""
        return {name: worker.progress for name, worker in self._workers.items()}

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    async def _start_connection(self, conn: dict) -> None:
        name = conn["name"]
        try:
            code = await self._db.get_plugin_code(
                conn.get("project", "default"), "source", conn["source"]
            )
            if not code:
                logger.error(f"Source script not found: project={conn.get('project')} source={conn['source']}")
                return

            config = json.loads(conn.get("config", "{}")) if isinstance(conn.get("config"), str) \
                     else conn.get("config", {})
            source = ScriptSource(code=code, config=config)

            agg_script = None
            agg_name = conn.get("aggregation", "").strip()
            if agg_name:
                agg_code = await self._db.get_plugin_code(
                    conn.get("project", "default"), "aggregation", agg_name
                )
                if not agg_code:
                    from tradingkit.core.plugin_registry import get_builtin_registry
                    _BUILTINS = get_builtin_registry("TRADINGKIT_AGGREGATIONS_MODULE", "BUILTIN_AGGREGATIONS")
                    agg_code = _BUILTINS.get(agg_name)
                if agg_code:
                    from tradingkit.aggregation import AggregationScript
                    agg_script = AggregationScript(agg_code)
                else:
                    logger.warning(f"Aggregation script not found: {agg_name!r}")

            worker = _ConnectionWorker(
                conn, self._db, source, self._live_feed, agg_script, **self._timing
            )
            self._workers[name] = worker
            task = asyncio.create_task(worker.run(), name=f"worker-{name}")
            self._worker_tasks[name] = task
            logger.info(f"DataCollector: started worker for {name} ({conn['symbol']} {conn['timeframe']})")
        except Exception as e:
            logger.error(f"DataCollector: failed to start {name}: {e}")


__all__ = ["DataCollector"]
