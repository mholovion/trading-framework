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
from datetime import datetime
from typing import Any

from tradingkit.core.clickhouse import ClickHouseManager
from tradingkit.source import ConnectionScriptSource

logger = logging.getLogger(__name__)

_CONCURRENCY      = 5
_GAP_INTERVAL     = 60
_HEALTH_INTERVAL  = 30
_RECONNECT_CD     = 120
_INFLIGHT_TTL     = 180
_MAX_BATCH        = 1440
_MAX_TIME_GAP     = 3600


# ---------------------------------------------------------------------------
# Gap batching
# ---------------------------------------------------------------------------

class _GapBatcher:
    def __init__(self, max_batch: int = _MAX_BATCH, max_time_gap: int = _MAX_TIME_GAP):
        self._max_batch    = max_batch
        self._max_time_gap = max_time_gap

    def create_batches(self, gaps: list[dict]) -> list[dict]:
        if not gaps:
            return []
        sorted_gaps = sorted(gaps, key=lambda x: x["start_timestamp"])
        batches: list[dict] = []
        cur = {
            "start_timestamp": sorted_gaps[0]["start_timestamp"],
            "end_timestamp":   sorted_gaps[0]["end_timestamp"],
            "total_rows":      sorted_gaps[0]["missing_rows"],
        }
        for gap in sorted_gaps[1:]:
            time_gap = gap["start_timestamp"] - cur["end_timestamp"]
            if (cur["total_rows"] + gap["missing_rows"] <= self._max_batch
                    and time_gap <= self._max_time_gap):
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
        source: ConnectionScriptSource,
        live_feed: Any = None,
        agg_script: Any = None,
        *,
        concurrency:        int = _CONCURRENCY,
        gap_interval:       int = _GAP_INTERVAL,
        health_interval:    int = _HEALTH_INTERVAL,
        reconnect_cooldown: int = _RECONNECT_CD,
        inflight_ttl:       int = _INFLIGHT_TTL,
        max_batch:          int = _MAX_BATCH,
        max_time_gap:       int = _MAX_TIME_GAP,
    ) -> None:
        self._conn       = conn
        self._db         = db
        self._source     = source
        self._live_feed  = live_feed
        self._agg_script = agg_script
        self._log        = logging.getLogger(f"collector.{conn['name']}")

        self._exchange  = (conn.get("source") or "").replace(".py", "")
        self._symbol    = conn["symbol"]
        self._timeframe = conn.get("timeframe", "1m")

        # Source always writes 60s units; framework aggregates upward via MV
        self._unit_interval_s: int = 60

        hist_cfg = json.loads(conn.get("config", "{}")) if isinstance(conn.get("config"), str) \
                   else conn.get("config", {})
        self._start_date    = conn.get("start_date", "2020-01-01T00:00:00Z")
        self._batch_size    = int(hist_cfg.get("batch_size",    1440))
        self._max_requests  = int(hist_cfg.get("max_requests",  10000))
        self._per_seconds   = int(hist_cfg.get("per_seconds",   10))
        self._rate_limit_ms = float(hist_cfg.get("rate_limit_ms", 50))

        # Per-connection timing overrides from config JSON
        self._concurrency        = int(hist_cfg.get("concurrency",          concurrency))
        self._gap_interval       = int(hist_cfg.get("gap_interval_s",       gap_interval))
        self._health_interval    = int(hist_cfg.get("health_interval_s",    health_interval))
        self._reconnect_cooldown = int(hist_cfg.get("reconnect_cooldown_s", reconnect_cooldown))
        self._inflight_ttl       = int(hist_cfg.get("inflight_ttl_s",       inflight_ttl))

        self._request_times: list[float] = []
        self._last_request:  float = 0.0
        self._last_row_ts: float | None = None
        self._last_reconnect: float = 0.0
        self._inflight: dict[tuple, float] = {}
        self._gap_batcher = _GapBatcher(max_batch=max_batch, max_time_gap=max_time_gap)
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

    async def _ensure_tables(self, df: "Any") -> None:
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

        batch_dur = self._batch_size * self._unit_interval_s
        batches: list[tuple[int, int, int]] = []
        cur, bn = start_ts, 1
        while cur <= end_ts:
            batch_end = min(cur + batch_dur - self._unit_interval_s, end_ts)
            batches.append((bn, cur, batch_end))
            cur = batch_end + self._unit_interval_s
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
            async for row in self._source.stream(self._symbol, self._timeframe):
                self._last_row_ts = time.time()
                df = pl.DataFrame([row])
                await self._ensure_tables(df)
                await self._db.insert_unit_batch(self._table, df, self._exchange, self._symbol)
                if self._live_feed is not None:
                    self._live_feed.publish(self._symbol, self._timeframe, row)
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
                await asyncio.sleep(self._gap_interval)
                prev_status = self.progress["status"]
                self.progress["status"] = "gap_recovery"
                await self._recover_gaps()
                if self.progress["status"] == "gap_recovery":
                    self.progress["status"] = prev_status
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._log.error(f"Gap recovery error: {e}")

    async def _recover_gaps(self) -> None:
        start_ts = self._parse_start_ts()
        end_ts   = self._safe_end()
        gaps = await self._detect_gaps(start_ts, end_ts)
        if not gaps:
            return

        self._log.info(f"Found {len(gaps)} gaps in {self._symbol}/{self._timeframe}")
        batches = self._gap_batcher.create_batches(gaps)
        now = time.time()
        self._inflight = {k: v for k, v in self._inflight.items() if now - v < self._inflight_ttl}

        for batch in batches:
            key = (self._symbol, self._timeframe, batch["start_timestamp"], batch["end_timestamp"])
            if key in self._inflight:
                continue
            self._inflight[key] = now
            n = await self._fetch_and_store(batch["start_timestamp"], batch["end_timestamp"], 1, 1)
            self.progress["total_stored"] += n

    async def _detect_gaps(self, start_ts: int, end_ts: int) -> list[dict]:
        gaps: list[dict] = []
        try:
            min_ts, max_ts = await self._db.get_unit_range(
                self._table, self._exchange, self._symbol,
            )
            iv = self._unit_interval_s
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
        await self._rate_limit()
        try:
            import polars as pl
            rows = await self._source.historical(self._symbol, self._timeframe, start_ts, end_ts)
            if isinstance(rows, pl.DataFrame):
                if rows.is_empty():
                    return 0
                df = rows
            else:
                if not rows:
                    return 0
                df = pl.DataFrame(rows)
            row_count = len(df)
            await self._ensure_tables(df)
            await self._db.insert_unit_batch(self._table, df, self._exchange, self._symbol)
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
            return min_ts <= start_ts and max_ts >= end_ts - self._unit_interval_s * 2
        except Exception:
            return False

    async def _rate_limit(self) -> None:
        now = time.time()
        self._request_times = [t for t in self._request_times if now - t <= self._per_seconds]
        if len(self._request_times) >= self._max_requests:
            oldest = min(self._request_times)
            wait = self._per_seconds - (now - oldest)
            if wait > 0:
                await asyncio.sleep(wait)
        min_delay = self._rate_limit_ms / 1000.0
        elapsed = now - self._last_request
        if elapsed < min_delay:
            await asyncio.sleep(min_delay - elapsed)
        now = time.time()
        self._request_times.append(now)
        self._last_request = now

    def _parse_start_ts(self) -> int:
        raw = int(datetime.fromisoformat(
            self._start_date.replace("Z", "+00:00")
        ).timestamp())
        iv = self._unit_interval_s
        period_start = (raw // iv) * iv
        return period_start + iv if period_start < raw else period_start

    def _safe_end(self) -> int:
        now = int(time.time())
        iv = self._unit_interval_s
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
        live_feed: "Any | None" = None,
        *,
        concurrency:        int = _CONCURRENCY,
        gap_interval:       int = _GAP_INTERVAL,
        health_interval:    int = _HEALTH_INTERVAL,
        reconnect_cooldown: int = _RECONNECT_CD,
        inflight_ttl:       int = _INFLIGHT_TTL,
        max_batch:          int = _MAX_BATCH,
        max_time_gap:       int = _MAX_TIME_GAP,
    ) -> None:
        self._db        = db
        self._live_feed = live_feed
        self._timing = dict(
            concurrency=concurrency,
            gap_interval=gap_interval,
            health_interval=health_interval,
            reconnect_cooldown=reconnect_cooldown,
            inflight_ttl=inflight_ttl,
            max_batch=max_batch,
            max_time_gap=max_time_gap,
        )
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

    async def __aenter__(self) -> "DataCollector":
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
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
            source = ConnectionScriptSource(code=code, config=config)

            agg_script = None
            agg_name = conn.get("aggregation", "").strip()
            if agg_name:
                agg_code = await self._db.get_plugin_code(
                    conn.get("project", "default"), "aggregation", agg_name
                )
                if not agg_code:
                    try:
                        from plugins.aggregations.loader import BUILTIN_AGGREGATIONS as _BUILTINS
                    except ImportError:
                        _BUILTINS = {}
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
