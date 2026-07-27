"""tradingkit.collector — _GapBatcher, _ConnectionWorker gap/date logic, DataCollector lifecycle."""
from __future__ import annotations

import time

import pytest

from tradingkit.collector import DataCollector, _ConnectionWorker, _GapBatcher
from tradingkit.source import ConnectionScriptSource

# ------------------------------------------------------------------ #
# _GapBatcher — pure logic                                             #
# ------------------------------------------------------------------ #

def test_gap_batcher_empty_input():
    assert _GapBatcher().create_batches([]) == []


def test_gap_batcher_merges_close_gaps_under_max_batch():
    batcher = _GapBatcher(max_batch=1000, max_time_gap=3600)
    gaps = [
        {"start_timestamp": 0, "end_timestamp": 60, "missing_rows": 10},
        {"start_timestamp": 120, "end_timestamp": 180, "missing_rows": 10},
    ]
    batches = batcher.create_batches(gaps)
    assert len(batches) == 1
    assert batches[0]["total_rows"] == 20
    assert batches[0]["start_timestamp"] == 0
    assert batches[0]["end_timestamp"] == 180


def test_gap_batcher_splits_when_over_max_batch():
    batcher = _GapBatcher(max_batch=15, max_time_gap=3600)
    gaps = [
        {"start_timestamp": 0, "end_timestamp": 60, "missing_rows": 10},
        {"start_timestamp": 120, "end_timestamp": 180, "missing_rows": 10},
    ]
    batches = batcher.create_batches(gaps)
    assert len(batches) == 2


def test_gap_batcher_splits_when_time_gap_too_large():
    batcher = _GapBatcher(max_batch=1000, max_time_gap=100)
    gaps = [
        {"start_timestamp": 0, "end_timestamp": 60, "missing_rows": 1},
        {"start_timestamp": 10000, "end_timestamp": 10060, "missing_rows": 1},
    ]
    batches = batcher.create_batches(gaps)
    assert len(batches) == 2


def test_gap_batcher_sorts_unordered_input():
    batcher = _GapBatcher(max_batch=1000, max_time_gap=3600)
    gaps = [
        {"start_timestamp": 120, "end_timestamp": 180, "missing_rows": 1},
        {"start_timestamp": 0, "end_timestamp": 60, "missing_rows": 1},
    ]
    batches = batcher.create_batches(gaps)
    assert batches[0]["start_timestamp"] == 0


# ------------------------------------------------------------------ #
# _ConnectionWorker — date math + gap detection with a fake db          #
# ------------------------------------------------------------------ #

def _make_conn(**overrides) -> dict:
    conn = {
        "name": "wb_btc_1m", "source": "whitebit.py", "symbol": "BTC_USDT",
        "timeframe": "1m", "start_date": "2020-01-01T00:00:00Z", "config": "{}",
    }
    conn.update(overrides)
    return conn


class FakeSource(ConnectionScriptSource):
    def __init__(self):
        super().__init__(code="TABLE_NAME = 'unit_test'\nasync def historical(*a): return []")


class FakeDb:
    def __init__(self):
        self.unit_range = (None, None)
        self.gaps = []

    async def get_unit_range(self, table, exchange, symbol):
        return self.unit_range

    async def find_unit_gaps(self, table, exchange, symbol, start_ts, end_ts, unit_interval_s=60):
        return self.gaps


def _async_return(value):
    """Stand-in for an async method that just returns a fixed value, for monkeypatching
    _recover_gaps in the gap-cycle tests below without needing a full fake data pipeline."""
    async def _fn():
        return value
    return _fn


@pytest.fixture
def worker():
    return _ConnectionWorker(_make_conn(), db=FakeDb(), source=FakeSource())


def test_parse_start_ts_aligns_to_interval(worker):
    ts = worker._parse_start_ts()
    assert ts % 60 == 0


def test_safe_end_is_at_least_one_interval_before_now(worker):
    now = int(time.time())
    end = worker._safe_end()
    assert end <= now - 60
    assert end % 60 == 0


async def test_detect_gaps_no_data_at_all(worker):
    worker._db.unit_range = (None, None)
    gaps = await worker._detect_gaps(0, 600)
    assert len(gaps) == 1
    assert gaps[0]["start_timestamp"] == 0
    assert gaps[0]["end_timestamp"] == 600


async def test_detect_gaps_missing_at_start_and_end(worker):
    worker._db.unit_range = (300, 600)  # data covers [300, 600], window is [0, 900]
    worker._db.gaps = []
    gaps = await worker._detect_gaps(0, 900)
    starts = [g["start_timestamp"] for g in gaps]
    assert 0 in starts        # missing before min_ts
    assert 660 in starts      # missing after max_ts (max_ts + interval)


async def test_detect_gaps_fully_covered_returns_nothing(worker):
    worker._db.unit_range = (0, 900)
    worker._db.gaps = []
    gaps = await worker._detect_gaps(0, 900)
    assert gaps == []


async def test_detect_gaps_includes_middle_gaps_from_db(worker):
    worker._db.unit_range = (0, 900)
    worker._db.gaps = [{"start_timestamp": 300, "end_timestamp": 360, "missing_rows": 1}]
    gaps = await worker._detect_gaps(0, 900)
    assert gaps == worker._db.gaps


async def test_detect_gaps_swallows_db_errors():
    class ExplodingDb(FakeDb):
        async def get_unit_range(self, *a, **kw):
            raise RuntimeError("db down")

    w = _ConnectionWorker(_make_conn(), db=ExplodingDb(), source=FakeSource())
    gaps = await w._detect_gaps(0, 900)
    assert gaps == []


async def test_batch_complete_true_when_range_covers_window(worker):
    worker._db.unit_range = (0, 900)
    assert await worker._batch_complete(0, 800) is True


async def test_batch_complete_false_when_no_data(worker):
    worker._db.unit_range = (None, None)
    assert await worker._batch_complete(0, 800) is False


def test_table_name_from_source_table_name():
    conn = _make_conn()
    source = ConnectionScriptSource(code="TABLE_NAME = 'funding_rates'")
    w = _ConnectionWorker(conn, db=FakeDb(), source=source)
    assert w._table == "funding_rates"


def test_table_name_falls_back_to_exchange_prefix():
    conn = _make_conn(source="whitebit.py")
    source = ConnectionScriptSource(code="pass")  # no TABLE_NAME declared
    w = _ConnectionWorker(conn, db=FakeDb(), source=source)
    assert w._table == "unit_whitebit"


def test_per_connection_config_overrides_timing_defaults():
    conn = _make_conn(config='{"gap_interval_s": 5, "batch_size": 100}')
    w = _ConnectionWorker(conn, db=FakeDb(), source=FakeSource())
    assert w._gap_interval == 5
    assert w._batch_size == 100


def test_per_connection_config_overrides_backoff_defaults():
    conn = _make_conn(config='{"max_gap_interval_s": 30, "stalled_threshold": 2}')
    w = _ConnectionWorker(conn, db=FakeDb(), source=FakeSource())
    assert w._max_gap_interval == 30
    assert w._stalled_threshold == 2


# ------------------------------------------------------------------ #
# Gap-recovery backoff/give-up (TASK-011)                              #
# ------------------------------------------------------------------ #

def test_gap_interval_is_unbacked_off_with_no_failures(worker):
    assert worker._current_gap_interval() == worker._gap_interval


def test_gap_interval_backs_off_exponentially(worker):
    worker._gap_failures = 1
    assert worker._current_gap_interval() == worker._gap_interval * 2
    worker._gap_failures = 3
    assert worker._current_gap_interval() == worker._gap_interval * 8


def test_gap_interval_caps_at_max_gap_interval(worker):
    worker._gap_failures = 20  # would be an enormous multiple uncapped
    assert worker._current_gap_interval() == worker._max_gap_interval


async def test_gap_cycle_resets_failures_and_restores_status_on_progress(worker):
    worker.progress["status"] = "streaming"
    worker._gap_failures = 3
    worker._recover_gaps = _async_return(True)

    await worker._gap_cycle()

    assert worker._gap_failures == 0
    assert worker.progress["status"] == "streaming"


async def test_gap_cycle_increments_failures_and_restores_status_below_threshold(worker):
    worker.progress["status"] = "streaming"
    worker._stalled_threshold = 5
    worker._recover_gaps = _async_return(False)

    await worker._gap_cycle()

    assert worker._gap_failures == 1
    assert worker.progress["status"] == "streaming"  # not yet at threshold


async def test_gap_cycle_marks_stalled_at_threshold(worker):
    worker.progress["status"] = "streaming"
    worker._stalled_threshold = 2
    worker._gap_failures = 1
    worker._recover_gaps = _async_return(False)

    await worker._gap_cycle()

    assert worker._gap_failures == 2
    assert worker.progress["status"] == "stalled"


async def test_gap_cycle_stays_stalled_across_repeated_failures():
    """Regression: capturing "prev_status" naively on every cycle would let a stalled
    connection's own "stalled" status get recorded as the thing to restore to -- it
    should keep restoring to the real underlying status (streaming) from before the
    first failure, not to "stalled" itself."""
    w = _ConnectionWorker(_make_conn(), db=FakeDb(), source=FakeSource())
    w.progress["status"] = "streaming"
    w._stalled_threshold = 1
    w._recover_gaps = _async_return(False)

    await w._gap_cycle()  # 1st failure -> stalled
    assert w.progress["status"] == "stalled"
    await w._gap_cycle()  # 2nd failure while already stalled -> still stalled
    assert w.progress["status"] == "stalled"
    assert w._gap_failures == 2

    w._recover_gaps = _async_return(True)  # source recovers
    await w._gap_cycle()

    assert w._gap_failures == 0
    assert w.progress["status"] == "streaming"  # restored to the real prior status


# ------------------------------------------------------------------ #
# DataCollector — connection lifecycle with a fully fake db            #
# ------------------------------------------------------------------ #

class FakeCollectorDb:
    def __init__(self):
        self.connections: dict[str, dict] = {}
        self.ensured = False

    async def ensure_connections_table(self):
        self.ensured = True

    async def list_connections(self):
        return list(self.connections.values())

    async def upsert_connection(self, conn):
        self.connections[conn["name"]] = conn

    async def delete_connection(self, name):
        self.connections.pop(name, None)

    async def get_plugin_code(self, project, type_, name):
        return None  # simulate "script not found" -- exercises the early-return path


async def test_data_collector_start_with_no_connections():
    db = FakeCollectorDb()
    collector = DataCollector(db=db)
    await collector.start()
    assert db.ensured is True
    assert collector.get_status() == {}


async def test_data_collector_add_connection_missing_script_does_not_crash():
    db = FakeCollectorDb()
    collector = DataCollector(db=db)
    await collector.add_connection(_make_conn())
    assert "wb_btc_1m" in db.connections
    # get_plugin_code returns None -> worker never registered, but must not raise
    assert collector.get_status() == {}


async def test_data_collector_remove_connection():
    db = FakeCollectorDb()
    collector = DataCollector(db=db)
    await collector.add_connection(_make_conn())
    await collector.remove_connection("wb_btc_1m")
    assert "wb_btc_1m" not in db.connections


async def test_data_collector_context_manager_starts_and_stops():
    db = FakeCollectorDb()
    async with DataCollector(db=db):
        assert db.ensured is True
    # __aexit__ must not raise even with zero active workers