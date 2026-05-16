#!/usr/bin/env python3
"""
Gap Recovery Service (ClickHouse edition)
==========================================

Detects and fills gaps in candle data stored in ClickHouse.
Uses ClickHouse neighbor() for efficient gap detection without loading
all timestamps into memory.
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

from core.universal_config_manager import UniversalConfigManager
from core.timeframe_utils import TimeframeUtils
from core.clickhouse import ClickHouseManager, TIMEFRAME_SECONDS
from core.queue_client import InProcessQueueClient, MessagePublisher


class GapBatcher:
    """Group adjacent gaps into batches for efficient download."""

    def __init__(self, max_batch_size=1000, max_time_gap_hours=1):
        self.max_batch_size = max_batch_size
        self.max_time_gap_seconds = max_time_gap_hours * 3600
        self.logger = logging.getLogger('GapBatcher')

    def create_batches(self, gaps: List[Dict]) -> List[Dict]:
        if not gaps:
            return []
        sorted_gaps = sorted(gaps, key=lambda x: x['start_timestamp'])
        batches = []
        cur = {
            'start_timestamp': sorted_gaps[0]['start_timestamp'],
            'end_timestamp':   sorted_gaps[0]['end_timestamp'],
            'total_candles':   sorted_gaps[0]['missing_candles'],
            'gaps_count': 1,
            'batch_id': f"batch_{int(time.time())}_0",
        }
        for gap in sorted_gaps[1:]:
            time_gap = gap['start_timestamp'] - cur['end_timestamp']
            if (cur['total_candles'] + gap['missing_candles'] <= self.max_batch_size
                    and time_gap <= self.max_time_gap_seconds):
                cur['end_timestamp']  = gap['end_timestamp']
                cur['total_candles'] += gap['missing_candles']
                cur['gaps_count']    += 1
            else:
                batches.append(cur)
                cur = {
                    'start_timestamp': gap['start_timestamp'],
                    'end_timestamp':   gap['end_timestamp'],
                    'total_candles':   gap['missing_candles'],
                    'gaps_count': 1,
                    'batch_id': f"batch_{int(time.time())}_{len(batches)}",
                }
        batches.append(cur)
        eff = (len(gaps) - len(batches)) / len(gaps) * 100 if gaps else 0
        self.logger.info(f"Batched {len(gaps)} gaps → {len(batches)} batches ({eff:.1f}% reduction)")
        return batches


class GapRecoveryService:
    """
    Detects gaps using ClickHouse neighbor() and triggers historical downloads
    via the in-process message queue.
    """

    def __init__(
        self,
        config_manager: UniversalConfigManager,
        clickhouse: ClickHouseManager,
        queue_client: InProcessQueueClient,
    ):
        self.config_manager  = config_manager
        self.clickhouse      = clickhouse
        self.queue_client    = queue_client
        self.message_publisher = MessagePublisher(queue_client, 'gap_recovery_service')

        self.gap_check_interval = 60
        self.batch_processor    = GapBatcher(max_batch_size=1440, max_time_gap_hours=1)

        self.active_connections: Dict[str, Dict]    = {}
        self.exchange_plugins:   Dict[str, Any]     = {}
        self._server_time_cache: Dict[str, tuple]   = {}

        self.gap_check_task: Optional[asyncio.Task] = None
        self.processing_semaphore = asyncio.Semaphore(10)

        from core.logging_config import setup_service_logging
        self.logger = setup_service_logging('gap_recovery')

        self.stats = {
            'gaps_detected': 0,
            'gaps_processed': 0,
            'batches_sent': 0,
            'historical_requests_sent': 0,
            'last_gap_check': None,
            'start_time': datetime.now(timezone.utc),
        }

    async def initialize(self):
        self.logger.info("Initializing Gap Recovery Service...")
        await self._setup_connections()
        self.logger.info("Gap Recovery Service initialized")

    def register_exchange_plugin(self, connection_name: str, plugin: Any):
        self.exchange_plugins[connection_name] = plugin

    async def check_and_recover_gaps(self):
        """Triggered by orchestrator to run one full gap-check cycle."""
        self.logger.info("Starting gap recovery cycle...")
        check_start = datetime.now(timezone.utc)
        try:
            await asyncio.gather(*[
                self._check_connection_gaps(name)
                for name in self.active_connections
            ])
        except Exception as e:
            self.logger.error(f"Gap recovery error: {e}")
        self.stats['last_gap_check'] = check_start
        elapsed = (datetime.now(timezone.utc) - check_start).total_seconds()
        self.logger.info(f"Gap recovery done in {elapsed:.2f}s")

    async def start(self):
        self.gap_check_task = asyncio.create_task(self._gap_monitoring_loop())

    async def stop(self):
        if self.gap_check_task:
            self.gap_check_task.cancel()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _setup_connections(self):
        connections_config = self.config_manager.get_config('connections')
        main_config        = self.config_manager.get_config('main')
        for conn_name, conn_cfg in connections_config.get('connections', {}).items():
            if not conn_cfg.get('enabled', False):
                continue
            exch = conn_cfg['exchange']
            if exch not in main_config['exchanges']:
                continue
            if not main_config['exchanges'][exch].get('enabled', False):
                continue
            self.active_connections[conn_name] = {
                'config':         {**main_config['exchanges'][exch], **conn_cfg},
                'last_check':     None,
                'gaps_detected':  0,
                'gaps_processed': 0,
            }
            self.logger.info(f"Monitoring {conn_name} for gaps")

    async def _gap_monitoring_loop(self):
        while True:
            try:
                await self.check_and_recover_gaps()
                await asyncio.sleep(self.gap_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Gap monitoring loop error: {e}")
                await asyncio.sleep(self.gap_check_interval)

    async def _check_connection_gaps(self, connection_name: str):
        try:
            cfg = self.active_connections[connection_name]['config']
            tf  = cfg['source_timeframe']
            await self._check_timeframe_gaps(connection_name, cfg['exchange'], cfg['symbol'], tf)
            connection_info = self.active_connections[connection_name]
            connection_info['last_check'] = datetime.now(timezone.utc)
        except Exception as e:
            self.logger.error(f"Error checking {connection_name}: {e}")

    async def _check_timeframe_gaps(self, connection_name: str,
                                    exchange: str, symbol: str, timeframe: str):
        expected_start = await self._get_expected_start_timestamp(connection_name, timeframe)
        if not expected_start:
            return

        import time as _time
        server_time = int(_time.time())
        tf_seconds  = TIMEFRAME_SECONDS.get(timeframe, 60)
        safe_end    = self._get_safe_trailing_end(server_time, timeframe)

        gaps = await self._find_gaps_clickhouse(
            exchange, symbol, timeframe, expected_start, safe_end, tf_seconds
        )

        if gaps:
            self.logger.info(f"Found {len(gaps)} gaps in {exchange}/{symbol}/{timeframe}")
            self.stats['gaps_detected'] += len(gaps)
            self.active_connections[connection_name]['gaps_detected'] += len(gaps)
            await self._process_gaps_batch(connection_name, exchange, symbol, timeframe, gaps)
        else:
            self.logger.debug(f"No gaps in {exchange}/{symbol}/{timeframe}")

    async def _find_gaps_clickhouse(
        self,
        exchange: str, symbol: str, timeframe: str,
        expected_start: int, safe_end: int, tf_seconds: int,
    ) -> List[Dict]:
        """Detect leading, middle (via neighbor()), and trailing gaps."""
        gaps: List[Dict] = []
        try:
            min_ts, max_ts = await self.clickhouse.get_candle_range(
                exchange, symbol, timeframe,
                start_ts=expected_start, end_ts=safe_end,
            )

            if min_ts is None:
                missing = (safe_end - expected_start) // tf_seconds
                if missing > 0:
                    gaps.append({
                        'start_timestamp': expected_start,
                        'end_timestamp':   safe_end,
                        'missing_candles': missing,
                    })
                return gaps

            # Leading gap
            if min_ts > expected_start:
                missing = (min_ts - expected_start) // tf_seconds
                if missing > 0:
                    gaps.append({
                        'start_timestamp': expected_start,
                        'end_timestamp':   min_ts - tf_seconds,
                        'missing_candles': missing,
                    })

            # Middle gaps via ClickHouse neighbor()
            middle = await self.clickhouse.find_candle_gaps(
                exchange, symbol, timeframe,
                expected_start, safe_end, tf_seconds,
            )
            gaps.extend(middle)

            # Trailing gap
            expected_next = max_ts + tf_seconds
            if expected_next <= safe_end:
                missing = (safe_end - max_ts) // tf_seconds
                if missing > 0:
                    gaps.append({
                        'start_timestamp': int(expected_next),
                        'end_timestamp':   int(safe_end),
                        'missing_candles': missing,
                    })

        except Exception as e:
            self.logger.error(f"Gap detection error for {exchange}/{symbol}/{timeframe}: {e}")

        return gaps

    async def _process_gaps_batch(self, connection_name: str, exchange: str,
                                  symbol: str, timeframe: str, gaps: List[Dict]):
        batches = self.batch_processor.create_batches(gaps)
        for batch in batches:
            try:
                async with self.processing_semaphore:
                    await self.message_publisher.publish_custom_message({
                        'type': 'historical_data_request',
                        'connection_name': connection_name,
                        'exchange': exchange,
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'start_timestamp': batch['start_timestamp'],
                        'end_timestamp':   batch['end_timestamp'],
                        'expected_candles': batch['total_candles'],
                        'source': 'gap_recovery',
                    })
                    self.stats['batches_sent'] += 1
                    self.stats['historical_requests_sent'] += 1
            except Exception as e:
                self.logger.error(f"Error sending gap fill request: {e}")

    async def _get_expected_start_timestamp(self, connection_name: str, timeframe: str) -> Optional[int]:
        try:
            conn_cfg   = self.config_manager.get_config('connections')
            conn       = conn_cfg.get('connections', {}).get(connection_name, {})
            start_str  = conn.get('historical', {}).get('start_date')
            if not start_str:
                return None
            raw = int(datetime.fromisoformat(start_str.replace('Z', '+00:00')).timestamp())
            period_start = TimeframeUtils.get_period_start(raw, timeframe)
            tf_sec = TimeframeUtils.get_timeframe_seconds(timeframe)
            return period_start + tf_sec if period_start < raw else period_start
        except Exception as e:
            self.logger.error(f"Error parsing start timestamp: {e}")
            return None

    def _get_safe_trailing_end(self, server_time: int, timeframe: str) -> int:
        current_period = TimeframeUtils.get_period_start(server_time, timeframe)
        tf_sec = TimeframeUtils.get_timeframe_seconds(timeframe)
        return current_period - tf_sec
