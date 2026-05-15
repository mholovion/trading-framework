#!/usr/bin/env python3
"""
Simplified Gap Recovery Service
===============================

Simple and efficient service for detecting and filling gaps in candle data.
Works with any timeframe - doesn't care if data is historical, real-time, or aggregated.
Just finds gaps and fills them.
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

from core.universal_config_manager import UniversalConfigManager
from core.timeframe_utils import TimeframeUtils
from rabbitmq.rabbitmq_client import RabbitMQClient, MessagePublisher
from models.base import Candle
from sqlalchemy import and_, func, text, select as _sa_select


class GapBatcher:
    """Simple gap batching for efficient processing"""
    
    def __init__(self, max_batch_size=1000, max_time_gap_hours=1):
        self.max_batch_size = max_batch_size
        self.max_time_gap_seconds = max_time_gap_hours * 3600
        self.logger = logging.getLogger('GapBatcher')
    
    def create_batches(self, gaps: List[Dict]) -> List[Dict]:
        """Group adjacent gaps into batches"""
        if not gaps:
            return []
        
        # Sort gaps by timestamp
        sorted_gaps = sorted(gaps, key=lambda x: x['start_timestamp'])
        
        batches = []
        current_batch = {
            'start_timestamp': sorted_gaps[0]['start_timestamp'],
            'end_timestamp': sorted_gaps[0]['end_timestamp'],
            'total_candles': sorted_gaps[0]['missing_candles'],
            'gaps_count': 1,
            'batch_id': f"batch_{int(time.time())}_{len(batches)}"
        }
        
        for gap in sorted_gaps[1:]:
            # Check if gap can be merged with current batch
            time_gap = gap['start_timestamp'] - current_batch['end_timestamp']
            size_ok = current_batch['total_candles'] + gap['missing_candles'] <= self.max_batch_size
            time_ok = time_gap <= self.max_time_gap_seconds
            
            if size_ok and time_ok:
                # Merge gaps (extend batch to cover the range)
                current_batch['end_timestamp'] = gap['end_timestamp']
                current_batch['total_candles'] += gap['missing_candles']
                current_batch['gaps_count'] += 1
            else:
                # Start new batch
                batches.append(current_batch)
                current_batch = {
                    'start_timestamp': gap['start_timestamp'],
                    'end_timestamp': gap['end_timestamp'],
                    'total_candles': gap['missing_candles'],
                    'gaps_count': 1,
                    'batch_id': f"batch_{int(time.time())}_{len(batches)}"
                }
        
        batches.append(current_batch)
        
        # Log efficiency
        total_gaps = len(gaps)
        total_batches = len(batches)
        efficiency = ((total_gaps - total_batches) / total_gaps * 100) if total_gaps > 0 else 0
        
        self.logger.info(f"Batched {total_gaps} gaps → {total_batches} batches ({efficiency:.1f}% reduction)")
        
        return batches


class GapRecoveryService:
    """
    Simplified gap recovery service:
    1. Find gaps in any timeframe
    2. Batch them efficiently  
    3. Send requests to fill them
    4. That's it!
    """
    
    def __init__(self, config_manager: UniversalConfigManager,
                 database_manager: Optional[Any],
                 queue_client: RabbitMQClient):
        self.config_manager = config_manager
        self.database_manager = database_manager
        self.queue_client = queue_client
        self.message_publisher = MessagePublisher(queue_client, 'gap_recovery_service')
        
        if not self.database_manager:
            raise ValueError("DatabaseManager is required")
        
        # Simple configuration
        self.gap_check_interval = 60  # Check every minute
        self.batch_processor = GapBatcher(max_batch_size=1440, max_time_gap_hours=1)
        
        # Active connections to monitor
        self.active_connections: Dict[str, Dict] = {}
        self.exchange_plugins: Dict[str, Any] = {}  # Access to exchange plugins for server time
        self._server_time_cache: Dict[str, tuple] = {}  # connection_name -> (time, fetched_at)
        
        # Background tasks
        self.gap_check_task: Optional[asyncio.Task] = None
        self.processing_semaphore = asyncio.Semaphore(10)
        
        from core.logging_config import setup_service_logging
        self.logger = setup_service_logging('gap_recovery')
        
        # Statistics
        self.stats = {
            'gaps_detected': 0,
            'gaps_processed': 0,
            'batches_sent': 0,
            'historical_requests_sent': 0,
            'aggregation_requests_sent': 0,  # deprecated — timescaledb handles aggregation
            'last_gap_check': None,
            'start_time': datetime.now(timezone.utc)
        }
    
    async def initialize(self):
        """Initialize gap recovery service"""
        self.logger.info("Initializing Simplified Gap Recovery Service...")
        
        # Initialize database if needed
        if not hasattr(self, '_db_initialized'):
            await self.database_manager.initialize()
            self._db_initialized = True
        
        # Setup connections to monitor
        await self._setup_connections()
        
        # Gap monitoring is now controlled by orchestrator
        # self.gap_check_task = asyncio.create_task(self._gap_monitoring_loop())
        
        self.logger.info("Simplified Gap Recovery Service initialized")
    
    async def check_and_recover_gaps(self):
        """Public method for orchestrator to trigger gap recovery"""
        self.logger.info("Starting candles gap recovery (orchestrator triggered)...")
        
        try:
            check_start = datetime.now(timezone.utc)
            
            # Check gaps for all active connections in parallel
            await asyncio.gather(*[
                self._check_connection_gaps(connection_name)
                for connection_name in self.active_connections.keys()
            ])
            
            self.stats['last_gap_check'] = check_start
            check_duration = (datetime.now(timezone.utc) - check_start).total_seconds()
            
            self.logger.info(f"Candles gap recovery completed in {check_duration:.2f}s")
            
        except Exception as e:
            self.logger.error(f"Error in candles gap recovery: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def _setup_connections(self):
        """Setup connections to monitor for gaps"""
        connections_config = self.config_manager.get_config('connections')
        main_config = self.config_manager.get_config('main')
        
        for connection_name, connection_config in connections_config.get('connections', {}).items():
            if not connection_config.get('enabled', False):
                continue
                
            exchange_name = connection_config['exchange']
            if exchange_name not in main_config['exchanges']:
                continue
                
            exchange_config = main_config['exchanges'][exchange_name]
            if not exchange_config.get('enabled', False):
                continue
            
            # Merge configs
            merged_config = {**exchange_config, **connection_config}
            
            self.active_connections[connection_name] = {
                'config': merged_config,
                'last_check': None,
                'gaps_detected': 0,
                'gaps_processed': 0
            }
            
            self.logger.info(f"Monitoring {connection_name} for gaps")
    
    async def _gap_monitoring_loop(self):
        """Main gap monitoring loop"""
        self.logger.info("Starting gap monitoring loop...")
        
        while True:
            try:
                check_start = datetime.now(timezone.utc)
                
                for connection_name in self.active_connections.keys():
                    await self._check_connection_gaps(connection_name)
                
                self.stats['last_gap_check'] = check_start
                check_duration = (datetime.now(timezone.utc) - check_start).total_seconds()
                
                self.logger.debug(f"Gap check completed in {check_duration:.2f}s")
                
                # Wait before next check
                await asyncio.sleep(self.gap_check_interval)
                
            except asyncio.CancelledError:
                self.logger.info("Gap monitoring cancelled")
                break
            except Exception as e:
                self.logger.error(f"Gap monitoring error: {e}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                await asyncio.sleep(self.gap_check_interval)
    
    async def _check_connection_gaps(self, connection_name: str):
        """Check for gaps in all timeframes for a connection"""
        try:
            connection_info = self.active_connections[connection_name]
            config = connection_info['config']
            
            self.logger.debug(f"Checking gaps for {connection_name}")
            
            # Check source timeframe gaps
            await self._check_timeframe_gaps(
                connection_name,
                config['exchange'],
                config['symbol'], 
                config['source_timeframe']
            )
            
            # Check aggregated timeframes if they exist in database
            aggregated_timeframes = await self._get_aggregated_timeframes(
                config['exchange'], 
                config['symbol']
            )
            
            self.logger.info(f"Found timeframes for {config['exchange']}/{config['symbol']}: {aggregated_timeframes}")
            
            for timeframe in aggregated_timeframes:
                if timeframe != config['source_timeframe']:
                    await self._check_timeframe_gaps(
                        connection_name,
                        config['exchange'],
                        config['symbol'],
                        timeframe
                    )
            
            connection_info['last_check'] = datetime.now(timezone.utc)
            
        except Exception as e:
            self.logger.error(f"Error checking gaps for {connection_name}: {e}")

    async def _get_aggregated_timeframes(self, exchange: str, symbol: str) -> List[str]:
        """Get all timeframes that should be aggregated according to config"""
        try:
            from core.aggregation_config import AggregationConfig
            
            aggregation_config = AggregationConfig()
            supported_targets = aggregation_config.get_supported_targets(exchange, symbol)
            
            result = list(supported_targets)
            self.logger.debug(f"Timeframes from config for {exchange}/{symbol}: {result}")
            return result
                
        except Exception as e:
            self.logger.error(f"Error getting timeframes from config: {e}")
            return []
    
    async def _check_timeframe_gaps(self, connection_name: str, exchange: str,
                                   symbol: str, timeframe: str):
        """Check for gaps in specific timeframe starting from start_date"""
        try:
            self.logger.info(f"Checking {exchange}/{symbol}/{timeframe} gaps...")

            # Get start_date from config
            expected_start_timestamp = await self._get_expected_start_timestamp(connection_name, timeframe)
            if not expected_start_timestamp:
                self.logger.warning(f"No valid start_date for {connection_name}, skipping gap check")
                return

            # Use local time for safe_end_time calculation — avoids blocking plugin call
            import time as _time
            server_time = int(_time.time())

            self.logger.info(f"Running gap query for {exchange}/{symbol}/{timeframe}...")
            # Use efficient gap detection that doesn't load all candles into memory
            gaps = await self._find_actual_gaps(exchange, symbol, timeframe, expected_start_timestamp, server_time)
            self.logger.info(f"Gap query done for {exchange}/{symbol}/{timeframe}, found {len(gaps)} gaps")
            
            if gaps:
                self.logger.info(f"Found {len(gaps)} gaps in {exchange}/{symbol}/{timeframe}")
                for i, gap in enumerate(gaps):
                    gap_start = datetime.fromtimestamp(gap['start_timestamp'], tz=timezone.utc)
                    gap_end = datetime.fromtimestamp(gap['end_timestamp'], tz=timezone.utc)
                    self.logger.info(f"Gap {i+1}: {gap['missing_candles']} candles from {gap_start} to {gap_end}")
                
                self.stats['gaps_detected'] += len(gaps)
                self.active_connections[connection_name]['gaps_detected'] += len(gaps)
                
                # Process gaps in batches
                await self._process_gaps_batch(connection_name, exchange, symbol, timeframe, gaps)
            else:
                self.logger.debug(f"No gaps found in {exchange}/{symbol}/{timeframe}")
            
        except Exception as e:
            self.logger.error(f"Error checking timeframe gaps: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def _get_expected_start_timestamp(self, connection_name: str, timeframe: str) -> Optional[int]:
        """Get expected start timestamp from config, aligned to timeframe"""
        try:
            # Get start_date from connections config
            connections_config = self.config_manager.get_config('connections')
            connection_config = connections_config.get('connections', {}).get(connection_name, {})
            start_date_str = connection_config.get('historical', {}).get('start_date')
            
            if not start_date_str:
                self.logger.warning(f"No start_date in config for {connection_name}")
                return None
            
            # Parse start_date
            raw_start_timestamp = int(datetime.fromisoformat(start_date_str.replace('Z', '+00:00')).timestamp())
            
            # Align to timeframe period start
            period_start = TimeframeUtils.get_period_start(raw_start_timestamp, timeframe)
            if period_start < raw_start_timestamp:
                # start_date is in middle of period, move to next period
                timeframe_seconds = TimeframeUtils.get_timeframe_seconds(timeframe)
                aligned_start = period_start + timeframe_seconds
            else:
                # start_date is exactly at period start
                aligned_start = period_start
            
            self.logger.debug(f"Expected start for {connection_name}/{timeframe}: {datetime.fromtimestamp(aligned_start, tz=timezone.utc)} (from config: {start_date_str})")
            return aligned_start
            
        except Exception as e:
            self.logger.error(f"Error getting expected start timestamp: {e}")
            return None

    async def _find_actual_gaps(self, exchange: str, symbol: str, timeframe: str,
                               expected_start: int, server_time: int) -> List[Dict]:
        """Find ALL gaps using SQL LAG-based detection."""
        return await self._find_actual_gaps_sync(exchange, symbol, timeframe, expected_start, server_time)

    async def _find_actual_gaps_sync(self, exchange: str, symbol: str, timeframe: str,
                                    expected_start: int, server_time: int) -> List[Dict]:
        try:
            gaps = []
            timeframe_seconds = TimeframeUtils.get_timeframe_seconds(timeframe)
            safe_end_time = self._get_safe_trailing_end(server_time, timeframe)

            candle_filter = and_(
                Candle.exchange == exchange, Candle.symbol == symbol,
                Candle.timeframe == timeframe,
                Candle.timestamp >= expected_start,
                Candle.timestamp <= safe_end_time,
            )

            async with self.database_manager.get_session() as session:
                first_ts = (await session.execute(
                    _sa_select(func.min(Candle.timestamp)).where(candle_filter)
                )).scalar()

                if first_ts is None:
                    missing = (safe_end_time - expected_start) // timeframe_seconds
                    if missing > 0:
                        gaps.append({
                            'start_timestamp': expected_start,
                            'end_timestamp': safe_end_time,
                            'missing_candles': int(missing),
                        })
                        self.logger.info(
                            f"Full gap: {missing} candles "
                            f"{datetime.fromtimestamp(expected_start, tz=timezone.utc)} → "
                            f"{datetime.fromtimestamp(safe_end_time, tz=timezone.utc)}"
                        )
                    return gaps

                last_ts = (await session.execute(
                    _sa_select(func.max(Candle.timestamp)).where(candle_filter)
                )).scalar()

                self.logger.info(
                    f"Data range: "
                    f"{datetime.fromtimestamp(first_ts, tz=timezone.utc)} → "
                    f"{datetime.fromtimestamp(last_ts, tz=timezone.utc)}"
                )

                # Leading gap
                if first_ts > expected_start:
                    missing = (first_ts - expected_start) // timeframe_seconds
                    if missing > 0:
                        gaps.append({
                            'start_timestamp': expected_start,
                            'end_timestamp': first_ts - timeframe_seconds,
                            'missing_candles': int(missing),
                        })
                        self.logger.info(
                            f"Leading gap: {missing} candles "
                            f"{datetime.fromtimestamp(expected_start, tz=timezone.utc)} → "
                            f"{datetime.fromtimestamp(first_ts - timeframe_seconds, tz=timezone.utc)}"
                        )

                # Middle gaps via SQL LAG
                middle_gaps = (await session.execute(
                    text("""
                        SELECT gap_start, gap_end,
                               (gap_end - gap_start) / :tf_sec AS missing_count
                        FROM (
                            SELECT
                                LAG(timestamp) OVER (ORDER BY timestamp) + :tf_sec AS gap_start,
                                timestamp AS gap_end
                            FROM candles
                            WHERE exchange  = :exchange
                              AND symbol    = :symbol
                              AND timeframe = :timeframe
                              AND timestamp BETWEEN :start_ts AND :end_ts
                        ) t
                        WHERE gap_end - gap_start > :tf_sec
                        ORDER BY gap_start
                    """),
                    {
                        'exchange': exchange, 'symbol': symbol, 'timeframe': timeframe,
                        'start_ts': expected_start, 'end_ts': safe_end_time,
                        'tf_sec': timeframe_seconds,
                    },
                )).fetchall()

                for row in middle_gaps:
                    missing = int(row.missing_count)
                    if missing > 0:
                        gaps.append({
                            'start_timestamp': int(row.gap_start),
                            'end_timestamp': int(row.gap_end) - timeframe_seconds,
                            'missing_candles': missing,
                        })
                        self.logger.info(
                            f"Middle gap: {missing} candles "
                            f"{datetime.fromtimestamp(row.gap_start, tz=timezone.utc)} → "
                            f"{datetime.fromtimestamp(row.gap_end - timeframe_seconds, tz=timezone.utc)}"
                        )

                # Trailing gap
                expected_next = last_ts + timeframe_seconds
                if expected_next <= safe_end_time:
                    missing = (safe_end_time - last_ts) // timeframe_seconds
                    if missing > 0:
                        gaps.append({
                            'start_timestamp': int(expected_next),
                            'end_timestamp': int(safe_end_time),
                            'missing_candles': int(missing),
                        })
                        self.logger.info(
                            f"Trailing gap: {missing} candles "
                            f"{datetime.fromtimestamp(expected_next, tz=timezone.utc)} → "
                            f"{datetime.fromtimestamp(safe_end_time, tz=timezone.utc)}"
                        )

            return gaps

        except Exception as e:
            self.logger.error(f"Error finding gaps: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return []

    def _detect_gaps_from_start_date(self, candles: List[Dict], timeframe: str, 
                                    expected_start: int, server_time: Optional[int] = None) -> List[Dict]:
        """Detect gaps starting from expected start_date"""
        gaps = []
        timeframe_seconds = TimeframeUtils.get_timeframe_seconds(timeframe)
        
        if not candles:
            # No candles at all - gap from start_date to now
            if server_time:
                safe_end_time = self._get_safe_trailing_end(server_time, timeframe)
                gap_duration = safe_end_time - expected_start + timeframe_seconds
                missing_candles = gap_duration // timeframe_seconds
                
                if missing_candles > 0:
                    gaps.append({
                        'start_timestamp': expected_start,
                        'end_timestamp': safe_end_time,
                        'missing_candles': int(missing_candles)
                    })
                    self.logger.info(f"Full gap from start_date: {missing_candles} candles from {datetime.fromtimestamp(expected_start, tz=timezone.utc)} to {datetime.fromtimestamp(safe_end_time, tz=timezone.utc)}")
            
            return gaps
        
        # Sort candles by timestamp
        sorted_candles = sorted(candles, key=lambda x: x['timestamp'])
        first_candle = sorted_candles[0]
        
        # Check if there's a gap from start_date to first candle
        if first_candle['timestamp'] > expected_start:
            gap_duration = first_candle['timestamp'] - expected_start
            missing_candles = gap_duration // timeframe_seconds
            
            if missing_candles > 0:
                gaps.append({
                    'start_timestamp': expected_start,
                    'end_timestamp': first_candle['timestamp'] - timeframe_seconds,
                    'missing_candles': int(missing_candles)
                })
                self.logger.info(f"Leading gap from start_date: {missing_candles} candles from {datetime.fromtimestamp(expected_start, tz=timezone.utc)} to {datetime.fromtimestamp(first_candle['timestamp'] - timeframe_seconds, tz=timezone.utc)}")
        
        # Check gaps between existing candles (same as before)
        if len(sorted_candles) >= 2:
            for i in range(1, len(sorted_candles)):
                prev_candle = sorted_candles[i-1]
                current_candle = sorted_candles[i]
                
                expected_next = prev_candle['timestamp'] + timeframe_seconds
                actual_next = current_candle['timestamp']
                
                if actual_next > expected_next:
                    # Gap detected
                    gap_duration = actual_next - expected_next
                    missing_candles = gap_duration // timeframe_seconds
                    
                    if missing_candles > 0:
                        gaps.append({
                            'start_timestamp': expected_next,
                            'end_timestamp': actual_next - timeframe_seconds,
                            'missing_candles': int(missing_candles)
                        })
                        self.logger.debug(f"Middle gap: {missing_candles} candles from {datetime.fromtimestamp(expected_next, tz=timezone.utc)} to {datetime.fromtimestamp(actual_next - timeframe_seconds, tz=timezone.utc)}")
        
        # Check for trailing gap (from last candle to current time)
        if server_time:
            last_candle = sorted_candles[-1]
            expected_next = last_candle['timestamp'] + timeframe_seconds
            
            # Use TimeframeUtils to get safe end time for any timeframe
            safe_end_time = self._get_safe_trailing_end(server_time, timeframe)
            
            log_level = self.logger.info if timeframe == '1w' else self.logger.debug
            log_level(f"Trailing gap check for {timeframe}: last_candle={datetime.fromtimestamp(last_candle['timestamp'], tz=timezone.utc)}, expected_next={datetime.fromtimestamp(expected_next, tz=timezone.utc)}, safe_end_time={datetime.fromtimestamp(safe_end_time, tz=timezone.utc)}")
            
            if expected_next <= safe_end_time:
                gap_duration = safe_end_time - expected_next + timeframe_seconds
                missing_candles = gap_duration // timeframe_seconds
                
                if missing_candles > 0:
                    gaps.append({
                        'start_timestamp': expected_next,
                        'end_timestamp': safe_end_time,
                        'missing_candles': int(missing_candles)
                    })
                    self.logger.debug(f"Added trailing gap: {missing_candles} candles from {datetime.fromtimestamp(expected_next, tz=timezone.utc)} to {datetime.fromtimestamp(safe_end_time, tz=timezone.utc)}")
            else:
                self.logger.debug(f"No trailing gap: expected_next ({expected_next}) > safe_end_time ({safe_end_time})")
        
        return gaps

    # def _detect_gaps(self, candles: List[Dict], timeframe: str, server_time: Optional[int] = None) -> List[Dict]:
    #     """Detect gaps in candle sequence including trailing gaps (old method - kept for compatibility)"""
    #     if len(candles) < 1:
    #         return []
        
    #     gaps = []
    #     timeframe_seconds = TimeframeUtils.get_timeframe_seconds(timeframe)
        
    #     # Check gaps between existing candles
    #     if len(candles) >= 2:
    #         for i in range(1, len(candles)):
    #             prev_candle = candles[i-1]
    #             current_candle = candles[i]
                
    #             expected_next = prev_candle['timestamp'] + timeframe_seconds
    #             actual_next = current_candle['timestamp']
                
    #             if actual_next > expected_next:
    #                 # Gap detected
    #                 gap_duration = actual_next - expected_next
    #                 missing_candles = gap_duration // timeframe_seconds
                    
    #                 if missing_candles > 0:
    #                     gaps.append({
    #                         'start_timestamp': expected_next,
    #                         'end_timestamp': actual_next - timeframe_seconds,
    #                         'missing_candles': int(missing_candles)
    #                     })
        
    #     # Check for trailing gap (from last candle to current time)
    #     if server_time:
    #         last_candle = candles[-1]
    #         expected_next = last_candle['timestamp'] + timeframe_seconds
            
    #         # Use TimeframeUtils to get safe end time for any timeframe
    #         safe_end_time = self._get_safe_trailing_end(server_time, timeframe)
            
    #         log_level = self.logger.info if timeframe == '1w' else self.logger.debug
    #         log_level(f"Trailing gap check for {timeframe}: last_candle={datetime.fromtimestamp(last_candle['timestamp'], tz=timezone.utc)}, expected_next={datetime.fromtimestamp(expected_next, tz=timezone.utc)}, safe_end_time={datetime.fromtimestamp(safe_end_time, tz=timezone.utc)}")
            
    #         if expected_next <= safe_end_time:
    #             gap_duration = safe_end_time - expected_next + timeframe_seconds
    #             missing_candles = gap_duration // timeframe_seconds
                
    #             if missing_candles > 0:
    #                 gaps.append({
    #                     'start_timestamp': expected_next,
    #                     'end_timestamp': safe_end_time,
    #                     'missing_candles': int(missing_candles)
    #                 })
    #                 self.logger.debug(f"Added trailing gap: {missing_candles} candles from {datetime.fromtimestamp(expected_next, tz=timezone.utc)} to {datetime.fromtimestamp(safe_end_time, tz=timezone.utc)}")
    #         else:
    #             self.logger.debug(f"No trailing gap: expected_next ({expected_next}) > safe_end_time ({safe_end_time})")
        
    #     return gaps
    
    def _get_safe_trailing_end(self, server_time: int, timeframe: str) -> int:
        """Get safe end time for trailing gaps using TimeframeUtils"""
        # Universal approach: get the last completed period before current time
        # This works for all timeframes automatically
        
        # Get current period start
        current_period_start = TimeframeUtils.get_period_start(server_time, timeframe)
        
        # For trailing gaps, we can safely fill up to the END of previous completed period
        # Current period is still active and shouldn't be considered as a gap
        timeframe_seconds = TimeframeUtils.get_timeframe_seconds(timeframe)
        safe_end_time = current_period_start - timeframe_seconds
        
        log_level = self.logger.info if timeframe == '1w' else self.logger.debug
        log_level(f"Universal safe end for {timeframe}: server_time={datetime.fromtimestamp(server_time, tz=timezone.utc)}, current_period_start={datetime.fromtimestamp(current_period_start, tz=timezone.utc)}, safe_end_time={datetime.fromtimestamp(safe_end_time, tz=timezone.utc)}")
        
        return safe_end_time
    
    async def _get_server_time(self, connection_name: str) -> Optional[int]:
        """Get server time from exchange plugin with 5-minute cache to avoid repeated slow calls."""
        import time as _time_mod
        cached = self._server_time_cache.get(connection_name)
        if cached:
            cached_time, fetched_at = cached
            if _time_mod.monotonic() - fetched_at < 300:
                return cached_time + int(_time_mod.monotonic() - fetched_at)

        try:
            if connection_name in self.exchange_plugins:
                plugin = self.exchange_plugins[connection_name]
                server_time = await asyncio.wait_for(plugin.get_server_time(), timeout=15)
                if server_time:
                    self._server_time_cache[connection_name] = (int(server_time), _time_mod.monotonic())
                    return int(server_time)

            self.logger.warning(f"No exchange plugin available for {connection_name}")
            return None

        except Exception as e:
            self.logger.error(f"Failed to get server time for {connection_name}: {e}")
            return None
    
    def register_exchange_plugin(self, connection_name: str, plugin: Any):
        """Register exchange plugin for server time access"""
        self.exchange_plugins[connection_name] = plugin
        self.logger.info(f"Registered exchange plugin for {connection_name}")
    
    async def _trigger_full_recovery(self, connection_name: str, exchange: str, 
                                   symbol: str, timeframe: str):
        """Trigger full recovery for timeframe with no candles"""
        try:
            # Use local time for safe_end_time boundary calculation
            import time as _time
            server_time = int(_time.time())

            # Get start_date from connections config
            connections_config = self.config_manager.get_config('connections')
            connection_config = connections_config.get('connections', {}).get(connection_name, {})
            start_date_str = connection_config.get('historical', {}).get('start_date')
            
            if not start_date_str:
                self.logger.warning(f"No start_date in config for {connection_name}")
                return
            
            # Parse start_date
            raw_start_timestamp = int(datetime.fromisoformat(start_date_str.replace('Z', '+00:00')).timestamp())
            
            # For start date, find the next complete period (not previous)
            # If start_date is in middle of period, start from next period
            period_start = TimeframeUtils.get_period_start(raw_start_timestamp, timeframe)
            if period_start < raw_start_timestamp:
                # start_date is in middle of period, move to next period
                timeframe_seconds = TimeframeUtils.get_timeframe_seconds(timeframe)
                start_timestamp = period_start + timeframe_seconds
            else:
                # start_date is exactly at period start
                start_timestamp = period_start
            
            # Calculate end time
            safe_end_time = self._get_safe_trailing_end(server_time, timeframe)
            
            # Calculate total candles needed
            timeframe_seconds = TimeframeUtils.get_timeframe_seconds(timeframe)
            total_candles = (safe_end_time - start_timestamp) // timeframe_seconds
            
            # Create one big gap - batch system will split it automatically
            full_gap = [{
                'start_timestamp': start_timestamp,
                'end_timestamp': safe_end_time,
                'missing_candles': int(total_candles)
            }]
            
            self.logger.info(f"Full recovery for {exchange}/{symbol}/{timeframe}: "
                           f"{total_candles} candles from {datetime.fromtimestamp(start_timestamp, tz=timezone.utc)} "
                           f"to {datetime.fromtimestamp(safe_end_time, tz=timezone.utc)}")
            
            # Send to batch processor - it will handle splitting
            await self._process_gaps_batch(connection_name, exchange, symbol, timeframe, full_gap)
            
        except Exception as e:
            self.logger.error(f"Error in full recovery for {exchange}/{symbol}/{timeframe}: {e}")
    
    async def _process_gaps_batch(self, connection_name: str, exchange: str, 
                                 symbol: str, timeframe: str, gaps: List[Dict]):
        """Process gaps in efficient batches"""
        try:
            # Create batches
            batches = self.batch_processor.create_batches(gaps)
            
            # Process each batch
            tasks = []
            for batch in batches:
                task = asyncio.create_task(
                    self._send_gap_fill_request(connection_name, exchange, symbol, timeframe, batch)
                )
                tasks.append(task)
            
            # Wait for all batches to be sent
            await asyncio.gather(*tasks, return_exceptions=True)
            
            self.stats['batches_sent'] += len(batches)
            self.stats['gaps_processed'] += len(gaps)
            self.active_connections[connection_name]['gaps_processed'] += len(gaps)
            
            self.logger.info(f"Sent {len(batches)} batch requests for {len(gaps)} gaps in {exchange}/{symbol}/{timeframe}")
            
        except Exception as e:
            self.logger.error(f"Error processing gaps batch: {e}")
    
    async def _send_gap_fill_request(self, connection_name: str, exchange: str, 
                                    symbol: str, timeframe: str, batch: Dict):
        """Send request to fill gaps in batch"""
        async with self.processing_semaphore:
            try:
                # Determine what service should handle this timeframe
                config = self.active_connections[connection_name]['config']
                source_timeframe = config['source_timeframe']
                
                if timeframe == source_timeframe:
                    # Historical data request for source timeframe
                    request_data = {
                        'connection_name': connection_name,
                        'exchange': exchange,
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'start_timestamp': batch['start_timestamp'],
                        'end_timestamp': batch['end_timestamp'],
                        'expected_candles': batch['total_candles'],
                        'batch_id': batch['batch_id'],
                        'priority': 'normal'
                    }
                    
                    self.logger.debug(f"Sending historical data request: {exchange}/{symbol}/{timeframe} from {datetime.fromtimestamp(batch['start_timestamp'], tz=timezone.utc)} to {datetime.fromtimestamp(batch['end_timestamp'], tz=timezone.utc)}")
                    
                    message = self.message_publisher._create_message(
                        'historical_data_request', 
                        request_data
                    )
                    
                    await self.queue_client.publish_message('historical_data_requests', message)
                    
                    self.stats['historical_requests_sent'] += 1
                    self.logger.info(f"Historical data request sent for {exchange}/{symbol}/{timeframe}: "
                                   f"{batch['total_candles']} candles (batch: {batch['batch_id']})")
                
                else:
                    # Higher timeframes are handled by TimescaleDB continuous aggregates — skip
                    self.logger.debug(f"Skipping gap fill for {exchange}/{symbol}/{timeframe}: "
                                      f"handled by TimescaleDB continuous aggregates")
                
            except Exception as e:
                self.logger.error(f"Error sending gap fill request: {e}")
                raise
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get gap recovery statistics"""
        uptime = datetime.now(timezone.utc) - self.stats['start_time']
        
        connection_stats = {}
        for name, info in self.active_connections.items():
            connection_stats[name] = {
                'gaps_detected': info['gaps_detected'],
                'gaps_processed': info['gaps_processed'],
                'last_check': info['last_check'].isoformat() if info['last_check'] else None
            }
        
        return {
            'uptime_seconds': uptime.total_seconds(),
            'total_gaps_detected': self.stats['gaps_detected'],
            'total_gaps_processed': self.stats['gaps_processed'],
            'batches_sent': self.stats['batches_sent'],
            'historical_requests_sent': self.stats['historical_requests_sent'],
            'aggregation_requests_sent': self.stats['aggregation_requests_sent'],
            'gaps_per_hour': self.stats['gaps_detected'] / max(uptime.total_seconds() / 3600, 1),
            'last_gap_check': self.stats['last_gap_check'].isoformat() if self.stats['last_gap_check'] else None,
            'connections': connection_stats
        }
    
    async def cleanup(self):
        """Cleanup gap recovery service"""
        self.logger.info("Cleaning up Gap Recovery Service...")
        
        # Stop gap monitoring
        if self.gap_check_task and not self.gap_check_task.done():
            self.gap_check_task.cancel()
            try:
                await self.gap_check_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("Gap Recovery Service cleanup completed")

async def main():
    """Main function for standalone testing"""
    import sys
    import os
    
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger('GapRecoveryMain')
    
    try:
        # This would be initialized by orchestrator in real usage
        logger.info("Simplified Gap Recovery Service ready")
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Gap Recovery Service stopped")


if __name__ == "__main__":
    asyncio.run(main())