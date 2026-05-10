#!/usr/bin/env python3
"""
Indicators Gap Service
======================

Service for detecting and filling gaps in indicator data.
Works independently from main gap recovery to maintain clean separation.
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, List, Set
from datetime import datetime, timezone

from core.universal_config_manager import UniversalConfigManager
from rabbitmq.rabbitmq_client import RabbitMQClient, MessagePublisher
from sqlalchemy import and_, func, distinct
from models.base import Candle


class IndicatorBatcher:
    """Simplified batching for indicator calculation requests - no splitting since we have bulk processing"""
    
    def __init__(self, max_batch_size=10000, max_time_gap_hours=72):  # Increased limits
        self.max_batch_size = max_batch_size  # Now just for logging
        self.max_time_gap_seconds = max_time_gap_hours * 3600
        self.logger = logging.getLogger('IndicatorBatcher')
    
    def create_batches(self, indicator_ranges: List[Dict]) -> List[Dict]:
        """Return ranges as-is since bulk processing can handle large ranges efficiently"""
        if not indicator_ranges:
            return []
        
        # Simply return the ranges without splitting - bulk processing handles the rest
        batches = []
        
        for range_info in indicator_ranges:
            batch = {
                'start_timestamp': range_info['start_timestamp'],
                'end_timestamp': range_info['end_timestamp'],
                'total_candles': range_info['missing_candles'],
                'batch_id': f"indicator_range_{int(time.time())}_{len(batches)}",
                'indicator_name': range_info['indicator_name'],
                'exchange': range_info['exchange'],
                'symbol': range_info['symbol'],
                'timeframe': range_info['timeframe'],
                'connection_name': range_info.get('connection_name')
            }
            batches.append(batch)
        
        # Log batching efficiency
        total_ranges = len(indicator_ranges)
        total_batches = len(batches)
        
        if total_ranges > 0:
            total_missing = sum(r['missing_candles'] for r in indicator_ranges)
            self.logger.info(f"Indicator ranges: {total_ranges} ranges → {total_batches} requests "
                           f"({total_missing} missing indicators)")
        
        return batches


class IndicatorsGapService:
    """
    Service for detecting missing indicators and triggering their calculation.
    
    Logic:
    1. Read indicators config to know what should exist
    2. Check which candles exist vs which indicators exist
    3. Send requests to calculate missing indicators
    """
    
    def __init__(self, config_manager: UniversalConfigManager,
                 database_manager: Optional[Any],
                 queue_client: RabbitMQClient):
        self.config_manager = config_manager
        self.database_manager = database_manager
        self.queue_client = queue_client
        self.message_publisher = MessagePublisher(queue_client, 'indicators_gap_service')
        
        if not self.database_manager:
            raise ValueError("DatabaseManager is required")
        
        # Configuration
        self.gap_check_interval = 300  # Check every 5 minutes
        self.max_indicators_per_request = 10000  # Process gaps in chunks
        self.enabled_indicators: Dict[str, Dict] = {}
        self.indicator_processing_state: Dict[str, Dict] = {}  # Track processing state per indicator
        
        # Initialize IndicatorBatcher for efficient processing
        self.batch_processor = IndicatorBatcher(max_batch_size=1000, max_time_gap_hours=24)
        
        # Background task
        self.gap_check_task: Optional[asyncio.Task] = None
        
        from core.logging_config import setup_service_logging
        self.logger = setup_service_logging('indicators_gap')
        
        # Statistics
        self.stats = {
            'indicators_checked': 0,
            'gaps_detected': 0,
            'calculation_requests_sent': 0,
            'last_gap_check': None,
            'start_time': datetime.now(timezone.utc)
        }
    
    async def initialize(self):
        """Initialize indicators gap service"""
        self.logger.info("Initializing Indicators Gap Service...")
        
        # Initialize database if needed
        if not hasattr(self, '_db_initialized'):
            await self.database_manager.initialize()
            self._db_initialized = True
        
        # Load enabled indicators
        await self._load_enabled_indicators()
        
        # Gap monitoring is now controlled by orchestrator
        # self.gap_check_task = asyncio.create_task(self._gap_monitoring_loop())
        
        self.logger.info("Indicators Gap Service initialized")
    
    async def check_and_recover_gaps(self):
        """Public method for orchestrator to trigger gap recovery"""
        self.logger.info("Starting indicators gap recovery (orchestrator triggered)...")
        
        try:
            check_start = datetime.now(timezone.utc)
            
            for indicator_name in self.enabled_indicators.keys():
                await self._check_indicator_gaps(indicator_name)
            
            self.stats['last_gap_check'] = check_start
            check_duration = (datetime.now(timezone.utc) - check_start).total_seconds()
            
            self.logger.info(f"Indicators gap recovery completed in {check_duration:.2f}s")
            
        except Exception as e:
            self.logger.error(f"Error in indicators gap recovery: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def _load_enabled_indicators(self):
        """Load enabled indicators from config"""
        indicators_config = self.config_manager.get_config('indicators')
        
        for indicator_name, indicator_config in indicators_config.get('indicators', {}).items():
            if indicator_config.get('enabled', False):
                self.enabled_indicators[indicator_name] = indicator_config
                self.logger.info(f"Monitoring indicator: {indicator_name} ({indicator_config['plugin']} on {indicator_config['source_timeframe']})")
        
        self.logger.info(f"Loaded {len(self.enabled_indicators)} enabled indicators")
    
    async def _gap_monitoring_loop(self):
        """Main indicators gap monitoring loop"""
        self.logger.info("Starting indicators gap monitoring loop...")
        
        while True:
            try:
                check_start = datetime.now(timezone.utc)
                
                for indicator_name in self.enabled_indicators.keys():
                    await self._check_indicator_gaps(indicator_name)
                
                self.stats['last_gap_check'] = check_start
                check_duration = (datetime.now(timezone.utc) - check_start).total_seconds()
                
                self.logger.debug(f"Indicators gap check completed in {check_duration:.2f}s")
                
                # Wait before next check
                await asyncio.sleep(self.gap_check_interval)
                
            except asyncio.CancelledError:
                self.logger.info("Indicators gap monitoring cancelled")
                break
            except Exception as e:
                self.logger.error(f"Indicators gap monitoring error: {e}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
                await asyncio.sleep(self.gap_check_interval)
    
    async def _check_indicator_gaps(self, indicator_name: str):
        """Check for gaps in specific indicator"""
        try:
            indicator_config = self.enabled_indicators[indicator_name]
            source_timeframe = indicator_config['source_timeframe']
            
            # Get connection info
            connection_name = indicator_config['connection']
            connections_config = self.config_manager.get_config('connections')
            connection_config = connections_config.get('connections', {}).get(connection_name, {})
            
            if not connection_config:
                self.logger.warning(f"Connection {connection_name} not found for indicator {indicator_name}")
                return
            
            exchange = connection_config['exchange']
            symbol = connection_config['symbol']
            
            self.logger.debug(f"Checking gaps for indicator {indicator_name}: {exchange}/{symbol}/{source_timeframe}")
            
            # Use single database session for all operations to avoid connection pool exhaustion
            try:
                with self.database_manager.get_session() as session:
                    # Get available candles count for this timeframe
                    candles_count = session.query(func.count(Candle.id)).filter(
                        and_(
                            Candle.exchange == exchange,
                            Candle.symbol == symbol,
                            Candle.timeframe == source_timeframe
                        )
                    ).scalar() or 0
                    
                    # Find specific missing timestamps (proper gap detection)
                    from models.base import Indicator
                    
                    # Get all candle timestamps  
                    self.logger.info(f"Querying candles: exchange='{exchange}', symbol='{symbol}', timeframe='{source_timeframe}'")
                    
                    candle_timestamps = session.query(Candle.timestamp).filter(
                        and_(
                            Candle.exchange == exchange,
                            Candle.symbol == symbol,
                            Candle.timeframe == source_timeframe
                        )
                    ).all()
                    candle_timestamps_set = {ts[0] for ts in candle_timestamps}
                    
                    self.logger.info(f"Candles query result: {len(candle_timestamps)} rows")
                    
                    # Get all existing indicator timestamps
                    self.logger.info(f"Querying indicators: indicator_name='{indicator_name}', exchange='{exchange}', symbol='{symbol}', timeframe='{source_timeframe}'")
                    
                    indicator_timestamps = session.query(Indicator.timestamp).filter(
                        and_(
                            Indicator.indicator_name == indicator_name,
                            Indicator.exchange == exchange,
                            Indicator.symbol == symbol,
                            Indicator.timeframe == source_timeframe
                        )
                    ).all()
                    indicator_timestamps_set = {ts[0] for ts in indicator_timestamps}
                    
                    self.logger.info(f"Raw query result: {len(indicator_timestamps)} rows, first 3: {indicator_timestamps[:3]}")
                    
                    # Find missing timestamps (real gaps)
                    missing_timestamps = candle_timestamps_set - indicator_timestamps_set
                    missing_count = len(missing_timestamps)
                    
                    # Debug the calculation
                    self.logger.info(f"Set operations debug:")
                    self.logger.info(f"Candles set size: {len(candle_timestamps_set)}")
                    self.logger.info(f"Indicators set size: {len(indicator_timestamps_set)}")
                    self.logger.info(f"Missing = Candles - Indicators = {missing_count}")
                    
                    if len(candle_timestamps_set) > 0 and len(indicator_timestamps_set) > 0:
                        candles_sample = sorted(list(candle_timestamps_set))[:3]
                        indicators_sample = sorted(list(indicator_timestamps_set))[:3]
                        self.logger.info(f"Candles sample: {candles_sample}")
                        self.logger.info(f"Indicators sample: {indicators_sample}")
                    
                    # Enhanced debugging
                    self.logger.info(f"GAP DEBUG for {indicator_name}:")
                    self.logger.info(f"Candles query: exchange={exchange}, symbol={symbol}, timeframe={source_timeframe}")
                    self.logger.info(f"Indicators query: indicator_name={indicator_name}, exchange={exchange}, symbol={symbol}, timeframe={source_timeframe}")
                    self.logger.info(f"Found: {len(candle_timestamps_set)} candles, {len(indicator_timestamps_set)} indicators")
                    self.logger.info(f"Missing: {missing_count} gaps")
                    
                    if len(candle_timestamps_set) > 0 and len(indicator_timestamps_set) > 0:
                        # Sample timestamps for comparison
                        sample_candle_ts = sorted(list(candle_timestamps_set))[:3]
                        sample_indicator_ts = sorted(list(indicator_timestamps_set))[:3]
                        
                        self.logger.info(f"Sample candle timestamps: {[datetime.fromtimestamp(ts, tz=timezone.utc) for ts in sample_candle_ts]}")
                        self.logger.info(f"Sample indicator timestamps: {[datetime.fromtimestamp(ts, tz=timezone.utc) for ts in sample_indicator_ts]}")
                        
                        # Check if timestamps match
                        common_timestamps = candle_timestamps_set & indicator_timestamps_set
                        self.logger.info(f"Common timestamps: {len(common_timestamps)}")
                        
                        if missing_count > 0 and missing_count < 10:
                            missing_sample = sorted(list(missing_timestamps))[:5]
                            self.logger.info(f"Sample missing timestamps: {[datetime.fromtimestamp(ts, tz=timezone.utc) for ts in missing_sample]}")
                    
                    if missing_count > 0:
                        self.logger.info(f"Found {missing_count} missing indicators for {indicator_name}")
                        
                        # Send calculation requests for specific missing timestamps
                        await self._send_gap_calculation_requests(indicator_name, exchange, symbol,
                                                                source_timeframe, connection_name,
                                                                missing_timestamps)
                        
                        self.stats['gaps_detected'] += missing_count
                        self.logger.info(f"Sent gap requests for {indicator_name}: {missing_count} missing timestamps")
                    else:
                        self.logger.debug(f"No gaps found for indicator {indicator_name}")
                    
            except Exception as db_e:
                self.logger.error(f"Database error for {indicator_name}: {db_e}")
                return
            
            self.stats['indicators_checked'] += 1
            
        except Exception as e:
            self.logger.error(f"Error checking indicator gaps for {indicator_name}: {e}")
    
    async def _send_gap_calculation_requests(self, indicator_name: str, exchange: str, symbol: str,
                                           timeframe: str, connection_name: str, missing_timestamps: set):
        """Send calculation requests for specific missing timestamps using batching"""
        try:
            if not missing_timestamps:
                return

            sorted_timestamps = sorted(missing_timestamps)

            # Group consecutive timestamps into ranges for efficiency
            ranges = self._group_consecutive_timestamps(sorted_timestamps, timeframe=timeframe)
            
            # Prepare range info for batching
            indicator_ranges = []
            for range_start, range_end in ranges:
                missing_in_range = len([ts for ts in sorted_timestamps if range_start <= ts <= range_end])
                
                range_info = {
                    'indicator_name': indicator_name,
                    'exchange': exchange,
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'start_timestamp': range_start,
                    'end_timestamp': range_end,
                    'missing_candles': missing_in_range,
                    'connection_name': connection_name
                }
                indicator_ranges.append(range_info)
            
            # Use IndicatorBatcher to create optimized batches
            batches = self.batch_processor.create_batches(indicator_ranges)
            
            # Send each batch as a separate request
            for batch in batches:
                await self._send_indicator_calculation_request(indicator_name, batch)
                    
        except Exception as e:
            self.logger.error(f"Error sending gap calculation requests for {indicator_name}: {e}")
    
    def _group_consecutive_timestamps(self, timestamps: List[int], timeframe: str = '1m') -> List[tuple]:
        """Group consecutive missing timestamps into ranges, universal for all timeframes"""
        if not timestamps:
            return []

        timeframe_intervals = {
            '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
            '1h': 3600, '4h': 14400, '1d': 86400, '1w': 604800
        }
        expected_interval = timeframe_intervals.get(timeframe, 60)

        sorted_timestamps = sorted(timestamps)
        ranges = []
        current_start = sorted_timestamps[0]
        current_end = sorted_timestamps[0]

        for i in range(1, len(sorted_timestamps)):
            prev_timestamp = sorted_timestamps[i-1]
            curr_timestamp = sorted_timestamps[i]
            gap = curr_timestamp - prev_timestamp
            
            # Check if timestamps are consecutive. Use 3x tolerance to handle
            # sparse data where alternate periods may be missing (e.g. 1d candle gaps).
            if gap <= expected_interval * 3:
                # Consecutive - extend current range
                current_end = curr_timestamp
            else:
                # Non-consecutive - close current range and start new one
                ranges.append((current_start, current_end))
                current_start = curr_timestamp
                current_end = curr_timestamp
        
        # Add the last range
        ranges.append((current_start, current_end))
        
        return ranges
    
    
    async def _send_indicator_calculation_request(self, indicator_name: str, range_info: Dict):
        """Send request to calculate indicators for range"""
        try:
            indicator_config = self.enabled_indicators[indicator_name]
            
            request_data = {
                'indicator_name': indicator_name,
                'plugin': indicator_config['plugin'],
                'exchange': range_info['exchange'],
                'symbol': range_info['symbol'],
                'timeframe': range_info['timeframe'],
                'start_timestamp': range_info['start_timestamp'],
                'end_timestamp': range_info['end_timestamp'],
                'parameters': indicator_config.get('parameters', {}),
                'data_source': indicator_config.get('data_source', 'aggregated'),
                'connection_name': range_info.get('connection_name'),
                'batch_id': range_info.get('batch_id', f"indicators_gap_{int(time.time())}")
            }
            
            message = self.message_publisher._create_message(
                'indicator_calculation_request',
                request_data
            )
            
            await self.queue_client.publish_message('indicator_calculation_requests', message)
            
            self.stats['calculation_requests_sent'] += 1
            
            start_dt = datetime.fromtimestamp(range_info['start_timestamp'], tz=timezone.utc)
            end_dt = datetime.fromtimestamp(range_info['end_timestamp'], tz=timezone.utc)
            
            self.logger.info(f"Indicator calculation request sent for {indicator_name}: "
                           f"from {start_dt} to {end_dt} "
                           f"[batch: {request_data['batch_id']}]")
            
        except Exception as e:
            self.logger.error(f"Error sending indicator calculation request: {e}")
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get indicators gap statistics"""
        uptime = datetime.now(timezone.utc) - self.stats['start_time']
        
        return {
            'uptime_seconds': uptime.total_seconds(),
            'enabled_indicators_count': len(self.enabled_indicators),
            'indicators_checked': self.stats['indicators_checked'],
            'gaps_detected': self.stats['gaps_detected'],
            'calculation_requests_sent': self.stats['calculation_requests_sent'],
            'last_gap_check': self.stats['last_gap_check'].isoformat() if self.stats['last_gap_check'] else None,
            'enabled_indicators': list(self.enabled_indicators.keys())
        }
    
    async def cleanup(self):
        """Cleanup indicators gap service"""
        self.logger.info("Cleaning up Indicators Gap Service...")
        
        # Stop gap monitoring
        if self.gap_check_task and not self.gap_check_task.done():
            self.gap_check_task.cancel()
            try:
                await self.gap_check_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("Indicators Gap Service cleanup completed")


async def main():
    """Main function for standalone testing"""
    import sys
    import os
    
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger('IndicatorsGapMain')
    
    try:
        logger.info("Indicators Gap Service ready")
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Indicators Gap Service stopped")


if __name__ == "__main__":
    asyncio.run(main())