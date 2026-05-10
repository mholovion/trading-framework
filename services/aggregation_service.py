#!/usr/bin/env python3
"""
Simplified Aggregation Service
==============================

Simple on-demand aggregation service that works like an indicator:
1. Receives aggregation request
2. Checks if source data is complete
3. Aggregates if 100% complete data available
4. Publishes aggregated candle
5. That's it!
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from decimal import Decimal

from core.universal_config_manager import UniversalConfigManager
from core.timeframe_utils import TimeframeUtils
from core.aggregation_config import AggregationConfig
from rabbitmq.rabbitmq_client import RabbitMQClient, QueueMessage, MessagePublisher
from models.base import Candle
from sqlalchemy import asc, and_


class AggregationService:
    """
    On-demand aggregation service:
    1. Listen for aggregation_request messages
    2. Verify 100% data completeness 
    3. Aggregate OHLCV data
    4. Publish aggregated candle
    """
    
    def __init__(self, config_manager: UniversalConfigManager,
                 database_manager: Optional[Any],
                 queue_client: RabbitMQClient):
        self.config_manager = config_manager
        self.database_manager = database_manager
        self.queue_client = queue_client
        self.message_publisher = MessagePublisher(queue_client, 'aggregation_service')
        self.aggregation_config = AggregationConfig()
        
        if not self.database_manager:
            raise ValueError("DatabaseManager is required")
        
        # Simple semaphore to control concurrent aggregations
        self.processing_semaphore = asyncio.Semaphore(5)  # Max 5 concurrent aggregations
        
        from core.logging_config import setup_service_logging
        self.logger = setup_service_logging('aggregation')
        
        # Statistics
        self.stats = {
            'aggregation_requests_received': 0,
            'aggregations_completed': 0,
            'aggregations_skipped_incomplete_data': 0,
            'aggregations_failed': 0,
            'start_time': datetime.now(timezone.utc)
        }
    
    async def initialize(self):
        """Initialize aggregation service"""
        self.logger.info("Initializing Simplified Aggregation Service...")
        
        # Initialize database if needed
        if not hasattr(self, '_db_initialized'):
            await self.database_manager.initialize()
            self._db_initialized = True
        
        # Setup queue consumer for aggregation requests
        await self.queue_client.consume_messages('aggregation_requests', self._handle_aggregation_request)
        
        self.logger.info("Simplified Aggregation Service initialized")
    
    async def _handle_aggregation_request(self, message: QueueMessage):
        """Handle incoming aggregation request"""
        async with self.processing_semaphore:
            try:
                if message.type != 'aggregation_request':
                    return
                
                data = message.data
                self.stats['aggregation_requests_received'] += 1
                
                # Extract request parameters
                connection_name = data.get('connection_name')
                exchange = data.get('exchange')
                symbol = data.get('symbol')
                source_timeframe = data.get('source_timeframe')
                target_timeframe = data.get('target_timeframe')
                start_timestamp = data.get('start_timestamp')
                end_timestamp = data.get('end_timestamp')
                batch_id = data.get('batch_id', f"agg_{int(time.time())}")
                
                # Get source timeframe from config if not provided
                if not source_timeframe:
                    source_timeframe = self.aggregation_config.get_source_timeframe(exchange, symbol, target_timeframe)
                    if not source_timeframe:
                        self.logger.error(f"No source timeframe configured for {exchange}/{symbol}/{target_timeframe}")
                        return
                
                # Validate required parameters
                if not all([exchange, symbol, source_timeframe, target_timeframe, start_timestamp, end_timestamp]):
                    self.logger.error(f"Missing required parameters in aggregation request: {data}")
                    return
                
                source_info = f" (from config)" if not data.get('source_timeframe') else ""
                self.logger.info(f"Processing aggregation request {batch_id}: "
                               f"{exchange}/{symbol} {source_timeframe}→{target_timeframe}{source_info} "
                               f"from {datetime.fromtimestamp(start_timestamp, tz=timezone.utc)} "
                               f"to {datetime.fromtimestamp(end_timestamp, tz=timezone.utc)}")
                
                # Perform aggregation
                result = await self._aggregate_timeframe_range(
                    connection_name, exchange, symbol, source_timeframe, 
                    target_timeframe, start_timestamp, end_timestamp, batch_id
                )
                
                if result:
                    self.stats['aggregations_completed'] += 1
                    self.logger.info(f"Aggregation completed: {batch_id}")
                else:
                    self.stats['aggregations_skipped_incomplete_data'] += 1
                    self.logger.warning(f"Aggregation skipped due to incomplete data: {batch_id}")
                
            except Exception as e:
                self.stats['aggregations_failed'] += 1
                self.logger.error(f"Aggregation request failed: {e}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def _aggregate_timeframe_range(self, connection_name: str, exchange: str, symbol: str,
                                       source_timeframe: str, target_timeframe: str,
                                       start_timestamp: int, end_timestamp: int, batch_id: str) -> bool:
        """Aggregate data for specific timeframe range, processing in time-bounded chunks."""
        try:
            target_seconds = TimeframeUtils.get_timeframe_seconds(target_timeframe)
            # Limit each DB query to at most 500 target periods worth of source data.
            chunk_seconds = 500 * target_seconds

            total_aggregated = 0
            total_source = 0
            chunk_start = start_timestamp

            while chunk_start < end_timestamp:
                chunk_end = min(chunk_start + chunk_seconds, end_timestamp)

                source_candles = await self._get_source_candles(
                    exchange, symbol, source_timeframe, chunk_start, chunk_end
                )

                if source_candles:
                    period_groups = self._group_candles_by_periods(source_candles, target_timeframe)
                    aggregated_candles = []

                    for period_start, period_candles in period_groups.items():
                        if not period_candles:
                            continue
                        if not self._is_period_complete(period_candles, period_start, source_timeframe, target_timeframe):
                            self.logger.debug(f"Incomplete period {datetime.fromtimestamp(period_start, tz=timezone.utc)}, skipping")
                            continue
                        aggregated_candles.append(
                            self._create_aggregated_candle(period_candles, period_start)
                        )

                    if aggregated_candles:
                        await self._store_aggregated_candles_batch(
                            exchange, symbol, target_timeframe, aggregated_candles, source_timeframe
                        )
                        await self._publish_aggregated_candles_bulk(
                            connection_name, exchange, symbol, target_timeframe, aggregated_candles
                        )
                        total_aggregated += len(aggregated_candles)
                        total_source += len(source_candles)

                chunk_start = chunk_end

            if total_aggregated > 0:
                self.logger.info(f"Aggregated {total_aggregated} {target_timeframe} candles from {total_source} {source_timeframe} candles")
                return True

            self.logger.warning(f"No source candles found for {exchange}/{symbol}/{source_timeframe}")
            return False

        except Exception as e:
            self.logger.error(f"Error in aggregation: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
    
    async def _get_source_candles(self, exchange: str, symbol: str, timeframe: str,
                                start_timestamp: int, end_timestamp: int) -> List[Dict]:
        """Get source candles from database (runs in thread pool to avoid blocking event loop)."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._get_source_candles_sync,
            exchange, symbol, timeframe, start_timestamp, end_timestamp
        )

    def _get_source_candles_sync(self, exchange: str, symbol: str, timeframe: str,
                                 start_timestamp: int, end_timestamp: int) -> List[Dict]:
        try:
            with self.database_manager.get_session() as session:
                candles = session.query(Candle).filter(
                    and_(
                        Candle.exchange == exchange,
                        Candle.symbol == symbol,
                        Candle.timeframe == timeframe,
                        Candle.timestamp >= start_timestamp,
                        Candle.timestamp < end_timestamp,
                    )
                ).order_by(asc(Candle.timestamp)).all()
                return [
                    {
                        'timestamp': c.timestamp,
                        'open': float(c.open_price),
                        'high': float(c.high_price),
                        'low': float(c.low_price),
                        'close': float(c.close_price),
                        'volume': float(c.volume),
                    }
                    for c in candles
                ]
        except Exception as e:
            self.logger.error(f"Error getting source candles: {e}")
            return []
    
    def _is_data_complete(self, candles: List[Dict], timeframe: str, 
                         start_timestamp: int, end_timestamp: int) -> bool:
        """Check if source data is 100% complete"""
        if not candles:
            return False
        
        timeframe_seconds = TimeframeUtils.get_timeframe_seconds(timeframe)
        
        # For inclusive range [start_timestamp, end_timestamp]
        # Expected candles = number of complete periods that fit in the range
        time_diff = end_timestamp - start_timestamp
        
        if time_diff == 0:
            expected_candles = 0  # No periods in zero-duration range
        else:
            # Number of complete periods for the range [start, end) exclusive
            expected_candles = int(time_diff // timeframe_seconds)
        actual_candles = len(candles)
        
        # Must have exactly the expected number of candles
        if actual_candles != expected_candles:
            self.logger.debug(f"Data completeness: {actual_candles}/{expected_candles} ({actual_candles/expected_candles*100:.1f}%)")
            return False
        
        # Check for gaps in sequence
        for i in range(1, len(candles)):
            expected_ts = candles[i-1]['timestamp'] + timeframe_seconds
            actual_ts = candles[i]['timestamp']
            
            if actual_ts != expected_ts:
                expected_dt = datetime.fromtimestamp(expected_ts, tz=timezone.utc)
                actual_dt = datetime.fromtimestamp(actual_ts, tz=timezone.utc)
                self.logger.warning(f"Gap detected in {timeframe}: expected {expected_dt}, got {actual_dt} (gap at position {i})")
                return False
        
        return True
    
    def _group_candles_by_periods(self, source_candles: List[Dict], target_timeframe: str) -> Dict[int, List[Dict]]:
        """Group candles by target timeframe periods"""
        periods = {}
        
        for candle in source_candles:
            period_start = TimeframeUtils.get_period_start(candle['timestamp'], target_timeframe)
            
            if period_start not in periods:
                periods[period_start] = []
            
            periods[period_start].append(candle)
        
        return periods
    
    def _is_period_complete(self, period_candles: List[Dict], period_start: int,
                          source_timeframe: str, target_timeframe: str) -> bool:
        """Check if period has complete data (100% requirement)"""
        if not period_candles:
            return False
        
        source_seconds = TimeframeUtils.get_timeframe_seconds(source_timeframe)
        target_seconds = TimeframeUtils.get_timeframe_seconds(target_timeframe)
        expected_candles = target_seconds // source_seconds
        
        # Must have exactly the expected number of candles
        if len(period_candles) != expected_candles:
            return False
        
        # Check for gaps in the period
        sorted_candles = sorted(period_candles, key=lambda x: x['timestamp'])
        
        for i in range(1, len(sorted_candles)):
            expected_ts = sorted_candles[i-1]['timestamp'] + source_seconds
            actual_ts = sorted_candles[i]['timestamp']
            
            if actual_ts != expected_ts:
                return False
        
        return True
    
    def _create_aggregated_candle(self, period_candles: List[Dict], period_start: int) -> Dict:
        """Create aggregated OHLCV candle from period candles"""
        if not period_candles:
            raise ValueError("No candles to aggregate")
        
        # Sort by timestamp to ensure correct OHLC calculation
        sorted_candles = sorted(period_candles, key=lambda x: x['timestamp'])
        
        # OHLCV aggregation
        open_price = Decimal(str(sorted_candles[0]['open']))
        close_price = Decimal(str(sorted_candles[-1]['close']))
        
        high_price = max(Decimal(str(candle['high'])) for candle in sorted_candles)
        low_price = min(Decimal(str(candle['low'])) for candle in sorted_candles)
        volume = sum(Decimal(str(candle['volume'])) for candle in sorted_candles)
        
        return {
            'timestamp': period_start,
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': volume,
            'source_candles_count': len(sorted_candles)
        }
    
    async def _store_aggregated_candles_batch(self, exchange: str, symbol: str, timeframe: str,
                                             candles: List[Dict], source_timeframe: str):
        """Upsert a list of aggregated candles in a single DB session (thread pool)."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, self._store_aggregated_candles_batch_sync,
            exchange, symbol, timeframe, candles, source_timeframe
        )

    def _store_aggregated_candles_batch_sync(self, exchange: str, symbol: str, timeframe: str,
                                             candles: List[Dict], source_timeframe: str):
        try:
            timestamps = [c['timestamp'] for c in candles]
            with self.database_manager.get_session() as session:
                existing_map = {
                    row.timestamp: row
                    for row in session.query(Candle).filter(
                        and_(
                            Candle.exchange == exchange,
                            Candle.symbol == symbol,
                            Candle.timeframe == timeframe,
                            Candle.source_timeframe == source_timeframe,
                            Candle.timestamp.in_(timestamps)
                        )
                    ).all()
                }

                now = datetime.now(timezone.utc)
                for cd in candles:
                    existing = existing_map.get(cd['timestamp'])
                    if existing:
                        existing.open_price = float(cd['open'])
                        existing.high_price = float(cd['high'])
                        existing.low_price = float(cd['low'])
                        existing.close_price = float(cd['close'])
                        existing.volume = float(cd['volume'])
                        existing.source_candles_count = cd.get('source_candles_count')
                        existing.data_completeness = 100.0
                        existing.updated_at = now
                    else:
                        session.add(Candle(
                            exchange=exchange,
                            symbol=symbol,
                            timeframe=timeframe,
                            timestamp=cd['timestamp'],
                            open_price=float(cd['open']),
                            high_price=float(cd['high']),
                            low_price=float(cd['low']),
                            close_price=float(cd['close']),
                            volume=float(cd['volume']),
                            source_type='aggregated',
                            source_timeframe=source_timeframe,
                            aggregation_method='ohlc_standard',
                            source_candles_count=cd.get('source_candles_count'),
                            data_completeness=100.0,
                            created_at=now,
                        ))
                session.commit()
        except Exception as e:
            self.logger.error(f"Error storing aggregated candles batch: {e}")
            raise
    
    async def _publish_aggregated_candle(self, connection_name: str, exchange: str, symbol: str,
                                       timeframe: str, candle_data: Dict):
        """Publish aggregated candle to candle_updates queue"""
        try:
            candle_update_data = {
                'connection_name': connection_name,
                'exchange': exchange,
                'symbol': symbol,
                'timeframe': timeframe,
                'timestamp': candle_data['timestamp'],
                'ohlcv': {
                    'open': float(candle_data['open']),
                    'high': float(candle_data['high']),
                    'low': float(candle_data['low']),
                    'close': float(candle_data['close']),
                    'volume': float(candle_data['volume'])
                },
                'source': 'aggregated',
                'is_closed': True,
                'metadata': {
                    'source_candles_count': candle_data.get('source_candles_count', 0),
                    'aggregation_quality': 'complete'
                }
            }
            
            message = self.message_publisher._create_message('candle_update', candle_update_data)
            await self.queue_client.publish_message('candle_updates', message)
            
            self.logger.debug(f"Published aggregated {timeframe} candle: {exchange}/{symbol} at {datetime.fromtimestamp(candle_data['timestamp'], tz=timezone.utc)}")
            
        except Exception as e:
            self.logger.error(f"Error publishing aggregated candle: {e}")
            raise
    
    async def _publish_aggregated_candles_bulk(self, connection_name: str, exchange: str,
                                              symbol: str, timeframe: str, candles: List[Dict]):
        """Publish aggregated candles as one bulk message for efficient pipeline processing."""
        try:
            if not candles:
                return
            bulk_candles = [
                {
                    'connection_name': connection_name,
                    'exchange': exchange,
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'timestamp': cd['timestamp'],
                    'ohlcv': {
                        'open': float(cd['open']),
                        'high': float(cd['high']),
                        'low': float(cd['low']),
                        'close': float(cd['close']),
                        'volume': float(cd['volume']),
                    },
                    'source': 'aggregated',
                    'is_closed': True,
                }
                for cd in candles
            ]
            message = self.message_publisher._create_message(
                'candles_bulk_update',
                {
                    'candles': bulk_candles,
                    'batch_info': {
                        'exchange': exchange,
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'count': len(bulk_candles),
                        'source': 'aggregated',
                    },
                },
            )
            await self.queue_client.publish_message('candle_updates', message)
            self.logger.debug(f"Published {len(candles)} aggregated {timeframe} candles as bulk for {exchange}/{symbol}")
        except Exception as e:
            self.logger.error(f"Error publishing aggregated candles bulk: {e}")
            raise

    async def get_statistics(self) -> Dict[str, Any]:
        """Get aggregation service statistics"""
        uptime = datetime.now(timezone.utc) - self.stats['start_time']
        
        return {
            'uptime_seconds': uptime.total_seconds(),
            'aggregation_requests_received': self.stats['aggregation_requests_received'],
            'aggregations_completed': self.stats['aggregations_completed'],
            'aggregations_skipped_incomplete_data': self.stats['aggregations_skipped_incomplete_data'],
            'aggregations_failed': self.stats['aggregations_failed'],
            'success_rate': (self.stats['aggregations_completed'] / max(self.stats['aggregation_requests_received'], 1)) * 100,
            'aggregations_per_hour': self.stats['aggregations_completed'] / max(uptime.total_seconds() / 3600, 1)
        }
    
    async def start(self):
        """Start aggregation service - runs continuously listening for requests"""
        self.logger.info("Starting Aggregation Service...")
        try:
            # Service runs by listening to queue, no additional startup needed
            # The queue consumer was already set up in initialize()
            self.logger.info("Aggregation Service started and listening for requests")
            
            # Keep service running (similar to indicators service)
            while True:
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            self.logger.info("Aggregation Service stopped")
            raise
        except Exception as e:
            self.logger.error(f"Aggregation Service error: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup aggregation service"""
        self.logger.info("Cleaning up Aggregation Service...")
        # No background tasks to cleanup in this simple version
        self.logger.info("Aggregation Service cleanup completed")


async def main():
    """Main function for standalone testing"""
    import sys
    import os
    
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger('AggregationMain')
    
    try:
        # This would be initialized by orchestrator in real usage
        logger.info("Simplified Aggregation Service ready")
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Aggregation Service stopped")


if __name__ == "__main__":
    asyncio.run(main())