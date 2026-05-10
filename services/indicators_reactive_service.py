#!/usr/bin/env python3
"""
Simplified Indicators Reactive Service
======================================

Simple reactive service for indicator calculations without complex dependency tracking.
Calculates indicators directly based on available candle data.
"""

import asyncio
import logging
import importlib
import os
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

from core.universal_config_manager import UniversalConfigManager
from rabbitmq.rabbitmq_client import RabbitMQClient, QueueMessage, MessagePublisher
from models.base import Candle, Indicator
from sqlalchemy import and_
from core.exceptions import ConfigurationError
from core.logging_config import get_indicators_logger


class IndicatorsReactiveService:
    """
    Simplified reactive service for automatic indicator calculations
    """
    
    def __init__(self, config_manager: UniversalConfigManager, 
                 database_manager: Optional[Any],
                 queue_client: RabbitMQClient):
        self.config_manager = config_manager
        self.database_manager = database_manager
        self.queue_client = queue_client
        self.message_publisher = MessagePublisher(queue_client, 'indicators_service')
        
        if not self.database_manager:
            raise ValueError("DatabaseManager is required - must be provided by orchestrator")
        
        # Load indicator plugins
        self.indicator_plugins: Dict[str, Any] = {}
        
        # Concurrency control to prevent database overload
        self.calculation_semaphore = asyncio.Semaphore(10)  # Increased to 10 for faster bulk processing
        
        self.logger = get_indicators_logger()
        
        # Statistics
        self.stats = {
            'candle_updates_received': 0,
            'indicators_calculated': 0,
            'calculation_errors': 0,
            'start_time': datetime.now(timezone.utc)
        }
        
        self.running = False
    
    async def initialize(self):
        """Initialize indicators reactive service"""
        self.logger.info("Initializing Simplified Indicators Reactive Service...")
        
        # Load indicator plugins
        await self._load_indicator_plugins()
        
        # Setup queue consumers
        await self._setup_queue_consumers()
        
        self.logger.info("Simplified Indicators Reactive Service initialized")
    
    async def _load_indicator_plugins(self):
        """Load indicator plugins dynamically"""
        indicators_config = self.config_manager.get_config('indicators')
        
        for indicator_name, indicator_config in indicators_config.get('indicators', {}).items():
            if not indicator_config.get('enabled', True):
                continue
                
            try:
                plugin_name = indicator_config['plugin']
                
                # Import plugin module
                module_path = f"plugins.indicators.{plugin_name}"
                module = importlib.import_module(module_path)
                
                # Get plugin class (capitalize first letter)
                class_name = f"{plugin_name.capitalize()}Plugin"
                plugin_class = getattr(module, class_name)
                
                # Initialize plugin with full config (not just parameters)
                plugin = plugin_class(indicator_config)
                self.indicator_plugins[indicator_name] = plugin
                
                self.logger.info(f"Loaded indicator plugin: {indicator_name} ({plugin_name})")
                
            except Exception as e:
                self.logger.error(f"Failed to load indicator plugin {indicator_name}: {e}")
                continue
        
        self.logger.info(f"Loaded {len(self.indicator_plugins)} indicator plugins")
    
    async def _setup_queue_consumers(self):
        """Setup message queue consumers"""
        # Consumer for candle updates that trigger indicator recalculation
        await self.queue_client.consume_messages('candle_updates', self._handle_candle_update)
        
        # Consumer for indicator calculation requests from gap service
        await self.queue_client.consume_messages('indicator_calculation_requests', self._handle_indicator_calculation_request)
        
        self.logger.info("Queue consumers setup completed")
    
    async def _handle_candle_update(self, message: QueueMessage):
        """Handle candle update message and trigger dependent indicators"""
        try:
            self.logger.info(f"Received candle update from {message.source_service}")
            
            if message.type != 'candle_update':
                self.logger.warning(f"Ignoring non-candle message: {message.type}")
                return
            
            data = message.data
            self.logger.debug(f"Processing candle: {data.get('exchange')}/{data.get('symbol')}/{data.get('timeframe')}, closed={data.get('is_closed', False)}")
            
            # Only process closed candles for indicator calculations
            if not data.get('is_closed', False):
                self.logger.debug(f"Ignoring active candle for {data.get('connection_name')}")
                return
            
            self.stats['candle_updates_received'] += 1
            
            # Find indicators that should be calculated for this candle
            connection_name = data.get('connection_name')
            exchange = data['exchange']
            symbol = data['symbol'] 
            timeframe = data['timeframe']
            timestamp = data['timestamp']
            
            # Get indicators that use this connection and timeframe
            matching_indicators = self._get_matching_indicators(connection_name, timeframe)
            
            # Calculate each matching indicator
            for indicator_name in matching_indicators:
                try:
                    self.logger.info(f"Calculate indicator: {indicator_name} for {exchange}/{symbol}/{timeframe} at {timestamp}")
                    await self._calculate_indicator(indicator_name, exchange, symbol, timeframe, timestamp)
                except Exception as e:
                    self.logger.error(f"Failed to calculate indicator {indicator_name}: {e}")
                    self.stats['calculation_errors'] += 1
            
        except Exception as e:
            self.logger.error(f"Error handling candle update: {e}")
            self.stats['calculation_errors'] += 1
    
    async def _handle_indicator_calculation_request(self, message: QueueMessage):
        """Handle indicator calculation request from indicators gap service"""
        try:
            self.logger.info(f"Received calculation request from {message.source_service}")
            
            if message.type != 'indicator_calculation_request':
                self.logger.warning(f"Unexpected message type: {message.type}")
                return
            
            data = message.data
            indicator_name = data['indicator_name']
            exchange = data['exchange']
            symbol = data['symbol']
            timeframe = data['timeframe']
            start_timestamp = data['start_timestamp']
            end_timestamp = data['end_timestamp']
            batch_id = data.get('batch_id', 'no_batch_id')
            
            self.logger.info(f"Processing calculation request for {indicator_name}: "
                           f"{exchange}/{symbol}/{timeframe} "
                           f"from {datetime.fromtimestamp(start_timestamp, tz=timezone.utc)} "
                           f"to {datetime.fromtimestamp(end_timestamp, tz=timezone.utc)} "
                           f"[batch: {batch_id}]")
            
            # Calculate indicators for the range using optimized batch processing
            await self._calculate_indicator_range(indicator_name, exchange, symbol, timeframe, 
                                                start_timestamp, end_timestamp)
                           
        except Exception as e:
            self.logger.error(f"Error handling indicator calculation request: {e}")
            self.stats['calculation_errors'] += 1
    
    def _get_matching_indicators(self, connection_name: str, timeframe: str) -> List[str]:
        """Get list of indicator names that should be calculated for this connection/timeframe"""
        matching_indicators = []
        indicators_config = self.config_manager.get_config('indicators')
        
        for indicator_name, indicator_config in indicators_config.get('indicators', {}).items():
            if (indicator_config.get('enabled', False) and 
                indicator_config.get('connection') == connection_name and
                indicator_config.get('source_timeframe') == timeframe):
                matching_indicators.append(indicator_name)
        
        return matching_indicators
    
    async def _calculate_indicator(self, indicator_name: str, exchange: str, symbol: str, 
                                 timeframe: str, timestamp: int):
        """Calculate single indicator for given timestamp"""
        async with self.calculation_semaphore:  # Limit concurrent calculations
            try:
                plugin = self.indicator_plugins.get(indicator_name)
                if not plugin:
                    self.logger.error(f"Plugin not found for {indicator_name}")
                    return
                
                # Get historical data needed for calculation
                with self.database_manager.get_session() as session:
                    # Get required candles for calculation (including lookback)
                    required_periods = getattr(plugin, 'get_required_periods', lambda: 50)()
                    
                    candles = session.query(Candle).filter(
                        and_(
                            Candle.exchange == exchange,
                            Candle.symbol == symbol,
                            Candle.timeframe == timeframe,
                            Candle.timestamp <= timestamp
                        )
                    ).order_by(Candle.timestamp.desc()).limit(required_periods).all()
                    
                    if len(candles) < required_periods:
                        self.logger.debug(f"Insufficient data for {indicator_name}: need {required_periods}, have {len(candles)}")
                        return
                    
                    # Reverse to chronological order
                    candles.reverse()
                    
                    # Convert to format expected by plugin
                    candle_data = []
                    for candle in candles:
                        candle_data.append({
                            'timestamp': candle.timestamp,
                            'open': float(candle.open_price),
                            'high': float(candle.high_price),
                            'low': float(candle.low_price),
                            'close': float(candle.close_price),
                            'volume': float(candle.volume)
                        })
                    
                    # Calculate indicator value
                    result = await plugin.calculate(candle_data)
                    
                    if result is not None and 'value' in result:
                        # Get connection name from config
                        indicators_config = self.config_manager.get_config('indicators')
                        connection_name = indicators_config['indicators'][indicator_name]['connection']
                        
                        # Publish indicator result to database update service
                        await self.queue_client.publish_message('indicator_updates', 
                            self.message_publisher._create_message('indicator_calculated', {
                                'connection_name': connection_name,
                                'indicator_name': indicator_name,
                                'timestamp': timestamp,
                                'value': result['value'],
                                'metadata': result.get('metadata', {}),
                                'source_candle': {
                                    'exchange': exchange,
                                    'symbol': symbol,
                                    'timeframe': timeframe,
                                    'timestamp': timestamp
                                }
                            })
                        )
                        
                        self.stats['indicators_calculated'] += 1
                        self.logger.debug(f" {indicator_name}: {result['value']} @ {timestamp}")
                    
            except Exception as e:
                self.logger.error(f"Error calculating indicator {indicator_name}: {e}")
                self.stats['calculation_errors'] += 1
                raise
    
    def _fetch_candles_for_range_sync(self, exchange: str, symbol: str, timeframe: str,
                                      warmup_start: int, end_timestamp: int) -> list:
        """Fetch all candles (warmup + target) in one query. Runs in thread pool."""
        with self.database_manager.get_session() as session:
            rows = session.query(Candle).filter(
                and_(
                    Candle.exchange == exchange,
                    Candle.symbol == symbol,
                    Candle.timeframe == timeframe,
                    Candle.timestamp >= warmup_start,
                    Candle.timestamp <= end_timestamp,
                )
            ).order_by(Candle.timestamp.asc()).all()
            return [
                {
                    'timestamp': c.timestamp,
                    'open': float(c.open_price),
                    'high': float(c.high_price),
                    'low': float(c.low_price),
                    'close': float(c.close_price),
                    'volume': float(c.volume),
                }
                for c in rows
            ]

    async def _calculate_indicator_range(self, indicator_name: str, exchange: str, symbol: str,
                                         timeframe: str, start_timestamp: int, end_timestamp: int):
        """Calculate indicator for a range using a single DB fetch + sliding window."""
        async with self.calculation_semaphore:
            try:
                plugin = self.indicator_plugins.get(indicator_name)
                if not plugin:
                    self.logger.error(f"Plugin not found for {indicator_name}")
                    return

                required_periods = getattr(plugin, 'get_required_periods', lambda: 50)()

                # Derive timeframe seconds from the gap between first two candles (or fallback)
                from core.timeframe_utils import TimeframeUtils
                tf_seconds = TimeframeUtils.get_timeframe_seconds(timeframe)
                warmup_start = start_timestamp - required_periods * tf_seconds

                # Single query: warmup candles + target candles
                loop = asyncio.get_event_loop()
                all_candles = await loop.run_in_executor(
                    None, self._fetch_candles_for_range_sync,
                    exchange, symbol, timeframe, warmup_start, end_timestamp
                )

                if not all_candles:
                    self.logger.warning(f"No candles found for {indicator_name} range calculation")
                    return

                # Find where the target range begins
                target_start_idx = next(
                    (i for i, c in enumerate(all_candles) if c['timestamp'] >= start_timestamp),
                    len(all_candles)
                )
                target_count = len(all_candles) - target_start_idx
                self.logger.info(f"Calculating {indicator_name} for {target_count} candles "
                                 f"(warmup: {target_start_idx})")

                PUBLISH_BATCH = 100
                calculated_indicators = []
                total_calculated = 0

                for i in range(target_start_idx, len(all_candles)):
                    window_start = max(0, i - required_periods + 1)
                    window = all_candles[window_start: i + 1]

                    if len(window) < required_periods:
                        continue

                    result = await plugin.calculate(window)

                    if result and 'value' in result:
                        calculated_indicators.append({
                            'indicator_name': indicator_name,
                            'timestamp': all_candles[i]['timestamp'],
                            'value': result['value'],
                            'metadata': result.get('metadata', {}),
                            'exchange': exchange,
                            'symbol': symbol,
                            'timeframe': timeframe,
                        })
                        total_calculated += 1

                    if len(calculated_indicators) >= PUBLISH_BATCH:
                        await self._bulk_publish_indicators(calculated_indicators)
                        calculated_indicators = []
                        self.logger.info(f" {indicator_name} batch progress: {total_calculated}/{target_count}")
                        await asyncio.sleep(0)  # yield to event loop

                if calculated_indicators:
                    await self._bulk_publish_indicators(calculated_indicators)

                self.logger.info(
                    f"Completed {indicator_name} range calculation: {total_calculated}/{target_count} successful"
                )

            except Exception as e:
                self.logger.error(f"Error in indicator range calculation: {e}")
    
    async def _bulk_publish_indicators(self, calculated_indicators: List[dict]):
        """Bulk publish calculated indicators as single message to reduce queue overhead"""
        try:
            if not calculated_indicators:
                return
                
            indicators_config = self.config_manager.get_config('indicators')
            
            # Prepare bulk data with connection names
            bulk_indicators = []
            for indicator_data in calculated_indicators:
                indicator_name = indicator_data['indicator_name']
                connection_name = indicators_config['indicators'][indicator_name]['connection']
                
                bulk_indicators.append({
                    'connection_name': connection_name,
                    'indicator_name': indicator_name,
                    'timestamp': indicator_data['timestamp'],
                    'value': indicator_data['value'],
                    'metadata': indicator_data['metadata'],
                    'source_candle': {
                        'exchange': indicator_data['exchange'],
                        'symbol': indicator_data['symbol'],
                        'timeframe': indicator_data['timeframe'],
                        'timestamp': indicator_data['timestamp']
                    }
                })
            
            # Send single message with array of indicators
            await self.queue_client.publish_message('indicator_updates', 
                self.message_publisher._create_message('indicators_calculated_bulk', {
                    'indicators': bulk_indicators,
                    'count': len(bulk_indicators)
                })
            )
            
            self.stats['indicators_calculated'] += len(calculated_indicators)
            self.logger.debug(f"Bulk published {len(calculated_indicators)} indicators")
            
        except Exception as e:
            self.logger.error(f"Error bulk publishing indicators: {e}")
    
    async def start(self):
        """Start the reactive service"""
        self.running = True
        
        try:
            # Keep service running
            while self.running:
                await asyncio.sleep(1)
                
                # Log statistics periodically
                if self.stats['indicators_calculated'] % 100 == 0 and self.stats['indicators_calculated'] > 0:
                    await self._log_statistics()
        
        except asyncio.CancelledError:
            self.logger.info("Indicators Reactive Service cancelled")
        except Exception as e:
            self.logger.error(f"Error in indicators reactive service: {e}")
        finally:
            self.running = False
    
    async def stop(self):
        """Stop the reactive service"""
        self.logger.info("Stopping Indicators Reactive Service...")
        self.running = False
        
        # Close queue connections
        if self.queue_client:
            await self.queue_client.close()
        
        self.logger.info("Indicators Reactive Service stopped")
    
    async def _log_statistics(self):
        """Log service statistics"""
        uptime = datetime.now(timezone.utc) - self.stats['start_time']
        
        self.logger.info(f"Indicators Reactive Service Statistics:")
        self.logger.info(f"Uptime: {uptime}")
        self.logger.info(f"Candle updates received: {self.stats['candle_updates_received']}")
        self.logger.info(f"Indicators calculated: {self.stats['indicators_calculated']}")
        self.logger.info(f"Calculation errors: {self.stats['calculation_errors']}")
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get service statistics"""
        uptime = datetime.now(timezone.utc) - self.stats['start_time']
        
        return {
            'uptime_seconds': uptime.total_seconds(),
            'candle_updates_received': self.stats['candle_updates_received'],
            'indicators_calculated': self.stats['indicators_calculated'],
            'calculation_errors': self.stats['calculation_errors'],
            'loaded_plugins': list(self.indicator_plugins.keys())
        }


# Main function for running the service standalone
async def main():
    """Main function for running indicators reactive service"""
    
    logger = logging.getLogger('indicators_main')
    logger.info("Starting Indicators Reactive Service...")
    
    try:
        # Initialize config manager
        config_manager = UniversalConfigManager()
        config_manager.load_all_configs()
        
        # Initialize queue client
        rabbitmq_url = os.getenv('RABBITMQ_URL')
        if not rabbitmq_url:
            raise ValueError("RABBITMQ_URL environment variable is required")
        queue_client = RabbitMQClient(rabbitmq_url)
        await queue_client.connect()
        
        # Initialize database manager (would be passed from orchestrator in real scenario)
        from core.database import DatabaseManager
        database_manager = DatabaseManager()
        await database_manager.initialize()
        
        # Initialize indicators reactive service
        service = IndicatorsReactiveService(config_manager, database_manager, queue_client)
        await service.initialize()
        
        # Start the service
        logger.info("Indicators Reactive Service is running. Press Ctrl+C to stop.")
        await service.start()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    finally:
        if 'service' in locals():
            await service.stop()
        
        logger.info("Indicators Reactive Service shutdown completed")


if __name__ == "__main__":
    asyncio.run(main())