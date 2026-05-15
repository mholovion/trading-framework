#!/usr/bin/env python3
"""
Historical Data Service
=======================

Microservice responsible for collecting and managing historical candle data.
This service replaces the historical data collection functionality from ConnectionManager.
"""

import asyncio
import logging
import importlib
import os
import time
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from core.universal_config_manager import UniversalConfigManager
from rabbitmq.rabbitmq_client import RabbitMQClient, MessagePublisher
from core.exceptions import ConfigurationError, ExchangeError
from core.logging_config import get_historical_logger


class HistoricalDataService:
    """
    Microservice for historical data collection
    """
    
    def __init__(self, config_manager: UniversalConfigManager, 
                 database_manager: Optional[Any],
                 queue_client: RabbitMQClient):
        self.config_manager = config_manager
        self.database_manager = database_manager
        self.queue_client = queue_client
        
        if not self.database_manager:
            raise ValueError("DatabaseManager is required - must be provided by orchestrator")
        self.message_publisher = MessagePublisher(queue_client, 'historical_data_service')
        
        self.active_connections: Dict[str, Dict] = {}
        self.plugins: Dict[str, Any] = {}
        self.collection_tasks: Dict[str, asyncio.Task] = {}
        
        # Rate limiting - track request times per connection
        self.request_times: Dict[str, list] = {}  # connection_name -> list of request timestamps
        self.last_request_time: Dict[str, float] = {}  # connection_name -> last request timestamp
        
        self.logger = get_historical_logger()
    
    async def initialize(self):
        """Initialize historical data service"""
        self.logger.info("Initializing Historical Data Service...")
        
        # Initialize database if we created it
        if not hasattr(self, '_db_initialized'):
            await self.database_manager.initialize()
            self._db_initialized = True
        
        await self._setup_connections()
        await self._load_exchange_plugins()
        
        # Setup queue consumer for historical data requests
        await self._setup_queue_consumer()
        
        self.logger.info("Historical Data Service initialized")
    
    async def _setup_connections(self):
        """Setup historical data connections from config"""
        connections_config = self.config_manager.get_config('connections')
        main_config = self.config_manager.get_config('main')
        
        for connection_name, connection_config in connections_config.get('connections', {}).items():
            if not connection_config.get('enabled', False):
                continue
                
            # Only setup connections that have historical data enabled
            if not connection_config.get('historical', {}).get('enabled', False):
                self.logger.info(f"Historical data disabled for {connection_name}, skipping")
                continue
            
            self._validate_connection_config(connection_config)
            
            # Get exchange configuration
            exchange_name = connection_config['exchange']
            if exchange_name not in main_config['exchanges']:
                raise ConfigurationError(f"Connection {connection_name} references unknown exchange: {exchange_name}")
            
            exchange_config = main_config['exchanges'][exchange_name]
            if not exchange_config.get('enabled', False):
                raise ConfigurationError(f"Connection {connection_name} references disabled exchange: {exchange_name}")
            
            # Merge configurations
            merged_config = {**exchange_config, **connection_config}
            
            self.active_connections[connection_name] = {
                'config': merged_config,
                'plugin': None,
                'status': 'initialized',
                'collection_progress': {
                    'start_timestamp': None,
                    'current_timestamp': None,
                    'end_timestamp': None,
                    'total_collected': 0,
                    'batches_processed': 0
                },
                'error_count': 0,
                'last_error': None
            }
            
            self.logger.info(f"Historical data connection configured: {connection_name}")
    
    def _validate_connection_config(self, config: Dict[str, Any]):
        """Validate connection configuration for historical data"""
        required_fields = ['exchange', 'symbol', 'source_timeframe', 'enabled', 'historical']
        
        for field in required_fields:
            if field not in config or config[field] is None:
                raise ConfigurationError(f"Connection config missing required field: {field}")
        
        # Validate historical configuration
        historical = config['historical']
        hist_required = ['enabled', 'start_date', 'batch_size']
        for field in hist_required:
            if field not in historical or historical[field] is None:
                raise ConfigurationError(f"Historical config missing required field: {field}")
    
    async def _load_exchange_plugins(self):
        """Load exchange plugins for historical data collection"""
        for connection_name, connection_info in self.active_connections.items():
            config = connection_info['config']
            exchange_name = config['exchange']
            
            try:
                # Use historical-specific plugin for Historical Data Service
                plugin_name = f"{exchange_name}_historical"
                
                # Dynamic import of historical exchange plugin
                plugin_module = importlib.import_module(f'plugins.exchanges.{plugin_name}_plugin')
                plugin_class = getattr(plugin_module, f'{exchange_name.title()}HistoricalPlugin')
                
                # Initialize plugin
                plugin_instance = plugin_class(config)
                await plugin_instance.initialize()
                
                self.plugins[connection_name] = plugin_instance
                connection_info['plugin'] = plugin_instance
                connection_info['status'] = 'ready'
                
                self.logger.info(f"Historical exchange plugin loaded: {connection_name} ({plugin_name})")
                
            except Exception as e:
                connection_info['status'] = 'failed'
                connection_info['last_error'] = str(e)
                raise ConfigurationError(f"Failed to load historical exchange plugin {connection_name}: {e}")
    
    async def _setup_queue_consumer(self):
        """Setup queue consumer for historical data requests"""
        try:
            await self.queue_client.consume_messages('historical_data_requests', self._handle_historical_data_request)
            self.logger.info("Historical data request consumer setup completed")
        except Exception as e:
            self.logger.error(f"Failed to setup queue consumer: {e}")
            raise
    
    async def _wait_for_rate_limit(self, connection_name: str, config: Dict):
        """Wait to respect rate limits for the connection"""
        current_time = time.time()

        # Initialize tracking for this connection if needed
        if connection_name not in self.request_times:
            self.request_times[connection_name] = []
            self.last_request_time[connection_name] = 0

        # Get rate limiting config
        historical_config = config.get('historical', {})
        max_requests = historical_config['max_requests']
        per_seconds = historical_config['per_seconds']
        rate_limit_ms = historical_config['rate_limit_ms']

        # Clean old request times (older than per_seconds)
        self.request_times[connection_name] = [
            req_time for req_time in self.request_times[connection_name]
            if current_time - req_time <= per_seconds
        ]

        # Check limit for N requests per M seconds
        if len(self.request_times[connection_name]) >= max_requests:
            oldest_request = min(self.request_times[connection_name])
            wait_time = per_seconds - (current_time - oldest_request)
            if wait_time > 0:
                self.logger.info(
                    f"⏳ Rate limit reached for {connection_name}, waiting {wait_time:.1f}s"
                )
                await asyncio.sleep(wait_time)

        # Check minimum delay between requests
        last_request = self.last_request_time[connection_name]
        time_since_last = current_time - last_request
        min_delay = rate_limit_ms / 1000.0

        if time_since_last < min_delay:
            wait_time = min_delay - time_since_last
            self.logger.debug(
                f"⏱ Waiting {wait_time:.1f}s for rate limit on {connection_name}"
            )
            await asyncio.sleep(wait_time)

        # Record this request
        now = time.time()
        self.request_times[connection_name].append(now)
        self.last_request_time[connection_name] = now
    
    async def _handle_historical_data_request(self, message):
        """Handle historical data request from Gap Recovery Service"""
        try:
            data = message.data
            exchange = data['exchange']
            symbol = data['symbol']
            timeframe = data['timeframe']
            start_timestamp = data['start_timestamp']
            end_timestamp = data['end_timestamp']
            reason = data.get('reason', 'unknown')
            
            self.logger.info(f"Received historical data request: {exchange}/{symbol} {timeframe} "
                           f"from {datetime.fromtimestamp(start_timestamp, tz=timezone.utc)} "
                           f"to {datetime.fromtimestamp(end_timestamp, tz=timezone.utc)} (reason: {reason})")
            self.logger.debug(f"Raw timestamps: start={start_timestamp}, end={end_timestamp}")
            
            # Find appropriate connection and plugin
            plugin = None
            connection_name = None
            config = None
            for conn_name, connection_info in self.active_connections.items():
                if (connection_info['config']['exchange'] == exchange and
                    connection_info['config']['symbol'] == symbol and
                    connection_info['config']['source_timeframe'] == timeframe):
                    plugin = connection_info['plugin']
                    connection_name = conn_name
                    config = connection_info['config']
                    break
            
            if not plugin:
                self.logger.error(f"No plugin found for {exchange}/{symbol} {timeframe}")
                return
            
            # Get batch_size from config
            batch_size = config.get('historical', {}).get('batch_size', 1440)
            
            # Calculate total candles needed
            from core.timeframe_utils import TimeframeUtils
            timeframe_seconds = TimeframeUtils.get_timeframe_seconds(timeframe)
            total_candles = (end_timestamp - start_timestamp) // timeframe_seconds + 1
            
            # Check if this request needs to be split into batches
            if total_candles > batch_size:
                self.logger.info(f"Large request ({total_candles} candles), splitting into batches of {batch_size}")
                await self._process_large_request_in_batches(
                    plugin, connection_name, config, exchange, symbol, timeframe,
                    start_timestamp, end_timestamp, batch_size, timeframe_seconds
                )
            else:
                # Small request - process directly
                await self._process_single_batch(
                    plugin, connection_name, config, exchange, symbol, timeframe,
                    start_timestamp, end_timestamp, 1, 1
                )
                
        except Exception as e:
            self.logger.error(f"Error handling historical data request: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def _process_large_request_in_batches(self, plugin, connection_name, config,
                                              exchange, symbol, timeframe,
                                              start_timestamp, end_timestamp,
                                              batch_size, timeframe_seconds):
        """Process large historical request by splitting into concurrent batches."""
        CONCURRENCY = 5  # parallel REST requests per source (5×connections = ~10 total)

        try:
            total_duration = end_timestamp - start_timestamp
            batch_duration = batch_size * timeframe_seconds
            total_batches = (total_duration + batch_duration - 1) // batch_duration

            self.logger.info(f"Starting batched processing: {total_batches} batches of {batch_size} candles each (concurrency={CONCURRENCY})")

            # Build the full list of (batch_number, start, end) upfront
            batches = []
            current_start = start_timestamp
            batch_number = 1
            while current_start <= end_timestamp:
                batch_end = min(current_start + batch_duration - timeframe_seconds, end_timestamp)
                batches.append((batch_number, current_start, batch_end))
                current_start = batch_end + timeframe_seconds
                batch_number += 1

            semaphore = asyncio.Semaphore(CONCURRENCY)
            total_stored = 0
            lock = asyncio.Lock()

            async def run_batch(bn, bs, be):
                async with semaphore:
                    stored = await self._process_single_batch(
                        plugin, connection_name, config, exchange, symbol, timeframe,
                        bs, be, bn, total_batches
                    )
                    async with lock:
                        nonlocal total_stored
                        total_stored += stored

            await asyncio.gather(*[run_batch(bn, bs, be) for bn, bs, be in batches])

            self.logger.info(f"Large request completed: {total_stored} total candles stored across {total_batches} batches")

        except Exception as e:
            self.logger.error(f"Error in large request processing: {e}")
            raise
    
    async def _batch_already_complete(self, exchange: str, symbol: str, timeframe: str,
                                      start_timestamp: int, end_timestamp: int) -> bool:
        """Return True if the DB already has all candles for this batch window."""
        try:
            from sqlalchemy import func, select as _select
            from models.base import Candle
            async with self.database_manager.get_session() as session:
                result = await session.execute(
                    _select(func.count(Candle.id)).where(
                        Candle.exchange == exchange,
                        Candle.symbol == symbol,
                        Candle.timeframe == timeframe,
                        Candle.timestamp >= start_timestamp,
                        Candle.timestamp <= end_timestamp,
                    )
                )
                actual = result.scalar() or 0
            tf_seconds = end_timestamp - start_timestamp
            expected = max(1, tf_seconds // 60) if timeframe == '1m' else 1
            return actual >= expected * 0.95
        except Exception:
            return False

    async def _process_single_batch(self, plugin, connection_name, config,
                                  exchange, symbol, timeframe,
                                  start_timestamp, end_timestamp,
                                  batch_number, total_batches):
        """Process a single batch of historical data"""
        try:
            start_dt = datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
            end_dt = datetime.fromtimestamp(end_timestamp, tz=timezone.utc)

            # Skip batch if data already fully present in DB
            if await self._batch_already_complete(exchange, symbol, timeframe, start_timestamp, end_timestamp):
                if total_batches > 1:
                    self.logger.debug(f"⏭ Batch {batch_number}/{total_batches}: already in DB, skipping ({start_dt.date()} – {end_dt.date()})")
                return 0

            self.logger.info(f"Processing batch {batch_number}/{total_batches}: {exchange}/{symbol} {timeframe} from {start_dt} to {end_dt}")

            # Wait for rate limit before making API call
            await self._wait_for_rate_limit(connection_name, config)

            # Fetch historical data for this batch
            candles = await plugin.get_historical_data(symbol, timeframe, start_timestamp, end_timestamp)
            
            stored_count = 0
            if candles:
                stored_count = await self.database_manager.store_candles_batch(
                    exchange, symbol, timeframe, candles, int(time.time())
                )
                
                # Publish candles as bulk message for efficient processing
                if candles:
                    bulk_candles = []
                    for candle in candles:
                        bulk_candles.append({
                            'connection_name': f"{exchange}_{symbol}_{timeframe}",
                            'exchange': exchange,
                            'symbol': symbol,
                            'timeframe': timeframe,
                            'timestamp': candle['timestamp'],
                            'ohlcv': {
                                'open': float(candle['open']),
                                'high': float(candle['high']),
                                'low': float(candle['low']),
                                'close': float(candle['close']),
                                'volume': float(candle['volume'])
                            },
                            'source': 'historical_recovery',
                            'is_closed': True
                        })
                    
                    # Send as single bulk message instead of 1440 individual messages
                    bulk_message = self.message_publisher._create_message(
                        'candles_bulk_update', 
                        {
                            'candles': bulk_candles,
                            'batch_info': {
                                'exchange': exchange,
                                'symbol': symbol,
                                'timeframe': timeframe,
                                'count': len(bulk_candles),
                                'source': 'historical_recovery'
                            }
                        }
                    )
                    await self.queue_client.publish_message('candle_updates', bulk_message)
                
                if total_batches > 1:
                    progress_pct = (batch_number / total_batches) * 100
                    self.logger.info(f"Batch {batch_number}/{total_batches} completed: {stored_count} candles stored ({progress_pct:.1f}% done)")
                else:
                    self.logger.info(f"Historical data request completed: {stored_count} candles stored")
            else:
                if total_batches > 1:
                    self.logger.warning(f"Batch {batch_number}/{total_batches}: no data received")
                else:
                    self.logger.warning(f"No data returned for historical request: {exchange}/{symbol} {timeframe}")
            
            return stored_count
            
        except Exception as e:
            self.logger.error(f"Error in batch {batch_number}/{total_batches}: {e}")
            return 0
    
    async def start(self):
        """Start the historical data service"""
        self.logger.info("Starting Historical Data Service...")
        
        try:
            # Keep the service running to handle requests
            self.logger.info("Historical data service is running...")
            while True:
                await asyncio.sleep(1)  # Keep alive
                
        except Exception as e:
            self.logger.error(f"Historical data service error: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup historical data service"""
        self.logger.info("Cleaning up Historical Data Service...")
        
        # Cancel collection tasks
        for task in self.collection_tasks.values():
            if not task.done():
                task.cancel()
        
        if self.collection_tasks:
            await asyncio.gather(*self.collection_tasks.values(), return_exceptions=True)
        
        # Cleanup exchange plugins
        for connection_name, plugin in self.plugins.items():
            try:
                await plugin.cleanup()
                self.logger.info(f"Cleaned up plugin: {connection_name}")
            except Exception as e:
                self.logger.error(f"Error cleaning up plugin {connection_name}: {e}")
        
        self.plugins.clear()
        self.active_connections.clear()
        self.collection_tasks.clear()
        
        self.logger.info("Historical Data Service cleanup completed")

    # async def start_all_historical_collection(self):
    #     """Start historical data collection for all connections"""
    #     self.logger.info("Starting historical data collection for all connections...")
    #
    #     tasks = []
    #     for connection_name in self.active_connections.keys():
    #         task = asyncio.create_task(
    #             self.start_historical_collection(connection_name)
    #         )
    #         self.collection_tasks[connection_name] = task
    #         tasks.append(task)
    #
    #     # Wait for all collections to complete
    #     try:
    #         await asyncio.gather(*tasks, return_exceptions=True)
    #         self.logger.info("All historical data collections completed")
    #     except Exception as e:
    #         self.logger.error(f"Error during historical data collection: {e}")
    #
    # async def start_historical_collection(self, connection_name: str):
    #     """Start historical data collection for specific connection"""
    #     if connection_name not in self.active_connections:
    #         raise ConfigurationError(f"Unknown connection: {connection_name}")
    #    
    #     connection_info = self.active_connections[connection_name]
    #     config = connection_info['config']
    #     plugin = connection_info['plugin']
        
    #     try:
    #         self.logger.info(f"Starting historical data collection for {connection_name}")
            
    #         # Parse start date
    #         start_date = datetime.fromisoformat(config['historical']['start_date'].replace('Z', '+00:00'))
    #         start_timestamp = int(start_date.timestamp())
            
    #         # Use server time for end timestamp
    #         current_server_time = await plugin.get_server_time()
    #         end_timestamp = current_server_time
            
    #         # Align timestamps to timeframe boundaries
    #         timeframe_seconds = self._get_timeframe_seconds(config['source_timeframe'])
    #         start_timestamp = (start_timestamp // timeframe_seconds) * timeframe_seconds
    #         end_timestamp = (end_timestamp // timeframe_seconds) * timeframe_seconds
            
    #         self.logger.info(f"Collection period: {datetime.fromtimestamp(start_timestamp, tz=timezone.utc)} to {datetime.fromtimestamp(end_timestamp, tz=timezone.utc)}")
            
    #         # Update progress tracking
    #         progress = connection_info['collection_progress']
    #         progress['start_timestamp'] = start_timestamp
    #         progress['end_timestamp'] = end_timestamp
    #         progress['current_timestamp'] = start_timestamp
            
    #         # Check existing data and resume from latest
    #         latest_candle = self.database_manager.get_latest_candle(
    #             config['exchange'],
    #             config['symbol'],
    #             config['source_timeframe']
    #         )
            
    #         if latest_candle:
    #             start_timestamp = latest_candle['timestamp'] + timeframe_seconds
    #             progress['current_timestamp'] = start_timestamp
    #             self.logger.info(f"Resuming from latest candle: {datetime.fromtimestamp(latest_candle['timestamp'], tz=timezone.utc)}")
            
    #         # Collect data in batches
    #         batch_size = config['historical']['batch_size']
    #         delay_ms = config['historical'].get('rate_limit_ms', 1000)
            
    #         current_start = start_timestamp
    #         total_collected = 0
    #         batch_count = 0
            
    #         while current_start < end_timestamp:
    #             current_end = min(
    #                 current_start + (batch_size * timeframe_seconds),
    #                 end_timestamp
    #             )
                
    #             try:
    #                 # Wait for rate limit before making API call
    #                 await self._wait_for_rate_limit(connection_name, config)
                    
    #                 candles = await plugin.get_historical_data(
    #                     config['symbol'],
    #                     config['source_timeframe'],
    #                     current_start,
    #                     current_end
    #                 )
                    
    #                 if candles:
    #                     # Store candles using ORM
    #                     stored_count = self.database_manager.store_candles_batch(
    #                         config['exchange'],
    #                         config['symbol'],
    #                         config['source_timeframe'],
    #                         candles,
    #                         current_server_time
    #                     )
                        
    #                     total_collected += stored_count
    #                     batch_count += 1
                        
    #                     # Update progress
    #                     progress['current_timestamp'] = current_end
    #                     progress['total_collected'] = total_collected
    #                     progress['batches_processed'] = batch_count
                        
    #                     # Publish candle updates to queue for downstream processing
    #                     for candle in candles:
    #                         await self.message_publisher.publish_candle_update({
    #                             'connection_name': connection_name,
    #                             'exchange': config['exchange'],
    #                             'symbol': config['symbol'],
    #                             'timeframe': config['source_timeframe'],
    #                             'timestamp': candle['timestamp'],
    #                             'ohlcv': {
    #                                 'open': float(candle['open']),
    #                                 'high': float(candle['high']),
    #                                 'low': float(candle['low']),
    #                                 'close': float(candle['close']),
    #                                 'volume': float(candle['volume'])
    #                             },
    #                             'source': 'historical',
    #                             'is_closed': True  # Historical candles are always closed
    #                         })
                        
    #                     self.logger.debug(f"Collected batch for {connection_name}: {len(candles)} candles (total: {total_collected})")
                    
    #                 current_start = current_end
                    
    #                 # Respect rate limits
    #                 if delay_ms > 0:
    #                     await asyncio.sleep(delay_ms / 1000.0)
                        
    #             except Exception as e:
    #                 self.logger.error(f"Error collecting batch for {connection_name}: {e}")
    #                 connection_info['error_count'] += 1
    #                 connection_info['last_error'] = str(e)
                    
    #                 # Skip problematic batch and continue
    #                 current_start = current_end
            
    #         connection_info['status'] = 'completed'
    #         self.logger.info(f"Historical data collection completed for {connection_name}: {total_collected} candles")
            
    #     except Exception as e:
    #         self.logger.error(f"Historical data collection failed for {connection_name}: {e}")
    #         connection_info['status'] = 'error'
    #         connection_info['last_error'] = str(e)
    #         raise ExchangeError(f"Historical data collection failed: {e}")
    #
    # def _get_timeframe_seconds(self, timeframe: str) -> int:
    #     """Convert timeframe to seconds"""
    #     timeframe_map = {
    #         '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
    #         '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '8h': 28800, '12h': 43200,
    #         '1d': 86400, '3d': 259200, '1w': 604800, '1M': 2592000
    #     }
    #    
    #     seconds = timeframe_map.get(timeframe)
    #     if seconds is None:
    #         raise ConfigurationError(f"Unsupported timeframe: {timeframe}")
    #    
    #     return seconds
    #
    # async def get_collection_status(self) -> Dict[str, Any]:
    #     """Get status of historical data collections"""
    #     status = {}
    #   
    #     for connection_name, connection_info in self.active_connections.items():
    #         config = connection_info['config']
    #         progress = connection_info['collection_progress']
            
    #         # Calculate progress percentage
    #         progress_pct = 0.0
    #         if progress['start_timestamp'] and progress['end_timestamp']:
    #             total_range = progress['end_timestamp'] - progress['start_timestamp']
    #             if total_range > 0:
    #                 completed_range = progress['current_timestamp'] - progress['start_timestamp']
    #                 progress_pct = (completed_range / total_range) * 100
            
    #         status[connection_name] = {
    #             'exchange': config['exchange'],
    #             'symbol': config['symbol'],
    #             'timeframe': config['source_timeframe'],
    #             'status': connection_info['status'],
    #             'progress_percentage': progress_pct,
    #             'total_collected': progress['total_collected'],
    #             'batches_processed': progress['batches_processed'],
    #             'error_count': connection_info['error_count'],
    #             'last_error': connection_info['last_error'],
    #             'start_time': progress['start_timestamp'],
    #             'current_time': progress['current_timestamp'],
    #             'end_time': progress['end_timestamp']
    #         }
    #   
    #     return status
    
