#!/usr/bin/env python3
"""
Real-time Data Service
======================

Microservice responsible for collecting and managing real-time candle data.
This service replaces the real-time data collection functionality from ConnectionManager.
"""

import asyncio
import logging
import os
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from core.universal_config_manager import UniversalConfigManager
from rabbitmq.rabbitmq_client import RabbitMQClient, MessagePublisher
from core.exceptions import ConfigurationError, ExchangeError
from core.logging_config import get_realtime_logger


class RealtimeDataService:
    """
    Microservice for real-time data collection
    """
    
    def __init__(self, config_manager: UniversalConfigManager,
                 clickhouse: Any,
                 queue_client: RabbitMQClient,
                 exchange_plugins: Optional[Dict[str, Any]] = None):
        self.config_manager = config_manager
        self.clickhouse = clickhouse
        self.queue_client = queue_client

        self.message_publisher = MessagePublisher(queue_client, 'realtime_data_service')
        
        self.active_connections: Dict[str, Dict] = {}
        self.plugins: Dict[str, Any] = exchange_plugins or {}  # Use plugins from orchestrator
        self.stream_tasks: Dict[str, asyncio.Task] = {}
        self.monitoring_task: Optional[asyncio.Task] = None
        
        self.logger = get_realtime_logger()
    
    async def initialize(self):
        """Initialize real-time data service"""
        self.logger.info("Initializing Real-time Data Service...")

        await self._setup_connections()
        # Exchange plugins are provided by orchestrator, just link them
        self._link_exchange_plugins()
        
        self.logger.info("Real-time Data Service initialized")
    
    async def _setup_connections(self):
        """Setup real-time data connections from config"""
        connections_config = self.config_manager.get_config('connections')
        main_config = self.config_manager.get_config('main')
        
        for connection_name, connection_config in connections_config.get('connections', {}).items():
            if not connection_config.get('enabled', False):
                continue
                
            # Only setup connections that have real-time data enabled
            if not connection_config.get('realtime', {}).get('enabled', False):
                self.logger.info(f"Real-time data disabled for {connection_name}, skipping")
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
                'server_time_offset': 0,
                'last_candle_time': None,
                'candles_received': 0,
                'closed_candles_processed': 0,
                'active_candle_updates': 0,
                'error_count': 0,
                'last_error': None,
                'stream_start_time': None,
                'last_processed_timestamp': 0
            }
            
            self.logger.info(f"Real-time data connection configured: {connection_name}")
    
    def _validate_connection_config(self, config: Dict[str, Any]):
        """Validate connection configuration for real-time data"""
        required_fields = ['exchange', 'symbol', 'source_timeframe', 'enabled', 'realtime']
        
        for field in required_fields:
            if field not in config or config[field] is None:
                raise ConfigurationError(f"Connection config missing required field: {field}")
        
        # Validate real-time configuration
        realtime = config['realtime']
        rt_required = ['enabled']
        for field in rt_required:
            if field not in realtime or realtime[field] is None:
                raise ConfigurationError(f"Real-time config missing required field: {field}")
    
    def _link_exchange_plugins(self):
        """Link exchange plugins provided by orchestrator to connections"""
        for connection_name, connection_info in self.active_connections.items():
            if connection_name in self.plugins:
                plugin = self.plugins[connection_name]
                connection_info['plugin'] = plugin
                connection_info['status'] = 'ready'
                
                # Server time will be obtained via get_server_time() method when needed
                connection_info['server_time_offset'] = 0  # Will be calculated dynamically
                
                self.logger.info(f"Linked exchange plugin for {connection_name}")
            else:
                connection_info['status'] = 'failed'
                connection_info['last_error'] = f"No exchange plugin provided for {connection_name}"
                self.logger.error(f"No exchange plugin available for {connection_name}")
    
    async def start_all_realtime_streams(self):
        """Start real-time data streams for all connections"""
        self.logger.info("Starting real-time data streams for all connections...")
        
        # Start streams for each connection
        for connection_name in self.active_connections.keys():
            task = asyncio.create_task(
                self.start_realtime_stream(connection_name)
            )
            self.stream_tasks[connection_name] = task
        
        # Start monitoring task
        self.monitoring_task = asyncio.create_task(self._monitor_connections())
        
        self.logger.info("All real-time streams started")
    
    async def start_realtime_stream(self, connection_name: str):
        """Start real-time data stream for specific connection"""
        if connection_name not in self.active_connections:
            raise ConfigurationError(f"Unknown connection: {connection_name}")
        
        connection_info = self.active_connections[connection_name]
        config = connection_info['config']
        plugin = connection_info['plugin']
        
        try:
            self.logger.info(f"Starting real-time stream for {connection_name}")
            
            # Mark stream start time
            connection_info['stream_start_time'] = datetime.now(timezone.utc).timestamp()
            
            # Create callback for real-time data
            async def realtime_callback(candle_data):
                await self._process_realtime_candle(connection_name, candle_data)
            
            # Start WebSocket stream
            success = await plugin.start_realtime_stream(
                config['symbol'],
                config['source_timeframe'],
                realtime_callback
            )
            
            if success:
                connection_info['status'] = 'streaming'
                self.logger.info(f"Real-time stream started for {connection_name}")
                
                # Keep the stream alive
                while connection_info['status'] == 'streaming':
                    await asyncio.sleep(1)
                    
            else:
                raise ExchangeError(f"Failed to start real-time stream for {connection_name}")
                
        except asyncio.CancelledError:
            self.logger.info(f"Real-time stream cancelled for {connection_name}")
            connection_info['status'] = 'stopped'
        except Exception as e:
            self.logger.error(f"Real-time stream failed for {connection_name}: {e}")
            connection_info['status'] = 'error'
            connection_info['last_error'] = str(e)
            raise ExchangeError(f"Real-time stream failed: {e}")
    
    async def _process_realtime_candle(self, connection_name: str, candle_data: Dict[str, Any]):
        """Process incoming real-time candle data"""
        try:
            connection_info = self.active_connections[connection_name]
            config = connection_info['config']
            
            # Update statistics
            connection_info['candles_received'] += 1
            connection_info['last_candle_time'] = candle_data['timestamp']
            
            # Get current server time from exchange plugin
            plugin = connection_info['plugin']
            current_server_time = await plugin.get_server_time() if plugin else int(datetime.now(timezone.utc).timestamp())
            timeframe_seconds = self._get_timeframe_seconds(config['source_timeframe'])
            candle_end_time = candle_data['timestamp'] + timeframe_seconds
            
            is_closed = current_server_time >= candle_end_time
            
            # Debug logging for candle closure logic
            if connection_info['candles_received'] % 10 == 0:  # Log every 10th candle
                self.logger.info(f"Candle timing for {connection_name}: "
                               f"timestamp={candle_data['timestamp']} "
                               f"({datetime.fromtimestamp(candle_data['timestamp'], tz=timezone.utc)}), "
                               f"end_time={candle_end_time} "
                               f"({datetime.fromtimestamp(candle_end_time, tz=timezone.utc)}), "
                               f"server_time={current_server_time} "
                               f"({datetime.fromtimestamp(current_server_time, tz=timezone.utc)}), "
                               f"is_closed={is_closed}")
            
            # Store candle in ClickHouse
            await self.clickhouse.store_candle({
                "timestamp": int(candle_data['timestamp']),
                "exchange": config['exchange'],
                "symbol": config['symbol'],
                "timeframe": config['source_timeframe'],
                "open": float(candle_data['open']),
                "high": float(candle_data['high']),
                "low": float(candle_data['low']),
                "close": float(candle_data['close']),
                "volume": float(candle_data.get('volume', 0)),
            })
            
            # Check if we have a new candle (different timestamp than last processed)
            last_candle_timestamp = connection_info.get('last_processed_timestamp', 0)
            
            # If this is a new candle, the previous candle is now closed
            if candle_data['timestamp'] > last_candle_timestamp and last_candle_timestamp > 0:
                # Get the previous closed candle from ClickHouse and publish it
                try:
                    rows = await self.clickhouse.fetch_candles(
                        config['exchange'], config['symbol'], config['source_timeframe'], limit=1
                    )
                    previous_candle = rows[0] if rows else None

                    if previous_candle and previous_candle['timestamp'] == last_candle_timestamp:
                        connection_info['closed_candles_processed'] += 1
                        
                        # Publish the previous closed candle for downstream processing
                        await self.message_publisher.publish_candle_update({
                            'connection_name': connection_name,
                            'exchange': config['exchange'],
                            'symbol': config['symbol'],
                            'timeframe': config['source_timeframe'],
                            'timestamp': previous_candle['timestamp'],
                            'ohlcv': {
                                'open': float(previous_candle['open']),
                                'high': float(previous_candle['high']),
                                'low': float(previous_candle['low']),
                                'close': float(previous_candle['close']),
                                'volume': float(previous_candle['volume'])
                            },
                            'source': 'realtime',
                            'is_closed': True
                        })
                        
                        self.logger.info(f"Published closed candle for {connection_name}: {datetime.fromtimestamp(previous_candle['timestamp'], tz=timezone.utc)}")
                        self.logger.info(f"Closed candle details: exchange={config['exchange']}, symbol={config['symbol']}, timeframe={config['source_timeframe']}, is_closed=True")
                
                except Exception as e:
                    self.logger.error(f"Error publishing previous closed candle: {e}")
            
            # Update last processed timestamp
            connection_info['last_processed_timestamp'] = candle_data['timestamp']
            
            # Only publish closed candles to downstream services
            if is_closed:
                connection_info['closed_candles_processed'] += 1
                self.logger.info(f"Closed candle processed for {connection_name}")
                # Publish closed candle for downstream processing
                await self.message_publisher.publish_candle_update({
                    'connection_name': connection_name,
                    'exchange': config['exchange'],
                    'symbol': config['symbol'],
                    'timeframe': config['source_timeframe'],
                    'timestamp': candle_data['timestamp'],
                    'ohlcv': {
                        'open': float(candle_data['open']),
                        'high': float(candle_data['high']),
                        'low': float(candle_data['low']),
                        'close': float(candle_data['close']),
                        'volume': float(candle_data['volume'])
                    },
                    'source': 'realtime',
                    'is_closed': True
                })
                
                self.logger.info(f"Published closed candle for downstream processing: {connection_name}")
            else:
                connection_info['active_candle_updates'] += 1
                self.logger.debug(f"⏳ Active candle update for {connection_name} (not published to downstream)")
            
            status_msg = "closed" if is_closed else "active"
            self.logger.debug(f"Processed {status_msg} candle for {connection_name}: {datetime.fromtimestamp(candle_data['timestamp'], tz=timezone.utc)}")
            
        except Exception as e:
            self.logger.error(f"Error processing real-time candle for {connection_name}: {e}")
            connection_info['error_count'] += 1
            connection_info['last_error'] = str(e)
    
    async def _monitor_connections(self):
        """Monitor all active real-time connections"""
        self.logger.info("Starting connection monitoring...")
        
        while True:
            try:
                for connection_name, connection_info in self.active_connections.items():
                    await self._check_connection_health(connection_name)
                
                # Check every 30 seconds
                await asyncio.sleep(30)
                
            except asyncio.CancelledError:
                self.logger.info("Connection monitoring cancelled")
                break
            except Exception as e:
                self.logger.error(f"Connection monitoring error: {e}")
                await asyncio.sleep(30)
    
    async def _check_connection_health(self, connection_name: str):
        """Check health of a specific connection"""
        try:
            connection_info = self.active_connections[connection_name]
            plugin = connection_info['plugin']
            
            # Perform health check
            health = await plugin.health_check()
            
            if health['healthy']:
                if connection_info['status'] == 'error':
                    connection_info['status'] = 'streaming'
                    self.logger.info(f"Connection {connection_name} recovered")
            else:
                connection_info['status'] = 'error'
                connection_info['last_error'] = health.get('error', 'Unknown health check failure')
                connection_info['error_count'] += 1
                
                self.logger.warning(f"Connection {connection_name} health check failed: {health.get('error')}")
            
            # Check for stale data
            if connection_info['last_candle_time']:
                time_since_last = int(datetime.now(timezone.utc).timestamp()) - connection_info['last_candle_time']
                timeframe_seconds = self._get_timeframe_seconds(connection_info['config']['source_timeframe'])
                
                if time_since_last > timeframe_seconds * 3:  # 3 intervals without data
                    self.logger.warning(f"Stale data detected for {connection_name}: {time_since_last}s since last candle")
                    
        except Exception as e:
            self.logger.error(f"Health check failed for {connection_name}: {e}")
            connection_info['error_count'] += 1
            connection_info['last_error'] = str(e)
    
    def _get_timeframe_seconds(self, timeframe: str) -> int:
        """Convert timeframe to seconds"""
        timeframe_map = {
            '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
            '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '8h': 28800, '12h': 43200,
            '1d': 86400, '3d': 259200, '1w': 604800, '1M': 2592000
        }
        
        seconds = timeframe_map.get(timeframe)
        if seconds is None:
            raise ConfigurationError(f"Unsupported timeframe: {timeframe}")
        
        return seconds
    
    async def get_stream_status(self) -> Dict[str, Any]:
        """Get status of real-time data streams"""
        status = {}
        
        for connection_name, connection_info in self.active_connections.items():
            config = connection_info['config']
            
            # Calculate uptime
            uptime_seconds = 0
            if connection_info['stream_start_time']:
                uptime_seconds = datetime.now(timezone.utc).timestamp() - connection_info['stream_start_time']
            
            status[connection_name] = {
                'exchange': config['exchange'],
                'symbol': config['symbol'],
                'timeframe': config['source_timeframe'],
                'status': connection_info['status'],
                'server_time_offset': connection_info['server_time_offset'],
                'last_candle_time': connection_info['last_candle_time'],
                'uptime_seconds': uptime_seconds,
                'candles_received': connection_info['candles_received'],
                'closed_candles_processed': connection_info['closed_candles_processed'],
                'active_candle_updates': connection_info['active_candle_updates'],
                'error_count': connection_info['error_count'],
                'last_error': connection_info['last_error']
            }
        
        return status
    
    async def stop_stream(self, connection_name: str):
        """Stop specific real-time stream"""
        if connection_name not in self.active_connections:
            raise ConfigurationError(f"Unknown connection: {connection_name}")
        
        connection_info = self.active_connections[connection_name]
        connection_info['status'] = 'stopping'
        
        # Cancel the stream task
        if connection_name in self.stream_tasks:
            task = self.stream_tasks[connection_name]
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            del self.stream_tasks[connection_name]
        
        # Cleanup plugin stream
        plugin = connection_info['plugin']
        if plugin:
            try:
                config = connection_info['config']
                await plugin.stop_realtime_stream(config['symbol'], config['source_timeframe'])
            except Exception as e:
                self.logger.error(f"Error stopping plugin stream for {connection_name}: {e}")
        
        connection_info['status'] = 'stopped'
        self.logger.info(f"Real-time stream stopped for {connection_name}")
    
    async def cleanup(self):
        """Cleanup real-time data service"""
        self.logger.info("Cleaning up Real-time Data Service...")
        
        # Stop monitoring
        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        
        # Stop all streams
        stop_tasks = []
        for connection_name in list(self.active_connections.keys()):
            task = asyncio.create_task(self.stop_stream(connection_name))
            stop_tasks.append(task)
        
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
        
        # Cleanup exchange plugins
        for connection_name, plugin in self.plugins.items():
            try:
                await plugin.cleanup()
                self.logger.info(f"Cleaned up plugin: {connection_name}")
            except Exception as e:
                self.logger.error(f"Error cleaning up plugin {connection_name}: {e}")
        
        self.plugins.clear()
        self.active_connections.clear()
        self.stream_tasks.clear()
        
        self.logger.info("Real-time Data Service cleanup completed")


