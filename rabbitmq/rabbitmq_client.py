#!/usr/bin/env python3
"""
RabbitMQ Message Queue Client
=============================

This implements the RabbitMQ message queue system for microservices communication
as outlined in ARCHITECTURE_MODERNIZATION_PLAN.md Phase 2.

Queue structure:
- candle_updates: New candle data
- indicator_updates: Indicator recalculation requests  
- strategy_updates: Strategy recalculation requests
- trading_signals: Generated trading signals
"""

import asyncio
import aio_pika
import json
import logging
from typing import Dict, Any, Callable, Optional
from dataclasses import dataclass
from datetime import datetime
import traceback
import time

@dataclass
class QueueMessage:
    """Standard message format for all queues"""
    type: str
    data: Dict[str, Any]
    timestamp: float
    message_id: str
    source_service: str

class RabbitMQClient:
    """
    RabbitMQ client for microservices communication
    """
    
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.connection: Optional[aio_pika.Connection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.logger = logging.getLogger('RabbitMQClient')
        self.max_retries = 5
        self.retry_delay = 5.0
        self.is_connected = False
        
        # Queue configurations
        self.queues = {
            'candle_updates': {'durable': True, 'routing_key': 'candles.new'},
            'indicator_updates': {'durable': True, 'routing_key': 'indicators.recalc'},
            'strategy_updates': {'durable': True, 'routing_key': 'strategies.recalc'},
            'trading_signals': {'durable': True, 'routing_key': 'signals.new'},
            'data_ingest': {'durable': True, 'routing_key': 'data.ingest'},
            'gap_recovery': {'durable': True, 'routing_key': 'data.recovery'},
            'gap_detection_requests': {'durable': True, 'routing_key': 'gap.detection'},
            'gap_recovery_requests': {'durable': True, 'routing_key': 'gap.range_recovery'},
            'aggregation_requests': {'durable': True, 'routing_key': 'aggregation.request'},
            'historical_data_requests': {'durable': True, 'routing_key': 'historical.request'},
            'database_updates': {'durable': True, 'routing_key': 'database.operations'},
            'indicator_recalc_updates': {'durable': True, 'routing_key': 'indicators.recalc_request'},
            'indicator_calculation_requests': {'durable': True, 'routing_key': 'indicators.calculation_request'},
            'strategy_calculation_requests': {'durable': True, 'routing_key': 'strategies.calculation_request'}
        }
        
        # Exchange configuration
        self.exchange_name = 'trading_bot_exchange'
        
    async def connect(self):
        """Connect to RabbitMQ with retry logic"""
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"Connecting to RabbitMQ... (attempt {attempt}/{self.max_retries})")
                
                self.connection = await aio_pika.connect_robust(
                    self.connection_url,
                    connection_name=f"trading_bot_conn_{int(time.time())}",
                    reconnect_interval=5.0,
                    fail_fast=False,
                    loop=asyncio.get_event_loop()
                )
                
                # Set up connection closed callback for reconnection handling  
                if hasattr(self.connection, 'add_close_callback'):
                    self.connection.add_close_callback(self._on_connection_closed)
                elif hasattr(self.connection, 'close_callbacks'):
                    self.connection.close_callbacks.add(self._on_connection_closed)
                
                # Start background error handler
                error_task = asyncio.create_task(self._handle_connection_errors())
                error_task.add_done_callback(lambda t: t.exception() if not t.cancelled() else None)
                
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=1)
                
                # Declare exchange
                self.exchange = await self.channel.declare_exchange(
                    self.exchange_name,
                    aio_pika.ExchangeType.TOPIC,
                    durable=True
                )
                
                # Declare all queues
                await self._declare_queues()
                
                self.is_connected = True
                self.logger.info("Connected to RabbitMQ successfully")
                return
                
            except Exception as e:
                self.logger.error(f"Connection attempt {attempt} failed: {e}")
                
                if attempt == self.max_retries:
                    self.logger.error("Max connection attempts reached. Giving up.")
                    raise
                    
                self.logger.info(f"⏳ Retrying in {self.retry_delay} seconds...")
                await asyncio.sleep(self.retry_delay)
                
    def _on_connection_closed(self, connection, reason):
        """Handle connection closure"""
        self.logger.warning(f"Connection closed: {reason}")
        self.is_connected = False
        
    async def _handle_connection_errors(self):
        """Handle connection errors in background task"""
        try:
            if self.connection:
                # Monitor connection state
                while not self.connection.is_closed:
                    await asyncio.sleep(1)
                self.logger.info("Connection monitoring ended")
        except Exception as e:
            self.logger.error(f"Connection error handler: {e}")
        finally:
            self.is_connected = False
            
    async def _declare_queues(self):
        """Declare all queues"""
        for queue_name, config in self.queues.items():
            queue = await self.channel.declare_queue(
                queue_name,
                durable=config['durable']
            )
            
            # Bind queue to exchange
            await queue.bind(
                self.exchange,
                routing_key=config['routing_key']
            )
            
            self.logger.debug(f"Declared queue: {queue_name}")
            
    async def disconnect(self):
        """Disconnect from RabbitMQ"""
        try:
            self.is_connected = False
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
                self.logger.info("Disconnected from RabbitMQ")
        except Exception as e:
            self.logger.error(f"Error disconnecting from RabbitMQ: {e}")
            
    async def ensure_connected(self):
        """Ensure connection is active, reconnect if needed"""
        if not self.is_connected or not self.connection or self.connection.is_closed:
            self.logger.info("Connection lost, attempting to reconnect...")
            await self.connect()
            
    async def publish_message(self, queue_name: str, message: QueueMessage):
        """Publish message to queue with automatic reconnection"""
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                await self.ensure_connected()
                
                if not self.channel:
                    raise RuntimeError("Channel not available")
                    
                routing_key = self.queues[queue_name]['routing_key']
                
                # Serialize message
                message_body = json.dumps({
                    'type': message.type,
                    'data': message.data,
                    'timestamp': message.timestamp,
                    'message_id': message.message_id,
                    'source_service': message.source_service
                })
                
                # Publish message
                await self.exchange.publish(
                    aio_pika.Message(
                        message_body.encode(),
                        message_id=message.message_id,
                        timestamp=datetime.fromtimestamp(message.timestamp),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    ),
                    routing_key=routing_key
                )
                
                self.logger.debug(f"Published message to {queue_name}: {message.type}")
                return
                
            except Exception as e:
                self.logger.error(f"Publish attempt {attempt} failed for {queue_name}: {e}")
                
                if attempt == max_attempts:
                    self.logger.error(f"Failed to publish after {max_attempts} attempts")
                    raise
                    
                # Reset connection state to force reconnection
                self.is_connected = False
                await asyncio.sleep(1.0)
            
    async def consume_messages(self, queue_name: str, callback: Callable):
        """Consume messages from queue with automatic reconnection"""
        async def _consume():
            while True:
                try:
                    await self.ensure_connected()
                    
                    if not self.channel:
                        raise RuntimeError("Channel not available")
                        
                    queue = await self.channel.declare_queue(
                        queue_name,
                        durable=self.queues[queue_name]['durable']
                    )
                    
                    async def message_handler(message: aio_pika.IncomingMessage):
                        try:
                            async with message.process():
                                try:
                                    # Deserialize message
                                    body = json.loads(message.body.decode())
                                    
                                    queue_message = QueueMessage(
                                        type=body['type'],
                                        data=body['data'],
                                        timestamp=body['timestamp'],
                                        message_id=body['message_id'],
                                        source_service=body['source_service']
                                    )
                                    
                                    # Call handler
                                    await callback(queue_message)
                                    
                                    self.logger.debug(f"Processed message from {queue_name}: {queue_message.type}")
                                    
                                except Exception as e:
                                    self.logger.error(f"Error processing message from {queue_name}: {e}")
                                    self.logger.error(traceback.format_exc())
                                    
                        except Exception as e:
                            self.logger.error(f"Message handler error: {e}")
                            
                    # Start consuming
                    consumer_tag = await queue.consume(message_handler)
                    self.logger.info(f"Started consuming from {queue_name}")
                    
                    # Keep consuming until connection is lost
                    try:
                        while self.is_connected and not self.connection.is_closed:
                            await asyncio.sleep(1)
                    except asyncio.CancelledError:
                        self.logger.info(f"Consumer for {queue_name} cancelled")
                        break
                        
                    self.logger.warning(f"Consumer for {queue_name} disconnected, will retry...")
                    
                except asyncio.CancelledError:
                    self.logger.info(f"Consumer for {queue_name} cancelled")
                    break
                except Exception as e:
                    self.logger.error(f"Consumer error for {queue_name}: {e}")
                    self.is_connected = False
                    
                    self.logger.info(f"⏳ Retrying consumer for {queue_name} in {self.retry_delay} seconds...")
                    await asyncio.sleep(self.retry_delay)
                    
        asyncio.create_task(_consume())

# Convenience message publishers
class MessagePublisher:
    """High-level message publisher with predefined message types"""
    
    def __init__(self, rabbitmq_client: RabbitMQClient, service_name: str):
        self.client = rabbitmq_client
        self.service_name = service_name
        self.logger = logging.getLogger(f'MessagePublisher.{service_name}')
        
    def _create_message(self, message_type: str, data: Dict[str, Any]) -> QueueMessage:
        """Create standard message"""
        import time
        import uuid
        
        return QueueMessage(
            type=message_type,
            data=data,
            timestamp=time.time(),
            message_id=str(uuid.uuid4()),
            source_service=self.service_name
        )
        
    async def publish_candle_update(self, candle_data: Dict[str, Any]):
        """Publish new candle data"""
        message = self._create_message('candle_update', candle_data)
        await self.client.publish_message('candle_updates', message)
        
    async def publish_indicator_recalc_request(self, indicator_data: Dict[str, Any]):
        """Publish indicator recalculation request"""
        message = self._create_message('indicator_recalc', indicator_data)
        await self.client.publish_message('indicator_recalc_updates', message)
        
    async def publish_strategy_recalc_request(self, strategy_data: Dict[str, Any]):
        """Publish strategy recalculation request"""
        message = self._create_message('strategy_recalc', strategy_data)
        await self.client.publish_message('strategy_calculation_requests', message)
        
    async def publish_trading_signal(self, signal_data: Dict[str, Any]):
        """Publish trading signal"""
        message = self._create_message('trading_signal', signal_data)
        await self.client.publish_message('trading_signals', message)
        
    async def publish_data_ingest_request(self, ingest_data: Dict[str, Any]):
        """Publish data ingest request"""
        message = self._create_message('data_ingest', ingest_data)
        await self.client.publish_message('data_ingest', message)
        
    async def publish_gap_recovery_request(self, recovery_data: Dict[str, Any]):
        """Publish gap recovery request"""
        message = self._create_message('gap_recovery', recovery_data)
        await self.client.publish_message('gap_recovery', message)
        
    async def publish_custom_message(self, message_data: Dict[str, Any]):
        """Publish custom message to appropriate queue based on type"""
        message_type = message_data.get('type', 'unknown')
        
        # Route to appropriate queue based on message type
        if message_type == 'historical_data_request':
            message = self._create_message(message_type, message_data)
            await self.client.publish_message('historical_data_requests', message)
        elif message_type == 'aggregation_request':
            message = self._create_message(message_type, message_data)
            await self.client.publish_message('aggregation_requests', message)
        elif message_type == 'gap_detection':
            message = self._create_message(message_type, message_data)
            await self.client.publish_message('gap_detection_requests', message)
        else:
            raise ValueError(f"Unknown message type: {message_type}")

# Queue management utility
class QueueManager:
    """Utility for managing RabbitMQ queues"""
    
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.logger = logging.getLogger('QueueManager')
        
    async def setup_queues(self):
        """Setup all required queues"""
        self.logger.info("Setting up RabbitMQ queues...")
        
        client = RabbitMQClient(self.connection_url)
        
        try:
            await client.connect()
            self.logger.info("RabbitMQ queues setup completed")
            
        except Exception as e:
            self.logger.error(f"Failed to setup queues: {e}")
            raise
        finally:
            await client.disconnect()
            
    async def purge_all_queues(self):
        """Purge all queues (for testing/reset)"""
        self.logger.warning("Purging all RabbitMQ queues...")
        
        client = RabbitMQClient(self.connection_url)
        
        try:
            await client.connect()
            
            for queue_name in client.queues:
                queue = await client.channel.declare_queue(queue_name)
                await queue.purge()
                self.logger.info(f"Purged queue: {queue_name}")
                
            self.logger.info("All queues purged")
            
        except Exception as e:
            self.logger.error(f"Failed to purge queues: {e}")
            raise
        finally:
            await client.disconnect()
            
    async def get_queue_stats(self):
        """Get statistics for all queues"""
        self.logger.info("Getting queue statistics...")
        
        client = RabbitMQClient(self.connection_url)
        
        try:
            await client.connect()
            
            stats = {}
            for queue_name in client.queues:
                queue = await client.channel.declare_queue(queue_name, passive=True)
                stats[queue_name] = {
                    'message_count': queue.declaration_result.message_count,
                    'consumer_count': queue.declaration_result.consumer_count
                }
                
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get queue stats: {e}")
            raise
        finally:
            await client.disconnect()

# Testing and utility functions
async def test_rabbitmq_connection():
    """Test RabbitMQ connection"""
    logger = logging.getLogger('RabbitMQTest')
    logger.info("Testing RabbitMQ connection...")
    
    try:
        client = RabbitMQClient()
        await client.connect()
        
        # Test message publishing
        publisher = MessagePublisher(client, 'test_service')
        await publisher.publish_candle_update({
            'exchange': 'test',
            'symbol': 'TEST_USDT',
            'timestamp': 1234567890,
            'price': 100.0
        })
        
        logger.info("RabbitMQ test passed")
        
        await client.disconnect()
        
    except Exception as e:
        logger.error(f"RabbitMQ test failed: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_rabbitmq_connection())