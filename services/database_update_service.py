#!/usr/bin/env python3
"""
Database Update Service
=======================

Centralized microservice responsible for processing all database updates from the message queue.
Receives candle updates from other services and writes them to ClickHouse.
"""

import asyncio
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from core.universal_config_manager import UniversalConfigManager
from core.clickhouse import ClickHouseManager
from core.logging_config import get_database_logger
from core.queue_client import InProcessQueueClient, QueueMessage


class DatabaseUpdateService:
    """
    Centralized database update service that processes candle writes from message queues
    into ClickHouse.
    """

    def __init__(self, config_manager: UniversalConfigManager,
                 clickhouse: ClickHouseManager,
                 queue_client: InProcessQueueClient):
        self.config_manager = config_manager
        self.clickhouse = clickhouse
        self.queue_client = queue_client

        self.logger = get_database_logger()

        self.stats = {
            'candles_processed': 0,
            'errors': 0,
            'start_time': datetime.now(timezone.utc),
        }

        self.running = False
        self._flush_task: Optional[asyncio.Task] = None

    async def initialize(self):
        """Initialize the database update service and set up queue consumers."""
        self.logger.info("Initializing Database Update Service...")
        await self._setup_queue_consumers()
        self.logger.info("Database Update Service initialized")

    async def _setup_queue_consumers(self):
        """Register message queue consumers."""
        await self.queue_client.consume_messages('candle_updates', self._process_candle_update)
        await self.queue_client.consume_messages('strategy_updates', self._process_strategy_update)
        self.logger.info("Queue consumers setup completed")

    # ------------------------------------------------------------------
    # Queue handlers
    # ------------------------------------------------------------------

    async def _process_candle_update(self, message: QueueMessage):
        """Route incoming candle queue messages to the appropriate handler."""
        if message.type == 'candles_bulk_update':
            await self._process_bulk_candles(message)
        elif message.type == 'candle_update':
            await self._process_single_candle(message)
        else:
            self.logger.warning(f"Unexpected message type in candle queue: {message.type}")

    async def _process_single_candle(self, message: QueueMessage):
        """Store a single candle update into ClickHouse."""
        try:
            data = message.data
            ts = data['timestamp']
            if isinstance(ts, datetime):
                ts = int(ts.timestamp())
            else:
                ts = int(ts)

            await self.clickhouse.store_candle({
                'timestamp': ts,
                'exchange': data['exchange'],
                'symbol': data['symbol'],
                'timeframe': data['timeframe'],
                'open': float(data['ohlcv']['open']),
                'high': float(data['ohlcv']['high']),
                'low': float(data['ohlcv']['low']),
                'close': float(data['ohlcv']['close']),
                'volume': float(data['ohlcv']['volume']),
            })

            self.stats['candles_processed'] += 1
            self.logger.debug(
                f"Stored candle: {data['exchange']} {data['symbol']} {data['timeframe']} @ {ts}"
            )

        except Exception as e:
            self.logger.error(f"Error processing candle update: {e}")
            self.stats['errors'] += 1

    async def _process_bulk_candles(self, message: QueueMessage):
        """Store a bulk candles message into ClickHouse."""
        try:
            data = message.data
            candles_list = data.get('candles', [])

            if not candles_list:
                self.logger.warning("Empty bulk candles message received")
                return

            batch_info = data.get('batch_info', {})
            exchange = batch_info.get('exchange', 'unknown')
            symbol = batch_info.get('symbol', 'unknown')
            timeframe = batch_info.get('timeframe', 'unknown')
            source = batch_info.get('source', 'unknown')

            self.logger.info(
                f"Processing bulk candles: {len(candles_list)} candles "
                f"({exchange}/{symbol}/{timeframe}) source={source}"
            )

            for candle_data in candles_list:
                ts = candle_data['timestamp']
                if isinstance(ts, datetime):
                    ts = int(ts.timestamp())
                else:
                    ts = int(ts)

                await self.clickhouse.store_candle({
                    'timestamp': ts,
                    'exchange': candle_data['exchange'],
                    'symbol': candle_data['symbol'],
                    'timeframe': candle_data['timeframe'],
                    'open': float(candle_data['ohlcv']['open']),
                    'high': float(candle_data['ohlcv']['high']),
                    'low': float(candle_data['ohlcv']['low']),
                    'close': float(candle_data['ohlcv']['close']),
                    'volume': float(candle_data['ohlcv']['volume']),
                })

            self.stats['candles_processed'] += len(candles_list)
            self.logger.info(f"Bulk stored {len(candles_list)} candles")

        except Exception as e:
            self.logger.error(f"Error processing bulk candles: {e}")
            self.stats['errors'] += 1

    async def _process_strategy_update(self, message: QueueMessage):
        """Strategy signals are handled by the resolver — log and skip."""
        self.logger.debug(
            f"Strategy update received (skipped — handled by resolver): "
            f"type={message.type} source={message.source_service}"
        )

    # ------------------------------------------------------------------
    # Flush loop
    # ------------------------------------------------------------------

    async def _flush_loop(self):
        """Periodically flush ClickHouse write buffer every 5 seconds."""
        while self.running:
            try:
                await asyncio.sleep(5)
                if self.running:
                    await self.clickhouse.flush()
                    self.logger.debug("ClickHouse buffer flushed")
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error during ClickHouse flush: {e}")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        """Start the periodic flush loop and block until stopped."""
        self.running = True
        self._flush_task = asyncio.create_task(self._flush_loop())
        self.logger.info("Database Update Service flush loop started")
        while self.running:
            await asyncio.sleep(1)

    async def stop(self):
        """Stop the flush loop and do a final flush."""
        self.logger.info("Stopping Database Update Service...")
        self.running = False

        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        # Final flush to drain any remaining buffered candles
        try:
            await self.clickhouse.flush()
        except Exception as e:
            self.logger.error(f"Error during final ClickHouse flush: {e}")

        self.logger.info("Database Update Service stopped")

    async def get_statistics(self) -> Dict[str, Any]:
        """Return current service statistics."""
        uptime = datetime.now(timezone.utc) - self.stats['start_time']
        return {
            'uptime_seconds': uptime.total_seconds(),
            'candles_processed': self.stats['candles_processed'],
            'errors': self.stats['errors'],
        }

    async def cleanup(self):
        """Cleanup resources."""
        await self.stop()
        self.logger.info("Database Update Service cleanup completed")
