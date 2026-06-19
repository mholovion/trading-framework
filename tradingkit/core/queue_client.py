#!/usr/bin/env python3
"""
In-process pub/sub queue client — drop-in replacement for RabbitMQClient.

Fan-out semantics: each topic has a list of per-subscriber asyncio.Queue
instances so multiple services can consume the same topic independently
(e.g. candle_updates → DatabaseUpdateService AND IndicatorsReactiveService).
"""

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class QueueMessage:
    type: str
    data: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source_service: str = ""


class InProcessQueueClient:
    """Asyncio-based in-process pub/sub replacement for RabbitMQClient.

    Accepts an optional connection_url for API compatibility with code that
    passes a RabbitMQ URL — the argument is silently ignored.
    """

    def __init__(self, connection_url: Optional[str] = None):
        # connection_url ignored — kept for drop-in compatibility
        self._subscribers: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        self._tasks: List[asyncio.Task] = []
        self.logger = logging.getLogger("InProcessQueueClient")

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def ensure_connected(self) -> bool:
        return True

    async def publish_message(self, queue_name: str, message: QueueMessage) -> None:
        subscribers = self._subscribers[queue_name]
        for q in subscribers:
            await q.put(message)

    async def consume_messages(self, queue_name: str, callback: Callable) -> None:
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers[queue_name].append(q)
        task = asyncio.create_task(self._consumer_loop(q, callback, queue_name))
        self._tasks.append(task)

    async def _consumer_loop(
        self, q: asyncio.Queue, callback: Callable, queue_name: str
    ) -> None:
        while True:
            message = await q.get()
            try:
                await callback(message)
            except Exception as exc:
                self.logger.error(
                    f"Error in consumer for queue '{queue_name}': {exc}", exc_info=True
                )
            finally:
                q.task_done()


class MessagePublisher:
    """High-level message publisher — identical API to the RabbitMQ version."""

    def __init__(self, queue_client: InProcessQueueClient, service_name: str):
        self.client = queue_client
        self.service_name = service_name
        self.logger = logging.getLogger(f"MessagePublisher.{service_name}")

    def _create_message(self, message_type: str, data: Dict[str, Any]) -> QueueMessage:
        return QueueMessage(
            type=message_type,
            data=data,
            timestamp=time.time(),
            message_id=str(uuid.uuid4()),
            source_service=self.service_name,
        )

    async def publish_candle_update(self, candle_data: Dict[str, Any]) -> None:
        message = self._create_message("candle_update", candle_data)
        await self.client.publish_message("candle_updates", message)

    async def publish_indicator_recalc_request(self, indicator_data: Dict[str, Any]) -> None:
        message = self._create_message("indicator_recalc", indicator_data)
        await self.client.publish_message("indicator_recalc_updates", message)

    async def publish_strategy_recalc_request(self, strategy_data: Dict[str, Any]) -> None:
        message = self._create_message("strategy_recalc", strategy_data)
        await self.client.publish_message("strategy_calculation_requests", message)

    async def publish_trading_signal(self, signal_data: Dict[str, Any]) -> None:
        message = self._create_message("trading_signal", signal_data)
        await self.client.publish_message("trading_signals", message)

    async def publish_data_ingest_request(self, ingest_data: Dict[str, Any]) -> None:
        message = self._create_message("data_ingest", ingest_data)
        await self.client.publish_message("data_ingest", message)

    async def publish_gap_recovery_request(self, recovery_data: Dict[str, Any]) -> None:
        message = self._create_message("gap_recovery", recovery_data)
        await self.client.publish_message("gap_recovery", message)

    async def publish_custom_message(self, message_data: Dict[str, Any]) -> None:
        message_type = message_data.get("type", "unknown")
        queue_map = {
            "historical_data_request": "historical_data_requests",
            "aggregation_request": "aggregation_requests",
            "gap_detection": "gap_detection_requests",
        }
        if message_type not in queue_map:
            raise ValueError(f"Unknown message type: {message_type}")
        message = self._create_message(message_type, message_data)
        await self.client.publish_message(queue_map[message_type], message)
