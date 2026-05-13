"""
Compatibility shim — all real logic lives in core.queue_client.

All service imports (from rabbitmq.rabbitmq_client import ...) continue to work
without any changes.  RabbitMQClient is aliased to InProcessQueueClient so the
type hints in services remain satisfied.  QueueManager is a no-op stub.
"""

from core.queue_client import (  # noqa: F401  (re-exported for existing imports)
    QueueMessage,
    MessagePublisher,
    InProcessQueueClient,
    InProcessQueueClient as RabbitMQClient,
)


class QueueManager:
    """No-op stub — RabbitMQ management is no longer needed."""

    def __init__(self, connection_url: str = None):
        pass

    async def setup_queues(self) -> None:
        pass

    async def purge_all_queues(self) -> None:
        pass

    async def get_queue_stats(self) -> dict:
        return {}
