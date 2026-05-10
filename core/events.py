import asyncio
from typing import Dict, List, Any, Optional, Callable, Set
from datetime import datetime, timezone
from enum import Enum
import logging
from dataclasses import dataclass

class EventType(Enum):
    CANDLE_COMPLETE = "candle_complete"
    AGGREGATION_COMPLETE = "aggregation_complete" 
    INDICATOR_COMPLETE = "indicator_complete"
    STRATEGY_SIGNAL = "strategy_signal"
    GAP_DETECTED = "gap_detected"
    GAP_FILLED = "gap_filled"

@dataclass
class TradingEvent:
    event_type: EventType
    timestamp: int
    connection_name: str
    timeframe: str
    data: Dict[str, Any]
    source_component: str

class EventManager:
    """Unified event system for coordinating trading bot components"""
    
    def __init__(self):
        self.subscribers: Dict[EventType, List[Callable]] = {}
        self.event_queue: asyncio.Queue = asyncio.Queue()
        self.processing_task: Optional[asyncio.Task] = None
        self.dependency_map: Dict[str, Set[str]] = {}
        self.logger = logging.getLogger(__name__)
        self._running = False
    
    def subscribe(self, event_type: EventType, callback: Callable):
        """Subscribe to events"""
        if event_type not in self.subscribers:
            self.subscribers[event_type] = []
        self.subscribers[event_type].append(callback)
        self.logger.debug(f"Subscribed {callback.__name__} to {event_type.value}")
    
    async def publish(self, event: TradingEvent):
        """Publish event to queue"""
        await self.event_queue.put(event)
        self.logger.debug(f"Published {event.event_type.value} for {event.connection_name}:{event.timeframe}")
    
    async def start_processing(self):
        """Start event processing loop"""
        self._running = True
        self.processing_task = asyncio.create_task(self._process_events())
        self.logger.info("Event manager started")
    
    async def stop_processing(self):
        """Stop event processing"""
        self._running = False
        if self.processing_task:
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass
        self.logger.info("Event manager stopped")
    
    async def _process_events(self):
        """Process events from queue"""
        while self._running:
            try:
                event = await asyncio.wait_for(self.event_queue.get(), timeout=1.0)
                await self._handle_event(event)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Event processing error: {e}")
    
    async def _handle_event(self, event: TradingEvent):
        """Handle single event by notifying subscribers"""
        if event.event_type in self.subscribers:
            tasks = []
            for callback in self.subscribers[event.event_type]:
                task = asyncio.create_task(callback(event))
                tasks.append(task)
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
    
    def add_dependency(self, parent: str, child: str):
        """Add dependency relationship"""
        if parent not in self.dependency_map:
            self.dependency_map[parent] = set()
        self.dependency_map[parent].add(child)
    
    def get_dependents(self, component: str) -> Set[str]:
        """Get all components that depend on this one"""
        return self.dependency_map.get(component, set())