#!/usr/bin/env python3
"""
Base Historical Plugin
=====================

Abstract base class for historical data plugins.
Defines the interface for fetching historical candle data from exchanges.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List


class HistoricalPlugin(ABC):
    """
    Abstract base class for historical data plugins
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = "base_historical"
    
    @abstractmethod
    async def initialize(self):
        """Initialize the historical plugin"""
        pass
    
    @abstractmethod
    async def cleanup(self):
        """Cleanup plugin resources"""
        pass
    
    @abstractmethod
    async def get_historical_data(self, symbol: str, timeframe: str, 
                                start_timestamp: int, end_timestamp: int) -> List[Dict[str, Any]]:
        """
        Fetch historical candle data
        
        Args:
            symbol: Trading pair symbol
            timeframe: Candle timeframe
            start_timestamp: Start time as Unix timestamp
            end_timestamp: End time as Unix timestamp
            
        Returns:
            List of candle dictionaries with keys: timestamp, open, high, low, close, volume
        """
        pass
    
    @abstractmethod
    async def get_server_time(self) -> int:
        """Get current server timestamp"""
        pass