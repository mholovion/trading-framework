from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncio

class ExchangePlugin(ABC):
    """Base class for exchange plugins"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = self.__class__.__name__.lower().replace('plugin', '')
        self._server_time_offset = 0  # Offset between server and exchange time
    
    @abstractmethod
    async def initialize(self) -> bool:
        """Initialize exchange connection"""
        pass
    
    @abstractmethod
    async def get_server_time(self) -> int:
        """Get server time from exchange (Unix timestamp in seconds)"""
        pass
    
    @abstractmethod
    async def get_historical_data(self, symbol: str, timeframe: str, start_time: int, end_time: int, limit: int) -> List[Dict]:
        """Get historical candlestick data"""
        pass
    
    @abstractmethod
    async def start_realtime_stream(self, symbol: str, timeframe: str, callback) -> bool:
        """Start real-time data stream"""
        pass
    
    @abstractmethod
    async def stop_realtime_stream(self, symbol: str, timeframe: str) -> bool:
        """Stop real-time data stream"""
        pass
    
    @abstractmethod
    async def health_check(self) -> Dict[str, Any]:
        """Check exchange connection health"""
        pass
    
    @abstractmethod
    async def get_supported_timeframes(self) -> List[str]:
        """Get list of supported timeframes"""
        pass
    
    @abstractmethod
    async def normalize_symbol(self, symbol: str) -> str:
        """Normalize symbol format for this exchange"""
        pass
    
    @abstractmethod
    async def cleanup(self):
        """Cleanup resources"""
        pass
    
    def convert_timeframe_to_seconds(self, timeframe: str) -> int:
        """Convert timeframe to seconds - used by all exchanges"""
        timeframe_map = {
            '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
            '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '8h': 28800, '12h': 43200,
            '1d': 86400, '3d': 259200, '1w': 604800, '1M': 2592000
        }
        return timeframe_map.get(timeframe, 60)
    
    async def sync_server_time(self):
        """Synchronize with exchange server time"""
        try:
            exchange_time = await self.get_server_time()
            local_time = int(datetime.utcnow().timestamp())
            self._server_time_offset = exchange_time - local_time
            return True
        except Exception:
            self._server_time_offset = 0
            return False
    
    def get_synchronized_time(self) -> int:
        """Get current time synchronized with exchange server"""
        local_time = int(datetime.utcnow().timestamp())
        return local_time + self._server_time_offset