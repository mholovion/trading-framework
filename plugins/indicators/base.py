from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from decimal import Decimal

class IndicatorPlugin(ABC):
    """Base class for indicator plugins"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = self.__class__.__name__.lower().replace('plugin', '').replace('indicator', '')
        self.parameters = config.get('parameters', {})
    
    @abstractmethod
    async def calculate(self, data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Calculate indicator value"""
        pass
    
    @abstractmethod
    def get_required_periods(self) -> int:
        """Get minimum number of periods required for calculation"""
        pass
    
    @abstractmethod
    def validate_parameters(self) -> bool:
        """Validate indicator parameters"""
        pass
    
    @abstractmethod
    def get_output_fields(self) -> List[str]:
        """Get list of output field names"""
        pass
    
    def get_required_timeframes(self) -> List[str]:
        """Get list of required timeframes for this indicator"""
        # Default: use the indicator's own timeframe
        return [self.config['source_timeframe']]
    
    def get_lookback_periods(self) -> int:
        """Get number of periods to look back for calculations"""
        # Default: same as required periods
        return self.get_required_periods()