from typing import Dict, List, Any, Optional
from decimal import Decimal
from plugins.indicators.base import IndicatorPlugin
from core.exceptions import IndicatorError

class SmaPlugin(IndicatorPlugin):
    """Simple Moving Average indicator plugin"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        if not self.validate_parameters():
            raise IndicatorError("Invalid SMA parameters")
    
    async def calculate(self, data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Calculate SMA value"""
        from datetime import datetime, timezone
        import logging
        
        period = self.parameters['period']
        logger = logging.getLogger(__name__)
        
        if len(data) < period:
            return None
        
        # Get closing prices for calculation
        prices = [Decimal(str(candle['close'])) for candle in data[-period:]]
        
        # Calculate Simple Moving Average
        sma_value = sum(prices) / len(prices)
        
        calculation_timestamp = data[-1]['timestamp']
        calc_time_str = datetime.fromtimestamp(calculation_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        logger.debug(f"SMA calculated: {float(sma_value):.4f} at {calc_time_str}")
        
        return {
            'value': str(sma_value),
            'additional_data': {
                'period': period,
                'data_points_used': len(data),
                'last_close_price': str(prices[-1]),
                'calculation_timestamp': calculation_timestamp,
                'calculation_datetime': calc_time_str
            }
        }
    
    def get_required_periods(self) -> int:
        """Get minimum periods required"""
        return self.parameters['period']
    
    def validate_parameters(self) -> bool:
        """Validate SMA parameters"""
        if 'period' not in self.parameters:
            return False
        
        period = self.parameters['period']
        if not isinstance(period, int) or period <= 1:
            return False
        
        return True
    
    def get_output_fields(self) -> List[str]:
        """Get output field names"""
        return ['value']