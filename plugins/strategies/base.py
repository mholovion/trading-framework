from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from decimal import Decimal
from enum import Enum
from core.timeframe_utils import TimeframeUtils

class SignalType(Enum):
    """Trading signal types"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"

class StrategySignal:
    """Trading strategy signal"""
    
    def __init__(self, signal_type: SignalType, confidence: float, 
                 price: float, indicators_data: Dict[str, Any], metadata: Optional[Dict] = None,
                 candle_open_timestamp: Optional[int] = None, primary_timeframe: Optional[str] = None):
        self.signal_type = signal_type
        self.confidence = confidence  # 0.0 to 1.0
        self.price = price
        self.indicators_data = indicators_data
        self.metadata = metadata or {}
        
        # Універсальна логіка для розрахунку timestamp закриття свічки
        if candle_open_timestamp is not None:
            # Автоматично визначаємо primary timeframe якщо не вказано
            if primary_timeframe is None:
                primary_timeframe = self._extract_primary_timeframe(indicators_data)
            
            # Розраховуємо timestamp закриття свічки використовуючи TimeframeUtils
            if primary_timeframe:
                try:
                    period_seconds = TimeframeUtils.get_timeframe_seconds(primary_timeframe)
                    self.timestamp = candle_open_timestamp + period_seconds
                except Exception:
                    # Fallback для невідомих timeframe
                    self.timestamp = candle_open_timestamp + 14400  # 4h за замовчуванням
            else:
                self.timestamp = candle_open_timestamp + 14400  # 4h за замовчуванням
        else:
            self.timestamp = None  # Will be set by manager for backwards compatibility
    
    def _extract_primary_timeframe(self, indicators_data: Dict[str, Any]) -> Optional[str]:
        """Витягує primary timeframe з indicators_data
        
        Логіка: шукаємо найбільший timeframe серед індикаторів,
        оскільки стратегії зазвичай базуються на більших timeframe
        """
        timeframes = set()
        
        for indicator_name, indicator_data in indicators_data.items():
            if isinstance(indicator_data, dict) and 'timeframe' in indicator_data:
                timeframe = indicator_data['timeframe']
                if TimeframeUtils.validate_timeframe(timeframe):
                    timeframes.add(timeframe)
        
        if not timeframes:
            return None
        
        # Повертаємо найбільший timeframe (оскільки стратегії зазвичай сигналять на primary timeframe)
        timeframes_by_seconds = {tf: TimeframeUtils.get_timeframe_seconds(tf) for tf in timeframes}
        return max(timeframes_by_seconds, key=timeframes_by_seconds.get)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert signal to dictionary"""
        return {
            'signal_type': self.signal_type.value,
            'confidence': self.confidence,
            'price': self.price,
            'indicators_data': self.indicators_data,
            'metadata': self.metadata,
            'timestamp': self.timestamp
        }

class StrategyPlugin(ABC):
    """Base class for strategy plugins"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = self.__class__.__name__.lower().replace('plugin', '').replace('strategy', '')
        self.parameters = config.get('parameters', {})
        self.required_indicators = config.get('required_indicators', [])
    
    @abstractmethod
    async def process(self, indicators_data: Dict[str, Any], 
                     current_price: float) -> Optional[StrategySignal]:
        """Process indicators data and generate trading signal"""
        pass
    
    @abstractmethod
    def validate_parameters(self) -> bool:
        """Validate strategy parameters"""
        pass
    
    @abstractmethod
    def get_required_indicators(self) -> List[str]:
        """Get list of required indicator names"""
        pass
    
    def validate_indicators_data(self, indicators_data: Dict[str, Any]) -> bool:
        """Validate that required indicators are present"""
        for indicator_name in self.required_indicators:
            if indicator_name not in indicators_data:
                return False
            if indicators_data[indicator_name] is None:
                return False
        return True