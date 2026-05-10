# ===== plugins/indicators/rsi.py =====
from typing import Dict, List, Any, Optional
from plugins.indicators.base import IndicatorPlugin
from core.exceptions import IndicatorError
import numpy as np
import logging

logger = logging.getLogger(__name__)


class RsiPlugin(IndicatorPlugin):
    """RSI (Relative Strength Index) indicator plugin"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        if not self.validate_parameters():
            raise IndicatorError("Invalid RSI parameters")

    async def calculate(self, data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Calculate RSI value using numpy for performance."""
        period = self.parameters['period']

        if len(data) < period + 1:
            return None

        # Extract close prices as numpy float64 array
        try:
            if 'close_price' in data[0]:
                prices = np.array([float(c['close_price']) for c in data], dtype=np.float64)
            else:
                prices = np.array([float(c['close']) for c in data], dtype=np.float64)
        except (KeyError, ValueError) as e:
            logger.error(f"RSI: failed to extract prices: {e}")
            return None

        if np.any(prices <= 0):
            logger.error("RSI: non-positive prices found")
            return None

        smoothing = self.parameters.get('smoothing', 'sma')
        if smoothing == 'wilder':
            rsi_value = self._wilder_rsi(prices, period)
        else:
            rsi_value = self._sma_rsi(prices, period)

        if rsi_value is None or not (0.0 <= rsi_value <= 100.0):
            logger.error(f"RSI: invalid result {rsi_value}")
            return None

        timestamp = data[-1]['timestamp']
        return {
            'value': str(round(rsi_value, 2)),
            'additional_data': {
                'period': period,
                'smoothing': smoothing,
                'data_points_used': len(data),
                'calculation_timestamp': timestamp,
            }
        }

    def _wilder_rsi(self, prices: np.ndarray, period: int) -> Optional[float]:
        """Wilder's RMA (TradingView-compatible). O(n) single pass."""
        changes = np.diff(prices)
        gains = np.where(changes > 0, changes, 0.0)
        losses = np.where(changes < 0, -changes, 0.0)

        if len(changes) < period:
            return None

        # Seed with SMA of first `period` values
        avg_gain = gains[:period].mean()
        avg_loss = losses[:period].mean()

        # Wilder smoothing: RMA[i] = (RMA[i-1] * (period-1) + value[i]) / period
        alpha = 1.0 / period
        one_minus = (period - 1) / period
        for i in range(period, len(changes)):
            avg_gain = avg_gain * one_minus + gains[i] * alpha
            avg_loss = avg_loss * one_minus + losses[i] * alpha

        if avg_loss == 0.0:
            return 100.0
        if avg_gain == 0.0:
            return 0.0
        rs = avg_gain / avg_loss
        return 100.0 - 100.0 / (1.0 + rs)

    def _sma_rsi(self, prices: np.ndarray, period: int) -> Optional[float]:
        """SMA-based RSI on last `period` changes."""
        changes = np.diff(prices)
        if len(changes) < period:
            return None
        window = changes[-period:]
        gains = np.where(window > 0, window, 0.0)
        losses = np.where(window < 0, -window, 0.0)
        avg_gain = gains.mean()
        avg_loss = losses.mean()
        if avg_loss == 0.0:
            return 100.0
        if avg_gain == 0.0:
            return 0.0
        rs = avg_gain / avg_loss
        return 100.0 - 100.0 / (1.0 + rs)

    def validate_input_data_strict(self, data: List[Dict[str, Any]]) -> bool:
        return True  # validation handled inside calculate()

    def validate_input_data(self, data: List[Dict[str, Any]]) -> bool:
        return True

    def get_required_periods(self) -> int:
        period = self.parameters['period']
        if self.parameters.get('smoothing', 'sma') == 'wilder':
            return period * 2
        return period + 1

    def validate_parameters(self) -> bool:
        if 'period' not in self.parameters:
            return False
        period = self.parameters['period']
        if not isinstance(period, int) or period <= 1:
            return False
        smoothing = self.parameters.get('smoothing', 'sma')
        if smoothing not in ['wilder', 'exponential', 'simple', 'sma']:
            return False
        return True

    def get_output_fields(self) -> List[str]:
        return ['value']
