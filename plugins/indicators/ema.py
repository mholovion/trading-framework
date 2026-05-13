from typing import Dict, List, Any, Optional
import numpy as np
import logging
from plugins.indicators.base import IndicatorPlugin
from core.exceptions import IndicatorError

logger = logging.getLogger(__name__)


class EmaPlugin(IndicatorPlugin):
    """Exponential Moving Average indicator plugin"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        if not self.validate_parameters():
            raise IndicatorError("Invalid EMA parameters")

    async def calculate(self, data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        period = self.parameters['period']

        if len(data) < period:
            return None

        try:
            key = 'close_price' if 'close_price' in data[0] else 'close'
            prices = np.array([float(c[key]) for c in data], dtype=np.float64)
        except (KeyError, ValueError) as e:
            logger.error(f"EMA: failed to extract prices: {e}")
            return None

        # Wilder/standard EMA with smoothing factor k = 2 / (period + 1)
        k = 2.0 / (period + 1)
        ema = prices[0]
        for price in prices[1:]:
            ema = price * k + ema * (1 - k)

        return {
            'value': round(float(ema), 8),
            'additional_data': {'period': period},
        }

    async def calculate_stream(self, all_candles: list, warmup_periods: int) -> list:
        """Stateful O(n) EMA for bulk backfill.

        Computes EMA incrementally (ema = price*k + prev_ema*(1-k)) over the
        full candle list, returning one result dict per target candle (i.e. all
        candles from index warmup_periods onward).  This is orders-of-magnitude
        faster than calling calculate() with a sliding window for each bar.
        """
        period = self.parameters['period']
        k = 2.0 / (period + 1)
        key = 'close_price' if (all_candles and 'close_price' in all_candles[0]) else 'close'

        ema = None
        results = []
        for i, c in enumerate(all_candles):
            price = float(c[key])
            ema = price if ema is None else price * k + ema * (1.0 - k)
            if i >= warmup_periods:
                results.append({'value': round(ema, 8), 'additional_data': {'period': period}})
        return results

    def get_required_periods(self) -> int:
        # Need ~3x period for EMA to warm up properly
        return self.parameters['period'] * 3

    def validate_parameters(self) -> bool:
        period = self.parameters.get('period')
        return isinstance(period, int) and period >= 1

    def get_output_fields(self) -> List[str]:
        return ['value']
