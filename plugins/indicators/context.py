from __future__ import annotations
import numpy as np
from typing import List, Dict, Any
from plugins.indicators.ta import ta


class IndicatorContext:
    """
    Wraps a candle list as typed numpy arrays + ta namespace.

    Available in indicator scripts:
      ctx.open, ctx.high, ctx.low, ctx.close, ctx.volume  — np.ndarray float64
      ctx.ts                                               — np.ndarray int64 (timestamps)
      ctx.ta.rsi(ctx.close, period)                        — any ta function
    """

    def __init__(self, candles: List[Dict[str, Any]]) -> None:
        arr = np.array(
            [
                [
                    float(c.get("open", c.get("open_price", 0))),
                    float(c.get("high", c.get("high_price", 0))),
                    float(c.get("low", c.get("low_price", 0))),
                    float(c.get("close", c.get("close_price", 0))),
                    float(c.get("volume", 0)),
                ]
                for c in candles
            ],
            dtype=np.float64,
        )
        self.open   = arr[:, 0]
        self.high   = arr[:, 1]
        self.low    = arr[:, 2]
        self.close  = arr[:, 3]
        self.volume = arr[:, 4]
        self.ts     = np.array([c.get("timestamp", 0) for c in candles], dtype=np.int64)
        self.ta     = ta
