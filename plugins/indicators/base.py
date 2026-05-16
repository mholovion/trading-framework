from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
import numpy as np
from plugins.indicators.context import IndicatorContext


class IndicatorPlugin(ABC):
    """
    Base class for all indicator plugins.

    Subclasses implement compute() and get_required_periods().
    calculate() and calculate_stream() are provided by the framework.

    Minimal example (class-based):
        class ATRPlugin(IndicatorPlugin):
            def compute(self, ctx: IndicatorContext) -> np.ndarray:
                return ctx.ta.atr(ctx.high, ctx.low, ctx.close, self.parameters["period"])
            def get_required_periods(self) -> int:
                return self.parameters["period"] + 1

    Script-style plugins (body-only .py files) are handled by loader.py and
    do not subclass IndicatorPlugin directly.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.name = (
            self.__class__.__name__.lower().replace("plugin", "").replace("indicator", "")
        )
        self.parameters = config.get("parameters", config)

    @abstractmethod
    def compute(self, ctx: IndicatorContext) -> np.ndarray:
        """Return a full result series (same length as ctx.close)."""

    @abstractmethod
    def get_required_periods(self) -> int:
        """Minimum number of candles needed before values are meaningful."""

    def validate_parameters(self) -> bool:
        return True

    def get_output_fields(self) -> List[str]:
        return ["value"]

    def get_required_timeframes(self) -> List[str]:
        return [self.config["source_timeframe"]]

    def get_lookback_periods(self) -> int:
        return self.get_required_periods()

    async def calculate(self, data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if len(data) < self.get_required_periods():
            return None
        ctx = IndicatorContext(data)
        result = self.compute(ctx)
        val = float(result[-1])
        if np.isnan(val) or np.isinf(val):
            return None
        return {"value": val, "additional_data": {}}

    async def calculate_stream(
        self, all_candles: list, warmup_periods: int
    ) -> list:
        ctx = IndicatorContext(all_candles)
        result = self.compute(ctx)
        out = []
        for i in range(warmup_periods, len(all_candles)):
            val = float(result[i])
            if np.isnan(val) or np.isinf(val):
                out.append(None)
            else:
                out.append({"value": val, "additional_data": {}})
        return out
