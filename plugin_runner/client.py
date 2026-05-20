"""HTTP client proxies for the plugin-runner container.

RemoteIndicatorPlugin and RemoteStrategyPlugin implement the same interfaces
as local plugins but delegate computation to the plugin-runner service.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from typing import Any, Dict, List, Optional

import aiohttp

from plugins.indicators.base import IndicatorPlugin
from plugins.indicators.context import IndicatorContext
from plugins.strategies.base import StrategyPlugin, StrategySignal

logger = logging.getLogger(__name__)

_TIMEOUT_META    = 10    # seconds for info / metadata calls
_TIMEOUT_COMPUTE = 180   # seconds for bulk computation


def _post_sync(url: str, payload: dict) -> dict:
    """Blocking JSON POST — used only for fast metadata calls."""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=_TIMEOUT_META) as resp:
        return json.loads(resp.read())


# --------------------------------------------------------------------------- #
# Indicator proxy                                                              #
# --------------------------------------------------------------------------- #

class RemoteIndicatorPlugin(IndicatorPlugin):
    """Delegates indicator computation to the plugin-runner container."""

    def __init__(self, name: str, config: Dict[str, Any], runner_url: str) -> None:
        super().__init__(config)
        self._name = name
        self._runner_url = runner_url
        self._required_periods: int | None = None

    # -- IndicatorPlugin interface ----------------------------------------- #

    def compute(self, ctx: IndicatorContext):
        raise NotImplementedError("RemoteIndicatorPlugin: use calculate_stream")

    def get_required_periods(self) -> int:
        if self._required_periods is None:
            data = _post_sync(
                f"{self._runner_url}/indicator/info",
                {"name": self._name, "config": self.config},
            )
            self._required_periods = int(data["required_periods"])
        return self._required_periods

    async def calculate_stream(self, all_candles: list, warmup_periods: int) -> list:
        """Legacy path: caller passes candles. Used by resolver for incremental updates."""
        payload = {
            "name":    self._name,
            "config":  self.config,
            "candles": all_candles,
            "warmup":  warmup_periods,
        }
        timeout = aiohttp.ClientTimeout(total=_TIMEOUT_COMPUTE)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{self._runner_url}/indicator/calculate_stream", json=payload
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        return data["results"]

    async def compute_for(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_ts: int | None = None,
        end_ts: int | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """
        Plugin-runner fetches candles from ClickHouse itself and computes.
        Returns list of {timestamp, value} dicts — all available history.
        """
        payload = {
            "name":     self._name,
            "config":   self.config,
            "exchange": exchange,
            "symbol":   symbol,
            "timeframe": timeframe,
        }
        if start_ts is not None:
            payload["start_ts"] = start_ts
        if end_ts is not None:
            payload["end_ts"] = end_ts
        if limit is not None:
            payload["limit"] = limit  # 0 = no limit (full history)

        timeout = aiohttp.ClientTimeout(total=_TIMEOUT_COMPUTE)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{self._runner_url}/indicator/compute", json=payload
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        return data["data"]


# --------------------------------------------------------------------------- #
# Strategy proxy                                                               #
# --------------------------------------------------------------------------- #

class RemoteStrategyPlugin(StrategyPlugin):
    """Delegates strategy computation to the plugin-runner container."""

    def __init__(self, name: str, config: Dict[str, Any], runner_url: str) -> None:
        super().__init__(config)
        self._name = name
        self._runner_url = runner_url
        self._required_cache: list | None = None

    # -- StrategyPlugin interface ------------------------------------------ #

    def validate_parameters(self) -> bool:
        return True

    def get_required_indicators(self, params=None) -> List[str]:
        if self._required_cache is None:
            data = _post_sync(
                f"{self._runner_url}/strategy/get_required",
                {"name": self._name, "config": self.config},
            )
            self._required_cache = data["required"]
        return self._required_cache

    async def process(
        self,
        indicators_data: Dict[str, Any],
        current_price: float,
        signal_timestamp: Optional[int] = None,
        **_kwargs,
    ) -> Optional[StrategySignal]:
        results = await self.process_batch([
            {
                "timestamp":       signal_timestamp,
                "indicators_data": indicators_data,
                "current_price":   current_price,
            }
        ])
        return results[0]

    # -- Batch helper (used by resolver) ------------------------------------ #

    async def process_batch(
        self, candle_inputs: list
    ) -> list[Optional[dict]]:
        """
        Send all candles at once; returns list of signal dicts (or None).
        Indexed 1-to-1 with candle_inputs.
        """
        payload = {
            "name":         self._name,
            "config":       self.config,
            "candle_inputs": candle_inputs,
        }
        timeout = aiohttp.ClientTimeout(total=_TIMEOUT_COMPUTE)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{self._runner_url}/strategy/process_batch", json=payload
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        return data["signals"]
