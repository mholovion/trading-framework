"""
Timeframe utilities for tradingkit.

parse_timeframe("4h") → 14400
parse_timeframe("90m") → 5400
parse_timeframe(3600)  → 3600
"""
from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from typing import Union


def parse_timeframe(tf: Union[str, int]) -> int:
    """
    Convert any timeframe representation to seconds.

    Accepts:
      - int (already seconds): returned as-is
      - strings: "1m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h",
                 "12h", "1d", "3d", "1w" and any custom N+unit like "90m", "3h"

    Units: s=1, m=60, h=3600, d=86400, w=604800
    """
    if isinstance(tf, int):
        return tf
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
    m = re.fullmatch(r"(\d+)([smhdw])", tf.strip().lower())
    if not m:
        raise ValueError(f"Invalid timeframe: {tf!r}. Use format like '4h', '90m', '1d'")
    return int(m.group(1)) * units[m.group(2)]


def seconds_to_tf_string(seconds: int) -> str:
    """Convert seconds back to a human-readable timeframe string."""
    for divisor, unit in [(604800, "w"), (86400, "d"), (3600, "h"), (60, "m")]:
        if seconds % divisor == 0:
            return f"{seconds // divisor}{unit}"
    return f"{seconds}s"


def get_period_start(timestamp: int, seconds: int) -> int:
    """Return the start of the period containing timestamp, aligned to seconds boundaries."""
    return (timestamp // seconds) * seconds


def get_period_end(timestamp: int, seconds: int) -> int:
    return get_period_start(timestamp, seconds) + seconds


# Backward-compat: old TimeframeUtils class still available for existing code
class TimeframeUtils:
    TIMEFRAME_SECONDS: dict[str, int] = {
        "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
        "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600, "8h": 28800,
        "12h": 43200, "1d": 86400, "3d": 259200, "1w": 604800, "1M": 2592000,
    }

    @classmethod
    def get_timeframe_seconds(cls, timeframe: str) -> int:
        if timeframe in cls.TIMEFRAME_SECONDS:
            return cls.TIMEFRAME_SECONDS[timeframe]
        try:
            return parse_timeframe(timeframe)
        except ValueError:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

    @classmethod
    def get_supported_timeframes(cls) -> list[str]:
        return list(cls.TIMEFRAME_SECONDS.keys())

    @classmethod
    def validate_timeframe(cls, timeframe: str) -> bool:
        return timeframe in cls.TIMEFRAME_SECONDS
