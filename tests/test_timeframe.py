"""tradingkit.core.timeframe — parse_timeframe and friends."""
from __future__ import annotations

import pytest

from tradingkit.core.timeframe import (
    TimeframeUtils,
    get_period_end,
    get_period_start,
    parse_timeframe,
    seconds_to_tf_string,
)


@pytest.mark.parametrize("tf,expected", [
    ("1m", 60), ("5m", 300), ("15m", 900), ("30m", 1800),
    ("1h", 3600), ("4h", 14400), ("1d", 86400), ("1w", 604800),
    ("90m", 5400), ("3h", 10800), ("2d", 172800),
    (3600, 3600), (0, 0),
])
def test_parse_timeframe_valid(tf, expected):
    assert parse_timeframe(tf) == expected


@pytest.mark.parametrize("tf", ["", "4x", "h4", "-4h", "4 h", "abc", "4.5h"])
def test_parse_timeframe_invalid_raises(tf):
    with pytest.raises(ValueError):
        parse_timeframe(tf)


def test_parse_timeframe_case_and_whitespace_insensitive():
    assert parse_timeframe(" 4H ") == 14400


@pytest.mark.parametrize("seconds,expected", [
    (60, "1m"), (3600, "1h"), (86400, "1d"), (604800, "1w"), (90, "90s"),
])
def test_seconds_to_tf_string(seconds, expected):
    assert seconds_to_tf_string(seconds) == expected


def test_get_period_start_and_end():
    assert get_period_start(3661, 3600) == 3600
    assert get_period_end(3661, 3600) == 7200
    assert get_period_start(0, 60) == 0


def test_timeframe_utils_lookup_and_fallback():
    assert TimeframeUtils.get_timeframe_seconds("1h") == 3600
    assert TimeframeUtils.get_timeframe_seconds("90m") == 5400  # falls back to parse_timeframe
    assert TimeframeUtils.validate_timeframe("1h") is True
    assert TimeframeUtils.validate_timeframe("90m") is False  # not in the fixed dict
    assert "1h" in TimeframeUtils.get_supported_timeframes()


def test_timeframe_utils_unsupported_raises():
    with pytest.raises(ValueError):
        TimeframeUtils.get_timeframe_seconds("not-a-timeframe")