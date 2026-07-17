"""tradingkit.core.params — IndicatorParams / StrategyParams."""
from __future__ import annotations

from tradingkit.core.params import IndicatorParams, StrategyParams


def test_indicator_params_create_sorts_extra():
    p = IndicatorParams.create("macd", "1h", period=14, slow=26, fast=12)
    assert p.extra == (("fast", 12), ("slow", 26))


def test_indicator_params_hash_is_deterministic():
    p1 = IndicatorParams.create("rsi", "1h", period=14)
    p2 = IndicatorParams.create("rsi", "1h", period=14)
    assert p1.to_hash() == p2.to_hash()
    assert len(p1.to_hash()) == 12


def test_indicator_params_hash_differs_on_different_params():
    p1 = IndicatorParams.create("rsi", "1h", period=14)
    p2 = IndicatorParams.create("rsi", "1h", period=21)
    assert p1.to_hash() != p2.to_hash()


def test_indicator_params_hash_ignores_extra_key_order():
    p1 = IndicatorParams.create("macd", "1h", fast=12, slow=26)
    p2 = IndicatorParams.create("macd", "1h", slow=26, fast=12)
    assert p1.to_hash() == p2.to_hash()


def test_indicator_params_json_roundtrip():
    p = IndicatorParams.create("bbands", "4h", period=20, std_dev=2)
    restored = IndicatorParams.from_json(p.to_json())
    assert restored == p


def test_indicator_params_from_dict_extracts_extra():
    p = IndicatorParams.from_dict({"type": "macd", "timeframe": "1h", "fast": 12, "slow": 26})
    assert p.type == "macd"
    assert dict(p.extra) == {"fast": 12, "slow": 26}


def test_indicator_params_defaults():
    p = IndicatorParams.from_dict({"type": "rsi", "timeframe": "1h"})
    assert p.period == 14
    assert p.source == "close"


def test_indicator_params_display_name():
    p = IndicatorParams.create("rsi", "4h", period=14)
    assert p.display_name() == "RSI (14, 4H)"


def test_indicator_params_is_hashable_and_frozen():
    p = IndicatorParams.create("rsi", "1h", period=14)
    {p}  # must be hashable to go in a set
    try:
        p.period = 21
        assert False, "should be frozen"
    except AttributeError:
        pass


def test_strategy_params_create_and_hash():
    p1 = StrategyParams.create("ema_deviation", threshold=0.02, cooldown=60)
    p2 = StrategyParams.create("ema_deviation", cooldown=60, threshold=0.02)
    assert p1.to_hash() == p2.to_hash()


def test_strategy_params_from_dict_accepts_type_or_strategy_type():
    p1 = StrategyParams.from_dict({"type": "simple_rsi", "overbought": 70})
    p2 = StrategyParams.from_dict({"strategy_type": "simple_rsi", "overbought": 70})
    assert p1.type == p2.type == "simple_rsi"
    assert p1.to_hash() == p2.to_hash()


def test_strategy_params_json_roundtrip():
    p = StrategyParams.create("ema_deviation", threshold=0.02)
    restored = StrategyParams.from_json(p.to_json())
    assert restored == p


def test_strategy_params_display_name():
    p = StrategyParams.create("ema_deviation_strategy")
    assert p.display_name() == "Ema Deviation Strategy"