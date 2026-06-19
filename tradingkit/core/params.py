from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, asdict


@dataclass(frozen=True)
class IndicatorParams:
    """
    Immutable descriptor for one indicator instance.

    Standard fields cover the most common parameters.
    Anything else (e.g. MACD slow/signal periods, BB std-dev) goes into `extra`
    as a sorted tuple of (key, value) pairs so frozen=True works correctly
    (dict is mutable and therefore not hashable).
    """
    type: str           # "rsi", "ema", "macd", "bbands", ...
    timeframe: str      # "1m", "5m", "1h", "4h", "1d", "1w"
    period: int = 14
    source: str = "close"
    extra: tuple = field(default_factory=tuple)  # ((k, v), ...) sorted

    # ------------------------------------------------------------------ #
    # Constructors                                                         #
    # ------------------------------------------------------------------ #

    @classmethod
    def create(
        cls,
        type: str,
        timeframe: str,
        period: int = 14,
        source: str = "close",
        **extra,
    ) -> "IndicatorParams":
        """Convenience constructor that accepts extra kwargs and sorts them."""
        return cls(
            type=type,
            timeframe=timeframe,
            period=period,
            source=source,
            extra=tuple(sorted(extra.items())),
        )

    @classmethod
    def from_dict(cls, d: dict) -> "IndicatorParams":
        standard = {"type", "timeframe", "period", "source"}
        extra = {k: v for k, v in d.items() if k not in standard}
        return cls.create(
            type=d["type"],
            timeframe=d["timeframe"],
            period=int(d.get("period", 14)),
            source=d.get("source", "close"),
            **extra,
        )

    # ------------------------------------------------------------------ #
    # Hashing / serialisation                                              #
    # ------------------------------------------------------------------ #

    def to_hash(self) -> str:
        """12-char SHA-1 prefix — deterministic, stable across restarts."""
        data = {
            "type": self.type,
            "timeframe": self.timeframe,
            "period": self.period,
            "source": self.source,
            **dict(self.extra),
        }
        return hashlib.sha1(
            json.dumps(data, sort_keys=True).encode()
        ).hexdigest()[:12]

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "timeframe": self.timeframe,
            "period": self.period,
            "source": self.source,
            **dict(self.extra),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_json(cls, s: str) -> "IndicatorParams":
        return cls.from_dict(json.loads(s))

    # ------------------------------------------------------------------ #
    # Display                                                              #
    # ------------------------------------------------------------------ #

    def display_name(self) -> str:
        """Human-readable label, e.g. 'RSI (14, 4H)'."""
        extra_str = ""
        if self.extra:
            extra_str = ", " + ", ".join(str(v) for _, v in self.extra)
        return f"{self.type.upper()} ({self.period}, {self.timeframe.upper()}{extra_str})"


@dataclass(frozen=True)
class StrategyParams:
    """
    Immutable descriptor for one strategy instance.

    All strategy-specific parameters (thresholds, cooldowns, indicator
    references, etc.) go into `extra` as sorted (k, v) pairs.
    """
    type: str           # "ema_deviation", "simple_rsi", ...
    extra: tuple = field(default_factory=tuple)  # ((k, v), ...) sorted

    # ------------------------------------------------------------------ #
    # Constructors                                                         #
    # ------------------------------------------------------------------ #

    @classmethod
    def create(cls, type: str, **kwargs) -> "StrategyParams":
        return cls(type=type, extra=tuple(sorted(kwargs.items())))

    @classmethod
    def from_dict(cls, d: dict) -> "StrategyParams":
        type_ = d.pop("type") if "type" in d else d.pop("strategy_type")
        return cls.create(type_, **d)

    # ------------------------------------------------------------------ #
    # Hashing / serialisation                                              #
    # ------------------------------------------------------------------ #

    def to_hash(self) -> str:
        data = {"type": self.type, **dict(self.extra)}
        return hashlib.sha1(
            json.dumps(data, sort_keys=True).encode()
        ).hexdigest()[:12]

    def to_dict(self) -> dict:
        return {"type": self.type, **dict(self.extra)}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_json(cls, s: str) -> "StrategyParams":
        return cls.from_dict(json.loads(s))

    def display_name(self) -> str:
        return self.type.replace("_", " ").title()
