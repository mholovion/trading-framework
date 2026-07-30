"""
tradingkit.strategy — everything needed to write a custom strategy.

Usage:
    from tradingkit import Strategy, Signal, BarContext, ta_indicator
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import polars as pl

from tradingkit.indicator import Indicator, IndicatorDeclaration

# ------------------------------------------------------------------ #
# Signal                                                               #
# ------------------------------------------------------------------ #

class Signal:
    """
    A sparse, timestamped record emitted by a strategy.

    The framework mandates no fields at all — the strategy author owns the schema, the
    same way a DataSource owns its row schema. `Signal` is convenience sugar; on_bar()
    may equally return a plain dict.

        Signal(action="short", price=42.0, zscore=4.2)
        Signal(regime="high_vol")                    # not a trade at all
        {"action": "buy", "price": 42.0}              # same thing, no import needed

    Two fields the framework *adds* when absent, rather than requires:
        timestamp      — the bar's timestamp, so signals form a queryable table
        gap_recovered  — True when Pipeline.run_live() produced this from a bar that was
                         backfilled after a stream gap rather than from a live tick, so a
                         consumer can treat an already-minutes-old signal differently

    What a field *means* is not the framework's business: pairing signals into trades and
    computing PnL is the job of a Metric over signals_df, which reads whichever columns
    it was configured with. That is why nothing here is privileged.
    """

    __slots__ = ("_fields",)

    def __init__(self, **fields: Any) -> None:
        object.__setattr__(self, "_fields", dict(fields))

    def __getattr__(self, name: str) -> Any:
        # __slots__ means _fields is the only real attribute, so anything else is a field
        # lookup. Raising AttributeError (not KeyError) keeps getattr(sig, x, default)
        # working, which callers rely on for optional fields.
        try:
            return object.__getattribute__(self, "_fields")[name]
        except KeyError:
            raise AttributeError(f"Signal has no field {name!r}") from None

    def __setattr__(self, name: str, value: Any) -> None:
        self._fields[name] = value

    def __contains__(self, name: str) -> bool:
        return name in self._fields

    # Explicit pickle protocol: unpickling builds the object without __init__, so the
    # default path would restore state through __setattr__ before _fields exists and
    # blow up looking it up. Setting the slot directly avoids that.
    def __getstate__(self) -> dict:
        return dict(self._fields)

    def __setstate__(self, state: dict) -> None:
        object.__setattr__(self, "_fields", dict(state))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Signal):
            return self._fields == other._fields
        return NotImplemented

    def __repr__(self) -> str:
        inner = ", ".join(f"{k}={v!r}" for k, v in self._fields.items())
        return f"Signal({inner})"

    # ------------------------------------------------------------------ #
    # Serialisation                                                        #
    # ------------------------------------------------------------------ #

    def to_dict(self) -> dict:
        """Every field, including any the framework added. Nothing can be lost here:
        there is only one place fields live, so this cannot drift from __setattr__."""
        return dict(self._fields)

    @classmethod
    def from_dict(cls, d: dict) -> Signal:
        return cls(**d)


# ------------------------------------------------------------------ #
# BarContext — per-bar snapshot given to on_bar()                      #
# ------------------------------------------------------------------ #

class BarContext:
    """
    Single-bar snapshot passed to Strategy.on_bar().

    Row columns available as dynamic attributes (bar.close, bar.price, ...).
    Indicator values available as dynamic attributes (bar.rsi, bar.ema, ...).
    Indicators take precedence over row columns on name collision.
    Full indicator series accessible via bar.series(name).
    """

    def __init__(
        self,
        row: dict,
        indicators: dict[str, float],
        series: dict[str, pl.Series] | None = None,
    ) -> None:
        self._row        = row
        self._indicators = indicators
        self._series     = series or {}
        self.timestamp   = int(row.get("timestamp", 0))

    def __getattr__(self, name: str):
        _ind = self.__dict__.get("_indicators")
        if _ind is not None and name in _ind:
            return _ind[name]
        _row = self.__dict__.get("_row")
        if _row is not None and name in _row:
            return _row[name]
        raise AttributeError(
            f"BarContext has no attribute '{name}'. "
            f"Indicators: {list(_ind or [])}. Row keys: {list(_row or [])}"
        )

    def series(self, name: str) -> pl.Series:
        """Return the full indicator series for look-back logic."""
        if name not in self._series:
            raise KeyError(
                f"Series '{name}' not in BarContext. "
                f"Available: {list(self._series.keys())}"
            )
        return self._series[name]

    def __repr__(self) -> str:
        return (
            f"BarContext(ts={self.timestamp}, "
            f"row_keys={list(self._row)}, "
            f"indicators={list(self._indicators)})"
        )


# ------------------------------------------------------------------ #
# Strategy ABC                                                          #
# ------------------------------------------------------------------ #

class Strategy(ABC):
    """
    Base class for all trading strategies.

    Declare indicator dependencies as class attributes:
        rsi = ta_indicator.rsi(period=14)    → bar.rsi in on_bar()
        ema = ta_indicator.ema(period=20)    → bar.ema in on_bar()
        my  = MyCustomIndicator(param=5)     → bar.my  in on_bar()

    Implement on_bar() to produce signals:
        async def on_bar(self, bar: BarContext) -> Optional[Signal]:
            if bar.rsi < 30:
                return Signal(action="buy", price=bar.close)

    The framework resolves and computes all declared indicators before
    calling on_bar() for each bar.
    """

    #: Class-level fallback so a subclass that writes its own __init__ without calling
    #: super() still has a usable config. Without this, get_required_indicators() raised
    #: AttributeError on such a strategy -- and writing one is the common case, since a
    #: strategy's parameters are usually plain __init__ arguments.
    _config: dict | None = None

    def __init__(self, config: dict | None = None) -> None:
        self._config = config or {}

    @property
    def config(self) -> dict:
        if self._config is None:
            self._config = {}   # per-instance, so the class-level default stays immutable
        return self._config

    @config.setter
    def config(self, value: dict | None) -> None:
        self._config = value or {}

    @abstractmethod
    async def on_bar(self, bar: BarContext) -> Signal | dict | None:
        """Process one bar. Return a Signal, a plain dict of the same shape, or None
        (nothing to emit). The fields are yours to choose — see Signal."""
        ...

    @classmethod
    def required_indicators(cls) -> dict[str, IndicatorDeclaration | Indicator]:
        """
        Auto-collect all IndicatorDeclaration / Indicator class attributes.
        Returns {attr_name: declaration_or_instance}.
        """
        result: dict[str, IndicatorDeclaration | Indicator] = {}
        for klass in reversed(cls.__mro__):
            for k, v in vars(klass).items():
                if isinstance(v, (IndicatorDeclaration, Indicator)):
                    result[k] = v
        return result

    def validate(self) -> bool:
        return True

    # ------------------------------------------------------------------ #
    # Backward compat — old plugins used process() + get_required_indicators()
    # ------------------------------------------------------------------ #

    async def process(
        self,
        indicators_data: dict[str, Any],
        row: dict,
        signal_timestamp: int | None = None,
    ) -> dict | None:
        """Deprecated. Implement on_bar() instead."""
        return None

    @classmethod
    def get_required_indicators(cls, params: Any = None) -> list:
        """Deprecated. Use required_indicators() class method."""
        return []

    def validate_parameters(self) -> bool:
        return self.validate()


# ------------------------------------------------------------------ #
# ScriptStrategy — for UI editor                                       #
# ------------------------------------------------------------------ #

class ScriptStrategy(Strategy):
    """
    Wraps user-provided strategy code for the UI editor and the resolver.

    UI editor interface (on_bar):
        bar, Signal — access row columns via bar.<col>, indicators via bar.<name>.
        code assigns to 'signal' or leaves it None (hold).

    Resolver interface (process / process_batch):
        Compatible with ScriptStrategyPlugin — same class, unified interface.
        Executes process() / process_batch() from the script's own namespace
        if defined; otherwise falls back to on_bar() style execution.
    """

    def __init__(
        self,
        code: str,
        indicators: dict[str, Indicator] | None = None,
        config: dict | None = None,
        **params: Any,
    ) -> None:
        super().__init__(config)
        self._code = code
        self._inds = indicators or {}
        self.params = params
        self._exec_ns()

    def _exec_ns(self) -> None:
        # Execute script once so top-level definitions (get_required_indicators,
        # process, process_batch) persist across calls.
        # on_bar-style scripts reference `bar` / `Signal` which aren't defined here —
        # that's expected; they get injected in on_bar(). Ignore NameError.
        self._ns: dict[str, Any] = {}
        try:
            exec(compile(self._code, "<strategy_script>", "exec"), self._ns)  # noqa: S102
        except (NameError, AttributeError):
            pass

    def __getstate__(self) -> dict:
        # exec() always populates the namespace's '__builtins__' with the full
        # builtins dict (eval, exec, open, __import__, ...) — pickling that would
        # embed every builtin as a reference in the stream. _ns is a derived cache
        # of self._code, not fundamental state, so exclude it and rebuild it in
        # __setstate__ instead (same pattern as JitIndicator.__getstate__ in
        # indicator.py, which clears its numba compile cache for the same reason).
        state = self.__dict__.copy()
        state.pop("_ns", None)
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._exec_ns()

    async def on_bar(self, bar: BarContext) -> Signal | None:
        namespace: dict[str, Any] = {
            **self._ns,
            "bar":    bar,
            "Signal": Signal,
            "signal": None,
            **{k: getattr(bar, k, None) for k in self._inds},
            **self.params,
        }
        exec(compile(self._code, "<ui_strategy>", "exec"), namespace)  # noqa: S102
        result = namespace.get("signal")
        if result is None:
            return None
        if isinstance(result, Signal):
            return result
        if isinstance(result, dict):
            # A dict is the same record without the import -- the shape the framework
            # actually consumes, so accept it rather than making Signal mandatory.
            return Signal(**result)
        raise TypeError(
            f"Strategy code must assign a Signal or dict to 'signal', got {type(result)}"
        )

    def get_required_indicators(self, params: Any = None) -> list:
        fn = self._ns.get("get_required_indicators")
        if fn:
            cfg = self.config or {}
            return fn(cfg)
        return list(self.config.get("required_indicators", []) if self.config else [])

    async def process(
        self,
        indicators_data: dict[str, Any],
        row: dict,
        signal_timestamp: int | None = None,
        **_: Any,
    ) -> dict | None:
        fn = self._ns.get("process")
        if fn is None:
            return None
        cfg = self.config or {}
        result = await fn(indicators_data, row, cfg, signal_timestamp)
        if result is None:
            return None
        return result.to_dict() if hasattr(result, "to_dict") else result

    async def process_batch(self, batch_data: list) -> list:
        results = []
        for data in batch_data:
            sig = await self.process(
                data["indicators_data"],
                data["row"],
                signal_timestamp=data.get("timestamp"),
            )
            results.append(sig)
        return results


StrategyPlugin = Strategy


# ------------------------------------------------------------------ #
# CppStrategyPlugin — C++ compiled strategy, requires CppRunnerPool   #
# ------------------------------------------------------------------ #

class CppStrategyPlugin:
    """
    C++ strategy plugin. Wraps compiled .so for the resolver interface.

    ABI (extern "C"):
        int strategy_process(
            double close, double high, double low, double open, double volume,
            const char* indicators_json, const char* params_json,
            char* signal_out, int signal_out_len)
        // signal_out: JSON {"signal_type":"buy","confidence":0.8} or ""
        // Returns: bytes written, or -1 on error
    """

    def __init__(self, cpp_code: str, so_bytes: bytes, config: dict) -> None:
        self._cpp_code = cpp_code
        self._so_bytes = so_bytes
        self._config   = config

    def get_required_indicators(self, params: Any = None) -> list:
        return list(self._config.get("required_indicators", []))

    async def process(self, *_: Any, **__: Any) -> dict | None:
        raise RuntimeError("CppStrategyPlugin must run through CppRunnerPool")

    async def process_batch(self, batch_data: list) -> list:
        raise RuntimeError("CppStrategyPlugin must run through CppRunnerPool")


def load_strategy_plugin(type_: str, params_dict: dict) -> ScriptStrategy | CppStrategyPlugin:
    """
    Create a strategy plugin from a type string and params dict.

    '__script__'  — ScriptStrategy wrapping user code (from _code param).
    '__cpp__'     — CppStrategyPlugin (compiled .so, requires CppRunnerPool).
    named types   — looks up in a host-registered builtin registry, see
                    tradingkit.core.plugin_registry.
    """
    if type_ == "__script__":
        code = params_dict.get("_code", "")
        if not code:
            raise ValueError("Strategy type '__script__' requires '_code' in params")
        return ScriptStrategy(code=code, config=params_dict)

    if type_ == "__cpp__":
        import base64
        so_b64 = params_dict.get("_so_b64", "")
        so_bytes = base64.b64decode(so_b64) if so_b64 else b""
        code = params_dict.get("_code", "")
        return CppStrategyPlugin(cpp_code=code, so_bytes=so_bytes, config=params_dict)

    from tradingkit.core.plugin_registry import get_builtin_registry
    BUILTIN_STRATEGIES = get_builtin_registry("TRADINGKIT_STRATEGIES_MODULE", "BUILTIN_STRATEGIES")
    code = BUILTIN_STRATEGIES.get(type_)

    if not code:
        raise ValueError(
            f"Unknown strategy type {type_!r}. "
            f"Use '__script__' with '_code', or a registered builtin name."
        )
    return ScriptStrategy(code=code, config=params_dict)


__all__ = [
    "BarContext",
    "CppStrategyPlugin",
    "ScriptStrategy",
    "Signal",
    "Strategy",
    "StrategyPlugin",
    "load_strategy_plugin",
]
