"""
tradingkit.indicator — everything needed to write a custom indicator.

Usage:
    from tradingkit import Indicator, IndicatorContext, ta_indicator
"""
from __future__ import annotations

import ast as _ast
import re as _re

import numpy as np
import polars as pl
from abc import ABC, abstractmethod
from typing import Any


# ------------------------------------------------------------------ #
# TA namespace                                                          #
# ------------------------------------------------------------------ #

class _TA:
    """
    Technical analysis namespace: ctx.ta.<name>(*args).

    Lookup order:
      1. Registered primitives (numba-compiled, e.g. rma)
      2. TA-Lib (200+ built-in indicators, C library — 5-20x faster than pandas-ta)

    Single-output → np.ndarray; multi-output (MACD, BB...) → tuple[np.ndarray, ...]
    Backward-compat: period=/length= kwargs are remapped to timeperiod=.
    """
    _primitives: dict[str, Any] = {}

    @classmethod
    def register_primitive(cls, name: str, fn: Any) -> None:
        cls._primitives[name] = fn

    def __getattr__(self, name: str):
        if name in _TA._primitives:
            return _TA._primitives[name]
        try:
            import talib
        except ImportError as exc:
            raise AttributeError(
                f"TA-Lib not installed — cannot call ta.{name}(). pip install TA-Lib"
            ) from exc
        fn = getattr(talib, name.upper(), None)
        if fn is None:
            raise AttributeError(f"'{name}' not found in primitives or TA-Lib")

        def _wrapper(*args, **kwargs):
            # Map pandas-ta style kwargs → TA-Lib (period=/length= → timeperiod=)
            if "length" in kwargs and "timeperiod" not in kwargs:
                kwargs["timeperiod"] = kwargs.pop("length")
            elif "period" in kwargs and "timeperiod" not in kwargs:
                kwargs["timeperiod"] = kwargs.pop("period")
            return fn(*args, **kwargs)

        return _wrapper


_ta_ns = _TA()


# ------------------------------------------------------------------ #
# Numpy view                                                           #
# ------------------------------------------------------------------ #

class _NP:
    """Lazy zero-copy numpy accessor: ctx.np.<column_name> → np.ndarray."""

    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def __getattr__(self, name: str) -> np.ndarray:
        _df = self.__dict__.get("_df")
        if _df is not None:
            try:
                return _df[name].to_numpy()
            except Exception:
                pass
        raise AttributeError(
            f"_NP has no column '{name}'. Available: {_df.columns if _df is not None else []}"
        )


# ------------------------------------------------------------------ #
# IndicatorContext — heart of the framework                            #
# ------------------------------------------------------------------ #

class IndicatorContext:
    """
    Heart of the framework. Wraps any Polars DataFrame as typed properties.

    Primary interface (Polars Series):
        ctx.<column>  → pl.Series  (any column by name via __getattr__)
        ctx.ts        → pl.Series  (shorthand for ctx.timestamp)
        ctx.df        → pl.DataFrame (underlying data)

    Numpy interface for TA libraries and numba:
        ctx.np.<column> → np.ndarray

    Technical analysis:
        ctx.ta.rsi(ctx.np.close, 14)   → np.ndarray
        ctx.ta.ema(ctx.np.close, 20)   → np.ndarray
    """

    def __init__(self, df: pl.DataFrame) -> None:
        if "timestamp" not in df.columns:
            raise ValueError(f"IndicatorContext: 'timestamp' column required. Got: {df.columns}")
        self._df = df

    @property
    def df(self) -> pl.DataFrame: return self._df

    @property
    def ts(self) -> pl.Series: return self._df["timestamp"]

    @property
    def np(self) -> _NP: return _NP(self._df)

    @property
    def ta(self) -> _TA: return _ta_ns

    def __getattr__(self, name: str) -> pl.Series:
        _df = self.__dict__.get("_df")
        if _df is not None and name in _df.columns:
            return _df[name]
        raise AttributeError(
            f"IndicatorContext has no '{name}'. Columns: {_df.columns if _df is not None else []}"
        )

    def __len__(self) -> int:
        return self._df.height

    def __repr__(self) -> str:
        return f"IndicatorContext({self._df.height} rows, cols={self._df.columns})"


# ------------------------------------------------------------------ #
# IndicatorDeclaration — descriptor for class-level declarations       #
# ------------------------------------------------------------------ #

class IndicatorDeclaration:
    """
    Descriptor that marks a class attribute as an indicator declaration.
    Used for class-level indicator declarations in Strategy subclasses.

    class MyStrategy(Strategy):
        rsi = ta_indicator.rsi(period=14)   # IndicatorDeclaration
        ema = MyCustomIndicator(period=20)  # also IndicatorDeclaration
    """
    def __init__(self, indicator_cls: type, **params: Any) -> None:
        self.indicator_cls = indicator_cls
        self.params = params
        self.name: str = ""

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    def build(self) -> "Indicator":
        return self.indicator_cls(**self.params)

    def __repr__(self) -> str:
        return f"IndicatorDeclaration({self.indicator_cls.__name__}, {self.params})"


# ------------------------------------------------------------------ #
# Indicator ABC                                                         #
# ------------------------------------------------------------------ #

class Indicator(ABC):
    """
    Base class for all indicators.

    Minimal contract:
        compute(ctx) → np.ndarray  (same length as ctx)
        required_periods() → int   (warmup bars needed)

    Called by framework:
        indicator(ctx) → pl.Series  (via __call__)
    """

    def __init__(self, **params: Any) -> None:
        self.params = params
        for k, v in params.items():
            setattr(self, k, v)

    @abstractmethod
    def compute(self, ctx: IndicatorContext) -> np.ndarray:
        """
        Compute indicator. Returns np.ndarray of same length as ctx.
        First required_periods() values should be np.nan.
        """
        ...

    @abstractmethod
    def required_periods(self) -> int:
        """Minimum number of bars before values are meaningful."""
        ...

    def __call__(self, ctx: IndicatorContext) -> pl.Series:
        """Execute compute() and return a Polars Series."""
        arr = self.compute(ctx)
        return pl.Series("value", arr, dtype=pl.Float64)

    # Backward compat: old base used get_required_periods()
    def get_required_periods(self) -> int:
        return self.required_periods()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.params})"


# ------------------------------------------------------------------ #
# ScriptIndicator — for UI editor with sandboxed execution             #
# ------------------------------------------------------------------ #

class ScriptIndicator(Indicator):
    """
    Wraps user-provided indicator code for sandboxed execution via SubprocessExecutor.

    The code receives all DataFrame columns as numpy arrays (by column name),
    plus ctx, ta, np, and any declared params:
        result = ta.rsi(close, period)      # for OHLCV data
        result = ta.rsi(price, period)      # for custom column

    The code MUST assign 'result' (np.ndarray of same length as input).

    Used by the UI indicator editor:
        indicator = ScriptIndicator(code="result = ta.rsi(close, period)", period=14)
        series = await executor.compute_indicator(indicator, ctx)
    """

    def __init__(self, code: str, **params: Any) -> None:
        super().__init__(**params)
        self._code = code

    def compute(self, ctx: IndicatorContext) -> np.ndarray:
        namespace: dict[str, Any] = {
            **{col: ctx.df[col].to_numpy() for col in ctx.df.columns},
            "ctx": ctx,
            "ta":  ctx.ta,
            "np":  np,
            **self.params,
        }
        exec(compile(self._code, "<ui_indicator>", "exec"), namespace)  # noqa: S102
        if "result" not in namespace:
            raise ValueError("Indicator code must assign to 'result'")
        return np.asarray(namespace["result"], dtype=np.float64)

    def required_periods(self) -> int:
        return int(self.params.get("period", self.params.get("length", 14)))


# ------------------------------------------------------------------ #
# ta_indicator namespace — built-in TA-Lib indicators                  #
# ------------------------------------------------------------------ #

class _TAIndicatorNS:
    """
    Namespace for declaring TA-Lib indicators in Strategy class definitions.

    Usage:
        from tradingkit import ta_indicator

        class MyStrategy(Strategy):
            rsi = ta_indicator.rsi(period=14)   → bar.rsi in on_bar()
            ema = ta_indicator.ema(period=20)   → bar.ema in on_bar()
            bb  = ta_indicator.bbands(period=20)→ bar.bb  in on_bar()

    Any function available via TA-Lib can be used.
    The indicator type is logged as 'ta.<name>' for clarity.
    """

    def __getattr__(self, name: str):
        def factory(**params: Any) -> IndicatorDeclaration:
            class _TaIndicator(Indicator):
                _ta_name = name

                def compute(self, ctx: IndicatorContext) -> np.ndarray:
                    result = getattr(ctx.ta, self._ta_name)(ctx.np.close, **self.params)
                    if isinstance(result, tuple):
                        return result[0]  # take first output for multi-output indicators
                    return result

                def required_periods(self) -> int:
                    return int(self.params.get("period", self.params.get("length", 14)))

            _TaIndicator.__name__ = f"ta.{name}"
            _TaIndicator.__qualname__ = f"ta.{name}"
            return IndicatorDeclaration(_TaIndicator, **params)

        return factory


ta_indicator = _TAIndicatorNS()


# ------------------------------------------------------------------ #
# Re-export TA namespace for direct use in indicator code              #
# ------------------------------------------------------------------ #

# ------------------------------------------------------------------ #
# Factory helpers — moved from plugins/indicators/loader.py            #
# ------------------------------------------------------------------ #

def _extract_defaults(code: str) -> dict[str, Any]:
    """Parse ``__params__ = {...}`` literal from indicator code and return defaults."""
    match = _re.search(r"__params__\s*=\s*(\{.*?\})", code, _re.DOTALL)
    if not match:
        return {}
    try:
        raw = _ast.literal_eval(match.group(1))
        return {
            k: (v["default"] if isinstance(v, dict) and "default" in v else v)
            for k, v in raw.items()
        }
    except Exception:
        return {}


def _make_talib_indicator(
    type_name: str,
    period: int,
    source: str,
    extra: dict,
) -> "Indicator":
    """Create an Indicator that delegates to TA-Lib (via ctx.ta namespace)."""

    class _TalibIndicator(Indicator):
        _tn = type_name
        _p  = period
        _s  = source
        _ex = extra

        def compute(self, ctx: "IndicatorContext") -> np.ndarray:
            src = getattr(ctx.np, self._s, ctx.np.close)
            result = getattr(ctx.ta, self._tn)(src, length=self._p, **self._ex)
            if isinstance(result, tuple):
                return np.asarray(result[0], dtype=np.float64)
            return np.asarray(result, dtype=np.float64)

        def required_periods(self) -> int:
            return self._p

    return _TalibIndicator(period=period, source=source)


def load_indicator_plugin(type_: str, params_dict: dict) -> "Indicator":
    """
    Create an Indicator from a type string and params dict.

    Supported types:
      '__script__'  — ScriptIndicator wrapping user code (from _code param)
      '__jit__'     — JitIndicator (Numba JIT, nopython-compatible code)
      '__cpp__'     — CppIndicator (compiled .so, requires CppRunnerPool)
      any other     — TA-Lib based indicator via _make_talib_indicator
    """
    user_params = {
        k: v for k, v in params_dict.items()
        if k not in ("type", "timeframe", "source", "_code", "_so_b64")
        and not k.startswith("_")
    }

    if type_ == "__script__":
        code = params_dict.get("_code", "")
        defaults = _extract_defaults(code)
        return ScriptIndicator(code=code, **{**defaults, **user_params})

    if type_ == "__jit__":
        code = params_dict.get("_code", "")
        defaults = _extract_defaults(code)
        return JitIndicator(code=code, **{**defaults, **user_params})

    if type_ == "__cpp__":
        import base64
        so_b64 = params_dict.get("_so_b64", "")
        so_bytes = base64.b64decode(so_b64) if so_b64 else b""
        code = params_dict.get("_code", "")
        return CppIndicator(cpp_code=code, so_bytes=so_bytes, **user_params)

    period = int(params_dict.get("period", 14))
    source = params_dict.get("source", "close")
    extra  = {
        k: v for k, v in params_dict.items()
        if k not in ("type", "timeframe", "period", "source", "_code", "_so_b64")
        and not k.startswith("_")
    }
    return _make_talib_indicator(type_, period, source, extra)


# ------------------------------------------------------------------ #
# JitIndicator — Numba JIT compiled (user writes nopython code)        #
# ------------------------------------------------------------------ #

class JitIndicator(Indicator):
    """
    Numba JIT-compiled indicator.

    User code must be nopython-compatible: no ta.* calls, no Python objects.
    Receives named numpy arrays matching DataFrame columns + params as constants.
    Must assign to `result` variable.

    Example:
        alpha = 2.0 / (period + 1)
        result = np.empty_like(close)
        result[0] = close[0]
        for i in range(1, len(close)):
            result[i] = alpha * close[i] + (1 - alpha) * result[i - 1]
    """

    def __init__(self, code: str, **params: Any) -> None:
        super().__init__(**params)
        self._code = code
        self._jit_cache: dict[tuple, Any] = {}

    def _build_jit(self, cols: tuple) -> Any:
        import ast as _ast2
        import numba

        tree = _ast2.parse(self._code, mode="exec")

        param_assigns = []
        for k, v in self.params.items():
            assign = _ast2.parse(f"{k} = {v!r}", mode="exec").body[0]
            param_assigns.append(assign)

        # User code assigns to `result` (same convention as ScriptIndicator) but
        # never returns it — this is a real function, not an exec()'d namespace, so
        # the compiled body needs an explicit return or numba always yields None.
        return_result = _ast2.Return(value=_ast2.Name(id="result", ctx=_ast2.Load()))

        fn_def = _ast2.FunctionDef(
            name="_jit_fn",
            args=_ast2.arguments(
                posonlyargs=[],
                args=[_ast2.arg(arg=c) for c in cols],
                vararg=None, kwonlyargs=[], kw_defaults=[],
                kwarg=None, defaults=[],
            ),
            body=param_assigns + tree.body + [return_result],
            decorator_list=[],
            returns=None,
            lineno=1, col_offset=0,
        )
        _ast2.fix_missing_locations(fn_def)
        module = _ast2.Module(body=[fn_def], type_ignores=[])

        ns: dict = {"np": np}
        exec(compile(module, "<jit_indicator>", "exec"), ns)  # noqa: S102
        # cache=True requires a real on-disk source file to key numba's persistent
        # cache against; _jit_fn is built from a dynamically-generated AST with no
        # such file, which makes numba raise at call time. self._jit_cache above is
        # already this indicator's own (in-process) memoization of the compiled
        # dispatcher, so a persistent disk cache adds nothing here.
        return numba.jit(nopython=True, cache=False)(ns["_jit_fn"])

    def compute(self, ctx: IndicatorContext) -> np.ndarray:
        cols = tuple(ctx.df.columns)
        if cols not in self._jit_cache:
            self._jit_cache[cols] = self._build_jit(cols)
        arrays = [ctx.df[c].to_numpy() for c in cols]
        return np.asarray(self._jit_cache[cols](*arrays), dtype=np.float64)

    def required_periods(self) -> int:
        return int(self.params.get("period", self.params.get("length", 1)))

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        state["_jit_cache"] = {}
        return state


# ------------------------------------------------------------------ #
# CppIndicator — compiled C++ .so, executed via CppRunnerPool          #
# ------------------------------------------------------------------ #

class CppIndicator(Indicator):
    """
    C++ indicator plugin. Compiled .so stored as bytes.
    Execution routed through CppRunnerPool in the executor.

    ABI (extern "C"):
        void indicator_compute(
            const double* close, const double* high, const double* low,
            const double* open,  const double* volume,
            int length, double* result, const char* params_json)
    """

    def __init__(self, cpp_code: str, so_bytes: bytes, **params: Any) -> None:
        super().__init__(**params)
        self._cpp_code = cpp_code
        self._so_bytes = so_bytes

    def compute(self, ctx: IndicatorContext) -> np.ndarray:
        raise RuntimeError(
            "CppIndicator.compute() cannot be called directly. "
            "Use an executor with CppRunnerPool support."
        )

    def required_periods(self) -> int:
        return int(self.params.get("period", 1))

    def __getstate__(self) -> dict:
        return self.__dict__.copy()


__all__ = [
    "IndicatorContext",
    "Indicator",
    "IndicatorDeclaration",
    "ScriptIndicator",
    "JitIndicator",
    "CppIndicator",
    "ta_indicator",
    "_TA",
    "_ta_ns",
    # Factory
    "_extract_defaults",
    "_make_talib_indicator",
    "load_indicator_plugin",
]
