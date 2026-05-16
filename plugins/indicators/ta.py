from __future__ import annotations
import numpy as np
from typing import Any

_primitive_registry: dict[str, Any] = {}


def _to_np(s) -> np.ndarray:
    return s.to_numpy() if hasattr(s, "to_numpy") else s


class TA:
    """
    Technical analysis namespace accessible as ctx.ta.

    Lookup order for ctx.ta.NAME():
      1. _primitive_registry  — numba-compiled custom primitives (e.g. rma)
      2. pandas_ta            — 130+ built-in indicators (auto numpy↔pandas conversion)

    Single-output functions return np.ndarray.
    Multi-output functions (MACD, BB, Stoch, …) return tuple[np.ndarray, ...].
    """

    def __getattr__(self, name: str):
        if name in _primitive_registry:
            return _primitive_registry[name]

        try:
            import pandas as pd
            import pandas_ta as pdta
        except ImportError as exc:
            raise AttributeError(
                f"pandas-ta not installed — cannot call ta.{name}(). "
                "Run: pip install pandas-ta"
            ) from exc

        pdta_func = getattr(pdta, name, None)
        if pdta_func is None:
            raise AttributeError(
                f"'{name}' not found in primitive registry or pandas_ta"
            )

        def wrapper(*args, **kwargs):
            import pandas as pd

            pd_args = [pd.Series(a) if isinstance(a, np.ndarray) else a for a in args]
            result = pdta_func(*pd_args, **kwargs)
            if isinstance(result, pd.Series):
                return _to_np(result)
            if isinstance(result, pd.DataFrame):
                return tuple(_to_np(result[c]) for c in result.columns)
            return result

        return wrapper


ta = TA()
