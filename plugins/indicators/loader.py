from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Dict, Any

import numpy as np

from plugins.indicators.ta import ta, _primitive_registry
from plugins.indicators.context import IndicatorContext
from plugins.indicators.base import IndicatorPlugin

_PROJECT_ROOT = Path(__file__).parents[3]
_INDICATORS_DIR = _PROJECT_ROOT / "plugins" / "indicators"
_PRIMITIVES_DIR = _PROJECT_ROOT / "plugins" / "ta_primitives"


def load_all_primitives() -> None:
    """Scan plugins/ta_primitives/ and compile every .py file as a numba primitive."""
    if not _PRIMITIVES_DIR.exists():
        return
    for path in sorted(_PRIMITIVES_DIR.glob("*.py")):
        if path.stem.startswith("_"):
            continue
        try:
            load_primitive(path.stem)
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning(
                f"Failed to load primitive '{path.stem}': {exc}"
            )


def load_primitive(name: str) -> Any:
    """
    Load plugins/ta_primitives/NAME.py, wrap as (source, period) -> np.ndarray,
    compile with numba @njit, register as ctx.ta.NAME().
    """
    from numba import njit

    path = _PRIMITIVES_DIR / f"{name}.py"
    code = path.read_text()

    body_lines = "\n".join(
        f"    {line}" for line in code.splitlines() if line.strip()
    )
    func_src = f"def _{name}(source, period):\n{body_lines}\n    return result"
    ns: dict = {"np": np}
    exec(compile(func_src, str(path), "exec"), ns)  # noqa: S102
    jitted = njit(ns[f"_{name}"])
    _primitive_registry[name] = jitted
    return jitted


def load_indicator_plugin(name: str, config: Dict[str, Any]) -> IndicatorPlugin:
    """
    Load an indicator plugin by name.

    Search order:
      1. plugins/indicators/NAME.py containing a class  → import and instantiate
      2. plugins/indicators/NAME.py body-only script     → _ScriptPlugin
      3. pandas_ta fallback                              → _make_ta_plugin
    """
    plugin_path = _INDICATORS_DIR / f"{name}.py"

    if plugin_path.exists():
        code = plugin_path.read_text()
        if "class " in code:
            mod = importlib.import_module(f"plugins.indicators.{name}")
            cls_name = f"{name.capitalize()}Plugin"
            plugin_cls = getattr(mod, cls_name)
            return plugin_cls(config)
        return _ScriptPlugin(code, config, str(plugin_path))

    return _make_ta_plugin(name, config)


def _make_ta_plugin(ta_func_name: str, config: Dict[str, Any]) -> "_ScriptPlugin":
    script = f"result = ta.{ta_func_name}(close, period)"
    return _ScriptPlugin(script, config, f"<ta:{ta_func_name}>")


class _ScriptPlugin(IndicatorPlugin):
    """
    Wraps a body-only indicator script as an IndicatorPlugin.

    The script is executed with a pre-built namespace:
      np, ta, open, high, low, close, volume, ts, **config.parameters

    The script must assign its result series to the variable `result`.
    """

    def __init__(self, code: str, config: Dict[str, Any], source_label: str = "<script>") -> None:
        super().__init__(config)
        self._code = compile(code, source_label, "exec")

    def compute(self, ctx: IndicatorContext) -> np.ndarray:
        ns: dict = {
            "np":     np,
            "ta":     ta,
            "open":   ctx.open,
            "high":   ctx.high,
            "low":    ctx.low,
            "close":  ctx.close,
            "volume": ctx.volume,
            "ts":     ctx.ts,
        }
        ns.update(self.parameters)
        exec(self._code, ns)  # noqa: S102
        result = ns.get("result")
        if result is None:
            raise RuntimeError(
                f"Indicator script did not assign to 'result'. "
                f"Source: {self._code.co_filename}"
            )
        return np.asarray(result, dtype=np.float64)

    def get_required_periods(self) -> int:
        warmup = self.parameters.get("warmup")
        if warmup is not None:
            return int(warmup)
        period = self.parameters.get("period", 14)
        return int(period) * 2
