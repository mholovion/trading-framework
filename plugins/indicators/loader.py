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
_PRIMITIVES_DIR = _PROJECT_ROOT / "plugins" / "indicators" / "primitives"


def load_all_primitives() -> None:
    """Scan plugins/indicators/primitives/ and compile every .py file as a numba primitive."""
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
    Load plugins/indicators/primitives/NAME.py, wrap as (source, period) -> np.ndarray,
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

    Requires PLUGIN_RUNNER_URL env var — computation is delegated to the
    plugin-runner container to keep database credentials out of plugin code.
    """
    runner_url = os.getenv("PLUGIN_RUNNER_URL")
    if not runner_url:
        raise RuntimeError(
            "PLUGIN_RUNNER_URL is not configured — "
            "indicator execution requires the plugin-runner container"
        )
    from plugin_runner.client import RemoteIndicatorPlugin
    return RemoteIndicatorPlugin(name, config, runner_url)


def _load_locally(name: str, config: Dict[str, Any]) -> IndicatorPlugin:
    """
    Internal: load and execute an indicator in-process.

    Used by plugin_runner/server.py (which runs inside the sandboxed container).
    Not for direct use from the dashboard.

    Handles the special '__script__' type for inline code sent from the editor.

    Search order:
      1. __script__ with _code in config → inline _ScriptPlugin
      2. plugins/indicators/user/NAME.py — user uploads (checked first)
      3. plugins/indicators/NAME.py      — built-in class or script
      4. pandas_ta fallback
    """
    # Inline script from the custom indicator editor
    inline_code = config.get("parameters", config).get("_code") or config.get("_code")
    if name == "__script__" or inline_code:
        if not inline_code:
            raise ValueError("__script__ indicator requires '_code' in config")
        return _ScriptPlugin(inline_code, config, "<editor>")

    importlib.invalidate_caches()  # pick up newly uploaded files without restart

    for search_dir in (_INDICATORS_DIR / "user", _INDICATORS_DIR):
        plugin_path = search_dir / f"{name}.py"
        if not plugin_path.exists():
            continue
        code = plugin_path.read_text()
        if "class " in code:
            # Derive module path relative to project root for importlib
            rel = plugin_path.relative_to(_PROJECT_ROOT)
            module = ".".join(rel.with_suffix("").parts)
            mod = importlib.import_module(module)
            cls_name = f"{name.capitalize()}Plugin"
            return getattr(mod, cls_name)(config)
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
