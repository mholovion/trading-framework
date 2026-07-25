"""
tradingkit.runner._safe_pickle — restricted unpickler for tradingkit-runner.

Plain pickle.loads() on network input is arbitrary code execution: a crafted payload's
REDUCE opcode can invoke any importable callable with attacker-controlled arguments
(classic gadget: module="os", name="system"). RemoteExecutor legitimately needs to ship
live Indicator/Strategy/DataSource instances over the wire, so pickle itself can't be
dropped — instead this restricts *which* classes/functions find_class() is allowed to
hand back to the unpickler.

Custom Indicator/Strategy/DataSource subclasses that live in *your own* application code
(see the subclassing example in the tradingkit top-level docstring) are not importable by
the runner unless you explicitly opt in via `trusted_modules=` / `--trust-module` — only
add modules you control, since the runner will import them and treat their classes with
the same trust as tradingkit's own.

Honest limitation: this is a targeted allowlist against the well-known pickle RCE gadgets
(os/subprocess/eval-style callables), not a full sandbox. It does not by itself make the
runner safe to expose on the public internet — see the "Security model" section of the
top-level README. Pair it with the mandatory auth token and keep the runner on a private
network.
"""
from __future__ import annotations

import io
import pickle
from collections.abc import Iterable
from typing import Any

# ---------------------------------------------------------------------------
# TRUSTED_DATA_PLANE_PACKAGES — the biggest lever in this file.
#
# BarContext carries pl.Series (see tradingkit.pipeline / tradingkit.backtest.runner),
# so the unpickler must be able to reconstruct polars/numpy/pyarrow objects, not just
# tradingkit's own classes. These three are first-party pinned dependencies of
# tradingkit itself (not attacker-supplied code), so every class/function in them is
# trusted — this is what makes RemoteExecutor's Strategy/BarContext path work at all.
#
# If you vendor or swap out a data library, add its top-level package name here.
# Anything NOT covered by this list or by ALLOWED_EXACT_MODULES below (e.g. os,
# subprocess, builtins.eval) is rejected.
# ---------------------------------------------------------------------------
TRUSTED_DATA_PLANE_PACKAGES = ("polars", "numpy", "pyarrow")

_SAFE_BUILTINS = {
    "dict", "list", "tuple", "set", "frozenset",
    "str", "bytes", "bytearray", "int", "float", "bool", "complex",
}

_ALLOWED_EXACT_MODULES: dict[str, set[str] | None] = {
    "tradingkit.indicator": None,   # None = every class in the module is allowed
    "tradingkit.strategy":  None,
    "tradingkit.source":    None,
    "builtins":             _SAFE_BUILTINS,
    "collections":          {"OrderedDict", "defaultdict"},
}


class UnsafeUnpicklingError(pickle.UnpicklingError):
    """Raised when a payload references a class/module outside the tradingkit-runner allowlist."""


def _is_trusted_package(module: str, package: str) -> bool:
    return module == package or module.startswith(package + ".")


class _RunnerUnpickler(pickle.Unpickler):
    def __init__(self, file: Any, *, trusted_modules: Iterable[str] = ()) -> None:
        super().__init__(file)
        self._trusted_modules = set(trusted_modules)

    def find_class(self, module: str, name: str) -> Any:
        if module in self._trusted_modules:
            return super().find_class(module, name)

        if module in _ALLOWED_EXACT_MODULES:
            allowed_names = _ALLOWED_EXACT_MODULES[module]
            if allowed_names is None or name in allowed_names:
                return super().find_class(module, name)

        if any(_is_trusted_package(module, pkg) for pkg in TRUSTED_DATA_PLANE_PACKAGES):
            return super().find_class(module, name)

        raise UnsafeUnpicklingError(
            f"Refused to unpickle {module}.{name}: not in the tradingkit-runner "
            f"allowlist. If this is your own trusted plugin code, pass "
            f"--trust-module {module} (or trusted_modules=[...]) to the runner."
        )


def loads(data: bytes, *, trusted_modules: Iterable[str] = ()) -> Any:
    return _RunnerUnpickler(io.BytesIO(data), trusted_modules=trusted_modules).load()


__all__ = ["TRUSTED_DATA_PLANE_PACKAGES", "UnsafeUnpicklingError", "loads"]