"""tradingkit.runner._safe_pickle — allowlisting unpickler."""
from __future__ import annotations

import os
import pickle
import sys
import types

import polars as pl
import pytest

from tradingkit.indicator import ScriptIndicator
from tradingkit.runner import _safe_pickle
from tradingkit.strategy import BarContext, Signal


def test_roundtrip_script_indicator():
    ind = ScriptIndicator(code="result = close * 2", period=14)
    restored = _safe_pickle.loads(pickle.dumps(ind))
    assert isinstance(restored, ScriptIndicator)
    assert restored._code == ind._code
    assert restored.period == 14


def test_roundtrip_signal():
    sig = Signal("buy", 0.8, metadata={"direction": "long"})
    restored = _safe_pickle.loads(pickle.dumps(sig))
    assert restored.type == "buy"
    assert restored.direction == "long"


def test_roundtrip_bar_context_with_polars_series():
    bar = BarContext(
        row={"timestamp": 1, "close": 42.0},
        indicators={"rsi": 55.0},
        series={"rsi": pl.Series("rsi", [1.0, 2.0, 3.0])},
    )
    restored = _safe_pickle.loads(pickle.dumps(bar))
    assert restored.rsi == 55.0
    assert list(restored.series("rsi")) == [1.0, 2.0, 3.0]


@pytest.mark.parametrize("gadget", [
    pytest.param(lambda: (os.system, ("echo pwned",)), id="os.system"),
    pytest.param(lambda: (__import__("subprocess").Popen, (["echo", "pwned"],)), id="subprocess.Popen"),
    pytest.param(lambda: (eval, ("1+1",)), id="builtins.eval"),
    pytest.param(lambda: (exec, ("pass",)), id="builtins.exec"),
    pytest.param(lambda: (open, ("/etc/passwd",)), id="builtins.open"),
])
def test_classic_pickle_gadgets_are_blocked(gadget):
    class Evil:
        def __reduce__(self):
            return gadget()

    with pytest.raises(_safe_pickle.UnsafeUnpicklingError):
        _safe_pickle.loads(pickle.dumps(Evil()))


def test_gadget_does_not_actually_execute(tmp_path):
    marker = tmp_path / "pwned"

    class Evil:
        def __reduce__(self):
            return (os.system, (f"echo pwned > {marker}",))

    with pytest.raises(_safe_pickle.UnsafeUnpicklingError):
        _safe_pickle.loads(pickle.dumps(Evil()))
    assert not marker.exists()


def test_untrusted_custom_module_blocked_by_default():
    parent = types.ModuleType("myapp")
    mod = types.ModuleType("myapp.indicators")

    class MyCustomIndicator:
        pass
    MyCustomIndicator.__module__ = "myapp.indicators"
    MyCustomIndicator.__qualname__ = "MyCustomIndicator"
    mod.MyCustomIndicator = MyCustomIndicator
    sys.modules["myapp"] = parent
    sys.modules["myapp.indicators"] = mod
    try:
        data = pickle.dumps(MyCustomIndicator())
        with pytest.raises(_safe_pickle.UnsafeUnpicklingError):
            _safe_pickle.loads(data)

        restored = _safe_pickle.loads(data, trusted_modules=["myapp.indicators"])
        assert isinstance(restored, MyCustomIndicator)
    finally:
        del sys.modules["myapp"]
        del sys.modules["myapp.indicators"]