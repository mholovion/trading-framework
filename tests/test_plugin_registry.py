"""tradingkit.core.plugin_registry — optional host-app builtin registries."""
from __future__ import annotations

from tradingkit.core.plugin_registry import get_builtin_registry


def test_returns_empty_dict_when_env_var_unset(monkeypatch):
    monkeypatch.delenv("TRADINGKIT_TEST_MODULE", raising=False)
    assert get_builtin_registry("TRADINGKIT_TEST_MODULE", "SOME_ATTR") == {}


def test_returns_empty_dict_when_module_not_importable(monkeypatch):
    monkeypatch.setenv("TRADINGKIT_TEST_MODULE", "no_such_module_xyz")
    assert get_builtin_registry("TRADINGKIT_TEST_MODULE", "SOME_ATTR") == {}


def test_returns_empty_dict_when_attr_missing(monkeypatch):
    # os.path exists as a real importable module but has no SOME_ATTR
    monkeypatch.setenv("TRADINGKIT_TEST_MODULE", "os.path")
    assert get_builtin_registry("TRADINGKIT_TEST_MODULE", "SOME_ATTR") == {}


def test_loads_attr_from_configured_module(monkeypatch):
    monkeypatch.setenv("TRADINGKIT_TEST_MODULE", "tests._fixtures.fake_registry")
    result = get_builtin_registry("TRADINGKIT_TEST_MODULE", "FAKE_BUILTINS")
    assert result == {"example": "code here"}
