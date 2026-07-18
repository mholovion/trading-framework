"""
tradingkit.core.plugin_registry — optional host-app builtin registries.

The framework ships with zero built-in strategies/aggregations beyond what
user-authored plugin_library scripts provide. A host application can register
its own named builtins (e.g. a curated set of production strategies) by
pointing an environment variable at a module that exposes the expected dict —
see get_builtin_registry(). This is entirely optional: unset, the registry is
just empty and named-type lookups fall through to "not found".
"""
from __future__ import annotations

import importlib
import logging
import os

logger = logging.getLogger(__name__)


def get_builtin_registry(env_var: str, attr: str) -> dict:
    """
    Dynamically import `attr` from the module path named in env var `env_var`.

    Returns {} if the env var is unset, the module can't be imported, or it
    doesn't define `attr` -- a host app registering builtins is always
    optional, never a hard dependency of the framework.
    """
    module_path = os.environ.get(env_var)
    if not module_path:
        return {}
    try:
        module = importlib.import_module(module_path)
        return getattr(module, attr)
    except (ImportError, AttributeError) as exc:
        logger.warning("Could not load builtin registry %s from %r: %s", attr, module_path, exc)
        return {}