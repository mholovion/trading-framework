from __future__ import annotations

import os

from plugins.strategies.base import StrategyPlugin


def load_strategy_plugin(name: str, config: dict) -> StrategyPlugin:
    runner_url = os.getenv("PLUGIN_RUNNER_URL")
    if not runner_url:
        raise RuntimeError(
            "PLUGIN_RUNNER_URL is not configured — "
            "strategy execution requires the plugin-runner container"
        )
    from plugin_runner.client import RemoteStrategyPlugin
    return RemoteStrategyPlugin(name, config, runner_url)
