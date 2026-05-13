#!/usr/bin/env python3
"""
Per-symbol process launcher.

Reads config/connections.yaml, collects all enabled unique symbols, and
spawns one MicroservicesOrchestratorV2 process per symbol.  Each child
process gets its own asyncio event loop and in-process queue — true CPU
parallelism for independent symbol pipelines.

Usage (docker-compose):
    command: python framework/launcher.py

Adding a new trading pair requires only one change in connections.yaml —
the launcher discovers it automatically on the next restart.
"""

import asyncio
import logging
import multiprocessing
import os
import sys
import yaml
from pathlib import Path


# ---------------------------------------------------------------------------
# Worker entry-point (runs in child process)
# ---------------------------------------------------------------------------

def _worker_process(symbol: str, config_dir: str) -> None:
    """Entry-point executed in a separate OS process for each symbol."""
    # Add framework directory to path so imports work
    framework_dir = Path(__file__).parent
    sys.path.insert(0, str(framework_dir))

    os.environ['TRADING_SYMBOL'] = symbol
    os.environ['CONFIG_DIR'] = config_dir

    # Lazy import after path setup
    from services.orchestrator import main  # noqa: PLC0415

    asyncio.run(main(config_dir=config_dir, symbol=symbol))


# ---------------------------------------------------------------------------
# Launcher
# ---------------------------------------------------------------------------

def _start_worker(symbol: str, config_dir: str) -> multiprocessing.Process:
    p = multiprocessing.Process(
        target=_worker_process,
        args=(symbol, config_dir),
        name=f"worker-{symbol}",
        daemon=False,
    )
    p.start()
    return p


async def _run_launcher(config_dir: str) -> None:
    logger = logging.getLogger("launcher")

    connections_path = Path(config_dir) / "connections.yaml"
    if not connections_path.exists():
        raise FileNotFoundError(f"connections.yaml not found: {connections_path}")

    with open(connections_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    symbols: list[str] = sorted({
        conn["symbol"]
        for conn in cfg.get("connections", {}).values()
        if conn.get("enabled") and conn.get("symbol")
    })

    if not symbols:
        raise ValueError("No enabled symbols found in connections.yaml")

    logger.info(f"Launching workers for symbols: {symbols}")

    processes: dict[str, multiprocessing.Process] = {
        symbol: _start_worker(symbol, config_dir) for symbol in symbols
    }

    while True:
        await asyncio.sleep(30)

        for symbol, proc in list(processes.items()):
            if not proc.is_alive():
                exit_code = proc.exitcode
                logger.warning(
                    f"Worker for {symbol} exited (exit_code={exit_code}), restarting..."
                )
                processes[symbol] = _start_worker(symbol, config_dir)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    )

    config_dir = os.environ.get("CONFIG_DIR", "/app/config")
    asyncio.run(_run_launcher(config_dir))


if __name__ == "__main__":
    # Required for multiprocessing on some platforms (macOS default is 'spawn')
    multiprocessing.set_start_method("spawn", force=True)
    main()
