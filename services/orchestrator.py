#!/usr/bin/env python3
"""
Microservices Orchestrator v2 (ClickHouse edition)
"""

import asyncio
import logging
import signal
import sys
import os
import importlib
from typing import Dict, Any, Optional
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.universal_config_manager import UniversalConfigManager
from core.clickhouse import ClickHouseManager, create_clickhouse_manager
from core.logging_config import get_orchestrator_logger
from core.exceptions import ConfigurationError
from core.queue_client import InProcessQueueClient
from services.historical_data_service import HistoricalDataService
from services.realtime_data_service import RealtimeDataService
from services.database_update_service import DatabaseUpdateService
from services.gap_recovery_service import GapRecoveryService


class Orchestrator:

    def __init__(self):
        self.logger = get_orchestrator_logger()

        self.config_manager:  Optional[UniversalConfigManager] = None
        self.clickhouse:      Optional[ClickHouseManager]      = None
        self.queue_client:    Optional[InProcessQueueClient]   = None
        self.symbol_filter:   Optional[str]                    = None

        self.exchange_plugins: Dict[str, Any]           = {}
        self.services:         Dict[str, Any]           = {}
        self.service_tasks:    Dict[str, asyncio.Task]  = {}

        self.running              = False
        self.shutdown_in_progress = False
        self.start_time           = datetime.now(timezone.utc)

        self.stats = {
            'services_started': 0,
            'services_running': 0,
            'services_failed':  0,
            'total_restarts':   0,
        }

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def initialize(self):
        self.logger.info("Initializing orchestrator...")
        await self._load_config()
        await self._initialize_clickhouse()
        await self._initialize_queue()
        await self._load_exchange_plugins()
        await self._initialize_services()
        self.logger.info("Orchestrator initialized")

    async def _load_config(self):
        self.config_manager = UniversalConfigManager()
        self.config_manager.load_all_configs(symbol_filter=self.symbol_filter)
        self.logger.info("Configuration loaded")

    async def _initialize_clickhouse(self):
        self.logger.info("Connecting to ClickHouse...")
        ch_cfg = self.config_manager.get_config('main').get('clickhouse', {})
        self.clickhouse = create_clickhouse_manager(ch_cfg)
        await self.clickhouse.initialize()
        self.logger.info("ClickHouse connected")

    async def _initialize_queue(self):
        self.queue_client = InProcessQueueClient()
        await self.queue_client.connect()
        self.logger.info("In-process queue initialized")

    async def _load_exchange_plugins(self):
        connections_config = self.config_manager.get_config('connections')
        main_config        = self.config_manager.get_config('main')

        for conn_name, conn_cfg in connections_config.get('connections', {}).items():
            if not conn_cfg.get('enabled', False):
                continue
            exch = conn_cfg['exchange']
            if exch not in main_config['exchanges']:
                continue
            exch_cfg = main_config['exchanges'][exch]
            if not exch_cfg.get('enabled', False):
                continue

            merged = {**exch_cfg, **conn_cfg}
            plugin_name = merged['plugin']
            try:
                mod = importlib.import_module(f'plugins.exchanges.{plugin_name}_plugin')
                cls = getattr(mod, f'{plugin_name.title()}Plugin')
                plugin = cls(merged)
                await plugin.initialize()
                await plugin.sync_server_time()
                self.exchange_plugins[conn_name] = plugin
                self.logger.info(f"Loaded exchange plugin: {conn_name} ({plugin_name})")
            except Exception as e:
                self.logger.error(f"Failed to load plugin {conn_name}: {e}")
                raise ConfigurationError(f"Failed to load plugin {conn_name}: {e}")

        self.logger.info(f"Loaded {len(self.exchange_plugins)} exchange plugins")

    async def _initialize_services(self):
        self.logger.info("Initializing services...")

        self.services['historical_data'] = HistoricalDataService(
            self.config_manager, self.clickhouse, self.queue_client
        )
        self.services['realtime_data'] = RealtimeDataService(
            self.config_manager, self.clickhouse, self.queue_client, self.exchange_plugins
        )
        self.services['database_update'] = DatabaseUpdateService(
            self.config_manager, self.clickhouse, self.queue_client
        )

        gap_svc = GapRecoveryService(self.config_manager, self.clickhouse, self.queue_client)
        for conn_name, plugin in self.exchange_plugins.items():
            gap_svc.register_exchange_plugin(conn_name, plugin)
        self.services['gap_recovery'] = gap_svc

        # Services that are nice-to-have but not required for live data collection
        _optional_services = {'historical_data', 'gap_recovery'}

        for svc_name, svc in self.services.items():
            try:
                await svc.initialize()
                self.logger.info(f"  {svc_name} initialized")
            except Exception as e:
                import traceback
                if svc_name in _optional_services:
                    self.logger.warning(f"Optional service {svc_name} failed to initialize "
                                        f"(will retry later): {e}")
                else:
                    self.logger.error(f"Failed to initialize {svc_name}: {e}")
                    self.logger.error(traceback.format_exc())
                    raise

        self.logger.info("All services initialized")

    # ------------------------------------------------------------------ #
    # Start / stop                                                         #
    # ------------------------------------------------------------------ #

    async def start_services(self):
        self.running = True

        await self._start_service('database_update')
        await self._start_service('gap_recovery')

        main_config    = self.config_manager.get_config('main')
        execution_mode = main_config.get('execution', {}).get('mode', 'full')
        self.logger.info(f"Execution mode: {execution_mode}")

        if execution_mode in ['full', 'historical_only']:
            await self._start_service('historical_data')
        if execution_mode in ['full', 'realtime_only']:
            await self._start_service('realtime_data')

        self.stats['services_started'] = len(self.service_tasks)
        self.stats['services_running'] = len(self.service_tasks)
        self.logger.info(f"Started {len(self.service_tasks)} services")

        self.service_tasks['_monitor'] = asyncio.create_task(self._monitor_services())

    async def _start_service(self, service_name: str):
        if service_name not in self.services:
            raise ValueError(f"Unknown service: {service_name}")

        service = self.services[service_name]

        async def wrapper():
            try:
                self.logger.info(f"Starting {service_name}...")
                if service_name == 'historical_data':
                    await service.start()
                elif service_name == 'realtime_data':
                    await service.start_all_realtime_streams()
                    while self.running:
                        await asyncio.sleep(1)
                elif service_name == 'gap_recovery':
                    await service.start()
                    if service.gap_check_task:
                        await service.gap_check_task
                else:
                    await service.start()
            except asyncio.CancelledError:
                self.logger.info(f"{service_name} cancelled")
            except Exception as e:
                self.logger.error(f"{service_name} failed: {e}")
                self.stats['services_failed'] += 1
                self.stats['services_running'] -= 1
                if not self.shutdown_in_progress:
                    await asyncio.sleep(5)
                    await self._start_service(service_name)
                    self.stats['total_restarts'] += 1

        self.service_tasks[service_name] = asyncio.create_task(wrapper())
        self.logger.info(f"  {service_name} started")

    async def stop_service(self, service_name: str):
        if service_name not in self.service_tasks:
            return
        task = self.service_tasks[service_name]
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        svc = self.services.get(service_name)
        if svc and hasattr(svc, 'cleanup'):
            await svc.cleanup()
        del self.service_tasks[service_name]

    async def stop_all_services(self):
        self.shutdown_in_progress = True
        self.running = False
        for name in reversed(list(self.service_tasks.keys())):
            await self.stop_service(name)

    async def restart_service(self, service_name: str):
        if service_name in self.service_tasks:
            await self.stop_service(service_name)
        await self._start_service(service_name)
        self.stats['total_restarts'] += 1

    # ------------------------------------------------------------------ #
    # Monitoring                                                           #
    # ------------------------------------------------------------------ #

    async def _monitor_services(self):
        tick = 0
        while self.running:
            try:
                await asyncio.sleep(60)
                tick += 1
                if not self.shutdown_in_progress:
                    await self._check_service_health()
                    if tick % 5 == 0:
                        await self._log_system_statistics()
                    await self._log_pipeline_progress()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Monitor error: {e}")
                await asyncio.sleep(60)

    async def _check_service_health(self):
        running = [n for n, t in self.service_tasks.items()
                   if not n.startswith('_') and not t.done()]
        self.stats['services_running'] = len(running)

    async def _log_system_statistics(self):
        uptime = datetime.now(timezone.utc) - self.start_time
        self.logger.info(f"Uptime: {uptime} | started={self.stats['services_started']} "
                         f"running={self.stats['services_running']} "
                         f"failed={self.stats['services_failed']}")

    async def _log_pipeline_progress(self):
        """Log candle + strategy counts from ClickHouse."""
        try:
            # Candle counts
            rows = await self.clickhouse._execute(
                "SELECT exchange, symbol, timeframe, count() AS cnt "
                "FROM candles GROUP BY exchange, symbol, timeframe"
            )
            cmap = {(r[0], r[1], r[2]): int(r[3]) for r in rows}

            # Strategy signal counts
            srows = await self.clickhouse._execute(
                "SELECT strategy_type, count() AS cnt FROM strategy_signals "
                "GROUP BY strategy_type"
            )
            smap = {r[0]: int(r[1]) for r in srows}

            lines = ['─── Pipeline Progress ───────────────────────────────────']
            connections_cfg = self.config_manager.get_config('connections').get('connections', {})

            for _, conn_cfg in connections_cfg.items():
                if not conn_cfg.get('enabled', False):
                    continue
                ex  = conn_cfg.get('exchange', '')
                sym = conn_cfg.get('symbol', '')
                tf  = conn_cfg.get('source_timeframe', '1m')
                cnt = cmap.get((ex, sym, tf), 0)
                lines.append(f'  CANDLES  {ex}/{sym}/{tf}: {cnt:,}')

            for strat, cnt in smap.items():
                lines.append(f'  SIGNALS  {strat}: {cnt:,}')

            lines.append('─' * 60)
            for line in lines:
                self.logger.info(line)
        except Exception as e:
            self.logger.debug(f"Progress log error: {e}")

    # ------------------------------------------------------------------ #
    # Run / shutdown                                                       #
    # ------------------------------------------------------------------ #

    async def get_system_status(self) -> Dict[str, Any]:
        uptime = datetime.now(timezone.utc) - self.start_time
        return {
            'uptime_seconds': uptime.total_seconds(),
            'running': self.running,
            'shutdown_in_progress': self.shutdown_in_progress,
            'statistics': self.stats,
            'services': {
                n: {'running': not t.done()}
                for n, t in self.service_tasks.items()
                if not n.startswith('_')
            },
            'clickhouse_connected': self.clickhouse and self.clickhouse._conn is not None,
        }

    async def run(self):
        self.logger.info("Starting Microservices Orchestrator v2...")
        signal.signal(signal.SIGINT,  lambda *_: asyncio.create_task(self.shutdown()))
        signal.signal(signal.SIGTERM, lambda *_: asyncio.create_task(self.shutdown()))

        await self.start_services()
        self.logger.info("All services started. Running...")

        while self.running:
            await asyncio.sleep(1)

    async def shutdown(self):
        self.logger.info("Graceful shutdown initiated...")
        await self.stop_all_services()
        for plugin in self.exchange_plugins.values():
            try:
                await plugin.cleanup()
            except Exception:
                pass
        if self.queue_client:
            await self.queue_client.disconnect()
        if self.clickhouse:
            await self.clickhouse.close()
        self.running = False
        self.logger.info("Shutdown complete")


async def main(config_dir: str = None, symbol: str = None):
    import argparse

    logger = get_orchestrator_logger()

    if config_dir is None and symbol is None:
        parser = argparse.ArgumentParser()
        parser.add_argument('--config-dir', default=None)
        parser.add_argument('--symbol', default=None)
        args, _ = parser.parse_known_args()
        config_dir = args.config_dir
        symbol     = args.symbol or os.getenv('TRADING_SYMBOL')

    orchestrator = None
    try:
        orchestrator = Orchestrator()
        orchestrator.symbol_filter = symbol
        if config_dir:
            os.environ.setdefault('CONFIG_DIR_OVERRIDE', config_dir)
        await orchestrator.initialize()
        await orchestrator.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
    except Exception as e:
        logger.error(f"Orchestrator failed: {e}")
        return 1
    finally:
        if orchestrator:
            await orchestrator.shutdown()

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
