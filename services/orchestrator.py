#!/usr/bin/env python3
"""
Microservices Orchestrator v2
"""

import asyncio
import logging
import signal
import sys
import os
import importlib
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import json

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.universal_config_manager import UniversalConfigManager
from core.database import DatabaseManager
from core.logging_config import get_orchestrator_logger
from core.exceptions import ConfigurationError
from core.queue_client import InProcessQueueClient
from services.historical_data_service import HistoricalDataService
from services.realtime_data_service import RealtimeDataService
from services.database_update_service import DatabaseUpdateService
from services.indicators_reactive_service import IndicatorsReactiveService
from services.strategies_reactive_service import StrategiesReactiveService
from services.gap_recovery_service import GapRecoveryService
from services.indicators_gap_service import IndicatorsGapService
from services.strategies_gap_service import StrategiesGapService


class MicroservicesOrchestratorV2:
    """
    Enhanced orchestrator for distributed microservices architecture
    """
    
    def __init__(self):
        self.logger = get_orchestrator_logger()
        
        # Core components
        self.config_manager: Optional[UniversalConfigManager] = None
        self.database_manager: Optional[DatabaseManager] = None
        self.queue_client: Optional[InProcessQueueClient] = None
        self.symbol_filter: Optional[str] = None
        
        # Microservices
        self.services: Dict[str, Any] = {}
        self.service_tasks: Dict[str, asyncio.Task] = {}
        
        # Exchange plugins (shared across services)
        self.exchange_plugins: Dict[str, Any] = {}
        
        # State management
        self.running = False
        self.shutdown_in_progress = False
        
        # Statistics
        self.start_time = datetime.now(timezone.utc)
        self.stats = {
            'services_started': 0,
            'services_running': 0,
            'services_failed': 0,
            'total_restarts': 0
        }
    
    async def initialize(self):
        """Initialize all core components and services"""
        self.logger.info("Initializing Microservices Orchestrator v2...")
        
        try:
            # Initialize configuration
            await self._initialize_config()
            
            # Initialize database
            await self._initialize_database()
            
            # Initialize message queue
            await self._initialize_queue()
            
            # Load exchange plugins
            await self._load_exchange_plugins()
            
            # Initialize microservices
            await self._initialize_services()
            
            self.logger.info("Microservices Orchestrator v2 initialized successfully")
            
        except Exception as e:
            import traceback
            self.logger.error(f"Failed to initialize orchestrator: {e}")
            self.logger.error(f"Full traceback:\n{traceback.format_exc()}")
            raise
    
    async def _initialize_config(self):
        """Initialize configuration manager"""
        self.logger.info("Initializing configuration...")

        self.config_manager = UniversalConfigManager()
        self.config_manager.load_all_configs(symbol_filter=self.symbol_filter)

        self.logger.info("Configuration loaded successfully")
    
    async def _initialize_database(self):
        """Initialize centralized database manager"""
        self.logger.info("Initializing database manager...")
        
        # Validate required environment variables
        required_env_vars = ['DATABASE_HOST', 'DATABASE_PORT', 'DATABASE_NAME', 'DATABASE_USER', 'DATABASE_PASSWORD']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        db_config = {
            'database': {
                'host': os.getenv('DATABASE_HOST'),
                'port': int(os.getenv('DATABASE_PORT')),
                'name': os.getenv('DATABASE_NAME'),
                'user': os.getenv('DATABASE_USER'),
                'password': os.getenv('DATABASE_PASSWORD'),
                'connection_pool_size': int(os.getenv('DATABASE_POOL_SIZE', '50')),
                'query_timeout': int(os.getenv('DATABASE_TIMEOUT', '30'))
            }
        }
        
        agg_rules = (
            self.config_manager.get_config('aggregation')
            .get('aggregation', {})
            .get('timeframe_rules', [])
        )
        self.database_manager = DatabaseManager(db_config, aggregation_rules=agg_rules)
        self.logger.info("Database manager initialized successfully")
    
    async def _initialize_queue(self):
        """Initialize in-process message queue."""
        self.logger.info("Initializing in-process message queue...")
        self.queue_client = InProcessQueueClient()
        await self.queue_client.connect()
        self.logger.info("In-process message queue initialized")
    
    async def _load_exchange_plugins(self):
        """Load and initialize exchange plugins for all connections"""
        self.logger.info("Loading exchange plugins...")
        
        connections_config = self.config_manager.get_config('connections')
        main_config = self.config_manager.get_config('main')
        
        for connection_name, connection_config in connections_config.get('connections', {}).items():
            if not connection_config.get('enabled', False):
                continue
                
            exchange_name = connection_config['exchange']
            if exchange_name not in main_config['exchanges']:
                continue
                
            exchange_config = main_config['exchanges'][exchange_name]
            if not exchange_config.get('enabled', False):
                continue
            
            # Merge configs
            merged_config = {**exchange_config, **connection_config}
            plugin_name = merged_config['plugin']
            
            try:
                # Dynamic import of exchange plugin
                plugin_module = importlib.import_module(f'plugins.exchanges.{plugin_name}_plugin')
                plugin_class = getattr(plugin_module, f'{plugin_name.title()}Plugin')
                
                # Initialize plugin
                plugin_instance = plugin_class(merged_config)
                await plugin_instance.initialize()
                
                # Sync server time
                await plugin_instance.sync_server_time()
                
                self.exchange_plugins[connection_name] = plugin_instance
                
                self.logger.info(f"Loaded exchange plugin: {connection_name} ({plugin_name})")
                
            except Exception as e:
                self.logger.error(f"Failed to load exchange plugin {connection_name}: {e}")
                raise ConfigurationError(f"Failed to load exchange plugin {connection_name}: {e}")
        
        self.logger.info(f"Loaded {len(self.exchange_plugins)} exchange plugins")
    
    async def _initialize_services(self):
        """Initialize all microservices"""
        self.logger.info("Initializing microservices...")
        
        # Historical Data Service
        self.services['historical_data'] = HistoricalDataService(
            self.config_manager, 
            self.database_manager,
            self.queue_client
        )
        
        # Real-time Data Service (with exchange plugins)
        self.services['realtime_data'] = RealtimeDataService(
            self.config_manager,
            self.database_manager,
            self.queue_client,
            self.exchange_plugins
        )
        
        # Database Update Service
        self.services['database_update'] = DatabaseUpdateService(
            self.config_manager,
            self.database_manager,
            self.queue_client
        )
        
        # Indicators Reactive Service
        self.services['indicators'] = IndicatorsReactiveService(
            self.config_manager,
            self.database_manager,
            self.queue_client
        )
        
        # Strategies Reactive Service
        self.services['strategies'] = StrategiesReactiveService(
            self.config_manager,
            self.database_manager,
            self.queue_client
        )
        
        # Gap Recovery Service (with exchange plugins access)
        gap_recovery_service = GapRecoveryService(
            self.config_manager,
            self.database_manager,
            self.queue_client
        )
        # Pass exchange plugins to gap recovery service
        for connection_name, plugin in self.exchange_plugins.items():
            gap_recovery_service.register_exchange_plugin(connection_name, plugin)
        self.services['gap_recovery'] = gap_recovery_service

        # Indicators Gap Service
        self.services['indicators_gap'] = IndicatorsGapService(
            self.config_manager,
            self.database_manager,
            self.queue_client
        )
        
        # Strategies Gap Service
        self.services['strategies_gap'] = StrategiesGapService(
            self.config_manager,
            self.database_manager,
            self.queue_client
        )
        
        # Initialize each service
        for service_name, service in self.services.items():
            try:
                self.logger.info(f"Initializing {service_name} service...")
                await service.initialize()
                self.logger.info(f" {service_name} service initialized")
            except Exception as e:
                import traceback
                self.logger.error(f"Failed to initialize {service_name} service: {e}")
                self.logger.error(f"Full traceback for {service_name}:\n{traceback.format_exc()}")
                raise
        
        self.logger.info("All microservices initialized successfully")
    
    async def start_services(self):
        """Start all microservices"""
        self.logger.info("Starting all microservices...")
        
        self.running = True
        
        # Start Database Update Service first (it processes messages from other services)
        await self._start_service('database_update')
        
        # Start Indicators and Strategies services (they process messages from database service)
        await self._start_service('indicators')
        await self._start_service('strategies')
        
        # Start Gap Recovery Service (monitors and fills data gaps)
        await self._start_service('gap_recovery')

        # Start data collection services
        main_config = self.config_manager.get_config('main')
        execution_mode = main_config.get('execution', {}).get('mode', 'full')
        self.logger.info(f"Execution mode: {execution_mode}")
        
        if execution_mode in ['full', 'historical_only']:
            self.logger.info("Starting historical data service...")
            await self._start_service('historical_data')
        
        if execution_mode in ['full', 'realtime_only']:
            self.logger.info("Starting realtime data service...")
            await self._start_service('realtime_data')
        
        self.stats['services_started'] = len(self.service_tasks)
        self.stats['services_running'] = len(self.service_tasks)
        
        self.logger.info(f"Started {len(self.service_tasks)} microservices")
        
        # Start monitoring
        monitoring_task = asyncio.create_task(self._monitor_services())
        self.service_tasks['_monitor'] = monitoring_task
    
    async def _start_service(self, service_name: str):
        """Start individual service"""
        if service_name not in self.services:
            raise ValueError(f"Unknown service: {service_name}")
        
        service = self.services[service_name]
        
        async def service_wrapper():
            try:
                self.logger.info(f"Starting {service_name} service...")
                
                if service_name == 'historical_data':
                    await service.start()
                elif service_name == 'realtime_data':
                    await service.start_all_realtime_streams()
                    # Keep realtime service running
                    while self.running:
                        await asyncio.sleep(1)
                elif service_name == 'gap_recovery':
                    # Gap recovery is now controlled by orchestrator, just keep service alive
                    while self.running:
                        await asyncio.sleep(1)
                elif service_name == 'indicators_gap':
                    # Indicators gap is now controlled by orchestrator, just keep service alive
                    while self.running:
                        await asyncio.sleep(1)
                elif service_name == 'strategies_gap':
                    # Strategies gap is now controlled by orchestrator, just keep service alive
                    while self.running:
                        await asyncio.sleep(1)
                else:
                    await service.start()
                    
            except asyncio.CancelledError:
                self.logger.info(f"Service {service_name} cancelled")
            except Exception as e:
                self.logger.error(f"Service {service_name} failed: {e}")
                self.stats['services_failed'] += 1
                self.stats['services_running'] -= 1
                
                # Attempt restart if not shutting down
                if not self.shutdown_in_progress:
                    self.logger.info(f"Attempting to restart {service_name} service...")
                    await asyncio.sleep(5)  # Wait before restart
                    await self._start_service(service_name)
                    self.stats['total_restarts'] += 1
        
        task = asyncio.create_task(service_wrapper())
        self.service_tasks[service_name] = task
        
        self.logger.info(f" {service_name} service started")
    
    async def _monitor_services(self):
        """Monitor service health and provide statistics"""
        self.logger.info("Starting services monitoring...")
        _progress_tick = 0

        while self.running:
            try:
                await asyncio.sleep(60)
                _progress_tick += 1

                if not self.shutdown_in_progress:
                    await self._check_service_health()
                    # Full stats every 5 min; progress every minute
                    if _progress_tick % 5 == 0:
                        await self._log_system_statistics()
                    await self._log_pipeline_progress()
                
            except asyncio.CancelledError:
                self.logger.info("Services monitoring cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in services monitoring: {e}")
                await asyncio.sleep(60)
    
    async def _check_service_health(self):
        """Check health of all services"""
        running_services = []
        failed_services = []
        
        for service_name, task in self.service_tasks.items():
            if service_name.startswith('_'):  # Skip internal tasks
                continue
                
            if task.done():
                failed_services.append(service_name)
            else:
                running_services.append(service_name)
        
        self.stats['services_running'] = len(running_services)
        
        if failed_services:
            self.logger.warning(f"Failed services detected: {failed_services}")
    
    async def _log_system_statistics(self):
        """Log system-wide statistics"""
        uptime = datetime.now(timezone.utc) - self.start_time
        
        self.logger.info("System Statistics:")
        self.logger.info(f"Uptime: {uptime}")
        self.logger.info(f"Services started: {self.stats['services_started']}")
        self.logger.info(f"Services running: {self.stats['services_running']}")
        self.logger.info(f"Services failed: {self.stats['services_failed']}")
        self.logger.info(f"Total restarts: {self.stats['total_restarts']}")
        
        # Get individual service statistics
        for service_name, service in self.services.items():
            if hasattr(service, 'get_statistics'):
                try:
                    service_stats = await service.get_statistics()
                    self.logger.info(f" {service_name}: {json.dumps(service_stats, indent=2)}")
                except Exception as e:
                    self.logger.debug(f"Could not get statistics for {service_name}: {e}")
    
    async def _log_pipeline_progress(self):
        """Log compact pipeline progress every monitor cycle."""
        try:
            from sqlalchemy import text

            indicators_cfg  = self.config_manager.get_config('indicators').get('indicators', {})
            strategies_cfg  = self.config_manager.get_config('strategies').get('strategies', {})
            connections_cfg = self.config_manager.get_config('connections').get('connections', {})
            tf_sec = {'1m': 60, '5m': 300, '1h': 3600, '4h': 14400,
                      '1d': 86400, '1w': 604800}

            def _bar(pct: float, width: int = 16) -> str:
                filled = int(pct / 100 * width)
                return '█' * filled + '░' * (width - filled)

            def _fmt(n: int) -> str:
                return f'{n:,}'

            async with self.database_manager.get_session() as session:
                # ── Candle counts per (exchange, symbol, timeframe, source) ──
                candle_rows = (await session.execute(text("""
                    SELECT exchange, symbol, timeframe, source_type, COUNT(*) AS cnt
                    FROM candles GROUP BY exchange, symbol, timeframe, source_type
                """))).fetchall()
                cmap = {(r.exchange, r.symbol, r.timeframe, r.source_type): r.cnt for r in candle_rows}

                # ── Indicator counts ──
                ind_rows = (await session.execute(text(
                    "SELECT indicator_name, COUNT(*) AS cnt FROM indicators GROUP BY indicator_name"
                ))).fetchall()
                imap = {r.indicator_name: r.cnt for r in ind_rows}

                # ── Strategy signal counts ──
                strat_rows = (await session.execute(text(
                    "SELECT strategy_name, COUNT(*) AS cnt FROM strategy_signals GROUP BY strategy_name"
                ))).fetchall()
                smap = {r.strategy_name: r.cnt for r in strat_rows}

            lines = ['─── Pipeline Progress ───────────────────────────────────']

            # Indicators
            for ind_name, cfg in indicators_cfg.items():
                if not cfg.get('enabled', True):
                    continue
                conn = cfg.get('connection', '')
                ex   = connections_cfg.get(conn, {}).get('exchange', '')
                sym  = connections_cfg.get(conn, {}).get('symbol', '')
                src_tf = cfg.get('source_timeframe', '1m')
                actual   = imap.get(ind_name, 0)
                expected = cmap.get((ex, sym, src_tf, 'aggregated'), 0) or cmap.get((ex, sym, src_tf, 'realtime'), 0)
                pct = min(actual / expected * 100, 100) if expected > 0 else (100.0 if actual > 0 else 0.0)
                lines.append(f'  IND  {ind_name:30s}  {_bar(pct)}  {pct:5.1f}%  {_fmt(actual)}/{_fmt(expected)}')

            lines.append('')

            # Strategies
            for strat_name, cfg in strategies_cfg.items():
                if not cfg.get('enabled', True):
                    continue
                base_ind = cfg.get('base_indicator') or (cfg.get('required_indicators') or [None])[0]
                actual   = smap.get(strat_name, 0)
                expected = imap.get(base_ind, 0) if base_ind else 0
                pct = min(actual / expected * 100, 100) if expected > 0 else (100.0 if actual > 0 else 0.0)
                lines.append(f'  STRA {strat_name:36s}  {_bar(pct)}  {pct:5.1f}%  {_fmt(actual)}/{_fmt(expected)}')

            lines.append('─' * 60)
            for line in lines:
                self.logger.info(line)

        except Exception as e:
            self.logger.debug(f"Progress log error: {e}")

    async def stop_service(self, service_name: str):
        """Stop individual service"""
        if service_name not in self.service_tasks:
            self.logger.warning(f"Service {service_name} is not running")
            return
        
        self.logger.info(f"Stopping {service_name} service...")
        
        # Cancel the service task
        task = self.service_tasks[service_name]
        task.cancel()
        
        try:
            await task
        except asyncio.CancelledError:
            pass
        
        # Cleanup service
        if service_name in self.services:
            service = self.services[service_name]
            if hasattr(service, 'cleanup'):
                await service.cleanup()
        
        del self.service_tasks[service_name]
        self.logger.info(f" {service_name} service stopped")
    
    async def stop_all_services(self):
        """Stop all microservices"""
        self.logger.info("Stopping all microservices...")
        
        self.shutdown_in_progress = True
        self.running = False
        
        # Stop services in reverse order
        service_names = list(self.service_tasks.keys())
        for service_name in reversed(service_names):
            await self.stop_service(service_name)
        
        self.logger.info("All microservices stopped")
    
    async def restart_service(self, service_name: str):
        """Restart individual service"""
        self.logger.info(f"Restarting {service_name} service...")
        
        if service_name in self.service_tasks:
            await self.stop_service(service_name)
        
        await self._start_service(service_name)
        self.stats['total_restarts'] += 1
        
        self.logger.info(f" {service_name} service restarted")
    
    async def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        uptime = datetime.now(timezone.utc) - self.start_time
        
        service_statuses = {}
        for service_name, task in self.service_tasks.items():
            if service_name.startswith('_'):
                continue
                
            service_statuses[service_name] = {
                'running': not task.done(),
                'task_done': task.done(),
                'exception': str(task.exception()) if task.done() and task.exception() else None
            }
        
        return {
            'uptime_seconds': uptime.total_seconds(),
            'running': self.running,
            'shutdown_in_progress': self.shutdown_in_progress,
            'statistics': self.stats,
            'services': service_statuses,
            'queue_connected': self.queue_client and not self.queue_client.connection.is_closed if self.queue_client else False,
            'database_connected': self.database_manager and self.database_manager.pool and not self.database_manager.pool._closed if self.database_manager else False
        }
    
    async def run(self):
        """Main run loop"""
        self.logger.info("Starting Microservices Orchestrator v2...")
        self.logger.info("Setting up signal handlers...")
        
        # Setup signal handlers
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            asyncio.create_task(self.shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        self.logger.info("Signal handlers set up")
        
        try:
            self.logger.info("About to start all services...")
            # Start all services
            await self.start_services()
            self.logger.info("All services started successfully")
            
            self.logger.info("Microservices Orchestrator v2 is running. Press Ctrl+C to stop.")
            
            # Start coordinated gap recovery loop
            gap_recovery_task = asyncio.create_task(self._coordinated_gap_recovery_loop())
            
            # Keep orchestrator running
            while self.running:
                await asyncio.sleep(1)
                
            # Cancel gap recovery task on shutdown
            gap_recovery_task.cancel()
        
        except Exception as e:
            self.logger.error(f"Orchestrator failed: {e}")
            raise
    
    async def _coordinated_gap_recovery_loop(self):
        """Coordinated gap recovery loop - ensures proper sequence"""
        gap_check_interval = 300  # seconds
        
        self.logger.info("Starting coordinated gap recovery loop...")
        
        while self.running:
            try:
                self.logger.info("Starting coordinated gap recovery cycle...")
                
                self.logger.info("Running gap recovery (candles + indicators + strategies in parallel)...")
                tasks = []
                if 'gap_recovery' in self.services:
                    tasks.append(self.services['gap_recovery'].check_and_recover_gaps())
                if 'indicators_gap' in self.services:
                    tasks.append(self.services['indicators_gap'].check_and_recover_gaps())
                if 'strategies_gap' in self.services:
                    tasks.append(self.services['strategies_gap'].check_and_recover_gaps())

                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for i, result in enumerate(results):
                        if isinstance(result, Exception):
                            self.logger.error(f"Gap recovery task {i} failed: {result}")

                self.logger.info("Coordinated gap recovery cycle completed")
                
                # Wait before next cycle
                await asyncio.sleep(gap_check_interval)
                
            except asyncio.CancelledError:
                self.logger.info("Gap recovery loop cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in gap recovery cycle: {e}")
                await asyncio.sleep(gap_check_interval)
    
    async def shutdown(self):
        """Graceful shutdown"""
        self.logger.info("Initiating graceful shutdown...")
        
        try:
            # Stop all services
            await self.stop_all_services()
            
            # Cleanup exchange plugins
            for connection_name, plugin in self.exchange_plugins.items():
                try:
                    await plugin.cleanup()
                    self.logger.info(f"Cleaned up exchange plugin: {connection_name}")
                except Exception as e:
                    self.logger.error(f"Error cleaning up plugin {connection_name}: {e}")
            
            # Cleanup core components
            if self.queue_client:
                await self.queue_client.disconnect()
            
            self.logger.info("Graceful shutdown completed")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
        finally:
            self.running = False


async def main(config_dir: str = None, symbol: str = None):
    """Main function.

    Args:
        config_dir: Override config directory (used when called from launcher.py).
        symbol: Restrict this worker to a single symbol, e.g. "SOL_USDT".
    """
    import argparse

    logger = get_orchestrator_logger()

    # Parse CLI args only when running standalone (not called from launcher)
    if config_dir is None and symbol is None:
        parser = argparse.ArgumentParser()
        parser.add_argument('--config-dir', default=None)
        parser.add_argument('--symbol', default=None)
        args, _ = parser.parse_known_args()
        config_dir = args.config_dir
        symbol = args.symbol or os.getenv('TRADING_SYMBOL')

    orchestrator = None

    try:
        logger.info(f"Creating orchestrator instance (symbol={symbol or 'ALL'})...")
        orchestrator = MicroservicesOrchestratorV2()
        orchestrator.symbol_filter = symbol
        if config_dir:
            # Override the config dir used by UniversalConfigManager
            os.environ.setdefault('CONFIG_DIR_OVERRIDE', config_dir)

        await orchestrator.initialize()
        logger.info("Orchestrator initialized, starting run loop...")
        await orchestrator.run()

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Orchestrator failed: {e}")
        return 1
    finally:
        if orchestrator:
            await orchestrator.shutdown()

    logger.info("Orchestrator shutdown completed")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))