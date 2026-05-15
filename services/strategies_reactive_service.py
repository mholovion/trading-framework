#!/usr/bin/env python3
"""
Strategies Reactive Service
===========================

Reactive microservice that automatically recalculates trading strategies when indicator data changes.
This service listens to the message queue for indicator updates and recalculates all dependent strategies.

This implements the reactive architecture from ARCHITECTURE_MODERNIZATION_PLAN.md
"""

import asyncio
import logging
import importlib
import json
import os

from typing import Dict, Any, Optional, List, Set
from datetime import datetime, timezone
from core.universal_config_manager import UniversalConfigManager
from rabbitmq.rabbitmq_client import RabbitMQClient, QueueMessage, MessagePublisher
from models.base import Indicator, StrategySignal, Candle
from sqlalchemy import and_, select, text
from core.exceptions import ConfigurationError
from core.logging_config import get_strategies_logger


class StrategiesReactiveService:
    """
    Reactive service for automatic strategy calculations
    """
    
    def __init__(self, config_manager: UniversalConfigManager,
                 database_manager: Optional[Any], 
                 queue_client: RabbitMQClient):
        self.config_manager = config_manager
        self.database_manager = database_manager
        self.queue_client = queue_client
        self.message_publisher = MessagePublisher(queue_client, 'strategies_service')
        
        if not self.database_manager:
            raise ValueError("DatabaseManager is required - must be provided by orchestrator")
        
        # Load strategy plugins
        self.strategy_plugins: Dict[str, Any] = {}
        # Per-strategy locks: prevent concurrent process_batch calls on the same plugin
        # (shared self.last_signals state causes race conditions across concurrent batches)
        self._strategy_locks: Dict[str, asyncio.Lock] = {}

        self.logger = get_strategies_logger()
        
        # Statistics
        self.stats = {
            'indicator_updates_received': 0,
            'strategies_calculated': 0,
            'signals_generated': 0,
            'calculation_errors': 0,
            'dependency_resolutions': 0,
            'start_time': datetime.now(timezone.utc)
        }
        
        self.running = False
    
    async def initialize(self):
        """Initialize strategies reactive service"""
        self.logger.info("Initializing Strategies Reactive Service...")
        
        # Load strategy plugins
        await self._load_strategy_plugins()
        
        # Setup queue consumers
        await self._setup_queue_consumers()
        
        self.logger.info("Strategies Reactive Service initialized")
    
    async def _load_strategy_plugins(self):
        """Load strategy plugins dynamically"""
        full_config = self.config_manager.get_config('strategies')
        
        self.logger.info(f"DEBUG: Full strategies config keys: {list(full_config.keys())}")
        
        # If 'strategies' key exists, use that, otherwise use full config
        if 'strategies' in full_config:
            strategies_config = full_config['strategies']
        else:
            strategies_config = full_config
        
        # Load strategy plugins
        for strategy_name, strategy_config in strategies_config.items():
            # Skip if it's not a dict (individual strategy configuration)
            if not isinstance(strategy_config, dict):
                continue
            if not strategy_config.get('enabled', True):
                continue
            
            try:
                plugin_name = strategy_config['plugin']
                
                # Dynamic import of strategy plugin
                plugin_module = importlib.import_module(f'plugins.strategies.{plugin_name}_plugin')
                
                # Convert plugin_name to proper class name (camelCase)
                class_name_parts = plugin_name.split('_')
                class_name = ''.join(word.capitalize() for word in class_name_parts) + 'Plugin'
                
                self.logger.info(f"DEBUG: Looking for class '{class_name}' in module {plugin_name}_plugin")

                plugin_class = getattr(plugin_module, class_name)

                enriched_config = dict(strategy_config)
                enriched_config['name'] = strategy_name
                enriched_config.setdefault('parameters', {})['connection_name'] = strategy_config['connection']

                plugin_instance = plugin_class(enriched_config, self.database_manager)
                self.strategy_plugins[strategy_name] = plugin_instance

                self.logger.info(f"Loaded strategy plugin: {strategy_name} ({plugin_name})")

            except KeyError as e:
                self.logger.error(f"Strategy {strategy_name} missing required config key: {e}")
                raise ConfigurationError(f"Strategy {strategy_name} missing required config key: {e}")
            except Exception as e:
                self.logger.error(f"Failed to load strategy plugin {strategy_name}: {e}")
                raise ConfigurationError(f"Failed to load strategy plugin: {e}")

    async def _load_strategy_plugins(self):
        """Load strategy plugins from configuration"""
        strategies_config = self.config_manager.get_config('strategies')
        
        for strategy_name, strategy_config in strategies_config.get('strategies', {}).items():
            if not strategy_config.get('enabled', False):
                continue
                
            try:
                plugin_name = strategy_config['plugin']
                
                # Import plugin module dynamically
                plugin_module = importlib.import_module(f'plugins.strategies.{plugin_name}_plugin')
                
                # Convert plugin_name to proper class name (camelCase)
                class_name_parts = plugin_name.split('_')
                class_name = ''.join(word.capitalize() for word in class_name_parts) + 'Plugin'
                
                self.logger.info(f"DEBUG: Looking for class '{class_name}' in module {plugin_name}_plugin")

                plugin_class = getattr(plugin_module, class_name)

                enriched_config = dict(strategy_config)
                enriched_config['name'] = strategy_name
                enriched_config.setdefault('parameters', {})['connection_name'] = strategy_config['connection']

                plugin_instance = plugin_class(enriched_config, self.database_manager)
                self.strategy_plugins[strategy_name] = plugin_instance
                self._strategy_locks[strategy_name] = asyncio.Lock()

                self.logger.info(f"Loaded strategy plugin: {strategy_name} ({plugin_name})")

            except Exception as e:
                self.logger.error(f"Failed to load strategy plugin {strategy_name}: {e}")
                raise ConfigurationError(f"Failed to load strategy plugin: {e}")

        self.logger.info("Strategy plugins loaded")
    
    async def _setup_queue_consumers(self):
        """Setup message queue consumers"""
        # Consumer removed - using gap recovery instead of reactive recalculation
        
        # Consumer for strategy calculation requests (from gap recovery service)
        await self.queue_client.consume_messages('strategy_calculation_requests', self._handle_strategy_recalc_request)
        
        self.logger.info("Queue consumers setup completed")
    
    async def _handle_strategy_recalc_request(self, message: QueueMessage):
        """Handle strategy recalculation request"""
        try:
            if message.type == 'strategy_recalc':
                # Single strategy recalculation (from indicator updates)
                data = message.data
                self.stats['indicator_updates_received'] += 1
                
                # Calculate the specific strategy
                await self._calculate_strategy(data['strategy_name'], data['connection_name'], data['timestamp'])
                
            elif message.type == 'strategy_calculation_request':
                # Range strategy calculation (from gap recovery)
                await self._handle_strategy_range_calculation(message)
                
            else:
                self.logger.warning(f"Unknown message type: {message.type}")
                return
            
        except Exception as e:
            self.logger.error(f"Error handling strategy request: {e}")
            self.stats['calculation_errors'] += 1
    
    async def _handle_strategy_range_calculation(self, message: QueueMessage):
        """Handle strategy range calculation request from gap recovery"""
        try:
            self.logger.info(f"Received range calculation request from {message.source_service}")
            
            data = message.data
            strategy_name = data['strategy_name']
            connection_name = data['connection_name']
            start_timestamp = data['start_timestamp']
            end_timestamp = data['end_timestamp']
            batch_id = data.get('batch_id', 'no_batch_id')
            
            self.logger.info(f"Processing range calculation for {strategy_name}: "
                           f"{connection_name} from {datetime.fromtimestamp(start_timestamp, tz=timezone.utc)} "
                           f"to {datetime.fromtimestamp(end_timestamp, tz=timezone.utc)} "
                           f"[batch: {batch_id}]")
            
            # Find all timestamps in the range where we can calculate the strategy
            await self._calculate_strategy_range(strategy_name, connection_name, start_timestamp, end_timestamp)
                           
        except Exception as e:
            self.logger.error(f"Error handling strategy range calculation: {e}")
            self.stats['calculation_errors'] += 1
    
    async def _calculate_strategy_range(self, strategy_name: str, connection_name: str,
                                      start_timestamp: int, end_timestamp: int):
        """Calculate strategy for a range of timestamps"""
        try:
            strategies_config = self.config_manager.get_config('strategies')
            strategy_config = strategies_config['strategies'][strategy_name]
            required_indicators = strategy_config.get('required_indicators', [])
            base_indicator = strategy_config.get('base_indicator', required_indicators[0] if required_indicators else None)

            if not required_indicators:
                self.logger.warning(f"Strategy {strategy_name} has no required indicators")
                return

            async with self.database_manager.get_session() as session:
                complete_timestamps = await self._find_complete_indicator_timestamps_in_range(
                    session, connection_name, required_indicators, start_timestamp, end_timestamp,
                    base_indicator=base_indicator
                )

                if not complete_timestamps:
                    self.logger.debug(f"No complete indicator sets found in range for {strategy_name}")
                    return

                self.logger.info(f"Found {len(complete_timestamps)} complete indicator sets for {strategy_name}")

                # Single query to get all already-evaluated timestamps in range
                _r = await session.execute(
                    select(StrategySignal.timestamp).where(
                        and_(
                            StrategySignal.strategy_name == strategy_name,
                            StrategySignal.connection_name == connection_name,
                            StrategySignal.timestamp >= start_timestamp,
                            StrategySignal.timestamp <= end_timestamp
                        )
                    )
                )
                existing_ts_in_range = {row[0] for row in _r.all()}

            timestamps_to_calculate = sorted(complete_timestamps - existing_ts_in_range)
            skipped_count = len(complete_timestamps) - len(timestamps_to_calculate)

            if not timestamps_to_calculate:
                self.logger.info(f"All {skipped_count} timestamps already calculated for {strategy_name}")
                return

            plugin = self.strategy_plugins.get(strategy_name)
            self.logger.info(f"Plugin found: {plugin is not None}, has process_batch: {hasattr(plugin, 'process_batch') if plugin else False}")
            if plugin and hasattr(plugin, 'process_batch'):
                self.logger.info(f"Using batch processing for {strategy_name} with {len(timestamps_to_calculate)} timestamps")
                calculated_count = await self._calculate_strategy_batch(
                    strategy_name, connection_name, timestamps_to_calculate
                )
            else:
                self.logger.info(f"Using individual calculations for {strategy_name} with {len(timestamps_to_calculate)} timestamps")
                calculated_count = 0
                for i, timestamp in enumerate(timestamps_to_calculate):
                    try:
                        await self._calculate_strategy(strategy_name, connection_name, timestamp)
                        calculated_count += 1
                    except Exception as e:
                        self.logger.error(f"Error calculating {strategy_name} for timestamp {timestamp}: {e}")
                    # Yield to event loop every 50 iterations to allow network IO (aiohttp, etc.)
                    if i % 50 == 49:
                        await asyncio.sleep(0.001)

            self.logger.info(f"Strategy range calculation completed: {calculated_count} signals, {skipped_count} skipped")

        except Exception as e:
            self.logger.error(f"Error in strategy range calculation: {e}")
    
    async def _fetch_batch_data_bulk(self, strategy_name: str, connection_name: str,
                                     timestamps: List[int]) -> List[Dict]:
        """Fetch indicator data for all timestamps using 4 bulk DB queries + in-memory binary search.

        Replaces the N×4 per-timestamp query loop with:
          1 query per required indicator (full history, sorted)
          1 IN query for all candle close prices
        then matches each base timestamp to the correct higher-TF indicator value
        using bisect — no DB round-trip per timestamp.
        """
        import bisect

        strategies_config = self.config_manager.get_config('strategies')
        strategy_config = strategies_config['strategies'][strategy_name]
        required_indicators = strategy_config.get('required_indicators', [])
        base_indicator = strategy_config.get('base_indicator', required_indicators[0])

        indicators_config = self.config_manager.get_config('indicators').get('indicators', {})
        connections_config = self.config_manager.get_config('connections')
        conn_cfg = connections_config.get('connections', {}).get(connection_name, {})
        source_timeframe = indicators_config.get(base_indicator, {}).get('source_timeframe', '4h')

        async with self.database_manager.get_session() as session:
            # One query per indicator — full sorted history
            all_rows: Dict[str, list] = {}
            for ind_name in required_indicators:
                _r = await session.execute(
                    select(
                        Indicator.timestamp, Indicator.value, Indicator.meta_data
                    ).where(
                        and_(
                            Indicator.connection_name == connection_name,
                            Indicator.indicator_name == ind_name,
                        )
                    ).order_by(Indicator.timestamp.asc())
                )
                all_rows[ind_name] = list(_r.all())

            # One IN query for all candle close prices
            candle_prices: Dict[int, float] = {}
            if conn_cfg:
                src_table = self.database_manager.candle_source_table(source_timeframe)
                tf_filter = f"AND timeframe = '{source_timeframe}'" if src_table == 'candles' else ""
                ts_list = ','.join(str(t) for t in timestamps)
                _r = await session.execute(
                    text(f"""
                        SELECT timestamp, close_price
                        FROM {src_table}
                        WHERE exchange = :ex AND symbol = :sym {tf_filter}
                        AND timestamp = ANY(:ts_arr)
                    """), {
                        'ex': conn_cfg.get('exchange', ''),
                        'sym': conn_cfg.get('symbol', ''),
                        'ts_arr': list(timestamps),
                    }
                )
                candle_prices = {r[0]: float(r[1]) for r in _r.fetchall()}

        # Pre-build sorted timestamp index per indicator for bisect
        ts_index: Dict[str, list] = {
            ind_name: [r[0] for r in rows]
            for ind_name, rows in all_rows.items()
        }

        batch_data = []
        for ts in sorted(timestamps):
            indicators_data: Dict[str, Any] = {}
            missing = False

            for ind_name in required_indicators:
                ind_tf = indicators_config.get(ind_name, {}).get('source_timeframe', '4h')
                if ind_tf == '1d':
                    cutoff = (ts // 86400) * 86400
                elif ind_tf == '1w':
                    cutoff = (ts // 604800) * 604800
                else:
                    cutoff = ts + 1  # 4H exact match: timestamp < ts+1 ≡ timestamp <= ts

                idx = bisect.bisect_left(ts_index[ind_name], cutoff) - 1
                if idx < 0:
                    missing = True
                    break

                row = all_rows[ind_name][idx]
                indicators_data[ind_name] = {
                    'value': float(row[1]) if row[1] is not None else None,
                    'timestamp': row[0],
                    'metadata': json.loads(row[2]) if row[2] else {},
                }

            if not missing:
                batch_data.append({
                    'timestamp': ts,
                    'indicators_data': indicators_data,
                    'current_price': candle_prices.get(ts, 0.0),
                })

        return batch_data

    async def _calculate_strategy_batch(self, strategy_name: str, connection_name: str, timestamps: List[int]) -> int:
        """Calculate strategy for batch of timestamps using process_batch method"""
        try:
            plugin = self.strategy_plugins.get(strategy_name)
            if not plugin:
                self.logger.error(f"Strategy plugin not found: {strategy_name}")
                return 0

            self.logger.info(f"Fetching indicator data for {len(timestamps)} timestamps in bulk...")
            batch_data = await self._fetch_batch_data_bulk(strategy_name, connection_name, timestamps)
            self.logger.info(f"Bulk fetch complete: {len(batch_data)} valid timestamps")

            if not batch_data:
                self.logger.warning(f"No valid batch data for {strategy_name}")
                return 0

            lock = self._strategy_locks.get(strategy_name) or asyncio.Lock()
            async with lock:
                signals = await plugin.process_batch(batch_data)

                signal_count = 0
                hold_timestamps = []
                signals_to_save = []
                for i, signal in enumerate(signals):
                    timestamp = batch_data[i]['timestamp']
                    if signal:
                        signals_to_save.append({
                            'timestamp': timestamp,
                            'signal_type': signal.signal_type.value,
                            'confidence': signal.confidence,
                            'price': signal.price,
                        })
                        signal_count += 1
                        self.logger.info(f"Generated {signal.signal_type.value} signal: {strategy_name} @ {timestamp} (confidence: {signal.confidence})")
                    else:
                        hold_timestamps.append(timestamp)

            # Save signals and HOLDs directly to DB — bypasses RabbitMQ so signals
            # persist even when the database_update_service loses its MQ connection.
            await self._bulk_save_strategy_signals(strategy_name, connection_name, signals_to_save)
            await self._bulk_save_hold_signals(strategy_name, connection_name, hold_timestamps)

            return signal_count

        except Exception as e:
            self.logger.error(f"Error in batch strategy calculation: {e}")
            return 0
    
    async def _get_indicators_for_timestamp(self, strategy_name: str, connection_name: str, timestamp: int) -> Dict[str, Any]:
        """Get indicators data for a specific 4H timestamp.

        For the base indicator: exact match at timestamp.
        For higher-timeframe indicators (1D, 1W): latest value at or before timestamp.
        Returns dict with 'indicators' and 'close_price'.
        """
        try:
            strategies_config = self.config_manager.get_config('strategies')
            strategy_config = strategies_config['strategies'][strategy_name]
            required_indicators = strategy_config.get('required_indicators', [])

            if not required_indicators:
                self.logger.warning(f"No required indicators for strategy {strategy_name}")
                return {}

            indicators_data = {}
            close_price = 0.0

            indicators_config = self.config_manager.get_config('indicators').get('indicators', {})

            async with self.database_manager.get_session() as session:
                for indicator_name in required_indicators:
                    ind_cfg = indicators_config.get(indicator_name, {})
                    ind_timeframe = ind_cfg.get('source_timeframe', '4h')

                    # Pine Script uses last COMPLETED higher-timeframe bar.
                    # A 1D bar timestamped at day D 00:00 only closes at day D+1 00:00.
                    # So when evaluating a 4H bar at T, only use 1D/1W RSI from periods
                    # that have already closed (timestamp < start of current period).
                    if ind_timeframe == '1d':
                        period_seconds = 86400
                        cutoff = (timestamp // period_seconds) * period_seconds
                    elif ind_timeframe == '1w':
                        period_seconds = 604800
                        cutoff = (timestamp // period_seconds) * period_seconds
                    else:
                        cutoff = timestamp + 1  # 4H: include exact match (<=)

                    indicator = (await session.execute(
                        select(Indicator).where(
                            and_(
                                Indicator.connection_name == connection_name,
                                Indicator.indicator_name == indicator_name,
                                Indicator.timestamp < cutoff
                            )
                        ).order_by(Indicator.timestamp.desc()).limit(1)
                    )).scalars().first()

                    if indicator:
                        indicators_data[indicator_name] = {
                            'value': float(indicator.value) if indicator.value is not None else None,
                            'timestamp': indicator.timestamp,
                            'metadata': json.loads(indicator.meta_data) if indicator.meta_data else {}
                        }
                    else:
                        return {}  # Missing required indicator — skip this candle

                # Fetch close price from candle
                base_indicator = strategy_config.get('base_indicator', required_indicators[0])
                indicator_cfg = self.config_manager.get_config('indicators').get('indicators', {}).get(base_indicator, {})
                source_timeframe = indicator_cfg.get('source_timeframe', '4h')
                connections_config = self.config_manager.get_config('connections')
                conn_cfg = connections_config.get('connections', {}).get(connection_name, {})
                if conn_cfg:
                    src_table = self.database_manager.candle_source_table(source_timeframe)
                    tf_filter = f"AND timeframe = '{source_timeframe}'" if src_table == 'candles' else ""
                    _r = await session.execute(text(f"""
                        SELECT close_price FROM {src_table}
                        WHERE exchange = :ex AND symbol = :sym {tf_filter}
                        AND timestamp = :ts LIMIT 1
                    """), {'ex': conn_cfg.get('exchange', ''), 'sym': conn_cfg.get('symbol', ''), 'ts': timestamp})
                    row = _r.first()
                    if row:
                        close_price = float(row[0])

            return {'indicators': indicators_data, 'close_price': close_price}

        except Exception as e:
            self.logger.error(f"Error getting indicators for timestamp {timestamp}: {e}")
            return {}
    
    async def _save_strategy_signal(self, strategy_name: str, connection_name: str, timestamp: int, signal):
        """Save strategy signal to database via message queue"""
        try:
            # Publish strategy signal to database update service
            message = self.message_publisher._create_message('strategy_signal', {
                'strategy_name': strategy_name,
                'connection_name': connection_name,
                'signal_type': signal.signal_type.value,
                'timestamp': timestamp,
                'confidence': signal.confidence,
                'price': signal.price,
                'indicators_data': {},  # Not needed for storage
                'metadata': signal.metadata,
                'source_indicators': []
            })
            await self.queue_client.publish_message('strategy_updates', message)
            
            self.stats['strategies_calculated'] += 1
            self.stats['signals_generated'] += 1
            
            self.logger.info(f"Generated {signal.signal_type.value} signal: {strategy_name} @ {timestamp} (confidence: {signal.confidence})")
            
        except Exception as e:
            self.logger.error(f"Error saving strategy signal: {e}")
    
    async def _find_complete_indicator_timestamps_in_range(self, session, connection_name: str,
                                                   required_indicators: List[str],
                                                   start_timestamp: int, end_timestamp: int,
                                                   base_indicator: str = None) -> Set[int]:
        """Return base_indicator timestamps in range where all other required indicators have data.

        Uses base_indicator (e.g. 4H RSI) as calculation trigger points.
        A timestamp is valid only if every other required indicator has at least one
        entry at or before that timestamp (i.e. historical data exists).
        """
        if not required_indicators:
            return set()

        trigger = base_indicator or required_indicators[0]

        _r = await session.execute(
            select(Indicator.timestamp).where(
                and_(
                    Indicator.connection_name == connection_name,
                    Indicator.indicator_name == trigger,
                    Indicator.timestamp >= start_timestamp,
                    Indicator.timestamp <= end_timestamp
                )
            )
        )
        trigger_ts = {r[0] for r in _r.all()}

        if not trigger_ts:
            return set()

        # For non-trigger indicators, find the earliest timestamp where data exists.
        # All trigger timestamps before that point are invalid (missing context).
        other_indicators = [ind for ind in required_indicators if ind != trigger]
        if not other_indicators:
            return trigger_ts

        earliest_required = 0
        for ind_name in other_indicators:
            _r = await session.execute(
                select(Indicator.timestamp).where(
                    and_(
                        Indicator.connection_name == connection_name,
                        Indicator.indicator_name == ind_name
                    )
                ).order_by(Indicator.timestamp.asc()).limit(1)
            )
            first = _r.first()
            if not first:
                return set()  # Required indicator has no data at all — wait until it's available
            earliest_required = max(earliest_required, first[0])

        return {ts for ts in trigger_ts if ts >= earliest_required}

    async def _bulk_save_strategy_signals(self, strategy_name: str, connection_name: str,
                                          signals_data: List[Dict]):
        """Save BUY/SELL signals directly to DB for batch operations (bypasses RabbitMQ)."""
        if not signals_data:
            return
        try:
            async with self.database_manager.get_session() as session:
                for item in signals_data:
                    existing = (await session.execute(
                        select(StrategySignal.id).where(
                            and_(
                                StrategySignal.strategy_name == strategy_name,
                                StrategySignal.connection_name == connection_name,
                                StrategySignal.timestamp == item['timestamp']
                            )
                        ).limit(1)
                    )).first()
                    if existing:
                        continue
                    session.add(StrategySignal(
                        strategy_name=strategy_name,
                        connection_name=connection_name,
                        signal_type=item['signal_type'],
                        timestamp=item['timestamp'],
                        confidence=item['confidence'],
                        price=item['price'],
                    ))
                await session.commit()
            self.logger.info(f"Saved {len(signals_data)} strategy signals to DB for {strategy_name}")
        except Exception as e:
            self.logger.error(f"Error bulk saving strategy signals for {strategy_name}: {e}")

    async def _bulk_save_hold_signals(self, strategy_name: str, connection_name: str,
                                      timestamps: List[int]):
        """Save HOLD markers for evaluated candles that produced no BUY/SELL signal.

        These markers allow the gap service to detect which 4H candles have already
        been evaluated, preventing re-evaluation on every gap recovery cycle.
        """
        if not timestamps:
            return
        try:
            async with self.database_manager.get_session() as session:
                for ts in timestamps:
                    existing = (await session.execute(
                        select(StrategySignal.id).where(
                            and_(
                                StrategySignal.strategy_name == strategy_name,
                                StrategySignal.connection_name == connection_name,
                                StrategySignal.timestamp == ts
                            )
                        ).limit(1)
                    )).first()
                    if existing:
                        continue
                    session.add(StrategySignal(
                        strategy_name=strategy_name,
                        connection_name=connection_name,
                        signal_type='HOLD',
                        timestamp=ts,
                        confidence=0.0,
                        price=0.0,
                    ))
                await session.commit()
            self.logger.info(f"Saved {len(timestamps)} HOLD markers for {strategy_name}")
        except Exception as e:
            self.logger.error(f"Error saving HOLD markers for {strategy_name}: {e}")
    
    async def _calculate_strategy(self, strategy_name: str, connection_name: str, timestamp: int):
        """Calculate strategy signal for given timestamp"""
        try:
            plugin = self.strategy_plugins[strategy_name]
            
            async with self.database_manager.get_session() as session:
                # Get strategy configuration to know required indicators
                strategies_config = self.config_manager.get_config('strategies')
                strategy_config = strategies_config['strategies'][strategy_name]
                required_indicators = strategy_config.get('required_indicators', [])
                
                # Collect indicator values
                indicators_data = {}
                missing_indicators = []
                indicators_config = self.config_manager.get_config('indicators').get('indicators', {})

                for indicator_name in required_indicators:
                    ind_cfg = indicators_config.get(indicator_name, {})
                    ind_timeframe = ind_cfg.get('source_timeframe', '4h')
                    if ind_timeframe == '1d':
                        cutoff = (timestamp // 86400) * 86400
                    elif ind_timeframe == '1w':
                        cutoff = (timestamp // 604800) * 604800
                    else:
                        cutoff = timestamp + 1

                    indicator = (await session.execute(
                        select(Indicator).where(
                            and_(
                                Indicator.connection_name == connection_name,
                                Indicator.indicator_name == indicator_name,
                                Indicator.timestamp < cutoff
                            )
                        ).order_by(Indicator.timestamp.desc()).limit(1)
                    )).scalars().first()
                    
                    if indicator:
                        indicators_data[indicator_name] = {
                            'value': float(indicator.value) if indicator.value is not None else None,
                            'timestamp': indicator.timestamp,
                            'metadata': json.loads(indicator.meta_data) if indicator.meta_data else {}
                        }
                    else:
                        missing_indicators.append(indicator_name)
                
                # Check if all required indicators are available
                if missing_indicators:
                    self.logger.debug(f"Missing required indicators for {strategy_name}: {missing_indicators}")
                    return

                # Get base indicator timeframe to find the right candle price
                base_indicator = strategy_config.get('base_indicator', required_indicators[0])
                indicator_config = self.config_manager.get_config('indicators').get('indicators', {}).get(base_indicator, {})
                source_timeframe = indicator_config.get('source_timeframe', '4h')

                # Fetch close price of the candle at this timestamp
                candle_price = 0.0
                connections_config = self.config_manager.get_config('connections')
                conn_cfg = connections_config.get('connections', {}).get(connection_name, {})
                if conn_cfg:
                    src_table = self.database_manager.candle_source_table(source_timeframe)
                    tf_filter = f"AND timeframe = '{source_timeframe}'" if src_table == 'candles' else ""
                    _r = await session.execute(text(f"""
                        SELECT close_price FROM {src_table}
                        WHERE exchange = :ex AND symbol = :sym {tf_filter}
                        AND timestamp = :ts LIMIT 1
                    """), {'ex': conn_cfg.get('exchange', ''), 'sym': conn_cfg.get('symbol', ''), 'ts': timestamp})
                    row = _r.first()
                    if row:
                        candle_price = float(row[0])

                # Calculate strategy signal using the plugin's process method
                result = await plugin.process(indicators_data, candle_price, timestamp)
                
                if result and hasattr(result, 'signal_type'):
                    self.logger.info(
                        f"Signal {result.signal_type.value:4s}  {strategy_name}"
                        f"  price={result.price:.4f}"
                        f"  conf={result.confidence:.2f}"
                    )
                
                if result and hasattr(result, 'signal_type'):
                    # Publish strategy signal to database update service
                    # Create signal message and publish via queue client
                    message = self.message_publisher._create_message('strategy_signal', {
                        'strategy_name': strategy_name,
                        'connection_name': connection_name,
                        'signal_type': result.signal_type.value,
                        'timestamp': timestamp,
                        'confidence': result.confidence,
                        'price': result.price,
                        'indicators_data': indicators_data,
                        'metadata': result.metadata,
                        'source_indicators': []  # No dependencies anymore
                    })
                    await self.queue_client.publish_message('strategy_updates', message)
                    
                    self.stats['strategies_calculated'] += 1
                    self.stats['signals_generated'] += 1
                    
                    self.logger.info(f"Generated {result.signal_type.value} signal: {strategy_name} @ {timestamp} (confidence: {result.confidence})")
                else:
                    self.stats['strategies_calculated'] += 1
                    self.logger.debug(f"Strategy {strategy_name} calculated, no signal @ {timestamp}")
                
        except Exception as e:
            self.logger.error(f"Error calculating strategy {strategy_name}: {e}")
            self.stats['calculation_errors'] += 1
            raise
    
    async def get_strategy_status(self) -> Dict[str, Any]:
        """Get status of all strategies"""
        status = {}
        
        async with self.database_manager.get_session() as session:
            for strategy_name in self.strategy_plugins.keys():
                # Get latest signals for this strategy
                latest_signals = (await session.execute(
                    select(StrategySignal).where(
                        StrategySignal.strategy_name == strategy_name
                    ).order_by(StrategySignal.timestamp.desc()).limit(5)
                )).scalars().all()
                
                status[strategy_name] = {
                    'plugin_loaded': True,
                    'latest_signals': [
                        {
                            'signal_type': signal.signal_type,
                            'timestamp': signal.timestamp,
                            'confidence': float(signal.confidence),
                            'price': float(signal.price)
                        }
                        for signal in latest_signals
                    ]
                }
        
        return status
    
    async def start(self):
        """Start the strategies reactive service"""
        self.logger.info("Starting Strategies Reactive Service...")
        self.running = True
        
        try:
            # Keep service running
            while self.running:
                await asyncio.sleep(1)
                
                # Log statistics periodically
                if self.stats['strategies_calculated'] % 50 == 0 and self.stats['strategies_calculated'] > 0:
                    await self._log_statistics()
        
        except KeyboardInterrupt:
            self.logger.info("Shutdown signal received")
        finally:
            await self.cleanup()
    
    async def stop(self):
        """Stop the strategies reactive service"""
        self.logger.info("Stopping Strategies Reactive Service...")
        self.running = False
    
    async def _log_statistics(self):
        """Log service statistics"""
        uptime = datetime.now(timezone.utc) - self.stats['start_time']
        
        self.logger.info(f"Strategies Reactive Service Statistics:")
        self.logger.info(f"Uptime: {uptime}")
        self.logger.info(f"Indicator updates received: {self.stats['indicator_updates_received']}")
        self.logger.info(f"Strategies calculated: {self.stats['strategies_calculated']}")
        self.logger.info(f"Signals generated: {self.stats['signals_generated']}")
        self.logger.info(f"Dependency resolutions: {self.stats['dependency_resolutions']}")
        self.logger.info(f"Calculation errors: {self.stats['calculation_errors']}")
        
        if uptime.total_seconds() > 0:
            strategies_per_hour = self.stats['strategies_calculated'] / (uptime.total_seconds() / 3600)
            signals_per_hour = self.stats['signals_generated'] / (uptime.total_seconds() / 3600)
            self.logger.info(f"Calculation rate: {strategies_per_hour:.1f} strategies/hour")
            self.logger.info(f"Signal generation rate: {signals_per_hour:.1f} signals/hour")
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get service statistics"""
        uptime = datetime.now(timezone.utc) - self.stats['start_time']
        
        return {
            'uptime_seconds': uptime.total_seconds(),
            'indicator_updates_received': self.stats['indicator_updates_received'],
            'strategies_calculated': self.stats['strategies_calculated'],
            'signals_generated': self.stats['signals_generated'],
            'dependency_resolutions': self.stats['dependency_resolutions'],
            'calculation_errors': self.stats['calculation_errors'],
            'calculation_rate_per_hour': self.stats['strategies_calculated'] / max(uptime.total_seconds() / 3600, 1),
            'signal_generation_rate_per_hour': self.stats['signals_generated'] / max(uptime.total_seconds() / 3600, 1),
            'loaded_strategies': list(self.strategy_plugins.keys())
        }
    
    async def cleanup(self):
        """Cleanup strategies reactive service"""
        self.logger.info("Cleaning up Strategies Reactive Service...")
        
        # Close database connections
        # Database connections are managed by DatabaseManager
        
        # Log final statistics
        await self._log_statistics()
        
        self.logger.info("Strategies Reactive Service cleanup completed")


async def main():
    """Main function for running Strategies Reactive Service standalone"""
    import sys
    
    # Add project root to Python path
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger('StrategiesReactiveServiceMain')
    
    service = None
    queue_client = None
    
    try:
        # Initialize config manager
        config_manager = UniversalConfigManager()
        config_manager.load_all_configs()
        
        # Initialize queue client
        rabbitmq_url = os.getenv('RABBITMQ_URL')
        if not rabbitmq_url:
            raise ValueError("RABBITMQ_URL environment variable is required")
        queue_client = RabbitMQClient(rabbitmq_url)
        await queue_client.sessionect()
        
        # Initialize strategies reactive service
        service = StrategiesReactiveService(config_manager, queue_client)
        await service.initialize()
        
        # Start the service
        logger.info("Strategies Reactive Service is running. Press Ctrl+C to stop.")
        await service.start()
        
    except Exception as e:
        logger.error(f"Strategies Reactive Service failed: {e}")
        raise
    finally:
        # Cleanup
        if service:
            await service.cleanup()
        if queue_client:
            await queue_client.dissessionect()
        
        logger.info("Strategies Reactive Service shutdown completed")


if __name__ == "__main__":
    asyncio.run(main())