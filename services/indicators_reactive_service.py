#!/usr/bin/env python3
import asyncio
import json
import importlib
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

from core.universal_config_manager import UniversalConfigManager
from rabbitmq.rabbitmq_client import RabbitMQClient, QueueMessage, MessagePublisher
from models.base import Candle, Indicator
from sqlalchemy import text
from sqlalchemy import select as _sa_select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from core.timeframe_utils import TimeframeUtils
from core.exceptions import ConfigurationError
from core.logging_config import get_indicators_logger


class IndicatorsReactiveService:

    def __init__(self, config_manager: UniversalConfigManager,
                 database_manager: Optional[Any],
                 queue_client: RabbitMQClient):
        self.config_manager = config_manager
        self.database_manager = database_manager
        self.queue_client = queue_client
        self.message_publisher = MessagePublisher(queue_client, 'indicators_service')

        if not self.database_manager:
            raise ValueError("DatabaseManager is required - must be provided by orchestrator")

        self.indicator_plugins: Dict[str, Any] = {}
        self.calculation_semaphore = asyncio.Semaphore(30)

        self.logger = get_indicators_logger()

        self.stats = {
            'candle_updates_received': 0,
            'indicators_calculated': 0,
            'calculation_errors': 0,
            'start_time': datetime.now(timezone.utc)
        }

        self.running = False

    async def initialize(self):
        await self._load_indicator_plugins()
        await self._setup_queue_consumers()
        self.logger.info("Indicators Reactive Service initialized")

    async def _load_indicator_plugins(self):
        indicators_config = self.config_manager.get_config('indicators')

        for indicator_name, indicator_config in indicators_config.get('indicators', {}).items():
            if not indicator_config.get('enabled', True):
                continue
            try:
                plugin_name = indicator_config['plugin']
                module = importlib.import_module(f"plugins.indicators.{plugin_name}")
                plugin_class = getattr(module, f"{plugin_name.capitalize()}Plugin")
                self.indicator_plugins[indicator_name] = plugin_class(indicator_config)
                self.logger.info(f"Loaded indicator plugin: {indicator_name} ({plugin_name})")
            except Exception as e:
                self.logger.error(f"Failed to load indicator plugin {indicator_name}: {e}")

        self.logger.info(f"Loaded {len(self.indicator_plugins)} indicator plugins")

    async def _setup_queue_consumers(self):
        await self.queue_client.consume_messages('candle_updates', self._handle_candle_update)
        await self.queue_client.consume_messages('indicator_calculation_requests', self._handle_indicator_calculation_request)

    async def _handle_candle_update(self, message: QueueMessage):
        try:
            if message.type != 'candle_update':
                return

            data = message.data
            if not data.get('is_closed', False):
                return

            self.stats['candle_updates_received'] += 1

            connection_name = data.get('connection_name')
            exchange = data['exchange']
            symbol = data['symbol']
            timeframe = data['timeframe']
            timestamp = data['timestamp']

            matching_indicators = self._get_matching_indicators(connection_name, timeframe)
            if matching_indicators:
                tasks = [
                    self._calculate_indicator(indicator_name, exchange, symbol, timeframe, timestamp)
                    for indicator_name in matching_indicators
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for indicator_name, result in zip(matching_indicators, results):
                    if isinstance(result, Exception):
                        self.logger.error(f"Failed to calculate indicator {indicator_name}: {result}")
                        self.stats['calculation_errors'] += 1

        except Exception as e:
            self.logger.error(f"Error handling candle update: {e}")
            self.stats['calculation_errors'] += 1

    async def _handle_indicator_calculation_request(self, message: QueueMessage):
        try:
            if message.type != 'indicator_calculation_request':
                return

            data = message.data
            indicator_name = data['indicator_name']
            exchange = data['exchange']
            symbol = data['symbol']
            timeframe = data['timeframe']
            start_timestamp = data['start_timestamp']
            end_timestamp = data['end_timestamp']
            batch_id = data.get('batch_id', 'no_batch_id')

            self.logger.info(
                f"Processing calculation request for {indicator_name}: "
                f"{exchange}/{symbol}/{timeframe} "
                f"from {datetime.fromtimestamp(start_timestamp, tz=timezone.utc)} "
                f"to {datetime.fromtimestamp(end_timestamp, tz=timezone.utc)} "
                f"[batch: {batch_id}]"
            )
            await self._calculate_indicator_range(indicator_name, exchange, symbol, timeframe,
                                                   start_timestamp, end_timestamp)
        except Exception as e:
            self.logger.error(f"Error handling indicator calculation request: {e}")
            self.stats['calculation_errors'] += 1

    def _get_matching_indicators(self, connection_name: str, timeframe: str) -> List[str]:
        indicators_config = self.config_manager.get_config('indicators')
        return [
            name for name, cfg in indicators_config.get('indicators', {}).items()
            if cfg.get('enabled', False)
            and cfg.get('connection') == connection_name
            and cfg.get('source_timeframe') == timeframe
        ]

    async def _calculate_indicator(self, indicator_name: str, exchange: str, symbol: str,
                                    timeframe: str, timestamp: int):
        async with self.calculation_semaphore:
            try:
                plugin = self.indicator_plugins.get(indicator_name)
                if not plugin:
                    self.logger.error(f"Plugin not found for {indicator_name}")
                    return

                required_periods = getattr(plugin, 'get_required_periods', lambda: 50)()

                src_table = self.database_manager.candle_source_table(timeframe)
                tf_filter = f"AND timeframe = '{timeframe}'" if src_table == 'candles' else ""
                async with self.database_manager.get_session() as session:
                    result = await session.execute(
                        text(f"""
                            SELECT timestamp, open_price, high_price, low_price, close_price, volume
                            FROM {src_table}
                            WHERE exchange = :ex AND symbol = :sym {tf_filter}
                            AND timestamp <= :ts
                            ORDER BY timestamp DESC LIMIT :n
                        """), {'ex': exchange, 'sym': symbol, 'ts': timestamp, 'n': required_periods}
                    )
                    candles = list(reversed(result.fetchall()))

                if len(candles) < required_periods:
                    self.logger.debug(f"Insufficient data for {indicator_name}: need {required_periods}, have {len(candles)}")
                    return

                candle_data = [
                    {
                        'timestamp': r.timestamp,
                        'open':  float(r.open_price),
                        'high':  float(r.high_price),
                        'low':   float(r.low_price),
                        'close': float(r.close_price),
                        'volume': float(r.volume),
                    }
                    for r in candles
                ]

                result = await plugin.calculate(candle_data)

                if result is not None and 'value' in result:
                    connection_name = self.config_manager.get_config('indicators')['indicators'][indicator_name]['connection']

                    await self.queue_client.publish_message('indicator_updates',
                        self.message_publisher._create_message('indicator_calculated', {
                            'connection_name': connection_name,
                            'indicator_name': indicator_name,
                            'timestamp': timestamp,
                            'value': result['value'],
                            'metadata': result.get('metadata', {}),
                            'source_candle': {
                                'exchange': exchange,
                                'symbol': symbol,
                                'timeframe': timeframe,
                                'timestamp': timestamp
                            }
                        })
                    )

                    await self._upsert_ml_features(
                        indicator_name, exchange, symbol, timeframe,
                        [candle_data[-1]], [result],
                    )

                    self.stats['indicators_calculated'] += 1
                    self.logger.debug(f" {indicator_name}: {result['value']} @ {timestamp}")

            except Exception as e:
                self.logger.error(f"Error calculating indicator {indicator_name}: {e}")
                self.stats['calculation_errors'] += 1
                raise

    async def _fetch_candles_for_range(self, exchange: str, symbol: str, timeframe: str,
                                        warmup_start: int, end_timestamp: int) -> list:
        src_table = self.database_manager.candle_source_table(timeframe)
        tf_filter = f"AND timeframe = '{timeframe}'" if src_table == 'candles' else ""
        async with self.database_manager.get_session() as session:
            result = await session.execute(
                text(f"""
                    SELECT timestamp, open_price, high_price, low_price, close_price, volume
                    FROM {src_table}
                    WHERE exchange = :ex AND symbol = :sym {tf_filter}
                    AND timestamp >= :start AND timestamp <= :end
                    ORDER BY timestamp ASC
                """), {'ex': exchange, 'sym': symbol, 'start': warmup_start, 'end': end_timestamp}
            )
            return [
                {
                    'timestamp': r.timestamp,
                    'open':  float(r.open_price),
                    'high':  float(r.high_price),
                    'low':   float(r.low_price),
                    'close': float(r.close_price),
                    'volume': float(r.volume),
                }
                for r in result.fetchall()
            ]

    async def _calculate_indicator_range(self, indicator_name: str, exchange: str, symbol: str,
                                          timeframe: str, start_timestamp: int, end_timestamp: int):
        """Fast path: calculate_stream() for O(n) stateful plugins.
        Slow path: sliding-window per candle via RabbitMQ (legacy plugins without calculate_stream).
        """
        async with self.calculation_semaphore:
            try:
                plugin = self.indicator_plugins.get(indicator_name)
                if not plugin:
                    self.logger.error(f"Plugin not found for {indicator_name}")
                    return

                required_periods = getattr(plugin, 'get_required_periods', lambda: 50)()
                tf_seconds = TimeframeUtils.get_timeframe_seconds(timeframe)
                warmup_start = start_timestamp - required_periods * tf_seconds

                all_candles = await self._fetch_candles_for_range(
                    exchange, symbol, timeframe, warmup_start, end_timestamp
                )
                if not all_candles:
                    self.logger.warning(f"No candles found for {indicator_name} range calculation")
                    return

                target_start_idx = next(
                    (i for i, c in enumerate(all_candles) if c['timestamp'] >= start_timestamp),
                    len(all_candles)
                )
                target_count = len(all_candles) - target_start_idx
                self.logger.info(f"Calculating {indicator_name} for {target_count} candles (warmup: {target_start_idx})")

                if hasattr(plugin, 'calculate_stream'):
                    results = await plugin.calculate_stream(all_candles, target_start_idx)
                    connection_name = self.config_manager.get_config('indicators')['indicators'][indicator_name]['connection']
                    await self._direct_bulk_insert(
                        indicator_name, exchange, symbol, timeframe, connection_name,
                        all_candles[target_start_idx:], results
                    )
                    return

                # Slow path: sliding window + RabbitMQ
                PUBLISH_BATCH = 500
                calculated_indicators = []
                total_calculated = 0

                for i in range(target_start_idx, len(all_candles)):
                    window_start = max(0, i - required_periods + 1)
                    window = all_candles[window_start: i + 1]

                    if len(window) < required_periods:
                        continue

                    result = await plugin.calculate(window)
                    if result and 'value' in result:
                        calculated_indicators.append({
                            'indicator_name': indicator_name,
                            'timestamp': all_candles[i]['timestamp'],
                            'value': result['value'],
                            'metadata': result.get('metadata', {}),
                            'exchange': exchange,
                            'symbol': symbol,
                            'timeframe': timeframe,
                        })
                        total_calculated += 1

                    if len(calculated_indicators) >= PUBLISH_BATCH:
                        await self._bulk_publish_indicators(calculated_indicators)
                        calculated_indicators = []
                        self.logger.info(f" {indicator_name} batch progress: {total_calculated}/{target_count}")
                        await asyncio.sleep(0)

                if calculated_indicators:
                    await self._bulk_publish_indicators(calculated_indicators)

                self.logger.info(f"Completed {indicator_name} range calculation: {total_calculated}/{target_count} successful")

            except Exception as e:
                self.logger.error(f"Error in indicator range calculation: {e}")

    async def _insert_chunk(self, rows: list):
        async with self.database_manager.get_session() as session:
            stmt = pg_insert(Indicator.__table__).values(rows).on_conflict_do_nothing(
                index_elements=['indicator_name', 'exchange', 'symbol', 'timeframe', 'timestamp']
            )
            await session.execute(stmt)
            await session.commit()

    async def _upsert_ml_features(self, indicator_name: str, exchange: str, symbol: str,
                                   timeframe: str, candles: list, results: list):
        """Upsert OHLCV + indicator value into ml_features using JSONB merge (||)."""
        rows = [
            {
                'timestamp':   c['timestamp'],
                'exchange':    exchange,
                'symbol':      symbol,
                'timeframe':   timeframe,
                'open_price':  c['open'],
                'high_price':  c['high'],
                'low_price':   c['low'],
                'close_price': c['close'],
                'volume':      c['volume'],
                'features':    json.dumps({indicator_name: r['value']}),
            }
            for c, r in zip(candles, results)
            if r is not None
        ]
        if not rows:
            return

        sql = text("""
            INSERT INTO ml_features
                (timestamp, exchange, symbol, timeframe,
                 open_price, high_price, low_price, close_price, volume, features)
            VALUES
                (:timestamp, :exchange, :symbol, :timeframe,
                 :open_price, :high_price, :low_price, :close_price, :volume,
                 CAST(:features AS jsonb))
            ON CONFLICT (exchange, symbol, timeframe, timestamp)
            DO UPDATE SET
                open_price  = EXCLUDED.open_price,
                high_price  = EXCLUDED.high_price,
                low_price   = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume      = EXCLUDED.volume,
                features    = ml_features.features || EXCLUDED.features,
                updated_at  = NOW()
        """)
        CHUNK = 5000
        for i in range(0, len(rows), CHUNK):
            async with self.database_manager.get_session() as session:
                await session.execute(sql, rows[i:i + CHUNK])
                await session.commit()
            await asyncio.sleep(0)

    async def _direct_bulk_insert(self, indicator_name: str, exchange: str, symbol: str,
                                   timeframe: str, connection_name: str,
                                   target_candles: list, results: list):
        """Write indicator values directly to DB, bypassing RabbitMQ for fast backfill."""
        CHUNK = 5000
        rows = [
            {
                'connection_name': connection_name,
                'indicator_name': indicator_name,
                'exchange': exchange,
                'symbol': symbol,
                'timeframe': timeframe,
                'timestamp': c['timestamp'],
                'value': r['value'],
                'meta_data': '{}',
            }
            for c, r in zip(target_candles, results)
            if r is not None
        ]
        total = 0
        for i in range(0, len(rows), CHUNK):
            chunk = rows[i: i + CHUNK]
            await self._insert_chunk(chunk)
            total += len(chunk)
            self.stats['indicators_calculated'] += len(chunk)
            self.logger.info(f"  {indicator_name} direct insert: {total}/{len(rows)}")
            await asyncio.sleep(0)
        self.logger.info(f"Direct insert complete: {total} {indicator_name} values")

        await self._upsert_ml_features(indicator_name, exchange, symbol, timeframe,
                                        target_candles, results)

    async def _bulk_publish_indicators(self, calculated_indicators: List[dict]):
        if not calculated_indicators:
            return
        try:
            indicators_config = self.config_manager.get_config('indicators')
            bulk_indicators = [
                {
                    'connection_name': indicators_config['indicators'][item['indicator_name']]['connection'],
                    'indicator_name': item['indicator_name'],
                    'timestamp': item['timestamp'],
                    'value': item['value'],
                    'metadata': item['metadata'],
                    'source_candle': {
                        'exchange': item['exchange'],
                        'symbol': item['symbol'],
                        'timeframe': item['timeframe'],
                        'timestamp': item['timestamp']
                    }
                }
                for item in calculated_indicators
            ]
            await self.queue_client.publish_message('indicator_updates',
                self.message_publisher._create_message('indicators_calculated_bulk', {
                    'indicators': bulk_indicators,
                    'count': len(bulk_indicators)
                })
            )
            self.stats['indicators_calculated'] += len(calculated_indicators)
        except Exception as e:
            self.logger.error(f"Error bulk publishing indicators: {e}")

    async def start(self):
        self.running = True
        try:
            while self.running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            self.running = False

    async def stop(self):
        self.running = False

    async def get_statistics(self) -> Dict[str, Any]:
        uptime = datetime.now(timezone.utc) - self.stats['start_time']
        return {
            'uptime_seconds': uptime.total_seconds(),
            'candle_updates_received': self.stats['candle_updates_received'],
            'indicators_calculated': self.stats['indicators_calculated'],
            'calculation_errors': self.stats['calculation_errors'],
            'loaded_plugins': list(self.indicator_plugins.keys())
        }
