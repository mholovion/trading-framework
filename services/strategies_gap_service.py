#!/usr/bin/env python3
"""
Strategies Gap Service
======================

Service to detect and recover gaps in strategy signals.
Works similar to IndicatorsGapService but for strategies.

This service:
1. Finds timestamps where indicators exist but strategies don't
2. Groups consecutive missing timestamps into ranges
3. Sends strategy calculation requests to strategies_reactive_service
4. Strategies service decides whether to calculate or skip based on business logic
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Set
from sqlalchemy import and_, asc, desc, select
from core.universal_config_manager import UniversalConfigManager
from rabbitmq.rabbitmq_client import RabbitMQClient, MessagePublisher
from models.base import Indicator, StrategySignal
from core.logging_config import get_strategies_gap_logger


class StrategiesBatcher:
    """Batches strategy calculation requests efficiently"""
    
    def __init__(self, max_batch_size=500):
        self.max_batch_size = max_batch_size
        
    def create_batches(self, strategy_ranges: List[Dict]) -> List[Dict]:
        """Create batches from strategy ranges"""
        batches = []
        
        for range_info in strategy_ranges:
            # Since strategies can handle large ranges efficiently, 
            # we can send the full range as one batch
            batch = {
                'strategy_name': range_info['strategy_name'],
                'connection_name': range_info['connection_name'],
                'exchange': range_info['exchange'],
                'symbol': range_info['symbol'],
                'timeframe': range_info['timeframe'],
                'start_timestamp': range_info['start_timestamp'],
                'end_timestamp': range_info['end_timestamp'],
                'missing_count': range_info['missing_count'],
                'batch_id': f"strategy_range_{int(datetime.now(timezone.utc).timestamp())}_{len(batches)}"
            }
            batches.append(batch)
            
        return batches


class StrategiesGapService:
    """
    Gap detection and recovery service for strategies
    """
    
    def __init__(self, config_manager: UniversalConfigManager, 
                 database_manager: Any, queue_client: RabbitMQClient):
        self.config_manager = config_manager
        self.database_manager = database_manager
        self.queue_client = queue_client
        self.message_publisher = MessagePublisher(queue_client, 'strategies_gap_service')
        
        # Initialize components
        self.batch_processor = StrategiesBatcher(max_batch_size=500)
        
        # Strategy configurations
        self.enabled_strategies: Dict[str, Dict] = {}
        
        self.logger = get_strategies_gap_logger()
        
        # Statistics
        self.stats = {
            'gaps_detected': 0,
            'gaps_processed': 0,
            'calculation_requests_sent': 0,
            'last_gap_check': None,
            'start_time': datetime.now(timezone.utc)
        }
    
    async def initialize(self):
        """Initialize strategies gap service"""
        self.logger.info("Initializing Strategies Gap Service...")
        
        # Initialize database if needed
        if not hasattr(self, '_db_initialized'):
            await self.database_manager.initialize()
            self._db_initialized = True
        
        # Load enabled strategies
        await self._load_enabled_strategies()
        
        # Gap monitoring is now controlled by orchestrator
        # self.gap_check_task = asyncio.create_task(self._gap_monitoring_loop())
        
        self.logger.info("Strategies Gap Service initialized")
    
    async def check_and_recover_gaps(self):
        """Public method for orchestrator to trigger gap recovery"""
        self.logger.info("Starting strategies gap recovery (orchestrator triggered)...")
        
        try:
            check_start = datetime.now(timezone.utc)
            
            for strategy_name in self.enabled_strategies.keys():
                await self._check_strategy_gaps(strategy_name)
            
            self.stats['last_gap_check'] = check_start
            check_duration = (datetime.now(timezone.utc) - check_start).total_seconds()
            
            self.logger.info(f"Strategies gap recovery completed in {check_duration:.2f}s")
            
        except Exception as e:
            self.logger.error(f"Error in strategies gap recovery: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def _load_enabled_strategies(self):
        """Load enabled strategies from config"""
        strategies_config = self.config_manager.get_config('strategies')
        
        for strategy_name, strategy_config in strategies_config.get('strategies', {}).items():
            if strategy_config.get('enabled', False):
                self.enabled_strategies[strategy_name] = strategy_config
                required_indicators = strategy_config.get('required_indicators', [])
                connection = strategy_config.get('connection', 'unknown')
                self.logger.info(f"Monitoring strategy: {strategy_name} on {connection} (requires: {len(required_indicators)} indicators)")
        
        self.logger.info(f"Loaded {len(self.enabled_strategies)} enabled strategies")
    
    async def _check_strategy_gaps(self, strategy_name: str):
        """Check for gaps in specific strategy"""
        try:
            strategy_config = self.enabled_strategies[strategy_name]
            required_indicators = strategy_config.get('required_indicators', [])
            connection_name = strategy_config.get('connection')
            
            if not required_indicators:
                self.logger.debug(f"Strategy {strategy_name} has no required indicators, skipping")
                return
            
            # Get connection info
            connections_config = self.config_manager.get_config('connections')
            connection_config = connections_config.get('connections', {}).get(connection_name, {})
            
            if not connection_config:
                self.logger.warning(f"Connection {connection_name} not found for strategy {strategy_name}")
                return
            
            exchange = connection_config['exchange']
            symbol = connection_config['symbol']
            timeframe = connection_config['timeframe']
            
            self.logger.debug(f"Checking gaps for strategy {strategy_name}: {exchange}/{symbol}/{timeframe}")
            
            base_indicator = strategy_config.get('base_indicator', required_indicators[0] if required_indicators else None)
            if not base_indicator:
                self.logger.warning(f"No base_indicator for {strategy_name}, skipping")
                return

            try:
                async with self.database_manager.get_session() as session:
                    gaps = await self._find_strategy_gaps_efficiently(
                        session, strategy_name, connection_name, required_indicators, base_indicator
                    )
                    
                    if not gaps:
                        self.logger.debug(f"No gaps found for strategy {strategy_name}")
                        return
                    
                    self.logger.info(f"Found {len(gaps)} strategy gaps for {strategy_name}")
                    
                    # Prepare range info for batching
                    strategy_ranges = []
                    total_missing = 0
                    
                    for gap in gaps:
                        range_info = {
                            'strategy_name': strategy_name,
                            'connection_name': connection_name,
                            'exchange': exchange,
                            'symbol': symbol,
                            'timeframe': timeframe,
                            'start_timestamp': gap['start_timestamp'],
                            'end_timestamp': gap['end_timestamp'],
                            'missing_count': gap['missing_count']
                        }
                        strategy_ranges.append(range_info)
                        total_missing += gap['missing_count']
                    
                    # Use StrategiesBatcher to create optimized batches
                    batches = self.batch_processor.create_batches(strategy_ranges)
                    
                    # Send calculation requests for each batch
                    for batch in batches:
                        await self._send_strategy_calculation_request(strategy_name, batch)
                    
                    self.stats['gaps_processed'] += total_missing
                    self.logger.info(f"Sent {len(batches)} calculation requests for {total_missing} missing strategy calculations")
                        
            except Exception as e:
                self.logger.error(f"Database error checking strategy gaps for {strategy_name}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error sending gap calculation requests for {strategy_name}: {e}")
    
    async def _find_strategy_gaps_efficiently(self, session, strategy_name: str, connection_name: str,
                                             required_indicators: List[str], base_indicator: str) -> List[Dict]:
        """Find all unevaluated base_indicator timestamps for the strategy.

        Uses HOLD/BUY/SELL signals as the 'evaluated' marker.  Any base_indicator
        timestamp that has no corresponding signal (of any type) is a gap.
        Splits large gaps into week-sized batches to avoid overwhelming the queue.
        """
        # Get all base indicator timestamps (full history)
        _r = await session.execute(
            select(Indicator.timestamp).where(
                and_(
                    Indicator.connection_name == connection_name,
                    Indicator.indicator_name == base_indicator
                )
            ).order_by(asc(Indicator.timestamp))
        )
        base_rows = _r.all()

        if not base_rows:
            self.logger.debug(f"No base indicator data for {strategy_name}")
            return []

        # Find earliest timestamp where ALL other required indicators have data.
        # Base timestamps before this point cannot be evaluated (missing context).
        # For higher-timeframe indicators (1D, 1W), the cutoff logic requires that
        # there be a completed bar BEFORE the base-indicator timestamp's period start.
        # E.g. a 4H bar needs 1W RSI from the PREVIOUS week, so valid_from starts
        # one full period after the first available higher-TF indicator value.
        other_indicators = [ind for ind in required_indicators if ind != base_indicator]
        indicators_config = self.config_manager.get_config('indicators').get('indicators', {})
        period_seconds = {'1d': 86400, '1w': 604800}
        valid_from_ts = 0
        for ind_name in other_indicators:
            _r = await session.execute(
                select(Indicator.timestamp).where(
                    and_(
                        Indicator.connection_name == connection_name,
                        Indicator.indicator_name == ind_name
                    )
                ).order_by(asc(Indicator.timestamp)).limit(1)
            )
            first = _r.first()
            if not first:
                self.logger.debug(f"No data for required indicator {ind_name}, skipping {strategy_name}")
                return []
            ind_timeframe = indicators_config.get(ind_name, {}).get('source_timeframe', '4h')
            period = period_seconds.get(ind_timeframe, 0)
            if period:
                # Advance to the next period boundary so the first base bar has
                # a completed prior higher-TF bar to look up (cutoff logic).
                valid_from_ts = max(valid_from_ts, (first[0] // period + 1) * period)
            else:
                valid_from_ts = max(valid_from_ts, first[0])

        valid_base_ts = sorted(r[0] for r in base_rows if r[0] >= valid_from_ts)
        if not valid_base_ts:
            return []

        self.logger.info(
            f" {strategy_name}: {len(valid_base_ts)} valid base timestamps "
            f"from {datetime.fromtimestamp(valid_base_ts[0], tz=timezone.utc)} "
            f"to {datetime.fromtimestamp(valid_base_ts[-1], tz=timezone.utc)}"
        )

        # Get all existing signal timestamps (BUY, SELL, or HOLD = already evaluated)
        _r = await session.execute(
            select(StrategySignal.timestamp).where(
                and_(
                    StrategySignal.strategy_name == strategy_name,
                    StrategySignal.connection_name == connection_name
                )
            )
        )
        existing_ts = {r[0] for r in _r.all()}

        missing_ts = [ts for ts in valid_base_ts if ts not in existing_ts]
        if not missing_ts:
            self.logger.debug(f"No gaps for {strategy_name}")
            return []

        self.logger.info(f" {strategy_name}: {len(missing_ts)} unevaluated timestamps found")

        # Group consecutive missing timestamps into batches.
        # Split if gap between two consecutive missing timestamps > 1 week (unusual data hole).
        MAX_SPLIT_GAP = 604800  # 1 week in seconds
        gaps = []
        range_start = missing_ts[0]
        prev_ts = missing_ts[0]
        count = 1

        for ts in missing_ts[1:]:
            if ts - prev_ts > MAX_SPLIT_GAP:
                gaps.append({'start_timestamp': range_start, 'end_timestamp': prev_ts, 'missing_count': count})
                range_start = ts
                count = 1
            else:
                count += 1
            prev_ts = ts

        gaps.append({'start_timestamp': range_start, 'end_timestamp': prev_ts, 'missing_count': count})
        return gaps
    
    def _group_consecutive_timestamps(self, timestamps: List[int], timeframe: str) -> List[tuple]:
        """Group consecutive missing timestamps into ranges, universal for all timeframes"""
        if not timestamps:
            return []
        
        # Sort timestamps to ensure proper ordering
        sorted_timestamps = sorted(timestamps)
        ranges = []
        current_start = sorted_timestamps[0]
        current_end = sorted_timestamps[0]
        
        # Get timeframe interval in seconds
        def get_timeframe_interval(tf: str) -> int:
            intervals = {
                '1m': 60,
                '5m': 300,
                '15m': 900,
                '30m': 1800,
                '1h': 3600,
                '4h': 14400,
                '1d': 86400,
                '1w': 604800
            }
            return intervals.get(tf, 60)  # Default to 1m if unknown
        
        expected_interval = get_timeframe_interval(timeframe)
        
        for i in range(1, len(sorted_timestamps)):
            prev_timestamp = sorted_timestamps[i-1]
            curr_timestamp = sorted_timestamps[i]
            gap = curr_timestamp - prev_timestamp
            
            # Check if timestamps are consecutive (within expected interval + small tolerance)
            if gap <= expected_interval * 1.5:  # Allow 50% tolerance for network delays
                # Consecutive - extend current range
                current_end = curr_timestamp
            else:
                # Non-consecutive - close current range and start new one
                ranges.append((current_start, current_end))
                current_start = curr_timestamp
                current_end = curr_timestamp
        
        # Add the last range
        ranges.append((current_start, current_end))
        
        return ranges
    
    async def _send_strategy_calculation_request(self, strategy_name: str, range_info: Dict):
        """Send request to calculate strategies for range"""
        try:
            request_data = {
                'strategy_name': range_info['strategy_name'],
                'connection_name': range_info['connection_name'],
                'exchange': range_info['exchange'],
                'symbol': range_info['symbol'],
                'timeframe': range_info['timeframe'],
                'start_timestamp': range_info['start_timestamp'],
                'end_timestamp': range_info['end_timestamp'],
                'batch_id': range_info['batch_id'],
                'trigger_reason': 'gap_recovery'
            }
            
            message = self.message_publisher._create_message('strategy_calculation_request', request_data)
            await self.queue_client.publish_message('strategy_calculation_requests', message)
            
            self.stats['calculation_requests_sent'] += 1
            
            start_time = datetime.fromtimestamp(range_info['start_timestamp'], tz=timezone.utc)
            end_time = datetime.fromtimestamp(range_info['end_timestamp'], tz=timezone.utc)
            
            self.logger.info(f"Strategy calculation request sent for {strategy_name}: "
                           f"{range_info['missing_count']} calculations from {start_time} to {end_time} "
                           f"[batch: {range_info['batch_id']}]")
            
        except Exception as e:
            self.logger.error(f"Error sending strategy calculation request: {e}")
    
    async def start(self):
        """Start the gap service"""
        self.logger.info("Starting Strategies Gap Service...")
        # Service is now controlled by orchestrator, just keep running
        while True:
            await asyncio.sleep(1)
    
    async def stop(self):
        """Stop the gap service"""
        self.logger.info("Stopping Strategies Gap Service...")
        # Clean shutdown logic here
        self.logger.info("Strategies Gap Service stopped")