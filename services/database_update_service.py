#!/usr/bin/env python3
"""
Database Update Service
=======================

Centralized microservice responsible for processing all database updates from the message queue.
This service receives candle updates, indicator updates, and strategy signals from other services
and manages all database writes using SQLAlchemy ORM.

This implements the queue-based architecture from ARCHITECTURE_MODERNIZATION_PLAN.md
"""

import asyncio
import logging
import json
import os
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from core.universal_config_manager import UniversalConfigManager
from core.database import DatabaseManager
from core.logging_config import get_database_logger
from rabbitmq.rabbitmq_client import RabbitMQClient, QueueMessage, MessagePublisher
from models.base import DatabaseORM, Candle, Indicator, StrategySignal
from core.exceptions import DatabaseError


class DatabaseUpdateService:
    """
    Centralized database update service that processes all database writes from message queues
    """
    
    def __init__(self, config_manager: UniversalConfigManager,
                 database_manager: Optional[DatabaseManager],
                 queue_client: RabbitMQClient):
        self.config_manager = config_manager
        self.database_manager = database_manager
        self.queue_client = queue_client
        self.message_publisher = MessagePublisher(queue_client, 'database_update_service')
        
        if not self.database_manager:
            raise ValueError("DatabaseManager is required - must be provided by orchestrator")
        
        # Keep db_orm for backward compatibility but don't use it for new connections
        database_url = f"postgresql://{os.getenv('DATABASE_USER')}:{os.getenv('DATABASE_PASSWORD')}@{os.getenv('DATABASE_HOST')}:{os.getenv('DATABASE_PORT')}/{os.getenv('DATABASE_NAME')}"
        self.db_orm = DatabaseORM(database_url)
        
        self.logger = get_database_logger()
        
        # Statistics
        self.stats = {
            'candles_processed': 0,
            'indicators_processed': 0,
            'strategy_signals_processed': 0,
            'database_errors': 0,
            'queue_processing_errors': 0,
            'start_time': datetime.now(timezone.utc)
        }
        
        self.running = False
    
    async def initialize(self):
        """Initialize database update service"""
        self.logger.info("Initializing Database Update Service...")
        
        # Initialize database if we created it
        if not hasattr(self, '_db_initialized') or not self._db_initialized:
            await self.database_manager.initialize()
            self._db_initialized = True
        
        # Create database tables if they don't exist (keep for compatibility)
        self.db_orm.create_tables()
        
        # Setup queue consumers
        await self._setup_queue_consumers()
        
        self.logger.info("Database Update Service initialized")
    
    async def _setup_queue_consumers(self):
        """Setup message queue consumers"""
        # Consumer for candle updates
        await self.queue_client.consume_messages('candle_updates', self._process_candle_update)
        
        # Consumer for indicator updates
        await self.queue_client.consume_messages('indicator_updates', self._process_indicator_update)
        
        # Consumer for strategy signals
        await self.queue_client.consume_messages('strategy_updates', self._process_strategy_signal)
        
        self.logger.info("Queue consumers setup completed")
    
    async def _process_candle_update(self, message: QueueMessage):
        """Process candle update message"""
        try:
            if message.type == 'candles_bulk_update':
                # Handle bulk candles message
                await self._process_bulk_candles(message)
                return
            elif message.type != 'candle_update':
                self.logger.warning(f"Unexpected message type in candle queue: {message.type}")
                return
            
            data = message.data
            
            # Create candle record using DatabaseManager (proper pool configuration)
            with self.database_manager.get_session() as session:
                # Check if candle already exists
                existing_candle = session.query(Candle).filter_by(
                    exchange=data['exchange'],
                    symbol=data['symbol'],
                    timeframe=data['timeframe'],
                    timestamp=data['timestamp']
                ).first()
                
                if existing_candle:
                    # Update existing candle (for active candles)
                    existing_candle.open_price = data['ohlcv']['open']
                    existing_candle.high_price = data['ohlcv']['high']
                    existing_candle.low_price = data['ohlcv']['low']
                    existing_candle.close_price = data['ohlcv']['close']
                    existing_candle.volume = data['ohlcv']['volume']
                    existing_candle.updated_at = datetime.now(timezone.utc)
                    
                    self.logger.info(f"Updated candle: {data['exchange']} {data['symbol']} {data['timeframe']} @ {data['timestamp']}")
                else:
                    # Create new candle
                    candle = Candle(
                        exchange=data['exchange'],
                        symbol=data['symbol'],
                        timeframe=data['timeframe'],
                        timestamp=data['timestamp'],
                        open_price=data['ohlcv']['open'],
                        high_price=data['ohlcv']['high'],
                        low_price=data['ohlcv']['low'],
                        close_price=data['ohlcv']['close'],
                        volume=data['ohlcv']['volume']
                    )
                    
                    session.add(candle)
                    session.flush()  # Get the ID
                    
                    self.logger.info(f"Created candle: {data['exchange']} {data['symbol']} {data['timeframe']} @ {data['timestamp']}")
                
                session.commit()
            
            self.stats['candles_processed'] += 1
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error processing candle update: {e}")
            self.stats['database_errors'] += 1
        except Exception as e:
            self.logger.error(f"Error processing candle update: {e}")
            self.stats['queue_processing_errors'] += 1
    
    async def _process_bulk_candles(self, message: QueueMessage):
        """Process bulk candles message for improved performance"""
        try:
            data = message.data
            candles_list = data.get('candles', [])
            
            if not candles_list:
                self.logger.warning("Empty bulk candles message")
                return
                
            batch_info = data.get('batch_info', {})
            exchange = batch_info.get('exchange', 'unknown')
            symbol = batch_info.get('symbol', 'unknown')
            timeframe = batch_info.get('timeframe', 'unknown')
            source = batch_info.get('source', 'unknown')
            
            self.logger.info(f"Processing bulk candles: {len(candles_list)} candles from {message.source_service} ({exchange}/{symbol}/{timeframe}) source: {source}")
            
            # Use bulk operations for better performance
            with self.database_manager.get_session() as session:
                candles_to_insert = []
                candles_to_update = []
                
                for candle_data in candles_list:
                    # Check if candle already exists
                    existing_candle = session.query(Candle).filter_by(
                        exchange=candle_data['exchange'],
                        symbol=candle_data['symbol'],
                        timeframe=candle_data['timeframe'],
                        timestamp=candle_data['timestamp']
                    ).first()
                    
                    if existing_candle:
                        # Update existing candle
                        existing_candle.open_price = candle_data['ohlcv']['open']
                        existing_candle.high_price = candle_data['ohlcv']['high']
                        existing_candle.low_price = candle_data['ohlcv']['low']
                        existing_candle.close_price = candle_data['ohlcv']['close']
                        existing_candle.volume = candle_data['ohlcv']['volume']
                        existing_candle.updated_at = datetime.now(timezone.utc)
                        candles_to_update.append(existing_candle)
                    else:
                        # Prepare for insert
                        candle = Candle(
                            exchange=candle_data['exchange'],
                            symbol=candle_data['symbol'],
                            timeframe=candle_data['timeframe'],
                            timestamp=candle_data['timestamp'],
                            open_price=candle_data['ohlcv']['open'],
                            high_price=candle_data['ohlcv']['high'],
                            low_price=candle_data['ohlcv']['low'],
                            close_price=candle_data['ohlcv']['close'],
                            volume=candle_data['ohlcv']['volume']
                        )
                        candles_to_insert.append(candle)
                
                # Bulk insert new candles
                if candles_to_insert:
                    session.add_all(candles_to_insert)
                    
                # Commit all changes
                session.commit()
                
                inserted_count = len(candles_to_insert)
                updated_count = len(candles_to_update)
                total_stored = inserted_count + updated_count
                
                self.logger.info(f"Bulk processed {inserted_count} new + {updated_count} updated = {total_stored} candles stored")
                self.stats['candles_processed'] += len(candles_list)
                
        except SQLAlchemyError as e:
            self.logger.error(f"Database error processing bulk candles: {e}")
            self.stats['database_errors'] += 1
        except Exception as e:
            self.logger.error(f"Error processing bulk candles: {e}")
            self.stats['queue_processing_errors'] += 1
    
    async def _process_indicator_update(self, message: QueueMessage):
        """Process indicator update message"""
        try:
            self.logger.info(f"Processing indicator message: type={message.type}, source={message.source_service}")
            
            if message.type == 'indicators_calculated_bulk':
                # Handle bulk indicators message
                await self._process_bulk_indicators(message)
                return
            elif message.type != 'indicator_calculated':
                self.logger.warning(f"Unexpected message type in indicator queue: {message.type}")
                return

            self.logger.info(f" indicator_calculated message received from {message.source_service}")

            data = message.data
            
            # Create or update indicator record using ORM
            with self.database_manager.get_session() as session:
                # Extract exchange, symbol, timeframe from source_candle data first
                source_candle_info = data.get('source_candle', {})
                
                # Check if indicator value already exists (use all unique fields like aggregation does)
                existing_indicator = session.query(Indicator).filter_by(
                    indicator_name=data['indicator_name'],
                    exchange=source_candle_info.get('exchange', ''),
                    symbol=source_candle_info.get('symbol', ''),
                    timeframe=source_candle_info.get('timeframe', ''),
                    timestamp=data['timestamp']
                ).first()
                
                if existing_indicator:
                    # Update existing indicator
                    existing_indicator.value = data['value']
                    existing_indicator.meta_data = json.dumps(data.get('metadata', {}))
                    existing_indicator.updated_at = datetime.now(timezone.utc)
                    
                    self.logger.debug(f"Updated indicator: {data['indicator_name']} @ {data['timestamp']}")
                else:
                    # Create new indicator
                    # Try to find source candle for dependency tracking
                    source_candle = None
                    if 'source_candle' in data:
                        source_candle = session.query(Candle).filter_by(
                            exchange=data['source_candle']['exchange'],
                            symbol=data['source_candle']['symbol'],
                            timeframe=data['source_candle']['timeframe'],
                            timestamp=data['source_candle']['timestamp']
                        ).first()
                    
                    
                    indicator = Indicator(
                        connection_name=data['connection_name'],
                        indicator_name=data['indicator_name'],
                        exchange=source_candle_info.get('exchange', ''),
                        symbol=source_candle_info.get('symbol', ''),
                        timeframe=source_candle_info.get('timeframe', ''),
                        timestamp=data['timestamp'],
                        value=data['value'],
                        meta_data=json.dumps(data.get('metadata', {})),
                        source_candle_id=source_candle.id if source_candle else None
                    )
                    
                    session.add(indicator)
                    self.logger.debug(f"Created indicator: {data['indicator_name']} @ {data['timestamp']}")
                
                session.commit()
            
            self.stats['indicators_processed'] += 1
            
            # Trigger strategy calculations that depend on this indicator
            await self._trigger_strategy_calculations(data)
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error processing indicator update: {e}")
            self.stats['database_errors'] += 1
        except Exception as e:
            self.logger.error(f"Error processing indicator update: {e}")
            self.stats['queue_processing_errors'] += 1
    
    async def _process_bulk_indicators(self, message: QueueMessage):
        """Process bulk indicators message for improved performance"""
        try:
            data = message.data
            indicators_list = data.get('indicators', [])

            if not indicators_list:
                self.logger.warning("Empty bulk indicators message")
                return

            self.logger.info(f"Processing bulk indicators: {len(indicators_list)} indicators from {message.source_service}")

            # Build lookup key → data map
            by_key = {}
            for ind in indicators_list:
                src = ind.get('source_candle', {})
                key = (
                    ind['indicator_name'],
                    src.get('exchange', ''),
                    src.get('symbol', ''),
                    src.get('timeframe', ''),
                    ind['timestamp'],
                )
                by_key[key] = ind

            with self.database_manager.get_session() as session:
                # Collect unique (indicator_name, timestamp) pairs for a single IN lookup
                ind_names = list({k[0] for k in by_key})
                timestamps = list({k[4] for k in by_key})

                existing_rows = session.query(
                    Indicator.indicator_name,
                    Indicator.exchange,
                    Indicator.symbol,
                    Indicator.timeframe,
                    Indicator.timestamp,
                    Indicator.id,
                ).filter(
                    Indicator.indicator_name.in_(ind_names),
                    Indicator.timestamp.in_(timestamps),
                ).all()

                existing_set = {
                    (r.indicator_name, r.exchange, r.symbol, r.timeframe, r.timestamp): r.id
                    for r in existing_rows
                }

                indicators_to_insert = []
                for key, ind in by_key.items():
                    src = ind.get('source_candle', {})
                    if key in existing_set:
                        # Skip update for historical backfill — value shouldn't change
                        continue
                    indicators_to_insert.append(Indicator(
                        connection_name=ind['connection_name'],
                        indicator_name=ind['indicator_name'],
                        exchange=src.get('exchange', ''),
                        symbol=src.get('symbol', ''),
                        timeframe=src.get('timeframe', ''),
                        timestamp=ind['timestamp'],
                        value=ind['value'],
                        meta_data=json.dumps(ind.get('metadata', {})),
                        source_candle_id=None,
                    ))

                if indicators_to_insert:
                    session.add_all(indicators_to_insert)
                session.commit()

                inserted_count = len(indicators_to_insert)
                self.logger.info(f"Bulk processed {inserted_count} new + {len(by_key) - inserted_count} skipped indicators")
                self.stats['indicators_processed'] += len(indicators_list)

            # After commit, trigger range strategy calculation (one message per affected strategy)
            await self._trigger_strategy_calculations_bulk(indicators_list)

        except SQLAlchemyError as e:
            self.logger.error(f"Database error processing bulk indicators: {e}")
            self.stats['database_errors'] += 1
        except Exception as e:
            self.logger.error(f"Error processing bulk indicators: {e}")
            self.stats['queue_processing_errors'] += 1
    
    async def _process_strategy_signal(self, message: QueueMessage):
        """Process strategy signal message"""
        try:
            if message.type != 'strategy_signal':
                self.logger.warning(f"Unexpected message type in strategy queue: {message.type}")
                return
            
            data = message.data
            
            # Create strategy signal record using ORM
            with self.database_manager.get_session() as session:
                # Find source indicators for dependency tracking
                source_indicator_ids = []
                if 'source_indicators' in data:
                    for indicator_ref in data['source_indicators']:
                        indicator = session.query(Indicator).filter_by(
                            connection_name=indicator_ref['connection_name'],
                            indicator_name=indicator_ref['indicator_name'],
                            timestamp=indicator_ref['timestamp']
                        ).first()
                        if indicator:
                            source_indicator_ids.append(indicator.id)
                
                strategy_signal = StrategySignal(
                    strategy_name=data['strategy_name'],
                    connection_name=data['connection_name'],
                    signal_type=data['signal_type'],
                    timestamp=data['timestamp'],
                    confidence=data['confidence'],
                    price=data['price'],
                    indicators_data=json.dumps(data.get('indicators_data', {})),
                    metadata=json.dumps(data.get('metadata', {}))
                )
                
                session.add(strategy_signal)
                session.commit()
            
            self.stats['strategy_signals_processed'] += 1
            
            # Publish trading signal for execution service
            await self.message_publisher.publish_trading_signal({
                'strategy_name': data['strategy_name'],
                'signal_type': data['signal_type'],
                'timestamp': data['timestamp'],
                'confidence': data['confidence'],
                'price': data['price'],
                'connection_name': data['connection_name'],
                'metadata': data.get('metadata', {})
            })
            
            self.logger.info(f"Processed strategy signal: {data['strategy_name']} {data['signal_type']} @ {data['timestamp']}")
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error processing strategy signal: {e}")
            self.stats['database_errors'] += 1
        except Exception as e:
            self.logger.error(f"Error processing strategy signal: {e}")
            self.stats['queue_processing_errors'] += 1
    
    async def _trigger_strategy_calculations(self, indicator_data: Dict[str, Any]):
        """Trigger strategy calculations that might depend on this indicator (single indicator)"""
        try:
            strategies_config = self.config_manager.get_config('strategies')
            triggered_count = 0

            for strategy_name, strategy_config in strategies_config.get('strategies', {}).items():
                if not strategy_config.get('enabled', False):
                    continue
                required_indicators = strategy_config.get('required_indicators', [])
                if indicator_data['indicator_name'] in required_indicators:
                    if strategy_config.get('connection') == indicator_data['connection_name']:
                        await self.message_publisher.publish_strategy_recalc_request({
                            'strategy_name': strategy_name,
                            'connection_name': indicator_data['connection_name'],
                            'timestamp': indicator_data['timestamp'],
                            'trigger_reason': 'indicator_update'
                        })
                        triggered_count += 1

            self.logger.debug(f"Triggered {triggered_count} strategy calculations for indicator {indicator_data['indicator_name']}")

        except Exception as e:
            self.logger.error(f"Error triggering strategy calculations: {e}")

    async def _trigger_strategy_calculations_bulk(self, indicators_list: List[Dict[str, Any]]):
        """Trigger strategy range calculations after saving a batch of indicators.

        Instead of N individual strategy_recalc messages, publishes one
        strategy_calculation_request per affected (strategy, connection) pair
        covering the full timestamp range of the batch.
        """
        try:
            strategies_config = self.config_manager.get_config('strategies')

            # Map each (strategy_name, connection_name) → (min_ts, max_ts)
            affected: Dict[tuple, list] = {}
            for ind in indicators_list:
                ind_name = ind['indicator_name']
                connection_name = ind['connection_name']
                ts = ind['timestamp']

                for strategy_name, strategy_config in strategies_config.get('strategies', {}).items():
                    if not strategy_config.get('enabled', False):
                        continue
                    if ind_name not in strategy_config.get('required_indicators', []):
                        continue
                    if strategy_config.get('connection') != connection_name:
                        continue
                    key = (strategy_name, connection_name)
                    if key not in affected:
                        affected[key] = [ts, ts]
                    else:
                        affected[key][0] = min(affected[key][0], ts)
                        affected[key][1] = max(affected[key][1], ts)

            for (strategy_name, connection_name), (start_ts, end_ts) in affected.items():
                await self.queue_client.publish_message(
                    'strategy_calculation_requests',
                    self.message_publisher._create_message('strategy_calculation_request', {
                        'strategy_name': strategy_name,
                        'connection_name': connection_name,
                        'start_timestamp': start_ts,
                        'end_timestamp': end_ts,
                        'batch_id': f"db_bulk_{int(datetime.now(timezone.utc).timestamp())}",
                        'trigger_reason': 'indicator_bulk_update'
                    })
                )
                self.logger.debug(
                    f"Range strategy request: {strategy_name} "
                    f"[{datetime.fromtimestamp(start_ts, tz=timezone.utc)} "
                    f"→ {datetime.fromtimestamp(end_ts, tz=timezone.utc)}]"
                )

        except Exception as e:
            self.logger.error(f"Error triggering bulk strategy calculations: {e}")
    
    async def start(self):
        """Start the database update service"""
        self.logger.info("Starting Database Update Service...")
        self.running = True
        
        try:
            # Keep service running
            while self.running:
                await asyncio.sleep(1)
                
                # Log statistics every 60 seconds
                if self.stats['candles_processed'] % 100 == 0 and self.stats['candles_processed'] > 0:
                    await self._log_statistics()
        
        except KeyboardInterrupt:
            self.logger.info("Shutdown signal received")
        finally:
            await self.cleanup()
    
    async def stop(self):
        """Stop the database update service"""
        self.logger.info("Stopping Database Update Service...")
        self.running = False
    
    async def _log_statistics(self):
        """Log service statistics"""
        uptime = datetime.now(timezone.utc) - self.stats['start_time']
        
        self.logger.info(f"Database Update Service Statistics:")
        self.logger.info(f"Uptime: {uptime}")
        self.logger.info(f"Candles processed: {self.stats['candles_processed']}")
        self.logger.info(f"Indicators processed: {self.stats['indicators_processed']}")
        self.logger.info(f"Strategy signals processed: {self.stats['strategy_signals_processed']}")
        self.logger.info(f"Database errors: {self.stats['database_errors']}")
        self.logger.info(f"Queue processing errors: {self.stats['queue_processing_errors']}")
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get service statistics"""
        uptime = datetime.now(timezone.utc) - self.stats['start_time']
        
        return {
            'uptime_seconds': uptime.total_seconds(),
            'candles_processed': self.stats['candles_processed'],
            'indicators_processed': self.stats['indicators_processed'],
            'strategy_signals_processed': self.stats['strategy_signals_processed'],
            'database_errors': self.stats['database_errors'],
            'queue_processing_errors': self.stats['queue_processing_errors'],
            'processing_rate': {
                'candles_per_hour': self.stats['candles_processed'] / max(uptime.total_seconds() / 3600, 1),
                'indicators_per_hour': self.stats['indicators_processed'] / max(uptime.total_seconds() / 3600, 1),
                'signals_per_hour': self.stats['strategy_signals_processed'] / max(uptime.total_seconds() / 3600, 1)
            }
        }
    
    async def cleanup(self):
        """Cleanup database update service"""
        self.logger.info("Cleaning up Database Update Service...")
        
        # Close database connections
        self.db_orm.close()
        
        # Log final statistics
        await self._log_statistics()
        
        self.logger.info("Database Update Service cleanup completed")


async def main():
    """Main function for running Database Update Service standalone"""
    import sys
    import os
    
    # Add project root to Python path
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger('DatabaseUpdateServiceMain')
    
    service = None
    queue_client = None
    database_manager = None
    
    try:
        # Initialize config manager
        config_manager = UniversalConfigManager()
        config_manager.load_all_configs()
        
        # Initialize database manager
        # Use environment variables for database connection
        db_config = {
            'database': {
                'host': os.getenv('DATABASE_HOST', 'localhost'),
                'port': int(os.getenv('DATABASE_PORT', '5432')),
                'name': os.getenv('DATABASE_NAME', 'trading_bot'),
                'user': os.getenv('DATABASE_USER', 'trading_bot'),
                'password': os.getenv('DATABASE_PASSWORD', 'trading_bot_pass'),
                'connection_pool_size': 50,
                'query_timeout': 30
            }
        }
        database_manager = DatabaseManager(db_config)
        await database_manager.initialize()
        
        # Initialize queue client
        rabbitmq_url = os.getenv('RABBITMQ_URL')
        if not rabbitmq_url:
            raise ValueError("RABBITMQ_URL environment variable is required")
        queue_client = RabbitMQClient(rabbitmq_url)
        await queue_client.connect()
        
        # Initialize database update service
        service = DatabaseUpdateService(config_manager, database_manager, queue_client)
        await service.initialize()
        
        # Start the service
        logger.info("Database Update Service is running. Press Ctrl+C to stop.")
        await service.start()
        
    except Exception as e:
        logger.error(f"Database Update Service failed: {e}")
        raise
    finally:
        # Cleanup
        if service:
            await service.cleanup()
        if queue_client:
            await queue_client.disconnect()
        if database_manager:
            await database_manager.cleanup()
        
        logger.info("Database Update Service shutdown completed")


if __name__ == "__main__":
    asyncio.run(main())