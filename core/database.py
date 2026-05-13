#!/usr/bin/env python3
"""
Database Manager - SQLAlchemy ORM
=================================

Production-ready database manager using SQLAlchemy ORM exclusively.
Replaced asyncpg with SQLAlchemy for unified ORM approach across all services.
"""

import logging
import os
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager

from sqlalchemy import create_engine, text, func, and_, or_, desc, asc
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.exceptions import DatabaseError
from models.base import Base, Candle, RealtimeCandle, Indicator, StrategySignal


class DatabaseManager:
    """
    Production database manager using SQLAlchemy ORM exclusively
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config['database']
        self.engine = None
        self.SessionLocal = None
        self.logger = logging.getLogger(__name__)
        self._initialized = False
    
    async def initialize(self):
        """Initialize SQLAlchemy engine and session factory"""
        try:
            # Build connection string for SQLAlchemy
            connection_url = (
                f"postgresql://{self.config['user']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}/{self.config['name']}"
            )
            
            # Create engine with connection pooling.
            # jit=off: PostgreSQL JIT compilation adds 10-30s overhead for aggregate queries
            # on small-to-medium tables — far exceeds any benefit it provides here.
            self.engine = create_engine(
                connection_url,
                poolclass=QueuePool,
                pool_size=self.config.get('connection_pool_size', 20),
                max_overflow=self.config.get('max_overflow', 10),
                pool_timeout=self.config.get('query_timeout', 30),
                pool_recycle=3600,
                echo=False,
                connect_args={"options": "-c jit=off"},
            )
            
            # Create session factory
            self.SessionLocal = sessionmaker(bind=self.engine)
            
            # Set initialized before testing connection
            self._initialized = True
            
            # Create tables if they don't exist
            Base.metadata.create_all(bind=self.engine)
            
            # Test connection
            with self.get_session() as session:
                session.execute(text('SELECT 1'))
                session.commit()
            self.logger.info("SQLAlchemy database engine initialized successfully")
            
        except Exception as e:
            raise DatabaseError(f"Failed to initialize database: {e}")
    
    def close(self):
        """Close database engine"""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database engine closed")
    
    @contextmanager
    def get_session(self) -> Session:
        """Get database session with automatic cleanup"""
        if not self._initialized:
            raise DatabaseError("Database not initialized")
        
        session = self.SessionLocal()
        try:
            yield session
        except Exception as e:
            session.rollback()
            self.logger.error(f"Database session error: {e}")
            raise DatabaseError(f"Database operation failed: {e}")
        finally:
            session.close()
    
    # Candle operations
    def store_candle(self, exchange: str, symbol: str, timeframe: str, 
                    candle_data: Dict, server_time: Optional[int] = None,
                    source_type: str = 'exchange', source_timeframe: Optional[str] = None,
                    aggregation_method: Optional[str] = None, source_candles_count: Optional[int] = None,
                    data_completeness: Optional[float] = None) -> bool:
        """Store single candle using ORM with source tracking"""
        try:
            with self.get_session() as session:
                # Check if candle exists with same source
                existing = session.query(Candle).filter(
                    Candle.exchange == exchange,
                    Candle.symbol == symbol,
                    Candle.timeframe == timeframe,
                    Candle.timestamp == candle_data['timestamp'],
                    Candle.source_timeframe == source_timeframe
                ).first()
                
                if existing:
                    # Update existing
                    existing.open_price = float(candle_data['open'])
                    existing.high_price = float(candle_data['high'])
                    existing.low_price = float(candle_data['low'])
                    existing.close_price = float(candle_data['close'])
                    existing.volume = float(candle_data['volume'])
                    existing.source_candles_count = source_candles_count
                    existing.data_completeness = data_completeness
                    existing.updated_at = datetime.now(timezone.utc)
                else:
                    # Create new
                    candle = Candle(
                        exchange=exchange,
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=candle_data['timestamp'],
                        open_price=float(candle_data['open']),
                        high_price=float(candle_data['high']),
                        low_price=float(candle_data['low']),
                        close_price=float(candle_data['close']),
                        volume=float(candle_data['volume']),
                        source_type=source_type,
                        source_timeframe=source_timeframe,
                        aggregation_method=aggregation_method,
                        source_candles_count=source_candles_count,
                        data_completeness=data_completeness
                    )
                    session.add(candle)
                
                session.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"Error storing candle: {e}")
            return False
    
    def store_candles_batch(self, exchange: str, symbol: str, timeframe: str,
                           candles: List[Dict], server_time: Optional[int] = None,
                           source_type: str = 'exchange', source_timeframe: Optional[str] = None,
                           aggregation_method: Optional[str] = None, source_candles_count: Optional[int] = None,
                           data_completeness: Optional[float] = None) -> int:
        """Bulk-upsert candles using a single INSERT ... ON CONFLICT statement."""
        if not candles:
            return 0
        try:
            now = datetime.now(timezone.utc)
            rows = [
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "timestamp": int(c["timestamp"]),
                    "open_price": float(c["open"]),
                    "high_price": float(c["high"]),
                    "low_price": float(c["low"]),
                    "close_price": float(c["close"]),
                    "volume": float(c["volume"]),
                    "source_type": source_type,
                    "source_timeframe": source_timeframe,
                    "aggregation_method": aggregation_method,
                    "source_candles_count": source_candles_count,
                    "data_completeness": data_completeness,
                    "updated_at": now,
                }
                for c in candles
            ]
            stmt = pg_insert(Candle).values(rows)
            # Use partial index for historical (source_timeframe IS NULL),
            # full business-key constraint for aggregated data.
            if source_timeframe is None:
                stmt = stmt.on_conflict_do_update(
                    index_elements=["exchange", "symbol", "timeframe", "timestamp"],
                    index_where=Candle.source_timeframe.is_(None),
                    set_={
                        "open_price": stmt.excluded.open_price,
                        "high_price": stmt.excluded.high_price,
                        "low_price": stmt.excluded.low_price,
                        "close_price": stmt.excluded.close_price,
                        "volume": stmt.excluded.volume,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
            else:
                stmt = stmt.on_conflict_do_update(
                    constraint="uq_candles_business_key",
                    set_={
                        "open_price": stmt.excluded.open_price,
                        "high_price": stmt.excluded.high_price,
                        "low_price": stmt.excluded.low_price,
                        "close_price": stmt.excluded.close_price,
                        "volume": stmt.excluded.volume,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
            with self.get_session() as session:
                session.execute(stmt)
                session.commit()
            return len(rows)
        except Exception as e:
            self.logger.error(f"Error storing candles batch: {e}")
            return 0
    
    # Realtime candle operations
    async def store_realtime_candle(self, exchange: str, symbol: str, timeframe: str, 
                                   candle_data: Dict, is_closed: bool = False) -> bool:
        """Store or update real-time candle data"""
        try:
            with self.get_session() as session:
                # Check if realtime candle exists
                existing = session.query(RealtimeCandle).filter(
                    RealtimeCandle.exchange == exchange,
                    RealtimeCandle.symbol == symbol,
                    RealtimeCandle.timeframe == timeframe,
                    RealtimeCandle.timestamp == candle_data['timestamp']
                ).first()
                
                current_time = datetime.now(timezone.utc)
                
                if existing:
                    # Update existing realtime candle with new OHLCV data
                    # Keep the original open price, update high/low/close/volume
                    if float(candle_data['high']) > existing.high_price:
                        existing.high_price = float(candle_data['high'])
                    if float(candle_data['low']) < existing.low_price:
                        existing.low_price = float(candle_data['low'])
                    
                    existing.close_price = float(candle_data['close'])
                    existing.volume = float(candle_data['volume'])
                    existing.is_closed = is_closed
                    existing.updated_at = current_time
                    existing.last_update = current_time
                    
                    # If candle is now closed, move to historical data
                    if is_closed and not existing.is_closed:
                        await self._move_to_historical(session, existing)
                        existing.is_active = False
                else:
                    # Create new realtime candle
                    realtime_candle = RealtimeCandle(
                        exchange=exchange,
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=candle_data['timestamp'],
                        open_price=float(candle_data['open']),
                        high_price=float(candle_data['high']),
                        low_price=float(candle_data['low']),
                        close_price=float(candle_data['close']),
                        volume=float(candle_data['volume']),
                        is_closed=is_closed,
                        is_active=not is_closed,
                        last_update=current_time
                    )
                    
                    session.add(realtime_candle)
                    
                    # If it's already closed, also move to historical
                    if is_closed:
                        session.flush()  # Get the ID
                        await self._move_to_historical(session, realtime_candle)
                        realtime_candle.is_active = False
                
                session.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"Error storing realtime candle: {e}")
            return False
    
    async def _move_to_historical(self, session: Session, realtime_candle: RealtimeCandle):
        """Move closed realtime candle to historical candles table"""
        try:
            # Check if historical candle already exists
            existing_historical = session.query(Candle).filter(
                Candle.exchange == realtime_candle.exchange,
                Candle.symbol == realtime_candle.symbol,
                Candle.timeframe == realtime_candle.timeframe,
                Candle.timestamp == realtime_candle.timestamp
            ).first()
            
            if existing_historical:
                # Update existing historical candle
                existing_historical.open_price = realtime_candle.open_price
                existing_historical.high_price = realtime_candle.high_price
                existing_historical.low_price = realtime_candle.low_price
                existing_historical.close_price = realtime_candle.close_price
                existing_historical.volume = realtime_candle.volume
                existing_historical.updated_at = datetime.now(timezone.utc)
            else:
                # Create new historical candle
                historical_candle = Candle(
                    exchange=realtime_candle.exchange,
                    symbol=realtime_candle.symbol,
                    timeframe=realtime_candle.timeframe,
                    timestamp=realtime_candle.timestamp,
                    open_price=realtime_candle.open_price,
                    high_price=realtime_candle.high_price,
                    low_price=realtime_candle.low_price,
                    close_price=realtime_candle.close_price,
                    volume=realtime_candle.volume
                )
                session.add(historical_candle)
            
            self.logger.info(f"Moved closed candle to historical: {realtime_candle.exchange} {realtime_candle.symbol} {realtime_candle.timeframe} {realtime_candle.timestamp}")
            
        except Exception as e:
            self.logger.error(f"Error moving realtime candle to historical: {e}")
            raise
    
    def get_active_realtime_candles(self, exchange: str, symbol: str, timeframe: str) -> List[Dict]:
        """Get active (unclosed) realtime candles"""
        try:
            with self.get_session() as session:
                candles = session.query(RealtimeCandle).filter(
                    RealtimeCandle.exchange == exchange,
                    RealtimeCandle.symbol == symbol,
                    RealtimeCandle.timeframe == timeframe,
                    RealtimeCandle.is_active == True,
                    RealtimeCandle.is_closed == False
                ).order_by(RealtimeCandle.timestamp.desc()).all()
                
                return [{
                    'id': candle.id,
                    'timestamp': candle.timestamp,
                    'open_price': float(candle.open_price),
                    'high_price': float(candle.high_price),
                    'low_price': float(candle.low_price),
                    'close_price': float(candle.close_price),
                    'volume': float(candle.volume),
                    'is_closed': candle.is_closed,
                    'created_at': candle.created_at,
                    'updated_at': candle.updated_at
                } for candle in candles]
                
        except Exception as e:
            self.logger.error(f"Error getting active realtime candles: {e}")
            return []
    
    def cleanup_old_realtime_candles(self, hours_old: int = 24) -> int:
        """Clean up old inactive realtime candles"""
        try:
            with self.get_session() as session:
                cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_old)
                
                deleted = session.query(RealtimeCandle).filter(
                    or_(
                        RealtimeCandle.is_active == False,
                        RealtimeCandle.updated_at < cutoff_time
                    )
                ).delete()
                
                session.commit()
                self.logger.info(f"Cleaned up {deleted} old realtime candles")
                return deleted
                
        except Exception as e:
            self.logger.error(f"Error cleaning up realtime candles: {e}")
            return 0
    
    def get_candles_range(self, exchange: str, symbol: str, timeframe: str,
                         start_timestamp: int, end_timestamp: int,
                         limit: Optional[int] = None) -> List[Dict]:
        """Get candles in timestamp range using ORM"""
        try:
            with self.get_session() as session:
                query = session.query(Candle).filter(
                    Candle.exchange == exchange,
                    Candle.symbol == symbol,
                    Candle.timeframe == timeframe,
                    Candle.timestamp >= start_timestamp,
                    Candle.timestamp <= end_timestamp
                ).order_by(asc(Candle.timestamp))
                
                if limit:
                    query = query.limit(limit)
                
                candles = query.all()
                
                # Convert to dict format
                result = []
                for candle in candles:
                    result.append({
                        'timestamp': candle.timestamp,
                        'open': float(candle.open_price),
                        'high': float(candle.high_price),
                        'low': float(candle.low_price),
                        'close': float(candle.close_price),
                        'volume': float(candle.volume)
                    })
                
                return result
                
        except Exception as e:
            self.logger.error(f"Error getting candles range: {e}")
            return []
    
    def get_latest_candle(self, exchange: str, symbol: str, timeframe: str,
                         source_type: str = None, source_timeframe: str = None) -> Optional[Dict]:
        """Get latest candle using ORM with optional source filtering"""
        try:
            with self.get_session() as session:
                query = session.query(Candle).filter(
                    Candle.exchange == exchange,
                    Candle.symbol == symbol,
                    Candle.timeframe == timeframe
                )
                
                # Add source filtering if specified
                if source_type:
                    query = query.filter(Candle.source_type == source_type)
                if source_timeframe:
                    query = query.filter(Candle.source_timeframe == source_timeframe)
                
                candle = query.order_by(desc(Candle.timestamp)).first()
                
                if candle:
                    return {
                        'timestamp': candle.timestamp,
                        'open': float(candle.open_price),
                        'high': float(candle.high_price),
                        'low': float(candle.low_price),
                        'close': float(candle.close_price),
                        'volume': float(candle.volume),
                        'source_type': candle.source_type,
                        'source_timeframe': candle.source_timeframe,
                        'aggregation_method': candle.aggregation_method,
                        'source_candles_count': candle.source_candles_count,
                        'data_completeness': float(candle.data_completeness) if candle.data_completeness else None
                    }
                
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting latest candle: {e}")
            return None
    
    def get_candles(self, exchange: str, symbol: str, timeframe: str,
                   limit: int = 100, offset: int = 0) -> List[Dict]:
        """Get candles with pagination using ORM"""
        try:
            with self.get_session() as session:
                candles = session.query(Candle).filter(
                    Candle.exchange == exchange,
                    Candle.symbol == symbol,
                    Candle.timeframe == timeframe
                ).order_by(desc(Candle.timestamp)).limit(limit).offset(offset).all()
                
                result = []
                for candle in candles:
                    result.append({
                        'timestamp': candle.timestamp,
                        'open': float(candle.open_price),
                        'high': float(candle.high_price),
                        'low': float(candle.low_price),
                        'close': float(candle.close_price),
                        'volume': float(candle.volume)
                    })
                
                return result
                
        except Exception as e:
            self.logger.error(f"Error getting candles: {e}")
            return []
    
    # Indicator operations
    def store_indicator_value(self, indicator_name: str, exchange: str, symbol: str,
                            timeframe: str, timestamp: int, value: float,
                            meta_data: Optional[Dict] = None) -> bool:
        """Store indicator value using ORM"""
        try:
            with self.get_session() as session:
                # Find source candle
                source_candle = session.query(Candle).filter(
                    Candle.exchange == exchange,
                    Candle.symbol == symbol,
                    Candle.timeframe == timeframe,
                    Candle.timestamp == timestamp
                ).first()
                
                if not source_candle:
                    self.logger.warning(f"Source candle not found for indicator {indicator_name}")
                    return False
                
                # Check if indicator value exists
                existing = session.query(Indicator).filter(
                    Indicator.indicator_name == indicator_name,
                    Indicator.exchange == exchange,
                    Indicator.symbol == symbol,
                    Indicator.timeframe == timeframe,
                    Indicator.timestamp == timestamp
                ).first()
                
                if existing:
                    # Update
                    existing.value = value
                    existing.meta_data = meta_data
                    existing.updated_at = datetime.now(timezone.utc)
                else:
                    # Create new
                    indicator = Indicator(
                        indicator_name=indicator_name,
                        exchange=exchange,
                        symbol=symbol,
                        timeframe=timeframe,
                        timestamp=timestamp,
                        value=value,
                        meta_data=meta_data,
                        source_candle_id=source_candle.id
                    )
                    session.add(indicator)
                
                session.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"Error storing indicator value: {e}")
            return False
    
    def get_indicator_values(self, indicator_name: str, exchange: str, symbol: str,
                           timeframe: str, limit: int = 100,
                           start_ts: int = None, end_ts: int = None) -> List[Dict]:
        """Get indicator values using ORM"""
        try:
            with self.get_session() as session:
                q = session.query(Indicator).filter(
                    Indicator.indicator_name == indicator_name,
                    Indicator.exchange == exchange,
                    Indicator.symbol == symbol,
                    Indicator.timeframe == timeframe
                )
                if start_ts is not None:
                    q = q.filter(Indicator.timestamp >= start_ts)
                if end_ts is not None:
                    q = q.filter(Indicator.timestamp <= end_ts)

                if start_ts is not None or end_ts is not None:
                    indicators = q.order_by(asc(Indicator.timestamp)).all()
                else:
                    indicators = q.order_by(desc(Indicator.timestamp)).limit(limit).all()
                    indicators = list(reversed(indicators))

                return [
                    {'timestamp': i.timestamp, 'value': float(i.value), 'meta_data': i.meta_data}
                    for i in indicators
                ]

        except Exception as e:
            self.logger.error(f"Error getting indicator values: {e}")
            return []
    
    def get_strategy_signals(self, strategy_name: str, exchange: str, symbol: str,
                           timeframe: str, limit: int = 100) -> List[Dict]:
        """Get strategy signals using ORM"""
        try:
            # Build connection_name - format is symbol_exchange_timeframe (e.g. sol_usdt_1m)
            # But API gets exchange=whitebit, symbol=SOL_USDT, so we need to parse correctly
            if '_' in symbol:
                # symbol is like "SOL_USDT", split it
                base, quote = symbol.split('_', 1)
                connection_name = f"{base.lower()}_{quote.lower()}_{timeframe}"
            else:
                # fallback format
                connection_name = f"{symbol.lower()}_{exchange.lower()}_{timeframe}"
            
            self.logger.debug(f"Looking for strategy signals with connection_name: {connection_name}")
            
            with self.get_session() as session:
                strategies = session.query(StrategySignal).filter(
                    StrategySignal.strategy_name == strategy_name,
                    StrategySignal.connection_name == connection_name
                ).order_by(desc(StrategySignal.timestamp)).limit(limit).all()
                
                result = []
                for strategy in strategies:
                    result.append({
                        'timestamp': strategy.timestamp,
                        'signal': strategy.signal_type,
                        'confidence': float(strategy.confidence),
                        'price': float(strategy.price),
                        'meta_data': strategy.meta_data,
                        'created_at': strategy.created_at.isoformat() if strategy.created_at else None
                    })
                
                return result
                
        except Exception as e:
            self.logger.error(f"Error getting strategy signals: {e}")
            return []

    def get_all_strategy_signals(self, limit: int = 1000):
        """Get all BUY/SELL strategy signals for chart display (HOLDs excluded)."""
        try:
            with self.get_session() as session:
                signals = session.query(StrategySignal).filter(
                    StrategySignal.signal_type.in_(['buy', 'sell', 'BUY', 'SELL'])
                ).order_by(
                    desc(StrategySignal.timestamp)
                ).limit(limit).all()

                return signals

        except Exception as e:
            self.logger.error(f"Error getting all strategy signals: {e}")
            return None
    
    # Statistics and monitoring
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics using TimescaleDB approximate counts (fast)."""
        try:
            from sqlalchemy import text
            with self.get_session() as session:
                # approximate_row_count uses TimescaleDB chunk stats — sub-millisecond
                row = session.execute(text("""
                    SELECT
                        approximate_row_count('candles') AS candles_count,
                        approximate_row_count('indicators') AS indicators_count,
                        approximate_row_count('strategy_signals') AS strategies_count
                """)).fetchone()

                # Restrict DISTINCT to latest chunk to avoid full scan
                latest_ts = session.execute(text("""
                    SELECT MAX(range_start_integer) FROM timescaledb_information.chunks
                    WHERE hypertable_name = 'candles'
                """)).scalar() or 0
                timeframes = [r[0] for r in session.execute(
                    text("SELECT DISTINCT timeframe FROM candles WHERE timestamp >= :ts ORDER BY timeframe"),
                    {'ts': latest_ts}
                ).fetchall()]
                exchanges = [r[0] for r in session.execute(
                    text("SELECT DISTINCT exchange FROM candles WHERE timestamp >= :ts ORDER BY exchange"),
                    {'ts': latest_ts}
                ).fetchall()]

                return {
                    'candles_count': int(row.candles_count or 0),
                    'indicators_count': int(row.indicators_count or 0),
                    'strategies_count': int(row.strategies_count or 0),
                    'timeframes': timeframes,
                    'exchanges': exchanges,
                    'engine_pool_size': self.engine.pool.size() if self.engine.pool else 0,
                    'engine_pool_checked_in': self.engine.pool.checkedin() if self.engine.pool else 0,
                    'engine_pool_checked_out': self.engine.pool.checkedout() if self.engine.pool else 0
                }

        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {}
    
    # Enhanced methods for source tracking
    def get_candles_by_source(self, exchange: str, symbol: str, timeframe: str,
                             source_type: str = None, source_timeframe: str = None,
                             limit: int = 300, before_timestamp: int = None) -> List[Dict]:
        """Get candles filtered by source type and timeframe"""
        try:
            with self.get_session() as session:
                query = session.query(Candle).filter(
                    Candle.exchange == exchange,
                    Candle.symbol == symbol,
                    Candle.timeframe == timeframe
                )

                if source_type:
                    query = query.filter(Candle.source_type == source_type)
                if source_timeframe:
                    query = query.filter(Candle.source_timeframe == source_timeframe)
                if before_timestamp is not None:
                    query = query.filter(Candle.timestamp < before_timestamp)

                candles = query.order_by(desc(Candle.timestamp)).limit(limit).all()
                
                result = []
                for candle in candles:
                    result.append({
                        'timestamp': candle.timestamp,
                        'open': float(candle.open_price),
                        'high': float(candle.high_price),
                        'low': float(candle.low_price),
                        'close': float(candle.close_price),
                        'volume': float(candle.volume),
                        'source_type': candle.source_type,
                        'source_timeframe': candle.source_timeframe,
                        'aggregation_method': candle.aggregation_method,
                        'source_candles_count': candle.source_candles_count,
                        'data_completeness': float(candle.data_completeness) if candle.data_completeness else None
                    })
                
                return result
                
        except Exception as e:
            self.logger.error(f"Error getting candles by source: {e}")
            return []
    
    def get_available_sources(self, exchange: str, symbol: str, timeframe: str) -> List[Dict]:
        """Get all available data sources for a timeframe"""
        try:
            with self.get_session() as session:
                sources = session.query(
                    Candle.source_type,
                    Candle.source_timeframe,
                    Candle.aggregation_method,
                    func.count(Candle.id).label('candles_count'),
                    func.max(Candle.timestamp).label('latest_timestamp'),
                    func.avg(Candle.data_completeness).label('avg_completeness')
                ).filter(
                    Candle.exchange == exchange,
                    Candle.symbol == symbol,
                    Candle.timeframe == timeframe
                ).group_by(
                    Candle.source_type,
                    Candle.source_timeframe,
                    Candle.aggregation_method
                ).all()
                
                result = []
                for source in sources:
                    result.append({
                        'source_type': source.source_type,
                        'source_timeframe': source.source_timeframe,
                        'aggregation_method': source.aggregation_method,
                        'candles_count': source.candles_count,
                        'latest_timestamp': source.latest_timestamp,
                        'avg_completeness': float(source.avg_completeness) if source.avg_completeness else None
                    })
                
                return result
                
        except Exception as e:
            self.logger.error(f"Error getting available sources: {e}")
            return []

    # Health check
    def health_check(self) -> bool:
        """Check database health"""
        try:
            with self.get_session() as session:
                session.execute(text('SELECT 1'))
                return True
        except Exception as e:
            self.logger.error(f"Database health check failed: {e}")
            return False