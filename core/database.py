#!/usr/bin/env python3
"""
Database Manager - SQLAlchemy 2.0 async (asyncpg driver)
=========================================================

All public methods are async. Session context manager is async.
Engine uses postgresql+asyncpg:// — no blocking calls on the event loop.
"""

import asyncio
import logging
import os
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager

from sqlalchemy import select, text, func, and_, or_, desc, asc, update
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError

from core.exceptions import DatabaseError
from models.base import Base, Candle, RealtimeCandle, Indicator, StrategySignal


_TF_SECONDS: Dict[str, int] = {
    '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
    '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '8h': 28800, '12h': 43200,
    '1d': 86400, '3d': 259200, '1w': 604800,
}


def _seconds_to_pg_interval(seconds: int) -> str:
    if seconds % 604800 == 0:
        return f"{seconds // 604800} weeks"
    if seconds % 86400 == 0:
        return f"{seconds // 86400} days"
    if seconds % 3600 == 0:
        return f"{seconds // 3600} hours"
    return f"{seconds // 60} minutes"


class DatabaseManager:
    """
    Async database manager using SQLAlchemy 2.0 + asyncpg driver.
    All public methods are coroutines — use `await` at call sites.
    """

    def __init__(self, config: Dict[str, Any], aggregation_rules: Optional[List] = None):
        self.config = config['database']
        self._aggregation_rules = aggregation_rules or []
        self.engine = None
        self._session_factory = None
        self.logger = logging.getLogger(__name__)
        self._initialized = False
        self._init_lock = asyncio.Lock()

    async def initialize(self):
        """Create async engine, session factory, tables, and continuous aggregates."""
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:  # double-checked locking
                return
            try:
                connection_url = (
                    f"postgresql+asyncpg://{self.config['user']}:{self.config['password']}"
                    f"@{self.config['host']}:{self.config['port']}/{self.config['name']}"
                )

                # jit=off: PostgreSQL JIT adds 10-30 s overhead for aggregate queries
                # on small-to-medium tables with no measurable benefit.
                self.engine = create_async_engine(
                    connection_url,
                    pool_size=self.config.get('connection_pool_size', 20),
                    max_overflow=self.config.get('max_overflow', 10),
                    pool_timeout=self.config.get('query_timeout', 30),
                    pool_recycle=3600,
                    echo=False,
                    connect_args={"server_settings": {"jit": "off"}},
                )

                self._session_factory = async_sessionmaker(
                    self.engine, expire_on_commit=False, class_=AsyncSession
                )

                # Create ORM-mapped tables
                async with self.engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)

                # Create ml_features hypertable (raw SQL — JSONB + hypertable, not in ORM)
                await self._ensure_ml_features_table()

                # Apply continuous aggregates from config
                if self._aggregation_rules:
                    await self._apply_continuous_aggregates()

                # Smoke test
                async with self._session_factory() as session:
                    await session.execute(text('SELECT 1'))

                self._initialized = True
                self.logger.info("Async database engine initialised (asyncpg)")

            except Exception as e:
                raise DatabaseError(f"Failed to initialise database: {e}")

    async def _ensure_ml_features_table(self):
        """Create ml_features hypertable if it doesn't exist.

        Not in ORM models because it uses JSONB and requires create_hypertable().
        All inserts/updates go through raw SQL anyway.
        """
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS ml_features (
                        timestamp     BIGINT NOT NULL,
                        exchange      VARCHAR(50) NOT NULL,
                        symbol        VARCHAR(20) NOT NULL,
                        timeframe     VARCHAR(10) NOT NULL,
                        open_price    FLOAT8,
                        high_price    FLOAT8,
                        low_price     FLOAT8,
                        close_price   FLOAT8,
                        volume        FLOAT8,
                        features      JSONB NOT NULL DEFAULT '{}',
                        trade_pnl_pct FLOAT8,
                        trade_label   SMALLINT,
                        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (exchange, symbol, timeframe, timestamp)
                    )
                """))
                await conn.execute(text("""
                    SELECT create_hypertable('ml_features', 'timestamp',
                        chunk_time_interval => 604800, if_not_exists => TRUE)
                """))
                await conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_ml_features_lookup
                    ON ml_features (exchange, symbol, timeframe, timestamp DESC)
                """))
            self.logger.info("ml_features hypertable ready")
        except Exception as e:
            self.logger.warning(f"Could not ensure ml_features table: {e}")

    async def _apply_continuous_aggregates(self):
        """Create TimescaleDB continuous aggregate views from aggregation_rules config.

        Rules are applied in topological order so every source view exists before
        any view that depends on it. Safe to call on every startup — all DDL uses
        IF NOT EXISTS / if_not_exists => TRUE.
        """
        # Build depth map for topological ordering
        depths: Dict[str, int] = {}
        source_for: Dict[str, str] = {}  # target_tf → source_tf

        for rule in self._aggregation_rules:
            src = rule['source']
            src_depth = depths.get(src, 0)
            for tgt in rule.get('targets', []):
                source_for[tgt] = src
                depths[tgt] = src_depth + 1

        ordered = sorted(source_for.keys(), key=lambda t: depths.get(t, 0))

        # Integer-based hypertables need a custom "now" function for continuous aggregates.
        # Use PL/pgSQL DO block so concurrent calls from multiple processes don't race —
        # the EXCEPTION handler silently absorbs "duplicate_object" and lock conflicts.
        try:
            autocommit_engine = self.engine.execution_options(isolation_level="AUTOCOMMIT")
            async with autocommit_engine.connect() as conn:
                await conn.execute(text("""
                    DO $$
                    BEGIN
                        CREATE OR REPLACE FUNCTION unix_now() RETURNS BIGINT
                            LANGUAGE SQL STABLE AS $f$ SELECT EXTRACT(epoch FROM NOW())::BIGINT $f$;
                        PERFORM set_integer_now_func('candles', 'unix_now', replace_if_exists => TRUE);
                    EXCEPTION WHEN others THEN
                        NULL;  -- concurrent process already did this, ignore
                    END
                    $$
                """))
            self.logger.info("Integer now function registered on candles hypertable")
        except Exception as e:
            self.logger.warning(f"Could not register integer now function: {e}")

        for tgt_tf in ordered:
            src_tf = source_for[tgt_tf]
            tgt_sec = _TF_SECONDS.get(tgt_tf)
            if not tgt_sec:
                self.logger.warning(f"Unknown timeframe '{tgt_tf}' in aggregation rules, skipping")
                continue

            view_name = f"candles_{tgt_tf}"
            src_name = "candles" if src_tf == '1m' else f"candles_{src_tf}"
            where = "\n                WHERE timeframe = '1m' AND source_type = 'exchange'" if src_tf == '1m' else ""

            schedule = _seconds_to_pg_interval(tgt_sec)

            create_sql = text(f"""
                CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name}
                WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
                SELECT
                    time_bucket({tgt_sec}, timestamp) AS timestamp,
                    exchange,
                    symbol,
                    first(open_price, timestamp) AS open_price,
                    max(high_price)              AS high_price,
                    min(low_price)               AS low_price,
                    last(close_price, timestamp) AS close_price,
                    sum(volume)                  AS volume
                FROM {src_name}{where}
                GROUP BY 1, 2, 3
            """)

            # Integer-based hypertables require BIGINT offsets (seconds), not INTERVAL
            policy_sql = text(f"""
                SELECT add_continuous_aggregate_policy('{view_name}',
                    start_offset      => {tgt_sec * 4}::BIGINT,
                    end_offset        => {tgt_sec}::BIGINT,
                    schedule_interval => INTERVAL '{schedule}',
                    if_not_exists     => TRUE)
            """)

            try:
                # TimescaleDB continuous aggregates cannot run inside a transaction block
                autocommit_engine = self.engine.execution_options(isolation_level="AUTOCOMMIT")
                async with autocommit_engine.connect() as conn:
                    await conn.execute(create_sql)
                    await conn.execute(policy_sql)
                self.logger.info(f"Continuous aggregate ready: {view_name} ← {src_name}")
            except Exception as e:
                self.logger.warning(f"Could not apply continuous aggregate '{view_name}': {e}")

    def close(self):
        """Dispose engine (sync entry point used by orchestrator teardown)."""
        if self.engine:
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.engine.dispose())
                else:
                    loop.run_until_complete(self.engine.dispose())
            except Exception:
                pass
            self.logger.info("Database engine closed")

    @asynccontextmanager
    async def get_session(self) -> AsyncSession:
        """Async session context manager with automatic rollback on error."""
        if not self._initialized:
            raise DatabaseError("Database not initialised")
        async with self._session_factory() as session:
            try:
                yield session
            except Exception as e:
                await session.rollback()
                self.logger.error(f"Database session error: {e}")
                raise DatabaseError(f"Database operation failed: {e}")

    # ── Candle operations ────────────────────────────────────────────────────

    async def store_candle(self, exchange: str, symbol: str, timeframe: str,
                           candle_data: Dict, server_time: Optional[int] = None,
                           source_type: str = 'exchange',
                           source_timeframe: Optional[str] = None,
                           aggregation_method: Optional[str] = None,
                           source_candles_count: Optional[int] = None,
                           data_completeness: Optional[float] = None) -> bool:
        """Upsert single candle."""
        try:
            async with self.get_session() as session:
                result = await session.execute(
                    select(Candle).where(
                        Candle.exchange == exchange,
                        Candle.symbol == symbol,
                        Candle.timeframe == timeframe,
                        Candle.timestamp == candle_data['timestamp'],
                        Candle.source_timeframe == source_timeframe,
                    )
                )
                existing = result.scalar_one_or_none()

                if existing:
                    existing.open_price = float(candle_data['open'])
                    existing.high_price = float(candle_data['high'])
                    existing.low_price = float(candle_data['low'])
                    existing.close_price = float(candle_data['close'])
                    existing.volume = float(candle_data['volume'])
                    existing.source_candles_count = source_candles_count
                    existing.data_completeness = data_completeness
                    existing.updated_at = datetime.now(timezone.utc)
                else:
                    session.add(Candle(
                        exchange=exchange, symbol=symbol, timeframe=timeframe,
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
                        data_completeness=data_completeness,
                    ))

                await session.commit()
                return True

        except Exception as e:
            self.logger.error(f"Error storing candle: {e}")
            return False

    async def store_candles_batch(self, exchange: str, symbol: str, timeframe: str,
                                  candles: List[Dict], server_time: Optional[int] = None,
                                  source_type: str = 'exchange',
                                  source_timeframe: Optional[str] = None,
                                  aggregation_method: Optional[str] = None,
                                  source_candles_count: Optional[int] = None,
                                  data_completeness: Optional[float] = None) -> int:
        """Bulk-upsert candles via INSERT … ON CONFLICT."""
        if not candles:
            return 0
        try:
            now = datetime.now(timezone.utc)
            rows = [
                {
                    "exchange": exchange, "symbol": symbol, "timeframe": timeframe,
                    "timestamp": int(c["timestamp"]),
                    "open_price": float(c["open"]), "high_price": float(c["high"]),
                    "low_price": float(c["low"]),   "close_price": float(c["close"]),
                    "volume": float(c["volume"]),
                    "source_type": source_type, "source_timeframe": source_timeframe,
                    "aggregation_method": aggregation_method,
                    "source_candles_count": source_candles_count,
                    "data_completeness": data_completeness,
                    "updated_at": now,
                }
                for c in candles
            ]
            stmt = pg_insert(Candle).values(rows)
            if source_timeframe is None:
                stmt = stmt.on_conflict_do_update(
                    index_elements=["exchange", "symbol", "timeframe", "timestamp"],
                    index_where=Candle.source_timeframe.is_(None),
                    set_={
                        "open_price": stmt.excluded.open_price,
                        "high_price": stmt.excluded.high_price,
                        "low_price":  stmt.excluded.low_price,
                        "close_price": stmt.excluded.close_price,
                        "volume":     stmt.excluded.volume,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
            else:
                stmt = stmt.on_conflict_do_update(
                    constraint="uq_candles_business_key",
                    set_={
                        "open_price": stmt.excluded.open_price,
                        "high_price": stmt.excluded.high_price,
                        "low_price":  stmt.excluded.low_price,
                        "close_price": stmt.excluded.close_price,
                        "volume":     stmt.excluded.volume,
                        "updated_at": stmt.excluded.updated_at,
                    },
                )
            async with self.get_session() as session:
                await session.execute(stmt)
                await session.commit()
            return len(rows)
        except Exception as e:
            self.logger.error(f"Error storing candles batch: {e}")
            return 0

    async def store_realtime_candle(self, exchange: str, symbol: str, timeframe: str,
                                    candle_data: Dict, is_closed: bool = False) -> bool:
        """Store or update real-time (unclosed) candle."""
        try:
            async with self.get_session() as session:
                result = await session.execute(
                    select(RealtimeCandle).where(
                        RealtimeCandle.exchange == exchange,
                        RealtimeCandle.symbol == symbol,
                        RealtimeCandle.timeframe == timeframe,
                        RealtimeCandle.timestamp == candle_data['timestamp'],
                    )
                )
                existing = result.scalar_one_or_none()
                now = datetime.now(timezone.utc)

                if existing:
                    if float(candle_data['high']) > existing.high_price:
                        existing.high_price = float(candle_data['high'])
                    if float(candle_data['low']) < existing.low_price:
                        existing.low_price = float(candle_data['low'])
                    existing.close_price = float(candle_data['close'])
                    existing.volume = float(candle_data['volume'])
                    existing.is_closed = is_closed
                    existing.updated_at = now
                    existing.last_update = now
                    if is_closed and not existing.is_closed:
                        await self._move_to_historical(session, existing)
                        existing.is_active = False
                else:
                    rt = RealtimeCandle(
                        exchange=exchange, symbol=symbol, timeframe=timeframe,
                        timestamp=candle_data['timestamp'],
                        open_price=float(candle_data['open']),
                        high_price=float(candle_data['high']),
                        low_price=float(candle_data['low']),
                        close_price=float(candle_data['close']),
                        volume=float(candle_data['volume']),
                        is_closed=is_closed,
                        is_active=not is_closed,
                        last_update=now,
                    )
                    session.add(rt)
                    if is_closed:
                        await session.flush()
                        await self._move_to_historical(session, rt)
                        rt.is_active = False

                await session.commit()
                return True

        except Exception as e:
            self.logger.error(f"Error storing realtime candle: {e}")
            return False

    async def _move_to_historical(self, session: AsyncSession, rt: RealtimeCandle):
        """Copy closed realtime candle to historical candles table."""
        try:
            result = await session.execute(
                select(Candle).where(
                    Candle.exchange == rt.exchange,
                    Candle.symbol == rt.symbol,
                    Candle.timeframe == rt.timeframe,
                    Candle.timestamp == rt.timestamp,
                )
            )
            existing = result.scalar_one_or_none()
            if existing:
                existing.open_price  = rt.open_price
                existing.high_price  = rt.high_price
                existing.low_price   = rt.low_price
                existing.close_price = rt.close_price
                existing.volume      = rt.volume
                existing.updated_at  = datetime.now(timezone.utc)
            else:
                session.add(Candle(
                    exchange=rt.exchange, symbol=rt.symbol, timeframe=rt.timeframe,
                    timestamp=rt.timestamp,
                    open_price=rt.open_price, high_price=rt.high_price,
                    low_price=rt.low_price, close_price=rt.close_price,
                    volume=rt.volume,
                ))
            self.logger.info(f"Moved closed candle to historical: {rt.exchange} {rt.symbol} {rt.timeframe} {rt.timestamp}")
        except Exception as e:
            self.logger.error(f"Error moving realtime candle to historical: {e}")
            raise

    async def get_active_realtime_candles(self, exchange: str, symbol: str,
                                           timeframe: str) -> List[Dict]:
        try:
            async with self.get_session() as session:
                result = await session.execute(
                    select(RealtimeCandle).where(
                        RealtimeCandle.exchange == exchange,
                        RealtimeCandle.symbol == symbol,
                        RealtimeCandle.timeframe == timeframe,
                        RealtimeCandle.is_active == True,
                        RealtimeCandle.is_closed == False,
                    ).order_by(desc(RealtimeCandle.timestamp))
                )
                return [
                    {
                        'id': c.id, 'timestamp': c.timestamp,
                        'open_price': float(c.open_price), 'high_price': float(c.high_price),
                        'low_price': float(c.low_price),   'close_price': float(c.close_price),
                        'volume': float(c.volume), 'is_closed': c.is_closed,
                        'created_at': c.created_at, 'updated_at': c.updated_at,
                    }
                    for c in result.scalars().all()
                ]
        except Exception as e:
            self.logger.error(f"Error getting active realtime candles: {e}")
            return []

    async def cleanup_old_realtime_candles(self, hours_old: int = 24) -> int:
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_old)
            async with self.get_session() as session:
                result = await session.execute(
                    select(RealtimeCandle).where(
                        or_(RealtimeCandle.is_active == False,
                            RealtimeCandle.updated_at < cutoff)
                    )
                )
                rows = result.scalars().all()
                for row in rows:
                    await session.delete(row)
                await session.commit()
                self.logger.info(f"Cleaned up {len(rows)} old realtime candles")
                return len(rows)
        except Exception as e:
            self.logger.error(f"Error cleaning up realtime candles: {e}")
            return 0

    async def get_candles_range(self, exchange: str, symbol: str, timeframe: str,
                                start_timestamp: int, end_timestamp: int,
                                limit: Optional[int] = None) -> List[Dict]:
        try:
            async with self.get_session() as session:
                q = select(Candle).where(
                    Candle.exchange == exchange,
                    Candle.symbol == symbol,
                    Candle.timeframe == timeframe,
                    Candle.timestamp >= start_timestamp,
                    Candle.timestamp <= end_timestamp,
                ).order_by(asc(Candle.timestamp))
                if limit:
                    q = q.limit(limit)
                result = await session.execute(q)
                return [
                    {
                        'timestamp': c.timestamp,
                        'open': float(c.open_price), 'high': float(c.high_price),
                        'low':  float(c.low_price),  'close': float(c.close_price),
                        'volume': float(c.volume),
                    }
                    for c in result.scalars().all()
                ]
        except Exception as e:
            self.logger.error(f"Error getting candles range: {e}")
            return []

    async def get_latest_candle(self, exchange: str, symbol: str, timeframe: str,
                                source_type: str = None,
                                source_timeframe: str = None) -> Optional[Dict]:
        try:
            async with self.get_session() as session:
                q = select(Candle).where(
                    Candle.exchange == exchange,
                    Candle.symbol == symbol,
                    Candle.timeframe == timeframe,
                )
                if source_type:
                    q = q.where(Candle.source_type == source_type)
                if source_timeframe:
                    q = q.where(Candle.source_timeframe == source_timeframe)
                q = q.order_by(desc(Candle.timestamp))
                result = await session.execute(q)
                c = result.scalars().first()
                if not c:
                    return None
                return {
                    'timestamp': c.timestamp,
                    'open': float(c.open_price), 'high': float(c.high_price),
                    'low':  float(c.low_price),  'close': float(c.close_price),
                    'volume': float(c.volume),
                    'source_type': c.source_type,
                    'source_timeframe': c.source_timeframe,
                    'aggregation_method': c.aggregation_method,
                    'source_candles_count': c.source_candles_count,
                    'data_completeness': float(c.data_completeness) if c.data_completeness else None,
                }
        except Exception as e:
            self.logger.error(f"Error getting latest candle: {e}")
            return None

    async def get_candles(self, exchange: str, symbol: str, timeframe: str,
                          limit: int = 100, offset: int = 0) -> List[Dict]:
        try:
            async with self.get_session() as session:
                result = await session.execute(
                    select(Candle).where(
                        Candle.exchange == exchange,
                        Candle.symbol == symbol,
                        Candle.timeframe == timeframe,
                    ).order_by(desc(Candle.timestamp)).limit(limit).offset(offset)
                )
                return [
                    {
                        'timestamp': c.timestamp,
                        'open': float(c.open_price), 'high': float(c.high_price),
                        'low':  float(c.low_price),  'close': float(c.close_price),
                        'volume': float(c.volume),
                    }
                    for c in result.scalars().all()
                ]
        except Exception as e:
            self.logger.error(f"Error getting candles: {e}")
            return []

    # ── Indicator operations ─────────────────────────────────────────────────

    async def store_indicator_value(self, indicator_name: str, exchange: str, symbol: str,
                                    timeframe: str, timestamp: int, value: float,
                                    meta_data: Optional[Dict] = None) -> bool:
        try:
            async with self.get_session() as session:
                # Find source candle (logical ref)
                src_result = await session.execute(
                    select(Candle.id).where(
                        Candle.exchange == exchange, Candle.symbol == symbol,
                        Candle.timeframe == timeframe, Candle.timestamp == timestamp,
                    )
                )
                src_id = src_result.scalar_one_or_none()
                if src_id is None:
                    self.logger.warning(f"Source candle not found for {indicator_name}@{timestamp}")
                    return False

                result = await session.execute(
                    select(Indicator).where(
                        Indicator.indicator_name == indicator_name,
                        Indicator.exchange == exchange,
                        Indicator.symbol == symbol,
                        Indicator.timeframe == timeframe,
                        Indicator.timestamp == timestamp,
                    )
                )
                existing = result.scalar_one_or_none()
                if existing:
                    existing.value = value
                    existing.meta_data = meta_data
                    existing.updated_at = datetime.now(timezone.utc)
                else:
                    session.add(Indicator(
                        indicator_name=indicator_name, exchange=exchange,
                        symbol=symbol, timeframe=timeframe, timestamp=timestamp,
                        value=value, meta_data=meta_data, source_candle_id=src_id,
                    ))
                await session.commit()
                return True
        except Exception as e:
            self.logger.error(f"Error storing indicator value: {e}")
            return False

    async def get_indicator_values(self, indicator_name: str, exchange: str, symbol: str,
                                   timeframe: str, limit: int = 100,
                                   start_ts: int = None, end_ts: int = None) -> List[Dict]:
        try:
            async with self.get_session() as session:
                q = select(Indicator).where(
                    Indicator.indicator_name == indicator_name,
                    Indicator.exchange == exchange,
                    Indicator.symbol == symbol,
                    Indicator.timeframe == timeframe,
                )
                if start_ts is not None:
                    q = q.where(Indicator.timestamp >= start_ts)
                if end_ts is not None:
                    q = q.where(Indicator.timestamp <= end_ts)

                if start_ts is not None or end_ts is not None:
                    q = q.order_by(asc(Indicator.timestamp))
                else:
                    q = q.order_by(desc(Indicator.timestamp)).limit(limit)

                result = await session.execute(q)
                indicators = result.scalars().all()
                if start_ts is None and end_ts is None:
                    indicators = list(reversed(indicators))
                return [
                    {'timestamp': i.timestamp, 'value': float(i.value), 'meta_data': i.meta_data}
                    for i in indicators
                ]
        except Exception as e:
            self.logger.error(f"Error getting indicator values: {e}")
            return []

    # ── Strategy signals ─────────────────────────────────────────────────────

    async def get_strategy_signals(self, strategy_name: str, exchange: str, symbol: str,
                                   timeframe: str, limit: int = 100) -> List[Dict]:
        try:
            if '_' in symbol:
                base, quote = symbol.split('_', 1)
                connection_name = f"{base.lower()}_{quote.lower()}_{timeframe}"
            else:
                connection_name = f"{symbol.lower()}_{exchange.lower()}_{timeframe}"

            async with self.get_session() as session:
                result = await session.execute(
                    select(StrategySignal).where(
                        StrategySignal.strategy_name == strategy_name,
                        StrategySignal.connection_name == connection_name,
                    ).order_by(desc(StrategySignal.timestamp)).limit(limit)
                )
                return [
                    {
                        'timestamp': s.timestamp,
                        'signal': s.signal_type,
                        'confidence': float(s.confidence),
                        'price': float(s.price),
                        'meta_data': s.meta_data,
                        'created_at': s.created_at.isoformat() if s.created_at else None,
                    }
                    for s in result.scalars().all()
                ]
        except Exception as e:
            self.logger.error(f"Error getting strategy signals: {e}")
            return []

    async def get_all_strategy_signals(self, limit: int = 1000):
        """Return ORM objects (used by chart display)."""
        try:
            async with self.get_session() as session:
                result = await session.execute(
                    select(StrategySignal).where(
                        StrategySignal.signal_type.in_(['buy', 'sell', 'BUY', 'SELL'])
                    ).order_by(desc(StrategySignal.timestamp)).limit(limit)
                )
                return result.scalars().all()
        except Exception as e:
            self.logger.error(f"Error getting all strategy signals: {e}")
            return None

    # ── Source / stats ───────────────────────────────────────────────────────

    async def get_candles_by_source(self, exchange: str, symbol: str, timeframe: str,
                                    source_type: str = None, source_timeframe: str = None,
                                    limit: int = 300,
                                    before_timestamp: int = None) -> List[Dict]:
        try:
            async with self.get_session() as session:
                q = select(Candle).where(
                    Candle.exchange == exchange,
                    Candle.symbol == symbol,
                    Candle.timeframe == timeframe,
                )
                if source_type:
                    q = q.where(Candle.source_type == source_type)
                if source_timeframe:
                    q = q.where(Candle.source_timeframe == source_timeframe)
                if before_timestamp is not None:
                    q = q.where(Candle.timestamp < before_timestamp)
                q = q.order_by(desc(Candle.timestamp)).limit(limit)
                result = await session.execute(q)
                return [
                    {
                        'timestamp': c.timestamp,
                        'open': float(c.open_price), 'high': float(c.high_price),
                        'low':  float(c.low_price),  'close': float(c.close_price),
                        'volume': float(c.volume),
                        'source_type': c.source_type,
                        'source_timeframe': c.source_timeframe,
                        'aggregation_method': c.aggregation_method,
                        'source_candles_count': c.source_candles_count,
                        'data_completeness': float(c.data_completeness) if c.data_completeness else None,
                    }
                    for c in result.scalars().all()
                ]
        except Exception as e:
            self.logger.error(f"Error getting candles by source: {e}")
            return []

    async def get_available_sources(self, exchange: str, symbol: str,
                                    timeframe: str) -> List[Dict]:
        try:
            async with self.get_session() as session:
                result = await session.execute(
                    select(
                        Candle.source_type, Candle.source_timeframe,
                        Candle.aggregation_method,
                        func.count(Candle.id).label('candles_count'),
                        func.max(Candle.timestamp).label('latest_timestamp'),
                        func.avg(Candle.data_completeness).label('avg_completeness'),
                    ).where(
                        Candle.exchange == exchange,
                        Candle.symbol == symbol,
                        Candle.timeframe == timeframe,
                    ).group_by(
                        Candle.source_type, Candle.source_timeframe, Candle.aggregation_method
                    )
                )
                return [
                    {
                        'source_type': r.source_type,
                        'source_timeframe': r.source_timeframe,
                        'aggregation_method': r.aggregation_method,
                        'candles_count': r.candles_count,
                        'latest_timestamp': r.latest_timestamp,
                        'avg_completeness': float(r.avg_completeness) if r.avg_completeness else None,
                    }
                    for r in result.all()
                ]
        except Exception as e:
            self.logger.error(f"Error getting available sources: {e}")
            return []

    async def get_database_stats(self) -> Dict[str, Any]:
        """Fast stats via TimescaleDB approximate_row_count."""
        try:
            async with self.get_session() as session:
                row = (await session.execute(text("""
                    SELECT
                        approximate_row_count('candles')          AS candles_count,
                        approximate_row_count('indicators')       AS indicators_count,
                        approximate_row_count('strategy_signals') AS strategies_count,
                        approximate_row_count('ml_features')      AS ml_features_count
                """))).fetchone()

                latest_ts = (await session.execute(text("""
                    SELECT MAX(range_start_integer) FROM timescaledb_information.chunks
                    WHERE hypertable_name = 'candles'
                """))).scalar() or 0

                timeframes = [r[0] for r in (await session.execute(
                    text("SELECT DISTINCT timeframe FROM candles WHERE timestamp >= :ts ORDER BY timeframe"),
                    {'ts': latest_ts}
                )).fetchall()]
                exchanges = [r[0] for r in (await session.execute(
                    text("SELECT DISTINCT exchange FROM candles WHERE timestamp >= :ts ORDER BY exchange"),
                    {'ts': latest_ts}
                )).fetchall()]

                return {
                    'candles_count':      int(row.candles_count or 0),
                    'indicators_count':   int(row.indicators_count or 0),
                    'strategies_count':   int(row.strategies_count or 0),
                    'ml_features_count':  int(row.ml_features_count or 0),
                    'timeframes': timeframes,
                    'exchanges':  exchanges,
                    'engine_pool_size':         self.engine.pool.size() if self.engine.pool else 0,
                    'engine_pool_checked_in':   self.engine.pool.checkedin() if self.engine.pool else 0,
                    'engine_pool_checked_out':  self.engine.pool.checkedout() if self.engine.pool else 0,
                }
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {}

    def candle_source_table(self, timeframe: str) -> str:
        """Return the SQL table or view name for the given timeframe.

        Timeframes configured as continuous aggregate targets read from their
        materialized view (candles_4h, candles_1d, …).  Everything else — including
        raw 1m exchange data — reads from the candles table.
        """
        configured = {t for rule in self._aggregation_rules for t in rule.get('targets', [])}
        return f"candles_{timeframe}" if timeframe in configured else 'candles'

    async def get_candles_from_source(self, exchange: str, symbol: str, timeframe: str,
                                       limit: int = 300,
                                       before_timestamp: int = None) -> List[Dict]:
        """Query OHLCV from the correct source: TimescaleDB view for aggregate TFs, candles table for raw."""
        try:
            src_table = self.candle_source_table(timeframe)
            tf_filter = "AND timeframe = :tf" if src_table == 'candles' else ""
            before_filter = "AND timestamp < :before" if before_timestamp is not None else ""
            params: Dict[str, Any] = {'ex': exchange, 'sym': symbol, 'tf': timeframe, 'lim': limit}
            if before_timestamp is not None:
                params['before'] = before_timestamp
            async with self.get_session() as session:
                result = await session.execute(text(f"""
                    SELECT timestamp, open_price, high_price, low_price, close_price, volume
                    FROM {src_table}
                    WHERE exchange = :ex AND symbol = :sym {tf_filter} {before_filter}
                    ORDER BY timestamp DESC LIMIT :lim
                """), params)
                return [
                    {
                        'timestamp': r[0],
                        'open':   float(r[1]),
                        'high':   float(r[2]),
                        'low':    float(r[3]),
                        'close':  float(r[4]),
                        'volume': float(r[5]),
                    }
                    for r in result.fetchall()
                ]
        except Exception as e:
            self.logger.error(f"Error in get_candles_from_source: {e}")
            return []

    async def health_check(self) -> bool:
        try:
            async with self.get_session() as session:
                await session.execute(text('SELECT 1'))
                return True
        except Exception as e:
            self.logger.error(f"Database health check failed: {e}")
            return False
