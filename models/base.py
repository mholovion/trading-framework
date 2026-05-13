#!/usr/bin/env python3
"""
SQLAlchemy Models
=================

ORM models for the trading bot. All time-series models (Candle, RealtimeCandle,
Indicator, StrategySignal, Strategy) use a composite PRIMARY KEY (id, timestamp)
required by TimescaleDB hypertables.

Referential integrity between hypertables is enforced at the application layer,
not via DB-level foreign keys, because TimescaleDB requires the partition column
in every unique/primary-key constraint and does not support FK references to a
non-unique single column of a hypertable.
"""

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Index, Integer, Numeric,
    PrimaryKeyConstraint, String, Text, UniqueConstraint, create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()


# ---------------------------------------------------------------------------
# Hypertable models — PRIMARY KEY (id, timestamp) required by TimescaleDB
# ---------------------------------------------------------------------------

class Candle(Base):
    """Closed OHLCV bar. Source can be 'exchange' (raw) or 'aggregated'."""

    __tablename__ = "candles"

    id                   = Column(BigInteger, autoincrement=True, nullable=False)
    exchange             = Column(String(50),  nullable=False)
    symbol               = Column(String(20),  nullable=False)
    timeframe            = Column(String(10),  nullable=False)
    timestamp            = Column(BigInteger,  nullable=False)   # Unix seconds
    open_price           = Column(Numeric(20, 8), nullable=False)
    high_price           = Column(Numeric(20, 8), nullable=False)
    low_price            = Column(Numeric(20, 8), nullable=False)
    close_price          = Column(Numeric(20, 8), nullable=False)
    volume               = Column(Numeric(20, 8), nullable=False)
    source_type          = Column(String(20),  nullable=False, default="exchange")
    source_timeframe     = Column(String(10),  nullable=True)
    aggregation_method   = Column(String(20),  nullable=True)
    source_candles_count = Column(Integer,     nullable=True)
    data_completeness    = Column(Numeric(5, 2), nullable=True)
    created_at           = Column(DateTime(timezone=True), server_default=func.now())
    updated_at           = Column(DateTime(timezone=True), server_default=func.now(),
                                  onupdate=func.now())

    __table_args__ = (
        PrimaryKeyConstraint("id", "timestamp"),
        UniqueConstraint(
            "exchange", "symbol", "timeframe", "timestamp", "source_timeframe",
            name="uq_candles_business_key",
        ),
        Index("idx_candles_lookup", "exchange", "symbol", "timeframe", "timestamp"),
        Index("idx_candles_source_type", "source_type", "source_timeframe"),
    )

    def __repr__(self) -> str:
        return (
            f"<Candle({self.exchange} {self.symbol} {self.timeframe} "
            f"ts={self.timestamp})>"
        )


class RealtimeCandle(Base):
    """Active (potentially unclosed) candle updated by the WebSocket feed."""

    __tablename__ = "candles_realtime"

    id           = Column(BigInteger, autoincrement=True, nullable=False)
    exchange     = Column(String(50),  nullable=False)
    symbol       = Column(String(20),  nullable=False)
    timeframe    = Column(String(10),  nullable=False)
    timestamp    = Column(BigInteger,  nullable=False)   # Unix seconds, bar open
    open_price   = Column(Numeric(20, 8), nullable=False)
    high_price   = Column(Numeric(20, 8), nullable=False)
    low_price    = Column(Numeric(20, 8), nullable=False)
    close_price  = Column(Numeric(20, 8), nullable=False)
    volume       = Column(Numeric(20, 8), nullable=False)
    volume_quote = Column(Numeric(20, 8), nullable=True)
    trades_count = Column(Integer,     nullable=True)
    is_closed    = Column(Boolean,     nullable=False, default=False)
    is_active    = Column(Boolean,     nullable=False, default=True)
    last_update  = Column(DateTime(timezone=True), nullable=True)
    server_time  = Column(BigInteger,  nullable=True)
    created_at   = Column(DateTime(timezone=True), server_default=func.now())
    updated_at   = Column(DateTime(timezone=True), server_default=func.now(),
                          onupdate=func.now())

    __table_args__ = (
        PrimaryKeyConstraint("id", "timestamp"),
        UniqueConstraint(
            "exchange", "symbol", "timeframe", "timestamp",
            name="uq_candles_realtime_business_key",
        ),
        Index("idx_candles_realtime_active",
              "exchange", "symbol", "is_active", "timestamp"),
    )

    def __repr__(self) -> str:
        return (
            f"<RealtimeCandle({self.exchange} {self.symbol} {self.timeframe} "
            f"ts={self.timestamp} closed={self.is_closed})>"
        )


class Indicator(Base):
    """Computed indicator value (RSI, MA, …) for a specific bar."""

    __tablename__ = "indicators"

    id               = Column(BigInteger, autoincrement=True, nullable=False)
    connection_name  = Column(String(100), nullable=False)
    indicator_name   = Column(String(100), nullable=False)
    exchange         = Column(String(50),  nullable=False)
    symbol           = Column(String(20),  nullable=False)
    timeframe        = Column(String(10),  nullable=False)
    timestamp        = Column(BigInteger,  nullable=False)   # Unix seconds
    value            = Column(Numeric(20, 8), nullable=True)
    meta_data        = Column(Text,        nullable=True)    # JSON
    # Logical reference to candles.id — no DB-level FK (hypertable constraint).
    source_candle_id = Column(BigInteger,  nullable=True)
    created_at       = Column(DateTime(timezone=True), server_default=func.now())
    updated_at       = Column(DateTime(timezone=True), server_default=func.now(),
                              onupdate=func.now())

    __table_args__ = (
        PrimaryKeyConstraint("id", "timestamp"),
        UniqueConstraint(
            "indicator_name", "exchange", "symbol", "timeframe", "timestamp",
            name="uq_indicators_business_key",
        ),
        Index("idx_indicators_lookup",
              "connection_name", "indicator_name", "timestamp"),
    )

    def __repr__(self) -> str:
        return (
            f"<Indicator({self.indicator_name} {self.connection_name} "
            f"ts={self.timestamp} val={self.value})>"
        )


class StrategySignal(Base):
    """Trading signal emitted by a strategy (BUY / SELL / HOLD)."""

    __tablename__ = "strategy_signals"

    id              = Column(BigInteger, autoincrement=True, nullable=False)
    strategy_name   = Column(String(100), nullable=False)
    connection_name = Column(String(100), nullable=False)
    signal_type     = Column(String(20),  nullable=False)   # BUY | SELL | HOLD
    timestamp       = Column(BigInteger,  nullable=False)   # Unix seconds
    confidence      = Column(Numeric(5, 4),  nullable=False)
    price           = Column(Numeric(20, 8), nullable=False)
    indicators_data = Column(Text, nullable=True)  # JSON snapshot
    meta_data       = Column(Text, nullable=True)  # JSON
    created_at      = Column(DateTime(timezone=True), server_default=func.now())
    updated_at      = Column(DateTime(timezone=True), server_default=func.now(),
                             onupdate=func.now())

    __table_args__ = (
        PrimaryKeyConstraint("id", "timestamp"),
        Index("idx_strategy_signals_lookup",
              "strategy_name", "connection_name", "timestamp"),
        Index("idx_strategy_signals_type", "signal_type", "timestamp"),
    )

    def __repr__(self) -> str:
        return (
            f"<StrategySignal({self.strategy_name} {self.signal_type} "
            f"ts={self.timestamp} conf={self.confidence})>"
        )


# ---------------------------------------------------------------------------
# ORM manager
# ---------------------------------------------------------------------------

class DatabaseORM:
    """SQLAlchemy engine + session factory."""

    def __init__(self, database_url: str) -> None:
        self.engine = create_engine(
            database_url,
            echo=False,
            pool_size=20,
            max_overflow=40,
            pool_timeout=60,
            pool_recycle=3600,
            pool_pre_ping=True,
            connect_args={"options": "-c jit=off"},
        )
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

    def create_tables(self) -> None:
        """Create any tables not yet present (idempotent)."""
        Base.metadata.create_all(bind=self.engine)

    def get_session(self):
        return self.SessionLocal()

    def close(self) -> None:
        self.engine.dispose()