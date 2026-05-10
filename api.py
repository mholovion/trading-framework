#!/usr/bin/env python3
"""
Trading Bot API v3 - Simplified with DatabaseManager
====================================================

Clean, simple API that uses only DatabaseManager methods.
No direct SQL queries, much cleaner code.
"""

from aiohttp import web
import aiohttp_jinja2
import jinja2
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

# Add project root to Python path
sys.path.append(str(Path(__file__).parent))

from core.universal_config_manager import UniversalConfigManager
from core.logging_config import setup_service_logging
from core.database import DatabaseManager
import time as _time

# Simple in-memory TTL cache for expensive aggregation queries
_api_cache: dict = {}

def _get_cached(key: str, ttl: float):
    entry = _api_cache.get(key)
    if entry and (_time.monotonic() - entry[0]) < ttl:
        return entry[1], True
    return None, False

def _set_cached(key: str, value):
    _api_cache[key] = (_time.monotonic(), value)


class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle Decimal objects"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def convert_decimals(data):
    """Convert Decimal objects to float for JSON serialization"""
    if isinstance(data, Decimal):
        return float(data)
    elif isinstance(data, datetime):
        return data.isoformat()
    elif isinstance(data, dict):
        return {key: convert_decimals(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_decimals(item) for item in data]
    return data


class TradingBotAPI:
    """Simplified Trading Bot API using DatabaseManager"""
    
    def __init__(self):
        self.logger = setup_service_logging('api')
        self.app = web.Application()
        self.config_manager: Optional[UniversalConfigManager] = None
        self.database_manager: Optional[DatabaseManager] = None
        
    async def initialize(self):
        """Initialize API server components"""
        self.logger.info("Initializing Trading Bot API v3...")
        
        # Load configuration
        self.config_manager = UniversalConfigManager()
        self.config_manager.load_all_configs()
        
        # Initialize database manager
        main_config = self.config_manager.get_config('main')
        database_config = main_config['database'].copy()
        
        # Override host for Docker containers
        if os.getenv('DATABASE_HOST'):
            database_config['host'] = os.getenv('DATABASE_HOST')
        
        self.database_manager = DatabaseManager({'database': database_config})
        await self.database_manager.initialize()
        
        # Setup routes
        self._setup_routes()
        
        # Setup Jinja2 templates
        _base_dir = Path(__file__).parent
        aiohttp_jinja2.setup(
            self.app,
            loader=jinja2.FileSystemLoader(str(_base_dir / 'templates')),
            enable_async=True
        )
        
        self.logger.info("Trading Bot API v3 initialized")
    
    def _setup_routes(self):
        """Setup API routes"""
        # API endpoints
        self.app.router.add_get('/api/health', self.health_check)
        self.app.router.add_get('/api/stats', self.get_stats)
        
        # Candles endpoints with source support
        self.app.router.add_get('/api/candles', self.get_candles)
        self.app.router.add_get('/api/data/candles', self.get_candles)  # Legacy compatibility
        self.app.router.add_get('/api/candles/sources', self.get_candle_sources)
        self.app.router.add_get('/api/data/sources', self.get_candle_sources)  # Legacy compatibility
        self.app.router.add_get('/api/candles/latest', self.get_latest_candle)
        
        # Chart endpoint for trading chart page
        self.app.router.add_get('/api/chart', self.get_chart_data)
        self.app.router.add_get('/api/data/timeframes', self.get_timeframes)
        
        # Indicators endpoints
        self.app.router.add_get('/api/indicators', self.get_indicators)
        self.app.router.add_get('/api/indicators/status', self.get_indicators_status)
        
        # Strategies endpoints
        self.app.router.add_get('/api/strategies', self.get_strategies)
        self.app.router.add_get('/api/strategies/status', self.get_strategies_status)
        self.app.router.add_get('/api/strategies/signals', self.get_strategies_signals)
        self.app.router.add_get('/api/strategies/performance', self.get_strategy_performance)
        self.app.router.add_post('/api/strategies/backfill-prices', self.backfill_signal_prices)
        
        # Web dashboard
        self.app.router.add_get('/', self.dashboard)
        self.app.router.add_get('/dashboard', self.dashboard)
        
        # Chart pages  
        self.app.router.add_get('/chart', self.trading_chart)
        self.app.router.add_get('/indicators', self.indicators_chart)
        self.app.router.add_get('/strategies', self.strategies_chart)
        
        # Static files
        _base_dir = Path(__file__).parent
        self.app.router.add_static('/static', str(_base_dir / 'static'))
    
    async def health_check(self, request):
        """Health check endpoint"""
        is_healthy = self.database_manager.health_check() if self.database_manager else False
        status_code = 200 if is_healthy else 503
        
        return web.json_response({
            'status': 'healthy' if is_healthy else 'unhealthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'database': 'connected' if is_healthy else 'disconnected'
        }, status=status_code)
    
    async def get_stats(self, request):
        """Get database statistics"""
        try:
            stats = self.database_manager.get_database_stats()
            return web.json_response({
                'status': 'success',
                'data': convert_decimals(stats)
            })
        except Exception as e:
            self.logger.error(f"Error getting stats: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_candles(self, request):
        """Get candles with source filtering support"""
        try:
            # Parse parameters
            exchange = request.query.get('exchange', '').strip().lower()
            symbol = request.query.get('symbol', '').strip().upper()
            timeframe = request.query.get('timeframe', '').strip().lower()
            limit = int(request.query.get('limit', 100))
            source_type = request.query.get('source_type')  # 'exchange' or 'aggregated'
            source_timeframe = request.query.get('source_timeframe')  # source timeframe
            
            if not all([exchange, symbol, timeframe]):
                return web.json_response({
                    'error': 'Missing required parameters: exchange, symbol, timeframe'
                }, status=400)
            
            # Get candles using DatabaseManager
            candles = self.database_manager.get_candles_by_source(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
                source_type=source_type,
                source_timeframe=source_timeframe,
                limit=limit
            )
            
            return web.json_response({
                'status': 'success',
                'data': {
                    'exchange': exchange,
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'source_type': source_type,
                    'source_timeframe': source_timeframe,
                    'candles': convert_decimals(candles),
                    'count': len(candles)
                }
            })
            
        except ValueError as e:
            return web.json_response({'error': f'Invalid parameter: {e}'}, status=400)
        except Exception as e:
            self.logger.error(f"Error getting candles: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_candle_sources(self, request):
        """Get available data sources for a timeframe"""
        try:
            exchange = request.query.get('exchange', '').strip().lower()
            symbol = request.query.get('symbol', '').strip().upper()
            timeframe = request.query.get('timeframe', '').strip().lower()
            
            # If no parameters provided, return dashboard-compatible format
            if not any([exchange, symbol, timeframe]):
                cached, hit = _get_cached('candle_sources', 60.0)
                if hit:
                    return web.json_response(cached)

                from sqlalchemy import text
                # Scan only the latest chunk per partition key for min/max timestamps,
                # use approximate_row_count for fast counts — avoids full 236-chunk scan.
                query = text("""
                    SELECT
                        exchange,
                        symbol,
                        timeframe,
                        source_type,
                        approximate_row_count(
                            format('%I.%I',
                                chunk_schema,
                                chunk_name)::regclass
                        )                         AS candles_count,
                        range_start_integer       AS first_timestamp,
                        range_end_integer         AS last_timestamp
                    FROM (
                        SELECT DISTINCT ON (c.exchange, c.symbol, c.timeframe, c.source_type)
                            c.exchange, c.symbol, c.timeframe, c.source_type,
                            ch.chunk_schema, ch.chunk_name,
                            ch.range_start_integer,
                            ch.range_end_integer
                        FROM timescaledb_information.chunks ch
                        JOIN candles c ON TRUE
                        WHERE ch.hypertable_name = 'candles'
                          AND NOT ch.is_compressed
                        ORDER BY c.exchange, c.symbol, c.timeframe, c.source_type,
                                 ch.range_end_integer DESC
                    ) sub
                """)

                # Fallback: simple query restricted to last-seen values only
                fallback_query = text("""
                    SELECT
                        exchange, symbol, timeframe, source_type,
                        COUNT(*)       AS candles_count,
                        MIN(timestamp) AS first_timestamp,
                        MAX(timestamp) AS last_timestamp
                    FROM candles
                    WHERE timestamp >= extract(epoch from now() - interval '90 days')::bigint
                       OR timestamp = (SELECT MIN(timestamp) FROM candles)
                    GROUP BY exchange, symbol, timeframe, source_type
                    ORDER BY exchange, symbol, timeframe, source_type
                """)

                from sqlalchemy import text
                with self.database_manager.get_session() as session:
                    # Step 1: get distinct combos from the latest chunk only (fast index scan)
                    latest_chunk_ts = session.execute(text("""
                        SELECT MAX(range_start_integer)
                        FROM timescaledb_information.chunks
                        WHERE hypertable_name = 'candles'
                    """)).scalar() or 0

                    combos = session.execute(text("""
                        SELECT DISTINCT exchange, symbol, timeframe, source_type
                        FROM candles
                        WHERE timestamp >= :ts
                        ORDER BY exchange, symbol, timeframe, source_type
                    """), {'ts': latest_chunk_ts}).fetchall()

                    # Step 2: for each combo get MIN/MAX via index — fast per combo
                    sources = []
                    for combo in combos:
                        ex, sym, tf, st = combo.exchange, combo.symbol, combo.timeframe, combo.source_type
                        bounds = session.execute(text("""
                            SELECT MIN(timestamp) AS first_ts, MAX(timestamp) AS last_ts
                            FROM candles
                            WHERE exchange = :ex AND symbol = :sym
                              AND timeframe = :tf AND source_type = :st
                        """), {'ex': ex, 'sym': sym, 'tf': tf, 'st': st}).fetchone()

                        approx_count = session.execute(text(
                            "SELECT approximate_row_count('candles')"
                        )).scalar() or 0

                        first_date = datetime.fromtimestamp(int(bounds.first_ts), tz=timezone.utc).isoformat() if bounds.first_ts else 'N/A'
                        last_date = datetime.fromtimestamp(int(bounds.last_ts), tz=timezone.utc).isoformat() if bounds.last_ts else 'N/A'
                        sources.append({
                            'exchange': ex, 'symbol': sym, 'timeframe': tf, 'source': st,
                            'candles_count': approx_count,
                            'first_datetime': first_date,
                            'last_datetime': last_date,
                            'status': 'online'
                        })

                resp = {'sources': sources, 'summary': {'total_sources': len(sources)}}
                _set_cached('candle_sources', resp)
                return web.json_response(resp)
            
            if not all([exchange, symbol, timeframe]):
                return web.json_response({
                    'error': 'Missing required parameters: exchange, symbol, timeframe'
                }, status=400)
            
            # Get available sources using DatabaseManager
            sources = self.database_manager.get_available_sources(exchange, symbol, timeframe)
            
            return web.json_response({
                'status': 'success',
                'data': {
                    'exchange': exchange,
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'sources': convert_decimals(sources),
                    'count': len(sources)
                }
            })
            
        except Exception as e:
            self.logger.error(f"Error getting candle sources: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_latest_candle(self, request):
        """Get latest candle with optional source filtering"""
        try:
            exchange = request.query.get('exchange', '').strip().lower()
            symbol = request.query.get('symbol', '').strip().upper()
            timeframe = request.query.get('timeframe', '').strip().lower()
            source_type = request.query.get('source_type')
            source_timeframe = request.query.get('source_timeframe')
            
            if not all([exchange, symbol, timeframe]):
                return web.json_response({
                    'error': 'Missing required parameters: exchange, symbol, timeframe'
                }, status=400)
            
            # Get latest candle using DatabaseManager
            candle = self.database_manager.get_latest_candle(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
                source_type=source_type,
                source_timeframe=source_timeframe
            )
            
            if not candle:
                return web.json_response({
                    'error': f'No candle found for {exchange}/{symbol}/{timeframe}'
                }, status=404)
            
            return web.json_response({
                'status': 'success',
                'data': convert_decimals(candle)
            })
            
        except Exception as e:
            self.logger.error(f"Error getting latest candle: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_chart_data(self, request):
        """Get chart data endpoint for trading chart"""
        try:
            # Parse parameters
            exchange = request.query.get('exchange', '').strip().lower()
            symbol = request.query.get('symbol', '').strip().upper()
            timeframe = request.query.get('timeframe', '').strip().lower()
            limit = int(request.query.get('limit', 300))
            source_type = request.query.get('source_type')
            source_timeframe = request.query.get('source_timeframe')
            before_timestamp = request.query.get('before_timestamp')
            before_timestamp = int(before_timestamp) if before_timestamp else None

            if not all([exchange, symbol, timeframe]):
                return web.json_response({
                    'error': 'Missing required parameters: exchange, symbol, timeframe'
                }, status=400)

            # Get candles using DatabaseManager with source filtering
            raw_candles = self.database_manager.get_candles_by_source(
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
                source_type=source_type,
                source_timeframe=source_timeframe,
                limit=limit,
                before_timestamp=before_timestamp
            )

            # Clean candles data - keep only OHLCV fields for chart compatibility
            candles = []
            for candle in raw_candles:
                candles.append({
                    'timestamp': candle['timestamp'],
                    'open': candle['open'],
                    'high': candle['high'],
                    'low': candle['low'],
                    'close': candle['close'],
                    'volume': candle['volume']
                })

            has_more = len(candles) == limit

            return web.json_response({
                'status': 'success',
                'data': convert_decimals(candles),
                'count': len(candles),
                'has_more': has_more,
                'exchange': exchange,
                'symbol': symbol,
                'timeframe': timeframe,
                'source_type': source_type,
                'source_timeframe': source_timeframe
            })
            
        except ValueError as e:
            return web.json_response({'error': f'Invalid parameter: {e}'}, status=400)
        except Exception as e:
            self.logger.error(f"Error getting chart data: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_timeframes(self, request):
        """Get ALL available timeframes with source info for exchange/symbol"""
        try:
            exchange = request.query.get('exchange', '').strip().lower()
            symbol = request.query.get('symbol', '').strip().upper()
            
            if not exchange or not symbol:
                return web.json_response({
                    'error': 'Missing required parameters: exchange, symbol'
                }, status=400)
            
            # Get ALL timeframes with source information
            from sqlalchemy import text
            
            query = text("""
                SELECT DISTINCT 
                    timeframe,
                    source_type,
                    source_timeframe,
                    COUNT(*) as candles_count
                FROM candles 
                WHERE exchange = :exchange AND symbol = :symbol
                GROUP BY timeframe, source_type, source_timeframe
                ORDER BY timeframe, source_type, source_timeframe
            """)
            
            with self.database_manager.get_session() as session:
                result = session.execute(query, {'exchange': exchange, 'symbol': symbol})
                rows = result.fetchall()
            
            # Create timeframes as array of objects with timeframe and source info
            timeframes = []
            for row in rows:
                source_label = row.source_type
                if row.source_type == 'aggregated' and row.source_timeframe:
                    source_label = f"agg from {row.source_timeframe}"
                
                timeframes.append({
                    'timeframe': row.timeframe,
                    'source': row.source_type,
                    'source_timeframe': row.source_timeframe,
                    'source_label': source_label,
                    'candles_count': row.candles_count
                })
            
            return web.json_response({
                'timeframes': timeframes,  # Array of objects with timeframe and source info
                'exchange': exchange,
                'symbol': symbol
            })
            
        except Exception as e:
            self.logger.error(f"Error getting timeframes: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_indicators(self, request):
        """Get indicator values"""
        try:
            indicator_name = request.query.get('indicator_name', '').strip()
            exchange = request.query.get('exchange', '').strip().lower()
            symbol = request.query.get('symbol', '').strip().upper()
            timeframe = request.query.get('timeframe', '').strip().lower()
            limit = int(request.query.get('limit', 100))
            
            if not all([indicator_name, exchange, symbol, timeframe]):
                return web.json_response({
                    'error': 'Missing required parameters: indicator_name, exchange, symbol, timeframe'
                }, status=400)
            
            # Get indicators using DatabaseManager
            indicators = self.database_manager.get_indicator_values(
                indicator_name=indicator_name,
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
                limit=limit
            )
            
            return web.json_response({
                'status': 'success',
                'data': {
                    'indicator_name': indicator_name,
                    'exchange': exchange,
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'indicators': convert_decimals(indicators),
                    'count': len(indicators)
                }
            })
            
        except ValueError as e:
            return web.json_response({'error': f'Invalid parameter: {e}'}, status=400)
        except Exception as e:
            self.logger.error(f"Error getting indicators: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_strategies(self, request):
        """Get strategy signals"""
        try:
            strategy_name = request.query.get('strategy_name', '').strip()
            exchange = request.query.get('exchange', '').strip().lower()
            symbol = request.query.get('symbol', '').strip().upper()
            timeframe = request.query.get('timeframe', '').strip().lower()
            limit = int(request.query.get('limit', 100))
            
            if not all([strategy_name, exchange, symbol, timeframe]):
                return web.json_response({
                    'error': 'Missing required parameters: strategy_name, exchange, symbol, timeframe'
                }, status=400)
            
            # Get strategies using DatabaseManager
            strategies = self.database_manager.get_strategy_signals(
                strategy_name=strategy_name,
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
                limit=limit
            )
            
            return web.json_response({
                'status': 'success',
                'data': {
                    'strategy_name': strategy_name,
                    'exchange': exchange,
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'strategies': convert_decimals(strategies),
                    'count': len(strategies)
                }
            })
            
        except ValueError as e:
            return web.json_response({'error': f'Invalid parameter: {e}'}, status=400)
        except Exception as e:
            self.logger.error(f"Error getting strategies: {e}")
            return web.json_response({'error': str(e)}, status=500)
    
    async def get_indicators_status(self, request):
        """Get indicators status (for dashboard compatibility)"""
        try:
            cached, hit = _get_cached('indicators_status', 30.0)
            if hit:
                return web.json_response(cached)

            from sqlalchemy import text
            # Restrict to latest chunk only — avoids scanning 236 chunks.
            query = text("""
                WITH latest_chunk AS (
                    SELECT MAX(range_start_integer) AS ts
                    FROM timescaledb_information.chunks
                    WHERE hypertable_name = 'indicators'
                )
                SELECT
                    indicator_name,
                    exchange,
                    symbol,
                    timeframe,
                    COUNT(*) AS count,
                    MAX(timestamp) AS last_update
                FROM indicators, latest_chunk
                WHERE timestamp >= latest_chunk.ts
                GROUP BY indicator_name, exchange, symbol, timeframe
                ORDER BY indicator_name, exchange, symbol, timeframe
            """)

            with self.database_manager.get_session() as session:
                rows = session.execute(query).fetchall()

            indicators = []
            for row in rows:
                last_update_date = datetime.fromtimestamp(row.last_update, tz=timezone.utc).isoformat() if row.last_update else 'N/A'
                indicators.append({
                    'name': row.indicator_name,
                    'connection': f"{row.exchange}_{row.symbol}_{row.timeframe}",
                    'status': 'active',
                    'count': row.count,
                    'last_update': last_update_date
                })

            resp = {
                'indicators': indicators,
                'summary': {
                    'total_indicators': len(indicators),
                    'active_indicators': len(indicators),
                    'last_update': indicators[0]['last_update'] if indicators else 'N/A'
                }
            }
            _set_cached('indicators_status', resp)
            return web.json_response(resp)

        except Exception as e:
            self.logger.error(f"Error getting indicators status: {e}")
            return web.json_response({
                'indicators': [],
                'summary': {'total_indicators': 0, 'active_indicators': 0, 'last_update': 'N/A'}
            })
    
    async def get_strategies_status(self, request):
        """Get strategies status — config entries merged with live signal counts."""
        try:
            from sqlalchemy import text

            # Seed from config
            strategies_config = self.config_manager.get_config('strategies') or {}
            strategies_data = {}
            for name, cfg in (strategies_config.get('strategies') or {}).items():
                strategies_data[name] = {
                    'enabled': cfg.get('enabled', True),
                    'total_signals': 0,
                    'plugin': cfg.get('plugin', 'unknown'),
                    'latest_signal': None
                }

            # Single SQL query: count + latest signal per strategy
            query = text("""
                SELECT
                    strategy_name,
                    COUNT(*) AS total_signals,
                    MAX(timestamp) AS last_ts
                FROM strategy_signals
                GROUP BY strategy_name
            """)
            latest_query = text("""
                SELECT DISTINCT ON (strategy_name)
                    strategy_name, signal_type, timestamp, price, confidence
                FROM strategy_signals
                ORDER BY strategy_name, timestamp DESC
            """)

            with self.database_manager.get_session() as session:
                counts = {r.strategy_name: r for r in session.execute(query).fetchall()}
                latests = {r.strategy_name: r for r in session.execute(latest_query).fetchall()}

            for name, row in counts.items():
                if name not in strategies_data:
                    strategies_data[name] = {
                        'enabled': True, 'total_signals': 0,
                        'plugin': 'unknown', 'latest_signal': None
                    }
                strategies_data[name]['total_signals'] = row.total_signals
                latest = latests.get(name)
                if latest:
                    strategies_data[name]['latest_signal'] = {
                        'signal_type': latest.signal_type,
                        'timestamp': latest.timestamp,
                        'price': float(latest.price) if latest.price else None,
                        'confidence': float(latest.confidence)
                    }

            return web.json_response(strategies_data)

        except Exception as e:
            self.logger.error(f"Error getting strategies status: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def get_strategies_signals(self, request):
        """Get all strategy signals for chart display"""
        try:
            limit = int(request.query.get('limit', 1000))
            
            # Get all strategy signals from database
            signals = self.database_manager.get_all_strategy_signals(limit=limit)
            
            if signals is None:
                return web.json_response({'error': 'Failed to fetch signals'}, status=500)
            
            # Convert signals to the format expected by the chart
            connections_config = self.config_manager.get_config('connections').get('connections', {})
            signals_data = []
            for signal in signals:
                conn_cfg = connections_config.get(signal.connection_name, {})
                exchange = conn_cfg.get('exchange', 'unknown')
                symbol = conn_cfg.get('symbol', 'UNKNOWN')
                timeframe = conn_cfg.get('timeframe', 'unknown')
                
                signals_data.append({
                    'id': signal.id,
                    'strategy_name': signal.strategy_name,
                    'connection_name': signal.connection_name,
                    'exchange': exchange,
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'signal_type': signal.signal_type,
                    'timestamp': signal.timestamp,
                    'price': float(signal.price) if signal.price else None,
                    'confidence': float(signal.confidence) if signal.confidence else None,
                    'metadata': signal.meta_data or "{}"
                })
            
            response_data = {
                'status': 'success',
                'data': {
                    'signals': signals_data,
                    'count': len(signals_data)
                }
            }
            
            return web.json_response(convert_decimals(response_data))
            
        except Exception as e:
            self.logger.error(f"Error getting strategy signals: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def get_strategy_performance(self, request):
        """Calculate strategy trading performance: pairs BUY/SELL signals into trades."""
        try:
            import math
            from models.base import StrategySignal
            from sqlalchemy import and_

            strategy_name = request.query.get('strategy_name', '').strip()
            initial_capital = float(request.query.get('initial_capital', 10000))

            if not strategy_name:
                return web.json_response({'error': 'strategy_name is required'}, status=400)

            from models.base import Candle as CandleModel

            strategies_config = self.config_manager.get_config('strategies')
            strategy_cfg = strategies_config.get('strategies', {}).get(strategy_name, {})
            connection_name = strategy_cfg.get('connection', '')
            connections_config = self.config_manager.get_config('connections')
            conn_cfg = connections_config.get('connections', {}).get(connection_name, {})

            with self.database_manager.get_session() as session:
                signals = session.query(StrategySignal).filter(
                    and_(
                        StrategySignal.strategy_name == strategy_name,
                        StrategySignal.signal_type.in_(['buy', 'sell', 'BUY', 'SELL']),
                        StrategySignal.price > 0,
                    )
                ).order_by(StrategySignal.timestamp.asc()).all()

                signal_list = [
                    {'type': s.signal_type.lower(), 'price': float(s.price), 'ts': s.timestamp}
                    for s in signals
                ]

                # Fetch current market price (latest 1m candle close)
                current_price = None
                if conn_cfg:
                    last_candle = session.query(CandleModel).filter(
                        and_(
                            CandleModel.exchange == conn_cfg.get('exchange', ''),
                            CandleModel.symbol == conn_cfg.get('symbol', ''),
                            CandleModel.timeframe == '1m',
                        )
                    ).order_by(CandleModel.timestamp.desc()).first()
                    if last_candle:
                        current_price = float(last_candle.close_price)

            import time as _time
            now_ts = int(_time.time())

            #  Pair signals into trades (FIFO) 
            # Each BUY = one Long entry. Each SELL closes the oldest open BUY (FIFO).
            # Unmatched BUYs remain open — shown with current market price as exit.
            trades = []
            buy_queue = []   # FIFO queue of open BUY signals

            for sig in signal_list:
                if sig['type'] == 'buy':
                    buy_queue.append(sig)
                elif sig['type'] == 'sell' and buy_queue:
                    entry = buy_queue.pop(0)
                    exit_price = sig['price']
                    pnl_pct = (exit_price - entry['price']) / entry['price'] * 100
                    duration_sec = sig['ts'] - entry['ts']
                    trades.append({
                        'entry_time': entry['ts'],
                        'exit_time': sig['ts'],
                        'entry_price': entry['price'],
                        'exit_price': exit_price,
                        'pnl_pct': pnl_pct,
                        'duration_days': round(duration_sec / 86400, 1),
                        'open': False,
                        'win': pnl_pct > 0,
                    })

            # Open positions: each remaining BUY is its own open trade
            open_trades = []
            for b in buy_queue:
                mark_price = current_price or b['price']
                pnl_pct = (mark_price - b['price']) / b['price'] * 100
                open_trades.append({
                    'entry_time': b['ts'],
                    'exit_time': now_ts,
                    'entry_price': b['price'],
                    'exit_price': mark_price,
                    'pnl_pct': pnl_pct,
                    'duration_days': round((now_ts - b['ts']) / 86400, 1),
                    'open': True,
                    'win': pnl_pct > 0,
                })

            # For summary stats: include open trades marked-to-market
            all_trades = trades + open_trades

            # Legacy single open_trade summary for banner
            open_trade = None
            if buy_queue:
                avg_entry = sum(b['price'] for b in buy_queue) / len(buy_queue)
                open_trade = {
                    'entry_time': buy_queue[0]['ts'],
                    'avg_entry': avg_entry,
                    'num_entries': len(buy_queue),
                    'current_price': current_price,
                }

            #  Compute statistics 
            if not trades:
                return web.json_response({
                    'status': 'success',
                    'strategy_name': strategy_name,
                    'initial_capital': initial_capital,
                    'stats': None,
                    'open_trade': open_trade,
                    'trades': [],
                })

            # Stats include open trades (mark-to-market) — same as TradingView
            wins = [t for t in all_trades if t['win']]
            losses = [t for t in all_trades if not t['win']]

            # Capital per trade = fixed allocation (initial_capital / total entries)
            # This prevents compounding distortion from overlapping positions.
            n_total = len(all_trades)
            capital_per_trade = initial_capital / n_total if n_total else initial_capital

            net_profit_usd = sum(t['pnl_pct'] / 100 * capital_per_trade for t in all_trades)
            net_profit_pct = net_profit_usd / initial_capital * 100
            final_capital = initial_capital + net_profit_usd

            gross_profit_usd = sum(t['pnl_pct'] / 100 * capital_per_trade for t in wins)
            gross_loss_usd = abs(sum(t['pnl_pct'] / 100 * capital_per_trade for t in losses))
            gross_profit_pct = sum(t['pnl_pct'] for t in wins)
            gross_loss_pct = abs(sum(t['pnl_pct'] for t in losses))
            profit_factor = (gross_profit_usd / gross_loss_usd) if gross_loss_usd > 0 else None

            win_rate = len(wins) / n_total * 100 if n_total else 0
            avg_trade_pct = sum(t['pnl_pct'] for t in all_trades) / n_total if n_total else 0
            avg_win_pct = (gross_profit_pct / len(wins)) if wins else 0
            avg_loss_pct = (-gross_loss_pct / len(losses)) if losses else 0

            # Max drawdown on equity curve (sequential, equal allocation)
            running = initial_capital
            peak = initial_capital
            max_dd_usd = 0.0
            max_dd_pct = 0.0
            for t in all_trades:
                running += t['pnl_pct'] / 100 * capital_per_trade
                if running > peak:
                    peak = running
                dd = peak - running
                dd_pct = dd / peak * 100 if peak > 0 else 0
                if dd > max_dd_usd:
                    max_dd_usd = dd
                if dd_pct > max_dd_pct:
                    max_dd_pct = dd_pct

            # Sharpe ratio (trade-level)
            returns = [t['pnl_pct'] for t in all_trades]
            mean_r = sum(returns) / len(returns) if returns else 0
            if len(returns) > 1:
                variance = sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
                std_r = math.sqrt(variance)
                sharpe = (mean_r / std_r) * math.sqrt(len(returns)) if std_r > 0 else 0
            else:
                sharpe = 0

            stats = {
                'net_profit_usd': round(net_profit_usd, 2),
                'net_profit_pct': round(net_profit_pct, 2),
                'gross_profit_pct': round(gross_profit_pct, 2),
                'gross_loss_pct': round(gross_loss_pct, 2),
                'max_drawdown_usd': round(max_dd_usd, 2),
                'max_drawdown_pct': round(max_dd_pct, 2),
                'total_trades': n_total,
                'closed_trades': len(trades),
                'open_trades_count': len(open_trades),
                'winning_trades': len(wins),
                'losing_trades': len(losses),
                'win_rate': round(win_rate, 1),
                'profit_factor': round(profit_factor, 2) if profit_factor is not None else None,
                'avg_trade_pct': round(avg_trade_pct, 2),
                'avg_win_pct': round(avg_win_pct, 2),
                'avg_loss_pct': round(avg_loss_pct, 2),
                'largest_win_pct': round(max((t['pnl_pct'] for t in wins), default=0), 2),
                'largest_loss_pct': round(min((t['pnl_pct'] for t in losses), default=0), 2),
                'sharpe_ratio': round(sharpe, 2),
                'final_capital': round(final_capital, 2),
                'capital_per_trade': round(capital_per_trade, 2),
                'current_price': current_price,
            }

            # Combine closed + open trades, sorted newest-first (like TradingView)
            all_trades_sorted = sorted(all_trades, key=lambda t: t['entry_time'], reverse=True)
            trades_out = [
                {
                    'num': n_total - i,
                    'entry_time': t['entry_time'],
                    'exit_time': t['exit_time'],
                    'entry_price': round(t['entry_price'], 4),
                    'exit_price': round(t['exit_price'], 4),
                    'pnl_pct': round(t['pnl_pct'], 2),
                    'pnl_usd': round(t['pnl_pct'] / 100 * capital_per_trade, 2),
                    'duration_days': t['duration_days'],
                    'open': t['open'],
                    'win': t['win'],
                }
                for i, t in enumerate(all_trades_sorted)
            ]

            return web.json_response(convert_decimals({
                'status': 'success',
                'strategy_name': strategy_name,
                'initial_capital': initial_capital,
                'stats': stats,
                'open_trade': open_trade,
                'trades': trades_out,
            }))

        except Exception as e:
            self.logger.error(f"Error computing strategy performance: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return web.json_response({'error': str(e)}, status=500)

    async def backfill_signal_prices(self, request):
        """Backfill price=0 signals with actual candle close prices"""
        try:
            from models.base import StrategySignal, Candle
            from sqlalchemy import and_

            connections_config = self.config_manager.get_config('connections').get('connections', {})

            updated = 0
            skipped = 0

            with self.database_manager.get_session() as session:
                signals = session.query(StrategySignal).filter(
                    StrategySignal.price == 0
                ).all()

                for signal in signals:
                    conn_cfg = connections_config.get(signal.connection_name, {})
                    if not conn_cfg:
                        skipped += 1
                        continue

                    # Determine source timeframe from indicators config
                    indicators_config = self.config_manager.get_config('indicators').get('indicators', {})
                    strategies_config = self.config_manager.get_config('strategies').get('strategies', {})
                    strategy_cfg = strategies_config.get(signal.strategy_name, {})
                    required_indicators = strategy_cfg.get('required_indicators', [])
                    base_indicator = strategy_cfg.get('base_indicator', required_indicators[0] if required_indicators else None)
                    source_timeframe = '4h'
                    if base_indicator and base_indicator in indicators_config:
                        source_timeframe = indicators_config[base_indicator].get('source_timeframe', '4h')

                    candle = session.query(Candle).filter(
                        and_(
                            Candle.exchange == conn_cfg.get('exchange', ''),
                            Candle.symbol == conn_cfg.get('symbol', ''),
                            Candle.timeframe == source_timeframe,
                            Candle.timestamp == signal.timestamp
                        )
                    ).first()

                    if candle:
                        signal.price = candle.close_price
                        updated += 1
                    else:
                        skipped += 1

                session.commit()

            return web.json_response({
                'status': 'success',
                'updated': updated,
                'skipped': skipped
            })

        except Exception as e:
            self.logger.error(f"Error backfilling signal prices: {e}")
            return web.json_response({'error': str(e)}, status=500)

    @aiohttp_jinja2.template('dashboard.html')
    async def dashboard(self, request):
        """Dashboard page"""
        try:
            # Get basic stats for dashboard
            stats = self.database_manager.get_database_stats()
            
            return {
                'title': 'Trading Bot Dashboard',
                'stats': convert_decimals(stats),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error loading dashboard: {e}")
            return {
                'title': 'Trading Bot Dashboard',
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
    
    @aiohttp_jinja2.template('trading_chart.html')
    async def trading_chart(self, request):
        """Trading chart page"""
        try:
            return {
                'title': 'Trading Chart'
            }
        except Exception as e:
            self.logger.error(f"Error loading trading chart: {e}")
            return {
                'title': 'Trading Chart',
                'error': str(e)
            }
    
    @aiohttp_jinja2.template('indicators_chart.html')
    async def indicators_chart(self, request):
        """Indicators chart page"""
        try:
            return {
                'title': 'Indicators Chart'
            }
        except Exception as e:
            self.logger.error(f"Error loading indicators chart: {e}")
            return {
                'title': 'Indicators Chart',
                'error': str(e)
            }
    
    @aiohttp_jinja2.template('strategies_chart.html') 
    async def strategies_chart(self, request):
        """Strategies chart page"""
        try:
            return {
                'title': 'Strategies Chart'
            }
        except Exception as e:
            self.logger.error(f"Error loading strategies chart: {e}")
            return {
                'title': 'Strategies Chart',
                'error': str(e)
            }
    
    async def start_server(self, host='0.0.0.0', port=8080):
        """Start the API server"""
        self.logger.info(f"Starting API server on {host}:{port}")
        
        runner = web.AppRunner(self.app)
        await runner.setup()
        
        site = web.TCPSite(runner, host, port)
        await site.start()
        
        self.logger.info(f"API server running on http://{host}:{port}")
        self.logger.info(f"Dashboard: http://{host}:{port}/dashboard")
        self.logger.info(f"Health check: http://{host}:{port}/api/health")
        
        return runner
    
    async def cleanup(self):
        """Cleanup resources"""
        self.logger.info("Cleaning up API v3...")
        
        if self.database_manager:
            self.database_manager.close()
        
        self.logger.info("API v3 cleanup completed")


async def main():
    """Main function for standalone running"""
    api = TradingBotAPI()
    
    try:
        await api.initialize()
        runner = await api.start_server()
        
        # Keep running
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            pass
    finally:
        await api.cleanup()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())