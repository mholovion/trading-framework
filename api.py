#!/usr/bin/env python3
"""Trading Bot API — ClickHouse backend."""

import os
os.environ.setdefault("NUMBA_DISABLE_JIT", "1")  # pandas_ta numba caching fix in Docker

from aiohttp import web
import aiohttp_jinja2
import jinja2
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

sys.path.append(str(Path(__file__).parent))

from core.universal_config_manager import UniversalConfigManager
from core.logging_config import setup_service_logging
from core.clickhouse import ClickHouseManager, create_clickhouse_manager
from core.resolver import DependencyResolver


def convert_decimals(data):
    from decimal import Decimal
    if isinstance(data, Decimal):
        return float(data)
    elif isinstance(data, datetime):
        return data.isoformat()
    elif isinstance(data, dict):
        return {k: convert_decimals(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_decimals(i) for i in data]
    return data


class TradingBotAPI:

    def __init__(self):
        self.logger         = setup_service_logging('api')
        self.app            = web.Application()
        self.config_manager: Optional[UniversalConfigManager] = None
        self.clickhouse:     Optional[ClickHouseManager]      = None
        self.resolver:       Optional[DependencyResolver]     = None

    async def initialize(self):
        self.logger.info("Initializing Trading Bot API...")

        self.config_manager = UniversalConfigManager()
        self.config_manager.load_all_configs()

        main_config     = self.config_manager.get_config('main')
        self.clickhouse = create_clickhouse_manager(main_config.get('clickhouse', {}))
        await self.clickhouse.initialize()
        self.resolver   = DependencyResolver(self.clickhouse)

        self._setup_routes()

        _base_dir = Path(__file__).parent
        aiohttp_jinja2.setup(
            self.app,
            loader=jinja2.FileSystemLoader(str(_base_dir / 'templates')),
            enable_async=True,
        )

        self.logger.info("Trading Bot API initialized")
    
    def _setup_routes(self):
        self.app.router.add_get('/api/health',                  self.health_check)
        self.app.router.add_get('/api/stats',                   self.get_stats)
        self.app.router.add_get('/api/chart',                   self.get_chart_data)
        self.app.router.add_get('/api/data/sources',            self.get_data_sources)
        self.app.router.add_get('/api/data/timeframes',         self.get_timeframes)
        self.app.router.add_get('/api/indicators',              self.get_indicator_data)
        self.app.router.add_get('/api/indicators/library',      self.get_indicators_library)
        self.app.router.add_get('/api/indicators/status',       self.get_indicators_status)
        self.app.router.add_post('/api/indicators/compute',     self.compute_indicator)
        self.app.router.add_post('/api/indicators/test',        self.test_indicator)
        self.app.router.add_post('/api/indicators/custom',      self.save_custom_indicator)
        self.app.router.add_get('/api/strategies/signals',      self.get_strategies_signals)
        self.app.router.add_get('/api/strategies/status',       self.get_strategies_status)
        self.app.router.add_post('/api/strategies/compute',     self.compute_strategy)
        self.app.router.add_get('/api/progress',                self.get_progress)
        self.app.router.add_get('/',                            self.dashboard)
        self.app.router.add_get('/dashboard',                   self.dashboard)
        self.app.router.add_get('/chart',                       self.trading_chart)
        self.app.router.add_get('/indicators',                  self.indicators_chart)
        self.app.router.add_get('/strategies',                  self.strategies_chart)
        _base_dir = Path(__file__).parent
        self.app.router.add_static('/static', str(_base_dir / 'static'))
    
    async def health_check(self, request):
        ch_ok = self.clickhouse and self.clickhouse._conn is not None
        return web.json_response({
            'status': 'healthy' if ch_ok else 'unhealthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'clickhouse': 'connected' if ch_ok else 'disconnected',
        }, status=200 if ch_ok else 503)
    
    async def get_stats(self, request):
        try:
            rows = await self.clickhouse._execute(
                "SELECT "
                " (SELECT count() FROM candles) AS candles,"
                " (SELECT count() FROM indicators) AS indicators,"
                " (SELECT count() FROM strategy_signals) AS signals"
            )
            r = rows[0] if rows else (0, 0, 0)
            return web.json_response({'status': 'success', 'data': {
                'candles_count': int(r[0]),
                'indicators_count': int(r[1]),
                'strategies_count': int(r[2]),
            }})
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)
    
    # ------------------------------------------------------------------ #
    # Data source / indicator metadata endpoints                           #
    # ------------------------------------------------------------------ #

    async def get_data_sources(self, request):
        """
        GET /api/data/sources
        Returns distinct (exchange, symbol, timeframe, count) from the candles table.
        """
        try:
            rows = await self.clickhouse._execute(
                "SELECT exchange, symbol, timeframe, count() AS cnt "
                "FROM candles GROUP BY exchange, symbol, timeframe "
                "ORDER BY exchange, symbol, timeframe"
            )
            sources = [
                {"exchange": r[0], "symbol": r[1], "timeframe": r[2], "count": int(r[3])}
                for r in rows
            ]
            return web.json_response({"sources": sources})
        except Exception as e:
            return web.json_response({"sources": [], "error": str(e)}, status=500)

    async def get_indicators_status(self, request):
        """
        GET /api/indicators/status
        Returns cached indicator types/hashes with their candle-level connection info.
        Dashboard uses this to populate the indicator selector.
        """
        try:
            rows = await self.clickhouse._execute(
                "SELECT pm.params_hash, pm.params_json, "
                "       count() AS cnt, min(i.timestamp) AS first_ts, max(i.timestamp) AS last_ts "
                "FROM indicators i "
                "JOIN params_meta pm ON i.params_hash = pm.params_hash "
                "GROUP BY pm.params_hash, pm.params_json"
            )
            indicators = []
            for r in rows:
                try:
                    p = json.loads(r[1])
                    tf  = p.get("timeframe", "")
                    ex  = p.get("exchange",  "")
                    sym = p.get("symbol",    "")
                    # connection string format expected by dashboard: exchange_SYMBOL_tf
                    conn = f"{ex}_{sym}_{tf}" if ex else tf
                    indicators.append({
                        "name":       f"{p.get('type','?')}_{p.get('period','')}_{tf}",
                        "params_hash": r[0],
                        "connection":  conn,
                        "count":       int(r[2]),
                        "params":      p,
                    })
                except Exception:
                    pass
            return web.json_response({"indicators": indicators})
        except Exception as e:
            return web.json_response({"indicators": [], "error": str(e)}, status=500)

    async def get_indicator_data(self, request):
        """
        GET /api/indicators?indicator_name=rsi_14_4h&exchange=...&symbol=...&timeframe=...
        Returns time-series data points for a cached indicator.
        """
        try:
            from core.params import IndicatorParams
            indicator_name = request.query.get('indicator_name', '').strip()
            exchange       = request.query.get('exchange', '').strip().lower()
            symbol         = request.query.get('symbol',   '').strip().upper()
            timeframe      = request.query.get('timeframe', '').strip()
            start_ts = int(request.query['start_ts']) if 'start_ts' in request.query else None
            end_ts   = int(request.query['end_ts'])   if 'end_ts'   in request.query else None

            # Parse "rsi_14_4h" → type=rsi, period=14, tf=4h
            parts  = indicator_name.rsplit('_', 2)
            ind_type = parts[0] if len(parts) >= 1 else indicator_name
            period   = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 14
            tf       = timeframe or (parts[2] if len(parts) >= 3 else '1m')

            params = IndicatorParams.create(ind_type, tf, period)
            rows   = await self.clickhouse.fetch_indicator(
                params, exchange, symbol, start_ts=start_ts, end_ts=end_ts
            )
            return web.json_response({
                "data": {
                    "indicators": [{"timestamp": r[0], "value": r[1]} for r in rows]
                }
            })
        except Exception as e:
            self.logger.error(f"get_indicator_data error: {e}")
            return web.json_response({"data": {"indicators": []}, "error": str(e)}, status=500)

    async def get_strategies_status(self, request):
        """
        GET /api/strategies/status
        Returns per-strategy signal count + latest signal for the strategies panel.
        """
        try:
            rows = await self.clickhouse._execute(
                "SELECT strategy_type, count() AS total, "
                "       argMax(signal_type, timestamp) AS last_type, "
                "       argMax(price,       timestamp) AS last_price, "
                "       argMax(confidence,  timestamp) AS last_conf, "
                "       max(timestamp) AS last_ts "
                "FROM strategy_signals "
                "GROUP BY strategy_type"
            )
            result = {}
            for r in rows:
                result[r[0]] = {
                    "total_signals": int(r[1]),
                    "latest_signal": {
                        "signal_type": r[2],
                        "price":       float(r[3]) if r[3] else None,
                        "confidence":  float(r[4]) if r[4] else None,
                        "timestamp":   int(r[5])   if r[5] else None,
                    } if r[5] else None,
                }
            return web.json_response(result)
        except Exception as e:
            return web.json_response({}, status=500)

    # ------------------------------------------------------------------ #
    # On-demand indicator endpoints                                        #
    # ------------------------------------------------------------------ #

    async def get_indicators_library(self, request):
        """Return all available indicator types: pandas-ta, custom primitives, user scripts."""
        try:
            import pandas_ta as pdta
            from pathlib import Path

            # pandas-ta public callables (skip private/utility names)
            _SKIP = {"version", "ticker", "trends", "non_unique", "above", "below",
                     "above_value", "below_value", "cross", "cross_value", "signals",
                     "percent_return", "log_return", "Strategy", "AllStrategy"}
            ta_names = sorted(
                n for n in dir(pdta)
                if not n.startswith("_") and n not in _SKIP
                   and callable(getattr(pdta, n, None))
            )

            # Custom primitives from plugins/ta_primitives/
            primitives_dir = Path("/app/plugins/ta_primitives")
            primitives = sorted(
                p.stem for p in primitives_dir.glob("*.py")
                if not p.stem.startswith("_")
            ) if primitives_dir.exists() else []

            # User-created indicators from plugins/indicators/user/
            user_dir = Path("/app/plugins/indicators/user")
            user_inds = sorted(
                p.stem for p in user_dir.glob("*.py")
                if not p.stem.startswith("_")
            ) if user_dir.exists() else []

            # Schema: well-known indicators with their default parameters
            INDICATOR_SCHEMA = {
                "rsi":    {"display": "RSI",              "category": "Oscillators",
                           "params": [{"key": "period", "type": "int", "default": 14, "label": "Length"},
                                      {"key": "source", "type": "select", "label": "Source",
                                       "options": ["close","open","high","low"], "default": "close"}]},
                "ema":    {"display": "EMA",              "category": "Trend",
                           "params": [{"key": "period", "type": "int", "default": 20, "label": "Length"},
                                      {"key": "source", "type": "select", "label": "Source",
                                       "options": ["close","open","high","low"], "default": "close"}]},
                "sma":    {"display": "SMA",              "category": "Trend",
                           "params": [{"key": "period", "type": "int", "default": 20, "label": "Length"},
                                      {"key": "source", "type": "select", "label": "Source",
                                       "options": ["close","open","high","low"], "default": "close"}]},
                "macd":   {"display": "MACD",             "category": "Oscillators",
                           "params": [{"key": "period",  "type": "int", "default": 12, "label": "Fast"},
                                      {"key": "slow",    "type": "int", "default": 26, "label": "Slow"},
                                      {"key": "signal",  "type": "int", "default": 9,  "label": "Signal"}]},
                "bbands": {"display": "Bollinger Bands",  "category": "Volatility",
                           "params": [{"key": "period", "type": "int",   "default": 20, "label": "Length"},
                                      {"key": "std",    "type": "float", "default": 2.0,"label": "StdDev"}]},
                "stoch":  {"display": "Stochastic",       "category": "Oscillators",
                           "params": [{"key": "k", "type": "int", "default": 14, "label": "%K"},
                                      {"key": "d", "type": "int", "default": 3,  "label": "%D"}]},
                "atr":    {"display": "ATR",              "category": "Volatility",
                           "params": [{"key": "period", "type": "int", "default": 14, "label": "Length"}]},
                "adx":    {"display": "ADX",              "category": "Trend",
                           "params": [{"key": "period", "type": "int", "default": 14, "label": "Length"}]},
                "cci":    {"display": "CCI",              "category": "Oscillators",
                           "params": [{"key": "period", "type": "int", "default": 20, "label": "Length"}]},
                "mfi":    {"display": "MFI",              "category": "Volume",
                           "params": [{"key": "period", "type": "int", "default": 14, "label": "Length"}]},
            }

            # Build full list: known schema first, remaining ta_names as generic
            library = []
            for name in ta_names:
                schema = INDICATOR_SCHEMA.get(name, {
                    "display": name.upper(),
                    "category": "Other",
                    "params": [{"key": "period", "type": "int", "default": 14, "label": "Period"}],
                })
                library.append({"name": name, **schema})

            return web.json_response({
                "pandas_ta": library,
                "primitives": [{"name": n, "display": n.upper(), "category": "Custom Primitives",
                                 "params": [{"key": "period", "type": "int", "default": 14, "label": "Period"}]}
                                for n in primitives],
                "user": [{"name": n, "display": n.replace("_", " ").title(),
                           "category": "My Indicators", "params": []}
                          for n in user_inds],
            })
        except Exception as e:
            self.logger.error(f"Error building indicator library: {e}")
            return web.json_response({"pandas_ta": [], "primitives": [], "user": []})

    async def compute_indicator(self, request):
        """
        POST /api/indicators/compute
        Body: { type, timeframe, period, source, exchange, symbol, ...extra }
        Returns SSE stream: progress events then data points.
        """
        body = await request.json()
        from core.params import IndicatorParams

        try:
            type_    = body.pop("type")
            tf       = body.pop("timeframe")
            period   = int(body.pop("period", 14))
            source   = body.pop("source", "close")
            exchange = body.pop("exchange")
            symbol   = body.pop("symbol")
            start_ts = int(body.pop("start_ts")) if "start_ts" in body else None
            end_ts   = int(body.pop("end_ts"))   if "end_ts"   in body else None
            params   = IndicatorParams.create(type_, tf, period, source, **body)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=400)

        response = web.StreamResponse(headers={"Content-Type": "text/event-stream",
                                                "Cache-Control": "no-cache"})
        await response.prepare(request)

        async def send(data: dict):
            await response.write(f"data: {json.dumps(data)}\n\n".encode())

        try:
            await self.resolver.resolve_indicator(params, exchange, symbol,
                                                   progress_cb=send)
            # Only return values for the requested visible range (avoids fetching millions of rows)
            rows = await self.clickhouse.fetch_indicator(params, exchange, symbol,
                                                          start_ts=start_ts, end_ts=end_ts)
            await send({"stage": "done",
                        "data": [{"timestamp": r[0], "value": r[1]} for r in rows]})
        except Exception as e:
            self.logger.error(f"compute_indicator error: {e}", exc_info=True)
            await send({"stage": "error", "message": str(e)})
        finally:
            await response.write_eof()
        return response

    async def test_indicator(self, request):
        """
        POST /api/indicators/test
        Computes indicator WITHOUT caching it. Returns data for preview.
        Body: same as /compute plus candle data range from chart (start_ts, end_ts).
        """
        try:
            body     = await request.json()
            from core.params import IndicatorParams
            from plugins.indicators.loader import load_indicator_plugin

            type_    = body.pop("type")
            tf       = body.pop("timeframe")
            period   = int(body.pop("period", 14))
            source   = body.pop("source", "close")
            exchange = body.pop("exchange")
            symbol   = body.pop("symbol")
            start_ts = body.pop("start_ts", None)
            end_ts   = body.pop("end_ts",   None)
            params   = IndicatorParams.create(type_, tf, period, source, **body)

            candles = await self.clickhouse.fetch_candles(
                exchange, symbol, tf, start_ts=start_ts, end_ts=end_ts
            )
            if not candles:
                return web.json_response({"data": []})

            plugin  = load_indicator_plugin(type_, params.to_dict())
            warmup  = plugin.get_required_periods()
            results = await plugin.calculate_stream(candles, warmup)

            data = []
            for i, r in enumerate(results):
                if r and r.get("value") is not None:
                    data.append({"timestamp": candles[warmup + i]["timestamp"],
                                  "value": float(r["value"])})

            return web.json_response({"data": data,
                                       "display_name": params.display_name()})
        except Exception as e:
            self.logger.error(f"test_indicator error: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def save_custom_indicator(self, request):
        """
        POST /api/indicators/custom
        Body: { name, code, params_schema: [{key, type, default, label}, ...] }
        Saves to plugins/indicators/user/{name}.py
        """
        try:
            body = await request.json()
            name = body.get("name", "").strip()
            code = body.get("code", "")

            if not name or not code:
                return web.json_response({"error": "name and code required"}, status=400)
            if not name.replace("_", "").isalnum():
                return web.json_response({"error": "name must be alphanumeric + underscores"}, status=400)

            from pathlib import Path
            user_dir = Path("/app/plugins/indicators/user")
            user_dir.mkdir(parents=True, exist_ok=True)
            (user_dir / f"{name}.py").write_text(code)

            # Save params schema alongside as JSON sidecar
            import json as _json
            schema = body.get("params_schema", [])
            (user_dir / f"{name}.json").write_text(_json.dumps(schema, indent=2))

            return web.json_response({"status": "saved", "name": name})
        except Exception as e:
            self.logger.error(f"save_custom_indicator error: {e}")
            return web.json_response({"error": str(e)}, status=500)

    # ------------------------------------------------------------------ #
    # On-demand strategy endpoint                                          #
    # ------------------------------------------------------------------ #

    async def compute_strategy(self, request):
        """
        POST /api/strategies/compute
        Body: { type, exchange, symbol, **strategy_params }
        Returns SSE stream: progress events then signals.
        """
        try:
            body     = await request.json()
            from core.params import StrategyParams

            type_    = body.pop("type")
            exchange = body.pop("exchange")
            symbol   = body.pop("symbol")
            params   = StrategyParams.create(type_, **body)

            response = web.StreamResponse(headers={"Content-Type": "text/event-stream",
                                                    "Cache-Control": "no-cache"})
            await response.prepare(request)

            async def send(data: dict):
                await response.write(f"data: {json.dumps(data)}\n\n".encode())

            signals = await self.resolver.resolve_strategy(params, exchange, symbol,
                                                            progress_cb=send)
            await send({"stage": "done", "signals": signals})
            await response.write_eof()
            return response

        except Exception as e:
            self.logger.error(f"compute_strategy error: {e}")
            return web.json_response({"error": str(e)}, status=500)

    # ------------------------------------------------------------------ #
    # Chart data + metadata                                                #
    # ------------------------------------------------------------------ #

    async def get_chart_data(self, request):
        """
        GET /api/chart?exchange=...&symbol=...&timeframe=...&limit=...&start_ts=...&end_ts=...
        Aggregates candles on-demand if the requested timeframe isn't cached yet.
        """
        try:
            exchange         = request.query.get('exchange', '').strip().lower()
            symbol           = request.query.get('symbol',   '').strip().upper()
            timeframe        = request.query.get('timeframe', '1m').strip()
            limit            = int(request.query.get('limit', 2000))
            start_ts         = int(request.query['start_ts'])         if 'start_ts'         in request.query else None
            end_ts           = int(request.query['end_ts'])           if 'end_ts'           in request.query else None
            before_timestamp = int(request.query['before_timestamp']) if 'before_timestamp' in request.query else None
            if not exchange or not symbol:
                return web.json_response({'error': 'exchange and symbol required'}, status=400)

            # Ensure the requested timeframe is aggregated (no-op if already cached)
            if timeframe != '1m':
                await self.resolver._ensure_candles(timeframe, exchange, symbol)

            if before_timestamp is not None:
                # Backwards pagination: fetch the `limit` most recent candles
                # strictly before before_timestamp (DESC + flip to ascending)
                candles = await self.clickhouse.fetch_candles(
                    exchange, symbol, timeframe,
                    start_ts=start_ts, end_ts=before_timestamp - 1,
                    limit=limit, order='DESC',
                )
                candles = list(reversed(candles))
            elif start_ts is None and end_ts is None:
                # Initial load — return the most recent `limit` candles
                candles = await self.clickhouse.fetch_candles(
                    exchange, symbol, timeframe,
                    limit=limit, order='DESC',
                )
                candles = list(reversed(candles))
            else:
                candles = await self.clickhouse.fetch_candles(
                    exchange, symbol, timeframe,
                    start_ts=start_ts, end_ts=end_ts, limit=limit,
                )

            return web.json_response({
                'status': 'success',
                'data': candles,
                'count': len(candles),
            })
        except Exception as e:
            self.logger.error(f"get_chart_data error: {e}")
            return web.json_response({'error': str(e)}, status=500)

    async def get_timeframes(self, request):
        """Return distinct timeframes available in ClickHouse for exchange/symbol."""
        try:
            exchange = request.query.get('exchange', '').strip().lower()
            symbol   = request.query.get('symbol',   '').strip().upper()
            if not exchange or not symbol:
                return web.json_response({'error': 'exchange and symbol required'}, status=400)
            rows = await self.clickhouse._execute(
                "SELECT DISTINCT timeframe, count() AS cnt FROM candles "
                "WHERE exchange=%(ex)s AND symbol=%(sym)s "
                "GROUP BY timeframe ORDER BY timeframe",
                {"ex": exchange, "sym": symbol},
            )
            timeframes = [{"timeframe": r[0], "candles_count": int(r[1])} for r in rows]
            return web.json_response({'status': 'success', 'data': timeframes})
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)

    async def get_strategies_signals(self, request):
        """Return strategy signals from ClickHouse for chart display."""
        try:
            exchange = request.query.get('exchange', '').strip().lower()
            symbol   = request.query.get('symbol',   '').strip().upper()
            limit    = int(request.query.get('limit', 1000))

            where = "1=1"
            kw: dict = {}
            if exchange:
                where += " AND exchange=%(ex)s"; kw["ex"] = exchange
            if symbol:
                where += " AND symbol=%(sym)s"; kw["sym"] = symbol

            rows = await self.clickhouse._execute(
                f"SELECT timestamp, strategy_type, signal_type, confidence, price, metadata "
                f"FROM strategy_signals WHERE {where} "
                f"ORDER BY timestamp DESC LIMIT {int(limit)}",
                kw,
            )
            signals = [
                {"timestamp": r[0], "strategy_name": r[1], "signal_type": r[2],
                 "confidence": float(r[3]), "price": float(r[4]),
                 "metadata": json.loads(r[5]) if r[5] else {}}
                for r in rows
            ]
            return web.json_response({'status': 'success', 'data': {'signals': signals, 'count': len(signals)}})
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)

    async def get_progress(self, request):
        """Return candle counts per (exchange, symbol, timeframe) from ClickHouse."""
        try:
            rows = await self.clickhouse._execute(
                "SELECT exchange, symbol, timeframe, count() AS cnt, "
                "min(timestamp) AS first_ts, max(timestamp) AS last_ts "
                "FROM candles GROUP BY exchange, symbol, timeframe "
                "ORDER BY exchange, symbol, timeframe"
            )
            pairs = []
            for r in rows:
                pairs.append({
                    'exchange': r[0], 'symbol': r[1], 'timeframe': r[2],
                    'candle_count': int(r[3]),
                    'first_candle': datetime.fromtimestamp(int(r[4]), tz=timezone.utc).strftime('%Y-%m-%d') if r[4] else None,
                    'last_candle':  datetime.fromtimestamp(int(r[5]), tz=timezone.utc).strftime('%Y-%m-%d') if r[5] else None,
                })
            return web.json_response({'pairs': pairs})
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)

    # ------------------------------------------------------------------ #
    # HTML pages                                                           #
    # ------------------------------------------------------------------ #

    @aiohttp_jinja2.template('dashboard.html')
    async def dashboard(self, request):
        return {
            'title': 'Trading Bot Dashboard',
            'timestamp': datetime.now(timezone.utc).isoformat(),
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
        if self.clickhouse:
            await self.clickhouse.close()


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