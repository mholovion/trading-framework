# Trading Framework

A reactive, plugin-based framework for building algorithmic trading systems. Handles the entire data pipeline — from raw exchange candles through indicator calculation to strategy signal generation — so you can focus on writing your trading logic.

**Example project:** [trading-bot-example](https://github.com/mholovion/trading-bot-example)

---

## Features

- **Plugin system** — implement your exchange connector, indicators, and strategies as isolated plugins; the framework discovers and wires them automatically
- **Reactive data pipeline** — candles arrive in real time, aggregated timeframes are computed on-the-fly, indicators recalculate, strategy signals are emitted
- **Built-in gap recovery** — on startup the framework detects missing candles, indicators, or signals and backfills them automatically
- **Historical backfill** — configurable date range, concurrent batched downloads, resume-safe
- **Web dashboard** — candlestick chart, indicator overlays, strategy signal overlay, performance statistics panel
- **TimescaleDB** — time-series optimised storage with automatic partitioning
- **RabbitMQ** — decoupled inter-service messaging; each service is independently scalable

---

## Architecture

```
Exchange API
    │  WebSocket (real-time) + REST (historical)
    ▼
┌─────────────────────┐
│  RealtimeDataService │  ◄─ streams live 1m candles
│  HistoricalData      │  ◄─ backfills missing ranges
└────────┬────────────┘
         │ candle_updates (RabbitMQ)
         ▼
┌─────────────────────┐
│  AggregationService  │  1m → 4h → 1d → 1w
└────────┬────────────┘
         │ aggregated candles (DB write)
         ▼
┌──────────────────────────┐
│  IndicatorsReactiveService│  RSI, SMA, … (plugin-based)
│  IndicatorsGapService     │  fills missing indicator rows
└────────┬─────────────────┘
         │ indicator_updates (RabbitMQ)
         ▼
┌───────────────────────────┐
│  StrategiesReactiveService │  your strategy plugin runs here
│  StrategiesGapService      │  recalculates historical signals
└────────┬──────────────────┘
         │ strategy_signals (DB write)
         ▼
┌─────────────┐
│  REST API   │  /api/* + web dashboard
└─────────────┘
```

All services are coordinated by `services/orchestrator.py` and communicate via RabbitMQ queues. Services can run in a single process (default) or be deployed independently.

---

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Git

### 1. Clone the example project

```bash
git clone --recurse-submodules https://github.com/mholovion/trading-bot-example.git
cd trading-bot-example
```

The `--recurse-submodules` flag pulls this framework into the `framework/` directory automatically.

### 2. Start

```bash
docker compose up -d
```

The demo exchange plugin generates synthetic BTC/USDT price data — no API keys required.

### 3. Open the dashboard

```
http://localhost:8080
```

---

## Building Your Own Bot

### Project structure

```
my-trading-bot/
├── framework/               ← this repo (git submodule)
├── plugins/
│   ├── exchanges/
│   │   └── myexchange_plugin.py
│   ├── indicators/          ← optional custom indicators
│   │   └── macd_plugin.py
│   └── strategies/
│       └── my_strategy_plugin.py
├── config/
│   ├── main.yaml
│   ├── connections.yaml
│   ├── indicators.yaml
│   ├── strategies.yaml
│   └── aggregation.yaml
├── Dockerfile
└── docker-compose.yml
```

### Add as a submodule

```bash
git submodule add https://github.com/mholovion/trading-framework.git framework
```

### Dockerfile

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY framework/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY framework/ ./framework/
COPY plugins/ ./plugins/
COPY config/ ./config/
RUN mkdir -p logs
ENV PYTHONPATH=/app/framework:/app
CMD ["python", "framework/services/orchestrator.py", "--config-dir", "/app/config"]
```

> **Why `PYTHONPATH=/app/framework:/app`?**
> The framework uses Python namespace packages. Both `framework/plugins/` and your `plugins/` directory contribute to the `plugins.*` namespace — no `__init__.py` required. `plugins.indicators.rsi` resolves to the framework's RSI; `plugins.strategies.my_strategy_plugin` resolves to yours.

---

## Plugin Development

### Exchange Plugin

Connects the framework to a real (or mock) exchange. Implement `ExchangePlugin` from `plugins/exchanges/base.py`.

```python
# plugins/exchanges/myexchange_plugin.py
from plugins.exchanges.base import ExchangePlugin

class MyexchangePlugin(ExchangePlugin):

    async def initialize(self) -> bool:
        # Connect, authenticate, validate credentials
        return True

    async def get_server_time(self) -> int:
        # Return current exchange server timestamp (Unix seconds)
        ...

    async def normalize_symbol(self, symbol: str) -> str:
        # Convert "BTC_USDT" to exchange-specific format, e.g. "BTCUSDT"
        return symbol.replace("_", "")

    async def get_historical_candles(
        self, symbol, timeframe, start_timestamp, end_timestamp, limit=1000
    ) -> list[dict]:
        # Fetch OHLCV candles from the exchange REST API.
        # Each dict must have: timestamp, open, high, low, close, volume
        ...

    async def start_realtime_stream(self, symbol, timeframe, callback) -> bool:
        # Open WebSocket, call `await callback(candle_dict)` for each new candle
        ...

    async def stop_realtime_stream(self, symbol, timeframe) -> bool:
        ...

    async def cleanup(self):
        ...
```

**Naming convention:** file `myexchange_plugin.py` → class `MyexchangePlugin`. The framework loads the plugin by reading `plugin: "myexchange"` from `config/main.yaml` and importing `plugins.exchanges.myexchange_plugin`.

**Register in config/main.yaml:**

```yaml
exchanges:
  myexchange:
    plugin: "myexchange"
    enabled: true
    api_key: ""
    api_secret: ""
    historical_url: "https://api.myexchange.com"
    realtime_url: "wss://stream.myexchange.com"
```

---

### Indicator Plugin

Calculates a single indicator value given a window of candles. Implement `IndicatorPlugin` from `plugins/indicators/base.py`.

```python
# plugins/indicators/macd_plugin.py
from typing import Any, Dict, List, Optional
from plugins.indicators.base import IndicatorPlugin

class MacdPlugin(IndicatorPlugin):

    async def calculate(self, data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        `data` is a list of candle dicts ordered oldest → newest.
        Each dict has: timestamp, open, high, low, close, volume.
        Return a dict with at least {"value": <float>} on success, or None.
        """
        if len(data) < self.parameters["slow_period"]:
            return None

        closes = [float(c["close"]) for c in data]
        macd_line = self._ema(closes, self.parameters["fast_period"]) \
                  - self._ema(closes, self.parameters["slow_period"])

        return {
            "value": macd_line,
            "metadata": {"signal": self._ema([macd_line], self.parameters["signal_period"])},
        }

    def get_required_periods(self) -> int:
        return self.parameters["slow_period"] + self.parameters["signal_period"]

    def validate_parameters(self) -> bool:
        return all(k in self.parameters for k in ("fast_period", "slow_period", "signal_period"))
```

**Register in config/indicators.yaml:**

```yaml
indicators:
  btc_macd_1h:
    connection: "myexchange_btc_1m"
    plugin: "macd"                  # loads plugins/indicators/macd_plugin.py → MacdPlugin
    enabled: true
    source_timeframe: "1h"
    calculate_timeframe: "1h"
    data_source: "aggregated"
    parameters:
      fast_period: 12
      slow_period: 26
      signal_period: 9
```

The framework ships with **RSI** (`plugins/indicators/rsi.py`) and **SMA** (`plugins/indicators/sma.py`) ready to use.

---

### Strategy Plugin

Receives indicator values for a given timestamp and decides whether to emit a BUY, SELL, or no signal. Implement `StrategyPlugin` from `plugins/strategies/base.py`.

```python
# plugins/strategies/my_strategy_plugin.py
from typing import Any, Dict, List, Optional
from plugins.strategies.base import StrategyPlugin, StrategySignal, SignalType

class MyStrategyPlugin(StrategyPlugin):

    def __init__(self, config: Dict[str, Any], database_manager=None):
        super().__init__(config, database_manager)
        # self.parameters contains values from strategies.yaml → parameters
        self.rsi_indicator = self.parameters["rsi_indicator"]
        self.overbought    = self.parameters["overbought"]
        self.oversold      = self.parameters["oversold"]

    async def evaluate(
        self,
        indicators_data: Dict[str, Any],   # {indicator_name: {value, metadata, ...}}
        current_price: float,
        timestamp: int,                    # Unix seconds, bar close time
        connection_name: str,
    ) -> Optional[StrategySignal]:

        row = indicators_data.get(self.rsi_indicator)
        if not row:
            return None
        rsi = float(row["value"])

        if rsi <= self.oversold:
            return StrategySignal(
                signal_type=SignalType.BUY,
                price=current_price,
                confidence=min(1.0, (self.oversold - rsi) / self.oversold),
                metadata={"rsi": rsi},
            )
        if rsi >= self.overbought:
            return StrategySignal(
                signal_type=SignalType.SELL,
                price=current_price,
                confidence=min(1.0, (rsi - self.overbought) / (100 - self.overbought)),
                metadata={"rsi": rsi},
            )
        return None

    def validate_parameters(self) -> bool:
        return all(k in self.parameters for k in ("rsi_indicator", "overbought", "oversold"))

    def get_required_indicators(self) -> List[str]:
        return [self.rsi_indicator]
```

**Register in config/strategies.yaml:**

```yaml
strategies:
  btc_rsi_strategy:
    enabled: true
    connection: "myexchange_btc_1m"
    plugin: "my_strategy"               # loads plugins/strategies/my_strategy_plugin.py
    required_indicators:
      - btc_rsi_14_1h
    parameters:
      rsi_indicator: "btc_rsi_14_1h"
      overbought: 70
      oversold: 30
```

---

## Configuration Reference

### config/main.yaml

| Key | Description |
|-----|-------------|
| `database.*` | PostgreSQL / TimescaleDB connection settings |
| `execution.mode` | `full` \| `historical_only` \| `realtime_only` |
| `logging.level` | `DEBUG` \| `INFO` \| `WARNING` |
| `api.port` | Dashboard and REST API port (default `8080`) |
| `exchanges.<name>.*` | Exchange plugin config; `plugin` matches the file prefix |

### config/connections.yaml

Defines which symbol/timeframe pairs to track.

```yaml
connections:
  <connection_name>:
    exchange: "<exchange_name>"    # must match an entry in main.yaml exchanges
    symbol: "BTC_USDT"
    timeframe: "1m"               # base timeframe fetched from exchange
    historical:
      start_date: "2023-01-01T00:00:00Z"
      batch_size: 1000
    realtime:
      enabled: true
```

### config/aggregation.yaml

Maps source timeframes to aggregated targets.

```yaml
aggregation:
  source_mapping:
    "myexchange:BTC_USDT:1h": "1m"   # build 1h bars from 1m
    "myexchange:BTC_USDT:4h": "1h"
    "myexchange:BTC_USDT:1d": "4h"
```

### config/indicators.yaml

One entry per indicator instance. Multiple connections can run the same plugin with different parameters.

### config/strategies.yaml

One entry per strategy instance. `required_indicators` must list every indicator key the strategy reads from `indicators_data`.

---

## REST API

| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Service health and uptime |
| `GET /api/candles` | OHLCV candle data |
| `GET /api/indicators/<name>/data` | Indicator time series |
| `GET /api/strategies/signals` | All strategy signals |
| `GET /api/strategies/performance` | Trade statistics (FIFO pairing) |
| `GET /chart` | Candlestick dashboard |
| `GET /indicators` | Indicator chart |
| `GET /strategies` | Strategy signal chart |

---

## License

MIT
