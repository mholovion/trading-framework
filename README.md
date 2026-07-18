# tradingkit

[![CI](https://github.com/mholovion/trading-framework/actions/workflows/ci.yml/badge.svg)](https://github.com/mholovion/trading-framework/actions/workflows/ci.yml)

Async trading strategy framework built on [Polars](https://pola.rs), [ClickHouse](https://clickhouse.com),
and pluggable executors. Write an `Indicator`/`Strategy`/`DataSource` once, then run it
in-process, in a sandboxed subprocess, on a remote worker, or against historical data in
a backtest — without changing the plugin code.

---

## Install

```bash
pip install tradingkit
```

Requires Python ≥3.11. `TA-Lib` needs the native library installed first (see
[ta-lib.org](https://ta-lib.org)); everything else is a normal wheel dependency.

For development:

```bash
git clone https://github.com/mholovion/trading-framework.git
cd trading-framework
pip install -e ".[dev]"
```

---

## Quick start

```python
import asyncio
import numpy as np
import polars as pl
from tradingkit import Indicator, IndicatorContext, Strategy, Signal, BarContext
from tradingkit.backtest import BacktestRunner


class RSI(Indicator):
    def compute(self, ctx: IndicatorContext) -> np.ndarray:
        return ctx.ta.rsi(ctx.np.close, self.period)

    def required_periods(self) -> int:
        return self.period + 1


class MeanReversion(Strategy):
    async def on_bar(self, bar: BarContext) -> Signal | None:
        # Signal.type is a free-form string in general, but BacktestRunner's
        # FIFO trade pairing specifically recognizes "buy"/"sell" — use those
        # two if you want trades in BacktestResult.trades.
        if bar.rsi < 30:
            return Signal("buy", confidence=0.8, metadata={"direction": "long"})
        if bar.rsi > 70:
            return Signal("sell", confidence=0.7)
        return None


async def main() -> None:
    data = pl.read_csv("candles.csv")  # timestamp, open, high, low, close, volume
    runner = BacktestRunner()          # defaults to LocalExecutor (in-process, no isolation)
    result = await runner.run(
        data=data,
        indicators={"rsi": RSI(period=14)},
        strategy=MeanReversion(),
    )
    print(result.summary())


asyncio.run(main())
```

---

## Architecture

```
DataSource  ──►  Pipeline  ──►  Indicator(s)  ──►  Strategy  ──►  Signal
    │                              │                  │
    └──────────────  all three run through a PluginExecutor  ──────────┘
                     (Local / Subprocess / Remote / CppRunnerPool)

ClickHouseManager + DependencyResolver (optional, application layer)
    caches indicator/strategy output in ClickHouse and resolves
    dependencies on demand instead of recomputing from scratch.
```

- **`DataSource`** ([tradingkit/source.py](tradingkit/source.py)) — fetches OHLCV data (exchange, CSV, custom API).
- **`Indicator`** ([tradingkit/indicator.py](tradingkit/indicator.py)) — `compute(ctx) -> np.ndarray`, given a `IndicatorContext` wrapping a `pl.DataFrame`. `ctx.ta.*` exposes 200+ TA-Lib functions plus numba-compiled primitives.
- **`Strategy`** ([tradingkit/strategy.py](tradingkit/strategy.py)) — `on_bar(bar) -> Signal | None`, given a `BarContext` with row + indicator values as dynamic attributes (`bar.close`, `bar.rsi`, ...).
- **`Pipeline`** ([tradingkit/pipeline.py](tradingkit/pipeline.py)) — wires a source + indicators + strategy into a single re-runnable, serializable unit.
- **`PluginExecutor`** ([tradingkit/executor/](tradingkit/executor/)) — where the above actually run:

  | Executor | Isolation | Use case |
  |---|---|---|
  | `LocalExecutor` | none | development, trusted plugins |
  | `SubprocessExecutor` | separate process, Arrow IPC, memory/timeout limits | untrusted-ish plugins, single machine |
  | `RemoteExecutor` | separate host over HTTP | horizontal scaling, dedicated compute nodes — see [Security model](#security-model) before exposing it beyond localhost |
  | `CppRunnerPool` (attach to any of the above) | Docker, `--network=none`, seccomp, read-only rootfs | compiled `CppIndicator`/`CppStrategyPlugin` payloads |

- **`BacktestRunner`** ([tradingkit/backtest/](tradingkit/backtest/)) — runs a strategy bar-by-bar over a pre-loaded `pl.DataFrame` and returns a `BacktestResult` (trades, win rate, drawdown, ...).
- **`ClickHouseManager` + `DependencyResolver`** ([tradingkit/core/](tradingkit/core/)) — optional application-layer caching: resolves an indicator/strategy request by walking its dependency chain and only recomputing what's missing from ClickHouse.
- **`AggregationContext` / `AggregationWorker`** ([tradingkit/aggregation.py](tradingkit/aggregation.py)) — periodic scripts that read arbitrary ClickHouse tables and write derived series (cross-symbol spreads, higher-timeframe values projected onto a lower timeframe, etc.).

---

## Dynamic (string-based) plugins

`Indicator`/`Strategy`/`DataSource` subclasses are regular Python classes — defined in
your codebase, imported at process start. Sometimes the plugin code itself isn't known
until runtime instead (loaded from a database row, a config file, a user-facing editor):
`ScriptIndicator` / `ScriptStrategy` / `ScriptSource` / `AggregationScript` cover that case
by taking the plugin body as a plain string and `exec()`-ing it on demand, instead of
requiring a class defined ahead of time:

```python
from tradingkit import ScriptIndicator

indicator = ScriptIndicator(code="""
result = ta.rsi(close, 14)
""", period=14)
```

Because this runs via `exec()` with no sandboxing, treat the `code` string with the same
trust as any other code you run — see [Security model](#security-model).

---

## Security model

tradingkit executes plugin code by design — that's the product. Two things are worth
being explicit about before you deploy it:

**1. In-process script execution has no sandbox.** `ScriptIndicator`, `ScriptStrategy`,
`ScriptSource`, and `AggregationScript` run their `code` string via `exec()` with full
interpreter privileges — no import restrictions, no resource limits. Only run code you
personally wrote or reviewed this way. If you need to run less-trusted plugin code, route
it through `SubprocessExecutor` (process boundary + memory/timeout limits) or
`CppRunnerPool` (Docker, `--network=none`, seccomp, read-only rootfs) instead of
`LocalExecutor`.

**2. `tradingkit-runner` requires a token and rejects non-loopback binds without one.**
`RemoteExecutor`/`tradingkit-runner` ship live plugin objects over the wire as pickle, so
the runner authenticates every `/compute/*` request (`Authorization: Bearer <token>`,
constant-time comparison, checked before the body is read) and decodes payloads with an
allowlisting unpickler (`tradingkit.runner._safe_pickle`) that blocks the standard
`os`/`subprocess`/`eval` pickle RCE gadgets even from an authenticated caller. Binding to
anything other than `127.0.0.1` requires both `--allow-remote` and a token — the process
refuses to start otherwise.

That said: **the token is a shared secret between trusted peers, not a full authz or
encryption layer.** Plain HTTP sends it in cleartext, and the restricted unpickler is a
targeted defense against known gadget classes, not a general-purpose sandbox. Don't put
`tradingkit-runner` on the public internet — run it behind a TLS-terminating reverse
proxy or keep it on a private network (VPN/VPC):

```bash
tradingkit-runner --token "$(openssl rand -hex 32)"                 # loopback only, default
tradingkit-runner --token "$TOKEN" --host 0.0.0.0 --allow-remote    # only behind TLS/VPN
```

```python
executor = RemoteExecutor("https://tradingkit-runner.internal:8082", token=TOKEN)
```

**3. `DockerRunnerLauncher` mounts `docker.sock`.** Whatever process can reach that socket
has effective root on the host — it's what lets the launcher build and start the sandboxed
C++ runner containers on first use. Treat access to the host running `tradingkit-runner`/
`CppRunnerPool` with that in mind; don't give untrusted users shell access to it.

---

## Aggregation

`AggregationContext.query(table, symbol=..., start_ts=..., end_ts=...)` reads *any*
ClickHouse table by name — tradingkit ships no built-in aggregation scripts
(`BUILTIN_AGGREGATIONS` is an empty registry by default), but the mechanism covers a
few patterns directly:

- **Cross-symbol** — query two symbols, combine them (spread, ratio, custom index).
- **Cross-timeframe** — query a pre-aggregated table for a different bucket size (e.g.
  `candles_3600s` for 1h buckets) and project it onto a lower-timeframe series; this is
  the exact case `query()`'s own docstring calls out.
- **Cross-source** — the same `query()` call works on any table regardless of what wrote
  it, so combining two independently-ingested tables (e.g. spot price + a funding-rate
  feed) uses the same call shape. There's no bundled example of this one yet — you write
  the join.

```python
OUTPUT_TABLE  = "spread_btc_eth"
OUTPUT_SCHEMA = {"timestamp": "Int64", "spread": "Float64", "ratio": "Float64"}
INTERVAL_S    = 60

async def aggregate(ctx, start_ts: int, end_ts: int) -> list[dict]:
    btc = await ctx.query("candles", symbol="BTC_USDT", start_ts=start_ts, end_ts=end_ts)
    eth = await ctx.query("candles", symbol="ETH_USDT", start_ts=start_ts, end_ts=end_ts)
    joined = btc.join(eth, on="timestamp", suffix="_eth")
    return [
        {"timestamp": int(r["timestamp"]), "spread": r["close"] - r["close_eth"],
         "ratio": r["close"] / r["close_eth"] if r["close_eth"] else 0.0}
        for r in joined.iter_rows(named=True)
    ]
```

`AggregationWorker` polls `plugin_library` for scripts like this one (`type="aggregation"`,
defines `aggregate()`) and runs each on `INTERVAL_S`, writing results to `OUTPUT_TABLE`.

**Known limitation — parametric ClickHouse aggregates.** Separately from the Python-script
path above, `tradingkit.core.clickhouse` also has a lower-level, ClickHouse-native
aggregation path (`Fold` in `tradingkit/schema.py`, plus `ensure_agg_table`/`ensure_mv`/
`backfill_agg`/`query_agg`), backed by `AggregatingMergeTree` tables and materialized
views using ClickHouse's `-State`/`-Merge` combinators. Argument-less functions (`sum`,
`count`, `avg`, `min`, `max`, ...) generate correct SQL and are covered by integration
tests against a real server. Parametric functions (`quantile`, `sumIf`, ...) don't yet —
the combinator suffix gets placed incorrectly relative to the function's own arguments,
producing SQL ClickHouse won't accept as written. This needs an actual syntax-generation
fix, not a quick patch, and isn't done yet — don't rely on `Fold` with parametric
functions until this is resolved.

**Registering named builtins.** `plugin_library` (ClickHouse-stored, per-project) is the
primary way to add aggregation/strategy scripts, but a host app can additionally register
its own named builtins — e.g. a curated set it always wants available regardless of
project — by pointing an environment variable at a module:

```bash
export TRADINGKIT_AGGREGATIONS_MODULE=myapp.aggregations   # exposes BUILTIN_AGGREGATIONS: dict[str, str]
export TRADINGKIT_STRATEGIES_MODULE=myapp.strategies       # exposes BUILTIN_STRATEGIES: dict[str, str]
```

Unset (the default), both registries are empty — see `tradingkit.core.plugin_registry`.

---

## Development

```bash
pip install -e ".[dev]"
ruff check .
pytest
mypy tradingkit   # non-blocking in CI — existing type-coverage gaps, not enforced yet
```

CI (`.github/workflows/ci.yml`) runs `ruff` and `pytest` on Python 3.11 and 3.12 for every
push/PR, including a real ClickHouse service container and (on Docker-capable runners) a
real `CppRunnerPool` container.

Most of the suite needs nothing beyond `pip install -e ".[dev]"`. Two groups of tests
auto-skip unless their dependency is actually reachable, and light up locally too if you
have it running:

```bash
# tests/test_clickhouse_integration.py — real ClickHouse instead of a mocked transport
docker run --rm -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
CLICKHOUSE_HOST=localhost pytest tests/test_clickhouse_integration.py

# tests/test_cpp_pool.py — SubprocessRunnerLauncher tier needs only g++ (runs by default);
# the DockerRunnerLauncher tier additionally needs `pip install -e ".[dev,cpp]"` and Docker
pytest tests/test_cpp_pool.py
```

---

## License

MIT — see [LICENSE](LICENSE).
