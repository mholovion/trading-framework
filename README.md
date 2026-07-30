# tradingkit

[![CI](https://github.com/nivolon/trading-framework/actions/workflows/ci.yml/badge.svg)](https://github.com/nivolon/trading-framework/actions/workflows/ci.yml)

Async trading strategy framework built on [Polars](https://pola.rs), [ClickHouse](https://clickhouse.com),
and pluggable executors. Write an `Indicator`/`Strategy`/`DataSource` once, then run it
in-process, in a sandboxed subprocess, on a remote worker, or against historical data in
a backtest — without changing the plugin code.

---

## Install

```bash
pip install tradingkit-py
```

(PyPI distribution name is `tradingkit-py` — an unrelated project already holds the bare
`tradingkit` name — but the actual import is unaffected: `import tradingkit`.)

Requires Python ≥3.11. `TA-Lib` needs the native library installed first (see
[ta-lib.org](https://ta-lib.org)); everything else is a normal wheel dependency.

For development:

```bash
git clone https://github.com/nivolon/trading-framework.git
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
        # A signal has no mandatory fields — you pick the schema, exactly as a
        # DataSource picks its row schema. The framework only adds `timestamp`.
        # A plain dict works just as well as Signal(...).
        if bar.rsi < 30:
            return Signal(action="open_long", price=bar.close, rsi=bar.rsi)
        if bar.rsi > 70:
            return Signal(action="close", price=bar.close, rsi=bar.rsi)
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

The snippet above is deliberately minimal — synthetic CSV data, `BacktestRunner` only. For
a complete, runnable project (a real `DataSource` over WebSocket, a custom indicator next
to a built-in one, and a `Strategy` combining both — the *same* code run unchanged against
both historical data and a live stream via `Pipeline`), see
[tradingkit-example](https://github.com/nivolon/tradingkit-example). Its CI tracks this
repo's `main` branch directly, so it also doubles as a downstream regression check.

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

- **`BacktestRunner`** ([tradingkit/backtest/](tradingkit/backtest/)) — runs a strategy bar-by-bar over a pre-loaded `pl.DataFrame` and returns a `BacktestResult` (the data, the indicator series, and `signals_df`: the emitted signals as a timestamped table). It computes no PnL of its own — a signal's fields are the strategy's own vocabulary, so interpreting them belongs to analytics you configure, not to a built-in rule that has to guess which string means "buy".
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

## Compiled C++ kernels

`CppIndicator(cpp_code, so_bytes, **params)` carries a compiled `.so` and runs through
`CppRunnerPool` (see the executor table above) — the sandboxed path for performance-
critical or genuinely untrusted compute. The runner (`_docker/runner/runner_worker.py`)
caches loaded libraries by a hash of their bytes, not a name you supply: identical
`.so` bytes from two different `CppIndicator` instances (or two different strategies)
skip the tempfile-write + `dlopen`/ELF-relocation cost on every call after the first,
automatically, with no coordination needed between callers — you always just pass the
same bytes, the runner notices they've been seen before.

To reuse the same compiled kernel by name across your own strategy code, keep a small
registry and point `CppIndicator(so_bytes=...)` at it — reusing `get_builtin_registry()`,
the same env-var-driven lookup the [Aggregation](#aggregation) section below uses for
named builtins:

```python
# myapp/kernels.py
KERNELS = {
    "fast_rsi": open("kernels/fast_rsi.so", "rb").read(),
}
```

```bash
export TRADINGKIT_KERNELS_MODULE=myapp.kernels
```

```python
from tradingkit.core.plugin_registry import get_builtin_registry
from tradingkit import CppIndicator

kernels = get_builtin_registry("TRADINGKIT_KERNELS_MODULE", "KERNELS")
rsi = CppIndicator(cpp_code="...", so_bytes=kernels["fast_rsi"], period=14)
```

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

Combine data across sources/symbols (spread, ratio, custom index) with `Aggregation` —
declare which sources you need, then either a SQL expression (fast, ClickHouse-native,
zero Python) or a Python function (flexible, for logic SQL can't express):

```python
from tradingkit.aggregation import Aggregation, SourceRef, setup_aggregation, query_aggregation

class BtcEthSpread(Aggregation):
    OUTPUT_TABLE = "btc_eth_spread"
    btc = SourceRef("candles_btc_usdt", field="close")
    eth = SourceRef("candles_eth_usdt", field="close")

    def combine_sql(self) -> str:
        return "btc - eth"

agg = BtcEthSpread()
await setup_aggregation(db, agg)                              # one-time: installs the MVs, backfills existing data
result = await query_aggregation(db, agg, start_ts, end_ts)    # always fresh, computed at read time
```

**`combine_sql()` (SQL-native, the fast path).** `setup_aggregation()` installs one
Materialized View per declared source, all writing into a shared `AggregatingMergeTree`
alignment table keyed by timestamp — every source contributes its own partial state
independently, so arrival order never matters: a source that arrives late (even a
backfilled gap) still correctly completes the row once it lands, verified directly
against a real server. Rows where a source hasn't arrived yet are excluded by default
(`only_complete=True` in `query_cross_source`) rather than surfacing a misleading `0` —
ClickHouse's `argMaxMerge()` on an unwritten state returns the type's zero value, not
`NULL`, unless the stored value is wrapped in `Nullable` (which `ensure_cross_source_table`
already does — also verified directly). `setup_aggregation()` also backfills each
source's already-existing rows on setup (`backfill_cross_source`) — MVs alone only fire
on rows inserted *after* the MV exists, so without this a new aggregation on top of
already-populated tables would silently have no historical data. Once installed,
ClickHouse keeps the result live on every insert — no polling, no worker.

**`combine(ctx, start_ts, end_ts)` (Python fallback).** For logic SQL can't express —
rolling windows, joins beyond the declared sources, external state — override `combine()`
instead of `combine_sql()`. Same signature and `ctx.query(table, symbol=, start_ts=,
end_ts=)` access `AggregationContext` always had (reads *any* ClickHouse table by name).
`AggregationWorker` drives this path on a timer (`_POLL_INTERVAL`, 60s by default) — but
only for aggregations that actually need it: one with `combine_sql()` defined is skipped
entirely, since ClickHouse already keeps it current.

**Dynamic aggregations (`ScriptAggregation`).** For a SaaS UI editor to create new
aggregations without a framework release, wrap a code string the same way
`ScriptStrategy`/`ScriptIndicator` wrap strategy/indicator code:

```python
code = '''
OUTPUT_TABLE = "btc_eth_spread"
SOURCES = {
    "btc": SourceRef("candles_btc_usdt", field="close"),
    "eth": SourceRef("candles_eth_usdt", field="close"),
}
COMBINE_SQL = "btc - eth"
'''
agg = load_aggregation_plugin("__script__", {"_code": code})
```

Stored in `plugin_library` (`type="aggregation"`) exactly like strategies/indicators —
`load_aggregation_plugin()` dispatches on a type string the same way
`load_strategy_plugin`/`load_indicator_plugin` do: `"__script__"` for a `_code` param, or
a name registered via `TRADINGKIT_AGGREGATIONS_MODULE` (below).

**`AggregationContext.query()` directly** still works standalone for one-off/ad-hoc reads
(e.g. a script that just wants `await ctx.query("candles", symbol="BTC_USDT", ...)`
without going through the `Aggregation` class at all) — it's what `combine()` uses
internally, nothing about it changed.

**ClickHouse-native single-table aggregation (`Fold`).** Separate from `Aggregation`
above — for bucketing ONE table's own rows by time (`candles` → `candles_3600s` for 1h
buckets), see `Fold` in `tradingkit/schema.py`, plus `ensure_agg_table`/`ensure_mv`/
`backfill_agg`/`query_agg`. `DataCollector` uses this automatically per connection via
`aggregation(unit) -> AggSpec`-style scripts (`AggregationScript.is_ch_mv()`) — a
different mechanism from cross-source `Aggregation` above, sharing only the same
`plugin_library` storage (distinguished by which top-level names each script defines).
`Fold` covers two calling conventions, verified against a real server:

- Argument-less functions (`sum`, `count`, `avg`, `min`, `max`, `argMin`/`argMax` via
  `.first()`/`.last()`, ...) and combinator-style functions taking extra args alongside
  the column (`sumIf`, `countIf`, ...): `unit.qty.sumIf("side = 1")`.
- Leading-parameter functions (`quantile`, `quantileExact`, `topK`, ...), where the
  parameter isn't part of the serialized state and has to be supplied again at read
  time: `unit.price.quantile(0.95)`.

Both are covered by real-server integration tests. `Fold.fn` isn't validated against an
exhaustive allowlist of ClickHouse's 90+ aggregate functions (see the safety comment at
the top of `schema.py`), so a function outside the common set above may need adding to
`_LEADING_PARAM_FUNCTIONS` in `tradingkit/schema.py` if it uses the leading-parameter
convention — an unlisted one falls through to the flat convention and fails loudly with
a ClickHouse syntax/type error if that's the wrong choice for it, rather than silently
producing a wrong result.

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

[Business Source License 1.1](LICENSE) — source-available, not OSI-approved "open
source." Free for effectively everything, including internal commercial use; the one
thing it excludes is offering `tradingkit` (or a product mostly built from it) to third
parties as a hosted/managed backtesting or charting platform that competes with a paid
product from the author. Converts automatically to Apache License 2.0 on 2030-07-25 (or
sooner per-version — see the license text). For a license exception, contact
holovin.maksym@gmail.com.
