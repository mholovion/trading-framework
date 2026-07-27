# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.2.0] - 2026-07-27

### Added
- `Aggregation` / `ScriptAggregation` — declarative cross-source aggregation (e.g. a
  BTC/ETH spread) via a gap-closing two-Materialized-View pattern: each declared source
  writes its own partial state into a shared `AggregatingMergeTree` table, keyed by
  timestamp, so arrival order never matters and a late or backfilled source still
  completes the row correctly once it lands. Two ways to compute the result: `combine_sql()`
  (a validated SQL expression over the declared sources, kept live by ClickHouse on every
  insert — no polling) or `combine(ctx, start_ts, end_ts)` (a Python fallback for logic SQL
  can't express, with the same signature and `ctx.query()` power as the free-function
  `aggregate()` convention it replaces). `ScriptAggregation` / `load_aggregation_plugin()`
  mirror `ScriptStrategy` / `load_strategy_plugin` so a SaaS UI editor can create new
  aggregations without a framework release. The within-table bucket-aggregation mechanism
  (`Fold`, `AggregationScript.is_ch_mv()`) is unrelated and unchanged.
- Gap-recovery backoff: `_gap_loop()` now backs off exponentially (capped at
  `max_gap_interval_s`, default 3600s) after consecutive no-progress cycles instead of
  retrying at a fixed interval forever, and surfaces a distinct `"stalled"` connection
  status once `stalled_threshold` (default 5) consecutive failures is crossed — previously
  a connection stuck offline for hours was indistinguishable from one with a small, normal
  gap except by watching the gap count grow, and retried at the same rate regardless.

### Fixed
- `LiveFeed` routed live candles by `(symbol, timeframe)` only, dropping `exchange` — two
  different exchanges streaming the same symbol/timeframe would cross-deliver each other's
  candles to the wrong subscribers. `publish()` now requires `exchange`; the routing key
  also normalizes exchange case, since the same exchange was observed tagged inconsistently
  (`"whitebit"` vs `"WhiteBit"`) depending on connection config.
- `insert_unit_batch()` never wrote a `timeframe` column, so every row landed with
  ClickHouse's empty-string default regardless of the connection's actual timeframe,
  breaking any timeframe-filtered query. `ensure_raw_table()` now also migrates existing
  tables that predate this column.
- Cross-source aggregation hardcoded `UInt64` for the timestamp type baked into its
  `AggregateFunction(argMax, ..., UInt64)` state; a real `DataSource`'s schema (e.g.
  `Int64`, as produced by `ensure_raw_table`'s own Polars-to-ClickHouse type mapping)
  made ClickHouse reject the write outright (`CANNOT_CONVERT_TYPE`) rather than silently
  coercing it. Found via dogfooding the aggregation demo against a real data source, not
  anticipated up front — now covered by a regression test using an `Int64` source table.

## [0.1.1] - 2026-07-25

### Fixed
- `Signal.is_buy` / `Signal.is_sell` were referenced by `PipelineResult.trades` but never
  defined on `Signal`, so calling `.trades` on any pipeline result raised `AttributeError`.
- `Fold` SQL generation for parametric ClickHouse aggregate functions: quantile-family
  functions (`quantile`, `topK`, `groupArrayMovingAvg`, ...) now repeat their parameter in
  the `Merge` expression as ClickHouse requires — previously `quantileMerge` silently
  computed the median instead of the requested level. Combinator-style functions (`sumIf`,
  ...) keep their arguments flat and correctly drop them in `Merge`.
- Stale `mholovion` GitHub username references replaced with `nivolon` throughout the
  source and docs.

### Added
- `[project.urls]` (Homepage/Repository/Issues) in `pyproject.toml`.
- Compiled C++ kernels are now cached by content hash (SHA-256) inside the runner process,
  so identical `.so` bytes are written to disk and `dlopen`'d only once instead of on every
  call.

### Changed
- **License changed from MIT to [Business Source License 1.1](LICENSE)**, converting
  automatically to Apache License 2.0 on 2030-07-25 (or sooner, per version — see the
  license text). Free for effectively all use, including internal commercial use; the
  one thing it excludes is offering `tradingkit` as a competing hosted/managed
  backtesting or charting service. This is **not retroactive** — v0.1.0 remains
  available under MIT under its original terms; only v0.1.1 and later are BUSL-1.1.
- CI (`ruff check .`) was silently exposed to drift: an unbounded `ruff>=0.4` dev dependency
  meant a newer ruff release could expand its default rule set and fail CI on old code with
  no corresponding change. Pinned to `ruff>=0.4,<0.17` and cleaned up ~180 pre-existing
  findings (mostly `UP037`/`UP045`/`I001` mechanical modernizations, plus a handful of real
  fixes — `ClassVar` on shared class-level lookup tables, `Self` return types on `__aenter__`,
  a `NaN` check rewritten from `x == x` to `math.isnan(x)`, and a Docker container-stop
  callback rebound through `functools.partial` instead of a loop-variable-capturing lambda);
  `ruff check .` is now clean.

## [0.1.0] - 2026-07-21

Initial public release.
