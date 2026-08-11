"""
tradingkit.pipeline — Pipeline: source + indicators + strategy → PipelineResult.

A Pipeline is the serializable "project" concept.
It can be saved to ClickHouse (plugin_library) and re-run later.

Usage:
    pipeline = Pipeline(
        name="my_rsi_strategy",
        source=ScriptSource(code="..."),
        indicators={"rsi": WeightedRSI(14), "ema": EMA(50)},
        strategy=MyStrategy(),
    )
    result = await pipeline.run(
        "SOL_USDT", parse_timeframe("4h"), start_ts=..., end_ts=...
    )
    print(result.signals_df)
"""
from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import polars as pl

from tradingkit.indicator import Indicator, IndicatorContext
from tradingkit.source import DataSource
from tradingkit.strategy import BarContext, Signal, Strategy

logger = logging.getLogger(__name__)


async def _gap_rows(
    rows: list[dict], source: DataSource, symbol: str, timeframe: int,
) -> list[dict]:
    """Real rows missing from a timestamp-ordered sequence, re-fetched from the source.

    Indicators are computed positionally — TA-Lib and numpy see an array index, not a
    timestamp — so a missing bar silently makes two rows that are minutes apart look
    adjacent. Filling the hole with synthetic data is not an option: verified against
    real TA-Lib, a single NaN poisons every subsequent output for the rest of the array
    (RSI, EMA and even SMA alike), and forward-filling invents prices that never traded.
    So the gap is closed with the real bars, re-fetched the same way DataCollector's
    gap recovery does.

    Detection, batching and fetching are all the source's own hooks, so a source that
    knows better — one with maintenance windows, or a tighter request limit — overrides
    them once and both run() and run_live() follow.
    """
    if len(rows) < 2:
        return []

    # Imported lazily: collector.py pulls in ClickHouse at module level, and Pipeline is
    # deliberately storage-agnostic — only the orchestration is shared, not the backend.
    from tradingkit.collector import recover_gaps

    filler: list[dict] = []

    async def _fetch(start_ts: int, end_ts: int) -> None:
        try:
            filler.extend(await source.fetch_gap(symbol, timeframe, start_ts, end_ts))
        except Exception as exc:
            # An unreachable exchange leaves the gap open, exactly as before this
            # existed — it must not take down the backtest or the live stream.
            logger.warning(f"Gap backfill failed for [{start_ts}, {end_ts}]: {exc}")

    await recover_gaps(
        detect=lambda: source.detect_gaps(rows, timeframe),
        batch=source.batch_gaps,
        fetch=_fetch,
    )
    return sorted(filler, key=lambda r: r["timestamp"])


def _as_signal(result: Signal | dict, row: dict) -> Signal:
    """Normalise whatever on_bar() returned into a Signal, stamping the bar's timestamp
    when the strategy didn't set one.

    Only `timestamp` is filled in: auto-filling anything else -- a `price` taken from
    `row["close"]`, as this used to do -- would privilege one field name and assume the
    source is OHLCV, which sources are not required to be. A strategy that wants a price
    on its signal has `bar.close` and can say so.
    """
    signal = result if isinstance(result, Signal) else Signal(**result)
    if getattr(signal, "timestamp", None) is None:
        signal.timestamp = row.get("timestamp")
    return signal


def _signals_to_df(signals: list[Signal]) -> pl.DataFrame:
    """Signals as a timestamped table: columns are whatever the strategy emitted, unioned
    across signals, so bars that carried different fields simply leave nulls. `timestamp`
    is always present (run() stamps it), which is what IndicatorContext requires."""
    if not signals:
        return pl.DataFrame(schema={"timestamp": pl.Int64})
    return pl.DataFrame([s.to_dict() for s in signals])


def _merge_rows(rows: list[dict], filler: list[dict]) -> list[dict]:
    """Splice backfilled rows into the original sequence, deduplicated by timestamp.
    Rows that were already there win — a re-fetch that overlaps must not replace data
    the source already streamed."""
    if not filler:
        return rows
    merged = {r["timestamp"]: r for r in filler}
    merged.update({r["timestamp"]: r for r in rows})
    return [merged[ts] for ts in sorted(merged)]


# ------------------------------------------------------------------ #
# PipelineResult                                                       #
# ------------------------------------------------------------------ #

@dataclass
class PipelineResult:
    """Carrier of what a run produced: the data it ran over, the indicator series, and
    the signals the strategy emitted. Deliberately holds no trade/PnL logic — a signal's
    fields are the strategy author's own vocabulary, so interpreting them (pairing
    positions, computing PnL) belongs to a Metric over signals_df, configured with which
    columns mean what."""

    signals:    list[Signal]
    data:       pl.DataFrame
    indicators: dict[str, pl.Series]
    #: Results of the metrics declared on the Pipeline, by name. Empty unless the pipeline
    #: declared any -- anything derived is opt-in, since it depends on what this strategy's
    #: signal fields mean.
    metrics:    dict[str, Any] = field(default_factory=dict)

    def compute(self, metric: Any) -> Any:
        """Run a Metric over this result, without having declared it up front."""
        from tradingkit.metric import MetricContext
        return metric.compute(MetricContext(
            data=self.data, signals=self.signals_df, indicators=self.indicators,
        ))

    @property
    def signals_df(self) -> pl.DataFrame:
        """Signals as a timestamped table, ready for IndicatorContext — the same
        primitive indicators are computed over."""
        return _signals_to_df(self.signals)


# ------------------------------------------------------------------ #
# Pipeline                                                             #
# ------------------------------------------------------------------ #

@dataclass
class Pipeline:
    """
    Serializable project: source + indicators + strategy.

    Can be saved to ClickHouse via db.save_pipeline() / db.load_pipeline().
    Run via pipeline.run() for historical backtesting.
    """
    name:       str
    source:     DataSource
    indicators: dict[str, Indicator]
    strategy:   Strategy
    executor:   Any = None   # PluginExecutor | None → defaults to LocalExecutor
    #: Metrics computed after every run(), by name -> PipelineResult.metrics. The framework
    #: ships none: what a signal's fields mean is the strategy author's business, so a
    #: metric has to be told which columns it reads (see tradingkit.metric).
    metrics:    dict[str, Any] = field(default_factory=dict)

    def _get_executor(self) -> Any:
        if self.executor is not None:
            return self.executor
        from tradingkit.executor.local import LocalExecutor
        return LocalExecutor()

    async def run(
        self,
        symbol: str,
        timeframe: int,
        start_ts: int,
        end_ts: int,
    ) -> PipelineResult:
        """
        Run pipeline over a historical date range.
        1. Fetch data via source
        2. Compute all indicators
        3. Call strategy.on_bar() for each bar
        Returns PipelineResult with signals, data, and indicator series.

        `timeframe` is an integer step in the source's own timestamp unit (see
        DataSource.timestamp_unit) — call parse_timeframe("4h") yourself to convert a
        human string, rather than the Pipeline guessing the unit for you.
        """
        executor = self._get_executor()

        df: pl.DataFrame = await executor.fetch_source_data(
            self.source, symbol, timeframe, start_ts, end_ts
        )
        if df.height == 0:
            return PipelineResult(signals=[], data=df, indicators={})

        # Close gaps before computing anything: indicators are positional, so a hole in
        # the history silently shifts every value after it (see _gap_rows).
        rows = df.to_dicts()
        filler = await _gap_rows(rows, self.source, symbol, timeframe)
        if filler:
            logger.info(f"Backfilled {len(filler)} missing bars for {symbol}")
            df = pl.DataFrame(_merge_rows(rows, filler))

        ctx = IndicatorContext(df)

        ind_series: dict[str, pl.Series] = {}
        for name, indicator in self.indicators.items():
            ind_series[name] = await executor.compute_indicator(indicator, ctx)

        signals: list[Signal] = []
        rows = df.to_dicts()

        for i, row in enumerate(rows):
            indicators_at_bar: dict[str, float] = {
                name: (series[i] if series[i] is not None else float("nan"))
                for name, series in ind_series.items()
            }
            series_map: dict[str, pl.Series] = {
                name: series[:i + 1] for name, series in ind_series.items()
            }
            bar = BarContext(row, indicators_at_bar, series_map)
            try:
                signal = await executor.process_strategy_bar(self.strategy, bar)
            except Exception as exc:
                logger.debug(f"Strategy error at bar {i}: {exc}")
                signal = None

            if signal is not None:
                signals.append(_as_signal(signal, row))

        return PipelineResult(
            signals=signals, data=df, indicators=ind_series,
            metrics=self._compute_metrics(df, _signals_to_df(signals), ind_series),
        )

    def _compute_metrics(
        self, data: pl.DataFrame, signals_df: pl.DataFrame, indicators: dict[str, pl.Series],
    ) -> dict[str, Any]:
        """Run the declared metrics over one shared context. A metric that asks for a
        column these signals don't have reports None and logs why, rather than discarding
        the whole run — the same per-item tolerance run() already gives a failing bar."""
        if not self.metrics:
            return {}
        from tradingkit.metric import MetricContext
        ctx = MetricContext(data=data, signals=signals_df, indicators=indicators)
        results: dict[str, Any] = {}
        for name, metric in self.metrics.items():
            try:
                results[name] = metric.compute(ctx)
            except Exception as exc:
                logger.warning(f"Metric {name!r} failed: {exc}")
                results[name] = None
        return results

    async def run_live(
        self,
        symbol: str,
        timeframe: int,
    ) -> AsyncIterator[Signal]:
        """
        Stream live signals as new rows arrive from source.stream().
        Yields Signal objects. Requires source to support streaming.

        `timeframe` is an integer step in the source's own timestamp unit — see run().

        A stream that drops bars (a WebSocket reconnect that doesn't replay what was
        missed) would otherwise corrupt every indicator value afterwards, since they are
        computed positionally. Missing bars are re-fetched as real data and run fully
        through the strategy, so they can produce signals of their own — those carry
        `gap_recovered=True` so a consumer can treat a signal for an already-minutes-old
        bar differently from a live one.
        """
        executor = self._get_executor()

        ind_series: dict[str, pl.Series] = {}
        rows_acc: list[dict] = []

        async def _step(row: dict, *, gap_recovered: bool) -> Signal | None:
            # A bar re-fetched to close a gap can race with the same bar arriving from
            # the stream moments later (delayed, not actually lost) -- replace rather
            # than append, or one bar would be processed twice and could yield two
            # signals.
            if rows_acc and rows_acc[-1]["timestamp"] == row["timestamp"]:
                rows_acc[-1] = row
            else:
                rows_acc.append(row)

            ctx = IndicatorContext(pl.DataFrame(rows_acc))
            for name, indicator in self.indicators.items():
                try:
                    ind_series[name] = await executor.compute_indicator(indicator, ctx)
                except Exception as exc:
                    # run() already tolerates a bad bar per-strategy; run_live() had no
                    # equivalent, so one exception killed the whole stream.
                    logger.debug(f"Indicator {name!r} error: {exc}")

            i = len(rows_acc) - 1
            indicators_at_bar = {
                name: (series[i] if i < len(series) and series[i] is not None
                       else float("nan"))
                for name, series in ind_series.items()
            }
            bar = BarContext(row, indicators_at_bar, {
                name: series[:i + 1] for name, series in ind_series.items()
            })
            try:
                signal = await executor.process_strategy_bar(self.strategy, bar)
            except Exception as exc:
                logger.debug(f"Strategy error at {row.get('timestamp')}: {exc}")
                return None

            if signal is None:
                return None
            signal = _as_signal(signal, row)
            signal.gap_recovered = gap_recovered
            return signal

        async for row in self.source.stream(symbol, timeframe):
            if rows_acc:
                filler = await _gap_rows(
                    [rows_acc[-1], row], self.source, symbol, timeframe,
                )
                if filler:
                    logger.info(f"Backfilled {len(filler)} missing bars for {symbol}")
                for missing_row in filler:
                    signal = await _step(missing_row, gap_recovered=True)
                    if signal is not None:
                        yield signal

            signal = await _step(row, gap_recovered=False)
            if signal is not None:
                yield signal

    def to_dict(self) -> dict:
        """Serialize to JSON-compatible dict for ClickHouse storage."""
        import base64
        import pickle
        return {
            "name":       self.name,
            "source":     base64.b64encode(pickle.dumps(self.source)).decode(),
            "indicators": {
                k: base64.b64encode(pickle.dumps(v)).decode()
                for k, v in self.indicators.items()
            },
            "strategy":   base64.b64encode(pickle.dumps(self.strategy)).decode(),
        }

    @classmethod
    def from_dict(cls, data: dict, executor: Any = None) -> Pipeline:
        """
        Deserialize from dict (loaded from ClickHouse plugin_library).

        Uses the same allowlisting unpickler as tradingkit-runner
        (tradingkit.runner._safe_pickle) rather than raw pickle.loads() — a saved
        pipeline is application-controlled data (editable via plugin_library), not
        a hardcoded trust boundary, so it gets the same treatment as network input.
        """
        import base64

        from tradingkit.runner import _safe_pickle
        return cls(
            name=data["name"],
            source=_safe_pickle.loads(base64.b64decode(data["source"])),
            indicators={
                k: _safe_pickle.loads(base64.b64decode(v))
                for k, v in data.get("indicators", {}).items()
            },
            strategy=_safe_pickle.loads(base64.b64decode(data["strategy"])),
            executor=executor,
        )


__all__ = ["Pipeline", "PipelineResult"]
