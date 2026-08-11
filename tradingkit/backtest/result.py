"""
tradingkit.backtest.result — BacktestResult.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl


@dataclass
class BacktestResult:
    """What a backtest produced: the data it ran over, the indicator series, and the
    signals the strategy emitted.

    Holds no trade or PnL logic on purpose. A signal's fields are the strategy author's
    own vocabulary (see Signal), so pairing them into positions and computing PnL is the
    job of a Metric over signals_df, told which columns mean what — not of a hardcoded
    rule that has to guess.
    """

    signals:    list[dict]
    data:       pl.DataFrame
    indicators: dict[str, pl.Series]

    @property
    def signals_df(self) -> pl.DataFrame:
        """Signals as a timestamped table, ready for IndicatorContext — the same
        primitive indicators are computed over. Columns are whatever the strategy
        emitted, unioned across signals, so bars carrying different fields leave nulls."""
        if not self.signals:
            return pl.DataFrame(schema={"timestamp": pl.Int64})
        return pl.DataFrame(self.signals)

    def compute(self, metric: Any) -> Any:
        """Run a Metric over this result. Anything derived — PnL, win rate, an equity
        curve — is computed here rather than baked into the result, because it depends on
        what this strategy's signal fields mean."""
        from tradingkit.metric import MetricContext
        return metric.compute(MetricContext(
            data=self.data, signals=self.signals_df, indicators=self.indicators,
        ))
