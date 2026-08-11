"""
tradingkit.metric — Metric, MetricContext, ScriptMetric.

A Metric computes something over a finished run: a scalar (win rate), a series for a chart
(equity curve), or a table (a trade list). It runs over the whole window at once, unlike an
Indicator, which produces one value per bar.

The framework ships the mechanism and no metrics — same split as strategies and
aggregations, where BUILTIN_* is empty here and a host app registers its own via
tradingkit.core.plugin_registry. That is not an omission: a signal's fields are the strategy
author's vocabulary (see Signal), so no metric shipped in the framework could know what a
column means without guessing. The framework once did guess — it paired trades by matching
Signal.type == "buy" and reported a short strategy's real +25 as a fabricated long trade of
+5, silently. Analytics that must be told which columns mean what belongs where those
decisions are made, not in the core.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import polars as pl

from tradingkit.indicator import IndicatorContext

# TODO: metrics for run_live(). A metric is defined over a whole window, while a live run
# yields one signal at a time -- "the equity curve so far" needs a recompute-vs-incremental
# decision the backtest case never forces.


class MetricContext:
    """
    Everything a metric may read: the source rows, the emitted signals, and the computed
    indicator series.

    `data` and `signals` are IndicatorContext — the same wrapper indicators compute
    through, so `ctx.signals.price` and `ctx.data.close` behave exactly as they do
    elsewhere. Both are built lazily, so a metric that only reads signals doesn't fail
    because the run carried an empty data frame. The raw frames stay available as
    `signals_df`/`data_df` for ordinary Polars work.
    """

    def __init__(
        self,
        data: pl.DataFrame | None = None,
        signals: pl.DataFrame | None = None,
        indicators: dict[str, pl.Series] | None = None,
    ) -> None:
        self.data_df    = data if data is not None else pl.DataFrame()
        self.signals_df = signals if signals is not None else pl.DataFrame()
        self.indicators = indicators or {}
        self._data_ctx: IndicatorContext | None = None
        self._signals_ctx: IndicatorContext | None = None

    @property
    def data(self) -> IndicatorContext:
        if self._data_ctx is None:
            self._data_ctx = IndicatorContext(self.data_df)
        return self._data_ctx

    @property
    def signals(self) -> IndicatorContext:
        if self._signals_ctx is None:
            self._signals_ctx = IndicatorContext(self.signals_df)
        return self._signals_ctx


class Metric(ABC):
    """
    Base class for analytics over a finished run.

        class AverageHoldTime(Metric):
            def compute(self, ctx: MetricContext):
                return ctx.signals_df.select(...)

    Params are stored and exposed as attributes, the same way Indicator does it, so a
    metric configured from JSON behaves like a hand-written one. Column names a metric
    depends on belong in those params rather than in its body — that keeps it usable
    against a vocabulary its author never saw.
    """

    def __init__(self, **params: Any) -> None:
        self.params = params
        for k, v in params.items():
            setattr(self, k, v)

    @abstractmethod
    def compute(self, ctx: MetricContext) -> Any:
        """Compute over the whole window. Return anything: a scalar, a pl.Series or
        pl.DataFrame for a chart, a list of dicts for a table."""
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.params})"


class ScriptMetric(Metric):
    """
    Wraps user-provided metric code, the same way ScriptIndicator/ScriptStrategy wrap
    indicator and strategy code.

    The code receives `ctx` (MetricContext), `signals`/`data` (pl.DataFrame),
    `indicators`, `pl`, `np`, each declared param by name, and `params` as a dict, and
    MUST assign `result`:

        ScriptMetric(code="result = signals.height")
        ScriptMetric(code="result = signals['pnl'].sum()")

    `params` is there so a script can default a column name it wasn't given
    (`params.get("action_col", "action")`) instead of raising NameError — the column names
    a metric reads are configuration, and configuration usually has defaults.

    Runs in-process, like every Script* plugin outside the executor path — sandboxing user
    metric code is the same open question as streaming's (see framework.todo TASK-018).
    """

    def __init__(self, code: str, **params: Any) -> None:
        super().__init__(**params)
        self._code = code

    def compute(self, ctx: MetricContext) -> Any:
        import numpy as np

        namespace: dict[str, Any] = {
            "ctx":        ctx,
            "signals":    ctx.signals_df,
            "data":       ctx.data_df,
            "indicators": ctx.indicators,
            "pl":         pl,
            "np":         np,
            "params":     dict(self.params),
            "result":     None,
            **self.params,
        }
        exec(compile(self._code, "<ui_metric>", "exec"), namespace)  # noqa: S102
        if namespace.get("result") is None:
            raise ValueError("Metric code must assign to 'result'")
        return namespace["result"]


def load_metric_plugin(type_: str, params_dict: dict) -> Metric:
    """
    Create a Metric plugin from a type string and params dict — same dispatch shape as
    load_aggregation_plugin/load_strategy_plugin/load_indicator_plugin.

    '__script__'  — ScriptMetric wrapping user code (from '_code' param).
    named types   — looks up in a host-registered builtin registry, see
                    tradingkit.core.plugin_registry.
    """
    if type_ == "__script__":
        code = params_dict.get("_code", "")
        if not code:
            raise ValueError("Metric type '__script__' requires '_code' in params")
        params = {k: v for k, v in params_dict.items() if k != "_code"}
        return ScriptMetric(code=code, **params)

    from tradingkit.core.plugin_registry import get_builtin_registry
    builtins_ = get_builtin_registry("TRADINGKIT_METRICS_MODULE", "BUILTIN_METRICS")
    code = builtins_.get(type_)

    if not code:
        raise ValueError(
            f"Unknown metric type {type_!r}. "
            f"Use '__script__' with '_code', or a registered builtin name."
        )
    params = {k: v for k, v in params_dict.items() if k != "_code"}
    return ScriptMetric(code=code, **params)


#: Empty by default -- a host app registers its own via get_builtin_registry()
#: (tradingkit.core.plugin_registry), the same way BUILTIN_AGGREGATIONS works.
BUILTIN_METRICS: dict[str, str] = {}


__all__ = [
    "Metric",
    "MetricContext",
    "ScriptMetric",
    "load_metric_plugin",
    "BUILTIN_METRICS",
]