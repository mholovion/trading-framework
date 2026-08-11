"""tradingkit.metric — Metric/MetricContext/ScriptMetric and the Pipeline wiring."""
from __future__ import annotations

import polars as pl
import pytest

from tradingkit.metric import Metric, MetricContext, ScriptMetric, load_metric_plugin
from tradingkit.pipeline import PipelineResult
from tradingkit.strategy import Signal


def _signals() -> pl.DataFrame:
    return pl.DataFrame([
        {"timestamp": 1, "action": "open_long", "price": 10.0},
        {"timestamp": 2, "action": "close", "price": 12.0},
    ])


# ------------------------------------------------------------------ #
# MetricContext                                                        #
# ------------------------------------------------------------------ #

def test_context_exposes_signals_as_indicator_context():
    """Analytics over signals is the same primitive as indicators over prices."""
    ctx = MetricContext(signals=_signals())
    assert ctx.signals.price.to_list() == [10.0, 12.0]
    assert ctx.signals_df.height == 2


def test_context_data_is_lazy():
    """A metric that only reads signals must not fail because the run carried an empty
    data frame -- IndicatorContext requires a `timestamp` column an empty frame lacks."""
    ctx = MetricContext(signals=_signals())
    assert ctx.signals.price.to_list() == [10.0, 12.0]   # never touches .data
    with pytest.raises(ValueError, match="timestamp"):
        ctx.data


def test_context_defaults_are_empty_not_none():
    ctx = MetricContext()
    assert ctx.signals_df.height == 0
    assert ctx.data_df.height == 0
    assert ctx.indicators == {}


# ------------------------------------------------------------------ #
# Metric / ScriptMetric                                                #
# ------------------------------------------------------------------ #

def test_metric_params_become_attributes():
    class M(Metric):
        def compute(self, ctx):
            return self.action_col

    m = M(action_col="act")
    assert m.action_col == "act"
    assert m.params == {"action_col": "act"}
    assert m.compute(MetricContext()) == "act"


def test_script_metric_reads_signals():
    m = ScriptMetric(code="result = signals.height")
    assert m.compute(MetricContext(signals=_signals())) == 2


def test_script_metric_gets_params_dict_for_defaulting():
    """A script must be able to default a column name it wasn't configured with, rather
    than raising NameError."""
    m = ScriptMetric(code='result = params.get("action_col", "action")')
    assert m.compute(MetricContext(signals=_signals())) == "action"
    assert ScriptMetric(code='result = params.get("action_col", "action")',
                        action_col="act").compute(MetricContext()) == "act"


def test_script_metric_requires_result():
    with pytest.raises(ValueError, match="must assign to 'result'"):
        ScriptMetric(code="x = 1").compute(MetricContext())


def test_script_metric_can_return_a_table_for_charting():
    m = ScriptMetric(code='result = pl.DataFrame({"timestamp": [1], "equity": [2.0]})')
    out = m.compute(MetricContext())
    assert isinstance(out, pl.DataFrame)
    assert out["equity"].to_list() == [2.0]


# ------------------------------------------------------------------ #
# load_metric_plugin                                                   #
# ------------------------------------------------------------------ #

def test_load_metric_plugin_script_type():
    m = load_metric_plugin("__script__", {"_code": "result = 1"})
    assert isinstance(m, ScriptMetric)
    assert m.compute(MetricContext()) == 1


def test_load_metric_plugin_script_type_requires_code():
    with pytest.raises(ValueError, match="_code"):
        load_metric_plugin("__script__", {})


def test_load_metric_plugin_passes_params_through():
    m = load_metric_plugin("__script__", {"_code": "result = action_col", "action_col": "act"})
    assert m.compute(MetricContext()) == "act"
    assert "_code" not in m.params


def test_load_metric_plugin_unknown_type_raises():
    with pytest.raises(ValueError, match="Unknown metric type"):
        load_metric_plugin("does-not-exist", {})


def test_load_metric_plugin_builtin_registry(monkeypatch):
    """The framework ships no metrics; a host app registers its own, same as strategies."""
    monkeypatch.setenv("TRADINGKIT_METRICS_MODULE", "tests._fixtures.fake_registry")
    m = load_metric_plugin("my_metric", {})
    assert isinstance(m, ScriptMetric)
    assert m.compute(MetricContext(signals=_signals())) == 2


# ------------------------------------------------------------------ #
# Result wiring                                                        #
# ------------------------------------------------------------------ #

def test_result_compute_runs_a_metric_ad_hoc():
    result = PipelineResult(
        signals=[Signal(timestamp=1, action="open_long", price=10.0)],
        data=pl.DataFrame(), indicators={},
    )
    assert result.compute(ScriptMetric(code="result = signals.height")) == 1


def test_result_metrics_default_to_empty():
    """Anything derived is opt-in: a run with no declared metrics carries none, rather
    than a number nobody asked for."""
    result = PipelineResult(signals=[], data=pl.DataFrame(), indicators={})
    assert result.metrics == {}


async def test_pipeline_computes_declared_metrics():
    """Declared like indicators: Pipeline(metrics={...}) -> result.metrics."""
    from tradingkit.pipeline import Pipeline
    from tradingkit.source import ScriptSource
    from tradingkit.strategy import ScriptStrategy

    p = Pipeline(
        name="m",
        source=ScriptSource(code=(
            'result = pl.DataFrame({"timestamp": [0, 60, 120], '
            '"close": [10.0, 11.0, 12.0]})'
        )),
        indicators={},
        strategy=ScriptStrategy(code='signal = Signal(action="open_long", price=bar.close)'),
        metrics={"count": ScriptMetric(code="result = signals.height")},
    )
    result = await p.run("BTC", 60, start_ts=0, end_ts=120)
    assert result.metrics == {"count": 3}


async def test_pipeline_metric_failure_does_not_discard_the_run():
    """A metric asking for a column these signals don't have reports None -- losing the
    whole backtest over one misconfigured metric would be worse than losing the metric."""
    from tradingkit.pipeline import Pipeline
    from tradingkit.source import ScriptSource
    from tradingkit.strategy import ScriptStrategy

    p = Pipeline(
        name="m",
        source=ScriptSource(code='result = pl.DataFrame({"timestamp": [0], "close": [1.0]})'),
        indicators={},
        strategy=ScriptStrategy(code='signal = Signal(action="x")'),
        metrics={"boom": ScriptMetric(code="result = signals['nope'].sum()")},
    )
    result = await p.run("BTC", 60, start_ts=0, end_ts=60)
    assert result.metrics == {"boom": None}
    assert len(result.signals) == 1        # the run itself survived
