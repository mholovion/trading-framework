"""tradingkit.schema — DataUnit/Field/Fold/PyFold + injection-guard validation."""
from __future__ import annotations

import polars as pl
import pytest

from tradingkit.schema import AggSpec, DataUnit, Fold, PyFold


@pytest.fixture
def unit() -> DataUnit:
    return DataUnit({"price": pl.Float64, "qty": pl.Float64, "timestamp": pl.Int64})


def test_field_access_and_missing_column(unit):
    assert unit.price.name == "price"
    assert unit.price.ch_type == "Float64"
    with pytest.raises(AttributeError):
        unit.nonexistent


def test_sum_max_min_produce_folds(unit):
    assert unit.price.sum().fn == "sum"
    assert unit.price.max().fn == "max"
    assert unit.price.min().fn == "min"


def test_first_last_use_argmin_argmax_with_timestamp(unit):
    first = unit.price.first()
    assert first.fn == "argMin" and first.by == "timestamp"
    last = unit.price.last()
    assert last.fn == "argMax" and last.by == "timestamp"


def test_alias_preserves_fields_and_sets_alias(unit):
    f = unit.price.sum().alias("total")
    assert f._alias == "total"
    assert f.fn == "sum" and f.field == "price"


def test_mul_produces_arithmetic_expression(unit):
    f = unit.price.mul(unit.qty).sum().alias("notional")
    assert f.field == "price * qty"
    assert f.ch_state_expr("notional") == "sumState(price * qty) AS notional"


def test_quantile_and_sumif_accept_args(unit):
    q = unit.price.quantile(0.95)
    assert q.args == [0.95]
    s = unit.qty.sumIf("side = 1")
    assert s.args == ["side = 1"]


def test_compute_returns_pyfold(unit):
    pf = unit.price.compute(pl.col("price") * 2)
    assert isinstance(pf, PyFold)


def test_agg_spec_holds_buckets_and_folds(unit):
    spec = AggSpec(buckets=[300, 3600], folds=[unit.price.sum().alias("total")])
    assert spec.buckets == [300, 3600]
    assert len(spec.folds) == 1


# ------------------------------------------------------------------ #
# Injection-shaped inputs are rejected at Fold construction            #
# ------------------------------------------------------------------ #

@pytest.mark.parametrize("bad_fn", [
    "sum); DROP TABLE x; --",
    "sum -- comment",
    "sum/*x*/",
    "123startdigit",
])
def test_malicious_fn_rejected(bad_fn):
    with pytest.raises(ValueError):
        Fold(bad_fn, "price", "Float64")


@pytest.mark.parametrize("bad_field", [
    "price); DROP TABLE x; --",
    "price, (SELECT 1)",
    "price + qty",       # only `name * name` arithmetic is recognized, not `+`
])
def test_malicious_or_unsupported_field_rejected(bad_field):
    with pytest.raises(ValueError):
        Fold("sum", bad_field, "Float64")


def test_malicious_alias_rejected(unit):
    with pytest.raises(ValueError):
        unit.price.sum().alias("x); DROP TABLE users; --")


def test_malicious_by_rejected():
    with pytest.raises(ValueError):
        Fold("argMin", "price", "Float64", by="timestamp); DROP TABLE x; --")


def test_malicious_ch_type_rejected():
    with pytest.raises(ValueError):
        Fold("sum", "price", "Float64); DROP TABLE x; --")


@pytest.mark.parametrize("bad_arg", [
    "1=1; DROP TABLE x; --",
    "1=1 -- comment",
    "1=1 /* comment */",
    "1=1 UNION SELECT password FROM users",
])
def test_malicious_string_arg_rejected(bad_arg):
    with pytest.raises(ValueError):
        Fold("sumIf", "qty", "Float64", args=[bad_arg])


def test_malicious_arg_type_rejected():
    with pytest.raises(ValueError):
        Fold("sum", "price", "Float64", args=[["nested", "list"]])


def test_numeric_args_always_pass():
    Fold("quantile", "price", "Float64", args=[0.95])
    Fold("topK", "price", "Float64", args=[5])
    Fold("sumIf", "qty", "Float64", args=[True])


def test_legit_sumif_condition_passes():
    f = Fold("sumIf", "qty", "Float64", args=["side = 1"])
    assert f.args == ["side = 1"]