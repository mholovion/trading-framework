"""
tradingkit.schema — DataUnit, Field, Fold, PyFold, AggSpec.

Used in source plugin scripts to declare aggregation rules:

    TABLE_NAME = "candles"

    def aggregation(unit):
        from tradingkit.schema import AggSpec
        return AggSpec(
            buckets=[300, 3600, 86400],
            folds=[
                unit.open.first().alias("open"),
                unit.high.max().alias("high"),
                unit.low.min().alias("low"),
                unit.close.last().alias("close"),
                unit.volume.sum().alias("volume"),
            ]
        )

Fold   — ClickHouse-backed aggregation via AggregatingMergeTree + MV (zero RAM at read time).
PyFold — Python/Polars aggregation (explicit RAM, no MV; use only when CH cannot express it).
"""
from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from typing import Any

import polars as pl

POLARS_TO_CH: dict[Any, str] = {
    pl.Float64: "Float64",
    pl.Float32: "Float32",
    pl.Int64:   "Int64",
    pl.Int32:   "Int32",
    pl.UInt64:  "UInt64",
    pl.UInt32:  "UInt32",
    pl.Utf8:    "String",
    pl.String:  "String",
    pl.Boolean: "UInt8",
}


@dataclass
class Fold:
    """
    ClickHouse-backed aggregation.

    Created via Field methods: unit.price.max().alias("high")
    Stored as AggregateFunction(*State) column in AggregatingMergeTree.
    Queried via *Merge() at SELECT time — no Python RAM involved.
    """
    fn:         str              # CH function name, e.g. "argMin", "max", "quantile"
    field:      str              # column name or arithmetic expr in raw table
    ch_type:    str              # CH type of the field, e.g. "Float64"
    by:         str | None = None        # second arg for argMin/argMax, e.g. "timestamp"
    by_ch_type: str | None = None
    args:       list = dc_field(default_factory=list)  # extra literal args: quantile level, cond
    _alias:     str | None = None

    def alias(self, name: str) -> "Fold":
        return Fold(self.fn, self.field, self.ch_type, self.by,
                    self.by_ch_type, list(self.args), name)

    def ch_agg_type(self) -> str:
        """AggregateFunction(...) DDL string for AggregatingMergeTree column."""
        fn_part    = self.fn if not self.args else f"{self.fn}({', '.join(str(a) for a in self.args)})"
        type_args  = [self.ch_type] + ([self.by_ch_type] if self.by_ch_type else [])
        return f"AggregateFunction({fn_part}, {', '.join(type_args)})"

    def ch_state_expr(self, alias: str) -> str:
        """MV SELECT fragment: argMinState(open, timestamp) AS open"""
        fn_part  = self.fn if not self.args else f"{self.fn}({', '.join(str(a) for a in self.args)})"
        col_args = self.field if not self.by else f"{self.field}, {self.by}"
        return f"{fn_part}State({col_args}) AS {alias}"

    def ch_merge_expr(self, col: str) -> str:
        """Query SELECT fragment: argMinMerge(open) AS open"""
        fn_part = self.fn if not self.args else f"{self.fn}({', '.join(str(a) for a in self.args)})"
        return f"{fn_part}Merge({col}) AS {col}"


@dataclass
class PyFold:
    """
    Python/Polars aggregation.

    Use ONLY when ClickHouse cannot express the aggregation (custom window logic, etc.).
    Cannot be stored in a Materialized View — computed in Python at query time (uses RAM).
    """
    expr:   pl.Expr
    _alias: str | None = None

    def alias(self, name: str) -> "PyFold":
        return PyFold(self.expr.alias(name), name)


@dataclass
class AggSpec:
    """
    Return type of `aggregation(unit)` in source plugin scripts.
    Combines target bucket sizes + fold rules in a single declaration.

    buckets — target aggregation intervals in seconds, e.g. [300, 3600, 86400] → 5m, 1h, 1d.
    folds   — list of Fold (CH-native) and/or PyFold (Python) aggregation rules.
    """
    buckets: list[int]
    folds:   list[Fold | PyFold]


class _ArithProxy:
    """
    Intermediate result of Field.mul(other).
    Enables sumState(price * qty) in a Materialized View.
    Call any CH function on it: .sum(), .avg(), etc.
    """
    def __init__(self, expr: str, ch_type: str) -> None:
        self._expr    = expr
        self._ch_type = ch_type

    def __getattr__(self, fn_name: str):
        if fn_name.startswith("_"):
            raise AttributeError(fn_name)

        def _call(*args) -> Fold:
            return Fold(fn_name, self._expr, self._ch_type, args=list(args))
        return _call


class Field:
    """
    Proxy for one column in a source plugin DataFrame.

    Calling any ClickHouse aggregate function name as a method returns a Fold:
        unit.price.sum()              → Fold("sum", "price", "Float64")
        unit.price.max()              → Fold("max", "price", "Float64")
        unit.price.quantile(0.95)     → Fold("quantile", ..., args=[0.95])
        unit.qty.sumIf("side = 1")    → Fold("sumIf", ..., args=["side = 1"])
        unit.price.exponentialMovingAverage(0.1)  → any valid CH function

    Special methods that need extra arguments:
        .first()   → argMin(field, timestamp)
        .last()    → argMax(field, timestamp)
        .mul(other)  → _ArithProxy for sumState(a * b) etc.
        .compute(expr) → PyFold (explicit Python/RAM escape hatch)
    """
    def __init__(self, name: str, dtype: Any) -> None:
        self.name    = name
        self.ch_type = POLARS_TO_CH.get(dtype, "String")
        self._dtype  = dtype

    def first(self) -> Fold:
        """argMin(field, timestamp) — first value in the bucket by time."""
        return Fold("argMin", self.name, self.ch_type, "timestamp", "UInt64")

    def last(self) -> Fold:
        """argMax(field, timestamp) — last value in the bucket by time."""
        return Fold("argMax", self.name, self.ch_type, "timestamp", "UInt64")

    def mul(self, other: "Field") -> _ArithProxy:
        """Arithmetic proxy: enables sumState(price * qty) in MV."""
        return _ArithProxy(f"{self.name} * {other.name}", self.ch_type)

    def compute(self, expr: pl.Expr) -> PyFold:
        """Escape hatch to Polars expression. Computed in Python RAM, not in MV."""
        return PyFold(expr)

    def __getattr__(self, fn_name: str):
        if fn_name.startswith("_"):
            raise AttributeError(fn_name)

        def _call(*args) -> Fold:
            return Fold(fn_name, self.name, self.ch_type, args=list(args))
        return _call


class DataUnit:
    """
    Schema proxy built from a source plugin's sample DataFrame.

    Access columns as attributes to get Field objects:
        unit.price  → Field("price", Float64)
        unit.qty    → Field("qty",   Float64)

    Pass as the argument to the source plugin's aggregation() function:
        def aggregation(unit):
            return AggSpec(buckets=[300], folds=[unit.close.last().alias("close")])
    """
    def __init__(self, schema: dict[str, Any]) -> None:
        self._schema = schema

    def __getattr__(self, name: str) -> Field:
        if name.startswith("_"):
            raise AttributeError(name)
        if name not in self._schema:
            raise AttributeError(
                f"Column '{name}' not in schema: {list(self._schema)}"
            )
        return Field(name, self._schema[name])


__all__ = [
    "POLARS_TO_CH",
    "Fold",
    "PyFold",
    "AggSpec",
    "DataUnit",
    "Field",
    "_ArithProxy",
]
