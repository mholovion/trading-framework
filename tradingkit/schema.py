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

import re
from dataclasses import dataclass, field as dc_field
from typing import Any

import polars as pl

# ---------------------------------------------------------------------------
# Fold safety — fn/field/args get interpolated straight into DDL/MV SQL
# (ch_agg_type/ch_state_expr/ch_merge_expr below), so they need the same
# treatment as table/column identifiers in core/clickhouse.py. A full allowlist
# of ClickHouse aggregate functions would need constant upkeep (90+ functions
# plus -If/-Array/-Merge/-State combinators); a charset check on the function-
# name *position* gets the same security property (can't break out of the
# token) without it — an unrecognized function just fails at query time in
# ClickHouse, which is a safe failure mode, not a hole.
# ---------------------------------------------------------------------------
_CH_IDENT_RE  = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# unit.price.mul(unit.qty).sum() -> Fold(field="price * qty") via _ArithProxy —
# the only non-identifier `field` shape the framework itself produces.
_CH_ARITH_RE  = re.compile(r"^[A-Za-z_][A-Za-z0-9_]* \* [A-Za-z_][A-Za-z0-9_]*$")
# Best-effort structural guard for string args (e.g. sumIf("side = 1")): these
# are genuinely arbitrary boolean expressions, not identifiers, so this is a
# blocklist rather than an allowlist — same trust model as ScriptIndicator/
# exec(): write these yourself, don't pipe in untrusted text. It only catches
# statement-injection shapes (extra statements, comments, DDL/DML keywords),
# not every conceivable malicious expression.
_CH_ARG_DANGER_RE = re.compile(
    r";|--|/\*|\*/|\b(DROP|ALTER|INSERT|DELETE|ATTACH|DETACH|GRANT|REVOKE|"
    r"UNION|EXEC|CREATE|TRUNCATE|RENAME|KILL|SYSTEM)\b",
    re.IGNORECASE,
)


def _validate_ch_token(value: str, *, kind: str) -> str:
    if not isinstance(value, str) or not _CH_IDENT_RE.match(value):
        raise ValueError(
            f"Invalid Fold {kind}: {value!r}. Expected a plain identifier "
            f"matching {_CH_IDENT_RE.pattern!r}."
        )
    return value


def _validate_ch_field(value: str) -> str:
    if isinstance(value, str) and (_CH_IDENT_RE.match(value) or _CH_ARITH_RE.match(value)):
        return value
    raise ValueError(
        f"Invalid Fold field: {value!r}. Expected a plain identifier or a "
        f"'<name> * <name>' expression (from Field.mul())."
    )


def _validate_ch_arg(value: Any) -> Any:
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        if _CH_ARG_DANGER_RE.search(value):
            raise ValueError(f"Fold arg rejected (looks like a statement/comment injection): {value!r}")
        return value
    raise ValueError(f"Invalid Fold arg type {type(value).__name__}: {value!r}. Expected str/int/float/bool.")


# ClickHouse aggregate functions using the "leading parameter" calling convention --
# fn(params)(columns) -- where the params are query-time-only and NOT part of the
# serialized aggregate state: e.g. quantile's level selects what to extract from the
# state, so it has to be supplied again at *every* read, State or Merge alike. Verified
# directly against a real server: quantileMerge(state) (no params) silently defaults to
# level=0.5 instead of erroring, so getting this list right actually matters --
# quantileMerge(0.95)(state) is the correct form.
#
# Everything else (sum, count, avg, sumIf, countIf, argMin, ...) uses the flat
# convention fn(columns, extra_args...), where Merge never needs the args at all --
# also verified directly (sumIfState(qty, cond) / sumIfMerge(state), no cond repeated).
#
# Best-effort list, not exhaustive: an unlisted parametric function falls through to
# the flat convention and fails loudly with a ClickHouse syntax/type error if that's
# wrong for it -- same trust model as the identifier validation above, add to this set
# as needed.
_LEADING_PARAM_FUNCTIONS = frozenset({
    "quantile", "quantiles", "quantileExact", "quantileExactLow", "quantileExactHigh",
    "quantileExactWeighted", "quantileTiming", "quantileTimingWeighted",
    "quantileDeterministic", "quantileTDigest", "quantileTDigestWeighted",
    "quantileBFloat16", "quantileInterpolatedWeighted",
    "topK", "topKWeighted",
    "groupArrayMovingAvg", "groupArrayMovingSum",
    "windowFunnel", "sequenceMatch", "sequenceCount",
})


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

    def __post_init__(self) -> None:
        # fn/field/by/ch_type/by_ch_type/_alias/args all get interpolated as raw SQL
        # syntax by ch_agg_type/ch_state_expr/ch_merge_expr — validate once here so
        # a bad Fold fails at construction, not deep inside SQL generation.
        _validate_ch_token(self.fn, kind="function name")
        _validate_ch_field(self.field)
        _validate_ch_token(self.ch_type, kind="ch_type")
        if self.by is not None:
            _validate_ch_token(self.by, kind="by")
        if self.by_ch_type is not None:
            _validate_ch_token(self.by_ch_type, kind="by_ch_type")
        if self._alias is not None:
            _validate_ch_token(self._alias, kind="alias")
        for a in self.args:
            _validate_ch_arg(a)

    def alias(self, name: str) -> "Fold":
        return Fold(self.fn, self.field, self.ch_type, self.by,
                    self.by_ch_type, list(self.args), name)

    def _args_sql(self) -> str:
        return ", ".join(str(a) for a in self.args)

    def ch_agg_type(self) -> str:
        """AggregateFunction(...) DDL string for AggregatingMergeTree column."""
        if self.fn in _LEADING_PARAM_FUNCTIONS:
            fn_part   = f"{self.fn}({self._args_sql()})" if self.args else self.fn
            type_args = [self.ch_type] + ([self.by_ch_type] if self.by_ch_type else [])
        else:
            fn_part = self.fn
            # Combinator-style extra args (e.g. sumIf's condition) need a type slot of
            # their own in the DDL. The only shape this framework produces for them is
            # a boolean condition, which ClickHouse represents as UInt8.
            type_args = ([self.ch_type] + ([self.by_ch_type] if self.by_ch_type else [])
                         + ["UInt8"] * len(self.args))
        return f"AggregateFunction({fn_part}, {', '.join(type_args)})"

    def ch_state_expr(self, alias: str) -> str:
        """
        MV SELECT fragment.

        Leading-parameter functions (quantile, topK, ...): argument(s) go in their own
        parens before the column(s) -- quantileState(0.95)(price) AS alias.
        Everything else: argMinState(open, timestamp) AS open, sumIfState(qty, cond).
        """
        col_args = self.field if not self.by else f"{self.field}, {self.by}"
        if self.fn in _LEADING_PARAM_FUNCTIONS:
            params = f"({self._args_sql()})" if self.args else ""
            return f"{self.fn}State{params}({col_args}) AS {alias}"
        all_args = col_args if not self.args else f"{col_args}, {self._args_sql()}"
        return f"{self.fn}State({all_args}) AS {alias}"

    def ch_merge_expr(self, col: str) -> str:
        """
        Query SELECT fragment.

        Leading-parameter functions need the same argument(s) supplied again here --
        verified against a real server that the state does NOT retain them (e.g.
        quantile's level chooses what to extract from the state at read time, so
        quantileMerge(state) with no level silently defaults to the median instead of
        erroring): quantileMerge(0.95)(state) AS alias.
        Everything else never needs the args repeated: argMinMerge(open) AS open,
        sumIfMerge(state) AS state -- the condition was already applied when the state
        was built.
        """
        if self.fn in _LEADING_PARAM_FUNCTIONS:
            params = f"({self._args_sql()})" if self.args else ""
            return f"{self.fn}Merge{params}({col}) AS {col}"
        return f"{self.fn}Merge({col}) AS {col}"


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
