"""
tradingkit.core.clickhouse._unit_tables — generic DataUnit tables + MV aggregation.

Unlike _candles.py (the fixed `candles` table), these methods work against arbitrary
tables declared by source/aggregation plugin scripts (TABLE_NAME, OUTPUT_TABLE) — the
data flow behind AggregationContext.query() and DataCollector's per-connection tables.
"""
from __future__ import annotations

import re
from typing import Any

import polars as pl

from tradingkit.core.clickhouse._sql import _validate_identifier, _validates_identifiers

# Tokenizer for cross-source combine_sql() expressions (e.g. "btc - eth", "(btc - eth) / eth").
# Unlike table/column identifiers (_validate_identifier, a closed grammar), this needs to
# accept a small arithmetic language over a caller-supplied set of aliases -- validated by
# requiring the ENTIRE string to tokenize with no gaps (a naive scan-for-danger check could
# miss characters between matches) and every identifier-like token to be a known alias.
_CROSS_SOURCE_EXPR_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|[0-9]+\.?[0-9]*|[+\-*/()]|\s+")

# Two single-character operator tokens can legally sit next to each other (the tokenizer
# below allows it) but still spell a dangerous *pair* SQL gives special meaning to -- "--"
# opens a line comment, "/*"/"*/" a block comment. Same class of check as schema.py's
# _CH_ARG_DANGER_RE, applied here because this validator's per-character tokenizing would
# otherwise wave both straight through as "two valid operators in a row".
_CROSS_SOURCE_EXPR_DANGER_RE = re.compile(r";|--|/\*|\*/")


def _validate_cross_source_expr(expr: str, aliases: list[str]) -> str:
    if not isinstance(expr, str) or not expr.strip():
        raise ValueError(f"Invalid cross-source expression: {expr!r}")
    if _CROSS_SOURCE_EXPR_DANGER_RE.search(expr):
        raise ValueError(f"Invalid cross-source expression {expr!r}: looks like a statement/comment injection")
    known = set(aliases)
    pos = 0
    for m in _CROSS_SOURCE_EXPR_TOKEN_RE.finditer(expr):
        if m.start() != pos:
            raise ValueError(
                f"Invalid cross-source expression {expr!r}: unexpected character at position {pos}"
            )
        pos = m.end()
        tok = m.group()
        if tok.isspace() or tok in "+-*/()" or re.fullmatch(r"[0-9]+\.?[0-9]*", tok):
            continue
        if tok not in known:
            raise ValueError(
                f"Invalid cross-source expression {expr!r}: unknown identifier {tok!r} "
                f"(expected one of {sorted(known)})"
            )
    if pos != len(expr):
        raise ValueError(f"Invalid cross-source expression {expr!r}: unexpected trailing content")
    return expr


class _UnitTablesMixin:
    """Generic raw/aggregated DataUnit tables, backing AggregationContext.query()."""

    @_validates_identifiers("table_name")
    async def ensure_raw_table(self, table_name: str, schema: dict) -> None:
        """CREATE TABLE IF NOT EXISTS {table_name} (ReplacingMergeTree) from Polars schema."""
        from tradingkit.schema import POLARS_TO_CH
        for col in schema:
            _validate_identifier(col, kind="column name")
        cols = ["exchange String", "symbol String", "timeframe String"]
        cols += [f"{col} {POLARS_TO_CH.get(dtype, 'String')}" for col, dtype in schema.items()]
        await self._execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols)})"
            " ENGINE = ReplacingMergeTree ORDER BY (exchange, symbol, timestamp)"
        )
        # Tables created before `timeframe` existed here won't get it from CREATE ... IF NOT
        # EXISTS alone -- ADD COLUMN IF NOT EXISTS is the idempotent migration for those.
        await self._execute(
            f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS timeframe String DEFAULT ''"
        )

    @_validates_identifiers("raw_table")
    async def ensure_agg_table(self, raw_table: str, bucket_s: int, folds: list) -> None:
        """CREATE TABLE IF NOT EXISTS {raw_table}_{bucket_s}s (AggregatingMergeTree)."""
        from tradingkit.schema import Fold
        bucket_s = int(bucket_s)
        agg_table = f"{raw_table}_{bucket_s}s"
        col_ddl   = ["exchange String", "symbol String", "bucket UInt64"]
        for f in folds:
            if isinstance(f, Fold):
                alias = _validate_identifier(f._alias or f.field, kind="fold alias")
                col_ddl.append(f"{alias} {f.ch_agg_type()}")
        await self._execute(
            f"CREATE TABLE IF NOT EXISTS {agg_table} ({', '.join(col_ddl)})"
            " ENGINE = AggregatingMergeTree ORDER BY (exchange, symbol, bucket)"
        )

    @_validates_identifiers("raw_table")
    async def ensure_mv(self, raw_table: str, bucket_s: int, folds: list) -> None:
        """CREATE MATERIALIZED VIEW IF NOT EXISTS mv_{raw_table}_to_{bucket_s}s."""
        from tradingkit.schema import Fold
        bucket_s = int(bucket_s)
        agg_table = f"{raw_table}_{bucket_s}s"
        mv_name   = f"mv_{raw_table}_to_{bucket_s}s"
        selects   = [
            "exchange",
            "symbol",
            f"intDiv(timestamp, {bucket_s}) * {bucket_s} AS bucket",
        ]
        for f in folds:
            if isinstance(f, Fold):
                alias = _validate_identifier(f._alias or f.field, kind="fold alias")
                selects.append(f.ch_state_expr(alias))
        await self._execute(
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name} TO {agg_table}"
            f" AS SELECT {', '.join(selects)} FROM {raw_table}"
            f" GROUP BY exchange, symbol, bucket"
        )

    @_validates_identifiers("raw_table")
    async def backfill_agg(self, raw_table: str, bucket_s: int, folds: list) -> None:
        """INSERT INTO agg_table SELECT *State(...) FROM raw_table GROUP BY bucket."""
        from tradingkit.schema import Fold
        bucket_s = int(bucket_s)
        agg_table = f"{raw_table}_{bucket_s}s"
        selects   = [
            "exchange",
            "symbol",
            f"intDiv(timestamp, {bucket_s}) * {bucket_s} AS bucket",
        ]
        for f in folds:
            if isinstance(f, Fold):
                alias = _validate_identifier(f._alias or f.field, kind="fold alias")
                selects.append(f.ch_state_expr(alias))
        await self._execute(
            f"INSERT INTO {agg_table}"
            f" SELECT {', '.join(selects)} FROM {raw_table}"
            f" GROUP BY exchange, symbol, bucket"
        )

    @_validates_identifiers("table_name")
    async def insert_unit_batch(
        self,
        table_name: str,
        df: pl.DataFrame,
        exchange: str,
        symbol: str,
        timeframe: str = "",
    ) -> None:
        """Bulk-insert a Polars DataFrame into a raw unit table."""
        if df.is_empty():
            return
        rows   = df.to_dicts()
        tuples = tuple(
            (exchange, symbol, timeframe, *[r[c] for c in df.columns]) for r in rows
        )
        await self._bulk_insert(table_name, ["exchange", "symbol", "timeframe"] + df.columns, tuples)

    @_validates_identifiers("table_name")
    async def get_unit_range(
        self,
        table_name: str,
        exchange: str,
        symbol: str,
    ) -> tuple[int | None, int | None]:
        """min/max timestamp in a raw unit table."""
        rows = await self._execute(
            f"SELECT min(timestamp), max(timestamp) FROM {table_name}"
            " WHERE exchange = %(ex)s AND symbol = %(sym)s",
            {"ex": exchange, "sym": symbol},
        )
        if rows and rows[0][0] is not None:
            return int(rows[0][0]), int(rows[0][1])
        return None, None

    @_validates_identifiers("table_name")
    async def find_unit_gaps(
        self,
        table_name: str,
        exchange: str,
        symbol: str,
        start_ts: int,
        end_ts: int,
        unit_interval_s: int = 60,
    ) -> list[dict]:
        """Generic gap detection via lagInFrame on any raw unit table."""
        unit_interval_s = int(unit_interval_s)
        sql = f"""
            SELECT
                prev_ts + {unit_interval_s}   AS gap_start,
                cur_ts                         AS gap_end,
                toUInt64((cur_ts - prev_ts) / {unit_interval_s} - 1) AS missing
            FROM (
                SELECT
                    timestamp AS cur_ts,
                    lagInFrame(timestamp) OVER (ORDER BY timestamp) AS prev_ts
                FROM {table_name} FINAL
                WHERE exchange = %(ex)s AND symbol = %(sym)s
                  AND timestamp BETWEEN %(st)s AND %(et)s
            )
            WHERE prev_ts > 0 AND (cur_ts - prev_ts) > {unit_interval_s}
        """
        rows = await self._execute(sql, {
            "ex": exchange, "sym": symbol, "st": start_ts, "et": end_ts,
        })
        return [
            {
                "start_timestamp": int(r[0]),
                "end_timestamp":   int(r[1]) - unit_interval_s,
                "missing_rows":    int(r[2]),
            }
            for r in rows if r[2] > 0
        ]

    @_validates_identifiers("table_name")
    async def get_table_schema(self, table_name: str) -> dict:
        """
        Return {column_name: polars_dtype} by parsing DESCRIBE TABLE output.
        Used by AggregationScript to build a DataUnit for aggregation(unit) calls.
        """
        _CH_TO_PL: dict[str, Any] = {
            "Int64":   pl.Int64,   "UInt64":  pl.UInt64,
            "Int32":   pl.Int32,   "UInt32":  pl.UInt32,
            "Float64": pl.Float64, "Float32": pl.Float32,
            "String":  pl.String,  "UInt8":   pl.UInt8,
        }
        rows = await self._execute(f"DESCRIBE TABLE {table_name}")
        schema: dict[str, Any] = {}
        for row in rows:
            col_name = row[0]
            ch_type  = row[1].split("(")[0]  # strip LowCardinality(…) etc.
            schema[col_name] = _CH_TO_PL.get(ch_type, pl.String)
        return schema

    @_validates_identifiers("table_name")
    async def fetch_from_unit_table(
        self,
        table_name: str,
        exchange: str,
        symbol: str | None = None,
        start_ts: int | None = None,
        end_ts:   int | None = None,
    ) -> pl.DataFrame:
        """
        Generic SELECT * FROM any raw unit table with exchange/symbol/time filters.
        Returns a Polars DataFrame. Used by AggregationContext.query().
        """
        where_parts = ["exchange = %(ex)s"]
        params: dict[str, Any] = {"ex": exchange}
        if symbol is not None:
            where_parts.append("symbol = %(sym)s")
            params["sym"] = symbol
        if start_ts is not None:
            where_parts.append("timestamp >= %(st)s")
            params["st"] = start_ts
        if end_ts is not None:
            where_parts.append("timestamp <= %(et)s")
            params["et"] = end_ts
        sql = (
            f"SELECT * FROM {table_name}"
            f" WHERE {' AND '.join(where_parts)}"
            f" ORDER BY timestamp ASC"
        )
        rows = await self._execute(sql, params)
        if not rows:
            return pl.DataFrame()
        schema = await self.get_table_schema(table_name)
        col_names = list(schema.keys())
        return pl.DataFrame(
            [dict(zip(col_names, r)) for r in rows],
            schema=schema,
        )

    @_validates_identifiers("raw_table")
    async def query_agg(
        self,
        raw_table: str,
        bucket_s: int,
        folds: list,
        exchange: str,
        symbol: str,
        start_ts: int | None = None,
        end_ts:   int | None = None,
    ) -> list[dict]:
        """SELECT *Merge() FROM {raw_table}_{bucket_s}s GROUP BY bucket."""
        from tradingkit.schema import Fold
        bucket_s  = int(bucket_s)
        agg_table = f"{raw_table}_{bucket_s}s"
        ch_folds  = [f for f in folds if isinstance(f, Fold)]
        aliases   = [_validate_identifier(f._alias or f.field, kind="fold alias") for f in ch_folds]
        selects   = ["bucket"] + [f.ch_merge_expr(a) for f, a in zip(ch_folds, aliases)]
        where     = "exchange = %(ex)s AND symbol = %(sym)s"
        kw: dict  = {"ex": exchange, "sym": symbol}
        if start_ts is not None:
            where += " AND bucket >= %(st)s"; kw["st"] = start_ts
        if end_ts is not None:
            where += " AND bucket < %(et)s";  kw["et"] = end_ts
        rows = await self._execute(
            f"SELECT {', '.join(selects)} FROM {agg_table}"
            f" WHERE {where} GROUP BY bucket ORDER BY bucket",
            kw,
        )
        cols = ["bucket"] + aliases
        return [dict(zip(cols, r)) for r in rows]

    # ------------------------------------------------------------------ #
    # Cross-source alignment (TASK-005) — backs tradingkit.aggregation.Aggregation.        #
    # ------------------------------------------------------------------ #

    @_validates_identifiers("output_table")
    async def ensure_cross_source_table(self, output_table: str, sources: dict[str, str]) -> None:
        """
        CREATE TABLE IF NOT EXISTS {output_table} (AggregatingMergeTree) for cross-source
        alignment: one Nullable(ch_type)-wrapped argMax state column per source, keyed by
        timestamp. Nullable is load-bearing, not decorative — argMaxMerge() on a state no
        source has written yet returns the type's zero value (0), not NULL; without it a
        reader can't tell "this source hasn't arrived yet" from "it arrived and was 0".
        Verified directly against a real server.
        """
        for alias in sources:
            _validate_identifier(alias, kind="cross-source alias")
        cols = ["timestamp UInt64"]
        cols += [
            f"{alias} AggregateFunction(argMax, Nullable({ch_type}), UInt64)"
            for alias, ch_type in sources.items()
        ]
        await self._execute(
            f"CREATE TABLE IF NOT EXISTS {output_table} ({', '.join(cols)})"
            " ENGINE = AggregatingMergeTree ORDER BY timestamp"
        )

    @_validates_identifiers("output_table", "source_table")
    async def ensure_cross_source_mv(
        self,
        output_table: str,
        source_table: str,
        alias: str,
        field: str,
        ch_type: str,
        timestamp_col: str = "timestamp",
    ) -> None:
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_{source_table}_to_{output_table}_{alias}.
        One MV per source, each writing its own partial argMax state into the SAME shared
        output_table — not a reactive JOIN (a single MV only ever triggers on its own FROM
        table's inserts, so it could never see a not-yet-arrived row from a different
        source). A late insert into source_table — even a backfilled one — triggers this MV
        and merges into the same key regardless of how late it is: gap-closing falls out of
        AggregatingMergeTree's merge semantics, it isn't separate logic. Verified directly:
        a late-arriving row correctly completed a previously-partial row at read time, even
        while still sitting in a separate, unmerged physical part.
        """
        _validate_identifier(alias, kind="cross-source alias")
        _validate_identifier(field, kind="column name")
        _validate_identifier(ch_type, kind="ch_type")
        _validate_identifier(timestamp_col, kind="column name")
        mv_name = f"mv_{source_table}_to_{output_table}_{alias}"
        await self._execute(
            f"CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name} TO {output_table} AS"
            f" SELECT {timestamp_col} AS timestamp,"
            f" argMaxState(CAST({field} AS Nullable({ch_type})), {timestamp_col}) AS {alias}"
            f" FROM {source_table} GROUP BY {timestamp_col}"
        )

    @_validates_identifiers("output_table", "source_table")
    async def backfill_cross_source(
        self,
        output_table: str,
        source_table: str,
        alias: str,
        field: str,
        ch_type: str,
        timestamp_col: str = "timestamp",
    ) -> None:
        """
        One-time INSERT INTO {output_table} (timestamp, {alias}) SELECT ... for rows that
        already existed in source_table before ensure_cross_source_mv() was called.
        Materialized Views are NOT retroactive — they only fire on rows inserted after
        the MV itself exists, so without this, setting up a new aggregation on top of
        already-populated source tables would silently have no historical data (found via
        a real test failure, not anticipated up front). Verified directly: a partial-column
        INSERT naming only (timestamp, {alias}) backfills that one source's state without
        disturbing any other source's already-written rows for the same timestamp — the
        same partial-write shape ensure_cross_source_mv's own MV already relies on, just
        issued once instead of continuously. Same convention as backfill_agg() for the
        single-table Fold/bucket-aggregation path this mirrors.
        """
        _validate_identifier(alias, kind="cross-source alias")
        _validate_identifier(field, kind="column name")
        _validate_identifier(ch_type, kind="ch_type")
        _validate_identifier(timestamp_col, kind="column name")
        await self._execute(
            f"INSERT INTO {output_table} (timestamp, {alias})"
            f" SELECT {timestamp_col}, argMaxState(CAST({field} AS Nullable({ch_type})), {timestamp_col})"
            f" FROM {source_table} GROUP BY {timestamp_col}"
        )

    @_validates_identifiers("output_table")
    async def query_cross_source(
        self,
        output_table: str,
        aliases: list[str],
        select_expr: str,
        start_ts: int | None = None,
        end_ts: int | None = None,
        only_complete: bool = True,
    ) -> list[dict]:
        """
        Read a cross-source alignment table: merge each alias's partial state, apply
        select_expr (an arithmetic expression over the aliases, e.g. "btc - eth" —
        validated by _validate_cross_source_expr, not trusted from the caller) to the
        merged columns. only_complete=True (default) drops rows where any source hasn't
        arrived yet — the safe default, since a partial row's arithmetic result would
        otherwise silently look like a real number. NULL propagates correctly through
        +-*/, verified directly, so with only_complete=False a partial row's `value`
        comes back NULL rather than a wrong number.
        """
        for alias in aliases:
            _validate_identifier(alias, kind="cross-source alias")
        _validate_cross_source_expr(select_expr, aliases)
        merged = ", ".join(f"argMaxMerge({a}) AS {a}" for a in aliases)
        where_parts = []
        params: dict[str, Any] = {}
        if start_ts is not None:
            where_parts.append("timestamp >= %(st)s")
            params["st"] = start_ts
        if end_ts is not None:
            where_parts.append("timestamp <= %(et)s")
            params["et"] = end_ts
        where = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        inner = f"SELECT timestamp, {merged} FROM {output_table}{where} GROUP BY timestamp"
        outer_where = ""
        if only_complete:
            outer_where = " WHERE " + " AND ".join(f"{a} IS NOT NULL" for a in aliases)
        sql = (
            f"SELECT timestamp, ({select_expr}) AS value FROM ({inner}){outer_where}"
            f" ORDER BY timestamp"
        )
        rows = await self._execute(sql, params)
        return [{"timestamp": int(r[0]), "value": r[1]} for r in rows]