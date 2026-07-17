"""
tradingkit.core.clickhouse._unit_tables — generic DataUnit tables + MV aggregation.

Unlike _candles.py (the fixed `candles` table), these methods work against arbitrary
tables declared by source/aggregation plugin scripts (TABLE_NAME, OUTPUT_TABLE) — the
data flow behind AggregationContext.query() and DataCollector's per-connection tables.
"""
from __future__ import annotations

from typing import Any

import polars as pl

from tradingkit.core.clickhouse._sql import _validate_identifier, _validates_identifiers


class _UnitTablesMixin:
    """Generic raw/aggregated DataUnit tables, backing AggregationContext.query()."""

    @_validates_identifiers("table_name")
    async def ensure_raw_table(self, table_name: str, schema: dict) -> None:
        """CREATE TABLE IF NOT EXISTS {table_name} (ReplacingMergeTree) from Polars schema."""
        from tradingkit.schema import POLARS_TO_CH
        for col in schema:
            _validate_identifier(col, kind="column name")
        cols = ["exchange String", "symbol String"]
        cols += [f"{col} {POLARS_TO_CH.get(dtype, 'String')}" for col, dtype in schema.items()]
        await self._execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols)})"
            " ENGINE = ReplacingMergeTree ORDER BY (exchange, symbol, timestamp)"
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
        df: "pl.DataFrame",
        exchange: str,
        symbol: str,
    ) -> None:
        """Bulk-insert a Polars DataFrame into a raw unit table."""
        if df.is_empty():
            return
        rows   = df.to_dicts()
        tuples = tuple(
            (exchange, symbol, *[r[c] for c in df.columns]) for r in rows
        )
        await self._bulk_insert(table_name, ["exchange", "symbol"] + df.columns, tuples)

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
    ) -> "pl.DataFrame":
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