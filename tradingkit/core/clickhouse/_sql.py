"""
tradingkit.core.clickhouse._sql — low-level SQL execution, value escaping, and the
identifier-validation guard shared by every other ClickHouseManager mixin.

_validate_identifier / _validates_identifiers guard against SQL injection through
table/column names that get interpolated as raw (unquoted) SQL syntax — e.g.
f"CREATE TABLE {table_name} (...)". Unlike query *values* (escaped via _fmt/
_interpolate below), identifiers can't be safely quoted away, so any name that isn't
a plain [A-Za-z_][A-Za-z0-9_]* token is rejected outright. table_name/column names can
originate from a user-authored script (AggregationScript.OUTPUT_TABLE/OUTPUT_SCHEMA,
ConnectionScriptSource.TABLE_NAME), so this boundary is not just defensive — it's
reachable from that surface.
"""
from __future__ import annotations

import base64
import functools
import inspect
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: Any, *, kind: str = "identifier") -> str:
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid ClickHouse {kind}: {name!r}. Expected to match "
            f"{_IDENTIFIER_RE.pattern!r} (letters/digits/underscore, not starting with a digit)."
        )
    return name


def _validates_identifiers(*param_names: str):
    """
    Method decorator: validate the named parameters as ClickHouse identifiers before
    the method body runs, instead of repeating the same inline check in every method
    that takes a table/raw_table name. Methods whose identifier list isn't a fixed
    parameter (e.g. per-Fold aliases derived from a `folds` list) still validate those
    inline — this only covers plain named params.
    """
    def _decorator(fn):
        sig = inspect.signature(fn)

        @functools.wraps(fn)
        async def _wrapper(*args, **kwargs):
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            for name in param_names:
                _validate_identifier(bound.arguments[name], kind=name)
            return await fn(*args, **kwargs)

        return _wrapper
    return _decorator


def _basic_auth_header(user: str, password: str) -> str:
    """
    HTTP Basic auth header value, built by hand rather than via aiohttp.BasicAuth:
    that class is deprecated as of newer aiohttp releases (removed in 4.0, which this
    project's `aiohttp<4.0.0` pin excludes, but the warning is just noise either way),
    and this has zero dependency-version surface — it's the entire spec in two lines.
    """
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return f"Basic {token}"


class _SqlMixin:
    """Low-level query execution shared by every other ClickHouseManager mixin."""

    @staticmethod
    def _fmt(val: Any) -> str:
        if val is None:
            return "NULL"
        if isinstance(val, bool):
            return "1" if val else "0"
        if isinstance(val, int):
            return str(val)
        if isinstance(val, float):
            return repr(val)
        escaped = str(val).replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"

    @staticmethod
    def _interpolate(sql: str, params: dict | None) -> str:
        if not params:
            return sql
        def _sub(m: re.Match) -> str:
            return _SqlMixin._fmt(params[m.group(1)])
        return re.sub(r"%\((\w+)\)s", _sub, sql)

    async def _execute(self, sql: str, params: dict | None = None, settings: dict | None = None) -> list:
        import aiohttp as _aiohttp
        rendered   = self._interpolate(sql, params)
        first_word = rendered.lstrip().split()[0].upper() if rendered.strip() else ""
        is_select  = first_word in ("SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXISTS")
        post_sql   = (rendered + " FORMAT JSONCompact") if is_select else rendered
        url = (
            f"http://{self.host}:{self.http_port}/"
            f"?database={self.database}&output_format_json_quote_64bit_integers=0"
        )
        for key, val in (settings or {}).items():
            url += f"&{key}={val}"
        headers = {"Authorization": _basic_auth_header(self.user, self.password)}
        try:
            async with _aiohttp.ClientSession() as sess, sess.post(
                url, data=post_sql.encode(), headers=headers
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(f"ClickHouse error HTTP {resp.status}: {body[:300]}")
                    return []
                if is_select:
                    result = await resp.json(content_type=None)
                    return result.get("data", [])
                return []
        except Exception as e:
            logger.error(f"ClickHouse query error: {e}")
            return []

    @_validates_identifiers("table")
    async def _bulk_insert(self, table: str, columns: list[str], rows: list[tuple]) -> None:
        for col in columns:
            _validate_identifier(col, kind="column name")
        if not rows or not self._conn:
            return
        async with self._lock:
            try:
                async with self._conn.cursor() as cur:
                    await cur.execute(
                        f"INSERT INTO {table} ({', '.join(columns)}) VALUES",
                        rows,
                    )
                logger.debug(f"Inserted {len(rows)} rows into {table}")
            except Exception as e:
                logger.error(f"Bulk insert into {table} failed: {e}")