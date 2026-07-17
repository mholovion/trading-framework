"""tradingkit.core.clickhouse._plugin_library — plugin_library code access + saved pipelines."""
from __future__ import annotations

import json


class _PluginLibraryMixin:
    """Script storage (indicators/strategies/sources/aggregations) and saved Pipelines."""

    async def get_plugin_code(self, namespace: str, type_: str, name: str) -> str | None:
        if not self._conn:
            return None
        rows = await self._execute(
            "SELECT code FROM plugin_library FINAL"
            " WHERE namespace=%(ns)s AND type=%(t)s AND name=%(n)s LIMIT 1",
            {"ns": namespace, "t": type_, "n": name},
        )
        return rows[0][0] if rows else None

    async def list_projects(self) -> list[str]:
        """Return distinct project namespaces from plugin_library."""
        if not self._conn:
            return []
        rows = await self._execute(
            "SELECT DISTINCT namespace FROM plugin_library FINAL"
            " WHERE namespace != 'shared' ORDER BY namespace"
        )
        return [r[0] for r in rows]

    async def save_pipeline(self, pipeline_dict: dict) -> None:
        name = pipeline_dict.get("name", "unnamed")
        sql = """
            INSERT INTO plugin_library (name, type, code, params_json)
            VALUES (%(n)s, 'pipeline', '', %(j)s)
        """
        await self._execute(sql, {"n": name, "j": json.dumps(pipeline_dict)})

    async def load_pipeline(self, name: str) -> dict | None:
        rows = await self._execute(
            "SELECT params_json FROM plugin_library WHERE name=%(n)s AND type='pipeline' LIMIT 1",
            {"n": name},
        )
        return json.loads(rows[0][0]) if rows else None