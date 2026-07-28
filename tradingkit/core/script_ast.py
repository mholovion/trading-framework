"""
tradingkit.core.script_ast — ScriptAST: parse-without-exec introspection shared by
AggregationScript (tradingkit.aggregation) and ConnectionScriptSource (tradingkit.source).

Both classes need the same two primitives on a plugin script string, without ever
exec()-ing it: "does it define function X at module level?" and "what's the literal
value assigned to module-level name Y?". This was previously duplicated across both
classes (and duplicated a second time *within* AggregationScript as near-identical
_ast_str/_ast_literal methods) — written once here, with the parsed tree cached the
way AggregationScript already did.
"""
from __future__ import annotations

import ast
from typing import Any


class ScriptAST:
    """Cached, exec-free AST introspection for a plugin script string."""

    def __init__(self, code: str) -> None:
        self._code = code
        self._tree: ast.Module | None = None

    def _get_tree(self) -> ast.Module:
        if self._tree is None:
            self._tree = ast.parse(self._code)
        return self._tree

    def has_function(self, name: str) -> bool:
        """True if `def name(...)` or `async def name(...)` is defined anywhere in the script."""
        try:
            return any(
                isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name
                for n in ast.walk(self._get_tree())
            )
        except Exception:
            return False

    def has_class(self, base: str | None = None) -> bool:
        """True if the script defines a class -- optionally, one listing `base` among its
        bases. Used to tell a class-style plugin script apart from a function-style or
        expression-style one without executing it."""
        try:
            for node in ast.walk(self._get_tree()):
                if not isinstance(node, ast.ClassDef):
                    continue
                if base is None:
                    return True
                for b in node.bases:
                    name = b.id if isinstance(b, ast.Name) else (
                        b.attr if isinstance(b, ast.Attribute) else None
                    )
                    if name == base:
                        return True
        except Exception:
            pass
        return False

    def get_literal(self, var_name: str) -> Any | None:
        """ast.literal_eval() of a `var_name = <literal>` module-level assignment, or None."""
        try:
            for node in ast.walk(self._get_tree()):
                if (
                    isinstance(node, ast.Assign)
                    and len(node.targets) == 1
                    and isinstance(node.targets[0], ast.Name)
                    and node.targets[0].id == var_name
                ):
                    return ast.literal_eval(node.value)
        except Exception:
            pass
        return None


__all__ = ["ScriptAST"]
