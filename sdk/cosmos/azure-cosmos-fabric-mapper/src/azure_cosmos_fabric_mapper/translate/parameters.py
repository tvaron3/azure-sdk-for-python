"""Parameter mapping from Cosmos @ parameters to driver ? placeholders."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

from ..errors import UnsupportedCosmosQueryError


# Pattern to match @paramName in SQL
_PARAM_RE = re.compile(r"@([A-Za-z_][A-Za-z0-9_]*)")


@dataclass(frozen=True)
class ParameterizedSql:
    """SQL with ordered parameters for driver execution.
    
    Attributes:
        sql: SQL string with ? placeholders
        params: Parameter values in order
    """
    
    sql: str
    params: list[Any]


def parameterize(sql_with_at_params: str, parameters: Iterable[dict[str, Any]] | None) -> ParameterizedSql:
    """Replace @param references with ? and build ordered parameter list.
    
    Args:
        sql_with_at_params: SQL with @paramName references
        parameters: List of parameter dicts with 'name' and 'value' keys
        
    Returns:
        ParameterizedSql with ? placeholders and ordered params
        
    Raises:
        UnsupportedCosmosQueryError: If referenced parameter is missing
    """
    # Build parameter lookup dict
    params_by_name: dict[str, Any] = {}
    if parameters:
        for p in parameters:
            name = str(p.get("name") or "")
            if name.startswith("@"):  # Cosmos SDK uses '@name'
                name = name[1:]
            params_by_name[name] = p.get("value")

    # Track parameter usage order
    used_names: list[str] = []

    def repl(match: re.Match[str]) -> str:
        """Replace @param with ? and track parameter name."""
        name = match.group(1)
        used_names.append(name)
        return "?"

    # Replace all @param with ?
    sql = _PARAM_RE.sub(repl, sql_with_at_params)
    
    # Build ordered parameter list
    try:
        ordered = [params_by_name[n] for n in used_names]
    except KeyError as exc:
        raise UnsupportedCosmosQueryError(f"Missing parameter value for @{exc.args[0]}") from exc
    
    return ParameterizedSql(sql=sql, params=ordered)
