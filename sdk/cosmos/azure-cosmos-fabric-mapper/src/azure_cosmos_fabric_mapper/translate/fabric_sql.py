"""Fabric SQL emitter - transforms AST to Fabric SQL syntax."""

from __future__ import annotations

from ..config import MirrorServingConfiguration, default_table_sql
from .ast import QueryAst


def emit_fabric_sql(ast: QueryAst, config: MirrorServingConfiguration) -> str:
    """Emit Fabric SQL from parsed Cosmos query AST.
    
    Args:
        ast: Parsed query AST
        config: Mirror serving configuration
        
    Returns:
        Fabric SQL query string (may contain @param references)
        
    Raises:
        ConfigurationError: If config is invalid
    """
    table_sql = default_table_sql(config)
    select_expr = ast.select_expr

    # Handle TOP + ORDER BY (no offset)
    if ast.limit is not None and ast.offset is None:
        sql = f"SELECT TOP {ast.limit} {select_expr} FROM {table_sql} AS c"
        if ast.where_expr:
            sql += f" WHERE {ast.where_expr}"
        if ast.group_by:
            sql += f" GROUP BY {ast.group_by}"
        if ast.having_expr:
            sql += f" HAVING {ast.having_expr}"
        if ast.order_by:
            sql += f" ORDER BY {ast.order_by}"
        return sql

    # Standard SELECT
    sql = f"SELECT {select_expr} FROM {table_sql} AS c"
    if ast.where_expr:
        sql += f" WHERE {ast.where_expr}"
    if ast.group_by:
        sql += f" GROUP BY {ast.group_by}"
    if ast.having_expr:
        sql += f" HAVING {ast.having_expr}"
    if ast.order_by:
        sql += f" ORDER BY {ast.order_by}"
    
    # OFFSET/LIMIT (requires ORDER BY in Fabric SQL)
    if ast.offset is not None and ast.limit is not None:
        sql += f" OFFSET {ast.offset} ROWS FETCH NEXT {ast.limit} ROWS ONLY"
    
    return sql
