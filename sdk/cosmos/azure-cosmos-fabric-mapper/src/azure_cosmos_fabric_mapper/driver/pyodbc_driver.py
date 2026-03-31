"""pyodbc-based driver implementation (optional dependency)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from ..config import MirrorServingConfiguration
from ..credentials import CredentialSource
from ..errors import DriverError, MissingOptionalDependencyError
from .base import ResultSet


def _import_pyodbc():
    """Attempt to import pyodbc with helpful error message if missing.
    
    Returns:
        pyodbc module
        
    Raises:
        MissingOptionalDependencyError: If pyodbc is not installed
    """
    try:
        import pyodbc  # type: ignore

        return pyodbc
    except ImportError as exc:  # pragma: no cover
        raise MissingOptionalDependencyError(
            "pyodbc is required for ODBC connectivity. "
            "Install with 'pip install azure-cosmos-fabric-mapper[odbc]'."
        ) from exc


@dataclass(frozen=True)
class PyOdbcDriverClient:
    """ODBC driver client using pyodbc for Fabric SQL connectivity.
    
    Attributes:
        config: Mirror serving configuration
        credentials: Credential source for SQL authentication
    """
    
    config: MirrorServingConfiguration
    credentials: CredentialSource

    def execute(self, sql: str, params: Sequence[Any]) -> ResultSet:
        """Execute a parameterized SQL query via pyodbc.
        
        Args:
            sql: Parameterized SQL query (uses '?' placeholders)
            params: Parameter values in order
            
        Returns:
            ResultSet containing columns and rows
            
        Raises:
            DriverError: If execution fails
            MissingOptionalDependencyError: If pyodbc is not installed
        """
        pyodbc = _import_pyodbc()
        self.config.validate()

        conn_str = (
            f"Driver={{{self.config.odbc_driver}}};"
            f"Server=tcp:{self.config.fabric_server};"
            f"Database={self.config.fabric_database};"
            "Encrypt=yes;TrustServerCertificate=no;"
        )

        try:
            token_struct = self.credentials.get_sql_access_token_struct()
            # SQL_COPT_SS_ACCESS_TOKEN = 1256
            conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
            try:
                cur = conn.cursor()
                cur.execute(sql, list(params))
                columns = [c[0] for c in cur.description] if cur.description else []
                rows = [tuple(r) for r in cur.fetchall()] if cur.description else []
                return ResultSet(columns=columns, rows=rows)
            finally:
                conn.close()
        except MissingOptionalDependencyError:
            raise
        except Exception as exc:
            raise DriverError(f"Driver execution failed: {type(exc).__name__}: {exc}") from exc
