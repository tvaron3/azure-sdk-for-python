"""SDK hook contract for Cosmos SDK integration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from ..config import MirrorServingConfiguration
from ..credentials import CredentialSource, DefaultAzureSqlCredential
from ..driver.base import DriverClient
from ..driver.pyodbc_driver import PyOdbcDriverClient
from ..results.mapper import map_result_set
from ..translate import translate


@dataclass(frozen=True)
class MirroredQueryRequest:
    """Request to run a Cosmos query against a Fabric mirror.
    
    Attributes:
        query: Cosmos SQL query string
        parameters: Query parameters (list of dicts with 'name' and 'value')
    """
    
    query: str
    parameters: Iterable[dict[str, Any]] | None = None


def run_mirrored_query(
    request: MirroredQueryRequest,
    config: MirrorServingConfiguration,
    credentials: CredentialSource | None = None,
    driver: DriverClient | None = None,
) -> list[Any]:
    """Run a Cosmos-style query against a Fabric mirror endpoint.
    
    This is the main entry point for the SDK hook. It translates the query,
    executes it via a driver, and maps results back to Cosmos-like format.
    
    Args:
        request: Query request with Cosmos SQL and parameters
        config: Mirror serving configuration
        credentials: Optional credential source (defaults to DefaultAzureSqlCredential)
        driver: Optional driver client (defaults to PyOdbcDriverClient)
        
    Returns:
        List of results in Cosmos format (dicts or scalars)
        
    Raises:
        ConfigurationError: If configuration is invalid
        UnsupportedCosmosQueryError: If query uses unsupported features
        DriverError: If query execution fails
        MissingOptionalDependencyError: If pyodbc is missing
    """
    config.validate()
    creds = credentials or DefaultAzureSqlCredential()
    drv: DriverClient = driver or PyOdbcDriverClient(config=config, credentials=creds)

    # Translate Cosmos query to Fabric SQL
    t = translate(request.query, request.parameters, config)
    
    # Execute via driver
    rs = drv.execute(t.sql, t.params)
    
    # Map results back to Cosmos format
    return map_result_set(rs, select_value=t.select_value)
