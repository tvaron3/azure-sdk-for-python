# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# -------------------------------------------------------------------------
"""Driver package exports."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from .base import DriverClient, ResultSet

if TYPE_CHECKING:
    from ..config import MirrorServingConfiguration
    from ..credentials import CredentialSource

def get_driver_client(
    config: MirrorServingConfiguration,
    credentials: CredentialSource,
    prefer_driver: str | None = None,
) -> DriverClient:
    """Get an appropriate driver client based on availability.
    
    Priority order (unless prefer_driver is specified):
    1. mssql-python (MssqlDriverClient) - recommended, pure Python
    2. pyodbc (PyOdbcDriverClient) - legacy, requires system ODBC driver
    
    Args:
        config: Mirror serving configuration
        credentials: Credential source
        prefer_driver: Optional driver preference ('mssql-python' or 'pyodbc')
        
    Returns:
        DriverClient instance
        
    Raises:
        ImportError: If no supported driver is available
    """
    # Try preferred driver first if specified
    if prefer_driver == "mssql-python":
        try:
            from .mssql_driver import MssqlDriverClient
            return MssqlDriverClient(config=config, credentials=credentials)
        except ImportError:
            warnings.warn(f"Preferred driver '{prefer_driver}' not available, falling back to auto-detection")
    
    elif prefer_driver == "pyodbc":
        try:
            from .pyodbc_driver import PyOdbcDriverClient
            return PyOdbcDriverClient(config=config, credentials=credentials)
        except ImportError:
            warnings.warn(f"Preferred driver '{prefer_driver}' not available, falling back to auto-detection")
    
    # Default: try mssql-python first (recommended)
    try:
        from .mssql_driver import MssqlDriverClient
        return MssqlDriverClient(config=config, credentials=credentials)
    except ImportError:
        pass
    
    # Fallback: try pyodbc
    try:
        from .pyodbc_driver import PyOdbcDriverClient
        return PyOdbcDriverClient(config=config, credentials=credentials)
    except ImportError:
        pass
    
    # No driver available
    raise ImportError(
        "No SQL driver available. Install one of:\n"
        "  - mssql-python (recommended): pip install azure-cosmos-fabric-mapper[sql]\n"
        "  - pyodbc (legacy): pip install azure-cosmos-fabric-mapper[odbc]"
    )


__all__ = ["DriverClient", "ResultSet", "get_driver_client"]
