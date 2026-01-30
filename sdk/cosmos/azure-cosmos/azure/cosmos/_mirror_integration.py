# The MIT License (MIT)
# Copyright (c) Microsoft Corporation

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Integration layer for optional Fabric mirror serving."""

from typing import Any, Dict, List, Optional


class MirrorServingNotAvailableError(Exception):
    """Raised when mirror serving is enabled but mapper package is not installed."""

    def __init__(self):
        super().__init__(
            "Mirror serving is enabled but the azure-cosmos-fabric-mapper package "
            "is not installed.\n\n"
            "To enable this feature, install the mapper package:\n"
            "  pip install azure-cosmos-fabric-mapper[sql]\n\n"
            "Or disable mirror serving:\n"
            "  - Set enable_mirror_serving=False in CosmosClient constructor\n"
            "  - Or unset COSMOS_ENABLE_MIRROR_SERVING environment variable"
        )


def _lazy_import_mapper():
    """Dynamically import mapper package only when needed.

    Returns:
        Module handle to azure_cosmos_fabric_mapper.sdk_hook.contract

    Raises:
        MirrorServingNotAvailableError: If package is not installed
    """
    try:
        from azure_cosmos_fabric_mapper.sdk_hook import contract
        return contract
    except ImportError as exc:
        raise MirrorServingNotAvailableError() from exc


def execute_mirrored_query(
    query: str,
    parameters: Optional[List[Dict[str, Any]]],
    mirror_config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Execute query against Fabric mirror using mapper package.

    Args:
        query: Cosmos SQL query text
        parameters: List of parameter dicts with 'name' and 'value' keys
        mirror_config: Dict with fabric_server, fabric_database, fabric_table, fabric_schema

    Returns:
        List of Cosmos-like document dicts

    Raises:
        MirrorServingNotAvailableError: If mapper package not installed
        UnsupportedCosmosQueryError: If query uses unsupported features
        DriverError: If connection to Fabric fails
    """
    contract = _lazy_import_mapper()

    # Import mapper types
    from azure_cosmos_fabric_mapper import MirrorServingConfiguration
    from azure_cosmos_fabric_mapper.credentials import DefaultAzureSqlCredential
    from azure_cosmos_fabric_mapper.driver import get_driver_client

    # Build configuration - normalize key names from SDK to mapper
    # SDK uses: server, database, table_override (optional), credential
    # Mapper expects: fabric_server, fabric_database, fabric_table, fabric_schema
    config = MirrorServingConfiguration(
        fabric_server=mirror_config["server"],
        fabric_database=mirror_config["database"],
        fabric_table=mirror_config.get("table_override", mirror_config.get("fabric_table", "")),
        fabric_schema=mirror_config.get("fabric_schema", mirror_config["database"]),  # Default schema = database name
    )

    # Create request - pass parameters as-is since the mapper expects
    # the same format as Cosmos SDK (list of dicts with 'name' and 'value')
    request = contract.MirroredQueryRequest(
        query=query,
        parameters=parameters,
    )

    # Create credentials and auto-select driver
    # (prefers mssql-python, falls back to pyodbc if unavailable)
    credentials = DefaultAzureSqlCredential()
    driver_client = get_driver_client(config=config, credentials=credentials)

    # Execute using credentials and auto-selected driver
    results = contract.run_mirrored_query(
        request=request,
        config=config,
        credentials=credentials,
        driver=driver_client,
    )

    return results
