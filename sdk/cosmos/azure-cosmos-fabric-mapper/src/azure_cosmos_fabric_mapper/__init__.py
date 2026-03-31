"""Azure Cosmos → Fabric mirror mapper.

This package is designed to be an optional dependency. When not installed, upstream
callers (like the Cosmos SDK) should behave normally until mirror serving is explicitly enabled.
"""

from .config import MirrorServingConfiguration
from .credentials import CredentialSource
from .errors import (
    ConfigurationError,
    CredentialError,
    DriverError,
    FabricMapperError,
    MissingOptionalDependencyError,
    UnsupportedCosmosQueryError,
)
from .sdk_hook import MirroredQueryRequest, run_mirrored_query

__version__ = "0.1.0"

__all__ = [
    "ConfigurationError",
    "CredentialError",
    "CredentialSource",
    "DriverError",
    "FabricMapperError",
    "MirrorServingConfiguration",
    "MirroredQueryRequest",
    "MissingOptionalDependencyError",
    "UnsupportedCosmosQueryError",
    "run_mirrored_query",
]
