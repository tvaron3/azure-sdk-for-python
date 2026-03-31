"""Credential abstractions for secure SQL connection."""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import Any, Protocol

from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential

from .errors import CredentialError


class CredentialSource(Protocol):
    """Protocol for producing credentials suitable for connecting to Fabric SQL endpoints."""

    def get_sql_access_token_struct(self) -> bytes:
        """Return the packed token bytes for msodbcsql (SQL_COPT_SS_ACCESS_TOKEN).
        
        Returns:
            Packed access token structure for ODBC connection
            
        Raises:
            CredentialError: If credentials cannot be obtained
        """

    def get_sql_access_token_string(self) -> str:
        """Return the access token as a string.
        
        Returns:
            Access token as string
            
        Raises:
            CredentialError: If credentials cannot be obtained
        """

@dataclass(frozen=True)
class DefaultAzureSqlCredential:
    """Uses DefaultAzureCredential to get a database.windows.net access token.
    
    This is the recommended credential source for production use.
    
    Attributes:
        credential: Optional TokenCredential instance (uses DefaultAzureCredential if None)
    """

    credential: TokenCredential | None = None
    _cached_credential: Any = field(default=None, init=False, repr=False)

    def _get_credential(self) -> TokenCredential:
        """Return the configured or cached DefaultAzureCredential."""
        if self.credential is not None:
            return self.credential
        if self._cached_credential is not None:
            return self._cached_credential
        cred = DefaultAzureCredential()
        object.__setattr__(self, '_cached_credential', cred)
        return cred

    def get_sql_access_token_struct(self) -> bytes:
        """Get SQL access token using Azure Identity.
        
        Returns:
            Packed access token structure for ODBC connection
            
        Raises:
            CredentialError: If token acquisition fails
        """
        try:
            cred = self._get_credential()
            token = cred.get_token("https://database.windows.net//.default").token
            token_bytes = bytes(token, "utf-16-le")
            return struct.pack("<I", len(token_bytes)) + token_bytes
        except Exception as exc:
            raise CredentialError(f"Failed to acquire SQL access token: {exc}") from exc

    def get_sql_access_token_string(self) -> str:
        """Get SQL access token as a string.
        
        Returns:
            Access token as string
            
        Raises:
            CredentialError: If token acquisition fails
        """
        try:
            cred = self._get_credential()
            return cred.get_token("https://database.windows.net//.default").token
        except Exception as exc:
            raise CredentialError(f"Failed to acquire SQL access token: {exc}") from exc
