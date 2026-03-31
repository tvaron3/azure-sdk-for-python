"""Credential abstractions for secure SQL connection."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Protocol

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

    def get_sql_access_token_bytes(self) -> bytes:
        """Return the access token bytes (for drivers that need raw token).
        
        Returns:
            Access token as bytes (UTF-16LE encoded with length prefix)
            
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

    def get_sql_access_token_struct(self) -> bytes:
        """Get SQL access token using Azure Identity.
        
        Returns:
            Packed access token structure for ODBC connection
            
        Raises:
            CredentialError: If token acquisition fails
        """
        try:
            cred = self.credential or DefaultAzureCredential()
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
            cred = self.credential or DefaultAzureCredential()
            return cred.get_token("https://database.windows.net//.default").token
        except Exception as exc:
            raise CredentialError(f"Failed to acquire SQL access token: {exc}") from exc

    def get_sql_access_token_bytes(self) -> bytes:
        """Get SQL access token bytes for drivers.
        
        Returns:
            Access token as bytes (UTF-16LE encoded with length prefix)
            
        Raises:
            CredentialError: If token acquisition fails
        """
        # Same as get_sql_access_token_struct
        return self.get_sql_access_token_struct()


@dataclass(frozen=True)
class SqlUsernamePasswordCredential:
    """Username/password for SQL authentication (discouraged; provided for flexibility).
    
    Note: This does not produce an access token struct and should be used only
    when explicitly required. Azure AD authentication is recommended instead.
    
    Attributes:
        username: SQL username
        password: SQL password (never logged)
    """

    username: str
    password: str

    def __post_init__(self) -> None:
        """Validate that username and password are provided."""
        if not self.username or not self.password:
            raise CredentialError("username/password are required")

    def get_sql_access_token_struct(self) -> bytes:
        """Raise error - username/password does not provide token struct.
        
        Raises:
            CredentialError: Always (this credential type doesn't support token auth)
        """
        raise CredentialError("Username/password credentials do not provide an access token struct")

    def get_sql_access_token_string(self) -> str:
        """Raise error - username/password does not provide token string.
        
        Raises:
            CredentialError: Always (this credential type doesn't support token auth)
        """
        raise CredentialError("Username/password credentials do not provide an access token string")

    def get_sql_access_token_bytes(self) -> bytes:
        """Raise error - username/password does not provide token bytes.
        
        Raises:
            CredentialError: Always (this credential type doesn't support token auth)
        """
        raise CredentialError("Username/password credentials do not provide an access token bytes")
