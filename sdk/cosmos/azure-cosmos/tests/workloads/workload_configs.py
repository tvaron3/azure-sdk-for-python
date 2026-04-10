# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.
# All configuration is driven by environment variables with sensible defaults.
import logging
import os

from azure.identity import DefaultAzureCredential

PREFERRED_LOCATIONS = os.environ.get("COSMOS_PREFERRED_LOCATIONS", "").split(",") if os.environ.get("COSMOS_PREFERRED_LOCATIONS") else []
CLIENT_EXCLUDED_LOCATIONS = os.environ.get("COSMOS_CLIENT_EXCLUDED_LOCATIONS", "").split(",") if os.environ.get("COSMOS_CLIENT_EXCLUDED_LOCATIONS") else []
REQUEST_EXCLUDED_LOCATIONS = os.environ.get("COSMOS_REQUEST_EXCLUDED_LOCATIONS", "").split(",") if os.environ.get("COSMOS_REQUEST_EXCLUDED_LOCATIONS") else []
COSMOS_PROXY_URI = os.environ.get("COSMOS_PROXY_URI", "0.0.0.0")
COSMOS_URI = os.environ.get("COSMOS_URI", "")
COSMOS_KEY = os.environ.get("COSMOS_KEY", "")
COSMOS_CREDENTIAL = COSMOS_KEY if COSMOS_KEY else DefaultAzureCredential()
COSMOS_CONTAINER = os.environ.get("COSMOS_CONTAINER", "scale_cont")
COSMOS_DATABASE = os.environ.get("COSMOS_DATABASE", "scale_db")
USER_AGENT_PREFIX = os.environ.get("COSMOS_USER_AGENT_PREFIX", "")
LOG_LEVEL = getattr(logging, os.environ.get("COSMOS_LOG_LEVEL", "DEBUG"), logging.DEBUG)
APP_INSIGHTS_CONNECTION_STRING = os.environ.get("APP_INSIGHTS_CONNECTION_STRING", "")
CIRCUIT_BREAKER_ENABLED = os.environ.get("AZURE_COSMOS_ENABLE_CIRCUIT_BREAKER", "false").lower() == "true"
USE_MULTIPLE_WRITABLE_LOCATIONS = os.environ.get("COSMOS_USE_MULTIPLE_WRITABLE_LOCATIONS", "false").lower() == "true"
CONCURRENT_REQUESTS = int(os.environ.get("COSMOS_CONCURRENT_REQUESTS", "100"))
CONCURRENT_QUERIES = int(os.environ.get("COSMOS_CONCURRENT_QUERIES", "2"))
PARTITION_KEY = os.environ.get("COSMOS_PARTITION_KEY", "id")
NUMBER_OF_LOGICAL_PARTITIONS = int(os.environ.get("COSMOS_NUMBER_OF_LOGICAL_PARTITIONS", "10000"))
THROUGHPUT = int(os.environ.get("COSMOS_THROUGHPUT", "1000000"))

# Workload behavior
_VALID_OPERATIONS = {"read", "write", "query"}
WORKLOAD_OPERATIONS = frozenset(
    op.strip().lower()
    for op in os.environ.get("WORKLOAD_OPERATIONS", "read,write,query").split(",")
    if op.strip()
)
_unknown_ops = WORKLOAD_OPERATIONS - _VALID_OPERATIONS
if _unknown_ops:
    raise ValueError(f"Unknown WORKLOAD_OPERATIONS: {_unknown_ops}. Valid: {_VALID_OPERATIONS}")
WORKLOAD_USE_PROXY = os.environ.get("WORKLOAD_USE_PROXY", "false").lower() == "true"
WORKLOAD_USE_SYNC = os.environ.get("WORKLOAD_USE_SYNC", "false").lower() == "true"
