# Azure Cosmos → Fabric Mirror Mapper (Python)


This package provides an **optional**, **separately installable** Python library that can translate a subset of Cosmos-style SQL queries into parameterized SQL suitable for querying a Fabric mirrored table (via ODBC), then map tabular results back to Cosmos-like result shapes.

## Features

- 🔄 **Transparent query translation**: Cosmos SQL → Fabric SQL for supported subset
- 🔒 **Secure credential pass-through**: No secrets in logs or exceptions
- 📊 **Result shape mapping**: Tabular rows → Cosmos-like documents
- 🎯 **ORDER BY support**: Side-benefit for queries not supported in Cosmos Python SDK today
- ⚡ **Aggregation-friendly**: Run expensive aggregations against Fabric mirror instead of Cosmos
- 🔌 **Optional dependency**: No impact on Cosmos SDK unless explicitly enabled

## Installation

### Recommended: With mssql-python (pure Python, no system dependencies on Windows)

```bash
pip install azure-cosmos-fabric-mapper[sql]
```

**That's it for Windows!** No system ODBC driver installation required. ✨

For Linux/macOS, minimal system libraries are needed:

**Linux (Debian/Ubuntu):**
```bash
apt-get install -y libltdl7 libkrb5-3 libgssapi-krb5-2
pip install azure-cosmos-fabric-mapper[sql]
```

**macOS:**
```bash
brew install openssl
pip install azure-cosmos-fabric-mapper[sql]
```

### Alternative: With pyodbc (legacy, requires system ODBC driver)

```bash
pip install azure-cosmos-fabric-mapper[odbc]
```

**Important**: The pyodbc option requires the **ODBC Driver 18 for SQL Server** to be installed separately:

- **Windows**: Download installer from [Microsoft](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)
- **Linux**: Follow [installation guide](https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
- **macOS**: Use Homebrew: `brew install microsoft/mssql-release/msodbcsql18`

> **Note**: The `mssql-python` driver is recommended for most users as it eliminates the need for system-level ODBC driver installation. Both drivers provide the same functionality and are officially supported by Microsoft for Fabric SQL endpoints.

### For development

```bash
pip install -e .[dev,sql,cosmos]
```

Or with legacy pyodbc:
```bash
pip install -e .[dev,odbc,cosmos]
```

### Integration with Cosmos SDK

To enable mirror serving in the Azure Cosmos DB Python SDK, you need to:

1. **Install this mapper package** (see above)
2. **Patch the Cosmos SDK** with mirror serving support (see [SDK Integration Guide](specs/001-fabric-mirror-mapper/contracts/sdk-integration-implementation.md))
3. **Configure mirror serving** when creating the Cosmos client:

```python
from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential

client = CosmosClient(
    url="https://my-account.documents.azure.com:443/",
    credential=DefaultAzureCredential(),
    enable_mirror_serving=True,  # Enable mirror serving
    mirror_config={
        "fabric_server": "your-fabric.msit-datawarehouse.fabric.microsoft.com",
        "fabric_database": "your-database",
        "fabric_table": "your-table",
        "fabric_schema": "dbo",
    }
)

# All queries now automatically route to Fabric mirror
container = client.get_database_client("mydb").get_container_client("mycont")
items = container.query_items(
    query="SELECT * FROM c WHERE c.category = @cat ORDER BY c.price",
    parameters=[{"name": "@cat", "value": "electronics"}]
)
```

**Note**: The Cosmos SDK integration is opt-in and requires SDK patches. See the [SDK Integration Guide](specs/001-fabric-mirror-mapper/contracts/sdk-integration-implementation.md) for implementation details.

## Quick Start

```python
from azure_cosmos_fabric_mapper import MirrorServingConfiguration, run_mirrored_query
from azure_cosmos_fabric_mapper.sdk_hook import MirroredQueryRequest

# Configure Fabric mirror connection
config = MirrorServingConfiguration(
    fabric_server="your-fabric-endpoint.msit-datawarehouse.fabric.microsoft.com",
    fabric_database="your-database",
    fabric_table="your-table",
    fabric_schema="dbo",
)

# Run a Cosmos-style query against Fabric mirror
request = MirroredQueryRequest(
    query="SELECT * FROM c WHERE c.partitionKey = @pk ORDER BY c.id",
    parameters=[{"name": "@pk", "value": "partition1"}]
)

results = run_mirrored_query(request, config)
# returns list of dicts matching Cosmos result shape
```

## Supported Cosmos SQL Subset

**Supported features** (initial version):
- `SELECT` projections (including `SELECT *`, `SELECT VALUE`)
- `FROM c` (container alias)
- `WHERE` filters with boolean expressions, comparisons, AND/OR/NOT
- Parameterized queries (`@param`)
- `ORDER BY` (single/multi-column, ASC/DESC)
- `OFFSET`/`LIMIT` pagination
- `TOP` limit
- Aggregations: `COUNT`, `SUM`, `MAX`, `MIN`, `AVG`

**Not yet supported** (will raise `UnsupportedCosmosQueryError`):
- Subqueries
- JOINs (array/object traversal)
- User-defined functions (UDFs)
- Spatial/geospatial functions
- Complex projections (nested object construction)
- `DISTINCT`
- `GROUP BY` / `HAVING`
- String functions beyond basic operators
- Type coercion functions

See [API contract documentation](specs/001-fabric-mirror-mapper/contracts/python-api.md) for details.

---

## Compatibility & Limitations

### Cosmos SDK Compatibility

**Supported**:
- Azure Cosmos DB Python SDK v4.x
- Python 3.9+

**Limitations**:
- Requires Cosmos SDK patches for full integration (see [SDK Integration Guide](specs/001-fabric-mirror-mapper/contracts/sdk-integration-implementation.md))
- Can be used standalone without SDK integration via `run_mirrored_query()` API

### Fabric Mirror Requirements

**Prerequisites**:
- Fabric Mirroring for Cosmos DB enabled on your Cosmos account
- Mirror database and table created in Fabric
- RBAC permissions: Reader or Custom role with read access to Fabric SQL endpoint

**Known Limitations**:
1. **Schema mismatch**: Fabric mirror schema must match Cosmos container structure
2. **Replication lag**: Fabric mirror has eventual consistency (typically seconds to minutes)
3. **No write operations**: Mapper is read-only; writes must go through Cosmos DB
4. **Container metadata**: `_rid`, `_self`, `_etag`, `_attachments`, `_ts` fields may differ

### Query Translation Behavior

| Cosmos SQL | Fabric SQL | Notes |
|-----------|-----------|-------|
| `SELECT VALUE expr` | `SELECT expr` | Returns scalar array, not document array |
| `SELECT *` | `SELECT [column list]` | All columns mapped to dict |
| `@param` | `?` | Parameters converted to positional placeholders |
| `TOP 10` | `SELECT TOP 10` | Direct mapping |
| `OFFSET 5 LIMIT 10` | `OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY` | T-SQL pagination syntax |
| `ORDER BY c.id` | `ORDER BY c.id` | Ascending by default |
| `COUNT(1)` | `COUNT(1)` | Direct mapping |

### Security Considerations

**Safe**:
- ✅ Credentials never logged or persisted
- ✅ Secrets redacted in error messages (regex-based)
- ✅ Parameters never interpolated into SQL (strict parameterization)
- ✅ Azure AD authentication via `DefaultAzureCredential`

**User Responsibility**:
- ⚠️ Ensure Fabric RBAC permissions are correctly configured
- ⚠️ Review query patterns for Fabric SQL injection (though parameters are safe)
- ⚠️ Monitor Fabric query costs and performance

### Performance Expectations

**Fabric mirror is faster for**:
- ✅ Aggregations (COUNT, SUM, AVG, MAX, MIN)
- ✅ Cross-partition scans with filters
- ✅ ORDER BY queries (not supported in Cosmos Python SDK)
- ✅ Analytics workloads on large datasets

**Cosmos DB is faster for**:
- ✅ Single-partition point reads
- ✅ Queries with partition key filters
- ✅ Write operations (Fabric is read-only)

**Trade-offs**:
- Fabric mirror has eventual consistency lag
- Cosmos guarantees strong consistency options

## Migrating from pyodbc to mssql-python

If you're currently using the `[odbc]` extra with pyodbc, migrating to `mssql-python` is straightforward:

### 1. Update installation

**Before (pyodbc):**
```bash
pip install azure-cosmos-fabric-mapper[odbc]
# + separate ODBC driver installation
```

**After (mssql-python):**
```bash
pip install azure-cosmos-fabric-mapper[sql]
# No additional system driver needed on Windows!
```

### 2. Code changes

**No code changes required!** Both drivers use the same DB-API 2.0 interface and connection string format.

Your existing configuration will work as-is:

```python
from azure_cosmos_fabric_mapper import MirrorServingConfiguration

config = MirrorServingConfiguration(
    fabric_server="your-fabric.msit-datawarehouse.fabric.microsoft.com",
    fabric_database="your-database",
    fabric_table="your-table",
    fabric_schema="dbo",
)
# This works with both pyodbc and mssql-python!
```

### 3. Benefits of mssql-python

- ✅ **Simpler deployment**: No system driver installation on Windows
- ✅ **Official Microsoft driver**: First-party support for Fabric SQL
- ✅ **Fewer dependencies**: Pure Python implementation
- ✅ **Same functionality**: Full DB-API 2.0 compliance
- ✅ **Better portability**: Easier CI/CD and containerization

### 4. Keeping pyodbc (if needed)

If you need to continue using pyodbc:
1. Keep using `[odbc]` extra: `pip install azure-cosmos-fabric-mapper[odbc]`
2. Ensure ODBC Driver 18 for SQL Server is installed
3. Both drivers will continue to be supported

---

## Testing

Run unit tests (no live services required):

```bash
pytest -m "not integration and not e2e"
```

Run integration tests (requires Fabric mirror endpoint configured via environment variables):

```bash
export FABRIC_SERVER="your-endpoint.msit-datawarehouse.fabric.microsoft.com"
export FABRIC_DATABASE="your-database"
export FABRIC_TABLE="your-table"
pytest -m integration
```

## Security & Redaction

- **Credentials are never logged**: All diagnostic outputs redact secrets
- **Parameters are never interpolated**: Query translation maintains strict parameterization
- **No persistence by default**: Credentials exist only in-memory during connection

## Architecture

```
┌─────────────────┐
│  Cosmos Query   │
│  (SQL + params) │
└────────┬────────┘
         │
         v
┌─────────────────────┐
│  Parser (lark)      │
│  → AST              │
└────────┬────────────┘
         │
         v
┌─────────────────────┐
│  Fabric SQL Emitter │
│  (parameterized)    │
└────────┬────────────┘
         │
         v
┌──────────────────────────┐
│  SQL Driver              │
│  - mssql-python (primary)│
│  - pyodbc (legacy)       │
│  → ResultSet             │
└────────┬─────────────────┘
         │
         v
┌─────────────────────┐
│  Result Mapper      │
│  → Cosmos-like docs │
└─────────────────────┘
```

**Driver Strategy**: 
- **Primary**: `mssql-python` (pure Python, no system deps on Windows)
- **Legacy**: `pyodbc` (requires system ODBC driver)
- Both use DB-API 2.0 interface for seamless interchangeability

## Contributing

See [specs/001-fabric-mirror-mapper/](specs/001-fabric-mirror-mapper/) for design docs.

## Known Limitations

- **Regex-based parser keyword collisions**: The parser may mis-parse column names that contain SQL keywords as substrings (e.g., `c.order_date`, `c.group_name`, `c.from_source`).
- **String literals containing SQL keywords**: String literals containing SQL keywords as whole words (e.g., `'ORDER processing'`, `'GROUP therapy'`) may cause parse failures. Use parameterized queries (`@param`) instead of inline string values to avoid this.
- **Supported SQL subset only**: Only a subset of Cosmos SQL is supported: `SELECT`, `FROM`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `OFFSET`/`LIMIT`, and `TOP`.
- **Unsupported features**: Nested subqueries, JOINs (array/object traversal), and user-defined functions (UDFs) are not supported and will raise `UnsupportedCosmosQueryError`.

## License

MIT
