# Release History

## 0.1.0 (2025-01-29)

### Features Added
- Initial implementation of Cosmos SQL to Fabric SQL translation
- Support for SELECT, WHERE, ORDER BY, OFFSET/LIMIT, TOP, GROUP BY, HAVING
- Parameterized query support with strict parameter handling
- Result mapping from tabular rows to Cosmos-like documents
- Secure credential pass-through with automatic redaction
- Dual driver support: mssql-python (recommended) and pyodbc (legacy)
- Connection caching with thread-safe driver clients
- Unit tests for translation, parameterization, result mapping, and redaction

### Security
- Credentials never logged or persisted
- Query parameters never interpolated into SQL strings
- Diagnostic output automatically redacts secrets
