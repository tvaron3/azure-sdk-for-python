# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-29

### Added
- Initial implementation of Cosmos SQL → Fabric SQL translation
- Support for SELECT, WHERE, ORDER BY, OFFSET/LIMIT, TOP
- Parameterized query support with strict parameter handling
- Result mapping from tabular rows to Cosmos-like documents
- Secure credential pass-through with redaction
- Optional pyodbc driver integration
- Unit tests for translation, parameterization, result mapping, and redaction
- Integration test harness for live Fabric mirror endpoints
- CI/CD pipeline for automated testing
- Comprehensive documentation and quickstart guide

### Security
- Credentials never logged or persisted
- Query parameters never interpolated into SQL strings
- Diagnostic output automatically redacts secrets
