## Why

The current codebase builds multiple DuckDB SQL statements by direct string interpolation of server/table/options values. This creates SQL injection and credential exposure risk in high-impact paths (connection setup, IMPORT FOREIGN SCHEMA, secret creation, and table mapping).

## What Changes

- Introduce centralized, tested SQL-escaping helpers for DuckDB identifiers and string literals.
- Replace unsafe string interpolation in `connection.c`, `duckdb_fdw.c`, `import.c`, and relation deparse paths with safe builders.
- Validate dynamic tokens (extension names, attach aliases, secret names) with strict allowlists; reject invalid tokens early.
- Redact credentials and sensitive tokens from surfaced error messages and logs.
- Add regression tests for injection payloads and secret redaction behavior.

## Capabilities

### New Capabilities
- `safe-duckdb-sql-construction`: Build DuckDB SQL using trusted escaping/validation primitives only.
- `secure-secret-handling`: Prevent credential leakage in SQL generation and error reporting.

### Modified Capabilities
- None.

## Impact

- Affected code: `connection.c`, `duckdb_fdw.c`, `import.c`, `deparse.c`, helper headers.
- Affected behavior: stricter validation may reject previously accepted malformed identifiers.
- Test impact: new security-focused regression tests and CI checks.
