## Why

The expert review surfaced real production-readiness gaps that are still visible in the current code: admin-like helper functions are open by default, some SQL construction paths still interpolate user-controlled values unsafely, and resource cleanup does not consistently survive PostgreSQL error unwinds. At the same time, repository claims drifted away from the actual implementation state, which makes subsequent work hard to scope and verify.

## What Changes

- Restrict `duckdb_execute` and `duckdb_create_s3_secret` by default so fresh installs and upgrades do not leave arbitrary execution helpers callable via default function privileges.
- Close remaining unsafe SQL interpolation sites in `deparse.c` and `connection.c` using existing escaping helpers.
- Harden DuckDB result cleanup in helper execution paths so `ereport(ERROR)` does not skip destruction of DuckDB resources.
- Align public capability claims with the code that actually exists today, especially the distinction between chunk-based scan iteration and the still-planned full Arrow C Data path.
- Add regression coverage for default helper privileges and document the current known limits.

## Capabilities

### New Capabilities
- `admin-execution-surface`: Admin-like helper functions are restricted by default and require explicit grants when exposed to non-owner roles.
- `fdw-resource-lifecycle-safety`: DuckDB result resources are cleaned up on both success and PostgreSQL error paths in helper execution code.

### Modified Capabilities
- `safe-duckdb-sql-construction`: Extend the requirement to remaining high-risk interpolation sites such as analyze SQL generation and derived S3 Tables endpoints.
- `verifiable-feature-documentation`: Public capability claims must match current code-backed behavior, not stale TODO intent.
- `capability-status-matrix`: The read-path matrix must distinguish the implemented chunk-result path from the unimplemented full Arrow C Data path.

## Impact

- Affected code: `duckdb_fdw.c`, `connection.c`, `deparse.c`, extension SQL install/upgrade scripts.
- Affected docs: `README.md`, `TODO.md`, OpenSpec change/spec artifacts.
- Affected tests: regression SQL/output for helper privilege defaults.
- Operational impact: deployments that relied on implicit `PUBLIC` execute privileges will need explicit `GRANT EXECUTE`.
