## Why

The repository claims Arrow-based vectorized reads and Appender/Batch insert performance, but runtime paths still use row-wise value access and SQL-string inserts. This gap blocks performance goals and undermines trust in feature claims.

## What Changes

- Replace row-wise scan execution with Arrow stream/chunk consumption from DuckDB.
- Implement typed vectorized conversion from Arrow arrays into PostgreSQL tuple slots.
- Implement Appender-based insert path and PG batch insert hooks for COPY/INSERT throughput.
- Add fallback behavior and feature flags for compatibility/safe rollout.
- Add performance and correctness regression tests for vectorized read/write paths.

## Capabilities

### New Capabilities
- `vectorized-arrow-read-path`: Stream data in Arrow chunks instead of per-row value API calls.
- `batch-appender-write-path`: Use Appender and PG batch hooks for high-throughput writes.

### Modified Capabilities
- None.

## Impact

- Affected code: `duckdb_fdw.c`, `duckdb_fdw.h`, Arrow conversion utilities, tests.
- API behavior: no SQL-level API break; internal execution path changes significantly.
- Operational impact: large performance uplift expected, with fallback for unsupported types.
