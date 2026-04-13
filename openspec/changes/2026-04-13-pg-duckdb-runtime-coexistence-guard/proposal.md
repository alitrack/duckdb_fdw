## Why

`duckdb_fdw` dynamically links `libduckdb.so` and executes DuckDB C API calls inside PostgreSQL backend processes. If `pg_duckdb` is also loaded into the same backend, the real risk is shared runtime ambiguity: version skew, different shared-object sources, ABI mismatch, or symbol binding changing with load order. Today the project has no explicit coexistence contract, no runtime guard, and no supported way to explain or block unsafe combinations.

## What Changes

- Introduce a Linux-first runtime guard that detects whether the current backend has already loaded `pg_duckdb`.
- Treat peer-loaded coexistence as blocked by default in v1 unless an explicit unsupported override is enabled.
- Add install-time soft preflight and runtime diagnostics so users understand when and why the guard triggers.
- Reserve a future extension point for stronger cooperative fingerprints without requiring `pg_duckdb` changes in v1.

## Capabilities

### New Capabilities

- `pg-duckdb-runtime-coexistence`: `duckdb_fdw` can preflight, diagnose, and block unsafe coexistence with `pg_duckdb` inside the current backend.

## Impact

- Affected code: `duckdb_fdw.c`, `connection.c`, `duckdb_fdw.h`, `Makefile`, extension SQL files, documentation, and new guard module files.
- Affected behavior: peer-loaded `pg_duckdb` no longer silently shares a backend with `duckdb_fdw`; v1 blocks by default and requires an explicit override for experiments.
- Platform scope: Linux-first for runtime detection in v1.
