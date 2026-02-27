## Context

Current read path executes `duckdb_query` and retrieves values through `duckdb_value_*` per row/column. Insert path builds SQL text `INSERT INTO ... VALUES (...)`. The code includes Arrow/nanoarrow dependencies but does not use them in the main scan loop.

## Goals / Non-Goals

**Goals:**
- Execute scans via Arrow chunk streaming with predictable memory lifecycle.
- Execute inserts via DuckDB Appender and support PG batch insert hooks.
- Preserve existing SQL interface and type-correctness guarantees.

**Non-Goals:**
- Full zero-copy integration for every complex type in first iteration.
- Reworking deparse pushdown logic in this change.

## Decisions

1. Read path architecture:
- Use DuckDB Arrow query APIs to fetch chunked `ArrowArray` + `ArrowSchema`.
- Maintain per-scan chunk cursor state in `DuckDBFdwExecState`.
- Convert each chunk vector to PG Datums using type-specific converters.
Rationale: chunked iteration matches FDW slot iteration while avoiding per-value API overhead.

2. Write path architecture:
- Initialize Appender in `BeginForeignModify`.
- Use typed appender APIs in `ExecForeignInsert`.
- Implement `GetForeignModifyBatchSize` and `ExecForeignBatchInsert` for PG14+.
Rationale: aligns with DuckDB optimized ingestion path and PG batch interfaces.

3. Compatibility/fallback:
- Fallback to existing path only for unsupported type combinations, with clear warning.
- Add a temporary server option to disable vectorized path for emergency rollback.
Rationale: safer rollout in mixed production environments.

## Risks / Trade-offs

- [Risk] Type conversion bugs in Arrow path for edge types.
  -> Mitigation: staged type support matrix and regression tests for each type class.
- [Risk] Memory lifetime issues with Arrow buffers.
  -> Mitigation: strict ownership model and valgrind/sanitizer checks in CI.

## Migration Plan

1. Introduce Arrow scan state structs and converter primitives.
2. Switch scan loop to Arrow chunk iteration with fallback.
3. Introduce Appender insert path and batch hooks.
4. Run correctness + benchmark tests; then remove legacy path where safe.

Rollback: keep fallback path behind option for one release cycle.

## Open Questions

- Which complex nested types are in scope for v1 of vectorized conversion?
- Should fallback be per-table option or global server option?
