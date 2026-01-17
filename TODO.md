# pg_duck (duckdb_fdw) TODO List

## 🎯 Current Focus: Analytical Power & Stability
We have completed the production-grade foundation (stability, type mapping, DuckDB 1.x support). The next phase is to unlock the "Vectorized Analytical Power" of DuckDB for PostgreSQL users.

## 🟥 High Priority: Advanced Pushdown
- [x] **Aggregate Pushdown (`GROUP BY` support)**
    - Implement `GetForeignUpperPaths` in `duckdb_fdw.c`.
    - Support pushing down `count()`, `sum()`, `avg()`, `min()`, `max()`.
    - Ensure correct type handling for aggregated results.
- [ ] **Join Pushdown**
    - Implement `GetForeignJoin` to allow joining two DuckDB foreign tables on the same server.
    - Offload complex join logic to DuckDB's vectorized engine.
- [ ] **Function Pushdown expansion**
    - Support more DuckDB-native functions (String functions, Math functions).

## 🟧 Performance: Zero-Copy & Vectorization
- [ ] **Deeper Nanoarrow Integration**
    - Refactor data extraction to use **Arrow C Data Interface** for all types.
    - Minimize C-string allocations by directly mapping DuckDB memory to Postgres tuples where possible.
- [ ] **Batch Fetching optimization**
    - Tune the number of rows fetched per batch to balance memory usage and throughput.

## 🟩 Features & Ecosystem
- [ ] **Write Performance (Turbo-Loader)**
    - Optimize the **Appender API** for large-scale `INSERT INTO ... SELECT` operations.
- [ ] **Broader Format Support**
    - Explicit support/wrappers for `Delta Lake` and `MySQL/Postgres` scanners.
- [ ] **Transaction Support (Investigation)**
    - Research how to better align DuckDB's local transactions with PostgreSQL's ACID properties for multi-statement operations.

## 🟦 Documentation & DX
- [x] Create Evolution Guide (docs/EVOLUTION.md)
- [x] Implement `.env` based local testing (run_tests.sh)
- [x] Update README with v2.0+ features.
- [ ] Add more complex analytical examples (TPC-H full suite).

---
*Last Updated: 2026-01-17*
