# Chapter 5: Performance & Benchmarking Landscape

## 5.1 JIT Compilation (Postgres) vs. Vectorization (DuckDB)

PostgreSQL introduced JIT (Just-In-Time) compilation using LLVM to speed up CPU-bound tasks.
-   **How it works**: It compiles expression evaluation (e.g., `price * quantity`) into native machine code at runtime.
-   **The Limit**: It optimizes the *instructions* but doesn't change the *data access pattern*. It is still row-at-a-time.

DuckDB's Vectorization is fundamentally different.
-   **How it works**: It changes the loop structure. `c[i] = a[i] + b[i]` for i=0..1024.
-   **The Win**: This allows the CPU to use SIMD (AVX2/AVX-512) to do 4, 8, or 16 additions in a single CPU cycle.

**Benchmark Reality**: In TPC-H queries involving heavy scanning and aggregation (e.g., Q1), DuckDB is typically **10x to 50x faster** than standard Postgres, and still **5x to 10x faster** than Postgres with JIT enabled.

## 5.2 Network/Protocol Overhead in FDW

In `duckdb_fdw`, the "protocol" is effectively the function call overhead of the FDW API.
-   **Overhead**: For every row, Postgres calls `IterateForeignScan`. This involves function pointer indirection, memory allocation context switching, and tuple formation.
-   **Impact**: Even if DuckDB scans the file instantly, the FDW interface caps the throughput. Empirically, FDWs struggle to exceed 5-10 MB/s of throughput for complex row construction, whereas DuckDB can scan Parquet at 1+ GB/s.

## 5.3 Benchmarks Review (TPC-H on Parquet)

Recent benchmarks (e.g., by CrunchyData, MotherDuck) highlight:
1.  **Count(*) on Parquet**:
    -   `pg_duckdb`: < 1 second (Metadata scan).
    -   `duckdb_fdw`: ~seconds (Depends on pushdown).
    -   `Multicorn (Python FDW)`: ~minutes.
2.  **Aggregation (SUM/AVG)**:
    -   `pg_duckdb`: Near native DuckDB speed.
    -   `duckdb_fdw`: Slower as row count increases. If aggregation is NOT pushed down, performance collapses.

**Conclusion**: For large datasets (GBs to TBs), the FDW model is essentially non-viable for interactive analytics unless the aggregation is perfectly pushed down.
