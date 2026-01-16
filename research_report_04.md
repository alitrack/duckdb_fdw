# Chapter 4: Comparative Architecture: duckdb_fdw vs. pg_duckdb

## 4.1 duckdb_fdw: The Loose Coupling Approach

`duckdb_fdw` adheres to the classic Postgres FDW architecture.
-   **Role**: Postgres is the "Leader", DuckDB is the "Follower".
-   **Flow**:
    1.  User sends SQL to Postgres.
    2.  Postgres parses and plans.
    3.  Postgres sees a `FOREIGN TABLE` and asks `duckdb_fdw` to scan it.
    4.  `duckdb_fdw` constructs a SQL string (e.g., `SELECT * FROM parquet_scan(...)`) and sends it to a DuckDB instance (linked library).
    5.  DuckDB executes and returns a result set.
    6.  `duckdb_fdw` iterates the result set, converting C++ types to Postgres Datums.
    7.  Postgres receives tuples and performs any final aggregation/sorting locally.

**The Bottleneck**: Step 6. The data is effectively "serialized" across the API boundary one row at a time.

## 4.2 pg_duckdb: The Tight Embedding Approach

`pg_duckdb` (developed by MotherDuck and Hydra) takes a "Co-Processor" approach.
-   **Role**: Postgres is the "Router", DuckDB is the "Engine".
-   **Flow**:
    1.  User sends SQL to Postgres.
    2.  `pg_duckdb` utilizes the **Custom Executor Hook**.
    3.  It inspects the plan. If it sees a supported analytical query (e.g., scan Parquet + Aggregate), it "hijacks" the execution.
    4.  It translates the *entire plan fragment* into a DuckDB plan.
    5.  DuckDB executes the query internally.
    6.  **Crucially**: The heavy aggregation happens *inside* DuckDB.
    7.  Only the *final result* (e.g., 5 rows of summary data) is converted back to Postgres tuples.

## 4.3 Execution Flow Differences

| Feature | duckdb_fdw | pg_duckdb |
| :--- | :--- | :--- |
| **Execution Engine** | Postgres (mostly) | DuckDB (mostly) |
| **Aggregation** | Often falls back to Postgres | Pushed into DuckDB |
| **Data Movement** | Moves all raw rows for non-pushed operations | Moves only final results |
| **Complex Joins** | Hard to optimize across engines | Can handle joins inside DuckDB |

## 4.4 Data Transfer: Serialization vs. Zero-Copy

While `pg_duckdb` currently reduces data transfer by pushing down aggregation, the "Holy Grail" is **Zero-Copy**.
-   **Concept**: If both Postgres and DuckDB could agree on a memory format (like Apache Arrow), one engine could hand a pointer to the other without copying bytes.
-   **Current State**: `pg_duckdb` is actively working towards this, leveraging DuckDB's Arrow interface. `duckdb_fdw` is limited by the FDW API, which mandates HeapTuple construction, making zero-copy structurally impossible without breaking the FDW abstraction.
