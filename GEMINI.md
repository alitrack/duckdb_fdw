# DuckDB Foreign Data Wrapper (FDW) for PostgreSQL

## Project Overview

`duckdb_fdw` is a PostgreSQL extension that allows PostgreSQL to interface natively with DuckDB. It enables PostgreSQL users to query DuckDB databases, Parquet files, and Iceberg tables directly as foreign tables.

**Key Features:**
*   **Native Integration:** Built on the DuckDB C API.
*   **Vectorized Execution:** Uses Arrow C Data Interface and Nanoarrow for high-performance data transfer.
*   **Lakehouse Support:** Supports Parquet, Iceberg, and S3 Tables.
*   **Cloud Native:** Integrated S3 secret management and automatic extension loading (httpfs, iceberg).
*   **Pushdown:** Supports filter and limit pushdown to DuckDB.

## Building and Running

### Prerequisites
*   PostgreSQL headers and libraries (`postgresql-server-dev-X.Y` or similar).
*   DuckDB library (`libduckdb.so` or `libduckdb.dylib`).
*   GCC or Clang.
*   Make.

### Build Commands

The project uses `make` with PostgreSQL's PGXS infrastructure.

```bash
# Clean build artifacts
make USE_PGXS=1 clean

# Build the extension
make USE_PGXS=1

# Install the extension (requires sudo/root)
sudo make install USE_PGXS=1
```

### Running Tests

The project includes a shell script `build_and_test.sh` that automates building, restarting a test PostgreSQL instance, and running a full functional test.

```bash
./build_and_test.sh
```

**Note:** The script currently assumes PostgreSQL 15 paths (`/usr/lib/postgresql/15/bin/pg_ctl`). You may need to adjust this if using a different PostgreSQL version.

## Key Files & Structure

*   **`duckdb_fdw.c`**: Main entry point for the FDW. Defines the handler and validator functions.
*   **`connection.c`**: Manages DuckDB connections, caching, and setup (loading extensions, creating secrets).
*   **`option.c`**: Handles parsing and validation of foreign server and table options.
*   **`import.c`**: Implements `IMPORT FOREIGN SCHEMA` functionality.
*   **`duckdb_optimization.c`**: Logic for query optimization and pushdown analysis.
*   **`nanoarrow.c` / `nanoarrow.h`**: Utilities for handling Arrow data structures.
*   **`Makefile`**: Build configuration using PGXS.
*   **`duckdb_fdw.control`**: PostgreSQL extension control file.
*   **`build_and_test.sh`**: Comprehensive test script.

## Development Conventions

*   **Language:** C (C++ for some DuckDB interfacing, linked with `-lstdc++`).
*   **Style:** Follows PostgreSQL coding conventions (indentation, error handling with `ereport`/`elog`, memory management with `palloc`/`pfree`).
*   **Error Handling:** DuckDB errors are caught and reported via PostgreSQL's error reporting mechanism.
*   **Memory:** Uses PostgreSQL's memory contexts for allocations where possible.
