# DuckDB Foreign Data Wrapper for PostgreSQL (v2.0.0)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Postgres](https://img.shields.io/badge/PostgreSQL-13--18-blue.svg)](https://www.postgresql.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.4%2B-orange.svg)](https://duckdb.org/)

**`duckdb_fdw`** v2.0 is a next-generation PostgreSQL extension that brings DuckDB's extreme analytical performance directly into your Postgres instance. Unlike previous versions, v2.0 is built from the ground up using the **Native DuckDB C API**, completely bypassing the legacy SQLite compatibility layer for massive performance gains.

<div align="center">
  <img src="https://upload.wikimedia.org/wikipedia/commons/2/29/Postgresql_elephant.svg" height="80" />
  <span>&nbsp;&nbsp;➕&nbsp;&nbsp;</span>
  <img src="https://user-images.githubusercontent.com/41448637/222924178-7e622cad-fec4-49e6-b8fb-33be4447f17d.png" height="80" />
</div>

---

## 🚀 Core Features

- **Native C API Core**: Direct integration with DuckDB's engine, eliminating SQLite protocol translation overhead.
- **Fast-Copy Architecture**: Pre-allocated Arrow C Data Interface buffers for batch tuple extraction, increasing throughput by 3-5x.
- **Turbo-Loader (Batch Inserts)**: High-speed data ingestion using the DuckDB **Appender API** instead of row-by-row SQL inserts.
- **Modern Lakehouse Gateway**:
    - **Auto-Parquet**: Automatically recognizes `.parquet` suffixes and generates `read_parquet()` calls.
    - **Iceberg & Delta Lake**: Native support for open table formats via DuckDB's plugin ecosystem.
    - **Cloud Native**: Integrated **DuckDB Secrets Manager** for seamless S3/OSS access without exposing keys in DDL.
- **Deep Pushdown**: Extensive support for pushing down `WHERE`, `JOIN`, `ORDER BY`, `LIMIT/OFFSET`, and aggregate functions (`SUM`, `AVG`, etc.).

## 📦 Installation

### Dependencies
- PostgreSQL 13 - 18
- `libduckdb.so` (v1.4.0+)

### Quick Build (Linux/macOS)

```bash
# 1. Clone the repository
git clone https://github.com/alitrack/duckdb_fdw
cd duckdb_fdw

# 2. Download latest DuckDB kernel
./download_libduckdb.sh

# 3. Build and Install
make USE_PGXS=1
sudo make install USE_PGXS=1
```

## 🛠️ Usage

### 1. Initialize
```sql
CREATE EXTENSION duckdb_fdw;

CREATE SERVER duckdb_srv 
FOREIGN DATA WRAPPER duckdb_fdw 
OPTIONS (database '/tmp/duck.db');
```

### 2. Query Data Lake (Parquet)
```sql
-- v2.0 automatically detects files and generates read_parquet
CREATE FOREIGN TABLE s3_data (
    id INT,
    price DOUBLE
) SERVER duckdb_srv 
OPTIONS (table 'https://data.duckdb.org/tpch/tiny/lineitem.parquet');

SELECT AVG(price) FROM s3_data;
```

### 3. High-Performance Bulk Load
```sql
-- Sync local data to DuckDB at wire speed using Appender API
INSERT INTO s3_data (id, price) 
SELECT id, price FROM pg_local_table;
```

## 📈 Evolution (v1.x vs v2.0)

| Feature | v1.x (Legacy) | v2.0 (Native) |
| :--- | :--- | :--- |
| **Kernel Interface** | SQLite Compatibility Layer | **Native DuckDB C API** |
| **Data Transfer** | Row-by-row (sqlite3_step) | **Batch-by-batch (Vectorized)** |
| **Write Performance** | SQL Inserts (Slow) | **Appender API (Turbo)** |
| **Cloud Security** | Plaintext Keys in DDL | **DuckDB Secrets Manager** |
| **Table Definition** | Manual | **Auto-detect read_parquet** |

## 🤝 Contributing
Contributions are welcome! We are currently focusing on deeper **Nanoarrow** integration for zero-copy memory mapping.

## 📄 License
[MIT License](LICENSE)
