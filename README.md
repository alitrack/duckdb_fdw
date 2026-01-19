# DuckDB Foreign Data Wrapper for PostgreSQL (v2.0.0+)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Postgres](https://img.shields.io/badge/PostgreSQL-13--18-blue.svg)](https://www.postgresql.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.x-orange.svg)](https://duckdb.org/)

**`duckdb_fdw`** v2.0+ is a high-performance PostgreSQL extension that bridges PostgreSQL's ecosystem with DuckDB's vectorized analytical power. Built natively on the **DuckDB C API**, it supports modern Lakehouse workflows including Parquet, Iceberg, and S3 Tables.

---

## 🚀 Key Enhancements (v2.0.0)

- **Vectorized Read Engine**: Powered by the **Apache Arrow C Data Interface** and **Nanoarrow**. Replaced legacy row-based fetching with high-speed, vectorized data transfer (typically 2048 rows per chunk).
- **High-Performance Ingestion**: 
    - Integrated DuckDB's **Appender API** for `INSERT` operations (Binary-to-Binary).
    - Implemented PostgreSQL **Batch Insert API** (PG 14+), enabling turbo-charged `COPY FROM` performance.
- **Native C API Integration**: Completely removed the SQLite compatibility layer. Built directly on the native DuckDB C API for maximum compatibility and future-proofing.
- **Enhanced Type Support**: Full binary mapping for `BOOLEAN`, `DATE`, `TIMESTAMP`, `DECIMAL`, and `HUGEINT`. Handles epoch conversions (1970 vs 2000) automatically.
- **Cloud Native**:
    - Built-in support for **S3 Tables**, Iceberg, and Delta Lake.
    - Integrated secret management via `duckdb_create_s3_secret()`.
- **Intelligent Pushdown**: 
    - Full support for filter, sorting, and limit pushdown.
    - **Advanced Analytics**: Direct pushdown of statistical functions (`stddev`, `variance`, etc.) and mathematical functions (`sqrt`, `log`, etc.).
    - **Cast Pushdown**: Enables complex joins and expressions by pushing down explicit type casts (`::int4`, `::date`, etc.) directly to DuckDB.
- **Turbo Architecture**: Vectorized data transfer via Arrow C Data Interface and high-speed ingestion using the **Appender API**.

## 📦 Installation

### Quick Build (Linux/macOS)

```bash
# 1. Download DuckDB headers and library
./download_libduckdb.sh

# 2. Build and Install (USE_PGXS is auto-detected)
make
sudo make install
```

### Requirements
* PostgreSQL 13 - 18 (headers required)
* DuckDB library (`libduckdb.so` or `libduckdb.dylib`) v1.0.0+
* GCC or Clang with C11/C++11 support

## 🛠️ Usage

### 1. Initialize
```sql
CREATE EXTENSION duckdb_fdw;

CREATE SERVER duckdb_srv 
FOREIGN DATA WRAPPER duckdb_fdw 
OPTIONS (database ':memory:');
```

### 2. Configure Cloud Credentials (S3)
```sql
-- Easily create secrets without complex SQL concatenation
SELECT duckdb_create_s3_secret('duckdb_srv', 'my_s3_key', 'YOUR_KEY', 'YOUR_SECRET', 'us-east-1');
```

### 3. Query Data Lake (Parquet/Iceberg)
```sql
-- Direct scan of S3 Parquet
CREATE FOREIGN TABLE s3_data (
    id INT,
    price DECIMAL
) SERVER duckdb_srv 
OPTIONS (table 's3://my-bucket/data.parquet');

-- Import whole DuckLake or Iceberg schema
CREATE SCHEMA remote_tpch;
IMPORT FOREIGN SCHEMA "tpch" FROM SERVER duckdb_srv INTO remote_tpch;
```

## 📉 Feature Comparison

| Feature | v1.x (Legacy) | v2.0+ (Native) |
| :--- | :--- | :--- |
| **Kernel Interface** | SQLite Compatibility | **Native DuckDB C API** |
| **Data Transfer** | Row-by-row | **Vectorized (Arrow)** |
| **Type Mapping** | Limited (Text-heavy) | **Full (Decimal/HugeInt/etc)** |
| **Cloud Security** | Plaintext Keys | **Integrated Secret Manager** |
| **Performance** | Basic | **Filter & Limit Pushdown** |

## 🤝 Contributing
Contributions are welcome! Focus areas: Zero-copy memory mapping via Nanoarrow and broader extension support.

## 📄 License
[MIT License](LICENSE)