# DuckDB Foreign Data Wrapper for PostgreSQL (v2.0.0+)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Postgres](https://img.shields.io/badge/PostgreSQL-13--18-blue.svg)](https://www.postgresql.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.x-orange.svg)](https://duckdb.org/)

**`duckdb_fdw`** v2.0+ is a high-performance PostgreSQL extension that bridges PostgreSQL's ecosystem with DuckDB's vectorized analytical power. Built natively on the **DuckDB C API**, it supports modern Lakehouse workflows including Parquet, Iceberg, and S3 Tables.

---

## 🚀 Key Enhancements (v2.0.0+)

- **DuckDB 1.x Native**: Fully compatible with the latest DuckDB core and extension ecosystem.
- **Smarter Type Mapping**: Automatic identification and mapping for `DECIMAL`, `HUGEINT`, and `VARCHAR` during `IMPORT FOREIGN SCHEMA`.
- **Improved Stability**: Fixed critical crashes (node type 210) during complex `WHERE` clause evaluation.
- **Advanced Filter Pushdown**: Basic filter pushdown is now enabled, ensuring DuckDB only processes relevant data at the source.
- **Enhanced Cloud Security**:
    - New `duckdb_create_s3_secret()` function for easy credential management.
    - Full support for **AWS S3 Tables** and Iceberg catalogs.
- **Turbo Architecture**: Vectorized data transfer via Arrow C Data Interface and high-speed ingestion using the **Appender API**.

For a detailed list of changes from 1.4.1 to now, see [**Evolution Guide (中英对照)**](docs/EVOLUTION.md).

## 📦 Installation

### Quick Build (Linux/macOS)

```bash
# 1. Download latest DuckDB kernel
./download_libduckdb.sh

# 2. Build and Install
make USE_PGXS=1
sudo make install USE_PGXS=1
```

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