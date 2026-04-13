# DuckDB Foreign Data Wrapper for PostgreSQL (v2.0.1+)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Postgres](https://img.shields.io/badge/PostgreSQL-13--18-blue.svg)](https://www.postgresql.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.x-orange.svg)](https://duckdb.org/)

**`duckdb_fdw`** v2.0+ is a high-performance PostgreSQL extension that bridges PostgreSQL's ecosystem with DuckDB's vectorized analytical power. Built natively on the **DuckDB C API**, it supports modern Lakehouse workflows including Parquet, Iceberg, and S3 Tables.

---

## 🚀 Capability Status (v2.0.1)

The table below reflects the **current implementation status** and required runtime prerequisites.

| Capability | Status | Validation Evidence | Prerequisites |
| :--- | :--- | :--- | :--- |
| Native DuckDB C API integration | Implemented | `duckdb_fdw.c`, `connection.c` | DuckDB shared library |
| Chunk-based scan iteration | Implemented | `duckdbBeginForeignScan`, `duckdbIterateForeignScan` | PostgreSQL 13+ |
| Prepared parameter binding (`?`) | Implemented | `duckdb_execute_query` bind path | Pushdown query with params |
| Appender insert path | Implemented | `duckdbBeginForeignModify`, `duckdbExecForeignInsert` | Writable foreign table |
| Batch insert hooks (PG14+) | Implemented | `ExecForeignBatchInsert`, `GetForeignModifyBatchSize` | PostgreSQL 14+ |
| Secret helper (`duckdb_create_s3_secret`) | Implemented | SQL function + `duckdb_fdw.c` | S3 credentials |
| Runtime coexistence guard for `pg_duckdb` | Implemented (Linux-first) | `runtime_guard.c`, `scripts/verify_pg_duckdb_coexistence.sh` | Same-backend peer detection |
| Iceberg/S3 examples | Partial | `examples/07-13` | Network, optional credentials |
| Full Arrow C Data scan path | Planned | no `duckdb_query_arrow` call in active scan path; tracked as future work | future release |

## 🧪 Test Profiles

- `core`: deterministic offline suite (default)
- `integration`: network/public dataset suite
- `cloud`: credential-required cloud suite
- `all`: run all tiers

```bash
./run_tests.sh --profile core
./run_tests.sh --profile integration
./run_tests.sh --profile cloud

# Optional: include Linux-first pg_duckdb coexistence guard verification
RUN_PG_DUCKDB_COEXISTENCE_CHECK=1 ./run_tests.sh --profile core
```

## 📦 Installation

### Quick Build (Linux/macOS)

```bash
# Optional: prepare PostgreSQL development prerequisites on Debian/Ubuntu/WSL
scripts/install_pg_env.sh --pg-major 17
scripts/install_pg_env.sh --pg-major 17 --apply
scripts/verify_pg_env.sh --pg-major 17

# 1. Download DuckDB headers and library
./download_libduckdb.sh

# Or pin a specific DuckDB release explicitly
DUCKDB_VERSION=1.5.1 ./download_libduckdb.sh

# 2. Build and Install (USE_PGXS is auto-detected)
make
sudo make install
```

### Requirements
* PostgreSQL 13 - 18 (headers required)
* DuckDB library (`libduckdb.so` or `libduckdb.dylib`) with repo-pinned bootstrap default `1.5.1`
* GCC or Clang with C11/C++11 support

## 🛠️ Usage

### 1. Initialize
```sql
CREATE EXTENSION duckdb_fdw;

CREATE SERVER duckdb_srv 
FOREIGN DATA WRAPPER duckdb_fdw 
OPTIONS (database '/tmp/duckdb_fdw_demo.db');
```

`database ':memory:'` 是连接级临时库。现在 `duckdb_fdw` 会在事务结束时刷新缓存连接，因此如果你要先用 `duckdb_execute(...)` 建表/建视图，再在后续 SQL 语句里通过外表读取它们，默认应使用文件型 DuckDB 数据库；如果确实想用 `:memory:`，请把整段建模和查询包在同一个显式事务里。

### 1.1 `pg_duckdb` 共存策略

`duckdb_fdw` v2.0.1 起默认启用严格的 runtime coexistence guard：

- Linux-first 检测当前 backend 是否已经加载 `pg_duckdb`
- 同一 backend 中一旦发现 peer-loaded `pg_duckdb`，默认阻断 DuckDB runtime 执行
- `duckdb_fdw` 不提供 v1 的公开 peer-loaded 成功路径
- 只保留一个显式的 unsupported override，且必须在扩展 load 后通过 session 级 `SET` 单独开启

诊断入口：

```sql
SELECT duckdb_fdw_runtime_compatibility_status();
SELECT duckdb_fdw_runtime_fingerprint();
SELECT duckdb_fdw_preflight();
```

实验性 override：

```sql
LOAD 'duckdb_fdw';
SET duckdb_fdw.allow_unsupported_pg_duckdb_coexistence = on;
```

这个 override 明确不属于公开支持合同。它默认关闭，不允许 preload placeholder，也不允许通过 `SET LOCAL` 藏在事务里。

Linux-first 共存验证脚本：

```bash
./scripts/verify_pg_duckdb_coexistence.sh
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

### 4. High-Speed S3 Tables (Lakehouse)
`duckdb_fdw` v2.0+ handles **AWS S3 Tables** with zero configuration. It automatically detects `arn:aws:s3tables` URIs and injects the required `sigv4` authorization.

```sql
CREATE SERVER lakehouse_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database '/tmp/duckdb_fdw_lakehouse.db',
    s3_region 'us-east-1',
    s3_access_key_id 'YOUR_KEY',
    s3_secret_access_key 'YOUR_SECRET',
    -- Attach S3 Table catalog (endpoint and auth are auto-injected)
    attach_catalogs 'my_res=arn:aws:s3tables:us-east-1:12345678:bucket/my-table;type iceberg'
);

-- Automated schema discovery using DESCRIBE
IMPORT FOREIGN SCHEMA "my_res" FROM SERVER lakehouse_srv INTO public;

-- Query natively with predicate pushdown
SELECT * FROM part WHERE p_partkey = 1;
```

## 📉 Feature Comparison

| Feature | v1.x (Legacy) | v2.0+ (Native) |
| :--- | :--- | :--- |
| **Kernel Interface** | SQLite Compatibility | **Native DuckDB C API** |
| **Data Transfer** | Row-by-row | **Chunk-based result scan via DuckDB C API** |
| **Type Mapping** | Limited (Text-heavy) | **Full (Decimal/HugeInt/etc)** |
| **Cloud Security** | Plaintext Keys | **Integrated Secret Manager** |
| **Performance** | Basic | **Filter & Limit Pushdown** |

## 🤝 Contributing
Contributions are welcome. Current high-priority areas are production hardening, deterministic regression coverage, and eventually a true Arrow C Data read path.

## 📄 License
[MIT License](LICENSE)
