# DuckDB Foreign Data Wrapper for PostgreSQL (v2.0.1+)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Postgres](https://img.shields.io/badge/PostgreSQL-13--18-blue.svg)](https://www.postgresql.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.x-orange.svg)](https://duckdb.org/)

**`duckdb_fdw`** v2.0+ is a high-performance PostgreSQL extension that bridges PostgreSQL's ecosystem with DuckDB's vectorized analytical power. Built natively on the **DuckDB C API**, it supports modern Lakehouse workflows including Parquet, Iceberg, S3 Tables, DuckLake, MotherDuck, and the Quack client-server protocol. PostgreSQL 13-18 are supported, and the project now includes a CMake-based build path for Windows deployments.

---

## 🚀 Capability Status (v2.0.1)

| Capability | Status | Validation Evidence | Prerequisites |
| :--- | :--- | :--- | :--- |
| Native DuckDB C API integration | Implemented | `duckdb_fdw.c`, `connection.c` | DuckDB shared library |
| Chunk-based scan iteration | Implemented | `duckdbBeginForeignScan`, `duckdbIterateForeignScan` | PostgreSQL 13+ |
| Prepared parameter binding (`?`) | Implemented | `duckdb_execute_query` bind path | Pushdown query with params |
| Appender insert path | Implemented | `duckdbBeginForeignModify`, `duckdbExecForeignInsert` | Writable foreign table |
| Batch insert hooks (PG14+) | Implemented | `ExecForeignBatchInsert`, `GetForeignModifyBatchSize` | PostgreSQL 14+ |
| Secret helper (`duckdb_create_s3_secret`) | Implemented | SQL function + `duckdb_fdw.c` | S3 credentials |
| MotherDuck integration | Implemented | `motherduck_token` option + auto-extension loading | MotherDuck account |
| Quack client-server protocol | Implemented | via `extensions 'quack'` + `duckdb_execute` | DuckDB with Quack extension |
| Runtime coexistence guard for `pg_duckdb` | Implemented (Linux-first) | `runtime_guard.c`, `scripts/verify_pg_duckdb_coexistence.sh` | Same-backend peer detection |
| Windows build and deployment path | Implemented | `CMakeLists.txt`, `install_duckdb_fdw.ps1` | PostgreSQL for Windows, CMake |
| Import foreign schema for tables and views | Implemented | `import.c` | Remote schema with objects |
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

### Quick Build (Linux/macOS) Legacy

```bash
# Optional: prepare PostgreSQL development prerequisites on Debian/Ubuntu/WSL
scripts/install_pg_env.sh --pg-major 17
scripts/install_pg_env.sh --pg-major 17 --apply
scripts/verify_pg_env.sh --pg-major 17

# 1. Download DuckDB headers and library
./download_libduckdb.sh

# Or pin a specific DuckDB release explicitly
DUCKDB_VERSION=1.5.3 ./download_libduckdb.sh

# 2. Build and Install (USE_PGXS is auto-detected)
make
sudo make install
```

### Quick Build Windows (It also works for Linux)

On Windows, `duckdb_fdw` can be built as a DLL with CMake and deployed into a local PostgreSQL installation with the included PowerShell helper.

```powershell
# 1. Download DuckDB headers and library
# Run this from Git Bash, MSYS2, or WSL
.\download_libduckdb.ps1
or
./download_libduckdb.sh

# 2. Configure and build from a Developer Command Prompt or terminal with MSVC/Ninja available
mkdir build
cd build
cmake -G "Ninja" -DCMAKE_BUILD_TYPE=Release ..
cmake --build . --config Release

# 3. Install into PostgreSQL
cd ..
.\install_duckdb_fdw.ps1 -PostgresBase "C:\Program Files\PostgreSQL\18"
or
./install_duckdb_fdw.sh
```

The PowerShell installer copies:

- `duckdb_fdw.dll` into `<PostgresBase>\lib`
- `duckdb_fdw.control` and `duckdb_fdw--*.sql` into `<PostgresBase>\share\extension`
- `duckdb.dll` into `<PostgresBase>\bin` when present


### Requirements

- PostgreSQL 13 - 18 (headers required)
- DuckDB shared library (`libduckdb.so`, `libduckdb.dylib`, or `duckdb.dll`) with repo-pinned bootstrap default `1.5.1`
- GCC or Clang with C11/C++11 support on Unix-like systems, or MSVC-compatible CMake toolchain on Windows

## 🛠️ Usage

### 1. Initialize

```sql
CREATE EXTENSION duckdb_fdw;

CREATE SERVER duckdb_srv
FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/duckdb_fdw_demo.db');
```

`database ':memory:'` is a connection-scoped temporary database. `duckdb_fdw` refreshes cached connections at transaction end, so if you create tables or views with `duckdb_execute(...)` and then read them through foreign tables in later SQL statements, use a file-backed DuckDB database by default. If you intentionally want `:memory:`, wrap the entire modeling and query sequence in the same explicit transaction.

### 2. `pg_duckdb` Coexistence Policy

`duckdb_fdw` v2.0.1 enables a strict runtime coexistence guard by default:

- Linux-first detection checks whether the current backend has already loaded `pg_duckdb`
- If peer-loaded `pg_duckdb` is detected in the same backend, DuckDB runtime execution is blocked by default
- `duckdb_fdw` no longer exposes the v1 public success path for peer-loaded coexistence
- Only one explicit unsupported override remains, and it must be enabled with a session-level `SET` after the extension is loaded

Diagnostics:

```sql
SELECT duckdb_fdw_runtime_compatibility_status();
SELECT duckdb_fdw_runtime_fingerprint();
SELECT duckdb_fdw_preflight();
```

Experimental override:

```sql
LOAD 'duckdb_fdw';
SET duckdb_fdw.allow_unsupported_pg_duckdb_coexistence = on;
```

This override is explicitly outside the supported public contract. It is off by default, does not allow preload placeholders, and cannot be hidden inside a transaction with `SET LOCAL`.

On Windows, the extension currently enables the unsupported coexistence override during `_PG_init()` to simplify DLL loading scenarios. This should still be treated as platform-specific compatibility behavior rather than a supported coexistence guarantee.

Linux-first coexistence verification script:

```bash
./scripts/verify_pg_duckdb_coexistence.sh
```

### 3. Configure Cloud Credentials

#### S3 (Recommended: USER MAPPING)

For security, prefer storing S3 credentials in **USER MAPPING** rather than server options. `pg_foreign_server` options are public-readable by default; `USER MAPPING` credentials are only visible to the mapped user and superusers.

```sql
CREATE SERVER s3_srv FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (
    database '/tmp/s3_cache.db',
    s3_region 'us-east-1'
);

-- Secure: credentials in USER MAPPING (not visible to other users)
CREATE USER MAPPING FOR current_user SERVER s3_srv
OPTIONS (
    s3_access_key_id 'YOUR_KEY',
    s3_secret_access_key 'YOUR_SECRET'
);
```

#### S3 (Legacy: Server Options)

```sql
CREATE SERVER s3_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database '/tmp/s3_cache.db',
    s3_region 'us-east-1',
    s3_access_key_id 'YOUR_KEY',
    s3_secret_access_key 'YOUR_SECRET'
);
```

#### S3 URL Style

Some S3-compatible endpoints require explicit path-style or virtual-host-style access. Use `s3_url_style` to control that behavior.

```sql
CREATE SERVER s3_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database '/tmp/s3_cache.db',
    s3_region 'us-east-1',
    s3_endpoint 'https://minio.local:9000',
    s3_url_style 'path'
);
```

Supported values are typically `path` or `vhost`, depending on the target object store.

#### Secret Helper Function

```sql
SELECT duckdb_create_s3_secret('s3_srv', 'my_s3_key', 'YOUR_KEY', 'YOUR_SECRET', 'us-east-1');
```

### 4. Connection Initialization Options

`duckdb_fdw` supports server-level connection options for read-only access and custom initialization SQL.

```sql
CREATE SERVER duckdb_ro_srv
FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (
    database '/tmp/analytics.db',
    read_only 'true',
    init_sql $$
      INSTALL httpfs;
      LOAD httpfs;
    $$
);
```

- `read_only`: opens the DuckDB database using `access_mode=read_only`
- `init_sql`: executes arbitrary DuckDB SQL after the connection is established; useful for `INSTALL`, `LOAD`, or session settings

### 5. Query Data Lake (Parquet / Iceberg / DuckLake)

```sql
-- Direct scan of S3 Parquet
CREATE FOREIGN TABLE s3_data (
    id INT,
    price DECIMAL
) SERVER s3_srv
OPTIONS (table 's3://my-bucket/data.parquet');

-- Import whole DuckLake or Iceberg schema
CREATE SCHEMA remote_tpch;
IMPORT FOREIGN SCHEMA "tpch" FROM SERVER s3_srv INTO remote_tpch;
```

**DuckLake** catalogs (`type=ducklake`) are auto-detected. Just point `attach_catalogs` at a DuckLake URL and `duckdb_fdw` automatically loads the Iceberg extension.

```sql
CREATE SERVER ducklake_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database '/tmp/dl.db',
    attach_catalogs 'tpch=https://blobs.duckdb.org/datalake/tpch-sf3.ducklake;type ducklake'
);

IMPORT FOREIGN SCHEMA "tpch" FROM SERVER ducklake_srv INTO public;
```

### 6. High-Speed S3 Tables (Lakehouse)

`duckdb_fdw` v2.0+ handles **AWS S3 Tables** with zero configuration. It automatically detects `arn:aws:s3tables` URIs and injects the required `sigv4` authorization.

```sql
CREATE SERVER lakehouse_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database '/tmp/duckdb_fdw_lakehouse.db',
    s3_region 'us-east-1',
    attach_catalogs 'my_res=arn:aws:s3tables:us-east-1:12345678:bucket/my-table;type iceberg'
);

IMPORT FOREIGN SCHEMA "my_res" FROM SERVER lakehouse_srv INTO public;
SELECT * FROM part WHERE p_partkey = 1;
```

### 7. MotherDuck

> Requires: DuckDB >= 1.1 with `motherduck` extension available.

Set your MotherDuck token in **USER MAPPING** (recommended for security) or server options. The extension is auto-installed and a MotherDuck SECRET is created automatically on connection.

```sql
CREATE SERVER md_srv FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/md_local.db');

CREATE USER MAPPING FOR current_user SERVER md_srv
OPTIONS (motherduck_token 'your_motherduck_token');

-- Attach a MotherDuck database
SELECT duckdb_execute('md_srv', $$ATTACH 'md:my_db' AS my_md;$$);

-- Map MotherDuck tables as PG foreign tables
IMPORT FOREIGN SCHEMA "my_md" FROM SERVER md_srv INTO public;

-- Or query MotherDuck directly via duckdb_execute
SELECT duckdb_execute('md_srv', 'CREATE TABLE my_md.my_table AS SELECT 42 AS answer');
```

You can also reference MotherDuck databases via `attach_catalogs`:

```sql
CREATE SERVER md_catalog_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database '/tmp/md_catalog.db',
    motherduck_token 'your_token',
    attach_catalogs 'my_db=md:my_db'
);

IMPORT FOREIGN SCHEMA "my_db" FROM SERVER md_catalog_srv INTO public;
```

### 8. Quack (Client-Server + Multi-Client Concurrent Writes)

> Requires: DuckDB >= 1.5.2 with `quack` extension available. See [Quack documentation](https://duckdb.org/quack/).

Quack is DuckDB's native client-server protocol. `duckdb_fdw` supports it in two modes.

#### Start the Quack Server

```bash
duckdb /path/to/shared.db -cmd "LOAD quack; SELECT * FROM quack_serve('quack://0.0.0.0:9494', token := 'shared_secret', allow_other_hostname := true);"
```

> **Prerequisite**: The PostgreSQL process needs a writable DuckDB home directory (`$HOME/.duckdb/`) to cache extensions. If you see `Can't find the home directory at ''`, either set `HOME` in the PG service (`systemctl edit postgresql` → `Environment=HOME=/var/lib/postgresql`) or use Manual Mode with a pre-created database file.

#### Native Proxy Mode (Recommended)

Set `quack_host` in the server options. `duckdb_fdw` automatically opens a local in-memory DuckDB, loads the Quack extension, creates a Quack SECRET, and `ATTACH`es the remote server. All foreign tables are created as `remote.schema.table`, so queries are transparently routed to the Quack server.

```sql
CREATE SERVER quack_srv FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (quack_host '192.168.1.10:9494');

CREATE USER MAPPING FOR current_user SERVER quack_srv
OPTIONS (quack_token 'shared_secret');

IMPORT FOREIGN SCHEMA main FROM SERVER quack_srv INTO public;
SELECT * FROM orders WHERE amount > 1000;
INSERT INTO orders VALUES (1, 99.9, '2026-05-29');
```

This mode solves the classic DuckDB concurrency problem: **multiple PG backends can concurrently read and write the same DuckDB database** through a single Quack server process. All PG clients share one `.duckdb` file without lock conflicts.

You can additionally disable TLS in Quack proxy mode when the deployment environment requires it:

```sql
CREATE SERVER quack_insecure_srv FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (
    quack_host '192.168.1.10:9494',
    disable_ssl 'true'
);
```

`IMPORT FOREIGN SCHEMA` now discovers both base tables and views through `information_schema.tables`. In Quack proxy mode, discovery and import resolution are performed using `remote.query(...)` and `remote.schema.table` references.

#### Manual Mode

For more control, use `duckdb_execute` to manage the Quack connection yourself. This mode also works around the home directory issue on machines where the PG process cannot set `HOME`.

```sql
CREATE SERVER quack_srv FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/quack_local.db', extensions 'quack');

SELECT duckdb_execute('quack_srv',
  $$LOAD quack;
    CREATE SECRET (TYPE quack, TOKEN 'your_token');
    ATTACH 'quack:remote-host:9494' AS remote_db;$$);
```

### 9. Arbitrary DuckDB SQL

The `duckdb_execute()` function lets you run any DuckDB SQL through the FDW connection, which is useful for DDL, extension management, or one-off operations.

```sql
SELECT duckdb_execute('duckdb_srv', 'CREATE TABLE tmp AS SELECT range AS id FROM range(1000)');
SELECT duckdb_execute('duckdb_srv', 'INSTALL spatial; LOAD spatial;');
```

Result messages containing credentials (`SECRET`, `KEY_ID`, `ACCESS_KEY`, `TOKEN`, `motherduck`) are automatically redacted in error output for security.

## 📉 Feature Comparison

| Feature | v1.x (Legacy) | v2.0+ (Native) |
| :--- | :--- | :--- |
| **Kernel Interface** | SQLite Compatibility | **Native DuckDB C API** |
| **Data Transfer** | Row-by-row | **Chunk-based result scan via DuckDB C API** |
| **Type Mapping** | Limited (Text-heavy) | **Full (Decimal/HugeInt/etc)** |
| **Cloud Security** | Plaintext Keys | **Integrated Secret Manager + USER MAPPING** |
| **Performance** | Basic | **Filter & Limit Pushdown** |
| **MotherDuck** | ❌ | ✅ Auto-extension + token management |
| **Quack** | ❌ | ✅ via `extensions 'quack'` (zero code changes) |
| **Windows build path** | ❌ | ✅ via CMake + PowerShell installer |

## 🦆 How is this different from `pg_duckdb`?

| | duckdb_fdw | pg_duckdb |
|---|---|---|
| **Direction** | PG → DuckDB data lake | DuckDB engine → PG tables |
| **What it does** | Lets PG query DuckDB's world (Parquet, Iceberg, S3, MotherDuck, Quack) | Embeds DuckDB engine inside PG to accelerate PG-native queries |
| **Deployment** | `CREATE EXTENSION` — no restart needed | `shared_preload_libraries` — requires PG restart |
| **Data lake access** | FOREIGN TABLE + IMPORT FOREIGN SCHEMA | `read_parquet()` DuckDB functions |
| **Learning curve** | Standard PG SQL | DuckDB-specific syntax for data lake functions |
| **Codebase** | ~6,500 lines C | ~81,000 lines C++ |
| **Can they coexist?** | Not in the same backend on Linux; Windows behavior is compatibility-oriented but unsupported | — |

**They solve different problems.** `pg_duckdb` asks "how can DuckDB make PostgreSQL faster?" `duckdb_fdw` asks "how can PostgreSQL users access everything DuckDB can read?" If you need both, run them on separate PG instances.

## 🤝 Contributing

Contributions are welcome. Current high-priority areas are production hardening, deterministic regression coverage, improved Windows CI validation, and eventually a true Arrow C Data read path.

## 🔧 Troubleshooting

### "Can't find the home directory at ''" (Quack mode)

DuckDB 1.5+ needs a writable `$HOME/.duckdb/` to cache extensions. The PG backend runs as the `postgres` user, which may not have `HOME` set.

**Fix A**: Set `HOME` in the PG service:

```bash
sudo systemctl edit postgresql
# Add: [Service] Environment=HOME=/var/lib/postgresql
sudo systemctl restart postgresql
```

**Fix B**: Use Manual Mode with a pre-created DB file (bypasses in-memory DuckDB):

```sql
CREATE SERVER quack_srv FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/quack_fdw.db', extensions 'quack');
```

### Compilation error: "implicit declaration of function 'GetUserId'" (PG 17)

PostgreSQL 17 requires explicit `#include "miscadmin.h"` for `GetUserId()`. This is fixed in the main branch, so ensure the checkout includes the PG17 compatibility fix already present upstream.

### PostgreSQL 18 build errors around `create_foreignscan_path`

PostgreSQL 18 changed the `create_foreignscan_path` signature to include the `disabled_nodes` argument. The extension now handles this with version guards, so verify that the build includes the PG18 compatibility changes before compiling.

### Downloading libduckdb times out (GitHub behind proxy)

```bash
export HTTP_PROXY=http://your-proxy:port HTTPS_PROXY=http://your-proxy:port
./download_libduckdb.sh
```

### ARM64 release asset naming mismatch

The DuckDB release asset download helper resolves Linux ARM64 builds using the `arm64` artifact suffix. If downloads fail on `aarch64`/`arm64`, verify that the script is up to date and targets the expected DuckDB release naming.

## 📄 License

[MIT License](LICENSE)
