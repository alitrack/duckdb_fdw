# Contributing

## Prerequisites

- PostgreSQL server headers and `pg_config`
- A C/C++ toolchain compatible with PGXS
- DuckDB client library and headers
- Optional: Docker and Docker Compose for isolated validation

## Bootstrap

Use the repository bootstrap script to download the default pinned DuckDB library:

```bash
./download_libduckdb.sh
```

To test another DuckDB release explicitly:

```bash
DUCKDB_VERSION=1.4.4 ./download_libduckdb.sh
```

## Build

```bash
make USE_PGXS=1
make USE_PGXS=1 install
```

If `pg_config` is missing, local compilation will fail until PostgreSQL development packages are installed.

## Validation

Preferred validation sequence:

```bash
openspec validate --specs --strict
git diff --check
```

If a working PostgreSQL environment is available:

```bash
make USE_PGXS=1
make USE_PGXS=1 installcheck
```

If Docker image resolution is working, containerized validation is also acceptable. In the current environment, Docker pulls may fail due upstream mirror/network issues, so treat that as an environment blocker rather than immediate code failure.

## Test Entry Points

- `sql/duckdb_fdw.sql` + `expected/duckdb_fdw.out`: baseline regression surface
- `run_tests.sh`: profile-driven example execution (`core`, `integration`, `cloud`, `all`)
- `build_and_test.sh`: older end-to-end helper, retained for now but not the preferred primary path

## Change Workflow

- Keep behavior claims aligned with code and regression evidence.
- Use OpenSpec changes for non-trivial work, then archive them once the slice is complete.
- Follow the repository Lore commit protocol for every commit.
