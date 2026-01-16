#!/bin/bash
set -e

echo "=== 1. Building Extension ==="
make USE_PGXS=1 clean
make USE_PGXS=1

echo "=== 2. Restarting Postgres ==="
/usr/lib/postgresql/15/bin/pg_ctl -D test_db stop || true
/usr/lib/postgresql/15/bin/pg_ctl -D test_db -l logfile -o "-p 5433 -k /tmp" start

echo "=== 3. Running Full Functional Test ==="
/usr/lib/postgresql/15/bin/psql -p 5433 -h /tmp -d postgres -c "
-- Cleanup
DROP FOREIGN TABLE IF EXISTS full_test CASCADE;
DROP SERVER IF EXISTS duck_srv CASCADE;
DROP FOREIGN DATA WRAPPER IF EXISTS duckdb_fdw CASCADE;

-- Load fresh SO
CREATE FUNCTION duckdb_fdw_handler() RETURNS fdw_handler AS '$(pwd)/duckdb_fdw.so' LANGUAGE C STRICT;
CREATE FUNCTION duckdb_fdw_validator(text[], oid) RETURNS void AS '$(pwd)/duckdb_fdw.so' LANGUAGE C STRICT;
CREATE FUNCTION duckdb_execute(server name, query text) RETURNS void AS '$(pwd)/duckdb_fdw.so' LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER duckdb_fdw HANDLER duckdb_fdw_handler VALIDATOR duckdb_fdw_validator;
CREATE SERVER duck_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database '/tmp/work.db');

-- Prepare complex data in DuckDB
SELECT duckdb_execute('duck_srv', 'CREATE OR REPLACE TABLE full_data AS SELECT 
    42 as id, 
    true as ok, 
    [1, 2, 3] as arr, 
    CAST(''{"val": 100}'' AS JSON) as js');

-- Map to Foreign Table
CREATE FOREIGN TABLE full_test (
    id INT4,
    ok BOOL,
    arr INT4[],
    js JSONB
) SERVER duck_srv OPTIONS (table 'full_data');

-- Query
SELECT * FROM full_test;
"
