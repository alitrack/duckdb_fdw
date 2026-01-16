-- Basic Extension Setup
CREATE EXTENSION duckdb_fdw;

-- Create Server
CREATE SERVER duckdb_test FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database ':memory:');

-- Test version function
SELECT duckdb_fdw_version() IS NOT NULL;

-- Create a table in DuckDB via DDL function
SELECT duckdb_execute('duckdb_test', 'CREATE TABLE test_types (i INTEGER, j BIGINT, d DOUBLE, s VARCHAR)');
SELECT duckdb_execute('duckdb_test', 'INSERT INTO test_types VALUES (1, 100, 3.14, ''hello''), (2, 200, 6.28, ''world'')');

-- Map it to PostgreSQL
CREATE FOREIGN TABLE test_types (
    i INT4,
    j INT8,
    d FLOAT8,
    s TEXT
) SERVER duckdb_test OPTIONS (table 'test_types');

-- Basic Query
SELECT * FROM test_types ORDER BY i;

-- Filter pushdown test
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM test_types WHERE i = 1;
SELECT * FROM test_types WHERE i = 1;

-- Cleanup
DROP FOREIGN TABLE test_types;
DROP SERVER duckdb_test CASCADE;
DROP EXTENSION duckdb_fdw;
