-- 1. Setup Extension
CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

-- 2. Create Server (Using memory mode for quick demo)
CREATE SERVER duck_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:');

-- 3. Execute DuckDB Native DDL
-- This shows we can control DuckDB directly from Postgres
SELECT duckdb_execute('duck_srv', 'CREATE TABLE t1 (id INTEGER, name VARCHAR)');
SELECT duckdb_execute('duck_srv', 'INSERT INTO t1 VALUES (1, ''Postgres''), (2, ''DuckDB'')');

-- 4. Map to Foreign Table
CREATE FOREIGN TABLE t1 (
    id INT4,
    name TEXT
) SERVER duck_srv OPTIONS (table 't1');

-- 5. Query
SELECT * FROM t1;
