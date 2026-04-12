-- 01 Quick Start
CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

CREATE SERVER duck_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database '/tmp/duckdb_fdw_quick_start.db');

SELECT duckdb_execute('duck_srv', 'CREATE OR REPLACE TABLE t1 (id INT, name TEXT)');
SELECT duckdb_execute('duck_srv', 'INSERT INTO t1 VALUES (1, ''Postgres''), (2, ''DuckDB'')');

CREATE FOREIGN TABLE t1 (
    id INT,
    name TEXT
) SERVER duck_srv OPTIONS (table 't1');

SELECT * FROM t1;
