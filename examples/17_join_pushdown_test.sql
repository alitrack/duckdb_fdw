-- 17 Join Pushdown Test
-- This example tests if joins between two DuckDB foreign tables are pushed down.

CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

-- 1. Setup Server
DROP SERVER IF EXISTS join_srv CASCADE;
CREATE SERVER join_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:');

-- 2. Create source tables in DuckDB
SELECT duckdb_execute('join_srv', 'CREATE TABLE t1 (id INT, val1 TEXT)');
SELECT duckdb_execute('join_srv', 'CREATE TABLE t2 (id INT, val2 TEXT)');

SELECT duckdb_execute('join_srv', 'INSERT INTO t1 VALUES (1, ''a''), (2, ''b'')');
SELECT duckdb_execute('join_srv', 'INSERT INTO t2 VALUES (1, ''x''), (3, ''y'')');

-- 3. Map to Foreign Tables
CREATE FOREIGN TABLE foreign_t1 (
    id INT,
    val1 TEXT
) SERVER join_srv OPTIONS (table 't1');

CREATE FOREIGN TABLE foreign_t2 (
    id INT,
    val2 TEXT
) SERVER join_srv OPTIONS (table 't2');

-- 4. Test Join Pushdown
-- If pushdown works, EXPLAIN should show a single Foreign Scan instead of two scans and a local join.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.id, t1.val1, t2.val2
FROM foreign_t1 t1
JOIN foreign_t2 t2 ON t1.id = t2.id;

SELECT t1.id, t1.val1, t2.val2
FROM foreign_t1 t1
JOIN foreign_t2 t2 ON t1.id = t2.id;
