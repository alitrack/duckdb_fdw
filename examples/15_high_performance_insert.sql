-- 15 High Performance Insert using Appender API
-- This example demonstrates the new Appender-based write support which is
-- significantly faster than individual SQL INSERT statements.

CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

-- 1. Setup Server
DROP SERVER IF EXISTS write_srv CASCADE;
CREATE SERVER write_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:');

-- 2. Create target table in DuckDB
SELECT duckdb_execute('write_srv', 'CREATE TABLE bulk_target (id INT, val TEXT, d DATE, ok BOOLEAN)');

-- 3. Map to Foreign Table
CREATE FOREIGN TABLE bulk_test (
    id INT,
    val TEXT,
    d DATE,
    ok BOOLEAN
) SERVER write_srv OPTIONS (table 'bulk_target');

-- 4. Perform Bulk Insert
-- This now uses the vectorized Appender API internally
INSERT INTO bulk_test (id, val, d, ok)
SELECT 
    gs, 
    'Value ' || gs, 
    '2025-01-01'::date + (gs % 365),
    (gs % 2 = 0)
FROM generate_series(1, 5000) gs;

-- 5. Verify results
SELECT count(*), min(id), max(id), count(DISTINCT ok) FROM bulk_test;
SELECT * FROM bulk_test ORDER BY id LIMIT 10;

-- 6. Test NULL handling
INSERT INTO bulk_test (id, val, d, ok) VALUES (9999, NULL, NULL, NULL);
SELECT * FROM bulk_test WHERE id = 9999;
