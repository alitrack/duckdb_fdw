-- 14 Arrow Vectorized Types Support
-- This example demonstrates the stability and correctness of the new Arrow vectorized execution path
-- for various data types including Date, Timestamp, Bool, and Arrays.

CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

-- 1. Setup Server
DROP SERVER IF EXISTS arrow_srv CASCADE;
CREATE SERVER arrow_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database '/tmp/duckdb_fdw_arrow_vectorized.db');

-- 2. Create source data in DuckDB with mixed types
SELECT duckdb_execute('arrow_srv', '
    CREATE OR REPLACE TABLE arrow_source (
        id INTEGER, 
        b BOOLEAN, 
        d DATE, 
        ts TIMESTAMP, 
        arr INTEGER[], 
        f FLOAT, 
        dbl DOUBLE,
        s VARCHAR
    )');

SELECT duckdb_execute('arrow_srv', '
    INSERT INTO arrow_source VALUES 
    (1, true, ''2025-01-01'', ''2025-01-01 10:00:00'', [1, 2, 3], 1.23, 4.56789, ''Arrow Test 1''),
    (2, false, ''1990-05-20'', ''1960-01-01 00:00:00'', [4, 5], NULL, 0.0, ''Arrow Test 2''),
    (3, NULL, NULL, NULL, NULL, 3.14, -1.0, NULL)');

-- 3. Define Foreign Table mapping to the DuckDB table
DROP FOREIGN TABLE IF EXISTS arrow_types_test;
CREATE FOREIGN TABLE arrow_types_test (
    id INT,
    b BOOL,
    d DATE,
    ts TIMESTAMP WITHOUT TIME ZONE,
    arr INT[],
    f FLOAT4,
    dbl FLOAT8,
    s TEXT
) SERVER arrow_srv OPTIONS (table 'arrow_source');

-- 4. Verify results
-- This query uses the vectorized path (Arrow) to fetch data
SELECT * FROM arrow_types_test ORDER BY id;

-- 5. Test Filter Pushdown with these types
SELECT id, s FROM arrow_types_test WHERE b = true;
SELECT id, d FROM arrow_types_test WHERE d > '2000-01-01'::date;
