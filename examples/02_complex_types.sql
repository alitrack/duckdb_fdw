-- Ensure Setup
CREATE SERVER IF NOT EXISTS duck_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:');

-- 1. Create Data with Array and JSON in DuckDB
SELECT duckdb_execute('duck_srv', 'CREATE OR REPLACE TABLE complex_demo AS 
    SELECT 
        [1, 2, 3] as my_array, 
        CAST(''{"tag": "powerful", "speed": "vectorized"}'' AS JSON) as my_json');

-- 2. Map to Foreign Table
CREATE FOREIGN TABLE complex_demo (
    my_array INT4[],
    my_json JSONB
) SERVER duck_srv OPTIONS (table 'complex_demo');

-- 3. Query - See the power of new conversion layer
SELECT * FROM complex_demo;
