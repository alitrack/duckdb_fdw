-- 18 High Performance v2.0 Test
-- This script tests Arrow chunking (2048 rows) and Batch Appender performance

CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

CREATE SERVER perf_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:');

-- 1. Test Vectorized Reading (Arrow)
-- Create a table with 10,000 rows (approx 5 Arrow chunks)
SELECT duckdb_execute('perf_srv', 'CREATE TABLE big_data AS SELECT 
    range as id, 
    range * 1.5 as val,
    ''label_'' || (range % 10)::VARCHAR as tag
    FROM range(10000)');

CREATE FOREIGN TABLE big_data_remote (
    id INT,
    val FLOAT8,
    tag TEXT
) SERVER perf_srv OPTIONS (table 'big_data');

-- Query should trigger Arrow chunk rotation multiple times
SELECT tag, SUM(val), COUNT(*) 
FROM big_data_remote 
GROUP BY tag 
ORDER BY tag;

-- 2. Test High-Speed Batch Ingestion (Appender)
SELECT duckdb_execute('perf_srv', 'CREATE TABLE target_table (id INT, val FLOAT8, tag TEXT)');

CREATE FOREIGN TABLE target_remote (
    id INT,
    val FLOAT8,
    tag TEXT
) SERVER perf_srv OPTIONS (table 'target_table');

-- This uses the Batch Insert API (PG 14+)
INSERT INTO target_remote 
SELECT id, val, tag FROM big_data_remote;

-- Verify count
SELECT COUNT(*) FROM target_remote;

-- 3. Advanced Aggregate Pushdown
EXPLAIN (VERBOSE, COSTS OFF)
SELECT 
    stddev(val) as sdev, 
    variance(val) as var,
    sqrt(avg(val)) as root_avg
FROM big_data_remote;

SELECT 
    round(stddev(val)::numeric, 2) as sdev, 
    round(variance(val)::numeric, 2) as var
FROM big_data_remote;

-- Cleanup
DROP FOREIGN TABLE big_data_remote;
DROP FOREIGN TABLE target_remote;
DROP SERVER perf_srv CASCADE;
