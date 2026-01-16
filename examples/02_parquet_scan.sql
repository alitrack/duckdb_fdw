-- Clean environment
DROP EXTENSION IF EXISTS duckdb_fdw CASCADE;
CREATE EXTENSION duckdb_fdw;

CREATE SERVER duck_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:');

-- Create a Parquet file via DuckDB
SELECT duckdb_execute('duck_srv', 'COPY (SELECT i as id, ''data_'' || i as name FROM range(5) t(i)) TO ''/tmp/test.parquet'' (FORMAT PARQUET)');

-- Scan it directly
CREATE FOREIGN TABLE parquet_scan (
    id INT,
    name TEXT
) SERVER duck_srv OPTIONS (table '/tmp/test.parquet');

SELECT * FROM parquet_scan;
