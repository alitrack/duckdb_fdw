-- Clean environment
DROP EXTENSION IF EXISTS duckdb_fdw CASCADE;
CREATE EXTENSION duckdb_fdw;

-- Full Demo
CREATE SERVER duck_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database '/tmp/duckdb_fdw_full_demo.db');

SELECT duckdb_execute('duck_srv', 'CREATE OR REPLACE TABLE sales (id INT, item TEXT, price DOUBLE, is_shipped BOOLEAN)');
SELECT duckdb_execute('duck_srv', 'INSERT INTO sales VALUES (101, ''Order_A'', 99.5, true)');

CREATE FOREIGN TABLE sales_fdw (
    id INT,
    item TEXT,
    price float8,
    is_shipped BOOLEAN
) SERVER duck_srv OPTIONS (table 'sales');

SELECT * FROM sales_fdw;
