-- 16 Aggregate Pushdown
-- This example demonstrates pushing down aggregation functions (SUM, AVG, COUNT, MIN, MAX) 
-- to DuckDB, which is much more efficient than fetching all rows and aggregating in PostgreSQL.

CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

-- 1. Setup Server
DROP SERVER IF EXISTS agg_srv CASCADE;
CREATE SERVER agg_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:');

-- 2. Create source data in DuckDB
SELECT duckdb_execute('agg_srv', '
    CREATE TABLE sales_data (
        category TEXT, 
        amount DOUBLE, 
        quantity INTEGER,
        sale_date DATE
    )');

SELECT duckdb_execute('agg_srv', '
    INSERT INTO sales_data SELECT 
        ''Category '' || (i % 5), 
        random() * 100, 
        (random() * 10)::int,
        ''2024-01-01''::date + (i % 365)::int
    FROM generate_series(1, 10000) s(i)');

-- 3. Map to Foreign Table
CREATE FOREIGN TABLE sales_remote (
    category TEXT,
    amount FLOAT8,
    quantity INT4,
    sale_date DATE
) SERVER agg_srv OPTIONS (table 'sales_data');

-- 4. Verify Aggregate Pushdown
-- We use EXPLAIN VERBOSE to see if the query sent to DuckDB contains GROUP BY and aggregate functions.

-- Test 1: Simple Count
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM sales_remote;

SELECT count(*) FROM sales_remote;

-- Test 2: Group By and Multiple Aggregates
EXPLAIN (VERBOSE, COSTS OFF)
SELECT 
    category, 
    count(*) as num_sales,
    sum(amount) as total_amount,
    avg(quantity) as avg_qty,
    min(sale_date) as first_sale,
    max(sale_date) as last_sale
FROM sales_remote
GROUP BY category
ORDER BY category;

SELECT 
    category, 
    count(*) as num_sales,
    sum(amount) as total_amount,
    avg(quantity) as avg_qty,
    min(sale_date) as first_sale,
    max(sale_date) as last_sale
FROM sales_remote
GROUP BY category
ORDER BY category;

-- Test 3: Aggregation with Filters (Pushdown check)
EXPLAIN (VERBOSE, COSTS OFF)
SELECT 
    category, 
    sum(amount)
FROM sales_remote
WHERE amount > 50
GROUP BY category
ORDER BY category;

SELECT 
    category, 
    sum(amount)
FROM sales_remote
WHERE amount > 50
GROUP BY category
ORDER BY category;
