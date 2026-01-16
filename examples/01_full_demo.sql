-- duckdb_fdw 生产级演示脚本
-- 1. 注册 FDW
CREATE OR REPLACE FUNCTION duckdb_fdw_handler() RETURNS fdw_handler AS '/home/coder/workspace/pg_duck/duckdb_fdw.so', 'duckdb_fdw_handler' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION duckdb_execute(server name, query text) RETURNS void AS '/home/coder/workspace/pg_duck/duckdb_fdw.so', 'duckdb_execute' LANGUAGE C STRICT;

DROP FOREIGN DATA WRAPPER IF EXISTS duckdb_fdw CASCADE;
CREATE FOREIGN DATA WRAPPER duckdb_fdw HANDLER duckdb_fdw_handler;

-- 2. 创建 Server (连接到磁盘数据库)
CREATE SERVER duck_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database '/tmp/production.db');

-- 3. 准备数据
SELECT duckdb_execute('duck_srv', 'CREATE OR REPLACE TABLE sales AS SELECT 
    101 as id, 
    ''Order_A'' as item, 
    CAST(99.50 AS DOUBLE) as price,
    true as is_shipped');

-- 4. 映射并查询
CREATE FOREIGN TABLE sales_fdw (
    id INT4,
    item TEXT,
    price FLOAT8,
    is_shipped BOOL
) SERVER duck_srv OPTIONS (table 'sales');

-- 5. 见证奇迹
SELECT * FROM sales_fdw;
