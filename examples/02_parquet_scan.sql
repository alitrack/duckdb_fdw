-- 1. 准备环境
CREATE OR REPLACE FUNCTION duckdb_fdw_handler() RETURNS fdw_handler AS '/home/coder/workspace/pg_duck/duckdb_fdw.so', 'duckdb_fdw_handler' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION duckdb_execute(server name, query text) RETURNS void AS '/home/coder/workspace/pg_duck/duckdb_fdw.so', 'duckdb_execute' LANGUAGE C STRICT;

DROP FOREIGN DATA WRAPPER IF EXISTS duckdb_fdw CASCADE;
CREATE FOREIGN DATA WRAPPER duckdb_fdw HANDLER duckdb_fdw_handler;
CREATE SERVER duck_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:');

-- 2. 导出一些数据到 Parquet (模拟外部文件)
-- 您也可以直接使用现有的 Parquet 文件路径
SELECT duckdb_execute('duck_srv', 'COPY (SELECT range as id, ''data_'' || range as name FROM range(5)) TO ''/tmp/test.parquet'' (FORMAT PARQUET)');

-- 3. 在 DuckDB 中创建一个 VIEW 映射 Parquet
-- 这是最灵活的方法，可以利用 DuckDB 强大的计算能力
SELECT duckdb_execute('duck_srv', 'CREATE VIEW v_parquet AS SELECT * FROM read_parquet(''/tmp/test.parquet'')');

-- 4. 在 Postgres 中映射该 VIEW
CREATE FOREIGN TABLE parquet_scan (
    id INT8,
    name TEXT
) SERVER duck_srv OPTIONS (table 'v_parquet');

-- 5. 直接查询
SELECT * FROM parquet_scan;
