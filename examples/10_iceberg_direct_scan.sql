-- Clean environment
DROP EXTENSION IF EXISTS duckdb_fdw CASCADE;
CREATE EXTENSION duckdb_fdw;

-- 1. 动态挂载
CREATE SERVER iceberg_perf_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database ':memory:'
);

SELECT duckdb_execute('iceberg_perf_srv', 'ATTACH ''https://blobs.duckdb.org/datalake/tpch-sf3.ducklake'' AS lake (TYPE DUCKLAKE)');

-- 2. 在 DuckDB 内部创建一个视图，预处理好我们想要的复杂逻辑
SELECT duckdb_execute('iceberg_perf_srv', 'CREATE VIEW v_lineitem_summary AS SELECT l_orderkey, l_quantity FROM lake.lineitem LIMIT 1000');

-- 3. 创建映射到该视图的外表
CREATE FOREIGN TABLE lake_summary (
    l_orderkey BIGINT,
    l_quantity float8
) SERVER iceberg_perf_srv OPTIONS (table 'v_lineitem_summary');

-- 4. 在 Postgres 中执行最终查询
SELECT count(*), sum(l_quantity) FROM lake_summary;