-- Clean environment
DROP EXTENSION IF EXISTS duckdb_fdw CASCADE;
CREATE EXTENSION duckdb_fdw;

-- 1. 动态挂载
CREATE SERVER iceberg_perf_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database '/tmp/duckdb_fdw_iceberg_direct.db',
    attach_catalogs 'lake=https://blobs.duckdb.org/datalake/tpch-sf3.ducklake;type ducklake'
);

-- 2. 直接映射到 DuckLake 查询片段，避免连接级 ATTACH 状态漂移
CREATE FOREIGN TABLE lake_summary (
    l_orderkey BIGINT,
    l_quantity float8
) SERVER iceberg_perf_srv OPTIONS (table '(SELECT l_orderkey, l_quantity FROM lake.main.lineitem LIMIT 1000)');

-- 3. 在 Postgres 中执行最终查询
SELECT count(*), sum(l_quantity) FROM lake_summary;
